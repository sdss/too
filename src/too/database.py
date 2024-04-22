#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: database.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import os
import pathlib
import warnings

import numpy
import polars
from erfa import ErfaWarning

from sdssdb.connection import PeeweeDatabaseConnection
from sdssdb.peewee.sdss5db import catalogdb
from sdsstools.time import get_sjd

from too import log
from too.datamodel import mag_columns, too_dtypes, too_metadata_columns
from too.exceptions import ValidationError
from too.tools import read_too_file
from too.validate import DesignMode, allDesignModes, bn_validation, mag_lim_validation


__all__ = [
    "connect_to_database",
    "get_database_uri",
    "validate_too_targets",
    "load_too_targets",
    "get_active_targets",
]

DEFAULT_USER: str = "sdss"
DEFAULT_HOST = "localhost"


def connect_to_database(
    dbname: str,
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
):
    """Connects the ``sdssdb`` ``sdss5db`` models to the database."""

    if port is None:
        port = int(os.environ.get("PGPORT", 5432))

    catalogdb.database.connect(
        dbname,
        host=host,
        port=port,
        user=user,
        password=password,
    )

    if not catalogdb.database.connected:
        raise RuntimeError("Could not connect to the database.")

    return catalogdb.database


def get_database_uri(
    dbname: str,
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
):
    """Returns the URI to the database."""

    user = user or str(os.environ.get("PGUSER", DEFAULT_USER))
    host = host or str(os.environ.get("PGHOST", DEFAULT_HOST))
    port = port or int(os.environ.get("PGPORT", 5432))

    if user is None and password is None:  # pragma: no cover
        # This should never happen. user is always set.
        auth: str = ""
    elif password is None:
        auth: str = f"{user}@"
    else:
        auth: str = f"{user}:{password}@"

    host_port: str = f"{host or ''}" if port is None else f"{host or ''}:{port}"

    return f"postgresql://{auth}{host_port}/{dbname}"


def database_uri_from_connection(database: PeeweeDatabaseConnection):
    """Returns a database URI from a connection."""

    return get_database_uri(database.dbname, **database.connect_params)


def worker(targets_bmode, design_modes, design_mode):
    try:
        with warnings.catch_warnings():
            # warnings.simplefilter("ignore", category=ErfaWarning)
            return bn_validation(
                targets_bmode,
                design_modes,
                design_mode,
                observatory="APO",
            )
    except Exception as err:
        log.error(err)
        return None


def validate_too_targets(
    targets: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    drop_bright_targets: bool = False,
):
    """Validates a list of ToO targets.

    Checks the following conditions:

    - The dataframe schema matches the ``too_target`` schema.
    - The ``too_id`` column is unique.
    - ``ra`` and ``dec`` are present and valid.
    - At least one of the magnitude columns is present.
    - The number of exposures is set and valid.
    - The ``active`` column is set.

    """

    n_targets = targets.height

    if targets.schema != too_dtypes:
        raise ValidationError("Invalid schema for ToO targets.")

    if targets.unique("too_id").height != n_targets:
        raise ValidationError("Duplicate too_id in ToO targets.")

    targets_coords = targets.select(["ra", "dec"]).drop_nulls()
    if len(targets_coords) < n_targets:
        raise ValidationError("Null ra/dec found in ToO targets.")

    bad_ra = (targets_coords["ra"] >= 360) | (targets_coords["ra"] < 0)
    bad_dec = (targets_coords["dec"] >= 90) | (targets_coords["dec"] <= -90)
    if bad_ra.any() or bad_dec.any():
        raise ValidationError("Invalid ra or dec found in ToO targets.")

    if targets["n_exposures"].is_null().any():
        raise ValidationError("Null 'n_exposures' column values found in ToO targets.")

    if targets["active"].is_null().any():
        raise ValidationError("Null 'active' column values found in ToO targets.")

    # Get magnitude columns and filted rows that do not have any value set.
    # This is equivalent to Pandas .drop(axis=1). In Polars is a bit harder. See
    # https://github.com/pola-rs/polars/issues/1613
    mag_data = targets[mag_columns]
    any_mags = mag_data.filter(
        ~polars.fold(
            True,
            lambda acc, s: acc & s.is_null(),
            polars.all(),
        )
    )
    if len(any_mags) < n_targets:
        raise ValidationError(
            "ToOs found with missing magnitudes. "
            "At least one magnitude value is required."
        )

    # Check that if one of the Sloan magnitudes is set, all are set.
    sloan_mags = targets.select(polars.col("^[ugriz]_mag$"))
    # Rows where at least one value is not null.
    sloan_mags_not_null = sloan_mags.filter(
        polars.fold(False, lambda a, b: a | b.is_not_null(), polars.all())
    )
    # Rows where at least one value is null.
    sloan_mags_null = sloan_mags_not_null.filter(
        polars.fold(False, lambda a, b: a | b.is_null(), polars.all())
    )
    if sloan_mags_null.height > 0:
        raise ValidationError("Found rows with incomplete Sloan magnitudes.")

    if set(targets["fiber_type"].unique()) != set(["APOGEE", "BOSS"]):
        raise ValidationError(
            "Invalid fiber_type values. Valid values are 'APOGEE' and 'BOSS'."
        )

    # Check can_offset is a boolean and is set.
    if targets["can_offset"].is_null().any():
        raise ValidationError("Null 'can_offset' column values found in ToO targets.")

    # Validate magnitude limits and bright neighbours.
    log.info("Running bright neighbour and magnitude limit checks.")
    bn_targets = validate_bright_limits(targets, database)

    # Require both checks to pass.
    bn_invalid = bn_targets.filter(~polars.col.bn_valid | ~polars.col.mag_lim_valid)

    if not drop_bright_targets and len(bn_invalid) > 0:
        raise ValidationError(
            f"{len(bn_invalid)} targets failed bright neighbour or "
            "magnitude limit checks."
        )
    else:
        log.warning(
            f"{len(bn_invalid)} targets failed bright neighbour or "
            "magnitude limit checks and will be rejected."
        )
        targets = targets.filter(~polars.col.too_id.is_in(bn_invalid["too_id"]))

    # Fill some optional columns.
    targets = targets.with_columns(
        active=polars.col.active.fill_null(True),
        inertial=polars.col.inertial.fill_null(False),
        observed=polars.col.observed.fill_null(False),
        optical_prov=polars.col.optical_prov.fill_null(""),
        delta_ra=polars.col.delta_ra.fill_null(polars.lit(0, dtype=polars.Float32)),
        delta_dec=polars.col.delta_dec.fill_null(polars.lit(0, dtype=polars.Float32)),
        n_exposures=polars.col.n_exposures.fill_null(polars.lit(1, dtype=polars.Int16)),
        priority=polars.col.priority.fill_null(polars.lit(5, dtype=polars.Int16)),
    )

    return targets


def validate_bright_limits(
    targets: polars.DataFrame,
    database: PeeweeDatabaseConnection,
):
    """Runs the Mugatu-like validation for bright targets."""

    targets = targets.clone()

    # Check that the sky_brightness_mode value are valid.
    valid_brightness_modes = ["bright", "dark"]

    targets_invalid_brightness_mode = targets.filter(
        polars.col.sky_brightness_mode.is_in(valid_brightness_modes).not_()
    )
    if len(targets_invalid_brightness_mode) > 0:
        raise ValidationError("Invalid sky_brightness_mode values found.")

    # Get a list of design modes.
    design_modes: dict[str, DesignMode] = allDesignModes(database)

    # List of validated targets for each brightness mode.
    targets_bmode_validated: list[polars.DataFrame] = []

    # First we split the targets by sky_brightness_mode (bright or dark). Then
    # we run the check for all the design modes that match that brightness mode.
    # We do not reject targets here but we mark if they pass the bright neighbour
    # and magnitude limit checks.
    for bmode in valid_brightness_modes:
        targets_bmode = targets.filter(polars.col.sky_brightness_mode == bmode)
        design_modes_bmode = [dm for dm in design_modes if dm.startswith(bmode)]

        valid_bn: list[numpy.ndarray] = []
        valid_mag_lim: list[numpy.ndarray] = []

        for dmb in design_modes_bmode:
            log.debug(f"Validating bright neighbours for design mode: {dmb}")
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=ErfaWarning)

                    valid_bn.append(
                        bn_validation(
                            targets_bmode,
                            dmb,
                            design_modes,
                            observatory="APO",
                        )
                    )

                    valid_mag_lim.append(
                        mag_lim_validation(
                            targets_bmode,
                            dmb,
                            design_modes,
                            observatory="APO",
                        )
                    )

            except Exception as err:
                log.warning(f"Error validating targets for design mode {dmb!r}: {err}")

        valid_bn_1d = numpy.all(numpy.stack(valid_bn), axis=0)
        valid_mag_lim_1d = numpy.all(numpy.stack(valid_mag_lim), axis=0)

        targets_bmode = targets_bmode.with_columns(
            bn_valid=polars.Series(valid_bn_1d),
            mag_lim_valid=polars.Series(valid_mag_lim_1d),
        )
        targets_bmode_validated.append(targets_bmode)

    return polars.concat(targets_bmode_validated)


def load_too_targets(
    targets: polars.DataFrame | str | pathlib.Path,
    database: PeeweeDatabaseConnection,
    update_existing: bool = False,
):
    """Loads a list of ToO targets into the database."""

    assert database.connected, "Database is not connected."

    targets = read_too_file(targets)
    targets = validate_too_targets(targets, database)

    too_columns = ["too_id"]
    too_columns += [col for col in too_dtypes if col not in too_metadata_columns]

    too_target_new = targets.select(too_columns)
    too_metadata_new = targets.select(too_metadata_columns)

    database_uri = get_database_uri(database.dbname, **database.connect_params)

    db_targets = polars.read_database_uri(
        "SELECT * from catalogdb.too_target ORDER BY too_id",
        database_uri,
        engine="adbc",
    )

    new_in_db = too_target_new.filter(polars.col.too_id.is_in(db_targets["too_id"]))
    if not new_in_db.equals(db_targets):
        log.warning(
            "Some ToO targets may have changed values. These changes will be "
            "ignored but should be reviewed."
        )

    new_targets = too_target_new.filter(~polars.col.too_id.is_in(db_targets["too_id"]))
    if len(new_targets) == 0:
        log.info("No new ToO targets to add.")
        if update_existing is False:
            return polars.DataFrame(schema=too_dtypes)
    else:
        log.info(f"Loading {len(new_targets)} new ToO(s) into catalogdb.too_target.")
        new_targets.write_database(
            "catalogdb.too_target",
            database_uri,
            if_table_exists="append",
            engine="adbc",
        )

        if not update_existing:
            too_metadata_new = too_metadata_new.filter(
                polars.col.too_id.is_in(new_targets["too_id"])
            )
            update_existing = True

    if update_existing:
        log.info("Updating ToO entries in catalogdb.too_metadata.")

        # First get the current metadata.
        too_metadata = polars.read_database(
            "SELECT * FROM catalogdb.too_metadata ORDER BY too_id",
            database_uri,
            engine="adbc",
        )

        # Update the metadata dataframe.
        too_metadata = too_metadata.update(
            too_metadata_new,
            on="too_id",
            how="outer",
            include_nulls=True,
        )

        # Replace the table (this is faster than atomic updates).
        too_metadata.write_database(
            "catalogdb.too_metadata",
            database_uri,
            if_table_exists="replace",
            engine="adbc",
        )

        # Recreate PK nand FK constraints.
        database.execute_sql(
            "ALTER TABLE catalogdb.too_metadata ADD PRIMARY KEY (too_id);"
        )
        database.execute_sql(
            "ALTER TABLE catalogdb.too_metadata ADD FOREIGN KEY (too_id) "
            "REFERENCES catalogdb.too_target (too_id) ON DELETE CASCADE;"
        )

    log.debug("Running VACUUM ANALYZE on catalogdb.too_target")
    database.execute_sql("VACUUM ANALYZE catalogdb.too_target;")
    database.execute_sql("VACUUM ANALYZE catalogdb.too_metadata;")

    return new_targets


def get_active_targets(database: PeeweeDatabaseConnection):
    """Returns the list of active ToO targets.

    Parameters
    ----------
    database
        The database connection.

    Returns
    -------
    dataframe
        A dataframe with the active ToO targets. These include targets from
        ``catalogdb.too_target`` that have ``active`` set to ``true`` and whose
        ``expiration_date`` is greater or equal the current MJD.

    """

    assert database.connected, "Database is not connected."

    database_uri = database_uri_from_connection(database)

    targets = polars.read_database_uri(
        "SELECT t.*,tm.* FROM catalogdb.too_target t JOIN catalogdb.too_metadata tm "
        "USING (too_id) WHERE tm.active = true AND tm.observed = false",
        database_uri,
        engine="adbc",
    )

    mjd = get_sjd("APO") + 1

    targets = targets.filter(polars.col.expiration_date >= mjd)

    return targets
