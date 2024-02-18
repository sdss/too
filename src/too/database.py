#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: database.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pathlib

import polars

from sdssdb.connection import PeeweeDatabaseConnection
from sdssdb.peewee.sdss5db import catalogdb

from too import log
from too.datamodel import mag_columns, too_dtypes, too_metadata_columns
from too.exceptions import ValidationError
from too.tools import read_too_file


__all__ = [
    "connect_to_database",
    "get_database_uri",
    "validate_too_targets",
    "load_too_targets",
]


def connect_to_database(
    dbname: str,
    host: str = "localhost",
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
):
    """Connects the ``sdssdb`` ``sdss5db`` models to the database."""

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
    host: str = "localhost",
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
):
    """Returns the URI to the database."""

    if user is None and password is None:
        auth: str = ""
    elif user is not None and password is None:
        auth: str = f"{user}@"
    elif user is not None and password is not None:
        auth: str = f"{user}:{password}@"
    else:
        raise ValueError("Passing a password requires also passing a user.")

    host_port: str = f"{host or ''}" if port is None else f"{host or ''}:{port}"

    return f"postgresql://{auth}{host_port}/{dbname}"


def database_uri_from_connection(database: PeeweeDatabaseConnection):
    """Returns a database URI from a connection."""

    return get_database_uri(database.dbname, **database.connect_params)


def validate_too_targets(targets: polars.DataFrame):
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


def load_too_targets(
    targets: polars.DataFrame | str | pathlib.Path,
    database: PeeweeDatabaseConnection,
    update_existing: bool = False,
):
    """Loads a list of ToO targets into the database."""

    assert database.connected, "Database is not connected."

    targets = read_too_file(targets)
    targets = validate_too_targets(targets)

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
