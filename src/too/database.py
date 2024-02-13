#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: database.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pathlib

import adbc_driver_postgresql.dbapi as dbapi
import peewee
import polars
from sdssdb.peewee import BaseModel

from too import log
from too.datamodel import mag_columns, too_dtypes
from too.exceptions import ValidationError


__all__ = ["ToO_Target"]


class ToOBaseModel(BaseModel):
    class Meta:
        schema = "catalogdb"


class ToO_Target(ToOBaseModel):
    too_id = peewee.IntegerField(primary_key=True)
    fiber_type = peewee.TextField()
    catalogid = peewee.IntegerField()
    sdss_id = peewee.IntegerField()
    gaia_dr3_source_id = peewee.IntegerField()
    twomass_pts_key = peewee.IntegerField()
    sky_brightness_mode = peewee.TextField()
    ra = peewee.FloatField()
    dec = peewee.FloatField()
    pmra = peewee.FloatField()
    pmdec = peewee.FloatField()
    epoch = peewee.FloatField()
    parallax = peewee.FloatField()
    lambda_eff = peewee.FloatField()
    u_mag = peewee.FloatField()
    g_mag = peewee.FloatField()
    r_mag = peewee.FloatField()
    i_mag = peewee.FloatField()
    z_mag = peewee.FloatField()
    optical_prov = peewee.TextField()
    gaia_bp_mag = peewee.FloatField()
    gaia_rp_mag = peewee.FloatField()
    gaia_g_mag = peewee.FloatField()
    h_mag = peewee.FloatField()
    delta_ra = peewee.FloatField()
    delta_dec = peewee.FloatField()
    inertial = peewee.BooleanField()
    n_exposures = peewee.IntegerField()
    priority = peewee.IntegerField()
    active = peewee.BooleanField()
    expiration_date = peewee.IntegerField()
    observed = peewee.BooleanField()

    class Meta:
        table_name = "too_target"


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

    host_port: str = f"{host}" if port is None else f"{host}:{port}"

    return f"postgresql://{auth}{host_port}/{dbname}"


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

    return True


def load_too_targets(
    targets: polars.DataFrame | str | pathlib.Path,
    database_uri: str,
    update_existing: bool = False,
):
    """Loads a list of ToO targets into the database."""

    if update_existing:
        raise NotImplementedError("update_existing not yet implemented.")

    if isinstance(targets, (str, pathlib.Path)):
        path = pathlib.Path(targets)

        if path.suffix == ".parquet":
            targets = polars.read_parquet(targets)
        elif path.suffix == ".csv":
            targets = polars.read_csv(targets, schema=too_dtypes)
        else:
            raise ValueError(f"Invalid file type {path.suffix!r}")

    with dbapi.connect(database_uri) as conn:
        current_targets = polars.read_database(
            "SELECT * from catalogdb.too_target",
            conn,  # type: ignore
        )
        current_targets = current_targets.cast(too_dtypes)  # type: ignore

    new_targets = targets.filter(~polars.col.too_id.is_in(current_targets["too_id"]))

    if len(new_targets) == 0:
        log.info("No new ToO targets to add.")
        return 0

    log.info(f"Loading {len(new_targets)} new ToO targets into the database.")
    n_added = new_targets.write_database(
        "catalogdb.too_target",
        database_uri,
        if_table_exists="append",
        engine="adbc",
    )

    return n_added
