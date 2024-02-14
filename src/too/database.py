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
from too.datamodel import mag_columns, too_dtypes
from too.exceptions import ValidationError


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
    database: PeeweeDatabaseConnection,
    update_existing: bool = False,
):
    """Loads a list of ToO targets into the database."""

    if update_existing:
        raise NotImplementedError("update_existing not yet implemented.")

    assert database.connected, "Database is not connected."

    if isinstance(targets, (str, pathlib.Path)):
        path = pathlib.Path(targets)

        if path.suffix == ".parquet":
            targets = polars.read_parquet(targets)
        elif path.suffix == ".csv":
            targets = polars.read_csv(targets, schema=too_dtypes)
        else:
            raise ValueError(f"Invalid file type {path.suffix!r}")

    database_uri = get_database_uri(database.dbname, **database.connect_params)

    current_targets = polars.read_database_uri(
        "SELECT * from catalogdb.too_target",
        database_uri,
        engine="adbc",
    )
    current_targets = current_targets.cast(too_dtypes)  # type: ignore

    new_targets = targets.filter(~polars.col.too_id.is_in(current_targets["too_id"]))

    if len(new_targets) == 0:
        log.info("No new ToO targets to add.")
        return 0

    log.info(f"Loading {len(new_targets)} new ToO targets into catalogdb.too_target.")
    n_added = new_targets.write_database(
        "catalogdb.too_target",
        database_uri,
        if_table_exists="append",
        engine="adbc",
    )

    log.debug("Running VACUUM ANALYZE on catalogdb.too_target")
    database.execute_sql("VACUUM ANALYZE catalogdb.too_target;")

    return n_added
