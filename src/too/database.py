#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: database.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import datetime
import os
import pathlib

from typing import TYPE_CHECKING

import polars

from too import log
from too.datamodel import too_dtypes, too_metadata_columns
from too.tools import deduplicate_too_targets, read_too_file
from too.validate import validate_too_targets


if TYPE_CHECKING:
    from sdssdb.connection import PeeweeDatabaseConnection


__all__ = ["connect_to_database", "get_database_uri", "load_too_targets"]

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

    from sdssdb.peewee.sdss5db import catalogdb

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


def load_too_targets(
    targets: polars.DataFrame | str | pathlib.Path,
    database: PeeweeDatabaseConnection,
    validate_magnitude_limits: bool = False,
    update_existing: bool = False,
):
    """Loads a list of ToO targets into the database."""

    assert database.connected, "Database is not connected."

    now = datetime.datetime.now(datetime.UTC)

    targets = read_too_file(targets, cast=True)
    targets = deduplicate_too_targets(targets)
    targets = validate_too_targets(
        targets,
        database,
        bright_limit_checks=validate_magnitude_limits,
    )

    # We always want to set the optical_prov column to too.
    targets = targets.with_columns(optical_prov=polars.lit("too"))

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
            "Some ToO targets are already in too_target. Only changes "
            "to metadata columns will be applied for those."
        )

    new_targets = too_target_new.filter(~polars.col.too_id.is_in(db_targets["too_id"]))
    if len(new_targets) == 0:
        log.info("No new ToO targets to add.")
        if update_existing is False:
            return polars.DataFrame(schema=too_dtypes)
    else:
        log.info(f"Loading {len(new_targets)} new ToO(s) into catalogdb.too_target.")

        # Add current date-time.
        new_targets = new_targets.with_columns(added_date=polars.lit(now))

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
        too_metadata = polars.read_database_uri(
            "SELECT * FROM catalogdb.too_metadata ORDER BY too_id",
            database_uri,
            engine="adbc",
        ).with_columns(
            last_modified_date=polars.col.last_modified_date.cast(
                polars.Datetime(time_zone="UTC")
            )
        )

        # Update the metadata dataframe.
        too_metadata_new = too_metadata_new.with_columns(
            last_modified_date=polars.lit(now)
        )
        too_metadata = too_metadata.update(
            too_metadata_new,
            on="too_id",
            how="full",
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
