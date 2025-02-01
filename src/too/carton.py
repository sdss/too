#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-16
# @Filename: carton.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pathlib
import warnings

from typing import TYPE_CHECKING

from too import log


if TYPE_CHECKING:
    from sdssdb.connection import PeeweeDatabaseConnection


__all__ = ["run_too_carton", "update_sdss_id_tables"]


TOO_TARGET_PLAN = "1.1.0"
TOO_TAG = "too"


def run_too_carton():
    """Runs the ToO carton. Database connection must have already been set."""

    from target_selection import log as ts_log
    from target_selection.exceptions import TargetSelectionImportWarning

    ts_log.setLevel(40)  # Disable target_selection logging but allow exceptions.
    warnings.simplefilter("ignore", TargetSelectionImportWarning)

    from target_selection.cartons.too import ToO_Carton  # Slow import

    too_carton = ToO_Carton(TOO_TARGET_PLAN)
    too_carton.tag = TOO_TAG
    too_carton.log = log

    too_carton.run(overwrite=True)
    too_carton.load(mode="append")


def backup_sdss_id_tables(
    database: PeeweeDatabaseConnection,
    tables=[
        "sdss_id_flat",
        "sdss_id_flat_addendum",
        "sdss_id_stacked",
        "sdss_id_stacked_addendum",
    ],
    schema="sandbox",
    outdir: pathlib.Path | str = ".",
    suffix: str = "",
):
    """Backs up the SDSS ID tables.

    Parameters
    ----------
    database
        The database connection.
    tables
        The tables to backup. Each table is backed up as a separate file in
        ``outdir`` with the format ``<schema>_<table_name>_<suffix>.csv``.
    schema
        The schema where the tables are located.
    outdir
        The output directory for the backup files. Defaults to the current
        directory.
    suffix
        A suffix to add to the backup files.

    """

    outdir = pathlib.Path(outdir).absolute()
    outdir.mkdir(parents=True, exist_ok=True)

    if suffix:
        suffix = f"_{suffix}"

    assert database.connected, "Database connection must be established."

    for table in tables:
        cursor = database.cursor()
        with open(outdir / f"{schema}_{table}{suffix}.csv", "w") as file:
            cursor.copy_expert(f"COPY {schema}.{table} TO STDOUT WITH CSV HEADER", file)


def update_sdss_id_tables(database: PeeweeDatabaseConnection):
    """Updates the SDSS ID tables."""

    from target_selection import log as ts_log
    from target_selection.sdss_id import append_to_sdss_id
    from target_selection.sdss_id.append_to_sdss_id import database as append_database
    from target_selection.sdss_id.create_catalogidx_to_catalogidy import (
        database as cidx_database,
    )

    # Override log
    if ts_log.sh in ts_log.handlers:
        ts_log.removeHandler(ts_log.sh)
    ts_log.addHandler(log.sh)
    ts_log.sh = log.sh

    # The sdss_id code uses whichever database profile is default for the machine.
    # This is necessary to ensure that the database that we pass to this function
    # is actually used.
    append_database.connect(dbname=database.dbname, **database.connect_params)
    append_database._config.update(**database.connect_params)

    cidx_database.connect(dbname=database.dbname, **database.connect_params)
    cidx_database._config.update(**database.connect_params)

    log.info("Updating SDSS ID tables.")

    apend_inst = append_to_sdss_id.AppendToTables(
        database,
        individual_table="catalogdb.catalog_to_too_target",
    )

    output_name = apend_inst.run_MetaXMatch(database)

    apend_inst.create_temp_catalogid_lists(database, output_name)
    apend_inst.create_sdss_id_stacked_addendum(database, output_name)
    apend_inst.add_to_sdss_id_stacked(database)
    apend_inst.create_sdss_id_flat_addendum(database)
    apend_inst.add_to_sdss_id_flat(database)

    log.info("SDSS ID tables have been updated.")
