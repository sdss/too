#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-16
# @Filename: carton.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import warnings

from typing import TYPE_CHECKING

from too import log


if TYPE_CHECKING:
    from sdssdb.connection import PeeweeDatabaseConnection


__all__ = ["run_too_carton", "update_sdss_id_tables"]


TOO_TARGET_PLAN = "1.1.0"


def run_too_carton():
    """Runs the ToO carton. Database connection must have already been set."""

    from target_selection import log as ts_log
    from target_selection.exceptions import TargetSelectionImportWarning

    ts_log.setLevel(10000)  # Disable target_selection logging
    warnings.simplefilter("ignore", TargetSelectionImportWarning)

    from target_selection.cartons.too import ToO_Carton  # Slow import

    too_carton = ToO_Carton(TOO_TARGET_PLAN)
    too_carton.log = log

    too_carton.run(overwrite=True)
    too_carton.load(mode="append")


def update_sdss_id_tables(database: PeeweeDatabaseConnection):
    """Updates the SDSS ID tables."""

    from target_selection.sdss_id import append_to_sdss_id

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
