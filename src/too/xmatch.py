#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-12
# @Filename: xmatch.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import peewee
import polars

from sdssdb.connection import PeeweeDatabaseConnection
from sdssdb.peewee.sdss5db.catalogdb import ToO_Target, Version
from target_selection.xmatch import XMatchPlanner

from too import log
from too.database import get_database_uri


__all__ = ["xmatch_too_targets"]


TOO_RUN_ID = 9
TOO_XMATCH_PLAN = "1.2.0"
TOO_XMATCH_CONFIG = {
    "1.2.0": {
        "run_id": TOO_RUN_ID,
        "query_radius": 1.0,
        "show_sql": True,
        "schema": "catalogdb",
        "output_table": "catalog",
        "start_node": "gaia_dr3_source",
        "debug": True,
        "log_path": "xmatch_{plan}.log",
        "path_mode": "config_list",
        "extra_nodes": [
            "gaia_dr3_source",
            "catalog_to_gaia_dr3_source",
            "twomass_psc",
            "catalog_to_twomass_psc",
        ],
        "version_id": 31,
        "order": ["too_target"],
        "tables": {
            "too_target": {
                "ra_column": "ra",
                "dec_column": "dec",
                "pmra_column": "pmra",
                "pmdec_column": "pmdec",
                "is_pmra_cos": "true",
                "parallax_column": "parallax",
                "has_missing_coordinates": False,
                "epoch": 2016.0,
            }
        },
        "join_paths": [
            [
                "too_target",
                "gaia_dr3_source",
                "catalog_to_gaia_dr3_source",
                "catalog",
            ],
            [
                "too_target",
                "twomass_psc",
                "catalog_to_twomass_psc",
                "catalog",
            ],
        ],
        "database_options": {
            "work_mem": "10GB",
            "temp_buffers": "5GB",
            "maintenance_work_mem": "5GB",
            "enable_hashjoin": False,
        },
    }
}


def xmatch_too_targets(
    database: PeeweeDatabaseConnection,
    version_plan: str | None = None,
):
    """Performs a cross-match of the ToO targets with the SDSS catalogues.

    The routine performs the folling steps:

    - Parses the ``too_target`` table.
    - Identifies ToO entries with either a ``catalogid`` or an ``sdss_id`` and
      manually them to the ``catalog_to_too_target`` table (if they are not
      already there).
    - For the remaining ToO entries, performs a cross-match with the SDSS
      catalogues and adds the matches to the ``catalog_to_too_target`` table.
      This is accomplished calling ``target_selection.XMatch`` in "addendum"
      mode.

    """

    assert database.connected, "Database is not connected"

    too_target_schema: str = ToO_Target._meta.schema  # type:ignore
    too_target_table_name: str = ToO_Target._meta.table_name  # type:ignore
    catalog_to_target_table_name: str = f"catalog_to_{too_target_table_name}"

    too_rel_fqtn = f"{too_target_schema}.{catalog_to_target_table_name}"
    too_fqtn = f"{too_target_schema}.{too_target_table_name}"

    database_uri = get_database_uri(database.dbname, **database.connect_params)

    # Some basic checks.
    assert database.table_exists(
        too_target_table_name,
        schema=too_target_schema,
    ), f"Table {too_fqtn} does not exist."

    assert database.table_exists(
        catalog_to_target_table_name,
        schema=too_target_schema,
    ), f"Table {too_rel_fqtn} does not exist."

    # Get version_id. This is all a bit silly since version_id has to be 31/1.0.0.
    if version_plan is None:
        version_id: int = Version.select(peewee.fn.MAX(Version.id)).scalar()
    else:
        version_id: int = Version.get(plan=version_plan).id

    assert version_id == 31, "version_id must be 31 to allow using sdss_id."

    too_unmatched = polars.read_database_uri(
        f"""
        SELECT t.* FROM {too_fqtn} t
        LEFT OUTER JOIN {too_rel_fqtn} c2t
        ON (c2t.target_id = t.too_id
            AND c2t.best IS TRUE
            AND c2t.version_id = {version_id})
        WHERE c2t.catalogid IS NULL
        ORDER BY t.too_id
        """,
        database_uri,
        engine="adbc",
    )

    if len(too_unmatched) == 0:
        log.warning("All ToO targets are already matched.")
        return

    # Step 1: select targets with sdss_id and without catalogid.
    # Populate the catalogid column.
    too_catalogid = too_unmatched.filter(
        polars.col("sdss_id").is_not_null() | polars.col("catalogid").is_not_null()
    ).select("too_id", "sdss_id", "catalogid")

    too_sdss_id_catalogid = polars.read_database_uri(
        f"""
        SELECT t.too_id, s.sdss_id, s.catalogid31 AS catalogid
        FROM {too_fqtn} t
        JOIN catalogdb.sdss_id_stacked s ON (t.sdss_id = s.sdss_id)
        ORDER BY t.too_id
        """,
        database_uri,
        engine="adbc",
    )
    too_sdss_id_catalogid = too_sdss_id_catalogid.filter(
        ~polars.col("catalogid").is_in(too_catalogid["catalogid"])
    )

    too_catalogid = too_catalogid.join(
        too_sdss_id_catalogid[["too_id", "catalogid"]],
        on="too_id",
        how="left",
    )
    too_catalogid = too_catalogid.with_columns(
        catalogid=polars.col.catalogid.fill_null(polars.col.catalogid_right)
    )
    too_catalogid = too_catalogid.select("too_id", "catalogid")

    # Step 2: insert the targets with catalogid into the catalog_to_too_target table.
    if len(too_catalogid) > 0:
        log.info(
            f"Adding {len(too_catalogid)} ToO targets with catalogid to {too_rel_fqtn}."
        )

        too_catalogid = too_catalogid.rename({"too_id": "target_id"})
        too_catalogid = too_catalogid.with_columns(
            best=True,
            version_id=polars.lit(version_id, dtype=polars.Int16),
        )

        too_catalogid.write_database(
            too_rel_fqtn,
            database_uri,
            if_table_exists="append",
            engine="adbc",
        )

        database.execute_sql(f"VACUUM ANALYZE {too_rel_fqtn};")

    # Step 3: cross-match the remaining targets.
    log.info("Running cross-match for remaining ToO targets.")
    xmatch_planner = XMatchPlanner.read(
        database,
        plan=TOO_XMATCH_PLAN,
        config_file=TOO_XMATCH_CONFIG,
        log=log,
        log_path=None,
    )

    # Set the starting catalogid as the max of the ToO catalogids. This is because
    # we always use run_id=9 for ToOs and we should have plenty of them (and if for
    # some reason we reached the catalogid of run_id=10 it would fail when appending
    # to catalog).
    C2ToO = database.models[too_rel_fqtn]
    max_catalogid = C2ToO.select(peewee.fn.MAX(C2ToO.catalogid)).scalar()
    if max_catalogid:
        xmatch_planner._max_cid = max_catalogid + 1

    xmatch_planner.run(load_catalog=False, keep_temp=True, force=True)
