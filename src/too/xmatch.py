#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-12
# @Filename: xmatch.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

from typing import TYPE_CHECKING

import peewee
import polars

from too import log
from too.database import get_database_uri


if TYPE_CHECKING:
    from sdssdb.connection import PeeweeDatabaseConnection


__all__ = ["xmatch_too_targets"]


TOO_RUN_ID = 10
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
    plan: str | None = None,
    dry_run: bool = False,
    overwrite=False,
    keep_temp: bool = False,
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

    Parameters
    ----------
    database
        The database connection to use.
    plan
        The cross-match plan to use. Defaults to ``TOO_XMATCH_PLAN``.
    dry_run
        Whether to load the ``catalog`` and ``catalog_to_too_target`` tables after
        the cross-match.
    overwrite
        Deletes the temporary tables if they exist.
    keep_temp
        Whether to keep the temporary tables after the cross-match.

    Returns
    -------
    model
        The catalog model where the cross-match results are stored. If
        ``load_catalog`` is ``False``, returns the model for the temporary
        table.

    """

    from sdssdb.peewee.sdss5db.catalogdb import ToO_Target
    from target_selection.xmatch import XMatchPlanner

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

    plan_config = TOO_XMATCH_CONFIG.copy()
    plan_config["1.2.0"]["debug"] = log.sh.level

    plan = plan or TOO_XMATCH_PLAN
    version_id: int = plan_config[plan]["version_id"]

    # This is just a sanity check. We need to use version_id=31 and maybe we should
    # just hardcode it.
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

    # Create the XMatch instance here. We'll need it to get the relational model.
    xmatch_planner = XMatchPlanner.read(
        database,
        plan=plan,
        config_file=TOO_XMATCH_CONFIG,
        log=log,
        log_path=None,
    )

    # Delete temporary tables or fail.
    md5 = xmatch_planner.md5
    for temp_table in ["catalog", "catalog_to_too_target"]:
        temp_table_name = f"{temp_table}_{md5}"
        temp_table_schema = xmatch_planner.temp_schema
        temp_table_full = f"{temp_table_schema}.{temp_table_name}"
        if database.table_exists(temp_table_name, schema=temp_table_schema):
            if overwrite:
                log.warning(f"Dropping table {temp_table_full}.")
                database.execute_sql(f"DROP TABLE {temp_table_full};")
            else:
                raise RuntimeError(f"Table {temp_table_full} already exists.")

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

    # Step 2: insert the targets with catalogid into the sanboxed (!)
    # catalog_to_too_target table.
    if len(too_catalogid) > 0:
        rel_model_sb = xmatch_planner.get_relational_model(
            ToO_Target,
            sandboxed=True,
            create=True,
            temp=False,
        )

        rel_model_sb_tn = f"{rel_model_sb._meta.schema}.{rel_model_sb._meta.table_name}"

        log.info(
            f"Adding {len(too_catalogid)} ToO targets with "
            f"catalogid to {rel_model_sb_tn}."
        )

        too_catalogid = too_catalogid.rename({"too_id": "target_id"})
        too_catalogid = too_catalogid.with_columns(
            best=True,
            version_id=polars.lit(version_id, dtype=polars.Int16),
        )

        too_catalogid.write_database(
            rel_model_sb_tn,
            database_uri,
            if_table_exists="append",
            engine="adbc",
        )

        database.execute_sql(f"VACUUM ANALYZE {rel_model_sb_tn};")

    # Step 3: cross-match the remaining targets.
    log.info("Running cross-match for remaining ToO targets.")

    # Set the starting catalogid as the max of the ToO catalogids. This is because
    # we always use run_id=9 for ToOs and we should have plenty of them (and if for
    # some reason we reached the catalogid of run_id=10 it would fail when appending
    # to catalog).
    Catalog = database.models["catalogdb.catalog"]

    # Calculate the range of catalogids for run_id=TOO_RUN_ID.
    min_cid_run = TOO_RUN_ID << (64 - 11)
    max_cid_run = ((TOO_RUN_ID + 1) << (64 - 11)) - 1

    # Get the max catalogid that is between those values.
    max_cid = (
        Catalog.select(peewee.fn.MAX(Catalog.catalogid))
        .where(Catalog.catalogid >= min_cid_run, Catalog.catalogid <= max_cid_run)
        .scalar()
    )

    # If the number exists (it will be None if are no targets for TOO_RUN_ID), set
    # that value as the initial catalogid to assign to new targets.
    if max_cid:
        xmatch_planner._max_cid = max_cid + 1

    xmatch_planner.run(dry_run=dry_run, keep_temp=keep_temp, force=True, vacuum=False)

    return xmatch_planner
