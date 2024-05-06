#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import pathlib

from typing import TYPE_CHECKING

import numpy
import polars
from peewee import JOIN, fn

from too import log
from too.tools import match_fields
from too.validate import add_bright_limits_columns


if TYPE_CHECKING:
    from sdssdb.connection import PeeweeDatabaseConnection


__all__ = ["dump_to_parquet"]


def dump_to_parquet(
    observatory: str,
    path: os.PathLike | str,
    database: PeeweeDatabaseConnection | None = None,
):
    """Dumps the ToO targets to a Parquet file.

    Parameters
    ----------
    observatory
        The observatory for which to select targets.
    path
        The path where to save the Parquet file.
    database
        The database connection. Used to check that the models are connected to
        the same database.

    """

    from sdssdb.peewee.sdss5db import catalogdb, opsdb

    path = pathlib.Path(path)

    observatory = observatory.upper()
    if observatory not in ["APO", "LCO"]:
        raise ValueError("Invalid observatory.")

    # Check that the model connection is the same. This should be the case as
    # long as database has been created using connect_to_database(), which will
    # update the models.
    if database and database != catalogdb.Catalog._meta.database:  # type: ignore
        raise ValueError("Mismatched database connection between too and models.")

    catalogdb.Catalog.bind(database)
    catalogdb.CatalogToToO_Target.bind(database)
    catalogdb.ToO_Target.bind(database)
    catalogdb.ToO_Metadata.bind(database)
    Catalog = catalogdb.Catalog
    c2too = catalogdb.CatalogToToO_Target
    TooTarget = catalogdb.ToO_Target
    TooMeta = catalogdb.ToO_Metadata

    opsdb.AssignmentToFocal.bind(database)
    Assn2Focal = opsdb.AssignmentToFocal

    columns = (
        TooTarget.too_id,
        TooTarget.fiber_type,
        Catalog.catalogid,
        TooTarget.sdss_id,
        TooTarget.ra,
        TooTarget.dec,
        TooTarget.pmra,
        TooTarget.pmdec,
        TooTarget.epoch,
        TooMeta.g_mag,
        TooMeta.r_mag,
        TooMeta.i_mag,
        TooMeta.z_mag,
        TooMeta.h_mag,
        TooMeta.gaia_g_mag,
        TooMeta.gaia_bp_mag,
        TooMeta.gaia_rp_mag,
        TooMeta.optical_prov,
        TooMeta.delta_ra,
        TooMeta.delta_dec,
        TooMeta.can_offset,
        TooMeta.inertial,
        TooMeta.sky_brightness_mode,
        TooMeta.n_exposures,
    )

    targets = (
        TooTarget.select(
            *columns,
            fn.count(Assn2Focal.catalogid).alias("count"),
        )
        .join(TooMeta, on=(TooMeta.too_id == TooTarget.too_id))
        .switch(TooTarget)
        .join(c2too)
        .join(Catalog)
        .join(
            Assn2Focal,
            JOIN.LEFT_OUTER,
            on=(Assn2Focal.catalogid == Catalog.catalogid),
        )
        .group_by(*columns)
        .dicts()
    )

    dataframe = polars.from_dicts(
        list(targets),
        schema={
            "too_id": polars.Int64,
            "fiber_type": polars.String,
            "catalogid": polars.Int64,
            "sdss_id": polars.Int64,
            "ra": polars.Float64,
            "dec": polars.Float64,
            "pmra": polars.Float32,
            "pmdec": polars.Float32,
            "epoch": polars.Float32,
            "g_mag": polars.Float32,
            "r_mag": polars.Float32,
            "i_mag": polars.Float32,
            "z_mag": polars.Float32,
            "h_mag": polars.Float32,
            "gaia_g_mag": polars.Float32,
            "gaia_bp_mag": polars.Float32,
            "gaia_rp_mag": polars.Float32,
            "optical_prov": polars.String,
            "delta_ra": polars.Float32,
            "delta_dec": polars.Float32,
            "can_offset": polars.Boolean,
            "inertial": polars.Boolean,
            "sky_brightness_mode": polars.String,
            "n_exposures": polars.Int32,
            "count": polars.Int32,
        },
    )

    df_field = match_fields(dataframe, database)
    df_observatory = df_field.filter(polars.col.observatory == observatory)

    n_requested = df_observatory["n_exposures"].to_numpy()
    n_done = df_observatory["count"].to_numpy()
    elligible = df_observatory.filter(numpy.greater(n_requested, n_done)).sort("too_id")

    log.info(f"Found {len(elligible)} targets of {len(targets)} total.")

    # Add bright neighbour and magnitude limit check columns.
    log.info("Adding bright neighbour and magnitude limit columns.")
    bn = add_bright_limits_columns(elligible, database, observatories=[observatory])

    log.debug(f"Saving data to {path!s}.")
    bn.write_parquet(path)

    return bn
