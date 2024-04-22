#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-08
# @Filename: mock.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pathlib

from typing import Literal, overload

import numpy
import polars

from too import console, log
from too.datamodel import too_dtypes
from too.tools import download_file


__all__ = ["create_mock_too_catalogue"]


@overload
def get_sample_data(
    table: Literal["gaia_dr3", "twomass", "photoobj"],
    lazy: Literal[True],
) -> polars.LazyFrame: ...


@overload
def get_sample_data(
    table: Literal["gaia_dr3", "twomass", "photoobj"],
    lazy: Literal[False],
) -> polars.DataFrame: ...


def get_sample_data(
    table: Literal["gaia_dr3", "twomass", "photoobj"],
    lazy: bool = True,
) -> polars.DataFrame | polars.LazyFrame:
    """Downloads and caches the table sample data.

    Parameters
    ----------
    table
        The table to download. Can be ``'gaia_dr3'`` or ``'twomass'``. If the table
        file is not found in the cache, it will be downloaded from the SAS server.
    lazy
        Whether to return a lazy dataframe.

    Returns
    -------
    A polars dataframe with the sample data.

    """

    BASE_URL = "https://data.sdss5.org/resources/target/mocks/samples/"
    CACHE_PATH = pathlib.Path("~/.cache/sdss/too/samples").expanduser()

    if table == "gaia_dr3":
        filename = "gaia_dr3_1M_sample.parquet"
    elif table == "twomass":
        filename = "twomass_psc_1M_sample.parquet"
    elif table == "photoobj":
        filename = "sdss_dr13_photoobj_1M_sample.parquet"
    else:
        raise ValueError(f"Invalid table {table!r}")

    if not (CACHE_PATH / filename).exists():
        log.debug(f"File {filename!r} not found in cache. Downloading from {BASE_URL}.")
        download_file(
            f"{BASE_URL}/{filename}",
            CACHE_PATH,
            transient_progress=True,
            console=console,
        )

    if lazy:
        return polars.scan_parquet(CACHE_PATH / filename)
    return polars.read_parquet(CACHE_PATH / filename)


def get_sample_targets(
    table: Literal["gaia_dr3", "twomass", "photoobj"],
    n_targets: int,
):
    """Returns a sample of table targets."""

    data = get_sample_data(table, lazy=False)
    sample = data.sample(n_targets)

    if table == "gaia_dr3":
        col_mapping = {
            "source_id": "gaia_dr3_source_id",
            "ra": "ra",
            "dec": "dec",
            "pmra": "pmra",
            "pmdec": "pmdec",
            "parallax": "parallax",
            "phot_g_mean_mag": "gaia_g_mag",
            "phot_bp_mean_mag": "gaia_bp_mag",
            "phot_rp_mean_mag": "gaia_rp_mag",
        }
    elif table == "twomass":
        col_mapping = {
            "pts_key": "twomass_pts_key",
            "ra": "ra",
            "decl": "dec",
            "h_m": "h_mag",
        }
    elif table == "photoobj":
        col_mapping = {
            "ra": "ra",
            "dec": "dec",
            "psfmag_u": "u_mag",
            "psfmag_g": "g_mag",
            "psfmag_r": "r_mag",
            "psfmag_i": "i_mag",
            "psfmag_z": "z_mag",
        }

    if "source_id" in sample.columns:
        sample = sample.cast({"source_id": polars.Int64})
    elif "pts_key" in sample.columns:
        sample = sample.cast({"pts_key": polars.Int32})

    sample = sample.select(*list(col_mapping), "catalogid", "sdss_id")
    sample = sample.rename(col_mapping)
    sample = sample.cast({"catalogid": polars.Int64, "sdss_id": polars.Int64})

    sample = sample.with_columns(polars.col(["ra", "dec"]).cast(polars.Float64))

    return sample


def create_mock_too_catalogue(
    n_targets: int = 1000000,
    fraction_unknown: float = 0.6,
    fraction_unknown_sdss: float = 0.2,
    fraction_unknown_gaia: float = 0.3,
    fraction_known_gaia: float = 0.8,
    fraction_known_sdss: float = 0.1,
    fraction_known_twomass: float = 0.1,
    catalogid_likelihood: float = 0.2,
    design_modes: list[str] = ["bright", "dark"],
):
    """Creates a mock ToO catalogue.

    Parameters
    ----------
    n_targets
        The number of targets in the mocked catalogue.
    fraction_unknown
        Fraction of the targets that will not have an associated target in
        ``catalogdb``.
    fraction_unknown_sdss : float
        Fraction of the unknown targets that will actually be drawn from
        ``sdss_dr13_photoobj``.
    fraction_unknown_gaia : float
        Fraction of the unknown targets that will actually be drawn from
        ``gaia_dr3_source``.
    fraction_known_gaia
        Fraction of the targets that will have a known Gaia source. Along with
        ``fraction_known_sdss`` and ``fraction_known_twomass``, must add up to 1.
        The total number of targets with ``gaia_dr3_source_id`` will be
        ``n_targets * fraction_known_gaia * (1 - fraction_unknown)``.
    fraction known_sdss
        Fraction of the targets that will have a known SDSS source.
    fraction_known_twomass
        Fraction of the targets that will have a known 2MASS source.
    catalogid_likelihood
        The likelihood (0-1) that a target will have a known ``catalogid`` or
        ``sdss_id``.
    design_modes
        A list of design modes to randomly assign to the ``sky_brightness_mode``
        column.

    Returns
    -------
    A polars dataframe with the mock catalogue.

    """

    n_known = n_targets * (1 - fraction_unknown)

    gaia_known_targets = get_sample_targets(
        "gaia_dr3",
        int(n_known * fraction_known_gaia),
    )

    twomass_known_targets = get_sample_targets(
        "twomass",
        int(n_known * fraction_known_twomass),
    )

    sdss_known_targets = get_sample_targets(
        "photoobj",
        int(n_known * fraction_known_sdss),
    )
    sdss_known_targets = sdss_known_targets.with_columns(catalogid=None, sdss_id=None)

    n_unknown = n_targets * fraction_unknown

    gaia_unknown_targets = get_sample_targets(
        "gaia_dr3",
        int(n_unknown * fraction_unknown_gaia),
    )
    gaia_unknown_targets = gaia_unknown_targets.with_columns(
        gaia_dr3_source_id=None,
        catalogid=None,
        sdss_id=None,
        gaia_bp_mag=None,
        gaia_rp_mag=None,
    )

    sdss_unknown_targets = get_sample_targets(
        "photoobj",
        int(n_unknown * fraction_unknown_sdss),
    )
    sdss_unknown_targets = sdss_unknown_targets.with_columns(
        catalogid=None,
        sdss_id=None,
    )

    df = polars.DataFrame(schema=too_dtypes)
    df = polars.concat(
        [
            df,
            gaia_known_targets,
            twomass_known_targets,
            sdss_known_targets,
            gaia_unknown_targets,
            sdss_unknown_targets,
        ],
        how="diagonal",
    )

    n_random = int(n_targets - df.height)

    # Give half of them Gaia mags and half SDSS.
    random_gaia = [None] * n_random
    random_sdss = [None] * n_random
    random_gaia[: n_random // 2] = numpy.random.uniform(10, 20, size=n_random // 2)
    random_sdss[n_random // 2 :] = numpy.random.uniform(10, 20, size=n_random // 2)

    random_targets = polars.DataFrame(
        {
            "ra": numpy.random.uniform(0, 360, n_random),
            "dec": numpy.random.uniform(-90, 90, n_random),
            "gaia_g_mag": polars.Series(random_gaia, dtype=polars.Float32),
            "u_mag": polars.Series(random_sdss, dtype=polars.Float32),
            "g_mag": polars.Series(random_sdss, dtype=polars.Float32),
            "r_mag": polars.Series(random_sdss, dtype=polars.Float32),
            "i_mag": polars.Series(random_sdss, dtype=polars.Float32),
            "z_mag": polars.Series(random_sdss, dtype=polars.Float32),
        },
    )

    df = polars.concat([df, random_targets], how="diagonal")

    df = df.with_columns(
        keep_cid=polars.Series(numpy.random.rand(df.height) < catalogid_likelihood)
    )
    df = df.with_columns(
        catalogid=polars.when(polars.col.keep_cid).then(polars.col.catalogid),
        sdss_id=polars.when(polars.col.keep_cid).then(polars.col.sdss_id),
    )
    df.drop_in_place("keep_cid")
    df = df.sample(df.height, shuffle=True)  # Shuffle

    df = df.with_columns(polars.int_range(1, df.height + 1).alias("too_id"))

    # Fill out some columns
    fiber_type = numpy.array(["APOGEE"] * df.height)
    boss_mask = numpy.random.rand(df.height) < 0.5
    fiber_type[numpy.where(boss_mask)[0]] = "BOSS"
    sky_brightness_mode = numpy.random.choice(design_modes, size=df.height)

    df = df.with_columns(
        fiber_type=polars.Series(fiber_type, dtype=polars.String),
        observed=False,
        can_offset=True,
        active=True,
        priority=polars.lit(5, dtype=polars.Int16),
        n_exposures=polars.lit(3, dtype=polars.Int16),
        sky_brightness_mode=polars.Series(sky_brightness_mode, dtype=polars.String),
    )

    return df
