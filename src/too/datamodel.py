#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-08
# @Filename: datamodel.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

from typing import Mapping

import polars
import polars.type_aliases as pta


__all__ = ["too_dtypes"]

PolarsTypeMapping = Mapping[str, pta.PolarsDataType]

too_dtypes: PolarsTypeMapping = {
    "too_id": polars.Int64,
    "fiber_type": polars.String,
    "catalogid": polars.Int64,
    "sdss_id": polars.Int64,
    "gaia_dr3_source_id": polars.Int64,
    "twomass_pts_key": polars.Int32,
    "sky_brightness_mode": polars.String,
    "ra": polars.Float64,
    "dec": polars.Float64,
    "pmra": polars.Float32,
    "pmdec": polars.Float32,
    "epoch": polars.Float32,
    "parallax": polars.Float32,
    "lambda_eff": polars.Float32,
    "u_mag": polars.Float32,
    "g_mag": polars.Float32,
    "r_mag": polars.Float32,
    "i_mag": polars.Float32,
    "z_mag": polars.Float32,
    "optical_prov": polars.String,
    "gaia_bp_mag": polars.Float32,
    "gaia_rp_mag": polars.Float32,
    "gaia_g_mag": polars.Float32,
    "h_mag": polars.Float32,
    "delta_ra": polars.Float32,
    "delta_dec": polars.Float32,
    "inertial": polars.Boolean,
    "n_exposures": polars.Int16,
    "priority": polars.Int16,
    "active": polars.Boolean,
    "expiration_date": polars.Int32,
    "observed": polars.Boolean,
}

too_metadata_columns = [
    "too_id",
    "sky_brightness_mode",
    "lambda_eff",
    "u_mag",
    "g_mag",
    "r_mag",
    "i_mag",
    "z_mag",
    "optical_prov",
    "gaia_bp_mag",
    "gaia_rp_mag",
    "gaia_g_mag",
    "h_mag",
    "delta_ra",
    "delta_dec",
    "inertial",
    "n_exposures",
    "priority",
    "active",
    "expiration_date",
    "observed",
]

mag_columns = [
    "u_mag",
    "g_mag",
    "r_mag",
    "i_mag",
    "z_mag",
    "gaia_bp_mag",
    "gaia_rp_mag",
    "gaia_g_mag",
    "h_mag",
]
fiber_type_values = ["APOGEE", "BOSS"]
