#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-08
# @Filename: datamodel.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import polars


too_dtypes = {
    "too_id": polars.UInt64,
    "fiber_type": polars.String,
    "catalogid": polars.UInt64,
    "sdss_id": polars.UInt64,
    "gaia_dr3_source_id": polars.UInt64,
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

too_fixed_columns = ["catalogid", "sdss_id", "gaia_dr3_source_id", "twomass_pts_key"]
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
fiber_type_values = ["APOGEE", "BOSS", ""]