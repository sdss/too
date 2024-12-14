#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-08
# @Filename: datamodel.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

# ruff: noqa: E501

from __future__ import annotations

import polars
import polars.type_aliases


__all__ = ["too_dtypes"]


too_dtypes: polars.type_aliases.SchemaDefinition = {
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
    "can_offset": polars.Boolean,
    "inertial": polars.Boolean,
    "n_exposures": polars.Int16,
    "priority": polars.Int16,
    "active": polars.Boolean,
    "observe_from_mjd": polars.Int32,
    "observe_until_mjd": polars.Int32,
    "observed": polars.Boolean,
    "added_on": polars.Date,
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
    "can_offset",
    "inertial",
    "n_exposures",
    "priority",
    "active",
    "observe_from_mjd",
    "observe_until_mjd",
    "observed",
    "added_on",
]

column_descriptions = {
    "too_id": "Unique identifier for the ToO target. [required]",
    "fiber_type": "Type of fiber to be used to observe the target (`APOGEE` or `BOSS`). [required]",
    "catalogid": "catalogid for this target, if already matched.",
    "sdss_id": "sdss_id for this target, if already matched.",
    "gaia_dr3_source_id": "Unique identifier of the target in the Gaia DR3 catalog (`source_id` column).",
    "twomass_pts_key": "Unique identifier of the target in the 2MASS catalog (`pts_key` column).",
    "sky_brightness_mode": "The sky brightness mode. Either `bright` or `dark`. [required]",
    "ra": "The Right Ascension of the target in ICRS coordinates as decimal degrees. [required]",
    "dec": "The declination of the target in ICRS coordinates as decimal degrees. [required]",
    "pmra": "The proper motion in RA in `mas/yr` as a true angle (the `cos(dec)` factor must have been applied).",
    "pmdec": "The proper motion in Dec in `mas/yr` as a true angle.",
    "epoch": "The epoch of the `ra`/`dec` coordinates. Required when `pmra`/`pmdec` are defined.",
    "parallax": "The parallax in `mas`.",
    "lambda_eff": "The effective wavelength to calculate the atmospheric refraction correction for the target.",
    "u_mag": "Sloan `u'` magnitude of the target.",
    "g_mag": "Sloan `g'` magnitude of the target.",
    "r_mag": "Sloan `r'` magnitude of the target.",
    "i_mag": "Sloan `i'` magnitude of the target.",
    "z_mag": "Sloan `z'` magnitude of the target.",
    "optical_prov": "Source of the optical magnitudes.",
    "gaia_bp_mag": "Gaia `BP` magnitude of the target.",
    "gaia_rp_mag": "Gaia `RP` magnitude of the target.",
    "gaia_g_mag": "Gaia `G` magnitude of the target.",
    "h_mag": "`H`-band magnitude of the target.",
    "delta_ra": "Fixed RA offset as a true angle, in arcsec.",
    "delta_dec": "Fixed Dec offset, in arcsec.",
    "can_offset": "`true` if the ToO is allowed to be offset. Offseting will only occur if target violates magnitude limits. [required]",
    "inertial": "If `true`, the proper motions for this target will be ignored (equivalent to `pmra = pmdec = 0.0`).",
    "n_exposures": "The minimum number of exposures required for the ToO to be complete and not assigned anymore. [required]",
    "priority": "The relative prioriry of this target (10: highest, 1: lowest, 0: the target will be ignored)",
    "active": "`true` if the target is active and should be assigned to configurations if possible. [required]",
    "observe_from_mjd": "MJD from which the target is considered observable. Default to the current date.",
    "observe_until_mjd": "MJD at which the target should automatically be consider inactive. If empty, the target never expires.",
    "observed": "`true` if the target has been fully observed and should not be assigned again. [required]",
    "added_on": "Date when the target was last added or modified, with the format YYYY-MM-DD. [required]",
}

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


def datamodel_to_markdown():  # pragma: no cover
    """Prints the datamodel as a markdown table."""

    df = polars.DataFrame(
        {
            "Column": list(too_dtypes.keys()),
            "Type": [v.__name__.lower() for v in too_dtypes.values()],
            "Description": [column_descriptions.get(k, "") for k in too_dtypes.keys()],
        }
    )

    with polars.Config(
        tbl_formatting="ASCII_MARKDOWN",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_rows=-1,
        tbl_width_chars=10000,
        fmt_str_lengths=10000,
    ):
        print(df)
