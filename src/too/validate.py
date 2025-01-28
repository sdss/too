#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: Ilija Medan (ilija.medan@vanderbilt.edu)
# @Date: 2024-04-02
# @Filename: validate.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import collections
import os
import warnings

from typing import TYPE_CHECKING, Literal, Tuple

import astropy.units as u
import numpy as np
import polars
from astropy.coordinates import SkyCoord
from astropy.time import Time
from erfa import ErfaWarning
from polars import selectors

from too import log
from too.datamodel import mag_columns, too_dtypes
from too.exceptions import ValidationError
from too.tools import match_fields


if TYPE_CHECKING:
    from sdssdb import PeeweeDatabaseConnection


__all__ = ["validate_too_targets", "add_bright_limits_columns"]


# load enviornment variable with path to healpix maps
BN_HEALPIX = os.getenv("BN_HEALPIX")


def check_assign_mag_limit(
    mag_metric_min,
    mag_metric_max,
    assign_mag,
):  # pragma: no cover
    """
    Checks the if magnitude of one assignment agrees with
    design mode for some instrument and carton class

    Parameters
    ----------
    mag_metric_min: float
        The minimum magitude for the specific DesignMode.

    mag_metric_min: float
        The maximum magitude for the specific DesignMode.

    assign_mag: float
        The magntiude of the assignment being checkaged against
        the DesignMode.

    Returns
    -------
    targ_check: boolean
        True of the assignment magntiude passed the DesignMode
        for the magnitude, False if not.

    complete_check: str
        'COMPLETE' if assign_mag is value, 'INCOMPLETE' if Null.

    """

    # set default values
    complete_check = "COMPLETE"
    targ_check = False

    # do checks
    # define Null cases for targetdb.magnitude table
    cases = [-999, -9999, 999, 0.0, np.nan, 99.9, None]
    if assign_mag in cases or np.isnan(assign_mag):
        complete_check = "INCOMPLETE"
        # set True, no mag is not a fail
        targ_check = True
    # check when greater than and less than
    elif mag_metric_min != -999.0 and mag_metric_max != -999.0:
        if mag_metric_min < assign_mag < mag_metric_max:
            targ_check = True
    # check when just greater than
    elif mag_metric_min != -999.0:
        if assign_mag > mag_metric_min:
            targ_check = True
    # check when less than
    else:
        if assign_mag < mag_metric_max:
            targ_check = True
    return targ_check, complete_check


def allDesignModes(database: PeeweeDatabaseConnection):
    """Function to return a dictionary with all design modes.

    Adapted from the original function in ``mugatu.designmode``.

    Parameters
    ----------
    database : PeeweeDatabaseConnection
        The database connection to use to query the design modes.

    Comments
    --------
    Returns an OrderedDict

    If filename is not provided, it reads from that file name
    and the given extension. If not, it reads from the database.

    If neither the filename is given nor the db is available, it
    returns None.

    """

    from sdssdb.peewee.sdss5db.targetdb import DesignMode as DesignModeDB

    desmodes = DesignModeDB.select()
    dmd = collections.OrderedDict()
    for desmode in desmodes:
        dmd[desmode.label] = DesignMode(database, label=desmode.label)
    return dmd


class DesignMode:
    """Class to store parameters for a design mode.

    Adapted from the original class in ``mugatu.designmode``.

    Parameters
    ----------
    database : PeeweeDatabaseConnection
        The database connection to use to query the design modes.
    label : str
        Label for design mode

    Attributes
    ----------
    n_stds_min: dict
        Dictionary with the minimum number of standards for a design
        for each instrument ('APOGEE' and 'BOSS').

    min_stds_fovmetric: dict
        Dictionary wih the FOV metric for the standards in a design
        for each instrument ('APOGEE' and 'BOSS').
        The FOV metric is described by three parameters, the
        nth neighbor to get distances to, the percentle distance
        to calculate and the distance to compare against for validation
        (in mm).

    stds_mags: dict
        Dictionary for the min/max magnitude for the standards in a
        design for each instrument ('APOGEE' and 'BOSS'). Indexes
        correspond to magntidues: [g, r, i, z, bp, gaia_g, rp, J, H, K].

    bright_limit_targets: dict
        Dictionary for the min/max magnitude for the science targets
        in adesign for each instrument ('APOGEE' and 'BOSS'). Indexes
        correspond to magntidues: [g, r, i, z, bp, gaia_g, rp, J, H, K].

    sky_neighbors_targets: dict
        Dictionary for the parameters used to check distance between
        skies and all possible sources in field for each instrument
        ('APOGEE' and 'BOSS'). Distances to targets (r, in arcseconds)
        must be r > R_0 * (lim - mags) ** beta, where mags is G band
        for BOSS and H band for APOGEE. Indexes correspond to:
        [R_0, beta, lim]

    trace_diff_targets: dict
        Dictionary for the maximum magnitude difference allowed between
        fibers next to each ther on the chip for each instrument
        ('APOGEE' and 'BOSS'). Here the magntidue difference is checked
        in the G band for BOSS and H band for APOGEE.

    """

    def __init__(self, database: PeeweeDatabaseConnection, label: str | None = None):
        from sdssdb.peewee.sdss5db.targetdb import DesignMode as DesignModeDB

        DesignModeDB._meta.database(database)  # type:ignore

        if label is not None:
            self.fromdb(label=label)
        return

    def fromdb(self, label: str):
        """Read in parameters for design mode from db.

        Parameters
        ----------

        label : str
            name of design mode

        """

        from sdssdb.peewee.sdss5db.targetdb import DesignMode as DesignModeDB

        self.desmode_label = label

        desmode = DesignModeDB.select().where(DesignModeDB.label == label)[0]

        self.n_skies_min = {}
        self.n_skies_min["BOSS"] = desmode.boss_skies_min
        self.n_skies_min["APOGEE"] = desmode.apogee_skies_min

        self.min_skies_fovmetric = {}
        self.min_skies_fovmetric["BOSS"] = desmode.boss_skies_fov
        self.min_skies_fovmetric["APOGEE"] = desmode.apogee_skies_fov

        self.n_stds_min = {}
        self.n_stds_min["BOSS"] = desmode.boss_stds_min
        self.n_stds_min["APOGEE"] = desmode.apogee_stds_min

        self.min_stds_fovmetric = {}
        self.min_stds_fovmetric["BOSS"] = desmode.boss_stds_fov
        self.min_stds_fovmetric["APOGEE"] = desmode.apogee_stds_fov

        self.stds_mags = {}
        self.stds_mags["BOSS"] = np.zeros(
            (len(desmode.boss_stds_mags_min), 2),
            dtype=np.float64,
        )
        self.stds_mags["BOSS"][:, 0] = desmode.boss_stds_mags_min
        self.stds_mags["BOSS"][:, 1] = desmode.boss_stds_mags_max
        self.stds_mags["APOGEE"] = np.zeros(
            (len(desmode.apogee_stds_mags_min), 2),
            dtype=np.float64,
        )
        self.stds_mags["APOGEE"][:, 0] = desmode.apogee_stds_mags_min
        self.stds_mags["APOGEE"][:, 1] = desmode.apogee_stds_mags_max

        self.bright_limit_targets = {}
        self.bright_limit_targets["BOSS"] = np.zeros(
            (len(desmode.boss_bright_limit_targets_min), 2),
            dtype=np.float64,
        )
        self.bright_limit_targets["BOSS"][:, 0] = desmode.boss_bright_limit_targets_min
        self.bright_limit_targets["BOSS"][:, 1] = desmode.boss_bright_limit_targets_max

        dmode_apogee_blimit_min = desmode.apogee_bright_limit_targets_min
        dmode_apogee_blimit_max = desmode.apogee_bright_limit_targets_max

        self.bright_limit_targets["APOGEE"] = np.zeros(
            (len(dmode_apogee_blimit_min), 2),
            dtype=np.float64,
        )
        self.bright_limit_targets["APOGEE"][:, 0] = dmode_apogee_blimit_min
        self.bright_limit_targets["APOGEE"][:, 1] = dmode_apogee_blimit_max

        self.sky_neighbors_targets = {}
        self.sky_neighbors_targets["BOSS"] = desmode.boss_sky_neighbors_targets
        self.sky_neighbors_targets["APOGEE"] = desmode.apogee_sky_neighbors_targets

        self.trace_diff_targets = {}
        self.trace_diff_targets["APOGEE"] = desmode.apogee_trace_diff_targets

        return


def magnitude_array(targets: polars.DataFrame) -> np.ndarray:  # pragma: no cover
    """
    create the magnitude array for the targets
    Parameters
    ----------
    targets: polars.DataFrame
        DataFrame with the ToO target information
    Returns
    -------
    magnitudes: np.array
        Magnitude array for the targets. Array is of size (N, 10),
        where columns correspond to
        g, r, i, z, bp, gaia_g, rp, J, H, K band
        magnitudes.

    """

    magnitudes = targets[
        [
            "g_mag",
            "r_mag",
            "i_mag",
            "z_mag",
            "gaia_bp_mag",
            "gaia_g_mag",
            "gaia_rp_mag",
        ]
    ].to_numpy()

    # add in the missing 2MASS mags as nan
    magnitudes = np.column_stack(
        (
            magnitudes,
            np.zeros(len(targets)) + np.nan,
            targets["h_mag"],
            np.zeros(len(targets)) + np.nan,
        )
    )
    return magnitudes


def calculate_offsets(
    targets: polars.DataFrame,
    design_mode: str,
    modes: dict,
    offset_min_skybrightness: float = 0.0,
    observatory: str = "APO",
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:  # pragma: no cover
    """
    Calculate the offsets for the targets

    Parameters
    ----------
    targets: polars.DataFrame
        DataFrame with the ToO target information

    design_mode: str
        The design_mode to run the validation for

    offset_min_skybrightness: float
        Minimum skybrightness targets will be offset

    observatory: str
        Observatory where observation is taking place, either
        'LCO' or 'APO'.

    Return
    ------
    delta_ra: np.array
        offset in the R.A. direction

    delta_dec: np.array
        offset in the Decl. direction

    offset_flag: np.array
        flags associated with the offseting

    """

    from coordio.utils import Moffat2dInterp, object_offset

    # add function for offseting
    fmagloss = Moffat2dInterp()

    delta_ra = np.zeros(len(targets), dtype=float)
    delta_dec = np.zeros(len(targets), dtype=float)
    offset_flag = np.zeros(len(targets), dtype=int)
    if "bright" in design_mode:
        boss_mag_lim = modes[design_mode].bright_limit_targets["BOSS"][:, 0]
        lunation = "bright"
        skybrightness = 1.0
    else:
        boss_mag_lim = modes[design_mode].bright_limit_targets["BOSS"][:, 0]
        lunation = "dark"
        skybrightness = 0.35
    apogee_mag_lim = modes[design_mode].bright_limit_targets["APOGEE"][:, 0]

    magnitudes = magnitude_array(targets)

    ev_boss = targets["fiber_type"] == "BOSS"
    res = object_offset(
        magnitudes[ev_boss, :],
        boss_mag_lim,
        lunation,
        "Boss",
        observatory,
        fmagloss=fmagloss,
        can_offset=targets["can_offset"].to_numpy()[ev_boss],
        skybrightness=skybrightness,
        offset_min_skybrightness=offset_min_skybrightness,
    )
    delta_ra[ev_boss] = res[0]
    delta_dec[ev_boss] = res[1]
    offset_flag[ev_boss] = res[2]

    ev_apogee = targets["fiber_type"] == "APOGEE"
    res = object_offset(
        magnitudes[ev_apogee, :],
        apogee_mag_lim,
        lunation,
        "Apogee",
        observatory,
        fmagloss=fmagloss,
        can_offset=targets["can_offset"].to_numpy()[ev_apogee],
        skybrightness=skybrightness,
        offset_min_skybrightness=offset_min_skybrightness,
    )
    delta_ra[ev_apogee] = res[0]
    delta_dec[ev_apogee] = res[1]
    offset_flag[ev_apogee] = res[2]
    return delta_ra, delta_dec, offset_flag


def bn_validation(
    targets: polars.DataFrame,
    design_mode: str,
    modes: dict,
    observatory: str = "APO",
) -> np.ndarray:  # pragma: no cover
    """
    Validate a ToO to see if it is too close
    to a bright neighbor. This functionm relies on the
    healpix map of bright neighbors for each designmode
    created with this script:
    https://github.com/sdss/mugatu/blob/master/bin/bright_neigh_healpix_map.py.
    To use this function, the environment variable BN_HEALPIX must be
    set to the path to these healpix maps.

    Parameters
    ----------
    targets: polars.DataFrame
        DataFrame with the ToO target information
    design_mode: str
        The design_mode to run the validation for
    modes : dict
        A dictionary of design mode to ``DesignMode`` object.
    observatory: str
        Observatory where observation is taking place, either
        'LCO' or 'APO'.

    Return
    ------
    valid_too_bn: np.array
        bright neighbor validation for specified design_mode.
        True means passes check.

    """

    from astropy_healpix import HEALPix

    from coordio.utils import _offset_radec

    log.debug(
        f"Running bright neighbour validation for observatory {observatory} "
        f"and design mode {design_mode}"
    )

    BN_HEALPIX = os.getenv("BN_HEALPIX")
    if BN_HEALPIX is None:
        raise ValueError("Environment variable BN_HEALPIX not set.")

    if not os.path.exists(BN_HEALPIX):
        raise ValueError(f"Path $BN_HEALPIX={BN_HEALPIX} does not exist.")

    # load the healpix indicies for the designmode
    bn_maps_boss_file = f"{BN_HEALPIX}/{design_mode}_boss_bn_healpix.parquet"
    if not os.path.exists(bn_maps_boss_file):
        raise FileNotFoundError(f"File {bn_maps_boss_file} does not exist.")
    bn_maps_boss = polars.scan_parquet(bn_maps_boss_file)

    bn_maps_apogee_file = f"{BN_HEALPIX}/{design_mode}_apogee_bn_healpix.parquet"
    if not os.path.exists(bn_maps_apogee_file):
        raise FileNotFoundError(f"File {bn_maps_apogee_file} does not exist.")
    bn_maps_apogee = polars.scan_parquet(bn_maps_apogee_file)

    # create the correct nside healpix object
    hp = HEALPix(nside=2**18, order="ring", frame="icrs")

    # calculate the offsets
    delta_ra, delta_dec, _ = calculate_offsets(
        targets,
        design_mode,
        modes=modes,
        observatory=observatory,
    )
    # get the new ra and dec
    # still need proper motion correction?
    ra_off, dec_off = _offset_radec(
        ra=targets["ra"].to_numpy(),
        dec=targets["dec"].to_numpy(),
        delta_ra=delta_ra,  # type: ignore
        delta_dec=delta_dec,  # type: ignore
    )
    # get proper motions
    pmra = targets["pmra"].to_numpy()
    pmdec = targets["pmdec"].to_numpy()
    epoch = targets["epoch"].to_numpy()
    # deal with nulls
    ev_pm_null = np.isnan(pmra) | np.isnan(pmdec)
    pmra[ev_pm_null] = 0.0
    pmdec[ev_pm_null] = 0.0
    epoch[np.isnan(epoch)] = 2016.0
    # get the indicies for all targets
    coord = SkyCoord(
        ra=ra_off * u.deg,
        dec=dec_off * u.deg,
        pm_ra_cosdec=pmra * u.mas / u.yr,
        pm_dec=pmdec * u.mas / u.yr,
        obstime=Time(epoch, format="decimalyear"),
    ).apply_space_motion(Time.now())
    hp_inds = hp.skycoord_to_healpix(coord)

    assert isinstance(hp_inds, np.ndarray)

    # check if ToOs in bright neighbor healpixels
    valid_too_bn = np.zeros(len(targets), dtype=bool) + True

    ev_boss = targets["fiber_type"] == "BOSS"
    ev_apogee = targets["fiber_type"] == "APOGEE"

    hp_inds_boss = hp_inds[ev_boss]
    hp_inds_apogee = hp_inds[ev_apogee]

    # Get bright healpixels that are in our target list.
    invalid_hp_boss = bn_maps_boss.filter(polars.col.data.is_in(hp_inds_boss))
    invalid_hp_apogee = bn_maps_apogee.filter(polars.col.data.is_in(hp_inds_apogee))

    # opposite as True == valid target
    valid_too_bn[ev_boss] = ~np.isin(hp_inds[ev_boss], invalid_hp_boss.collect())
    valid_too_bn[ev_apogee] = ~np.isin(hp_inds[ev_apogee], invalid_hp_apogee.collect())

    return valid_too_bn


def mag_limit_check(
    magnitudes: np.ndarray,
    can_offset: np.ndarray,
    offset_flag: np.ndarray,
    mag_metric: np.ndarray,
) -> np.ndarray:
    """
    perform a specific magnitude check
    Parameters
    ----------
    magnitudes: np.array
        Magnitude array for the targets. Array is of size (N, 10),
        where columns correspond to
        g, r, i, z, bp, gaia_g, rp, J, H, K band
        magnitudes.
    can_offset: np.array
        If target can be offset or not
    offset_flag: np.array
        offset_flags from offset function
    mag_metric: np.array
        the magnitude limits for the specific designmode
        and intrument
    Return
    ------
    valid_too_mag_lim: np.array
        magnitude limit validation for specified design_mode.
        True means passes check.
    """
    valid_too_mag_lim = np.zeros(len(magnitudes), dtype=bool)
    check_inds = []
    for i in range(mag_metric.shape[0]):
        if mag_metric[i][0] != -999.0 or mag_metric[i][1] != -999.0:
            check_inds.append(i)

    # run checks
    for i in range(len(valid_too_mag_lim)):
        # don't do check and make true if offset target
        if can_offset[i] and offset_flag[i] == 0:
            valid_too_mag_lim[i] = True
        else:
            # check in each band that has check defined
            targ_check = np.zeros(len(check_inds), dtype=bool)
            for j, ind in enumerate(check_inds):
                # check the magntiude for this assignment
                targ_check[j], _ = check_assign_mag_limit(
                    mag_metric[ind][0],
                    mag_metric[ind][1],
                    magnitudes[i][ind],
                )
            # if all True, then passes
            if np.all(targ_check):
                valid_too_mag_lim[i] = True
    return valid_too_mag_lim


def mag_lim_validation(
    targets: polars.DataFrame,
    design_mode: str,
    modes: dict,
    observatory: str = "APO",
) -> np.ndarray:
    """
    Perform a magnitude limit check on the ToOs
    Parameters
    ----------
    targets: polars.DataFrame
        DataFrame with the ToO target information
    design_mode: str
        The design_mode to run the validation for
    modes : dict
        A dictionary of design mode to ``DesignMode`` object.
    observatory: str
        Observatory where observation is taking place, either
        'LCO' or 'APO'.
    Return
    ------
    valid_too_mag_lim: np.array
        magnitude limit validation for specified design_mode.
        True means passes check.
    """

    log.debug(
        f"Running magnitude limit check for observatory {observatory} "
        f"and design mode {design_mode}"
    )

    valid_too_mag_lim = np.zeros(len(targets), dtype=bool)

    # get the magnitude array
    magnitudes = magnitude_array(targets)

    # get offset flags
    # calculate the offsets
    _, _, offset_flag = calculate_offsets(
        targets,
        design_mode,
        modes,
        observatory=observatory,
    )

    # do check for BOSS
    ev_boss = targets["fiber_type"] == "BOSS"
    valid_too_mag_lim[ev_boss] = mag_limit_check(
        magnitudes[ev_boss, :],
        targets["can_offset"].to_numpy()[ev_boss],
        offset_flag[ev_boss],
        modes[design_mode].bright_limit_targets["BOSS"],
    )

    # do check for APOGEE
    ev_apogee = targets["fiber_type"] == "APOGEE"
    valid_too_mag_lim[ev_apogee] = mag_limit_check(
        magnitudes[ev_apogee, :],
        targets["can_offset"].to_numpy()[ev_apogee],
        offset_flag[ev_apogee],
        modes[design_mode].bright_limit_targets["APOGEE"],
    )

    return valid_too_mag_lim


def validate_too_targets(
    targets: polars.DataFrame,
    database: PeeweeDatabaseConnection | None = None,
    bright_limit_checks: bool = False,
    bright_limit_mode: Literal["error", "discard"] = "discard",
):
    """Validates a list of ToO targets.

    Checks the following conditions:

    - The dataframe schema matches the ``too_target`` schema.
    - The ``too_id`` column is unique.
    - ``ra`` and ``dec`` are present and valid.
    - At least one of the magnitude columns is present.
    - The number of exposures is set and valid.
    - The ``active`` column is set.

    If ``bright_limit_checks`` is ``True``, the function will also run the bright
    neighbour and bright limit checks and will fail if any of the targets do not pass
    the checks. When ``bright_limit_mode`` is set to 'error', the function will raise
    a ``ValidationError`` if any of the targets do not pass the checks. Otherwise,
    invalid rows will be discarded.

    """

    from sdsstools import get_sjd

    n_targets = targets.height

    if targets.schema != too_dtypes:
        raise ValidationError("Invalid schema for ToO targets.")

    if targets.unique("too_id").height != n_targets:
        raise ValidationError("Duplicate too_id in ToO targets.")

    if targets["too_id"].has_nulls():
        raise ValidationError("Null too_id found in ToO targets.")

    if targets["sky_brightness_mode"].has_nulls():
        raise ValidationError("Null sky_brightness_mode found in ToO targets.")

    targets_coords = targets.select(["ra", "dec"]).drop_nulls()
    if len(targets_coords) < n_targets:
        raise ValidationError("Null ra/dec found in ToO targets.")

    bad_ra = (targets_coords["ra"] >= 360) | (targets_coords["ra"] < 0)
    bad_dec = (targets_coords["dec"] >= 90) | (targets_coords["dec"] <= -90)
    if bad_ra.any() or bad_dec.any():
        raise ValidationError("Invalid ra or dec found in ToO targets.")

    if targets["n_exposures"].is_null().any():
        raise ValidationError("Null 'n_exposures' column values found in ToO targets.")

    if targets["active"].is_null().any():
        raise ValidationError("Null 'active' column values found in ToO targets.")

    # Get magnitude columns and filtered rows that do not have any value set.
    # This is equivalent to Pandas .drop(axis=1). In Polars is a bit harder. See
    # https://github.com/pola-rs/polars/issues/1613
    mag_data = targets[mag_columns]
    any_mags = mag_data.with_columns(
        no_mags=polars.fold(
            True,
            lambda acc, s: acc & s.is_null(),
            polars.all(),
        )
    )
    if len(any_mags.filter(polars.col.no_mags.not_())) < n_targets:
        raise ValidationError(
            "ToOs found with missing magnitudes. "
            "At least one magnitude value is required."
        )

    if len(set(targets["fiber_type"].unique()) - set(["APOGEE", "BOSS"])) > 0:
        raise ValidationError(
            "Invalid fiber_type values. Valid values are 'APOGEE' and 'BOSS'."
        )

    # Check can_offset is a boolean and is set.
    if targets["can_offset"].is_null().any():
        raise ValidationError("Null 'can_offset' column values found in ToO targets.")

    # Check that the sky_brightness_mode value are valid.
    valid_brightness_modes = ["bright", "dark", "either"]

    targets_invalid_brightness_mode = targets.filter(
        polars.col.sky_brightness_mode.is_in(valid_brightness_modes).not_()
    )
    if len(targets_invalid_brightness_mode) > 0:
        raise ValidationError("Invalid sky_brightness_mode values found.")

    invalid_bright_limits = set()
    if bright_limit_checks:
        if database is None:
            raise ValueError("Database connection is required for bright limit checks.")

        # Validate magnitude limits and bright neighbours.
        log.info("Running bright neighbour and magnitude limit checks.")
        bn_targets = add_bright_limits_columns(targets, database)

        # For the sky brightness mode of the target,
        # check if the bright neighbour and magnitude limit.
        for bmode in valid_brightness_modes:
            bm_targets = bn_targets.filter(polars.col.sky_brightness_mode == bmode)
            bn_cols = bm_targets.select(selectors.starts_with(f"bn_{bmode}"))

            if len(bm_targets) == 0 or len(bn_cols) == 0:
                continue

            bn_invalid = bm_targets.filter(~bn_cols.fold(lambda x, y: x & y))

            mag_lim_cols = bm_targets.select(selectors.starts_with(f"mag_lim_{bmode}"))
            mag_lim_invalid = bm_targets.filter(~mag_lim_cols.fold(lambda x, y: x & y))

            if len(bn_invalid) > 0 or len(mag_lim_invalid) > 0:
                if bright_limit_mode == "error":
                    raise ValidationError(
                        f"{len(bn_invalid)} targets failed bright neighbour or "
                        "magnitude limit checks."
                    )
                else:
                    invalid_bright_limits |= set(bn_invalid["too_id"].to_list())
                    invalid_bright_limits |= set(mag_lim_invalid["too_id"].to_list())
                    log.warning(
                        f"{len(invalid_bright_limits)} targets failed bright "
                        f"neighbour or magnitude limit checks and have been discarded."
                    )

    # Fill some optional columns.
    observe_from_now = polars.lit(get_sjd("APO"), dtype=polars.Int32)
    targets = targets.with_columns(
        active=polars.col.active.fill_null(True),
        inertial=polars.col.inertial.fill_null(False),
        observed=polars.col.observed.fill_null(False),
        optical_prov=polars.col.optical_prov.fill_null(""),
        delta_ra=polars.col.delta_ra.fill_null(polars.lit(0, dtype=polars.Float32)),
        delta_dec=polars.col.delta_dec.fill_null(polars.lit(0, dtype=polars.Float32)),
        n_exposures=polars.col.n_exposures.fill_null(polars.lit(1, dtype=polars.Int16)),
        priority=polars.col.priority.fill_null(polars.lit(5, dtype=polars.Int16)),
        observe_from_mjd=polars.col.observe_from_mjd.fill_null(observe_from_now),
    ).filter(polars.col.too_id.is_in(invalid_bright_limits).not_())

    return targets


def add_bright_limits_columns(
    targets: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    observatories: list[str] = ["APO", "LCO"],
):
    """Runs the Mugatu-like validation for bright targets.

    Parameters
    ----------
    targets
        The ToO targets to validate.
    database
        The database connection to use to query the design modes.
    observatories
        The list of observatories to check.

    Returns
    -------
    data_frame
        A data frame with the same rows as the input ``targets`` with additional
        columns that indicate if a target passes the bright neighbour and magnitude
        limit checks. The order of the returned data frame is sorted by ``too_id``
        and may be different as that of the input one.

    """

    targets = targets.clone()

    # Get the field/observatory for each tile.
    if "observatory" not in targets.columns:
        targets = match_fields(targets, database)

    # Check that the sky_brightness_mode value are valid.
    valid_brightness_modes = ["bright", "dark"]

    # Get a list of design modes.
    design_modes: dict[str, DesignMode] = allDesignModes(database)

    # Add all columns for all design modes and set to false for now.
    targets = targets.with_columns(
        **{f"bn_{dm}_valid": polars.lit(False) for dm in design_modes},
        **{f"mag_lim_{dm}_valid": polars.lit(False) for dm in design_modes},
    )

    processed: list[polars.DataFrame] = []

    # First we split the targets by observatory and sky_brightness_mode (bright or
    # dark). Then we run the check for all the design modes that match that brightness
    # mode. We do not reject targets here but we mark if they pass the bright neighbour
    # and magnitude limit checks.
    for observatory in observatories:
        for bmode in valid_brightness_modes:
            targets_bmode = targets.filter(
                (
                    (polars.col.sky_brightness_mode == bmode)
                    | (polars.col.sky_brightness_mode == "either")
                )
                & (polars.col.observatory == observatory)
            )

            if len(targets_bmode) == 0:
                continue

            design_modes_bmode = [dm for dm in design_modes if dm.startswith(bmode)]

            for dmb in design_modes_bmode:
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore", category=ErfaWarning)

                        bn_mask = bn_validation(
                            targets_bmode,
                            dmb,
                            design_modes,
                            observatory=observatory,
                        )

                except FileNotFoundError:
                    # If there is not a BN file for this design mode, we just skip it
                    # and assume all targets pass the check.
                    bn_mask = np.ones(len(targets_bmode), dtype=bool)

                except Exception:
                    raise

                mag_lim_mask = mag_lim_validation(
                    targets_bmode,
                    dmb,
                    design_modes,
                    observatory=observatory,
                )

                targets_bmode = targets_bmode.with_columns(
                    **{
                        f"bn_{dmb}_valid": polars.Series(bn_mask),
                        f"mag_lim_{dmb}_valid": polars.Series(mag_lim_mask),
                    }
                )

            processed.append(targets_bmode)

    # Concatenate all the processed data frames. For targets that do not have a
    # corresponding design mode, the columns will be nulls. We fill those with False
    # as in principle we should not be able to observe the targets in those modes.
    processed_df = polars.concat(processed, how="diagonal")
    processed_df = processed_df.with_columns(
        selectors.starts_with("bn_").fill_null(False),
        selectors.starts_with("mag_lim_").fill_null(False),
    )

    # For targets with sky_brightness_mode='either', we will have two rows, one
    # evaluated for each brightness mode. We collapse the bn_ and mag_lim_ columns
    # and remove the duplicates.
    processed_either = (
        processed_df.filter(polars.col.sky_brightness_mode == "either")
        .with_columns(
            polars.selectors.starts_with("mag_").any(),
            polars.selectors.starts_with("bn_").any(),
        )
        .unique("too_id")
    )

    final_df = polars.concat(
        [
            processed_df.filter(polars.col.sky_brightness_mode != "either"),
            processed_either,
        ]
    )

    return final_df.sort("too_id")
