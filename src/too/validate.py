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

from typing import TYPE_CHECKING, Tuple

import astropy.units as u
import numpy as np
import polars
from astropy.coordinates import SkyCoord
from astropy.time import Time
from astropy_healpix import HEALPix

from coordio.utils import Moffat2dInterp, _offset_radec, object_offset
from sdssdb.peewee.sdss5db.targetdb import DesignMode as DesignModeDB


if TYPE_CHECKING:
    from sdssdb import PeeweeDatabaseConnection


# load enviornment variable with path to healpix maps
BN_HEALPIX = os.getenv("BN_HEALPIX")

# add function for offseting
fmagloss = Moffat2dInterp()


def check_assign_mag_limit(mag_metric_min, mag_metric_max, assign_mag):
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
        to calculate and the distace to compare against for validation
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


def magnitude_array(targets: polars.DataFrame) -> np.ndarray:
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
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
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
) -> np.ndarray:
    """
    Validate a ToO to see if it is too close
    to a bright neighbor. This functionm relies on the
    healpix map of bright neighbors for each designmode
    created with this script:
    https://github.com/sdss/mugatu/blob/master/bin/bright_neigh_healpix_map.py.
    To use this function, the enviornment varriable BN_HEALPIX must be
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

    BN_HEALPIX = os.getenv("BN_HEALPIX")
    if BN_HEALPIX is None:
        raise ValueError("Environment variable BN_HEALPIX not set.")

    if not os.path.exists(BN_HEALPIX):
        raise ValueError(f"Path $BN_HEALPIX={BN_HEALPIX} does not exist.")

    # load the healpix indicies for the designmode
    bn_maps_boss_file = f"{BN_HEALPIX}/{design_mode}_boss_bn_healpix.parquet"
    if not os.path.exists(bn_maps_boss_file):
        raise ValueError(f"File {bn_maps_boss_file} does not exist.")
    bn_maps_boss = polars.scan_parquet(bn_maps_boss_file)

    bn_maps_apogee_file = f"{BN_HEALPIX}/{design_mode}_apogee_bn_healpix.parquet"
    if not os.path.exists(bn_maps_apogee_file):
        raise ValueError(f"File {bn_maps_apogee_file} does not exist.")
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
        and isntrument
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
                    mag_metric[ind][0], mag_metric[ind][1], magnitudes[i][ind]
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
