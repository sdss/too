#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: Ilija Medan (ilija.medan@vanderbilt.edu)
# @Date: 2024-04-02
# @Filename: validate.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from astropy_healpix import HEALPix
from astropy.coordinates import SkyCoord
from astropy.time import Time
import astropy.units as u
from coordio.utils import (object_offset, Moffat2dInterp,
                           _offset_radec)
from mugatu.designmode import allDesignModes, check_assign_mag_limit
import numpy as np
import os
import pickle
import polars
from typing import Tuple

# load enviornment variable with path to healpix maps
BN_HEALPIX = os.getenv('BN_HEALPIX')

# add function for offseting
fmagloss = Moffat2dInterp()

# grab all designmode values
modes = allDesignModes()


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
    magnitudes = targets[['g_mag','r_mag', 'i_mag',
                          'z_mag', 'gaia_bp_mag',
                          'gaia_g_mag', 'gaia_rp_mag']].to_numpy()
    # add in the missing 2MASS mags as nan
    magnitudes = np.column_stack((magnitudes,
                                  np.zeros(len(targets)) + np.nan,
                                  targets['h_mag'],
                                  np.zeros(len(targets)) + np.nan))
    return magnitudes


def calculate_offsets(targets: polars.DataFrame,
                      design_mode: str,
                      offset_min_skybrightness: float = 0.,
                      observatory: str = 'APO') -> Tuple[np.ndarray,
                                                         np.ndarray,
                                                         np.ndarray]:
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
    if 'bright' in design_mode:
        boss_mag_lim = modes[design_mode].bright_limit_targets['BOSS'][:, 0]
        lunation = 'bright'
        skybrightness = 1.0
    else:
        boss_mag_lim = modes[design_mode].bright_limit_targets['BOSS'][:, 0]
        lunation = 'dark'
        skybrightness = 0.35
    apogee_mag_lim = modes[design_mode].bright_limit_targets['APOGEE'][:, 0]

    magnitudes = magnitude_array(targets)

    ev_boss = targets['fiber_type'] == 'BOSS'
    res = object_offset(magnitudes[ev_boss, :],
                        boss_mag_lim,
                        lunation,
                        'Boss',
                        observatory,
                        fmagloss=fmagloss,
                        can_offset=targets['can_offset'].to_numpy()[ev_boss],
                        skybrightness=skybrightness,
                        offset_min_skybrightness=offset_min_skybrightness)
    delta_ra[ev_boss] = res[0]
    delta_dec[ev_boss] = res[1]
    offset_flag[ev_boss] = res[2]

    ev_apogee = targets['fiber_type'] == 'APOGEE'
    res = object_offset(magnitudes[ev_apogee, :],
                        apogee_mag_lim,
                        lunation,
                        'Apogee',
                        observatory,
                        fmagloss=fmagloss,
                        can_offset=targets['can_offset'].to_numpy()[ev_apogee],
                        skybrightness=skybrightness,
                        offset_min_skybrightness=offset_min_skybrightness)
    delta_ra[ev_apogee] = res[0]
    delta_dec[ev_apogee] = res[1]
    offset_flag[ev_apogee] = res[2]
    return delta_ra, delta_dec, offset_flag


def bn_validation(targets: polars.DataFrame,
                  design_mode: str,
                  observatory: str = 'APO') -> np.ndarray:
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
    
    observatory: str
        Observatory where observation is taking place, either
        'LCO' or 'APO'.

    Return
    ------
    valid_too_bn: np.array
        bright neighbor validation for specified design_mode.
        True means passes check.
    """
    # load the healpix indicies for the designmode
    bn_maps_boss_file = '{BN_HEALPIX}/{design_mode}_{instrument}_bn_healpix.pkl'\
                            .format(BN_HEALPIX=BN_HEALPIX,
                                    instrument='boss',
                                    design_mode=design_mode)
    with open(bn_maps_boss_file, 'rb') as f:
        bn_maps_boss = pickle.load(f)

    bn_maps_apogee_file = '{BN_HEALPIX}/{design_mode}_{instrument}_bn_healpix.pkl'\
                              .format(BN_HEALPIX=BN_HEALPIX,
                                      instrument='apogee',
                                      design_mode=design_mode)
    with open(bn_maps_apogee_file, 'rb') as f:
        bn_maps_apogee = pickle.load(f)

    # create the correct nside healpix object
    hp = HEALPix(nside=2 ** 18, order='ring', frame='icrs')

    # calculate the offsets
    delta_ra, delta_dec, offset_flag = calculate_offsets(targets,
                                                         design_mode,
                                                         observatory=observatory)
    # get the new ra and dec
    # still need proper motion correction?
    ra_off, dec_off = _offset_radec(ra=targets['ra'].to_numpy(),
                                    dec=targets['dec'].to_numpy(),
                                    delta_ra=delta_ra,
                                    delta_dec=delta_dec)
    # get proper motions
    pmra = targets['pmra'].to_numpy()
    pmdec = targets['pmdec'].to_numpy()
    epoch = targets['epoch'].to_numpy()
    # deal with nulls
    ev_pm_null = np.isnan(pmra)
    pmra[ev_pm_null] = 0.
    pmdec[ev_pm_null] = 0.
    epoch[ev_pm_null] = 2016.
    # get the indicies for all targets
    coord = SkyCoord(ra=ra_off * u.deg,
                     dec=dec_off * u.deg,
                     pm_ra_cosdec=pmra * u.mas/u.yr,
                     pm_dec=pmdec * u.mas/u.yr,
                     obstime=Time(epoch, format='decimalyear')).apply_space_motion(Time.now())
    hp_inds = hp.skycoord_to_healpix(coord)

    # check if ToOs in bright neighbor healpixels
    valid_too_bn = np.zeros(len(targets), dtype=bool) + True

    ev_boss = targets['fiber_type'] == 'BOSS'
    # oppisite as True == valid target
    valid_too_bn[ev_boss] = ~np.isin(hp_inds[ev_boss], bn_maps_boss)
    # in future, is there faster way to do this? would using sets be better?

    ev_apogee = targets['fiber_type'] == 'APOGEE'
    # oppisite as True == valid target
    valid_too_bn[ev_apogee] = ~np.isin(hp_inds[ev_apogee], bn_maps_apogee)
    return valid_too_bn


def mag_limit_check(magnitudes: np.ndarray,
                    can_offset: np.ndarray,
                    offset_flag: np.ndarray,
                    mag_metric: np.ndarray) -> np.ndarray:
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
    valid_too_mag_lim = np.zeros(len(magnitudes),
                                 dtype=bool)
    check_inds = []
    for i in range(mag_metric.shape[0]):
        if mag_metric[i][0] != -999. or mag_metric[i][1] != -999.:
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
                    magnitudes[i][ind])
            # if all True, then passes
            if np.all(targ_check):
                valid_too_mag_lim[i] = True
    return valid_too_mag_lim


def mag_lim_validation(targets: polars.DataFrame,
                       design_mode: str,
                       observatory: str = 'APO') -> np.ndarray:
    """
    Perform a magnitude limit check on the ToOs

    Parameters
    ----------
    targets: polars.DataFrame
        DataFrame with the ToO target information

    design_mode: str
        The design_mode to run the validation for

    observatory: str
        Observatory where observation is taking place, either
        'LCO' or 'APO'.

    Return
    ------
    valid_too_mag_lim: np.array
        magnitude limit validation for specified design_mode.
        True means passes check.
    """
    valid_too_mag_lim = np.zeros(len(targets),
                                 dtype=bool)

    # get the magnitude array
    magnitudes = magnitude_array(targets)

    # get offset flags
    # calculate the offsets
    _, _, offset_flag = calculate_offsets(targets,
                                          design_mode,
                                          observatory=observatory)

    # do check for BOSS
    ev_boss = targets['fiber_type'] == 'BOSS'
    valid_too_mag_lim[ev_boss] = mag_limit_check(
        magnitudes[ev_boss, :],
        targets['can_offset'].to_numpy()[ev_boss],
        offset_flag[ev_boss],
        modes[design_mode].bright_limit_targets['BOSS'])

    # do check for APOGEE
    ev_apogee = targets['fiber_type'] == 'APOGEE'
    valid_too_mag_lim[ev_boss] = mag_limit_check(
        magnitudes[ev_apogee, :],
        targets['can_offset'].to_numpy()[ev_apogee],
        offset_flag[ev_apogee],
        modes[design_mode].bright_limit_targets['APOGEE'])
    return valid_too_mag_lim
