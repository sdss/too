#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: Ilija Medan (ilija.medan@vanderbilt.edu)
# @Date: 2024-04-02
# @Filename: validate.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from astropy.coordinates import SkyCoord
import astropy.units as u
from astropy_healpix import HEALPix

import numpy as np
import os
import pickle
import polars

# load enviornment variable with path to healpix maps
BN_HEALPIX = os.getenv('BN_HEALPIX')


def bn_validation(targets: polars.DataFrame, design_mode: str):
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

    # get the indicies for all targets
    coord = SkyCoord(ra=targets['ra'].to_numpy() * u.deg,
                     dec=targets['dec'].to_numpy() * u.deg)
    hp_inds = hp.skycoord_to_healpix(coord)

    # check if ToOs in bright neighbor healpixels
    valid_too = np.zeros(len(targets), dtype=bool) + True

    ev_boss = targets['fiber_type'] == 'BOSS'
    # oppisite as True == valid target
    valid_too[ev_boss] = ~np.isin(hp_inds[ev_boss], bn_maps_boss)
    # in future, is there faster way to do this? would using sets be better?

    ev_apogee = targets['fiber_type'] == 'APOGEE'
    # oppisite as True == valid target
    valid_too[ev_apogee] = ~np.isin(hp_inds[ev_apogee], bn_maps_apogee)
    return valid_too
