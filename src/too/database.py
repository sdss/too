#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: database.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import peewee
from sdssdb.peewee import BaseModel


__all__ = ["ToO_Target"]


class ToOBaseModel(BaseModel):
    class Meta:
        schema = "catalogdb"


class ToO_Target(ToOBaseModel):
    too_id = peewee.IntegerField(primary_key=True)
    fiber_type = peewee.TextField()
    catalogid = peewee.IntegerField()
    sdss_id = peewee.IntegerField()
    gaia_dr3_source_id = peewee.IntegerField()
    twomass_pts_key = peewee.IntegerField()
    sky_brightness_mode = peewee.TextField()
    ra = peewee.FloatField()
    dec = peewee.FloatField()
    pmra = peewee.FloatField()
    pmdec = peewee.FloatField()
    epoch = peewee.FloatField()
    parallax = peewee.FloatField()
    lambda_eff = peewee.FloatField()
    u_mag = peewee.FloatField()
    g_mag = peewee.FloatField()
    r_mag = peewee.FloatField()
    i_mag = peewee.FloatField()
    z_mag = peewee.FloatField()
    optical_prov = peewee.TextField()
    gaia_bp_mag = peewee.FloatField()
    gaia_rp_mag = peewee.FloatField()
    gaia_g_mag = peewee.FloatField()
    h_mag = peewee.FloatField()
    delta_ra = peewee.FloatField()
    delta_dec = peewee.FloatField()
    inertial = peewee.BooleanField()
    n_exposures = peewee.IntegerField()
    priority = peewee.IntegerField()
    active = peewee.BooleanField()
    expiration_date = peewee.IntegerField()
    observed = peewee.BooleanField()

    class Meta:
        table_name = "too_target"
