#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-17
# @Filename: test_carton.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import polars

from sdssdb.peewee.sdss5db import targetdb

from too.carton import run_too_carton
from too.database import load_too_targets
from too.xmatch import xmatch_too_targets


def test_prepare_carton(too_mock: polars.DataFrame):
    sample = too_mock[0:10000]

    # database = connect_to_database(DBNAME)
    load_too_targets(sample, targetdb.database)
    xmatch_too_targets(targetdb.database)

    assert targetdb.Target.select().count() == 0


def test_run_carton_1():
    run_too_carton()

    assert targetdb.Target.select().count() > 0
    assert targetdb.CartonToTarget.select().count() > 0
    assert targetdb.Magnitude.select().count() > 0
    assert targetdb.Carton.select().where(targetdb.Carton.carton == "too").exists()


def test_run_carton_2(too_mock: polars.DataFrame):
    sample = too_mock[10000:20000]

    targetdb.database.connect()

    load_too_targets(sample, targetdb.database)
    xmatch_too_targets(targetdb.database)

    run_too_carton()

    assert targetdb.Target.select().count() > 15000
    assert targetdb.CartonToTarget.select().count() > 15000
    assert targetdb.Magnitude.select().count() > 15000
    assert targetdb.Carton.select().where(targetdb.Carton.carton == "too").exists()
