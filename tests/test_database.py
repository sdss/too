#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: test_database.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

from conftest import DBNAME
from sdssdb.peewee.sdss5db import catalogdb

from too.database import ToO_Target


def test_database_exists():
    assert catalogdb.Catalog.table_exists()
    assert catalogdb.database.dbname == DBNAME

    n_catalog = catalogdb.Catalog.select().count()
    assert n_catalog > 1000000 and n_catalog < 10000000


def test_models_exist():
    assert catalogdb.SDSS_DR13_PhotoObj.table_exists()
    assert catalogdb.CatalogToSDSS_DR13_PhotoObj_Primary.table_exists()  # type: ignore
    assert catalogdb.SDSS_DR13_PhotoObj.select().count() > 1
    assert catalogdb.CatalogToSDSS_DR13_PhotoObj_Primary.select().count() > 1  # type: ignore

    assert catalogdb.Gaia_DR3.table_exists()
    assert catalogdb.CatalogToGaia_DR3.table_exists()  # type: ignore
    assert catalogdb.Gaia_DR3.select().count() > 1
    assert catalogdb.CatalogToGaia_DR3.select().count() > 1  # type: ignore

    assert catalogdb.TwoMassPSC.table_exists()
    assert catalogdb.CatalogToTwoMassPSC.table_exists()  # type: ignore
    assert catalogdb.TwoMassPSC.select().count() > 1
    assert catalogdb.CatalogToTwoMassPSC.select().count() > 1  # type: ignore

    ToO_Target.bind(catalogdb.database)
    assert ToO_Target.table_exists()
    assert ToO_Target.select().count() == 0
