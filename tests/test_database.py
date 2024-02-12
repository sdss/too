#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: test_database.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import polars
import pytest
from conftest import DBNAME
from sdssdb.peewee.sdss5db import catalogdb

from too.database import ToO_Target, get_database_uri, load_too_targets


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


@pytest.mark.parametrize(
    "dbname,user,password,host,port,expected",
    [
        ("testdb", None, None, "localhost", None, "localhost/testdb"),
        ("testdb", "user", "1234", "localhost", None, "user:1234@localhost/testdb"),
        ("testdb", "user", None, "localhost", 5432, "user@localhost:5432/testdb"),
    ],
)
def test_get_database_uri(
    dbname: str,
    user: str | None,
    password: str | None,
    host: str,
    port: int | None,
    expected: str,
):

    uri = get_database_uri(
        dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )

    assert uri == f"postgresql://{expected}"


def test_get_database_uri_password_fails():
    with pytest.raises(ValueError):
        get_database_uri("testdb", password="1234")


def test_load_too_targets(too_mock: polars.DataFrame):
    n_added = load_too_targets(too_mock[0:10], get_database_uri(DBNAME))

    assert n_added == 10
    assert ToO_Target.select().count() == 10

    # Repeat. No new targets should be added.
    n_added = load_too_targets(too_mock[0:10], get_database_uri(DBNAME))
    assert n_added == 0
    assert ToO_Target.select().count() == 10

    n_added = load_too_targets(too_mock[5:100000], get_database_uri(DBNAME))
    assert n_added == 99990
    assert ToO_Target.select().count() == 100000
