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

from too.database import (
    ToO_Target,
    connect_to_database,
    get_database_uri,
    load_too_targets,
    validate_too_targets,
)
from too.exceptions import ValidationError


def test_database_exists():
    assert catalogdb.Catalog.table_exists()
    assert catalogdb.database.dbname == DBNAME

    n_catalog = catalogdb.Catalog.select().count()
    assert n_catalog > 1000000 and n_catalog < 10000000


def test_connect_to_database_fails():
    with pytest.raises(RuntimeError):
        connect_to_database("non_existent_db")

    # Reconnect the database for the following tests.
    connect_to_database(DBNAME)
    assert catalogdb.database.connected


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


def test_validate_too_target_passes(too_mock: polars.DataFrame):
    assert validate_too_targets(too_mock)


@pytest.mark.parametrize(
    "test_mode,error_message",
    [
        ("too_id", "Duplicate too_id in ToO targets."),
        ("schema", "Invalid schema for ToO targets."),
        ("radec_null", "Null ra/dec found in ToO targets."),
        ("radec_invalid", "Invalid ra or dec found in ToO targets."),
        ("n_exposures", "Null 'n_exposures' column values found"),
        ("active", "Null 'active' column values found"),
        ("mag_columns", "ToOs found with missing magnitudes"),
    ],
)
def test_validate_too_target_fails(
    too_mock: polars.DataFrame,
    test_mode: str,
    error_message: str,
):
    too_mock_test = too_mock.clone()

    if test_mode == "too_id":
        too_mock_test[0, "too_id"] = too_mock_test[1, "too_id"]
    elif test_mode == "schema":
        too_mock_test.drop_in_place("too_id")
    elif test_mode == "radec_null":
        too_mock_test[0, "ra"] = None
    elif test_mode == "radec_invalid":
        too_mock_test[0, "ra"] = 360
    elif test_mode == "n_exposures":
        too_mock_test[0, "n_exposures"] = None
    elif test_mode == "active":
        too_mock_test[0, "active"] = None
    elif test_mode == "mag_columns":
        too_mock_test = too_mock_test.with_columns(
            gaia_g_mag=polars.lit(None, dtype=polars.Float32),
            h_mag=polars.lit(None, dtype=polars.Float32),
        )

    with pytest.raises(ValidationError, match=error_message):
        validate_too_targets(too_mock_test)


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


@pytest.mark.parametrize("extension", ["parquet", "csv", "json"])
def test_load_too_targets_from_file(
    too_mock: polars.DataFrame,
    truncate_too_target,
    tmp_path,
    extension: str,
):

    too_mock_sample = too_mock[0:1000]

    if extension == "parquet":
        file = tmp_path / "too_mock.parquet"
        too_mock_sample.write_parquet(file)
    elif extension == "csv":
        file = tmp_path / "too_mock.csv"
        too_mock_sample.write_csv(file)
    elif extension == "json":
        with pytest.raises(ValueError):
            load_too_targets("too_mock.json", get_database_uri(DBNAME))
        return

    n_added = load_too_targets(file, get_database_uri(DBNAME))

    assert n_added == 1000
    assert ToO_Target.select().count() == n_added
