#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: test_database.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import os

from typing import TYPE_CHECKING

import polars
import pytest
from conftest import DBNAME

from sdssdb.peewee.sdss5db import catalogdb

from too.database import (
    connect_to_database,
    database_uri_from_connection,
    get_active_targets,
    get_database_uri,
    load_too_targets,
    validate_too_targets,
)
from too.exceptions import ValidationError


if TYPE_CHECKING:
    from sdssdb.connection import PeeweeDatabaseConnection


PGPORT = os.environ.get("PGPORT", 5432)


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
    assert catalogdb.Gaia_DR3.table_exists()
    assert catalogdb.CatalogToGaia_DR3.table_exists()  # type: ignore
    assert catalogdb.Gaia_DR3.select().count() > 1
    assert catalogdb.CatalogToGaia_DR3.select().count() > 1  # type: ignore

    assert catalogdb.TwoMassPSC.table_exists()
    assert catalogdb.CatalogToTwoMassPSC.table_exists()  # type: ignore
    assert catalogdb.TwoMassPSC.select().count() > 1
    assert catalogdb.CatalogToTwoMassPSC.select().count() > 1  # type: ignore

    catalogdb.ToO_Target.bind(catalogdb.database)
    assert catalogdb.ToO_Target.table_exists()
    assert catalogdb.ToO_Target.select().count() == 0


@pytest.mark.parametrize(
    "dbname,user,password,host,port,expected",
    [
        ("testdb", None, None, "localhost", PGPORT, f"sdss@localhost:{PGPORT}/testdb"),
        ("db", "user", "1234", "10.1.1.1", PGPORT, f"user:1234@10.1.1.1:{PGPORT}/db"),
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


def test_validate_too_target_passes(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
):
    assert isinstance(validate_too_targets(too_mock, database), polars.DataFrame)


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
        ("fiber_type", "Invalid fiber_type values."),
        ("sloan_mags", "Found rows with incomplete Sloan magnitudes."),
        ("can_offset", "Null 'can_offset' column values found"),
        ("sky_brightness_mode", "Invalid sky_brightness_mode values found."),
    ],
)
def test_validate_too_target_fails(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
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
    elif test_mode == "fiber_type":
        too_mock_test[0, "fiber_type"] = "INVALID"
    elif test_mode == "sloan_mags":
        too_mock_test = too_mock_test.with_columns(
            u_mag=polars.lit(None, dtype=polars.Float32)
        )
    elif test_mode == "can_offset":
        too_mock_test[0, "can_offset"] = None
    elif test_mode == "sky_brightness_mode":
        too_mock_test[0, "sky_brightness_mode"] = "INVALID"

    with pytest.raises(ValidationError, match=error_message):
        validate_too_targets(too_mock_test, database)


def test_load_too_targets(too_mock: polars.DataFrame, caplog: pytest.LogCaptureFixture):
    added = load_too_targets(too_mock[0:10], catalogdb.database)

    assert added.height == 10
    assert catalogdb.ToO_Target.select().count() == 10

    # Repeat. No new targets should be added.
    added = load_too_targets(too_mock[0:10], catalogdb.database)
    assert added.height == 0
    assert catalogdb.ToO_Target.select().count() == 10

    added = load_too_targets(too_mock[5:100000], catalogdb.database)
    assert added.height == 99990
    assert catalogdb.ToO_Target.select().count() == 100000

    log_tuples = caplog.record_tuples
    assert log_tuples[-1][2] == "Running VACUUM ANALYZE on catalogdb.too_target"


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
            load_too_targets("too_mock.json", catalogdb.database)
        return

    added = load_too_targets(file, catalogdb.database)

    assert added.height == 1000
    assert catalogdb.ToO_Target.select().count() == added.height


def test_update_too_targets(
    too_mock: polars.DataFrame,
    caplog: pytest.LogCaptureFixture,
):
    load_too_targets(too_mock[0:2000], catalogdb.database)

    too_mock_sample = too_mock[0:1000]

    database_uri = database_uri_from_connection(catalogdb.database)
    db_metadata = polars.read_database(
        "SELECT * FROM catalogdb.too_metadata ORDER BY too_id",
        database_uri,
        engine="adbc",
    )
    assert len(db_metadata) == 2000
    assert db_metadata[10, "gaia_bp_mag"] is None

    # Modify a value in a row
    too_mock_sample[10, "gaia_bp_mag"] = 100.0

    # Update the metadata
    load_too_targets(too_mock_sample, catalogdb.database, update_existing=True)

    assert "Updating ToO entries" in caplog.record_tuples[-2][2]

    db_metadata = polars.read_database(
        "SELECT * FROM catalogdb.too_metadata ORDER BY too_id",
        database_uri,
        engine="adbc",
    )
    assert len(db_metadata) == 2000
    assert db_metadata[10, "gaia_bp_mag"] == 100.0


def test_update_too_targets_warning(
    too_mock: polars.DataFrame,
    caplog: pytest.LogCaptureFixture,
):
    load_too_targets(too_mock[0:1000], catalogdb.database)

    too_mock[0, "catalogid"] = 1

    result = load_too_targets(too_mock[0:1000], catalogdb.database)

    assert result.height == 0

    log_tuples = caplog.record_tuples
    assert "Some ToO targets may have changed values." in log_tuples[-2][2]
    assert "No new ToO targets to add." in log_tuples[-1][2]


def test_get_too_targets(too_mock: polars.DataFrame):

    targets = too_mock[0:10].with_columns(
        expiration_date=polars.lit(100000),
        active=polars.lit(True),
    )
    targets[5, "active"] = False

    load_too_targets(targets, catalogdb.database, update_existing=True)

    active_targets = get_active_targets(catalogdb.database)
    assert len(active_targets) == 9
