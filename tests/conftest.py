#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: conftest.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import random

import numpy
import peewee
import polars
import pytest
import pytest_mock

from sdssdb.peewee.sdss5db import catalogdb

import too.validate
from too.database import connect_to_database
from too.mock import create_mock_too_catalogue


DBNAME: str = "sdss5db_too_test"
SEED: int = 42


@pytest.fixture(scope="session")
def max_cid():
    """Returns the maximum ``catalogid`` in the database."""

    connect_to_database(DBNAME)
    assert catalogdb.database.connected, "Database is not connected"

    yield catalogdb.Catalog.select(peewee.fn.MAX(catalogdb.Catalog.catalogid)).scalar()


@pytest.fixture(autouse=True, scope="module")
def connect_and_revert_database(max_cid: int):
    """Reverts the database to the original state."""

    mj5 = "62abc69fd3fad42d"

    execute_sql = catalogdb.database.execute_sql

    connect_to_database(DBNAME)

    execute_sql("TRUNCATE TABLE catalogdb.too_target CASCADE;")
    execute_sql("TRUNCATE TABLE catalogdb.too_metadata;")
    execute_sql("TRUNCATE TABLE catalogdb.catalog_to_too_target;")

    execute_sql("TRUNCATE TABLE targetdb.target;")
    execute_sql("TRUNCATE TABLE targetdb.carton_to_target;")
    execute_sql("TRUNCATE TABLE targetdb.carton;")
    execute_sql("TRUNCATE TABLE targetdb.magnitude;")

    execute_sql(f"DROP TABLE IF EXISTS sandbox.catalog_{mj5};")
    execute_sql(f"DROP TABLE IF EXISTS sandbox.catalog_to_too_{mj5};")

    execute_sql("DELETE FROM catalogdb.catalog WHERE lead = 'too_target';")

    yield

    execute_sql(f"DROP TABLE IF EXISTS sandbox.catalog_{mj5};")
    execute_sql(f"DROP TABLE IF EXISTS sandbox.catalog_to_too_{mj5};")
    execute_sql(f"DELETE FROM catalogdb.catalog WHERE catalogid > {max_cid};")


@pytest.fixture(autouse=True)
def mock_bn_validation(mocker: pytest_mock.MockerFixture):
    """Mocks the bright neighbour and magnitude limit checks."""

    def mock_bn_mag_lim(bn_targets, *args, **kargs):
        return numpy.ones(len(bn_targets), dtype=bool)

    mocker.patch.object(too.validate, "bn_validation", side_effect=mock_bn_mag_lim)
    mocker.patch.object(too.validate, "mag_lim_validation", side_effect=mock_bn_mag_lim)


@pytest.fixture()
def truncate_too_target():
    """Truncates ``too_target``."""

    assert catalogdb.database.connected, "Database is not connected"
    catalogdb.database.execute_sql("TRUNCATE TABLE catalogdb.too_target CASCADE;")
    catalogdb.database.execute_sql("TRUNCATE TABLE catalogdb.too_metadata;")


@pytest.fixture(scope="session")
def too_mock():

    random.seed(SEED)
    numpy.random.seed(SEED)
    polars.set_random_seed(SEED)

    sample = create_mock_too_catalogue()

    yield sample


@pytest.fixture(scope="session")
def database():
    database = connect_to_database(DBNAME)
    assert database.connected, "Database is not connected"

    yield database
