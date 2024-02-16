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

from sdssdb.peewee.sdss5db import catalogdb

from too.database import connect_to_database
from too.mock import create_mock_too_catalogue


SEED: int = 42
random.seed(SEED)
numpy.random.seed(SEED)
polars.set_random_seed(SEED)


DBNAME: str = "sdss5db_too_test"


@pytest.fixture(scope="session")
def max_cid():
    """Returns the maximum ``catalogid`` in the database."""

    connect_to_database(DBNAME)
    assert catalogdb.database.connected, "Database is not connected"

    yield catalogdb.Catalog.select(peewee.fn.MAX(catalogdb.Catalog.catalogid)).scalar()


@pytest.fixture(autouse=True, scope="module")
def connect_and_revert_database(max_cid: int):
    """Reverts the database to the original state."""

    connect_to_database(DBNAME)
    catalogdb.database.execute_sql("TRUNCATE TABLE catalogdb.too_target CASCADE;")
    catalogdb.database.execute_sql("TRUNCATE TABLE catalogdb.too_metadata;")
    catalogdb.database.execute_sql("TRUNCATE TABLE catalogdb.catalog_to_too_target;")

    yield

    catalogdb.database.execute_sql(
        "DROP TABLE IF EXISTS sandbox.catalog_62abc69fd3fad42d;"
    )
    catalogdb.database.execute_sql(
        f"DELETE FROM catalogdb.catalog WHERE catalogid > {max_cid};"
    )


@pytest.fixture()
def truncate_too_target():
    """Truncates ``too_target``."""

    assert catalogdb.database.connected, "Database is not connected"
    catalogdb.database.execute_sql("TRUNCATE TABLE catalogdb.too_target CASCADE;")
    catalogdb.database.execute_sql("TRUNCATE TABLE catalogdb.too_metadata;")


@pytest.fixture(scope="session")
def too_mock():
    sample = create_mock_too_catalogue()

    yield sample
