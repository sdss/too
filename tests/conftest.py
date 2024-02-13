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


@pytest.fixture(autouse=True, scope="module")
def connect_and_revert_database():
    """Reverts the database to the original state."""

    connect_to_database(DBNAME)
    catalogdb.database.execute_sql("TRUNCATE TABLE catalogdb.too_target;")


@pytest.fixture()
def truncate_too_target():
    """Truncates ``too_target``."""

    assert catalogdb.database.connected, "Database is not connected"
    catalogdb.database.execute_sql("TRUNCATE TABLE catalogdb.too_target;")


@pytest.fixture(scope="session")
def too_mock():
    sample = create_mock_too_catalogue()

    yield sample
