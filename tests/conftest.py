#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: conftest.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pytest
from sdssdb.peewee.sdss5db import catalogdb

from too.mock import create_mock_too_catalogue


DBNAME: str = "sdss5db_too_test"


@pytest.fixture(autouse=True, scope="session")
def connect_and_revert_database():
    """Reverts the database to the original state."""

    catalogdb.database.connect(DBNAME)
    catalogdb.database.execute_sql("TRUNCATE TABLE catalogdb.too_target;")


@pytest.fixture(scope="session")
def too_mock():
    sample = create_mock_too_catalogue()

    yield sample
