#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: conftest.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pytest

from too.mock import create_mock_too_catalogue


DBNAME: str = "sdss5db_too_test"


@pytest.fixture(autouse=True, scope="session")
def sdss5db():
    """A fixture that returns a connection to the ToO test database."""

    from sdssdb.peewee import sdss5db

    sdss5db.database.connect(DBNAME)

    yield

    sdss5db.database.close()


@pytest.fixture(scope="session")
def too_mock():
    sample = create_mock_too_catalogue()

    yield sample
