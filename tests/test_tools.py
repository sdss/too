#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-04-23
# @Filename: test_tools.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

from typing import TYPE_CHECKING

import polars
import pytest

from too.datamodel import too_dtypes
from too.exceptions import ValidationError
from too.tools import match_fields, read_too_file


if TYPE_CHECKING:
    from sdssdb.connection import PeeweeDatabaseConnection


@pytest.mark.parametrize("rs_version", ["eta-6", None])
def test_match_fields(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    rs_version: str | None,
    monkeypatch: pytest.MonkeyPatch,
):
    if rs_version is None:
        monkeypatch.setenv("RS_VERSION", "eta-6")

    targets = match_fields(too_mock, database, rs_version=rs_version)

    assert isinstance(targets, polars.DataFrame)
    assert "field_ra" in targets.columns


def test_match_fields_no_rs_version(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("RS_VERSION", raising=False)

    with pytest.raises(ValueError):
        match_fields(too_mock, database)


def test_match_fields_no_fields(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
):
    with pytest.raises(ValueError):
        match_fields(too_mock, database, "eta-9")


def test_match_fields_check_separation(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
):
    # Most random coordinates will have some that do not fall inside our tiling.
    with pytest.raises(ValueError):
        match_fields(too_mock, database, "eta-6", check_separation=True)


def test_read_too_file_missing_column():
    df = polars.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    with pytest.raises(ValidationError) as err:
        read_too_file(df)

    assert "Missing column" in str(err)


def test_read_too_file_cast_failes():
    df = polars.DataFrame({name: None for name in too_dtypes}, schema=too_dtypes)
    df = df.with_columns(g_mag=polars.Series(["hello"], dtype=polars.String))

    with pytest.raises(ValidationError) as err:
        read_too_file(df, cast=True)

    assert "Error reading input file" in str(err)
