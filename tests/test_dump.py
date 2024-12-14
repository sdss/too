#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-04-24
# @Filename: test_dump.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pathlib

from typing import TYPE_CHECKING

import polars
import pytest

from too.database import load_too_targets
from too.dump import dump_targets_to_parquet
from too.xmatch import xmatch_too_targets


if TYPE_CHECKING:
    from sdssdb.connection import PeeweeDatabaseConnection


def test_dump(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    tmp_path: pathlib.Path,
):
    too_mock_sample = too_mock[0:1000]

    load_too_targets(too_mock_sample, database)
    xmatch_too_targets(
        database,
        dry_run=False,
        keep_temp=False,
        overwrite=True,
    )

    path = tmp_path / "too_dump.parquet"

    df = dump_targets_to_parquet("APO", path, database=database)

    assert isinstance(df, polars.DataFrame)
    assert path.exists()

    assert df.height > 0
    assert df.height < too_mock_sample.height


def test_dump_invalid_observatory(database: PeeweeDatabaseConnection):
    with pytest.raises(ValueError):
        dump_targets_to_parquet("ManuaKea", "/a/b.parquet", database=database)


def test_dump_invalid_database():
    with pytest.raises(ValueError):
        dump_targets_to_parquet("APO", "/a/b.parquet", database="abc")  # type: ignore
