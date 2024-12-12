#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-17
# @Filename: test_cli.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pathlib

import polars
import pytest
from typer.testing import CliRunner

from sdssdb.peewee.sdss5db import catalogdb, targetdb

from too.__main__ import too_cli


@pytest.fixture(scope="module")
def files_path(tmp_path_factory: pytest.TempPathFactory):
    yield tmp_path_factory.mktemp("too_files")


def test_cli_only_load(files_path: pathlib.Path, too_mock: polars.DataFrame):
    too_mock[0:100000].clone().write_csv(files_path / "too1.csv")

    runner = CliRunner()
    result = runner.invoke(
        too_cli,
        [
            "process",
            "--dbname",
            "sdss5db_too_test",
            "--no-cross-match",
            "--no-run-carton",
            str(files_path / "too1.csv"),
        ],
    )

    assert result.exit_code == 0

    n_loaded = catalogdb.ToO_Target.select().count()
    assert n_loaded > 0
    assert catalogdb.ToO_Metadata.select().count() == n_loaded


def test_cli_only_process():
    runner = CliRunner()
    result = runner.invoke(too_cli, ["process", "--dbname", "sdss5db_too_test", "-v"])

    assert result.exit_code == 0

    n_target = targetdb.Target.select().count()
    assert n_target > 0


def test_cli_update(files_path: pathlib.Path, too_mock: polars.DataFrame):
    too_mock[100000:150000].clone().write_csv(files_path / "too2.csv")
    too_mock[150000:200000].clone().write_parquet(files_path / "too3.parquet")

    runner = CliRunner()
    result = runner.invoke(
        too_cli,
        [
            "process",
            "--dbname",
            "sdss5db_too_test",
            "--write-log",
            str(files_path / "too.log"),
            str(files_path / "too2.csv"),
            str(files_path / "too3.parquet"),
        ],
    )

    assert result.exit_code == 0
    assert (files_path / "too.log").exists()

    n_loaded = catalogdb.ToO_Target.select().count()
    assert n_loaded > 150000
    assert catalogdb.ToO_Metadata.select().count() == n_loaded

    n_target = targetdb.Target.select().count()
    assert n_target > 150000

    n_carton_to_target = (
        targetdb.CartonToTarget.select()
        .join(targetdb.Carton)
        .where(targetdb.Carton.carton == "too")
        .count()
    )
    assert n_carton_to_target == n_target


def test_cli_dump(tmp_path: pathlib.Path):
    tmp_file = tmp_path / "dump_APO.parquet"

    runner = CliRunner()
    result = runner.invoke(
        too_cli,
        [
            "dump",
            "--observatory",
            "APO",
            "--dbname",
            "sdss5db_too_test",
            str(tmp_file),
        ],
    )

    assert result.exit_code == 0
    assert tmp_file.exists()


def test_cli_validate(files_path: pathlib.Path, too_mock: polars.DataFrame):
    too_mock[150000:200000].clone().write_parquet(files_path / "too.parquet")

    runner = CliRunner()
    result = runner.invoke(
        too_cli,
        [
            "validate",
            "--cast",
            str(files_path / "too.parquet"),
        ],
    )

    assert result.exit_code == 0
