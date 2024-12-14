#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-17
# @Filename: test_cli.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import datetime
import os
import pathlib

import polars
import pytest
import pytest_mock
from typer.testing import CliRunner

from sdssdb.peewee.sdss5db import catalogdb, targetdb

import too.validate
from too.__main__ import too_cli
from too.database import connect_to_database


@pytest.fixture(scope="module")
def files_path(tmp_path_factory: pytest.TempPathFactory):
    yield tmp_path_factory.mktemp("too_files")


@pytest.fixture()
def mock_validation(mocker: pytest_mock.MockFixture):
    def mock_validate_too_targets(too: polars.DataFrame, *args, **kwargs):
        return too

    mocker.patch.object(
        too.validate,
        "validate_too_targets",
        side_effect=mock_validate_too_targets,
    )


def test_cli_only_load(
    files_path: pathlib.Path,
    too_mock: polars.DataFrame,
    mock_validation,
):
    too_mock[0:1000].clone().write_csv(files_path / "too1.csv")

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


def test_cli_only_process(tmp_path_factory: pytest.TempPathFactory, mock_validation):
    database = connect_to_database("sdss5db_too_test", user="sdss", host="localhost")

    n_sdss_id_flat_pre = int(
        database.execute_sql("SELECT COUNT(1) FROM sandbox.sdss_id_flat").fetchone()[0]
    )

    runner = CliRunner()
    result = runner.invoke(too_cli, ["process", "--dbname", "sdss5db_too_test", "-v"])

    assert result.exit_code == 0

    n_target = targetdb.Target.select().count()
    assert n_target > 0

    n_sdss_id_flat_post = int(
        database.execute_sql("SELECT COUNT(1) FROM sandbox.sdss_id_flat").fetchone()[0]
    )

    assert n_sdss_id_flat_post > n_sdss_id_flat_pre

    # Now that we have run the sdss_id update, test that the dump-sdss-id command works.
    # First move to a temporary directory for the sdss_id CSV files
    sdss_id_dir = tmp_path_factory.mktemp("sdss_id")
    os.chdir(sdss_id_dir)

    today = datetime.datetime.now().strftime("%Y-%m-%d")

    result_dump_sdss_id = runner.invoke(
        too_cli,
        ["dump-sdss-id", "--dbname", "sdss5db_too_test", today],
    )
    assert result_dump_sdss_id.exit_code == 0

    assert (sdss_id_dir / "sdss_id_flat.csv").exists()
    assert (sdss_id_dir / "sdss_id_stacked.csv").exists()

    stacked = polars.read_csv(sdss_id_dir / "sdss_id_stacked.csv")
    assert len(stacked) > 0


def test_cli_update(
    files_path: pathlib.Path,
    too_mock: polars.DataFrame,
    mock_validation,
):
    too_mock[1000:1500].clone().write_csv(files_path / "too2.csv")
    too_mock[1500:2000].clone().write_parquet(files_path / "too3.parquet")

    runner = CliRunner()
    result = runner.invoke(
        too_cli,
        [
            "process",
            "--dbname",
            "sdss5db_too_test",
            "--write-log",
            str(files_path / "too.log"),
            str(files_path),
        ],
    )

    assert result.exit_code == 0
    assert (files_path / "too.log").exists()

    n_loaded = catalogdb.ToO_Target.select().count()
    assert n_loaded > 1500
    assert catalogdb.ToO_Metadata.select().count() == n_loaded

    n_target = targetdb.Target.select().count()
    assert n_target > 1500

    n_carton_to_target = (
        targetdb.CartonToTarget.select()
        .join(targetdb.Carton)
        .where(targetdb.Carton.carton == "too")
        .count()
    )
    assert n_carton_to_target == n_target


def test_cli_dump_targets(tmp_path: pathlib.Path, mock_validation):
    tmp_file = tmp_path / "dump_APO.parquet"

    runner = CliRunner()
    result = runner.invoke(
        too_cli,
        [
            "dump-targets",
            "--observatory",
            "APO",
            "--dbname",
            "sdss5db_too_test",
            str(tmp_file),
        ],
    )

    assert result.exit_code == 0
    assert tmp_file.exists()


def test_cli_validate(
    files_path: pathlib.Path,
    too_mock: polars.DataFrame,
    mock_validation,
):
    too_mock[1500:2000].clone().write_parquet(files_path / "too.parquet")

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
