#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-04-23
# @Filename: test_validate.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy
import pytest

import too.validate
from too.exceptions import ValidationError
from too.validate import add_bright_limits_columns, validate_too_targets


if TYPE_CHECKING:
    import polars
    import pytest_mock

    from too.database import PeeweeDatabaseConnection

import polars


def test_validate_too_target_passes(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
):
    assert isinstance(validate_too_targets(too_mock, database), polars.DataFrame)


@pytest.mark.parametrize(
    "test_mode,error_message",
    [
        ("too_id_duplicate", "Duplicate too_id in ToO targets."),
        ("too_id_null", "Null too_id found in ToO targets."),
        ("schema", "Invalid schema for ToO targets."),
        ("radec_null", "Null ra/dec found in ToO targets."),
        ("radec_invalid", "Invalid ra or dec found in ToO targets."),
        ("n_exposures", "Null 'n_exposures' column values found"),
        ("active", "Null 'active' column values found"),
        ("mag_columns", "ToOs found with missing magnitudes"),
        ("fiber_type", "Invalid fiber_type values."),
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

    if test_mode == "too_id_duplicate":
        too_mock_test[0, "too_id"] = too_mock_test[1, "too_id"]
    elif test_mode == "too_id_null":
        too_mock_test[0, "too_id"] = None
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
    elif test_mode == "can_offset":
        too_mock_test[0, "can_offset"] = None
    elif test_mode == "sky_brightness_mode":
        too_mock_test[0, "sky_brightness_mode"] = "INVALID"

    with pytest.raises(ValidationError, match=error_message):
        validate_too_targets(too_mock_test, database)


def test_add_bright_limits_columns_fails(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch.object(too.validate, "bn_validation", side_effect=RuntimeError)

    with pytest.raises(RuntimeError):
        add_bright_limits_columns(too_mock, database)


def test_validate_too_targets_bn_invalid(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    mocker: pytest_mock.MockerFixture,
):
    too_mock_bn = too_mock.with_columns(
        bn_dark_monit_valid=polars.lit(True, dtype=polars.Boolean),
        mag_lim_dark_monit_valid=polars.lit(True, dtype=polars.Boolean),
    )
    too_mock_bn[0, "sky_brightness_mode"] = "dark"
    too_mock_bn[0, "mag_lim_dark_monit_valid"] = False

    mocker.patch.object(
        too.validate,
        "add_bright_limits_columns",
        return_value=too_mock_bn,
    )

    with pytest.raises(ValidationError):
        validate_too_targets(
            too_mock,
            database,
            bright_limit_checks=True,
            bright_limit_mode="error",
        )


def test_validate_too_targets_bn_no_database(too_mock: polars.DataFrame):
    with pytest.raises(ValueError):
        validate_too_targets(
            too_mock,
            database=None,
            bright_limit_checks=True,
        )


def test_add_bright_limits_columns(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    mocker: pytest_mock.MockerFixture,
):
    # Simulate that bn_validation returns all False
    mocker.patch.object(
        too.validate,
        "bn_validation",
        side_effect=lambda targets, *_, **__: numpy.zeros(len(targets)).astype(bool),
    )

    df = add_bright_limits_columns(too_mock[0:1000], database)

    assert isinstance(df, polars.DataFrame)
    assert "bn_dark_monit_valid" in df.columns
    assert not df[0, "bn_dark_monit_valid"]
    assert not df["mag_lim_dark_monit_valid"].all()


def test_add_bright_limits_columns_filenotfound(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch.object(
        too.validate,
        "bn_validation",
        side_effect=FileNotFoundError,
    )

    df = add_bright_limits_columns(too_mock[0:1000], database)

    assert isinstance(df, polars.DataFrame)
    assert "bn_dark_monit_valid" in df.columns

    assert df["bn_dark_monit_valid"].any()


def test_add_bright_limits_columns_raises(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch.object(
        too.validate,
        "bn_validation",
        side_effect=ValueError,
    )

    with pytest.raises(ValueError):
        add_bright_limits_columns(too_mock[0:1000], database)
