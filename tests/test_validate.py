#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-04-23
# @Filename: test_validate.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import too.validate
from too.exceptions import ValidationError
from too.validate import validate_bright_limits, validate_too_targets


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
        ("too_id", "Duplicate too_id in ToO targets."),
        ("schema", "Invalid schema for ToO targets."),
        ("radec_null", "Null ra/dec found in ToO targets."),
        ("radec_invalid", "Invalid ra or dec found in ToO targets."),
        ("n_exposures", "Null 'n_exposures' column values found"),
        ("active", "Null 'active' column values found"),
        ("mag_columns", "ToOs found with missing magnitudes"),
        ("fiber_type", "Invalid fiber_type values."),
        ("sloan_mags", "Found rows with incomplete Sloan magnitudes."),
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

    if test_mode == "too_id":
        too_mock_test[0, "too_id"] = too_mock_test[1, "too_id"]
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
    elif test_mode == "sloan_mags":
        too_mock_test = too_mock_test.with_columns(
            u_mag=polars.lit(None, dtype=polars.Float32)
        )
    elif test_mode == "can_offset":
        too_mock_test[0, "can_offset"] = None
    elif test_mode == "sky_brightness_mode":
        too_mock_test[0, "sky_brightness_mode"] = "INVALID"

    with pytest.raises(ValidationError, match=error_message):
        validate_too_targets(too_mock_test, database)


def test_validate_bright_limits_fails(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    mocker: pytest_mock.MockerFixture,
    caplog: pytest.LogCaptureFixture,
):
    mocker.patch.object(too.validate, "bn_validation", side_effect=RuntimeError)

    with pytest.raises(ValidationError):
        validate_bright_limits(too_mock, database)

    assert "Error validating targets for design mode" in caplog.record_tuples[-1][2]


@pytest.mark.parametrize("drop_bright_targets", [True, False])
def test_validate_too_targets_bn_invalid(
    too_mock: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    mocker: pytest_mock.MockerFixture,
    drop_bright_targets: bool,
    caplog: pytest.LogCaptureFixture,
):
    too_mock_bn = too_mock.with_columns(
        bn_valid=polars.lit(True, dtype=polars.Boolean),
        mag_lim_valid=polars.lit(True, dtype=polars.Boolean),
    )
    too_mock_bn[0, -1] = False

    mocker.patch.object(
        too.validate,
        "validate_bright_limits",
        return_value=too_mock_bn,
    )

    if not drop_bright_targets:
        with pytest.raises(ValidationError):
            validate_too_targets(
                too_mock,
                database,
                drop_bright_targets=drop_bright_targets,
            )
    else:
        validate_too_targets(
            too_mock,
            database,
            drop_bright_targets=drop_bright_targets,
        )
        assert "1 targets failed bright neighbour" in caplog.record_tuples[-1][2]
