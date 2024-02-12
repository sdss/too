#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: test_mock.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import polars


def test_mock_sample(too_mock: polars.DataFrame):
    assert isinstance(too_mock, polars.DataFrame)
    assert too_mock.height > 500000
