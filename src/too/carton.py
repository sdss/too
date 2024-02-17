#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-16
# @Filename: carton.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

from too import log


__all__ = ["run_too_carton"]


TOO_TARGET_PLAN = "1.1.0"


def run_too_carton():
    """Runs the ToO carton."""

    from target_selection.cartons.too import ToO_Carton  # Slow import

    log.info("Running the ToO carton.")
    too_carton = ToO_Carton(TOO_TARGET_PLAN)
    too_carton.log = log

    too_carton.run(overwrite=True)
    too_carton.load(mode="append")
