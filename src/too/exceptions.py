#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-12
# @Filename: exceptions.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations


__all__ = ["TooError", "ValidationError"]


class TooError(Exception):
    """A base class for exceptions in the ``too`` package."""

    pass


class ValidationError(TooError):
    """An exception raised when ToO target validation fails."""

    pass
