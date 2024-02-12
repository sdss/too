#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: models.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import peewee
from sdssdb.peewee.sdss5db.catalogdb import BaseModel


__all__ = ["ToO_Target"]


class ToO_Target(BaseModel):
    too_id = peewee.IntegerField(primary_key=True)

    class Meta:
        table_name = "too_target"
