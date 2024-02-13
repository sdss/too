#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-13
# @Filename: test_xmatch.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import polars
from conftest import DBNAME

from sdssdb.peewee.sdss5db import catalogdb

from too.database import get_database_uri, load_too_targets
from too.xmatch import xmatch_too_targets


def test_xmatch(too_mock: polars.DataFrame):
    too_mock_sample = too_mock[0:10000]

    # In the mock all entries with sdss_id also has a catalogid. Manually set
    # one to null to test joining with sdss_id_mock.
    n_catalogid = too_mock_sample.filter(polars.col.catalogid.is_not_null()).height

    row_remove = too_mock_sample.filter(polars.col.catalogid.is_not_null()).head(1)
    too_mock_sample.with_columns(
        catalogid=polars.when(polars.col.too_id == row_remove[0, "too_id"])
        .then(None)
        .otherwise(polars.col.catalogid)
    )

    load_too_targets(too_mock_sample, get_database_uri(DBNAME))

    xmatch_too_targets(catalogdb.database)

    CatalogToToO_Target = catalogdb.database.models["catalogdb.catalog_to_too_target"]
    n_catalogid_after = CatalogToToO_Target.select().count()

    assert n_catalogid_after == n_catalogid

    # Assert that the row from which we removed the catalogid has been added and its
    # catalogid is the original one.
    assert (
        CatalogToToO_Target.select()
        .where(
            CatalogToToO_Target.target_id == row_remove[0, "too_id"],
            CatalogToToO_Target.catalogid == row_remove[0, "catalogid"],
        )
        .count()
    ) == 1
