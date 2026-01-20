#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-13
# @Filename: test_xmatch.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import polars
import pytest

from sdssdb.peewee.sdss5db import catalogdb

from too.database import load_too_targets
from too.xmatch import xmatch_too_targets


def test_xmatch_prepare(too_mock: polars.DataFrame):
    assert catalogdb.ToO_Target.select().count() == 0

    too_mock_sample = too_mock[0:100000]

    # In the mock all entries with sdss_id also has a catalogid. Manually set
    # one to null to test joining with sdss_id_mock.
    row_remove = too_mock_sample.filter(polars.col.catalogid.is_not_null()).head(1)
    too_mock_sample.with_columns(
        catalogid=polars.when(polars.col.too_id == row_remove[0, "too_id"])
        .then(None)
        .otherwise(polars.col.catalogid)
    )

    load_too_targets(too_mock_sample, catalogdb.database)
    assert catalogdb.ToO_Target.select().count() == too_mock_sample.height


def test_xmatch_1():
    n_too = catalogdb.ToO_Target.select().count()

    xmatch_planner = xmatch_too_targets(
        catalogdb.database,
        dry_run=True,
        keep_temp=True,
        overwrite=True,
    )

    assert xmatch_planner is not None

    TempCatalog = xmatch_planner.get_output_model(temporary=True)
    TempC2T = xmatch_planner.get_relational_model(
        catalogdb.ToO_Target,
        sandboxed=True,
        create=False,
    )

    n_target_id = (
        TempC2T.select().where(TempC2T.best >> 1).distinct(TempC2T.target_id).count()
    )

    assert n_target_id == n_too
    assert TempCatalog.select().count() == 30407


def test_xmatch_2(too_mock: polars.DataFrame):
    too_mock_sample = too_mock[100000:200000]
    load_too_targets(too_mock_sample, catalogdb.database)

    xmatch_planner = xmatch_too_targets(
        catalogdb.database,
        dry_run=True,
        keep_temp=True,
        overwrite=True,
    )

    assert xmatch_planner is not None

    TempCatalog = xmatch_planner.get_output_model(temporary=True)

    assert TempCatalog is not None
    assert TempCatalog.select().count() == 61498


def test_xmatch_3():
    with pytest.raises(RuntimeError):
        xmatch_too_targets(catalogdb.database, overwrite=False)


def test_xmatch_4(too_mock: polars.DataFrame):
    too_mock_sample = too_mock[300000:400000]
    load_too_targets(too_mock_sample, catalogdb.database)

    xmatch_planner = xmatch_too_targets(catalogdb.database, overwrite=True)
    assert xmatch_planner is not None

    Model = xmatch_planner.get_output_model(temporary=False)

    assert Model is not None and Model._meta.table_name == "catalog"
    assert Model.select().count() == 3090549


def test_xmatch_5(caplog: pytest.LogCaptureFixture):
    xmatch_too_targets(catalogdb.database)

    assert caplog.record_tuples[-1][2] == "All ToO targets are already matched."


def test_xmatch_duplicates(too_mock: polars.DataFrame):
    """Tests that xmatch assigns the same catalogid to ToOs that are the same object."""

    # Select a target that we have already loaded but change its too_id.
    # Load it to too_target.
    too_mock_sample = too_mock[10:500]
    too_mock_sample[10, "too_id"] = 1234567890

    load_too_targets(too_mock_sample, catalogdb.database)

    # Now perform the cross-match.
    xmatch_planner = xmatch_too_targets(catalogdb.database, overwrite=True)
    assert xmatch_planner is not None

    # We should now have two targets, with too_id=21 and too_id=1234567890, both
    # of which are associated with the same catalogid. In both cases best=True
    # (this is a special allowance for too_target).

    # First check that too_id=1234567890 was added to the database in phase 2.
    assert (
        catalogdb.CatalogToToO_Target.select()
        .where(catalogdb.CatalogToToO_Target.target == 1234567890)
        .count()
        == 1
    )

    c2t_obj = (
        catalogdb.CatalogToToO_Target.select()
        .where(catalogdb.CatalogToToO_Target.target == 1234567890)
        .first()
    )
    assert c2t_obj.best is True
    assert c2t_obj.catalogid is not None
    assert c2t_obj.added_by_phase == 2

    # Check that there are two matches for the same catalogid with best=True.
    assert (
        catalogdb.CatalogToToO_Target.select()
        .where(
            catalogdb.CatalogToToO_Target.catalog == c2t_obj.catalogid,
            catalogdb.CatalogToToO_Target.best == True,  # noqa: E712
        )
        .count()
        == 2
    )
