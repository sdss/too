#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-11
# @Filename: create_test_database.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess

from sdssdb.peewee.sdss5db import catalogdb

from too import console, log
from too.tools import download_file


BASE_URL = "https://data.sdss5.org/resources/target/mocks/samples/sdss5db_too_test"
CACHE_PATH = pathlib.Path("~/.cache/sdss/too/samples/sdss5db_too_test").expanduser()

DBNAME = "sdss5db_too_test"


def create_test_database(
    user: str | None = None,
    host: str | None = None,
    port: int | None = None,
):
    """Recreates a simple copy of ``catalogdb`` for testing.

    This script creates populates a database called ``sdss5db_too_test`` with
    several of the ``catalogdb`` tables, with rows limited to the sample files.

    While in principle it should not be possible to affect the production database
    as the database names are different, please do not run this script at Utah or
    with a tunnel to the production database.

    This script requires the database to have been previously created.

    Parameters
    ----------
    user
        The user to connect to the database. If not provided, the default user
        will be used.
    host
        The host where the database is running. If not provided, the default
        host will be used.
    port
        The port where the database is running. If not provided, the default
        port will be used.

    """

    if os.environ.get("CI") == "true":
        log.warning("Running in CI.")

    log.info("Verifying that the database exists.")

    if not catalogdb.database.connect(DBNAME, user=user, host=host, port=port):
        raise ConnectionError("Failed connecting to the database.")

    catalog_exists = catalogdb.Catalog.table_exists()
    if catalog_exists:
        raise RuntimeError(
            "The table catalog exists. "
            "This script can only run on an empty database."
        )

    files = [
        "catalog.csv.gz",
        "sdss_id_stacked.csv.gz",
        # "catalog_to_sdss_dr13_photoobj_primary.csv.gz",
        "catalog_to_gaia_dr3_source.csv.gz",
        "catalog_to_twomass_psc.csv.gz",
        # "sdss_dr13_photoobj.csv.gz",
        "gaia_dr3_source.csv.gz",
        "twomass_psc.csv.gz",
    ]
    for file in files:
        if not (CACHE_PATH / file).exists():
            url = f"{BASE_URL}/{file}"
            log.info(f"File {file!r} not found in cache. Downloading from {url}")
            download_file(url, CACHE_PATH, transient_progress=True, console=console)

    shutil.copy2(pathlib.Path(__file__).parent / "design_mode.csv", CACHE_PATH)

    log.info("Proceeding with the creation of the test database.")
    script_path = pathlib.Path(__file__).parent / "sdss5db_too_test.sql"
    subprocess.run(
        f"psql -d {DBNAME} -f {script_path!s}",
        shell=True,
        cwd=CACHE_PATH,
    )


if __name__ == "__main__":
    create_test_database()
