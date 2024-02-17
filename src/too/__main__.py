#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-17
# @Filename: __main__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import click
import polars
from click_option_group import OptionGroup


run_options = OptionGroup(
    "Run options",
    help="Options for loading ToO targets and running the too carton.",
)
server_options = OptionGroup(
    "Server options",
    help="Options for connecting to the database server.",
)


@click.command()
@click.argument("FILES", type=click.Path(exists=True), nargs=-1)
@run_options.option(
    "--cross-match/--no-cross-match",
    default=True,
    help="Cross-match the input files. "
    "If --no-cross-match is passed, the carton is not run.",
)
@run_options.option(
    "--run-carton/--no-run-carton",
    default=True,
    help="Run the carton after cross-matching.",
)
@server_options.option(
    "--dbname",
    default="sdss5db",
    help="The name of the database to connect to.",
)
@server_options.option(
    "--host",
    default="localhost",
    help="The host of the database server.",
)
@server_options.option(
    "--port",
    default=None,
    help="The port of the database server.",
)
@server_options.option(
    "--user",
    default=None,
    help="The user to connect to the database.",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Prints more information.",
)
@click.option(
    "--write-log",
    type=click.Path(),
    help="Path where to write the log file.",
)
def too_cli(
    files: tuple[str],
    cross_match: bool = True,
    run_carton: bool = True,
    dbname: str = "sdss5db",
    host: str = "localhost",
    port: int | None = None,
    user: str | None = None,
    verbose: bool = False,
    write_log: str | None = None,
):
    """Command line interface for ToOs."""

    from too import (
        connect_to_database,
        load_too_targets,
        log,
        read_too_file,
        run_too_carton,
        too_dtypes,
        xmatch_too_targets,
    )

    if verbose:
        log.sh.setLevel("DEBUG")

    if write_log:
        log.start_file_logger(write_log, rotating=False)

    database = connect_to_database(dbname, host=host, port=port, user=user)

    if len(files) > 0:
        log.debug("Reading input files.")
        targets = polars.DataFrame({}, schema=too_dtypes)
        for file in files:
            targets = targets.vstack(read_too_file(file))

        log.info("Loading targets into the database.")
        load_too_targets(targets, database)

    if cross_match:
        log.info("Cross-matching targets.")
        xmatch_too_targets(database)

        if run_carton:
            log.info("Running the ToO carton.")
            run_too_carton()


if __name__ == "__main__":
    too_cli()
