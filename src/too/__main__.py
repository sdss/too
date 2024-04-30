#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-17
# @Filename: __main__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import functools

import click
import polars
from click_option_group import OptionGroup

from too import (
    connect_to_database,
    dump_to_parquet,
    load_too_targets,
    log,
    read_too_file,
    run_too_carton,
    too_dtypes,
    xmatch_too_targets,
)


run_option_group = OptionGroup(
    "Run options",
    help="Options for loading ToO targets and running the too carton.",
)
server_option_group = OptionGroup(
    "Server options",
    help="Options for connecting to the database server.",
)


def server_options(f):
    @server_option_group.option(
        "--dbname",
        default="sdss5db",
        help="The name of the database to connect to.",
    )
    @server_option_group.option(
        "--host",
        default="localhost",
        help="The host of the database server.",
    )
    @server_option_group.option(
        "--port",
        default=None,
        help="The port of the database server.",
    )
    @server_option_group.option(
        "--user",
        default="sdss",
        help="The user to connect to the database.",
    )
    @functools.wraps(f)
    def wrapper_server_options(*args, **kwargs):
        return f(*args, **kwargs)

    return wrapper_server_options


@click.group()
def too_cli():
    """Command line interface for ToOs."""

    pass


@too_cli.command()
@click.argument("FILES", type=click.Path(exists=True), nargs=-1)
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
@server_options
@run_option_group.option(
    "--cross-match/--no-cross-match",
    default=True,
    help="Cross-match the input files. "
    "If --no-cross-match is passed, the carton is not run.",
)
@run_option_group.option(
    "--run-carton/--no-run-carton",
    default=True,
    help="Run the carton after cross-matching.",
)
def process(
    files: tuple[str],
    cross_match: bool = True,
    run_carton: bool = True,
    dbname: str = "sdss5db",
    host: str = "localhost",
    port: int | None = None,
    user: str = "sdss",
    verbose: bool = False,
    write_log: str | None = None,
):
    """Ingest and process new lists of ToOs."""

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
        load_too_targets(targets, database, update_existing=True)

    if cross_match:
        log.info("Cross-matching targets.")
        xmatch_too_targets(database)

        if run_carton:
            log.info("Running the ToO carton.")
            run_too_carton()


@too_cli.command()
@click.argument("FILE", type=str)
@click.option(
    "--observatory",
    type=click.Choice(["APO", "LCO"], case_sensitive=False),
    help="The observatory for which to dump the ToO targets.",
    required=True,
)
@server_options
def dump(
    file: str,
    observatory: str,
    dbname: str = "sdss5db",
    host: str = "localhost",
    port: int | None = None,
    user: str = "sdss",
):
    """Dumps the current ToO targets into a Parquet file."""

    database = connect_to_database(dbname, host=host, port=port, user=user)
    dump_to_parquet(observatory.upper(), file, database=database)


if __name__ == "__main__":
    too_cli()
