#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-17
# @Filename: __main__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import enum
import pathlib

from typing import Annotated

import typer
from typer.core import TyperGroup


class NaturalOrderGroup(TyperGroup):
    """A Typer group that lists commands in order of definition."""

    def list_commands(self, ctx):
        return self.commands.keys()


too_cli = typer.Typer(
    cls=NaturalOrderGroup,
    rich_markup_mode="rich",
    context_settings={"obj": {}},
    no_args_is_help=True,
    help="Command line interface for ToOs.",
)

dbname = typer.Option(
    help="The name of the database to connect to.",
    rich_help_panel="Options for connecting to the database server",
    envvar="TOO_DBNAME",
)
host = typer.Option(
    help="The host of the database server.",
    rich_help_panel="Options for connecting to the database server",
    envvar="TOO_DBHOST",
)
port = typer.Option(
    help="The port of the database server.",
    rich_help_panel="Options for connecting to the database server",
    envvar="TOO_DBPORT",
)
user = typer.Option(
    help="The user to connect to the database.",
    rich_help_panel="Options for connecting to the database server",
    envvar="TOO_DBUSER",
)


class Observatories(enum.Enum):
    APO = "APO"
    LCO = "LCO"


@too_cli.command()
def process(
    files: Annotated[
        list[pathlib.Path] | None,
        typer.Argument(
            exists=True,
            dir_okay=True,
            envvar="TOO_TARGET_FILES",
            help="The list of files to process. If a directory is passed, all the "
            "files in the directory will be processed.",
        ),
    ] = None,
    ignore_invalid: Annotated[
        bool,
        typer.Option(
            "--ignore-invalid",
            help="Exclude target files that cannot be read or cast.",
        ),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("-v", "--verbose", help="Prints more information."),
    ] = False,
    write_log: Annotated[
        pathlib.Path | None,
        typer.Option(help="Path where to write the log file."),
    ] = None,
    dbname: Annotated[str, dbname] = "sdss5db",
    host: Annotated[str, host] = "localhost",
    port: Annotated[int | None, port] = None,
    user: Annotated[str, user] = "sdss",
    cross_match: Annotated[
        bool,
        typer.Option(
            help="Cross-match the input files."
            "If --no-cross-match is passed, the carton is not run.",
            rich_help_panel="Options for loading ToOs and running the too carton.",
        ),
    ] = True,
    run_carton: Annotated[
        bool,
        typer.Option(
            help="Run the carton after cross-matching.",
            rich_help_panel="Options for loading ToOs and running the too carton.",
        ),
    ] = True,
):
    """Ingest and process new lists of ToOs."""

    import polars

    from too import (
        connect_to_database,
        load_too_targets,
        log,
        read_too_file,
        run_too_carton,
        too_dtypes,
        update_sdss_id_tables,
        xmatch_too_targets,
    )

    if verbose:
        log.sh.setLevel("DEBUG")

    if write_log:
        log.start_file_logger(str(write_log), rotating=False)

    database = connect_to_database(
        dbname,
        host=host,
        port=port,
        user=user,
    )

    if files is not None and len(files) > 0:
        process_files: list[pathlib.Path] = []
        for file in files:
            if file.is_dir():
                process_files.extend(file.glob("*.csv"))
                process_files.extend(file.glob("*.parquet"))
            else:
                process_files.append(file)

        log.debug(f"Reading {len(process_files)} input file(s).")
        targets = polars.DataFrame({}, schema=too_dtypes)
        for file in process_files:
            try:
                new_targets = read_too_file(file, cast=True)
            except Exception as ee:
                log.error(f"Failed to read file {file}: {ee}")
                if ignore_invalid:
                    continue
                else:
                    raise
            else:
                targets = targets.vstack(new_targets)
        targets = targets.sort(["added_on", "too_id"])

        log.info("Loading targets into the database.")
        load_too_targets(
            targets,
            database,
            update_existing=True,
            validate_magnitude_limits=True,
        )

    else:
        log.warning("No new files to process.")

    if cross_match:
        log.info("Cross-matching targets.")
        xmatch_too_targets(database)

        if run_carton:
            log.info("Running the ToO carton.")
            run_too_carton()
            update_sdss_id_tables(database)


@too_cli.command(name="dump-targets")
def dump_targets(
    file: Annotated[str, typer.Argument(help="The file to dump the ToO targets into.")],
    observatory: Annotated[
        Observatories,
        typer.Option(help="The observatory for which to dump the ToO targets."),
    ],
    dbname: Annotated[str, dbname] = "sdss5db",
    host: Annotated[str, host] = "localhost",
    port: Annotated[int | None, port] = None,
    user: Annotated[str, user] = "sdss",
):
    """Dumps the current ToO targets into a Parquet file."""

    from too import connect_to_database, dump_targets_to_parquet

    database = connect_to_database(dbname, host=host, port=port, user=user)
    dump_targets_to_parquet(observatory.value.upper(), file, database=database)


@too_cli.command(name="dump-sdss-id")
def dump_sdss_id(
    last_updated: Annotated[
        str,
        typer.Argument(help="The last_updated value to use to search for new targets."),
    ],
    root: Annotated[
        str,
        typer.Option(help="The root directory where to save the CSV files."),
    ] = ".",
    dbname: Annotated[str, dbname] = "sdss5db",
    host: Annotated[str, host] = "localhost",
    port: Annotated[int | None, port] = None,
    user: Annotated[str, user] = "sdss",
):
    """Dumps the ``sdss_id_flat`` and ``sdss_id_stacked`` tables for a given date.

    Searches the ``sandbox.sdss_id_stacked`` table for targets matching
    ``last_updated`` and uses that to determine the ``sdss_id_flat`` associated rows.
    The tables are saved to the current directory as CSV files.

    """

    from too import connect_to_database, dump_sdss_id_tables

    database = connect_to_database(
        dbname,
        host=host,
        port=port,
        user=user,
    )

    dump_sdss_id_tables(last_updated, database, root=root, overwrite=True)


@too_cli.command()
def validate(
    file: Annotated[str, typer.Argument(help="The file to validate.")],
    cast: Annotated[
        bool,
        typer.Option(help="Cast the columns to the datamodel types."),
    ],
):
    """Validates a ToO file."""

    import sys

    from too import log, read_too_file, validate_too_targets
    from too.exceptions import ValidationError

    df = read_too_file(file, cast=cast)

    try:
        validate_too_targets(df)
    except ValidationError as ee:
        log.error(f"Validation failed with error: {ee}")
        sys.exit(1)
    except Exception as ee:
        log.error(f"Unexpected error: {ee}")
        sys.exit(1)


if __name__ == "__main__":
    too_cli()
