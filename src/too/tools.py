#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-10
# @Filename: tools.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import os
import pathlib
import shutil
import tempfile

from typing import TYPE_CHECKING

import httpx
import polars
import rich.progress

from too.datamodel import too_dtypes
from too.exceptions import ValidationError


if TYPE_CHECKING:
    import rich.console

    from sdssdb.connection import PeeweeDatabaseConnection


__all__ = ["download_file", "read_too_file", "match_fields"]


def read_too_file(
    path: polars.DataFrame | pathlib.Path | str,
    cast: bool = False,
) -> polars.DataFrame:
    """Reads a ToO file in CSV or parquet format.

    Parameters
    ----------
    path
        The path to the file or the dataframe to read.
    cast
        If ``True``, casts the columns to the datamodel types. This may affect
        the precision of the data.

    """

    if isinstance(path, (str, pathlib.Path)):
        path = pathlib.Path(path)

        if path.suffix == ".parquet":
            targets = polars.read_parquet(path)
        elif path.suffix == ".csv":
            targets = polars.read_csv(path)  # type: ignore
        else:
            raise ValueError(f"Invalid file type {path.suffix!r}")

    else:
        targets = path

    try:
        if cast:
            targets = targets.cast(too_dtypes)

        targets = targets.sort("added_on", "too_id").select(list(too_dtypes))

    except polars.exceptions.ColumnNotFoundError as err:
        column = str(err).splitlines()[0]
        raise ValidationError(f"Missing column {column!r} in input file.")

    except Exception as err:
        raise ValidationError(f"Error reading input file: {err!r}")

    return targets


def deduplicate_too_targets(targets: polars.DataFrame) -> polars.DataFrame:
    """Deduplicates a list of ToO targets preferring the latest ``added_on`` value."""

    targets = targets.sort("added_on", "too_id")
    targets = targets.unique(subset=["too_id"], keep="last")

    return targets


def download_file(
    url: str,
    path: os.PathLike | str,
    transient_progress: bool = False,
    console: rich.console.Console | None = None,
):
    """Downloads a file from a URL to a local path."""

    path = pathlib.Path(path).expanduser()
    path.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile() as download_file:
        with httpx.stream("GET", url) as response:
            if response.status_code != 200:
                raise RuntimeError(f"URL {url!r} does not exist.")

            total = int(response.headers["Content-Length"])

            with rich.progress.Progress(
                "[progress.percentage]{task.percentage:>3.0f}%",
                rich.progress.BarColumn(bar_width=None),
                rich.progress.DownloadColumn(),
                rich.progress.TransferSpeedColumn(),
                transient=transient_progress,
                console=console,
            ) as progress:
                download_task = progress.add_task("Download", total=total)
                for chunk in response.iter_bytes():
                    download_file.write(chunk)
                    progress.update(
                        download_task,
                        completed=response.num_bytes_downloaded,
                    )

        download_file.flush()
        shutil.move(download_file.name, path / pathlib.Path(url).name)


def match_fields(
    targets: polars.DataFrame,
    database: PeeweeDatabaseConnection,
    rs_version: str | None = None,
    check_separation: bool = False,
) -> polars.DataFrame:
    """Matches a list of targets with their fields and observatories.

    Parameters
    ----------
    targets
        The data frame of targets. It must include fully populated ``ra`` and
        ``dec`` columns.
    database
        The database connection.
    rs_version
        The robostrategy plan to use to select fields. Defaults to the
        value of the ``$RS_VERSION`` environment variable.
    check_separation
        If ``True``, checks that the separation between the target and the
        field centre is less than the FPS FoV.

    Returns
    -------
    dataframe
        The input dataframe with ``field_id`` and ``observatory`` columns.

    """

    from astropy.coordinates import SkyCoord, match_coordinates_sky

    from coordio.defaults import APO_MAX_FIELD_R, LCO_MAX_FIELD_R

    targets = targets.clone()

    if rs_version is None:
        rs_version = os.environ.get("RS_VERSION", None)
        if rs_version is None:
            raise ValueError("No rs_version provided and $RS_VERSION not set.")

    too_sc = SkyCoord(
        ra=targets["ra"].to_numpy(),
        dec=targets["dec"].to_numpy(),
        unit="deg",
        frame="icrs",
    )

    fields = polars.read_database(
        "SELECT f.field_id, f.racen AS field_ra, "
        "       f.deccen AS field_dec, o.label AS observatory "
        "FROM targetdb.field f "
        "JOIN targetdb.version v ON f.version_pk = v.pk "
        "JOIN targetdb.observatory o ON o.pk = f.observatory_pk "
        f"WHERE v.plan = '{rs_version}' AND v.robostrategy;",
        database,
    )

    if len(fields) == 0:
        raise ValueError(f"No fields found for rs_version={rs_version!r}")

    fields_sc = SkyCoord(
        ra=fields["field_ra"].to_numpy(),
        dec=fields["field_dec"].to_numpy(),
        unit="deg",
        frame="icrs",
    )

    field_idx, sep2d, _ = match_coordinates_sky(too_sc, fields_sc)

    field_to_target = fields[field_idx]
    field_to_target = field_to_target.with_columns(field_separation=sep2d.deg)

    targets = targets.hstack(field_to_target)

    if check_separation:
        for obs, max_field_r in [("APO", APO_MAX_FIELD_R), ("LCO", LCO_MAX_FIELD_R)]:
            obs_targets = targets.filter(polars.col.observatory == obs)
            if (obs_targets["field_separation"] > max_field_r).any():
                raise ValueError(
                    f"Targets with separation larger than {max_field_r} deg "
                    f"found for observatory {obs}."
                )

    return targets
