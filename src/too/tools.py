#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-10
# @Filename: tools.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import pathlib
import shutil
import tempfile

from typing import TYPE_CHECKING

import httpx
import polars
import rich.progress

from too.datamodel import too_dtypes


if TYPE_CHECKING:
    import os

    import rich.console


__all__ = ["download_file", "read_too_file"]


def read_too_file(path: polars.DataFrame | pathlib.Path | str) -> polars.DataFrame:
    """Reads a ToO file in CSV or parquet format."""

    if isinstance(path, (str, pathlib.Path)):
        path = pathlib.Path(path)

        if path.suffix == ".parquet":
            targets = polars.read_parquet(path)
        elif path.suffix == ".csv":
            targets = polars.read_csv(path, schema=too_dtypes)
        else:
            raise ValueError(f"Invalid file type {path.suffix!r}")

    else:
        targets = path

    targets = targets.sort("too_id")
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
