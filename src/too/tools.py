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
import rich.progress


if TYPE_CHECKING:
    import os

    import rich.console


__all__ = ["download_file"]


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
