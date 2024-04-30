#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2024-02-07
# @Filename: __init__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import warnings

import sdssdb
from sdsstools import get_logger, get_package_version


sdssdb.autoconnect = False


NAME = "sdss-too"


log = get_logger(NAME, use_rich_handler=True)

console = log.rich_console
assert console is not None


__version__ = get_package_version(path=__file__, package_name=NAME)


from .carton import *
from .database import *
from .datamodel import *
from .dump import *
from .mock import *
from .tools import *
from .validate import *
from .xmatch import *
