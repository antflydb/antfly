# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Locate the ``libantfly`` shared library.

Discovery order (first match wins), documented in the antfly-lite README:

1. ``ANTFLY_LIBRARY`` environment variable: an explicit path to the shared
   library file. If set but the file does not exist, discovery fails
   without falling back to any other mechanism (an explicit override that
   points nowhere is treated as a configuration error, not a hint to keep
   guessing).
2. ``ANTFLY_LIB_DIR`` environment variable: a directory containing the
   platform-appropriate library file.
3. A library bundled inside this package at ``antfly_lite/_lib/`` (for
   future platform-specific wheels; empty in the pure-Python wheel).
4. The ``antfly_cli`` package's bundled ``lib/`` directory, if antfly-cli is
   installed (its release wheels ship ``antfly_cli/lib/libantfly.*``
   alongside the native ``antfly`` binary).
5. ``zig/zig-out/lib`` found by walking up from this file's location
   (development checkouts of the antfly monorepo).
6. The system dynamic linker's search path, via
   ``ctypes.util.find_library("antfly")``.
"""

from __future__ import annotations

import ctypes.util
import importlib.util
import os
import platform
from pathlib import Path

__all__ = ["LibraryNotFoundError", "find_library", "library_file_names"]


class LibraryNotFoundError(RuntimeError):
    """Raised when the libantfly shared library cannot be located or loaded."""


def library_file_names() -> tuple[str, ...]:
    """Platform-appropriate libantfly file name(s), most preferred first."""
    system = platform.system()
    if system == "Darwin":
        return ("libantfly.dylib",)
    if system == "Windows":
        return ("antfly.dll",)
    return ("libantfly.so",)


def _find_in_dir(directory: Path) -> Path | None:
    if not directory.is_dir():
        return None
    for name in library_file_names():
        candidate = directory / name
        if candidate.is_file():
            return candidate
    return None


def _antfly_cli_lib_dir() -> Path | None:
    try:
        spec = importlib.util.find_spec("antfly_cli")
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.origin:
        return None
    return Path(spec.origin).resolve().parent / "lib"


def _source_tree_lib_dir() -> Path | None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "zig" / "zig-out" / "lib"
        if candidate.is_dir():
            return candidate
    return None


def find_library() -> Path | None:
    """Return the path to libantfly, or None if it cannot be found."""
    explicit = os.environ.get("ANTFLY_LIBRARY")
    if explicit:
        path = Path(explicit)
        return path if path.is_file() else None

    lib_dir = os.environ.get("ANTFLY_LIB_DIR")
    if lib_dir:
        found = _find_in_dir(Path(lib_dir))
        if found is not None:
            return found

    bundled = _find_in_dir(Path(__file__).resolve().parent / "_lib")
    if bundled is not None:
        return bundled

    cli_lib_dir = _antfly_cli_lib_dir()
    if cli_lib_dir is not None:
        found = _find_in_dir(cli_lib_dir)
        if found is not None:
            return found

    source_lib_dir = _source_tree_lib_dir()
    if source_lib_dir is not None:
        found = _find_in_dir(source_lib_dir)
        if found is not None:
            return found

    system_found = ctypes.util.find_library("antfly")
    if system_found:
        return Path(system_found)

    return None
