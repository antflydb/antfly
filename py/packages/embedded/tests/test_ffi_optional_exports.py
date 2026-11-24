# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

"""Binding configuration against libraries that predate additive exports."""

from __future__ import annotations

from antfly_embedded import _ffi


class _Function:
    argtypes = None
    restype = None


class _LibraryWithout:
    """A stand-in library exposing every bound export except `missing`."""

    def __init__(self, *missing: str) -> None:
        self._missing = set(missing)
        self._functions: dict[str, _Function] = {}

    def __getattr__(self, name: str) -> _Function:
        if name.startswith("_") or name in self._missing:
            raise AttributeError(name)
        return self._functions.setdefault(name, _Function())


def test_configure_accepts_a_library_without_the_binary_batch_export() -> None:
    lib = _LibraryWithout("antfly_db_batch_wire")
    _ffi._configure(lib)  # type: ignore[arg-type]
    assert lib.antfly_db_batch_json.argtypes is not None


def test_configure_still_requires_core_exports() -> None:
    lib = _LibraryWithout("antfly_db_batch_json")
    try:
        _ffi._configure(lib)  # type: ignore[arg-type]
    except AttributeError:
        return
    raise AssertionError("a missing core export must fail configuration")
