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

"""Cross-checks the pure-Python error metadata tables in antfly_lite.errors
against the loaded libantfly C ABI, and ABI/struct-layout agreement.

Mirrors go/pkg/lite's TestErrorCodeMetadataMatchesCABI.
"""

from __future__ import annotations

import ctypes

import pytest

import antfly_lite
from antfly_lite import _ffi, errors

pytestmark = pytest.mark.usefixtures("require_native")

# 0-9 and 255 are the defined antfly_error_code values; 127 exercises the
# "unknown code" fallback on both sides.
CODES = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 255, 127]


def test_error_code_metadata_matches_c_abi() -> None:
    lib = _ffi.get_lib()
    for code in CODES:
        got_name = errors.error_code_name(code)
        want_name = lib.antfly_error_code_name(code).decode()
        assert got_name == want_name, f"error code {code} name = {got_name!r}, C ABI = {want_name!r}"

        got_description = errors.error_code_description(code)
        want_description = lib.antfly_error_code_description(code).decode()
        assert got_description == want_description, (
            f"error code {code} description = {got_description!r}, C ABI = {want_description!r}"
        )


def test_validate_abi_succeeds() -> None:
    antfly_lite.validate_abi()


def test_abi_version_matches_supported_version() -> None:
    assert antfly_lite.abi_version() == _ffi.SUPPORTED_ABI_VERSION


def test_lite_open_options_struct_size_matches_c_abi() -> None:
    lib = _ffi.get_lib()
    got = lib.antfly_lite_open_options_size()
    want = ctypes.sizeof(_ffi.LiteOpenOptions)
    assert got == want, f"C ABI open options size {got}, compiled struct size {want}"
