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

"""Tests that never touch the native library: error class mapping and
library discovery logic. This file always runs, even in a CI job with no
libantfly available, so lint/type-check jobs have real test coverage.
"""

from __future__ import annotations

import ctypes
import json
from pathlib import Path

import pytest

from antfly_embedded import _ffi, errors
from antfly_embedded._database import Database
from antfly_embedded._library import find_library, library_file_names


def test_open_table_rejects_success_without_a_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    closed = []

    class NullTableLibrary:
        def antfly_db_open_table(self, handle, name, out):
            return errors.OK

        def antfly_db_close(self, handle):
            closed.append(handle.value)

    library = NullTableLibrary()
    monkeypatch.setattr(_ffi, "get_lib", lambda: library)
    with Database(7) as database:
        with pytest.raises(errors.InternalError, match="antfly_db_open_table.*null handle"):
            database.open_table("items")
        assert database._active == 0
    assert closed == [7]


def test_error_code_name_and_description_are_stable() -> None:
    assert errors.error_code_name(errors.BUSY) == "ANTFLY_BUSY"
    assert "retry" in errors.error_code_description(errors.BUSY)
    assert errors.error_code_name(12345) == "ANTFLY_UNKNOWN_ERROR"
    assert errors.error_code_description(12345) == "unknown Antfly error code"


@pytest.mark.parametrize(
    "code,expected_cls",
    [
        (errors.INVALID_ARGUMENT, errors.InvalidArgumentError),
        (errors.NOT_FOUND, errors.NotFoundError),
        (errors.VERSION_CONFLICT, errors.VersionConflictError),
        (errors.INTENT_CONFLICT, errors.IntentConflictError),
        (errors.TXN_NOT_FOUND, errors.TxnNotFoundError),
        (errors.BUSY, errors.BusyError),
        (errors.OUTCOME_UNKNOWN, errors.OutcomeUnknownError),
        (errors.UNSUPPORTED, errors.UnsupportedError),
        (errors.STALLED, errors.StalledError),
        (errors.INTERNAL, errors.InternalError),
    ],
)
def test_error_class_for_code(code: int, expected_cls: type[errors.AntflyError]) -> None:
    assert errors.error_class_for_code(code) is expected_cls
    with pytest.raises(expected_cls) as exc_info:
        errors.raise_for_code(code)
    err = exc_info.value
    assert err.code == code
    assert err.name == errors.error_code_name(code)


def test_error_class_for_unknown_code_is_base() -> None:
    assert errors.error_class_for_code(127) is errors.AntflyError
    with pytest.raises(errors.AntflyError) as exc_info:
        errors.raise_for_code(127)
    assert type(exc_info.value) is errors.AntflyError


def test_raise_for_code_ok_is_noop() -> None:
    errors.raise_for_code(errors.OK)


def test_find_library_prefers_explicit_env_var(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lib_path = tmp_path / library_file_names()[0]
    lib_path.write_bytes(b"")
    monkeypatch.setenv("ANTFLY_LIBRARY", str(lib_path))
    monkeypatch.delenv("ANTFLY_LIB_DIR", raising=False)
    assert find_library() == lib_path


def test_find_library_missing_explicit_env_var_does_not_fall_through(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("ANTFLY_LIBRARY", str(tmp_path / "does-not-exist"))
    monkeypatch.delenv("ANTFLY_LIB_DIR", raising=False)
    assert find_library() is None


def test_find_library_uses_lib_dir_env_var(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lib_path = tmp_path / library_file_names()[0]
    lib_path.write_bytes(b"")
    monkeypatch.delenv("ANTFLY_LIBRARY", raising=False)
    monkeypatch.setenv("ANTFLY_LIB_DIR", str(tmp_path))
    assert find_library() == lib_path


def test_native_error_detail_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    class Library:
        def antfly_last_error_code(self):
            return errors.INVALID_ARGUMENT

        def antfly_last_error_detail(self):
            return b"MissingIndexType"

    monkeypatch.setattr(_ffi, "_lib", Library())
    with pytest.raises(errors.InvalidArgumentError, match="MissingIndexType"):
        errors.raise_for_code(errors.INVALID_ARGUMENT)


def test_unrelated_native_detail_is_not_attached(monkeypatch: pytest.MonkeyPatch) -> None:
    class Library:
        def antfly_last_error_code(self):
            return errors.INVALID_ARGUMENT

        def antfly_last_error_detail(self):
            return b"MissingIndexType"

    monkeypatch.setattr(_ffi, "_lib", Library())
    with pytest.raises(errors.NotFoundError) as result:
        errors.raise_for_code(errors.NOT_FOUND)
    assert "MissingIndexType" not in str(result.value)


def test_models_directory_survives_options_marshalling(monkeypatch: pytest.MonkeyPatch) -> None:
    import ctypes
    import gc

    import antfly_embedded

    class Library:
        def antfly_open_options_init(self, options):
            return errors.OK

    monkeypatch.setattr(_ffi, "get_lib", lambda: Library())
    options, keep_alive = antfly_embedded._build_c_options(
        antfly_embedded.OpenOptions(models_dir=Path("/tmp/model-café"))
    )
    gc.collect()
    assert ctypes.string_at(options.models_dir.ptr, options.models_dir.len) == "/tmp/model-café".encode()
    assert keep_alive is not None


class _ReceiptLibrary:
    """Fake libantfly whose last call failed with an outcome receipt."""

    def __init__(self, receipt: bytes | None) -> None:
        self.receipt = receipt
        self.calls: list[str] = []
        self._held: list[object] = []

    def antfly_last_error_outcome_receipt(self, out):
        self.calls.append("receipt")
        if self.receipt is None:
            return errors.NOT_FOUND
        data = (ctypes.c_uint8 * len(self.receipt)).from_buffer_copy(self.receipt)
        self._held.append(data)
        buffer = out._obj
        buffer.ptr = ctypes.cast(data, ctypes.POINTER(ctypes.c_uint8))
        buffer.len = len(self.receipt)
        return errors.OK

    def antfly_buffer_free(self, buffer):
        self.calls.append("free")

    def antfly_last_error_code(self):
        self.calls.append("code")
        return errors.OUTCOME_UNKNOWN

    def antfly_last_error_detail(self):
        return b"commit durability unconfirmed"


RECEIPT = "or1:000102030405060708090a0b0c0d0e0f:3"


def test_outcome_unknown_carries_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    library = _ReceiptLibrary(RECEIPT.encode())
    monkeypatch.setattr(_ffi, "_lib", library)
    monkeypatch.setattr(_ffi, "get_lib", lambda: library)
    with pytest.raises(errors.OutcomeUnknownError, match="durability") as result:
        errors.raise_for_outcome_code(errors.OUTCOME_UNKNOWN)
    assert result.value.outcome_receipt == RECEIPT
    assert library.calls[0] == "receipt"


def test_outcome_unknown_without_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    library = _ReceiptLibrary(None)
    monkeypatch.setattr(_ffi, "_lib", library)
    with pytest.raises(errors.OutcomeUnknownError) as result:
        errors.raise_for_outcome_code(errors.OUTCOME_UNKNOWN)
    assert result.value.outcome_receipt is None
    assert errors.OutcomeUnknownError().outcome_receipt is None


def test_other_codes_do_not_read_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    library = _ReceiptLibrary(RECEIPT.encode())
    monkeypatch.setattr(_ffi, "_lib", library)
    with pytest.raises(errors.BusyError):
        errors.raise_for_outcome_code(errors.BUSY)
    assert "receipt" not in library.calls


def test_sql_error_body_carries_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    from antfly_embedded._sql import SQLStateError, output

    library = _ReceiptLibrary(None)
    monkeypatch.setattr(_ffi, "_lib", library)
    monkeypatch.setattr(_ffi, "get_lib", lambda: library)
    body = json.dumps(
        {"error": {"code": "40003", "message": "commit outcome unknown"}, "outcome_receipt": RECEIPT}
    ).encode()
    data = (ctypes.c_uint8 * len(body)).from_buffer_copy(body)
    buffer = _ffi.AntflyBuffer(ptr=ctypes.cast(data, ctypes.POINTER(ctypes.c_uint8)), len=len(body))
    with pytest.raises(SQLStateError) as result:
        output(errors.OUTCOME_UNKNOWN, buffer)
    assert result.value.outcome_receipt == RECEIPT
