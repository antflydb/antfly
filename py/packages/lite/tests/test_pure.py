"""Tests that never touch the native library: error class mapping and
library discovery logic. This file always runs, even in a CI job with no
libantfly available, so lint/type-check jobs have real test coverage.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from antfly_lite import errors
from antfly_lite._library import find_library, library_file_names


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
