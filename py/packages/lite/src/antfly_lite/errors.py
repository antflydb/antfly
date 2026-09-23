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

"""Stable Antfly C ABI error codes (antfly_error_code in antfly.h).

The name/description tables here are a pure-Python mirror of
zig/pkg/antfly/src/capi/types.zig's errorCodeName/errorCodeDescription (and
go/pkg/lite/errors.go), so error classification works even when the native
library is not loaded. test_errors.py cross-checks these tables against the
loaded C library's antfly_error_code_name/antfly_error_code_description for
agreement.
"""

from __future__ import annotations

from typing import ClassVar

__all__ = [
    "OK",
    "INVALID_ARGUMENT",
    "NOT_FOUND",
    "VERSION_CONFLICT",
    "INTENT_CONFLICT",
    "TXN_NOT_FOUND",
    "BUSY",
    "OUTCOME_UNKNOWN",
    "UNSUPPORTED",
    "STALLED",
    "INTERNAL",
    "error_code_name",
    "error_code_description",
    "AntflyError",
    "InvalidArgumentError",
    "NotFoundError",
    "VersionConflictError",
    "IntentConflictError",
    "TxnNotFoundError",
    "BusyError",
    "OutcomeUnknownError",
    "UnsupportedError",
    "StalledError",
    "InternalError",
    "raise_for_code",
    "error_class_for_code",
]

OK = 0
INVALID_ARGUMENT = 1
NOT_FOUND = 2
VERSION_CONFLICT = 3
INTENT_CONFLICT = 4
TXN_NOT_FOUND = 5
BUSY = 6
# The operation crossed its publication point, but crash durability could
# not be confirmed. Inspect the destination; do not retry automatically.
OUTCOME_UNKNOWN = 7
# The operation requires a capability unavailable on this platform or
# filesystem. Retrying unchanged will not succeed.
UNSUPPORTED = 8
# A bounded drain (e.g. run_until_idle) detected a managed index making no
# forward progress and gave up.
STALLED = 9
INTERNAL = 255

_NAMES: dict[int, str] = {
    OK: "ANTFLY_OK",
    INVALID_ARGUMENT: "ANTFLY_INVALID_ARGUMENT",
    NOT_FOUND: "ANTFLY_NOT_FOUND",
    VERSION_CONFLICT: "ANTFLY_VERSION_CONFLICT",
    INTENT_CONFLICT: "ANTFLY_INTENT_CONFLICT",
    TXN_NOT_FOUND: "ANTFLY_TXN_NOT_FOUND",
    BUSY: "ANTFLY_BUSY",
    OUTCOME_UNKNOWN: "ANTFLY_OUTCOME_UNKNOWN",
    UNSUPPORTED: "ANTFLY_UNSUPPORTED",
    STALLED: "ANTFLY_STALLED",
    INTERNAL: "ANTFLY_INTERNAL",
}

_DESCRIPTIONS: dict[int, str] = {
    OK: "operation completed successfully",
    INVALID_ARGUMENT: "an argument, request, path, or open mode is invalid",
    NOT_FOUND: "the requested database object was not found",
    VERSION_CONFLICT: "a version predicate did not match the current document version",
    INTENT_CONFLICT: "a transaction intent conflicts with the requested operation",
    TXN_NOT_FOUND: "the requested transaction was not found",
    BUSY: "the requested resource is temporarily busy or changed during streaming; stabilize it and retry",
    OUTCOME_UNKNOWN: (
        "the operation was published, but crash durability could not be confirmed; "
        "inspect the destination and do not retry automatically"
    ),
    UNSUPPORTED: "the operation requires a capability that is not supported by this platform or filesystem",
    STALLED: "a bounded drain made no forward progress for its configured stall window and gave up",
    INTERNAL: "an internal error occurred",
}

_UNKNOWN_NAME = "ANTFLY_UNKNOWN_ERROR"
_UNKNOWN_DESCRIPTION = "unknown Antfly error code"


def error_code_name(code: int) -> str:
    """Stable symbolic C ABI name for `code`, e.g. "ANTFLY_BUSY"."""
    return _NAMES.get(code, _UNKNOWN_NAME)


def error_code_description(code: int) -> str:
    """Short stable description for `code`."""
    return _DESCRIPTIONS.get(code, _UNKNOWN_DESCRIPTION)


class AntflyError(Exception):
    """Base class for all Antfly C ABI errors.

    Attributes:
        code: the stable numeric antfly_error_code.
        name: the stable symbolic name, e.g. "ANTFLY_BUSY".
    """

    CODE: ClassVar[int | None] = None

    def __init__(self, code: int | None = None, message: str | None = None) -> None:
        if code is None:
            code = self.CODE
        if code is None:
            raise TypeError("AntflyError requires a code")
        self.code = int(code)
        self.name = error_code_name(self.code)
        self.message = message if message is not None else error_code_description(self.code)
        super().__init__(f"{self.name}: {self.message}")


class InvalidArgumentError(AntflyError):
    CODE = INVALID_ARGUMENT


class NotFoundError(AntflyError):
    CODE = NOT_FOUND


class VersionConflictError(AntflyError):
    CODE = VERSION_CONFLICT


class IntentConflictError(AntflyError):
    CODE = INTENT_CONFLICT


class TxnNotFoundError(AntflyError):
    CODE = TXN_NOT_FOUND


class BusyError(AntflyError):
    CODE = BUSY


class OutcomeUnknownError(AntflyError):
    CODE = OUTCOME_UNKNOWN


class UnsupportedError(AntflyError):
    CODE = UNSUPPORTED


class StalledError(AntflyError):
    CODE = STALLED


class InternalError(AntflyError):
    CODE = INTERNAL


_ERROR_CLASSES: dict[int, type[AntflyError]] = {
    INVALID_ARGUMENT: InvalidArgumentError,
    NOT_FOUND: NotFoundError,
    VERSION_CONFLICT: VersionConflictError,
    INTENT_CONFLICT: IntentConflictError,
    TXN_NOT_FOUND: TxnNotFoundError,
    BUSY: BusyError,
    OUTCOME_UNKNOWN: OutcomeUnknownError,
    UNSUPPORTED: UnsupportedError,
    STALLED: StalledError,
    INTERNAL: InternalError,
}


def error_class_for_code(code: int) -> type[AntflyError]:
    """The AntflyError subclass for `code`, or AntflyError itself for an
    unrecognized code."""
    return _ERROR_CLASSES.get(code, AntflyError)


def raise_for_code(code: int) -> None:
    """Raise the appropriate AntflyError subclass for `code`, or return None
    for ANTFLY_OK."""
    if code == OK:
        return
    raise error_class_for_code(code)(code)
