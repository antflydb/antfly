# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""SQL wire helpers. Error buffers are owned even when the native call fails."""

from __future__ import annotations

import ctypes
import json
from typing import Any

from . import _ffi, errors


class SQLStateError(RuntimeError):
    def __init__(self, diagnostic: dict[str, Any], transaction_id: str | None = None) -> None:
        self.sqlstate = diagnostic["code"]
        self.diagnostic = diagnostic
        self.transaction_id = transaction_id
        super().__init__(f"{self.sqlstate}: {diagnostic['message']}")


def output(code: int, buffer: _ffi.AntflyBuffer) -> Any:
    body = _ffi.take_buffer(buffer)
    result = json.loads(body) if body else None
    if code:
        if isinstance(result, dict) and "error" in result:
            raise SQLStateError(result["error"], result.get("transaction_id"))
        errors.raise_for_code(code)
    return result


def call(database: Any, function: Any, *args: Any) -> Any:
    handle = database._acquire()
    try:
        buffer = _ffi.AntflyBuffer()
        code = function(ctypes.c_void_p(handle), *args, ctypes.byref(buffer))
        return output(code, buffer)
    finally:
        database._release()


class SQLSession:
    def __init__(self, database: Any) -> None:
        self.database = database
        self.closed = False
        handle = database._acquire()
        try:
            session = ctypes.c_uint64()
            errors.raise_for_code(
                database._lib.antfly_db_sql_session_open(ctypes.c_void_p(handle), ctypes.byref(session))
            )
            self.id = session.value
        finally:
            database._release()

    def execute(self, statement: str, parameters: Any = ()) -> Any:
        if self.closed:
            raise errors.InvalidArgumentError()
        request, keep = _ffi.make_slice(
            json.dumps(
                {"statement": statement, "parameters": list(parameters), "session_id": self.id, "limit": 4096}
            ).encode()
        )
        return call(self.database, self.database._lib.antfly_db_sql_json, request)

    def open_cursor(self, statement: str, parameters: Any = ()) -> SQLCursor:
        if self.closed:
            raise errors.InvalidArgumentError()
        return SQLCursor(self.database, statement, parameters, self.id)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        handle = self.database._acquire()
        try:
            errors.raise_for_code(self.database._lib.antfly_db_sql_session_close(ctypes.c_void_p(handle), self.id))
        finally:
            self.database._release()


class SQLCursor:
    def __init__(self, database: Any, statement: str, parameters: Any, session_id: int | None = None) -> None:
        self.database = database
        self.closed = False
        request = {"statement": statement, "parameters": list(parameters)}
        if session_id is not None:
            request["session_id"] = session_id
        data, keep = _ffi.make_slice(json.dumps(request).encode())
        cursor = ctypes.c_uint64()
        call(database, database._lib.antfly_db_sql_open_cursor_json, data, ctypes.byref(cursor))
        self.id = cursor.value

    def fetch(self, rows: int = 128) -> Any:
        if self.closed:
            raise errors.InvalidArgumentError()
        return call(self.database, self.database._lib.antfly_db_sql_fetch_cursor_json, self.id, rows)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        handle = self.database._acquire()
        try:
            errors.raise_for_code(self.database._lib.antfly_db_sql_close_cursor(ctypes.c_void_p(handle), self.id))
        finally:
            self.database._release()
