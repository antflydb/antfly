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

"""PEP 249 interface to embedded Antfly. Positional parameters use :1, :2, …; native $1 syntax is also accepted.

Connections use READ COMMITTED transactions and share one native database
owner per path. Each connection has its own SQL session. CREATE/DROP TABLE
outside an active transaction execute immediately; transactional DDL and
stronger isolation are rejected by the engine.
"""

from __future__ import annotations

import datetime
import math
import re
import threading
from collections import deque
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from . import OpenOptions, create_with_options, errors, open_with_options
from ._sql import SQLStateError

apilevel = "2.0"
threadsafety = 1
paramstyle = "numeric"


class Warning(Exception):
    pass


class Error(Exception):
    sqlstate: str | None = None
    transaction_id: str | None = None


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


def _mapped(exc: Exception) -> Error:
    if isinstance(exc, SQLStateError):
        cls = {
            "0A": NotSupportedError,
            "22": DataError,
            "23": IntegrityError,
            "24": ProgrammingError,
            "25": ProgrammingError,
            "42": ProgrammingError,
            "XX": InternalError,
        }.get(exc.sqlstate[:2], OperationalError)
        result = cls(str(exc))
        result.sqlstate = exc.sqlstate
        result.transaction_id = exc.transaction_id
        return result
    if isinstance(exc, errors.AntflyError):
        return OperationalError(str(exc))
    return InterfaceError(str(exc))


def _parameters(values: Sequence[Any]) -> list[Any]:
    result = []
    for value in values:
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = bytes(value).decode("utf-8")
        elif isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            value = value.isoformat()
        elif isinstance(value, int) and not isinstance(value, bool) and not -(1 << 63) <= value < (1 << 63):
            raise DataError("integer parameter is outside the signed 64-bit range")
        elif isinstance(value, float) and not math.isfinite(value):
            raise DataError("floating-point parameter must be finite")
        result.append(value)
    return result


_owners: dict[str, tuple[Any, int, bool]] = {}
_owner_lock = threading.Lock()


def connect(path: str | Path, *, autocommit: bool = False, no_sync: bool = False) -> Connection:
    canonical = str(Path(path).resolve())
    with _owner_lock:
        if canonical in _owners:
            database, refs, existing_no_sync = _owners[canonical]
            if no_sync != existing_no_sync:
                raise InterfaceError("database is already open with different no_sync settings")
        else:
            options = OpenOptions(no_sync=no_sync)
            try:
                database = open_with_options(canonical, options)
            except errors.NotFoundError:
                database = create_with_options(canonical, options)
            except errors.AntflyError:
                if Path(canonical).exists():
                    raise
                database = create_with_options(canonical, options)
            refs = 0
        try:
            connection = Connection(database, canonical, autocommit)
        except Exception:
            if refs == 0:
                database.close()
            raise
        _owners[canonical] = (database, refs + 1, no_sync)
        return connection


class Connection:
    def __init__(self, database: Any, path: str, autocommit: bool) -> None:
        self._database = database
        self._path = path
        self._session = database.sql_session()
        self._autocommit = autocommit
        self._active = False
        self._closed = False
        self._cursors: set[Cursor] = set()

    def _check(self) -> None:
        if self._closed:
            raise InterfaceError("connection is closed")

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._check()
        if value and self._active:
            self.commit()
        self._autocommit = bool(value)

    def cursor(self) -> Cursor:
        self._check()
        cursor = Cursor(self)
        self._cursors.add(cursor)
        return cursor

    def _begin(self, statement: str) -> None:
        keyword = _keyword(statement)
        if not self._autocommit and not self._active and keyword in {"SELECT", "INSERT", "UPDATE", "DELETE", "WITH"}:
            self._session.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
            self._active = True

    def _finish(self, statement: str) -> None:
        self._check()
        for cursor in list(self._cursors):
            cursor._discard()
        try:
            self._session.execute(statement)
            self._active = False
        except Exception as exc:
            raise _mapped(exc) from exc

    def commit(self) -> None:
        self._finish("COMMIT")

    def rollback(self) -> None:
        self._finish("ROLLBACK")

    def close(self) -> None:
        if self._closed:
            return
        for cursor in list(self._cursors):
            cursor.close()
        try:
            self._session.close()
        finally:
            self._closed = True
            with _owner_lock:
                database, refs, no_sync = _owners[self._path]
                if refs == 1:
                    del _owners[self._path]
                    database.close()
                else:
                    _owners[self._path] = (database, refs - 1, no_sync)

    def __enter__(self) -> Connection:
        self._check()
        return self

    def __exit__(self, exc_type: Any, *_: Any) -> None:
        if self._closed:
            return
        if exc_type is None:
            self.commit()
        else:
            self.rollback()


class Cursor:
    arraysize = 1

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.description: tuple[Any, ...] | None = None
        self.rowcount = -1
        self.lastrowid = None
        self._closed = False
        self._native: Any = None
        self._rows: deque[tuple[Any, ...]] = deque()
        self._exhausted = True

    def _check(self) -> None:
        self.connection._check()
        if self._closed:
            raise InterfaceError("cursor is closed")

    def _discard(self) -> None:
        if self._native is not None:
            self._native.close()
            self._native = None
        self._rows.clear()
        self._exhausted = True
        self.description = None

    def close(self) -> None:
        if self._closed:
            return
        self._discard()
        self._closed = True
        self.connection._cursors.discard(self)

    def _accept(self, result: dict[str, Any]) -> None:
        columns = result["columns"]
        self.description = (
            tuple((col["name"], col["type"], None, None, None, None, None) for col in columns) if columns else None
        )
        self.rowcount = result["rows_affected"] if self.description is None else -1
        nulls = result.get("sql_nulls")
        for index, row in enumerate(result["rows"]):
            values = []
            for col_index, (column, value) in enumerate(zip(columns, row, strict=True)):
                if nulls is not None and nulls[index][col_index]:
                    value = None
                elif column["type"] == "integer" and value is not None:
                    value = int(value)
                values.append(value)
            self._rows.append(tuple(values))

    def execute(self, operation: str, parameters: Sequence[Any] = ()) -> Cursor:
        self._check()
        self._discard()
        if not operation.strip():
            raise ProgrammingError("statement is empty")
        try:
            values = _parameters(parameters)
            operation = _statement(operation)
            self.connection._begin(operation)
            try:
                self._native = self.connection._session.open_cursor(operation, values)
            except SQLStateError as exc:
                if exc.sqlstate != "0A000":
                    raise
                self._accept(self.connection._session.execute(operation, values))
                keyword = _keyword(operation)
                if keyword in {"BEGIN", "START"}:
                    self.connection._active = True
                elif keyword in {"COMMIT", "END"} or (keyword == "ROLLBACK" and "TO" not in _control_tokens(operation)):
                    self.connection._active = False
                return self
            page = self._native.fetch()
            self._accept(page["result"])
            self._exhausted = page["exhausted"]
            return self
        except Error:
            raise
        except Exception as exc:
            try:
                self._discard()
            except Exception:
                pass
            raise _mapped(exc) from exc

    def executemany(self, operation: str, seq_of_parameters: Iterable[Sequence[Any]]) -> Cursor:
        affected = 0
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
            if self.description is not None:
                self._discard()
                raise NotSupportedError("executemany does not accept statements returning rows")
            affected += self.rowcount
        self.rowcount = affected
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        self._check()
        if self.description is None:
            raise ProgrammingError("the statement does not return rows")
        try:
            while not self._rows and not self._exhausted:
                page = self._native.fetch()
                self._accept(page["result"])
                self._exhausted = page["exhausted"]
            if not self._rows:
                if self._native is not None:
                    self._native.close()
                    self._native = None
                return None
            return self._rows.popleft()
        except Exception as exc:
            raise _mapped(exc) from exc

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        size = self.arraysize if size is None else size
        if size < 0:
            raise ProgrammingError("fetch size must be nonnegative")
        result = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)
        return result

    def fetchall(self) -> list[tuple[Any, ...]]:
        result = []
        while (row := self.fetchone()) is not None:
            result.append(row)
        return result

    def setinputsizes(self, sizes: Any) -> None:
        self._check()

    def setoutputsize(self, size: Any, column: Any = None) -> None:
        self._check()

    def __iter__(self) -> Cursor:
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> Cursor:
        self._check()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
Binary = bytes


def DateFromTicks(ticks: float) -> datetime.date:
    return datetime.date.fromtimestamp(ticks)


def TimeFromTicks(ticks: float) -> datetime.time:
    return datetime.datetime.fromtimestamp(ticks).time()


def TimestampFromTicks(ticks: float) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ticks)


class _TypeCategory:
    def __init__(self, *names: str) -> None:
        self.names = names

    def __eq__(self, other: object) -> bool:
        return other in self.names


STRING = _TypeCategory("string", "uuid")
BINARY = _TypeCategory("binary")
NUMBER = _TypeCategory("integer", "number", "boolean")
DATETIME = _TypeCategory("datetime")
ROWID = _TypeCategory("uuid", "string")


def _keyword(statement: str) -> str:
    words = _control_tokens(statement)
    return words[0] if words else ""


def _control_tokens(statement: str) -> list[str]:
    clean = re.sub(r"/\*.*?\*/|--[^\n]*", " ", statement, flags=re.S)
    return re.findall(r"[A-Za-z_][A-Za-z_0-9]*|[^\s]", clean.upper())


_PARAM_TOKENS = re.compile(
    r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|--[^\n]*|/\*.*?\*/|"
    r"(?P<tag>\$(?:[A-Za-z_]\w*)?\$).*?(?P=tag)|(?<!:):(?P<parameter>[1-9]\d*)",
    re.S,
)


def _statement(operation: str) -> str:
    return _PARAM_TOKENS.sub(lambda match: "$" + match["parameter"] if match["parameter"] else match[0], operation)
