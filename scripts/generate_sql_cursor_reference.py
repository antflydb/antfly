#!/usr/bin/env python3
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

"""Byte-level PostgreSQL cursor oracle: simple BINARY and Bind overrides."""

import argparse
import json
import socket
import struct
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres
from psycopg.pq import ExecStatus

TARGET = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly/src/pgwire/fixtures/cursor_reference.json"
)


def verify_suspended_fetch(db):
    """libpq exposes no Execute row limit; check utility portal semantics raw."""

    def frame(connection, tag, payload=b""):
        connection.sendall(tag + struct.pack("!I", len(payload) + 4) + payload)

    def read(connection, length):
        value = bytearray()
        while len(value) < length:
            chunk = connection.recv(length - len(value))
            if not chunk:
                raise AssertionError("oracle socket closed before completion")
            value.extend(chunk)
        return bytes(value)

    def until(connection, terminal=b"Z", expected_error=None):
        events = []
        while True:
            header = read(connection, 5)
            body = read(connection, struct.unpack("!I", header[1:])[0] - 4)
            if header[:1] == b"E":
                if expected_error is None or b"C" + expected_error + b"\0" not in body:
                    raise AssertionError(body)
                events.append((b"E", expected_error))
            if header[:1] in (b"D", b"C", b"s"):
                events.append((header[:1], body))
            if header[:1] == terminal:
                return events

    def data(n):
        return (b"D", struct.pack("!Hiq", 1, 8, n))

    for scroll in (False, True):
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(15)
            connection.connect(db.info.host + "/.s.PGSQL." + str(db.info.port))
            startup = b"user\0" + db.info.user.encode() + b"\0database\0postgres\0\0"
            connection.sendall(struct.pack("!II", len(startup) + 8, 196608) + startup)
            until(connection)
            for sql in (
                "BEGIN ISOLATION LEVEL READ COMMITTED",
                f"DECLARE rows BINARY {'SCROLL' if scroll else 'NO SCROLL'} CURSOR FOR SELECT id FROM usage_records ORDER BY id",
            ):
                frame(connection, b"Q", sql.encode() + b"\0")
                until(connection)
            frame(connection, b"P", b"fetch\0FETCH FORWARD 4 FROM rows\0\0\0")
            frame(
                connection, b"B", b"portal\0fetch\0" + struct.pack("!HHHH", 0, 0, 1, 1)
            )
            frame(connection, b"E", b"portal\0" + struct.pack("!I", 1))
            frame(connection, b"H")
            assert until(connection, b"s") == [data(0), (b"s", b"")]
            # Evaluation has consumed all four rows before the first page.
            frame(connection, b"Q", b"FETCH NEXT FROM rows\0")
            assert until(connection) == [data(4), (b"C", b"FETCH 1\0")]
            # Already evaluated portal results survive CLOSE without adoption
            # of a new cursor using the same name.
            frame(connection, b"Q", b"CLOSE rows\0")
            assert until(connection) == [(b"C", b"CLOSE CURSOR\0")]
            frame(connection, b"Q", b"DECLARE rows CURSOR FOR SELECT 99::bigint\0")
            until(connection)
            frame(connection, b"E", b"portal\0" + struct.pack("!I", 0))
            frame(connection, b"S")
            assert until(connection) == [
                data(1),
                data(2),
                data(3),
                (b"C", b"FETCH 3\0"),
            ]
            frame(connection, b"E", b"portal\0" + struct.pack("!I", 0))
            frame(connection, b"S")
            assert until(connection) == [(b"C", b"FETCH 0\0")]
            frame(connection, b"Q", b"ROLLBACK\0")
            until(connection)
            # A previously evaluated portal cannot escape an aborted block.
            for sql in (
                "BEGIN ISOLATION LEVEL READ COMMITTED",
                "DECLARE rows BINARY SCROLL CURSOR FOR SELECT id FROM usage_records ORDER BY id",
            ):
                frame(connection, b"Q", sql.encode() + b"\0")
                until(connection)
            frame(connection, b"P", b"failed\0FETCH FORWARD 4 FROM rows\0\0\0")
            frame(
                connection, b"B", b"broken\0failed\0" + struct.pack("!HHHH", 0, 0, 1, 1)
            )
            frame(connection, b"E", b"broken\0" + struct.pack("!I", 1))
            frame(connection, b"H")
            assert until(connection, b"s") == [data(0), (b"s", b"")]
            frame(connection, b"Q", b"CLOSE missing_cursor\0")
            assert until(connection, expected_error=b"34000") == [(b"E", b"34000")]
            frame(connection, b"E", b"broken\0" + struct.pack("!I", 0))
            frame(connection, b"S")
            assert until(connection, expected_error=b"25P02") == [(b"E", b"25P02")]
            frame(connection, b"Q", b"ROLLBACK\0")
            until(connection)


def reference():
    original = next(
        e
        for e in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        if e["id"] == "sql-0049"
    )
    assert original["params"] == []
    profiles = []
    with postgres() as db:
        db.execute("CREATE TABLE usage_records(id BIGINT PRIMARY KEY)")
        db.execute("INSERT INTO usage_records SELECT generate_series(0,4)")
        verify_suspended_fetch(db)
        for binary in (False, True):
            for scroll, hold in ((False, False), (True, False), (True, True)):
                name = "usage_scroll_cursor"
                declaration = (
                    original["sql"]
                    if binary and scroll and hold
                    else f"DECLARE {name} {'BINARY ' if binary else ''}{'SCROLL' if scroll else 'NO SCROLL'} CURSOR {'WITH' if hold else 'WITHOUT'} HOLD FOR SELECT id FROM usage_records ORDER BY id"
                )
                steps = [
                    ("BEGIN ISOLATION LEVEL READ COMMITTED", None),
                    (declaration, None),
                ]
                if hold:
                    steps.append(("COMMIT", None))
                steps.extend(
                    [
                        (f"FETCH FORWARD 2 FROM {name}", None),
                        (f"FETCH NEXT FROM {name}", 0),
                        (f"FETCH NEXT FROM {name}", 1),
                    ]
                )
                if scroll:
                    steps.extend(
                        [
                            (f"FETCH ABSOLUTE -1 FROM {name}", None),
                            (f"FETCH BACKWARD 2 FROM {name}", 0),
                            (f"FETCH RELATIVE -1 FROM {name}", 1),
                            (f"MOVE ABSOLUTE 0 FROM {name}", 1),
                            (f"FETCH FORWARD 0 FROM {name}", None),
                        ]
                    )
                steps.extend(
                    [
                        (f"FETCH ALL FROM {name}", None),
                        (f"FETCH NEXT FROM {name}", 1),
                        (f"CLOSE {name}", None),
                    ]
                )
                if not hold:
                    steps.append(("COMMIT", None))
                observations = []
                for sql, result_format in steps:
                    result = (
                        db.pgconn.exec_(sql.encode())
                        if result_format is None
                        else db.pgconn.exec_params(
                            sql.encode(), None, result_format=result_format
                        )
                    )
                    if result.status not in (
                        ExecStatus.COMMAND_OK,
                        ExecStatus.TUPLES_OK,
                    ):
                        raise AssertionError((sql, result.error_message.decode()))
                    observations.append(
                        dict(
                            sql=sql,
                            result_format=result_format,
                            columns=[
                                result.fname(i).decode() for i in range(result.nfields)
                            ],
                            oids=[result.ftype(i) for i in range(result.nfields)],
                            formats=[result.fformat(i) for i in range(result.nfields)],
                            rows=[
                                [
                                    None
                                    if (v := result.get_value(r, c)) is None
                                    else v.hex()
                                    for c in range(result.nfields)
                                ]
                                for r in range(result.ntuples)
                            ],
                            tag=result.command_status.decode(),
                        )
                    )
                profiles.append(
                    dict(binary=binary, scroll=scroll, hold=hold, steps=observations)
                )
    return dict(
        original_id=original["id"], original_sql=original["sql"], profiles=profiles
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    text = json.dumps(reference(), indent=2) + "\n"
    if args.check:
        if TARGET.read_text() != text:
            raise SystemExit("cursor reference differs from PostgreSQL")
        print("PostgreSQL cursor reference verified")
    else:
        TARGET.write_text(text)


if __name__ == "__main__":
    main()
