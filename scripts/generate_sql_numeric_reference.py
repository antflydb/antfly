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

"""Generate/verify the exact NUMERIC kernel oracle; no float intermediates."""

import argparse
import json
import random
from pathlib import Path

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_exact_numeric_reference.json"
)


def cases():
    result = [
        {"op": "parse", "left": text}
        for text in (
            "0",
            "-0.000",
            "+0.00",
            "00012.3400",
            ".00100",
            "1.",
            "1e3",
            "1.2300e2",
            "1.2300e-2",
            "123e-6",
            "-123e3",
            "  +12.5 \n",
            "9007199254740993.0000000000000001",
            "NaN",
            "nan",
            "+NaN",
            "-NaN",
            "Infinity",
            "+inf",
            "-Inf",
            "  -Infinity  ",
            " 1 e2",
            "",
            ".",
            "1e",
            "1.2.3",
            "1_000",
            "1_2.3_4e1_0",
            "1__2",
            "_1",
            "1_",
            "1_.2",
            "1._2",
            "1e_2",
            "1e2_",
            "0x12",
            "-0XFFFF_FFFF_FFFF_FFFF_FFFF",
            "0x_FF",
            "0b1010_1100",
            "0o777_123",
            "0x",
            "0x__1",
            "0b2",
            "0o8",
            "0xfg",
            "0b_1",
            "0e131072",
            "0e-16384",
            "1e-16384",
            "1e131072",
            "0e2147483647",
            "1e1073741824",
        )
    ]
    pairs = [
        ("1.20", "2.003"),
        ("9007199254740993", "0.1"),
        ("1e30", "-0.000000001"),
        ("9999.9999", ".0001"),
        ("-2.5", "2.5"),
        ("-1e30", "1e30"),
        ("NaN", "1"),
        ("Infinity", "-Infinity"),
        ("0", "Infinity"),
        ("1e-100", "1e-100"),
        ("-Infinity", "-2"),
    ]
    rng = random.Random(20261008)
    for _ in range(24):
        pairs.append(
            tuple(
                f"{rng.randint(-(10**50), 10**50)}e-{rng.randrange(31)}"
                for _ in range(2)
            )
        )
    for left, right in pairs:
        for op in ("add", "subtract", "multiply", "order"):
            result.append({"op": op, "left": left, "right": right})
    for left, scale in (
        ("2.5", 0),
        ("-2.5", 0),
        ("2.50000000000001", 0),
        ("9999.9999", 3),
        (".0005", 3),
        ("15.5", -1),
        ("25.5", -1),
        ("999.99", -3),
        (".00004", 4),
        ("1.2300", 6),
        ("1e-100", 99),
        ("NaN", 0),
        ("Infinity", -2),
        ("-0.00001", 0),
    ):
        for op in ("round", "truncate"):
            result.append({"op": op, "left": left, "scale": scale})
    for left, precision, scale in (
        ("12.345", 4, 2),
        ("99.995", 4, 2),
        ("-99.995", 4, 2),
        (".00994", 2, 4),
        (".00995", 2, 4),
        ("-.00995", 2, 4),
        ("0", 2, 4),
        ("99499", 2, -3),
        ("99500", 2, -3),
        ("-99500", 2, -3),
        ("NaN", 1, 0),
        ("Infinity", 1000, 0),
        ("-Infinity", 1000, 0),
        ("1e-1000", 1, 1000),
        ("1e-999", 1, 1000),
        ("9e999", 1000, 0),
        ("0", 0, 0),
        ("NaN", 1001, 0),
        ("0", 1, -1001),
        ("0", 1, 1001),
    ):
        result.append(
            {"op": "typmod", "left": left, "precision": precision, "scale": scale}
        )
    for left in (
        "0",
        "-.49",
        ".5",
        "-.5",
        "2.5",
        "-2.5",
        "32767.49",
        "32767.5",
        "-32768.49",
        "-32768.5",
        "2147483647.49",
        "2147483647.5",
        "-2147483648.49",
        "-2147483648.5",
        "9223372036854775807.49",
        "9223372036854775807.5",
        "-9223372036854775808.49",
        "-9223372036854775808.5",
        "9007199254740993.1",
        "NaN",
        "Infinity",
        "-Infinity",
        "1e131071",
        "1e-16383",
    ):
        for op in ("int16", "int32", "int64"):
            result.append({"op": op, "left": left})
    division_pairs = pairs + [
        ("1", "3"),
        ("-10.00", "3.0"),
        ("10.00", "-3.0"),
        ("1", "200000000000000000000"),
        ("999999999999999999999", "100000000000000000000"),
        ("1e-1001", "1"),
        ("1e-999", "1"),
        ("1e100", "1e-100"),
        ("0.000000000000000000001", "1.000000000000000000002"),
        ("10000000000000001", "10000000000000000"),
        ("9999999999999999999999999999", "99999999999999999999"),
        ("5000000000000000000000000001", "50000000000000000001"),
        ("1000000000000000000000000001", "999999999999999999999999999"),
        ("1.00000000000000000000", "0.00000000000000000003"),
    ]
    division_rng = random.Random(20261009)
    for _ in range(64):
        division_pairs.append(
            tuple(
                f"{division_rng.randint(-(10**200), 10**200)}e-{division_rng.randrange(401)}"
                for _ in range(2)
            )
        )
    specials = ("0", "-0.00", "1", "-1", "NaN", "Infinity", "-Infinity")
    division_pairs.extend((left, right) for left in specials for right in specials)
    for left, right in division_pairs:
        for op in ("divide", "divide_trunc", "remainder"):
            result.append({"op": op, "left": left, "right": right})
    result.append({"op": "remainder", "left": "1e131071", "right": "12345"})
    roots = [
        "0",
        "0.00",
        "1",
        "2",
        "4",
        "9",
        "10000",
        "1e-20",
        "1.2345678901234567890123456789",
        "9007199254740993",
        "99999999999999999999999999999999999999",
        "NaN",
        "Infinity",
        "-Infinity",
        "-1",
        "-0.0000",
        "0.00000000000000000000000000000000000001",
        "1e-16383",
        "9999",
        "10001",
        "99999999",
        "100000001",
        "2.25",
        "0.0025",
        "123456789.123456789",
        "1.00000000000000100000000000000025",
        "1.00000000000000099999999999999999",
        "1.00000000000000100000000000000026",
        "1e1000",
        "1e-1000",
    ]
    root_rng = random.Random(20261011)
    roots.extend(
        f"{root_rng.randrange(1, 10**200)}e-{root_rng.randrange(801)}"
        for _ in range(64)
    )
    result.extend({"op": "sqrt", "left": text} for text in roots)
    return result


def evaluate(db, case):
    op = case["op"]
    if op == "parse":
        sql, args = "SELECT (%s::numeric)::text", (case["left"],)
    elif op == "sqrt":
        sql, args = "SELECT sqrt(%s::numeric)::text", (case["left"],)
    elif op in {"add", "subtract", "multiply"}:
        symbol = {"add": "+", "subtract": "-", "multiply": "*"}[op]
        sql = f"SELECT (%s::numeric {symbol} %s::numeric)::text"
        args = (case["left"], case["right"])
    elif op in {"divide", "remainder"}:
        symbol = "/" if op == "divide" else "%%"
        sql, args = (
            f"SELECT (%s::numeric {symbol} %s::numeric)::text",
            (case["left"], case["right"]),
        )
    elif op == "divide_trunc":
        sql, args = (
            "SELECT div(%s::numeric,%s::numeric)::text",
            (case["left"], case["right"]),
        )
    elif op == "typmod":
        # Modifiers are generated integers, never interpolated user SQL.
        sql = (
            f"SELECT (%s::numeric::numeric({case['precision']},{case['scale']}))::text"
        )
        args = (case["left"],)
    elif op in {"int16", "int32", "int64"}:
        target = {"int16": "int2", "int32": "int4", "int64": "int8"}[op]
        sql, args = f"SELECT (%s::numeric::{target})::text", (case["left"],)
    elif op == "order":
        sql = "SELECT CASE WHEN %s::numeric < %s::numeric THEN -1 WHEN %s::numeric > %s::numeric THEN 1 ELSE 0 END"
        args = (case["left"], case["right"]) * 2
    elif op in {"round", "truncate"}:
        function = "round" if op == "round" else "trunc"
        sql, args = (
            f"SELECT {function}(%s::numeric,%s::int4)::text",
            (case["left"], case["scale"]),
        )
    else:
        raise ValueError("unknown NUMERIC oracle operation")
    with db.transaction(force_rollback=True):
        db.execute("SET TRANSACTION READ ONLY")
        return db.execute(sql, args).fetchone()[0]


def verify_boundaries(db):
    import psycopg

    # Keep giant outputs out of the fixture, but verify their exact domain and
    # display scale with PostgreSQL rather than inferring them from small cases.
    contracts = (
        ("%s::numeric", ("1e131071",), (131072, 0, False)),
        ("%s::numeric", ("1e-16383",), (16385, 16383, False)),
        ("%s::numeric * %s::numeric", ("1e-16383", "1e-16383"), (16385, 16383, True)),
        ("round(%s::numeric,%s::int4)", ("1.0", 2147483647), (16385, 16383, False)),
        ("round(%s::numeric,%s::int4)", ("1e131071", -2147483648), (1, 0, True)),
        ("%s::numeric", ("0x" + "f" * 4096,), (4933, 0, False)),
        ("%s::numeric / %s::numeric", ("1e-16383", "1e-16383"), (1002, 1000, False)),
        ("%s::numeric / %s::numeric", ("1e-16383", "1"), (1002, 1000, True)),
        ("%s::numeric %% %s::numeric", ("1e131071", "1e-16383"), (16385, 16383, True)),
        ("%s::numeric / %s::numeric", ("1e131071", "1"), (131072, 0, False)),
    )
    for expression, args, expected in contracts:
        with db.transaction(force_rollback=True):
            actual = db.execute(
                f"SELECT length(n::text),scale(n),n=0 FROM (SELECT {expression} AS n) q",
                args,
            ).fetchone()
            if actual != expected:
                raise ValueError(f"PostgreSQL NUMERIC boundary drift: {actual!r}")
    for expression in ("%s::numeric / %s::numeric", "div(%s::numeric,%s::numeric)"):
        try:
            with db.transaction(force_rollback=True):
                db.execute(f"SELECT {expression}", ("1e131071", "1e-16383"))
        except psycopg.Error as error:
            if error.sqlstate != "22003":
                raise ValueError(
                    f"PostgreSQL NUMERIC overflow drift: {error.sqlstate}"
                ) from error
        else:
            raise ValueError(
                "PostgreSQL NUMERIC quotient overflow unexpectedly accepted"
            )


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()
    output = {"reference": "PostgreSQL exact NUMERIC kernel", "entries": []}
    with postgres() as db:
        verify_boundaries(db)
        for case in cases():
            entry = dict(case)
            try:
                entry["expected"] = evaluate(db, case)
            except psycopg.Error as error:
                entry["error"] = error.sqlstate
            output["entries"].append(entry)
    if args.generate:
        print('{\n  "reference": "PostgreSQL exact NUMERIC kernel",\n  "entries": [')
        print(
            ",\n".join(
                "    " + json.dumps(entry, separators=(",", ":"))
                for entry in output["entries"]
            )
        )
        print("  ]\n}")
    else:
        if output != json.loads(FIXTURE.read_text()):
            raise ValueError("PostgreSQL NUMERIC kernel oracle drift")
        print(f"Verified {len(output['entries'])} PostgreSQL exact NUMERIC contracts")


if __name__ == "__main__":
    main()
