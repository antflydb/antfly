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

"""Independent typed-temporal boundary oracle (PostgreSQL 18+ compatibility lane).

Run with uv run --no-project --with 'psycopg[binary]==3.3.6'
python scripts/generate_sql_temporal_reference.py [--check].
The native engine never participates in expected-value generation.
"""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_temporal_reference.json"
)
TYPES = {
    "date": ("date", "date_send", 1082),
    "time": ("time", "time_send", 1083),
    "time_tz": ("timetz", "timetz_send", 1266),
    "timestamp": ("timestamp", "timestamp_send", 1114),
    "timestamp_tz": ("timestamptz", "timestamptz_send", 1184),
    "interval": ("interval", "interval_send", 1186),
}
SAMPLES = {
    "date": [
        "2000-01-01",
        "1969-12-31",
        "2000-02-29",
        "1900-03-01",
        "0001-01-01 BC",
        "4714-11-24 BC",
        "5874897-12-31",
        "-infinity",
        "infinity",
    ],
    "time": ["00:00:00", "24:00:00", "12:34:56.123456", "23:59:59.999999"],
    "time_tz": [
        "00:00:00+00",
        "24:00:00+00",
        "12:34:56.123456+05:45",
        "00:00:00-15:59:59",
        "23:59:59.999999+15:59:59",
    ],
    "timestamp": [
        "2000-01-01 00:00:00",
        "1999-12-31 23:59:59.999999",
        "1969-12-31 23:59:59.999999",
        "0001-01-01 00:00:00 BC",
        "4714-11-24 00:00:00 BC",
        "294276-12-31 23:59:59.999999",
        "-infinity",
        "infinity",
    ],
    "timestamp_tz": [
        "2000-01-01 00:00:00+00",
        "1969-12-31 23:59:59.999999+00",
        "2025-01-01 01:30:00+01:30",
        "2000-02-29 12:34:56.123456+05:45",
        "0001-01-01 00:00:00+00 BC",
        "294276-12-31 23:59:59.999999+00",
        "-infinity",
        "infinity",
    ],
    "interval": [
        "0",
        "1 month 2 days 03:04:05.123456",
        "-1 month 2 days -03:04:05.123456",
        "1 year -2 months -3 days 25 hours",
        "2147483647 months",
        "-2147483648 days",
        "-infinity",
        "infinity",
    ],
}

# Sign transitions, large-hour intervals and decimal trimming are independent
# protocol contracts, not expected strings derived from the native formatter.
SAMPLES["date"] += ["1582-10-04", "1582-10-15", "0001-12-31 BC", "0001-01-01 AD"]
SAMPLES["timestamp"] += [
    f"2000-01-01 00:00:00.{digits}" for digits in ("1", "01", "001", "0001", "00001")
]
SAMPLES["time_tz"] += ["01:00:00+05:00", "01:00:00+05:00:01", "01:00:00-00:00:01"]
SAMPLES["interval"] += [
    "-1 year 2 months 3 days 4 hours",
    "1 year -2 months 3 days 4 hours",
    "-1 year -2 months 3 days 4 hours",
    "-1 day 2 hours",
    "1 month -2 days 3 hours",
    "-12 months 3 days",
    "-9223372036854775808 microseconds",
    "9223372036854775807 microseconds",
]


def integer(value):
    result = int(value)
    if value != result:
        raise AssertionError(f"oracle unexpectedly lost integral microseconds: {value}")
    return result


def contract(db, kind, expression, source=None, precision=None):
    _, sender, oid = TYPES[kind]
    local_timestamp = "(v AT TIME ZONE 'UTC')" if kind == "timestamp_tz" else "v"
    # EXTRACT(epoch) rounds at the upper timestamp boundary in PostgreSQL 18.
    # Independent calendar fields retain the binary value's exact microsecond;
    # do not turn an extraction-function rounding artifact into codec bytes.
    timestamp_fields = (
        f"(({local_timestamp})::date - DATE '2000-01-01')::numeric*86400000000"
        f" + extract(hour from {local_timestamp})*3600000000"
        f" + extract(minute from {local_timestamp})*60000000"
        f" + extract(second from {local_timestamp})*1000000"
    )
    # Static owned expressions only; these are not user-supplied SQL.
    fields = (
        "(v - DATE '2000-01-01')::numeric"
        if kind == "date"
        else timestamp_fields
        if kind in ("timestamp", "timestamp_tz")
        else "extract(hour from v)*3600000000 + extract(minute from v)*60000000 + extract(second from v)*1000000"
        if kind in ("time", "time_tz")
        else "(extract(year from v)*12 + extract(month from v))::numeric, extract(day from v)::numeric, extract(hour from v)*3600000000 + extract(minute from v)*60000000 + extract(second from v)*1000000"
    )
    finite = "true" if kind in ("time", "time_tz") else "isfinite(v)"
    first = db.execute(
        f"SELECT encode(pg_catalog.{sender}(v),'hex'), pg_typeof(v)::oid, {finite}, v::text FROM (SELECT {expression} AS v) s"
    ).fetchone()
    if first[1] != oid:
        raise AssertionError(f"oracle type changed: {expression}")
    infinity = 0 if first[2] else -1 if first[3].startswith("-") else 1
    if infinity:
        components = []
    else:
        if kind == "time_tz":
            fields += ", -extract(timezone from v)::numeric"
        components = [
            integer(x)
            for x in db.execute(
                f"SELECT {fields} FROM (SELECT {expression} AS v) s"
            ).fetchone()
        ]
    result = {
        "kind": kind,
        "sql": expression,
        "oid": oid,
        "binary": first[0],
        "text": first[3],
        "components": components,
        "infinity": infinity,
    }
    if source is not None:
        result["source_binary"] = db.execute(
            f"SELECT encode(pg_catalog.{sender}({source}),'hex')"
        ).fetchone()[0]
        result["precision"] = precision
    return result


def reference(db):
    db.execute("SET TIME ZONE 'UTC'")
    db.execute("SET DateStyle = 'ISO, YMD'")
    db.execute("SET IntervalStyle = 'postgres'")
    entries = []
    for kind, samples in SAMPLES.items():
        sql_type = TYPES[kind][0]
        for value in samples:
            entries.append(contract(db, kind, f"{sql_type} '{value}'"))
    # Half ties on either side of the PostgreSQL epoch, all typmods, carry,
    # BC values, boundary overflow and infinities need separate qualification.
    for kind in ("timestamp", "timestamp_tz"):
        for value in (
            "2000-01-01 00:00:00.555555",
            "1999-12-31 23:59:59.444445",
            "1999-12-31 23:59:59.500000",
            "2000-01-01 23:59:59.999999",
        ):
            source = f"{TYPES[kind][0]} '{value}'"
            for precision in range(7):
                entries.append(
                    contract(
                        db,
                        kind,
                        f"({source})::{TYPES[kind][0]}({precision})",
                        source,
                        precision,
                    )
                )
    for kind, values in (
        ("time", ("00:00:00.555555", "23:59:59.999999")),
        ("time_tz", ("00:00:00.555555+05:45", "23:59:59.999999-15:59:59")),
        (
            "interval",
            (
                "0.555555 seconds",
                "-0.555555 seconds",
                "1 month -2 days 0.555555 seconds",
                "-0.000001 seconds",
            ),
        ),
    ):
        for value in values:
            source = f"{TYPES[kind][0]} '{value}'"
            for precision in range(7):
                entries.append(
                    contract(
                        db,
                        kind,
                        f"({source})::{TYPES[kind][0]}({precision})",
                        source,
                        precision,
                    )
                )
    pairs = (
        ("date", "-infinity", "1970-01-01"),
        ("date", "0001-01-01 BC", "0001-01-01 AD"),
        ("date", "infinity", "-infinity"),
        ("time", "00:00:00", "24:00:00"),
        ("time", "00:00:00.000001", "00:00:00.000002"),
        ("time_tz", "01:00:00+01", "00:00:00+00"),
        ("time_tz", "00:00:00+00", "01:00:00+01"),
        ("time_tz", "24:00:00+00", "00:00:00+00"),
        ("time_tz", "23:59:59-15:59:59", "00:00:00+15:59:59"),
        ("timestamp", "1969-12-31 23:59:59.999999", "1970-01-01 00:00:00"),
        ("timestamp", "-infinity", "4714-11-24 00:00:00 BC"),
        ("timestamp_tz", "2000-01-01 00:00:00+01", "1999-12-31 23:00:00+00"),
        ("timestamp_tz", "infinity", "294276-12-31 23:59:59.999999+00"),
        ("interval", "1 month", "30 days"),
        ("interval", "1 year", "360 days"),
        ("interval", "1 day", "24 hours"),
        ("interval", "1 month -30 days", "0"),
        ("interval", "1 month", "31 days"),
        ("interval", "-infinity", "-9223372036854775808 microseconds"),
        ("interval", "infinity", "9223372036854775807 microseconds"),
        (
            "interval",
            "2147483647 months 2147483647 days -9223372036854775808 microseconds",
            "0",
        ),
    )
    comparisons = []
    for kind, left, right in pairs:
        sql_type, sender, _ = TYPES[kind]
        observed = db.execute(
            f"SELECT encode(pg_catalog.{sender}(a),'hex'), encode(pg_catalog.{sender}(b),'hex'), CASE WHEN a<b THEN -1 WHEN a>b THEN 1 ELSE 0 END FROM (SELECT %s::{sql_type} a, %s::{sql_type} b) s",
            (left, right),
        ).fetchone()
        comparisons.append(
            {
                "kind": kind,
                "left": observed[0],
                "right": observed[1],
                "order": observed[2],
            }
        )
    return {
        "reference": "PostgreSQL exact binary and extracted temporal fields",
        "oracle_major": db.info.server_version // 10000,
        "server_version": db.info.server_version,
        "server_build": db.execute("SELECT version()").fetchone()[0],
        "encoding": db.execute("SHOW server_encoding").fetchone()[0],
        "timezone": "UTC",
        "date_style": "ISO, YMD",
        "interval_style": "postgres",
        "entries": entries,
        "comparisons": comparisons,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true")
    mode.add_argument(
        "--write",
        action="store_true",
        help="Regenerate the independent checked-in fixture",
    )
    args = parser.parse_args()
    with postgres() as db:
        result = reference(db)
    if args.check:
        expected = json.loads(FIXTURE.read_text())
        if result != expected:
            raise AssertionError("independent PostgreSQL temporal reference drift")
        print(
            f"Verified {len(result['entries'])} PostgreSQL temporal binary/field/precision contracts"
        )
    elif args.write:
        FIXTURE.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
