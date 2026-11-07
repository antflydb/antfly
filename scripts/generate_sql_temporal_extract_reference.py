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

"""Independent exact PostgreSQL timestamp EXTRACT/float8 date_part vectors."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

UNITS = (
    "epoch",
    "microseconds",
    "milliseconds",
    "second",
    "minute",
    "hour",
    "day",
    "month",
    "quarter",
    "year",
    "decade",
    "century",
    "millennium",
    "dow",
    "isodow",
    "doy",
    "week",
    "isoyear",
    "timezone",
    "timezone_hour",
    "timezone_minute",
)
DATES = (
    "1969-12-31 23:59:59.999999+00",
    "2000-01-01 00:00:00+00",
    "2016-01-01 23:14:15.123456+00",
    "2026-12-31 23:59:59.000001+00",
    "0001-01-01 00:00:00+00 BC",
    "0101-02-28 13:04:05.000006+00 BC",
)


def reference():
    from psycopg import sql

    entries = []
    with postgres() as db:
        for date in DATES:
            binary = bytes(
                db.execute(
                    "SELECT timestamptz_send(%s::timestamptz)", (date,)
                ).fetchone()[0]
            ).hex()
            for unit in UNITS:
                query = sql.SQL(
                    "SELECT EXTRACT({} FROM %s::timestamptz),date_part(%s,%s::timestamptz)"
                ).format(sql.SQL(unit))
                exact, floating = db.execute(query, (date, unit, date)).fetchone()
                sign, digits, exponent = exact.as_tuple()
                coefficient = int("".join(map(str, digits))) * (-1 if sign else 1)
                if exponent > 0:
                    coefficient *= 10**exponent
                    exponent = 0
                entries.append(
                    {
                        "timestamp": binary,
                        "unit": unit,
                        "coefficient": str(coefficient),
                        "scale": -exponent,
                        "floating": floating,
                    }
                )
    return {"reference": "PostgreSQL 18 exact UTC extraction", "entries": entries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("temporal extraction PostgreSQL drift")
        print(f"verified {len(result['entries'])} exact timestamp extraction vectors")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
