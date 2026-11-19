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

"""PostgreSQL typed bin overloads, signed bounds, and transaction-date witnesses."""

import argparse
import json
from pathlib import Path
from generate_sql_postgres_reference import FIXTURES, postgres
from generate_sql_document_alias_reference import output


def reference():
    import psycopg

    strides = [
        "1 microsecond",
        "15 minutes",
        "1 day -23 hours",
        "-1 day 25 hours",
        "0",
        "-1 microsecond",
        "1 month",
        "infinity",
        "2147483647 days",
        "9223372036854775807 microseconds",
    ]
    sources = [
        "2000-01-01 00:00:00",
        "1999-12-31 23:59:59.999999",
        "2025-01-01 01:30:00.123456+01:30",
        "0001-01-01 BC",
        "294276-12-31 23:59:59.999999",
        "infinity",
        "-infinity",
    ]
    origins = ["2000-01-01", "2000-01-01 00:02:30", "4713-01-01 BC", "infinity"]
    expressions = [
        f"date_bin(INTERVAL '{stride}',{kind} '{source}',{kind} '{origin}')"
        for kind in ["TIMESTAMP", "TIMESTAMPTZ"]
        for stride in strides
        for source in sources
        for origin in origins
    ]
    expressions += [
        "date_bin('15 minutes','2000-01-01 00:38:12','2000-01-01')",
        "date_bin('1 day',DATE '2025-01-02',DATE '2025-01-01')",
        "date_bin('1 hour',TIMESTAMP '2000-01-01 01:30',TIMESTAMPTZ '2000-01-01')",
        "date_bin(NULL,TIMESTAMP '2000-01-01',TIMESTAMP '2000-01-01')",
        "date_bin(INTERVAL '1 month',NULL::timestamp,TIMESTAMP '2000-01-01')",
        "date_bin(1,2,3)",
        "date_bin(INTERVAL '1 hour',1,0)",
        "date_bin('1 hour','2000-01-01'::text,'2000-01-01')",
        "date_bin(INTERVAL '1 hour',TIME '01:00',TIMESTAMP '2000-01-01')",
        "date_bin(INTERVAL '1 hour',INTERVAL '1 day',TIMESTAMP '2000-01-01')",
        "date_bin()",
        "date_bin(INTERVAL '1 hour',TIMESTAMP '2000-01-01')",
    ]
    for source in [
        "DATE '1969-12-31'",
        "DATE '2000-01-01'",
        "DATE 'infinity'",
        "DATE '-infinity'",
        "DATE '5874897-12-31'",
        "TIMESTAMP '1999-12-31 23:59:59.999999'",
        "TIMESTAMPTZ '1999-12-31 23:59:59.999999+00'",
        "TIMESTAMP '-infinity'",
        "TIMESTAMPTZ 'infinity'",
        "NULL::date",
        "NULL::timestamp",
    ]:
        for target in ["date", "timestamp", "timestamptz"]:
            expressions.append(f"({source})::{target}")
    expressions += [
        "TIME '01:00'",
        "TIMETZ '01:00+01:30'",
        "TIMESTAMP '2024-01-01 01:30+01:30'",
        "TIMESTAMPTZ '2024-01-01 01:30+01:30'",
    ]
    fractions = [
        "0000005",
        "0000015",
        "0000025",
        "0000035",
        "1234565",
        "1234555",
        "9999995",
        "123456789",
    ]
    for fraction in fractions:
        expressions.append(f"TIME '23:59:59.{fraction}'")
        for day in ["2024-01-01", "1969-12-31"]:
            for kind in ["TIMESTAMP", "TIMESTAMPTZ"]:
                expressions.append(f"{kind} '{day} 23:59:59.{fraction}'")
    expressions.append("TIMESTAMP '294276-12-31 23:59:59.9999995'")
    vectors = []
    with postgres() as db:
        for expression in expressions:
            try:
                text, oid = db.execute(
                    "SELECT ("
                    + expression
                    + ")::text,pg_typeof("
                    + expression
                    + ")::oid"
                ).fetchone()
                vectors.append(
                    dict(expression=expression, oid=oid, result=text, sqlstate=None)
                )
            except psycopg.Error as error:
                if error.sqlstate is None:
                    raise
                vectors.append(
                    dict(
                        expression=expression,
                        oid=None,
                        result=None,
                        sqlstate=error.sqlstate,
                    )
                )
        dates = []
        for stamp in [
            "2000-01-01",
            "1999-12-31 23:59:59.999999+00",
            "2024-02-29 23:59:59.999999+00",
            "0001-01-01 BC",
        ]:
            result = db.execute(
                f"SELECT ('{stamp}'::timestamptz)::date::text"
            ).fetchone()[0]
            dates.append(dict(timestamp=stamp, result=result))
        # CURRENT_DATE is a real DATE tied to the same transaction instant as
        # transaction_timestamp(), not a sampled wall date or an epoch integer.
        with db.transaction(force_rollback=True):
            db.execute("CREATE TEMP TABLE usage_records(id text)")
            db.execute("INSERT INTO usage_records(id) VALUES ('u1')")
            original = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
                "entries"
            ]
            current = next(c for c in original if c["id"] == "sql-0179")
            cursor = db.cursor(row_factory=None)
            cursor.execute(current["sql"].replace("$1", "%s"), ("u1",))
            value = cursor.fetchone()[0]
            assert cursor.description[0].type_code == 1082
            assert (
                str(value)
                == db.execute("SELECT transaction_timestamp()::date::text").fetchone()[
                    0
                ]
            )
        entries = []
        create = "CREATE TABLE usage_records(id TEXT PRIMARY KEY,amount NUMERIC,created_at TIMESTAMPTZ,updated_at_ns TIMESTAMPTZ)"
        seed = "INSERT INTO usage_records(id,amount,created_at,updated_at_ns) VALUES ('u1',5,TIMESTAMPTZ '2024-02-01 12:00:00+00',TIMESTAMPTZ '2024-01-31 23:59:59.123456+00'),('u2',8,TIMESTAMPTZ '2024-02-02 12:00:00+00',NULL)"
        read = (
            "SELECT id,amount,created_at,updated_at_ns FROM usage_records ORDER BY id"
        )
        for identity in [
            "sql-0179",
            "sql-0264",
            "sql-0265",
            "sql-0266",
            "sql-1467",
            "sql-1477",
        ]:
            case = next(c for c in original if c["id"] == identity)
            params = [next(iter(p.values())) for p in case["params"]]
            with db.transaction(force_rollback=True):
                db.execute(create)
                db.execute(seed)
                date = db.execute("SELECT CURRENT_DATE::text").fetchone()[0]
                midnight = db.execute(
                    "SELECT CURRENT_DATE::timestamptz::text"
                ).fetchone()[0]
                before = output(db.execute(read))
                sqlstate = None
                result = None
                affected = 0
                try:
                    with db.transaction():
                        cursor = psycopg.RawCursor(db)
                        cursor.execute(case["sql"], params)
                        affected = (
                            0
                            if cursor.statusmessage.startswith("SELECT")
                            else cursor.rowcount
                        )
                        result = output(cursor)
                except psycopg.Error as error:
                    if error.sqlstate is None:
                        raise
                    sqlstate = error.sqlstate
                final = output(db.execute(read))
                if identity in ("sql-0179", "sql-1467"):
                    for result_set in [result, final]:
                        for row in result_set["rows"]:
                            for column, oid in enumerate(result_set["column_oids"]):
                                if oid == 1082 and row[column] == date:
                                    row[column] = "$transaction_date"
                                elif oid == 1184 and row[column] == midnight:
                                    row[column] = "$transaction_midnight"
                cold = []
                if sqlstate is None:
                    for probe in [
                        "SELECT TIMESTAMP '2024-01-01 23:59:59.9999995' AS local_rounded,TIMESTAMPTZ '2024-01-01 23:59:59.9999995+00' AS utc_rounded,TIME '23:59:59.9999995' AS day_end,date_bin(INTERVAL '15 minutes',TIMESTAMPTZ '1969-12-31 23:59:59.999999999+00',TIMESTAMPTZ '2000-01-01') AS bucket",
                        "UPDATE usage_records SET updated_at_ns=date_bin(INTERVAL '15 minutes',updated_at_ns,TIMESTAMPTZ '2000-01-01') WHERE id='u1' RETURNING id,updated_at_ns",
                        "INSERT INTO usage_records(id,amount) VALUES('u1',10) ON CONFLICT(id) DO UPDATE SET updated_at_ns=TIMESTAMPTZ '2024-01-31 23:59:59.123456+00' + INTERVAL '1 month 1 day' RETURNING id,updated_at_ns",
                        "SELECT id,date_bin(INTERVAL '7 minutes',created_at,TIMESTAMPTZ '2000-01-01') AS bucket FROM usage_records WHERE date_bin(INTERVAL '1 hour',created_at,TIMESTAMPTZ '2000-01-01') >= TIMESTAMPTZ '2024-02-01' ORDER BY bucket DESC,id",
                    ]:
                        probe_output = output(db.execute(probe))
                        for row in probe_output["rows"]:
                            for column, oid in enumerate(probe_output["column_oids"]):
                                if oid == 1184 and row[column] == midnight:
                                    row[column] = "$transaction_midnight"
                        cold.append(
                            dict(
                                sql=probe,
                                output=probe_output,
                                final=output(db.execute(read)),
                            )
                        )
                        if identity == "sql-1467":
                            for row in cold[-1]["final"]["rows"]:
                                for column, oid in enumerate(
                                    cold[-1]["final"]["column_oids"]
                                ):
                                    if oid == 1184 and row[column] == midnight:
                                        row[column] = "$transaction_midnight"
                if identity == "sql-1477":
                    # Keep the original rejection above. This separate witness
                    # proves that qualifying its target source executes the
                    # calendar expression through an actual conflict update.
                    qualified = case["sql"].replace(
                        "updated_at_ns = updated_at_ns +",
                        "updated_at_ns = usage_records.updated_at_ns +",
                    )
                    assert qualified != case["sql"]
                    cold.append(
                        dict(
                            sql=qualified,
                            output=output(db.execute(qualified)),
                            final=output(db.execute(read)),
                        )
                    )
                entries.append(
                    dict(
                        id=identity,
                        sql=case["sql"],
                        parameters=params,
                        create=create,
                        seed=seed,
                        read=read,
                        before=before,
                        output=result,
                        final=final,
                        sqlstate=sqlstate,
                        affected=affected,
                        cold=cold,
                    )
                )
    return dict(format=1, vectors=vectors, transaction_dates=dates, entries=entries)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    actual = reference()
    if args.check:
        if json.loads(args.check.read_text()) != actual:
            raise SystemExit("temporal bin reference is stale")
        print(
            f"verified {len(actual['vectors'])} PostgreSQL bin contracts and transaction dates"
        )
    else:
        print(json.dumps(actual, indent=2))


if __name__ == "__main__":
    main()
