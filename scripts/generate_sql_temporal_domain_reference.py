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

"""Exact source-domain temporal originals and independent typed witnesses.

Read witnesses retain the baseline TEXT created_at identity. Supplemental
mutation witnesses deliberately use a BIGINT updated_at_ns target.
The original source SQL and parameters are never rewritten into timestamps.
The three upsert witnesses use explicit supplemental domains and earn no credit
for the source upserts, whose original UUID/temporal declarations differ.
"""

import argparse
import json
from pathlib import Path

import psycopg
from generate_sql_document_alias_reference import output
from generate_sql_postgres_reference import FIXTURES, postgres

READ_IDS = ("sql-0201", "sql-0592")
SUPPLEMENTAL_SOURCE_IDS = ("sql-1403", "sql-1468", "sql-1469")
IDS = READ_IDS + SUPPLEMENTAL_SOURCE_IDS
CREATE = "CREATE TABLE usage_records(id TEXT PRIMARY KEY,email TEXT UNIQUE,created_at TEXT,updated_at_ns BIGINT)"
SEED = "INSERT INTO usage_records(id,email,created_at,updated_at_ns) VALUES('u1','a@example.test','2025-01-01T00:00:00Z',10),('u9','z@example.test','2025-01-02T00:00:00Z',20)"
READ = "SELECT id,email,created_at,updated_at_ns FROM usage_records ORDER BY id"


def reference():
    originals = {
        c["id"]: c
        for c in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    expected = ("42883", "42883", "42702", "42883", "42804")
    entries = []
    assignment_vectors = []
    with postgres() as db:
        sources = [
            "DATE '2024-02-29'",
            "TIME '12:34:56.123456'",
            "TIMETZ '12:34:56+01:30'",
            "TIMESTAMP '2024-02-29 12:34:56'",
            "TIMESTAMPTZ '2024-02-29 12:34:56+01:30'",
            "INTERVAL '1 month 2 days 03:04:05'",
            "NULL::timestamp",
            "'42'::text",
        ]
        targets = [
            ("BIGINT", "integer", "int64"),
            ("NUMERIC", "number", "numeric"),
            ("BOOLEAN", "boolean", "boolean"),
            ("UUID", "uuid", "uuid"),
            ("JSONB", "json", "jsonb"),
            ("NUMRANGE", "numrange", None),
            ("TEXT", "string", "text"),
        ]
        for expression in sources:
            for sql_type, kind, element in targets:
                with db.transaction(force_rollback=True):
                    db.execute(f"CREATE TEMP TABLE assignment_target(value {sql_type})")
                    result = None
                    sqlstate = None
                    try:
                        with db.transaction():
                            result = db.execute(
                                f"INSERT INTO assignment_target(value) VALUES({expression}) RETURNING value::text"
                            ).fetchone()[0]
                    except psycopg.Error as error:
                        sqlstate = error.sqlstate
                    assert sqlstate == (None if kind == "string" else "42804"), (
                        expression,
                        sql_type,
                        sqlstate,
                    )
                    assignment_vectors.append(
                        dict(
                            expression=expression,
                            target=dict(kind=kind, element_type=element),
                            result=result,
                            sqlstate=sqlstate,
                        )
                    )
        for identity, code in zip(IDS, expected, strict=True):
            with db.transaction(force_rollback=True):
                db.execute(CREATE)
                db.execute(SEED)
                before = output(db.execute(READ))
                versions = db.execute(
                    "SELECT id,xmin::text FROM usage_records ORDER BY id"
                ).fetchall()
                case = originals[identity]
                parameters = [next(iter(p.values())) for p in case["params"]]
                if parameters:
                    with psycopg.RawCursor(db) as cursor:
                        cursor.execute("SELECT pg_typeof($1)::oid", parameters)
                        assert cursor.fetchone()[0] == 20  # Source BIGINT input.
                try:
                    with db.transaction(), psycopg.RawCursor(db) as cursor:
                        cursor.execute(case["sql"], parameters)
                except psycopg.Error as error:
                    assert error.sqlstate == code, (identity, error.sqlstate)
                else:
                    raise AssertionError(
                        f"{identity}: invalid source-domain expression admitted"
                    )
                final = output(db.execute(READ))
                assert final == before
                assert (
                    versions
                    == db.execute(
                        "SELECT id,xmin::text FROM usage_records ORDER BY id"
                    ).fetchall()
                )
                cold = []
                # These are separate well-typed statements, not substitutes for
                # the original source contracts above. Exercise the target row,
                # excluded row, calendar arithmetic, casts and unique owner.
                probes = [
                    "SELECT id,created_at::timestamptz AS instant FROM usage_records WHERE created_at::timestamptz >= TIMESTAMPTZ '2025-01-01' ORDER BY instant DESC,id",
                    "SELECT id,created_at::timestamptz + INTERVAL '1 month 1 day 01:30:00' AS shifted FROM usage_records ORDER BY id",
                    "INSERT INTO usage_records(id,email,updated_at_ns) VALUES('u2','a@example.test',3700000000000) ON CONFLICT(email) DO UPDATE SET updated_at_ns=usage_records.updated_at_ns+1000000000 RETURNING id,updated_at_ns",
                    "INSERT INTO usage_records(id,email,updated_at_ns) VALUES('u2','a@example.test',3700000000000) ON CONFLICT(email) DO UPDATE SET updated_at_ns=excluded.updated_at_ns RETURNING id,updated_at_ns",
                ]
                source = "SELECT DATE '2024-02-29' AS day,TIME '12:34:56.123456' AS clock,TIMETZ '12:34:56+01:30' AS zoned,TIMESTAMP '2024-02-29 12:34:56' AS local,INTERVAL '1 month 2 days 03:04:05' AS span"
                probes += [
                    f"UPDATE usage_records u SET updated_at_ns=u.updated_at_ns+1 FROM({source}) s WHERE u.id='u1' RETURNING u.id,s.day,s.clock,s.zoned,s.local,s.span",
                    f"MERGE INTO usage_records u USING({source}) s ON u.id='u1' WHEN MATCHED THEN UPDATE SET updated_at_ns=u.updated_at_ns+1 RETURNING u.id,s.day,s.clock,s.zoned,s.local,s.span",
                ]
                for sql in probes:
                    cold.append(
                        dict(
                            sql=sql,
                            output=output(db.execute(sql)),
                            final=output(db.execute(READ)),
                        )
                    )
                entries.append(
                    dict(
                        id=identity
                        if identity in READ_IDS
                        else "bigint-domain-" + identity,
                        source_id=identity,
                        sql=case["sql"],
                        parameters=parameters,
                        create=CREATE,
                        seed=SEED,
                        read=READ,
                        before=before,
                        output=None,
                        final=final,
                        sqlstate=code,
                        affected=0,
                        cold=cold if identity == IDS[0] else [],
                        parameter_descriptors=[
                            dict(kind="integer", element_type="int64")
                        ]
                        if identity == "sql-0201"
                        else [],
                    )
                )
    return dict(format=1, entries=entries, assignment_vectors=assignment_vectors)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    actual = reference()
    if args.check:
        if json.loads(args.check.read_text()) != actual:
            raise SystemExit("temporal source-domain reference changed")
        print(
            "verified two temporal originals, three supplemental domains and typed cold postimages"
        )
    else:
        print(json.dumps(actual, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
