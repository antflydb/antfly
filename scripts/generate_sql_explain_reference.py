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

"""PostgreSQL EXPLAIN execution counts, option dependencies and custom plans.

Native physical plans and storage accounting have their own declared units.
This oracle qualifies SQL planning/execution and option semantics rather than
freezing PostgreSQL's optimizer-specific node names, costs or storage counters.
"""

import argparse
import json
import re
from pathlib import Path
from generate_sql_postgres_reference import FIXTURES, postgres

OPTIONS = [
    "",
    "ANALYZE",
    "ANALYZE,COSTS OFF",
    "ANALYZE,TIMING OFF",
    "ANALYZE,SUMMARY OFF",
    "ANALYZE,TIMING OFF,SUMMARY OFF",
    "ANALYZE,BUFFERS OFF",
    "ANALYZE,WAL,SETTINGS",
    "ANALYZE,BUFFERS,TIMING OFF,SUMMARY OFF,SETTINGS ON,WAL,COSTS ON",
    "BUFFERS,SUMMARY",
    "COSTS OFF,VERBOSE OFF",
    "VERBOSE,SETTINGS",
    "ANALYZE OFF,TIMING OFF,WAL OFF,SUMMARY OFF",
    "ANALYZE ON,TIMING ON,TIMING OFF,SUMMARY ON",
]
QUERIES = [
    "SELECT (SELECT COALESCE(o.x,7)+COUNT(*) FROM (SELECT 1 AS y WHERE FALSE) i) FROM (SELECT 9::bigint AS x) o",
    "SELECT (SELECT (SELECT o.x+q.z+COUNT(*) FROM (SELECT 1 AS y WHERE FALSE) i) FROM (SELECT 3 AS z) q) FROM (SELECT 1 AS x UNION ALL SELECT 2) o ORDER BY o.x",
    "SELECT n FROM (VALUES(1),(2),(3),(4),(5)) t(n)",
    "SELECT n FROM (VALUES(1),(2),(3),(4),(5)) t(n) WHERE n>2",
    "SELECT n FROM (VALUES(1),(2),(3),(4),(5)) t(n) ORDER BY n DESC",
    "SELECT n FROM (VALUES(1),(2),(3),(4),(5)) t(n) ORDER BY n DESC LIMIT 2 OFFSET 1",
    "SELECT n FROM (VALUES(1),(2),(3)) t(n) LIMIT 9223372036854775807",
    "SELECT n FROM (VALUES(1),(2),(3)) t(n) LIMIT 0",
    "SELECT count(*) FROM (VALUES(1),(2),(3)) t(n)",
    "SELECT n%2,count(*),sum(n) FROM (VALUES(1),(2),(3),(4),(5)) t(n) GROUP BY n%2 HAVING count(*)>1",
    "SELECT string_agg(n::text,',' ORDER BY n),array_agg(n ORDER BY n) FROM (VALUES(1),(2),(3)) t(n)",
    "SELECT a.n,b.n FROM (VALUES(1),(2),(3)) a(n) LEFT JOIN (VALUES(2),(3)) b(n) ON a.n=b.n",
    "SELECT DISTINCT n FROM (VALUES(1),(2),(1),(NULL)) t(n)",
    "SELECT n FROM (VALUES(1),(2)) a(n) UNION SELECT n FROM (VALUES(2),(3)) b(n)",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n<5) SELECT n FROM r",
    "SELECT n,row_number() OVER(ORDER BY n),sum(n) OVER(ORDER BY n ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM (VALUES(1),(2),(3)) t(n)",
    "SELECT q.n,x.m FROM (VALUES(1),(2),(3)) q(n) CROSS JOIN LATERAL(SELECT q.n+1 AS m) x",
    "SELECT 1/(n-n) FROM (VALUES(1),(2)) t(n) WHERE false",
    "SELECT CASE WHEN false THEN 1/0 ELSE n END FROM (VALUES(1),(2)) t(n)",
    "SELECT sum(1/(n-n)) FILTER(WHERE false) FROM (VALUES(1),(2)) t(n)",
    "SELECT ARRAY[1,NULL,3],1.234::numeric(10,3),DATE '2025-01-01'",
    "SELECT count(*) FROM (VALUES(1),(2)) t(n) WHERE false",
    "SELECT keep FROM (SELECT count(*) AS keep,sum(1/0) AS dead FROM (VALUES(1),(2)) q(n)) d",
    "SELECT n FROM (SELECT n,array_agg(1/0) AS dead FROM (VALUES(1),(2)) q(n) GROUP BY n) d",
    "SELECT count(*) FROM (SELECT n,percentile_disc(0.5) WITHIN GROUP(ORDER BY 1/0) AS dead FROM (VALUES(1),(2)) q(n) GROUP BY n) d",
]


def expected(body):
    root = body[0]["Plan"]
    return dict(
        rows=int(root["Actual Rows"]) if "Actual Rows" in root else None,
        options=dict(
            analyze="Actual Rows" in root,
            timing="Actual Total Time" in root,
            summary="Planning Time" in body[0] or "Execution Time" in body[0],
            buffers="Shared Read Blocks" in root or "Planning" in body[0],
            settings="Settings" in body[0],
            wal="WAL Records" in root,
            costs="Startup Cost" in root,
        ),
    )


def reference():
    import psycopg

    entries = []
    with postgres() as db:

        def capture(sql, parameters=()):
            try:
                with psycopg.RawCursor(db) as cursor:
                    cursor.execute(sql, parameters)
                    body = cursor.fetchone()[0]
                return dict(
                    sql=sql,
                    parameters=list(parameters),
                    sqlstate=None,
                    **expected(body),
                )
            except psycopg.Error as error:
                if error.sqlstate is None:
                    raise
                return dict(
                    sql=sql,
                    parameters=list(parameters),
                    sqlstate=error.sqlstate,
                    rows=None,
                    options=None,
                )

        for options in OPTIONS:
            for query in QUERIES:
                prefix = (options + ",") if options else ""
                entries.append(capture("EXPLAIN (" + prefix + "FORMAT JSON) " + query))
        for option in (
            "ANALYZE",
            "VERBOSE",
            "COSTS",
            "BUFFERS",
            "TIMING",
            "SUMMARY",
            "SETTINGS",
            "WAL",
        ):
            for value in (
                "ON",
                "OFF",
                "TRUE",
                "FALSE",
                "1",
                "0",
                "'t'",
                "'f'",
                "'yes'",
                "'no'",
                "'of'",
                "'o'",
                "'invalid'",
                "2",
                "'true'",
                "'false'",
                "'on'",
                "'off'",
                "'1'",
                "'0'",
                "1.0",
            ):
                entries.append(
                    capture(f"EXPLAIN (ANALYZE,{option} {value},FORMAT JSON) SELECT 1")
                )
        for options in (
            "WAL",
            "TIMING ON",
            "TIMING OFF",
            "SUMMARY ON",
            "SUMMARY OFF",
            "ANALYZE OFF,WAL OFF",
            "ANALYZE OFF,TIMING ON",
            "UNKNOWN_OPTION ON",
            "FORMAT invalid",
        ):
            entries.append(capture(f"EXPLAIN ({options},FORMAT JSON) SELECT 1"))
        for query in (
            "SELECT CASE WHEN false THEN (1,2) IN (SELECT 1/0,2) ELSE false END",
            "SELECT CASE WHEN true THEN (1,2) IN (SELECT 1/0,2) ELSE false END",
            "SELECT 1/0",
            "SELECT 1/0 WHERE false",
            "SELECT sum(1/0) FILTER(WHERE false)",
            "SELECT CASE WHEN false THEN 1/0 ELSE 42 END",
            "SELECT 1 FROM (SELECT 1/0 AS bad) t",
            "SELECT count(*) FROM (SELECT 1/0 AS bad) t",
            "SELECT count(*) FROM (SELECT sum(1/0) AS bad) t",
            "SELECT 1/(n-n) FROM (VALUES(1),(2)) t(n)",
        ):
            for analyze in ("ANALYZE,", ""):
                entries.append(
                    capture(f"EXPLAIN ({analyze}FORMAT JSON,TIMING OFF) " + query)
                )
        for query, values in (
            (
                "SELECT CASE WHEN $1 THEN (1,2) IN (SELECT 1/0,2) ELSE false END",
                (False,),
            ),
            (
                "SELECT CASE WHEN $1 THEN (1,2) IN (SELECT 1/0,2) ELSE false END",
                (True,),
            ),
            ("SELECT CASE WHEN $1 THEN 1/0 ELSE 42 END", (False,)),
            ("SELECT CASE WHEN $1 THEN 1/0 ELSE 42 END", (True,)),
            ("SELECT CASE WHEN $1::int IS NULL THEN 42 ELSE 1/0 END", (None,)),
            ("SELECT 1/$1", (0,)),
            ("SELECT 1/$1", (2,)),
            ("SELECT n FROM (VALUES(1),(2),(3)) t(n) WHERE n>$1", (1,)),
        ):
            for options in ("ANALYZE,", "", "ANALYZE,COSTS ON,SUMMARY OFF,TIMING OFF,"):
                entries.append(
                    capture("EXPLAIN (" + options + "FORMAT JSON) " + query, values)
                )
        truncations = []
        for timestamp in (
            "1969-12-31 23:59:59.999999+00",
            "2024-02-29 13:14:15.123456+01",
            "12000-02-29 13:14:15.123456+00",
            "0001-02-28 13:14:15.123456+00 BC",
            "infinity",
            "-infinity",
        ):
            for unit in (
                "microseconds",
                "milliseconds",
                "second",
                "minute",
                "hour",
                "day",
                "week",
                "month",
                "quarter",
                "year",
            ):
                query = f"SELECT date_trunc('{unit}','{timestamp}'::timestamptz)"
                value = db.execute("SELECT (" + query[7:] + ")::text").fetchone()[0]
                truncations.append(dict(sql=query, value=value))
                entries.append(capture("EXPLAIN (ANALYZE,FORMAT JSON) " + query))
        create = (
            "CREATE TABLE usage_records(id TEXT PRIMARY KEY,status TEXT,amount NUMERIC)"
        )
        seed = "INSERT INTO usage_records(id,status,amount) VALUES('a','open',1),('b','closed',2),('c','open',3),('d',NULL,NULL)"
        read = "SELECT id,status,amount FROM usage_records ORDER BY id"
        db.execute(create)
        db.execute(seed)
        inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        override_source = next(x for x in inventory if x["id"] == "sql-1409")
        override = dict(
            id=override_source["id"],
            sql=override_source["sql"],
            rows=[list(x) for x in db.execute(override_source["sql"])],
        )
        originals = []
        for identity in ("sql-0070", "sql-0071"):
            item = next(x for x in inventory if x["id"] == identity)
            if identity == "sql-0070":
                lines = [x[0] for x in db.execute(item["sql"]).fetchall()]
                count = int(
                    float(
                        re.search(r"actual .*rows=([0-9.]+) loops=", lines[0]).group(1)
                    )
                )
                info = dict(rows=count, options=None)
            else:
                info = expected(db.execute(item["sql"]).fetchone()[0])
            originals.append(dict(id=identity, sql=item["sql"], **info))
        before = [
            list(x)
            for x in db.execute(
                "SELECT id,status,amount::text FROM usage_records ORDER BY id"
            )
        ]
        db.execute("CREATE ROLE explain_reader NOLOGIN")
        db.execute("GRANT SELECT ON usage_records TO explain_reader")
        db.execute("ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY")
        db.execute(
            "CREATE POLICY explain_visible ON usage_records FOR SELECT TO explain_reader USING(status='open')"
        )
        db.execute("SET ROLE explain_reader")
        filtered = [
            capture("EXPLAIN (ANALYZE,FORMAT JSON,TIMING OFF,SUMMARY OFF) " + q)
            for q in [
                "SELECT id FROM usage_records",
                "SELECT count(*) FROM usage_records",
                "SELECT status,count(*) FROM usage_records GROUP BY status",
            ]
        ]
        db.execute("RESET ROLE")
        after = [
            list(x)
            for x in db.execute(
                "SELECT id,status,amount::text FROM usage_records ORDER BY id"
            )
        ]
        assert before == after
    return dict(
        format=1,
        reference="PostgreSQL 18 EXPLAIN semantic contracts",
        entries=entries,
        truncations=truncations,
        override=override,
        create=create,
        seed=seed,
        read=read,
        rows=before,
        originals=originals,
        filtered=filtered,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("EXPLAIN PostgreSQL witness drift")
        print(
            f"verified {len(result['entries'])} EXPLAIN contracts, 2 unchanged EXPLAIN originals, OVERRIDING and RLS"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
