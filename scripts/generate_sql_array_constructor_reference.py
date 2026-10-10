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

"""Verify durable ARRAY constructor expectations using an isolated PostgreSQL."""

import argparse
import json
from pathlib import Path

import psycopg

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_constructor_reference.json"
)
INPUTS = "SELECT 12::integer n,'hello'::text t,true flag,'[-2:-1]={1,NULL}'::integer[] a,'[-2:-1]={3,4}'::integer[] b,'{}'::integer[] e,NULL::integer[] z"
CASES = [
    "ARRAY[n,NULL,n+1]",
    "ARRAY['1',n]",
    "ARRAY[CAST(n AS real),CAST(2.5 AS real),NULL]",
    "ARRAY[CAST(n AS numeric),1.245::numeric,NULL]",
    "ARRAY[t,'hello',NULL]",
    "ARRAY[flag,true,NULL]",
    "ARRAY['00000000-0000-0000-0000-000000000001'::uuid,NULL]",
    "ARRAY[a,b]",
    "ARRAY[e,e]",
    "ARRAY[z,z]",
    "ARRAY[z,e]",
    "ARRAY[a,e]",
    "ARRAY[a,z]",
    "ARRAY[a,'[1:2]={3,4}'::integer[]]",
    "ARRAY[ARRAY[n,n+1],ARRAY[n+2,NULL]]",
    "ARRAY[NULL,NULL]",
    "ARRAY[]::numeric[]",
    "CASE WHEN false THEN ARRAY[32768::smallint] ELSE ARRAY[1::smallint] END",
    "ARRAY[32768::smallint]",
    "CAST('[-2:-1]={1,NULL}' AS integer[])",
    "CAST('{12.345,NULL}' AS numeric(4,2)[])",
    "CAST('{true,NULL,false}' AS boolean[])",
    "CAST('{\"null\",NULL}' AS jsonb[])",
    "ARRAY['bad',n]",
    "CASE WHEN false THEN ARRAY['bad',n] ELSE ARRAY[n] END",
    "ARRAY['1'::text,n]",
    "ARRAY[ARRAY[n],ARRAY[n,n+1]]",
]


def reference():
    entries = []
    with postgres() as connection:
        for sql in CASES:
            entry = {"sql": sql}
            try:
                text, kind = connection.execute(
                    f"SELECT ({sql})::text, pg_typeof({sql})::text FROM ({INPUTS}) q"
                ).fetchone()
                entry.update(text=text, pg_type=kind)
            except psycopg.Error as error:
                entry["sqlstate"] = error.sqlstate
                connection.rollback()
            entries.append(entry)
    return {"inputs": INPUTS, "entries": entries}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()
    actual = reference()
    if args.generate:
        print(json.dumps(actual, indent=2))
    else:
        expected = json.loads(FIXTURE.read_text())
        if actual != expected:
            raise SystemExit("PostgreSQL ARRAY constructor oracle differs from fixture")
        print(f"verified {len(actual['entries'])} ARRAY constructor values/SQLSTATEs")


if __name__ == "__main__":
    main()
