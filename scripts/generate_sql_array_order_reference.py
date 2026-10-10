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

"""Independently verify stored-array ordering using PostgreSQL binary values."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_array_order_reference.json"
)
CASES = [
    ("int16 signed", "int16", "ARRAY[-32768,2]::int2[]", "ARRAY[32767,2]::int2[]"),
    ("int32 count", "int32", "ARRAY[1,2]::int4[]", "ARRAY[1,2,0]::int4[]"),
    (
        "int64 exact",
        "int64",
        "ARRAY[9007199254740993]::int8[]",
        "ARRAY[9007199254740992]::int8[]",
    ),
    (
        "int64 extremes",
        "int64",
        "ARRAY[-9223372036854775808]::int8[]",
        "ARRAY[9223372036854775807]::int8[]",
    ),
    ("empty", "int32", "ARRAY[]::int4[]", "ARRAY[NULL]::int4[]"),
    ("empty equal", "text", "ARRAY[]::text[]", "ARRAY[]::text[]"),
    (
        "null after value",
        "int32",
        "ARRAY[1,NULL]::int4[]",
        "ARRAY[1,2147483647]::int4[]",
    ),
    ("null equal", "int32", "ARRAY[NULL,1]::int4[]", "ARRAY[NULL,1]::int4[]"),
    ("rank tie", "int32", "ARRAY[1,2]::int4[]", "ARRAY[[1,2]]::int4[]"),
    (
        "dimension lengths",
        "int32",
        "ARRAY[[1,2,3],[4,5,6]]::int4[]",
        "ARRAY[[1,2],[3,4],[5,6]]::int4[]",
    ),
    ("lower bounds", "int32", "'[-7:-6]={1,2}'::int4[]", "'[1:2]={1,2}'::int4[]"),
    (
        "second lower bound",
        "int32",
        "'[0:0][-1:0]={{1,2}}'::int4[]",
        "'[0:0][1:2]={{1,2}}'::int4[]",
    ),
    (
        "elements precede bounds",
        "int32",
        "'[-7:-6]={2,1}'::int4[]",
        "'[1:2]={1,2}'::int4[]",
    ),
    ("float32 nan", "float32", "ARRAY['NaN']::float4[]", "ARRAY['Infinity']::float4[]"),
    ("float32 zero", "float32", "ARRAY['-0']::float4[]", "ARRAY['0']::float4[]"),
    (
        "float32 infinities",
        "float32",
        "ARRAY['-Infinity',NULL]::float4[]",
        "ARRAY['Infinity',NULL]::float4[]",
    ),
    (
        "float64 nan equal",
        "float64",
        "ARRAY['NaN']::float8[]",
        "ARRAY['NaN']::float8[]",
    ),
    (
        "float64 null above nan",
        "float64",
        "ARRAY[NULL]::float8[]",
        "ARRAY['NaN']::float8[]",
    ),
    ("float64 zero", "float64", "ARRAY['-0']::float8[]", "ARRAY['0']::float8[]"),
    ("boolean", "boolean", "ARRAY[false,true]", "ARRAY[true,false]"),
    ("boolean null", "boolean", "ARRAY[NULL]::bool[]", "ARRAY[true]"),
    ("text C", "text", "ARRAY['z']::text[]", "ARRAY['ä']::text[]"),
    ("text prefix", "text", "ARRAY['abc']::text[]", "ARRAY['abcd']::text[]"),
    ("text null", "text", "ARRAY[NULL]::text[]", "ARRAY['zz']::text[]"),
    (
        "uuid",
        "uuid",
        "ARRAY['00000000-0000-0000-0000-000000000001']::uuid[]",
        "ARRAY['ffffffff-ffff-ffff-ffff-ffffffffffff']::uuid[]",
    ),
    (
        "uuid null",
        "uuid",
        "ARRAY[NULL]::uuid[]",
        "ARRAY['ffffffff-ffff-ffff-ffff-ffffffffffff']::uuid[]",
    ),
    (
        "numeric scale",
        "numeric",
        "ARRAY[1.0,-0.00]::numeric[]",
        "ARRAY[1.000,0]::numeric[]",
    ),
    (
        "numeric exact",
        "numeric",
        "ARRAY[9007199254740993]::numeric[]",
        "ARRAY[9007199254740992]::numeric[]",
    ),
    (
        "numeric nan",
        "numeric",
        "ARRAY['NaN']::numeric[]",
        "ARRAY['Infinity']::numeric[]",
    ),
    (
        "numeric infinity",
        "numeric",
        "ARRAY['-Infinity']::numeric[]",
        "ARRAY[-1e1000]::numeric[]",
    ),
    ("numeric null", "numeric", "ARRAY[NULL]::numeric[]", "ARRAY['NaN']::numeric[]"),
    ("jsonb null distinction", "jsonb", "ARRAY['null'::jsonb]", "ARRAY[NULL]::jsonb[]"),
    (
        "jsonb numeric exact",
        "jsonb",
        "ARRAY['9007199254740993'::jsonb]",
        "ARRAY['9007199254740992'::jsonb]",
    ),
    ("jsonb structural", "jsonb", "ARRAY['[1,2]'::jsonb]", "ARRAY['[9]'::jsonb]"),
    (
        "jsonb object order",
        "jsonb",
        'ARRAY[\'{"b":2,"a":1}\'::jsonb]',
        'ARRAY[\'{"a":1.00,"b":2}\'::jsonb]',
    ),
    ("jsonb heterogeneous", "jsonb", "ARRAY['\"x\"'::jsonb]", "ARRAY['true'::jsonb]"),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    entries = []
    with postgres() as db:
        for name, kind, left, right in CASES:
            collate = ' COLLATE "C"' if kind == "text" else ""
            query = f"WITH v AS (SELECT ({left}){collate} AS a, ({right}){collate} AS b) SELECT encode(array_send(a),'hex'), encode(array_send(b),'hex'), CASE WHEN a < b THEN 'lt' WHEN a > b THEN 'gt' ELSE 'eq' END FROM v"
            a, b, order = db.execute(query).fetchone()
            entries.append(
                {
                    "name": name,
                    "element_type": kind,
                    "left_binary": a,
                    "right_binary": b,
                    "order": order,
                }
            )
    fixture = {
        "reference": "PostgreSQL 18 array ordering and binary send",
        "entries": entries,
    }
    if args.write:
        FIXTURE.write_text(json.dumps(fixture, indent=2, ensure_ascii=False) + "\n")
    elif json.loads(FIXTURE.read_text()) != fixture:
        raise ValueError("PostgreSQL array ordering oracle drift")
    print(f"Verified {len(entries)} PostgreSQL array ordering cases")


if __name__ == "__main__":
    main()
