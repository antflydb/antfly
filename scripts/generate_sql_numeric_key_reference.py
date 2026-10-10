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

"""Verify NUMERIC index order independently with PostgreSQL dense ranks."""

import argparse
import json
import random
from pathlib import Path

from generate_sql_postgres_reference import postgres

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = (
    ROOT
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_exact_numeric_key_reference.json"
)


def inputs():
    source = json.loads(
        (FIXTURE.parent / "sql_exact_numeric_binary_reference.json").read_text()
    )
    values = [entry["input"] for entry in source["senders"]]
    values += [
        "-Infinity",
        "Infinity",
        "NaN",
        "nan",
        "0",
        "-0",
        "0.00000",
        "1",
        "1.0",
        "1.00000",
        "10e-1",
        "0.1e1",
        "1.0001",
        "1.00000001",
        "-1",
        "-1.0",
        "-1.0001",
        "-1.00000001",
        "9999",
        "10000",
        "10001",
        "99999999",
        "100000000",
        "100000001",
        "-9999",
        "-10000",
        "-10001",
        "-99999999",
        "-100000000",
        "-100000001",
        "0.0001",
        "0.00000001",
        "-0.0001",
        "-0.00000001",
        "9007199254740992",
        "9007199254740993",
        "-9007199254740992",
        "-9007199254740993",
        "1e131071",
        "-1e131071",
        "1e-16383",
        "-1e-16383",
    ]
    rng = random.Random(773041)
    for _ in range(128):
        digits = "".join(str(rng.randrange(10)) for _ in range(rng.randrange(1, 257)))
        sign = "-" if rng.randrange(2) else ""
        values.append(f"{sign}{digits}e{rng.randrange(-512, 513)}")
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()
    values = inputs()
    with postgres() as db:
        ranks = db.execute(
            "SELECT id, dense_rank() OVER (ORDER BY text::numeric) "
            "FROM unnest(%s::text[]) WITH ORDINALITY AS t(text,id) ORDER BY id",
            (values,),
        ).fetchall()
    output = {
        "reference": "PostgreSQL NUMERIC index dense ranks",
        "entries": [
            {"input": text, "rank": ranks[i][1]} for i, text in enumerate(values)
        ],
    }
    if args.generate:
        print(json.dumps(output, indent=2))
    else:
        if output != json.loads(FIXTURE.read_text()):
            raise ValueError("PostgreSQL NUMERIC index order oracle drift")
        print(f"Verified {len(values)} PostgreSQL NUMERIC index dense ranks")


if __name__ == "__main__":
    main()
