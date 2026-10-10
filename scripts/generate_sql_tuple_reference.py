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

"""Independent PostgreSQL truth tables for NULL-aware row IN and NOT IN.

Run with uv run --no-project --with 'psycopg[binary]==3.3.6' python
scripts/generate_sql_tuple_reference.py [--check fixture.json]. This is an
operator contract, not original-corpus disposition evidence.
"""

import argparse
from itertools import combinations_with_replacement, product
import json
from pathlib import Path

from generate_sql_postgres_reference import postgres


def reference(db):
    cases = []
    for width in (1, 2, 3):
        domain = list(product((None, 1, 2), repeat=width))
        # Exhaustive one/two-column multisets include duplicates and empty
        # sources. Three-column samples span independent NULL positions and
        # definite mismatches while probing every possible left-hand tuple.
        source_ids = range(len(domain)) if width < 3 else (0, 1, 3, 9, 13, 14, 22, 26)

        def row(values):
            return (
                "("
                + ",".join(
                    "NULL::bigint" if value is None else str(value) + "::bigint"
                    for value in values
                )
                + ")"
            )

        for count in range(3):
            for rhs in combinations_with_replacement(source_ids, count):
                source = (
                    "VALUES " + ",".join(row(domain[index]) for index in rhs)
                    if rhs
                    else "SELECT " + ",".join(["NULL::bigint"] * width) + " WHERE false"
                )
                expressions = [
                    f"{row(values)} {operator} ({source})"
                    for values in domain
                    for operator in ("IN", "NOT IN")
                ]
                result = db.execute("SELECT " + ",".join(expressions)).fetchone()
                yes = "".join(
                    "u" if value is None else "y" if value else "n"
                    for value in result[::2]
                )
                no = "".join(
                    "u" if value is None else "y" if value else "n"
                    for value in result[1::2]
                )
                if no != yes.translate(str.maketrans("yn", "ny")):
                    raise ValueError("PostgreSQL NOT IN violated three-valued negation")
                cases.append({"width": width, "rhs": list(rhs), "truth": yes})
    return {"reference": "PostgreSQL row IN and NOT IN", "cases": cases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    with postgres() as db:
        result = reference(db)
    if args.check:
        if json.loads(args.check.read_text()) != result:
            parser.error("PostgreSQL tuple membership reference drift")
        print(f"Verified {len(result['cases'])} PostgreSQL tuple source sets")
    else:
        print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
