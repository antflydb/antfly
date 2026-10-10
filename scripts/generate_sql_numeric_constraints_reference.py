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

"""Verify immutable NUMERIC constraint plans against a disposable PostgreSQL."""

import argparse
import json
from decimal import Decimal
from pathlib import Path

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_numeric_constraints_reference.json"
)
SPECIAL = {
    "nan",
    "+nan",
    "-nan",
    "infinity",
    "+infinity",
    "-infinity",
    "inf",
    "+inf",
    "-inf",
}


def literal(value):
    # Finite JSON strings are not numeric const/enum members.
    if (
        isinstance(value, Decimal)
        or isinstance(value, str)
        and value.lower() in SPECIAL
    ):
        return str(value)
    return None


def cases():
    for key in (
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
    ):
        bound = "0.0001" if key == "multipleOf" else "9007199254740993.2500"
        schema = '{"' + key + '":' + bound + "}"
        for value in (
            "9007199254740993.2499",
            "9007199254740993.25",
            "9007199254740993.2501",
            "9007199254740993.25001",
            "NaN",
            "Infinity",
            "-Infinity",
        ):
            yield schema, value
    for schema in (
        '{"minimum":1e-1000,"maximum":1e1000,"multipleOf":1e-1000}',
        '{"const":9007199254740993.2500}',
        '{"const":"NaN"}',
        '{"const":"9007199254740993.25"}',
        '{"enum":[9007199254740993.2500,9007199254740993.25,"NaN","Infinity","9007199254740993.26",null,true]}',
    ):
        for value in (
            "9007199254740993.25",
            "9007199254740993.26",
            "1e-999",
            "1e-1001",
            "NaN",
            "Infinity",
            "-Infinity",
        ):
            yield schema, value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", action="store_true")
    args = parser.parse_args()
    output = {"reference": "PostgreSQL 18 NUMERIC constraint predicates", "entries": []}
    operators = {
        "minimum": ">=",
        "maximum": "<=",
        "exclusiveMinimum": ">",
        "exclusiveMaximum": "<",
    }
    with postgres() as db:
        for schema, value in cases():
            document = json.loads(schema, parse_float=Decimal, parse_int=Decimal)
            predicates, params = [], []
            for key, bound in document.items():
                if key in operators:
                    predicates.append(f"n {operators[key]} %s::numeric")
                    params.append(str(bound))
                elif key == "multipleOf":
                    predicates.append("mod(n,%s::numeric)=0")
                    params.append(str(bound))
                elif key in ("const", "enum"):
                    values = [bound] if key == "const" else bound
                    values = [
                        parsed
                        for item in values
                        if (parsed := literal(item)) is not None
                    ]
                    predicates.append(
                        "(" + " OR ".join("n=%s::numeric" for _ in values) + ")"
                        if values
                        else "false"
                    )
                    params.extend(values)
                else:
                    raise ValueError(f"Unrecognized oracle keyword: {key}")
            sql = (
                "SELECT "
                + " AND ".join(predicates)
                + " FROM (SELECT %s::numeric AS n) AS inputs"
            )
            expected = db.execute(sql, (*params, value)).fetchone()[0]
            output["entries"].append(
                {"schema": schema, "value": value, "expected": expected}
            )
    if args.generate:
        print(json.dumps(output, indent=2))
    else:
        if output != json.loads(FIXTURE.read_text()):
            raise ValueError("PostgreSQL NUMERIC constraint oracle drift")
        print(
            f"Verified {len(output['entries'])} PostgreSQL NUMERIC constraint predicates"
        )


if __name__ == "__main__":
    main()
