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

"""PostgreSQL witness for domain-backed rows and coercion provenance."""

import argparse
import json
from pathlib import Path

from generate_sql_operator_reference import reference

ROWS = "SELECT id,amount::text AS amount,status::text AS status FROM domain_rows ORDER BY id"
BEFORE = [
    "sql-0306",
    "sql-0307",
    "CREATE TABLE domain_rows(id BIGINT PRIMARY KEY,amount positive_amount,status status_text)",
    "INSERT INTO domain_rows(id,amount) VALUES(1,1)",
    "INSERT INTO domain_rows(id,amount) VALUES(3,0)",
    "INSERT INTO domain_rows(id,amount,status) VALUES(3,3,NULL)",
    "INSERT INTO domain_rows(id,amount,status) SELECT 2,2,r.status FROM (VALUES(1)) AS seed(id) LEFT JOIN domain_rows r ON r.id=999",
    "UPDATE domain_rows SET amount=3 WHERE id=2",
    ROWS,
    "INSERT INTO domain_rows(id,amount) VALUES(4,NULL)",
    "ALTER DOMAIN positive_amount SET NOT NULL",
    "DELETE FROM domain_rows WHERE id=4",
    "sql-0308",
    "INSERT INTO domain_rows(id,amount) VALUES(4,NULL)",
]
COLD = [
    ROWS,
    "UPDATE domain_rows SET amount=4 WHERE id=2",
    "UPDATE domain_rows SET amount=-1 WHERE id=1",
    "UPDATE domain_rows SET status=NULL WHERE id=1",
    "INSERT INTO domain_rows(id,amount) VALUES(3,5)",
    ROWS,
]

AFTER = [
    "sql-0310",
    "SELECT id,status::text AS status FROM domain_rows ORDER BY id",
    "INSERT INTO domain_rows(id) VALUES(4)",
    "SELECT id,status::text AS status FROM domain_rows ORDER BY id",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = reference(
        BEFORE,
        COLD,
        AFTER,
        ("sql-0306", "sql-0307", "sql-0308", "sql-0310"),
        "PostgreSQL 18 domain rows, defaults, checks and typed NULL provenance",
    )
    assert all("sqlstate" not in step for step in value["before"][:4])
    assert value["before"][4]["sqlstate"] == "23514"
    assert value["before"][5]["sqlstate"] == "23502"
    assert value["before"][10]["sqlstate"] == "23502"
    assert value["before"][13]["sqlstate"] == "23502"
    assert value["cold"][2]["sqlstate"] == "23514"
    assert value["cold"][3]["sqlstate"] == "23502"
    assert all("sqlstate" not in step for step in value["after"])
    text = json.dumps(value, indent=2, ensure_ascii=False) + "\n"
    if args.check:
        if args.output.read_text() != text:
            raise SystemExit("domain row PostgreSQL witness drift")
    else:
        args.output.write_text(text)


if __name__ == "__main__":
    main()
