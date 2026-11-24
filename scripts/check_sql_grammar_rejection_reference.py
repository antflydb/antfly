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

"""Parse-only PostgreSQL qualification of explicit native syntax rejections."""

import json
import re

from generate_sql_postgres_reference import ROOT, FIXTURES, postgres


def main():
    from psycopg.pq import DiagnosticField, ExecStatus

    source = (
        ROOT / "zig/pkg/antfly/src/api/sql/testing/document_root_test.zig"
    ).read_text()
    anchor = 'test "httpx SQL original PostgreSQL invalid grammar rejects before catalog access"'
    section = source.split(anchor, 1)[1].split("\ntest ", 1)[0]
    ids = re.findall(r'"(sql-\d{4})"', section)
    dispositions = json.loads((FIXTURES / "sql_parity_dispositions.json").read_text())
    expected = {
        entry["id"]
        for entry in dispositions["entries"]
        if any(
            witness["gate"] == "sql-original-grammar-postgres"
            for witness in entry.get("evidence", ())
        )
    }
    if not expected or len(ids) != len(set(ids)) or set(ids) != expected:
        raise ValueError(
            f"explicit original syntax cohort changed: "
            f"missing {sorted(expected - set(ids))}, extra {sorted(set(ids) - expected)}"
        )
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    cases = {entry["id"]: entry for entry in inventory["entries"]}
    with postgres() as db:
        for identity in ids:
            case = cases[identity]
            result = db.pgconn.prepare(b"", case["sql"].encode())
            if (
                result.status != ExecStatus.FATAL_ERROR
                or result.error_field(DiagnosticField.SQLSTATE) != b"42601"
            ):
                raise ValueError(f"{identity}: PostgreSQL grammar contract changed")
    print(f"verified {len(ids)} exact PostgreSQL parse-only grammar rejections")


if __name__ == "__main__":
    main()
