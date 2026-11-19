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

"""Reproduce the exact public mutation-syntax gate against PostgreSQL 18+.

The native test owns its explicit case IDs. No SQL is rewritten, no inference
from an error-message substring is made, and this check changes no ledger.
"""

import json
import re

from generate_sql_postgres_reference import (
    ROOT,
    FIXTURES,
    mutation_profile,
    mutation_reference,
    postgres,
)


def main():
    source = (ROOT / "zig/pkg/antfly/src/api/sql_document_root_test.zig").read_text()
    anchor = 'test "httpx SQL original PostgreSQL mutation syntax errors never access storage"'
    section = source.split(anchor, 1)[1].split("\ntest ", 1)[0]
    ids = re.findall(r'"(sql-\d{4})"', section)
    if len(ids) != 105 or len(set(ids)) != 105:
        raise ValueError("the explicit 105-case syntax cohort changed")
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    selected = set(ids)
    cases = [entry for entry in inventory["entries"] if entry["id"] in selected]
    if len(cases) != len(ids):
        raise ValueError("syntax cohort refers to missing original cases")
    with postgres() as db:
        result = mutation_reference(db, cases, mutation_profile())
    actual = {entry["id"]: entry.get("sqlstate") for entry in result["excluded"]}
    if result["entries"] or actual != dict.fromkeys(ids, "42601"):
        raise ValueError(f"PostgreSQL mutation syntax contracts changed: {actual}")
    print(f"Verified {len(ids)} exact PostgreSQL mutation syntax diagnostics")


if __name__ == "__main__":
    main()
