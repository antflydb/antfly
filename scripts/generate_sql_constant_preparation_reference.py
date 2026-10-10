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

"""Verify speculative constant preparation preserves PostgreSQL lazy demand."""

import json
from pathlib import Path

from generate_sql_postgres_reference import postgres

FIXTURE = (
    Path(__file__).resolve().parents[1]
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_constant_preparation_reference.json"
)


def main():
    import psycopg

    fixture = json.loads(FIXTURE.read_text())
    with postgres() as db:
        for entry in fixture["entries"]:
            try:
                value = db.execute(f"SELECT ({entry['sql']})::text").fetchone()[0]
            except psycopg.Error as error:
                if entry.get("error") != error.sqlstate:
                    raise ValueError(f"SQLSTATE mismatch: {entry['sql']}") from error
            else:
                if "error" in entry or value != entry["expected"]:
                    raise ValueError(f"Value mismatch: {entry['sql']}")
    print(f"Verified {len(fixture['entries'])} PostgreSQL constant-preparation cases")


if __name__ == "__main__":
    main()
