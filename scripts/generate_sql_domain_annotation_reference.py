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

"""PostgreSQL witness for domain annotations and independent type retirement."""

import argparse
import json
from pathlib import Path

from generate_sql_operator_reference import reference

BEFORE = [
    "CREATE DOMAIN usage_status AS text CHECK (VALUE IN ('queued','done'))",
    "sql-0150",
    "CREATE DOMAIN positive_amount AS numeric CHECK (VALUE > 0)",
    "sql-0309",
    "DROP DOMAIN positive_amount",
    "CREATE TYPE enum_wrong AS ENUM ('a')",
    "COMMENT ON DOMAIN enum_wrong IS 'wrong kind'",
    "DROP DOMAIN enum_wrong",
]
COLD = [
    "COMMENT ON DOMAIN usage_status IS NULL",
    "COMMENT ON DOMAIN usage_status IS 'status domain'",
]
AFTER = [
    "DROP DOMAIN usage_status",
    "CREATE DOMAIN usage_status AS text",
    "COMMENT ON TYPE usage_status IS 'type annotation'",
    "DROP TYPE usage_status",
    "DROP TYPE enum_wrong",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("destination", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = reference(
        BEFORE,
        COLD,
        AFTER,
        ("sql-0150", "sql-0309"),
        "PostgreSQL 18 domain annotation, independent retirement and object-kind diagnostics",
    )
    text = json.dumps(value, indent=2, ensure_ascii=False) + "\n"
    if args.check:
        if args.destination.read_text() != text:
            raise SystemExit("domain annotation witness is stale")
    else:
        args.destination.write_text(text)


if __name__ == "__main__":
    main()
