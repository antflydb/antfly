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

"""PostgreSQL witnesses for enum catalog publication; row activation is separate."""

import argparse
import json
from pathlib import Path

from generate_sql_operator_reference import reference

BEFORE = [
    "sql-0096",
    "sql-0096",
    "sql-0097",
    "sql-0098",
    "ALTER TYPE usage_status ADD VALUE 'started' BEFORE 'processing'",
    "ALTER TYPE usage_status ADD VALUE 'started'",
    "ALTER TYPE usage_status ADD VALUE IF NOT EXISTS 'started' BEFORE 'absent'",
    "ALTER TYPE usage_status ADD VALUE 'new' BEFORE 'absent'",
    "CREATE TYPE usage_status AS ENUM ('" + "x" * 64 + "')",
    "CREATE TYPE too_long_enum AS ENUM ('" + "x" * 64 + "')",
    "CREATE TYPE empty_enum AS ENUM ()",
    "ALTER TYPE empty_enum ADD VALUE ''",
    "CREATE SCHEMA enum_tools",
    "SET search_path=enum_tools,public",
    "CREATE TYPE usage_status AS ENUM ('other')",
    "ALTER TYPE usage_status ADD VALUE 'another'",
    "SET search_path=public",
]
AFTER = [
    "sql-0099",
    "sql-0100",
    "DROP TYPE enum_tools.usage_status",
    "DROP SCHEMA enum_tools",
    "DROP TYPE empty_enum",
    "CREATE TYPE usage_status AS ENUM ('fresh')",
    "DROP TYPE usage_status",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    value = reference(
        BEFORE,
        [],
        AFTER,
        ("sql-0096", "sql-0097", "sql-0098", "sql-0099", "sql-0100"),
        "PostgreSQL 18 enum catalog declarations; typed rows and dependent activation remain unqualified",
    )
    encoded = json.dumps(value, indent=2) + "\n"
    if args.check:
        assert args.output.read_text() == encoded, (
            "PostgreSQL enum catalog witness drift"
        )
    else:
        args.output.write_text(encoded)


if __name__ == "__main__":
    main()
