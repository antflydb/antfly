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

"""PostgreSQL witness for the unchanged disconnected subscription original."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres


def reference():
    original = next(
        value
        for value in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
        if value["id"] == "sql-0120"
    )
    assert not original["params"]
    with postgres(logical_replication=True) as db:
        cursor = db.execute(original["sql"])
        row = db.execute(
            "SELECT subname,subconninfo,subenabled,subbinary,substream,"
            "subtwophasestate,subdisableonerr,subpasswordrequired,subrunasowner,"
            "subfailover,suborigin,subslotname,subsynccommit,subpublications "
            "FROM pg_subscription WHERE subname='usage_sub_options'"
        ).fetchone()
        assert row is not None
        assert row[4] == "p" and row[5] == "d"
        return {
            "id": original["id"],
            "sql": original["sql"],
            "command_tag": cursor.statusmessage,
            "name": row[0],
            "connection": row[1],
            "enabled": row[2],
            "binary": row[3],
            "streaming": "parallel",
            "two_phase": False,
            "disable_on_error": row[6],
            "password_required": row[7],
            "run_as_owner": row[8],
            "failover": row[9],
            "origin": row[10],
            "slot_name": row[11],
            "synchronous_commit": row[12],
            "publications": row[13],
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    actual = reference()
    if args.check:
        if actual != json.loads(args.check.read_text()):
            raise SystemExit("disconnected subscription PostgreSQL witness changed")
        print("PostgreSQL disconnected subscription options witness verified")
    else:
        print(json.dumps(actual, indent=2))


if __name__ == "__main__":
    main()
