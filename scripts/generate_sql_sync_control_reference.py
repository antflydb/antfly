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

"""PostgreSQL lifetime witnesses for native Antfly sync-level controls.

PostgreSQL's custom GUC supplies the scoping oracle only. Antfly's native
visibility levels, enum validation and write-durability floor are separately
verified against real native owners, not inferred from this custom GUC.
"""

import argparse
import json
from pathlib import Path

import psycopg
from generate_sql_postgres_reference import FIXTURES, postgres

IDS = ("sql-0774", "sql-0775")


def reference():
    originals = {
        entry["id"]: entry
        for entry in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    statements = [
        (None, "SHOW antfly.sync_level"),
        (IDS[0], originals[IDS[0]]["sql"]),
        (None, "SHOW antfly.sync_level"),
        (IDS[1], originals[IDS[1]]["sql"]),
        (None, "SHOW antfly.sync_level"),
        (None, "BEGIN"),
        (IDS[1], originals[IDS[1]]["sql"]),
        (None, "SHOW antfly.sync_level"),
        (None, "SAVEPOINT inner_scope"),
        (None, "SET antfly.sync_level='full_text'"),
        (None, "SHOW antfly.sync_level"),
        (None, "ROLLBACK TO inner_scope"),
        (None, "SHOW antfly.sync_level"),
        (None, "RELEASE inner_scope"),
        (None, "COMMIT"),
        (None, "SHOW antfly.sync_level"),
        (None, "BEGIN"),
        (None, "SET antfly.sync_level='enrichments'"),
        (None, "SET LOCAL antfly.sync_level='write'"),
        (None, "COMMIT"),
        (None, "SHOW antfly.sync_level"),
        (None, "BEGIN"),
        (None, "SET antfly.sync_level='full_text'"),
        (None, "ROLLBACK"),
        (None, "SHOW antfly.sync_level"),
        (None, "RESET antfly.sync_level"),
        (None, "SHOW antfly.sync_level"),
        (None, "SET antfly.sync_level='full_index'"),
        (None, "RESET ALL"),
        (None, "SHOW antfly.sync_level"),
    ]
    actions = []
    with postgres() as admin:
        with psycopg.connect(
            admin.info.dsn, autocommit=True, options="-c antfly.sync_level=write"
        ) as db:
            warnings = []
            db.add_notice_handler(lambda notice: warnings.append(notice.sqlstate))
            for identity, statement in statements:
                warnings.clear()
                cursor = db.execute(statement)
                actions.append(
                    dict(
                        id=identity,
                        sql=statement,
                        command_tag=cursor.statusmessage,
                        rows=[list(row) for row in cursor.fetchall()]
                        if cursor.description
                        else [],
                        notices=list(warnings),
                    )
                )
    assert len(actions) == 30
    assert actions[3]["notices"] == ["25P01"]
    assert actions[4]["rows"] == [["full_index"]]
    assert actions[7]["rows"] == [["propose"]]
    assert actions[12]["rows"] == [["propose"]]
    assert actions[15]["rows"] == [["full_index"]]
    assert actions[20]["rows"] == [["enrichments"]]
    assert actions[24]["rows"] == [["enrichments"]]
    assert actions[26]["rows"] == [["write"]]
    return dict(
        oracle="PostgreSQL 18 custom GUC lifetime; native visibility verified separately",
        cases=[originals[id] for id in IDS],
        actions=actions,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=FIXTURES / "sql_sync_control_reference.json"
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    rendered = json.dumps(reference(), indent=2) + "\n"
    if args.check:
        if not args.output.exists() or args.output.read_text() != rendered:
            raise SystemExit(
                f"PostgreSQL sync-control reference differs: {args.output}"
            )
        print("Verified 2 unchanged originals and 30 PostgreSQL lifetime actions")
    else:
        args.output.write_text(rendered)
        print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
