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

"""Independent PostgreSQL witness for unchanged trigger-function definitions."""

import argparse
import json

from generate_sql_postgres_reference import FIXTURES, postgres


def reference():
    from psycopg import Error

    cases = {
        x["id"]: x
        for x in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres() as db:
        for number in range(405, 411):
            case = cases[f"sql-{number:04d}"]
            entry = {k: case[k] for k in ("id", "source_entry_sha256", "sql")}
            entry["command_tag"] = db.execute(case["sql"]).statusmessage
            entries.append(entry)
        probes = []
        for sql in [
            "SELECT audit_body()",
            "SELECT audit_old_body()",
            "SELECT audit_null_body()",
            "SELECT audit_notice_body()",
            "SELECT audit_perform_body()",
            "SELECT audit_perform_arg_body()",
            "CREATE FUNCTION audit_body() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$",
            "CREATE OR REPLACE FUNCTION audit_body() RETURNS integer LANGUAGE sql AS $$SELECT 1$$",
            "CREATE FUNCTION invalid_trigger(integer) RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$",
            "CREATE FUNCTION invalid_trigger() RETURNS trigger LANGUAGE sql AS $$SELECT 1$$",
        ]:
            try:
                db.execute(sql)
                raise AssertionError(f"negative probe accepted: {sql}")
            except Error as exc:
                probes.append({"sql": sql, "sqlstate": exc.sqlstate})
        # CREATE deliberately does not resolve PERFORM names. Invocation does.
        db.execute("CREATE TABLE trigger_witness(id integer PRIMARY KEY, value text)")
        invocations = []
        for entry in entries:
            name = entry["sql"].split("(", 1)[0].split()[-1]
            db.execute("TRUNCATE trigger_witness")
            db.execute("INSERT INTO trigger_witness VALUES(1,'old')")
            db.execute(
                f"CREATE TRIGGER witness BEFORE UPDATE ON trigger_witness FOR EACH ROW EXECUTE FUNCTION {name}()"
            )
            try:
                db.execute("UPDATE trigger_witness SET value='new' WHERE id=1")
                invocations.append(
                    {
                        "id": entry["id"],
                        "value": db.execute(
                            "SELECT value FROM trigger_witness"
                        ).fetchone()[0],
                    }
                )
            except Error as exc:
                invocations.append({"id": entry["id"], "sqlstate": exc.sqlstate})
            db.execute("DROP TRIGGER witness ON trigger_witness")
        for sql in [
            "CREATE FUNCTION audit_log() RETURNS integer LANGUAGE sql AS $$SELECT 1$$",
            "CREATE FUNCTION audit_log(integer) RETURNS integer LANGUAGE sql AS $$SELECT $1$$",
        ]:
            db.execute(sql)
        # Both lazily bound signatures become executable without replacement.
        for name in ["audit_perform_body", "audit_perform_arg_body"]:
            db.execute(
                f"CREATE TRIGGER witness BEFORE UPDATE ON trigger_witness FOR EACH ROW EXECUTE FUNCTION {name}()"
            )
            db.execute("UPDATE trigger_witness SET value='new' WHERE id=1")
            assert (
                db.execute("SELECT value FROM trigger_witness").fetchone()[0] == "new"
            )
            db.execute("DROP TRIGGER witness ON trigger_witness")
        db.execute(
            "CREATE FUNCTION fallthrough() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NULL; END$$"
        )
        db.execute(
            "CREATE TRIGGER witness BEFORE UPDATE ON trigger_witness FOR EACH ROW EXECUTE FUNCTION fallthrough()"
        )
        try:
            db.execute("UPDATE trigger_witness SET value='new'")
            raise AssertionError("fallthrough unexpectedly succeeded")
        except Error as exc:
            assert exc.sqlstate == "2F005"
    return {
        "format": 1,
        "reference": "PostgreSQL 18 trigger-function definitions and row-return ABI",
        "entries": entries,
        "probes": probes,
        "invocations": invocations,
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--check", action="store_true")
    args = p.parse_args()
    target = FIXTURES / "sql_trigger_reference.json"
    value = reference()
    if args.check:
        assert json.loads(target.read_text()) == value, "trigger reference drift"
    else:
        target.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
