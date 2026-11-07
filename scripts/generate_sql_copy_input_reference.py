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

"""PostgreSQL 18+ COPY input originals, complete postimages and atomic failures."""

import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres
from generate_sql_temporal_key_reference import output

CREATE = "CREATE TABLE usage_records(id BIGINT PRIMARY KEY,status TEXT CHECK(status <> 'blocked'),amount NUMERIC(12,2) DEFAULT 3.50,normalized TEXT GENERATED ALWAYS AS (lower(status)) STORED)"
READ = "SELECT id,status,amount,normalized FROM usage_records ORDER BY id"


def reference():
    import psycopg

    inventory = {
        e["id"]: e
        for e in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())[
            "entries"
        ]
    }
    entries = []
    with postgres() as db:
        db.execute(CREATE)
        db.execute("CREATE TABLE copy_source(id BIGINT,status TEXT)")

        def payload(rows, codec):
            db.execute("DELETE FROM copy_source")
            with db.cursor() as cursor:
                cursor.executemany("INSERT INTO copy_source VALUES(%s,%s)", rows)
                with cursor.copy(
                    f"COPY (SELECT id,status FROM copy_source) TO STDOUT (FORMAT {codec})"
                ) as stream:
                    return b"".join(stream)

        def state():
            return output(db.execute(READ))

        for case_id, codec in [
            ("sql-0370", "csv"),
            ("sql-0371", "text"),
            ("sql-0379", "binary"),
        ]:
            original = inventory[case_id]
            db.execute("DELETE FROM usage_records")
            rows = [(i, ["READY", 'a\tb\nc\\d"é', "", None][i % 4]) for i in range(300)]
            data = payload(rows, codec)
            with db.cursor() as cursor:
                with cursor.copy(original["sql"]) as stream:
                    for offset in range(0, len(data), 7):
                        stream.write(data[offset : offset + 7])
                tag = cursor.statusmessage
            expected = state()
            faults = []
            for name, final, code in [
                ("duplicate", (0, "READY"), "23505"),
                ("check", (1128, "blocked"), "23514"),
                ("null", (None, "READY"), "23502"),
            ]:
                broken = payload(
                    [(1000 + i, "READY") for i in range(128)] + [final], codec
                )
                try:
                    with db.transaction():
                        with db.cursor().copy(original["sql"]) as stream:
                            stream.write(broken)
                except psycopg.Error as error:
                    if error.sqlstate != code:
                        raise
                else:
                    raise AssertionError((case_id, name, "accepted"))
                if state() != expected:
                    raise AssertionError((case_id, name, "partial import"))
                faults.append({"name": name, "hex": broken.hex(), "sqlstate": code})
            prefix = payload([(2000 + i, "READY") for i in range(128)], codec)
            malformed = (
                prefix[:-2] + b"\x00\x03"
                if codec == "binary"
                else prefix + (b"1,2,3\n" if codec == "csv" else b"1\t2\t3\n")
            )
            try:
                with db.transaction():
                    with db.cursor().copy(original["sql"]) as stream:
                        stream.write(malformed)
            except psycopg.Error as error:
                if error.sqlstate != "22P04":
                    raise
            else:
                raise AssertionError((case_id, "malformed accepted"))
            if state() != expected:
                raise AssertionError((case_id, "malformed partial import"))
            faults.append(
                {"name": "malformed", "hex": malformed.hex(), "sqlstate": "22P04"}
            )
            entries.append(
                {
                    "id": case_id,
                    "sql": original["sql"],
                    "format": codec,
                    "hex": data.hex(),
                    "tag": tag,
                    "output": expected,
                    "faults": faults,
                }
            )
        advanced = []
        for codec in ["csv", "text"]:
            db.execute("DELETE FROM usage_records")
            separator = "," if codec == "csv" else "\t"
            options = (
                "FORMAT "
                + codec
                + ", HEADER match, DEFAULT 'n/a', ON_ERROR ignore, REJECT_LIMIT 10, LOG_VERBOSITY verbose"
            )
            if codec == "csv":
                options += ", FORCE_NULL(status), FORCE_NOT_NULL(status)"
            sql = "COPY usage_records(id,status,amount) FROM STDIN (" + options + ")"
            header = separator.join(["id", "status", "amount"]) + "\n"

            def record(*cells):
                return separator.join(str(cell) for cell in cells) + "\n"

            data = header + "".join(record(i, "READY", "n/a") for i in range(128))
            data += record("bad", "READY", "1") + record(
                128, "READY", "100000000000000"
            )
            data += record(129, "READY", "n/a") + record(130, "", "2.345")
            if codec == "csv":
                data += record(131, '""', "n/a") + record(132, "READY", '"n/a"')
            else:
                data += record(131, "READY", "\\156/a")
            with db.cursor() as cursor:
                with cursor.copy(sql) as stream:
                    stream.write(data.encode())
                tag = cursor.statusmessage
            expected = state()
            prefix = header + "".join(
                record(1000 + i, "READY", "n/a") for i in range(128)
            )
            faults = []
            for name, broken, code in [
                ("duplicate", prefix + record(0, "READY", "n/a"), "23505"),
                ("check_not_ignored", prefix + record(1128, "blocked", "n/a"), "23514"),
                (
                    "default_without_default",
                    prefix + record("n/a", "READY", "n/a"),
                    "22P04",
                ),
                (
                    "bad_header",
                    record("status", "id", "amount") + record(2000, "READY", "n/a"),
                    "22P04",
                ),
                (
                    "early_check_before_bad_framing",
                    header
                    + record(2000, "blocked", "n/a")
                    + record(2001, "READY", "n/a", "extra"),
                    "23514",
                ),
                ("missing_header", "", "22P04"),
                ("reject_limit", prefix + record("bad", "READY", "n/a") * 11, "22P02"),
                (
                    "framing_not_ignored",
                    prefix + record(2000, "READY", "n/a", "extra"),
                    "22P04",
                ),
            ]:
                try:
                    with db.transaction():
                        with db.cursor().copy(sql) as stream:
                            stream.write(broken.encode())
                except psycopg.Error as error:
                    if error.sqlstate != code:
                        raise
                else:
                    raise AssertionError((codec, name, "accepted"))
                if state() != expected:
                    raise AssertionError((codec, name, "partial import"))
                faults.append(
                    {"name": name, "hex": broken.encode().hex(), "sqlstate": code}
                )
            advanced.append(
                {
                    "id": "copy-options-" + codec,
                    "sql": sql,
                    "format": codec,
                    "hex": data.encode().hex(),
                    "tag": tag,
                    "output": expected,
                    "faults": faults,
                }
            )
        predicates = []
        original_where = inventory["sql-0380"]
        for case_id, sql, parameters in [
            (original_where["id"], original_where["sql"], []),
            (
                "copy-where-defaults",
                "COPY usage_records(id,status) FROM STDIN (FORMAT csv) WHERE amount = 3.50 AND status <> 'blocked'",
                [],
            ),
            (
                "copy-where-null-parameter",
                "COPY usage_records(id,status) FROM STDIN (FORMAT csv) WHERE id >= $1 AND status = 'active' AND amount > $2",
                ["128", None],
            ),
            (
                "copy-where-parameters",
                "COPY usage_records(id,status) FROM STDIN (FORMAT csv) WHERE id >= $1 AND status = 'active' AND amount > $2",
                ["128", "3.4999"],
            ),
        ]:
            db.execute("DELETE FROM usage_records")
            data = payload(
                [
                    (i, ["active", "inactive", "blocked", None][i % 4])
                    for i in range(260)
                ]
                + [(None, "blocked")],
                "csv",
            )
            wire_sql = sql
            for index in range(len(parameters)):
                wire_sql = wire_sql.replace("$" + str(index + 1), "%s")
            with db.cursor() as cursor:
                with cursor.copy(
                    wire_sql, tuple(parameters) if parameters else None
                ) as stream:
                    stream.write(data)
                tag = cursor.statusmessage
            expected = state()
            prefix = [(1000 + i, "active") for i in range(128)]
            duplicate = expected["rows"][0][0] if expected["rows"] else None
            faults = []
            cases = [
                (
                    "duplicate",
                    payload(prefix + [(duplicate, "active")], "csv"),
                    "23505",
                ),
                (
                    "bad_type_even_when_excluded",
                    payload(prefix, "csv") + b"not-an-integer,blocked\n",
                    "22P02",
                ),
                ("framing", payload(prefix, "csv") + b"2000,active,extra\n", "22P04"),
            ]
            if duplicate is None:
                cases = [entry for entry in cases if entry[0] != "duplicate"]
            if not parameters:
                cases.append(
                    (
                        "selected_not_null",
                        payload(prefix + [(None, "active")], "csv"),
                        "23502",
                    )
                )
            for name, broken, code in cases:
                try:
                    with db.transaction():
                        with db.cursor().copy(
                            wire_sql, tuple(parameters) if parameters else None
                        ) as stream:
                            stream.write(broken)
                except psycopg.Error as error:
                    if error.sqlstate != code:
                        raise
                else:
                    raise AssertionError((case_id, name, "accepted"))
                if state() != expected:
                    raise AssertionError((case_id, name, "partial import"))
                faults.append({"name": name, "hex": broken.hex(), "sqlstate": code})
            predicates.append(
                {
                    "id": case_id,
                    "sql": sql,
                    "format": "csv",
                    "parameters": parameters,
                    "hex": data.hex(),
                    "tag": tag,
                    "output": expected,
                    "faults": faults,
                }
            )
        predicate_errors = []
        for sql, code in [
            (
                "COPY usage_records(id,status) FROM STDIN WHERE normalized = 'active'",
                "42P10",
            ),
            ("COPY usage_records(id,status) FROM STDIN WHERE count(*) > 0", "42803"),
            ("COPY usage_records(id,status) FROM STDIN WHERE (SELECT true)", "0A000"),
        ]:
            try:
                with db.transaction():
                    with db.cursor().copy(sql) as stream:
                        stream.write(b"")
            except psycopg.Error as error:
                if error.sqlstate != code:
                    raise
            else:
                raise AssertionError((sql, "forbidden COPY predicate admitted"))
            predicate_errors.append({"sql": sql, "sqlstate": code})
        invalid = inventory["sql-0381"]
        for invalid_id in ["sql-0381", "sql-0382"]:
            original = inventory[invalid_id]
            try:
                with db.transaction():
                    with db.cursor().copy(original["sql"]) as stream:
                        stream.write(b"")
            except psycopg.Error as error:
                if error.sqlstate != "42601":
                    raise
            else:
                raise AssertionError(
                    (invalid_id, "removed PostgreSQL OIDS option admitted")
                )
        db.execute("CREATE TABLE copy_options(id BIGINT)")
        option_checks = []
        for direction, options in [
            ("FROM", "FORMAT csv, HEADER match"),
            ("FROM", "FORMAT text, HEADER match"),
            ("FROM", "FORMAT binary, HEADER false"),
            ("FROM", "FORMAT csv, FORCE_NULL *"),
            ("FROM", "FORMAT csv, FORCE_NOT_NULL *"),
            (
                "FROM",
                "FORMAT csv, ON_ERROR 'IGNORE', LOG_VERBOSITY 'SILENT', REJECT_LIMIT '10'",
            ),
            ("TO", "LOG_VERBOSITY silent"),
            ("FROM", "FORMAT csv, OIDS false"),
            ("FROM", "FORMAT binary, ON_ERROR ignore"),
            ("FROM", "FORMAT text, FORCE_NULL(id)"),
            ("FROM", "FORMAT csv, FORCE_QUOTE *"),
            ("FROM", "FORMAT csv, FORCE_NULL(id,id)"),
            ("FROM", "FORMAT csv, FORCE_NOT_NULL(id,id)"),
            ("FROM", "FORMAT csv, REJECT_LIMIT 2"),
            ("FROM", "FORMAT csv, ON_ERROR ignore, REJECT_LIMIT 0"),
            ("FROM", "FORMAT csv, DEFAULT ''"),
            ("TO", "ON_ERROR stop"),
            ("TO", "FORMAT csv, DEFAULT 'd'"),
            ("TO", "FORMAT csv, HEADER match"),
            ("TO", "FORMAT csv, FORCE_NULL(id)"),
            ("TO", "FORMAT csv, FORCE_NOT_NULL(id)"),
        ]:
            sql = "COPY copy_options " + direction + " STDIN (" + options + ")"
            code = None
            try:
                with db.transaction():
                    with db.cursor().copy(sql) as stream:
                        if direction == "FROM":
                            stream.write(
                                b"id\n"
                                if "HEADER match" in options
                                else b"PGCOPY\n\xff\r\n\x00" + bytes(8) + b"\xff\xff"
                                if "FORMAT binary" in options
                                else b""
                            )
                        else:
                            list(stream)
            except psycopg.Error as error:
                code = error.sqlstate
            option_checks.append({"sql": sql, "sqlstate": code})
        identity_create = "CREATE TABLE copy_identity(id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,status TEXT)"
        db.execute(identity_create)
        identity_copy = "COPY copy_identity(id,status) FROM STDIN"
        with db.cursor().copy(identity_copy) as stream:
            stream.write(b"42\tREADY\n")
        db.execute("INSERT INTO copy_identity(status) VALUES('default')")
        with db.cursor().copy("COPY copy_identity(status) FROM STDIN") as stream:
            stream.write(b"next\n")
        identity_fault = b"before_failure\nbad\textra\n"
        try:
            with db.transaction():
                with db.cursor().copy(
                    "COPY copy_identity(status) FROM STDIN"
                ) as stream:
                    stream.write(identity_fault)
        except psycopg.Error as error:
            if error.sqlstate != "22P04":
                raise
        else:
            raise AssertionError("malformed identity COPY admitted")
        with db.cursor().copy("COPY copy_identity(status) FROM STDIN") as stream:
            stream.write(b"after_failure\n")
        identity_filter_sql = "COPY copy_identity(status) FROM STDIN WHERE id % 2 = 0"
        identity_filter = b"excluded_by_default_id\nretained_by_default_id\n"
        with db.cursor().copy(identity_filter_sql) as stream:
            stream.write(identity_filter)
        identity = {
            "create": identity_create,
            "copy": identity_copy,
            "filter_sql": identity_filter_sql,
            "filter_hex": identity_filter.hex(),
            "fault_hex": identity_fault.hex(),
            "fault_sqlstate": "22P04",
            "after_hex": b"after_failure\n".hex(),
            "hex": b"42\tREADY\n".hex(),
            "output": output(
                db.execute("SELECT id,status FROM copy_identity ORDER BY id")
            ),
        }
        db.execute("CREATE TABLE copy_rls(id BIGINT PRIMARY KEY,status TEXT)")
        db.execute("CREATE ROLE copy_writer")
        db.execute("GRANT INSERT ON copy_rls TO copy_writer")
        db.execute(
            "CREATE POLICY allow_all ON copy_rls FOR ALL USING(TRUE) WITH CHECK(TRUE)"
        )
        db.execute("ALTER TABLE copy_rls ENABLE ROW LEVEL SECURITY")
        db.execute("SET ROLE copy_writer")
        try:
            with db.transaction():
                with db.cursor().copy("COPY copy_rls FROM STDIN") as stream:
                    stream.write(b"1\tREADY\n")
        except psycopg.Error as error:
            if error.sqlstate != "0A000":
                raise
        else:
            raise AssertionError("COPY unexpectedly admitted RLS")
        db.execute("RESET ROLE")
        if db.execute("SELECT * FROM copy_rls").fetchall():
            raise AssertionError("RLS COPY wrote rows")
    return {
        "reference": "PostgreSQL exact COPY input",
        "create": CREATE,
        "read": READ,
        "entries": entries,
        "advanced": advanced,
        "predicates": predicates,
        "predicate_errors": predicate_errors,
        "option_checks": option_checks,
        "removed_oids": {
            "id": invalid["id"],
            "sql": invalid["sql"],
            "sqlstate": "42601",
        },
        "identity": identity,
        "rls_sqlstate": "0A000",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit("COPY input PostgreSQL witness drift")
        print(
            f"verified three original COPY input codecs, {len(result['predicates'])} WHERE profiles, "
            f"{sum(len(e['faults']) for e in result['entries'] + result['advanced'] + result['predicates'])} atomic-failure postimages "
            f"and {len(result['option_checks'])} option contracts"
        )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    else:
        print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
