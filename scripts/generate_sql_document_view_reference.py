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

"""Canonical PostgreSQL view oracle for the declared keyword projection.

Run with uv run --no-project --with 'psycopg[binary]==3.3.6' python SCRIPT.
This validates typed document projection semantics, not Antfly's managed index
ownership policy: PostgreSQL itself has no equivalent required-index view clause.
The immutable original SELECT executes unchanged on warm and cold connections.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from generate_sql_postgres_reference import FIXTURES, postgres


CASES = [
    ("string", {"metadata": {"plan": "pro"}}, [["doc:a", "pro"]]),
    ("other_string", {"metadata": {"plan": "free"}}, []),
    (
        "literal_dotted_key",
        {"metadata": {"plan": "pro"}, "metadata.plan": 17},
        [["doc:a", "pro"]],
    ),
    ("missing", {}, []),
    ("missing_child", {"metadata": {}}, []),
    ("null_parent", {"metadata": None}, []),
    ("scalar_parent", {"metadata": 17}, []),
    ("json_null", {"metadata": {"plan": None}}, []),
    ("number", {"metadata": {"plan": 17}}, "42804"),
    ("object", {"metadata": {"plan": {}}}, "42804"),
    ("array", {"metadata": {"plan": []}}, "42804"),
    ("boolean", {"metadata": {"plan": True}}, "42804"),
]


def reference():
    import psycopg

    if psycopg.__version__ != "3.3.6":
        raise RuntimeError("use the pinned psycopg[binary]==3.3.6 oracle environment")
    inventory = json.loads((FIXTURES / "sql_parity_inventory.json").read_text())
    source = subprocess.check_output(
        ["git", "show", inventory["source_commit"] + ":" + inventory["source_path"]]
    )
    if hashlib.sha256(source).hexdigest() != inventory["source_sha256"]:
        raise RuntimeError("immutable source corpus digest mismatch")
    entry = json.loads(source)["entries"][853]
    original = entry["sql"]
    if original != "SELECT _id, plan FROM support_view WHERE plan = 'pro' LIMIT 10":
        raise RuntimeError("unexpected immutable original SQL")

    def checks(db):
        observations = []
        for label, body, expected in CASES:
            db.execute("TRUNCATE docs")
            db.execute(
                "INSERT INTO docs VALUES ('doc:a',%s::jsonb)", (json.dumps(body),)
            )
            try:
                rows = [list(row) for row in db.execute(original).fetchall()]
            except psycopg.Error as error:
                if error.sqlstate != expected:
                    raise AssertionError((label, expected, error.sqlstate)) from error
                demands = []
                for statement, demanded in [
                    (
                        "SELECT CASE WHEN _id='doc:a' THEN 'kept' ELSE plan END FROM support_view",
                        False,
                    ),
                    ("SELECT COALESCE('kept',plan) FROM support_view", False),
                    (
                        "WITH q AS (SELECT _id,plan FROM support_view) SELECT CASE WHEN _id='doc:a' THEN 'kept' ELSE plan END FROM q",
                        False,
                    ),
                    (
                        "SELECT CASE WHEN a._id='doc:a' THEN 'kept' ELSE b.plan END FROM support_view a JOIN support_view b ON a._id=b._id",
                        False,
                    ),
                    (
                        "SELECT CASE WHEN _id='never' THEN 'kept' ELSE plan END FROM support_view",
                        True,
                    ),
                    (
                        "SELECT COALESCE(CAST(NULL AS TEXT),plan) FROM support_view",
                        True,
                    ),
                ]:
                    try:
                        actual = [list(row) for row in db.execute(statement).fetchall()]
                    except psycopg.Error as cause:
                        if not demanded or cause.sqlstate != "42804":
                            raise AssertionError(
                                (label, statement, demanded, cause.sqlstate)
                            ) from cause
                        demands.append({"sql": statement, "sqlstate": cause.sqlstate})
                    else:
                        if demanded or actual != [["kept"]]:
                            raise AssertionError((label, statement, demanded, actual))
                        demands.append({"sql": statement, "rows": actual})
                observations.append(
                    {"case": label, "sqlstate": error.sqlstate, "demands": demands}
                )
            else:
                if rows != expected:
                    raise AssertionError((label, expected, rows))
                observations.append({"case": label, "rows": rows})
        return observations

    with postgres() as db:
        version = db.execute("SELECT version()").fetchone()[0]
        db.execute("CREATE TABLE docs(_id text PRIMARY KEY, document jsonb NOT NULL)")
        # This is an explicit immutable typed projection. JSON ->> alone would
        # stringify numbers/objects and is not the declared keyword contract.
        db.execute("""CREATE FUNCTION keyword_projection(value jsonb) RETURNS text
          LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN
            IF value IS NULL OR jsonb_typeof(value) = 'null' THEN RETURN NULL; END IF;
            IF jsonb_typeof(value) <> 'string' THEN
              RAISE EXCEPTION 'keyword projection requires JSON string' USING ERRCODE='42804';
            END IF;
            RETURN value #>> '{}';
          END $$""")
        db.execute(
            "CREATE VIEW support_view AS SELECT _id,keyword_projection(document #> '{metadata,plan}') AS plan FROM docs"
        )
        warm = checks(db)
        with psycopg.connect(db.info.dsn, autocommit=True) as cold:
            cold.execute("SET statement_timeout='2s'")
            cold_observations = checks(cold)
        if warm != cold_observations:
            raise AssertionError("warm/cold view projection drift")
        result = {
            "source_case": "sql-0854",
            "source_sha256": inventory["source_sha256"],
            "original_sql": original,
            "projection_contract": "literal metadata.plan keyword; JSON string or SQL NULL; other leaf types 42804",
            "index_policy_scope": "not modeled by PostgreSQL; native managed-index custody qualification is separate",
            "postgres_version": version,
            "warm": warm,
            "cold": cold_observations,
        }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    actual = reference()
    if args.check:
        expected = json.loads(args.output.read_text())
        expected.pop("postgres_version", None)
        actual.pop("postgres_version", None)
        if expected != actual:
            raise SystemExit("document view projection reference drift")
    else:
        args.output.write_text(json.dumps(actual, indent=2) + "\n")
    print(
        "sql-0854 unchanged SELECT typed canonical VIEW warm/cold PASS; owned cluster stopped"
    )


if __name__ == "__main__":
    main()
