#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Qualify exact SUPPORT admission and wrong-signature controls in PostgreSQL 18+."""
import argparse
import json
from pathlib import Path

from generate_sql_postgres_reference import FIXTURES, postgres
from generate_sql_document_alias_reference import output


def reference():
    import psycopg

    original = next(item for item in json.loads((FIXTURES / "sql_parity_inventory.json").read_text())["entries"] if item["id"] == "sql-0394")
    bootstrap = ["CREATE FUNCTION support_sentinel() RETURNS text LANGUAGE sql AS $$SELECT 'kept'$$"]
    setup = [
        "CREATE FUNCTION audit_support(text) RETURNS text LANGUAGE sql AS $$SELECT lower($1)$$",
        "CREATE SCHEMA helper_scope",
        "CREATE FUNCTION helper_scope.audit_support(text) RETURNS text LANGUAGE sql AS $$SELECT lower($1)$$",
    ]
    probes = [
        (original["sql"], "42883"),
        ("CREATE FUNCTION support_audit() RETURNS trigger LANGUAGE plpgsql SUPPORT audit_support AS $$BEGIN RETURN NEW; END$$", "42883"),
        ("CREATE FUNCTION support_audit() RETURNS trigger LANGUAGE plpgsql SUPPORT helper_scope.audit_support", "42883"),
        ("CREATE FUNCTION support_audit() RETURNS trigger LANGUAGE plpgsql SUPPORT helper_scope.absent_support", "42883"),
        ("CREATE FUNCTION support_audit() RETURNS trigger LANGUAGE plpgsql SUPPORT absent_scope.audit_support", "42883"),
    ]
    query = "SELECT support_sentinel() AS sentinel,audit_support('KePt') AS value,helper_scope.audit_support('AlSo KePt') AS qualified_value"
    with postgres() as db:
        def absent():
            assert db.execute("SELECT to_regprocedure('public.support_audit()') IS NULL").fetchone() == (True,)

        def rejected(statement, code):
            try:
                db.execute(statement)
            except psycopg.Error as error:
                assert error.sqlstate == code, str(error)
            else:
                raise AssertionError("PostgreSQL accepted the invalid SUPPORT declaration")
            absent()

        for statement in bootstrap:
            db.execute(statement)
        rejected(original["sql"], "42883")
        for statement in setup:
            db.execute(statement)
        before = db.execute("SELECT oid,xmin::text,prosrc FROM pg_proc WHERE proname='audit_support' ORDER BY oid").fetchall()
        expected = output(db.execute(query))
        results = []
        for statement, code in probes:
            rejected(statement, code)
            assert output(db.execute(query)) == expected
            assert db.execute("SELECT oid,xmin::text,prosrc FROM pg_proc WHERE proname='audit_support' ORDER BY oid").fetchall() == before
            results.append({"sql": statement, "sqlstate": code})
        db.execute("SET search_path=helper_scope,public")
        rejected(original["sql"], "42883")
        assert db.execute("SELECT to_regprocedure('helper_scope.support_audit()') IS NULL").fetchone() == (True,)
        assert output(db.execute(query)) == expected
    return {
        "format": 1,
        "reference": "PostgreSQL exact SUPPORT catalog admission",
        "original": {**{key: original[key] for key in ("id", "source_entry_sha256", "sql")}, "sqlstate": "42883"},
        "bootstrap": bootstrap,
        "setup": setup,
        "probes": results,
        "query": query,
        "output": expected,
        "target_absent": True,
        "preserved_on_refusal": True,
        "search_path": "helper_scope,public",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--output", type=Path)
    target.add_argument("--check", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        assert json.loads(args.check.read_text()) == result, "PostgreSQL SUPPORT reference drift"
        print("verified exact SUPPORT original, wrong-signature/path controls and preserved functions")


if __name__ == "__main__":
    main()
