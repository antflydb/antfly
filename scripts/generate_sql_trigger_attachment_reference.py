#!/usr/bin/env python3
"""Independent PostgreSQL witnesses for unchanged table-trigger declarations.

The source corpus did not declare touch_updated_at. This campaign declares an
ordinary PL/pgSQL function explicitly; there is no function-name special case.
Its local NEW assignment demonstrates why the original omitted FOR EACH ROW
must retain PostgreSQL's statement semantics rather than become an update rule.
"""

from pathlib import Path
import argparse
import difflib
import json
import subprocess
from generate_sql_postgres_reference import postgres

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "79644dfa1605e8da0f486d021d1c1393577d6265"
FIXTURE = (
    ROOT
    / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_trigger_attachment_reference.json"
)


def statement_scope_witness(db, notices):
    db.execute("CREATE TABLE trigger_scope_rows(id INT PRIMARY KEY, n INT, m INT)")
    db.execute("INSERT INTO trigger_scope_rows VALUES (1,1,1),(2,2,2)")
    for timing in ("BEFORE", "AFTER"):
        for event in ("INSERT", "UPDATE", "DELETE"):
            name = f"{timing.lower()}_{event.lower()}"
            db.execute(
                f"CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE NOTICE '{timing} {event}'; RETURN NULL; END$$"
            )
            db.execute(
                f"CREATE TRIGGER {name} {timing} {event} ON trigger_scope_rows EXECUTE FUNCTION {name}()"
            )
    witnesses = {}
    queries = {
        "same_event_ctes": "WITH x AS (UPDATE trigger_scope_rows SET n=n+1 WHERE id=1 RETURNING *) UPDATE trigger_scope_rows SET n=n+1 WHERE id=2",
        "zero_row_ctes": "WITH x AS (UPDATE trigger_scope_rows SET n=n+1 WHERE id=99 RETURNING *) UPDATE trigger_scope_rows SET n=n+1 WHERE id=98",
        "mixed_events": "WITH x AS (UPDATE trigger_scope_rows SET n=n+1 WHERE id=1 RETURNING *) INSERT INTO trigger_scope_rows VALUES (3,3,3)",
        "dependent_events": "WITH x AS (UPDATE trigger_scope_rows SET n=n+1 WHERE id=1 RETURNING n) INSERT INTO trigger_scope_rows SELECT 4,n,4 FROM x",
    }
    for key, query in queries.items():
        notices.clear()
        db.execute(query)
        witnesses[key] = {
            "sql": query,
            "notices": list(notices),
            "rows": db.execute("TABLE trigger_scope_rows ORDER BY id").fetchall(),
        }
    db.execute("DROP TRIGGER before_update ON trigger_scope_rows")
    db.execute("DROP TRIGGER after_update ON trigger_scope_rows")
    db.execute(
        "CREATE TRIGGER before_update BEFORE UPDATE OF n ON trigger_scope_rows EXECUTE FUNCTION before_update()"
    )
    db.execute(
        "CREATE TRIGGER after_update AFTER UPDATE OF n ON trigger_scope_rows EXECUTE FUNCTION after_update()"
    )
    for key, query in {
        "update_of_last_only": "WITH x AS (UPDATE trigger_scope_rows SET n=n+1 WHERE id=1 RETURNING *) UPDATE trigger_scope_rows SET m=m+1 WHERE id=2",
        "update_of_first_only": "WITH x AS (UPDATE trigger_scope_rows SET m=m+1 WHERE id=1 RETURNING *) UPDATE trigger_scope_rows SET n=n+1 WHERE id=2",
    }.items():
        notices.clear()
        db.execute(query)
        witnesses[key] = {
            "sql": query,
            "notices": list(notices),
            "rows": db.execute("TABLE trigger_scope_rows ORDER BY id").fetchall(),
        }
    return witnesses


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    destination = parser.add_mutually_exclusive_group()
    destination.add_argument(
        "--check",
        type=Path,
        nargs="?",
        const=FIXTURE,
        help="verify PostgreSQL evidence without modifying the fixture",
    )
    destination.add_argument(
        "--output",
        type=Path,
        default=FIXTURE,
        help="write independently generated PostgreSQL evidence",
    )
    args = parser.parse_args()
    corpus = json.loads(
        subprocess.check_output(
            [
                "git",
                "show",
                f"{SOURCE}:zig/pkg/antfly/src/sql/fixtures/sql_api_parity_source_corpus.json",
            ],
            cwd=ROOT,
        )
    )["entries"]
    ids = ["sql-0751", "sql-0752", "sql-0753"]
    sources = {case_id: corpus[int(case_id[4:]) - 1]["sql"] for case_id in ids}
    notices = []
    evidence = {}
    with postgres() as db:
        db.add_notice_handler(lambda notice: notices.append(notice.message_primary))
        version = db.execute("SHOW server_version").fetchone()[0]
        db.execute(
            "CREATE TABLE usage_records(id UUID PRIMARY KEY, updated_at_ns BIGINT)"
        )
        db.execute("""CREATE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at_ns := 42;
  PERFORM TG_ARGV[0];
  RAISE NOTICE 'trigger fired';
  RETURN NEW;
END$$""")
        db.execute(
            "INSERT INTO usage_records VALUES ('00000000-0000-0000-0000-000000000001',1), ('00000000-0000-0000-0000-000000000002',2)"
        )
        prior_identity = None
        for case_id in ids:
            notices.clear()
            db.execute(sources[case_id])
            trigger = db.execute(
                "SELECT oid,tgfoid,tgtype,tgnargs FROM pg_trigger WHERE tgname='update_timestamp' AND tgrelid='usage_records'::regclass"
            ).fetchone()
            replaced_identity = (
                prior_identity is not None
                and trigger is not None
                and prior_identity == trigger[:2]
            )
            if trigger is not None:
                prior_identity = trigger[:2]
            db.execute("UPDATE usage_records SET updated_at_ns = updated_at_ns + 10")
            rows = db.execute(
                "SELECT updated_at_ns FROM usage_records ORDER BY id"
            ).fetchall()
            evidence[case_id] = {
                "sql": sources[case_id],
                "statement_level": trigger is not None and not bool(trigger[2] & 1),
                "trigger_present": trigger is not None,
                "trigger_arguments": trigger[3] if trigger else None,
                "replacement_kept_identity": replaced_identity,
                "notices": list(notices),
                "rows": rows,
            }
        when = {}
        for expression in (
            "true",
            "false",
            "TG_OP = 'INSERT'",
            "NEW.updated_at_ns > 0",
        ):
            try:
                db.execute(
                    f"CREATE TRIGGER when_probe BEFORE INSERT ON usage_records WHEN ({expression}) EXECUTE FUNCTION touch_updated_at()"
                )
                when[expression] = {"accepted": True}
                db.execute("DROP TRIGGER when_probe ON usage_records")
            except Exception as error:
                when[expression] = {
                    "accepted": False,
                    "sqlstate": error.sqlstate,
                    "message": error.diag.message_primary,
                }
        statement_scope = statement_scope_witness(db, notices)
    actual = (
        json.dumps(
            {
                "postgres_version": version,
                "source_commit": SOURCE,
                "function_setup": "Explicit ordinary PL/pgSQL declaration, not source-corpus native update-policy fallback",
                "cases": evidence,
                "statement_when": when,
                "statement_scope": statement_scope,
            },
            indent=2,
        )
        + "\n"
    )
    if args.check is not None:
        expected = json.dumps(json.loads(args.check.read_text()), indent=2) + "\n"
        if actual != expected:
            difference = "".join(
                difflib.unified_diff(
                    expected.splitlines(keepends=True),
                    actual.splitlines(keepends=True),
                    fromfile=str(args.check),
                    tofile="PostgreSQL observation",
                )
            )
            raise SystemExit(difference)
        print(
            f"PostgreSQL trigger attachment and statement-scope evidence matches {args.check}"
        )
    else:
        args.output.write_text(actual)


if __name__ == "__main__":
    main()
