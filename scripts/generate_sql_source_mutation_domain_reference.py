#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Independent PostgreSQL adjudication of original typed mutation domains.

Run with uv run --no-project --with 'psycopg[binary]==3.3.6' python ...
No altered TEXT schemas, placeholder UUID coercions or skipped setup errors
qualify these originals. Later expressions are deliberately not certified by
an earlier input-domain diagnostic.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path.cwd() / "scripts"))
from generate_sql_postgres_reference import postgres, ROOT, encoded

CASES = [1403, 1468, 1469, 1476]
SOURCE = "zig/pkg/antfly/src/sql/fixtures/sql_api_parity_source_corpus.json"


def image(db, sql):
    cursor = db.execute(sql)
    rows = cursor.fetchall()
    return dict(
        columns=[c.name for c in cursor.description],
        column_oids=[c.type_code for c in cursor.description],
        rows=[[encoded(v) for v in row] for row in rows],
        sql_nulls=[[v is None for v in row] for row in rows],
    )


def controls(number):
    key = "00000000-0000-4000-8000-000000000001"
    proposed = "00000000-0000-4000-8000-000000000002"
    if number == 1476:
        seed = f"INSERT INTO usage_records(id,request_id) VALUES ('{key}','before')"
        read = "SELECT id,request_id FROM usage_records ORDER BY id"
        control = f"INSERT INTO usage_records(id,request_id) VALUES ('{key}','proposed') ON CONFLICT(id) DO UPDATE SET request_id='control-warm' RETURNING id,request_id"
        cold = control.replace("control-warm", "control-cold")
    else:
        temporal = number in (1468, 1469)
        seed_value = "TIMESTAMPTZ '2025-01-01T00:00:00Z'" if temporal else "10"
        warm_value = "TIMESTAMPTZ '2025-01-01T01:00:00Z'" if temporal else "11"
        cold_value = "TIMESTAMPTZ '2025-01-01T02:00:00Z'" if temporal else "12"
        seed = f"INSERT INTO usage_records(id,email,updated_at_ns) VALUES ('{key}','a@example.test',{seed_value})"
        read = "SELECT id,email,updated_at_ns FROM usage_records ORDER BY id"
        control = f"INSERT INTO usage_records(id,email,updated_at_ns) VALUES ('{proposed}','a@example.test',{warm_value}) ON CONFLICT(email) DO UPDATE SET updated_at_ns=excluded.updated_at_ns RETURNING id,email,updated_at_ns"
        cold = control.replace(warm_value, cold_value)
    return seed, read, control, cold


def reference():
    import psycopg

    inventory = json.loads(
        (
            ROOT / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_parity_inventory.json"
        ).read_text()
    )
    raw = subprocess.check_output(
        ["git", "show", f"{inventory['source_commit']}:{SOURCE}"], cwd=ROOT
    )
    if hashlib.sha256(raw).hexdigest() != inventory["source_sha256"]:
        raise RuntimeError("original mutation source hash changed")
    originals = json.loads(raw)["entries"]
    results = []
    with postgres() as db:
        for number in CASES:
            entry = originals[number - 1]
            item = inventory["entries"][number - 1]
            if item["sql"] != entry["sql"]:
                raise RuntimeError("original statement identity changed")
            db.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
            setup = []
            for sql in entry["apply_setup_sql"]:
                try:
                    db.execute(sql)
                    state = None
                except psycopg.Error as error:
                    state = error.sqlstate
                setup.append(dict(sql=sql, sqlstate=state))
                if state is not None:
                    break
            seed, read, control, cold_control = controls(number)
            db.execute(seed)
            before = image(db, read)
            try:
                db.execute(entry["sql"])
            except psycopg.Error as error:
                state = error.sqlstate
            else:
                raise RuntimeError(f"original {item['id']} unexpectedly succeeded")
            if state != "22P02":
                raise RuntimeError(
                    f"original {item['id']} changed UUID diagnostic: {state}"
                )
            if image(db, read) != before:
                raise RuntimeError("invalid original mutation changed rows")
            returned = image(db, control)
            after = image(db, read)
            cold_returned = image(db, cold_control)
            cold_after = image(db, read)
            results.append(
                dict(
                    id=item["id"],
                    source_entry_sha256=item["source_entry_sha256"],
                    namespace=f"domain_{number}",
                    setup=setup,
                    sql=entry["sql"],
                    sqlstate=state,
                    seed=seed,
                    read=read,
                    before=before,
                    control=control,
                    returned=returned,
                    after=after,
                    cold_control=cold_control,
                    cold_returned=cold_returned,
                    cold_after=cold_after,
                )
            )
    return dict(
        source_commit=inventory["source_commit"],
        source_sha256=inventory["source_sha256"],
        entries=results,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = reference()
    if args.check:
        if json.loads(args.check.read_text()) != result:
            raise SystemExit(
                f"PostgreSQL source-domain reference differs: {args.check}"
            )
    elif args.output:
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    else:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
