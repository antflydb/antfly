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

# The genuine upstream registration callback API is exercised through a native
# test provider with independently implemented versioned antfly relabel policy.
# It makes no SELinux/MAC claim and never edits originals or implementation counts.
from pathlib import Path
import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
from tempfile import TemporaryDirectory

parser = argparse.ArgumentParser(
    description="PostgreSQL18+ SECURITY LABEL originals and real native provider lifecycle oracle. The test provider is antfly relabel policy, never SELinux."
)
parser.add_argument("--repo-root", type=Path, default=Path.cwd())
parser.add_argument("--provider-library", type=Path)
parser.add_argument("--output", type=Path)
args = parser.parse_args()
root = args.repo_root.resolve()
sys.path.insert(0, str(root / "scripts"))
from generate_sql_postgres_reference import postgres, postgres_binary_dir
import psycopg
from psycopg import sql

if args.provider_library is None:
    provider_dir = TemporaryDirectory(prefix="antfly-sql-label-provider-")
    args.provider_library = Path(provider_dir.name) / "antfly_security_label_provider.so"
    pg_config = postgres_binary_dir() / "pg_config"
    includes = [
        subprocess.check_output([pg_config, option], text=True).strip()
        for option in ("--includedir-server", "--includedir")
    ]
    cppflags = shlex.split(
        subprocess.check_output([pg_config, "--cppflags"], text=True)
    )
    link = (
        ["-bundle", "-undefined", "dynamic_lookup"]
        if sys.platform == "darwin"
        else ["-shared", "-fPIC"]
    )
    subprocess.run(
        [
            os.environ.get("CC", "cc"),
            *link,
            *cppflags,
            *(f"-I{directory}" for directory in includes),
            "-o",
            args.provider_library,
            root / "scripts/fixtures/antfly_security_label_provider.c",
        ],
        check=True,
    )

inventory = json.loads(
    (
        root / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_parity_inventory.json"
    ).read_text()
)
entries = (
    inventory
    if isinstance(inventory, list)
    else inventory.get("cases", inventory.get("entries", []))
)
originals = [
    dict(row, statement_sha256=hashlib.sha256(row["sql"].encode()).hexdigest())
    for row in entries
    if row["id"] in {"sql-0153", "sql-0154", "sql-0155"}
]
assert {row["id"] for row in originals} == {"sql-0153", "sql-0154", "sql-0155"}
result = {
    "oracle": "PostgreSQL 18 real executable antfly native provider",
    "provider_claim": "relabel validation and restricted-transition authorization; no SELinux or MAC row policy",
    "before_load": [],
    "loaded_originals": [],
    "lifecycle": [],
}
with postgres() as db:
    result["postgres_version"] = db.execute("SELECT version()").fetchone()[0]
    db.execute("CREATE TABLE usage_records(id text,status text)")
    db.execute("INSERT INTO usage_records VALUES ('u1','before'),('u9','untouched')")
    original_rows = db.execute(
        "SELECT xmin::text,ctid::text,id,status FROM usage_records ORDER BY id"
    ).fetchall()
    db.execute("CREATE PROCEDURE rotate_usage() LANGUAGE SQL AS 'SELECT 1'")
    for query in (
        "COMMENT ON TABLE usage_records IS 'table sentinel'",
        "COMMENT ON SCHEMA public IS 'schema sentinel'",
        "COMMENT ON PROCEDURE rotate_usage() IS 'procedure sentinel'",
    ):
        db.execute(query)

    def observe(query):
        try:
            cursor = db.execute(query)
            return {"status": "ok", "command_tag": cursor.statusmessage}
        except psycopg.Error as e:
            return {
                "status": "error",
                "sqlstate": e.sqlstate,
                "message": e.diag.message_primary,
            }

    for row in originals:
        out = observe(row["sql"])
        assert out["sqlstate"] == "22023", out
        result["before_load"].append(
            {
                "id": row["id"],
                "sql": row["sql"],
                "source_entry_sha256": row["source_entry_sha256"],
                "statement_sha256": row["statement_sha256"],
                "outcome": out,
            }
        )
    provider = args.provider_library.resolve()
    result["provider_module_sha256"] = hashlib.sha256(provider.read_bytes()).hexdigest()
    db.execute(sql.SQL("LOAD {}").format(sql.Literal(str(provider))))
    db.execute("SECURITY LABEL ON PROCEDURE rotate_usage() IS 'public'")
    for row in originals:
        out = observe(row["sql"])
        assert out["status"] == ("error" if row["id"] == "sql-0153" else "ok"), out
        if out["status"] == "error":
            assert out["sqlstate"] == "22023"
        else:
            assert out["command_tag"] == "SECURITY LABEL"
        assert (
            db.execute(
                "SELECT xmin::text,ctid::text,id,status FROM usage_records ORDER BY id"
            ).fetchall()
            == original_rows
        )
        result["loaded_originals"].append(
            {
                "id": row["id"],
                "sql": row["sql"],
                "statement_sha256": row["statement_sha256"],
                "outcome": out,
            }
        )

    def label(oid, klass):
        return db.execute(
            "SELECT provider,label FROM pg_seclabel WHERE objoid=%s AND classoid=%s::regclass AND objsubid=0 ORDER BY provider",
            (oid, klass),
        ).fetchall()

    result["original_heap_postimage"] = {
        "columns": ["xmin", "ctid", "id", "status"],
        "rows": original_rows,
        "unchanged_after_originals": True,
    }
    original_schema_oid = db.execute("SELECT 'public'::regnamespace::oid").fetchone()[0]
    original_procedure_oid = db.execute(
        "SELECT 'rotate_usage()'::regprocedure::oid"
    ).fetchone()[0]
    assert label(original_schema_oid, "pg_namespace") == [("antfly", "internal")]
    assert label(original_procedure_oid, "pg_proc") == []

    table_oid = db.execute("SELECT 'usage_records'::regclass::oid").fetchone()[0]
    db.execute("SECURITY LABEL FOR antfly ON TABLE usage_records IS 'internal'")
    assert label(table_oid, "pg_class") == [("antfly", "internal")]
    db.execute("BEGIN")
    db.execute("SECURITY LABEL ON TABLE usage_records IS 'public'")
    db.execute("ROLLBACK")
    assert label(table_oid, "pg_class") == [("antfly", "internal")]
    assert (
        db.execute(
            "SELECT xmin::text,ctid::text,id,status FROM usage_records ORDER BY id"
        ).fetchall()
        == original_rows
    )
    db.execute("ALTER TABLE usage_records RENAME TO renamed_usage_records")
    assert (
        db.execute("SELECT 'renamed_usage_records'::regclass::oid").fetchone()[0]
        == table_oid
    )
    assert label(table_oid, "pg_class") == [("antfly", "internal")]
    db.execute("DROP TABLE renamed_usage_records")
    db.execute("CREATE TABLE renamed_usage_records(id integer)")
    assert (
        db.execute("SELECT 'renamed_usage_records'::regclass::oid").fetchone()[0]
        != table_oid
    )
    assert label(table_oid, "pg_class") == []
    result["lifecycle"].append(
        "table rename retains identity/label; transaction rollback preserves predecessor; drop/recreate has no inherited label"
    )
    db.execute("CREATE SCHEMA label_namespace")
    schema_oid = db.execute("SELECT 'label_namespace'::regnamespace::oid").fetchone()[0]
    db.execute("SECURITY LABEL ON SCHEMA label_namespace IS 'internal'")
    db.execute("ALTER SCHEMA label_namespace RENAME TO renamed_namespace")
    assert label(schema_oid, "pg_namespace") == [("antfly", "internal")]
    db.execute("DROP SCHEMA renamed_namespace")
    assert label(schema_oid, "pg_namespace") == []
    result["lifecycle"].append(
        "schema rename retains labels; schema drop retires identity labels"
    )
    db.execute("CREATE PROCEDURE rotate_usage(integer) LANGUAGE SQL AS 'SELECT 1'")
    proc_oid = db.execute("SELECT 'rotate_usage()'::regprocedure::oid").fetchone()[0]
    overload_oid = db.execute(
        "SELECT 'rotate_usage(integer)'::regprocedure::oid"
    ).fetchone()[0]
    db.execute("SECURITY LABEL ON PROCEDURE rotate_usage() IS 'internal'")
    db.execute("SECURITY LABEL ON PROCEDURE rotate_usage(integer) IS 'public'")
    assert label(proc_oid, "pg_proc") == [("antfly", "internal")] and label(
        overload_oid, "pg_proc"
    ) == [("antfly", "public")]
    db.execute("ALTER PROCEDURE rotate_usage() RENAME TO renamed_procedure")
    assert label(proc_oid, "pg_proc") == [("antfly", "internal")]
    db.execute("DROP PROCEDURE renamed_procedure()")
    assert label(proc_oid, "pg_proc") == [] and label(overload_oid, "pg_proc") == [
        ("antfly", "public")
    ]
    db.execute("SECURITY LABEL ON PROCEDURE rotate_usage(integer) IS NULL")
    assert label(overload_oid, "pg_proc") == []
    result["lifecycle"].append(
        "overloaded procedures isolate labels, rename retains identity, DROP and NULL remove only selected identity/provider labels"
    )
    db.execute("CREATE ROLE fixture_owner")
    db.execute("CREATE ROLE fixture_other")
    db.execute("CREATE TABLE owned_label(id integer)")
    db.execute("ALTER TABLE owned_label OWNER TO fixture_owner")
    db.execute("SET ROLE fixture_owner")
    assert (
        observe("SECURITY LABEL ON TABLE owned_label IS 'internal'")["status"] == "ok"
    )
    out = observe("SECURITY LABEL ON TABLE owned_label IS 'restricted'")
    assert out["sqlstate"] == "42501"
    out = observe("SECURITY LABEL ON TABLE owned_label IS 'arbitrary'")
    assert out["sqlstate"] == "22023"
    db.execute("RESET ROLE")
    db.execute("SET ROLE fixture_other")
    out = observe("SECURITY LABEL ON TABLE owned_label IS 'internal'")
    assert out["sqlstate"] == "42501"
    db.execute("RESET ROLE")
    db.execute("SECURITY LABEL ON TABLE owned_label IS 'restricted'")
    db.execute("SET ROLE fixture_owner")
    out = observe("SECURITY LABEL ON TABLE owned_label IS NULL")
    assert out["sqlstate"] == "42501"
    db.execute("RESET ROLE")
    db.execute("SECURITY LABEL ON TABLE owned_label IS NULL")
    result["lifecycle"].append(
        "ownership checked before executable provider; provider vetoes invalid and restricted labels and NULL removal from restricted label"
    )
    assert (
        db.execute(
            "SELECT obj_description('public'::regnamespace,'pg_namespace')"
        ).fetchone()[0]
        == "schema sentinel"
    )
    result["final_labels"] = db.execute(
        "SELECT classoid::regclass::text,objoid::text,objsubid,provider,label FROM pg_seclabel ORDER BY 1,2,3,4"
    ).fetchall()
if args.output is not None:
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
print(
    json.dumps(
        {
            "originals": len(result["loaded_originals"]),
            "lifecycle_witnesses": len(result["lifecycle"]),
            "postgres": result["postgres_version"],
        }
    )
)
