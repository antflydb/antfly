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

"""Capture Codex SQLite sources and emit complete, schema-pinned migration plans.

This prepares plans; it never changes Codex configuration or runs a cutover.
The profile belongs to the Codex integration contract, not the generic runner.
"""

import argparse
import hashlib
import json
import re
import sqlite3
import subprocess
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

PINNED_SECTION = "01984de2-8f74-7c91-a3b2-5c5e937cf318"
SOURCE_FILES = {
    "state": "state_5.sqlite",
    "goals": "goals_1.sqlite",
    "queue": "queue_1.sqlite",
    "memories_v1": "memories_1.sqlite",
    "memories_v2": "memories_v2_1.sqlite",
    "history": "thread_history_1.sqlite",
    "board": "agent_message_board_1.sqlite",
}
HISTORY_TABLES = {
    "codex_thread_turns",
    "codex_thread_items",
    "codex_thread_realtime_items",
    "codex_thread_history_projection_state",
}
HISTORY_OWNED_THREAD_COLUMNS = {
    "extra",
    "next_ordinal",
    "forked_from_id",
    "history_base_thread_id",
    "history_base_end_ordinal",
    "subagent_history_start_ordinal",
    "multi_agent_version",
}
BOARD_NAMES = {
    "codex_message_board_tombstones": "deleted_boards",
    "codex_message_board_channels": "channels",
    "codex_message_board_posts": "posts",
    "codex_message_board_subscriptions": "subscriptions",
    "codex_message_board_subscription_opt_outs": "subscription_opt_outs",
}
KINDS = {"TEXT": "text", "BIGINT": "integer", "BOOLEAN": "boolean", "JSONB": "json"}


def quote(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name):
        raise ValueError(f"unsupported identifier: {name!r}")
    return '"' + name + '"'


def readonly(path):
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=30)


def rust_strings(text):
    # The pinned schema uses ordinary Rust strings, including multiline SQL
    # and escaped line continuations. Comments are not SQL inputs.
    text = re.sub(r"(?m)^\s*//[^\n]*", "", text)
    for match in re.finditer(r'"((?:\\[\s\S]|[^"\\])*)"', text):
        body = re.sub(r"\\\r?\n\s*", "", match.group(1))
        yield json.loads(
            '"'
            + body.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
            + '"'
        )


def split_fields(body):
    fields, start, depth, quoted = [], 0, 0, False
    for index, ch in enumerate(body):
        if ch == "'":
            quoted = not quoted
        if quoted:
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            fields.append(body[start:index].strip())
            start = index + 1
    fields.append(body[start:].strip())
    return fields


def schema_contract(codex_repo):
    path = codex_repo / "codex-rs/antfly/src/schema.rs"
    text = path.read_text()
    expected = json.loads((Path(__file__).parent / "schema-contract.json").read_text())
    actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual_hash != expected["schema_sha256"]:
        raise ValueError(
            "Codex target schema changed; review and refresh the pinned schema contract before generating a plan"
        )
    section = text.split("pub const MIGRATIONS:", 1)[1].split(
        "const MIGRATIONS_TABLE:", 1
    )[0]
    statements = [
        s
        for s in rust_strings(section)
        if s.startswith(("CREATE ", "ALTER ", "INSERT "))
    ]
    migration_table = next(
        s
        for s in rust_strings(text.split("const MIGRATIONS_TABLE:", 1)[1])
        if s.startswith("CREATE TABLE")
    )
    migrations = [
        (int(v), n)
        for v, n in re.findall(r'version:\s*(\d+),\s*name:\s*"([^"\n]+)"', section)
    ]
    if not migrations or not statements:
        raise ValueError(
            "Codex schema format changed; export/review the authoritative schema"
        )
    tables = {}
    for statement in statements + [migration_table]:
        match = re.match(
            r"CREATE TABLE(?: IF NOT EXISTS)? (\w+)\s*\(([\s\S]*)\)$", statement
        )
        if not match:
            continue
        name, body = match.groups()
        columns, key = [], []
        for field in split_fields(body):
            if field.startswith("PRIMARY KEY"):
                key = [
                    s.strip()
                    for s in field[field.index("(") + 1 : field.rindex(")")].split(",")
                ]
                continue
            if field.startswith(("UNIQUE", "CHECK", "FOREIGN KEY", "CONSTRAINT")):
                continue
            column = re.match(r"(\w+) (TEXT|BIGINT|BOOLEAN|JSONB)\b([\s\S]*)", field)
            if not column:
                raise ValueError(
                    f"unsupported target column declaration: {name}: {field}"
                )
            col, kind, tail = column.groups()
            if "PRIMARY KEY" in tail:
                key = [col]
            columns.append(
                {"name": col, "type": KINDS[kind], "nullable": "NOT NULL" not in tail}
            )
        tables[name] = {"columns": columns, "key": key}
    # Apply the integration's currently supported ADD COLUMN migrations to
    # the exported final catalog without inventing a second target schema.
    for statement in statements:
        if statement.startswith("ALTER "):
            match = re.fullmatch(
                r"ALTER TABLE (\w+) ADD COLUMN (\w+) (TEXT|BIGINT|BOOLEAN|JSONB)",
                statement,
            )
            if not match:
                raise ValueError(f"unsupported schema alteration: {statement}")
            table, col, kind = match.groups()
            tables[table]["columns"].append(
                {"name": col, "type": KINDS[kind], "nullable": True}
            )
    for table in tables.values():
        if not table["key"]:
            raise ValueError("every target table must declare a primary key")
        for col in table["columns"]:
            if col["name"] in table["key"]:
                col["nullable"] = False
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=codex_repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {
        "codex_commit": commit,
        "schema_sha256": actual_hash,
        "migrations": [{"version": v, "name": n} for v, n in migrations],
        "statements": statements,
        "migration_table": migration_table,
        "tables": tables,
    }


def inventory(path):
    with closing(readonly(path)) as db:
        db.execute("BEGIN")
        result = {}
        for name, kind in db.execute(
            "SELECT name,type FROM sqlite_schema WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ):
            try:
                rows = db.execute(f"SELECT count(*) FROM {quote(name)}").fetchone()[0]
            except sqlite3.Error as error:
                rows = {"error": str(error)}
            columns = [
                row[1] for row in db.execute(f"PRAGMA table_xinfo({quote(name)})")
            ]
            result[name] = {"kind": kind, "rows": rows, "columns": columns}
        return result


def digest(path):
    sha = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(8 * 1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()


def snapshot(source, destination):
    # Online backup captures a consistent individual database, including WAL.
    # It does not establish a common cut across databases or rollout files.
    with closing(readonly(source)) as src, closing(sqlite3.connect(destination)) as dst:
        src.execute("BEGIN")
        src.execute("SELECT count(*) FROM sqlite_schema").fetchone()
        src.backup(dst, pages=4096, sleep=0.05)
        # Make the capture a self-contained main database. Readers need no
        # writable WAL/SHM sidecars beside these immutable migration inputs.
        dst.execute("PRAGMA journal_mode=DELETE")
    destination.chmod(0o600)


def prepare_control(output, catalogs, captured):
    path = output / "control.sqlite"
    with closing(sqlite3.connect(path)) as dst:
        dst.execute("CREATE TABLE profile_control (id INTEGER PRIMARY KEY)")
        dst.execute("INSERT INTO profile_control VALUES(1)")
        # The generic runner reads one named SQLite connection per mapping.
        # Consolidate versioned memory stores so both versions feed one target
        # table and primary keys retain the version namespace.
        for table in ["stage1_outputs", "jobs", "consolidation_progress"]:
            versions = [
                name
                for name in ["memories_v1", "memories_v2"]
                if table in catalogs.get(name, {})
            ]
            if not versions:
                continue
            column_names = sorted(
                {
                    col
                    for name in versions
                    for col in catalogs[name][table]["columns"]
                    if col != "singleton"
                }
            )
            declarations = []
            for col in column_names:
                # Preserve SQLite's storage classes; target type checking is
                # performed by the Zig runner on the selected values.
                declarations.append(quote(col))
            dst.execute(
                f"CREATE TABLE {quote(table)} (version TEXT NOT NULL, {','.join(declarations)})"
            )
            for version in versions:
                with closing(readonly(captured[version])) as src:
                    source_columns = catalogs[version][table]["columns"]
                    select = ",".join(
                        quote(col) if col in source_columns else "NULL"
                        for col in column_names
                    )
                    cursor = src.execute(f"SELECT {select} FROM {quote(table)}")
                    placeholders = ",".join("?" for _ in range(len(column_names) + 1))
                    for row in cursor:
                        dst.execute(
                            f"INSERT INTO {quote(table)} VALUES ({placeholders})",
                            (version.removeprefix("memories_"), *row),
                        )
        dst.commit()
    path.chmod(0o600)
    return path


def source_location(target):
    if target in HISTORY_TABLES:
        return "history", target.removeprefix("codex_")
    if target in BOARD_NAMES:
        return "board", BOARD_NAMES[target]
    if target in {"codex_thread_goals", "codex_thread_goal_continuation_deferrals"}:
        return "goals", target.removeprefix("codex_")
    if target.startswith("codex_queued_"):
        return "queue", target.removeprefix("codex_")
    if target in {"codex_stage1_outputs", "codex_jobs", "codex_consolidation_progress"}:
        return "control", target.removeprefix("codex_")
    return "state", target.removeprefix("codex_")


def mapping(target, definition, catalogs):
    source, source_table = source_location(target)
    if (
        target == "codex_thread_attachments"
        and source_table not in catalogs.get(source, {})
        and "thread_artifacts" in catalogs.get(source, {})
    ):
        source_table = "thread_artifacts"
    metadata = catalogs.get(source, {}).get(source_table)
    cols, expressions = [], []
    if metadata:
        available = set(metadata["columns"])
        for column in definition["columns"]:
            name = column["name"]
            expression = quote(name) if name in available else None
            transform = "identity"
            if target == "codex_threads" and name in {"created_at_ms", "updated_at_ms"}:
                seconds = name.removesuffix("_ms")
                expression = (
                    f"COALESCE({quote(name)}, {quote(seconds)} * 1000)"
                    if name in available
                    else f"{quote(seconds)} * 1000"
                )
            elif target == "codex_guardian_review_feedback" and name == "record_bytes":
                expression = 'length("record")'
            elif target == "codex_guardian_review_feedback" and name == "record":
                transform = "base64"
            elif source_table == "thread_artifacts" and name == "attachment_type":
                expression = '"artifact_type"'
            elif source == "board" and name == "timestamp_us":
                expression = '"timestamp"'
            elif target == "codex_message_board_posts" and name == "is_root":
                expression = 'CASE WHEN "id" = "root" THEN 1 ELSE 0 END'
            if expression is None:
                # Omitted target-only fields retain authoritative DDL defaults
                # or NULL; a subsequent history import fills its own fields.
                if not column["nullable"]:
                    if target == "codex_threads" and name == "next_ordinal":
                        continue
                    if target == "codex_thread_items" and name in {
                        "updated_at_ordinal",
                        "item_type",
                    }:
                        expression = "0" if name == "updated_at_ordinal" else "''"
                    else:
                        raise ValueError(f"unmapped required column: {target}.{name}")
                else:
                    continue
            cols.append({**column, "from": name, "transform": transform})
            expressions.append(f"{expression} AS {quote(name)}")
        # Do not silently lose columns added by upstream SQLite migrations.
        used = {col["from"] for col in cols}
        renamed = {"timestamp"} if source == "board" else set()
        if source_table == "thread_artifacts":
            renamed.add("artifact_type")
        retired = {
            "thread_turns": {"rollout_byte_offset", "rollout_end_byte_offset"},
            "thread_history_projection_state": {"next_rollout_byte_offset"},
        }.get(source_table, set())
        unknown = available - used - renamed - retired
        if unknown:
            raise ValueError(
                f"unclassified source columns: {source}.{source_table}: {sorted(unknown)}"
            )
        query = f"SELECT {', '.join(expressions)} FROM {quote(source_table)}"
        if target == "codex_thread_sections":
            query += f" UNION ALL SELECT '{PINNED_SECTION}', 'Pinned', NULL WHERE NOT EXISTS (SELECT 1 FROM thread_sections WHERE id='{PINNED_SECTION}')"
        disposition = "copy"
    else:
        source = "control"
        cols = [{**col, "from": col["name"]} for col in definition["columns"]]
        casts = {
            "text": "TEXT",
            "integer": "INTEGER",
            "boolean": "INTEGER",
            "json": "TEXT",
        }
        query = (
            "SELECT "
            + ",".join(
                f"CAST(NULL AS {casts[col['type']]}) AS {quote(col['name'])}"
                for col in cols
            )
            + " WHERE 0"
        )
        if target == "codex_thread_sections":
            query = f"SELECT '{PINNED_SECTION}' AS id, 'Pinned' AS name, CAST(NULL AS TEXT) AS appearance"
        disposition = "absent-source-empty-target"
    return {
        "source": source,
        "query": query,
        "target": target,
        "key": definition["key"],
        "columns": cols,
    }, {
        "source": source_location(target)[0],
        "table": source_table,
        "target": target,
        "disposition": disposition,
        "retired_columns": sorted(retired) if metadata else [],
    }


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    path.chmod(0o600)


def prepare(codex_repo, codex_home, sqlite_home, output, capture=True):
    if output.exists():
        raise ValueError("output directory must be new")
    output.mkdir(parents=True, mode=0o700)
    contract = schema_contract(codex_repo)
    previous_capture = None
    if not capture:
        previous_capture = json.loads((sqlite_home / "manifest.json").read_text())
        if previous_capture["schema"]["schema_sha256"] != contract["schema_sha256"]:
            raise ValueError("existing capture has a different target schema pin")
    captured, catalogs, sources = {}, {}, []
    inventory_report = {}
    for name, filename in SOURCE_FILES.items():
        original = sqlite_home / filename
        if not original.is_file():
            inventory_report[name] = {"filename": filename, "present": False}
            continue
        destination = output / filename
        if capture:
            snapshot(original, destination)
        else:
            # Only for already immutable/quiesced inputs and fixture tests.
            destination = original.resolve()
            if digest(destination) != previous_capture["sources"][name]["sha256"]:
                raise ValueError(f"existing source snapshot changed: {filename}")
        captured[name] = destination
        catalogs[name] = inventory(destination)
        if any(isinstance(table["rows"], dict) for table in catalogs[name].values()):
            raise ValueError(
                f"source table could not be read: {filename}; see source integrity"
            )
        sources.append(
            {"name": name, "path": str(destination if not capture else Path(filename))}
        )
        inventory_report[name] = {
            "filename": filename,
            "snapshot_path": str(destination if not capture else Path(filename)),
            "present": True,
            "bytes": destination.stat().st_size,
            "sha256": digest(destination),
            "tables": catalogs[name],
        }
    if "state" not in catalogs or "threads" not in catalogs["state"]:
        raise ValueError("a Codex state database with a threads table is required")
    control = prepare_control(output, catalogs, captured)
    catalogs["control"] = inventory(control)
    sources.append({"name": "control", "path": "control.sqlite"})
    statements = [
        s
        for s in contract["statements"]
        if not s.startswith("INSERT INTO codex_thread_sections")
    ]
    statements.append(contract["migration_table"])
    now = int(datetime.now(timezone.utc).timestamp() * 1000)  # noqa: UP017 - Python 3.9 compatibility
    for migration in contract["migrations"]:
        name = migration["name"].replace("'", "''")
        statements.append(
            f"INSERT INTO codex_schema_migrations (version,name,applied_at_ms) VALUES ({migration['version']},'{name}',{now})"
        )
    mappings, coverage = [], []
    for target, definition in contract["tables"].items():
        if target == "codex_schema_migrations":
            continue
        table, entry = mapping(target, definition, catalogs)
        mappings.append(table)
        coverage.append(entry)
    # Classify every source table, including internal SQLite bookkeeping.
    for source, catalog in catalogs.items():
        if source == "control":
            continue
        for name, metadata in catalog.items():
            covered = any(
                entry["source"] == source and entry["table"] == name
                for entry in coverage
            )
            if source.startswith("memories_") and name in {
                "stage1_outputs",
                "jobs",
                "consolidation_progress",
            }:
                covered = True
                coverage.append(
                    {
                        "source": source,
                        "table": name,
                        "disposition": "merge-by-version",
                        "version": source.removeprefix("memories_"),
                    }
                )
            if covered:
                continue
            if name == "_sqlx_migrations":
                action = "retain-in-source-snapshot-not-antfly-schema-migrations"
            elif source == "board" and name == "board_templates":
                action = "retain-in-source-snapshot-no-antfly-board-template-backend"
            else:
                raise ValueError(f"unclassified source table: {source}.{name}")
            coverage.append(
                {
                    "source": source,
                    "table": name,
                    "disposition": action,
                    "rows": metadata["rows"],
                }
            )
    plan = {
        "version": 1,
        "sources": sources,
        "destination": "01-relational.aflite",
        "schema": {"statements": statements},
        "tables": mappings,
        "batch_rows": 8,
        "batch_bytes": 8 * 1024 * 1024,
    }
    overlay = next(
        table.copy() for table in mappings if table["target"] == "codex_threads"
    )
    overlay["columns"] = [
        col
        for col in overlay["columns"]
        if col["name"] not in HISTORY_OWNED_THREAD_COLUMNS
    ]
    overlay["query"] = (
        "SELECT "
        + ",".join(quote(col["from"]) for col in overlay["columns"])
        + " FROM ("
        + overlay["query"]
        + ") AS captured_metadata"
    )
    overlay["write_mode"] = "upsert"
    overlay_plan = {
        "version": 1,
        "sources": sources,
        "destination": "02-complete.aflite",
        "schema": {"seed": "01-relational.aflite"},
        "tables": [overlay],
        "batch_rows": 8,
        "batch_bytes": 8 * 1024 * 1024,
    }
    history_files = []
    for directory in ["sessions", "archived_sessions"]:
        for path in sorted((codex_home / directory).rglob("rollout-*")):
            if path.is_file() and (
                path.name.endswith(".jsonl") or path.name.endswith(".jsonl.zst")
            ):
                stat = path.stat()
                history_files.append(
                    {
                        "path": str(path.relative_to(codex_home)),
                        "bytes": stat.st_size,
                        "mtime_ns": stat.st_mtime_ns,
                    }
                )
    with closing(readonly(captured["state"])) as db:
        threads = db.execute("SELECT id,rollout_path FROM threads").fetchall()
    missing_rollouts = sum(not Path(path).is_file() for _, path in threads)
    logs = sqlite_home / "logs_2.sqlite"
    log_report = {
        "disposition": "retain-in-place-no-antfly-log-backend",
        "present": logs.is_file(),
    }
    if logs.is_file():
        log_report["bytes"] = logs.stat().st_size
        log_report["tables"] = inventory(logs)
    elif previous_capture is not None:
        log_report = previous_capture["logs"]
    manifest = {
        "profile_version": 1,
        "schema": {
            key: value
            for key, value in contract.items()
            if key not in {"statements", "migration_table", "tables"}
        },
        "capture_consistency": "per-database-online-backup; rollout files are inventory-only",
        "cutover_ready": False,
        "blockers": [
            "quiesced common source capture and full history/application acceptance have not run"
        ],
        "sources": inventory_report,
        "table_coverage": coverage,
        "logs": log_report,
        "history": {
            "rollout_files": len(history_files),
            "rollout_bytes": sum(file["bytes"] for file in history_files),
            "thread_rows": len(threads),
            "missing_thread_rollout_paths": missing_rollouts,
            "capture_required": True,
            "files_manifest": "rollouts.json",
        },
        "phases": [
            {
                "name": "relational",
                "plan": "relational.json",
                "destination": "01-relational.aflite",
            },
            {
                "name": "history",
                "tool": "codex-antfly-import",
                "args": [
                    "--codex-home",
                    "<quiesced-captured-codex-home>",
                    "--to",
                    "01-relational.aflite",
                    "--replace",
                    "--no-semantic",
                ],
                "owns": [
                    "codex_history_items",
                    *sorted(HISTORY_TABLES),
                    "thread.extra",
                    "thread.next_ordinal",
                    "thread.lineage",
                ],
                "acceptance": "no skipped/failed threads; all source thread IDs imported; source lineage resolved",
            },
            {
                "name": "metadata-overlay",
                "plan": "metadata-overlay.json",
                "preserves": sorted(HISTORY_OWNED_THREAD_COLUMNS),
            },
            {
                "name": "acceptance",
                "requires": [
                    "all databases and rollouts captured while Codex stopped",
                    "history importer binary built from pinned Codex commit",
                    "rollout timestamps retained when events omit lifecycle timestamps; validate original projection times",
                    "no history skipped/failed or source thread missing",
                    "application listing/resume/goals/queue/memories/sections/attachments checks",
                    "indexes drained; optional semantic indexes built",
                    "antfly file integrity check",
                    "retain original SQLite and rollout data until acceptance",
                ],
            },
        ],
        "documents": {
            "codex_history_items": "reconstruct-from-rollouts-with-codex-importer",
            "codex_approvals": "new-empty-no-sqlite-source",
            "codex_memory_notes": "filesystem-remains-source-of-truth-index-via-Codex-backend",
        },
        "preserved_files": [
            "memories/",
            "memories_v2/",
            "sessions/",
            "archived_sessions/",
            "SQLite source snapshots",
        ],
    }
    write_json(output / "relational.json", plan)
    write_json(output / "metadata-overlay.json", overlay_plan)
    write_json(output / "manifest.json", manifest)
    write_json(output / "rollouts.json", history_files)
    write_json(output / "schema-contract.json", contract)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codex-repo", type=Path, required=True)
    parser.add_argument("--codex-home", type=Path, required=True)
    parser.add_argument(
        "--sqlite-home",
        type=Path,
        help="defaults to --codex-home; honor CODEX_SQLITE_HOME/config explicitly",
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--existing-snapshots",
        action="store_true",
        help="use hash-validated snapshots from --sqlite-home containing a prior manifest, without recapturing live databases",
    )
    args = parser.parse_args()
    report = prepare(
        args.codex_repo.resolve(),
        args.codex_home.resolve(),
        (args.sqlite_home or args.codex_home).resolve(),
        args.out.resolve(),
        capture=not args.existing_snapshots,
    )
    print(
        json.dumps(
            {
                "output": str(args.out),
                "sources": len(report["sources"]),
                "classified_tables": len(report["table_coverage"]),
                "history": {
                    k: v for k, v in report["history"].items() if k != "files_manifest"
                },
                "cutover_ready": False,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
