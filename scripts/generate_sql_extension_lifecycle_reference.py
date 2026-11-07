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

"""Owned PG18 controlled-package lifecycle oracle; no GIS/crypto parity claim."""

import argparse
import difflib
import shutil
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile

INVENTORY_SHA256 = "c203a4dcf0094b75e1d3764e90beebddaf844a9ce604543c0a4dbc0ffe8ae173"
CASE_IDS = (
    "sql-0148",
    "sql-0754",
    "sql-0755",
    "sql-0756",
    "sql-0757",
    "sql-0758",
    "sql-0759",
    "sql-0760",
    "sql-0761",
    "sql-0762",
    "sql-0763",
)
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--check",
    action="store_true",
    help="compare actual independent PostgreSQL results with the committed reference",
)
parser.add_argument(
    "--repository-root", type=Path, default=Path(__file__).resolve().parents[1]
)
parser.add_argument(
    "--postgres-bin",
    type=Path,
    default=None,
    help="directory containing an isolated PostgreSQL 18+ runtime",
)
parser.add_argument("--reference", type=Path, default=None)
parser.add_argument(
    "--evidence-dir",
    type=Path,
    default=None,
    help="write actual run diagnostics outside the committed reference",
)
args = parser.parse_args()
ROOT = args.repository_root.resolve()
REFERENCE = (
    args.reference
    or ROOT / "zig/pkg/antfly/src/api/fixtures/sql_extension_lifecycle_reference.json"
).resolve()
if args.postgres_bin is not None:
    PG = args.postgres_bin.resolve()
elif os.environ.get("ANTFLY_SQL_POSTGRES_BIN"):
    PG = Path(os.environ["ANTFLY_SQL_POSTGRES_BIN"]).resolve()
else:
    config = shutil.which("pg_config")
    if config is None:
        raise SystemExit(
            "PostgreSQL 18+ is required; set ANTFLY_SQL_POSTGRES_BIN or --postgres-bin"
        )
    PG = Path(
        subprocess.check_output([config, "--bindir"], text=True).strip()
    ).resolve()
inventory_bytes = (
    ROOT / "zig/pkg/antfly-embedded/src/sql/fixtures/sql_parity_inventory.json"
).read_bytes()
if hashlib.sha256(inventory_bytes).hexdigest() != INVENTORY_SHA256:
    raise SystemExit("immutable SQL source inventory digest changed")
inventory = json.loads(inventory_bytes)
by_id = {entry["id"]: entry for entry in inventory["entries"]}
originals_path = (
    ROOT / "zig/pkg/antfly/src/api/fixtures/sql_extension_lifecycle_originals.json"
)
originals_fixture = json.loads(originals_path.read_bytes())
actual_originals = {entry["id"]: entry for entry in originals_fixture["entries"]}
if tuple(actual_originals) != CASE_IDS:
    raise SystemExit("extension lifecycle original case roster changed")
entries = []
for case_id in CASE_IDS:
    original = by_id[case_id]
    actual = actual_originals[case_id]
    for field in ("source_entry_sha256", "sql"):
        if original[field] != actual[field]:
            raise SystemExit(
                case_id + ": immutable original statement or entry digest changed"
            )
    digest = hashlib.sha256(original["sql"].encode()).hexdigest()
    if actual["sql_sha256"] != digest:
        raise SystemExit(case_id + ": original SQL digest changed")
    entry = dict(original)
    entry["sql_sha256"] = digest
    entries.append(entry)
ORIGINALS = {"inventory_sha256": INVENTORY_SHA256, "entries": entries}
ORIGINALS_BYTES = (json.dumps(ORIGINALS, indent=2) + "\n").encode()
if (
    args.check
    and hashlib.sha256(REFERENCE.read_bytes()).hexdigest()
    != originals_fixture["postgres_reference_sha256"]
):
    raise SystemExit("extension lifecycle PostgreSQL reference digest changed")
PACKAGES = {
    "vector": {"default": "1.0.0", "versions": {"1.0.0": 1}},
    "pgcrypto": {"default": "1.0.0", "versions": {"1.0.0": 1}},
    "postgis": {
        "default": "3.5.0",
        "versions": {"3.4.0": 340, "3.4.5": 345, "3.5.0": 350},
    },
    "uuid-ossp": {"default": "1.0.0", "versions": {"1.0.0": 100}},
    "spatial_tools": {
        "default": "1.0.0",
        "versions": {"1.0.0": 1},
        "requires": "postgis",
    },
}
ENV = {key: value for key, value in os.environ.items() if not key.startswith("PG")}
ENV.update(TZ="UTC", LC_ALL="C", PGCONNECT_TIMEOUT="3", PSQLRC="/dev/null")
REPORT = {
    "format": 1,
    "scope": "controlled local extension package lifecycle only; no GIS, crypto, UUID or vector function parity",
    "originals_manifest_sha256": hashlib.sha256(ORIGINALS_BYTES).hexdigest(),
    "packages": PACKAGES,
    "entries": [],
    "assertions": [],
}


def command(args, **kw):
    return subprocess.run(
        [str(a) for a in args],
        text=True,
        capture_output=True,
        timeout=30,
        env=ENV,
        **kw,
    )


def semantic_reference(value):
    """Ignore machine provenance and diagnostic source-code line numbers only."""
    if isinstance(value, list):
        return [semantic_reference(entry) for entry in value]
    if isinstance(value, dict):
        return {
            key: semantic_reference(entry)
            for key, entry in value.items()
            if key not in ("postgres_version", "server_binary")
        }
    if isinstance(value, str) and "LOCATION:" in value:
        return "\n".join(
            line for line in value.splitlines() if not line.startswith("LOCATION:")
        )
    return value


def main():
    with tempfile.TemporaryDirectory(prefix="antfly-extension-pg18-") as temporary:
        root = Path(temporary).resolve()
        data, sock, share = root / "data", root / "socket", root / "share"
        sock.mkdir()
        (share / "extension").mkdir(parents=True)
        controls = share / "extension"
        files = {}
        for name, package in PACKAGES.items():
            control = (
                "default_version = '%s'\nrelocatable = true\nsuperuser = false\ncomment = 'Controlled lifecycle fixture, not the upstream implementation'\n"
                % package["default"]
            )
            if package.get("requires"):
                control += "requires = '%s'\n" % package["requires"]
            files[name + ".control"] = control
            function = name.replace("-", "_") + "_version_marker"
            for version, marker in package["versions"].items():
                files[name + "--" + version + ".sql"] = (
                    "CREATE FUNCTION %s(bigint) RETURNS bigint LANGUAGE SQL IMMUTABLE STRICT AS 'SELECT $1 + %d';\n"
                    % (function, marker)
                )
        for old, new in [("3.4.0", "3.4.5"), ("3.4.5", "3.5.0")]:
            files["postgis--" + old + "--" + new + ".sql"] = (
                "CREATE OR REPLACE FUNCTION postgis_version_marker(bigint) RETURNS bigint LANGUAGE SQL IMMUTABLE STRICT AS 'SELECT $1 + %d';\n"
                % PACKAGES["postgis"]["versions"][new]
            )
        for name, content in files.items():
            (controls / name).write_text(content)
        REPORT["package_files_sha256"] = {
            name: hashlib.sha256(text.encode()).hexdigest()
            for name, text in sorted(files.items())
        }
        initialized = command(
            [
                PG / "initdb",
                "-D",
                data,
                "-A",
                "trust",
                "-U",
                "fixture_bootstrap",
                "--no-locale",
                "-E",
                "UTF8",
            ]
        )
        if initialized.returncode:
            raise RuntimeError(initialized.stderr)
        options = "-k %s -h %s -p 59433 -c extension_control_path=%s" % (
            shlex.quote(str(sock)),
            shlex.quote(""),
            shlex.quote(str(share)),
        )
        running = False

        def start():
            nonlocal running
            outcome = command(
                [
                    PG / "pg_ctl",
                    "-D",
                    data,
                    "-l",
                    root / "server.log",
                    "-o",
                    options,
                    "-w",
                    "-t",
                    "20",
                    "start",
                ]
            )
            if outcome.returncode:
                raise RuntimeError(outcome.stderr + outcome.stdout)
            running = True
            assert Path(sql("SHOW data_directory;").strip()).resolve() == data
            assert sql("SHOW extension_control_path;").strip() == str(share)
            assert sql("SHOW listen_addresses;").strip() == ""

        def stop():
            nonlocal running
            if running or (data / "postmaster.pid").exists():
                assert data.parent == root and data.name == "data"
                outcome = command(
                    [PG / "pg_ctl", "-D", data, "-m", "fast", "-w", "-t", "20", "stop"]
                )
                if outcome.returncode:
                    raise RuntimeError(outcome.stderr + outcome.stdout)
                running = False

        def run(sql_text, db="postgres", actor=None):
            args = [
                PG / "psql",
                "-X",
                "-A",
                "-t",
                "-h",
                sock,
                "-p",
                "59433",
                "-d",
                db,
                "-v",
                "ON_ERROR_STOP=1",
                "-v",
                "VERBOSITY=verbose",
            ]
            args += ["-U", actor or "fixture_bootstrap"]
            outcome = command(args + ["-c", sql_text])
            code = None
            if outcome.returncode:
                match = re.search(r"ERROR:\s+([A-Z0-9]{5}):", outcome.stderr)
                if not match:
                    raise RuntimeError(outcome.stderr)
                code = match.group(1)
            return {
                "sqlstate": code,
                "stdout": outcome.stdout.strip(),
                "stderr": outcome.stderr.strip()[:2048],
            }

        def sql(text, db="postgres", actor=None):
            result = run(text, db, actor)
            if result["sqlstate"]:
                raise AssertionError(result)
            return result["stdout"]

        def expected(text, code, db, actor="fixture_owner"):
            result = run(text, db, actor)
            assert result["sqlstate"] == code, (text, result, code)
            REPORT["assertions"].append({"sql": text, "sqlstate": code})
            return result

        def snapshot(db, extension):
            query = """SELECT json_build_object('extensions',COALESCE((SELECT json_agg(json_build_object('name',e.extname,'version',e.extversion,'owner',r.rolname,'schema',n.nspname,'comment',obj_description(e.oid,'pg_extension')) ORDER BY e.extname) FROM pg_extension e JOIN pg_roles r ON r.oid=e.extowner JOIN pg_namespace n ON n.oid=e.extnamespace WHERE e.extname<>'plpgsql'),'[]'::json),'members',COALESCE((SELECT json_agg(json_build_object('extension',e.extname,'name',p.proname,'owner',r.rolname,'signature',pg_get_function_identity_arguments(p.oid),'dependency',d.deptype) ORDER BY e.extname,p.proname) FROM pg_depend d JOIN pg_extension e ON d.refclassid='pg_extension'::regclass AND e.oid=d.refobjid JOIN pg_proc p ON d.classid='pg_proc'::regclass AND p.oid=d.objid JOIN pg_roles r ON r.oid=p.proowner WHERE d.deptype='e' AND e.extname<>'plpgsql'),'[]'::json),'extension_dependencies',COALESCE((SELECT json_agg(json_build_object('from',e.extname,'to',target.extname,'dependency',d.deptype) ORDER BY e.extname,target.extname) FROM pg_depend d JOIN pg_extension e ON d.classid='pg_extension'::regclass AND e.oid=d.objid JOIN pg_extension target ON d.refclassid='pg_extension'::regclass AND target.oid=d.refobjid),'[]'::json));"""
            value = json.loads(sql(query, db))
            matching = [row for row in value["extensions"] if row["name"] == extension]
            if matching:
                function = extension.replace("-", "_") + "_version_marker"
                value["marker_at_10"] = int(
                    sql("SELECT public.%s(10);" % function, db, "fixture_owner")
                )
            return value

        try:
            start()
            REPORT["postgres_version"] = sql("SHOW server_version;")
            if int(REPORT["postgres_version"].split(".")[0]) < 18:
                raise RuntimeError(
                    "PostgreSQL 18+ with private extension_control_path is required"
                )
            REPORT["server_binary"] = str(PG / "postgres")
            REPORT["private_extension_control_path_verified"] = True
            REPORT["tcp_disabled_verified"] = True
            sql("CREATE ROLE fixture_owner LOGIN; CREATE ROLE fixture_other LOGIN;")
            database_cases = []
            for original in ORIGINALS["entries"]:
                case_id = original["id"]
                db = case_id.replace("-", "_")
                extension = (
                    "vector"
                    if case_id == "sql-0148"
                    else "pgcrypto"
                    if case_id in ("sql-0754", "sql-0755")
                    else "uuid-ossp"
                    if case_id == "sql-0759"
                    else "postgis"
                )
                sql("CREATE DATABASE %s OWNER fixture_owner;" % db)
                if case_id == "sql-0148":
                    sql("CREATE EXTENSION vector;", db, "fixture_owner")
                if case_id in ("sql-0758",):
                    sql(
                        "CREATE EXTENSION postgis VERSION '3.5.0';", db, "fixture_owner"
                    )
                if case_id in ("sql-0760", "sql-0761", "sql-0762", "sql-0763"):
                    sql(
                        "CREATE EXTENSION postgis VERSION '3.4.0';", db, "fixture_owner"
                    )
                if case_id == "sql-0763":
                    sql(
                        "CREATE EXTENSION spatial_tools; CREATE VIEW public.external_marker_view AS SELECT public.postgis_version_marker(10);",
                        db,
                        "fixture_owner",
                    )
                    expected("DROP EXTENSION postgis;", "2BP01", db)
                if case_id == "sql-0756":
                    expected("CREATE EXTENSION postgis VERSION '9.9.9';", "22023", db)
                    assert not snapshot(db, extension)["extensions"]
                before = snapshot(db, extension)
                outcome = run(original["sql"], db, "fixture_owner")
                assert outcome["sqlstate"] is None, outcome
                after = snapshot(db, extension)
                entry = {
                    key: original[key]
                    for key in ("id", "source_entry_sha256", "sql", "sql_sha256")
                }
                entry.update(before=before, outcome=outcome, after=after)
                REPORT["entries"].append(entry)
                database_cases.append((db, extension, entry))
                if case_id == "sql-0148":
                    assert (
                        after["extensions"][0]["comment"] == "vector search extension"
                    )
                    expected(
                        "COMMENT ON EXTENSION vector IS 'wrong owner';",
                        "42501",
                        db,
                        "fixture_other",
                    )
                elif case_id in ("sql-0754", "sql-0755"):
                    assert after["marker_at_10"] == 11
                    assert snapshot(db, extension) == after
                    again = run(original["sql"], db, "fixture_owner")
                    assert again["sqlstate"] is None
                    entry["repeat"] = again
                elif case_id == "sql-0756":
                    assert after["marker_at_10"] == 360
                elif case_id == "sql-0757":
                    assert after["marker_at_10"] == 350
                    expected("CREATE EXTENSION postgis;", "42710", db)
                    expected(
                        "DROP FUNCTION public.postgis_version_marker(bigint);",
                        "2BP01",
                        db,
                    )
                    expected("ALTER EXTENSION postgis UPDATE TO '9.9.9';", "22023", db)
                    expected("DROP EXTENSION postgis;", "42501", db, "fixture_other")
                    assert snapshot(db, extension) == after
                elif case_id == "sql-0758":
                    assert after["marker_at_10"] == 360
                    existing_unknown = run(
                        "CREATE EXTENSION IF NOT EXISTS postgis VERSION '9.9.9';",
                        db,
                        "fixture_owner",
                    )
                    assert existing_unknown["sqlstate"] is None
                    assert snapshot(db, extension) == after
                    entry["existing_extension_unknown_requested_version"] = (
                        existing_unknown
                    )
                elif case_id == "sql-0759":
                    assert after["marker_at_10"] == 110
                elif case_id in ("sql-0760", "sql-0761"):
                    assert (
                        before["marker_at_10"] == 350 and after["marker_at_10"] == 360
                    )
                    entry["declared_update_path"] = sql(
                        "SELECT path FROM pg_extension_update_paths('postgis') WHERE source='3.4.0' AND target='3.5.0';",
                        db,
                    )
                    assert entry["declared_update_path"] == "3.4.0--3.4.5--3.5.0"
                elif case_id in ("sql-0762", "sql-0763"):
                    assert not after["extensions"] and not after["members"]
                    if case_id == "sql-0763":
                        assert (
                            sql(
                                "SELECT to_regclass('public.external_marker_view') IS NULL;",
                                db,
                            )
                            == "t"
                        )
                if after["extensions"]:
                    assert all(
                        x["owner"] == "fixture_owner" for x in after["extensions"]
                    )
                    assert all(
                        x["owner"] == "fixture_owner" and x["dependency"] == "e"
                        for x in after["members"]
                    )
            stop()
            start()
            for db, extension, entry in database_cases:
                entry["cold"] = snapshot(db, extension)
                assert entry["cold"] == entry["after"]
            sql("COMMENT ON EXTENSION vector IS NULL;", "sql_0148", "fixture_owner")
            assert snapshot("sql_0148", "vector")["extensions"][0]["comment"] is None
            stop()
            start()
            assert snapshot("sql_0148", "vector")["extensions"][0]["comment"] is None
            REPORT["comment_null_removal_cold"] = True
            REPORT["clean_cold_reopen_equivalence"] = True
            REPORT["result"] = "PASS"
        finally:
            try:
                stop()
            finally:
                if args.evidence_dir is not None:
                    args.evidence_dir.mkdir(parents=True, exist_ok=True)
                    if (root / "server.log").exists():
                        (args.evidence_dir / "postgres-server.log").write_bytes(
                            (root / "server.log").read_bytes()
                        )
                REPORT["owned_cluster_stopped"] = not running

    REPORT["owned_temporary_cluster_removed"] = not root.exists()
    if args.evidence_dir is not None:
        (args.evidence_dir / "actual-reference.json").write_text(
            json.dumps(REPORT, indent=2) + "\n"
        )
    if args.check:
        expected_report = semantic_reference(json.loads(REFERENCE.read_bytes()))
        actual_report = semantic_reference(REPORT)
        if actual_report != expected_report:
            expected = (
                json.dumps(expected_report, sort_keys=True, indent=2) + "\n"
            ).splitlines(keepends=True)
            actual = (
                json.dumps(actual_report, sort_keys=True, indent=2) + "\n"
            ).splitlines(keepends=True)
            raise SystemExit(
                "".join(
                    difflib.unified_diff(
                        expected,
                        actual,
                        fromfile="committed PostgreSQL reference",
                        tofile="actual independent PostgreSQL",
                    )
                )
            )
    else:
        REFERENCE.parent.mkdir(parents=True, exist_ok=True)
        REFERENCE.write_text(json.dumps(REPORT, indent=2) + "\n")
    print(
        json.dumps(
            {
                "result": REPORT["result"],
                "postgres_version": REPORT["postgres_version"],
                "cases": [row["id"] for row in REPORT["entries"]],
                "cluster_stopped": REPORT["owned_cluster_stopped"],
                "cluster_removed": REPORT["owned_temporary_cluster_removed"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
