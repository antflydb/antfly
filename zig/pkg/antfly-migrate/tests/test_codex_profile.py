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

"""Codex profile coverage, version namespaces, and capture ownership tests."""

import copy
import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROFILE_DIR = Path(__file__).resolve().parents[1] / "profiles/codex"
spec = importlib.util.spec_from_file_location(
    "codex_profile", PROFILE_DIR / "prepare.py"
)
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)
sys.path.insert(0, str(PROFILE_DIR))
from capture_history import capture


class CodexProfileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.home = self.root / "home"
        self.home.mkdir()
        self.contract = json.loads((PROFILE_DIR / "schema-contract.json").read_text())
        self.id = "01984de2-8f74-7c91-a3b2-5c5e937cf319"
        (self.home / "sessions").mkdir()
        self.rollout = self.home / "sessions/rollout-fixture.jsonl"
        self.rollout.write_text('{"type":"session_meta"}\n')
        self.create_table(
            self.home / "state_5.sqlite",
            "threads",
            "codex_threads",
            excluded=profile.HISTORY_OWNED_THREAD_COLUMNS,
        )
        with sqlite3.connect(self.home / "state_5.sqlite") as db:
            values = {}
            for column in self.contract["tables"]["codex_threads"]["columns"]:
                name = column["name"]
                if name in profile.HISTORY_OWNED_THREAD_COLUMNS:
                    continue
                values[name] = (
                    None
                    if column["nullable"]
                    else {"text": "", "integer": 0, "boolean": 0, "json": "{}"}[
                        column["type"]
                    ]
                )
            values.update(
                id=self.id,
                rollout_path=str(self.rollout),
                history_mode="legacy",
                memory_mode="enabled",
                source="cli",
                model_provider="fixture",
            )
            db.execute(
                "INSERT INTO threads("
                + ",".join(values)
                + ") VALUES("
                + ",".join("?" for _ in values)
                + ")",
                tuple(values.values()),
            )

    def create_table(self, path, source, target, excluded=()):
        types = {
            "text": "TEXT",
            "integer": "INTEGER",
            "boolean": "INTEGER",
            "json": "TEXT",
        }
        columns = [
            col
            for col in self.contract["tables"][target]["columns"]
            if col["name"] not in excluded
        ]
        with sqlite3.connect(path) as db:
            db.execute(
                "CREATE TABLE "
                + source
                + "("
                + ",".join(col["name"] + " " + types[col["type"]] for col in columns)
                + ")"
            )

    def prepare(self, name="bundle"):
        with patch.object(
            profile, "schema_contract", return_value=copy.deepcopy(self.contract)
        ):
            return profile.prepare(
                self.root, self.home, self.home, self.root / name, capture=True
            )

    def test_every_target_and_source_table_has_a_disposition(self):
        report = self.prepare()
        plan = json.loads((self.root / "bundle/relational.json").read_text())
        self.assertEqual(
            {t["target"] for t in plan["tables"]},
            set(self.contract["tables"]) - {"codex_schema_migrations"},
        )
        self.assertFalse(report["cutover_ready"])
        self.assertEqual(report["history"]["missing_thread_rollout_paths"], 0)
        self.assertTrue(report["history"]["capture_required"])
        self.assertFalse(
            any(
                s.startswith("INSERT INTO codex_thread_sections")
                for s in plan["schema"]["statements"]
            )
        )
        overlay = json.loads((self.root / "bundle/metadata-overlay.json").read_text())
        self.assertEqual(overlay["tables"][0]["write_mode"], "upsert")
        self.assertFalse(
            profile.HISTORY_OWNED_THREAD_COLUMNS
            & {c["name"] for c in overlay["tables"][0]["columns"]}
        )

    def test_memory_versions_do_not_collide(self):
        for version in ["v1", "v2"]:
            path = self.home / (
                "memories_1.sqlite" if version == "v1" else "memories_v2_1.sqlite"
            )
            self.create_table(
                path, "stage1_outputs", "codex_stage1_outputs", excluded={"version"}
            )
            with sqlite3.connect(path) as db:
                db.execute(
                    "INSERT INTO stage1_outputs(thread_id,source_updated_at,raw_memory,rollout_summary,generated_at,selected_for_phase2) VALUES(?,?,?,?,?,?)",
                    (self.id, 123, version, "summary", 456, 0),
                )
                db.execute(
                    "CREATE TABLE consolidation_progress(singleton INTEGER PRIMARY KEY,max_thread_count INTEGER)"
                )
                db.execute(
                    "INSERT INTO consolidation_progress VALUES(1,?)",
                    (1 if version == "v1" else 2,),
                )
        report = self.prepare()
        with sqlite3.connect(self.root / "bundle/control.sqlite") as db:
            self.assertEqual(
                db.execute(
                    "SELECT version,thread_id,raw_memory FROM stage1_outputs ORDER BY version"
                ).fetchall(),
                [("v1", self.id, "v1"), ("v2", self.id, "v2")],
            )
            self.assertEqual(
                db.execute(
                    "SELECT version,max_thread_count FROM consolidation_progress ORDER BY version"
                ).fetchall(),
                [("v1", 1), ("v2", 2)],
            )
        self.assertEqual(
            sum(
                e["disposition"] == "merge-by-version" for e in report["table_coverage"]
            ),
            4,
        )
        binary = os.environ.get("ANTFLY_MIGRATE_BIN")
        if binary:
            result = subprocess.run(
                [
                    binary,
                    "migrate",
                    "--plan",
                    str(self.root / "bundle/relational.json"),
                    "--dry-run",
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=120,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_guardian_bytes_are_encoded_and_sized_explicitly(self):
        with sqlite3.connect(self.home / "state_5.sqlite") as db:
            db.execute(
                "CREATE TABLE guardian_review_feedback(id TEXT,thread_id TEXT,record BLOB)"
            )
            db.execute(
                "INSERT INTO guardian_review_feedback VALUES('record',?,?)",
                (self.id, b"\x00\xff"),
            )
        self.prepare()
        plan = json.loads((self.root / "bundle/relational.json").read_text())
        table = next(
            t for t in plan["tables"] if t["target"] == "codex_guardian_review_feedback"
        )
        self.assertEqual(
            next(c for c in table["columns"] if c["name"] == "record")["transform"],
            "base64",
        )
        self.assertIn('length("record")', table["query"])

    def test_unknown_columns_and_tables_require_review(self):
        with sqlite3.connect(self.home / "state_5.sqlite") as db:
            db.execute("ALTER TABLE threads ADD COLUMN upstream_new_field TEXT")
        with self.assertRaisesRegex(ValueError, "unclassified source columns"):
            self.prepare()
        with sqlite3.connect(self.home / "state_5.sqlite") as db:
            db.execute("ALTER TABLE threads DROP COLUMN upstream_new_field")
            db.execute("CREATE TABLE upstream_new_table(value TEXT)")
        with self.assertRaisesRegex(ValueError, "unclassified source table"):
            self.prepare("bundle2")

    def test_schema_drift_fails_before_capture(self):
        path = self.root / "repo/codex-rs/antfly/src/schema.rs"
        path.parent.mkdir(parents=True)
        path.write_text("a different target schema")
        with self.assertRaisesRegex(ValueError, "target schema changed"):
            profile.schema_contract(self.root / "repo")

    def test_history_capture_rewrites_only_the_importer_input(self):
        self.prepare()
        bundle = self.root / "bundle"
        before = profile.digest(bundle / "state_5.sqlite")
        with self.assertRaisesRegex(ValueError, "stop Codex"):
            capture(bundle, self.home, False)
        home = capture(bundle, self.home, True)
        self.assertEqual(profile.digest(bundle / "state_5.sqlite"), before)
        with sqlite3.connect(home / "state_5.sqlite") as db:
            self.assertEqual(
                db.execute("SELECT rollout_path FROM threads").fetchone()[0],
                str(home / "sessions/rollout-fixture.jsonl"),
            )
        self.assertEqual(
            (home / "sessions/rollout-fixture.jsonl").read_bytes(),
            self.rollout.read_bytes(),
        )
        manifest = json.loads((bundle / "manifest.json").read_text())
        self.assertFalse(manifest["history"]["capture_required"])
        self.assertFalse(manifest["cutover_ready"])

    def test_changed_rollout_rejects_old_capture(self):
        self.prepare()
        self.rollout.write_text("changed since database capture\n")
        with self.assertRaisesRegex(ValueError, "rollout changed"):
            capture(self.root / "bundle", self.home, True)
        self.assertTrue(
            json.loads((self.root / "bundle/manifest.json").read_text())["history"][
                "capture_required"
            ]
        )

    def test_reused_snapshots_are_hash_bound_and_captureable(self):
        self.prepare()
        original = self.root / "bundle"
        with patch.object(
            profile, "schema_contract", return_value=copy.deepcopy(self.contract)
        ):
            profile.prepare(
                self.root, self.home, original, self.root / "replanned", capture=False
            )
        captured = capture(self.root / "replanned", self.home, True)
        self.assertTrue((captured / "state_5.sqlite").is_file())
        with sqlite3.connect(original / "state_5.sqlite") as db:
            db.execute("UPDATE threads SET title='changed snapshot'")
        with (
            patch.object(
                profile, "schema_contract", return_value=copy.deepcopy(self.contract)
            ),
            self.assertRaisesRegex(ValueError, "snapshot changed"),
        ):
            profile.prepare(
                self.root,
                self.home,
                original,
                self.root / "invalid-replan",
                capture=False,
            )


if __name__ == "__main__":
    if len(sys.argv) > 1 and Path(sys.argv[1]).is_file():
        os.environ["ANTFLY_MIGRATE_BIN"] = str(Path(sys.argv.pop(1)).resolve())
    unittest.main()
