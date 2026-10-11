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

"""End-to-end acceptance tests against the real SQLite and libantfly libraries.

ANTFLY_MIGRATE_BIN=/absolute/path/antfly-migrate python3 -m unittest discover \
    -s zig/pkg/antfly-migrate/tests -v
"""

import copy
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.binary = Path(os.environ["ANTFLY_MIGRATE_BIN"]).resolve()
        self.source = self.root / "source.sqlite"
        with sqlite3.connect(self.source) as db:
            db.execute(
                "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT, enabled INTEGER, payload TEXT, opaque BLOB, score REAL)"
            )
            for i in range(23):
                db.execute(
                    "INSERT INTO items VALUES(?,?,?,?,?,?)",
                    (
                        i if i < 22 else 9223372036854775807,
                        None
                        if i == 0
                        else f"hello 'quote' \\ newline\n unicode 🍃 {i}",
                        i % 2,
                        None
                        if i == 0
                        else "null"
                        if i == 1
                        else json.dumps(
                            [
                                {"n": i, "nested": [True, None]},
                                "JSON string",
                                True,
                                [9007199254740993, 1.25],
                            ][i % 4]
                        ),
                        bytes([0, 255, i]),
                        i / 2,
                    ),
                )
        self.plan = {
            "version": 1,
            "sources": [{"name": "state", "path": "source.sqlite"}],
            "destination": "output.aflite",
            "schema": {
                "statements": [
                    "CREATE TABLE items(id BIGINT PRIMARY KEY, name TEXT, enabled BOOLEAN NOT NULL, payload JSONB, opaque TEXT NOT NULL, score DOUBLE PRECISION NOT NULL)"
                ]
            },
            "tables": [
                {
                    "source": "state",
                    "query": "SELECT * FROM items ORDER BY id",
                    "target": "items",
                    "key": ["id"],
                    "columns": [
                        {
                            "name": "id",
                            "from": "id",
                            "type": "integer",
                            "nullable": False,
                        },
                        {"name": "name", "from": "name", "type": "text"},
                        {
                            "name": "enabled",
                            "from": "enabled",
                            "type": "boolean",
                            "nullable": False,
                        },
                        {"name": "payload", "from": "payload", "type": "json"},
                        {
                            "name": "opaque",
                            "from": "opaque",
                            "type": "text",
                            "transform": "base64",
                        },
                        {"name": "score", "from": "score", "type": "real"},
                    ],
                }
            ],
            "batch_rows": 3,
            "batch_bytes": 2048,
        }

    def run_plan(self, plan=None, dry_run=False):
        path = self.root / "plan.json"
        path.write_text(json.dumps(plan or self.plan))
        args = [str(self.binary), "migrate", "--plan", str(path)]
        if dry_run:
            args.append("--dry-run")
        return subprocess.run(
            args, check=False, cwd="/", capture_output=True, text=True, timeout=120
        )

    def assert_failure(self, error, plan=None):
        result = self.run_plan(plan)
        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertIn(error, result.stderr)
        self.assertFalse((self.root / "output.aflite").exists())
        self.assertEqual(list(self.root.glob("*.migrate-*")), [])

    def test_batches_exact_values_and_source_unchanged(self):
        before = hashlib.sha256(self.source.read_bytes()).digest()
        result = self.run_plan()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("verify items: 23 rows", result.stderr)
        self.assertIn("published", result.stderr)
        self.assertTrue((self.root / "output.aflite").is_file())
        self.assertEqual((self.root / "output.aflite").stat().st_mode & 0o777, 0o600)
        self.assertEqual(before, hashlib.sha256(self.source.read_bytes()).digest())
        self.assertEqual(list(self.root.glob("*.migrate-*")), [])
        # A second run cannot replace the completed output.
        before_output = (self.root / "output.aflite").read_bytes()
        result = self.run_plan()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("DestinationExists", result.stderr)
        self.assertEqual(before_output, (self.root / "output.aflite").read_bytes())

    def test_dry_run_and_schema_seed(self):
        result = self.run_plan(dry_run=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.root / "output.aflite").exists())
        # Publish an empty schema seed using an empty selection.
        seed_plan = copy.deepcopy(self.plan)
        seed_plan["destination"] = "seed.aflite"
        seed_plan["tables"][0]["query"] = "SELECT * FROM items WHERE 0"
        result = self.run_plan(seed_plan)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.plan["schema"] = {"seed": "seed.aflite"}
        result = self.run_plan()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_mixed_storage_class_rejected_before_copy(self):
        with sqlite3.connect(self.source) as db:
            db.execute("UPDATE items SET enabled='true' WHERE id=2")
        self.assert_failure("ExpectedBoolean")

    def test_duplicate_key(self):
        self.plan["tables"][0]["query"] = (
            "SELECT * FROM items UNION ALL SELECT * FROM items"
        )
        self.assert_failure("DuplicateSourceKey")

    def test_mutating_source_query(self):
        self.plan["tables"][0]["query"] = "DELETE FROM items RETURNING *"
        self.assert_failure("SqlitePrepareFailed")
        with sqlite3.connect(self.source) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM items").fetchone()[0], 23)

    def test_unknown_version_and_fields(self):
        self.plan["version"] = 2
        self.assert_failure("UnsupportedPlanVersion")
        self.plan["version"] = 1
        self.plan["typo"] = True
        self.assert_failure("UnknownField")

    def test_target_type_mismatch_never_publishes(self):
        self.plan["schema"]["statements"] = [
            "CREATE TABLE items(id BIGINT PRIMARY KEY, name BIGINT, enabled BOOLEAN, payload JSONB, opaque TEXT, score DOUBLE PRECISION)"
        ]
        self.assert_failure("TargetTypeMismatch")

    def test_oversized_row(self):
        with sqlite3.connect(self.source) as db:
            db.execute("UPDATE items SET name=? WHERE id=2", ("x" * 5000,))
        self.assert_failure("CellTooLarge")

    def test_dangling_destination_symlink(self):
        (self.root / "output.aflite").symlink_to(self.root / "missing")
        result = self.run_plan()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("DestinationExists", result.stderr)
        self.assertTrue((self.root / "output.aflite").is_symlink())

    def test_destination_constraint_violation_never_publishes(self):
        self.plan["schema"]["statements"] = [
            "CREATE TABLE items(id BIGINT PRIMARY KEY, name TEXT NOT NULL, enabled BOOLEAN, payload JSONB, opaque TEXT, score DOUBLE PRECISION)"
        ]
        self.assert_failure("AntflyOperationFailed")

    def test_wal_source_snapshot_survives_a_concurrent_writer(self):
        with sqlite3.connect(self.source) as db:
            db.execute("PRAGMA journal_mode=WAL")
            path = self.root / "plan.json"
            path.write_text(json.dumps(self.plan))
            process = subprocess.Popen(
                [str(self.binary), "migrate", "--plan", str(path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            def reap():
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=10)

            self.addCleanup(reap)
            first = process.stderr.readline()
            self.assertIn("preflight items", first)
            db.execute("UPDATE items SET name='changed after snapshot' WHERE id=2")
            db.execute("INSERT INTO items VALUES(100,'late row',1,'{}',x'ff',1.0)")
            db.commit()
            _, remaining = process.communicate(timeout=120)
            self.assertEqual(process.returncode, 0, first + remaining)
            self.assertIn("verify items: 23 rows", remaining)
            self.assertEqual(db.execute("SELECT count(*) FROM items").fetchone()[0], 24)

    def test_multi_source_foreign_keys_and_composite_key(self):
        with sqlite3.connect(self.root / "parents.sqlite") as db:
            db.execute("CREATE TABLE parents(id TEXT PRIMARY KEY)")
            db.execute("INSERT INTO parents VALUES ('parent')")
        with sqlite3.connect(self.root / "children.sqlite") as db:
            db.execute(
                "CREATE TABLE children(parent_id TEXT, position INTEGER, value TEXT)"
            )
            db.executemany(
                "INSERT INTO children VALUES ('parent',?,?)",
                [(0, "first"), (1, "second")],
            )
        plan = {
            "version": 1,
            "sources": [
                {"name": "p", "path": "parents.sqlite"},
                {"name": "c", "path": "children.sqlite"},
            ],
            "destination": "output.aflite",
            "schema": {
                "statements": [
                    "CREATE TABLE parents(id TEXT PRIMARY KEY)",
                    "CREATE TABLE children(parent_id TEXT NOT NULL REFERENCES parents(id), position BIGINT NOT NULL, value TEXT, PRIMARY KEY(parent_id,position))",
                ]
            },
            "tables": [
                {
                    "source": "p",
                    "query": "SELECT * FROM parents",
                    "target": "parents",
                    "key": ["id"],
                    "columns": [{"name": "id", "from": "id", "type": "text"}],
                },
                {
                    "source": "c",
                    "query": "SELECT * FROM children",
                    "target": "children",
                    "key": ["parent_id", "position"],
                    "columns": [
                        {"name": "parent_id", "from": "parent_id", "type": "text"},
                        {"name": "position", "from": "position", "type": "integer"},
                        {"name": "value", "from": "value", "type": "text"},
                    ],
                },
            ],
        }
        result = self.run_plan(plan)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("verify children: 2 rows", result.stderr)

    def test_nonempty_seed_target_rejected(self):
        seed_plan = copy.deepcopy(self.plan)
        seed_plan["destination"] = "seed.aflite"
        result = self.run_plan(seed_plan)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.plan["schema"] = {"seed": "seed.aflite"}
        self.assert_failure("TargetMustBeEmpty")

    def test_racing_publishers_cannot_replace_each_other(self):
        path = self.root / "plan.json"
        path.write_text(json.dumps(self.plan))
        processes = [
            subprocess.Popen(
                [str(self.binary), "migrate", "--plan", str(path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for _ in range(2)
        ]
        results = []
        for process in processes:
            _, stderr = process.communicate(timeout=120)
            results.append((process.returncode, stderr))
        self.assertEqual(sum(code == 0 for code, _ in results), 1, results)
        for code, stderr in results:
            if code != 0:
                self.assertTrue(
                    "PublishFailed" in stderr or "DestinationExists" in stderr, stderr
                )
        self.assertTrue((self.root / "output.aflite").is_file())
        self.assertEqual(list(self.root.glob("*.migrate-*")), [])


if __name__ == "__main__":
    if len(sys.argv) > 1 and Path(sys.argv[1]).is_file():
        os.environ["ANTFLY_MIGRATE_BIN"] = sys.argv.pop(1)
    unittest.main()
