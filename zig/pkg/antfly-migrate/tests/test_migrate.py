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
import ctypes
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

    def read_aflite(self, path, statement):
        class Buffer(ctypes.Structure):
            _fields_ = [("ptr", ctypes.c_void_p), ("len", ctypes.c_size_t)]

        directory = Path(os.environ["ANTFLY_MIGRATE_LIB"])
        lib = ctypes.CDLL(
            str(
                directory
                / ("libantfly.dylib" if sys.platform == "darwin" else "libantfly.so")
            )
        )
        lib.antfly_lite_open_readonly.argtypes = [
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.antfly_db_close.argtypes = [ctypes.c_void_p]
        lib.antfly_db_sql_json.argtypes = [
            ctypes.c_void_p,
            Buffer,
            ctypes.POINTER(Buffer),
        ]
        lib.antfly_buffer_free.argtypes = [ctypes.POINTER(Buffer)]
        handle = ctypes.c_void_p()
        self.assertEqual(
            lib.antfly_lite_open_readonly(str(path).encode(), ctypes.byref(handle)), 0
        )
        out = Buffer()
        try:
            request = json.dumps({"statement": statement}).encode()
            memory = ctypes.create_string_buffer(request)
            code = lib.antfly_db_sql_json(
                handle,
                Buffer(ctypes.cast(memory, ctypes.c_void_p), len(request)),
                ctypes.byref(out),
            )
            self.assertEqual(code, 0)
            return json.loads(ctypes.string_at(out.ptr, out.len))
        finally:
            lib.antfly_buffer_free(ctypes.byref(out))
            lib.antfly_db_close(handle)

    def test_upsert_preserves_unmapped_columns_extra_rows_and_seed(self):
        seed_plan = copy.deepcopy(self.plan)
        seed_plan["destination"] = "seed.aflite"
        seed_plan["schema"]["statements"][0] = (
            seed_plan["schema"]["statements"][0]
            .replace(
                "enabled BOOLEAN NOT NULL", "enabled BOOLEAN NOT NULL DEFAULT false"
            )
            .replace("opaque TEXT NOT NULL", "opaque TEXT NOT NULL DEFAULT ''")
            .replace(
                "score DOUBLE PRECISION NOT NULL",
                "score DOUBLE PRECISION NOT NULL DEFAULT 0",
            )
            .removesuffix(")")
            + ", next_ordinal BIGINT NOT NULL DEFAULT 73)"
        )
        result = self.run_plan(seed_plan)
        self.assertEqual(result.returncode, 0, result.stderr)
        seed = self.root / "seed.aflite"
        before = hashlib.sha256(seed.read_bytes()).digest()
        with sqlite3.connect(self.source) as db:
            db.execute("UPDATE items SET name='captured metadata' WHERE id=2")
            db.execute("DELETE FROM items WHERE id=3")
        self.plan["schema"] = {"seed": "seed.aflite"}
        table = self.plan["tables"][0]
        table["write_mode"] = "upsert"
        table["columns"] = table["columns"][:2]
        result = self.run_plan()
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = self.read_aflite(
            self.root / "output.aflite",
            "SELECT name,next_ordinal,opaque FROM items WHERE id=2",
        )["rows"]
        self.assertEqual(rows, [["captured metadata", "73", "AP8C"]])
        count = self.read_aflite(
            self.root / "output.aflite", "SELECT count(*) FROM items"
        )["rows"][0][0]
        self.assertEqual(int(count), 23)
        self.assertEqual(before, hashlib.sha256(seed.read_bytes()).digest())

    def test_wide_tables_split_at_the_sql_parameter_limit(self):
        with sqlite3.connect(self.root / "wide.sqlite") as db:
            columns = ["id INTEGER PRIMARY KEY"] + [
                f"c{i} INTEGER NOT NULL" for i in range(40)
            ]
            db.execute("CREATE TABLE wide(" + ",".join(columns) + ")")
            db.executemany(
                "INSERT INTO wide VALUES(" + ",".join("?" for _ in range(41)) + ")",
                [tuple([i] * 41) for i in range(50)],
            )
        plan = {
            "version": 1,
            "sources": [{"name": "source", "path": "wide.sqlite"}],
            "destination": "output.aflite",
            "schema": {
                "statements": [
                    "CREATE TABLE wide(id BIGINT PRIMARY KEY,"
                    + ",".join(f"c{i} BIGINT NOT NULL" for i in range(40))
                    + ")"
                ]
            },
            "tables": [
                {
                    "source": "source",
                    "query": "SELECT * FROM wide",
                    "target": "wide",
                    "key": ["id"],
                    "columns": [
                        {"name": name, "from": name, "type": "integer"}
                        for name in ["id"] + [f"c{i}" for i in range(40)]
                    ],
                }
            ],
            "batch_rows": 100,
        }
        result = self.run_plan(plan)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("verify wide: 50 rows", result.stderr)

    def test_verify_only_detects_changed_values_without_publishing(self):
        result = self.run_plan()
        self.assertEqual(result.returncode, 0, result.stderr)
        seed = self.root / "output.aflite"
        before = hashlib.sha256(seed.read_bytes()).digest()
        plan = copy.deepcopy(self.plan)
        plan["schema"] = {"seed": "output.aflite"}
        plan["destination"] = "never-published.aflite"
        path = self.root / "verify.json"
        path.write_text(json.dumps(plan))
        args = [str(self.binary), "verify", "--plan", str(path)]
        result = subprocess.run(
            args, check=False, capture_output=True, text=True, timeout=120
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.root / "never-published.aflite").exists())
        with sqlite3.connect(self.source) as db:
            db.execute("UPDATE items SET name='a changed value' WHERE id=2")
        result = subprocess.run(
            args, check=False, capture_output=True, text=True, timeout=120
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("TargetValueMismatch", result.stderr)
        self.assertFalse((self.root / "never-published.aflite").exists())
        self.assertEqual(before, hashlib.sha256(seed.read_bytes()).digest())


if __name__ == "__main__":
    if len(sys.argv) > 1 and Path(sys.argv[1]).is_file():
        os.environ["ANTFLY_MIGRATE_BIN"] = sys.argv.pop(1)
    unittest.main()
