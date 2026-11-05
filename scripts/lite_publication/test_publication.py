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

"""Run with ANTFLY_BIN pointing at this checkout's native CLI."""

import gzip
import hashlib
import json
import os
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from . import contract, native
from .store import Destination

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "zig/testdata/publication-v1"
CLI = ROOT / "scripts/antfly-publication"


def worker(*args, error=None, preexec=None):
    result = subprocess.run(
        [str(CLI), *map(str, args)],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
        preexec_fn=preexec,
    )
    if error:
        if not result.returncode:
            raise AssertionError(f"Expected {error}, received {result.stdout}")
        problem = json.loads(result.stderr)
        if problem["error"] != error:
            raise AssertionError(problem)
        return problem
    if result.returncode:
        raise AssertionError(result.stderr)
    return json.loads(result.stdout)


class Contract(unittest.TestCase):
    def test_frozen_profile(self):
        for count, name in enumerate(("zero", "partial", "full")):
            corpus = native.load_json(FIXTURES / (name + ".json"))
            rows = dict(contract.rows(corpus))
            self.assertEqual(rows["publication"]["coverage"]["semantic_chunks"], count)
            self.assertEqual(sum(k.startswith("c:") for k in rows), 2)

    def test_refuses_mixed_revision_or_wrong_task(self):
        for field, value in [
            ("chunk_id", "c:" + "0" * 64),
            ("task", "RETRIEVAL_QUERY"),
            ("model", "other"),
            ("values", [1.0, 2.0]),
            ("values", [0] * 768),
        ]:
            with self.subTest(field=field):
                corpus = native.load_json(FIXTURES / "partial.json")
                corpus["documents"][0]["chunks"][0]["embedding"][field] = value
                with self.assertRaises(contract.Error):
                    list(contract.rows(corpus))

    def test_provenance_locators_and_boundaries(self):
        for field, value in [
            ("content", "a new revision"),
            ("source_url", "https://example.org"),
            ("source_path", "/Users/me/plan.pdf"),
            ("credentials", "no"),
        ]:
            corpus = native.load_json(FIXTURES / "zero.json")
            corpus["documents"][0][field] = value
            with self.assertRaises(contract.Error):
                list(contract.rows(corpus))
        for loc in (
            {"kind": "text"},
            {"kind": "slide", "slide": 2},
            {"kind": "sheet", "sheet": "Data"},
        ):
            contract.locator(loc)
        with self.assertRaises(contract.Error):
            contract.locator(
                {"kind": "rows", "sheet": "Data", "row_start": 2, "row_end": 1}
            )
        corpus = native.load_json(FIXTURES / "zero.json")
        doc = corpus["documents"][0]
        doc["chunks"][0]["end_byte"] = 4  # bisects the UTF-8 é in Café
        with self.assertRaises(UnicodeError):
            list(contract.rows(corpus))


@unittest.skipUnless(
    os.environ.get("ANTFLY_BIN"), "set ANTFLY_BIN to run real Lite fixtures"
)
class Runtime(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.base = Path(cls.temp.name)
        cls.artifacts = {}
        for i, name in enumerate(("zero", "partial", "full")):
            file = cls.base / (name + ".aflite")
            info = worker(
                "build", "--input", FIXTURES / (name + ".json"), "--out", file
            )
            assert info["coverage"]["semantic_chunks"] == i
            cls.artifacts[name] = file, info["artifact_sha256"]

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=self.base)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "serving"
        self.args = ["--root", self.root, "--destination", "instance-1"]

    def artifact(self, op):
        return self.root / ("op-" + hashlib.sha256(op.encode()).hexdigest() + ".aflite")

    def cmd(self, action, *args, **kwargs):
        return worker(action, *self.args, *args, **kwargs)

    def stage(self, name, op, expected, **kwargs):
        path, checksum = self.artifacts[name]
        return self.cmd(
            "prepare",
            "--operation",
            op,
            "--expected-generation",
            expected,
            "--artifact",
            path,
            "--digest",
            checksum,
            **kwargs,
        )

    def publish(self, name, op, expected):
        self.stage(name, op, expected)
        self.cmd("validate", "--operation", op)
        return self.cmd("activate", "--operation", op)

    def query(self, generation, **query):
        inp = self.root.parent / "query.json"
        inp.write_text(json.dumps(query))
        return self.cmd("search", "--generation", generation, "--input", inp)["hits"]

    def test_golden_native_artifacts(self):
        inventory = native.load_json(FIXTURES / "artifacts.json")
        query = native.load_json(FIXTURES / "solar-query.json")
        for name, expected in inventory.items():
            with self.subTest(name=name):
                path = self.root.parent / (name + ".aflite")
                path.write_bytes(
                    gzip.decompress((FIXTURES / (name + ".aflite.gz")).read_bytes())
                )
                report = worker(
                    "inspect",
                    "--artifact",
                    path,
                    "--digest",
                    expected["artifact_sha256"],
                )
                self.assertEqual({k: report[k] for k in expected}, expected)
                hits = native.search(path, query)
                self.assertEqual(len(hits), expected["coverage"]["semantic_chunks"])
                if hits:
                    self.assertEqual(
                        hits[0]["_source"]["document_id"], "drive-fixture-1"
                    )

    def test_owner_advances_while_snapshot_is_built(self):
        # Pause at the real native init after freezing the producer input.
        # An owner can install newer completed work while that snapshot is in flight.
        from unittest.mock import patch

        source = self.root.parent / "working.json"
        shutil.copyfile(FIXTURES / "zero.json", source)
        native_lite = native.lite

        def advance(command, *args, **kwargs):
            if command == "init":
                next_source = source.with_suffix(".next")
                shutil.copyfile(FIXTURES / "full.json", next_source)
                os.replace(next_source, source)
            return native_lite(command, *args, **kwargs)

        with patch.object(native, "lite", side_effect=advance):
            frozen = native.build(source, self.root.parent / "frozen.aflite")
        self.assertEqual(frozen["coverage"]["semantic_chunks"], 0)
        later = native.build(source, self.root.parent / "later.aflite")
        self.assertEqual(later["coverage"]["semantic_chunks"], 2)
        self.assertNotEqual(later["artifact_sha256"], frozen["artifact_sha256"])

    def test_manual_publication_and_reader(self):
        q = native.load_json(FIXTURES / "solar-query.json")
        for previous, name in enumerate(("zero", "partial", "full")):
            generation = previous + 1
            with self.subTest(name=name):
                if previous:
                    self.assertEqual(
                        self.cmd("status")["publication"]["coverage"][
                            "semantic_chunks"
                        ],
                        previous - 1,
                    )
                info = self.publish(name, name, previous)
                self.assertEqual(info["generation"], generation)
                self.assertEqual(
                    info["publication"]["coverage"]["semantic_chunks"], previous
                )
                self.assertEqual(
                    self.query(generation, text="solar")[0]["_source"]["locator"][
                        "page"
                    ],
                    1,
                )
                self.assertEqual(len(self.query(generation, **q)), previous)
                if previous == 2:
                    self.assertEqual(
                        self.query(generation, **q)[0]["_source"]["document_id"],
                        "drive-fixture-1",
                    )
                row = self.cmd(
                    "read", "--generation", generation, "--key", "d:drive-fixture-1"
                )
                self.assertEqual(row["_source"]["content"], "Café solar budget.")
                self.cmd(
                    "read",
                    "--generation",
                    previous,
                    "--key",
                    "d:drive-fixture-1",
                    error="stale_generation",
                )
                on_disk = self.artifact(name)
                self.assertEqual(native.sha256_file(on_disk), self.artifacts[name][1])
                status = native.lite("status", on_disk)
                self.assertEqual(
                    status["stats"]["enrichment"]["embed_items_started"], 0
                )
                self.assertFalse(status["stats"]["enrichment"]["enabled"])
        inp = self.root.parent / "bad-query.json"
        inp.write_text(
            json.dumps(dict(q, embedding=dict(contract.PROFILE, model="other")))
        )
        self.cmd(
            "search", "--generation", 3, "--input", inp, error="incompatible_model"
        )
        self.assertTrue(self.query(3, text="orbital"))

    def test_bound_cursor_and_foreign_destination(self):
        self.publish("full", "A", 0)
        inp = self.root.parent / "paged.json"
        query = {
            "vector": [1] + [0] * 767,
            "embedding": contract.PROFILE,
            "limit": 2,
            "page_size": 1,
        }
        inp.write_text(json.dumps(query))
        first = self.cmd("search", "--generation", 1, "--input", inp)
        query["cursor"] = first["next_cursor"]
        inp.write_text(json.dumps(query))
        second = self.cmd("search", "--generation", 1, "--input", inp)
        self.assertNotEqual(first["hits"][0]["_id"], second["hits"][0]["_id"])
        self.publish("zero", "a", 1)
        self.cmd("search", "--generation", 2, "--input", inp, error="stale_generation")
        worker(
            "status",
            "--root",
            self.root,
            "--destination",
            "unrelated",
            error="destination_mismatch",
        )
        foreign = self.root.parent / "foreign"
        foreign.mkdir(mode=0o700)
        (foreign / "instance.aflite").write_bytes(b"unrelated")
        worker(
            "status",
            "--root",
            foreign,
            "--destination",
            "instance-1",
            error="destination_mismatch",
        )
        self.assertEqual(list(foreign.iterdir()), [foreign / "instance.aflite"])

    def test_cancel_and_disk_or_checksum_failure_preserve_old_copy(self):
        self.publish("zero", "first", 0)
        self.stage("full", "wrong-generation", 0, error="stale_generation")
        source, _ = self.artifacts["full"]
        self.cmd(
            "prepare",
            "--operation",
            "bad",
            "--expected-generation",
            1,
            "--artifact",
            source,
            "--digest",
            "a" * 64,
            error="digest_mismatch",
        )
        self.cmd("cancel", "--operation", "bad")

        def pressure():
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
            resource.setrlimit(resource.RLIMIT_FSIZE, (65536, 65536))

        self.stage("full", "retry", 1, error="runtime_error", preexec=pressure)
        self.assertTrue(self.query(1, text="solar"))
        self.stage("full", "retry", 1)
        self.cmd("validate", "--operation", "retry")
        self.assertEqual(
            self.cmd("cancel", "--operation", "retry")["state"], "cancelled"
        )
        self.cmd("activate", "--operation", "retry", error="invalid_state")
        self.stage("partial", "retry", 1, error="operation_conflict")
        self.assertEqual(self.cmd("cleanup")["generation"], 1)
        self.assertFalse(self.artifact("retry").exists())

    def test_staged_file_changed_after_validation(self):
        self.publish("zero", "first", 0)
        self.stage("full", "op", 1)
        self.cmd("validate", "--operation", "op")
        path = self.artifact("op")
        path.chmod(0o600)
        with path.open("r+b") as f:
            f.truncate(4096)
        path.chmod(0o400)
        self.cmd("activate", "--operation", "op", error="storage_error")
        self.assertTrue(self.query(1, text="solar"))

    def test_process_death_before_and_after_commit(self):
        self.publish("zero", "first", 0)

        def interrupt(where):
            code = """
import os,sys
from pathlib import Path
from lite_publication import store
s=store.Destination(Path(sys.argv[1]),'instance-1')
if sys.argv[2]=='staging':
    store.os.replace=lambda *args: os._exit(91)
    s.prepare('op',1,sys.argv[4],Path(sys.argv[3]))
else:
    s.status=lambda *args: os._exit(91)
    s.activate('op')
"""
            source, checksum = self.artifacts["full"]
            result = subprocess.run(
                [sys.executable, "-c", code, self.root, where, source, checksum],
                check=False,
                env={**os.environ, "PYTHONPATH": str(CLI.parent)},
            )
            self.assertEqual(result.returncode, 91)

        interrupt("staging")
        self.assertEqual(self.cmd("status", "--operation", "op")["state"], "preparing")
        self.assertEqual(self.cmd("cleanup")["generation"], 1)
        self.stage("full", "op", 1)
        self.cmd("validate", "--operation", "op")
        self.assertTrue(self.query(1, text="solar"))
        interrupt("committed")
        for action in ("status", "cancel", "activate"):
            receipt = self.cmd(action, "--operation", "op")
            self.assertEqual(
                (receipt["state"], receipt["committed_generation"]), ("committed", 2)
            )
        self.assertTrue(self.query(2, text="solar"))
        self.assertEqual(self.cmd("cleanup")["generation"], 2)
        self.assertFalse(self.artifact("first").exists())

    def test_concurrent_updates_and_pinned_reader(self):
        self.publish("zero", "first", 0)

        def prepare(op):
            try:
                return self.stage("partial", op, 1)["state"]
            except AssertionError as e:
                return json.loads(str(e))["error"]

        with ThreadPoolExecutor(2) as threads:
            outcome = list(threads.map(prepare, ("a", "b")))
        self.assertEqual(sorted(outcome), ["operation_busy", "prepared"])
        op = ("a", "b")[outcome.index("prepared")]
        self.cmd("validate", "--operation", op)
        store = Destination(self.root, "instance-1")
        self.addCleanup(store.db.close)
        with store.reader(1) as pinned:
            self.cmd("activate", "--operation", op)
            self.cmd("cleanup", error="operation_busy")
            self.assertEqual(
                native.lite("lookup", pinned, "--key", "publication")["_source"][
                    "coverage"
                ]["semantic_chunks"],
                0,
            )
        self.cmd("cleanup")
        self.assertEqual(
            self.cmd("status", "--operation", "first")["committed_generation"], 1
        )

    def test_native_rejections_preserve_generation(self):
        self.publish("zero", "first", 0)
        for defect in (
            "truncated",
            "model",
            "schema",
            "extra-index",
            "control-row",
            "producer",
            "vector",
            "coverage",
        ):
            with self.subTest(defect=defect):
                candidate = self.root.parent / (defect + ".aflite")
                shutil.copyfile(self.artifacts["partial"][0], candidate)
                if defect == "truncated":
                    with candidate.open("r+b") as stream:
                        stream.truncate(candidate.stat().st_size // 2)
                elif defect == "schema":
                    native.lite(
                        "schema set",
                        candidate,
                        body={"version": 1, "default_type": "doc"},
                    )
                elif defect == "control-row":
                    native.lite(
                        "batch",
                        candidate,
                        body={
                            "inserts": {"personal-session": {"opaque": "fixture"}},
                            "sync_level": "full_index",
                        },
                    )
                elif defect == "extra-index":
                    native.lite(
                        "index create",
                        candidate,
                        body={
                            "name": "extra",
                            "kind": "full_text",
                            "config_json": "{}",
                        },
                    )
                elif defect == "producer":
                    native.lite(
                        "enrichment create",
                        candidate,
                        body={
                            "name": "document_chunkvec_v1",
                            "kind": "embedding",
                            "field": "content",
                            "expected_dims": 768,
                            "producer_json": json.dumps(
                                {
                                    "provider": "antfly",
                                    "model": contract.PROFILE["model"],
                                }
                            ),
                        },
                    )
                else:
                    if defect == "vector":
                        key, row = next(
                            (k, r)
                            for k, r in native.scan(candidate)
                            if r.get("vector_sha256")
                        )
                        del row["_artifacts"]
                        row["_embeddings"][contract.DENSE] = [0, 1] + [0] * 766
                    else:
                        key = "publication"
                        row = native.lite("lookup", candidate, "--key", key)["_source"]
                        if defect == "model":
                            row["embedding"]["model"] = "other"
                        else:
                            row["coverage"]["semantic_chunks"] += 1
                    native.lite(
                        "batch",
                        candidate,
                        body={"inserts": {key: row}, "sync_level": "full_index"},
                    )
                self.cmd(
                    "prepare",
                    "--operation",
                    defect,
                    "--expected-generation",
                    1,
                    "--artifact",
                    candidate,
                    "--digest",
                    native.sha256_file(candidate),
                )
                result = subprocess.run(
                    [CLI, "validate", *self.args, "--operation", defect],
                    check=False,
                    capture_output=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(
                    self.cmd("status", "--operation", defect)["state"], "prepared"
                )
                self.cmd("activate", "--operation", defect, error="invalid_state")
                self.cmd("cancel", "--operation", defect)
                self.assertTrue(self.query(1, text="solar"))
