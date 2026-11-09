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

"""Prevent partial compiler summaries from qualifying session isolation."""

import hashlib
import json
from pathlib import Path
import shutil
import sys
import tarfile
import tempfile
import threading
import unittest

import check_read_session_owner_cache as cache


def summary():
    return "\n".join(
        f"+- compile {kind} {name} debug native-native cached 1s"
        for kind, name in cache.ARTIFACTS.values()
    )


class SessionCompilerAttribution(unittest.TestCase):
    def fixture_parent(self):
        parent = cache.ZIG / ".zig-cache/tool-fixtures"
        parent.mkdir(parents=True, exist_ok=True)
        return parent

    def test_partial_cache_eviction_invalidates_without_recreating_marker(self):
        with tempfile.TemporaryDirectory(dir=self.fixture_parent()) as directory:
            root = Path(directory) / "cache"
            with cache.CacheLease((root,)) as lease:
                removed = root / "o"
                shutil.rmtree(removed)
                with self.assertRaises(cache.CacheEvicted):
                    lease.pulse()
                with self.assertRaises(cache.CacheEvicted):
                    lease.protect(Path(directory) / "another-cache")
                self.assertFalse(removed.exists())
                self.assertFalse((Path(directory) / "another-cache").exists())

    def test_running_build_is_terminated_and_diagnosed_when_cache_disappears(self):
        with tempfile.TemporaryDirectory(dir=self.fixture_parent()) as directory:
            root = Path(directory) / "zig"
            local = root / ".zig-cache"
            with cache.CacheLease((local,)) as lease:
                arguments = local / "args/command"
                arguments.parent.mkdir(exist_ok=True)
                arguments.write_text("--name fixture\n")
                timer = threading.Timer(0.2, lambda: shutil.rmtree(local / "o"))
                timer.start()
                try:
                    result = cache.run(
                        root,
                        [sys.executable, "-c", "import time; time.sleep(30)"],
                        Path(directory) / "build.log",
                        5,
                        lease,
                    )
                finally:
                    timer.join()
                self.assertTrue(result["cache_invalid"])
                self.assertEqual(result["interruption"], "CacheEvicted")
                self.assertIsNotNone(result["returncode"])
                self.assertNotEqual(result["returncode"], 0)
                self.assertLess(result["elapsed_s"], 5)
                diagnostic = cache.failure_diagnostics(
                    root, Path(directory) / "report.json", "fixture", lease
                )
                saved = json.loads(Path(diagnostic["cache_diagnostics"]).read_text())
                self.assertTrue(saved["lease_invalid"])
                self.assertEqual(len(saved["preserved_compiler_args"]), 1)
                self.assertEqual(
                    Path(saved["preserved_compiler_args"][0]).read_text(),
                    arguments.read_text(),
                )
                self.assertFalse((local / "o").exists())

    def test_source_fixture_cannot_substitute_for_independent_native_client(self):
        output = "\n".join(
            row
            for row in summary().splitlines()
            if "read-session-compiled-client-probe " not in row
        )
        states, errors = cache.compiler_states(output)
        self.assertEqual(states["source_fixture"], "cached")
        self.assertNotIn("session_client", states)
        self.assertTrue(errors)

    def test_ambiguous_repeated_compiler_result_is_rejected(self):
        output = summary() + "\n" + summary().splitlines()[0]
        states, errors = cache.compiler_states(output)
        self.assertNotIn("api_client", states)
        self.assertTrue(errors)

    def test_failed_compiler_is_not_misread_as_cached(self):
        output = summary().replace(
            "antfly-storage-kernel debug native-native cached",
            "antfly-storage-kernel debug native-native failure",
        )
        states, errors = cache.compiler_states(output)
        self.assertNotIn("kernel_client", states)
        self.assertTrue(errors)

    def test_capture_fence_implementation_is_private_but_handle_is_shared(self):
        self.assertEqual(
            set(cache.expected_states("shared_handle").values()), {"success"}
        )
        for role, state in cache.expected_states("capture_fence_state").items():
            self.assertEqual(state, "cached" if role.endswith("client") else "success")
        self.assertIn("capture_fence_state", cache.PROFILES["capture"])

    def test_publication_profile_distinguishes_private_holder_and_shared_capability(
        self,
    ):
        self.assertEqual(
            set(cache.expected_states("shared_publication_lease").values()), {"success"}
        )
        for role, state in cache.expected_states("publication_lease_state").items():
            self.assertEqual(state, "cached" if role.endswith("client") else "success")
        self.assertEqual(
            cache.PROFILES["publication"],
            ("publication_lease_state", "shared_publication_lease"),
        )
        relative, _ = cache.EDITS["publication_lease_state"]
        source = (cache.ZIG / relative).read_text()
        modified = cache.field_probe("publication_lease_state", source)
        self.assertIn("        cache_isolation_epoch: u64 = 0,", modified)

    def test_shared_reader_request_is_not_treated_as_private_state(self):
        self.assertEqual(
            set(cache.expected_states("shared_reader_request").values()), {"success"}
        )
        for role, state in cache.expected_states("reader_state").items():
            self.assertEqual(state, "cached" if role.endswith("client") else "success")

    def test_test_count_requires_complete_exact_summary(self):
        self.assertTrue(
            cache.linked_tests_passed("3 passed; 0 skipped; 0 failed; 0 leaked.", 3)
        )
        for output in (
            "13 passed; 0 skipped; 0 failed; 0 leaked.",
            "3 passed; 0 skipped; 1 failed; 0 leaked.",
            "3 passed; 0 skipped; 0 failed; 1 leaked.",
        ):
            self.assertFalse(cache.linked_tests_passed(output, 3))

    def test_snapshot_prunes_generated_trees_and_archives_explicit_inputs_only(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "zig"
            source = root / "pkg/owner/root.zig"
            source.parent.mkdir(parents=True)
            source.write_text("pub const value = 1;\n")
            for excluded in (
                ".zig-cache",
                "zig-out-old",
                "zig-pkg",
                "target",
                "node_modules",
            ):
                generated = source.parent / excluded / "generated.zig"
                generated.parent.mkdir()
                generated.write_text("must not enter authored snapshot\n")
            self.assertEqual(list(cache.authored_files(root, {".zig"})), [source])
            schema = parent / "spec.yaml"
            schema.write_text("openapi: 3.0.0\n")
            report = {
                "source_snapshot_sha256": {
                    "pkg/owner/root.zig": hashlib.sha256(
                        source.read_bytes()
                    ).hexdigest()
                },
                "repository_input_snapshot_sha256": {
                    "spec.yaml": hashlib.sha256(schema.read_bytes()).hexdigest()
                },
            }
            receipt = cache.retain_source_snapshot(root, parent / "result.json", report)
            archive_path = Path(receipt["source_snapshot_archive"])
            with tarfile.open(archive_path) as archive:
                self.assertEqual(
                    set(archive.getnames()),
                    {"zig/pkg/owner/root.zig", "spec.yaml", "snapshot-manifest.json"},
                )
                self.assertEqual(
                    archive.extractfile("zig/pkg/owner/root.zig").read(),
                    source.read_bytes(),
                )
            self.assertEqual(
                receipt["source_snapshot_archive_sha256"],
                hashlib.sha256(archive_path.read_bytes()).hexdigest(),
            )


if __name__ == "__main__":
    unittest.main()
