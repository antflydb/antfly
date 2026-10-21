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

import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location(
    "previous_reader", Path(__file__).with_name("prepare_lite_previous_reader.py")
)
previous_reader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(previous_reader)


class PreviousReaderBuildTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[2]
        scratch = root / "zig/.zig-cache"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.previous = Path(self.temp.name)
        self.build = self.previous / "zig/embedded.build.zig"
        self.build.parent.mkdir()
        # Exact file at e8666a5b758e1e588a6b2b74b260212e984b40a9.
        self.original = (
            root / "scripts/testdata/lite_previous_reader/embedded.build.zig"
        ).read_bytes()
        self.build.write_bytes(self.original)
        self.fixed = self.original.replace(
            b"    shared.build_info.link(embedded.libantfly_link_mod);\n", b"", 1
        )

    def test_fixture_matches_pinned_original_and_official_one_line_fix(self):
        self.assertEqual(
            previous_reader.REVISION, "e8666a5b758e1e588a6b2b74b260212e984b40a9"
        )
        self.assertEqual(
            hashlib.sha256(self.original).hexdigest(), previous_reader.BUILD_SHA256
        )
        self.assertEqual(
            hashlib.sha256(self.fixed).hexdigest(), previous_reader.FIXED_BUILD_SHA256
        )
        self.assertEqual(
            len(self.original.splitlines()) - len(self.fixed.splitlines()), 1
        )
        self.assertIn(b"shared.build_info.link(lite_module);", self.fixed)

    def test_repairs_only_build_composition(self):
        storage = self.previous / "zig/pkg/antfly-embedded/src/storage/reader.zig"
        storage.parent.mkdir(parents=True)
        storage.write_bytes(b"historical reader bytes must not change")
        previous_reader.prepare_previous_reader_build(self.previous)
        self.assertEqual(self.build.read_bytes(), self.fixed)
        self.assertEqual(
            storage.read_bytes(), b"historical reader bytes must not change"
        )

    def test_existing_repaired_worktree_is_idempotent(self):
        previous_reader.prepare_previous_reader_build(self.previous)
        with patch.object(Path, "write_bytes") as write:
            previous_reader.prepare_previous_reader_build(self.previous)
        write.assert_not_called()
        self.assertEqual(self.build.read_bytes(), self.fixed)

    def test_rejects_unexpected_original_or_repaired_contents_without_writing(self):
        for contents in (
            self.original + b"// unexpected\n",
            self.fixed + b"// unexpected\n",
        ):
            with self.subTest(contents=contents[-30:]):
                self.build.write_bytes(contents)
                with self.assertRaisesRegex(RuntimeError, "unexpected pinned"):
                    previous_reader.prepare_previous_reader_build(self.previous)
                self.assertEqual(self.build.read_bytes(), contents)

    def test_rejects_inconsistent_expected_repair_without_writing(self):
        with patch.object(previous_reader, "FIXED_BUILD_SHA256", "0" * 64):
            with self.assertRaisesRegex(RuntimeError, "repair result"):
                previous_reader.prepare_previous_reader_build(self.previous)
        self.assertEqual(self.build.read_bytes(), self.original)

    def test_missing_build_file_fails_closed(self):
        self.build.unlink()
        with self.assertRaises(FileNotFoundError):
            previous_reader.prepare_previous_reader_build(self.previous)
        self.assertFalse(self.build.exists())

    def test_main_repairs_pinned_reader_before_build_without_changing_flags(self):
        root = self.previous / "repo"
        baseline = root / ".worktrees/reader-baseline-e8666a5b75"
        build = baseline / "zig/embedded.build.zig"
        build.parent.mkdir(parents=True)
        build.write_bytes(self.original)
        builds = []

        def run(args, **kwargs):
            if args[:2] == ["git", "cat-file"]:
                self.assertEqual(args[3], previous_reader.REVISION + "^{commit}")
            else:
                builds.append(args)
                self.assertEqual(build.read_bytes(), self.fixed)
                self.assertEqual(kwargs["cwd"], baseline / "zig")
                self.assertEqual(kwargs["env"]["TZ"], "UTC0")
                self.assertEqual(
                    kwargs["env"]["ZIG_GLOBAL_CACHE_DIR"],
                    str(baseline / "zig/.zig-global-cache"),
                )
                extension = (
                    ".dylib" if previous_reader.sys.platform == "darwin" else ".so"
                )
                library = baseline / "zig/zig-out/lib" / f"libantfly{extension}"
                library.parent.mkdir(parents=True)
                library.write_bytes(b"historical library fixture")
            return subprocess.CompletedProcess(args, 0)

        with (
            patch.object(
                previous_reader, "__file__", str(root / "scripts/ci/prepare.py")
            ),
            patch.object(previous_reader.subprocess, "run", side_effect=run),
            patch.object(
                previous_reader.subprocess,
                "check_output",
                return_value=previous_reader.REVISION + "\n",
            ),
            patch("builtins.print"),
        ):
            previous_reader.main()
        self.assertEqual(
            builds,
            [
                [
                    previous_reader.sys.executable,
                    "tools/run_bounded_zig_build.py",
                    "build",
                    "capi",
                    "-Dembedded-only=true",
                    "-Dlite-local-inference-runtime=false",
                    "-Dmetal=false",
                    "-Dcuda=false",
                    "-j1",
                ]
            ],
        )

    def test_main_rejects_wrong_revision_before_repair_or_build(self):
        root = self.previous / "repo"
        baseline = root / ".worktrees/reader-baseline-e8666a5b75"
        baseline.mkdir(parents=True)
        with (
            patch.object(
                previous_reader, "__file__", str(root / "scripts/ci/prepare.py")
            ),
            patch.object(
                previous_reader.subprocess,
                "run",
                return_value=subprocess.CompletedProcess([], 0),
            ) as run,
            patch.object(
                previous_reader.subprocess, "check_output", return_value="wrong\n"
            ),
            patch.object(previous_reader, "prepare_previous_reader_build") as repair,
        ):
            with self.assertRaisesRegex(RuntimeError, "worktree head"):
                previous_reader.main()
        self.assertEqual(run.call_count, 1)
        repair.assert_not_called()


class LiteEmbeddingBundleTests(unittest.TestCase):
    def test_ci_bundle_matches_runtime_registry_pins_and_profile(self):
        root = Path(__file__).resolve().parents[2]
        receipt = json.loads(
            (
                root / "scripts/testdata/eval_extract/qwen_embedding_bundle.json"
            ).read_text()
        )
        catalog = (root / receipt["source_catalog"]).read_text()
        for name, pin in receipt["model_files"].items():
            match = re.search(
                r'\.path = "'
                + re.escape(name)
                + r'",\s*\.size = ([0-9_]+),\s*\.sha256 = "([a-f0-9]+)"',
                catalog,
            )
            self.assertIsNotNone(match, name)
            self.assertEqual(int(match[1].replace("_", "")), pin["size_bytes"])
            self.assertEqual(match[2], pin["sha256"])
            self.assertIn('"' + pin.get("model_id", receipt["model_id"]) + '"', catalog)
            self.assertIn('"' + pin.get("revision", receipt["revision"]) + '"', catalog)
        digest = hashlib.sha256(receipt["model_manifest"].encode()).hexdigest()
        self.assertEqual(receipt["model_manifest_sha256"], digest)
        self.assertIn('"' + digest + '"', catalog)


if __name__ == "__main__":
    unittest.main()
