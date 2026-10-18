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

import importlib.util
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "deps", Path(__file__).with_name("prepare_zig_dependencies.py")
)
deps = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deps)


class DependencyTests(unittest.TestCase):
    def test_inventory_covers_all_remote_project_packages(self):
        manifest = deps.load_manifest()
        expected = {
            (url, p["hash"])
            for p in manifest["packages"]
            for url in [p["url"], *p.get("declared_urls", [])]
        }
        root = Path(__file__).resolve().parents[2]
        observed = set()
        for path in (root / "zig").rglob("build.zig.zon"):
            if ".zig-cache" in path.parts:
                continue
            observed.update(
                re.findall(
                    r'\.url\s*=\s*"([^"]+)"\s*,\s*\.hash\s*=\s*"([^"]+)"',
                    path.read_text(),
                )
            )
        self.assertTrue(observed <= expected)

    def test_mismatched_mirror_falls_back_to_verified_upstream(self):
        manifest = {
            "mirror": "https://mirror.test",
            "packages": [
                {"hash": "expected", "url": "https://upstream.test/archive.tar.gz"}
            ],
        }
        calls = []

        def fetch(zig, source, identity, target):
            calls.append(source)
            if source.startswith("https://mirror"):
                raise ValueError("hash mismatch")
            (target / identity).mkdir()
            return target / identity

        with tempfile.TemporaryDirectory() as temp:
            cache = Path(temp)
            with patch.object(deps, "fetch_verified", side_effect=fetch):
                deps.prepare("zig", manifest, cache, cache / "missing-seed")
            self.assertEqual(len(calls), 2)
            self.assertFalse((cache / "p/wrong-hash").exists())
            self.assertTrue((cache / "p/expected").is_dir())

    def test_seed_only_never_contacts_upstream(self):
        manifest = {
            "mirror": "https://mirror.test",
            "packages": [
                {"hash": "expected", "url": "https://upstream.test/archive.tar.gz"}
            ],
        }
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(
                deps, "fetch_verified", side_effect=OSError("offline")
            ) as fetch:
                with self.assertRaises(RuntimeError):
                    deps.prepare("zig", manifest, Path(temp), Path(temp), True)
                self.assertEqual(fetch.call_count, 1)

    def test_fetch_rejects_wrong_zig_hash(self):
        with (
            tempfile.TemporaryDirectory() as temp,
            patch.object(
                deps.subprocess,
                "run",
                return_value=subprocess.CompletedProcess([], 0, "wrong\n", ""),
            ),
        ):
            with self.assertRaises(ValueError):
                deps.fetch_verified(
                    "zig", "/seed/archive.tar.gz", "expected", Path(temp) / "p"
                )

    def test_archive_cache_hit_never_fetches_or_recompiles_driver(self):
        import tarfile

        manifest = {
            "mirror": "https://mirror.test",
            "packages": [
                {"hash": "expected", "url": "https://upstream.test/archive.tar.gz"}
            ],
        }
        with tempfile.TemporaryDirectory() as temp:
            cache = Path(temp)
            (cache / "p").mkdir()
            import io

            with tarfile.open(cache / "p/expected.tar.gz", "w:gz") as archive:
                data = b".{ .dependencies = .{} }"
                member = tarfile.TarInfo("expected/build.zig.zon")
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
            with patch.object(deps, "fetch_verified") as fetch:
                deps.prepare("zig", manifest, cache)
            fetch.assert_not_called()

    def test_archived_transitive_dependencies_require_inventory(self):
        import io
        import tarfile

        text = b'.{ .dependencies = .{ .nested = .{ .url = "git+https://upstream.test/nested#commit", .hash = "nested-hash" }, }, }'
        with tempfile.TemporaryDirectory() as temp:
            archive = Path(temp) / "parent.tar.gz"
            with tarfile.open(archive, "w:gz") as tar:
                member = tarfile.TarInfo("parent/build.zig.zon")
                member.size = len(text)
                tar.addfile(member, io.BytesIO(text))
            with self.assertRaises(ValueError):
                deps.check_remote_declarations(archive, {"packages": []})
            deps.check_remote_declarations(
                archive,
                {
                    "packages": [
                        {
                            "url": "https://upstream.test/nested.tar.gz",
                            "declared_urls": [
                                "git+https://upstream.test/nested#commit"
                            ],
                            "hash": "nested-hash",
                        }
                    ]
                },
            )

    @unittest.skipUnless(
        os.environ.get("ANTFLY_TEST_ZIG"), "requires the pinned Zig compiler"
    )
    def test_admitted_archive_survives_a_real_build_cache_read(self):
        import io
        import tarfile

        zig = os.environ["ANTFLY_TEST_ZIG"]
        repo = Path(__file__).resolve().parents[2]
        global_cache = Path(
            os.environ.get("ZIG_GLOBAL_CACHE_DIR", repo / "zig/.zig-cache/global")
        ).resolve()
        scratch_root = repo / "zig/.zig-cache/tmp"
        scratch_root.mkdir(parents=True, exist_ok=True)
        env = {**os.environ, "ZIG_GLOBAL_CACHE_DIR": str(global_cache)}
        with tempfile.TemporaryDirectory(dir=scratch_root) as temp:
            work = Path(temp)
            source = work / "fixture.tar.gz"
            zon = '.{ .name = .antfly_zig, .version = "0.0.1", .fingerprint = 0xaf6720431d9566e1, .paths = .{"build.zig", "build.zig.zon"}, .dependencies = .{}, }'
            with tarfile.open(source, "w:gz") as archive:
                for name, data in {
                    "build.zig": b"pub fn build() void {}",
                    "build.zig.zon": zon.encode(),
                }.items():
                    member = tarfile.TarInfo("fixture/" + name)
                    member.size = len(data)
                    archive.addfile(member, io.BytesIO(data))
            identity = subprocess.check_output(
                [zig, "fetch", str(source)], env=env, text=True, timeout=600
            ).strip()
            deps.fetch_verified(zig, str(source), identity, global_cache / "p")
            (work / "build.zig").write_text("pub fn build() void {}")
            (work / "build.zig.zon").write_text(
                zon.replace(
                    ".dependencies = .{}",
                    '.dependencies = .{ .fixture = .{ .url = "https://offline.invalid/fixture.tar.gz", .hash = "'
                    + identity
                    + '" }, }',
                )
            )
            # The expected cache entry must work; a malformed extra archive root
            # reproduces the real mismatch before build execution or networking.
            subprocess.run(
                [zig, "build", "--fetch=all"],
                cwd=work,
                env=env,
                capture_output=True,
                text=True,
                check=True,
                timeout=120,
            )
