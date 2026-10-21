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
import json
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
    def test_workflow_action_bootstraps_an_older_checkout_and_validates_its_sources(
        self,
    ):
        import shutil

        root = Path(__file__).resolve().parents[2]
        action = root / ".github/actions/prepare-zig-dependencies/action.yml"
        shell = "\n".join(
            line[8:]
            for line in action.read_text().split("      run: |\n", 1)[1].splitlines()
        )
        empty = {"schema_version": 1, "mirror": deps.MIRROR, "packages": []}
        with tempfile.TemporaryDirectory() as temp:
            checkout = Path(temp)
            trusted = checkout / ".ci-dependency-policy"
            action_root = trusted / ".github/actions/prepare-zig-dependencies"
            action_root.mkdir(parents=True)
            policy = trusted / "scripts/ci"
            policy.mkdir(parents=True)
            shutil.copyfile(Path(deps.__file__), policy / "prepare_zig_dependencies.py")
            (policy / "zig-dependencies.json").write_text(json.dumps(empty))
            source = checkout / "zig/build.zig.zon"
            source.parent.mkdir()
            source.write_text(".{ .dependencies = .{} }")
            env = {
                **os.environ,
                "ACTION_ROOT": str(action_root),
                "GITHUB_WORKSPACE": str(checkout),
                "MIRROR_ONLY": "false",
                "ZIG_GLOBAL_CACHE_DIR": str(checkout / "cache"),
            }

            def run():
                return subprocess.run(
                    ["bash", "-euo", "pipefail", "-c", shell],
                    cwd=checkout,
                    env=env,
                    capture_output=True,
                    text=True,
                )

            # An older branch has neither the action nor the bootstrap script.
            accepted = run()
            self.assertEqual(accepted.returncode, 0, accepted.stderr)
            self.assertFalse(
                (checkout / "scripts/ci/prepare_zig_dependencies.py").exists()
            )
            env["MIRROR_ONLY"] = "true"
            self.assertEqual(run().returncode, 0)
            source.write_text(
                '.{ .dependencies = .{ .x = .{ .url = "https://unlisted.test/source", .hash = "unlisted" }, }, }'
            )
            rejected = run()
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn(
                "remote package missing from pinned dependency inventory",
                rejected.stderr,
            )
            # A proposed inventory must take precedence over the fallback.
            proposed = checkout / "scripts/ci"
            proposed.mkdir(parents=True)
            (proposed / "zig-dependencies.json").write_text('{"schema_version":99}')
            self.assertIn("unsupported Zig dependency inventory", run().stderr)

    def test_mirror_token_is_sent_only_to_the_exact_bucket(self):
        import io

        with (
            tempfile.TemporaryDirectory() as temp,
            patch.dict(os.environ, {deps.MIRROR_TOKEN_ENV: "test-token"}),
        ):
            target = Path(temp) / "download"
            with patch.object(deps, "build_opener") as opener:
                opener.return_value.open.return_value = io.BytesIO(b"source")
                deps.download(deps.MIRROR + "/expected.tar.gz", target)
                request = opener.return_value.open.call_args.args[0]
                self.assertEqual(
                    request.get_header("Authorization"), "Bearer test-token"
                )
            for source in (
                "https://upstream.test/source.tar.gz",
                deps.MIRROR + "-other/source.tar.gz",
            ):
                with patch.object(
                    deps, "urlopen", return_value=io.BytesIO(b"source")
                ) as fetch:
                    deps.download(source, target)
                    self.assertEqual(fetch.call_args.args[0], source)

    def test_authenticated_redirect_cannot_forward_credentials_to_other_origin(self):
        request = deps.Request(
            deps.MIRROR + "/source.tar.gz",
            headers={"Authorization": "Bearer test-token"},
        )
        for destination in (
            "https://upstream.test/archive",
            "http://storage.googleapis.com/archive",
            "https://storage.googleapis.com/another-bucket/archive",
        ):
            with self.subTest(destination=destination), self.assertRaises(ValueError):
                deps.MirrorRedirects().redirect_request(
                    request, None, 302, "redirect", {}, destination
                )

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
            ) as run,
            patch.dict(os.environ, {deps.MIRROR_TOKEN_ENV: "test-token"}),
        ):
            with self.assertRaises(ValueError):
                deps.fetch_verified(
                    "zig", "/seed/archive.tar.gz", "expected", Path(temp) / "p"
                )
            self.assertNotIn(deps.MIRROR_TOKEN_ENV, run.call_args.kwargs["env"])

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

    def test_corrupt_cached_archives_are_removed_before_verified_refetch(self):
        import gzip
        import io
        import tarfile

        manifest = {
            "mirror": "https://mirror.test",
            "packages": [
                {"hash": "expected", "url": "https://upstream.test/archive.tar.gz"}
            ],
        }
        for damaged in (b"not an archive", gzip.compress(b"partial")[:12]):
            with self.subTest(damaged=damaged), tempfile.TemporaryDirectory() as temp:
                cache = Path(temp)
                packages = cache / "p"
                packages.mkdir()
                cached = packages / "expected.tar.gz"
                cached.write_bytes(damaged)
                neighbor = packages / "other.tar.gz"
                neighbor.write_bytes(b"preserve neighboring packages")

                def fetch(zig, source, identity, target):
                    self.assertFalse(cached.exists())
                    self.assertEqual(source, "https://mirror.test/expected.tar.gz")
                    with tarfile.open(cached, "w:gz") as archive:
                        data = b".{ .dependencies = .{} }"
                        member = tarfile.TarInfo("expected/build.zig.zon")
                        member.size = len(data)
                        archive.addfile(member, io.BytesIO(data))
                    return cached

                with patch.object(deps, "fetch_verified", side_effect=fetch) as fetcher:
                    deps.prepare("zig", manifest, cache)
                fetcher.assert_called_once()
                deps.check_remote_declarations(cached, manifest)
                self.assertEqual(
                    neighbor.read_bytes(), b"preserve neighboring packages"
                )
                with patch.object(deps, "fetch_verified") as fetcher:
                    deps.prepare("zig", manifest, cache)
                fetcher.assert_not_called()

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
            seed = work / "seed"
            seed.mkdir()
            (seed / (identity + ".tar.gz")).write_bytes(source.read_bytes())
            (global_cache / "p" / (identity + ".tar.gz")).write_bytes(b"truncated")
            deps.prepare(
                zig,
                {
                    "mirror": "https://offline.invalid",
                    "packages": [
                        {
                            "hash": identity,
                            "url": "https://offline.invalid/fixture.tar.gz",
                        }
                    ],
                },
                global_cache,
                seed=seed,
                offline=True,
            )
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
