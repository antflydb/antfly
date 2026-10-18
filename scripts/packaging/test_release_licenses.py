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

"""Exercise release assembly with fixture binaries and real product licenses."""

import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


class ReleaseLicenseTests(unittest.TestCase):
    def test_missing_macho_linker_fails_before_compilation(self):
        with tempfile.TemporaryDirectory() as directory:
            tools = Path(directory)
            for name, output in (("uname", "Linux"), ("llvm-config", str(tools))):
                command = tools / name
                command.write_text(f"#!/bin/sh\necho '{output}'\n")
                command.chmod(0o755)
            result = subprocess.run(
                [
                    "/bin/bash",
                    str(ROOT / "scripts/packaging/build_zig_release_archive.sh"),
                    "--version",
                    "test",
                    "--target",
                    "aarch64-macos.15.0",
                    "--archive-name",
                    "test.tar.gz",
                    "--out-dir",
                    str(tools / "out"),
                ],
                env={**os.environ, "PATH": str(tools)},
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn(
                "macOS cross builds require ld64.lld and llvm-nm", result.stderr
            )
            self.assertFalse((tools / "out").exists())

    def test_product_builds_and_archive_licenses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in (
                "scripts/packaging/build_zig_release_archive.sh",
                "scripts/packaging/fixture_process.py",
                "scripts/packaging/create_reproducible_tar.py",
                "scripts/packaging/render_libantfly_pkgconfig.py",
                "scripts/packaging/package_grep_release.py",
                "zig/pkg/antfly-embedded/libantfly.pc.in",
                "scripts/packaging/lite-release-README.md",
                "scripts/packaging/embedded-release-README.md",
                "scripts/packaging/inference-release-README.md",
                "scripts/apache_engine_files.txt",
                "scripts/source_license_roots.json",
                "scripts/embedded_asset_licenses.json",
                "LICENSE",
                "LICENSES/Apache-2.0.txt",
                "LICENSES/Elastic-2.0.txt",
                "LICENSING.md",
                "THIRD_PARTY_NOTICES.md",
                "README.md",
            ):
                destination = root / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(ROOT / name, destination)
            shutil.copytree(ROOT / "zig/pkg/antfly-grep", root / "zig/pkg/antfly-grep")
            shutil.copytree(
                ROOT / "LICENSES/third-party", root / "LICENSES/third-party"
            )
            (root / "zig/tools").mkdir(parents=True)
            (root / "zig/pkg/inference").mkdir(parents=True)
            (root / "zig/tools/run_bounded_zig_build.py").write_text(
                "import json, os, sys\n"
                "from pathlib import Path\n"
                "sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts/packaging'))\n"
                "from fixture_process import run_fixture\n"
                "args = sys.argv[1:]\n"
                "Path('build-args.json').write_text(json.dumps(args))\n"
                "prefix = Path(args[args.index('--prefix') + 1])\n"
                "if os.environ.get('RELEASE_CACHE_PROBE_ZIG'):\n"
                "    root = Path(__file__).resolve().parents[1]\n"
                "    entry = 'cache-embedded.build.zig' if 'lite' in args else 'cache-server.build.zig'\n"
                "    options = [arg for arg in args if arg.startswith(('-Dtarget=', '-Doptimize='))]\n"
                "    built = run_fixture([os.environ['RELEASE_CACHE_PROBE_ZIG'], 'build', '--build-file', entry, '--summary', 'all', '--cache-dir', args[args.index('--cache-dir') + 1], *options], cwd=root, timeout=180)\n"
                "    Path('cache-summary.txt').write_text(built.stdout + built.stderr)\n"
                "    if built.returncode: raise RuntimeError(built.stdout + built.stderr)\n"
                "inference = Path.cwd().name == 'inference'\n"
                "library = 'lib/libantfly.dylib' if any(arg.startswith('-Dtarget=aarch64-macos') for arg in args) else 'lib/libantfly.so'\n"
                "names = ('bin/antfly-inference',) if inference else (('bin/antfly-lite',) if 'lite' in args else ('bin/antfly',)) + (library, 'include/antfly.h')\n"
                "if 'grep' in args: names += ('bin/antfly-grep',)\n"
                "for name in names:\n"
                "    path = prefix / name\n"
                "    path.parent.mkdir(parents=True, exist_ok=True)\n"
                "    path.write_text('fixture artifact\\n')\n"
                "    path.chmod(0o755)\n"
            )
            probe_zig = os.environ.get("RELEASE_CACHE_PROBE_ZIG")
            if probe_zig:
                # Both product entrypoints compile the same provider, as in
                # the real graph. Give it a real compiler/cache, not a marker.
                for entry in ("cache-server.build.zig", "cache-embedded.build.zig"):
                    (root / "zig" / entry).write_text(
                        'pub const build = @import("cache-support.zig").build;\n'
                    )
                (root / "zig/cache-support.zig").write_text("""
const std = @import("std");
pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const provider = b.addLibrary(.{ .name = "release-cache-probe", .linkage = .static, .root_module = b.createModule(.{ .root_source_file = b.path("cache-provider.zig"), .target = target, .optimize = optimize }), .use_llvm = true });
    b.installArtifact(provider);
}
""")
                (root / "zig/cache-provider.zig").write_text(
                    "export fn cache_probe() c_int { return 42; }\n"
                )
            completions = root / "scripts/completions.sh"
            completions.write_text(
                '#!/bin/sh\nrm -rf "$1"\nmkdir -p "$1"\n'
                'for shell in bash zsh fish; do echo fixture > "$1/antfly.$shell"; done\n'
            )
            completions.chmod(0o755)
            tools = root / "tools"
            llvm = root / "llvm/bin"
            tools.mkdir()
            llvm.mkdir(parents=True)
            for name, script in {
                "uname": "#!/bin/sh\necho Linux\n",
                "llvm-config": f"#!/bin/sh\necho '{llvm}'\n",
            }.items():
                command = tools / name
                command.write_text(script)
                command.chmod(0o755)
            for name in ("ld64.lld", "llvm-nm"):
                command = llvm / name
                command.write_text("#!/bin/sh\nexit 0\n")
                command.chmod(0o755)
            policy = json.loads((ROOT / "scripts/release/platforms.json").read_text())
            optimize = {
                entry["zig_target"]: entry["optimize"] for entry in policy["platforms"]
            }
            darwin_target = next(
                entry["zig_target"]
                for entry in policy["platforms"]
                if entry["archive_os"] == "Darwin"
            )
            compile_failed = False
            for product, target, steps, license_name in (
                ("embedded", darwin_target, ["lite"], "LICENSES/Apache-2.0.txt"),
                ("lite", darwin_target, ["lite"], "LICENSES/Apache-2.0.txt"),
                ("inference", darwin_target, [], "LICENSES/Apache-2.0.txt"),
                ("server", darwin_target, ["antfly", "capi", "grep"], "LICENSE"),
                ("server", darwin_target, ["antfly", "capi", "grep"], "LICENSE"),
                ("server", "aarch64-linux-musl", ["antfly", "capi", "grep"], "LICENSE"),
            ):
                if compile_failed:
                    break  # Later cases require this first provider/cache to exist.
                with self.subTest(product=product, target=target):
                    built = subprocess.run(
                        [
                            "bash",
                            str(
                                root / "scripts/packaging/build_zig_release_archive.sh"
                            ),
                            "--product",
                            product,
                            "--version",
                            "test",
                            "--target",
                            target,
                            "--optimize",
                            optimize[target],
                            "--archive-name",
                            product + ".tar.gz",
                            "--out-dir",
                            str(root / "out"),
                        ],
                        env={
                            **os.environ,
                            "PATH": f"{tools}:{os.environ['PATH']}",
                            "RUNNER_TEMP": str(root / "build"),
                            "SOURCE_DATE_EPOCH": "0",
                        },
                        capture_output=True,
                        text=True,
                    )
                    compile_failed = built.returncode != 0
                    self.assertEqual(built.returncode, 0, built.stdout + built.stderr)
                    build_dir = root / (
                        "zig/pkg/inference" if product == "inference" else "zig"
                    )
                    args = json.loads((build_dir / "build-args.json").read_text())
                    self.assertEqual(
                        steps,
                        [
                            arg
                            for arg in args
                            if arg in ("lite", "antfly", "capi", "grep")
                        ],
                    )
                    self.assertEqual(
                        product == "server", "-Dapple-providers=false" in args
                    )
                    self.assertNotIn("--max-rss-cap", args)
                    project_cache = Path(args[args.index("--cache-dir") + 1])
                    self.assertEqual(project_cache, root / ".zig-cache/release")
                    # Product scratch is reset, but objects from earlier
                    # products and rebuilds must survive that cleanup.
                    marker = project_cache / "earlier-product-object"
                    if product != "embedded":
                        self.assertEqual(marker.read_text(), "keep compiled objects")
                    marker.write_text("keep compiled objects")
                    self.assertIn("--summary", args)
                    self.assertEqual(args[args.index("--summary") + 1], "all")
                    if probe_zig:
                        summary = (build_dir / "cache-summary.txt").read_text()
                        expected = (
                            "success"
                            if product == "embedded" or target != darwin_target
                            else "cached"
                        )
                        self.assertRegex(
                            summary,
                            rf"compile lib release-cache-probe fast \S+ {expected}",
                        )
                        if product == "embedded":
                            inference_summary = (
                                root / "zig/pkg/inference/cache-summary.txt"
                            ).read_text()
                            self.assertRegex(
                                inference_summary,
                                r"compile lib release-cache-probe fast \S+ cached",
                            )
                    if product == "embedded":
                        inference_args = json.loads(
                            (root / "zig/pkg/inference/build-args.json").read_text()
                        )
                        self.assertNotIn("--max-rss-cap", inference_args)
                        self.assertEqual(
                            args[args.index("--cache-dir") + 1],
                            inference_args[inference_args.index("--cache-dir") + 1],
                        )
                        self.assertNotIn("--build-file", inference_args)
                        self.assertNotIn("-Dapple-providers=false", inference_args)
                        for option in (
                            f"-Dtarget={darwin_target}",
                            "-Dantfly-version=test",
                            "-Doptimize=fast",
                        ):
                            self.assertIn(option, args)
                            self.assertIn(option, inference_args)
                        self.assertEqual(
                            args[args.index("--prefix") + 1],
                            inference_args[inference_args.index("--prefix") + 1],
                        )
                    with tarfile.open(root / "out" / (product + ".tar.gz")) as archive:
                        self.assertFalse(
                            any("stale-product" in name for name in archive.getnames())
                        )
                        self.assertEqual(
                            (ROOT / license_name).read_bytes(),
                            archive.extractfile("./LICENSE").read(),
                        )
                        for name in (
                            "./LICENSES/Apache-2.0.txt",
                            "./THIRD_PARTY_NOTICES.md",
                        ):
                            self.assertIn(name, archive.getnames())
                        for name in (
                            "./scripts/apache_engine_files.txt",
                            "./scripts/embedded_asset_licenses.json",
                            "./lib/libantfly.dylib"
                            if target == darwin_target
                            else "./lib/libantfly.so",
                            "./lib/pkgconfig/libantfly.pc",
                            "./include/antfly.h",
                        ):
                            self.assertEqual(
                                product != "inference", name in archive.getnames()
                            )
                        if product != "inference":
                            pc = (
                                archive.extractfile("./lib/pkgconfig/libantfly.pc")
                                .read()
                                .decode()
                            )
                            self.assertIn("Version: test", pc)
                            self.assertIn("prefix=${pcfiledir}/../..", pc)
                        self.assertEqual(
                            product == "server",
                            "./LICENSES/Elastic-2.0.txt" in archive.getnames(),
                        )
                        self.assertEqual(
                            product == "server",
                            "./LICENSING.md" in archive.getnames(),
                        )
                        bundled_notices = (
                            archive.extractfile("./THIRD_PARTY_NOTICES.md")
                            .read()
                            .decode()
                        )
                        for notice in (ROOT / "LICENSES/third-party").glob("*.txt"):
                            archived = archive.extractfile(
                                "./LICENSES/third-party/" + notice.name
                            ).read()
                            self.assertEqual(notice.read_bytes(), archived)
                            self.assertIn(
                                " ".join(notice.read_text().split()),
                                " ".join(bundled_notices.split()),
                            )
                        if product == "server":
                            license_map = (
                                archive.extractfile("./LICENSING.md").read().decode()
                            )
                            for linked_file in re.findall(
                                r"\]\(([^)]+)\)", license_map
                            ):
                                self.assertIn("./" + linked_file, archive.getnames())
                        if product != "inference":
                            self.assertEqual(
                                (ROOT / "scripts/apache_engine_files.txt").read_bytes(),
                                archive.extractfile(
                                    "./scripts/apache_engine_files.txt"
                                ).read(),
                            )
                        if product == "server":
                            self.assertEqual(
                                (ROOT / "LICENSE").read_bytes(),
                                archive.extractfile(
                                    "./LICENSES/Elastic-2.0.txt"
                                ).read(),
                            )
                        self.assertEqual(
                            (ROOT / "LICENSES/Apache-2.0.txt").read_bytes(),
                            archive.extractfile("./LICENSES/Apache-2.0.txt").read(),
                        )
                        readme = archive.extractfile("./README.md").read().decode()
                        for label, linked_file in re.findall(
                            r"\[([^]]+)\]\((LICENSE[^)]+)\)", readme
                        ):
                            expected = (
                                "LICENSES/Elastic-2.0.txt"
                                if "ELv2" in label
                                else "LICENSES/Apache-2.0.txt"
                            )
                            self.assertEqual(expected, linked_file)
                            self.assertIn("./" + linked_file, archive.getnames())
                        self.assertEqual(
                            product == "server",
                            "./completions/antfly.bash" in archive.getnames(),
                        )
                        expected_binaries = (
                            {"./antfly"}
                            if product == "server"
                            else (
                                {"./antfly-inference"}
                                if product == "inference"
                                else (
                                    {
                                        "./antfly-lite",
                                        "./antfly-inference-worker",
                                        "./antfly-inference",
                                    }
                                    if product == "embedded"
                                    else {"./antfly-lite", "./antfly-inference-worker"}
                                )
                            )
                        )
                        self.assertEqual(
                            expected_binaries,
                            {
                                name
                                for name in archive.getnames()
                                if name
                                in {
                                    "./antfly",
                                    "./antfly-lite",
                                    "./antfly-inference",
                                    "./antfly-inference-worker",
                                }
                            },
                        )

                    scratch = root / "build" / f"antfly-zig-release-{product}-{target}"
                    for relative in (
                        "stage/stale-product",
                        "zig-out/lib/stale-product",
                    ):
                        stale = scratch / relative
                        stale.parent.mkdir(parents=True, exist_ok=True)
                        stale.write_text("must not enter the next archive")

    @unittest.skipUnless(shutil.which("zig"), "Zig compiler required")
    def test_product_entrypoints_reuse_compiled_provider(self):
        with patch.dict(os.environ, {"RELEASE_CACHE_PROBE_ZIG": shutil.which("zig")}):
            self.test_product_builds_and_archive_licenses()


if __name__ == "__main__":
    unittest.main()
