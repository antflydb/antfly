# Copyright 2026 Antfly, Inc.
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

ROOT = Path(__file__).resolve().parents[2]


class ReleaseLicenseTests(unittest.TestCase):
    def test_product_builds_and_archive_licenses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in (
                "scripts/packaging/build_zig_release_archive.sh",
                "scripts/packaging/create_reproducible_tar.py",
                "scripts/packaging/lite-release-README.md",
                "scripts/packaging/inference-release-README.md",
                "scripts/apache_engine_files.txt",
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
            shutil.copytree(
                ROOT / "LICENSES/third-party", root / "LICENSES/third-party"
            )
            (root / "zig/tools").mkdir(parents=True)
            (root / "zig/pkg/inference").mkdir(parents=True)
            (root / "zig/tools/run_bounded_zig_build.py").write_text(
                "import json, sys\n"
                "from pathlib import Path\n"
                "args = sys.argv[1:]\n"
                "Path('build-args.json').write_text(json.dumps(args))\n"
                "prefix = Path(args[args.index('--prefix') + 1])\n"
                "for name in ('bin/antfly', 'bin/antfly-lite', 'bin/antfly-inference', 'lib/libantfly.dylib', 'include/antfly.h'):\n"
                "    path = prefix / name\n"
                "    path.parent.mkdir(parents=True, exist_ok=True)\n"
                "    path.write_text('fixture artifact\\n')\n"
                "    path.chmod(0o755)\n"
            )
            completions = root / "scripts/completions.sh"
            completions.write_text(
                '#!/bin/sh\nmkdir -p "$1"\n'
                'for shell in bash zsh fish; do echo fixture > "$1/antfly.$shell"; done\n'
            )
            completions.chmod(0o755)
            for product, steps, license_name in (
                ("lite", ["lite"], "LICENSES/Apache-2.0.txt"),
                ("inference", [], "LICENSES/Apache-2.0.txt"),
                ("server", ["antfly", "capi"], "LICENSE"),
            ):
                with self.subTest(product=product):
                    subprocess.run(
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
                            "aarch64-macos",
                            "--archive-name",
                            product + ".tar.gz",
                            "--out-dir",
                            str(root / "out"),
                        ],
                        env={
                            **os.environ,
                            "RUNNER_TEMP": str(root / "build"),
                            "SOURCE_DATE_EPOCH": "0",
                        },
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    build_dir = root / ("zig/pkg/inference" if product == "inference" else "zig")
                    args = json.loads((build_dir / "build-args.json").read_text())
                    self.assertEqual(
                        steps,
                        [arg for arg in args if arg in ("lite", "antfly", "capi")],
                    )
                    with tarfile.open(root / "out" / (product + ".tar.gz")) as archive:
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
                            "./lib/libantfly.dylib",
                            "./include/antfly.h",
                        ):
                            self.assertEqual(product != "inference", name in archive.getnames())
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
                                archive.extractfile("./scripts/apache_engine_files.txt").read(),
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
                                else {"./antfly-lite", "./antfly-inference-worker"}
                            )
                        )
                        self.assertEqual(
                            expected_binaries,
                            {
                                name
                                for name in archive.getnames()
                                if name
                                in {"./antfly", "./antfly-lite", "./antfly-inference", "./antfly-inference-worker"}
                            },
                        )


if __name__ == "__main__":
    unittest.main()
