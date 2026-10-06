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

"""Verify that packaged Lite artifacts contain the matching Apache runtime."""

from __future__ import annotations

import argparse
import json
import sys
import tarfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "release"))
from release_channels import normalize_release_version, python_version_from_release  # noqa: E402

from package_cli_release import PACKAGE_PLATFORMS, lite_library_name  # noqa: E402
from package_lite_release import SOURCE_LICENSE_FILES, archive_name  # noqa: E402


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def verify(
    version: str,
    archive_dir: Path,
    wheel_dir: Path,
    npm_dir: Path,
    build_contract_schema: int = 5,
) -> None:
    version = normalize_release_version(version)
    python_version = python_version_from_release(version)
    apache = (ROOT / "LICENSES/Apache-2.0.txt").read_bytes()
    for platform in PACKAGE_PLATFORMS:
        archive_path = archive_dir / archive_name(
            version, platform, build_contract_schema
        )
        with tarfile.open(archive_path, "r:gz") as source:
            names = {name.removeprefix("./"): name for name in source.getnames()}
            require("antfly" not in names, f"server executable in {archive_path}")
            require(
                "LICENSES/Elastic-2.0.txt" not in names,
                f"ELv2 license in {archive_path}",
            )

            def source_bytes(name: str) -> bytes:
                actual_name = names.get(name.removeprefix("./"))
                require(actual_name is not None, f"missing {name} in {archive_path}")
                member = source.extractfile(actual_name)
                require(member is not None, f"missing {name} in {archive_path}")
                return member.read()

            lib_name = lite_library_name(platform)
            library = source_bytes(f"./lib/{lib_name}")
            source_bytes("./antfly-lite")
            if build_contract_schema >= 3:
                source_bytes("./antfly-inference")
            worker = source_bytes("./antfly-inference-worker")
            pkgconfig = (
                source_bytes("./lib/pkgconfig/libantfly.pc")
                if build_contract_schema >= 4
                else None
            )
            if pkgconfig is not None:
                require(
                    f"Version: {version}\n".encode() in pkgconfig,
                    f"wrong pkg-config version: {archive_path}",
                )
            source_maps = {
                name: source_bytes(f"./scripts/{name}") for name in SOURCE_LICENSE_FILES
            }
            require(
                source_bytes("./LICENSE") == apache,
                f"wrong archive license: {archive_path}",
            )
            require(
                source_bytes("./LICENSES/Apache-2.0.txt") == apache,
                f"wrong archive license bundle: {archive_path}",
            )

        wheel_path = (
            wheel_dir
            / f"antfly_embedded-{python_version}-py3-none-{platform.wheel_platform}.whl"
        )
        with zipfile.ZipFile(wheel_path) as wheel:
            require(
                wheel.read(f"antfly_embedded/_lib/{lib_name}") == library,
                f"library mismatch: {wheel_path}",
            )
            if build_contract_schema >= 4:
                require(
                    "antfly_embedded/_lib/antfly-inference-worker"
                    not in wheel.namelist(),
                    f"unused worker in {wheel_path}",
                )
            if "antfly_embedded/_lib/antfly-inference-worker" in wheel.namelist():
                require(
                    wheel.read("antfly_embedded/_lib/antfly-inference-worker")
                    == worker,
                    f"worker mismatch: {wheel_path}",
                )
            require(
                not any(
                    name.startswith("antfly_embedded/_bin/")
                    or name.endswith("/entry_points.txt")
                    for name in wheel.namelist()
                ),
                f"embedded wheel exposes CLI commands: {wheel_path}",
            )
            require(
                wheel.read(f"antfly_embedded-{python_version}.dist-info/LICENSE")
                == apache,
                f"wrong wheel license: {wheel_path}",
            )
            require(
                wheel.read(
                    f"antfly_embedded-{python_version}.dist-info/LICENSES/Apache-2.0.txt"
                )
                == apache,
                f"wrong wheel license bundle: {wheel_path}",
            )
            require(
                b"Metadata-Version: 2.4"
                in wheel.read(f"antfly_embedded-{python_version}.dist-info/METADATA"),
                f"wrong wheel metadata version: {wheel_path}",
            )
            require(
                b"License-Expression: Apache-2.0"
                in wheel.read(f"antfly_embedded-{python_version}.dist-info/METADATA"),
                f"wrong wheel metadata: {wheel_path}",
            )
            require(
                not any(name.startswith("antfly_cli/") for name in wheel.namelist()),
                f"server package in {wheel_path}",
            )

            for name, content in source_maps.items():
                require(
                    wheel.read(
                        f"antfly_embedded-{python_version}.dist-info/LICENSES/source-map/{name}"
                    )
                    == content,
                    f"source license map mismatch: {wheel_path}: {name}",
                )

        package_name = platform.npm_package_dir.replace("cli-", "embedded-")
        npm_path = npm_dir / f"antfly-{package_name}-{version}.tgz"
        with tarfile.open(npm_path, "r:gz") as npm:

            def npm_bytes(name: str) -> bytes:
                member = npm.extractfile("package/" + name)
                require(member is not None, f"missing {name} in {npm_path}")
                return member.read()

            manifest = json.loads(npm_bytes("package.json"))
            require(
                manifest["name"] == f"@antfly/{package_name}",
                f"wrong npm package: {npm_path}",
            )
            require(
                manifest["license"] == "Apache-2.0", f"wrong npm license: {npm_path}"
            )
            require(
                npm_bytes(f"lib/{lib_name}") == library, f"library mismatch: {npm_path}"
            )
            if build_contract_schema >= 4:
                require(
                    "package/lib/antfly-inference-worker" not in npm.getnames(),
                    f"unused worker in {npm_path}",
                )
                require(
                    npm_bytes("lib/pkgconfig/libantfly.pc") == pkgconfig,
                    f"pkg-config mismatch: {npm_path}",
                )
            if "package/lib/antfly-inference-worker" in npm.getnames():
                require(
                    npm_bytes("lib/antfly-inference-worker") == worker,
                    f"worker mismatch: {npm_path}",
                )
            require(
                not any(
                    member.name.startswith("package/bin/")
                    for member in npm.getmembers()
                )
                and "bin" not in manifest,
                f"embedded npm package exposes CLI commands: {npm_path}",
            )
            require(
                npm_bytes("LICENSE") == apache, f"wrong npm license text: {npm_path}"
            )
            require(
                npm_bytes("LICENSES/Apache-2.0.txt") == apache,
                f"wrong npm license bundle: {npm_path}",
            )

            for name, content in source_maps.items():
                require(
                    npm_bytes(f"LICENSES/source-map/{name}") == content,
                    f"source license map mismatch: {npm_path}: {name}",
                )

    selector = npm_dir / f"antfly-embedded-{version}.tgz"
    with tarfile.open(selector, "r:gz") as npm:
        member = npm.extractfile("package/package.json")
        require(member is not None, f"missing package.json in {selector}")
        manifest = json.load(member)
        require(
            manifest["name"] == "@antfly/embedded"
            and manifest["license"] == "Apache-2.0",
            f"wrong Lite selector: {selector}",
        )
        expected = {
            f"@antfly/embedded-{name}"
            for name in ("darwin-arm64", "linux-arm64", "linux-x64")
        }
        require(
            set(manifest["optionalDependencies"]) == expected,
            f"wrong native dependencies: {selector}",
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--wheel-dir", type=Path, required=True)
    parser.add_argument("--npm-dir", type=Path, required=True)
    parser.add_argument(
        "--build-contract-schema", type=int, choices=(2, 3, 4, 5), default=5
    )
    args = parser.parse_args()
    verify(
        args.version,
        args.archive_dir,
        args.wheel_dir,
        args.npm_dir,
        args.build_contract_schema,
    )
    print("Apache embedded release artifacts verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
