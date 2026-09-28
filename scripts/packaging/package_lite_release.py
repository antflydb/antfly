#!/usr/bin/env python3
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

"""Build Apache-only Lite platform wheels and npm platform packages."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import shutil
import sys
import tarfile
import tempfile
import zipfile
from email.message import Message
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "release"))
from release_channels import normalize_release_version, python_version_from_release  # noqa: E402

from package_cli_release import (  # noqa: E402
    PACKAGE_PLATFORMS,
    Platform,
    clean_path,
    is_packaging_noise,
    lite_library_name,
    project_requires_python,
    safe_extract,
    update_json_version,
    update_pyproject_version,
)


def archive_name(version: str, platform: Platform) -> str:
    variant = f"_{platform.release_variant}" if platform.release_variant else ""
    return f"antfly-lite_{version}_{platform.release_os}_{platform.release_arch}{variant}.tar.gz"


def extract_lite_archive(
    archive_dir: Path, version: str, platform: Platform, dest: Path
) -> None:
    path = archive_dir / archive_name(version, platform)
    if not path.is_file():
        raise ValueError(f"missing Lite release archive: {path}")
    with tarfile.open(path, "r:gz") as archive:
        safe_extract(archive, dest)
    required = (
        dest / "antfly-lite",
        dest / "antfly-inference",
        dest / "lib" / lite_library_name(platform),
        dest / "include" / "antfly.h",
        dest / "LICENSE",
        dest / "THIRD_PARTY_NOTICES.md",
        dest / "LICENSES" / "Apache-2.0.txt",
    )
    for item in required:
        if not item.is_file():
            raise ValueError(f"Lite archive missing {item.relative_to(dest)}: {path}")
    if (dest / "antfly").exists() or (dest / "LICENSES" / "Elastic-2.0.txt").exists():
        raise ValueError(
            f"Lite archive contains server artifacts or ELv2 license: {path}"
        )
    if (dest / "LICENSE").read_bytes() != (
        ROOT / "LICENSES" / "Apache-2.0.txt"
    ).read_bytes():
        raise ValueError(
            f"Lite archive does not carry the Apache-2.0 product license: {path}"
        )


def populate_npm_package(platform: Platform, extracted: Path, version: str) -> Path:
    assert platform.npm_package_dir is not None
    package_dir = (
        ROOT / "ts" / "packages" / platform.npm_package_dir.replace("cli-", "lite-")
    )
    manifest = package_dir / "package.json"
    if json.loads(manifest.read_text())["license"] != "Apache-2.0":
        raise ValueError(f"not an Apache platform package: {manifest}")
    update_json_version(manifest, version)
    for name in ("bin", "lib", "include", "share", "LICENSES"):
        clean_path(package_dir / name)
    (package_dir / "bin").mkdir()
    shutil.copy2(extracted / "antfly-lite", package_dir / "bin" / "antfly-lite")
    shutil.copytree(
        extracted / "lib",
        package_dir / "lib",
        ignore=lambda d, n: {x for x in n if is_packaging_noise(Path(x))},
    )
    shutil.copy2(
        extracted / "antfly-inference", package_dir / "lib" / "antfly-inference"
    )
    shutil.copytree(extracted / "include", package_dir / "include")
    if (extracted / "share").is_dir():
        shutil.copytree(extracted / "share", package_dir / "share")
    shutil.copy2(extracted / "LICENSE", package_dir / "LICENSE")
    shutil.copy2(
        extracted / "THIRD_PARTY_NOTICES.md", package_dir / "THIRD_PARTY_NOTICES.md"
    )
    shutil.copytree(extracted / "LICENSES", package_dir / "LICENSES")
    return package_dir


def write_wheel(
    platform: Platform, extracted: Path, version: str, out_dir: Path
) -> Path:
    assert platform.wheel_platform is not None
    tag = f"py3-none-{platform.wheel_platform}"
    dist_info = f"antfly_lite-{version}.dist-info"
    wheel_path = out_dir / f"antfly_lite-{version}-{tag}.whl"
    source = ROOT / "py" / "packages" / "lite" / "src" / "antfly_lite"
    records: list[tuple[str, str, str]] = []
    out_dir.mkdir(parents=True, exist_ok=True)

    def add_bytes(
        archive: zipfile.ZipFile, name: str, data: bytes, mode: int = 0o644
    ) -> None:
        info = zipfile.ZipInfo(name)
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = (mode & 0xFFFF) << 16
        archive.writestr(info, data)
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(data).digest())
            .rstrip(b"=")
            .decode()
        )
        records.append((name, f"sha256={digest}", str(len(data))))

    metadata = Message()
    metadata["Metadata-Version"] = "2.3"
    metadata["Name"] = "antfly-lite"
    metadata["Version"] = version
    metadata["Summary"] = "Apache-2.0 embedded Antfly Lite and Inference"
    metadata["License-Expression"] = "Apache-2.0"
    metadata["Requires-Python"] = project_requires_python(
        ROOT / "py" / "packages" / "lite" / "pyproject.toml"
    )
    wheel = f"Wheel-Version: 1.0\nGenerator: package_lite_release.py\nRoot-Is-Purelib: false\nTag: {tag}\n"
    entry_points = "[console_scripts]\nantfly-lite = antfly_lite._cli:lite_main\nantfly-inference = antfly_lite._cli:inference_main\n"

    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in sorted(source.rglob("*")):
            if item.is_file() and "__pycache__" not in item.parts:
                add_bytes(
                    archive,
                    f"antfly_lite/{item.relative_to(source).as_posix()}",
                    item.read_bytes(),
                )
        for binary in ("antfly-lite", "antfly-inference"):
            add_bytes(
                archive,
                f"antfly_lite/_bin/{binary}",
                (extracted / binary).read_bytes(),
                0o755,
            )
        for item in sorted((extracted / "lib").rglob("*")):
            if item.is_file():
                add_bytes(
                    archive,
                    f"antfly_lite/_lib/{item.relative_to(extracted / 'lib').as_posix()}",
                    item.read_bytes(),
                )
        add_bytes(
            archive,
            "antfly_lite/_lib/antfly-inference",
            (extracted / "antfly-inference").read_bytes(),
            0o755,
        )
        for name in ("LICENSE", "THIRD_PARTY_NOTICES.md"):
            add_bytes(archive, f"{dist_info}/{name}", (extracted / name).read_bytes())
        for item in sorted((extracted / "LICENSES").rglob("*")):
            if item.is_file():
                add_bytes(
                    archive,
                    f"{dist_info}/LICENSES/{item.relative_to(extracted / 'LICENSES').as_posix()}",
                    item.read_bytes(),
                )
        add_bytes(archive, f"{dist_info}/METADATA", metadata.as_bytes())
        add_bytes(archive, f"{dist_info}/WHEEL", wheel.encode())
        add_bytes(archive, f"{dist_info}/entry_points.txt", entry_points.encode())
        buffer = io.StringIO()
        csv.writer(buffer).writerows([*records, (f"{dist_info}/RECORD", "", "")])
        add_bytes(archive, f"{dist_info}/RECORD", buffer.getvalue().encode())
    return wheel_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    version = normalize_release_version(args.version)
    python_version = python_version_from_release(version)
    update_pyproject_version(
        ROOT / "py" / "packages" / "lite" / "pyproject.toml", python_version
    )
    update_json_version(
        ROOT / "ts" / "packages" / "lite" / "package.json", version, optional_deps=True
    )
    for platform in PACKAGE_PLATFORMS:
        with tempfile.TemporaryDirectory() as raw:
            extracted = Path(raw)
            extract_lite_archive(args.archive_dir, version, platform, extracted)
            populate_npm_package(platform, extracted, version)
            if platform.wheel_platform:
                print(
                    write_wheel(
                        platform, extracted, python_version, args.out_dir / "python"
                    )
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
