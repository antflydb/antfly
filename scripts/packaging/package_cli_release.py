#!/usr/bin/env python3
"""Repack Antfly release archives as PyPI wheels and npm CLI packages."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import re
import shutil
import stat
import sys
import tarfile
import tempfile
import zipfile
from dataclasses import dataclass
from email.message import Message
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "release"))
from release_channels import (  # noqa: E402
    normalize_release_version,
    python_version_from_release,
)
from release_platforms import load_policy  # noqa: E402


@dataclass(frozen=True)
class Platform:
    key: str
    npm_package_dir: str | None
    release_os: str
    release_arch: str
    wheel_platform: str | None
    release_variant: str | None = None
    npm_libc: str | None = None

    def __post_init__(self) -> None:
        if (
            self.wheel_platform
            and self.wheel_platform.startswith("manylinux_")
            and self.release_variant != "gnu"
        ):
            raise ValueError(
                f"manylinux platform {self.key} must use a GNU release archive"
            )
        if self.npm_package_dir and self.release_os == "Linux":
            expected_libc = "glibc" if self.release_variant == "gnu" else "musl"
            if self.npm_libc != expected_libc:
                raise ValueError(
                    f"Linux platform {self.key} must declare npm libc {expected_libc}"
                )
        if not self.npm_package_dir and self.npm_libc:
            raise ValueError(f"non-npm platform {self.key} must not declare npm libc")


def release_platforms() -> tuple[Platform, ...]:
    platforms: list[Platform] = []
    for entry in load_policy()["platforms"]:
        suffix = entry["archive_suffix"]
        platforms.append(
            Platform(
                entry["id"],
                entry.get("npm_package_dir"),
                entry["archive_os"],
                entry["archive_arch"],
                entry.get("wheel_platform"),
                suffix.removeprefix("_") or None,
                entry.get("npm_libc"),
            )
        )
    return tuple(platforms)


PLATFORMS = release_platforms()

PACKAGE_PLATFORMS = tuple(
    platform for platform in PLATFORMS if platform.npm_package_dir
)


def archive_name(version: str, platform: Platform) -> str:
    variant = f"_{platform.release_variant}" if platform.release_variant else ""
    return f"antfly_{version}_{platform.release_os}_{platform.release_arch}{variant}.tar.gz"


def project_requires_python(pyproject: Path) -> str:
    """Read project.requires-python without duplicating release metadata."""
    in_project = False
    for line in pyproject.read_text().splitlines():
        section = re.fullmatch(r"\s*\[([^]]+)]\s*", line)
        if section:
            in_project = section.group(1) == "project"
            continue
        if not in_project:
            continue
        match = re.fullmatch(r'\s*requires-python\s*=\s*"([^"]+)"\s*', line)
        if match:
            return match.group(1)
    raise SystemExit(f"missing project.requires-python in {pyproject}")


def lite_library_name(platform: Platform) -> str:
    if platform.release_os == "Darwin":
        return "libantfly.dylib"
    return "libantfly.so"


def is_packaging_noise(path: Path) -> bool:
    return any(part == "__MACOSX" or part.startswith("._") for part in path.parts)


def ignore_packaging_noise(_dir: str, names: list[str]) -> set[str]:
    return {name for name in names if name == "__MACOSX" or name.startswith("._")}


def safe_extract(tar: tarfile.TarFile, dest: Path) -> None:
    dest_resolved = dest.resolve()
    for member in tar.getmembers():
        member_path = (dest / member.name).resolve()
        if dest_resolved != member_path and dest_resolved not in member_path.parents:
            raise SystemExit(f"archive member escapes destination: {member.name}")
    tar.extractall(dest)


def extract_archive(
    archive_dir: Path, version: str, platform: Platform, dest: Path
) -> None:
    archive = archive_dir / archive_name(version, platform)
    if not archive.exists():
        raise SystemExit(f"missing release archive: {archive}")
    with tarfile.open(archive, "r:gz") as tar:
        safe_extract(tar, dest)
    binary = dest / "antfly"
    if not binary.exists():
        raise SystemExit(f"release archive does not contain antfly binary: {archive}")
    header = dest / "include" / "antfly.h"
    if not header.exists():
        raise SystemExit(f"release archive does not contain antfly.h: {archive}")
    lite_lib = dest / "lib" / lite_library_name(platform)
    if not lite_lib.exists():
        raise SystemExit(f"release archive does not contain {lite_lib.name}: {archive}")
    binary.chmod(binary.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def copy_antfarm(repo_root: Path, extracted: Path) -> None:
    dst = extracted / "share" / "antfly" / "antfarm"
    if dst.exists():
        return
    src = repo_root / "zig" / "pkg" / "antfly" / "antfarm"
    if not src.exists():
        raise SystemExit(f"missing Antfarm assets: {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, dst, ignore=ignore_packaging_noise)


def clean_path(path: Path) -> None:
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def update_json_version(path: Path, version: str, optional_deps: bool = False) -> None:
    data = json.loads(path.read_text())
    data["version"] = version
    if optional_deps:
        for package_name in data.get("optionalDependencies", {}):
            data["optionalDependencies"][package_name] = version
    path.write_text(json.dumps(data, indent=2) + "\n")


def update_pyproject_version(path: Path, version: str) -> None:
    text = path.read_text()
    text = re.sub(
        r'^version = "[^"]+"$',
        f'version = "{version}"',
        text,
        count=1,
        flags=re.MULTILINE,
    )
    path.write_text(text)


def validate_npm_package(repo_root: Path, platform: Platform) -> None:
    if not platform.npm_package_dir:
        raise ValueError(f"platform {platform.key} does not produce an npm package")
    package_json = (
        repo_root / "ts" / "packages" / platform.npm_package_dir / "package.json"
    )
    data = json.loads(package_json.read_text())
    expected_name = f"@antfly/{platform.npm_package_dir}"
    if data.get("name") != expected_name:
        raise SystemExit(f"{package_json} must declare package name {expected_name}")
    expected_libc = [platform.npm_libc] if platform.npm_libc else None
    if data.get("libc") != expected_libc:
        raise SystemExit(f"{package_json} must declare libc {expected_libc}")


def populate_npm_package(repo_root: Path, platform: Platform, extracted: Path) -> None:
    if not platform.npm_package_dir:
        raise ValueError(f"platform {platform.key} does not produce an npm package")
    package_dir = repo_root / "ts" / "packages" / platform.npm_package_dir
    clean_path(package_dir / "bin")
    clean_path(package_dir / "include")
    clean_path(package_dir / "lib")
    clean_path(package_dir / "share")
    (package_dir / "bin").mkdir(parents=True)
    shutil.copy2(extracted / "antfly", package_dir / "bin" / "antfly")
    shutil.copytree(
        extracted / "include", package_dir / "include", ignore=ignore_packaging_noise
    )
    shutil.copytree(
        extracted / "lib", package_dir / "lib", ignore=ignore_packaging_noise
    )
    shutil.copytree(
        extracted / "share", package_dir / "share", ignore=ignore_packaging_noise
    )


def package_python_wheel(
    repo_root: Path,
    out_dir: Path,
    python_version: str,
    platform: Platform,
    extracted: Path,
) -> Path:
    if platform.wheel_platform is None:
        raise ValueError(f"platform {platform.key} does not produce a Python wheel")
    dist_name = "antfly_cli"
    package_name = "antfly_cli"
    tag = f"py3-none-{platform.wheel_platform}"
    wheel_name = f"{dist_name}-{python_version}-{tag}.whl"
    wheel_path = out_dir / wheel_name
    source_dir = repo_root / "py" / "packages" / "cli" / "src" / package_name
    dist_info = f"{dist_name}-{python_version}.dist-info"

    records: list[tuple[str, str, str]] = []

    def write_bytes(
        zf: zipfile.ZipFile, arcname: str, data: bytes, mode: int | None = None
    ) -> None:
        info = zipfile.ZipInfo(arcname)
        info.compress_type = zipfile.ZIP_DEFLATED
        info.external_attr = ((mode or 0o644) & 0xFFFF) << 16
        zf.writestr(info, data)
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(data).digest())
            .rstrip(b"=")
            .decode()
        )
        records.append((arcname, f"sha256={digest}", str(len(data))))

    def write_file(
        zf: zipfile.ZipFile, arcname: str, path: Path, mode: int | None = None
    ) -> None:
        write_bytes(zf, arcname, path.read_bytes(), mode)

    metadata = Message()
    metadata["Metadata-Version"] = "2.3"
    metadata["Name"] = "antfly-cli"
    metadata["Version"] = python_version
    metadata["Summary"] = "Native Antfly CLI installer package"
    metadata["Author-email"] = "Antfly, Inc. <ajroetker@antfly.io>"
    metadata["License"] = "Elastic-2.0"
    metadata["Requires-Python"] = project_requires_python(
        repo_root / "py" / "packages" / "cli" / "pyproject.toml"
    )
    metadata["Project-URL"] = "Homepage, https://antfly.io"
    metadata["Project-URL"] = "Documentation, https://docs.antfly.io"
    metadata["Project-URL"] = "Repository, https://github.com/antflydb/antfly"

    wheel = (
        "Wheel-Version: 1.0\n"
        "Generator: antfly package_cli_release.py\n"
        "Root-Is-Purelib: false\n"
        f"Tag: {tag}\n"
    )
    entry_points = "[console_scripts]\nantfly = antfly_cli:main\n"

    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(source_dir.glob("*.py")):
            write_file(zf, f"{package_name}/{path.name}", path)
        write_file(zf, f"{package_name}/bin/antfly", extracted / "antfly", 0o755)
        for dirname in ("include", "lib", "share"):
            for path in sorted((extracted / dirname).rglob("*")):
                if path.is_file() and not is_packaging_noise(
                    path.relative_to(extracted)
                ):
                    rel = path.relative_to(extracted)
                    write_file(zf, f"{package_name}/{rel.as_posix()}", path)
        write_bytes(zf, f"{dist_info}/METADATA", metadata.as_bytes())
        write_bytes(zf, f"{dist_info}/WHEEL", wheel.encode())
        write_bytes(zf, f"{dist_info}/entry_points.txt", entry_points.encode())

        record_path = f"{dist_info}/RECORD"
        record_rows = [*records, (record_path, "", "")]
        record_buf = []
        for row in record_rows:
            record_buf.append(row)
        csv_io = io.StringIO()
        writer = csv.writer(csv_io)
        writer.writerows(record_buf)
        csv_text = csv_io.getvalue()
        write_bytes(zf, record_path, csv_text.encode())
        records.pop()
    return wheel_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--version", required=True, help="Antfly version, with or without v prefix"
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=Path("dist"),
        help="Directory containing release archives",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("dist/cli-packages"),
        help="Output directory",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    version = normalize_release_version(args.version)
    python_version = python_version_from_release(version)
    archive_dir = args.archive_dir.resolve()
    out_dir = args.out_dir.resolve()
    py_out = out_dir / "python"

    update_pyproject_version(
        repo_root / "py" / "packages" / "cli" / "pyproject.toml", python_version
    )
    update_json_version(
        repo_root / "ts" / "packages" / "cli" / "package.json",
        version,
        optional_deps=True,
    )
    for platform in PACKAGE_PLATFORMS:
        validate_npm_package(repo_root, platform)
        assert platform.npm_package_dir is not None
        update_json_version(
            repo_root / "ts" / "packages" / platform.npm_package_dir / "package.json",
            version,
        )

    clean_path(py_out)
    for platform in PACKAGE_PLATFORMS:
        with tempfile.TemporaryDirectory() as tmp_raw:
            extracted = Path(tmp_raw)
            extract_archive(archive_dir, version, platform, extracted)
            copy_antfarm(repo_root, extracted)
            populate_npm_package(repo_root, platform, extracted)
            if platform.wheel_platform:
                wheel = package_python_wheel(
                    repo_root, py_out, python_version, platform, extracted
                )
                print(f"wrote {wheel}")

    print("npm package directories populated under ts/packages/cli-*")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
