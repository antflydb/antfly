#!/usr/bin/env python3
"""Build the Antfly release payload and manifest files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

SOURCE_MANIFEST = "source-snapshot.json"
REQUIRED_SOURCE_FILES = {"install.sh", "openapi.yaml"}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_payload_file(src: Path, out_dir: Path) -> Path:
    if not src.exists():
        raise SystemExit(f"missing release payload file: {src}")
    dst = out_dir / src.name
    shutil.copy2(src, dst)
    return dst


def artifact_kind(path: Path) -> str:
    name = path.name
    if name.startswith("antfly_") and name.endswith(".tar.gz"):
        return "runtime-archive"
    if name.endswith("_checksums.txt"):
        return "checksums"
    if name == "install.sh":
        return "installer"
    if name == "openapi.yaml":
        return "openapi"
    if name == SOURCE_MANIFEST:
        return "source-manifest"
    if name == "cli-snapshot.json":
        return "cli-manifest"
    if name.endswith(".whl"):
        return "python-wheel"
    if name.endswith(".tgz"):
        return "npm-package"
    return "support"


def artifact_scope(kind: str) -> str:
    if kind == "runtime-archive":
        return "runtime"
    if kind in {"cli-manifest", "python-wheel", "npm-package"}:
        return "cli"
    return "support"


def generated_at(repo_root: Path, commit: str) -> str:
    raw_epoch = os.environ.get("SOURCE_DATE_EPOCH")
    if raw_epoch is None:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "show", "-s", "--format=%ct", commit],
            check=True,
            capture_output=True,
            text=True,
        )
        raw_epoch = result.stdout.strip()
    try:
        epoch = int(raw_epoch)
    except ValueError as exc:
        raise SystemExit(f"invalid release source timestamp: {raw_epoch}") from exc
    return (
        datetime.fromtimestamp(epoch, timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def verify_source_snapshot(source_dir: Path, commit: str) -> list[Path]:
    manifest_path = source_dir / SOURCE_MANIFEST
    if not manifest_path.is_file():
        raise SystemExit(f"missing release source manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1 or manifest.get("commit") != commit:
        raise SystemExit("release source snapshot does not match the release commit")
    entries = manifest.get("artifacts")
    if not isinstance(entries, list):
        raise SystemExit("release source snapshot contains no artifacts")

    verified: list[Path] = []
    names: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise SystemExit("release source snapshot contains an invalid artifact")
        name = entry.get("name")
        if not isinstance(name, str) or name != Path(name).name or name in names:
            raise SystemExit(
                "release source snapshot contains an invalid artifact name"
            )
        path = source_dir / name
        if (
            not path.is_file()
            or entry.get("size") != path.stat().st_size
            or entry.get("sha256") != sha256(path)
        ):
            raise SystemExit(f"release source artifact differs from manifest: {name}")
        names.add(name)
        verified.append(path)
    if names != REQUIRED_SOURCE_FILES:
        raise SystemExit(
            f"release source artifact set mismatch: expected {sorted(REQUIRED_SOURCE_FILES)}, got {sorted(names)}"
        )
    return [*verified, manifest_path]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tag", required=True, help="release tag, for example v0.2.0-rc.1"
    )
    parser.add_argument("--commit", required=True, help="commit SHA for this release")
    parser.add_argument(
        "--archive-dir",
        type=Path,
        required=True,
        help="directory containing antfly_*.tar.gz",
    )
    parser.add_argument(
        "--extra-dir",
        type=Path,
        help="directory containing prebuilt registry package artifacts",
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        required=True,
        help="directory containing commit-bound support files and source-snapshot.json",
    )
    parser.add_argument(
        "--out-dir", type=Path, required=True, help="output payload directory"
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    tag = args.tag
    version = tag.removeprefix("v")
    prerelease = "-" in version

    out_dir = args.out_dir.resolve()
    if out_dir.exists() and any(out_dir.iterdir()):
        raise SystemExit(f"release payload directory must be empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    copied: list[Path] = []
    archives = sorted(args.archive_dir.glob("antfly_*.tar.gz"))
    if not archives:
        raise SystemExit(f"no antfly release archives found in {args.archive_dir}")

    for archive in archives:
        copied.append(copy_payload_file(archive, out_dir))

    registry_versions = None
    if args.extra_dir:
        extra_files = sorted(
            path for path in args.extra_dir.iterdir() if path.is_file()
        )
        if not extra_files:
            raise SystemExit(f"no extra release artifacts found in {args.extra_dir}")
        for extra in extra_files:
            if any(path.name == extra.name for path in copied):
                raise SystemExit(f"duplicate release payload file: {extra.name}")
            copied.append(copy_payload_file(extra, out_dir))
        cli_manifest = json.loads((args.extra_dir / "cli-snapshot.json").read_text())
        if (
            cli_manifest.get("commit") != args.commit
            or cli_manifest.get("version") != version
        ):
            raise SystemExit(
                "CLI snapshot does not match the release version and commit"
            )
        registry_versions = cli_manifest.get("registry_versions")

    checksums = out_dir / "antfly_zig_checksums.txt"
    with checksums.open("w", encoding="utf-8") as dst:
        for archive in copied:
            if artifact_kind(archive) == "runtime-archive":
                dst.write(f"{sha256(archive)}  {archive.name}\n")
    copied.append(checksums)

    for source in verify_source_snapshot(args.source_dir, args.commit):
        copied.append(copy_payload_file(source, out_dir))

    release_generated_at = generated_at(repo_root, args.commit)
    artifacts = []
    for path in copied:
        kind = artifact_kind(path)
        artifacts.append(
            {
                "name": path.name,
                "kind": kind,
                "scope": artifact_scope(kind),
                "size": path.stat().st_size,
                "sha256": sha256(path),
            }
        )
    metadata = {
        "tag": tag,
        "version": version,
        "commit": args.commit,
        "prerelease": prerelease,
        "generated_at": release_generated_at,
        "registry_versions": registry_versions,
        "artifacts": artifacts,
    }

    metadata_path = out_dir / "metadata.json"
    artifacts_path = out_dir / "artifacts.json"
    metadata_path.write_text(
        json.dumps(metadata, separators=(",", ":")) + "\n", encoding="utf-8"
    )
    ledger_artifacts = [
        *artifacts,
        {
            "name": metadata_path.name,
            "kind": "support",
            "scope": "support",
            "size": metadata_path.stat().st_size,
            "sha256": sha256(metadata_path),
        },
    ]
    artifacts_path.write_text(
        json.dumps(
            {
                "tag": tag,
                "version": version,
                "commit": args.commit,
                "schema_version": 2,
                "generated_at": release_generated_at,
                "registry_versions": registry_versions,
                "artifacts": ledger_artifacts,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print(f"wrote release payload to {out_dir}")
    for path in sorted(out_dir.iterdir()):
        if path.is_file():
            print(f"  {path.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
