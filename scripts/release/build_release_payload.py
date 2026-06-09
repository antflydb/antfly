#!/usr/bin/env python3
"""Build the Antfly release payload and manifest files."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


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
    return "support"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="release tag, for example v0.2.0-rc.1")
    parser.add_argument("--commit", required=True, help="commit SHA for this release")
    parser.add_argument("--archive-dir", type=Path, required=True, help="directory containing antfly_*.tar.gz")
    parser.add_argument("--out-dir", type=Path, required=True, help="output payload directory")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    tag = args.tag
    version = tag[1:] if tag.startswith("v") else tag
    prerelease = "-" in version

    out_dir = args.out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    copied: list[Path] = []
    archives = sorted(args.archive_dir.glob("antfly_*.tar.gz"))
    if not archives:
        raise SystemExit(f"no antfly release archives found in {args.archive_dir}")

    for archive in archives:
        copied.append(copy_payload_file(archive, out_dir))

    checksums = out_dir / "antfly_zig_checksums.txt"
    with checksums.open("w", encoding="utf-8") as dst:
        for archive in copied:
            if artifact_kind(archive) == "runtime-archive":
                dst.write(f"{sha256(archive)}  {archive.name}\n")
    copied.append(checksums)

    copied.append(copy_payload_file(repo_root / "scripts" / "install.sh", out_dir))
    copied.append(copy_payload_file(repo_root / "openapi.yaml", out_dir))

    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    artifacts = [
        {
            "name": path.name,
            "kind": artifact_kind(path),
            "size": path.stat().st_size,
            "sha256": sha256(path),
        }
        for path in copied
    ]
    metadata = {
        "tag": tag,
        "version": version,
        "commit": args.commit,
        "prerelease": prerelease,
        "generated_at": generated_at,
        "artifacts": artifacts,
    }

    metadata_path = out_dir / "metadata.json"
    artifacts_path = out_dir / "artifacts.json"
    metadata_path.write_text(json.dumps(metadata, separators=(",", ":")) + "\n", encoding="utf-8")
    artifacts_path.write_text(
        json.dumps(
            {
                "tag": tag,
                "version": version,
                "commit": args.commit,
                "generated_at": generated_at,
                "artifacts": artifacts,
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
