#!/usr/bin/env python3
"""Stage commit-bound support files for an immutable Antfly release."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

SOURCE_FILES = {
    "install.sh": "scripts/install.sh",
    "openapi.yaml": "openapi.yaml",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_object(repo_root: Path, commit: str, source: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "show", f"{commit}:{source}"],
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        detail = result.stderr.decode(errors="replace").strip()
        raise SystemExit(f"cannot read {source} from release commit {commit}: {detail}")
    return result.stdout


def stage_source(repo_root: Path, commit: str, out_dir: Path) -> Path:
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise SystemExit(f"invalid release commit: {commit}")
    if out_dir.exists() and any(out_dir.iterdir()):
        raise SystemExit(f"release source directory must be empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    artifacts = []
    for name, source in SOURCE_FILES.items():
        destination = out_dir / name
        destination.write_bytes(git_object(repo_root, commit, source))
        artifacts.append(
            {
                "name": name,
                "source": source,
                "size": destination.stat().st_size,
                "sha256": sha256(destination),
            }
        )

    manifest = out_dir / "source-snapshot.json"
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "commit": commit,
                "artifacts": artifacts,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    manifest = stage_source(repo_root, args.commit.lower(), args.out_dir.resolve())
    print(f"wrote commit-bound release source manifest to {manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
