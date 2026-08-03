#!/usr/bin/env python3
"""Hash-locked application of reviewed split-declaration candidates."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import stat
import tempfile


ROOT = pathlib.Path(__file__).resolve().parents[2]
CONFLICT_MARKERS = (b"<<<<<<<", b"=======", b">>>>>>>")


def sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def contained_path(root: pathlib.Path, relative: str) -> pathlib.Path:
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"path escapes root: {relative}") from exc
    return path


def select_paths(
    manifest: dict[str, object],
    requested: list[str],
) -> list[str]:
    raw_files = manifest.get("candidate_files")
    if (
        not isinstance(raw_files, list)
        or any(not isinstance(path, str) for path in raw_files)
    ):
        raise ValueError("manifest candidate_files must be a string list")
    available = set(raw_files)
    if not requested:
        return sorted(available)
    selected = set(requested)
    unknown = selected - available
    if unknown:
        raise ValueError(
            "requested path is not in candidate manifest: "
            + ", ".join(sorted(unknown))
        )
    return sorted(selected)


def preflight(
    manifest_path: pathlib.Path,
    requested: list[str],
) -> list[tuple[str, pathlib.Path, bytes, int]]:
    manifest_path = manifest_path.resolve()
    candidate_root = manifest_path.parent
    try:
        candidate_root.relative_to(ROOT.resolve())
    except ValueError:
        pass
    else:
        raise ValueError("candidate directory must be outside the repository")

    manifest = json.loads(manifest_path.read_text())
    current_hashes = manifest.get("current_file_sha256")
    candidate_hashes = manifest.get("candidate_file_sha256")
    if not isinstance(current_hashes, dict) or not isinstance(candidate_hashes, dict):
        raise ValueError("manifest is missing candidate/current file hashes")

    prepared: list[tuple[str, pathlib.Path, bytes, int]] = []
    for relative in select_paths(manifest, requested):
        current = contained_path(ROOT, relative)
        candidate = contained_path(candidate_root, relative)
        if current.is_symlink() or candidate.is_symlink():
            raise ValueError(f"candidate application rejects symlinks: {relative}")
        if not current.is_file() or not candidate.is_file():
            raise ValueError(f"candidate/current file is missing: {relative}")
        current_body = current.read_bytes()
        candidate_body = candidate.read_bytes()
        if sha256_bytes(current_body) != current_hashes.get(relative):
            raise ValueError(f"current file hash changed: {relative}")
        if sha256_bytes(candidate_body) != candidate_hashes.get(relative):
            raise ValueError(f"candidate file hash changed: {relative}")
        if any(marker in candidate_body for marker in CONFLICT_MARKERS):
            raise ValueError(f"candidate contains conflict markers: {relative}")
        prepared.append(
            (relative, current, candidate_body, stat.S_IMODE(current.stat().st_mode))
        )
    return prepared


def apply_preflighted(
    prepared: list[tuple[str, pathlib.Path, bytes, int]],
) -> None:
    staged: list[tuple[pathlib.Path, pathlib.Path]] = []
    try:
        for _, destination, body, mode in prepared:
            fd, raw_path = tempfile.mkstemp(
                prefix=f".{destination.name}.merge-audit-",
                dir=destination.parent,
            )
            temp_path = pathlib.Path(raw_path)
            try:
                with os.fdopen(fd, "wb") as output:
                    output.write(body)
                    output.flush()
                    os.fsync(output.fileno())
                os.chmod(temp_path, mode)
            except BaseException:
                temp_path.unlink(missing_ok=True)
                raise
            staged.append((temp_path, destination))
        for temp_path, destination in staged:
            os.replace(temp_path, destination)
    finally:
        for temp_path, _ in staged:
            temp_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", help="split-declaration-candidates.json")
    parser.add_argument(
        "--path",
        action="append",
        default=[],
        help="apply only this exact repository-relative candidate file",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="perform the preflighted atomic replacements; default is dry-run",
    )
    args = parser.parse_args()

    prepared = preflight(pathlib.Path(args.manifest), args.path)
    for relative, _, body, _ in prepared:
        print(f"{'apply' if args.apply else 'ready'} {relative} {sha256_bytes(body)}")
    if args.apply:
        apply_preflighted(prepared)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
