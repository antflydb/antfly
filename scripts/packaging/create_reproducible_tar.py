#!/usr/bin/env python3
"""Create a byte-reproducible tar.gz archive from a directory tree.

Used by scripts/packaging/build_zig_release_archive.sh to package release
artifacts. Rebuilding the same tree on any machine yields a bit-identical
archive (fixed mtime, zeroed owners, normalized modes, sorted entries, no
gzip timestamp), so release checksums stay stable across build
environments and can be independently verified. Symlinks and special
files are rejected rather than silently normalized.
"""

from __future__ import annotations

import argparse
import gzip
import os
import stat
import tarfile
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple


def _archive_entries(source: Path) -> List[Tuple[Path, os.stat_result]]:
    entries: List[Tuple[Path, os.stat_result]] = []
    for path in sorted(
        source.rglob("*"), key=lambda item: item.relative_to(source).as_posix()
    ):
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError(f"archive source contains a symlink: {path}")
        if not (stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode)):
            raise ValueError(f"archive source contains a special file: {path}")
        entries.append((path, metadata))
    return entries


def _tar_info(name: str, metadata: os.stat_result, mtime: int) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = mtime
    info.pax_headers = {}

    if stat.S_ISDIR(metadata.st_mode):
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
        info.size = 0
    else:
        info.type = tarfile.REGTYPE
        info.mode = 0o755 if metadata.st_mode & 0o111 else 0o644
        info.size = metadata.st_size
    return info


def create_archive(source: Path, output: Path, mtime: int) -> None:
    source = source.resolve()
    output = output.resolve()
    if not source.is_dir():
        raise ValueError(f"archive source is not a directory: {source}")
    if mtime < 0:
        raise ValueError("archive mtime must be non-negative")
    if output == source or source in output.parents:
        raise ValueError("archive output must be outside the source directory")

    entries = _archive_entries(source)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=output.parent,
            prefix=f".{output.name}.",
            suffix=".tmp",
            delete=False,
        ) as raw:
            temporary_path = Path(raw.name)
            with gzip.GzipFile(
                filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0
            ) as compressed:
                with tarfile.open(
                    fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT
                ) as archive:
                    root_metadata = source.lstat()
                    archive.addfile(_tar_info(".", root_metadata, mtime))
                    for path, metadata in entries:
                        name = f"./{path.relative_to(source).as_posix()}"
                        info = _tar_info(name, metadata, mtime)
                        if info.isreg():
                            with path.open("rb") as src:
                                archive.addfile(info, src)
                        else:
                            archive.addfile(info)
            raw.flush()
            os.fsync(raw.fileno())
        temporary_path.chmod(0o644)
        os.replace(temporary_path, output)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _non_negative_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if value < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mtime", type=_non_negative_int, required=True)
    args = parser.parse_args()

    try:
        create_archive(args.source, args.output, args.mtime)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
