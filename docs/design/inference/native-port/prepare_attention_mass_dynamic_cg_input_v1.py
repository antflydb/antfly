#!/usr/bin/env python3
"""Create a fresh, portable input manifest for the isolated native mass CG CLI.

This hashes opaque binary inputs only.  It never imports NumPy/SciPy, invokes a
solver, or grants execution/publication authority.  The Zig CLI independently
validates every byte, shape, finite value, and warm-row index before solving.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path

SCHEMA = "antfly.attention-matching.mass-dynamic-cg-input.v1"
PROFILES = {(912, 352), (912, 288), (3000, 288)}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(1024 * 1024):
            h.update(block)
    return h.hexdigest()


def relative_child(owner: Path, child: Path) -> str:
    owner = owner.resolve()
    child = child.resolve()
    try:
        value = child.relative_to(owner)
    except ValueError as exc:
        raise ValueError("binary inputs must be below manifest directory") from exc
    if not value.parts or any(part in ("", ".", "..") for part in value.parts):
        raise ValueError("invalid relative input path")
    return value.as_posix()


def edge(path: Path) -> dict[str, object]:
    path = path.resolve(strict=True)
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"not a regular nonsymlink file: {path}")
    size = path.stat().st_size
    if size <= 0:
        raise ValueError(f"empty input: {path}")
    return {"path": str(path), "sha256": sha256(path), "bytes": size}


def build(args: argparse.Namespace) -> dict[str, object]:
    profile = (args.query_rows, args.retained_rows)
    if profile not in PROFILES:
        raise ValueError("unsupported profile")
    output = args.output.resolve()
    if output.exists() or output.is_symlink():
        raise FileExistsError(output)
    owner = output.parent.resolve(strict=True)
    matrix = edge(args.matrix)
    expected = args.query_rows * args.retained_rows * 8
    if matrix["bytes"] != expected:
        raise ValueError("matrix byte length does not match profile")
    matrix["path"] = relative_child(owner, args.matrix)
    matrix.update(dtype="float64-le", order="C", shape=[args.query_rows, args.retained_rows])
    warm = None
    if args.warm_rows is not None:
        warm = edge(args.warm_rows)
        if warm["bytes"] % 8 or warm["bytes"] // 8 > args.query_rows:
            raise ValueError("invalid warm-row byte length")
        warm["path"] = relative_child(owner, args.warm_rows)
        warm.update(dtype="uint64-le", order="C", count=warm["bytes"] // 8)
    source = edge(args.source)
    identity = hashlib.sha256()
    identity.update(b"antfly.attention-matching.mass-dynamic-cg-request.v1\0")
    identity.update(args.query_rows.to_bytes(8, "little"))
    identity.update(args.retained_rows.to_bytes(8, "little"))
    identity.update(matrix["sha256"].encode())
    if warm is None:
        identity.update(b"\0")
    else:
        identity.update(b"\1")
        identity.update(warm["sha256"].encode())
    identity.update(source["sha256"].encode())
    request_sha = identity.hexdigest()
    return {
        "schema": SCHEMA,
        "completed": True,
        "diagnostic_only": True,
        "execution_authority": False,
        "publication_authority": False,
        "fit_invoked": False,
        "held_tensors_loaded": False,
        "query_rows": args.query_rows,
        "retained_rows": args.retained_rows,
        "matrix": matrix,
        "warm_rows": warm,
        "source": source,
        "request_sha256": request_sha,
    }


def write_fresh(path: Path, value: dict[str, object]) -> None:
    encoded = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, 0o600)
    try:
        view = memoryview(encoded)
        while view:
            count = os.write(fd, view)
            if count <= 0:
                raise OSError("short manifest write")
            view = view[count:]
        os.fsync(fd)
    finally:
        os.close(fd)


def parse() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--query-rows", type=int, required=True)
    p.add_argument("--retained-rows", type=int, required=True)
    p.add_argument("--matrix", type=Path, required=True)
    p.add_argument("--warm-rows", type=Path)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    return p.parse_args()


def main() -> None:
    args = parse()
    write_fresh(args.output.resolve(), build(args))


if __name__ == "__main__":
    main()
