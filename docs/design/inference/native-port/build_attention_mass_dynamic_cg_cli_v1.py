#!/usr/bin/env python3
"""Link the isolated dynamic mass-CG CLI and seal its executable receipt."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

SCHEMA = "antfly.attention-matching.highs-v180-real-driver-executable.v1"
LIBRARY_SCHEMA = "antfly.attention-matching.highs-v180-static-library.v2"
PINNED = {
    "lp": "ff1d98cab21450271694ab524dd0733294ae0ff15793c57ed105f4d18d9354de",
    "controller": "abdcc63e7d914b174f3fb8db2302db16af126386b610fcf6e80bf1dbb2ec82fa",
    "adapter": "f9962c9ec47d6399c15eeda30c674fee0993ead3fbcf6f4527bc8037c75e0dc8",
}


def edge(path: Path) -> dict[str, object]:
    path = path.resolve(strict=True)
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"not a regular nonsymlink file: {path}")
    h = hashlib.sha256()
    with path.open("rb") as f:
        while block := f.read(1024 * 1024):
            h.update(block)
    return {"path": str(path), "sha256": h.hexdigest(), "bytes": path.stat().st_size}


def require_edge(value: dict[str, object]) -> None:
    actual = edge(Path(str(value["path"])))
    expected = {k: value[k] for k in ("path", "sha256", "bytes")}
    if actual != expected:
        raise ValueError(f"edge changed: {value['path']}")


def check_edges(value: object) -> None:
    if isinstance(value, dict):
        if {"path", "sha256", "bytes"} <= value.keys():
            require_edge(value)
        else:
            for child in value.values():
                check_edges(child)
    elif isinstance(value, list):
        for child in value:
            check_edges(child)


def write_fresh(path: Path, value: dict[str, object]) -> None:
    encoded = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(encoded)
        while view:
            written = os.write(fd, view)
            if written <= 0:
                raise OSError("short receipt write")
            view = view[written:]
        os.fsync(fd)
    finally:
        os.close(fd)


def run(args: argparse.Namespace, runner=subprocess.run) -> dict[str, object]:
    paths = {
        "driver": args.driver,
        "lp": args.lp,
        "controller": args.controller,
        "adapter": args.adapter,
    }
    supplied = {
        "driver": args.driver_sha256,
        "lp": args.lp_sha256,
        "controller": args.controller_sha256,
        "adapter": args.adapter_sha256,
    }
    sources = {name: edge(path) for name, path in paths.items()}
    for name, expected in supplied.items():
        if sources[name]["sha256"] != expected:
            raise ValueError(f"{name} changed")
    for name, expected in PINNED.items():
        if supplied[name] != expected:
            raise ValueError(f"wrong reviewed {name} identity")
    zig = edge(args.zig)
    if zig["sha256"] != args.zig_sha256:
        raise ValueError("compiler changed")
    library_edge = edge(args.library_receipt)
    if library_edge["sha256"] != args.library_receipt_sha256:
        raise ValueError("library receipt changed")
    library = json.loads(args.library_receipt.read_text())
    if (
        library.get("schema") != LIBRARY_SCHEMA
        or library.get("completed") is not True
        or library.get("execution_authority") is not False
        or library.get("publication_authority") is not False
        or library.get("source", {}).get("commit") != "fcfb5341462f8a7db5ef5038613413f97d6cac3d"
        or library.get("build", {}).get("highsint64") is not False
        or library.get("build", {}).get("highs_no_default_threads") is not False
    ):
        raise ValueError("invalid library contract")
    check_edges(library)
    binary = args.output_binary.resolve()
    receipt = args.output_receipt.resolve()
    if binary.exists() or binary.is_symlink() or receipt.exists() or receipt.is_symlink():
        raise FileExistsError("output not fresh")
    if not binary.parent.is_dir() or not receipt.parent.is_dir():
        raise ValueError("output parent missing")
    for cache in (args.cache_dir, args.global_cache_dir):
        if not cache.resolve().is_dir() or cache.is_symlink():
            raise ValueError("cache directory missing or symlinked")
    argv = [
        str(Path(zig["path"])), "build-exe", "-OReleaseSafe", "-fstrip",
        "--dep", "attention_mass_lp", "--dep", "attention_mass_cg", "--dep", "attention_mass_highs",
        "-Mroot=" + sources["driver"]["path"],
        "-OReleaseSafe", "-Mattention_mass_lp=" + sources["lp"]["path"],
        "-OReleaseSafe", "--dep", "attention_mass_lp", "-Mattention_mass_cg=" + sources["controller"]["path"],
        "-OReleaseSafe", "--dep", "attention_mass_lp", "--dep", "attention_mass_cg", "-Mattention_mass_highs=" + sources["adapter"]["path"],
        library["artifacts"]["libhighs"]["path"], "-lc++",
        "--cache-dir", str(args.cache_dir.resolve()),
        "--global-cache-dir", str(args.global_cache_dir.resolve()),
        "-femit-bin=" + str(binary),
    ]
    environment = {key: value for key, value in os.environ.items() if key in {
        "PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "SDKROOT", "DEVELOPER_DIR",
    }}
    result = runner(argv, cwd=args.work_dir.resolve(), env=environment, check=False)
    if result.returncode:
        raise subprocess.CalledProcessError(result.returncode, argv)
    binary_edge = edge(binary)
    if edge(args.zig) != zig or edge(args.library_receipt) != library_edge:
        raise ValueError("tool or library receipt changed during link")
    check_edges(library)
    for name, path in paths.items():
        if edge(path) != sources[name]:
            raise ValueError(f"{name} changed during link")
    value = {
        "schema": SCHEMA,
        "completed": True,
        "diagnostic_only": True,
        "execution_authority": False,
        "publication_authority": False,
        "target": args.target,
        "optimize": "ReleaseSafe",
        "strip": True,
        "zig_executable": zig,
        "link_argv": argv,
        "sources": sources,
        "library_receipt": library_edge,
        "libhighs": library["artifacts"]["libhighs"],
        "binary": binary_edge,
    }
    write_fresh(receipt, value)
    return value


def parse() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    for name in ("zig", "driver", "lp", "controller", "adapter", "library-receipt"):
        p.add_argument("--" + name, type=Path, required=True)
        p.add_argument("--" + name + "-sha256", required=True)
    p.add_argument("--output-binary", type=Path, required=True)
    p.add_argument("--output-receipt", type=Path, required=True)
    p.add_argument("--cache-dir", type=Path, required=True)
    p.add_argument("--global-cache-dir", type=Path, required=True)
    p.add_argument("--work-dir", type=Path, required=True)
    p.add_argument("--target", required=True)
    return p.parse_args()


if __name__ == "__main__":
    run(parse())
