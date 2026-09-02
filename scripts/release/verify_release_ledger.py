#!/usr/bin/env python3
"""Verify a promoted release subset against its immutable release ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path


def inferred_scope(entry: dict[str, object]) -> str:
    scope = entry.get("scope")
    if scope in {"runtime", "cli", "support"}:
        return str(scope)
    kind = entry.get("kind")
    if kind == "runtime-archive":
        return "runtime"
    if kind in {"cli-manifest", "python-wheel", "npm-package"}:
        return "cli"
    return "support"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--payload-dir", required=True, type=Path)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--ledger-sha256", required=True)
    parser.add_argument(
        "--scope",
        choices=("runtime", "cli", "support"),
        help="require the payload directory to contain the complete named scope",
    )
    args = parser.parse_args()

    expected_ledger_digest = args.ledger_sha256.lower()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_ledger_digest):
        raise SystemExit("ledger SHA-256 must be exactly 64 hexadecimal characters")
    actual_ledger_digest = sha256(args.ledger)
    if actual_ledger_digest != expected_ledger_digest:
        raise SystemExit(
            "release ledger digest differs:\n"
            f"expected: {expected_ledger_digest}\nactual:   {actual_ledger_digest}"
        )

    ledger = json.loads(args.ledger.read_text(encoding="utf-8"))
    if ledger.get("schema_version") not in {1, 2, 3}:
        raise SystemExit("unsupported release ledger schema")
    if ledger.get("tag") != args.tag or ledger.get("commit") != args.commit:
        raise SystemExit("release ledger does not match the requested tag and commit")

    entries: dict[str, dict[str, object]] = {}
    ledger_artifacts = ledger.get("artifacts")
    if not isinstance(ledger_artifacts, list) or not ledger_artifacts:
        raise SystemExit("release ledger contains no artifacts")
    for entry in ledger_artifacts:
        if not isinstance(entry, dict):
            raise SystemExit("release ledger contains an invalid artifact")
        name = entry.get("name")
        if (
            not isinstance(name, str)
            or not name
            or name != Path(name).name
            or name in entries
        ):
            raise SystemExit("release ledger contains an invalid or duplicate artifact")
        if ledger.get("schema_version") in {2, 3} and entry.get("scope") not in {
            "runtime",
            "cli",
            "support",
        }:
            raise SystemExit(f"release ledger artifact has invalid scope: {name}")
        entries[name] = entry

    if args.scope:
        expected_names = {
            name
            for name, entry in entries.items()
            if inferred_scope(entry) == args.scope
        }
    else:
        expected_names = set(entries)
    if not expected_names:
        raise SystemExit(f"release ledger has no {args.scope} artifacts")
    actual_names = {
        path.name
        for path in args.payload_dir.iterdir()
        if path.is_file() and path.resolve() != args.ledger.resolve()
    }
    if actual_names != expected_names:
        scope_description = args.scope or "payload"
        raise SystemExit(
            f"release {scope_description} scope mismatch: "
            f"expected {sorted(expected_names)}, got {sorted(actual_names)}"
        )

    verified = 0
    for path in sorted(args.payload_dir.iterdir()):
        if not path.is_file() or path.resolve() == args.ledger.resolve():
            continue
        entry = entries.get(path.name)
        if entry is None:
            raise SystemExit(
                f"promoted artifact is absent from release ledger: {path.name}"
            )
        if (
            entry.get("sha256") != sha256(path)
            or entry.get("size") != path.stat().st_size
        ):
            raise SystemExit(
                f"promoted artifact differs from release ledger: {path.name}"
            )
        verified += 1

    if verified == 0:
        raise SystemExit("no promoted artifacts were verified")
    print(
        f"verified {verified} promoted artifacts against release ledger "
        f"{actual_ledger_digest}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
