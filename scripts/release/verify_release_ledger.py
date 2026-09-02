#!/usr/bin/env python3
"""Verify a promoted release subset against its immutable release ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path


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
    if ledger.get("schema_version") != 1:
        raise SystemExit("unsupported release ledger schema")
    if ledger.get("tag") != args.tag or ledger.get("commit") != args.commit:
        raise SystemExit("release ledger does not match the requested tag and commit")

    entries: dict[str, dict[str, object]] = {}
    for entry in ledger.get("artifacts", []):
        name = entry.get("name")
        if not isinstance(name, str) or not name or name in entries:
            raise SystemExit("release ledger contains an invalid or duplicate artifact")
        entries[name] = entry

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
