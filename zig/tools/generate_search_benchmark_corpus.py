#!/usr/bin/env python3
"""Generate a deterministic, non-claim synthetic corpus for local tuning.

The checked-in smoke corpus is intentionally tiny. This generator creates
enough documents to exercise postings chunks, multiple ingestion batches, and
the production merge scheduler without pretending to replace the archived
full-corpus benchmark required for publishable results.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--documents", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.documents <= 0:
        parser.error("--documents must be positive")
    return args


def document_text(ordinal: int) -> str:
    terms = ["common", f"bucket{ordinal % 257}", f"group{ordinal % 31}"]
    if ordinal % 3 == 0:
        terms.extend(("alpha", "beta"))  # exact phrase plus intersection
    elif ordinal % 5 == 0:
        terms.extend(("beta", "alpha"))
    elif ordinal % 2 == 0:
        terms.append("alpha")
    if ordinal % 7 == 0:
        terms.extend(("gamma", "gamma"))
    if ordinal % 11 == 0:
        terms.extend(("rare", "signal"))
    terms.extend([f"tail{(ordinal * 17 + offset) % 4093}" for offset in range(ordinal % 9)])
    return " ".join(terms)


def generate(documents: int, output: Path) -> dict[str, object]:
    digest = hashlib.sha256()
    byte_count = 0
    with output.open("x", encoding="utf-8", newline="\n") as destination:
        for ordinal in range(documents):
            record = json.dumps(
                {
                    "id": f"synthetic-{ordinal:010d}",
                    "text": document_text(ordinal),
                    "sort_field": ordinal,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
            encoded = (record + "\n").encode()
            destination.write(record + "\n")
            digest.update(encoded)
            byte_count += len(encoded)
    return {
        "schema_version": 1,
        "kind": "deterministic-local-tuning-corpus",
        "documents": documents,
        "bytes": byte_count,
        "sha256": digest.hexdigest(),
        "output": str(output),
    }


def main() -> int:
    args = arguments()
    result = generate(args.documents, args.output)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
