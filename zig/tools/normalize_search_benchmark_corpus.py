#!/usr/bin/env python3
"""Create the canonical JSONL corpus consumed identically by all adapters."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--text-field", default="text")
    return parser.parse_args()


def normalize(
    input_path: Path, output_path: Path, manifest_path: Path, text_field: str
) -> dict:
    if input_path.resolve() == output_path.resolve():
        raise ValueError("input and output paths must differ")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    source_hash = hashlib.sha256()
    normalized_hash = hashlib.sha256()
    source_bytes = 0
    normalized_bytes = 0
    input_documents = 0
    empty_text_documents = 0

    with input_path.open("rb") as source, output_path.open("wb") as destination:
        for line_number, raw in enumerate(source, 1):
            source_hash.update(raw)
            source_bytes += len(raw)
            stripped = raw.strip()
            if not stripped:
                continue
            input_documents += 1
            try:
                value = json.loads(stripped)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"{input_path}:{line_number}: invalid JSON") from exc
            if not isinstance(value, dict):
                raise ValueError(f"{input_path}:{line_number}: expected a JSON object")
            text = value.get(text_field)
            if not isinstance(text, str):
                raise ValueError(
                    f"{input_path}:{line_number}: field {text_field!r} is not a string"
                )
            if not text:
                empty_text_documents += 1
            encoded = (
                json.dumps({"text": text}, ensure_ascii=False, separators=(",", ":"))
                + "\n"
            ).encode("utf-8")
            destination.write(encoded)
            normalized_hash.update(encoded)
            normalized_bytes += len(encoded)

    manifest = {
        "schema_version": 1,
        "normalization": "json_object_extract_string_to_canonical_text_jsonl",
        "text_field": text_field,
        "input": {
            "path": str(input_path),
            "bytes": source_bytes,
            "sha256": source_hash.hexdigest(),
            "nonblank_documents": input_documents,
        },
        "output": {
            "path": str(output_path),
            "bytes": normalized_bytes,
            "sha256": normalized_hash.hexdigest(),
            "documents": input_documents,
            "empty_text_documents": empty_text_documents,
            "rejected_documents": 0,
            "ordinal_assignment": "zero_based_normalized_input_order_u32",
        },
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def main() -> int:
    args = arguments()
    manifest = normalize(args.input, args.output, args.manifest, args.text_field)
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
