#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# Licensed under the Apache License, Version 2.0 (the "License").
"""Extend the compact embedding fixture using a pinned, local tokenizer.

Every expanded case is re-tokenized, including EOS, before writing. Existing
prefixes are preserved; additional single-token ASCII words are selected in
token-ID order. No model weights are loaded and no network access is needed.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re

from benchmark_qwen3_embedding_endpoint import fixture_cases_sha256, sha256_file


def extend_fixture(payload: dict, tokenizer, count: int, lengths: list[int]) -> dict:
    if count < len(payload["recipe"]["prefixes"]):
        raise ValueError("cannot discard existing prefixes")
    if not lengths or len(set(lengths)) != len(lengths) or any(n < 2 for n in lengths):
        raise ValueError("token lengths must be distinct and at least two")
    result = json.loads(json.dumps(payload))
    recipe = result["recipe"]
    recipe["token_counts"] = lengths
    prefixes = recipe["prefixes"]
    existing_ids = {p["token_id"] for p in prefixes}
    existing_names = {p["id"] for p in prefixes}
    for word, token_id in sorted(tokenizer.get_vocab().items(), key=lambda p: p[1]):
        if len(prefixes) >= count:
            break
        if (
            token_id in existing_ids
            or word in existing_names
            or not re.fullmatch("[A-Za-z]{3,12}", word)
        ):
            continue
        if tokenizer.encode(word, add_special_tokens=False).ids != [token_id]:
            continue
        prefixes.append({"id": word, "text": word, "token_id": token_id})
        existing_ids.add(token_id)
        existing_names.add(word)
    if len(prefixes) != count:
        raise ValueError("tokenizer has insufficient distinct single-token prefixes")
    cases = []
    for n in lengths:
        for prefix in prefixes:
            text = prefix["text"] + recipe["continuation"]["text"] * (n - 2)
            expected = (
                [prefix["token_id"]]
                + [recipe["continuation"]["token_id"]] * (n - 2)
                + [recipe["eos_token_id"]]
            )
            observed = tokenizer.encode(text).ids
            if observed != expected:
                raise ValueError(
                    f"tokenizer mismatch for {n} tokens, prefix {prefix['id']}"
                )
            cases.append(
                {
                    "id": f"tokens_{n}_{prefix['id']}",
                    "text": text,
                    "token_ids": expected,
                }
            )
    result["expanded_cases_sha256"] = fixture_cases_sha256(cases)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--tokenizer-file", type=Path, required=True)
    parser.add_argument("--prefixes", type=int, default=128)
    parser.add_argument("--token-counts", default="20,256,511,2551,4096,8192")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    from tokenizers import Tokenizer

    result = extend_fixture(
        json.loads(args.fixture.read_text()),
        Tokenizer.from_file(str(args.tokenizer_file)),
        args.prefixes,
        [int(n) for n in args.token_counts.split(",")],
    )
    result["tokenizer"]["verification"] = {
        "implementation": "Hugging Face tokenizers; every expanded case including EOS",
        "tokenizer_json_sha256": sha256_file(args.tokenizer_file),
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(f"verified {len(result['recipe']['token_counts']) * args.prefixes} cases")


if __name__ == "__main__":
    main()
