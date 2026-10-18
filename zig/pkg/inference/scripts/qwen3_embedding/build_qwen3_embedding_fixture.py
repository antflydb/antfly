#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Build exact, tokenizer-verified Qwen fixtures without downloading artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from benchmark_qwen3_embedding_endpoint import LEGACY_FIXTURE_SCHEMA, sha256_file


def build_cases(tokenizer, lengths, count, eos, query_prefix=""):
    continuation_ids = tokenizer.encode(" token", add_special_tokens=False).ids
    if len(continuation_ids) != 1:
        raise ValueError("fixture continuation must encode to one token")
    prefixes = []
    for token_id in sorted(tokenizer.get_vocab().values()):
        text = tokenizer.decode([token_id], skip_special_tokens=False)
        if not text or "<|" in text:
            continue
        if tokenizer.encode(text, add_special_tokens=False).ids != [token_id]:
            continue
        if tokenizer.encode(text + " token", add_special_tokens=False).ids != [
            token_id,
            *continuation_ids,
        ]:
            continue
        prefixes.append((token_id, text))
        if len(prefixes) == count:
            break
    if len(prefixes) != count:
        raise ValueError(
            f"only {len(prefixes)} verified single-token prefixes, need {count}"
        )
    cases = []
    for length in lengths:
        for token_id, prefix in prefixes:
            base = query_prefix + prefix
            ids = tokenizer.encode(base, add_special_tokens=False).ids
            continuation = length - len(ids) - 1
            if continuation < 0:
                raise ValueError(f"rendered prefix exceeds {length} tokens")
            text = base + " token" * continuation
            ids = tokenizer.encode(text, add_special_tokens=False).ids + [eos]
            if len(ids) != length:
                raise ValueError(f"tokenization boundary changed for prefix {token_id}")
            cases.append(
                {"id": f"tokens_{length}_{token_id}", "text": text, "token_ids": ids}
            )
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tokenizer", type=Path, required=True)
    parser.add_argument("--model-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--lengths", default="20,32,64,256,511,2551")
    parser.add_argument("--cases-per-length", type=int, default=768)
    parser.add_argument("--eos-id", type=int, default=151643)
    parser.add_argument("--query-prefix", default="")
    args = parser.parse_args()
    lengths = [int(value) for value in args.lengths.split(",")]
    if args.cases_per_length < 1 or not lengths or any(value < 2 for value in lengths):
        parser.error(
            "positive case count and lengths of at least two tokens are required"
        )
    from tokenizers import Tokenizer

    tokenizer = Tokenizer.from_file(str(args.tokenizer))
    payload = {
        "schema": LEGACY_FIXTURE_SCHEMA,
        "model_sha256": sha256_file(args.model_file),
        "tokenizer_sha256": sha256_file(args.tokenizer),
        "query_prefix": args.query_prefix,
        "cases": build_cases(
            tokenizer, lengths, args.cases_per_length, args.eos_id, args.query_prefix
        ),
    }
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
    )
    print(
        f"verified {len(payload['cases'])} cases; fixture SHA-256 {sha256_file(args.output)}"
    )


if __name__ == "__main__":
    main()
