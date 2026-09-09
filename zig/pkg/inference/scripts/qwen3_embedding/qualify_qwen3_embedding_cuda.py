#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0

"""Fail-closed Qwen3-Embedding CUDA qualification of a running Antfly server.

The numerical and request-contract gates are shared with the Metal lane. This
entry point admits only the two model references promoted for CUDA and derives
the precision threshold from that exact reference, preventing evidence from
being mislabeled with a caller-supplied tier.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import qualify_qwen3_embedding_common as common


QualificationError = common.QualificationError


SCHEMA = "antfly.qwen3_embedding.cuda_qualification.v1"
TIER_MIN_COSINE = {
    "bf16": common.TIER_MIN_COSINE["bf16"],
    "q8_0": common.TIER_MIN_COSINE["q8_0"],
}
CUDA_MODEL_TIERS = {
    "qwen3-embedding": "q8_0",
    "qwen3-embedding-0.6b": "q8_0",
    "Qwen/Qwen3-Embedding-0.6B-GGUF:q8-0-bundle-v1": "q8_0",
    "qwen3-embedding-0.6b-safetensors": "bf16",
    "Qwen/Qwen3-Embedding-0.6B:bf16-safetensors-bundle-v1": "bf16",
}


def model_tier(model: str) -> str:
    normalized = model[3:] if model.startswith("hf:") else model
    try:
        return CUDA_MODEL_TIERS[normalized]
    except KeyError as exc:
        raise QualificationError(
            "CUDA qualification requires a promoted Q8_0 or BF16 model reference"
        ) from exc


def batch_document_case_ids(cases: dict[str, dict[str, object]]) -> list[str]:
    """Keep the mixed batch bounded; long context is already tested alone."""

    return [
        case_id
        for case_id in sorted(cases)
        if cases[case_id]["role"] == "document"
        and not cases[case_id].get("truncated", False)
    ]


def needs_reduced_dimension_replay(case: dict[str, object]) -> bool:
    """Avoid a duplicate full-context forward for output-only MRL checking."""

    return not case.get("truncated", False)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--model", default="qwen3-embedding-0.6b")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    try:
        args.tier = model_tier(args.model)
    except QualificationError as exc:
        parser.error(str(exc))
    return args


def main(argv: list[str] | None = None) -> int:
    return common.main(
        sys.argv[1:] if argv is None else argv,
        parse_args_fn=parse_args,
        schema=SCHEMA,
        document_case_ids_fn=batch_document_case_ids,
        reduced_dimension_replay_fn=needs_reduced_dimension_replay,
    )


if __name__ == "__main__":
    raise SystemExit(main())
