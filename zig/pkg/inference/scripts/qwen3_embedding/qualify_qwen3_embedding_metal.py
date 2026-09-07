#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0

"""Fail-closed Qwen3-Embedding Metal qualification of a running Antfly server."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import qualify_qwen3_embedding_common as common
from qualify_qwen3_embedding_common import (
    BATCH_MIN_COSINE,
    DIMENSIONS_MIN_COSINE,
    ORACLE_SCHEMA,
    QualificationError,
    RETRIEVAL_QUERY_CASES,
    TIER_MIN_COSINE,
    batch_gates,
    case_gates,
    embedding_request_body,
    gate,
    mrl_gates,
    render_table,
    retrieval_gates,
    retrieval_top1,
    shared_text_gate,
    validate_oracle,
)


SCHEMA = "antfly.qwen3_embedding.metal_qualification.v1"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--model", default="Qwen/Qwen3-Embedding-0.6B-GGUF")
    parser.add_argument("--tier", choices=sorted(TIER_MIN_COSINE), required=True)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    return common.main(
        sys.argv[1:] if argv is None else argv,
        parse_args_fn=parse_args,
        schema=SCHEMA,
    )


if __name__ == "__main__":
    raise SystemExit(main())
