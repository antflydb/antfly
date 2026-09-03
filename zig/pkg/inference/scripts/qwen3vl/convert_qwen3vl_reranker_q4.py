#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Compatibility entry point for the ranking-only Q4_K_M reranker tier."""

from __future__ import annotations

import sys

from convert_qwen3vl_reranker import main


def has_option(args: list[str], name: str) -> bool:
    return any(arg == name or arg.startswith(f"{name}=") for arg in args)


if __name__ == "__main__":
    if not has_option(sys.argv[1:], "--decoder-quantization"):
        sys.argv[1:1] = ["--decoder-quantization", "Q4_K_M"]
    raise SystemExit(main())
