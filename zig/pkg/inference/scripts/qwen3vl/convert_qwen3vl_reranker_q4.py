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
