#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: scripts/laya/prepare_laya_fixtures.sh <output-dir>

Builds the PyTorch oracles that the Laya parity tests read through
ANTFLY_LAYA_REFERENCE=<output-dir>/ref. Everything is derived from the pinned
upstream common.py; no model weights are downloaded. Outputs are
deterministic, so they are regenerated rather than committed.

  <output-dir>/common.py   upstream laya/common.py at UPSTREAM_COMMIT
  <output-dir>/ref/        reference, training and tree-packed fixtures

Then, from zig/pkg/inference:

  ANTFLY_LAYA_REFERENCE=<output-dir>/ref zig build test -- --test-filter laya
  ANTFLY_LAYA_METAL=1 ANTFLY_LAYA_BACKEND=metal ANTFLY_LAYA_REFERENCE=<output-dir>/ref \
    zig build test -- --test-filter laya

The finetuned-export oracle needs a trained checkpoint; see
laya_export_reference.py.
USAGE
}

[ $# -eq 1 ] || { usage; exit 2; }
UPSTREAM_COMMIT=6a5819129eb220570792e417e49723d697efd76f
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$1"
out=$(cd "$1" && pwd)

if [ ! -f "$out/common.py" ]; then
  curl -sSfL -o "$out/common.py" \
    "https://raw.githubusercontent.com/NandhaKishorM/laya/$UPSTREAM_COMMIT/laya/common.py"
fi
cd "$here"
uv run --script laya_reference.py --common "$out/common.py" --output "$out/ref"
uv run --script laya_training_reference.py --fixture "$out/ref" --common "$out/common.py"
uv run --script laya_packed_reference.py --fixture "$out/ref" --common "$out/common.py"
