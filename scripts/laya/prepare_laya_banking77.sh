#!/usr/bin/env bash
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

set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: scripts/laya/prepare_laya_banking77.sh <output-dir>

Downloads Banking77 (PolyAI task-specific-datasets, pinned commit) and writes
native Laya records for candidate-packed qualification (LAYA.md step 0b):

  <output-dir>/b77/{train,calibration,eval}.jsonl

Score the released unpacked model on the same eval cases with upstream's own
code (no 20-option cap):

  uv run --script scripts/laya/laya_upstream_baseline.py <output-dir>/b77/eval.jsonl \
    --model <prepared laya> --common <upstream common.py> --output baseline.json
USAGE
}

[ $# -eq 1 ] || { usage; exit 2; }
DATA_COMMIT=57ec275d8078af65b7731c2a98be812d844a6d6b
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$1/b77"
out=$(cd "$1/b77" && pwd)
for split in train test; do
  [ -f "$out/$split.csv" ] || curl -sSfL -o "$out/$split.csv" \
    "https://raw.githubusercontent.com/PolyAI-LDN/task-specific-datasets/$DATA_COMMIT/banking_data/$split.csv"
done
python3 "$here/prepare_laya_banking77.py" --train "$out/train.csv" --test "$out/test.csv" --output "$out"
