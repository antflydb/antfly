#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
usage: scripts/laya/prepare_laya_training_data.sh <output-dir>

Downloads the released Laya checkpoint and the typed-decisions dataset at
pinned revisions, and derives the splits and subsets used for the packed
fine-tune and trainer timings in zig/pkg/inference/models/laya/LAYA.md.

  <output-dir>/laya-released/          prepared convaiinnovations/laya (~808 MB)
  <output-dir>/td/{train,calibration,eval}.jsonl
                                       native records; calibration holds out 50
                                       cases per workflow (seed 20260924)
  <output-dir>/td/*-fit.jsonl          cases whose state fits the 512-token
                                       upstream budget (<= 316 state tokens)
  <output-dir>/td/s0-*.jsonl           step-0 subsets: 100/20/38 cases per workflow
  <output-dir>/td/prof.json            14-microbatch packed Metal job for step timing

Time a trainer build with:

  antfly-inference finetune train laya <output-dir>/td/prof.json
USAGE
}

[ $# -eq 1 ] || { usage; exit 2; }
CHECKPOINT_REVISION=c5d78730f3493e4fe16d61507ef4b78eef7318cf
DATASET_REVISION=c76749ec58bd8c3d2ea706b31c333a9059c38f90
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$1/td"
out=$(cd "$1" && pwd)

if [ ! -f "$out/laya-released/model.safetensors" ]; then
  (cd "$here" && uv run --script prepare_laya.py convaiinnovations/laya \
    --revision "$CHECKPOINT_REVISION" --output "$out/laya-released")
fi

cd "$out/td"
for split in train test; do
  [ -f "all-$split.parquet" ] || curl -sSfL -o "all-$split.parquet" \
    "https://huggingface.co/datasets/LocalLLaMA/typed-decisions/resolve/$DATASET_REVISION/all/$split-00000-of-00001.parquet"
done

uv run --with pyarrow python - <<'EOF'
import json
import random

import pyarrow.parquet as pq

rows = {s: pq.read_table(f"all-{s}.parquet").to_pylist() for s in ("train", "test")}
random.seed(20260924)
train = rows["train"][:]
random.shuffle(train)
calibration, rest, seen = [], [], {}
for row in train:
    n = seen.get(row["workflow"], 0)
    (calibration if n < 50 else rest).append(row)
    seen[row["workflow"]] = n + 1


def dump(name, items):
    with open(f"{name}-cases.jsonl", "w") as f:
        for row in items:
            case = {k: row[k] for k in ("id", "state", "questions", "gold")}
            f.write(json.dumps(case, ensure_ascii=False) + "\n")


dump("train", rest)
dump("calibration", calibration)
dump("eval", rows["test"])
print("cases", len(rest), len(calibration), len(rows["test"]))
EOF

for split in train calibration eval; do
  rm -f "$split.jsonl"
  python3 "$here/prepare_laya_finetune.py" "$split-cases.jsonl" --output "$out/td/$split.jsonl"
done

uv run --with tokenizers python - <<'EOF'
import json

from tokenizers import Tokenizer

tok = Tokenizer.from_file("../laya-released/tokenizer.json")
for split in ("train", "calibration", "eval"):
    rows = [json.loads(line) for line in open(f"{split}.jsonl")]
    lengths = {}
    for row in rows:
        text = row["text"].replace("[MASK]", " ")
        lengths[row["group_id"]] = len(tok.encode(text, add_special_tokens=False).ids)
    keep = {group for group, n in lengths.items() if n <= 316}
    with open(f"{split}-fit.jsonl", "w") as f:
        for row in rows:
            if row["group_id"] in keep:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(split, "cases", len(lengths), "fit", len(keep))
EOF

python3 - <<'EOF'
import collections
import json


def subset(src, dst, per_workflow):
    groups = collections.OrderedDict()
    for line in open(src):
        row = json.loads(line)
        groups.setdefault(row["group_id"], []).append(row)
    counts, out = collections.Counter(), []
    for group, rows in groups.items():
        workflow = group.split("_", 1)[1].rsplit("_", 1)[0]
        if counts[workflow] < per_workflow:
            counts[workflow] += 1
            out.extend(rows)
    with open(dst, "w") as f:
        for row in out:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(dst, len(out))


subset("train-fit.jsonl", "s0-train.jsonl", 100)
subset("calibration-fit.jsonl", "s0-calibration.jsonl", 20)
subset("eval-fit.jsonl", "s0-eval.jsonl", 38)
EOF
head -5 s0-eval.jsonl > s0-eval-tiny.jsonl

cat > prof.json <<EOF
{"model_dir":"$out/laya-released","train_file":"$out/td/s0-train.jsonl","eval_file":"$out/td/s0-eval-tiny.jsonl","output_dir":"$out/td/prof","backend":"metal","epochs":1,"batch_size":1,"objective":"rlcd","seed":42,"packing":"question","stop_after_microbatches":14}
EOF
