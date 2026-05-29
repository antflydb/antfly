#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
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

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

bash ./scripts/prepare_layoutdoc_parity.sh

python3 - <<'PY'
import json
from math import isclose
from pathlib import Path

base = Path("/tmp/layoutdoc_termite_parity")
summary = json.load(open(base / "parity_summary.json"))
seq_req = json.load(open(base / "sequence_request.json"))
tok_req = json.load(open(base / "token_request.json"))
seq_out = json.load(open(base / "sequence_probe_output.json"))
tok_out = json.load(open(base / "token_probe_output.json"))

assert seq_out["checkpoint_path"] == summary["sequence_checkpoint"]
assert tok_out["checkpoint_path"] == summary["token_checkpoint"]

assert seq_out["input"]["image_path"] == seq_req["image_path"]
assert seq_out["input"]["num_tokens"] == seq_req["num_tokens"]
assert len(seq_out["scores"]) == len(seq_req["labels"])
assert seq_out["best"] is not None
assert seq_out["best"]["label"] in seq_req["labels"]
assert isclose(sum(item["score"] for item in seq_out["scores"]), 1.0, rel_tol=0.0, abs_tol=1e-4)

assert tok_out["num_tokens"] == len(tok_req["tokens"])
assert len(tok_out["predictions"]) == len(tok_req["tokens"])
assert len(tok_req["labels"]) == len(summary["token_labels"])

for idx, pred in enumerate(tok_out["predictions"]):
    req_tok = tok_req["tokens"][idx]
    assert pred["token_index"] == idx
    assert pred["text"] == req_tok["text"]
    assert pred["bbox"] == req_tok["bbox"]
    assert pred["best"] is not None
    assert pred["best"]["label"] in tok_req["labels"]
    assert len(pred["scores"]) == len(tok_req["labels"])
    assert isclose(sum(item["score"] for item in pred["scores"]), 1.0, rel_tol=0.0, abs_tol=1e-4)

print("layoutdoc_runtime_verification_passed: 1")
print(f"sequence_best_label: {seq_out['best']['label']}")
print(f"token_predictions: {len(tok_out['predictions'])}")
PY
