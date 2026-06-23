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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export ANTFLY_INFERENCE_GEMMA4_MODEL_NAME="${ANTFLY_INFERENCE_GEMMA4_MODEL_NAME:-ggml-org/gemma-4-E4B-it-GGUF}"
export OUT_DIR="${OUT_DIR:-/tmp/antfly-inference-gemma4-e4b-metal-$(date -u +%Y%m%d-%H%M%S)}"
export ANTFLY_INFERENCE_GEMMA4_MIN_Q4_PAIR_ACT_REDUCE_OUT_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_PAIR_ACT_REDUCE_OUT_F16:-1}"
export ANTFLY_INFERENCE_GEMMA4_MIN_Q6_REDUCE_IN_F16="${ANTFLY_INFERENCE_GEMMA4_MIN_Q6_REDUCE_IN_F16:-1}"

exec "$SCRIPT_DIR/bench_metal_gemma4_e2b.sh"
