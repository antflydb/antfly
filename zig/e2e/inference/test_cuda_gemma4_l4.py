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

"""Opt-in Gemma 4 CUDA qualification on a dedicated NVIDIA L4.

Run explicitly on an L4 host:

    ANTFLY_E2E_CUDA_GEMMA4_L4=1 \
    E2B_MODEL=/path/to/e2b.gguf \
    GEMMA12B_Q4_MODEL=/path/to/12b.gguf \
    LLAMA_CPP_BIN=/path/to/llama-completion \
      uv run --project zig/e2e/inference pytest -m cuda_l4 \
        zig/e2e/inference/test_cuda_gemma4_l4.py

Set CUDA_RELEASE_MODE=release and the locked long-context/E4B inputs documented
by ``run_cuda_gemma4_l4_e2e.sh --help`` to enforce the release contract.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest


ENABLE_ENV = "ANTFLY_E2E_CUDA_GEMMA4_L4"
RUNNER = Path(__file__).with_name("run_cuda_gemma4_l4_e2e.sh")

pytestmark = [
    pytest.mark.cuda_l4,
    pytest.mark.model_integration,
    pytest.mark.slow,
]


def test_cuda_gemma4_l4_evidence(tmp_path: Path) -> None:
    if os.environ.get(ENABLE_ENV) != "1":
        pytest.skip(f"set {ENABLE_ENV}=1 to run the dedicated L4 qualification lane")

    env = os.environ.copy()
    evidence_dir = Path(env.setdefault("CUDA_EVIDENCE_DIR", str(tmp_path / "cuda-gemma4-l4")))
    subprocess.run(["bash", str(RUNNER)], check=True, env=env)

    summary_path = evidence_dir / "release_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["release_scope"] == "target_only"
    assert summary["passed"] is True
