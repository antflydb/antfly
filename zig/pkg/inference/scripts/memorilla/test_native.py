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

"""Optional compiled-tool parity and fail-closed tests, without model downloads.

Set MEMORILLA_VERIFY_BIN to the built antfly-memorilla-verify executable. Native
is the default; MEMORILLA_VERIFY_BACKEND=cuda requires actual CUDA execution.
"""

import json
import os
from pathlib import Path
import subprocess

import pytest
from safetensors.torch import load_file, save_file

from reference import generate_fixtures, sha256
import test_reference


# Reuse the tiny non-release fixture without shadowing an imported symbol in
# each test signature. Pytest registers the decorated function under this name.
bundle = test_reference.bundle


pytestmark = pytest.mark.skipif(
    not os.environ.get("MEMORILLA_VERIFY_BIN"),
    reason="optional compiled native/CUDA verifier",
)


@pytest.fixture
def invocation(bundle: Path, tmp_path: Path) -> list[str]:
    fixtures = tmp_path / "reference"
    generate_fixtures(bundle, fixtures)
    return [
        str(Path(os.environ["MEMORILLA_VERIFY_BIN"]).resolve()),
        "--bundle",
        str(bundle),
        "--reference",
        str(fixtures),
        "--backend",
        os.environ.get("MEMORILLA_VERIFY_BACKEND", "native"),
        "--allow-synthetic",
    ]


def execute(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args, text=True, capture_output=True, timeout=120, check=False
    )


def mutate_manifest(args: list[str], mutation) -> None:
    directory = Path(args[args.index("--bundle") + 1])
    path = directory / "manifest.json"
    value = json.loads(path.read_text())
    mutation(value)
    path.write_text(json.dumps(value))


def test_compiled_module_matches_every_pytorch_boundary(invocation: list[str]) -> None:
    result = execute(invocation)
    assert result.returncode == 0, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines()]
    ready = events[0]
    assert ready["precision_profile"] == "strict-f32-v1"
    assert ready["intermediate_atol"] == ready["memory_tokens_atol"] == 1e-5
    assert ready["relative_l2_limit"] is None
    boundary = [event for event in events if event["event"] == "boundary"]
    assert len(boundary) == 6 * (6 + 2 * 8)
    assert all(event["mismatches"] == 0 for event in boundary)
    complete = events[-1]
    assert complete["event"] == "complete"
    assert complete["cases"] == 6
    assert complete["module_parity_passed"] is True
    assert complete["decoder_qualified"] is False
    if os.environ.get("MEMORILLA_VERIFY_BACKEND") == "cuda":
        proof = next(event for event in events if event["event"] == "cuda_execution")
        assert proof["kernel_launches"] > 0
        assert proof["linear_launches"] > 0
        assert proof["attention_launches"] == 6 * 3
        assert proof["cross_backend_sync_fallbacks"] == 0


@pytest.mark.parametrize(
    "field,value",
    (
        ("layer_norm_epsilon", 0),
        ("gelu", "tanh"),
        ("question_prefix", "Instruct: retrieval"),
        ("projection_layout", "in_features,out_features"),
        ("encoder_pooling", "mean"),
        ("enable_thinking", True),
    ),
)
def test_execution_contract_tampering_rejected(
    invocation: list[str], field: str, value
) -> None:
    mutate_manifest(
        invocation, lambda manifest: manifest["execution"].__setitem__(field, value)
    )
    result = execute(invocation)
    assert result.returncode != 0
    assert "InvalidMemorillaManifest" in result.stderr
    assert '"event":"boundary"' not in result.stdout


def test_artifact_checksum_tampering_rejected(invocation: list[str]) -> None:
    directory = Path(invocation[invocation.index("--bundle") + 1])
    path = directory / "memory.safetensors"
    data = bytearray(path.read_bytes())
    data[-1] ^= 1
    path.write_bytes(data)
    result = execute(invocation)
    assert result.returncode != 0
    assert "MemorillaArtifactMismatch" in result.stderr


def test_synthetic_bundle_requires_explicit_opt_in(invocation: list[str]) -> None:
    invocation.remove("--allow-synthetic")
    result = execute(invocation)
    assert result.returncode != 0
    assert "SyntheticMemorillaBundleNotAllowed" in result.stderr


def test_tensor_inventory_tampering_rejected(invocation: list[str]) -> None:
    mutate_manifest(
        invocation, lambda manifest: manifest.__setitem__("parameter_count", 1)
    )
    result = execute(invocation)
    assert result.returncode != 0
    assert "InvalidMemorillaTensorInventory" in result.stderr


def test_bf16_reference_cannot_be_mislabeled_as_f32_execution(
    invocation: list[str],
) -> None:
    path = Path(invocation[invocation.index("--reference") + 1]) / "reference.json"
    receipt = json.loads(path.read_text())
    receipt["execution"]["dtype"] = "bfloat16"
    path.write_text(json.dumps(receipt))
    result = execute(invocation)
    assert result.returncode != 0
    assert "InvalidReferenceReceipt" in result.stderr


def test_collection_budget_is_enforced(invocation: list[str]) -> None:
    result = execute(invocation + ["--max-documents", "1"])
    assert result.returncode != 0
    assert "MemorillaCollectionBudgetExceeded" in result.stderr
    assert '"module_parity_passed":true' not in result.stdout


def test_named_trained_profile_has_frozen_tolerances(invocation: list[str]) -> None:
    result = execute(invocation + ["--precision-profile", "trained-f32-v1"])
    assert result.returncode == 0, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines()]
    ready = events[0]
    assert ready["precision_profile"] == "trained-f32-v1"
    assert ready["intermediate_atol"] == 2e-4
    assert ready["memory_tokens_atol"] == 5e-5
    assert ready["rtol"] == 1e-5
    assert ready["relative_l2_limit"] == 5e-6
    for event in events:
        if event["event"] == "boundary":
            assert event["passed"] is True
            assert event["relative_l2_error"] <= 5e-6
            assert event["atol"] == (5e-5 if event["name"] == "memory_tokens" else 2e-4)


@pytest.mark.parametrize("flag", ("--atol", "--rtol"))
def test_named_profile_cannot_be_overridden(invocation: list[str], flag: str) -> None:
    result = execute(
        invocation + ["--precision-profile", "trained-f32-v1", flag, "0.5"]
    )
    assert result.returncode != 0
    assert "NamedPrecisionProfileCannotBeOverridden" in result.stderr


def replace_tensor(invocation: list[str], name: str, replacement) -> None:
    directory = Path(invocation[invocation.index("--reference") + 1])
    path = directory / "reference.safetensors"
    # Own the bytes before replacing the file that originally backed its mmap.
    tensors = {key: value.clone() for key, value in load_file(path).items()}
    tensors[name] = replacement(tensors[name])
    save_file(tensors, path)
    receipt_path = directory / "reference.json"
    receipt = json.loads(receipt_path.read_text())
    receipt["tensors"]["sha256"] = sha256(path)
    receipt_path.write_text(json.dumps(receipt))


def test_normwise_gate_rejects_errors_below_component_floor(
    invocation: list[str],
) -> None:
    replace_tensor(
        invocation, "single_document.slots.initial", lambda value: value + 1e-5
    )
    result = execute(invocation + ["--precision-profile", "trained-f32-v1"])
    assert result.returncode != 0
    assert "MemorillaReferenceMismatch" in result.stderr
    boundary = next(
        json.loads(line)
        for line in result.stdout.splitlines()
        if '"case":"single_document","name":"slots.initial"' in line
    )
    assert boundary["mismatches"] == 0
    assert boundary["normwise_passed"] is False
    assert boundary["passed"] is False


def test_zero_reference_norm_uses_absolute_guard(invocation: list[str]) -> None:
    replace_tensor(invocation, "single_document.slots.initial", lambda value: value * 0)
    result = execute(invocation + ["--precision-profile", "trained-f32-v1"])
    assert result.returncode != 0
    boundary = next(
        json.loads(line)
        for line in result.stdout.splitlines()
        if '"case":"single_document","name":"slots.initial"' in line
    )
    assert boundary["relative_l2_error"] is None
    assert boundary["normwise_passed"] is False
    assert boundary["passed"] is False
