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

"""Process-mocked baseline protocol tests; no model or GPU qualification."""

import argparse
import json
import subprocess

import pytest

import quality_baseline as qb


class Tokenizer:
    def apply_chat_template(self, messages, **kwargs):
        assert kwargs == {
            "tokenize": False,
            "add_generation_prompt": True,
            "enable_thinking": False,
        }
        return json.dumps(messages)

    def encode(self, prompt, **kwargs):
        assert kwargs == {"add_special_tokens": False}
        return [1, 2, 3]

    def decode(self, ids, **kwargs):
        return "A wrong mocked answer"


def arguments(tmp_path):
    model = tmp_path / "model"
    model.mkdir()
    (model / "model_manifest.json").write_bytes(qb.MANIFEST)
    (model / ".antfly-download-complete.json").write_text("{}")
    binary = tmp_path / "native"
    binary.write_text("mock executable")
    return argparse.Namespace(
        native_binary=binary,
        model=model,
        output=tmp_path / "out",
        suite=qb.DEFAULT_SUITE,
        case_id=["copper_badge_a", "copper_badge_b"],
        max_new_tokens=48,
        timeout=10,
    )


def timing():
    return {
        "backend": "cuda",
        "tokens": 1,
        "token_ids": [42],
        "finish_reason": "eos",
        "cuda": {"bf16_scalar_linear_calls": 0, "cross_backend_sync_fallbacks": 0},
        "cuda_generate": {"kernel_launches": 10, "bf16_scalar_linear_calls": 0},
    }


def logs():
    return (
        "prompt_token_ids: 1 2 3\ntoken_ids: 42\n"
        "finish_reason=eos tokens=1\n"
        f"cuda_runtime_bytes: h2d=0 d2h=1 d2d=0 resident_weights={qb.F32_WEIGHT_BYTES} device_allocated=1\n"
        "cuda_fallback_counts: host_attention=0 rope=0 rope_per_item=0 gqa_dense=0 paged_attention=0\n"
    )


def fake_execute(calls):
    def execute(argv, **kwargs):
        calls.append(argv)
        assert kwargs == {
            "capture_output": True,
            "text": True,
            "timeout": 10,
            "check": False,
        }
        path = argv[argv.index("--json-timing") + 1]
        qb.save(qb.Path(path), timing())
        return subprocess.CompletedProcess(argv, 0, "", logs())

    return execute


def test_messages_include_only_raw_context_and_unchanged_question():
    case = {
        "question": "\u2003Who owns it?\u2003",
        "documents": ["raw one", "raw two"],
        "answer": {"aliases": ["DO_NOT_LEAK"]},
        "evidence_document_indices": [999],
    }
    full = qb.messages(case, "full_context")
    assert full[0]["content"] == qb.SYSTEM + "\nMemory\nraw one\n\nraw two\nMemory"
    assert full[1]["content"] == "Who owns it?"
    closed = qb.messages(case, "closed_book")
    assert closed[0]["content"] == qb.SYSTEM
    assert "raw" not in json.dumps(closed)
    assert "DO_NOT_LEAK" not in json.dumps(full)
    assert "999" not in json.dumps(full)


def test_sequential_runner_persists_prompts_ids_and_scoring(tmp_path):
    args = arguments(tmp_path)
    calls = []
    result = qb.run(
        args,
        tokenizer=Tokenizer(),
        verify_model=lambda *_: {"files": []},
        execute=fake_execute(calls),
    )
    assert len(calls) == 4
    assert result["completed"] and result["execution_passed"]
    assert result["assets_unchanged"] and not result["rollout_qualified"]
    assert [r["condition"] for r in result["results"]] == list(qb.CONDITIONS) * 2
    assert result["scores"]["full_context"]["selected_case_count"] == 2
    assert result["scores"]["full_context"]["selected_case_accuracy"] == 0
    assert all(r["prompt_ids_equal"] for r in result["results"])
    assert not result["results"][0]["arithmetic_evidence"][
        "complete_actual_inventory_and_capture_proof"
    ]
    assert len(list(args.output.glob("*.prompt.txt"))) == 4
    assert len(list(args.output.glob("*.stderr"))) == 4
    for argv in calls:
        # Standalone antfly-inference-dev dispatches direct subcommands.
        assert argv[1:3] == ["generate", str(args.model)]
        assert "inference" not in argv
        assert argv[argv.index("--cache-dtype") + 1] == "f32"
        assert "--raw-prompt" in argv and "--no-bos" in argv
        assert "--ignore-eos" not in argv


def test_failure_preserved_without_second_condition_or_fallback(tmp_path):
    args = arguments(tmp_path)
    calls = []

    def fail(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(
            argv, 1, "partial stdout", "UnqualifiedMemorillaDecoderPrecision"
        )

    with pytest.raises(RuntimeError, match="no fallback"):
        qb.run(
            args,
            tokenizer=Tokenizer(),
            verify_model=lambda *_: {"files": []},
            execute=fail,
        )
    record = json.loads((args.output / "summary.json").read_text())
    assert not record["completed"] and not record["execution_passed"]
    assert len(calls) == 1
    assert (
        next(args.output.glob("*.stderr")).read_text()
        == "UnqualifiedMemorillaDecoderPrecision"
    )
    with pytest.raises(ValueError, match="already exists"):
        qb.run(args, tokenizer=Tokenizer())


def test_wrong_native_prompt_ids_fail_after_logs_are_saved(tmp_path):
    args = arguments(tmp_path)

    def execute(argv, **kwargs):
        return subprocess.CompletedProcess(
            argv, 0, "", logs().replace("1 2 3", "1 2 4")
        )

    with pytest.raises(ValueError, match="prompt token IDs differ"):
        qb.run(
            args,
            tokenizer=Tokenizer(),
            verify_model=lambda *_: {"files": []},
            execute=execute,
        )
    assert len(list(args.output.glob("*.stderr"))) == 1


@pytest.mark.parametrize("change", ["weights", "fallback", "bf16", "kernel", "backend"])
def test_arithmetic_counter_violations_rejected(change):
    data, log = timing(), logs()
    if change == "weights":
        log = log.replace(str(qb.F32_WEIGHT_BYTES), "123")
    elif change == "fallback":
        log = log.replace("host_attention=0", "host_attention=1")
    elif change == "bf16":
        data["cuda"]["bf16_scalar_linear_calls"] = 1
    elif change == "kernel":
        data["cuda_generate"]["kernel_launches"] = 0
    else:
        data["backend"] = "native"
    with pytest.raises(ValueError):
        qb.arithmetic_evidence(data, log)


@pytest.mark.parametrize(
    "log",
    ["", "prompt_token_ids: 1\nprompt_token_ids: 1\n", "prompt_token_ids: 1 xx\n"],
)
def test_prompt_id_parser_rejects_missing_ambiguous_or_malformed(log):
    with pytest.raises(ValueError):
        qb.printed_ids(log, "prompt_token_ids")


def test_unqualified_asset_not_installed_or_repaired(tmp_path):
    args = arguments(tmp_path)
    manifest = args.model / "model_manifest.json"
    manifest.write_text("{}")
    with pytest.raises(ValueError, match="no asset mutation"):
        qb.run(args, tokenizer=Tokenizer())
    assert manifest.read_text() == "{}"
    assert not json.loads((args.output / "summary.json").read_text())[
        "execution_passed"
    ]


def test_timeout_preserves_partial_output(tmp_path):
    args = arguments(tmp_path)

    def execute(argv, **kwargs):
        raise subprocess.TimeoutExpired(
            argv, 10, output=b"partial", stderr=b"timeout details"
        )

    with pytest.raises(subprocess.TimeoutExpired):
        qb.run(
            args,
            tokenizer=Tokenizer(),
            verify_model=lambda *_: {"files": []},
            execute=execute,
        )
    assert next(args.output.glob("*.stdout")).read_bytes() == b"partial"
    assert next(args.output.glob("*.stderr")).read_bytes() == b"timeout details"


def test_actual_archived_native_timing_stop_and_unretained_ids():
    fixture = json.loads((qb.HERE / "fixtures/baseline-native-output.json").read_text())
    ids, provenance = qb.validate_generation(fixture["timing"], fixture["log"], 48)
    assert ids == [51, 859, 1579]
    assert provenance["timing_ids"] == "not_retained"
    assert "not relabeled" in provenance["finish_reason_semantics"]
    assert (
        qb.arithmetic_evidence(fixture["timing"], fixture["log"])[
            "resident_weight_bytes"
        ]
        == qb.F32_WEIGHT_BYTES
    )


@pytest.mark.parametrize("change", ["nonnull_ids", "count", "printed_count", "finish"])
def test_actual_native_token_contract_rejects_inconsistency(change):
    fixture = json.loads((qb.HERE / "fixtures/baseline-native-output.json").read_text())
    timing, log = fixture["timing"], fixture["log"]
    if change == "nonnull_ids":
        timing["token_ids"] = [51, 859, 1580]
    elif change == "count":
        timing["tokens"] = 4
    elif change == "printed_count":
        log = log.replace("finish_reason=stop tokens=3", "finish_reason=stop tokens=4")
    else:
        timing["finish_reason"] = "eos"
    with pytest.raises(ValueError, match="token IDs/count/finish"):
        qb.validate_generation(timing, log, 48)


def test_explicit_resume_reuses_valid_artifacts_preserves_failure(
    tmp_path, monkeypatch
):
    args = arguments(tmp_path)
    calls = []
    valid = fake_execute(calls)

    original_validator = qb.validate_generation

    def broken_validator(*args):
        raise ValueError("simulated old parser bug after successful native execution")

    monkeypatch.setattr(qb, "validate_generation", broken_validator)

    with pytest.raises(ValueError):
        qb.run(
            args,
            tokenizer=Tokenizer(),
            verify_model=lambda *_: {"files": []},
            execute=valid,
        )
    failed = (args.output / "summary.json").read_bytes()
    monkeypatch.setattr(qb, "validate_generation", original_validator)
    args.resume = True
    result = qb.run(
        args,
        tokenizer=Tokenizer(),
        verify_model=lambda *_: {"files": []},
        execute=valid,
    )
    assert result["completed"] and len(calls) == 4  # three new calls, one reuse
    assert result["results"][0]["native_artifacts_reused"]
    assert qb.Path(result["resumed_from"]["summary"]).read_bytes() == failed


def test_resume_rejects_changed_prompt_without_execution_or_replacing_summary(tmp_path):
    args = arguments(tmp_path)
    calls = []

    def fail(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, 10)

    with pytest.raises(subprocess.TimeoutExpired):
        qb.run(
            args,
            tokenizer=Tokenizer(),
            verify_model=lambda *_: {"files": []},
            execute=fail,
        )
    original = (args.output / "summary.json").read_bytes()
    args.resume = True
    next(args.output.glob("*.prompt.txt")).write_text("tampered")
    with pytest.raises(ValueError, match="incomplete saved native artifacts"):
        qb.run(
            args,
            tokenizer=Tokenizer(),
            verify_model=lambda *_: {"files": []},
            execute=fake_execute(calls),
        )
    assert not calls and (args.output / "summary.json").read_bytes() == original
