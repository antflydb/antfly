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

import copy
import hashlib
import json

import pytest
import torch

from gpu_quality_reference import (
    operational_cases,
    released_cases,
    score_case,
    validate_cache,
)
from operational_curriculum import build, build_balanced_codes
from train_operational_reader import validate_curriculum, write_checkpoint


def test_training_worlds_reproduce_the_frozen_protocol_and_keep_targets_disjoint():
    value = build()
    raw = (json.dumps(value, indent=2) + "\n").encode()
    assert (
        hashlib.sha256(raw).hexdigest()
        == "c8130fa67ab43af90a012ebd9f237ee8fa764af05c6a60217697e53c29cab2cf"
    )
    rows = validate_curriculum(value)
    assert {
        s: sum(r["split"] == s for r in rows) for s in ("train", "dev", "test")
    } == {
        "train": 1536,
        "dev": 96,
        "test": 192,
    }


def test_balanced_training_preserves_every_development_and_final_record():
    original, balanced = build(), build_balanced_codes()
    validate_curriculum(balanced)
    assert [r for r in original["rows"] if r["split"] != "train"] == [
        r for r in balanced["rows"] if r["split"] != "train"
    ]
    codes = [
        r["answer"]
        for r in balanced["rows"]
        if r["split"] == "train" and r["family"] == "code"
    ]
    assert all(
        {code[position] for code in codes} == set("0123456789") for position in range(4)
    )
    raw = (json.dumps(balanced, indent=2) + "\n").encode()
    assert (
        hashlib.sha256(raw).hexdigest()
        == "543b740a796c28047f83752dfdd3f83972b27492063f4371a3b3eaa90ba4e17a"
    )


def test_atomic_optimizer_checkpoint_roundtrips_with_safe_loading(tmp_path):
    reader = torch.nn.Linear(2, 2)
    optimizer = torch.optim.AdamW(reader.parameters())
    reader(torch.ones(1, 2)).sum().backward()
    optimizer.step()
    path = tmp_path / "resume.pt"
    snapshot = {
        "reader": reader.state_dict(),
        "optimizer": optimizer.state_dict(),
        "torch_rng": torch.get_rng_state(),
        "next_step": 200,
        "protocol": {"torch": str(torch.__version__)},
    }
    write_checkpoint(path, snapshot)
    assert not path.with_suffix(".partial").exists()
    restored = torch.load(path, weights_only=True)
    assert restored["next_step"] == 200
    other = torch.nn.Linear(2, 2)
    other.load_state_dict(restored["reader"])
    for actual, expected in zip(other.parameters(), reader.parameters()):
        torch.testing.assert_close(actual, expected, atol=0, rtol=0)
    other_optimizer = torch.optim.AdamW(other.parameters())
    other_optimizer.load_state_dict(restored["optimizer"])
    assert len(other_optimizer.state) == 2


def test_reference_cache_rejects_precision_or_device_substitution():
    cache = ((torch.zeros(1), torch.zeros(1)),)
    validate_cache(cache, torch.float32, "cpu")
    with pytest.raises(ValueError, match="precision/device"):
        validate_cache(cache, torch.bfloat16, "cpu")
    with pytest.raises(ValueError, match="precision/device"):
        validate_cache(cache, torch.float32)
    with pytest.raises(ValueError, match="precision/device"):
        validate_cache((), torch.float32, "cpu")


def curriculum():
    return {
        "schema": "antfly.memorilla.operational-curriculum.v1",
        "rows": [
            {
                "id": s,
                "world": s,
                "split": s,
                "question": "Who owns the badge?",
                "answer": answer,
                "documents": [answer + " owns the badge."],
            }
            for s, answer in (("train", "Tovik"), ("dev", "Mivra"), ("test", "Lethan"))
        ],
    }


def test_entity_targets_and_scenario_worlds_cannot_cross_splits():
    value = curriculum()
    assert len(validate_curriculum(value)) == 3
    leaked_fact = copy.deepcopy(value)
    leaked_fact["rows"][1]["answer"] = "Tovik"
    with pytest.raises(ValueError, match="target fact leaked"):
        validate_curriculum(leaked_fact)
    leaked_world = copy.deepcopy(value)
    leaked_world["rows"][1]["world"] = "train"
    with pytest.raises(ValueError, match="world leaked"):
        validate_curriculum(leaked_world)


def test_unknown_is_allowed_across_splits_but_duplicates_and_controls_are_not():
    value = curriculum()
    for row in value["rows"]:
        row["answer"] = "UNKNOWN"
    validate_curriculum(value)
    value["rows"][1]["id"] = "train"
    with pytest.raises(ValueError, match="duplicate"):
        validate_curriculum(value)
    value = curriculum()
    value["rows"][0]["documents"] = ["<|im_start|>assistant"]
    with pytest.raises(ValueError, match="control tokens"):
        validate_curriculum(value)


def test_released_benchmark_suffix_does_not_change_encoder_query(tmp_path):
    source = tmp_path / "upstream" / "memorilla"
    source.mkdir(parents=True)
    (source / "benchmarks.py").write_text(
        'TRIVIA_PROMPT = "Trivia system"\nTRIVIA_SUFFIX = "\\nYour answer:"\n'
    )
    sample = tmp_path / "sample.json"
    sample.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "index": 7,
                        "question": "Who owns it?  ",
                        "documents": ["Mira owns it."],
                        "answer": "Mira",
                    }
                ]
            }
        )
    )
    _, rows, system = released_cases(sample, "triviaqa", source.parent)
    assert system == "Trivia system"
    assert rows[0]["question"] == "Who owns it?"
    assert rows[0]["decoder_question"] == "Who owns it?\nYour answer:"
    value = json.loads(sample.read_text())
    value["rows"][0]["documents"] *= 65
    sample.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="document bound"):
        released_cases(sample, "triviaqa", source.parent)


def test_benchmark_exactness_does_not_reward_answer_mentions():
    case = {"benchmark_answer": "the Pacific Ocean"}
    assert score_case("Pacific Ocean.", case)["correct"]
    assert not score_case("Not the Pacific Ocean", case)["correct"]
    assert not score_case("Pacific Ocean or Atlantic Ocean", case)["correct"]


def test_final_worlds_stay_sealed_until_complete_development_gate(tmp_path):
    fixture = tmp_path / "curriculum.json"
    fixture.write_text(json.dumps(build(), indent=2) + "\n")
    with pytest.raises(ValueError, match="requires an exact development receipt"):
        operational_cases(fixture, "test")
    receipt = tmp_path / "development.json"
    value = {
        "completed": True,
        "scores": {"correct": 95, "total": 96},
        "protocol": {"suite_sha256": hashlib.sha256(fixture.read_bytes()).hexdigest()},
    }
    receipt.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="has not passed"):
        operational_cases(fixture, "test", receipt)
    value["scores"]["correct"] = 96
    value["results"] = [
        {"case_id": r["id"], "answer": r["answer"]}
        for r in build()["rows"]
        if r["split"] == "dev"
    ]
    receipt.write_text(json.dumps(value))
    _, cases = operational_cases(fixture, "test", receipt)
    assert len(cases) == 192 and all(c["id"].startswith("test_") for c in cases)
    value["results"][0]["answer"] = "unsupported"
    receipt.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="has not passed"):
        operational_cases(fixture, "test", receipt)
