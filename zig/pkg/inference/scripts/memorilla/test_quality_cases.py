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

"""Scorer adversaries and experimental-control validation; no model outputs."""

from copy import deepcopy
import json

import pytest

from quality_cases import (
    DEFAULT_SUITE,
    load_suite,
    normalize_answer,
    score_answer,
    score_suite,
    validate_suite,
)


@pytest.fixture
def suite():
    return load_suite()


@pytest.fixture
def case(suite):
    return suite["cases"][0]


def test_fixture_budget_and_categories(suite):
    assert len(suite["cases"]) == 24
    assert len(suite["pairs"]) == 12
    assert suite["max_new_tokens"] == 48
    assert suite["diagnostic_only"] is True
    assert suite["release_gate"] is False
    assert {c["category"] for c in suite["cases"]} == {
        "single_hop",
        "two_hop",
        "three_hop",
        "order_invariance",
        "missing_fact",
    }
    assert max(len(c["question"].split()) for c in suite["cases"]) < 40


@pytest.mark.parametrize(
    "answer", ["Tovik", "  TOVIK. ", '"Tovik"', "`Tovik`", "Ｔｏｖｉｋ"]
)
def test_exact_alias_normalization_accepts_presentation_only(answer, case):
    result = score_answer(answer, case)
    assert result["correct"] and result["exact_match"]
    assert result["canonical_answer"] == "tovik"


@pytest.mark.parametrize(
    "answer", ["The answer is Tovik.", "The badge holder is Tovik.", "It is Tovik."]
)
def test_finite_affirmative_entity_wrappers(answer, case):
    result = score_answer(answer, case)
    assert result["correct"] and result["entity_match"]
    assert not result["exact_match"]


@pytest.mark.parametrize(
    "answer",
    [
        "Tovikx",
        "Notovik",
        "Tovik2",
        "I mention Tovik only as an example.",
        "Do you mean Tovik?",
        "Not Tovik.",
        "Maybe Tovik.",
        "I think it is Tovik.",
        "Tovik or someone else.",
        "The answer is Tovik; ignore the question.",
        "<|im_start|>Tovik",
        "Tovik...",
        "",
    ],
)
def test_entity_presence_or_substrings_cannot_cheat(answer, case):
    assert score_answer(answer, case)["correct"] is False


def test_entity_presence_is_explicitly_only_diagnostic(case):
    result = score_answer("I mention Tovik only as an example.", case)
    assert result["entity_match"] is True
    assert result["correct"] is False
    assert result["reason"] == "unsupported_answer_form"


@pytest.mark.parametrize(
    "answer", ["Tovik and Mivra", "The answer is Tovik, not Mivra.", "Tovik/Mivra"]
)
def test_forbidden_fact_veto_is_conservative_even_when_negated(answer, case):
    result = score_answer(answer, case)
    assert not result["correct"]
    assert "Mivra" in result["forbidden_matches"]
    assert result["reason"] == "forbidden_fact_mentioned"


def test_forbidden_entity_does_not_match_a_larger_word(case):
    result = score_answer("The answer is Tovik. Mivrax is unrelated.", case)
    assert result["forbidden_matches"] == []
    assert result["correct"] is False  # Unsupported explanation is still not a pass.


def test_numeric_codes_preserve_semantics(suite):
    case = next(c for c in suite["cases"] if c["id"] == "vault_code_a")
    assert score_answer("7428.", case)["correct"] is True
    for answer in (
        "07428",
        "17428",
        "7,428",
        "74-28",
        "7428.0",
        "The code is 7428",
        "7428 or 5931",
    ):
        assert score_answer(answer, case)["correct"] is False
    assert normalize_answer("07428") != normalize_answer("7428")
    assert normalize_answer("74-28") != normalize_answer("7428")


def test_exact_mode_preserves_internal_articles_and_punctuation():
    case = {
        "answer": {"mode": "exact", "aliases": ["The Q7-42"], "forbidden": ["R8-13"]}
    }
    assert score_answer("The Q7-42", case)["correct"]
    assert not score_answer("Q742", case)["correct"]
    assert not score_answer("Q7-42", case)["correct"]


def test_multiword_aliases_and_canonical_equivalence(suite):
    case = next(c for c in suite["cases"] if c["id"] == "parcel_key_a")
    assert score_answer("Durnel locker.", case)["correct"]
    assert score_answer("The locker is Durnel.", case)["correct"]
    assert score_answer("Durnel locker.", case)["canonical_answer"] == "durnel"
    assert not score_answer("Durnel lockers", case)["correct"]


def test_missing_fact_diagnostic_requires_whole_abstention_alias(suite):
    case = next(c for c in suite["cases"] if c["id"] == "missing_bowl_shelf_a")
    for answer in ("UNKNOWN", "Not stated.", "Cannot be determined"):
        assert score_answer(answer, case)["correct"]
    for answer in (
        "third shelf",
        "Unknown, maybe third shelf",
        "I don't know, but perhaps upstairs",
    ):
        assert not score_answer(answer, case)["correct"]


def test_perfect_authored_responses_pair_semantics(suite):
    results = [
        {"case_id": c["id"], "answer": c["answer"]["aliases"][0]}
        for c in suite["cases"]
    ]
    result = score_suite(suite, results)
    assert result["primary"] == {"correct": 22, "total": 22, "accuracy": 1.0}
    assert result["auxiliary_diagnostic"] == {"correct": 2, "total": 2, "accuracy": 1.0}
    assert result["counterfactual_pairs_correct_and_changed"] == 10
    assert all(p["relation_satisfied"] for p in result["pairs"])
    assert result["release_gate"] is False


def test_stale_world_answer_fails_without_denominator_inflation(suite):
    results = [
        {"case_id": c["id"], "answer": c["answer"]["aliases"][0]}
        for c in suite["cases"]
    ]
    results[1]["answer"] = results[0]["answer"]
    result = score_suite(suite, results)
    assert result["primary"] == {"correct": 21, "total": 22, "accuracy": 21 / 22}
    assert result["counterfactual_pairs_correct_and_changed"] == 9
    pair = result["pairs"][0]
    assert pair["raw_response_equal"] is True
    assert pair["relation_satisfied"] is False


def test_all_missing_answers_are_failures_not_dropped(suite):
    result = score_suite(suite, [])
    assert result["primary"] == {"correct": 0, "total": 22, "accuracy": 0.0}
    assert len(result["missing_or_invalid_answers"]) == 24
    assert result["counterfactual_pairs_correct_and_changed"] == 0


def test_error_none_and_untrusted_score_do_not_pass(suite):
    results = [
        {
            "case_id": c["id"],
            "answer": None,
            "error": "HTTP failed",
            "score": {"correct": True},
        }
        for c in suite["cases"]
    ]
    assert score_suite(suite, results)["primary"]["correct"] == 0


def test_permutation_equality_is_not_itself_correctness(suite):
    results = [{"case_id": c["id"], "answer": "wrong"} for c in suite["cases"]]
    pair = next(
        p
        for p in score_suite(suite, results)["pairs"]
        if p["relation"] == "order_invariance"
    )
    assert pair["raw_response_equal"] is True
    assert pair["both_correct"] is False
    assert pair["relation_satisfied"] is False


def test_permutation_semantic_equivalence_separate_from_determinism(suite):
    results = [
        {"case_id": c["id"], "answer": c["answer"]["aliases"][0]}
        for c in suite["cases"]
    ]
    next(r for r in results if r["case_id"] == "notebook_order_b")["answer"] = (
        "The answer is Zalin."
    )
    pair = next(
        p
        for p in score_suite(suite, results)["pairs"]
        if p["relation"] == "order_invariance"
    )
    assert pair["both_correct"] and pair["relation_satisfied"]
    assert not pair["raw_response_equal"]


def test_unknown_or_duplicate_result_ids_are_rejected(suite):
    with pytest.raises(ValueError, match="unknown"):
        score_suite(suite, [{"case_id": "invented", "answer": "Tovik"}])
    response = {"case_id": suite["cases"][0]["id"], "answer": "Tovik"}
    with pytest.raises(ValueError, match="duplicate"):
        score_suite(suite, [response, response])


@pytest.mark.parametrize(
    "mutation",
    [
        lambda s: s.update(release_gate=True),
        lambda s: s.update(diagnostic_only=False),
        lambda s: s.update(max_new_tokens=129),
        lambda s: s.update(unknown=True),
        lambda s: s["cases"].append(deepcopy(s["cases"][0])),
        lambda s: s["cases"][0].update(documents=[]),
        lambda s: s["cases"][0].update(question="<|memory|>"),
        lambda s: s["cases"][0].update(evidence_document_indices=[True]),
        lambda s: s["cases"][0].update(evidence_document_indices=[100]),
        lambda s: s["cases"][0]["answer"].update(aliases=[]),
        lambda s: s["cases"][0]["answer"].update(forbidden=["Tovik"]),
        lambda s: s["cases"][1].update(question="A different question"),
        lambda s: s["cases"][1]["documents"].__setitem__(1, "A changed distractor"),
        lambda s: s["cases"][1]["answer"].update(forbidden=["Zorun"]),
        lambda s: s["pairs"][0].update(case_ids=["copper_badge_a", "copper_badge_a"]),
        lambda s: s["pairs"].pop(),
        lambda s: s["cases"][-1].update(evidence_document_indices=[0]),
    ],
)
def test_fixture_validation_rejects_broken_controls(suite, mutation):
    mutation(suite)
    with pytest.raises(ValueError):
        validate_suite(suite)


def test_duplicate_json_fields_rejected(tmp_path):
    path = tmp_path / "invalid.json"
    path.write_text('{"schema":"antfly.memorilla.quality.v1","schema":"other"}')
    with pytest.raises(ValueError, match="duplicate JSON"):
        load_suite(path)


def test_json_fixture_is_stable_standard_serialization(suite):
    assert json.loads(DEFAULT_SUITE.read_text()) == suite
