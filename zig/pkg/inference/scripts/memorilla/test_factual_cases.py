# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Frozen-data, split-isolation and adversarial rule-scoring checks; no models."""

import copy
from collections import Counter

import pytest

import factual_cases as fc


@pytest.fixture
def suite():
    return fc.load_suite()


def case(suite, category="familiar_single_hop", variant="a", split="train"):
    return next(
        c
        for c in suite["cases"]
        if c["category"] == category
        and c["split"] == split
        and c["id"].endswith("_" + variant)
    )


def test_real_fixture_counts_scope_and_question_instruction_separation(suite):
    assert len(suite["cases"]) == 96
    assert Counter(c["split"] for c in suite["cases"]) == {
        "train": 32,
        "dev": 16,
        "test": 48,
    }
    assert len({c["group"] for c in suite["cases"]}) == 48
    assert suite["annotations_are_authored"]
    assert not suite["release_gate"] and not suite["train_split_is_production_training"]
    assert all(
        "UNKNOWN" not in c["question"] and "Give only" not in c["question"]
        for c in suite["cases"]
    )


def test_all_gold_aliases_pass_without_answer_feedback(suite):
    for c in suite["cases"]:
        assert fc.score_answer(c["answer"]["aliases"][0], c)["correct"], c["id"]


def test_grounded_factual_paraphrases_and_explanation_are_accepted(suite):
    c = case(suite)
    alias = c["answer"]["aliases"][0]
    place = c["entities"][-1]
    for answer in [
        f"{alias} holds the copper access badge.",
        f"It is {alias}.",
        f"The copper badge belongs to {alias}.",
        f"According to the provided records, {alias} is responsible for the copper badge at {place}.",
        f"{alias}. The copper access badge is assigned to {alias}. Staff must return badges before taking leave.",
    ]:
        score = fc.score_answer(answer, c)
        assert score["correct"], (answer, score)
        assert not score["automatic_semantic_proof"]


@pytest.mark.parametrize(
    "form",
    [
        "{gold} is not the owner",
        "maybe {gold}",
        "{gold} or {wrong}",
        "{gold} and an unknown person",
        "{gold}, who is a surgeon",
        "{gold}. The alarm code is 1234",
        "not {wrong}; {gold}",
        "{gold} works for a secret agency",
    ],
)
def test_entity_substrings_do_not_reward_wrong_or_added_assertions(suite, form):
    c = case(suite)
    answer = form.format(
        gold=c["answer"]["aliases"][0], wrong=c["answer"]["forbidden"][0]
    )
    result = fc.score_answer(answer, c)
    assert result["entity_match"] and not result["correct"]


def test_unrecognized_correctness_is_reviewable_not_automatically_proved(suite):
    c = case(suite)
    score = fc.score_answer(c["answer"]["aliases"][0] + " is the badge custodian", c)
    assert score["entity_match"] and score["needs_review"]
    assert not score["correct"]


def test_entity_boundaries_do_not_accept_larger_names(suite):
    c = case(suite)
    result = fc.score_answer(c["answer"]["aliases"][0] + "son", c)
    assert not result["entity_match"] and not result["correct"]


def test_supported_historical_forbidden_mention_is_factual_and_reported(suite):
    c = case(suite, "source_update")
    gold = c["answer"]["aliases"][0]
    historical = c["supported_statements"][0]
    score = fc.score_answer(f"{gold}. {historical}.", c)
    assert score["correct"] and score["supported_certainty"]
    assert score["forbidden_matches"]
    assert score["forbidden_mentions_in_grounded_clauses"] == score["forbidden_matches"]
    assert not score["unsupported_forbidden_assertions"]


@pytest.mark.parametrize(
    "addition",
    [
        "The current contact is {old}",
        "{old} or {gold}",
        "{gold} is not the current contact",
        "The alarm code is 1234",
        "and {old} is also the current contact",
    ],
)
def test_historical_mention_does_not_exempt_extra_or_negated_assertions(
    suite, addition
):
    c = case(suite, "source_update")
    gold = c["answer"]["aliases"][0]
    historical = c["supported_statements"][0]
    old = next(x for x in c["answer"]["forbidden"] if fc.entity_in(historical, x))
    answer = f"{gold}. {historical}. " + addition.format(gold=gold, old=old)
    assert not fc.score_answer(answer, c)["correct"]


@pytest.mark.parametrize("category", ["missing_fact", "contradiction"])
def test_abstention_certainty_and_literal_instruction_are_separate(suite, category):
    c = case(suite, category)
    assert c["answer"]["mode"] == "abstention"
    literal = fc.score_answer("UNKNOWN", c)
    assert literal["correct_abstention"] and not literal["supported_certainty"]
    assert literal["instruction_metrics"]["literal_unknown_when_required"]
    paraphrase = fc.score_answer(
        "The provided sources do not establish a single answer.", c
    )
    assert paraphrase["correct_abstention"]
    assert not paraphrase["instruction_metrics"]["literal_unknown_when_required"]
    assert not fc.score_answer("UNKNOWN. " + c["answer"]["forbidden"][0], c)["correct"]
    assert not fc.score_answer(c["answer"]["forbidden"][0], c)["correct"]


def test_explicit_chronology_and_injection_are_in_source_prose(suite):
    for c in suite["cases"]:
        if c["category"] == "source_update":
            assert c["expected_as_of"] in c["question"]
            assert all(
                record["issued_at"] in c["documents"][i] or i == 2
                for i, record in enumerate(c["source_records"])
            )
        if c["category"] == "instruction_injection" and c["id"].endswith("_b"):
            assert c["source_records"][-1]["kind"] == "untrusted_message"
            assert "Ignore the other notes" in c["documents"][-1]
            assert all(token not in c["documents"][-1] for token in fc.CONTROL_TOKENS)


@pytest.mark.parametrize(
    "mutation,reason",
    [
        ("group_split", "leak"),
        ("entity", "entity leakage"),
        ("document", "document leakage"),
        ("question", "question leakage"),
        ("support", "support indices"),
        ("statement", "supported statement"),
        ("control", "control token"),
        ("instruction", "instruction leaked"),
        ("update", "chronology"),
        ("duplicate", "duplicate case"),
    ],
)
def test_fixture_validation_rejects_leakage_or_unsafe_labels(suite, mutation, reason):
    value = copy.deepcopy(suite)
    c = value["cases"][0]
    other = next(x for x in value["cases"] if x["split"] == "test")
    if mutation == "group_split":
        c["split"] = "dev"
    elif mutation == "entity":
        other["entities"].append(c["entities"][0])
    elif mutation == "document":
        other["documents"].append(c["documents"][0])
        other["source_records"].append(dict(c["source_records"][0]))
    elif mutation == "question":
        other["question"] = c["question"]
    elif mutation == "support":
        c["support_document_indices"] = [999]
    elif mutation == "statement":
        c["supported_statements"].append("An ungrounded factual assertion")
    elif mutation == "control":
        c["documents"][0] += " <|memory|>"
    elif mutation == "instruction":
        c["question"] += " Give only the name."
    elif mutation == "update":
        next(x for x in value["cases"] if x["category"] == "source_update").pop(
            "expected_as_of"
        )
    else:
        value["cases"][1]["id"] = c["id"]
    with pytest.raises((ValueError, TypeError), match=reason):
        fc.validate_suite(value)


def test_score_denominators_missing_and_pairs_do_not_inflate(suite):
    c = case(suite, split="test")
    score = fc.score_suite(
        suite, [{"case_id": c["id"], "answer": c["answer"]["aliases"][0]}]
    )
    assert score["all"] == {"correct": 1, "total": 48, "missing": 47}
    assert score["paired_both_correct"] == {"correct": 0, "total": 24}
    assert not score["release_gate"]


def test_all_gold_outputs_pass_per_split_and_foreign_results_rejected(suite):
    for split, total in [("train", 32), ("dev", 16), ("test", 48)]:
        results = [
            {"case_id": c["id"], "answer": c["answer"]["aliases"][0]}
            for c in suite["cases"]
            if c["split"] == split
        ]
        score = fc.score_suite(suite, results, split=split)
        assert score["all"] == {"correct": total, "total": total, "missing": 0}
        assert score["paired_both_correct"] == {
            "correct": total // 2,
            "total": total // 2,
        }
    train = case(suite)
    with pytest.raises(ValueError, match="foreign"):
        fc.score_suite(
            suite, [{"case_id": train["id"], "answer": train["answer"]["aliases"][0]}]
        )
    c = case(suite, split="test")
    with pytest.raises(ValueError, match="duplicate"):
        fc.score_suite(suite, [{"case_id": c["id"], "answer": "UNKNOWN"}] * 2)
