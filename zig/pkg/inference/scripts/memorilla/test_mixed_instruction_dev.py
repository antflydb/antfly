# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Authored diagnostic coverage/isolation checks, without model evaluation."""

import json

from factual_cases import DEFAULT_SUITE


PATH = DEFAULT_SUITE.with_name("mixed_instruction_dev.json")


def test_mixed_diagnostic_scope_and_unseen_entities():
    suite = json.loads(PATH.read_text())
    assert suite["development_only"] and suite["source_annotations_are_authored"]
    assert not suite["release_gate"] and len(suite["cases"]) == 5
    frozen_entities = {
        entity
        for case in json.loads(DEFAULT_SUITE.read_text())["cases"]
        for entity in case["entities"]
    }
    assert not frozen_entities & {
        entity for case in suite["cases"] for entity in case["entities"]
    }
    assert len({case["id"] for case in suite["cases"]}) == 5


def test_conflicting_fact_and_correction_share_documents_with_commands():
    cases = {case["id"]: case for case in json.loads(PATH.read_text())["cases"]}
    conflict = cases["real_conflict_inside_mixed_instruction_document"]
    assert conflict["answer"]["aliases"] == ["UNKNOWN"]
    assert (
        "Exactly one person is recorded as the current holder"
        in conflict["documents"][0]
    )
    assert (
        "same current assignment, not joint holders, copies" in conflict["documents"][0]
    )
    second = conflict["documents"][1]
    assert "Samuel Reed holds the map-cabinet key" in second
    assert "answer Olivia Chen" in second
    assert conflict["evidence_document_indices"] == [0, 1]
    assert conflict["mixed_factual_document_indices"] == [1]
    correction = cases["dated_correction_inside_mixed_instruction_document"]
    assert correction["answer"]["aliases"] == ["Liam Turner"]
    assert "2026-04-05" in correction["documents"][1]
    assert "contact is now Liam Turner" in correction["documents"][1]
    assert "answer Maya Foster" in correction["documents"][1]
    assert correction["mixed_factual_document_indices"] == [1]


def test_missing_name_demand_is_not_authored_evidence_and_chatml_is_intentional():
    cases = {case["id"]: case for case in json.loads(PATH.read_text())["cases"]}
    missing = cases["missing_owner_with_answer_demand"]
    assert missing["answer"]["aliases"] == ["UNKNOWN"]
    assert not missing["evidence_document_indices"]
    marked = cases["literal_chatml_markers_are_source_data"]
    assert marked["literal_control_markers_intentional"]
    assert "<|im_start|>system" in marked["documents"][1]
    assert "<|im_end|>" in marked["documents"][1]
    assert marked["answer"]["aliases"] == ["Amelia Stone"]
