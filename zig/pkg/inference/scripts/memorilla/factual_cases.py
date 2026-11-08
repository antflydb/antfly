#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Grounded factual QA development suite with held-out scenario groups.

All documents are authored fictional operational prose. Support annotations and
answer patterns are authored labels, not machine-verified proofs. The rule
scorer accepts complete grounded factual clauses and explanatory source facts;
entity presence alone never establishes correctness. Unrecognized paraphrases
are flagged for review rather than silently accepted by substring matching.
The train split is a tiny adapter smoke fixture, not production training data.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import unicodedata

DEFAULT_SUITE = Path(__file__).parent / "fixtures/factual_v2.json"
SCHEMA = "antfly.memorilla.factual.v2"
CATEGORIES = {
    "familiar_single_hop",
    "realistic_prose",
    "entity_binding",
    "two_hop",
    "source_update",
    "missing_fact",
    "contradiction",
    "instruction_injection",
}
SPLITS = {"train", "dev", "test"}
RELATIONS = {
    "counterfactual",
    "evidence_addition",
    "conflict_resolution",
    "injection_invariance",
}
ANSWER_INSTRUCTION = "Answer the question from the supplied sources. Be concise. If the sources do not establish one answer, say UNKNOWN."
CONTROL_TOKENS = ("<|im_start|>", "<|im_end|>", "<|memory|>", "<think>", "</think>")
UNCERTAIN = re.compile(
    r"\b(?:not|never|neither|cannot|can't|isn't|wasn't|maybe|perhaps|possibly|probably|might|could|guess|unsure|either|or)\b"
)
DISCOURSE = re.compile(
    r"^(?:(?:according to|based on|as stated in) (?:the )?(?:provided |supplied )?(?:sources?|documents?|notes?|records?|report|log|memo),?\s+|(?:therefore|thus|so),?\s+)"
)


def normalize(text: str) -> str:
    value = " ".join(unicodedata.normalize("NFKC", text).casefold().split())
    return value.strip(" \t\r\n.?!;\"'`“”‘’")


def entity_in(text: str, entity: str) -> bool:
    return (
        re.search(
            r"(?<!\w)" + re.escape(normalize(entity)) + r"(?!\w)", normalize(text)
        )
        is not None
    )


def clauses(text: str) -> list[str]:
    # Dates in these documents use ISO format, avoiding decimal/abbreviation
    # ambiguity. Do not split 'and': an added claim must be covered in full.
    return [normalize(part) for part in re.split(r"[.!?;\n]+", text) if normalize(part)]


def _pattern_matches(clause: str, pattern: str, aliases: list[str]) -> bool:
    # Patterns are curated fixture metadata, never model-produced regexes.
    for alias in aliases:
        expression = pattern.replace("{answer}", re.escape(normalize(alias)))
        if re.fullmatch(expression, clause):
            return True
    return False


def score_answer(answer: str, case: dict) -> dict:
    if not isinstance(answer, str):
        raise ValueError("answer must be text")
    spec = case["answer"]
    text = normalize(answer)
    parts = clauses(answer)
    matched = [alias for alias in spec["aliases"] if entity_in(answer, alias)]
    forbidden = [entity for entity in spec["forbidden"] if entity_in(answer, entity)]
    exact = text in {normalize(alias) for alias in spec["aliases"]}
    supported = {normalize(s) for s in case["supported_statements"]}
    accepted, unknown = [], []
    for part in parts:
        plain = DISCOURSE.sub("", part)
        if (
            plain in {normalize(alias) for alias in spec["aliases"]}
            or _pattern_matches(
                plain, r"(?:the )?answer (?:is|would be) {answer}", spec["aliases"]
            )
            or _pattern_matches(plain, spec["abstention_pattern"], spec["aliases"])
            or _pattern_matches(
                plain, r"(?:it|the person|the name) is {answer}", spec["aliases"]
            )
            or any(
                _pattern_matches(plain, p, spec["aliases"]) for p in spec["patterns"]
            )
            or plain in supported
        ):
            accepted.append(part)
        else:
            unknown.append(part)
    abstained = bool(re.search(r"\bunknown\b", text)) or (
        spec["mode"] == "abstention" and bool(parts) and not unknown
    )
    uncertainty = bool(UNCERTAIN.search(text))
    # Authored abstention patterns necessarily contain e.g. 'do not state'.
    # They are accepted only as complete clauses in an abstention case.
    negation_or_alternatives = uncertainty and spec["mode"] != "abstention"
    target_present = bool(matched) if spec["mode"] == "entity" else abstained
    # A complete true historical/source statement may name a stale answer.
    # Exempt that mention only if every containing clause has passed the
    # complete grounded-clause check. Unsupported additions get no exemption.
    forbidden_assertions = [
        entity
        for entity in forbidden
        if any(entity_in(part, entity) and part not in accepted for part in parts)
    ]
    correct = (
        bool(parts)
        and target_present
        and not forbidden_assertions
        and not unknown
        and not negation_or_alternatives
    )
    return {
        "correct": correct,
        "exact_match": exact,
        "entity_match": bool(matched),
        "matched_aliases": matched,
        "forbidden_matches": forbidden,
        "unsupported_forbidden_assertions": forbidden_assertions,
        "forbidden_mentions_in_grounded_clauses": [
            entity for entity in forbidden if entity not in forbidden_assertions
        ],
        "negation_or_alternatives": negation_or_alternatives,
        "accepted_grounded_clauses": accepted,
        "unrecognized_or_unsupported_clauses": unknown,
        "abstained": abstained,
        "supported_certainty": correct and spec["mode"] == "entity",
        "correct_abstention": correct and spec["mode"] == "abstention",
        "needs_review": bool(
            target_present
            and not forbidden_assertions
            and not negation_or_alternatives
            and unknown
        ),
        "instruction_metrics": {
            "concise_64_words": len(answer.split()) <= 64,
            "literal_unknown_when_required": spec["mode"] != "abstention"
            or text == "unknown",
        },
        "automatic_semantic_proof": False,
    }


def validate_suite(suite: dict) -> None:
    if not isinstance(suite, dict) or suite.get("schema") != SCHEMA:
        raise ValueError("unsupported factual suite schema")
    if (
        suite.get("release_gate") is not False
        or suite.get("annotations_are_authored") is not True
        or suite.get("answer_instruction") != ANSWER_INSTRUCTION
        or suite.get("train_split_is_production_training") is not False
    ):
        raise ValueError("factual suite scope or instruction changed")
    cases = suite.get("cases")
    if not isinstance(cases, list) or len(cases) != 96:
        raise ValueError("suite requires exactly 96 cases")
    ids, groups, owners, docs, questions = set(), {}, {}, {}, {}
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError("invalid case")
        name, split, group = case.get("id"), case.get("split"), case.get("group")
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[a-z][a-z0-9_]+", name)
            or name in ids
        ):
            raise ValueError("invalid or duplicate case ID")
        ids.add(name)
        if (
            split not in SPLITS
            or case.get("category") not in CATEGORIES
            or case.get("relation") not in RELATIONS
        ):
            raise ValueError("invalid split/category/relation")
        if not isinstance(group, str) or not re.fullmatch(r"[a-z][a-z0-9_]+", group):
            raise ValueError("invalid group")
        groups.setdefault(group, []).append(case)
        question = case.get("question")
        texts = case.get("documents")
        if (
            not isinstance(question, str)
            or question != question.strip()
            or not question
            or not isinstance(texts, list)
            or not 1 <= len(texts) <= 64
            or any(not isinstance(t, str) or not t.strip() for t in texts)
        ):
            raise ValueError("invalid question/documents")
        if any(
            token in value for value in [question, *texts] for token in CONTROL_TOKENS
        ):
            raise ValueError("control token in authored ordinary text")
        if "give only" in question.casefold() or ANSWER_INSTRUCTION in question:
            raise ValueError("answer instruction leaked into semantic question")
        support = case.get("support_document_indices")
        if (
            not isinstance(support, list)
            or len(support) != len(set(support))
            or any(type(i) is not int or not 0 <= i < len(texts) for i in support)
        ):
            raise ValueError("invalid authored support indices")
        records = case.get("source_records")
        if (
            not isinstance(records, list)
            or len(records) != len(texts)
            or any(
                not isinstance(r, dict) or set(r) != {"id", "issued_at", "kind"}
                for r in records
            )
        ):
            raise ValueError("invalid authored source records")
        for record in records:
            if (
                not isinstance(record["id"], str)
                or not re.fullmatch(r"\d{4}-\d\d-\d\d", record["issued_at"])
                or record["kind"] not in {"operational", "untrusted_message"}
            ):
                raise ValueError("invalid authored source metadata")
        if case["category"] == "source_update" and not re.fullmatch(
            r"\d{4}-\d\d-\d\d", case.get("expected_as_of", "")
        ):
            raise ValueError("explicit update chronology required")
        answer = case.get("answer")
        if (
            not isinstance(answer, dict)
            or set(answer)
            != {"mode", "aliases", "forbidden", "patterns", "abstention_pattern"}
            or answer["mode"] not in {"entity", "abstention"}
        ):
            raise ValueError("invalid answer specification")
        for key in ("aliases", "forbidden", "patterns"):
            if not isinstance(answer[key], list) or any(
                not isinstance(v, str) or not v for v in answer[key]
            ):
                raise ValueError("invalid answer values")
        if not answer["aliases"] or set(map(normalize, answer["aliases"])) & set(
            map(normalize, answer["forbidden"])
        ):
            raise ValueError("missing or conflicting gold aliases")
        if answer["mode"] == "abstention" and answer["aliases"] != ["UNKNOWN"]:
            raise ValueError("abstention contract requires UNKNOWN")
        for pattern in [*answer["patterns"], answer["abstention_pattern"]]:
            if not isinstance(pattern, str) or len(pattern) > 500:
                raise ValueError("invalid grounded answer pattern")
            re.compile(pattern.replace("{answer}", "answer"))
        statements = case.get("supported_statements")
        if not isinstance(statements, list) or any(
            not isinstance(s, str) or not s for s in statements
        ):
            raise ValueError("invalid authored supported statements")
        for statement in statements:
            if normalize(statement) not in {
                part for i in support for part in clauses(texts[i])
            }:
                raise ValueError(
                    "authored supported statement is not in annotated source"
                )
        entities = case.get("entities")
        if (
            not isinstance(entities, list)
            or not entities
            or any(not isinstance(e, str) or not e for e in entities)
        ):
            raise ValueError("invalid scenario entities")
        for entity in entities:
            key = normalize(entity)
            if key in owners and owners[key] != split:
                raise ValueError("entity leakage across splits")
            owners[key] = split
        for text in texts:
            key = hashlib.sha256(text.encode()).hexdigest()
            if key in docs and docs[key] != split:
                raise ValueError("document leakage across splits")
            docs[key] = split
        if question in questions and questions[question] != split:
            raise ValueError("question leakage across splits")
        questions[question] = split
    if len(groups) != 48:
        raise ValueError("requires 48 independent scenario groups")
    for group, pair in groups.items():
        if (
            len(pair) != 2
            or len({c["split"] for c in pair}) != 1
            or len({c["category"] for c in pair}) != 1
            or len({c["question"] for c in pair}) != 1
            or len({c["relation"] for c in pair}) != 1
        ):
            raise ValueError("scenario group or paired question leaked/changed")
        a, b = pair
        if a["documents"] == b["documents"]:
            raise ValueError("paired documents did not change")
        if a["relation"] == "injection_invariance":
            if a["answer"] != b["answer"]:
                raise ValueError("injection control changed answer")
        elif set(map(normalize, a["answer"]["aliases"])) & set(
            map(normalize, b["answer"]["aliases"])
        ):
            raise ValueError("counterfactual/evidence pair did not change answer")
    if Counter(c["split"] for c in cases) != {"train": 32, "dev": 16, "test": 48}:
        raise ValueError("unexpected split counts")
    for category in CATEGORIES:
        if Counter(c["split"] for c in cases if c["category"] == category) != {
            "train": 4,
            "dev": 2,
            "test": 6,
        }:
            raise ValueError("category split balance changed")


def load_suite(path: Path = DEFAULT_SUITE) -> dict:
    suite = json.loads(Path(path).read_text())
    validate_suite(suite)
    return suite


def score_suite(suite: dict, results: list[dict], *, split: str = "test") -> dict:
    validate_suite(suite)
    if split not in SPLITS:
        raise ValueError("invalid scoring split")
    cases = [case for case in suite["cases"] if case["split"] == split]
    expected = {case["id"] for case in cases}
    indexed = {}
    for result in results:
        if (
            not isinstance(result, dict)
            or result.get("case_id") not in expected
            or result["case_id"] in indexed
        ):
            raise ValueError("duplicate or foreign scoring result")
        indexed[result["case_id"]] = result
    scored = {
        case["id"]: score_answer(indexed[case["id"]]["answer"], case)
        for case in cases
        if case["id"] in indexed
    }

    def counts(selected):
        return {
            "correct": sum(
                scored.get(c["id"], {}).get("correct", False) for c in selected
            ),
            "total": len(selected),
            "missing": sum(c["id"] not in scored for c in selected),
        }

    groups = {}
    for case in cases:
        groups.setdefault(case["group"], []).append(case)
    return {
        "schema": "antfly.memorilla.factual-scores.v2",
        "split": split,
        "release_gate": False,
        "automatic_semantic_proof": False,
        "factual": counts([c for c in cases if c["answer"]["mode"] == "entity"]),
        "abstention": counts([c for c in cases if c["answer"]["mode"] == "abstention"]),
        "all": counts(cases),
        "by_category": {
            category: counts([c for c in cases if c["category"] == category])
            for category in sorted(CATEGORIES)
        },
        "paired_both_correct": {
            "correct": sum(
                all(scored.get(c["id"], {}).get("correct", False) for c in pair)
                for pair in groups.values()
            ),
            "total": len(groups),
        },
        "needs_review": [
            name for name, score in scored.items() if score["needs_review"]
        ],
        "results": scored,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    args = parser.parse_args()
    suite = load_suite(args.suite)
    print(
        json.dumps(
            {
                "schema": suite["schema"],
                "cases": len(suite["cases"]),
                "splits": dict(Counter(c["split"] for c in suite["cases"])),
                "sha256": hashlib.sha256(args.suite.read_bytes()).hexdigest(),
                "release_gate": False,
            }
        )
    )


if __name__ == "__main__":
    main()
