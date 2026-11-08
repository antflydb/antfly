#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Bounded synthetic QA development diagnostics, not a production release gate.

The suite follows upstream's document-list/question/short-answer interface, but
is not an upstream benchmark reproduction. Alias exactness is deliberately
stricter than SQuAD normalization: articles and internal punctuation remain.
Entity presence is reported separately; it is never sufficient for correctness.
Only a whole alias or a small documented set of affirmative answer wrappers is
accepted. This can reject a correct free-form explanation; it avoids rewarding
negated mentions, alternatives, or answers that list both counterfactual facts.
Forbidden mentions are conservative vetoes, even when themselves negated.

Fixture evidence indices are authored annotations, not machine-verified logical
entailment. Token limits must still be checked by the actual encoder tokenizer.
No network, model, judge, or GPU is used here.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import unicodedata

DEFAULT_SUITE = Path(__file__).parent / "fixtures" / "quality_v1.json"
SCHEMA = "antfly.memorilla.quality.v1"
CATEGORIES = {"single_hop", "two_hop", "three_hop", "order_invariance", "missing_fact"}
MODES = {"entity", "exact", "abstention"}
RELATIONS = {"counterfactual", "order_invariance", "missing_fact"}
ENTITY_PREFIXES = (
    "answer is",
    "the answer is",
    "the correct answer is",
    "it is",
    "the name is",
    "the person is",
    "the courier is",
    "the holder is",
    "the badge holder is",
    "the owner is",
    "the destination is",
    "the caretaker is",
    "the place is",
    "the locker is",
    "the key is",
    "the color is",
    "the fabric is",
    "the material is",
)
UNCERTAIN_WORDS = {
    "not",
    "no",
    "never",
    "neither",
    "cannot",
    "can't",
    "isn't",
    "wasn't",
    "unknown",
    "unsure",
    "maybe",
    "perhaps",
    "possibly",
    "probably",
    "might",
    "could",
    "guess",
    "think",
    "either",
    "or",
    "uncertain",
    "hypothetically",
}
IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
WORD = re.compile(r"[^\W_]+(?:['’][^\W_]+)*", re.UNICODE)


def normalize_answer(text: str) -> str:
    """Case/Unicode/spacing normalization; preserve meaningful punctuation."""
    value = " ".join(unicodedata.normalize("NFKC", text).casefold().split())
    if value.endswith(".") and not value.endswith(".."):
        value = value[:-1].rstrip()
    for left, right in (('"', '"'), ("'", "'"), ("`", "`"), ("“", "”"), ("‘", "’")):
        if value.startswith(left) and value.endswith(right) and len(value) > 1:
            value = value[1:-1].strip()
            break
    return value


def _tokens(text: str) -> tuple[str, ...]:
    return tuple(WORD.findall(unicodedata.normalize("NFKC", text).casefold()))


def _contains(text: str, phrase: str) -> bool:
    words, target = _tokens(text), _tokens(phrase)
    return bool(target) and any(
        words[i : i + len(target)] == target
        for i in range(len(words) - len(target) + 1)
    )


def _text(value, *, limit: int, name: str) -> None:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError(f"invalid {name}")
    if len(value.encode("utf-8")) > limit:
        raise ValueError(f"{name} exceeds byte budget")


def _identifier(value, name: str) -> None:
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid {name}")


def _answer_contract(value) -> None:
    if not isinstance(value, dict) or set(value) != {"mode", "aliases", "forbidden"}:
        raise ValueError("invalid answer contract")
    if value["mode"] not in MODES:
        raise ValueError("unsupported scoring mode")
    for field in ("aliases", "forbidden"):
        values = value[field]
        if not isinstance(values, list) or not 1 <= len(values) <= 16:
            raise ValueError(f"invalid {field}")
        for item in values:
            _text(item, limit=128, name=field)
            if not _tokens(item):
                raise ValueError("answer phrases need lexical tokens")
        if len(set(map(normalize_answer, values))) != len(values):
            raise ValueError(f"duplicate normalized {field}")
    if any(
        _contains(a, f) or _contains(f, a)
        for a in value["aliases"]
        for f in value["forbidden"]
    ):
        raise ValueError("gold and forbidden phrases overlap")


def validate_suite(suite: dict) -> dict:
    """Validate the frozen v1 contract and cross-case experimental controls."""
    fields = {
        "schema",
        "description",
        "diagnostic_only",
        "release_gate",
        "max_new_tokens",
        "cases",
        "pairs",
    }
    if not isinstance(suite, dict) or set(suite) != fields or suite["schema"] != SCHEMA:
        raise ValueError("unsupported quality suite schema")
    if suite["diagnostic_only"] is not True or suite["release_gate"] is not False:
        raise ValueError("synthetic quality suite cannot become a release gate")
    _text(suite["description"], limit=4096, name="description")
    if (
        type(suite["max_new_tokens"]) is not int
        or not 1 <= suite["max_new_tokens"] <= 128
    ):
        raise ValueError("invalid generation budget")
    if not isinstance(suite["cases"], list) or not 1 <= len(suite["cases"]) <= 24:
        raise ValueError("quality case budget exceeded")
    by_id = {}
    case_fields = {
        "id",
        "scenario",
        "category",
        "question",
        "documents",
        "answer",
        "evidence_document_indices",
        "tags",
        "diagnostic_only",
        "permutation_check",
    }
    for case in suite["cases"]:
        if not isinstance(case, dict) or set(case) != case_fields:
            raise ValueError("invalid quality case fields")
        for field in ("id", "scenario"):
            _identifier(case[field], field)
        if case["id"] in by_id:
            raise ValueError("duplicate quality case ID")
        by_id[case["id"]] = case
        if case["category"] not in CATEGORIES:
            raise ValueError("unsupported category")
        _text(case["question"], limit=1024, name="question")
        docs = case["documents"]
        if not isinstance(docs, list) or not 1 <= len(docs) <= 64:
            raise ValueError("invalid document count")
        for doc in docs:
            _text(doc, limit=1024, name="document")
            if "<|memory|>" in doc:
                raise ValueError("reserved memory marker in document")
        if "<|memory|>" in case["question"]:
            raise ValueError("reserved memory marker in question")
        if len(set(docs)) != len(docs):
            raise ValueError("duplicate documents obscure evidence controls")
        _answer_contract(case["answer"])
        if (
            type(case["diagnostic_only"]) is not bool
            or type(case["permutation_check"]) is not bool
        ):
            raise ValueError("quality flags must be booleans")
        evidence = case["evidence_document_indices"]
        if not isinstance(evidence, list) or any(
            type(i) is not int or not 0 <= i < len(docs) for i in evidence
        ):
            raise ValueError("invalid evidence indices")
        if evidence != sorted(set(evidence)):
            raise ValueError("evidence indices must be unique and sorted")
        if case["category"] == "missing_fact":
            if (
                evidence
                or case["answer"]["mode"] != "abstention"
                or not case["diagnostic_only"]
            ):
                raise ValueError(
                    "missing-fact cases must be explicit abstention diagnostics"
                )
        else:
            minimum = {"two_hop": 2, "three_hop": 3}.get(case["category"], 1)
            if len(evidence) < minimum or case["answer"]["mode"] == "abstention":
                raise ValueError("insufficient annotated supporting documents")
            if not any(
                _contains(docs[i], alias)
                for i in evidence
                for alias in case["answer"]["aliases"]
            ):
                raise ValueError("gold entity absent from annotated evidence")
        if case["permutation_check"] != (case["category"] == "order_invariance"):
            raise ValueError("permutation category mismatch")
        if not isinstance(case["tags"], list) or not case["tags"]:
            raise ValueError("missing tags")
        for tag in case["tags"]:
            _identifier(tag, "tag")
        if len(set(case["tags"])) != len(case["tags"]):
            raise ValueError("duplicate tag")
    if not isinstance(suite["pairs"], list) or not suite["pairs"]:
        raise ValueError("paired controls are required")
    membership = []
    pair_ids = set()
    for pair in suite["pairs"]:
        if not isinstance(pair, dict) or set(pair) != {"id", "relation", "case_ids"}:
            raise ValueError("invalid pair")
        _identifier(pair["id"], "pair ID")
        if pair["id"] in pair_ids or pair["relation"] not in RELATIONS:
            raise ValueError("invalid pair identity or relation")
        pair_ids.add(pair["id"])
        ids = pair["case_ids"]
        if (
            not isinstance(ids, list)
            or len(ids) != 2
            or any(i not in by_id for i in ids)
            or ids[0] == ids[1]
        ):
            raise ValueError("invalid pair members")
        membership.extend(ids)
        a, b = (by_id[i] for i in ids)
        if (
            a["scenario"] != pair["id"]
            or b["scenario"] != pair["id"]
            or a["category"] != b["category"]
            or a["question"] != b["question"]
        ):
            raise ValueError("pair question/scenario controls changed")
        aa, ba = a["answer"], b["answer"]
        if pair["relation"] == "counterfactual":
            ag, bg = (
                set(map(normalize_answer, aa["aliases"])),
                set(map(normalize_answer, ba["aliases"])),
            )
            if (
                ag & bg
                or aa["mode"] != ba["mode"]
                or a["category"] not in {"single_hop", "two_hop", "three_hop"}
            ):
                raise ValueError("counterfactual gold answers must differ")
            if not bg <= set(map(normalize_answer, aa["forbidden"])) or not ag <= set(
                map(normalize_answer, ba["forbidden"])
            ):
                raise ValueError("counterfactual stale answers must be forbidden")
            if len(a["documents"]) != len(b["documents"]):
                raise ValueError("counterfactual document count changed")
            changed = {
                i
                for i, (x, y) in enumerate(
                    zip(a["documents"], b["documents"], strict=True)
                )
                if x != y
            }
            if not changed or not changed <= set(a["evidence_document_indices"]) | set(
                b["evidence_document_indices"]
            ):
                raise ValueError("counterfactual changed non-evidence distractors")
        elif pair["relation"] == "order_invariance":
            if (
                a["category"] != "order_invariance"
                or aa != ba
                or a["documents"] == b["documents"]
                or Counter(a["documents"]) != Counter(b["documents"])
            ):
                raise ValueError("permutation must preserve facts and answer")
        elif (
            a["category"] != "missing_fact"
            or aa["mode"] != "abstention"
            or ba["mode"] != "abstention"
        ):
            raise ValueError("missing-fact pair must contain abstention diagnostics")
    if Counter(membership) != Counter(by_id.keys()):
        raise ValueError("every case must belong to exactly one pair")
    return suite


def load_suite(path: Path = DEFAULT_SUITE) -> dict:
    path = Path(path)
    if path.stat().st_size > 512 * 1024:
        raise ValueError("quality fixture exceeds byte budget")

    def unique_object(items):
        obj = {}
        for key, value in items:
            if key in obj:
                raise ValueError("duplicate JSON field")
            obj[key] = value
        return obj

    return validate_suite(
        json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
    )


def score_answer(answer: str, case: dict) -> dict:
    """Score one answer conservatively; entity-presence alone never passes."""
    if not isinstance(answer, str):
        raise ValueError("answer must be a string")
    contract = case["answer"]
    _answer_contract(contract)
    normalized = normalize_answer(answer)
    aliases = contract["aliases"]
    exact = any(normalized == normalize_answer(alias) for alias in aliases)
    mentions = [alias for alias in aliases if _contains(answer, alias)]
    forbidden = [
        phrase for phrase in contract["forbidden"] if _contains(answer, phrase)
    ]
    uncertain = bool(set(_tokens(answer)) & UNCERTAIN_WORDS)
    control = any(marker in answer for marker in ("<|", "|>", "\x00")) or "?" in answer
    form = exact
    if contract["mode"] == "entity":
        form |= any(
            normalized == prefix + " " + normalize_answer(alias)
            for prefix in ENTITY_PREFIXES
            for alias in aliases
        )
    blocked = control or len(_tokens(answer)) > 64
    if contract["mode"] != "abstention":
        blocked |= uncertain
    correct = bool(form and not forbidden and not blocked)
    reason = (
        "forbidden_fact_mentioned"
        if forbidden
        else "ambiguous_or_control_text"
        if blocked
        else "correct"
        if correct
        else "unsupported_answer_form"
        if mentions
        else "gold_answer_absent"
    )
    return {
        "correct": correct,
        "exact_match": exact,
        "entity_match": bool(mentions),
        "matched_aliases": mentions,
        "forbidden_matches": forbidden,
        "negation_or_uncertainty": uncertain,
        "supported_answer_form": bool(form),
        "canonical_answer": normalize_answer(aliases[0]) if correct else None,
        "reason": reason,
    }


def score_suite(suite: dict, results: list[dict]) -> dict:
    """Recompute case and paired scores; missing/error responses stay failures."""
    validate_suite(suite)
    by_id = {case["id"]: case for case in suite["cases"]}
    provided = {}
    for result in results:
        if not isinstance(result, dict) or result.get("case_id") not in by_id:
            raise ValueError("unknown result case ID")
        key = result["case_id"]
        if key in provided:
            raise ValueError("duplicate result case ID")
        provided[key] = result
    scores = {}
    missing = []
    for key, case in by_id.items():
        result = provided.get(key, {})
        answer = result.get("answer")
        if not isinstance(answer, str):
            missing.append(key)
            answer = ""
        scores[key] = score_answer(answer, case)
    pairs = []
    for pair in suite["pairs"]:
        a, b = pair["case_ids"]
        sa, sb = scores[a], scores[b]
        both = sa["correct"] and sb["correct"]
        response_equal = (
            a not in missing
            and b not in missing
            and provided[a]["answer"] == provided[b]["answer"]
        )
        answer_equal = both and sa["canonical_answer"] == sb["canonical_answer"]
        relation = pair["relation"]
        passed = both and (
            not answer_equal if relation == "counterfactual" else answer_equal
        )
        pairs.append(
            {
                **pair,
                "both_correct": both,
                "raw_response_equal": response_equal,
                "correct_answers_equivalent": answer_equal,
                "relation_satisfied": passed,
            }
        )
    primary = [case for case in suite["cases"] if not case["diagnostic_only"]]
    auxiliary = [case for case in suite["cases"] if case["diagnostic_only"]]

    def totals(cases):
        total = len(cases)
        correct = sum(scores[case["id"]]["correct"] for case in cases)
        return {
            "correct": correct,
            "total": total,
            "accuracy": correct / total if total else None,
        }

    return {
        "schema": "antfly.memorilla.quality-scores.v1",
        "diagnostic_only": True,
        "release_gate": False,
        "primary": totals(primary),
        "auxiliary_diagnostic": totals(auxiliary),
        "by_category": {
            category: totals(
                [case for case in suite["cases"] if case["category"] == category]
            )
            for category in sorted({case["category"] for case in suite["cases"]})
        },
        "missing_or_invalid_answers": missing,
        "cases": scores,
        "pairs": pairs,
        "counterfactual_pairs_correct_and_changed": sum(
            p["relation_satisfied"] for p in pairs if p["relation"] == "counterfactual"
        ),
        "counterfactual_pairs_total": sum(
            p["relation"] == "counterfactual" for p in pairs
        ),
        "limitations": "Synthetic authored facts and conservative answer forms; no semantic judge, statistical power, corpus quality, or production release threshold.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    parser.add_argument("--responses", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    suite = load_suite(args.suite)
    if args.responses:
        results = json.loads(args.responses.read_text())
        if isinstance(results, dict):
            results = results["results"]
        report = score_suite(suite, results)
    else:
        report = {
            "schema": suite["schema"],
            "validated": True,
            "cases": len(suite["cases"]),
            "pairs": len(suite["pairs"]),
            "diagnostic_only": True,
            "release_gate": False,
        }
    report["suite_sha256"] = hashlib.sha256(args.suite.read_bytes()).hexdigest()
    report["scorer_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        with args.output.open("x") as output:
            output.write(encoded)
    else:
        print(encoded, end="")


if __name__ == "__main__":
    main()
