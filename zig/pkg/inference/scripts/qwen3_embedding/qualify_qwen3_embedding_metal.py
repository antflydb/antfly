#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0

"""Fail-closed Qwen3-Embedding qualification of a running Antfly server.

The driver replays every case from a ``transformers_embedding_oracle.py`` JSON
document against the OpenAI-compatible ``/ai/v1/embeddings`` endpoint and
gates, per precision tier, the cosine similarity against the fp32 Transformers
reference. It additionally proves batch-vs-single equivalence, that a
``dimensions`` request equals a host-side truncate-then-renormalize of the
server's own full vector, unit L2 norms, and top-1 retrieval-rank agreement on
a small query-to-document matrix built from the oracle cases. Any failed gate
exits nonzero with a failure table. Stdlib only; no model downloads.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import sys
from typing import Any
import urllib.error
import urllib.request

from transformers_embedding_oracle import (
    SCHEMA as ORACLE_SCHEMA,
    cosine,
    truncate_and_renormalize,
)


SCHEMA = "antfly.qwen3_embedding.metal_qualification.v1"
EMBEDDINGS_PATH = "/ai/v1/embeddings"
QUERY_TASK_TYPE = "RETRIEVAL_QUERY"
TIER_MIN_COSINE = {
    "bf16": 0.999,
    "f16": 0.999,
    "q8_0": 0.995,
    "q4_k": 0.99,
}
BATCH_MIN_COSINE = 0.9999
DIMENSIONS_MIN_COSINE = 0.99999
NORM_ABS_TOLERANCE = 1e-3
CHECK_DIMENSIONS = 256
ORACLE_DIMS = ("1024", "256", "32")
RETRIEVAL_QUERY_CASES = ("query_custom", "query_default", "query_shared_text", "query_short")
RETRIEVAL_DOCUMENT_CASES = ("doc_cjk", "doc_emoji", "doc_english", "doc_shared_text")


class QualificationError(RuntimeError):
    pass


def l2_norm(vector: list[float]) -> float:
    return math.sqrt(sum(value * value for value in vector))


def gate(name: str, case: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"gate": name, "case": case, "pass": bool(passed), "detail": detail}


def load_oracle(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QualificationError(f"invalid oracle JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise QualificationError(f"oracle JSON root is not an object: {path}")
    return payload


def _finite_floats(value: Any) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(item, (int, float)) and math.isfinite(item) for item in value)
    )


def validate_oracle(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if payload.get("schema") != ORACLE_SCHEMA:
        raise QualificationError(f"unexpected oracle schema: {payload.get('schema')!r}")
    contract = payload.get("contract")
    if not isinstance(contract, dict) or not isinstance(contract.get("default_instruction"), str):
        raise QualificationError("oracle contract lacks a default_instruction")
    raw_cases = payload.get("cases")
    if not isinstance(raw_cases, list) or not raw_cases:
        raise QualificationError("oracle has no cases")

    cases: dict[str, dict[str, Any]] = {}
    for case in raw_cases:
        if not isinstance(case, dict):
            raise QualificationError("oracle case is not an object")
        case_id = case.get("id")
        if not isinstance(case_id, str) or not case_id:
            raise QualificationError(f"oracle case has an invalid id: {case_id!r}")
        if case_id in cases:
            raise QualificationError(f"duplicate oracle case id: {case_id}")
        if case.get("role") not in ("query", "document"):
            raise QualificationError(f"case {case_id}: invalid role {case.get('role')!r}")
        if not isinstance(case.get("text"), str):
            raise QualificationError(f"case {case_id}: text is not a string")
        served_text = case.get("served_text")
        if served_text is not None and not isinstance(served_text, str):
            raise QualificationError(f"case {case_id}: served_text is not a string")
        instruction = case.get("instruction")
        if instruction is not None and not isinstance(instruction, str):
            raise QualificationError(f"case {case_id}: invalid instruction {instruction!r}")
        token_ids = case.get("token_ids")
        if not isinstance(token_ids, list) or not token_ids or not all(
            isinstance(token, int) for token in token_ids
        ):
            raise QualificationError(f"case {case_id}: invalid token_ids")
        embeddings = case.get("embeddings")
        if not isinstance(embeddings, dict):
            raise QualificationError(f"case {case_id}: missing embeddings")
        for key in ORACLE_DIMS:
            vector = embeddings.get(key)
            if not _finite_floats(vector) or len(vector) != int(key):
                raise QualificationError(f"case {case_id}: invalid {key}-dim embedding")
        cases[case_id] = case

    missing = [
        case_id
        for case_id in (*RETRIEVAL_QUERY_CASES, *RETRIEVAL_DOCUMENT_CASES)
        if case_id not in cases
    ]
    if missing:
        raise QualificationError(f"oracle lacks retrieval matrix cases: {missing}")
    return cases


def embedding_request_body(
    model: str,
    inputs: str | list[str],
    role: str,
    instruction: str | None,
    default_instruction: str,
    dimensions: int | None = None,
) -> dict[str, Any]:
    """Map an oracle case onto the OpenAI-compatible request contract.

    Queries carry ``task_type`` and, only when it deviates from the server
    default, a custom ``instruction``. Documents are raw text with neither.
    """

    body: dict[str, Any] = {"model": model, "input": inputs}
    if role == "query":
        body["task_type"] = QUERY_TASK_TYPE
        if instruction is not None and instruction != default_instruction:
            body["instruction"] = instruction
    elif role != "document":
        raise QualificationError(f"unknown oracle role: {role!r}")
    if dimensions is not None:
        body["dimensions"] = int(dimensions)
    return body


def post_embeddings(base_url: str, body: dict[str, Any], timeout: float) -> list[list[float]]:
    request = urllib.request.Request(
        base_url.rstrip("/") + EMBEDDINGS_PATH,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read())
    data = payload.get("data") if isinstance(payload, dict) else None
    expected = 1 if isinstance(body["input"], str) else len(body["input"])
    if not isinstance(data, list) or len(data) != expected:
        raise QualificationError(
            f"expected {expected} embeddings, got "
            f"{len(data) if isinstance(data, list) else 'none'}"
        )
    rows = sorted(
        enumerate(data),
        key=lambda item: item[1].get("index", item[0]) if isinstance(item[1], dict) else item[0],
    )
    vectors: list[list[float]] = []
    for position, item in rows:
        embedding = item.get("embedding") if isinstance(item, dict) else None
        if not _finite_floats(embedding):
            raise QualificationError(f"response row {position} has no finite embedding vector")
        vectors.append([float(value) for value in embedding])
    return vectors


def case_gates(
    case_id: str,
    oracle_vector: list[float],
    server_vector: list[float],
    min_cosine: float,
) -> list[dict[str, Any]]:
    rows = [
        gate(
            "dimensions",
            case_id,
            len(server_vector) == len(oracle_vector),
            f"got {len(server_vector)}, want {len(oracle_vector)}",
        )
    ]
    if not rows[0]["pass"]:
        return rows
    norm = l2_norm(server_vector)
    rows.append(
        gate(
            "unit_norm",
            case_id,
            math.isfinite(norm) and abs(norm - 1.0) <= NORM_ABS_TOLERANCE,
            f"|v|={norm:.6f} tol={NORM_ABS_TOLERANCE}",
        )
    )
    if norm <= 0.0:
        rows.append(gate("oracle_cosine", case_id, False, "server vector has zero norm"))
        return rows
    value = cosine(server_vector, oracle_vector)
    rows.append(
        gate(
            "oracle_cosine",
            case_id,
            value >= min_cosine,
            f"cosine={value:.6f} min={min_cosine}",
        )
    )
    return rows


def mrl_gates(
    case_id: str,
    server_full: list[float],
    server_reduced: list[float],
    dimensions: int,
) -> list[dict[str, Any]]:
    rows = [
        gate(
            "mrl_dimensions",
            case_id,
            len(server_reduced) == dimensions,
            f"got {len(server_reduced)}, want {dimensions}",
        )
    ]
    if not rows[0]["pass"]:
        return rows
    norm = l2_norm(server_reduced)
    rows.append(
        gate(
            "mrl_unit_norm",
            case_id,
            math.isfinite(norm) and abs(norm - 1.0) <= NORM_ABS_TOLERANCE,
            f"|v|={norm:.6f} tol={NORM_ABS_TOLERANCE}",
        )
    )
    if norm <= 0.0:
        rows.append(gate("mrl_truncate_renormalize", case_id, False, "reduced vector has zero norm"))
        return rows
    expected = truncate_and_renormalize(server_full, dimensions)
    value = cosine(server_reduced, expected)
    rows.append(
        gate(
            "mrl_truncate_renormalize",
            case_id,
            value >= DIMENSIONS_MIN_COSINE,
            f"cosine={value:.7f} min={DIMENSIONS_MIN_COSINE}",
        )
    )
    return rows


def batch_gates(
    case_id: str,
    single_vector: list[float],
    batch_vector: list[float],
) -> list[dict[str, Any]]:
    if len(batch_vector) != len(single_vector):
        return [
            gate(
                "batch_vs_single",
                case_id,
                False,
                f"width {len(batch_vector)} != {len(single_vector)}",
            )
        ]
    value = cosine(batch_vector, single_vector)
    return [
        gate(
            "batch_vs_single",
            case_id,
            value >= BATCH_MIN_COSINE,
            f"cosine={value:.6f} min={BATCH_MIN_COSINE}",
        )
    ]


def retrieval_top1(query_vector: list[float], documents: dict[str, list[float]]) -> str:
    if not documents:
        raise QualificationError("retrieval matrix has no documents")
    ranked = sorted(
        documents.items(),
        key=lambda item: (-cosine(query_vector, item[1]), item[0]),
    )
    return ranked[0][0]


def retrieval_gates(
    oracle_vectors: dict[str, list[float]],
    server_vectors: dict[str, list[float]],
) -> list[dict[str, Any]]:
    oracle_documents = {case_id: oracle_vectors[case_id] for case_id in RETRIEVAL_DOCUMENT_CASES}
    server_documents = {case_id: server_vectors[case_id] for case_id in RETRIEVAL_DOCUMENT_CASES}
    rows = []
    for query_id in RETRIEVAL_QUERY_CASES:
        oracle_top = retrieval_top1(oracle_vectors[query_id], oracle_documents)
        server_top = retrieval_top1(server_vectors[query_id], server_documents)
        rows.append(
            gate(
                "retrieval_top1_agreement",
                query_id,
                oracle_top == server_top,
                f"oracle={oracle_top} server={server_top}",
            )
        )
    return rows


def shared_text_gate(
    query_vector: list[float],
    document_vector: list[float],
) -> dict[str, Any]:
    return gate(
        "shared_text_role_distinct",
        "query_shared_text/doc_shared_text",
        query_vector != document_vector,
        "query and document embeddings of identical text must differ",
    )


def render_table(rows: list[dict[str, Any]]) -> str:
    header = ("GATE", "CASE", "STATUS", "DETAIL")
    table = [header] + [
        (row["gate"], row["case"], "PASS" if row["pass"] else "FAIL", row["detail"])
        for row in rows
    ]
    widths = [max(len(entry[column]) for entry in table) for column in range(len(header))]
    return "\n".join(
        "  ".join(entry[column].ljust(widths[column]) for column in range(len(header))).rstrip()
        for entry in table
    )


def case_input(case: dict[str, Any]) -> str:
    """Text to post for a case.

    Over-length cases carry ``served_text`` — the decoded, roundtrip-verified
    truncated sequence — because the server embeds the model's full context
    and would otherwise see more tokens than the golden embedding covers.
    """
    served_text = case.get("served_text")
    return served_text if isinstance(served_text, str) else case["text"]


def run_qualification(args: argparse.Namespace) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    payload = load_oracle(args.oracle)
    cases = validate_oracle(payload)
    default_instruction = payload["contract"]["default_instruction"]
    min_cosine = TIER_MIN_COSINE[args.tier]

    rows: list[dict[str, Any]] = []
    server_full: dict[str, list[float]] = {}
    for case_id in sorted(cases):
        case = cases[case_id]
        body = embedding_request_body(
            args.model,
            case_input(case),
            case["role"],
            case["instruction"],
            default_instruction,
        )
        vector = post_embeddings(args.base_url, body, args.timeout)[0]
        server_full[case_id] = vector
        rows.extend(case_gates(case_id, case["embeddings"]["1024"], vector, min_cosine))

        reduced_body = embedding_request_body(
            args.model,
            case_input(case),
            case["role"],
            case["instruction"],
            default_instruction,
            dimensions=CHECK_DIMENSIONS,
        )
        reduced = post_embeddings(args.base_url, reduced_body, args.timeout)[0]
        rows.extend(mrl_gates(case_id, vector, reduced, CHECK_DIMENSIONS))

    document_ids = [case_id for case_id in sorted(cases) if cases[case_id]["role"] == "document"]
    batch_body = embedding_request_body(
        args.model,
        [case_input(cases[case_id]) for case_id in document_ids],
        "document",
        None,
        default_instruction,
    )
    batch_vectors = post_embeddings(args.base_url, batch_body, args.timeout)
    for case_id, batch_vector in zip(document_ids, batch_vectors):
        rows.extend(batch_gates(case_id, server_full[case_id], batch_vector))

    oracle_vectors = {case_id: cases[case_id]["embeddings"]["1024"] for case_id in cases}
    rows.extend(retrieval_gates(oracle_vectors, server_full))
    if "query_shared_text" in server_full and "doc_shared_text" in server_full:
        rows.append(
            shared_text_gate(server_full["query_shared_text"], server_full["doc_shared_text"])
        )

    evidence = {
        "schema": SCHEMA,
        "oracle": {"path": str(args.oracle), "schema": ORACLE_SCHEMA, "cases": len(cases)},
        "base_url": args.base_url,
        "model": args.model,
        "tier": args.tier,
        "min_cosine": min_cosine,
        "gates": rows,
        "pass": all(row["pass"] for row in rows),
    }
    return rows, evidence


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--model", default="qwen3-embedding-0.6b")
    parser.add_argument("--tier", choices=sorted(TIER_MIN_COSINE), required=True)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        rows, evidence = run_qualification(args)
    except (QualificationError, OSError, RuntimeError, ValueError, urllib.error.URLError) as exc:
        print(f"qwen3-embedding qualification failed: {exc}", file=sys.stderr)
        return 2
    if args.report:
        args.report.write_text(
            json.dumps(evidence, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
    failures = [row for row in rows if not row["pass"]]
    if failures:
        print(f"FAIL: {len(failures)} of {len(rows)} gates failed (tier={args.tier})\n")
        print(render_table(failures))
        return 1
    print(
        f"PASS: all {len(rows)} gates passed "
        f"(tier={args.tier}, min_cosine={TIER_MIN_COSINE[args.tier]})\n"
    )
    print(render_table(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
