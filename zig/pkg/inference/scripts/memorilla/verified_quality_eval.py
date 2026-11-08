#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Raw-source QA diagnostics for the opt-in source_verified native API.

Development runs the frozen v1 suite and v2 dev groups. Held-out evaluation
requires a passing development execution receipt with identical protocol and
source fingerprints. This is a model-entailment-checked diagnostic, not a
guarantee of truth, feature/text association, authorization, or release quality.
"""

from __future__ import annotations

import argparse
from collections import Counter
import datetime
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import factual_cases
import quality_cases
from http_smoke import DECODER_IDENTITY, IDENTITY, STAGE3_SHA256
from quality_eval import document_key, json_bytes, validate_row

HERE = Path(__file__).parent
V1_SHA256 = "8e0b90709bca8893e0d5d5a7b8be381f280edaed5200c8296e277e4be4067630"
FEATURE_SCHEMA = "antfly.memorilla.verified-quality-features.v1"


def digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def save(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, allow_nan=False, indent=2) + "\n")
    temporary.replace(path)


class Client:
    def __init__(self, base_url: str, timeout_ms: int, events: list[dict], checkpoint):
        url = urlsplit(base_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.netloc
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "use an HTTP(S) base URL without credentials/query/fragment"
            )
        self.base_url, self.timeout_ms = base_url.rstrip("/"), timeout_ms
        self.events, self.checkpoint = events, checkpoint

    def post(self, path: str, body: dict) -> dict:
        raw = json_bytes(body)
        headers = {
            "Content-Type": "application/json",
            "X-Antfly-Inference-Timeout-Ms": str(self.timeout_ms),
        }
        if os.environ.get("ANTFLY_API_KEY"):
            headers["Authorization"] = "Bearer " + os.environ["ANTFLY_API_KEY"]
        request = Request(self.base_url + "/ai/v1/" + path, raw, headers, method="POST")
        event = {
            "path": path,
            "request_sha256": hashlib.sha256(raw).hexdigest(),
            "status": None,
        }
        started = time.monotonic()
        try:
            try:
                response = urlopen(request, timeout=self.timeout_ms / 1000 + 30)
            except HTTPError as exc:
                response = exc
            with response:
                status, data = response.status, response.read()
            event.update(
                status=status, response_sha256=hashlib.sha256(data).hexdigest()
            )
            value = json.loads(data)
            if status != 200:
                event["error_response"] = value
                raise RuntimeError(f"{path} returned {status}: {value}")
            if not isinstance(value, dict):
                raise ValueError("native response must be an object")
            return value
        except Exception as exc:
            event["error"] = {"type": type(exc).__name__, "message": str(exc)}
            raise
        finally:
            event["seconds"] = time.monotonic() - started
            self.events.append(event)
            self.checkpoint()


def validate_response(
    value: dict, *, profile: str, module_sha256: str, documents: list[str], maximum: int
) -> None:
    expected = {
        "model": profile,
        "context_source": "supplied_embeddings_and_documents",
        "input_documents": len(documents),
        "source_alignment": "caller_supplied",
        "encoder_execution_identity": IDENTITY,
        "decoder_execution_identity": DECODER_IDENTITY,
        "memory_module_sha256": module_sha256,
        "document_text_sha256s": [document_key(text) for text in documents],
    }
    for key, item in expected.items():
        if value.get(key) != item:
            raise ValueError(f"verified response {key} mismatch")
    path, status = value.get("answer_path"), value.get("verification_status")
    if path not in {"memory_verified", "source_fallback_verified", "abstained"}:
        raise ValueError("unknown verified answer path")
    usage, ids = value.get("usage"), value.get("token_ids")
    if (
        not isinstance(usage, dict)
        or not isinstance(ids, list)
        or not isinstance(value.get("text"), str)
    ):
        raise ValueError("invalid verified text/token/usage shape")
    if any(type(i) is not int or not 0 <= i < 151936 for i in ids):
        raise ValueError("invalid verified token IDs")
    for key in (
        "encoder_query_tokens",
        "prompt_tokens",
        "completion_tokens",
        "total_prompt_tokens",
        "total_completion_tokens",
        "verification_calls",
    ):
        if type(usage.get(key)) is not int or usage[key] < 0:
            raise ValueError(f"invalid verified usage {key}")
    if (
        not 1 <= usage["encoder_query_tokens"] <= 256
        or usage["completion_tokens"] != len(ids)
        or usage["total_prompt_tokens"] < usage["prompt_tokens"]
        or usage["total_completion_tokens"] < usage["completion_tokens"]
        or not 0 <= usage["verification_calls"] <= 1
        or type(usage.get("source_fallback_used")) is not bool
        or usage["source_fallback_used"] != (path != "memory_verified")
        or usage["total_prompt_tokens"] <= usage["prompt_tokens"]
        or usage["total_completion_tokens"] <= usage["completion_tokens"]
    ):
        raise ValueError("inconsistent verified usage")
    if path == "abstained":
        if (
            status != "abstained"
            or value.get("finish_reason") != "abstained"
            or value["text"] != "UNKNOWN"
            or ids
            or usage["completion_tokens"] != 0
            or value.get("memory_tokens") != 0
        ):
            raise ValueError("inconsistent policy abstention")
    else:
        if (
            status != "model_supported"
            or value.get("finish_reason") != "eos"
            or not 1 <= len(ids) <= maximum
            or not value["text"].strip()
            or value["text"].strip() == "UNKNOWN"
            or value.get("memory_tokens") != (16 if path == "memory_verified" else 0)
            or usage["verification_calls"] < 1
        ):
            raise ValueError("inconsistent supported answer metadata")


def feature_rows(store: dict) -> dict:
    if (
        store.get("schema") != FEATURE_SCHEMA
        or store.get("encoder_execution_identity") != IDENTITY
    ):
        raise ValueError("incompatible persisted native feature store")
    result = {}
    for row in store.get("documents", []):
        text, vector = row["text"], row["embedding"]
        validate_row(vector)
        key = document_key(text)
        if (
            row.get("text_sha256") != key
            or key in result
            or row.get("embedding_sha256")
            != hashlib.sha256(json_bytes(vector)).hexdigest()
        ):
            raise ValueError("persisted document/vector identity mismatch")
        result[key] = vector
    return result


def producer_vectors(value: dict, count: int) -> list:
    if value.get("model_identity") != IDENTITY:
        raise ValueError("native producer execution identity mismatch")
    vectors = value.get("embeddings")
    if vectors is None:
        data = value.get("data")
        if not isinstance(data, list) or any(
            not isinstance(item, dict)
            or type(item.get("index")) is not int
            or item["index"] != i
            for i, item in enumerate(data)
        ):
            raise ValueError(
                "native OpenAI embedding indices differ from document order"
            )
        vectors = [item["embedding"] for item in data]
    if not isinstance(vectors, list) or len(vectors) != count:
        raise ValueError("native producer row count mismatch")
    for vector in vectors:
        validate_row(vector)
    return vectors


def development_gate(path: Path | None, current: dict) -> None:
    if path is None:
        raise ValueError("held-out test requires --development-receipt")
    previous = json.loads(path.read_text())
    if (
        not previous.get("completed")
        or not previous.get("execution_passed")
        or previous.get("split") != "dev"
    ):
        raise ValueError("development receipt did not complete all execution checks")
    for key in (
        "source_sha256",
        "profile",
        "encoder",
        "module_sha256",
        "max_new_tokens",
        "protocol",
        "local_binary_sha256",
    ):
        if previous.get(key) != current.get(key):
            raise ValueError(
                f"development protocol changed before held-out test: {key}"
            )
    if previous.get("attempted_cases") != {"v1": 24, "v2": 16}:
        raise ValueError("development receipt does not cover the complete suites")
    results = previous.get("results", [])
    if len(results) != 40 or any(
        r.get("execution_passed") is not True for r in results
    ):
        raise ValueError("development receipt contains missing or failed requests")
    # Recompute from the stored answers, not cached per-result score flags.
    v1 = quality_cases.score_suite(
        quality_cases.load_suite(quality_cases.DEFAULT_SUITE),
        [r for r in results if r["suite"] == "v1"],
    )
    v2 = factual_cases.score_suite(
        factual_cases.load_suite(),
        [r for r in results if r["suite"] == "v2"],
        split="dev",
    )
    if (
        v1["primary"]["correct"] != 22
        or v1["primary"]["total"] != 22
        or v1["auxiliary_diagnostic"]["correct"] != 2
        or v1["auxiliary_diagnostic"]["total"] != 2
        or v2["all"] != {"correct": 16, "total": 16, "missing": 0}
    ):
        raise ValueError(
            "development quality target requires all22 facts,2negatives,and16dev answers correct before held-out evaluation"
        )


def run(args, client=None) -> dict:
    if args.output.exists():
        raise ValueError("preserve previous receipts; choose a new --output")
    if (
        args.split not in {"dev", "test"}
        or not 1 <= args.max_new_tokens <= 128
        or not 1 <= args.timeout_ms <= 3_600_000
    ):
        raise ValueError("invalid verified evaluation bounds")
    if (
        not isinstance(args.module_sha256, str)
        or len(args.module_sha256) != 64
        or any(c not in "0123456789abcdef" for c in args.module_sha256)
    ):
        raise ValueError("module SHA256 must be lowercase hexadecimal")
    if digest(quality_cases.DEFAULT_SUITE) != V1_SHA256:
        raise ValueError("frozen v1 fixture changed")
    v1, v2 = (
        quality_cases.load_suite(quality_cases.DEFAULT_SUITE),
        factual_cases.load_suite(),
    )
    selected = [("v1", c) for c in v1["cases"]] + [
        ("v2", c) for c in v2["cases"] if c["split"] == args.split
    ]
    documents = list(
        dict.fromkeys(text for _, case in selected for text in case["documents"])
    )
    features_path = args.output.with_name(args.output.stem + "-features.json")
    if features_path.exists():
        raise ValueError("preserve previous native feature store")
    record = {
        "schema": "antfly.memorilla.verified-quality.v1",
        "completed": False,
        "execution_passed": False,
        "rollout_qualified": False,
        "split": args.split,
        "profile": args.profile,
        "encoder": args.encoder,
        "module_sha256": args.module_sha256,
        "base_url": args.base_url,
        "max_new_tokens": args.max_new_tokens,
        "local_binary_sha256": digest(args.native_binary)
        if args.native_binary
        else None,
        "binary_provenance": "optional controller-supplied local artifact hash; not HTTP attestation",
        "source_sha256": {
            p.name: digest(p)
            for p in (
                Path(__file__),
                HERE / "quality_eval.py",
                HERE / "quality_cases.py",
                HERE / "factual_cases.py",
                quality_cases.DEFAULT_SUITE,
                factual_cases.DEFAULT_SUITE,
            )
        },
        "protocol": {
            "context_mode": "source_verified",
            "source_alignment": "caller_supplied",
            "verification": "independent source answer always generated; strict source-answer verifier at most once; memory publication requires conservative exact agreement; not formal proof or guaranteed truth",
            "source_fallback_used": "publication selects source answer or policy abstention; not whether independent source work occurred",
            "v1_question": "frozen unchanged including existing answer instruction",
            "v2_question": "bare semantic question; separate authored answer_instruction recorded but not appended",
            "v2_answer_instruction": v2["answer_instruction"],
            "source_annotations": "authored only; support/gold/metadata never sent to API",
            "feature_production": "native raw document batches4, persisted and reloaded before generation",
            "heldout": "same-protocol completed development execution and recomputed22v1facts+2negatives+16v2dev all correct required; this diagnostic gate is not production qualification",
        },
        "results": [],
        "events": [],
        "errors": [],
        "attempted_cases": {"v1": 0, "v2": 0},
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "feature_store": str(features_path),
    }
    if args.split == "test":
        development_gate(args.development_receipt, record)
        record["development_receipt_sha256"] = digest(args.development_receipt)

    def checkpoint():
        save(args.output, record)

    checkpoint()
    client = client or Client(
        args.base_url, args.timeout_ms, record["events"], checkpoint
    )
    store = {
        "schema": FEATURE_SCHEMA,
        "encoder_execution_identity": IDENTITY,
        "documents": [],
    }
    try:
        for offset in range(0, len(documents), 4):
            batch = documents[offset : offset + 4]
            try:
                value = client.post(
                    "embed",
                    {
                        "model": args.encoder,
                        "input": batch,
                        "task_type": "RETRIEVAL_DOCUMENT",
                    },
                )
                vectors = producer_vectors(value, len(batch))
                for text, vector in zip(batch, vectors, strict=True):
                    store["documents"].append(
                        {
                            "text": text,
                            "text_sha256": document_key(text),
                            "embedding": vector,
                            "embedding_sha256": hashlib.sha256(
                                json_bytes(vector)
                            ).hexdigest(),
                        }
                    )
            except Exception as exc:
                record["errors"].append(
                    {
                        "stage": "embed",
                        "document_sha256s": [document_key(t) for t in batch],
                        "type": type(exc).__name__,
                        "message": str(exc),
                    }
                )
            save(features_path, store)
            checkpoint()
        # Use the published JSON readback, not the in-memory API arrays.
        rows = feature_rows(json.loads(features_path.read_text()))
        record["feature_store_sha256"] = digest(features_path)
        record["stored_feature_round_trip"] = True
        for name, case in selected:
            result = {
                "suite": name,
                "case_id": case["id"],
                "category": case["category"],
                "execution_passed": False,
            }
            record["attempted_cases"][name] += 1
            started = time.monotonic()
            try:
                body = {
                    "model": args.profile,
                    "question": case["question"],
                    "document_embeddings": [
                        rows[document_key(text)] for text in case["documents"]
                    ],
                    "encoder_execution_identity": IDENTITY,
                    "max_new_tokens": args.max_new_tokens,
                    "context_mode": "source_verified",
                    "document_texts": case["documents"],
                }
                result["request_sha256"] = hashlib.sha256(json_bytes(body)).hexdigest()
                value = client.post("memorilla/generate", body)
                result["response"] = value  # Preserve malformed responses too.
                validate_response(
                    value,
                    profile=args.profile,
                    module_sha256=args.module_sha256,
                    documents=case["documents"],
                    maximum=args.max_new_tokens,
                )
                result.update(answer=value["text"], execution_passed=True)
                result["score"] = (
                    quality_cases if name == "v1" else factual_cases
                ).score_answer(value["text"], case)
            except Exception as exc:
                result["error"] = {"type": type(exc).__name__, "message": str(exc)}
                record["errors"].append(
                    {
                        "stage": "generate",
                        "suite": name,
                        "case_id": case["id"],
                        **result["error"],
                    }
                )
            finally:
                result["request_wall_seconds"] = time.monotonic() - started
                record["results"].append(result)
                checkpoint()
        record["completed"] = True
        record["execution_passed"] = not record["errors"]
    except BaseException as exc:
        record["fatal_error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        valid = [r for r in record["results"] if r["execution_passed"]]
        record["quality_v1"] = quality_cases.score_suite(
            v1, [r for r in valid if r["suite"] == "v1"]
        )
        record["quality_v2"] = factual_cases.score_suite(
            v2, [r for r in valid if r["suite"] == "v2"], split=args.split
        )
        record["answer_paths"] = dict(
            Counter(r["response"]["answer_path"] for r in valid)
        )
        record["finished_utc"] = datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat()
        checkpoint()
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:18091")
    parser.add_argument("--profile", default="memorilla-stage3")
    parser.add_argument("--encoder", default="encoder")
    parser.add_argument("--module-sha256", default=STAGE3_SHA256)
    parser.add_argument("--split", choices=("dev", "test"), default="dev")
    parser.add_argument("--development-receipt", type=Path)
    parser.add_argument("--native-binary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-new-tokens", type=int, default=48)
    parser.add_argument("--timeout-ms", type=int, default=300000)
    args = parser.parse_args()
    result = run(args)
    print(
        json.dumps(
            {
                "completed": result["completed"],
                "execution_passed": result["execution_passed"],
                "answer_paths": result["answer_paths"],
                "rollout_qualified": False,
            }
        )
    )
    if not result["execution_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
