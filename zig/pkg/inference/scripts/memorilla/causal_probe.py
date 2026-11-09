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

"""Fixed v1 question/collection ablations through the unchanged native API.

Oracle support is an annotated diagnostic, not a retrieval product. Retrieval
uses only a declared semantic query and document vectors, never answer labels.
Nothing in this runner rewrites questions inside the serving API.
"""

from __future__ import annotations

import argparse
import datetime
import json
import math
from pathlib import Path
import struct

from assets import LOCK_PATH
import quality_eval as qe
from quality_cases import score_answer

HERE = Path(__file__).resolve().parent
SUITE_SHA256 = "8e0b90709bca8893e0d5d5a7b8be381f280edaed5200c8296e277e4be4067630"
SCORER_SHA256 = "c3499c3645d5a42dacdf43c4131e6680a65fa22287016de18a5e5bc6b91cb80b"
CASE_IDS = (
    "copper_badge_a",
    "copper_badge_b",
    "lantern_courier_a",
    "lantern_courier_b",
    "indicator_color_a",
    "indicator_color_b",
)
# Exact authored inputs and hand-declared variants. No suffix stripping rule,
# answer-dependent rewriting, or held-out v2 inputs are involved.
VARIANTS = {
    "copper_badge": (
        "Who owns the copper access badge at Neral station? Give only the name.",
        "Who owns the copper access badge at Neral station?",
        "single_hop",
        [0],
    ),
    "lantern_courier": (
        "Who carries the crate containing the signal lantern? Give only the name.",
        "Who carries the crate containing the signal lantern?",
        "two_hop",
        [0, 1],
    ),
    "indicator_color": (
        "What color is the Selnix indicator? Give only the color.",
        "What color is the Selnix indicator?",
        "single_hop",
        [0],
    ),
}
QUESTION_MODES = ("authored", "bare_semantic")
COLLECTION_MODES = ("full_collection", "oracle_support", "cosine_topk")


def protocol(suite_path: Path) -> tuple[dict, list[dict]]:
    if qe.sha256(suite_path.read_bytes()) != SUITE_SHA256:
        raise ValueError("This probe requires the frozen v1 suite")
    if qe.sha256((HERE / "quality_cases.py").read_bytes()) != SCORER_SHA256:
        raise ValueError("Frozen v1 scorer changed")
    suite = qe.load_suite(suite_path)
    by_id = {case["id"]: case for case in suite["cases"]}
    cases = [by_id[name] for name in CASE_IDS]
    declared = []
    for case in cases:
        full, bare, category, support = VARIANTS[case["scenario"]]
        if (
            case["question"] != full
            or case["category"] != category
            or case["evidence_document_indices"] != support
        ):
            raise ValueError("Fixed case variant or support annotation changed")
        declared.append(
            {
                "case_id": case["id"],
                "authored_question": full,
                "bare_semantic_question": bare,
                "category": category,
                "oracle_support_indices": support,
                "retrieval_k": 1 if category == "single_hop" else 2,
                "document_sha256": [qe.document_key(t) for t in case["documents"]],
            }
        )
    value = {
        "schema": "antfly.memorilla.causal-probe-protocol.v1",
        "diagnostic_only": True,
        "rollout_qualified": False,
        "suite_sha256": SUITE_SHA256,
        "scorer_sha256": SCORER_SHA256,
        "runner_sha256": qe.sha256(Path(__file__).read_bytes()),
        "quality_eval_sha256": qe.sha256((HERE / "quality_eval.py").read_bytes()),
        "base_models_lock_sha256": qe.sha256(LOCK_PATH.read_bytes()),
        "profile_identity_source_sha256": qe.sha256(
            (HERE / "http_smoke.py").read_bytes()
        ),
        "encoder_execution_identity": qe.IDENTITY,
        "decoder_execution_identity": qe.DECODER_IDENTITY,
        "memory_module_sha256": qe.STAGE3_SHA256,
        "max_new_tokens": 48,
        "questions": list(QUESTION_MODES),
        "collections": list(COLLECTION_MODES),
        "expected_generation_calls": len(cases) * 6,
        "retrieval_query": "bare_semantic, fixed across decoder-question modes",
        "question_intervention": "Both the internal reader query embedding and decoder question change through the current API; this is not an isolated query-embedding intervention",
        "retrieval_precision": "FP16-rounded stored document vectors; native F32 query; Python f64 cosine accumulation",
        "retrieval_order": "descending cosine, ties by original index; selected rows restored to original order",
        "oracle_scope": "Uses frozen annotated evidence indices; oracle diagnostic, not a product capability",
        "provenance_scope": "Hashes bind local evidence integrity and declared profiles, not source authorization",
        "selection_uses_answers": False,
        "cases": declared,
    }
    return value, cases


def fp16_stored(row: list[float]) -> list[float]:
    # Same FP16 storage precision as the serving conversion, without requiring
    # Torch or a GPU. BF16 readout rounding is performed later by the API.
    return [struct.unpack("<e", struct.pack("<e", v))[0] for v in row]


def cosine_topk(documents: list[list[float]], query: list[float], k: int) -> dict:
    if not 1 <= k <= len(documents):
        raise ValueError("Invalid retrieval k")
    qe.validate_row(query)
    qnorm = math.sqrt(math.fsum(v * v for v in query))
    scores = []
    for index, native in enumerate(documents):
        qe.validate_row(native)
        row = fp16_stored(native)
        norm = math.sqrt(math.fsum(v * v for v in row))
        score = math.fsum(x * y for x, y in zip(row, query, strict=True)) / (
            norm * qnorm
        )
        scores.append({"index": index, "cosine": score})
    ranked = sorted(scores, key=lambda item: (-item["cosine"], item["index"]))
    return {
        "ranking": ranked,
        "k": k,
        "selected_indices": sorted(item["index"] for item in ranked[:k]),
    }


def summarize(results: list[dict], cases: list[dict]) -> dict:
    summary = {}
    for query_mode in QUESTION_MODES:
        for collection_mode in COLLECTION_MODES:
            rows = [
                r
                for r in results
                if r["question_mode"] == query_mode
                and r["collection_mode"] == collection_mode
            ]
            valid = {r["case_id"] for r in rows}
            correct = sum(r["score"]["correct"] for r in rows)
            summary[query_mode + "/" + collection_mode] = {
                "correct": correct,
                "total": len(cases),
                "accuracy": correct / len(cases),
                "completed": len(rows),
                "missing_case_ids": [c["id"] for c in cases if c["id"] not in valid],
                "oracle_diagnostic": collection_mode == "oracle_support",
            }
    return summary


def run(args: argparse.Namespace, client=None) -> dict:
    if args.output.exists():
        raise ValueError("Preserve previous probe attempts; choose a new output")
    locked, cases = protocol(args.suite)
    args.output.mkdir(parents=True)
    qe.save_json(args.output / "protocol.json", locked)
    if args.protocol_only:
        return locked
    if args.features is None:
        raise ValueError("A native v1 document feature store is required")
    if args.timeout_ms <= 0:
        raise ValueError("timeout_ms must be positive")
    documents = list(dict.fromkeys(t for c in cases for t in c["documents"]))
    try:
        source_bytes = args.features.read_bytes()
        source_store = json.loads(source_bytes)
        rows = qe.validate_store(source_store, documents)
        selected = {qe.document_key(t) for t in documents}
        store = {
            "schema": qe.FEATURE_SCHEMA,
            "encoder_execution_identity": qe.IDENTITY,
            "documents": [
                item
                for item in source_store["documents"]
                if item["text_sha256"] in selected
            ],
        }
        store_path = args.output / "document-features.json"
        qe.save_json(store_path, store)
        reloaded = json.loads(store_path.read_bytes())
        if reloaded != store:
            raise ValueError("Feature store JSON round trip changed values")
        rows = qe.validate_store(reloaded, documents)
    except BaseException as error:
        qe.save_json(
            args.output / "result.json",
            {
                "completed": False,
                "execution_passed": False,
                "rollout_qualified": False,
                "results": [],
                "summary": summarize([], cases),
                "error": {"type": type(error).__name__, "message": str(error)},
            },
        )
        raise
    result_path = args.output / "result.json"
    record = {
        "schema": "antfly.memorilla.causal-probe.v1",
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "completed": False,
        "execution_passed": False,
        "rollout_qualified": False,
        "profile": args.profile,
        "encoder": args.encoder,
        "protocol_sha256": qe.sha256((args.output / "protocol.json").read_bytes()),
        "feature_provenance": {
            "source": str(args.features),
            "source_sha256": qe.sha256(source_bytes),
            "copied_store_sha256": qe.sha256(store_path.read_bytes()),
            "json_round_trip_equal": True,
            "scope": locked["provenance_scope"],
        },
        "query_features": [],
        "retrieval": [],
        "results": [],
        "events": [],
    }

    def checkpoint():
        qe.save_json(result_path, record)

    checkpoint()
    client = client or qe.HttpClient(
        args.base_url, args.timeout_ms, record["events"], checkpoint
    )
    serial = 0

    def post(path, body):
        nonlocal serial
        serial += 1
        stem = args.output / f"http-{serial:03}"
        qe.save_json(stem.with_suffix(".request.json"), body)
        try:
            value = client.post(path, body)
        except BaseException as error:
            qe.save_json(
                stem.with_suffix(".error.json"),
                {"type": type(error).__name__, "message": str(error)},
            )
            raise
        qe.save_json(stem.with_suffix(".response.json"), value)
        return value, stem.name

    try:
        queries = {}
        unique = list(
            dict.fromkeys(
                text for case in cases for text in VARIANTS[case["scenario"]][:2]
            )
        )
        for text in unique:
            response, evidence = post(
                "embed",
                {
                    "model": args.encoder,
                    "input": [text],
                    "task_type": "RETRIEVAL_QUERY",
                },
            )
            if response.get("model_identity") != qe.IDENTITY:
                raise ValueError("Unqualified query producer identity")
            values = response.get("embeddings")
            if values is None:
                data = response.get("data")
                if (
                    not isinstance(data, list)
                    or len(data) != 1
                    or data[0].get("index") != 0
                ):
                    raise ValueError("Invalid indexed query producer data")
                values = [data[0].get("embedding")]
            if not isinstance(values, list) or len(values) != 1:
                raise ValueError("Invalid query producer row count")
            qe.validate_row(values[0])
            queries[text] = values[0]
            record["query_features"].append(
                {
                    "text": text,
                    "text_sha256": qe.document_key(text),
                    "embedding": values[0],
                    "embedding_sha256": qe.sha256(qe.json_bytes(values[0])),
                    "producer_response": evidence + ".response.json",
                }
            )
            checkpoint()
        # Persist and reload query vectors before any retrieval/generation.
        query_store = args.output / "query-features.json"
        qe.save_json(query_store, {"queries": record["query_features"]})
        loaded = json.loads(query_store.read_bytes())["queries"]
        if loaded != record["query_features"]:
            raise ValueError("Query store JSON round trip changed values")
        queries = {item["text"]: item["embedding"] for item in loaded}
        record["query_store_sha256"] = qe.sha256(query_store.read_bytes())
        for case in cases:
            full, bare, category, support = VARIANTS[case["scenario"]]
            native = [rows[qe.document_key(t)] for t in case["documents"]]
            ranking = cosine_topk(
                native, queries[bare], 1 if category == "single_hop" else 2
            )
            record["retrieval"].append(
                {"case_id": case["id"], "query": bare, **ranking}
            )
            collections = {
                "full_collection": list(range(len(native))),
                "oracle_support": support,
                "cosine_topk": ranking["selected_indices"],
            }
            for query_mode, question in zip(QUESTION_MODES, (full, bare), strict=True):
                for collection_mode in COLLECTION_MODES:
                    indices = collections[collection_mode]
                    body = {
                        "model": args.profile,
                        "question": question,
                        "document_embeddings": [native[i] for i in indices],
                        "encoder_execution_identity": qe.IDENTITY,
                        "max_new_tokens": 48,
                    }
                    response, evidence = post("memorilla/generate", body)
                    qe.validate_response(
                        response,
                        profile=args.profile,
                        module_sha256=qe.STAGE3_SHA256,
                        documents=len(indices),
                        max_new_tokens=48,
                    )
                    record["results"].append(
                        {
                            "case_id": case["id"],
                            "question_mode": query_mode,
                            "question": question,
                            "collection_mode": collection_mode,
                            "oracle_diagnostic": collection_mode == "oracle_support",
                            "selected_document_indices": indices,
                            "selected_document_sha256": [
                                qe.document_key(case["documents"][i]) for i in indices
                            ],
                            "response": response,
                            "http_evidence": evidence,
                            "score": score_answer(response["text"], case),
                        }
                    )
                    record["summary"] = summarize(record["results"], cases)
                    checkpoint()
        record["completed"] = record["execution_passed"] = True
    except BaseException as error:
        record["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        record["summary"] = summarize(record["results"], cases)
        record["finished_utc"] = datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat()
        checkpoint()
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:18092")
    parser.add_argument("--profile", default="memorilla-stage3")
    parser.add_argument("--encoder", default="encoder")
    parser.add_argument("--suite", type=Path, default=HERE / "fixtures/quality_v1.json")
    parser.add_argument("--features", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-ms", type=int, default=300000)
    parser.add_argument("--protocol-only", action="store_true")
    result = run(parser.parse_args())
    print(json.dumps(result.get("summary", result), indent=2))


if __name__ == "__main__":
    main()
