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

"""Evaluate raw-document QA through an already-running native Memorilla API.

Execution checks and answer scores are separate. A completed diagnostic run is
not a workload-quality qualification. Gold answers never enter API requests.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from http_smoke import DECODER_IDENTITY, IDENTITY, STAGE3_SHA256
from quality_cases import load_suite, score_answer, score_suite


HERE = Path(__file__).resolve().parent
FEATURE_SCHEMA = "antfly.memorilla.quality-features.v1"


def json_bytes(value) -> bytes:
    return json.dumps(
        value, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode()


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def save_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, allow_nan=False, indent=2) + "\n")
    temporary.replace(path)


def validate_row(row) -> None:
    if not isinstance(row, list) or len(row) != 2560:
        raise ValueError("Feature row must contain 2560 numbers")
    if any(type(x) not in (int, float) or not math.isfinite(x) for x in row):
        raise ValueError("Feature row must contain finite numbers")
    if abs(math.sqrt(sum(x * x for x in row)) - 1.0) > 1e-4:
        raise ValueError("Native producer feature row is not unit normalized")


def document_key(text: str) -> str:
    return sha256(text.encode())


def validate_store(store: dict, documents: list[str]) -> dict[str, list[float]]:
    if (
        store.get("schema") != FEATURE_SCHEMA
        or store.get("encoder_execution_identity") != IDENTITY
    ):
        raise ValueError("Incompatible feature store schema or encoder identity")
    rows = {}
    for item in store.get("documents", []):
        text, row = item["text"], item["embedding"]
        key = document_key(text)
        if item.get("text_sha256") != key or key in rows:
            raise ValueError("Feature store has a modified or duplicate document")
        validate_row(row)
        if item.get("embedding_sha256") != sha256(json_bytes(row)):
            raise ValueError("Feature store embedding digest mismatch")
        rows[key] = row
    if any(document_key(text) not in rows for text in documents):
        raise ValueError("Feature store does not cover the selected documents")
    return rows


class HttpClient:
    def __init__(self, base_url: str, timeout_ms: int, events: list[dict], checkpoint):
        parsed = urlsplit(base_url)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "Use an HTTP(S) base URL without credentials, query or fragment"
            )
        self.base_url = base_url.rstrip("/")
        self.timeout_ms, self.events, self.checkpoint = timeout_ms, events, checkpoint

    def post(self, path: str, body: dict) -> dict:
        raw = json_bytes(body)
        headers = {
            "Content-Type": "application/json",
            "X-Antfly-Inference-Timeout-Ms": str(self.timeout_ms),
        }
        if os.environ.get("ANTFLY_API_KEY"):
            headers["Authorization"] = "Bearer " + os.environ["ANTFLY_API_KEY"]
        request = Request(self.base_url + "/ai/v1/" + path, raw, headers, method="POST")
        started = time.monotonic()
        try:
            response = urlopen(request, timeout=self.timeout_ms / 1000 + 30)
        except HTTPError as error:
            response = error
        with response:
            status, data = response.status, response.read()
        self.events.append(
            {
                "path": path,
                "status": status,
                "seconds": time.monotonic() - started,
                "request_sha256": sha256(raw),
                "response_sha256": sha256(data),
            }
        )
        self.checkpoint()
        value = json.loads(data)
        if status != 200:
            raise RuntimeError(f"{path} returned {status}: {value}")
        return value


def validate_response(
    value: dict,
    *,
    profile: str,
    module_sha256: str,
    documents: int,
    max_new_tokens: int,
) -> None:
    expected = {
        "model": profile,
        "context_source": "supplied_embeddings",
        "input_documents": documents,
        "memory_tokens": 16,
        "encoder_execution_identity": IDENTITY,
        "decoder_execution_identity": DECODER_IDENTITY,
        "memory_module_sha256": module_sha256,
    }
    for key, item in expected.items():
        if value.get(key) != item:
            raise ValueError(
                f"Generation response {key} differs from the requested qualified composition"
            )
    ids = value.get("token_ids")
    if (
        not isinstance(value.get("text"), str)
        or not isinstance(ids, list)
        or not 1 <= len(ids) <= max_new_tokens
    ):
        raise ValueError("Generation response text or token count is invalid")
    if any(type(x) is not int or not 0 <= x < 151936 for x in ids):
        raise ValueError("Generation response token IDs are invalid")
    usage = value.get("usage", {})
    if (
        usage.get("completion_tokens") != len(ids)
        or type(usage.get("encoder_query_tokens")) is not int
        or not 1 <= usage["encoder_query_tokens"] <= 256
    ):
        raise ValueError("Generation response usage is invalid")
    if type(usage.get("prompt_tokens")) is not int or usage["prompt_tokens"] <= 16:
        raise ValueError("Generation response prompt usage is invalid")
    if value.get("finish_reason") not in {"eos", "length"}:
        raise ValueError("Generation response finish reason is invalid")


def run(args: argparse.Namespace, client=None) -> dict:
    if args.output.exists():
        raise ValueError("Preserve previous evaluation receipts; choose a new --output")
    suite = load_suite(args.suite)
    cases = suite["cases"]
    if args.case_id:
        unknown = set(args.case_id) - {case["id"] for case in cases}
        if unknown:
            raise ValueError(f"Unknown case IDs: {sorted(unknown)}")
        cases = [case for case in cases if case["id"] in args.case_id]
    budget = (
        suite["max_new_tokens"] if args.max_new_tokens is None else args.max_new_tokens
    )
    if not 1 <= budget <= 128:
        raise ValueError("max_new_tokens must be between 1 and 128")
    if len(args.module_sha256) != 64 or any(
        c not in "0123456789abcdef" for c in args.module_sha256
    ):
        raise ValueError("Expected a lowercase SHA256 for the memory module")
    documents = list(dict.fromkeys(doc for case in cases for doc in case["documents"]))
    feature_path = args.output.with_name(args.output.stem + "-features.json")
    if feature_path.exists():
        raise ValueError("Preserve previous feature stores; choose a new --output")
    record = {
        "schema": "antfly.memorilla.quality-evaluation.v1",
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "completed": False,
        "execution_passed": False,
        "rollout_qualified": False,
        "scope": "Synthetic development diagnostic; raw documents to native embeddings to query-conditioned memory to native answers",
        "profile": args.profile,
        "encoder": args.encoder,
        "encoder_execution_identity": IDENTITY,
        "decoder_execution_identity": DECODER_IDENTITY,
        "memory_module_sha256": args.module_sha256,
        "max_new_tokens": budget,
        "suite": str(args.suite),
        "suite_sha256": sha256(args.suite.read_bytes()),
        "runner_sha256": sha256(Path(__file__).read_bytes()),
        "scorer_sha256": sha256((HERE / "quality_cases.py").read_bytes()),
        "selected_case_ids": [case["id"] for case in cases],
        "all_suite_cases_selected": len(cases) == len(suite["cases"]),
        "feature_store": str(feature_path),
        "results": [],
        "events": [],
        "checks": {},
    }

    def checkpoint():
        save_json(args.output, record)

    checkpoint()
    client = client or HttpClient(
        args.base_url, args.timeout_ms, record["events"], checkpoint
    )
    try:
        if args.reuse_features:
            store = json.loads(args.reuse_features.read_text())
            rows = validate_store(store, documents)
            record["feature_provenance"] = (
                "reused locally recorded native producer output; digests establish integrity, not authorization"
            )
            record["reused_feature_store"] = {
                "path": str(args.reuse_features),
                "sha256": sha256(args.reuse_features.read_bytes()),
            }
        else:
            store = {
                "schema": FEATURE_SCHEMA,
                "encoder_execution_identity": IDENTITY,
                "documents": [],
            }
            rows = {}
            for offset in range(0, len(documents), 4):
                batch = documents[offset : offset + 4]
                value = client.post(
                    "embed",
                    {
                        "model": args.encoder,
                        "input": batch,
                        "task_type": "RETRIEVAL_DOCUMENT",
                    },
                )
                if value.get("model_identity") != IDENTITY:
                    raise ValueError("Native document producer identity mismatch")
                embeddings = value.get("embeddings")
                if embeddings is None:
                    embeddings = [item["embedding"] for item in value["data"]]
                if len(embeddings) != len(batch):
                    raise ValueError("Native document producer row count mismatch")
                for text, row in zip(batch, embeddings, strict=True):
                    validate_row(row)
                    key = document_key(text)
                    rows[key] = row
                    store["documents"].append(
                        {
                            "text": text,
                            "text_sha256": key,
                            "embedding": row,
                            "embedding_sha256": sha256(json_bytes(row)),
                        }
                    )
                save_json(feature_path, store)
            record["feature_provenance"] = (
                "native qualified embedding HTTP producer in this run"
            )
        save_json(feature_path, store)
        # All generations use rows read back from persistent JSON, not transient
        # producer objects. This is the same boundary an application must cross.
        rows = validate_store(json.loads(feature_path.read_text()), documents)
        record["feature_store_sha256"] = sha256(feature_path.read_bytes())
        record["checks"]["stored_feature_round_trip"] = True
        record["unique_documents"] = len(documents)
        first_body = first_answer = None
        for case in cases:
            body = {
                "model": args.profile,
                "question": case["question"],
                "document_embeddings": [
                    rows[document_key(text)] for text in case["documents"]
                ],
                "encoder_execution_identity": IDENTITY,
                "max_new_tokens": budget,
            }
            started = time.monotonic()
            value = client.post("memorilla/generate", body)
            validate_response(
                value,
                profile=args.profile,
                module_sha256=args.module_sha256,
                documents=len(case["documents"]),
                max_new_tokens=budget,
            )
            result = {
                "case_id": case["id"],
                "category": case["category"],
                "answer": value["text"],
                "score": score_answer(value["text"], case),
                "response": value,
                "seconds": time.monotonic() - started,
                "request_sha256": sha256(json_bytes(body)),
                "document_sha256s": [document_key(text) for text in case["documents"]],
            }
            record["results"].append(result)
            if first_body is None:
                first_body, first_answer = body, value
            checkpoint()
            print(
                json.dumps(
                    {
                        "case_id": case["id"],
                        "answer": value["text"],
                        "score": result["score"],
                    }
                ),
                flush=True,
            )
        if first_body is not None:
            repeated = client.post("memorilla/generate", first_body)
            validate_response(
                repeated,
                profile=args.profile,
                module_sha256=args.module_sha256,
                documents=len(first_body["document_embeddings"]),
                max_new_tokens=budget,
            )
            record["checks"]["first_case_after_all_cases_response_equal"] = (
                repeated == first_answer
            )
            if repeated != first_answer:
                raise ValueError(
                    "First response changed after intervening document collections"
                )
        record["execution_passed"] = True
        record["completed"] = True
    except Exception as error:
        record["error"] = {"type": type(error).__name__, "message": str(error)}
        raise
    finally:
        # Failed or interrupted cases retain the full denominator. A partial
        # transport failure cannot inflate accuracy by dropping missing answers.
        record["quality"] = score_suite(suite, record["results"])
        record["selected_case_accuracy"] = sum(
            r["score"]["correct"] for r in record["results"]
        ) / len(cases)
        record["truncated_answers"] = sum(
            r["response"]["finish_reason"] == "length" for r in record["results"]
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
    parser.add_argument("--suite", type=Path, default=HERE / "fixtures/quality_v1.json")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--reuse-features", type=Path)
    parser.add_argument("--case-id", action="append")
    parser.add_argument("--max-new-tokens", type=int)
    parser.add_argument("--timeout-ms", type=int, default=300000)
    args = parser.parse_args()
    if args.timeout_ms <= 0:
        parser.error("--timeout-ms must be positive")
    result = run(args)
    print(
        json.dumps(
            {
                key: result[key]
                for key in (
                    "completed",
                    "execution_passed",
                    "rollout_qualified",
                    "quality",
                    "checks",
                )
            }
        )
    )


if __name__ == "__main__":
    main()
