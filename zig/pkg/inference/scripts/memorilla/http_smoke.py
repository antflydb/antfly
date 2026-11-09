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

"""Exercise an already-running local Memorilla server; never starts a listener."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import json
import math
from pathlib import Path
import threading
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen


IDENTITY = "e3d6de85a3204759c8d575bfe109aba3ec1a49318ae01a88c71515415e0ce91a"
DECODER_IDENTITY = "ee58f87b6717501ca7ce0f788945a38c339560d1294cdeb20fc9df14bd77fd71"
STAGE3_SHA256 = "3a7ec610dc29818c2aa29c3867d3be3e69908a191a94d0d6f0f416d564d3470e"


def run(args: argparse.Namespace) -> dict:
    events: list[dict] = []
    event_lock = threading.Lock()

    def post(
        path: str,
        body: dict,
        expected: set[int],
        timeout_ms: int = 300000,
        label: str | None = None,
    ):
        raw = json.dumps(body, allow_nan=False, separators=(",", ":")).encode()
        request = Request(
            args.base_url.rstrip("/") + "/ai/v1/" + path,
            raw,
            {
                "Content-Type": "application/json",
                "X-Antfly-Inference-Timeout-Ms": str(timeout_ms),
            },
            method="POST",
        )
        started = time.monotonic()
        try:
            response = urlopen(request, timeout=360)
        except HTTPError as error:
            response = error
        with response:
            status, data = response.status, response.read()
        value = json.loads(data)
        with event_lock:
            events.append(
                {
                    "path": path,
                    "case": label or path,
                    "status": status,
                    "timeout_ms": timeout_ms,
                    "expected_statuses": sorted(expected),
                    "seconds": time.monotonic() - started,
                    "request_sha256": hashlib.sha256(raw).hexdigest(),
                    "response_sha256": hashlib.sha256(data).hexdigest(),
                    "response": value,
                }
            )
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps({"passed": False, "events": events}, indent=2) + "\n"
            )
        assert status in expected, (path, status, value)
        return value

    def generate(body: dict, expected: set[int] | None = None, timeout_ms=300000):
        return post("memorilla/generate", body, expected or {200}, timeout_ms)

    # These cases fail before model assets or caller vectors are needed.
    invalid = {
        "model": args.profile,
        "question": "q",
        "document_embeddings": [[1.0]],
        "encoder_execution_identity": IDENTITY,
        "max_new_tokens": 3,
    }
    generate({**invalid, "stream": False}, {400})
    generate({**invalid, "model": "unknown-profile"}, {404})
    generate({**invalid, "encoder_execution_identity": "0" * 64}, {409})
    generate(invalid, {400})
    generate({**invalid, "max_new_tokens": 0}, {400})
    generate({**invalid, "max_new_tokens": 129}, {400})
    generate({**invalid, "document_embeddings": [[1.0]] * 65}, {413})
    generate({**invalid, "question": "q" * 8193}, {413})
    ordinary = post(
        "generate",
        {
            "model": args.decoder,
            "messages": [{"role": "user", "content": "q"}],
            "max_tokens": 1,
        },
        {400},
    )
    assert ordinary["error"] == "MEMORILLA_COMPOSITION_REQUIRED", ordinary
    for options, label in (
        (
            {"task_type": "RETRIEVAL_QUERY", "instruction": "override"},
            "encoder_instruction_override",
        ),
        ({"dimensions": 128}, "encoder_truncated_dimensions"),
        ({"task_type": "CLUSTERING"}, "encoder_incompatible_task"),
    ):
        post(
            "embed",
            {"model": args.encoder, "input": "q", **options},
            {400},
            label=label,
        )
    embedded = post(
        "embed",
        {
            "model": args.encoder,
            "input": [
                "Antfly preserves source revisions and applies current authorization when memory changes.",
                "A forget request removes eligible memory and prevents stale derived results from being published.",
            ],
            "task_type": "RETRIEVAL_DOCUMENT",
        },
        {200},
    )
    assert embedded.get("model_identity") == IDENTITY, embedded
    rows = embedded.get("embeddings")
    if rows is None:
        rows = [item["embedding"] for item in embedded["data"]]
    assert len(rows) == 2
    for row in rows:
        assert len(row) == 2560 and all(math.isfinite(x) for x in row)
        assert abs(math.sqrt(sum(x * x for x in row)) - 1) < 1e-4
    query_producer = post(
        "embed",
        {
            "model": args.encoder,
            "input": [
                "Antfly preserves source revisions and applies current authorization when memory changes.",
                "A forget request removes eligible memory and prevents stale derived results from being published.",
            ],
            "task_type": "RETRIEVAL_QUERY",
        },
        {200},
        label="encoder_query_document_raw_recipe_parity",
    )
    query_rows = query_producer.get("embeddings")
    if query_rows is None:
        query_rows = [item["embedding"] for item in query_producer["data"]]
    assert query_producer["model_identity"] == IDENTITY
    assert query_rows == rows, (
        "Qualified query/document producers must use the same raw recipe"
    )
    body = {
        "model": args.profile,
        "question": "What does Antfly preserve when a source changes?",
        "document_embeddings": rows,
        "encoder_execution_identity": embedded["model_identity"],
        "max_new_tokens": 3,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.with_name(args.output.stem + "-stored-features.json").write_text(
        json.dumps(body) + "\n"
    )
    generate({**body, "max_new_tokens": "3"}, {400})
    quoted_value = copy.deepcopy(body)
    # Preserve the numerical value: permissive string-to-float coercion would
    # otherwise produce a fully valid normalized request and mask the bug.
    quoted_value["document_embeddings"][0][0] = str(
        quoted_value["document_embeddings"][0][0]
    )
    generate(quoted_value, {400})
    zero = copy.deepcopy(body)
    zero["document_embeddings"][0] = [0.0] * 2560
    generate(zero, {400})
    # With --input-token-limit, the server must be configured to that ceiling.
    if args.input_token_limit:
        generate({**body, "question": "source " * args.input_token_limit}, {413})
        if args.input_token_limit >= 16:
            # The raw question fits, but the system framing and memory slots
            # exceed the same operator input ceiling.
            generate(
                {**body, "question": "source " * (args.input_token_limit - 8)},
                {413},
            )
    if args.output_token_limit:
        generate({**body, "max_new_tokens": args.output_token_limit + 1}, {413})
    a1 = generate(body)
    assert a1["usage"]["prompt_tokens"] == 53, a1
    b = {
        **body,
        "question": "Which policy applies to a forget request?",
        "document_embeddings": rows * 32,
    }
    b1 = generate(b)
    a2 = generate(body)
    for value, count in ((a1, 2), (b1, 64), (a2, 2)):
        assert value["model"] == args.profile
        assert value["context_source"] == "supplied_embeddings"
        assert value["input_documents"] == count and value["memory_tokens"] == 16
        assert value["encoder_execution_identity"] == IDENTITY
        assert value["decoder_execution_identity"] == DECODER_IDENTITY
        assert value["memory_module_sha256"] == args.module_sha256
        assert value["finish_reason"] in {"eos", "length"}
        assert 1 <= len(value["token_ids"]) <= 3
        assert value["usage"]["completion_tokens"] == len(value["token_ids"])
        assert value["usage"]["encoder_query_tokens"] <= 256
        if args.input_token_limit:
            assert value["usage"]["prompt_tokens"] <= args.input_token_limit
    assert a1 == a2, (a1, a2)
    with ThreadPoolExecutor(max_workers=2) as pool:
        together = list(pool.map(generate, [body, b]))
    assert together == [a1, b1], together
    generate(body, {408, 504}, timeout_ms=1)
    recovered = generate(body)
    assert recovered == a1
    return {
        "passed": True,
        "scope": "live-http-native-embedding-to-supplied-feature-memory-generation",
        "base_url": args.base_url,
        "encoder_identity": IDENTITY,
        "decoder_identity": DECODER_IDENTITY,
        "memory_module_sha256": args.module_sha256,
        "encoder_query_document_recipe_equal": True,
        "a_prompt_tokens": a1["usage"]["prompt_tokens"],
        "a_completion_tokens": a1["usage"]["completion_tokens"],
        "b_document_count": b1["input_documents"],
        "a_b_a_equal": True,
        "concurrent_isolation": True,
        "deadline_recovery_equal": True,
        "workload_quality_evaluated": False,
        "events": events,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:18090")
    parser.add_argument("--profile", default="memorilla-stage3")
    parser.add_argument("--encoder", default="encoder")
    parser.add_argument("--decoder", default="decoder")
    parser.add_argument("--module-sha256", default=STAGE3_SHA256)
    parser.add_argument("--input-token-limit", type=int, default=0)
    parser.add_argument("--output-token-limit", type=int, default=0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run(args)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "events"}))


if __name__ == "__main__":
    main()
