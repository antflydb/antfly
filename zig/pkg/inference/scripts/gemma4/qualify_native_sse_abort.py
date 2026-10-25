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

"""Abort a native checkpoint continuation after its first actual content delta.

First verify the appended request produces a long deterministic continuation.
Then reset that same stream while its native guard is held, require bounded
cleanup and immediately restore the immutable reference on the same worker.
"""

import argparse
import copy
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path.cwd() / "zig/pkg/inference/scripts/gemma4"))
from native_checkpoint_transport import send_resume
from qualify_native_checkpoint_http import (
    REPO,
    canonical_choices,
    git_provenance,
    read_response,
    requests,
    response_headers,
    resume,
    server,
    sha256_file,
)
from qualify_native_cancellation import AbortableRequest, observe_guard


def first_content_delta(response):
    for _ in range(256):
        raw = response.readline(65537)
        if not raw:
            raise AssertionError("SSE ended before content")
        if len(raw) > 65536:
            raise AssertionError("oversized SSE line")
        line = raw.decode().strip()
        if line.startswith("event:") and line[6:].strip() == "error":
            raise AssertionError("SSE error before content")
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            raise AssertionError("SSE completed before content")
        event = json.loads(data)
        if event.get("error"):
            raise AssertionError("SSE error before content")
        if event.get("usage") is not None:
            raise AssertionError("terminal usage preceded first content")
        choices = event.get("choices", [])
        if len(choices) > 1:
            raise AssertionError("unexpected multiple choices")
        for choice in choices:
            if choice.get("index", 0) != 0 or choice.get("finish_reason") is not None:
                raise AssertionError(
                    "stream already finished or returned another choice"
                )
            content = choice.get("delta", {}).get("content")
            if content is not None and not isinstance(content, str):
                raise AssertionError("invalid content delta")
            if content:
                return {"event": event, "content": content}
    raise AssertionError("too many SSE lines without content")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument(
        "--server-kind", choices=["inference", "standalone"], default="standalone"
    )
    parser.add_argument("--models-dir", type=Path, default=Path(".models"))
    parser.add_argument("--model", default="google/gemma-4-E4B-it-qat-q4_0-gguf")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8520)
    parser.add_argument("--timeout", type=float, default=1800)
    args = parser.parse_args()
    args.fixture = "fact"
    if args.output.exists():
        raise FileExistsError(args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    evidence = {
        "passed": False,
        "production_ready": False,
        "scope": __doc__,
        "source": git_provenance(REPO),
        "binary_sha256": sha256_file(args.server),
        "reference_sha256": sha256_file(args.reference),
        "checkpoint_sha256": sha256_file(args.checkpoint),
        "runner_sha256": sha256_file(Path(__file__)),
        "abort_helper_sha256": sha256_file(
            Path(__file__).with_name("qualify_native_cancellation.py")
        ),
        "abort": {"passed": False},
    }

    def publish():
        temporary = args.output.with_suffix(".json.part")
        temporary.write_text(json.dumps(evidence, indent=2) + "\n")
        temporary.replace(args.output)

    try:
        reference = json.loads(args.reference.read_text())
        if not reference.get("passed"):
            raise AssertionError("reference campaign did not pass")
        exports = [
            r["result"]
            for r in reference["exporter"]["requests"]
            if r["kind"] == "prepare"
        ]
        if len(exports) != 1:
            raise AssertionError("ambiguous reference")
        prepared = exports[0]
        if (
            prepared["file_sha256"] != evidence["checkpoint_sha256"]
            or prepared["metadata"]["logical_tokens"] != 8189
        ):
            raise AssertionError("reference checkpoint mismatch")
        body = copy.deepcopy(
            next(iter(requests(args.model, "f16", [742], False, "fact")))
        )
        body.pop("context_management")
        body.pop("cache_dtype")
        long_body = copy.deepcopy(body)
        long_body["messages"] += [
            {"role": "assistant", "content": "violet-729"},
            {
                "role": "user",
                "content": "Now write a detailed 1000-word explanation of how ants organize a colony. "
                "Include numbered sections on foraging, communication, division of labor, and maintaining the nest.",
            },
        ]
        long_body["max_tokens"] = 512
        long_body["stream"] = True
        evidence["long_continuation_request"] = long_body
        with server(args, "worker", evidence) as record:
            before = resume(args, body, record, prepared, "before_stream_abort")
            # This calibration proves the intended continuation is long; its
            # timing does not by itself qualify the subsequent cancellation.
            calibration_request = AbortableRequest("127.0.0.1", args.port, args.timeout)
            try:
                started = time.monotonic()
                response = send_resume(
                    calibration_request.connection,
                    "/ai/v1/generate/context/resume",
                    long_body,
                    args.checkpoint,
                )
                calibration_request.response = response
                if response.status != 200:
                    raise AssertionError(
                        f"long reference HTTP {response.status}: {response.read(65536)!r}"
                    )
                response_headers(response, record)
                result = read_response(response, True)
                evidence["long_reference"] = {
                    "seconds": time.monotonic() - started,
                    "result": result,
                }
                usage = result["usage"]
                if (
                    usage["cached_prompt_tokens"] != 8189
                    or not 0 < usage["prompt_tokens"] - 8189 <= 8192
                ):
                    raise AssertionError(
                        "long reference did not append to the saved prefix"
                    )
                if not 128 <= usage["completion_tokens"] <= 512:
                    raise AssertionError(
                        "long reference did not execute enough decode work"
                    )
            finally:
                calibration_request.close()
            publish()
            request = AbortableRequest("127.0.0.1", args.port, args.timeout)
            case = evidence["abort"]
            try:
                started = time.monotonic()
                response = send_resume(
                    request.connection,
                    "/ai/v1/generate/context/resume",
                    long_body,
                    args.checkpoint,
                )
                request.response = response
                if response.status != 200:
                    raise AssertionError(
                        f"abort request HTTP {response.status}: {response.read(65536)!r}"
                    )
                response_headers(response, record)
                if not response.getheader("Content-Type", "").startswith(
                    "text/event-stream"
                ):
                    raise AssertionError("abort request did not start SSE")
                first = first_content_delta(response)
                case.update(
                    first_content=first,
                    first_content_seconds=time.monotonic() - started,
                )
                expected = result["choices"][0]["message"]["content"]
                if not expected.startswith(first["content"]):
                    raise AssertionError(
                        "aborted continuation changed its first content"
                    )
                observe_guard(args, case, 503, 2)
                reset_started = time.monotonic()
                request.abort()
                case["tcp_reset_sent"] = True
                observe_guard(args, case, 400, 5)
                case["reset_to_released_seconds"] = time.monotonic() - reset_started
                if case["reset_to_released_seconds"] > 5:
                    raise AssertionError("SSE abort cleanup exceeded 5s")
            finally:
                request.close()
            recovered = resume(args, body, record, prepared, "after_stream_abort")
            if (
                canonical_choices(recovered["choices"])
                != canonical_choices(before["choices"])
                or recovered["usage"] != before["usage"]
            ):
                raise AssertionError("post-abort saved-prefix continuation changed")
            case["passed"] = True
            publish()
        memory = evidence["worker"]["memory"]
        if memory["samples"] < 2 or memory["errors"] or not memory["gpu_uuids"]:
            raise AssertionError("incomplete memory sampling")
        if (
            not 0 < memory["peak_host_rss_bytes"] <= 8 * 1024**3
            or not 0 < memory["peak_gpu_memory_bytes"] <= 18 * 1024**3
        ):
            raise AssertionError("SSE campaign exceeded memory cap")
        if git_provenance(REPO) != evidence["source"]:
            raise AssertionError("source changed")
        for path, key in [
            (args.server, "binary_sha256"),
            (args.reference, "reference_sha256"),
            (args.checkpoint, "checkpoint_sha256"),
            (Path(__file__), "runner_sha256"),
            (
                Path(__file__).with_name("qualify_native_cancellation.py"),
                "abort_helper_sha256",
            ),
        ]:
            if sha256_file(path) != evidence[key]:
                raise AssertionError(f"evidence changed: {path}")
        evidence["passed"] = True
    except Exception as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
    finally:
        publish()
    print(
        json.dumps({"passed": evidence["passed"], "error": evidence.get("error")}),
        flush=True,
    )
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
