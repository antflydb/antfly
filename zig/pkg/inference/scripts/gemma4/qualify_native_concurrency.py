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

"""Mixed ordinary generation and native uploads on one real worker.

Hold a valid partial native upload, reject competing valid native prepare/resume
requests before their full checkpoint bodies arrive, run ordinary generation
under the retained upload, then reset the upload and restore without retry.
This is bounded request overlap, not simultaneous CUDA model execution.
"""

import argparse
import copy
import http.client
import json
from pathlib import Path
import re
import sys
import time

sys.path.insert(0, str(Path.cwd() / "zig/pkg/inference/scripts/gemma4"))
from native_checkpoint_transport import CONTENT_TYPE, envelope_prefix
from qualify_native_checkpoint_http import (
    REPO,
    canonical_choices,
    generate,
    git_provenance,
    requests,
    resume,
    server,
    sha256_file,
    validate_backend_configuration,
    validate_metal_memory,
)
from qualify_native_cancellation import AbortableRequest, observe_guard


# The designated 8K text fixture has 45,325 UTF-8 bytes and 16 output tokens.
# Node.estimateGenerateAdmissionUnitsFromShape charges 2 + bytes // 2048 +
# max_tokens // 256 = 24 units per request. Capacity 2 lets one request run
# through the admission clamp but cannot admit overlapping weighted work.
# Reserve both workloads explicitly; verify the actual worker counters below.
FIXTURE_ADMISSION_UNITS = 24
OVERLAP_ADMISSION_CAPACITY = 2 * FIXTURE_ADMISSION_UNITS
ADMISSION_FIELDS = (
    "capacity_units",
    "in_flight_units",
    "available_units",
    "in_flight_requests",
)


def parse_admission_metrics(text):
    result = {}
    for field in ADMISSION_FIELDS:
        pattern = r"^antfly_admission_inference_" + field + r" ([0-9]+)$"
        matches = re.findall(pattern, text, flags=re.MULTILINE)
        if len(matches) != 1:
            raise AssertionError(
                "missing, invalid or duplicate admission metric: " + field
            )
        result[field] = int(matches[0])
    if (
        result["capacity_units"]
        != result["in_flight_units"] + result["available_units"]
    ):
        raise AssertionError("incoherent admission metrics")
    return result


def observe_admission(args, case, phase, units, requests_count, timeout=5):
    """Require real weighted-accounting evidence, not merely configured capacity."""
    started = time.monotonic()
    observations = case.setdefault("admission_observations", [])
    while True:
        connection = http.client.HTTPConnection("127.0.0.1", args.port, timeout=2)
        try:
            connection.request("GET", "/ml/v1/metrics")
            response = connection.getresponse()
            if response.status != 200:
                raise AssertionError(f"admission metrics HTTP {response.status}")
            payload = response.read(1024 * 1024)
            if len(payload) == 1024 * 1024:
                raise AssertionError("metrics response exceeds qualification bound")
            row = parse_admission_metrics(payload.decode())
        finally:
            connection.close()
        elapsed = time.monotonic() - started
        observations.append(dict(row, phase=phase, elapsed_seconds=elapsed))
        if row["capacity_units"] != args.inference_admission_capacity:
            raise AssertionError(
                "worker did not apply qualification admission capacity"
            )
        if elapsed > timeout:
            raise TimeoutError("admission counters did not settle: " + phase)
        if (
            row["in_flight_units"] == units
            and row["in_flight_requests"] == requests_count
        ):
            return row
        time.sleep(0.025)


def competing_request(args, body, kind):
    connection = http.client.HTTPConnection("127.0.0.1", args.port, timeout=2)
    started = time.monotonic()
    sent = 0
    try:
        if kind == "prepare":
            payload = json.dumps(body).encode()
            sent = len(payload)
            connection.request(
                "POST",
                "/ai/v1/generate/context/checkpoint",
                payload,
                {"Content-Type": "application/json",
                 "X-Antfly-KV-Encoding": args.encoding},
            )
        elif kind == "resume":
            size = args.checkpoint.stat().st_size
            prefix = envelope_prefix(body, size)
            connection.putrequest("POST", "/ai/v1/generate/context/resume")
            connection.putheader("Content-Type", CONTENT_TYPE)
            connection.putheader("Content-Length", str(len(prefix) + size))
            connection.endheaders()
            connection.send(prefix)
            sent = len(prefix)
            # No checkpoint payload is sent. An adapter that buffers the full
            # request will time out here, instead of falsely passing rejection.
        else:
            raise ValueError(kind)
        response = connection.getresponse()
        payload = json.loads(response.read(65536))
        elapsed = time.monotonic() - started
        if response.status != 503 or payload.get("error") != "MODEL_RESOURCE_BUSY":
            raise AssertionError(f"competing {kind}: HTTP {response.status}: {payload}")
        if elapsed > 2:
            raise AssertionError("competing native request rejection exceeded 2s")
        return {
            "kind": kind,
            "status": response.status,
            "body": payload,
            "seconds": elapsed,
            "body_bytes_sent": sent,
            "checkpoint_payload_bytes_sent": 0,
        }
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--backend", choices=["cuda", "metal"], default="cuda")
    parser.add_argument(
        "--server-kind", choices=["inference", "standalone", "metal-test"], default="standalone"
    )
    parser.add_argument("--models-dir", type=Path, default=Path(".models"))
    parser.add_argument("--model", default="google/gemma-4-E4B-it-qat-q4_0-gguf")
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8518)
    parser.add_argument("--timeout", type=float, default=1800)
    parser.add_argument(
        "--encoding", choices=["f32", "f16", "fp8", "rotated4"], default="f16"
    )
    parser.add_argument("--experimental-rotated4", action="store_true")
    args = parser.parse_args()
    args.fixture = "fact"
    args.qualification_minimum_savings = 1.5 if args.backend == "metal" else None
    try:
        validate_backend_configuration(args)
    except ValueError as error:
        parser.error(str(error))
    args.inference_admission_capacity = OVERLAP_ADMISSION_CAPACITY
    if args.output.exists():
        raise FileExistsError(args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    evidence = {
        "passed": False,
        "production_ready": False,
        "scope": __doc__,
        "backend": args.backend,
        "server_kind": args.server_kind,
        "encoding": args.encoding,
        "source": git_provenance(REPO),
        "binary_sha256": sha256_file(args.server),
        "reference_sha256": sha256_file(args.reference),
        "checkpoint_sha256": sha256_file(args.checkpoint),
        "runner_sha256": sha256_file(Path(__file__)),
        "abort_helper_sha256": sha256_file(
            Path(__file__).with_name("qualify_native_cancellation.py")
        ),
        "overlap": {"passed": False},
        "competing_requests": [],
        "admission": {
            "capacity_units": OVERLAP_ADMISSION_CAPACITY,
            "expected_units_per_request": FIXTURE_ADMISSION_UNITS,
        },
    }

    def publish():
        temporary = args.output.with_suffix(".json.part")
        temporary.write_text(json.dumps(evidence, indent=2) + "\n")
        temporary.replace(args.output)

    try:
        reference = json.loads(args.reference.read_text())
        if not reference.get("passed"):
            raise AssertionError("reference campaign did not pass")
        prepared = next(
            r["result"]
            for r in reference["exporter"]["requests"]
            if r["kind"] == "prepare"
        )
        if (
            prepared["file_sha256"] != evidence["checkpoint_sha256"]
            or prepared["metadata"]["logical_tokens"] != 8189
            or prepared["metadata"].get("encoding", "f32") != args.encoding
        ):
            raise AssertionError("reference checkpoint mismatch")
        original = copy.deepcopy(
            next(iter(requests(args.model, "f16", [742], False, "fact")))
        )
        body = copy.deepcopy(original)
        body.pop("context_management")
        body.pop("cache_dtype")
        with server(args, "worker", evidence) as record:
            before = resume(args, body, record, prepared, "before_overlap")
            case = evidence["overlap"]
            held = AbortableRequest("127.0.0.1", args.port, args.timeout)
            try:
                observe_admission(args, case, "before_overlap", 0, 0)
                size = args.checkpoint.stat().st_size
                prefix = envelope_prefix(body, size)
                held.connection.putrequest("POST", "/ai/v1/generate/context/resume")
                held.connection.putheader("Content-Type", CONTENT_TYPE)
                held.connection.putheader("Content-Length", str(len(prefix) + size))
                held.connection.endheaders()
                held.connection.send(prefix)
                with args.checkpoint.open("rb") as source:
                    held.connection.send(source.read(64 * 1024))
                observe_guard(args, case, 503, 2)
                observe_admission(args, case, "held_upload", FIXTURE_ADMISSION_UNITS, 1)
                for kind in ("prepare", "resume"):
                    evidence["competing_requests"].append(
                        competing_request(args, body, kind)
                    )
                    publish()
                # Requiring a correct ordinary response proves progress while the
                # native transfer retains its admission and incomplete body.
                ordinary = generate(args, original, record)
                if canonical_choices(ordinary["choices"]) != canonical_choices(
                    before["choices"]
                ):
                    raise AssertionError(
                        "ordinary generation changed under native upload overlap"
                    )
                if (
                    ordinary["usage"]["prompt_tokens"] != 8189
                    or ordinary["usage"]["completion_tokens"]
                    != before["usage"]["completion_tokens"]
                ):
                    raise AssertionError("ordinary generation token accounting changed")
                observe_guard(args, case, 503, 2)
                observe_admission(
                    args,
                    case,
                    "ordinary_finished_upload_held",
                    FIXTURE_ADMISSION_UNITS,
                    1,
                )
                started = time.monotonic()
                held.abort()
                case["tcp_reset_sent"] = True
                observe_guard(args, case, 400, 5)
                case["reset_to_release_seconds"] = time.monotonic() - started
                if case["reset_to_release_seconds"] > 5:
                    raise AssertionError("overlap cleanup exceeded 5s")
                observe_admission(args, case, "after_upload_reset", 0, 0)
            finally:
                held.close()
            recovered = resume(args, body, record, prepared, "after_overlap")
            if (
                canonical_choices(recovered["choices"])
                != canonical_choices(before["choices"])
                or recovered["usage"] != before["usage"]
            ):
                raise AssertionError("post-overlap restore changed")
            observe_admission(args, case, "after_recovery", 0, 0)
            case["passed"] = True
            publish()
        memory = evidence["worker"]["memory"]
        if args.backend == "metal":
            validate_metal_memory(memory)
            if (not evidence["worker"].get("graceful_stop") or
                    evidence["worker"].get("remaining_group_members")):
                raise AssertionError("Metal concurrency worker did not drain")
        else:
            if memory["samples"] < 2 or memory["errors"] or not memory["gpu_uuids"]:
                raise AssertionError("incomplete memory sampling")
            if (not 0 < memory["peak_host_rss_bytes"] <= 8 * 1024**3 or
                    not 0 < memory["peak_gpu_memory_bytes"] <= 18 * 1024**3):
                raise AssertionError("overlapping requests exceeded memory cap")
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
