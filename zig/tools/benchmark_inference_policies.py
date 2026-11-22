#!/usr/bin/env python3
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

"""Compare identical inference fixtures with private resumable campaign state.

No corpus mutation or model-management writes. Measured requests require a
fresh warmup and matching resident-model identity before and after execution.
"""

import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import inference_benchmark_state as state
from qualify_cpu_media import normalize

MAX_RESPONSE = 16 * 1024 * 1024
MAX_FIXTURE = 8 * 1024 * 1024
DEFAULT_TOLERANCES = dict(min_cosine=0.9999, max_abs_error=0.001, max_rmse=0.0001)


class BenchmarkError(ValueError):
    """Only fixed diagnostic codes; never response text or fixture contents."""


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def validate_endpoint(endpoint):
    parsed = urllib.parse.urlsplit(endpoint)
    local = parsed.scheme == "http" and parsed.hostname in (
        "localhost",
        "127.0.0.1",
        "::1",
    )
    if parsed.scheme != "https" and not local:
        raise BenchmarkError("ENDPOINT_REQUIRES_HTTPS_OR_LOCAL_FORWARD")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise BenchmarkError("ENDPOINT_MUST_NOT_CONTAIN_CREDENTIALS_QUERY_OR_FRAGMENT")
    if not parsed.hostname or parsed.path.rstrip("/") != "/ai/v1":
        raise BenchmarkError("ENDPOINT_REQUIRES_CANONICAL_AI_V1_PATH")


def request(endpoint, route, body, timeout, response_grace=30):
    validate_endpoint(endpoint)
    headers = {
        "Content-Type": "application/json",
        "X-Antfly-Inference-Timeout-Ms": str(max(1, min(3600000, int(timeout * 1000)))),
    }
    token = os.environ.get("ANTFLY_INFERENCE_TOKEN")
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(
        endpoint.rstrip("/") + route,
        data=state.canonical(body) if body is not None else None,
        headers=headers,
    )
    try:
        response = urllib.request.build_opener(NoRedirect()).open(
            req, timeout=timeout + response_grace
        )
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read(MAX_RESPONSE + 1)
        if len(raw) > MAX_RESPONSE:
            raise BenchmarkError("RESPONSE_EXCEEDS_16_MIB")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise BenchmarkError("RESPONSE_MUST_BE_OBJECT")
        return response.code, payload


def validate_case(case):
    if (
        not isinstance(case, dict)
        or not isinstance(case.get("id"), str)
        or not case["id"]
    ):
        raise BenchmarkError("CASE_REQUIRES_STRING_ID")
    if not isinstance(case.get("model"), str) or not re.fullmatch(
        r"[^@\s]+@[0-9a-f]{40,64}", case["model"]
    ):
        raise BenchmarkError("CASE_REQUIRES_REVISION_PINNED_MODEL")
    if not isinstance(case.get("fixture"), str) or not case["fixture"]:
        raise BenchmarkError("CASE_REQUIRES_FIXTURE_PATH")
    if case.get("task") not in ("read", "embed"):
        raise BenchmarkError("CASE_TASK_MUST_BE_READ_OR_EMBED")
    options = case.get("options", {})
    if not isinstance(options, dict):
        raise BenchmarkError("OPTIONS_MUST_BE_OBJECT")
    if any(
        key in options
        for key in (
            "model",
            "images",
            "audio",
            "inputs",
            "input",
            "documents",
            "content",
            "url",
            "max_tokens",
        )
    ):
        raise BenchmarkError("OPTIONS_CANNOT_OVERRIDE_IDENTITY_OR_TOKEN_POLICY")
    if case["task"] == "read":
        if case.get("kind") not in ("image", "audio"):
            raise BenchmarkError("READ_KIND_MUST_BE_IMAGE_OR_AUDIO")
        expected = case.get("expected")
        if (
            not isinstance(expected, list)
            or not expected
            or not all(isinstance(item, str) and item.strip() for item in expected)
        ):
            raise BenchmarkError("READ_REQUIRES_EXPECTED_FRAGMENT_ARRAY")
    else:
        dimensions = case.get("dimensions", 768)
        if type(dimensions) is not int or not 1 <= dimensions <= 65536:
            raise BenchmarkError("INVALID_EMBEDDING_DIMENSIONS")


def fixture_hash(path):
    """Bounded streaming pass: campaign identity never retains payloads."""
    hasher = hashlib.sha256()
    count = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            count += len(chunk)
            if count > MAX_FIXTURE:
                raise BenchmarkError("FIXTURE_EXCEEDS_8_MIB")
            hasher.update(chunk)
    return hasher.hexdigest()


def prepare_case(case, root, expected_hash=None):
    validate_case(case)
    with (root / case["fixture"]).open("rb") as stream:
        raw = stream.read(MAX_FIXTURE + 1)
    if len(raw) > MAX_FIXTURE:
        raise BenchmarkError("FIXTURE_EXCEEDS_8_MIB")
    sha = digest(raw)
    if expected_hash is not None and sha != expected_hash:
        raise BenchmarkError("FIXTURE_CHANGED_DURING_CAMPAIGN")
    body = dict(case.get("options", {}), model=case["model"])
    if case["task"] == "embed":
        body["input"] = [raw.decode("utf-8")]
    else:
        mime = {"image": "image/png", "audio": "audio/wav"}[case["kind"]]
        uri = "data:" + mime + ";base64," + base64.b64encode(raw).decode()
        body.update(
            {"images": [{"url": uri}]} if case["kind"] == "image" else {"audio": uri}
        )
    return body, sha


def embedding_vector(case, response):
    data = response.get("data")
    if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
        raise BenchmarkError("EMBEDDING_REQUIRES_ONE_RESULT")
    vector = data[0].get("embedding")
    if not isinstance(vector, list) or len(vector) != case.get("dimensions", 768):
        raise BenchmarkError("EMBEDDING_DIMENSIONS_MISMATCH")
    if not vector or not all(
        type(v) in (int, float) and math.isfinite(v) for v in vector
    ):
        raise BenchmarkError("EMBEDDING_MUST_BE_FINITE")
    if not math.isclose(math.fsum(v * v for v in vector), 1, abs_tol=0.02):
        raise BenchmarkError("EMBEDDING_MUST_HAVE_UNIT_NORM")
    return vector


def compare_embedding(vector, reference, tolerances):
    if len(vector) != len(reference):
        raise BenchmarkError("REFERENCE_DIMENSIONS_MISMATCH")
    norm = math.sqrt(
        math.fsum(v * v for v in vector) * math.fsum(v * v for v in reference)
    )
    if norm == 0:
        raise BenchmarkError("REFERENCE_VECTOR_HAS_ZERO_NORM")
    cosine = max(
        -1.0, min(1.0, math.fsum(a * b for a, b in zip(vector, reference)) / norm)
    )
    errors = [abs(a - b) for a, b in zip(vector, reference)]
    rmse = math.sqrt(math.fsum(e * e for e in errors) / len(errors))
    maximum = max(errors)
    return dict(
        reference_sha256=state.checksum(reference),
        cosine=cosine,
        max_abs_error=maximum,
        rmse=rmse,
        passed=cosine >= tolerances["min_cosine"]
        and maximum <= tolerances["max_abs_error"]
        and rmse <= tolerances["max_rmse"],
    )


def inspect_response(case, response):
    if case["task"] == "embed":
        vector = embedding_vector(case, response)
        return {"complete": True, "dimensions": len(vector)}
    data = response.get("data")
    if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
        raise BenchmarkError("READ_REQUIRES_ONE_RESULT")
    document = response.get("document")
    if document is not None and not isinstance(document, dict):
        raise BenchmarkError("INVALID_DOCUMENT_STATUS")
    # An independent failure must never be hidden by a truncation elsewhere.
    if response.get("error") or data[0].get("error"):
        raise BenchmarkError("READ_ITEM_ERROR")
    statuses = [data[0].get("status")]
    if document is not None:
        statuses.append(document.get("status"))
    if "unresolved" in statuses:
        raise BenchmarkError("READ_UNRESOLVED")
    if any(status not in ("complete", "truncated") for status in statuses):
        raise BenchmarkError("READ_INVALID_COMPLETION_STATUS")
    truncated = (
        "truncated" in statuses
        or bool(response.get("truncated"))
        or bool(data[0].get("truncated"))
        or data[0].get("finish_reason") == "length"
    )
    if truncated:
        return {"complete": False, "truncated": True}
    text = data[0].get("text")
    if not isinstance(text, str) or not text.strip():
        raise BenchmarkError("READ_EMPTY_TEXT")
    text = normalize(text)
    if any(normalize(fragment) not in text for fragment in case["expected"]):
        raise BenchmarkError("READ_EXPECTED_TEXT_MISSING")
    return {"complete": True, "text_sha256": digest(text.encode())}


def resident_instance(args, endpoint, model):
    code, response = request(
        args.endpoints[endpoint],
        "/models/instances",
        None,
        min(args.timeout, 30),
        args.response_grace,
    )
    data = response.get("data")
    if code != 200 or not isinstance(data, list):
        raise BenchmarkError("MODEL_INSTANCE_INSPECTION_FAILED")
    loaded = [
        item for item in data if isinstance(item, dict) and item.get("model") == model
    ]
    if not loaded:
        return None
    if len(loaded) != 1 or loaded[0].get("backend") != args.backends[endpoint]:
        raise BenchmarkError("MODEL_BACKEND_MISMATCH")
    if (
        loaded[0].get("state") != "loaded"
        or not isinstance(loaded[0].get("id"), str)
        or not loaded[0]["id"]
    ):
        return None
    return loaded[0]["id"]


def invoke(args, case, endpoint, body, tokens, trial, warmup, expected_instance=None):
    row = dict(
        case=case["id"],
        endpoint=endpoint,
        trial=trial,
        max_tokens=tokens,
        warmup=warmup,
        complete=False,
    )
    payload = dict(body)
    if tokens is not None:
        payload["max_tokens"] = tokens
    vector = None
    started = time.monotonic()
    try:
        route = "/embeddings" if case["task"] == "embed" else "/read"
        status, response = request(
            args.endpoints[endpoint], route, payload, args.timeout, args.response_grace
        )
        row.update(seconds=time.monotonic() - started, status=status)
        usage = response.get("usage")
        if isinstance(usage, dict):
            row["usage"] = {
                key: value
                for key, value in usage.items()
                if type(value) in (int, float) and math.isfinite(value)
            }
        if status == 200:
            row.update(inspect_response(case, response))
            if case["task"] == "embed":
                vector = embedding_vector(case, response)
        elif (
            status in (400, 422)
            and response.get("error") == "OUTPUT_TRUNCATED"
            and case["task"] == "read"
        ):
            row["truncated"] = True
        else:
            # Do not persist arbitrary server diagnostic strings.
            raise BenchmarkError("INFERENCE_HTTP_FAILURE")
        instance = resident_instance(args, endpoint, case["model"])
        if instance is None:
            raise BenchmarkError("MODEL_NOT_RESIDENT_AFTER_REQUEST")
        if expected_instance is not None and instance != expected_instance:
            raise BenchmarkError("MODEL_CHANGED_DURING_SAMPLE")
        row.update(instance_id=instance, backend=args.backends[endpoint])
    except Exception as error:
        row.update(
            complete=False,
            error_code=str(error)
            if isinstance(error, BenchmarkError)
            else "REQUEST_" + type(error).__name__,
        )
        vector = None
    row.setdefault("seconds", time.monotonic() - started)
    return row, vector


def acceptable(row):
    return (
        not row.get("error_code")
        and bool(row.get("backend"))
        and (row.get("complete") or row.get("truncated") and row["max_tokens"] == 4096)
    )


def policy_budgets(case):
    return (
        [4096, 8192] if case["task"] == "read" and case["kind"] == "image" else [None]
    )


def record_result(args, store, case, endpoint, row, vector, sha):
    row["fixture_sha256"] = sha
    if vector is not None:
        if case["id"] not in store.references:
            if endpoint != args.reference_endpoint or not row["warmup"]:
                raise BenchmarkError("EMBEDDING_REFERENCE_UNAVAILABLE")
            store.reference(case["id"], endpoint, vector)
        reference = store.reference_vector(case["id"])
        # Revalidate private journal references against this case's contract.
        embedding_vector(case, {"data": [{"embedding": reference}]})
        comparison = compare_embedding(vector, reference, args.tolerances)
        row["embedding_comparison"] = comparison
        if not comparison["passed"]:
            row.update(complete=False, error_code="EMBEDDING_REFERENCE_MISMATCH")
    store.record(row)
    print(json.dumps(dict(row, epoch=store.epoch), allow_nan=False), flush=True)


def warm_endpoint(args, store, case, endpoint, body, budgets, sha, reason):
    instance = None
    for tokens in budgets:
        row, vector = invoke(args, case, endpoint, body, tokens, None, True)
        row["warmup_reason"] = reason
        record_result(args, store, case, endpoint, row, vector, sha)
        if row.get("error_code"):
            return None
        instance = row.get("instance_id")
    return instance


def run_campaign(args, store, cases, hashes):
    root = args.cases.resolve().parent
    for case, sha in zip(cases, hashes):
        budgets = policy_budgets(case)
        pending = {
            endpoint: [
                tokens
                for tokens in budgets
                if any(
                    (case["id"], endpoint, trial, tokens) not in store.samples
                    for trial in range(args.trials)
                )
            ]
            for endpoint in args.endpoints
        }
        pending = {key: value for key, value in pending.items() if value}
        if not pending:
            continue
        # Materialize only one fixture, after all campaign identities are hashed.
        body, _ = prepare_case(case, root, sha)
        order = list(pending)
        if case["task"] == "embed" and case["id"] not in store.references:
            order.remove(args.reference_endpoint)
            order.insert(0, args.reference_endpoint)
        resident = {}
        for endpoint in order:
            if (
                case["task"] == "embed"
                and endpoint != args.reference_endpoint
                and case["id"] not in store.references
            ):
                continue
            resident[endpoint] = warm_endpoint(
                args,
                store,
                case,
                endpoint,
                body,
                pending[endpoint],
                sha,
                "new_execution_epoch",
            )
        for trial in range(args.trials):
            order = list(pending)
            if trial % 2:
                order.reverse()
            for endpoint in order:
                if not resident.get(endpoint):
                    continue
                for tokens in budgets if trial % 2 == 0 else reversed(budgets):
                    key = (case["id"], endpoint, trial, tokens)
                    if key in store.samples:
                        continue
                    try:
                        current = resident_instance(args, endpoint, case["model"])
                        if current != resident[endpoint]:
                            current = warm_endpoint(
                                args,
                                store,
                                case,
                                endpoint,
                                body,
                                [tokens],
                                sha,
                                "resident_model_changed",
                            )
                        if (
                            current is None
                            or resident_instance(args, endpoint, case["model"])
                            != current
                        ):
                            raise BenchmarkError("RESIDENCY_UNSTABLE_BEFORE_SAMPLE")
                        resident[endpoint] = current
                        row, vector = invoke(
                            args, case, endpoint, body, tokens, trial, False, current
                        )
                    except Exception as error:
                        row, vector = (
                            dict(
                                case=case["id"],
                                endpoint=endpoint,
                                trial=trial,
                                max_tokens=tokens,
                                warmup=False,
                                complete=False,
                                seconds=0,
                                error_code=str(error)
                                if isinstance(error, BenchmarkError)
                                else "RESIDENCY_" + type(error).__name__,
                            ),
                            None,
                        )
                    record_result(args, store, case, endpoint, row, vector, sha)
        del body


def summarize(rows, hourly):
    """Charge all measured work, including failed/truncated attempts."""
    groups = {}
    for row in rows:
        if row.get("warmup"):
            continue
        key = (row["endpoint"], row["case"], row["max_tokens"])
        groups.setdefault(key, []).append(row)
    result = []
    for (endpoint, case, tokens), items in sorted(
        groups.items(), key=lambda kv: str(kv[0])
    ):
        complete = [item for item in items if item.get("complete")]
        seconds = sum(item["seconds"] for item in items)
        entry = dict(
            endpoint=endpoint,
            case=case,
            max_tokens=tokens,
            calls=len(items),
            complete=len(complete),
            truncated=sum(bool(item.get("truncated")) for item in items),
            seconds=seconds,
            median_complete_seconds=statistics.median(
                item["seconds"] for item in complete
            )
            if complete
            else None,
            completed_per_hour=3600 * len(complete) / seconds if seconds else None,
        )
        entry["usd_per_complete"] = (
            seconds / 3600 * hourly[endpoint] / len(complete)
            if complete and endpoint in hourly
            else None
        )
        result.append(entry)
    return result


def compare_policies(rows):
    pairs = {}
    for row in rows:
        if row.get("warmup") or row["max_tokens"] not in (4096, 8192):
            continue
        pairs.setdefault((row["endpoint"], row["case"], row["trial"]), {})[
            row["max_tokens"]
        ] = row
    result = []
    for (endpoint, case, trial), pair in sorted(pairs.items()):
        low, high = pair.get(4096), pair.get(8192)
        if (
            not low
            or not high
            or not high.get("complete")
            or not acceptable(low)
            or not acceptable(high)
        ):
            continue
        if low.get("epoch") != high.get("epoch") or low.get("instance_id") != high.get(
            "instance_id"
        ):
            continue
        if not low.get("complete") and not low.get("truncated"):
            continue
        retry = bool(low.get("truncated"))
        old_seconds = low["seconds"] + (high["seconds"] if retry else 0)
        result.append(
            dict(
                endpoint=endpoint,
                case=case,
                trial=trial,
                old_policy_calls=2 if retry else 1,
                new_policy_calls=1,
                old_policy_seconds=old_seconds,
                new_policy_seconds=high["seconds"],
                estimated_speedup=old_seconds / high["seconds"],
            )
        )
    return result


def named_values(parser, values, field):
    result = {}
    for entry in values:
        key, separator, value = entry.partition("=")
        if (
            not separator
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", key)
            or not value
            or key in result
        ):
            parser.error("invalid or duplicate " + field + " label=value")
        result[key] = value
    return result


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument(
        "--endpoint",
        action="append",
        required=True,
        help="label=https://host/ai/v1 or local forward",
    )
    parser.add_argument(
        "--backend", action="append", required=True, help="label=native or label=cuda"
    )
    parser.add_argument("--hourly-usd", action="append", default=[])
    parser.add_argument(
        "--runtime-image",
        required=True,
        help="operator-verified serving OCI index digest",
    )
    parser.add_argument(
        "--reference-endpoint",
        help="trusted embedding baseline; required for embedding cases",
    )
    parser.add_argument(
        "--embedding-min-cosine", type=float, default=DEFAULT_TOLERANCES["min_cosine"]
    )
    parser.add_argument(
        "--embedding-max-abs-error",
        type=float,
        default=DEFAULT_TOLERANCES["max_abs_error"],
    )
    parser.add_argument(
        "--embedding-max-rmse", type=float, default=DEFAULT_TOLERANCES["max_rmse"]
    )
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument(
        "--response-grace",
        type=float,
        default=30,
        help="socket cleanup allowance; does not extend inference deadline",
    )
    args = parser.parse_args()
    if (
        not 1 <= args.trials <= 20
        or not math.isfinite(args.timeout)
        or not 0 < args.timeout <= 3600
        or not math.isfinite(args.response_grace)
        or not 0 <= args.response_grace <= 60
    ):
        parser.error("invalid trials, timeout or response-grace budget")
    if not re.fullmatch(r"[^@\s]+@sha256:[0-9a-f]{64}", args.runtime_image):
        parser.error("runtime-image must pin an OCI digest")
    args.endpoints = named_values(parser, args.endpoint, "endpoint")
    for endpoint in args.endpoints.values():
        validate_endpoint(endpoint)
    args.backends = named_values(parser, args.backend, "backend")
    if args.backends.keys() != args.endpoints.keys() or any(
        value not in ("native", "cuda") for value in args.backends.values()
    ):
        parser.error("supply one native/cuda backend expectation per endpoint")
    try:
        args.hourly = {
            key: float(value)
            for key, value in named_values(parser, args.hourly_usd, "price").items()
        }
    except ValueError:
        parser.error("prices must be numbers")
    if any(
        key not in args.endpoints or not math.isfinite(value) or value < 0
        for key, value in args.hourly.items()
    ):
        parser.error("prices must be finite, nonnegative and name supplied endpoints")
    args.tolerances = dict(
        min_cosine=args.embedding_min_cosine,
        max_abs_error=args.embedding_max_abs_error,
        max_rmse=args.embedding_max_rmse,
    )
    if (
        not all(math.isfinite(value) for value in args.tolerances.values())
        or not 0 <= args.tolerances["min_cosine"] <= 1
        or not 0 <= args.tolerances["max_abs_error"] <= 2
        or not 0 <= args.tolerances["max_rmse"] <= 2
    ):
        parser.error("invalid embedding comparison tolerances")
    args.report = args.report.resolve()
    return args


def main():
    args = parse_args()
    cases = json.loads(args.cases.read_text())
    if not isinstance(cases, list) or not cases:
        raise BenchmarkError("CASES_MUST_BE_NONEMPTY_ARRAY")
    for case in cases:
        validate_case(case)
    if len({case["id"] for case in cases}) != len(cases):
        raise BenchmarkError("CASE_IDS_MUST_BE_UNIQUE")
    if (
        any(case["task"] == "embed" for case in cases)
        and args.reference_endpoint not in args.endpoints
    ):
        raise BenchmarkError("EMBEDDING_CASES_REQUIRE_REFERENCE_ENDPOINT")
    hashes = [
        fixture_hash(args.cases.resolve().parent / case["fixture"]) for case in cases
    ]
    identity = dict(
        schema=2,
        python_version=list(sys.version_info[:3]),
        cases_sha256=state.checksum(cases),
        fixture_inventory_sha256=state.checksum(hashes),
        runner_sha256=digest(Path(__file__).read_bytes()),
        state_contract_sha256=digest(Path(state.__file__).read_bytes()),
        normalization_sha256=digest(Path(normalize.__code__.co_filename).read_bytes()),
        endpoints=args.endpoints,
        backends=args.backends,
        runtime_image=args.runtime_image,
        reference_endpoint=args.reference_endpoint,
        embedding_tolerances=args.tolerances,
        trials=args.trials,
        timeout=args.timeout,
        response_grace=args.response_grace,
        hourly_usd=args.hourly,
    )
    planned = sum(
        len(policy_budgets(case)) * len(args.endpoints) * args.trials for case in cases
    )
    planned_pairs = sum(
        len(args.endpoints) * args.trials
        for case in cases
        if case["task"] == "read" and case["kind"] == "image"
    )
    with state.CampaignStore(args.report, identity, args.resume) as store:
        succeeded = False
        try:
            run_campaign(args, store, cases, hashes)
            succeeded = len(store.samples) == planned and all(
                acceptable(row) for row in store.rows
            )
        finally:
            pairs = compare_policies(store.rows)
            report = dict(
                identity=identity,
                success=succeeded,
                rows=store.rows,
                coverage=dict(
                    planned_samples=planned,
                    recorded_samples=len(store.samples),
                    missing_samples=planned - len(store.samples),
                    planned_policy_pairs=planned_pairs,
                    matched_policy_pairs=len(pairs),
                ),
                epochs=len(store.epochs),
                references={
                    key: dict(
                        endpoint=value["endpoint"],
                        dimensions=value["dimensions"],
                        sha256=value["sha256"],
                    )
                    for key, value in store.references.items()
                },
                summary=summarize(store.rows, args.hourly),
                policy_comparison=pairs,
                policy_comparison_complete=len(pairs) == planned_pairs,
                warnings=[]
                if len(pairs) == planned_pairs
                else ["POLICY_COMPARISON_INCOMPLETE"],
            )
            store.snapshot(report)
    return 0 if succeeded else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (BenchmarkError, state.StateError) as error:
        print(json.dumps({"error_code": str(error)}), file=sys.stderr)
        raise SystemExit(2)
