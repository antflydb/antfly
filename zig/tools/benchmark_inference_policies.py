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

"""Compare identical fixtures across inference endpoints and OCR token policies.

Uses /ai/v1/read (image/audio) and /ai/v1/embeddings. Never changes database
producers, extraction checkpoints, model installations, or production routing.
Reports contain hashes and counters, not document text or credentials.
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
import time
import urllib.error
import urllib.parse
import urllib.request

from qualify_cpu_media import validate_read


MAX_RESPONSE = 16 * 1024 * 1024


def digest(value):
    return hashlib.sha256(value).hexdigest()


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def validate_endpoint(endpoint):
    parsed = urllib.parse.urlsplit(endpoint)
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1", "::1")
    ):
        raise ValueError("use HTTPS or an explicitly local port-forward")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("endpoint must not contain credentials, query or fragment")
    if not parsed.hostname or parsed.path.rstrip("/") != "/ai/v1":
        raise ValueError("endpoint must include the canonical /ai/v1 base path")


def request(endpoint, route, body, timeout):
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
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers,
    )
    opener = urllib.request.build_opener(NoRedirect())
    try:
        response = opener.open(req, timeout=timeout)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = response.read(MAX_RESPONSE + 1)
        if len(payload) > MAX_RESPONSE:
            raise ValueError("response exceeded 16 MiB")
        return response.code, json.loads(payload)


def prepare_case(case, root):
    if (
        not isinstance(case.get("id"), str)
        or not case["id"]
        or not re.fullmatch(r"[^@\s]+@[0-9a-f]{40,64}", case.get("model", ""))
    ):
        raise ValueError("each case requires an id and revision-pinned model")
    fixture = (root / case["fixture"]).resolve()
    if fixture.stat().st_size > 8 * 1024 * 1024:
        raise ValueError("fixture exceeds 8 MiB; use a bounded page/audio sample")
    raw = fixture.read_bytes()
    options = dict(case.get("options", {}))
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
        raise ValueError("options cannot override fixture identity or token policy")
    body = dict(options, model=case["model"])
    if case["task"] == "embed":
        body["input"] = [raw.decode("utf-8")]
    elif case["task"] == "read":
        if not case.get("expected") or not all(
            isinstance(item, str) and item.strip() for item in case["expected"]
        ):
            raise ValueError("read cases require nonempty expected text fragments")
        mime = {"image": "image/png", "audio": "audio/wav"}[case["kind"]]
        uri = "data:" + mime + ";base64," + base64.b64encode(raw).decode()
        body.update(
            {"images": [{"url": uri}]} if case["kind"] == "image" else {"audio": uri}
        )
    else:
        raise ValueError("task must be read or embed")
    return body, digest(raw)


def inspect_response(case, response):
    if case["task"] == "embed":
        rows = response.get("data", [])
        if len(rows) != 1:
            raise ValueError("expected exactly one embedding")
        vector = rows[0].get("embedding", [])
        if not vector or not all(
            isinstance(v, (int, float)) and math.isfinite(v) for v in vector
        ):
            raise ValueError("embedding must be finite and nonempty")
        if not math.isclose(sum(v * v for v in vector), 1, abs_tol=0.02):
            raise ValueError("embedding must have unit norm")
        if len(vector) != case.get("dimensions", 768):
            raise ValueError("embedding dimensions differ from the fixture contract")
        return {"complete": True, "dimensions": len(vector)}
    data = response.get("data", [])
    truncated = bool(response.get("truncated")) or any(
        item.get("truncated") or item.get("finish_reason") == "length" for item in data
    )
    if truncated:
        return {"complete": False, "truncated": True}
    text = validate_read(response, case["expected"])
    return {"complete": True, "text_sha256": digest(text.encode())}


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
        if not low or not high or not high.get("complete") or low.get("error_type"):
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


def save(path, report):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n")
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument(
        "--endpoint",
        action="append",
        required=True,
        help="label=https://host/ai/v1 or local port-forward",
    )
    parser.add_argument(
        "--hourly-usd",
        action="append",
        default=[],
        help="label=total provisioned hourly price",
    )
    parser.add_argument(
        "--runtime-image",
        required=True,
        help="verified serving OCI digest, shared by all endpoints",
    )
    parser.add_argument(
        "--backend",
        action="append",
        required=True,
        help="label=native or label=cuda; verified after each successful call",
    )
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    if not 1 <= args.trials <= 20 or not 0 < args.timeout <= 3600:
        parser.error("trials must be 1..20 and timeout must be in (0,3600]")
    if not re.fullmatch(r"[^@\s]+@sha256:[0-9a-f]{64}", args.runtime_image):
        parser.error("runtime-image must pin an OCI digest")
    endpoints = dict(item.split("=", 1) for item in args.endpoint)
    if len(endpoints) != len(args.endpoint):
        parser.error("endpoint labels must be unique")
    for endpoint in endpoints.values():
        validate_endpoint(endpoint)
    backends = dict(item.split("=", 1) for item in args.backend)
    if backends.keys() != endpoints.keys() or any(
        value not in ("native", "cuda") for value in backends.values()
    ):
        parser.error("supply exactly one native/cuda backend expectation per endpoint")
    hourly = {
        key: float(value)
        for key, value in (item.split("=", 1) for item in args.hourly_usd)
    }
    if any(
        key not in endpoints or not math.isfinite(value) or value < 0
        for key, value in hourly.items()
    ):
        parser.error("prices must be finite, nonnegative and name a supplied endpoint")
    cases = json.loads(args.cases.read_text())
    if not cases or len({case["id"] for case in cases}) != len(cases):
        parser.error("cases must be nonempty and have unique ids")
    prepared = [
        (case, *prepare_case(case, args.cases.resolve().parent)) for case in cases
    ]
    identity = dict(
        cases_sha256=digest(json.dumps(cases, sort_keys=True).encode()),
        runner_sha256=digest(Path(__file__).read_bytes()),
        read_contract_sha256=digest(
            Path(validate_read.__code__.co_filename).read_bytes()
        ),
        fixture_sha256=[sha for _, _, sha in prepared],
        endpoints=endpoints,
        backends=backends,
        runtime_image=args.runtime_image,
        trials=args.trials,
        timeout=args.timeout,
        hourly_usd=hourly,
        schema=1,
    )
    if args.report.exists():
        if not args.resume:
            parser.error(
                "report already exists; use --resume to retain measured checkpoints"
            )
        report = json.loads(args.report.read_text())
        if report["identity"] != identity:
            parser.error("resume inputs differ from the original campaign")
    else:
        report = dict(identity=identity, rows=[])
    args.report.parent.mkdir(parents=True, exist_ok=True)
    seen = {
        (r["case"], r["endpoint"], r["trial"], r["max_tokens"]) for r in report["rows"]
    }
    for case, body, sha in prepared:
        budgets = (
            [4096, 8192]
            if case["task"] == "read" and case["kind"] == "image"
            else [None]
        )
        for trial in range(-1, args.trials):
            order = list(endpoints)
            if trial % 2:
                order.reverse()
            for label in order:
                for tokens in budgets if trial % 2 == 0 else reversed(budgets):
                    key = (case["id"], label, trial, tokens)
                    if key in seen:
                        continue
                    payload = dict(body)
                    if tokens is not None:
                        payload["max_tokens"] = tokens
                    row = dict(
                        case=case["id"],
                        endpoint=label,
                        trial=trial,
                        max_tokens=tokens,
                        fixture_sha256=sha,
                        warmup=trial == -1,
                    )
                    started = time.monotonic()
                    try:
                        route = "/embeddings" if case["task"] == "embed" else "/read"
                        status, response = request(
                            endpoints[label], route, payload, args.timeout
                        )
                        row["seconds"] = time.monotonic() - started
                        row["status"] = status
                        if status != 200:
                            code = response.get("error", "HTTP_ERROR")
                            row.update(
                                complete=False,
                                error_code=code
                                if isinstance(code, str)
                                and code.replace("_", "").isalnum()
                                and len(code) < 80
                                else "HTTP_ERROR",
                            )
                            if code == "OUTPUT_TRUNCATED" and case["task"] == "read":
                                row["truncated"] = True
                        else:
                            row.update(inspect_response(case, response))
                            row["usage"] = {
                                key: value
                                for key, value in (response.get("usage") or {}).items()
                                if isinstance(value, (int, float))
                                and math.isfinite(value)
                            }
                        if status == 200 or row.get("truncated"):
                            code, instances = request(
                                endpoints[label],
                                "/models/instances",
                                None,
                                min(args.timeout, 30),
                            )
                            loaded = [
                                item
                                for item in instances.get("data", [])
                                if item.get("model") == case["model"]
                            ]
                            if (
                                code != 200
                                or len(loaded) != 1
                                or loaded[0].get("backend") != backends[label]
                            ):
                                raise ValueError(
                                    "pinned model backend was not confirmed"
                                )
                            row["backend"] = loaded[0]["backend"]
                    except Exception as error:
                        # Avoid exception messages that can contain response text, URLs or credentials.
                        row.update(complete=False, error_type=type(error).__name__)
                    row.setdefault("seconds", time.monotonic() - started)
                    report["rows"].append(row)
                    report["summary"] = summarize(report["rows"], hourly)
                    report["policy_comparison"] = compare_policies(report["rows"])
                    save(args.report, report)
                    print(json.dumps(row), flush=True)
    # A known lower-budget truncation is an expected measurement; all other
    # failures (including warmup) and every full-budget truncation fail the run.
    return int(
        any(
            not row.get("complete")
            and not (
                row.get("truncated")
                and row["max_tokens"] == 4096
                and "error_type" not in row
            )
            for row in report["rows"]
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
