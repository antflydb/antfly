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

"""Bounded gateway smoke/cursor evidence, before and after an operator restart.

This does not restart infrastructure or establish archive-scale qualification.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time
from urllib.error import HTTPError
from urllib.request import Request

from managed_api import (
    authorization_headers,
    correlation_evidence,
    open_request,
    validate_backend,
)


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
    temporary.replace(path)


def timing_evidence(headers, elapsed):
    # Accept only our numeric metric, never retain raw response/auth headers.
    value = headers.get("Server-Timing", "") if headers else ""
    match = re.fullmatch(r"antfly_upstream;dur=([0-9]+(?:\.[0-9]+)?)", value)
    if match:
        seconds = float(match[1]) / 1000
        if math.isfinite(seconds) and 0 <= seconds <= elapsed + 0.001:
            return {
                "backend_seconds": seconds,
                "outside_backend_seconds": max(0, elapsed - seconds),
            }
    return {}


def probe(base, route, payload, authorization=None):
    headers = {"Content-Type": "application/json"}
    if authorization:
        # This is a gateway identity token, never the backend Antfly key.
        headers["Authorization"] = authorization
    started = time.monotonic()
    started_at = time.time()
    try:
        with open_request(
            Request(
                base + route,
                None if payload is None else json.dumps(payload).encode(),
                headers,
            ),
            timeout=30,
        ) as response:
            headers_seconds = time.monotonic() - started
            raw = response.read(4194305)
            if len(raw) > 4194304:
                raise ValueError("Response too large")
            result = json.loads(raw)
            status = response.status
            body_finished = time.monotonic()
            correlation = correlation_evidence(response.headers)
            closing = time.monotonic()
        finished = time.monotonic()
        elapsed = finished - started
        timing = timing_evidence(response.headers, elapsed)
        return result, {
            "status": status,
            "seconds": elapsed,
            "started_at": started_at,
            "headers_seconds": headers_seconds,
            "body_seconds": body_finished - started - headers_seconds,
            "close_seconds": finished - closing,
            **timing,
            **correlation,
        }
    except HTTPError as error:
        status = error.code
        headers_seconds = time.monotonic() - started
        correlation = correlation_evidence(error.headers)
        closing = time.monotonic()
        error.close()
        finished = time.monotonic()
        elapsed = finished - started
        timing = timing_evidence(error.headers, elapsed)
        return None, {
            "status": status,
            "seconds": elapsed,
            "started_at": started_at,
            "headers_seconds": headers_seconds,
            "close_seconds": finished - closing,
            **timing,
            **correlation,
        }
    except (OSError, ValueError) as error:
        return None, {
            "transport_error": type(error).__name__,
            "seconds": time.monotonic() - started,
            "started_at": started_at,
        }


def identities(result):
    ids = [str(hit["_source"]["hn_id"]) for hit in result["hits"]["hits"]]
    if (
        not ids
        or len(ids) != len(set(ids))
        or any(not value.isdecimal() for value in ids)
    ):
        raise ValueError("Expected nonempty, unique HN results")
    return ids


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--gateway", required=True, help="HTTPS service or local authenticated proxy"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--query", default="database")
    parser.add_argument("--resume-from", type=Path)
    parser.add_argument(
        "--search-first",
        action="store_true",
        help="Search before health/readiness so those probes do not warm the gateway",
    )
    parser.add_argument(
        "--max-search-seconds",
        type=float,
        help="Fail a separate latency gate if any observed search exceeds this duration; requests are never retried",
    )
    parser.add_argument(
        "--runtime-image",
        required=True,
        help="Operator-observed exact image@sha256:digest",
    )
    args = parser.parse_args()
    if args.max_search_seconds is not None and (
        not math.isfinite(args.max_search_seconds) or args.max_search_seconds <= 0
    ):
        parser.error("Search latency limit must be finite and positive")
    authorization = authorization_headers(
        {"ANTFLY_AUTHORIZATION": os.environ.get("HN_GATEWAY_AUTHORIZATION", "")}
    ).get("Authorization")
    base = args.gateway.rstrip("/")
    validate_backend(base + "/db/v1", bool(authorization))
    if "@sha256:" not in args.runtime_image or args.output.exists():
        parser.error("Use a digest-pinned image and a new output file")
    record = {
        "version": 1,
        "gateway": base,
        "operator_reported_runtime": args.runtime_image,
        "started_at": time.time(),
        "phase": "resume" if args.resume_from else "capture",
        "checks": [],
        "smoke_passed": False,
        "archive_qualified": False,
        "search_first": args.search_first,
        "max_search_seconds": args.max_search_seconds,
    }
    previous = json.loads(args.resume_from.read_text()) if args.resume_from else None
    if previous and (
        previous.get("gateway") != base
        or previous.get("operator_reported_runtime") != args.runtime_image
        or not previous.get("smoke_passed")
    ):
        parser.error("Resume requires successful evidence for this gateway and image")
    save(args.output, record)

    def health_checks():
        for route in ("/health", "/ready"):
            result, evidence = probe(base, route, None, authorization)
            record["checks"].append({"route": route, **evidence})
            save(args.output, record)
            if not result or evidence.get("status") != 200:
                parser.exit(1, "Gateway health/readiness failed; evidence retained\n")

    if not args.search_first:
        health_checks()
    originals = (
        {entry["view"]: entry for entry in previous["checks"] if "view" in entry}
        if previous
        else {}
    )
    for view in ("history", "recent", "all"):
        payload = {"q": args.query, "view": view}
        if previous:
            original = originals[view]
            payload = {**original["request"], "cursor": original["cursor"]}
            if original.get("recent_since") is not None:
                payload["recent_since"] = original["recent_since"]
        result, evidence = probe(base, "/api/search", payload, authorization)
        entry = {"view": view, "request": payload, **evidence}
        if args.max_search_seconds is not None:
            entry["latency_passed"] = evidence["seconds"] <= args.max_search_seconds
        record["checks"].append(entry)
        # Persist transport/HTTP failures before inspecting their payload.
        save(args.output, record)
        if not result or evidence.get("status") != 200:
            parser.exit(1, "Search failed; evidence retained\n")
        try:
            ids = identities(result)
            if previous and set(ids) & set(originals[view]["ids"]):
                raise ValueError("Cursor repeated rows")
            cursor = result.get("cursor")
            if not previous and not cursor:
                raise ValueError(
                    "Use a term with enough results to qualify continuation"
                )
            entry.update(
                ids=ids,
                cursor=cursor,
                recent_since=result.get("recent_since"),
                result_sha256=hashlib.sha256(
                    json.dumps(result, sort_keys=True).encode()
                ).hexdigest(),
            )
            save(args.output, record)
        except (ValueError, KeyError, TypeError) as error:
            entry["validation_error"] = str(error)
            save(args.output, record)
            parser.exit(1, "Search validation failed; evidence retained\n")
        time.sleep(0.6)  # Stay below the gateway's default aggregate admission rate.
    if args.search_first:
        health_checks()
    record["smoke_passed"] = True
    if args.max_search_seconds is not None:
        record["latency_passed"] = all(
            entry["latency_passed"] for entry in record["checks"] if "view" in entry
        )
    record["completed_at"] = time.time()
    save(args.output, record)
    if record.get("latency_passed") is False:
        parser.exit(
            1,
            "Functional smoke passed; search latency gate failed; evidence retained\n",
        )
    print(
        json.dumps(
            {
                "evidence": str(args.output),
                "smoke_passed": True,
                "archive_qualified": False,
            }
        )
    )


if __name__ == "__main__":
    main()
