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
import os
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.request import Request

from managed_api import authorization_headers, open_request, validate_backend


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
    temporary.replace(path)


def probe(base, route, payload, authorization=None):
    headers = {"Content-Type": "application/json"}
    if authorization:
        # This is a gateway identity token, never the backend Antfly key.
        headers["Authorization"] = authorization
    started = time.monotonic()
    try:
        with open_request(
            Request(
                base + route,
                None if payload is None else json.dumps(payload).encode(),
                headers,
            ),
            timeout=30,
        ) as response:
            raw = response.read(4194305)
            if len(raw) > 4194304:
                raise ValueError("Response too large")
            result = json.loads(raw)
            status = response.status
        return result, {"status": status, "seconds": time.monotonic() - started}
    except HTTPError as error:
        status = error.code
        error.close()
        return None, {"status": status, "seconds": time.monotonic() - started}
    except (OSError, ValueError) as error:
        return None, {
            "transport_error": type(error).__name__,
            "seconds": time.monotonic() - started,
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
        "--runtime-image",
        required=True,
        help="Operator-observed exact image@sha256:digest",
    )
    args = parser.parse_args()
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
    }
    previous = json.loads(args.resume_from.read_text()) if args.resume_from else None
    if previous and (
        previous.get("gateway") != base
        or previous.get("operator_reported_runtime") != args.runtime_image
        or not previous.get("smoke_passed")
    ):
        parser.error("Resume requires successful evidence for this gateway and image")
    save(args.output, record)
    for route in ("/health", "/ready"):
        result, evidence = probe(base, route, None, authorization)
        record["checks"].append({"route": route, **evidence})
        save(args.output, record)
        if not result or evidence.get("status") != 200:
            parser.exit(1, "Gateway health/readiness failed; evidence retained\n")
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
    record["smoke_passed"] = True
    record["completed_at"] = time.time()
    save(args.output, record)
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
