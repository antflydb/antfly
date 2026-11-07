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

"""Bounded availability retries for read-only privacy findings checks."""

import math
import time

import requests


def read_findings_page(
    session: requests.Session,
    privacy_url: str,
    payload: dict,
    *,
    deadline: float,
    method: str = "POST",
) -> dict:
    """Read one page without changing its cursor or replaying a mutation.

    The caller shares one monotonic deadline across the entire pagination check.
    Only typed privacy availability responses are retried; successful page
    contents and all pagination assertions remain the caller's responsibility.
    """
    if method not in ("GET", "POST"):
        raise ValueError("Findings reads support only GET and POST")
    if not math.isfinite(deadline):
        raise ValueError("Findings deadline must be finite")
    backoff = 0.2
    last_problem = "no response"
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"Findings pagination deadline exhausted: {last_problem}"
            )
        response = session.request(
            method,
            privacy_url.rstrip("/") + "/findings",
            **({"params": payload} if method == "GET" else {"json": payload}),
            timeout=min(30.0, remaining),
        )
        try:
            body = response.json()
            if response.status_code == 200:
                if not isinstance(body, dict):
                    raise AssertionError("Findings response must be a JSON object")
                # A completed request (including JSON decoding) cannot turn an
                # expired overall budget into a successful pagination check.
                if time.monotonic() >= deadline:
                    raise TimeoutError("Findings pagination deadline exhausted")
                return body
            code = body.get("code") if isinstance(body, dict) else None
            retryable = (response.status_code == 504 and code == "privacy_timeout") or (
                response.status_code == 503
                and code
                in ("privacy_storage_unavailable", "privacy_leader_unavailable")
                and body.get("retryable") is True
            )
            if not retryable:
                response.raise_for_status()
                raise AssertionError(
                    f"Unexpected findings HTTP status {response.status_code}"
                )
            last_problem = f"HTTP {response.status_code} {code}"
            delay = backoff
            if raw := response.headers.get("Retry-After"):
                try:
                    retry_after = float(raw)
                except ValueError:
                    retry_after = -1
                if math.isfinite(retry_after) and retry_after >= 0:
                    delay = max(delay, retry_after)
        finally:
            response.close()
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(delay, remaining))
        backoff = min(2.0, backoff * 2)
