# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Admission and observation for exclusively owned catalog test resources.

These helpers require an isolated fixture and a name no other actor mutates.
A transient response alone never authorizes replay of a catalog write.
"""

import time
from collections.abc import Callable

import requests

_OUTCOME = "X-Antfly-Raft-Mutation-Outcome"
_NOT_ADMITTED = "X-Antfly-Metadata-Mutation-Not-Admitted"


def mutate_owned_resource(
    session,
    url: str,
    *,
    method: str,
    verify: Callable[[dict], None],
    assert_alive: Callable[[], None],
    timeout_s: float = 30.0,
) -> None:
    """Create an absent resource or delete an observed, exclusively owned one.

    Once a write's outcome is unknown, only observations may finish the call.
    In particular, an absent read never permits another uncertain create.
    """
    assert method in {"POST", "DELETE"}
    deadline = time.monotonic() + timeout_s
    last = None

    def remaining():
        assert_alive()
        budget = deadline - time.monotonic()
        assert budget > 0, (
            "owned catalog mutation deadline exceeded",
            method,
            url,
            last.status_code if last is not None else None,
            last.text if last is not None else None,
        )
        return budget

    def pause(delay_s=0.1):
        time.sleep(min(delay_s, remaining()))

    # Establish ownership before submitting a mutation. Successful observation
    # after an uncertain POST cannot be borrowed from a pre-existing resource.
    while True:
        try:
            last = session.get(url, timeout=remaining())
        except (requests.Timeout, requests.ConnectionError):
            pause()
            continue
        if method == "POST" and last.status_code == 404:
            break
        if method == "DELETE" and last.status_code == 200:
            verify(last.json())
            break
        if last.status_code != 503:
            raise AssertionError(
                ("unexpected initial catalog state", last.status_code, last.text)
            )
        pause()

    while True:
        try:
            last = session.request(
                method, url, json={} if method == "POST" else None, timeout=remaining()
            )
        except (requests.Timeout, requests.ConnectionError):
            # Delivery is unknown. Do not replay even if the next GET is absent.
            break
        outcome = last.headers.get(_OUTCOME)
        if last.ok or (last.status_code in (409, 503) and outcome == "unknown-v1"):
            break
        if (
            last.status_code == 503
            and last.headers.get(_NOT_ADMITTED, "").lower() == "true"
            and outcome in (None, "not-proposed-v1")
        ):
            # Non-admission responses advertise Retry-After: 1. Avoid
            # hammering authority discovery during a persistence stall.
            pause(1.0)
            continue
        raise AssertionError(
            (
                "catalog mutation rejected",
                last.status_code,
                dict(last.headers),
                last.text,
            )
        )

    while True:
        try:
            last = session.get(url, timeout=remaining())
        except (requests.Timeout, requests.ConnectionError):
            pause()
            continue
        if method == "POST" and last.status_code == 200:
            verify(last.json())
            return
        if method == "DELETE" and last.status_code == 404:
            return
        if last.status_code not in (200, 404, 503):
            raise AssertionError(
                ("catalog observation failed", last.status_code, last.text)
            )
        pause()
