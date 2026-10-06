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

"""Catalog writes retain at-most-once behavior across authority loss."""

import json

import catalog_mutations as mutations
import pytest
import requests


def response(status, body=None, headers=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(body or {}).encode()
    result.headers.update(headers or {})
    return result


def harness(monkeypatch, reads, writes):
    clock = [0.0]
    monkeypatch.setattr(mutations.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        mutations.time,
        "sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )
    events = []

    class Session:
        def get(self, url, **kwargs):
            events.append("GET")
            value = reads.pop(0) if reads else response(404)
            if isinstance(value, Exception):
                raise value
            return value

        def request(self, method, url, **kwargs):
            events.append(method)
            value = writes.pop(0)
            if isinstance(value, Exception):
                raise value
            return value

    return Session(), events


def run(session, method="POST"):
    def verify(value):
        assert value == {"name": "owned", "database_id": 7}

    mutations.mutate_owned_resource(
        session,
        "http://cluster/databases/owned",
        method=method,
        verify=verify,
        assert_alive=lambda: None,
        timeout_s=3,
    )


@pytest.mark.parametrize(
    "write",
    [
        response(503, headers={"X-Antfly-Raft-Mutation-Outcome": "unknown-v1"}),
        response(409, headers={"X-Antfly-Raft-Mutation-Outcome": "unknown-v1"}),
        requests.Timeout("reply lost"),
    ],
)
def test_uncertain_create_only_observes_and_checks_owned_resource(monkeypatch, write):
    session, events = harness(
        monkeypatch,
        [
            response(404),
            response(404),
            response(503),
            response(200, {"name": "owned", "database_id": 7}),
        ],
        [write],
    )
    run(session)
    assert events == ["GET", "POST", "GET", "GET", "GET"]


def test_proven_non_admission_permits_replay(monkeypatch):
    session, events = harness(
        monkeypatch,
        [response(404), response(200, {"name": "owned", "database_id": 7})],
        [
            response(
                503,
                headers={
                    "X-Antfly-Metadata-Mutation-Not-Admitted": "true",
                    "X-Antfly-Raft-Mutation-Outcome": "not-proposed-v1",
                },
            ),
            response(201),
        ],
    )
    run(session)
    assert events == ["GET", "POST", "POST", "GET"]


def test_unknown_outcome_overrides_non_admission_header(monkeypatch):
    session, events = harness(
        monkeypatch,
        [response(404), response(200, {"name": "owned", "database_id": 7})],
        [
            response(
                503,
                headers={
                    "X-Antfly-Metadata-Mutation-Not-Admitted": "true",
                    "X-Antfly-Raft-Mutation-Outcome": "unknown-v1",
                },
            )
        ],
    )
    run(session)
    assert events.count("POST") == 1


def test_unmarked_availability_failure_is_not_replayed(monkeypatch):
    session, events = harness(monkeypatch, [response(404)], [response(503)])
    with pytest.raises(AssertionError, match="catalog mutation rejected"):
        run(session)
    assert events == ["GET", "POST"]


def test_uncertain_absence_never_authorizes_another_create(monkeypatch):
    session, events = harness(
        monkeypatch, [response(404)], [requests.Timeout("lost reply")]
    )
    with pytest.raises(AssertionError, match="deadline exceeded"):
        run(session)
    assert events.count("POST") == 1


def test_preexisting_resource_is_not_borrowed_as_our_creation(monkeypatch):
    session, events = harness(
        monkeypatch, [response(200, {"name": "owned", "database_id": 7})], []
    )
    with pytest.raises(AssertionError, match="initial catalog state"):
        run(session)
    assert events == ["GET"]


def test_unknown_delete_observes_absence_without_replay(monkeypatch):
    session, events = harness(
        monkeypatch,
        [
            response(200, {"name": "owned", "database_id": 7}),
            response(200, {"name": "owned", "database_id": 7}),
            response(404),
        ],
        [response(409, headers={"X-Antfly-Raft-Mutation-Outcome": "unknown-v1"})],
    )
    run(session, "DELETE")
    assert events == ["GET", "DELETE", "GET", "GET"]


def test_observed_definition_mismatch_fails(monkeypatch):
    session, _ = harness(
        monkeypatch,
        [response(404), response(200, {"name": "wrong", "database_id": 7})],
        [response(201)],
    )
    with pytest.raises(AssertionError):
        run(session)
