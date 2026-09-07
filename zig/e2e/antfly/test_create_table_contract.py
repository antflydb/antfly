# Copyright 2026 Antfly, Inc.
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

"""Client-side handling of committed, not-yet-observable table creation."""

from types import SimpleNamespace

import conftest
import pytest


class Client:
    def __init__(self, replies=()):
        self._created_tables = set()
        self.replies = iter(replies)
        self.calls = []

    def _check(self, response):
        assert response.status_code < 400
        return response.body

    def _request(self, method, path):
        self.calls.append((method, path))
        return next(self.replies)


def response(status, body):
    return SimpleNamespace(status_code=status, body=body)


@pytest.mark.parametrize(
    "status", ["committed_repair_required", "committed_visibility_pending"]
)
def test_committed_create_observes_without_resubmitting(monkeypatch, status):
    monkeypatch.setattr(conftest.time, "sleep", lambda _: None)
    client = Client([response(404, {}), response(200, {"name": "table"})])
    assert conftest.finish_create_table(
        client, "table", response(202, {"status": status})
    ) == {"name": "table"}
    assert client.calls == [("GET", "/tables/table")] * 2
    assert client._created_tables == {"table"}


def test_synchronous_create_needs_no_polling():
    client = Client()
    assert conftest.finish_create_table(
        client, "table", response(200, {"name": "table"})
    ) == {"name": "table"}
    assert client.calls == []


def test_committed_create_timeout_remains_registered_for_cleanup():
    client = Client()
    with pytest.raises(AssertionError, match="did not become visible"):
        conftest.finish_create_table(
            client,
            "table",
            response(202, {"status": "committed_repair_required"}),
            timeout_s=0,
        )
    assert client.calls == []
    assert client._created_tables == {"table"}


def test_unavailable_repair_is_not_treated_as_success():
    client = Client()
    with pytest.raises(AssertionError, match="needs intervention"):
        conftest.finish_create_table(
            client, "table", response(202, {"status": "committed_repair_unavailable"})
        )
    assert client.calls == []
