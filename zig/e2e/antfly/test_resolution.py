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

"""End-to-end entity-resolution test over a live multi-node raft cluster.

Exercises the full cross-shard loop with no inference dependency: a document's
``relations`` field is materialized into an extraction artifact by a graph
index; the resolution worker blocks candidate entities against a *separate*
``entities`` table (cross-shard read) and records a resolution artifact; the
promoter upserts the canonical entity documents into that table (cross-shard
write); and the document graph is queried back through the mention edge with
cross-table entity hydration. See zig/RESOLUTION.md.

Run with the built binary, e.g.:

    ANTFLY_BIN=zig-out/bin/antfly uv run pytest e2e/antfly/test_resolution.py
"""

from __future__ import annotations

import json
import os
import signal
import time
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import quote

import pytest
import requests
from conftest import (
    DEFAULT_ANTFLY_BIN,
    resolve_binary_path,
)
from test_scaling import (
    METADATA_MUTATION_NOT_ADMITTED_HEADER,
    METADATA_MUTATION_NOT_ADMITTED_VALUE,
    MultiNodeScalingCluster,
)

AUTOGRAPH_E2E_TIMEOUT_S = 115.0
AUTOGRAPH_CLUSTER_STARTUP_TIMEOUT_S = 115.0
AUTOGRAPH_E2E_TEARDOWN_TIMEOUT_S = 5.0
POLL_INTERVAL_S = 0.5
POLL_REQUEST_TIMEOUT_S = 5.0
WRITE_REQUEST_TIMEOUT_FLOOR_S = 5.0
WRITE_OUTCOME_RECONCILE_TIMEOUT_S = 5.0
METADATA_MUTATION_NOT_ADMITTED_RESPONSE_HEADERS = {
    METADATA_MUTATION_NOT_ADMITTED_HEADER: METADATA_MUTATION_NOT_ADMITTED_VALUE
}


def _new_e2e_deadline() -> _Deadline:
    return _Deadline(AUTOGRAPH_E2E_TIMEOUT_S)


def _retry_after_delay_s(response: requests.Response) -> float:
    """Read the same-service Retry-After delta, falling back fail-safe."""
    raw_delay = response.headers.get("Retry-After", "").strip()
    try:
        delay_s = int(raw_delay, 10)
    except ValueError:
        return POLL_INTERVAL_S
    return delay_s if delay_s >= 0 else POLL_INTERVAL_S


DOCUMENTS_INDEXES = {
    # Materializes each document's `relations` field into the `relations_v1`
    # extraction asset the resolver consumes (no LLM needed). The resolver is
    # declared in the typed `resolvers` section nested in the index config so
    # admission validates it before the provisioner registers it.
    "relations_graph": {
        "type": "graph",
        "source": {
            "artifact": "relations_v1",
            "path": "$.relations[*]",
            "format": "extraction_relation",
            # Emit a doc->entity provenance edge per mention, tagged with the
            # resolved entity's home table so a graph query starting from a
            # document can hydrate the canonical entity cross-table (DocRef
            # hydration routing).
            "mention_edge_type": "mentions",
        },
        "artifact": {
            "name": "relations_v1",
            "kind": "asset",
            "source": {"type": "field", "value": "relations"},
            "content_type": "application/json",
        },
        "edge_types": [{"name": "mentions"}],
        # Prefix blocking links a mention to an existing entity under the same
        # `label/` namespace (cross-shard read of the entities table).
        "resolvers": [
            {
                "name": "kg",
                "table": "entities",
                "source_artifact": "relations_v1",
                "resolution_artifact": "resolution_v1",
                "key_template": "{{ lower _entity.label }}/{{ slug _entity.text }}",
                "candidate_search": "prefix",
                "scorer_json": json.dumps(
                    {
                        "comparisons": [
                            {
                                "name": "name",
                                "left": "canonical_text",
                                "right": "canonical_name",
                                "levels": [
                                    {"when": "exact", "weight": 8.0},
                                    {"else": True, "weight": -6.0},
                                ],
                            }
                        ],
                        "combine": {"bias": -3.0},
                        "decision": {"match": 0.9},
                    }
                ),
                "config_generation": 1,
            }
        ],
    },
}


@pytest.fixture(scope="function")
def resolution_cluster(request: pytest.FixtureRequest):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).exists():
        pytest.skip(f"Antfly binary not found: {binary} (set ANTFLY_BIN)")
    if Path(binary).name != "antfly":
        pytest.skip("distributed autograph e2e requires the antfly binary")
    startup_deadline = _Deadline(AUTOGRAPH_CLUSTER_STARTUP_TIMEOUT_S)
    cluster = MultiNodeScalingCluster(
        binary,
        initial_data_node_count=3,
        startup_deadline_at=startup_deadline.expires_at,
    )
    try:
        yield cluster
    finally:
        report = getattr(request.node, "rep_call", None)
        if report and report.failed:
            # Capture while the six node processes are still alive, including
            # failures that now stop immediately on an unexpected HTTP 500.
            _capture_failure_stacks(cluster)
        cluster.stop(
            timeout_s=AUTOGRAPH_E2E_TEARDOWN_TIMEOUT_S,
            test_failed=bool(report and report.failed),
            reject_data_crashes=not bool(report and report.failed),
        )


def _capture_failure_stacks(cluster: MultiNodeScalingCluster) -> str:
    # Capture once at the first failure, then reuse during fixture teardown.
    # Six default 25-second debugger waits followed by another capture could
    # otherwise consume the soak's evidence-upload budget on every failed case.
    captured = getattr(cluster, "_autograph_failure_stacks", None)
    if captured is None:
        captured = cluster.native_stack_dumps(per_process_timeout_s=5.0)
        cluster._autograph_failure_stacks = captured
        try:
            (cluster.root / "native-stacks.txt").write_text(captured, encoding="utf-8")
        except OSError as exc:
            print(f"failed to preserve Autograph native stacks: {exc!r}")
    return captured


def test_autograph_failure_stacks_are_bounded_and_retained_once(tmp_path):
    class Cluster:
        root = tmp_path
        calls: ClassVar = []

        def native_stack_dumps(self, *, per_process_timeout_s):
            self.calls.append(per_process_timeout_s)
            return "resolver waiting for read barrier"

    cluster = Cluster()
    first = _capture_failure_stacks(cluster)
    assert _capture_failure_stacks(cluster) == first
    assert cluster.calls == [5.0]
    assert (tmp_path / "native-stacks.txt").read_text() == first


class _Api:
    def __init__(self, base_url: str, server: MultiNodeScalingCluster):
        self.url = base_url.rstrip("/")
        self.s = requests.Session()
        self.s.headers["Content-Type"] = "application/json"
        self._server = server

    def _check(self, response: requests.Response) -> dict:
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"{response.status_code} {response.reason} for {response.request.method} "
                f"{response.url}\n[body]\n{response.text}\n[logs]\n{self._server.debug_logs()}",
                response=response,
            )
        return response.json() if response.content else {}

    def create_table(
        self,
        name: str,
        *,
        num_shards: int = 1,
        indexes: dict | None = None,
        deadline: _Deadline | None = None,
    ) -> dict:
        payload: dict = {"num_shards": num_shards}
        if indexes is not None:
            payload["indexes"] = indexes
        max_timeout = 90.0 if indexes is not None or num_shards > 1 else 30.0
        last_not_admitted_response: str | None = None
        while True:
            try:
                timeout = (
                    deadline.request_timeout(max_timeout)
                    if deadline is not None
                    else max_timeout
                )
            except AssertionError as exc:
                stacks = _capture_failure_stacks(self._server)
                index_names = sorted(indexes.keys()) if indexes is not None else []
                raise AssertionError(
                    f"create table exhausted its {deadline.timeout_s:.1f}s deadline "
                    f"table={name!r} shards={num_shards} indexes={index_names!r} "
                    f"last_not_admitted_response={last_not_admitted_response!r}"
                    f"\n[native stacks]\n{stacks}"
                    f"\n[metadata snapshot]\n{self._server.metadata_snapshot_diagnostic()}"
                    f"\n[logs]\n{self._server.debug_logs()}"
                ) from exc
            try:
                response = self.s.post(
                    f"{self.url}/tables/{name}", json=payload, timeout=timeout
                )
            except requests.RequestException as exc:
                # A transport failure does not prove whether the DDL reached
                # Raft, so never replay it automatically.
                stacks = _capture_failure_stacks(self._server)
                index_names = sorted(indexes.keys()) if indexes is not None else []
                raise AssertionError(
                    f"create table timed out/failed table={name!r} shards={num_shards} "
                    f"indexes={index_names!r}: {exc!r}\n[native stacks]\n{stacks}"
                    f"\n[logs]\n{self._server.debug_logs()}"
                ) from exc

            mutation_not_admitted = (
                response.status_code == 503
                and response.headers.get(METADATA_MUTATION_NOT_ADMITTED_HEADER, "")
                .strip()
                .lower()
                == METADATA_MUTATION_NOT_ADMITTED_VALUE
            )
            if deadline is None or not mutation_not_admitted:
                return self._check(response)

            last_not_admitted_response = (
                f"HTTP {response.status_code}: {response.text[:512]}"
            )
            deadline.sleep(_retry_after_delay_s(response))

    def insert(
        self,
        table: str,
        doc_id: str,
        body: dict,
        *,
        sync_level: str = "write",
        deadline: _Deadline | None = None,
    ) -> dict:
        payload = {"inserts": {doc_id: body}, "sync_level": sync_level}
        max_timeout = 120.0 if sync_level in {"enrichments", "full_index"} else 30.0
        last_retryable_response: str | None = None
        while True:
            try:
                timeout = (
                    deadline.request_timeout(
                        max_timeout,
                        minimum_timeout_s=WRITE_REQUEST_TIMEOUT_FLOOR_S,
                    )
                    if deadline is not None
                    else max_timeout
                )
            except AssertionError as exc:
                stacks = _capture_failure_stacks(self._server)
                raise AssertionError(
                    f"batch insert exhausted its {deadline.timeout_s:.1f}s deadline "
                    f"table={table!r} key={doc_id!r} sync_level={sync_level!r} "
                    f"last_retryable_response={last_retryable_response!r}"
                    f"\n[native stacks]\n{stacks}"
                    f"\n[metadata snapshot]\n{self._server.metadata_snapshot_diagnostic()}"
                    f"\n[logs]\n{self._server.debug_logs()}"
                ) from exc
            try:
                response = self.s.post(
                    f"{self.url}/tables/{table}/batch", json=payload, timeout=timeout
                )
            except requests.RequestException as exc:
                # A timed-out write usually means a node wedged in memory without
                # logging anything; capture native stacks before teardown so the
                # CI failure is diagnosable.
                stacks = _capture_failure_stacks(self._server)
                raise AssertionError(
                    f"batch insert timed out/failed table={table!r} key={doc_id!r} "
                    f"sync_level={sync_level!r}: {exc!r}\n[native stacks]\n{stacks}"
                    f"\n[metadata snapshot]\n{self._server.metadata_snapshot_diagnostic()}"
                    f"\n[logs]\n{self._server.debug_logs()}"
                ) from exc
            if (
                deadline is not None
                and response.status_code == 503
                and response.text.strip() == "write unavailable"
                and not deadline.expired()
            ):
                last_retryable_response = (
                    f"{response.status_code} {response.text.strip()} from {self.url}"
                )
                deadline.sleep()
                continue
            if (
                deadline is not None
                and response.status_code == 409
                and response.text.strip() == "write outcome unknown"
                and not deadline.expired()
            ):
                last_retryable_response = (
                    f"{response.status_code} {response.text.strip()} from {self.url}"
                )
                # This helper only emits a single deterministic keyed upsert,
                # unlike the generic batch API whose transforms may not be
                # replay-safe. Reconcile the desired state before retrying so
                # a lost acknowledgement cannot create a second journal event.
                if sync_level == "write" and self._reconcile_upsert(
                    table,
                    doc_id,
                    body,
                    deadline=deadline,
                ):
                    return {}
                deadline.sleep()
                continue
            return self._check(response)

    def _reconcile_upsert(
        self,
        table: str,
        doc_id: str,
        body: dict,
        *,
        deadline: _Deadline,
    ) -> bool:
        reconcile_expires_at = min(
            deadline.expires_at,
            time.monotonic() + WRITE_OUTCOME_RECONCILE_TIMEOUT_S,
        )
        while True:
            remaining = min(
                deadline.remaining(), reconcile_expires_at - time.monotonic()
            )
            if remaining < 0.1:
                return False
            try:
                observed = self.lookup(
                    table,
                    doc_id,
                    timeout=min(POLL_REQUEST_TIMEOUT_S, remaining),
                )
            except requests.RequestException as exc:
                if not _transient_poll_error(exc):
                    raise
            else:
                if observed == body:
                    return True
            time.sleep(min(POLL_INTERVAL_S, remaining))

    def lookup(self, table: str, key: str, *, timeout: float = 10.0) -> dict | None:
        response = self.s.get(
            f"{self.url}/tables/{table}/documents/{quote(key, safe='')}",
            timeout=timeout,
        )
        if response.status_code == 404:
            return None
        return self._check(response)

    def query_table(self, table: str, payload: dict, *, timeout: float = 30.0) -> dict:
        return self._check(
            self.s.post(
                f"{self.url}/tables/{table}/query", json=payload, timeout=timeout
            )
        )

    def diagnostic(self, *, graph_payload: dict | None = None) -> str:
        parts: list[str] = []
        for label, path in (
            ("documents table", "/tables/documents"),
            ("relations graph index", "/tables/documents/indexes/relations_graph"),
            ("entities table", "/tables/entities"),
        ):
            try:
                response = self.s.get(f"{self.url}{path}", timeout=5)
                parts.append(f"[{label}] {response.status_code} {response.text[:4000]}")
            except requests.RequestException as exc:
                parts.append(f"[{label}] unavailable: {exc!r}")
        if graph_payload is not None:
            parts.append(self._graph_probe_diagnostic(graph_payload))
        parts.append(
            f"[metadata snapshot]\n{self._server.metadata_snapshot_diagnostic()}"
        )
        parts.append(f"[logs]\n{self._server.debug_logs()}")
        return "\n".join(parts)

    def _graph_probe_diagnostic(self, payload: dict) -> str:
        parts: list[str] = ["[graph query probes]"]
        for index, base_url in enumerate(self._server.data_api_urls):
            url = base_url.rstrip("/")
            try:
                status = self.s.get(
                    f"{url}/tables/documents/indexes/relations_graph",
                    timeout=5,
                )
                parts.append(
                    f"[data {index} graph index] {status.status_code} {status.text[:3000]}"
                )
            except requests.RequestException as exc:
                parts.append(f"[data {index} graph index] unavailable: {exc!r}")

            try:
                query = self.s.post(
                    f"{url}/tables/documents/query",
                    json=payload,
                    timeout=5,
                )
                parts.append(
                    f"[data {index} graph query] {query.status_code} {query.text[:3000]}"
                )
            except requests.RequestException as exc:
                parts.append(f"[data {index} graph query] unavailable: {exc!r}")
        return "\n".join(parts)


class _Deadline:
    def __init__(self, timeout_s: float):
        self.timeout_s = timeout_s
        self.started_at = time.monotonic()
        self.expires_at = time.monotonic() + timeout_s

    def elapsed(self) -> float:
        return max(0.0, time.monotonic() - self.started_at)

    def remaining(self) -> float:
        return max(0.0, self.expires_at - time.monotonic())

    def expired(self) -> bool:
        return self.remaining() <= 0.0

    def request_timeout(
        self,
        max_timeout_s: float = POLL_REQUEST_TIMEOUT_S,
        *,
        minimum_timeout_s: float = 0.1,
    ) -> float:
        remaining = self.remaining()
        if remaining < minimum_timeout_s:
            raise AssertionError(
                f"deadline has {remaining:.3f}s remaining, below the "
                f"{minimum_timeout_s:.3f}s request floor"
            )
        return min(max_timeout_s, remaining)

    def sleep(self, delay_s: float = POLL_INTERVAL_S) -> None:
        remaining = self.remaining()
        bounded_delay_s = min(max(0.0, delay_s), remaining)
        if bounded_delay_s > 0.0:
            time.sleep(bounded_delay_s)


def _test_response(
    url: str,
    status: int,
    *,
    headers: dict[str, str] | None = None,
    body: bytes = b"",
) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.headers.update(headers or {})
    response._content = body
    response.url = url
    response.reason = "test response"
    response.request = requests.Request("POST", url).prepare()
    return response


@pytest.mark.parametrize(
    ("retry_after", "expected_delay_s"),
    [
        (None, POLL_INTERVAL_S),
        ("invalid", POLL_INTERVAL_S),
        ("-1", POLL_INTERVAL_S),
        ("0", 0.0),
        ("2", 2.0),
    ],
)
def test_retry_after_delay_uses_valid_delta_seconds_or_poll_fallback(
    retry_after: str | None,
    expected_delay_s: float,
):
    headers = {"Retry-After": retry_after} if retry_after is not None else None
    response = _test_response(
        "http://data-a/db/v1/tables/documents", 503, headers=headers
    )

    assert _retry_after_delay_s(response) == expected_delay_s


def test_deadline_sleep_clamps_retry_after_to_remaining_time(
    monkeypatch: pytest.MonkeyPatch,
):
    deadline = _Deadline(10.0)
    sleep_delays: list[float] = []
    monkeypatch.setattr(deadline, "remaining", lambda: 0.25)
    monkeypatch.setattr(time, "sleep", sleep_delays.append)

    deadline.sleep(2.0)

    assert sleep_delays == [0.25]


def test_create_table_retries_explicit_non_admission_within_deadline(
    monkeypatch: pytest.MonkeyPatch,
):
    class _Server:
        pass

    api = _Api("http://data-a/db/v1", _Server())
    url = "http://data-a/db/v1/tables/documents"
    responses = iter(
        [
            _test_response(
                url,
                503,
                headers={
                    **METADATA_MUTATION_NOT_ADMITTED_RESPONSE_HEADERS,
                    "Retry-After": "1",
                },
                body=b"metadata leader unavailable",
            ),
            _test_response(url, 200, body=b"{}"),
        ]
    )
    post_calls = 0
    sleep_delays: list[float] = []

    def post(*_: Any, **__: Any) -> requests.Response:
        nonlocal post_calls
        post_calls += 1
        return next(responses)

    deadline = _Deadline(10.0)
    monkeypatch.setattr(api.s, "post", post)
    monkeypatch.setattr(deadline, "sleep", sleep_delays.append)

    assert api.create_table("documents", deadline=deadline) == {}
    assert post_calls == 2
    assert sleep_delays == [1.0]


@pytest.mark.parametrize(
    ("status", "headers", "with_deadline"),
    [
        (503, {"X-Antfly-Metadata-Not-Leader": "true"}, True),
        (
            409,
            METADATA_MUTATION_NOT_ADMITTED_RESPONSE_HEADERS,
            True,
        ),
        (
            503,
            METADATA_MUTATION_NOT_ADMITTED_RESPONSE_HEADERS,
            False,
        ),
    ],
)
def test_create_table_never_retries_without_safe_bounded_non_admission_contract(
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    headers: dict[str, str],
    with_deadline: bool,
):
    class _Server:
        @staticmethod
        def debug_logs() -> str:
            return "test logs"

    api = _Api("http://data-a/db/v1", _Server())
    url = "http://data-a/db/v1/tables/documents"
    post_calls = 0

    def post(*_: Any, **__: Any) -> requests.Response:
        nonlocal post_calls
        post_calls += 1
        return _test_response(url, status, headers=headers, body=b"mutation failed")

    monkeypatch.setattr(api.s, "post", post)
    deadline = _Deadline(10.0) if with_deadline else None

    with pytest.raises(requests.HTTPError):
        api.create_table("documents", deadline=deadline)
    assert post_calls == 1


def test_insert_reconciles_unknown_upsert_before_retry(monkeypatch: pytest.MonkeyPatch):
    class _Server:
        pass

    api = _Api("http://data-a/db/v1", _Server())
    body = {"relations": {"entities": [{"id": "e0", "text": "Ada Lovelace"}]}}
    post_calls: list[str] = []
    lookup_calls: list[str] = []

    def post(url: str, **_: Any) -> requests.Response:
        post_calls.append(url)
        response = requests.Response()
        response.status_code = 409
        response._content = b"write outcome unknown"
        response.url = url
        return response

    def get(url: str, **_: Any) -> requests.Response:
        lookup_calls.append(url)
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(body).encode()
        response.url = url
        return response

    monkeypatch.setattr(api.s, "post", post)
    monkeypatch.setattr(api.s, "get", get)

    assert api.insert("documents", "doc:a", body, deadline=_Deadline(10.0)) == {}
    assert post_calls == ["http://data-a/db/v1/tables/documents/batch"]
    assert lookup_calls == ["http://data-a/db/v1/tables/documents/documents/doc%3Aa"]


def _wait_for_entities(
    api: _Api, expected_names: dict[str, str], *, deadline: _Deadline
) -> dict[str, dict]:
    pending = set(expected_names.keys())
    found: dict[str, dict] = {}
    last: dict[str, dict | None] = {}
    last_error: str | None = None

    while not deadline.expired():
        exhausted = False
        for key in list(pending):
            remaining = deadline.remaining()
            if remaining < 0.1:
                exhausted = True
                break
            try:
                timeout = deadline.request_timeout()
            except AssertionError:
                # Preserve the pending keys and diagnostics when there is no
                # longer enough budget to issue another bounded request.
                break
            try:
                doc = api.lookup("entities", key, timeout=timeout)
            except requests.RequestException as exc:
                if not _transient_poll_error(exc):
                    raise
                last_error = repr(exc)
                continue
            last[key] = doc
            if doc is not None and expected_names[key] in _doc_text(doc):
                found[key] = doc
                pending.remove(key)
        if not pending:
            return found
        if exhausted:
            break
        deadline.sleep()

    raise AssertionError(
        f"entities were not promoted within {deadline.timeout_s}s "
        f"(elapsed={deadline.elapsed():.1f}s, pending={sorted(pending)!r}, "
        f"last={last!r}, last_error={last_error!r})"
        f"\n[native stacks]\n{_capture_failure_stacks(api._server)}"
        f"\n{api.diagnostic()}"
    )


def test_entity_deadline_retains_diagnostics_below_request_floor(monkeypatch, tmp_path):
    class Server:
        root = tmp_path

        def native_stack_dumps(self, *, per_process_timeout_s):
            assert per_process_timeout_s == 5.0
            return "captured native stacks"

    class Api:
        _server = Server()

        def lookup(self, *args, **kwargs):
            raise AssertionError("must not start a request below the time floor")

        def diagnostic(self):
            return "captured entity diagnostics"

    deadline = _Deadline(115.0)
    monkeypatch.setattr(deadline, "remaining", lambda: 0.09)
    with pytest.raises(AssertionError) as failure:
        _wait_for_entities(Api(), {"entity:a": "Alice"}, deadline=deadline)
    assert "pending=['entity:a']" in str(failure.value)
    assert "captured native stacks" in str(failure.value)
    assert "captured entity diagnostics" in str(failure.value)


def _doc_text(doc: dict) -> str:
    """The lookup response carries the stored document; flatten it to text so the
    assertions tolerate whichever envelope the public API uses."""
    return json.dumps(doc)


def _transient_poll_error(exc: requests.RequestException) -> bool:
    response = getattr(exc, "response", None)
    if response is not None:
        # Availability has an explicit contract. Retrying an INTERNAL_ERROR
        # hid an untransportable ReadIndexTimeout until the promotion deadline.
        return response.status_code == 503 and bool(
            response.headers.get("Retry-After", "").strip()
        )
    return isinstance(
        exc,
        (
            requests.ConnectionError,
            requests.ReadTimeout,
            requests.Timeout,
        ),
    )


@pytest.mark.parametrize(
    ("status", "retry_after", "expected"),
    [
        (503, "1", True),
        (503, None, False),
        (500, "1", False),
        (502, None, False),
        (504, None, False),
        (409, None, False),
    ],
)
def test_autograph_poll_retries_only_explicit_availability(
    status, retry_after, expected
):
    response = requests.Response()
    response.status_code = status
    if retry_after is not None:
        response.headers["Retry-After"] = retry_after
    assert _transient_poll_error(requests.HTTPError(response=response)) is expected


def test_autograph_entity_poll_propagates_internal_failure():
    class BrokenApi:
        def lookup(self, *_args, **_kwargs):
            response = requests.Response()
            response.status_code = 500
            raise requests.HTTPError("RuntimeBoundaryFailure", response=response)

    with pytest.raises(requests.HTTPError, match="RuntimeBoundaryFailure"):
        _wait_for_entities(
            BrokenApi(),
            {"person/ada_lovelace": "Ada Lovelace"},
            deadline=_Deadline(1.0),
        )


@pytest.mark.parametrize("hydrate", [False, True])
def test_autograph_poll_preserves_context_at_request_deadline(monkeypatch, hydrate):
    now = [0.0]
    monkeypatch.setattr(time, "monotonic", lambda: now[0])
    monkeypatch.setattr(time, "sleep", lambda delay: now.__setitem__(0, now[0] + delay))
    monkeypatch.setitem(globals(), "_capture_failure_stacks", lambda _: "stacks")

    class Api:
        _server = None

        def diagnostic(self, **_kwargs):
            return "retained graph status"

    deadline = _Deadline(0.05)
    expected = {"person/ada_lovelace": "Ada Lovelace"}
    with pytest.raises(AssertionError, match="retained graph status") as failure:
        if hydrate:
            _wait_for_mention_hydration(
                Api(), start_node="doc:a", expected_names=expected, deadline=deadline
            )
        else:
            _wait_for_entities(Api(), expected, deadline=deadline)
    assert "person/ada_lovelace" in str(failure.value)
    assert "request floor" not in str(failure.value)
    assert now[0] <= deadline.timeout_s


def _graph_result(result: dict, name: str) -> dict | None:
    responses = result.get("responses", [])
    if not responses:
        return None
    return responses[0].get("graph_results", {}).get(name)


def _wait_for_mention_hydration(
    api: _Api,
    *,
    start_node: str,
    expected_names: dict[str, str],
    deadline: _Deadline,
) -> dict:
    payload = {
        "query": {"match_all": {}},
        "graph_queries": {
            "mentions": {
                "index": "relations_graph",
                "traverse": {
                    "start": {"keys": [start_node]},
                    "edge_types": ["mentions"],
                    "max_depth": 1,
                    "limit": 10,
                    "include_documents": True,
                    "fields": ["entity_type", "canonical_name", "aliases"],
                },
            }
        },
        "limit": 10,
    }

    last: dict[str, Any] | None = None
    last_error: str | None = None
    while not deadline.expired():
        try:
            timeout = deadline.request_timeout()
        except AssertionError:
            break
        try:
            last = api.query_table("documents", payload, timeout=timeout)
        except requests.RequestException as exc:
            if not _transient_poll_error(exc):
                raise
            last_error = repr(exc)
            deadline.sleep()
            continue
        graph = _graph_result(last, "mentions")
        if graph is None:
            deadline.sleep()
            continue
        nodes = graph.get("nodes", [])
        by_key = {node.get("key"): node for node in nodes if isinstance(node, dict)}
        hydrated = True
        for key, canonical_name in expected_names.items():
            node = by_key.get(key)
            hydrated = (
                hydrated
                and isinstance(node, dict)
                and isinstance(node.get("document"), dict)
                and node["document"].get("canonical_name") == canonical_name
            )
        if hydrated:
            return graph
        deadline.sleep()
    raise AssertionError(
        f"mention graph did not hydrate promoted entities within {deadline.timeout_s}s "
        f"(elapsed={deadline.elapsed():.1f}s, start_node={start_node!r}, "
        f"expected={expected_names!r}, last={last!r}, last_error={last_error!r})\n"
        f"{api.diagnostic(graph_payload=payload)}"
    )


@pytest.mark.parametrize("candidate_search", ["prefix", "exact_key"])
def test_multinode_autograph_resolves_promotes_and_hydrates_entities(
    resolution_cluster,
    candidate_search,
):
    _exercise_multinode_autograph(
        resolution_cluster, candidate_search=candidate_search, restart_data=False
    )


def test_multinode_autograph_recovers_after_data_restart(resolution_cluster):
    _exercise_multinode_autograph(resolution_cluster, restart_data=True)


def _restart_data_owners(cluster):
    for node in cluster.data_nodes:
        node_id = int(node["id"])
        cluster.stop_data_node(node_id)
        returncode = cluster.data_proc_by_node_id[node_id].returncode
        # The fixture may force a crash after its bounded graceful-stop wait.
        # A spontaneous assertion/segfault must not disappear behind restart.
        assert returncode in (0, -signal.SIGKILL), (
            f"data node {node_id} crashed during restart rc={returncode}\n"
            f"{cluster.debug_logs()}"
        )
    for node in cluster.data_nodes:
        cluster._start_data_node(node)


@pytest.mark.parametrize(
    "returncode", [0, -signal.SIGKILL, -signal.SIGABRT, -signal.SIGSEGV]
)
def test_autograph_restart_rejects_spontaneous_crashes(returncode):
    class Process:
        pass

    process = Process()
    process.returncode = returncode

    class Cluster:
        data_nodes: ClassVar = [{"id": 101}, {"id": 102}]
        data_proc_by_node_id: ClassVar = {101: process, 102: process}
        actions: ClassVar = []

        def stop_data_node(self, node_id):
            self.actions.append(("stop", node_id))

        def _start_data_node(self, node):
            self.actions.append(("start", node["id"]))

        def debug_logs(self):
            return "retained crash evidence"

    cluster = Cluster()
    if returncode in (0, -signal.SIGKILL):
        _restart_data_owners(cluster)
        assert cluster.actions == [
            ("stop", 101),
            ("stop", 102),
            ("start", 101),
            ("start", 102),
        ]
    else:
        with pytest.raises(AssertionError, match="retained crash evidence"):
            _restart_data_owners(cluster)
        assert cluster.actions == [("stop", 101)]


@pytest.mark.parametrize(
    "returncode", [0, -signal.SIGKILL, -signal.SIGABRT, -signal.SIGSEGV]
)
def test_autograph_teardown_crash_preserves_failure_evidence(monkeypatch, returncode):
    import test_scaling

    class Process:
        def poll(self):
            return self.returncode

    process = Process()
    process.returncode = returncode

    class Resources:
        cleaned = False

        def close(self):
            pass

        def cleanup(self):
            self.cleaned = True

    class Cluster:
        port_reservations = Resources()
        tempdir = Resources()
        data_procs: ClassVar = [process]
        metadata_procs: ClassVar = []
        data_proc_by_node_id: ClassVar = {101: process}
        log_files: ClassVar = []
        diagnostics_saved = False

        def debug_logs(self):
            return "promotion callback crash"

        def preserve_failure_diagnostics(self):
            self.diagnostics_saved = True

    preserved = []

    def preserve(tempdir, *, failed):
        preserved.append(failed)
        return failed

    monkeypatch.setattr(test_scaling, "maybe_preserve_tempdir", preserve)
    cluster = Cluster()
    crashed = returncode not in (0, -signal.SIGKILL)
    if crashed:
        with pytest.raises(AssertionError, match="promotion callback crash"):
            MultiNodeScalingCluster.stop(cluster, reject_data_crashes=True)
    else:
        MultiNodeScalingCluster.stop(cluster, reject_data_crashes=True)
    assert preserved == [crashed]
    assert cluster.diagnostics_saved == crashed
    assert cluster.tempdir.cleaned != crashed


def _exercise_multinode_autograph(
    cluster, *, restart_data: bool, candidate_search="prefix"
):
    indexes = json.loads(json.dumps(DOCUMENTS_INDEXES))
    indexes["relations_graph"]["resolvers"][0]["candidate_search"] = candidate_search
    api = _Api(cluster.data_api_urls[0], cluster)

    # Entities live in their own table (own shard group); documents are spread
    # across multiple shards in an explicit multi-node metadata/data raft setup.
    # Resolution reads entities cross-shard; promotion writes them cross-shard;
    # graph query hydrates the promoted entity documents through mention edges.
    api.create_table("entities", num_shards=1, deadline=_new_e2e_deadline())
    api.create_table(
        "documents",
        num_shards=3,
        indexes=indexes,
        deadline=_new_e2e_deadline(),
    )

    api.insert(
        "documents",
        "doc:a",
        {
            "relations": {
                "entities": [
                    {"id": "e0", "label": "person", "text": "Ada Lovelace"},
                    {"id": "e1", "label": "org", "text": "Antfly"},
                ]
            }
        },
        deadline=_new_e2e_deadline(),
    )

    if restart_data:
        # Reopen every data owner with the committed document on disk. Do not
        # issue another write before observing resolution/promotion recovery.
        _restart_data_owners(cluster)

    # The promoter upserts a canonical entity document per resolved mention into
    # the entity table on its own shard.
    _wait_for_entities(
        api,
        {
            "person/ada_lovelace": "Ada Lovelace",
            "org/antfly": "Antfly",
        },
        deadline=_new_e2e_deadline(),
    )

    # At least two coordinators must forward to the entity shard's owner.
    # This covers decoding native physical table names on internal lookups.
    for base_url in cluster.data_api_urls:
        node_api = _Api(base_url, cluster)
        try:
            assert "Ada Lovelace" in _doc_text(
                node_api.lookup("entities", "person/ada_lovelace")
            )
        finally:
            node_api.s.close()

    # A curated redirect makes exact-key candidate reads observable: minting
    # without reading the existing candidate would choose the old key.
    second_entity_key = "person/ada_lovelace"
    if candidate_search == "exact_key":
        second_entity_key = "person/ada_curated"
        api.insert(
            "entities",
            second_entity_key,
            {
                "canonical_name": "Ada Lovelace",
                "entity_type": "person",
            },
            sync_level="full_index",
            deadline=_new_e2e_deadline(),
        )
        api.insert(
            "entities",
            "person/ada_lovelace",
            {
                "canonical_name": "Ada Lovelace",
                "entity_type": "person",
                "merged_into": second_entity_key,
            },
            sync_level="full_index",
            deadline=_new_e2e_deadline(),
        )

    # Repeated mentions share candidate reads and retain the canonical redirect
    # destination for every mention in both exact and prefix workloads.
    api.insert(
        "documents",
        "doc:b",
        {
            "relations": {
                "entities": [
                    {"id": f"e{i}", "label": "person", "text": "Ada Lovelace"}
                    for i in range(100)
                ]
            }
        },
        deadline=_new_e2e_deadline(),
    )

    mentions = _wait_for_mention_hydration(
        api,
        start_node="doc:a",
        expected_names={
            "person/ada_lovelace": "Ada Lovelace",
            "org/antfly": "Antfly",
        },
        deadline=_new_e2e_deadline(),
    )
    node_keys = {node["key"] for node in mentions["nodes"]}
    assert {"person/ada_lovelace", "org/antfly"} <= node_keys

    second_mentions = _wait_for_mention_hydration(
        api,
        start_node="doc:b",
        expected_names={second_entity_key: "Ada Lovelace"},
        deadline=_new_e2e_deadline(),
    )
    second_node_keys = {node["key"] for node in second_mentions["nodes"]}
    assert second_entity_key in second_node_keys


def test_multinode_exact_candidates_follow_redirects_across_entity_shards(
    resolution_cluster,
):
    """A batch spans owners; redirects cross owners and duplicate mentions reuse it."""
    api = _Api(resolution_cluster.data_api_urls[0], resolution_cluster)
    indexes = json.loads(json.dumps(DOCUMENTS_INDEXES))
    resolver = indexes["relations_graph"]["resolvers"][0]
    resolver["candidate_search"] = "exact_key"
    resolver["key_template"] = "{{ slug _entity.text }}"
    api.create_table("entities", num_shards=8, deadline=_new_e2e_deadline())
    api.create_table(
        "documents", num_shards=3, indexes=indexes, deadline=_new_e2e_deadline()
    )
    expected = {}
    names = ["0 Ada", "5 Grace", "a Alan", "f Edsger"]
    for name, survivor in zip(
        names, ["f_curated", "a_curated", "5_curated", "0_curated"]
    ):
        expected[survivor] = name
        for key, fields in [
            (survivor, {}),
            (name.lower().replace(" ", "_"), {"merged_into": survivor}),
        ]:
            api.insert(
                "entities",
                key,
                {"canonical_name": name, "entity_type": "person", **fields},
                sync_level="full_index",
                deadline=_new_e2e_deadline(),
            )
    api.insert(
        "documents",
        "7:batch",
        {
            "relations": {
                "entities": [
                    {"id": f"e{i}", "label": "person", "text": names[i % len(names)]}
                    for i in range(100)
                ]
            }
        },
        deadline=_new_e2e_deadline(),
    )
    graph = _wait_for_mention_hydration(
        api, start_node="7:batch", expected_names=expected, deadline=_new_e2e_deadline()
    )
    keys = {node["key"] for node in graph["nodes"]}
    assert set(expected) <= keys
    assert not {name.lower().replace(" ", "_") for name in names} & keys


def test_multinode_autograph_deleted_target_does_not_fail_surviving_graph(
    resolution_cluster,
):
    _exercise_multinode_autograph(
        resolution_cluster, candidate_search="exact_key", restart_data=False
    )
    api = _Api(resolution_cluster.data_api_urls[0], resolution_cluster)
    response = api.s.delete(f"{api.url}/tables/entities", timeout=30)
    api._check(response)
    result = api.query_table(
        "documents",
        {
            "query": {"match_all": {}},
            "graph_queries": {
                "mentions": {
                    "index": "relations_graph",
                    "traverse": {
                        "start": {"keys": ["doc:a"]},
                        "edge_types": ["mentions"],
                        "max_depth": 1,
                        "limit": 10,
                        "include_documents": True,
                    },
                }
            },
            "limit": 10,
        },
    )
    graph = _graph_result(result, "mentions")
    assert graph is not None
    assert not any(node.get("table") == "entities" for node in graph.get("nodes", []))
