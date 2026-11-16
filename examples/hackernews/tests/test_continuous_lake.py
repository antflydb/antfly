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

"""Qualification must preserve evidence and reject unsafe or misleading results."""

import copy
from http.client import IncompleteRead
import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import continuous_lake as lake


@pytest.fixture
def run(tmp_path):
    args = SimpleNamespace(
        state=tmp_path, provider="local", port=19197, max_seconds=120, seed_rows=8
    )
    value = lake.Run(args)
    yield value
    value.events.close()


def search_row(key, version):
    return {
        name: key
        if name == "hn_id"
        else version
        if name == "qual_version"
        else 0
        if kind == "integer"
        else "database"
        for name, kind in lake.KINDS.items()
    }


def test_retry_preserves_pending_intent_until_durable_ack(run, monkeypatch):
    calls = []
    row = {"hn_id": 42, "qual_version": 1, "body": "database"}

    def request(method, route, body):
        assert (
            json.loads((run.state / (lake.TABLES[1] + "-pending.json")).read_text())
            == body
        )
        calls.append(copy.deepcopy(body))
        if len(calls) == 1:
            raise lake.RequestFailure.transport(
                method, route, ConnectionResetError(), 1
            )
        return {"state": "accepted", "wal_lsn": 3}

    monkeypatch.setattr(run, "call", request)
    monkeypatch.setattr(lake.time, "sleep", lambda _: None)
    run.transaction(lake.TABLES[1], [{"op": "upsert", "row": row}])
    assert calls[0] == calls[1]
    assert run.expected == {42: row}
    assert [batch for _, batch in run.search_intents] == [((42, 1),)]
    assert not (run.state / (lake.TABLES[1] + "-pending.json")).exists()


@pytest.mark.parametrize(
    "error", [TimeoutError("PRIVATE"), IncompleteRead(b"PRIVATE", 10)]
)
def test_response_body_transport_evidence_preserves_stage_and_status(
    run, monkeypatch, error
):
    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def read(self, *_):
            raise error

    monkeypatch.setattr(lake, "open_request", lambda *_, **__: Response())
    with pytest.raises(lake.RequestFailure) as caught:
        run.call("POST", "/query", {})
    assert caught.value.record["transport_phase"] == "response_body"
    assert caught.value.record["response_status"] == 200
    assert "http_status" not in caught.value.record
    assert caught.value.record["code"] == "transport_error"
    assert "PRIVATE" not in (run.state / "events.jsonl").read_text()


def test_before_headers_transport_evidence_does_not_invent_status(run, monkeypatch):
    def fail(*_, **__):
        raise TimeoutError("PRIVATE")

    monkeypatch.setattr(lake, "open_request", fail)
    with pytest.raises(lake.RequestFailure) as caught:
        run.call("POST", "/query", {})
    assert caught.value.record["transport_phase"] == "connect_or_headers"
    assert "response_status" not in caught.value.record
    assert "PRIVATE" not in (run.state / "events.jsonl").read_text()


def test_error_body_timeout_retains_a_transport_record(run, monkeypatch):
    class Body:
        def read(self, *_):
            raise TimeoutError("PRIVATE")

        def close(self):
            pass

    def fail(*_, **__):
        raise lake.HTTPError("http://127.0.0.1/private", 503, "PRIVATE", None, Body())

    monkeypatch.setattr(lake, "open_request", fail)
    with pytest.raises(lake.RequestFailure) as caught:
        run.call("POST", "/query", {})
    assert caught.value.record["code"] == "transport_error"
    assert caught.value.record["transport_phase"] == "response_body"
    assert caught.value.record["response_status"] == 503
    assert "PRIVATE" not in (run.state / "events.jsonl").read_text()


def test_failed_admission_retains_intent_without_acknowledging(run, monkeypatch):
    def reject(method, route, body):
        raise lake.RequestFailure.http(method, route, 403, b'{"token":"SECRET"}', 1)

    monkeypatch.setattr(run, "call", reject)
    with pytest.raises(lake.RequestFailure):
        run.transaction(lake.TABLES[1], [{"op": "delete", "row": {"hn_id": 42}}])
    assert not run.transactions[lake.TABLES[1]]
    assert (run.state / (lake.TABLES[1] + "-pending.json")).exists()
    assert "SECRET" not in (run.state / "events.jsonl").read_text()


def test_maintenance_resumes_incomplete_job_without_recreating_completed_job(
    run, monkeypatch
):
    run.args.maintenance_interval = 15
    now = [100.0]
    calls = []
    monkeypatch.setattr(lake.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(run, "record_vacuum", lambda *_: None)

    def request(method, route, body):
        table = route.split("/")[2]
        assert (
            json.loads((run.state / "pending-vacuum.json").read_text())[table] == body
        )
        calls.append((table, copy.deepcopy(body)))
        return {"complete": table == lake.TABLES[1]}

    monkeypatch.setattr(run, "call", request)
    next_start = {}
    sequence = run.maintenance_turn(0, next_start)
    assert sequence == 1
    assert len(calls) == 2
    now[0] += 1
    sequence = run.maintenance_turn(sequence, next_start)
    assert len(calls) == 3
    assert calls[0] == calls[2]
    now[0] += 15
    run.maintenance_turn(sequence, next_start)
    assert len(calls) == 5
    assert calls[-1][1]["operation_id"] != calls[1][1]["operation_id"]


def test_gcs_configuration_has_only_secret_reference(tmp_path):
    args = SimpleNamespace(
        state=tmp_path,
        provider="gcs",
        bucket="existing",
        prefix="hn-qualification/fresh",
    )
    config, warehouses = lake.configuration(args)
    assert "${secret:HN_GCS_BEARER}" in json.dumps(config)
    assert (
        config["connections"]["source"]["external_io"]["prefix"]
        == args.prefix + "/warehouse"
    )
    assert all(
        uri.startswith("gs://existing/hn-qualification/fresh/warehouse/")
        for uri in warehouses.values()
    )


def test_existing_gcs_namespace_prevents_process_start(run, monkeypatch):
    run.args.provider = "gcs"
    run.args.project = "test"
    monkeypatch.setattr(lake, "gcs_bearer", lambda *_: "SECRET")
    monkeypatch.setattr(run, "inventory", lambda: {"objects": 1, "bytes": 20})
    monkeypatch.setattr(
        lake.subprocess, "Popen", lambda *a, **kw: pytest.fail("must not start")
    )
    with pytest.raises(ValueError, match="must be empty"):
        run.start()
    assert "SECRET" not in (run.state / "config.json").read_text()


def test_unknown_query_image_is_a_correctness_failure(run, monkeypatch):
    run.anchor_id = 42
    expected = search_row(42, 1)
    expected["body"] = "database expected"
    run.known[(42, 1)] = expected
    monkeypatch.setattr(
        run,
        "call",
        lambda *_: {
            "responses": [
                {
                    "hits": {
                        "hits": [
                            {
                                "_source": dict(expected, body="database CORRUPT"),
                                "highlight": {"body": ["database"]},
                            }
                        ]
                    }
                }
            ]
        },
    )
    with pytest.raises(AssertionError, match="unknown row image"):
        run.query("published")


def test_unleased_observer_race_does_not_hide_authorization_failure(run, monkeypatch):
    metadata = {
        "snapshots": [{"snapshot-id": 1, "manifest-list": "object://antfly/list"}],
        "current-snapshot-id": 1,
    }
    monkeypatch.setattr(run, "catalog", lambda _: {"metadata": metadata})

    def missing(*_):
        raise FileNotFoundError()

    monkeypatch.setattr(run, "object_bytes", missing)
    assert run.observe(lake.TABLES[0], True)["unleased_observer_race"]

    def denied(*_):
        raise lake.HTTPError("https://example", 403, "denied", {}, None)

    monkeypatch.setattr(run, "object_bytes", denied)
    with pytest.raises(lake.HTTPError):
        run.observe(lake.TABLES[0], True)


def test_native_and_json_schemas_share_stable_field_identity():
    table = lake.table_definition(lake.TABLES[1], "file:///warehouse/table", True)
    properties = table["schema"]["document_schemas"]["row"]["schema"]["properties"]
    fields = lake.catalog_definition(lake.TABLES[1])["schema"]["fields"]
    assert [f["name"] for f in fields] == list(properties)
    assert [f["id"] for f in fields] == list(range(1, len(fields) + 1))
    assert all(
        (f["type"] == "long") == (properties[f["name"]]["type"] == "integer")
        for f in fields
    )


def test_local_observer_decodes_and_verifies_native_object_envelope(run):
    payload = b"Obj\x01manifest-list"
    root = (
        run.state / "source-store/warehouse" / lake.TABLES[0] / "buckets/antfly/objects"
    )
    root.mkdir(parents=True)
    path = root / "list.avro"
    header = (
        b"AFOBJ001"
        + lake.struct.pack("<QI", len(payload), 4)
        + lake.hashlib.sha256(payload).hexdigest().encode()
    )
    path.write_bytes(header + b"avro" + payload)
    assert run.object_bytes(lake.TABLES[0], "object://antfly/list.avro") == payload
    path.write_bytes(header + b"avro" + b"X" * len(payload))
    with pytest.raises(ValueError, match="digest mismatch"):
        run.object_bytes(lake.TABLES[0], "object://antfly/list.avro")


def test_fixture_nulls_are_normalized_without_modifying_input(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    path = tmp_path / "seed.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"hn_id": 7, "title": "title", "body": None, "points": None},
                {"hn_id": 8, "title": None, "body": "x" * 2048, "points": 10},
            ]
        ),
        path,
    )
    original = path.read_bytes()
    rows = lake.fixture(path, 2)
    assert path.read_bytes() == original
    assert rows[0]["points"] == 0
    assert "hnqualificationanchor" in rows[0]["body"]
    assert rows[1]["title"] == ""
    assert len(rows[1]["body"]) == len("database ") + 1024
    assert all(set(row) == set(lake.KINDS) for row in rows)


def test_vacuum_deletion_counts_accumulate_acknowledged_turns(run):
    request = {"operation_id": "stable-job"}
    run.record_vacuum(
        lake.TABLES[0], request, {"deleted_objects": 16, "complete": False}
    )
    run.record_vacuum(
        lake.TABLES[0], request, {"deleted_objects": 3, "complete": False}
    )
    run.record_vacuum(lake.TABLES[0], request, {"deleted_objects": 0, "complete": True})
    job = run.vacuum_jobs[(lake.TABLES[0], "stable-job")]
    assert job["deleted_objects_observed"] == 19
    assert job["acknowledged_turns"] == 3
    assert job["result"]["complete"]


def test_local_configuration_uses_standalone_artifact_owner(tmp_path):
    config, _ = lake.configuration(SimpleNamespace(state=tmp_path, provider="local"))
    assert "artifacts" not in config["storage"]
    assert "artifacts" not in config["connections"]
    assert config["connections"]["source"]["capabilities"] == [
        "lake.read",
        "lake.write",
    ]


def test_query_requires_per_hit_public_highlights_and_unique_keys(run, monkeypatch):
    row = {
        name: (7 if kind == "integer" else "database")
        for name, kind in lake.KINDS.items()
    }
    run.anchor_id = row["hn_id"]
    run.known[(row["hn_id"], row["qual_version"])] = row
    hit = {"_source": row, "_highlights": {"body": ["<mark>database</mark>"]}}
    hits = [hit]
    monkeypatch.setattr(
        run, "call", lambda *_: {"responses": [{"hits": {"hits": hits}}]}
    )
    assert run.query("published")["hits"] == 1
    hit.pop("_highlights")
    hit["highlight"] = {"body": ["wrong response contract"]}
    with pytest.raises(AssertionError, match="omitted requested highlights"):
        run.query("published")
    hits.append(hit)
    with pytest.raises(AssertionError, match="duplicate HN keys"):
        run.query("published")


def test_transport_evidence_pairs_attempt_start_and_failure_without_exception_text(
    run, monkeypatch
):
    def lost(*args, **kwargs):
        raise lake.URLError("SECRET transport detail")

    monkeypatch.setattr(lake, "open_request", lost)
    for _ in range(2):
        with pytest.raises(lake.RequestFailure):
            run.call("GET", "/tables")
    evidence = (run.state / "events.jsonl").read_text()
    assert "SECRET" not in evidence
    rows = [json.loads(line) for line in evidence.splitlines()]
    assert [(row["kind"], row["request_id"]) for row in rows] == [
        ("request_start", 1),
        ("request_failure", 1),
        ("request_start", 2),
        ("request_failure", 2),
    ]


def test_short_smoke_still_exits_nonzero_after_search_failures(tmp_path, monkeypatch):
    args = SimpleNamespace(run=True, state=tmp_path / "attempt")
    monkeypatch.setattr(lake, "arguments", lambda: args)
    monkeypatch.setattr(
        lake,
        "Run",
        lambda _: SimpleNamespace(
            run=lambda: {
                "full_profile": False,
                "checks": {"queries_succeeded": False},
            }
        ),
    )
    with pytest.raises(SystemExit, match="search failures observed"):
        lake.main()


@pytest.mark.parametrize(
    "option,value",
    [("--query-workers", "0"), ("--query-workers", "9")]
    + [
        (option, value)
        for option in (
            "--update-interval",
            "--query-interval",
            "--maintenance-interval",
        )
        for value in ("nan", "inf", "-inf")
    ],
)
def test_workload_pacing_is_finite_and_query_fanout_is_bounded(
    tmp_path, monkeypatch, option, value
):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "continuous_lake.py",
            "--binary",
            "antfly",
            "--engine-commit",
            "e" * 40,
            "--fixture",
            "seed.parquet",
            "--state",
            str(tmp_path / "state"),
            option + "=" + value,
        ],
    )
    with pytest.raises(SystemExit):
        lake.arguments()


def test_builtin_metadata_admission_failure_keeps_only_public_code():
    failure = lake.RequestFailure.http(
        "POST",
        "/query",
        503,
        b'{"code":"metadata_leader_unavailable","message":"SECRET", "retryable":true}',
        5000,
    )
    assert failure.record["code"] == "metadata_leader_unavailable"
    assert failure.record["retryable"] is True
    assert "SECRET" not in json.dumps(failure.record)


def test_stalled_workers_cannot_keep_the_engine_past_the_workload_deadline(
    run, monkeypatch
):
    release = lake.threading.Event()
    run.args.duration_seconds = 1
    run.args.query_workers = 1
    run.deadline = lake.time.monotonic() + 0.01
    monkeypatch.setattr(run, "worker", lambda _: release.wait(2))
    try:
        with pytest.raises(TimeoutError, match="workers exceeded wall-clock limit"):
            run.continuous([])
        assert run.stop.is_set()
        assert all(thread.daemon for thread in run.threads)
    finally:
        release.set()
        for thread in run.threads:
            thread.join(2)


def test_stalled_credential_helper_has_a_bound_and_redacts_captured_output(monkeypatch):
    def hung(command, **options):
        assert 0 < options["timeout"] <= 60
        raise lake.subprocess.TimeoutExpired(
            command, options["timeout"], output="SECRET", stderr="SECRET"
        )

    monkeypatch.setattr(lake.subprocess, "run", hung)
    with pytest.raises(RuntimeError, match="Unable to acquire") as failure:
        lake.gcs_bearer("qualification")
    assert "SECRET" not in str(failure.value)


def test_independent_watchdog_stops_the_owned_process_and_fences_restart(run):
    terminated = lake.threading.Event()

    class Process:
        def poll(self):
            return 0 if terminated.is_set() else None

        def terminate(self):
            terminated.set()

        def wait(self, timeout):
            assert terminated.is_set()

    run.process = Process()
    run.deadline = lake.time.monotonic() + 0.01
    watchdog = run.deadline_watchdog()
    try:
        assert terminated.wait(2)
        assert run.stop.is_set()
        with pytest.raises(TimeoutError, match="wall-clock limit"):
            run.start()
    finally:
        watchdog.cancel()
        watchdog.join(2)


@pytest.mark.parametrize("listening", [True, False])
def test_foreign_or_reserved_port_prevents_daemon_and_api_admission(
    run, monkeypatch, listening
):
    class Socket:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def settimeout(self, value):
            pass

        def connect_ex(self, address):
            return 0 if listening else lake.errno.ECONNREFUSED

        def setsockopt(self, *args):
            pass

        def bind(self, address):
            raise OSError("SECRET reserved listener detail")

    monkeypatch.setattr(lake.socket, "socket", lambda *_: Socket())
    monkeypatch.setattr(
        lake.subprocess, "Popen", lambda *a, **kw: pytest.fail("must not start")
    )
    with pytest.raises(RuntimeError, match="listen ports are unavailable") as failure:
        run.start()
    assert "SECRET" not in str(failure.value)


def test_worker_failure_stops_the_engine_without_waiting_for_other_workers(
    run, monkeypatch
):
    stopped = lake.threading.Event()
    monkeypatch.setattr(run, "stop_server", stopped.set)

    def exceeds_storage_guard():
        raise AssertionError("qualification object storage envelope exceeded")

    run.worker(exceeds_storage_guard)
    assert stopped.is_set()
    assert run.stop.is_set()
    assert run.failures == [
        {
            "type": "AssertionError",
            "code": "qualification object storage envelope exceeded",
        }
    ]


def test_seed_reader_projects_columns_and_stops_at_the_row_budget(monkeypatch):
    import pyarrow.parquet as pq

    closed = []

    class File:
        schema_arrow = SimpleNamespace(names=["hn_id", "body", "unused"])

        def __enter__(self):
            return self

        def __exit__(self, *args):
            closed.append(True)

        def iter_batches(self, *, batch_size, columns):
            assert batch_size == 2
            assert columns == ["hn_id", "body"]
            yield SimpleNamespace(
                to_pylist=lambda: [
                    {"hn_id": 1, "body": "first"},
                    {"hn_id": 2, "body": "second"},
                ]
            )
            pytest.fail("seed reader advanced beyond its bounded sample")

    monkeypatch.setattr(pq, "ParquetFile", lambda _: File())
    monkeypatch.setattr(
        pq, "read_table", lambda *_: pytest.fail("must not read the full archive")
    )
    assert len(lake.fixture(Path("archive.parquet"), 2)) == 2
    assert closed == [True]


@pytest.mark.parametrize("duplicate_version", [3, 4])
def test_final_accepted_sql_oracle_rejects_duplicate_keys(duplicate_version):
    with pytest.raises(AssertionError, match="duplicate HN keys"):
        lake.accepted_sql_versions([[7, 3], [7, duplicate_version]])
    assert lake.accepted_sql_versions([[7, 3], [8, 4]]) == {7: 3, 8: 4}


@pytest.mark.parametrize("deleted", [False, True])
def test_live_accepted_search_rejects_acknowledged_stale_or_deleted_rows(
    run, monkeypatch, deleted
):
    def row(key, version):
        return {
            name: key
            if name == "hn_id"
            else version
            if name == "qual_version"
            else 0
            if kind == "integer"
            else "database"
            for name, kind in lake.KINDS.items()
        }

    anchor, old, new = row(1, 0), row(2, 0), row(2, 1)
    run.anchor_id = 1
    run.known = {(1, 0): anchor, (2, 0): old, (2, 1): new}
    run.expected = {1: anchor} if deleted else {1: anchor, 2: new}
    run.transactions[lake.TABLES[1]] = [{}, {}]
    run.search_intents = [(0, ((1, 0), (2, 0))), (0, ((2, None if deleted else 1),))]
    monkeypatch.setattr(
        run,
        "call",
        lambda *_: {
            "responses": [
                {
                    "hits": {
                        "hits": [
                            {"_source": value, "_highlights": {"body": ["database"]}}
                            for value in (anchor, old)
                        ]
                    }
                }
            ]
        },
    )
    with pytest.raises(AssertionError, match="consistent acknowledged snapshot"):
        run.query("accepted")
    assert run.query("published")["hits"] == 2


def test_accepted_snapshot_allows_pending_atomic_writes_but_rejects_torn_batches():
    baseline = {1: 0, 2: 0, 3: 0}
    intents = [((2, 1), (3, 1)), ((2, None),), ((2, 3),)]
    for actual in (baseline, {1: 0, 2: 1, 3: 1}, {1: 0, 3: 1}, {1: 0, 2: 3, 3: 1}):
        lake.require_accepted_snapshot(baseline, intents, actual)
    with pytest.raises(AssertionError, match="consistent acknowledged snapshot"):
        lake.require_accepted_snapshot(baseline, intents, {1: 0, 2: 1, 3: 0})
    assert baseline == {1: 0, 2: 0, 3: 0}


def test_live_accepted_search_allows_a_write_in_flight_at_invocation(run, monkeypatch):
    anchor = {
        name: 1 if kind == "integer" else "database"
        for name, kind in lake.KINDS.items()
    }
    old = dict(anchor, hn_id=2, qual_version=0)
    new = dict(old, qual_version=1)
    run.anchor_id = 1
    run.expected = {1: anchor, 2: old}
    run.known = {(1, 1): anchor, (2, 0): old, (2, 1): new}
    run.transactions[lake.TABLES[1]] = [{}]
    run.search_intents = [(0, ((1, 1), (2, 0))), (0, ((2, 1),))]
    monkeypatch.setattr(
        run,
        "call",
        lambda *_: {
            "responses": [
                {
                    "hits": {
                        "hits": [
                            {"_source": value, "_highlights": {"body": ["database"]}}
                            for value in (anchor, new)
                        ]
                    }
                }
            ]
        },
    )
    assert run.query("accepted")["row_versions"] == {"1": 1, "2": 1}


@pytest.mark.parametrize(
    "rows",
    [
        [[7.9, 3.9]],
        [[7.0, 3]],
        [[7, "3"]],
        [[True, 3]],
        [[7, False]],
        [[7, None]],
        [[7]],
        [[7, 3, 4]],
        ["73"],
        [{0: 7, 1: 3}],
        {7: 3},
    ],
)
def test_sql_oracle_rejects_lossy_values_and_invalid_row_shapes(rows):
    with pytest.raises(AssertionError):
        lake.accepted_sql_versions(rows)
    assert lake.accepted_sql_versions([[2**63 - 1, 3], [-7, 0]]) == {
        2**63 - 1: 3,
        -7: 0,
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("hn_id", 7.9),
        ("hn_id", True),
        ("qual_version", 3.9),
        ("qual_version", "3"),
        ("points", False),
        ("created_at", 0.0),
        ("title", 7),
    ],
)
def test_search_oracle_rejects_numeric_coercion_and_wrong_field_types(field, value):
    row = search_row(7, 3)
    assert lake.search_versions([{"_source": row}]) == {7: 3}
    row[field] = value
    with pytest.raises(AssertionError, match="invalid typed row"):
        lake.search_versions([{"_source": row}])


@pytest.fixture
def search_engine(run, monkeypatch):
    rows, queries = {}, []

    def call(method, route, body):
        if route.endswith("/changes"):
            for change in body["changes"]:
                row = change["row"]
                if change["op"] == "delete":
                    rows.pop(row["hn_id"], None)
                else:
                    rows[row["hn_id"]] = dict(row)
            return {
                "state": "accepted",
                "wal_lsn": len(run.transactions[lake.TABLES[1]]) + 1,
            }
        assert method == "POST" and route.endswith("/query")
        queries.append(copy.deepcopy(body))
        return {
            "responses": [
                {
                    "hits": {
                        "hits": [
                            {
                                "_source": dict(row),
                                "_highlights": {"body": ["database"]},
                            }
                            for row in rows.values()
                        ]
                    }
                }
            ]
        }

    monkeypatch.setattr(run, "call", call)
    run.anchor_id = 1
    run.transaction(
        lake.TABLES[1], [{"op": "upsert", "row": search_row(key, 0)} for key in (1, 2)]
    )
    return rows, queries


def change(run, operation, key=2, version=1):
    return run.transaction(
        lake.TABLES[1], [{"op": operation, "row": search_row(key, version)}]
    )


def test_qualification_requires_actual_verified_churn_even_when_vacuum_succeeds(
    run, search_engine
):
    args = SimpleNamespace(
        history_commits=100, burst_transactions=2, duration_seconds=660, max_updates=1
    )
    old_checks = {
        name: True
        for name in (
            "final_rows",
            "queries_succeeded",
            "retention_window_exercised",
            "manifest_merge_observed",
            "group_commit_observed",
            "vacuum_progress_during_continuous",
            "vacuum_deleted_objects",
            "independent_deletion_observed",
        )
    }
    receipt = change(run, "upsert")
    run.verify_search_churn(receipt)
    coverage = run.search_churn_coverage()
    assert coverage == {
        "acknowledged": {"update": 1, "delete": 0},
        "accepted_verified": {"update": 1, "delete": 0},
    }
    result = lake.qualification_result(args, old_checks, coverage)
    assert result["full_profile"] and not result["qualified"]
    assert not result["checks"]["search_delete_acknowledged"]
    receipt = change(run, "delete")
    assert not lake.qualification_result(args, old_checks, run.search_churn_coverage())[
        "qualified"
    ]
    run.verify_search_churn(receipt)
    assert lake.qualification_result(args, old_checks, run.search_churn_coverage())[
        "qualified"
    ]
    assert all(old_checks.values())  # Reporting never mutates the other checks.


def test_seed_missing_delete_and_atomic_replacement_do_not_fabricate_delete_coverage(
    run, search_engine
):
    assert run.search_churn_coverage()["acknowledged"] == {"update": 0, "delete": 0}
    change(run, "delete", key=9)
    run.transaction(
        lake.TABLES[1],
        [
            {"op": "delete", "row": {"hn_id": 2}},
            {"op": "upsert", "row": search_row(2, 1)},
        ],
    )
    run.query("accepted")
    assert run.search_churn_coverage() == {
        "acknowledged": {"update": 1, "delete": 0},
        "accepted_verified": {"update": 1, "delete": 0},
    }


def test_published_and_pre_ack_reads_do_not_verify_a_later_delete(
    run, search_engine, monkeypatch
):
    original = run.call

    def racing_call(method, route, body):
        result = original(method, route, body)
        if route.endswith("/query"):
            change(run, "delete")
        return result

    monkeypatch.setattr(run, "call", racing_call)
    run.query(
        "accepted"
    )  # Valid earlier snapshot; the delete ACK arrives during this read.
    assert run.search_churn_coverage()["accepted_verified"]["delete"] == 0
    monkeypatch.setattr(run, "call", original)
    run.query("published")
    assert run.search_churn_coverage()["accepted_verified"]["delete"] == 0
    run.query("accepted")
    assert run.search_churn_coverage()["accepted_verified"]["delete"] == 1


def test_reinsert_does_not_retroactively_verify_a_missed_delete(run, search_engine):
    change(run, "delete")
    change(run, "upsert", version=2)
    run.query("accepted")
    assert run.search_churn_coverage()["acknowledged"]["delete"] == 1
    assert run.search_churn_coverage()["accepted_verified"]["delete"] == 0


def test_first_update_and_delete_witnesses_add_only_two_reads(run, search_engine):
    _, queries = search_engine
    for version in range(1, 5):
        run.verify_search_churn(change(run, "upsert", version=version))
    run.verify_search_churn(change(run, "delete"))
    run.verify_search_churn(change(run, "upsert", version=6))
    run.verify_search_churn(change(run, "delete"))
    assert len(queries) == 2
    assert all(
        sample["purpose"] == "search_churn_verification" for sample in run.samples
    )
    assert run.search_churn_coverage()["accepted_verified"] == {
        "update": 1,
        "delete": 1,
    }


def test_failed_witness_read_retains_failure_and_cannot_qualify(
    run, search_engine, monkeypatch
):
    receipt = change(run, "delete")

    def fail(*_):
        raise lake.RequestFailure.transport("POST", "/query", TimeoutError(), 1)

    monkeypatch.setattr(run, "call", fail)
    with pytest.raises(lake.RequestFailure):
        run.verify_search_churn(receipt)
    assert run.search_churn_coverage()["accepted_verified"]["delete"] == 0
    assert run.samples[-1]["purpose"] == "search_churn_verification"
    assert run.samples[-1]["failure"]["code"] == "transport_error"


def test_invalid_highlights_do_not_credit_churn_verification(
    run, search_engine, monkeypatch
):
    change(run, "upsert")
    original = run.call

    def incomplete(*args):
        result = original(*args)
        for hit in result["responses"][0]["hits"]["hits"]:
            hit.pop("_highlights")
        return result

    monkeypatch.setattr(run, "call", incomplete)
    with pytest.raises(AssertionError, match="omitted requested highlights"):
        run.query("accepted")
    assert run.search_churn_coverage()["accepted_verified"]["update"] == 0


@pytest.mark.parametrize("lsn", [True, False, 0, -1, 1.0, "1"])
def test_non_integer_or_non_positive_wal_receipts_do_not_count_churn(
    run, monkeypatch, lsn
):
    run.expected = {2: search_row(2, 0)}
    monkeypatch.setattr(run, "call", lambda *_: {"state": "accepted", "wal_lsn": lsn})
    with pytest.raises(AssertionError, match="durable receipt"):
        change(run, "delete")
    assert run.search_churn_coverage()["acknowledged"]["delete"] == 0


@pytest.mark.parametrize(
    "history,burst,duration", [(99, 2, 660), (100, 1, 660), (100, 2, 659)]
)
def test_verified_churn_cannot_upgrade_a_short_profile(history, burst, duration):
    coverage = {
        phase: {"update": 1, "delete": 1}
        for phase in ("acknowledged", "accepted_verified")
    }
    args = SimpleNamespace(
        history_commits=history, burst_transactions=burst, duration_seconds=duration
    )
    result = lake.qualification_result(args, {"final_rows": True}, coverage)
    assert not result["full_profile"] and not result["qualified"]


@pytest.mark.parametrize("failed_check", [False, None])
def test_verified_churn_cannot_mask_other_qualification_failures(failed_check):
    coverage = {
        phase: {"update": 1, "delete": 1}
        for phase in ("acknowledged", "accepted_verified")
    }
    args = SimpleNamespace(
        history_commits=100, burst_transactions=2, duration_seconds=660
    )
    result = lake.qualification_result(
        args, {"queries_succeeded": failed_check}, coverage
    )
    assert result["full_profile"] and not result["qualified"]
