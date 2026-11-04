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
    assert not (run.state / (lake.TABLES[1] + "-pending.json")).exists()


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
    run.known[(42, 1)] = {"body": "database expected"}
    monkeypatch.setattr(
        run,
        "call",
        lambda *_: {
            "responses": [
                {
                    "hits": {
                        "hits": [
                            {
                                "_source": {
                                    "hn_id": 42,
                                    "qual_version": 1,
                                    "body": "database CORRUPT",
                                },
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


@pytest.mark.parametrize("workers", [0, 9])
def test_query_fanout_is_bounded(tmp_path, monkeypatch, workers):
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
            "--query-workers",
            str(workers),
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
