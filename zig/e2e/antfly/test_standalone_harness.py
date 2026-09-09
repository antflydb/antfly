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

"""Fast regression tests for the standalone inference test harness."""

import pytest
import requests
import os
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace

import conftest as e2e_conftest
import helpers
import test_backup_restore as backups
import test_standalone as standalone


@pytest.mark.parametrize(
    "failure_phase, preservation, retained",
    [
        ("none", "failure", False),
        ("setup", "failure", True),
        ("call", "failure", True),
        ("teardown", "failure", True),
        ("earlier_call", "failure", True),
        ("teardown", "never", False),
        ("none", "always", True),
        ("cleanup", "never", False),
    ],
)
def test_cli_runtime_preservation_uses_completed_module_reports(
    tmp_path, failure_phase, preservation, retained
):
    # Run real pytest finalizers and the real CLI fixture: a mocked report cannot
    # expose the ordering between module shutdown and the last teardown report.
    probe = tmp_path / "test_cli_preservation.py"
    probe.write_text("""
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace

import pytest
import conftest as harness
import test_cli as cli_tests

cli_server = cli_tests.cli_server
phase = os.environ["PROBE_FAILURE_PHASE"]

@pytest.fixture(scope="module")
def cli_inference_servers():
    def server_factory(*args):
        server = object.__new__(harness.StandaloneAntflyServer)
        server.tempdir = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        root = Path(server.tempdir.name)
        Path("runtime-path").write_text(str(root))
        server.log_file = (root / "server.log").open("w")
        server.log_file.write("retained diagnostics")
        server.proc = None
        server.port_reservations = SimpleNamespace(close=lambda: None)
        server._stop_process = lambda: Path("process-stopped").touch()
        if phase == "cleanup":
            def cleanup_error():
                raise OSError("injected cleanup failure")
            server.tempdir.cleanup = cleanup_error
        return server

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("ANTFLY_BIN", sys.executable)
        patch.setattr(cli_tests, "find_free_port", lambda: 0)
        patch.setattr(cli_tests, "StandaloneAntflyServer", server_factory)
        yield {}
        # This dependency finalizes after cli_server has stopped its process.
        assert Path("process-stopped").exists()
        assert Path(Path("runtime-path").read_text()).exists()
        if phase == "teardown":
            raise RuntimeError("injected teardown failure")

@pytest.fixture
def setup_probe(cli_server):
    if phase == "setup":
        raise RuntimeError("injected setup failure")

def test_first(cli_server):
    assert phase != "earlier_call", "injected earlier call failure"

def test_last(cli_server, setup_probe):
    assert phase != "call", "injected call failure"
""")
    env = os.environ.copy()
    env.update(
        PYTHONPATH=str(Path(e2e_conftest.__file__).parent),
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
        PROBE_FAILURE_PHASE=failure_phase,
        ANTFLY_E2E_PRESERVE_ROOT="1" if preservation == "always" else "0",
        ANTFLY_E2E_PRESERVE_ROOT_ON_FAILURE=("1" if preservation == "failure" else "0"),
    )
    env.pop("PYTEST_ADDOPTS", None)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-p",
            "xdist.plugin",
            "-p",
            "conftest",
            "--confcutdir",
            str(tmp_path),
            "-q",
            str(probe),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    output = result.stdout + result.stderr
    assert "INTERNALERROR" not in output, output
    assert result.returncode == (0 if failure_phase == "none" else 1), output
    if failure_phase != "none":
        assert "injected" in output, output
    root = Path((tmp_path / "runtime-path").read_text())
    assert root.exists() is retained, output
    if retained:
        assert (root / "server.log").read_text() == "retained diagnostics"


@pytest.mark.parametrize("total", [10 * 1024**3, 1024**4])
def test_storage_preflight_matches_absolute_and_fractional_safety_floor(
    monkeypatch, tmp_path, total
):
    floor = max(1024**3, total // 20)
    required = floor + 256 * 1024**2
    observation = SimpleNamespace(total=total, free=required - 1)
    paths = []

    def disk_usage(path):
        paths.append(path)
        return observation

    monkeypatch.setattr(e2e_conftest.shutil, "disk_usage", disk_usage)
    with pytest.raises(
        RuntimeError, match="Insufficient E2E storage headroom"
    ) as error:
        e2e_conftest.require_standalone_storage_headroom(tmp_path)
    assert f"safety_floor_bytes={floor}" in str(error.value)
    assert "TMPDIR" in str(error.value)
    observation.free = required
    e2e_conftest.require_standalone_storage_headroom(tmp_path)
    assert paths == [tmp_path, tmp_path]


def test_storage_preflight_does_not_hide_observation_failure(monkeypatch, tmp_path):
    def unavailable(_):
        raise OSError("capacity observation failed")

    monkeypatch.setattr(e2e_conftest.shutil, "disk_usage", unavailable)
    with pytest.raises(OSError, match="capacity observation failed"):
        e2e_conftest.require_standalone_storage_headroom(tmp_path)


def test_model_preflight_recognizes_atomic_variant_publication(tmp_path):
    models_dir = tmp_path / "models"
    published = models_dir / "ggml-org" / "gemma-4-e2b-it-gguf--antfly-0123456789abcdef"
    published.mkdir(parents=True)

    assert standalone._model_exists(models_dir, "ggml-org/gemma-4-e2b-it-gguf")


def test_model_preflight_rejects_incomplete_variant_names(tmp_path):
    models_dir = tmp_path / "models"
    incomplete = (
        models_dir
        / "generators"
        / "ggml-org"
        / "gemma-4-e2b-it-gguf--antfly-0123456789abcdef.tmp"
    )
    incomplete.mkdir(parents=True)

    assert not standalone._model_exists(models_dir, "ggml-org/gemma-4-e2b-it-gguf")


def test_warmup_uses_configured_first_use_deadline(monkeypatch):
    observed: dict[str, object] = {}

    class Response:
        status_code = 200
        text = ""

    def post(url: str, **kwargs):
        observed["url"] = url
        observed["timeout"] = kwargs["timeout"]
        return Response()

    monkeypatch.setattr(standalone.requests, "post", post)

    standalone._warm_inference_generator(
        "http://127.0.0.1:8080/ai/v1",
        "test/model",
        request_timeout=1800.0,
    )

    assert observed == {
        "url": "http://127.0.0.1:8080/ai/v1/generate",
        "timeout": 1800.0,
    }


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "invalid"])
def test_first_use_deadline_rejects_invalid_values(monkeypatch, value):
    monkeypatch.setenv("ANTFLY_INFERENCE_FIRST_USE_REQUEST_TIMEOUT", value)

    with pytest.raises(
        ValueError,
        match="ANTFLY_INFERENCE_FIRST_USE_REQUEST_TIMEOUT must be a positive finite number",
    ):
        standalone._positive_timeout(
            "ANTFLY_INFERENCE_FIRST_USE_REQUEST_TIMEOUT",
            standalone.DEFAULT_INFERENCE_STANDALONE_FIRST_USE_REQUEST_TIMEOUT,
        )


class _FakeStandaloneServer:
    def __init__(
        self,
        *,
        forced_kill: bool = False,
        returncode: int = 0,
        stop_failure: Exception | None = None,
    ):
        self.forced_kill = forced_kill
        self.returncode = returncode
        self.stop_failure = stop_failure
        self.final_logs = "standalone diagnostic logs"
        self.stop_calls = 0

    def stop(self) -> None:
        self.stop_calls += 1
        if self.stop_failure is not None:
            raise self.stop_failure


def test_cleanup_preserves_primary_failure():
    server = _FakeStandaloneServer(returncode=1)
    primary_failure = RuntimeError("warmup timed out")

    standalone._finish_standalone_server(server, primary_failure)

    assert server.stop_calls == 1
    assert primary_failure.__notes__ == [
        "standalone inference did not shut down cleanly "
        "(forced_kill=False, returncode=1)\n"
        "last logs:\nstandalone diagnostic logs"
    ]


def test_cleanup_exception_is_attached_to_primary_failure():
    server = _FakeStandaloneServer(stop_failure=OSError("cleanup failed"))
    primary_failure = RuntimeError("warmup timed out")

    standalone._finish_standalone_server(server, primary_failure)

    assert server.stop_calls == 1
    assert primary_failure.__notes__ == [
        "standalone inference cleanup raised OSError: cleanup failed"
    ]


def test_cleanup_failure_is_primary_after_successful_test():
    server = _FakeStandaloneServer(forced_kill=True, returncode=-9)

    with pytest.raises(AssertionError, match="did not shut down cleanly"):
        standalone._finish_standalone_server(server, None)

    assert server.stop_calls == 1


def test_request_failure_attaches_one_bounded_server_log_tail():
    logs = "old-log-entry\n" + ("x" * 25_000) + "\nactionable-tail"

    class Server:
        def debug_logs(self) -> str:
            return logs

    original = requests.HTTPError(
        "503 Service Unavailable body=index_rebuilding\n"
        "server logs:\nprevious-unbounded-copy"
    )

    with pytest.raises(requests.HTTPError) as raised:
        e2e_conftest.raise_request_error_with_logs(original, Server())

    message = str(raised.value)
    assert raised.value is original
    assert message.count("server logs:") == 1
    assert "previous-unbounded-copy" not in message
    assert "old-log-entry" not in message
    omitted = len(logs) - e2e_conftest.FAILURE_LOG_TAIL_LIMIT
    assert f"omitted {omitted} earlier server-log characters" in message
    assert message.endswith("actionable-tail")
    assert raised.value.__cause__ is None


def _http_error(status: int, body: bytes, **headers: str) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status
    response._content = body
    response.headers.update(headers)
    return requests.HTTPError(response=response)


def _cleanup_api(monkeypatch, outcomes, *, exit_status=None):
    calls = []
    pending = iter(outcomes)

    def delete(url, **kwargs):
        calls.append((url, kwargs))
        outcome = next(pending)
        if isinstance(outcome, Exception):
            raise outcome
        return _http_error(outcome, b"delete response").response

    monkeypatch.setattr(e2e_conftest.time, "sleep", lambda _: None)
    server = SimpleNamespace(
        proc=SimpleNamespace(poll=lambda: exit_status),
        debug_logs=lambda: "cleanup server diagnostics",
    )
    return (
        SimpleNamespace(
            s=SimpleNamespace(delete=delete),
            url="http://localhost/api/v1",
            _server=server,
            _request_lock=threading.Lock(),
        ),
        calls,
    )


@pytest.mark.parametrize("status", [200, 202, 204, 404])
def test_table_cleanup_retries_reset_including_already_deleted(monkeypatch, status):
    api, calls = _cleanup_api(
        monkeypatch, [requests.ConnectionError("connection reset"), status]
    )
    assert e2e_conftest._cleanup_created_tables(api, {"table/a"}) == []
    assert len(calls) == 2
    assert all(url.endswith("/tables/table%2Fa") for url, _ in calls)


def test_table_cleanup_stops_retrying_and_keeps_diagnostics(monkeypatch):
    api, calls = _cleanup_api(
        monkeypatch, [requests.ConnectionError("connection reset")] * 3 + [204]
    )
    errors = e2e_conftest._cleanup_created_tables(api, {"z_table", "a_table"})
    assert len(errors) == 1
    assert "z_table: connection reset" in errors[0]
    assert "cleanup server diagnostics" in errors[0]
    assert "proc: None" in errors[0]
    assert len(calls) == 4  # Other owned tables still get cleaned up.


def test_table_cleanup_preserves_http_failures_without_retry(monkeypatch):
    api, calls = _cleanup_api(monkeypatch, [500])
    errors = e2e_conftest._cleanup_created_tables(api, {"table"})
    assert len(errors) == 1
    assert "HTTP 500 delete response" in errors[0]
    assert len(calls) == 1


def test_table_cleanup_rejects_exited_server_before_request(monkeypatch):
    api, calls = _cleanup_api(monkeypatch, [204], exit_status=-11)
    errors = e2e_conftest._cleanup_created_tables(api, {"table"})
    assert len(errors) == 1
    assert "proc: -11" in errors[0]
    assert "cleanup server diagnostics" in errors[0]
    assert calls == []


@pytest.mark.parametrize("outcome", [requests.ConnectionError("connection reset"), 204])
def test_table_cleanup_does_not_hide_server_crash(monkeypatch, outcome):
    api, calls = _cleanup_api(monkeypatch, [outcome, 204])
    api._server.proc.poll = lambda: -11 if calls else None
    errors = e2e_conftest._cleanup_created_tables(api, {"table"})
    assert len(errors) == 1
    assert "proc: -11" in errors[0]
    assert len(calls) == 1


def test_table_cleanup_does_not_retry_after_deadline(monkeypatch):
    api, calls = _cleanup_api(monkeypatch, [requests.Timeout("delete timed out"), 204])
    clock = [0.0]
    monkeypatch.setattr(e2e_conftest.time, "monotonic", lambda: clock[0])
    delete = api.s.delete

    def timed_out_delete(*args, **kwargs):
        clock[0] = 30.0
        return delete(*args, **kwargs)

    api.s.delete = timed_out_delete
    errors = e2e_conftest._cleanup_created_tables(api, {"table"})
    assert len(errors) == 1
    assert "delete timed out" in errors[0]
    assert len(calls) == 1
    assert calls[0][1]["timeout"] == 30
    assert not api._request_lock.locked()


def test_table_cleanup_does_not_retry_when_sleep_passes_deadline(monkeypatch):
    api, calls = _cleanup_api(monkeypatch, [requests.ConnectionError("reset"), 204])
    clock = [0.0]
    monkeypatch.setattr(e2e_conftest.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        e2e_conftest.time, "sleep", lambda _: clock.__setitem__(0, 31.0)
    )
    errors = e2e_conftest._cleanup_created_tables(api, {"table"})
    assert len(errors) == 1
    assert "deadline expired" in errors[0]
    assert len(calls) == 1
    assert not api._request_lock.locked()


@pytest.mark.parametrize(
    "delay, acquired, request_timeout",
    [(30.0, False, None), (31.0, True, None), (29.0, True, 1.0)],
)
def test_table_cleanup_bounds_lock_wait_and_rechecks_deadline(
    monkeypatch, delay, acquired, request_timeout
):
    api, calls = _cleanup_api(monkeypatch, [204])
    clock = [0.0]
    monkeypatch.setattr(e2e_conftest.time, "monotonic", lambda: clock[0])

    class ContendedLock:
        releases = 0

        def acquire(self, *, timeout=-1):
            assert timeout == 30.0, "cleanup must bound the lock wait"
            clock[0] += delay
            return acquired

        def release(self):
            self.releases += 1

        def __enter__(self):
            self.acquire()

        def __exit__(self, *args):
            self.release()

    lock = ContendedLock()
    api._request_lock = lock
    errors = e2e_conftest._cleanup_created_tables(api, {"table"})
    assert lock.releases == int(acquired)
    if request_timeout is None:
        assert calls == []
        assert len(errors) == 1
        assert "cleanup server diagnostics" in errors[0]
        assert "request lock" in errors[0] or "deadline" in errors[0]
    else:
        assert errors == []
        assert len(calls) == 1
        assert calls[0][1]["timeout"] == request_timeout


def _seed_cluster(monkeypatch, outcomes):
    calls = []
    pending = iter(outcomes)
    clock = [0.0]
    monkeypatch.setattr(backups.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        backups.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay)
    )

    def post(url, **kwargs):
        calls.append(kwargs)
        outcome = next(pending)
        if isinstance(outcome, Exception):
            raise outcome
        status, body, *headers = outcome
        response = requests.Response()
        response.status_code = status
        response._content = body
        response.url = url
        response.request = requests.Request("POST", url).prepare()
        if headers:
            response.headers.update(headers[0])
        return response

    return (
        SimpleNamespace(
            data_api_urls=["http://localhost/db/v1"],
            assert_processes_alive=lambda: None,
            debug_logs=lambda: "cluster write diagnostics",
        ),
        SimpleNamespace(post=post),
        calls,
    )


def _create_not_admitted():
    return (
        503,
        b'{"code":"metadata_leader_unavailable","retryable":true}',
        {"X-Antfly-Metadata-Mutation-Not-Admitted": "true", "Retry-After": "1"},
    )


@pytest.mark.parametrize("success_status", [200, 202])
def test_cluster_create_retries_only_proven_non_admission(
    monkeypatch, capsys, success_status
):
    cluster, session, calls = _seed_cluster(
        monkeypatch, [_create_not_admitted(), (success_status, b"{}")]
    )
    definition = {"num_shards": 3, "description": "backup"}
    assert (
        backups._create_cluster_table_when_admitted(
            cluster, session, "docs", definition
        )
        == {}
    )
    assert [call["json"] for call in calls] == [definition, definition]
    assert [call["timeout"] for call in calls] == [30.0, 29.0]
    assert "admitted after 2 attempts" in capsys.readouterr().out


@pytest.mark.parametrize(
    "outcome",
    [
        (
            409,
            b"table mutation outcome is unknown; observe table state before retrying",
        ),
        (409, b"table already exists"),
        (503, _create_not_admitted()[1]),
        (503, _create_not_admitted()[1], {"X-Antfly-Metadata-Not-Leader": "true"}),
        (
            503,
            _create_not_admitted()[1],
            {"X-Antfly-Metadata-Mutation-Not-Admitted": "false"},
        ),
        (
            503,
            _create_not_admitted()[1],
            {
                **_create_not_admitted()[2],
                "X-Antfly-Raft-Mutation-Outcome": "unknown-v1",
            },
        ),
        (
            503,
            _create_not_admitted()[1],
            {
                **_create_not_admitted()[2],
                "X-Antfly-Raft-Mutation-Outcome": "committed-v1",
            },
        ),
        (
            503,
            b'{"code":"metadata_leader_unavailable","retryable":false}',
            _create_not_admitted()[2],
        ),
        (
            503,
            b'{"code":"different_error","retryable":true}',
            _create_not_admitted()[2],
        ),
        (503, b"malformed response", _create_not_admitted()[2]),
        (500, b"internal failure"),
        requests.ConnectionError("response lost"),
        requests.Timeout("request timed out"),
    ],
)
def test_cluster_create_does_not_replay_uncertain_outcomes(monkeypatch, outcome):
    cluster, session, calls = _seed_cluster(monkeypatch, [outcome])
    with pytest.raises(AssertionError, match="cluster write diagnostics"):
        backups._create_cluster_table_when_admitted(cluster, session, "docs", {})
    assert len(calls) == 1


def test_cluster_create_deadline_bounds_requests_and_retains_rejection(monkeypatch):
    cluster, session, calls = _seed_cluster(monkeypatch, [_create_not_admitted()] * 3)
    with pytest.raises(AssertionError, match="admission deadline exceeded") as exc:
        backups._create_cluster_table_when_admitted(
            cluster, session, "docs", {}, timeout_s=2.5
        )
    assert [call["timeout"] for call in calls] == [2.5, 1.5, 0.5]
    assert "last_status=503" in str(exc.value)
    assert "cluster write diagnostics" in str(exc.value)


def test_cluster_create_does_not_send_after_backoff_overshoots_deadline(monkeypatch):
    cluster, session, calls = _seed_cluster(monkeypatch, [_create_not_admitted()])
    monkeypatch.setattr(
        backups.time,
        "sleep",
        lambda _: monkeypatch.setattr(backups.time, "monotonic", lambda: 31.0),
    )
    with pytest.raises(AssertionError, match="admission deadline exceeded"):
        backups._create_cluster_table_when_admitted(cluster, session, "docs", {})
    assert len(calls) == 1


def test_cluster_create_stops_when_server_exits(monkeypatch):
    cluster, session, calls = _seed_cluster(monkeypatch, [_create_not_admitted()])

    def assert_alive():
        if calls:
            raise RuntimeError("data server exited")

    cluster.assert_processes_alive = assert_alive
    with pytest.raises(RuntimeError, match="data server exited"):
        backups._create_cluster_table_when_admitted(cluster, session, "docs", {})
    assert len(calls) == 1


def test_cluster_seed_waits_for_precommit_write_admission(monkeypatch):
    cluster, session, calls = _seed_cluster(
        monkeypatch, [(503, b"write unavailable"), (200, b'{"inserted":1}')]
    )
    docs = {"doc:a": {"title": "a"}}
    assert backups._seed_cluster_docs_when_writable(cluster, session, "docs", docs) == {
        "inserted": 1
    }
    assert len(calls) == 2
    assert all(
        call["json"] == {"inserts": docs, "sync_level": "write"} for call in calls
    )


@pytest.mark.parametrize(
    "outcome",
    [
        (503, b"write committed locally; standby durability acknowledgment pending"),
        (409, b"transaction outcome unknown"),
        (500, b"internal failure"),
        (400, b"invalid batch request"),
        requests.ConnectionError("response lost"),
    ],
)
def test_cluster_seed_preserves_non_admission_failures(monkeypatch, outcome):
    cluster, session, calls = _seed_cluster(monkeypatch, [outcome])
    with pytest.raises((AssertionError, requests.ConnectionError)):
        backups._seed_cluster_docs_when_writable(cluster, session, "docs", {})
    assert len(calls) == 1


def test_cluster_seed_deadline_retains_cluster_diagnostics(monkeypatch):
    cluster, session, calls = _seed_cluster(
        monkeypatch, [(503, b"write unavailable")] * 3
    )
    with pytest.raises(AssertionError, match="cluster write diagnostics"):
        backups._seed_cluster_docs_when_writable(
            cluster, session, "docs", {}, timeout_s=0.25
        )
    assert len(calls) == 3
    assert calls[-1]["timeout"] < calls[0]["timeout"]


def test_cluster_seed_stops_when_server_exits(monkeypatch):
    cluster, session, calls = _seed_cluster(monkeypatch, [(503, b"write unavailable")])

    def assert_alive():
        if calls:
            raise RuntimeError("data server exited")

    cluster.assert_processes_alive = assert_alive
    with pytest.raises(RuntimeError, match="data server exited"):
        backups._seed_cluster_docs_when_writable(cluster, session, "docs", {})
    assert len(calls) == 1


def test_wait_until_retries_structured_retryable_service_unavailable():
    calls = 0

    def probe() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise _http_error(
                503,
                b'{"code":"index_rebuilding","retryable":true}',
                **{"Content-Type": "application/json", "Retry-After": "0"},
            )
        return "ready"

    assert helpers.wait_until(probe, timeout_s=1.0, interval_s=0) == "ready"
    assert calls == 2


def test_wait_until_preserves_nonretryable_service_unavailable():
    expected = _http_error(
        503,
        b'{"code":"storage_failed","retryable":false}',
        **{"Content-Type": "application/json"},
    )

    def probe() -> None:
        raise expected

    with pytest.raises(requests.HTTPError) as raised:
        helpers.wait_until(probe, timeout_s=1.0)

    assert raised.value is expected
