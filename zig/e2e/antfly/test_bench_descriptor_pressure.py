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

"""The qualification must reject incomplete commits and invalid query evidence."""

import os
from builtins import BaseExceptionGroup
from types import SimpleNamespace

import pytest

from bench_descriptor_pressure import (
    call,
    corpus,
    descriptor_reader,
    distribution,
    query,
)


def test_latency_percentiles_use_observed_tail_and_reject_empty_samples():
    values = [i / 1000 for i in range(1, 101)]
    result = distribution(reversed(values))
    assert result["p95_ms"] == 95
    assert result["p99_ms"] == 99
    assert result["max_ms"] == 100
    assert result["count"] == 100
    with pytest.raises(ValueError):
        distribution([])


def test_synthetic_rows_are_reproducible_varied_and_have_eight_permission_bits():
    first, second = corpus(None, 256), corpus(None, 256)
    assert first(0) == second(0)
    assert first(0) != first(1)
    assert len(first(0)[0]) == 768
    assert all(0 <= first(i)[1] < 256 for i in range(256))


def test_http_failure_is_not_retried_or_counted_as_a_commit():
    class Session:
        calls = 0

        def request(self, *_args, **_kwargs):
            self.calls += 1
            return SimpleNamespace(
                ok=False, status_code=503, text="storage_descriptor_pressure"
            )

    session = Session()
    with pytest.raises(RuntimeError, match="503.*storage_descriptor_pressure"):
        call(session, "http://unused", "POST", "/batch", {})
    assert session.calls == 1


def test_query_requires_the_ingested_self_match():
    class Session:
        def request(self, *_args, **_kwargs):
            return SimpleNamespace(
                ok=True,
                json=lambda: {"responses": [{"hits": {"hits": [{"_id": "wrong"}]}}]},
            )

    with pytest.raises(RuntimeError, match="self-query"):
        query(Session(), "http://unused", [1, 0])


def test_process_descriptor_sampler_counts_live_descriptors():
    read = descriptor_reader(os.getpid())
    assert read() >= 3


def test_linux_mapping_rss_includes_deleted_vectors_and_excludes_other_maps(
    tmp_path, monkeypatch
):
    import bench_descriptor_pressure as bench

    smaps = tmp_path / "smaps"
    smaps.write_text(
        "1000-2000 r--s 00000000 00:00 1 /blocks/a.afvb\nRss: 12 kB\n"
        "2000-3000 r--s 00000000 00:00 2 /blocks/b.afvb (deleted)\nRss: 8 kB\n"
        "3000-4000 rw-p 00000000 00:00 0 [heap]\nRss: 100 kB\n"
    )
    monkeypatch.setattr(bench.sys, "platform", "linux")
    monkeypatch.setattr(bench, "Path", lambda _: smaps)
    assert bench.vector_mapping_rss(123) == 20 * 1024


def test_server_wrapper_limits_only_child_and_preserves_spaced_arguments(tmp_path):
    import json
    import resource
    import subprocess
    import sys
    from pathlib import Path

    from bench_descriptor_pressure import write_server_wrapper

    wrapper = tmp_path / "server with spaces.sh"
    before = resource.getrlimit(resource.RLIMIT_NOFILE)
    write_server_wrapper(wrapper, Path(sys.executable), 256)
    code = "import json, resource, sys; print(json.dumps([resource.getrlimit(resource.RLIMIT_NOFILE)[0], sys.argv[1:]]))"
    result = subprocess.run(
        [str(wrapper), "-c", code, "argument with spaces"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert json.loads(result.stdout) == [256, ["argument with spaces"]]
    assert resource.getrlimit(resource.RLIMIT_NOFILE) == before


def test_queries_crossing_phase_boundaries_are_never_steady_samples():
    from bench_descriptor_pressure import QueryMeasurements

    now = [0.0]
    measurements = QueryMeasurements(clock=lambda: now[0])
    crossing = measurements.begin()
    now[0] = 2.0
    measurements.switch("steady")
    now[0] = 3.0
    measurements.finish(crossing)
    steady = measurements.begin()
    now[0] = 4.0
    measurements.finish(steady)
    measurements.switch("finished")
    # A worker may pass its stop check just before the parent ends the phase.
    trailing = measurements.begin()
    now[0] = 5.0
    measurements.finish(trailing)
    summary = measurements.summary()
    assert summary["ingestion"] is None
    assert summary["steady"]["count"] == 1
    assert summary["steady"]["max_ms"] == 1000
    assert summary["transition"]["count"] == 2
    assert summary["transition"]["max_ms"] == 3000


@pytest.fixture
def qualification_fakes(tmp_path, monkeypatch):
    import bench_descriptor_pressure as bench

    state = SimpleNamespace(servers=[], sessions=[], monitors=[], events=[])

    class FakeMonitor:
        def __init__(self, process):
            self.process = process
            self.samples = []
            self.vector_rss_samples = []
            self.closed = False
            self.failure = None
            state.monitors.append(self)

        def start(self):
            assert not self.process.ready
            state.events.append((self.process.pid, "sample"))
            # Recovery's startup peak must contribute to the overall result.
            self.samples.append(
                {"fds": self.process.pid * 100, "rss_bytes": self.process.pid * 10000}
            )
            self.vector_rss_samples.append({"rss_bytes": self.process.pid * 1000})

        def close(self):
            self.closed = True

    class FakeServer:
        def __init__(self, _binary, _host, _port, *, on_process_started, **_kwargs):
            self.observer = on_process_started
            self.api_url = "http://qualification.invalid"
            self.root = tmp_path / "server"
            self.root.mkdir()
            self.log_path = self.root / "server.log"
            self.log_path.write_text("server diagnostics")
            self.stopped = False
            state.servers.append(self)
            self.launch(1)

        def launch(self, pid):
            self.proc = SimpleNamespace(pid=pid, ready=False, returncode=None)
            self.observer(self.proc)
            self.proc.ready = True
            state.events.append((pid, "ready"))

        def pause(self):
            self.proc.returncode = 0

        def resume(self):
            self.launch(2)

        def stop(self, **_kwargs):
            self.stopped = True
            self.proc.returncode = 0

    class FakeSession:
        def __init__(self):
            self.closed = False
            state.sessions.append(self)

        def close(self):
            self.closed = True

        def __enter__(self):
            return self

        def __exit__(self, *_):
            self.close()

    monkeypatch.setattr(bench, "Monitor", FakeMonitor)
    monkeypatch.setattr(bench, "StandaloneAntflyServer", FakeServer)
    monkeypatch.setattr(bench.requests, "Session", FakeSession)
    monkeypatch.setattr(bench, "find_free_port", lambda: 1)
    return state


@pytest.mark.parametrize("recovery_peak", [200, 256])
def test_single_batch_qualifies_and_recovery_startup_samples_are_retained(
    tmp_path, monkeypatch, qualification_fakes, recovery_peak
):
    import json
    from contextlib import nullcontext
    import threading
    from pathlib import Path
    import bench_descriptor_pressure as bench

    started = threading.Event()
    release = threading.Event()
    visible_calls = []
    original_measurements = bench.QueryMeasurements

    class Measurements(original_measurements):
        def switch(self, phase):
            super().switch(phase)
            if phase == "steady":
                release.set()

    def query(*_args):
        started.set()
        assert release.wait(2)

    def visible(*_args, **_kwargs):
        if not visible_calls:
            assert started.wait(2)
        visible_calls.append(True)
        return {"total_indexed": 256, "query_visible_doc_count": 256}

    monkeypatch.setattr(bench, "QueryMeasurements", Measurements)
    monkeypatch.setattr(
        bench, "call", lambda *_args, **_kwargs: {"status": "committed"}
    )
    monkeypatch.setattr(bench, "query", query)
    monkeypatch.setattr(bench, "wait_visible", visible)
    original_start = bench.Monitor.start

    def start(monitor):
        original_start(monitor)
        if monitor.process.pid == 2:
            monitor.samples[0]["fds"] = recovery_peak

    monkeypatch.setattr(bench.Monitor, "start", start)
    outcome = (
        pytest.raises(RuntimeError, match="reached the soft limit")
        if recovery_peak == 256
        else nullcontext()
    )
    with outcome:
        bench.qualify(Path("/unused"), 256, 256, None, tmp_path, 0.06)
    result = json.loads((tmp_path / "limit-256.json").read_text())
    assert result["completed"] == (recovery_peak < 256)
    assert result["query_latency"]["ingestion"] is None
    assert result["query_latency"]["transition"]["count"] >= 1
    assert result["query_latency"]["steady"]["count"] >= 1
    assert result["peak_fds"] == recovery_peak
    assert result["peak_rss_bytes"] == 20000
    assert result["peak_vector_mapping_rss_bytes"] == 2000
    persisted = json.loads((tmp_path / "limit-256.json").read_text())
    assert persisted["phase_measurements"]["recovery"][
        "vector_mapping_rss_samples"
    ] == [{"rss_bytes": 2000}]
    assert qualification_fakes.events == [
        (1, "sample"),
        (1, "ready"),
        (2, "sample"),
        (2, "ready"),
    ]
    assert all(m.closed for m in qualification_fakes.monitors)
    assert all(s.closed for s in qualification_fakes.sessions)
    assert qualification_fakes.servers[0].stopped


@pytest.mark.parametrize(
    "failure", ["copy_log", "close_session", "unlink_wrapper", "write_report"]
)
def test_qualification_preserves_primary_error_and_cleans_every_resource(
    tmp_path, monkeypatch, qualification_fakes, failure
):
    from pathlib import Path
    import bench_descriptor_pressure as bench

    def fail(*_args, **_kwargs):
        raise OSError(f"injected {failure}")

    def request_failure(*_args, **_kwargs):
        raise RuntimeError("primary request failure")

    monkeypatch.setattr(bench, "call", request_failure)
    if failure == "copy_log":
        monkeypatch.setattr(bench.shutil, "copyfile", fail)
    elif failure == "close_session":
        # Register closure before returning a session whose close will fail.
        factory = bench.requests.Session

        def session():
            value = factory()
            value.close = fail
            return value

        monkeypatch.setattr(bench.requests, "Session", session)
    elif failure == "unlink_wrapper":
        original = Path.unlink

        def unlink(path, *args, **kwargs):
            if path.suffix == ".sh":
                fail()
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", unlink)
    else:
        original = Path.write_text

        def write(path, *args, **kwargs):
            if path.name == "limit-256.json":
                fail()
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", write)
    with pytest.raises(RuntimeError, match="primary request failure") as caught:
        bench.qualify(Path("/unused"), 256, 256, None, tmp_path, 0.01)
    assert qualification_fakes.servers[0].stopped
    assert all(m.closed for m in qualification_fakes.monitors)
    assert failure in " ".join(caught.value.__notes__)


@pytest.mark.parametrize("exited", [False, True])
def test_lifetime_monitor_only_ignores_disappearing_process_after_exit(
    monkeypatch, exited
):
    import bench_descriptor_pressure as bench

    polls = []

    def poll():
        polls.append(True)
        return 0 if exited and len(polls) > 1 else None

    def missing():
        raise FileNotFoundError("process disappeared during sampling")

    monkeypatch.setattr(bench, "descriptor_reader", lambda _pid: lambda: 3)
    monkeypatch.setattr(bench, "logical_process_reader", lambda _pid: missing)
    monitor = bench.Monitor(SimpleNamespace(pid=123, poll=poll))
    monitor.lookup_grace_seconds = 0
    monitor.run()
    assert (monitor.failure is None) == exited


def test_cleanup_failure_fails_an_otherwise_completed_qualification(
    tmp_path, monkeypatch, qualification_fakes
):
    import json
    import bench_descriptor_pressure as bench

    state = qualification_fakes
    measurements = bench.ProcessMeasurements()
    server = bench.StandaloneAntflyServer(
        "/unused", "", 1, on_process_started=measurements.start
    )
    session = bench.requests.Session()
    wrapper = tmp_path / "unused.sh"
    wrapper.touch()
    report = {"completed": True}

    def copy_failure(*_args):
        raise OSError("artifact destination full")

    monkeypatch.setattr(bench.shutil, "copyfile", copy_failure)
    with pytest.raises(BaseExceptionGroup, match="qualification cleanup failed"):
        bench.finish_qualification(
            report,
            tmp_path,
            256,
            server=server,
            session=session,
            wrapper=wrapper,
            measurements=measurements,
            primary_error=None,
        )
    assert server.stopped and session.closed and all(m.closed for m in state.monitors)
    assert not wrapper.exists()
    saved = json.loads((tmp_path / "limit-256.json").read_text())
    assert not saved["completed"]
    assert saved["cleanup_errors"][0]["operation"] == "copy_log"


def test_all_process_monitors_close_even_when_one_sampler_fails(
    monkeypatch, qualification_fakes
):
    import bench_descriptor_pressure as bench

    measurements = bench.ProcessMeasurements()
    measurements.start(SimpleNamespace(pid=1, ready=False))
    measurements.phase = "recovery"
    measurements.start(SimpleNamespace(pid=2, ready=False))
    first, second = qualification_fakes.monitors
    first.failure = OSError("sampling denied")
    original = first.close

    def fail():
        original()
        raise first.failure

    monkeypatch.setattr(first, "close", fail)
    with pytest.raises(BaseExceptionGroup, match="process monitoring failed"):
        measurements.close()
    assert first.closed and second.closed
    assert "sampling denied" in measurements.records()["initial"]["sampling_error"]


def test_transient_exec_lookup_race_does_not_hide_live_process_sampling(monkeypatch):
    import bench_descriptor_pressure as bench

    attempts = []

    def descriptors():
        attempts.append(True)
        if len(attempts) == 1:
            raise ProcessLookupError("image changing during exec")
        return 4

    def memory():
        monitor.done.set()
        return {"rss_bytes": 100, "cpu_ns": 0}

    monkeypatch.setattr(bench, "descriptor_reader", lambda _pid: descriptors)
    monkeypatch.setattr(bench, "logical_process_reader", lambda _pid: memory)
    monkeypatch.setattr(bench, "vector_mapping_rss", lambda _pid: None)
    monitor = bench.Monitor(SimpleNamespace(pid=123, poll=lambda: None))
    monitor.run()
    monitor.close()
    assert monitor.failure is None
    assert len(attempts) == 2 and monitor.samples[0]["fds"] == 4


def test_permission_failure_is_not_treated_as_an_exec_lookup_race(monkeypatch):
    import bench_descriptor_pressure as bench

    def denied():
        raise PermissionError("sampling forbidden")

    monkeypatch.setattr(bench, "descriptor_reader", lambda _pid: denied)
    monkeypatch.setattr(bench, "logical_process_reader", lambda _pid: lambda: {})
    monitor = bench.Monitor(SimpleNamespace(pid=123, poll=lambda: None))
    monitor.run()
    assert isinstance(monitor.failure, PermissionError)


@pytest.mark.parametrize("error_type", [PermissionError, OSError])
def test_nonlookup_sampling_errors_remain_fatal_when_process_exits(
    monkeypatch, error_type
):
    import bench_descriptor_pressure as bench
    import threading

    process = SimpleNamespace(pid=123, returncode=None)
    process.poll = lambda: process.returncode

    def denied():
        process.returncode = 0
        raise error_type("genuine sampler failure during shutdown")

    monkeypatch.setattr(bench, "descriptor_reader", lambda _pid: denied)
    monkeypatch.setattr(bench, "logical_process_reader", lambda _pid: lambda: {})
    monitor = bench.Monitor(process)
    monitor.samples.append({"fds": 3, "rss_bytes": 100})
    monitor.failure_event = threading.Event()
    monitor.run()
    assert isinstance(monitor.failure, error_type)
    assert monitor.failure_event.is_set()
    with pytest.raises(error_type, match="genuine sampler failure"):
        monitor.close()


def test_query_thread_start_failure_is_not_replaced_by_join_error(
    tmp_path, monkeypatch, qualification_fakes
):
    from pathlib import Path
    import bench_descriptor_pressure as bench

    joined = []

    def fail_start(_thread):
        raise RuntimeError("query thread creation failed")

    def unexpected_join(_thread, **_kwargs):
        joined.append(True)
        raise RuntimeError("cannot join an unstarted thread")

    monkeypatch.setattr(bench.threading.Thread, "start", fail_start)
    monkeypatch.setattr(bench.threading.Thread, "join", unexpected_join)
    monkeypatch.setattr(
        bench, "call", lambda *_args, **_kwargs: {"status": "committed"}
    )
    with pytest.raises(RuntimeError, match="query thread creation failed"):
        bench.qualify(Path("/unused"), 256, 256, None, tmp_path, 0.01)
    assert not joined
    assert qualification_fakes.servers[0].stopped
    assert all(m.closed for m in qualification_fakes.monitors)


@pytest.mark.parametrize("join_failure", [False, True])
def test_primary_write_failure_stops_server_before_query_join(
    tmp_path, monkeypatch, qualification_fakes, join_failure
):
    from pathlib import Path
    import bench_descriptor_pressure as bench

    state = qualification_fakes
    joins, batches = [], []

    class Worker:
        def __init__(self, **_kwargs):
            pass

        def start(self):
            pass

        def join(self, *, timeout):
            assert state.servers[0].stopped, (
                "failure cleanup must unblock the reader first"
            )
            joins.append(timeout)
            if join_failure:
                raise RuntimeError("secondary query join failure")

        def is_alive(self):
            return False

    def call(_session, _url, _method, path, *_args):
        if path.endswith("/batch"):
            batches.append(True)
            if len(batches) == 2:
                raise RuntimeError("primary write failure")
        return {"status": "committed"}

    monkeypatch.setattr(bench.threading, "Thread", Worker)
    monkeypatch.setattr(bench, "call", call)
    with pytest.raises(RuntimeError, match="primary write failure") as caught:
        bench.qualify(Path("/unused"), 256, 512, None, tmp_path, 0.01)
    assert joins == [10]
    if join_failure:
        assert "close_query_worker" in " ".join(caught.value.__notes__)
    assert all(m.closed for m in state.monitors)
    assert all(s.closed for s in state.sessions)


@pytest.mark.parametrize("fail_at", ["startup", "first_batch"])
def test_sampler_failure_prevents_admitting_another_batch(
    tmp_path, monkeypatch, qualification_fakes, fail_at
):
    from pathlib import Path
    import bench_descriptor_pressure as bench

    state = qualification_fakes
    original_start = bench.Monitor.start
    batches, calls = [], []

    def fail(monitor):
        monitor.failure = PermissionError("sampling permission revoked")
        monitor.failure_event.set()

    def start(monitor):
        original_start(monitor)
        if fail_at == "startup":
            fail(monitor)

    def call(_session, _url, _method, path, *_args):
        calls.append(path)
        if path.endswith("/batch"):
            batches.append(True)
            if len(batches) > 1:
                pytest.fail("a batch was admitted after sampling failed")
            fail(state.monitors[0])
        return {"status": "committed"}

    monkeypatch.setattr(bench.Monitor, "start", start)
    monkeypatch.setattr(bench, "call", call)
    with pytest.raises(BaseExceptionGroup, match="process monitoring failed"):
        bench.qualify(Path("/unused"), 256, 512, None, tmp_path, 3600)
    assert len(batches) == (0 if fail_at == "startup" else 1)
    if fail_at == "startup":
        assert not calls
    assert state.servers[0].stopped
    assert all(m.closed for m in state.monitors)


def test_steady_wait_observes_sampler_failure_without_waiting_for_deadline(
    monkeypatch, qualification_fakes
):
    import bench_descriptor_pressure as bench

    measurements = bench.ProcessMeasurements()
    measurements.start(SimpleNamespace(pid=1, ready=False))
    monitor = qualification_fakes.monitors[0]
    monitor.failure = PermissionError("sampling lost during steady phase")
    monitor.failure_event.set()
    waits = []

    def wait(seconds):
        waits.append(seconds)
        assert measurements.failed.is_set()
        return True

    monkeypatch.setattr(measurements.failed, "wait", wait)
    with pytest.raises(BaseExceptionGroup, match="process monitoring failed"):
        measurements.wait(3600)
    assert waits == [3600]
    measurements.close()


def test_initial_constructor_failure_retains_log_and_partial_report(
    tmp_path, monkeypatch, qualification_fakes
):
    import json
    from pathlib import Path
    import bench_descriptor_pressure as bench

    root = tmp_path / "failed-startup"
    root.mkdir()
    log = root / "server.log"
    log.write_text("complete startup diagnostics")

    def failed_constructor(*_args, on_startup_failed, **_kwargs):
        on_startup_failed(SimpleNamespace(root=root, log_path=log))
        log.unlink()
        root.rmdir()
        raise RuntimeError("startup readiness failed")

    monkeypatch.setattr(bench, "StandaloneAntflyServer", failed_constructor)
    with pytest.raises(RuntimeError, match="startup readiness failed"):
        bench.qualify(Path("/unused"), 256, 256, None, tmp_path, 1)
    report = json.loads((tmp_path / "limit-256.json").read_text())
    assert report["server_root"] == str(root)
    assert not report["completed"]
    assert (tmp_path / "server-256.log").read_text() == "complete startup diagnostics"
    assert all(session.closed for session in qualification_fakes.sessions)


@pytest.mark.parametrize("failure_at", ["before_request", "during_request", "wait"])
@pytest.mark.parametrize("phase", ["initial", "recovery"])
def test_visibility_polling_propagates_health_failures_promptly(
    monkeypatch, failure_at, phase
):
    import bench_descriptor_pressure as bench

    failed = [failure_at == "before_request"]
    requests, waits = [], []

    def check():
        if failed[0]:
            raise RuntimeError(f"{phase} monitoring failed")

    def call(*_args, timeout):
        requests.append(timeout)
        if failure_at == "during_request":
            failed[0] = True
        return {"status": {"total_indexed": 0}}

    def wait(seconds):
        waits.append(seconds)
        failed[0] = True

    monkeypatch.setattr(bench, "call", call)
    with pytest.raises(RuntimeError, match=f"{phase} monitoring failed"):
        bench.wait_visible(
            None,
            "http://unused",
            256,
            check_health=check,
            failure_event=SimpleNamespace(wait=wait),
        )
    assert len(requests) == (0 if failure_at == "before_request" else 1)
    assert all(0 < timeout <= 5 for timeout in requests)
    assert len(waits) == (1 if failure_at == "wait" else 0)


def test_visibility_checks_health_even_when_response_reports_complete(monkeypatch):
    import bench_descriptor_pressure as bench

    failed = [False]

    def call(*_args, **_kwargs):
        failed[0] = True
        return {"status": {"total_indexed": 256, "query_visible_doc_count": 256}}

    def check():
        if failed[0]:
            raise RuntimeError("query worker failed")

    monkeypatch.setattr(bench, "call", call)
    with pytest.raises(RuntimeError, match="query worker failed"):
        bench.wait_visible(
            None, "http://unused", 256, check_health=check, failure_event=None
        )


@pytest.mark.parametrize("unhealthy", [False, True])
def test_visibility_status_timeouts_recheck_health_before_retry(monkeypatch, unhealthy):
    import bench_descriptor_pressure as bench
    import requests

    calls, waits = [], []

    def call(*_args, **_kwargs):
        calls.append(True)
        if len(calls) == 1:
            raise requests.Timeout("slow status")
        return {"status": {"total_indexed": 256, "query_visible_doc_count": 256}}

    def health():
        if calls and unhealthy:
            raise RuntimeError("sampler failed")

    monkeypatch.setattr(bench, "call", call)
    kwargs = dict(check_health=health, failure_event=SimpleNamespace(wait=waits.append))
    if unhealthy:
        with pytest.raises(RuntimeError, match="sampler failed"):
            bench.wait_visible(None, "http://unused", 256, **kwargs)
        assert len(calls) == 1
        assert not waits
    else:
        assert (
            bench.wait_visible(None, "http://unused", 256, **kwargs)["total_indexed"]
            == 256
        )
        assert len(calls) == 2
        assert len(waits) == 1


def test_visibility_status_timeouts_cannot_extend_overall_deadline(monkeypatch):
    import bench_descriptor_pressure as bench
    import requests

    now = [0]
    calls = []

    def call(*_args, **_kwargs):
        calls.append(True)
        now[0] = 301
        raise requests.Timeout("status deadline")

    monkeypatch.setattr(bench, "call", call)
    monkeypatch.setattr(bench.time, "monotonic", lambda: now[0])
    with pytest.raises(RuntimeError, match="visibility did not reach"):
        bench.wait_visible(
            None,
            "http://unused",
            256,
            check_health=lambda: None,
            failure_event=SimpleNamespace(wait=lambda _: None),
        )
    assert len(calls) == 1


@pytest.fixture
def body_timeout_session():
    """Exercise Requests' real response consumption and exception wrapping."""
    import requests
    from urllib3.exceptions import ReadTimeoutError

    class TimedOutBody:
        def stream(self, *_args, **_kwargs):
            yield b'{"status":'
            raise ReadTimeoutError(None, "/indexes/vec", "body read stalled")

    class Adapter(requests.adapters.BaseAdapter):
        calls = 0

        def send(self, request, **_kwargs):
            self.calls += 1
            response = requests.Response()
            response.status_code = 200
            response.request = request
            response.url = request.url
            if self.calls == 1:
                response.raw = TimedOutBody()
            else:
                response._content = (
                    b'{"status":{"total_indexed":256,"query_visible_doc_count":256}}'
                )
            return response

        def close(self):
            pass

    with requests.Session() as session:
        session.trust_env = False
        adapter = Adapter()
        session.mount("http://", adapter)
        yield session, adapter


@pytest.mark.parametrize("failure", [None, "health", "deadline"])
def test_visibility_retries_real_requests_body_timeout_with_health_and_deadline(
    monkeypatch, body_timeout_session, failure
):
    import bench_descriptor_pressure as bench

    session, adapter = body_timeout_session
    now, waits = [0], []
    monkeypatch.setattr(bench.time, "monotonic", lambda: now[0])

    def health():
        if adapter.calls == 1:
            if failure == "health":
                raise RuntimeError("sampler failed during body read")
            if failure == "deadline":
                now[0] = 301

    kwargs = dict(check_health=health, failure_event=SimpleNamespace(wait=waits.append))
    if failure:
        message = (
            "sampler failed" if failure == "health" else "visibility did not reach"
        )
        with pytest.raises(RuntimeError, match=message):
            bench.wait_visible(session, "http://unused", 256, **kwargs)
        assert adapter.calls == 1
    else:
        assert (
            bench.wait_visible(session, "http://unused", 256, **kwargs)["total_indexed"]
            == 256
        )
        assert adapter.calls == 2
        assert len(waits) == 1


@pytest.mark.parametrize("operation", ["batch", "query"])
def test_body_timeout_does_not_retry_writes_or_queries(body_timeout_session, operation):
    import bench_descriptor_pressure as bench
    import requests
    from urllib3.exceptions import ReadTimeoutError

    session, adapter = body_timeout_session
    with pytest.raises(requests.ConnectionError) as caught:
        if operation == "batch":
            bench.call(session, "http://unused", "POST", "/batch", {})
        else:
            bench.query(session, "http://unused", [1, 0])
    assert isinstance(caught.value.args[0], ReadTimeoutError)
    assert adapter.calls == 1


@pytest.mark.parametrize("kind", ["reset", "refused", "tls", "plain_timeout_text"])
def test_visibility_does_not_retry_other_connection_failures(kind):
    import bench_descriptor_pressure as bench
    import requests

    error = {
        "reset": requests.ConnectionError(ConnectionResetError("reset")),
        "refused": requests.ConnectionError(ConnectionRefusedError("refused")),
        "tls": requests.exceptions.SSLError("TLS failure"),
        "plain_timeout_text": requests.ConnectionError("read timed out"),
    }[kind]
    calls = []

    def request(*_args, **_kwargs):
        calls.append(True)
        raise error

    with pytest.raises(type(error)) as caught:
        bench.wait_visible(
            SimpleNamespace(request=request),
            "http://unused",
            256,
            check_health=lambda: None,
            failure_event=None,
        )
    assert caught.value is error
    assert len(calls) == 1
