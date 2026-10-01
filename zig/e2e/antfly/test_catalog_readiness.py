# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

from types import SimpleNamespace

import pytest

import catalog_readiness
import test_catalog_resilience


class Clock:
    now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def status(*, ready=17, role="leader", incarnation="same-cluster"):
    return {
        "metadata_raft_role": role,
        "metadata_incarnation": incarnation,
        "runtime_status_record_version": 17,
        "runtime_status_protocol_ready_version": ready,
        "runtime_status_protocol_activated_version": 0,
    }


@pytest.fixture
def readiness(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(catalog_readiness, "time", clock)
    cluster = SimpleNamespace(
        metadata_procs=[SimpleNamespace(poll=lambda: None)],
        metadata_urls=["http://metadata"],
        debug_logs=lambda: "protocol probe deferred: incarnation unavailable",
    )
    observations = []

    def serve(payload):
        def get(url, *, timeout):
            observations.append((clock.now, url, timeout))
            return SimpleNamespace(
                status_code=200, raise_for_status=lambda: None, json=payload
            )

        monkeypatch.setattr(catalog_readiness.requests, "get", get)

    return clock, cluster, observations, serve


def test_catalog_fixture_waits_for_leader_profile_before_exposing_cluster(
    readiness, monkeypatch, tmp_path
):
    clock, cluster, observations, serve = readiness
    serve(lambda: status(ready=0 if clock.now < 0.5 else 17))
    cluster.log_paths = []
    cluster.stop = lambda **kwargs: None
    monkeypatch.setattr(
        test_catalog_resilience, "MultiNodeScalingCluster", lambda *a, **kw: cluster
    )
    binary = tmp_path / "antfly"
    binary.touch()
    monkeypatch.setenv("ANTFLY_BIN", str(binary))
    fixture = test_catalog_resilience.catalog_cluster.__wrapped__(
        SimpleNamespace(node=SimpleNamespace())
    )
    try:
        assert next(fixture) is cluster
        assert clock.now >= 0.5
        assert len(observations) > 1
    finally:
        fixture.close()


@pytest.mark.parametrize(
    "payload",
    [
        status(ready=0),
        status(ready=16),
        status(ready=18),
        status(role="follower"),
        status(incarnation=None),
    ],
)
def test_catalog_readiness_rejects_unready_or_unknown_profiles(readiness, payload):
    clock, cluster, observations, serve = readiness
    serve(lambda: payload)
    with pytest.raises(AssertionError, match="protocol probe deferred"):
        catalog_readiness.wait_for_catalog_protocol(cluster, timeout_s=0.25)
    assert clock.now == 0.25
    assert observations
    assert all(timeout <= 0.25 - now for now, _, timeout in observations)


def test_catalog_readiness_does_not_require_activation_to_already_be_committed(
    readiness,
):
    _, cluster, _, serve = readiness
    serve(status)
    assert catalog_readiness.wait_for_catalog_protocol(cluster) == 0


def test_catalog_readiness_reports_process_exit_without_waiting(readiness):
    clock, cluster, observations, _ = readiness
    cluster.metadata_procs = [SimpleNamespace(poll=lambda: 7, returncode=7)]
    with pytest.raises(AssertionError, match="node 0 exited"):
        catalog_readiness.wait_for_catalog_protocol(cluster)
    assert clock.now == 0
    assert not observations


def test_catalog_readiness_does_not_hide_http_500(readiness, monkeypatch):
    _, cluster, _, _ = readiness

    def fail():
        raise catalog_readiness.requests.HTTPError("internal server error")

    monkeypatch.setattr(
        catalog_readiness.requests,
        "get",
        lambda *a, **kw: SimpleNamespace(status_code=500, raise_for_status=fail),
    )
    with pytest.raises(catalog_readiness.requests.HTTPError):
        catalog_readiness.wait_for_catalog_protocol(cluster)


def test_catalog_readiness_can_observe_recovery_after_read_503(readiness, monkeypatch):
    clock, cluster, _, _ = readiness

    def get(*args, **kwargs):
        if clock.now < 0.2:
            return SimpleNamespace(status_code=503, text="not ready")
        return SimpleNamespace(
            status_code=200, raise_for_status=lambda: None, json=status
        )

    monkeypatch.setattr(catalog_readiness.requests, "get", get)
    assert catalog_readiness.wait_for_catalog_protocol(cluster) == 0
    assert clock.now >= 0.2


def test_catalog_fixture_preserves_diagnostics_when_readiness_fails(
    readiness, monkeypatch, tmp_path
):
    _, cluster, _, serve = readiness
    serve(lambda: status(ready=0))
    stopped = []
    cluster.log_paths = []
    cluster.stop = lambda **kwargs: stopped.append(kwargs)
    monkeypatch.setattr(
        test_catalog_resilience, "MultiNodeScalingCluster", lambda *a, **kw: cluster
    )
    binary = tmp_path / "antfly"
    binary.touch()
    monkeypatch.setenv("ANTFLY_BIN", str(binary))
    fixture = test_catalog_resilience.catalog_cluster.__wrapped__(
        SimpleNamespace(node=SimpleNamespace())
    )
    with pytest.raises(AssertionError, match="protocol probe deferred"):
        next(fixture)
    assert stopped == [{"test_failed": True}]
