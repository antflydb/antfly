# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0
import subprocess

import pytest

import process_lifecycle as lifecycle


class Process:
    def __init__(self, clock, events, name, delay, *, unkillable=False):
        self.clock, self.events, self.name, self.delay = clock, events, name, delay
        self.started = None
        self.killed = False
        self.unkillable = unkillable

    def poll(self):
        return (
            0
            if self.started is not None and self.clock[0] >= self.started + self.delay
            else None
        )

    def send_signal(self, signal):
        self.events.append(("signal", self.name))
        self.started = self.clock[0]

    def kill(self):
        self.events.append(("kill", self.name))
        self.killed = True

    def wait(self, timeout):
        self.events.append(("wait", self.name))
        required = (
            0
            if self.killed and not self.unkillable
            else self.started + self.delay - self.clock[0]
        )
        self.clock[0] += max(0, min(timeout, required))
        if required > timeout:
            raise subprocess.TimeoutExpired(self.name, timeout)
        return 0


def fixture(monkeypatch, delays, **kwargs):
    clock, events = [0.0], []
    monkeypatch.setattr(lifecycle.time, "monotonic", lambda: clock[0])
    processes = [
        Process(clock, events, str(i), delay, **kwargs)
        for i, delay in enumerate(delays)
    ]
    return clock, events, processes


def test_shutdown_signals_all_owned_processes_before_waiting(monkeypatch):
    clock, events, processes = fixture(monkeypatch, [1, 2, 3])
    lifecycle.stop_processes(processes)
    assert clock[0] == 3
    assert events[:3] == [("signal", str(i)) for i in range(3)]
    assert not any(process.killed for process in processes)


def test_shutdown_shares_grace_deadline_and_kills_all_before_reaping(monkeypatch):
    clock, events, processes = fixture(monkeypatch, [30, 30, 30])
    lifecycle.stop_processes(processes, grace_s=2)
    assert clock[0] == 2
    assert events[6:9] == [("kill", str(i)) for i in range(3)]
    assert all(process.killed for process in processes)


def test_shutdown_reports_unreaped_processes_with_bounded_kill_deadline(monkeypatch):
    clock, events, processes = fixture(monkeypatch, [30, 30], unkillable=True)
    with pytest.raises(ExceptionGroup) as error:
        lifecycle.stop_processes(processes, grace_s=2, kill_s=3)
    assert clock[0] == 5
    assert len(error.value.exceptions) == 2


def test_cluster_shutdown_attempts_both_roles_and_retains_unreaped_ownership(
    monkeypatch, tmp_path
):
    import test_backup_restore as backup
    from types import SimpleNamespace

    clock, events, data = fixture(monkeypatch, [30], unkillable=True)
    metadata = [Process(clock, events, "metadata", 1)]
    closed = []
    preserved = []
    cluster = backup.ThreeByThreeBackupCluster.__new__(backup.ThreeByThreeBackupCluster)
    cluster.root = tmp_path
    cluster.data_procs = data
    cluster.metadata_procs = metadata
    cluster._metadata_probe_executor_shutdown = True
    cluster.port_reservations = SimpleNamespace(close=lambda: closed.append("ports"))
    cluster.data_log_files = []
    cluster.metadata_log_files = []
    cluster.tempdir = SimpleNamespace(cleanup=lambda: closed.append("root"))
    monkeypatch.setattr(
        backup,
        "maybe_preserve_tempdir",
        lambda _, failed: preserved.append(failed) or failed,
    )
    with pytest.raises(ExceptionGroup):
        cluster.stop()
    assert cluster.data_procs == data
    assert cluster.metadata_procs == []
    assert ("signal", "metadata") in events
    assert closed == []
    assert preserved == [True]
    data[0].unkillable = False
    cluster.stop()
    assert cluster.data_procs == []
    assert closed == ["ports", "root"]
