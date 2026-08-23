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

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace

import pytest

from e2e_scheduler import (
    PERSISTENT_PROCESS_GROUP_PREFIX,
    PROCESS_GROUP_PREFIX,
    DurationHistory,
    IsolationAwareScheduling,
    normalize_nodeid,
    scheduling_group,
)


class FakeItem:
    def __init__(
        self,
        nodeid: str,
        *,
        fixtures: dict[str, str] | None = None,
        markers: list[pytest.Mark] | None = None,
    ):
        self.nodeid = nodeid
        fixture_scopes = fixtures or {}
        self.fixturenames = list(fixture_scopes)
        self._fixtureinfo = SimpleNamespace(
            name2fixturedefs={
                name: [SimpleNamespace(scope=scope)]
                for name, scope in fixture_scopes.items()
            }
        )
        self._markers = markers or []

    def iter_markers(self, name: str):
        return (mark for mark in self._markers if mark.name == name)

    def get_closest_marker(self, name: str):
        return next(self.iter_markers(name), None)


class FakeWorker:
    def __init__(self, *, exitstatus: int | None = None):
        self.sent: list[list[int]] = []
        if exitstatus is not None:
            self.workeroutput = {"exitstatus": exitstatus}

    def send_runtest_some(self, indexes: list[int]) -> None:
        self.sent.append(indexes)


def mark(name: str, *args: object, **kwargs: object) -> pytest.Mark:
    return pytest.Mark(name, args, kwargs, _ispytest=True)


def test_normalize_nodeid_only_removes_xdist_group_suffix() -> None:
    assert normalize_nodeid("test_a.py::test_case@group") == "test_a.py::test_case"
    assert normalize_nodeid("test_a.py::test_case[user@example.com]") == (
        "test_a.py::test_case[user@example.com]"
    )


def test_function_scoped_process_fixture_is_independently_schedulable() -> None:
    item = FakeItem(
        "test_backup_restore.py::test_cluster_restore_modes",
        fixtures={"backup_api": "function"},
    )
    group = scheduling_group(item)  # type: ignore[arg-type]
    assert group.startswith(f"{PROCESS_GROUP_PREFIX}test--")


def test_reused_or_session_process_fixture_stays_module_grouped() -> None:
    reused = FakeItem(
        "test_retrieval.py::test_pipeline",
        fixtures={"backup_api": "function"},
        markers=[mark("reuse_antfly_process")],
    )
    session_owned = FakeItem(
        "test_sync_levels.py::test_visibility",
        fixtures={
            "serverless_api": "session",
            "serverless_runtime": "session",
        },
    )
    assert "module--" in scheduling_group(reused)  # type: ignore[arg-type]
    session_group = scheduling_group(session_owned)  # type: ignore[arg-type]
    assert session_group.startswith(f"{PERSISTENT_PROCESS_GROUP_PREFIX}module--")


def test_explicit_isolation_and_resource_override_fixture_inference() -> None:
    item = FakeItem(
        "test_custom.py::test_cluster",
        markers=[
            mark("e2e_isolation", "test"),
            mark("e2e_resource", "antfly_process"),
        ],
    )
    group = scheduling_group(item)  # type: ignore[arg-type]
    assert group.startswith(f"{PROCESS_GROUP_PREFIX}test--")


def test_duration_history_round_trips_and_smooths_observations(tmp_path: Path) -> None:
    path = tmp_path / "durations.json"
    history = DurationHistory(path)
    history.observe("test_a.py::test_case@group", 2.0)
    history.save()

    reloaded = DurationHistory(path)
    assert reloaded.estimate("test_a.py::test_case", process_owned=False) == 2.0
    reloaded.observe("test_a.py::test_case", 4.0)
    reloaded.save()

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["tests"]["test_a.py::test_case"] == {
        "samples": 2,
        "seconds": 2.6,
    }


def test_scheduler_prefers_longest_eligible_work_without_exceeding_process_slots(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    scheduler._persistent_process_workers = set()
    scheduler.duration_history.tests = {
        "test_process.py::test_long": {"seconds": 20.0, "samples": 1},
        "test_light.py::test_short": {"seconds": 1.0, "samples": 1},
    }
    scheduler.workqueue = OrderedDict(
        {
            f"{PROCESS_GROUP_PREFIX}test--long": {
                "test_process.py::test_long": False,
            },
            "light--test--short": {"test_light.py::test_short": False},
        }
    )
    active_node = object()
    target_node = object()
    scheduler.assigned_work = {
        active_node: {
            f"{PROCESS_GROUP_PREFIX}test--active": {
                "test_process.py::test_active": False,
            }
        },
        target_node: {},
    }

    assert scheduler._next_eligible_scope(target_node) == "light--test--short"
    scheduler.assigned_work = {target_node: {}}
    assert (
        scheduler._next_eligible_scope(target_node)
        == f"{PROCESS_GROUP_PREFIX}test--long"
    )


def test_scheduler_reserves_process_capacity_per_worker_not_queued_unit(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    scheduler._persistent_process_workers = set()
    process_scope = f"{PROCESS_GROUP_PREFIX}test--next"
    scheduler.workqueue = OrderedDict(
        {process_scope: {"test_process.py::test_next": False}}
    )
    reserved_node = object()
    other_node = object()
    scheduler.assigned_work = {
        reserved_node: {
            f"{PROCESS_GROUP_PREFIX}test--first": {
                "test_process.py::test_first": False,
            },
            f"{PROCESS_GROUP_PREFIX}test--second": {
                "test_process.py::test_second": False,
            },
        },
        other_node: {},
    }

    assert scheduler._reserved_process_workers() == 1
    assert scheduler._next_eligible_scope(reserved_node) == process_scope
    assert scheduler._next_eligible_scope(other_node) is None


def test_session_process_reservation_lasts_for_worker_lifetime(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    transient_scope = f"{PROCESS_GROUP_PREFIX}test--transient"
    owner = object()
    other_node = object()
    scheduler._persistent_process_workers = {owner}
    scheduler.assigned_work = {
        owner: {
            f"{PERSISTENT_PROCESS_GROUP_PREFIX}module--done": {
                "test_serverless.py::test_done": True,
            }
        },
        other_node: {},
    }
    scheduler.workqueue = OrderedDict(
        {
            transient_scope: {"test_process.py::test_transient": False},
        }
    )

    assert scheduler._reserved_process_workers() == 1
    assert scheduler._next_eligible_scope(owner) == transient_scope
    assert scheduler._next_eligible_scope(other_node) is None


def test_assigning_session_process_makes_worker_reservation_sticky(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    scheduler._persistent_process_workers = set()
    node = FakeWorker()
    nodeid = "test_serverless.py::test_session"
    scope = f"{PERSISTENT_PROCESS_GROUP_PREFIX}module--serverless"
    scheduler.workqueue = OrderedDict({scope: {nodeid: False}})
    scheduler.assigned_work = {node: {}}
    scheduler.registered_collections = {node: [nodeid]}

    scheduler._assign_work_unit(node)

    assert node.sent == [[0]]
    assert node in scheduler._persistent_process_workers
    scheduler.assigned_work[node][scope][nodeid] = True
    assert scheduler._reserved_process_workers() == 1


def test_transient_process_capacity_is_released_after_completion(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    scheduler._persistent_process_workers = set()
    first_node = object()
    other_node = object()
    next_scope = f"{PROCESS_GROUP_PREFIX}test--next"
    scheduler.assigned_work = {
        first_node: {
            f"{PROCESS_GROUP_PREFIX}test--done": {
                "test_process.py::test_done": True,
            }
        },
        other_node: {},
    }
    scheduler.workqueue = OrderedDict(
        {next_scope: {"test_process.py::test_next": False}}
    )

    assert scheduler._reserved_process_workers() == 0
    assert scheduler._next_eligible_scope(other_node) == next_scope


def test_intentionally_retired_worker_requeues_only_incomplete_work(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    retired_node = FakeWorker(exitstatus=0)
    scheduler._retiring_nodes = {retired_node}
    scheduler._persistent_process_workers = {retired_node}
    scheduler.workqueue = OrderedDict()
    scheduler.assigned_work = {
        retired_node: {
            "light--test--finished": {"test_light.py::test_finished": True},
            "light--test--pending": {"test_light.py::test_pending": False},
        }
    }

    assert scheduler.remove_node(retired_node) is None
    assert scheduler.workqueue == {
        "light--test--pending": {"test_light.py::test_pending": False}
    }
    assert retired_node not in scheduler._persistent_process_workers


@pytest.mark.parametrize("exitstatus", [None, 2, 3])
def test_retiring_worker_crash_is_reported_and_requeued(
    tmp_path: Path,
    exitstatus: int | None,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    crashed_node = FakeWorker(exitstatus=exitstatus)
    scheduler._retiring_nodes = {crashed_node}
    scheduler._persistent_process_workers = {crashed_node}
    scheduler.workqueue = OrderedDict()
    scheduler.assigned_work = {
        crashed_node: {
            "light--test--pending": {"test_light.py::test_pending": False},
        }
    }

    assert scheduler.remove_node(crashed_node) == "test_light.py::test_pending"
    assert scheduler.workqueue == {
        "light--test--pending": {"test_light.py::test_pending": False}
    }
    assert crashed_node not in scheduler._retiring_nodes
    assert crashed_node not in scheduler._persistent_process_workers
