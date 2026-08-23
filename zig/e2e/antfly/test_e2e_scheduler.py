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
    MIXED_PROCESS_GROUP_PREFIX,
    PERSISTENT_PROCESS_GROUP_PREFIX,
    PROCESS_GROUP_PREFIX,
    DurationHistory,
    IsolationAwareScheduling,
    _consolidate_scheduling_groups,
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
        params: dict[str, object] | None = None,
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
        if params is not None:
            self.callspec = SimpleNamespace(params=params)

    def iter_markers(self, name: str):
        return (mark for mark in self._markers if mark.name == name)

    def get_closest_marker(self, name: str):
        return next(self.iter_markers(name), None)


class FakeWorker:
    def __init__(self, *, exitstatus: int | None = None):
        self.sent: list[list[int]] = []
        self.shutting_down = False
        if exitstatus is not None:
            self.workeroutput = {"exitstatus": exitstatus}

    def send_runtest_some(self, indexes: list[int]) -> None:
        self.sent.append(indexes)

    def shutdown(self) -> None:
        self.shutting_down = True


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
    assert session_group.startswith(
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}serverless_runtime--module--"
    )


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


def test_declared_session_process_has_stable_identity_and_module_scope() -> None:
    item = FakeItem(
        "test_custom.py::test_session_server",
        markers=[
            mark(
                "e2e_resource",
                "antfly_process",
                lifetime="session",
                identity="custom_runtime",
            ),
        ],
    )

    group = scheduling_group(item)  # type: ignore[arg-type]

    assert group.startswith(
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}custom_runtime--module--"
    )


def test_dynamic_serverless_table_parameter_has_persistent_identity() -> None:
    item = FakeItem(
        "test_foreign_sources.py::test_query[serverless]",
        fixtures={"table_api": "function"},
        params={"table_api": "serverless"},
    )

    group = scheduling_group(item)  # type: ignore[arg-type]

    assert group.startswith(
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}serverless_runtime--module--"
    )


def test_shared_group_uses_strictest_resource_policy_for_every_item() -> None:
    stateful = FakeItem(
        "test_foreign_sources.py::test_query[stateful]",
        fixtures={"table_api": "function"},
        markers=[mark("reuse_antfly_process")],
        params={"table_api": "stateful"},
    )
    serverless = FakeItem(
        "test_foreign_sources.py::test_query[serverless]",
        fixtures={"table_api": "function"},
        markers=[mark("reuse_antfly_process")],
        params={"table_api": "serverless"},
    )

    groups = _consolidate_scheduling_groups(
        [
            scheduling_group(stateful),  # type: ignore[arg-type]
            scheduling_group(serverless),  # type: ignore[arg-type]
        ]
    )

    assert groups[0] == groups[1]
    assert groups[0].startswith(
        f"{MIXED_PROCESS_GROUP_PREFIX}serverless_runtime--module--"
    )


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
    scheduler._persistent_processes = {}
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
    scheduler._persistent_processes = {}
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

    assert scheduler._reserved_process_slots() == 1
    assert scheduler._next_eligible_scope(reserved_node) == process_scope
    assert scheduler._next_eligible_scope(other_node) is None


def test_session_process_reservation_lasts_for_worker_lifetime(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    transient_scope = f"{PROCESS_GROUP_PREFIX}test--transient"
    matching_scope = (
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}serverless_runtime--module--next"
    )
    owner = object()
    other_node = object()
    scheduler._persistent_processes = {owner: {"serverless_runtime"}}
    scheduler.assigned_work = {
        owner: {
            f"{PERSISTENT_PROCESS_GROUP_PREFIX}serverless_runtime--module--done": {
                "test_serverless.py::test_done": True,
            }
        },
        other_node: {},
    }
    scheduler.workqueue = OrderedDict(
        {
            transient_scope: {"test_process.py::test_transient": False},
            matching_scope: {"test_serverless.py::test_next": False},
        }
    )

    assert scheduler._reserved_process_slots() == 1
    assert scheduler._next_eligible_scope(owner) == matching_scope
    assert scheduler._next_eligible_scope(other_node) is None


def test_transient_work_needs_an_additional_slot_on_session_owner(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 2
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    owner = object()
    other_node = object()
    transient_scope = f"{PROCESS_GROUP_PREFIX}test--transient"
    scheduler._persistent_processes = {owner: {"serverless_runtime"}}
    scheduler.assigned_work = {owner: {}, other_node: {}}
    scheduler.workqueue = OrderedDict(
        {transient_scope: {"test_process.py::test_transient": False}}
    )

    assert scheduler._next_eligible_scope(owner) == transient_scope
    scheduler.assigned_work[owner][transient_scope] = scheduler.workqueue.pop(
        transient_scope
    )
    assert scheduler._reserved_process_slots() == 2
    scheduler.workqueue = OrderedDict(
        {
            f"{PROCESS_GROUP_PREFIX}test--blocked": {
                "test_process.py::test_blocked": False
            }
        }
    )
    assert scheduler._next_eligible_scope(other_node) is None


def test_mixed_group_reserves_persistent_and_transient_slots(tmp_path: Path) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 2
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    scheduler._persistent_processes = {}
    node = object()
    mixed_scope = f"{MIXED_PROCESS_GROUP_PREFIX}serverless_runtime--module--mixed"
    scheduler.assigned_work = {node: {}}
    scheduler.workqueue = OrderedDict(
        {mixed_scope: {"test_mixed.py::test_case": False}}
    )

    assert scheduler._additional_process_slots(node, mixed_scope) == 2
    assert scheduler._next_eligible_scope(node) == mixed_scope

    scheduler.process_slots = 1
    with pytest.raises(pytest.UsageError, match="requires 2 Antfly process slots"):
        scheduler._next_eligible_scope(node)


def test_new_session_identity_prefers_idle_clean_worker(tmp_path: Path) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 2
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    owner = FakeWorker()
    clean = FakeWorker()
    embedded_scope = (
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}embedded_standalone_runtime--module--next"
    )
    scheduler._persistent_processes = {owner: {"serverless_runtime"}}
    scheduler.assigned_work = {owner: {}, clean: {}}
    scheduler.workqueue = OrderedDict(
        {embedded_scope: {"test_standalone.py::test_next": False}}
    )

    assert scheduler._next_eligible_scope(owner) is None
    assert scheduler._next_eligible_scope(clean) == embedded_scope


def test_assigning_session_process_makes_worker_reservation_sticky(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    scheduler._persistent_processes = {}
    node = FakeWorker()
    nodeid = "test_serverless.py::test_session"
    scope = f"{PERSISTENT_PROCESS_GROUP_PREFIX}serverless_runtime--module--serverless"
    scheduler.workqueue = OrderedDict({scope: {nodeid: False}})
    scheduler.assigned_work = {node: {}}
    scheduler.registered_collections = {node: [nodeid]}

    scheduler._assign_work_unit(node)

    assert node.sent == [[0]]
    assert scheduler._persistent_processes[node] == {"serverless_runtime"}
    scheduler.assigned_work[node][scope][nodeid] = True
    assert scheduler._reserved_process_slots() == 1


def test_transient_process_capacity_is_released_after_completion(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    scheduler._persistent_processes = {}
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

    assert scheduler._reserved_process_slots() == 0
    assert scheduler._next_eligible_scope(other_node) == next_scope


def test_idle_worker_waits_while_session_owner_rotates_to_release_slot(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    owner = FakeWorker(exitstatus=0)
    waiter = FakeWorker()
    completed_scope = (
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}serverless_runtime--module--done"
    )
    transient_scope = f"{PROCESS_GROUP_PREFIX}test--waiting"
    waiting_nodeid = "test_process.py::test_waiting"
    scheduler._retiring_nodes = set()
    scheduler._persistent_processes = {owner: {"serverless_runtime"}}
    scheduler.assigned_work = {
        owner: {completed_scope: {"test_serverless.py::test_done": True}},
        waiter: {},
    }
    scheduler.workqueue = OrderedDict({transient_scope: {waiting_nodeid: False}})
    scheduler.registered_collections = {waiter: [waiting_nodeid]}

    scheduler._reschedule(waiter)
    assert not waiter.shutting_down
    scheduler._reschedule(owner)
    assert owner.shutting_down
    assert owner in scheduler._retiring_nodes

    assert scheduler.remove_node(owner) is None
    assert waiter.sent == [[0]]


def test_intentionally_retired_worker_requeues_only_incomplete_work(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    retired_node = FakeWorker(exitstatus=0)
    scheduler._retiring_nodes = {retired_node}
    scheduler._persistent_processes = {retired_node: {"serverless_runtime"}}
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
    assert retired_node not in scheduler._persistent_processes


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
    scheduler._persistent_processes = {crashed_node: {"serverless_runtime"}}
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
    assert crashed_node not in scheduler._persistent_processes
