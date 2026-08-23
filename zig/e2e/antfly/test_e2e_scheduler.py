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
import sys
from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from e2e_scheduler import (
    MIXED_PROCESS_GROUP_PREFIX,
    PERSISTENT_PROCESS_GROUP_PREFIX,
    PROCESS_GROUP_PREFIX,
    DurationHistory,
    IsolationAwareScheduling,
    _consolidate_scheduling_groups,
    e2e_resource,
    normalize_nodeid,
    pytest_addoption,
    pytest_configure,
    scheduling_group,
)


class FakeItem:
    def __init__(
        self,
        nodeid: str,
        *,
        fixtures: dict[str, str] | None = None,
        fixture_baseids: dict[str, str] | None = None,
        fixture_functions: dict[str, object] | None = None,
        fixture_parameter_scopes: dict[str, str] | None = None,
        test_class: bool = False,
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
        for name, definitions in self._fixtureinfo.name2fixturedefs.items():
            definition = definitions[-1]
            definition.baseid = (fixture_baseids or {}).get(name, "")
            definition.func = (fixture_functions or {}).get(name)
        self._markers = markers or []
        self.cls = object() if test_class else None
        if params is not None or fixture_parameter_scopes:
            self.callspec = SimpleNamespace(
                params=params or {},
                _arg2scope={
                    name: SimpleNamespace(value=scope)
                    for name, scope in (fixture_parameter_scopes or {}).items()
                },
            )

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


def test_process_slot_environment_default_uses_pytest_integer_parser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ANTFLY_E2E_PROCESS_SLOTS", "invalid")

    with pytest.raises(pytest.UsageError, match="invalid int value: 'invalid'"):
        parser = pytest.Parser(_ispytest=True)
        pytest_addoption(parser)
        parser.parse([])


@pytest.mark.parametrize("slots", [0, -1])
def test_process_slot_configuration_rejects_non_positive_values(slots: int) -> None:
    config = SimpleNamespace(getoption=lambda name: slots)

    with pytest.raises(
        pytest.UsageError,
        match="--e2e-process-slots must be a positive integer",
    ):
        pytest_configure(config)  # type: ignore[arg-type]


def test_parallel_configuration_rejects_incompatible_distribution_mode() -> None:
    options = {
        "e2e_process_slots": 2,
        "dist": "load",
        "tx": ["popen", "popen"],
    }
    config = SimpleNamespace(
        getoption=lambda name, default=None: options.get(name, default)
    )

    with pytest.raises(
        pytest.UsageError,
        match=(
            "parallel Antfly E2E requires --dist=loadgroup so fixture isolation "
            "and --e2e-process-slots remain enforced"
        ),
    ):
        pytest_configure(config)  # type: ignore[arg-type]


def test_fixture_resource_decorator_rejects_inverted_order() -> None:
    def fixture_function() -> None:
        pass

    fixture_definition = pytest.fixture(fixture_function)
    with pytest.raises(TypeError, match="must be placed below @pytest.fixture"):
        e2e_resource("antfly_process")(fixture_definition)  # type: ignore[arg-type]


@pytest.fixture(scope="session")
@e2e_resource("antfly_process")
def declared_session_process_fixture() -> None:
    """Exercise inferred lifetime metadata on a real session fixture."""


@pytest.fixture(scope="module")
@e2e_resource("antfly_process")
def declared_module_process_fixture() -> None:
    """Exercise scope inference for a transient custom process fixture."""


@pytest.fixture(scope="class")
@e2e_resource("antfly_process")
def declared_class_process_fixture() -> None:
    """Exercise class-level process isolation without module serialization."""


@pytest.fixture(scope="function")
@e2e_resource("antfly_process")
def declared_parametrized_process_fixture(request: pytest.FixtureRequest) -> object:
    """Exercise pytest's per-param effective scope metadata."""
    return request.param


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


def test_class_scoped_process_fixture_stays_class_grouped() -> None:
    first = FakeItem(
        "test_custom.py::TestProcess::test_first",
        fixtures={"backup_api": "class"},
        test_class=True,
    )
    second = FakeItem(
        "test_custom.py::TestProcess::test_second",
        fixtures={"backup_api": "class"},
        test_class=True,
    )
    unrelated = FakeItem(
        "test_custom.py::TestOtherProcess::test_first",
        fixtures={"backup_api": "class"},
        test_class=True,
    )

    first_group = scheduling_group(first)  # type: ignore[arg-type]
    second_group = scheduling_group(second)  # type: ignore[arg-type]
    unrelated_group = scheduling_group(unrelated)  # type: ignore[arg-type]

    assert first_group.startswith(f"{PROCESS_GROUP_PREFIX}class--")
    assert first_group == second_group
    assert first_group != unrelated_group


def test_class_scoped_process_fixture_outside_class_stays_test_grouped() -> None:
    first = FakeItem(
        "test_custom.py::test_first",
        fixtures={"backup_api": "class"},
    )
    second = FakeItem(
        "test_custom.py::test_second",
        fixtures={"backup_api": "class"},
    )

    first_group = scheduling_group(first)  # type: ignore[arg-type]
    second_group = scheduling_group(second)  # type: ignore[arg-type]

    assert first_group.startswith(f"{PROCESS_GROUP_PREFIX}test--")
    assert first_group != second_group


def test_package_scoped_process_fixture_stays_package_grouped() -> None:
    @e2e_resource("antfly_process")
    def package_runtime() -> None:
        pass

    first = FakeItem(
        "pkg/test_first.py::test_process",
        fixtures={"runtime": "package"},
        fixture_baseids={"runtime": "pkg"},
        fixture_functions={"runtime": package_runtime},
    )
    second = FakeItem(
        "pkg/test_second.py::test_process",
        fixtures={"runtime": "package"},
        fixture_baseids={"runtime": "pkg"},
        fixture_functions={"runtime": package_runtime},
    )
    unrelated = FakeItem(
        "other/test_process.py::test_process",
        fixtures={"runtime": "package"},
        fixture_baseids={"runtime": "other"},
        fixture_functions={"runtime": package_runtime},
    )

    first_group = scheduling_group(first)  # type: ignore[arg-type]
    second_group = scheduling_group(second)  # type: ignore[arg-type]
    unrelated_group = scheduling_group(unrelated)  # type: ignore[arg-type]

    assert first_group.startswith(f"{PROCESS_GROUP_PREFIX}package--")
    assert first_group == second_group
    assert first_group != unrelated_group


def test_root_package_process_fixtures_share_root_boundary() -> None:
    @e2e_resource("antfly_process")
    def first_runtime() -> None:
        pass

    @e2e_resource("antfly_process")
    def second_runtime() -> None:
        pass

    first = FakeItem(
        "test_first.py::test_process",
        fixtures={"first_runtime": "package"},
        fixture_functions={"first_runtime": first_runtime},
    )
    second = FakeItem(
        "test_second.py::test_process",
        fixtures={"second_runtime": "package"},
        fixture_functions={"second_runtime": second_runtime},
    )

    first_group = scheduling_group(first)  # type: ignore[arg-type]
    second_group = scheduling_group(second)  # type: ignore[arg-type]

    assert first_group.startswith(f"{PROCESS_GROUP_PREFIX}package--")
    assert first_group == second_group


def test_effective_parameter_scope_overrides_fixture_definition_scope() -> None:
    @e2e_resource("antfly_process")
    def declared_runtime() -> None:
        pass

    widened_first = FakeItem(
        "test_custom.py::test_first[value]",
        fixtures={"stateful_api": "function"},
        fixture_parameter_scopes={"stateful_api": "module"},
    )
    widened_second = FakeItem(
        "test_custom.py::test_second[value]",
        fixtures={"stateful_api": "function"},
        fixture_parameter_scopes={"stateful_api": "module"},
    )
    narrowed = FakeItem(
        "test_custom.py::test_narrowed[value]",
        fixtures={"serverless_runtime": "session"},
        fixture_parameter_scopes={"serverless_runtime": "function"},
    )
    narrowed_declaration = FakeItem(
        "test_custom.py::test_declared_narrowed[value]",
        fixtures={"runtime": "session"},
        fixture_functions={"runtime": declared_runtime},
        fixture_parameter_scopes={"runtime": "function"},
    )

    first_group = scheduling_group(widened_first)  # type: ignore[arg-type]
    second_group = scheduling_group(widened_second)  # type: ignore[arg-type]
    narrowed_group = scheduling_group(narrowed)  # type: ignore[arg-type]
    narrowed_declaration_group = scheduling_group(  # type: ignore[arg-type]
        narrowed_declaration
    )

    assert first_group.startswith(f"{PROCESS_GROUP_PREFIX}module--")
    assert first_group == second_group
    assert narrowed_group.startswith(f"{PROCESS_GROUP_PREFIX}test--")
    assert narrowed_declaration_group.startswith(f"{PROCESS_GROUP_PREFIX}test--")


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


def test_inferred_session_identities_in_one_module_remain_independent() -> None:
    first = FakeItem(
        "test_custom.py::test_first",
        markers=[
            mark(
                "e2e_resource",
                "antfly_process",
                lifetime="session",
                identity="runtime_a",
            )
        ],
    )
    second = FakeItem(
        "test_custom.py::test_second",
        markers=[
            mark(
                "e2e_resource",
                "antfly_process",
                lifetime="session",
                identity="runtime_b",
            )
        ],
    )

    groups = _consolidate_scheduling_groups(
        [
            scheduling_group(first),  # type: ignore[arg-type]
            scheduling_group(second),  # type: ignore[arg-type]
        ]
    )

    assert groups[0] != groups[1]
    assert groups[0].startswith(f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime_a--module--")
    assert groups[1].startswith(f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime_b--module--")


def test_explicit_session_group_consolidates_all_required_identities() -> None:
    groups = []
    for nodeid, identity in (
        ("test_custom.py::test_first", "runtime_a"),
        ("test_custom.py::test_second", "runtime_b"),
    ):
        item = FakeItem(
            nodeid,
            markers=[
                mark("e2e_isolation", "module", group="shared-runtime-tests"),
                mark(
                    "e2e_resource",
                    "antfly_process",
                    lifetime="session",
                    identity=identity,
                ),
            ],
        )
        groups.append(scheduling_group(item))  # type: ignore[arg-type]

    consolidated = _consolidate_scheduling_groups(groups)

    assert consolidated[0] == consolidated[1]
    assert consolidated[0].startswith(
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime_a+runtime_b--module--"
    )


@pytest.mark.e2e_isolation("module", group="declared-session-fixture")
def test_fixture_resource_declaration_is_applied_to_consuming_test(
    request: pytest.FixtureRequest,
    declared_session_process_fixture: None,
) -> None:
    del declared_session_process_fixture
    groups = [mark.args[0] for mark in request.node.iter_markers("xdist_group")]

    assert len(groups) == 1
    assert str(groups[0]).startswith(
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}declared_session_process_fixture."
    )
    assert "--module--" in str(groups[0])


@pytest.mark.e2e_isolation("module", group="declared-module-fixture")
def test_fixture_resource_declaration_preserves_fixture_scope(
    request: pytest.FixtureRequest,
    declared_module_process_fixture: None,
) -> None:
    del declared_module_process_fixture
    groups = [mark.args[0] for mark in request.node.iter_markers("xdist_group")]

    assert len(groups) == 1
    assert str(groups[0]).startswith(f"{PROCESS_GROUP_PREFIX}module--")


class TestFixtureResourceClassScope:
    def test_first_method_uses_class_group(
        self,
        request: pytest.FixtureRequest,
        declared_class_process_fixture: None,
    ) -> None:
        del declared_class_process_fixture
        groups = [mark.args[0] for mark in request.node.iter_markers("xdist_group")]

        assert len(groups) == 1
        assert str(groups[0]).startswith(f"{PROCESS_GROUP_PREFIX}class--")

    def test_second_method_uses_same_class_group(
        self,
        request: pytest.FixtureRequest,
        declared_class_process_fixture: None,
    ) -> None:
        del declared_class_process_fixture
        groups = [mark.args[0] for mark in request.node.iter_markers("xdist_group")]

        assert len(groups) == 1
        assert str(groups[0]).startswith(f"{PROCESS_GROUP_PREFIX}class--")


def test_real_class_scoped_fixture_outside_class_uses_test_group(
    request: pytest.FixtureRequest,
    declared_class_process_fixture: None,
) -> None:
    del declared_class_process_fixture
    groups = [mark.args[0] for mark in request.node.iter_markers("xdist_group")]

    assert len(groups) == 1
    assert str(groups[0]).startswith(f"{PROCESS_GROUP_PREFIX}test--")


@pytest.mark.parametrize(
    "declared_parametrized_process_fixture",
    [None],
    indirect=True,
    scope="module",
)
def test_real_effective_parameter_scope_is_used(
    request: pytest.FixtureRequest,
    declared_parametrized_process_fixture: object,
) -> None:
    del declared_parametrized_process_fixture
    groups = [mark.args[0] for mark in request.node.iter_markers("xdist_group")]

    assert len(groups) == 1
    assert str(groups[0]).startswith(f"{PROCESS_GROUP_PREFIX}module--")


def test_inferred_session_identity_includes_fixture_provenance() -> None:
    @e2e_resource("antfly_process")
    def first_runtime() -> None:
        pass

    @e2e_resource("antfly_process")
    def second_runtime() -> None:
        pass

    first = FakeItem(
        "test_a.py::test_runtime",
        fixtures={"runtime": "session"},
        fixture_baseids={"runtime": "test_a.py"},
        fixture_functions={"runtime": first_runtime},
    )
    second = FakeItem(
        "test_b.py::test_runtime",
        fixtures={"runtime": "session"},
        fixture_baseids={"runtime": "test_b.py"},
        fixture_functions={"runtime": second_runtime},
    )
    shared_definition = FakeItem(
        "test_c.py::test_runtime",
        fixtures={"runtime": "session"},
        fixture_baseids={"runtime": "test_a.py"},
        fixture_functions={"runtime": first_runtime},
    )

    first_group = scheduling_group(first)  # type: ignore[arg-type]
    second_group = scheduling_group(second)  # type: ignore[arg-type]
    shared_group = scheduling_group(shared_definition)  # type: ignore[arg-type]
    first_identity = first_group.split("--", 4)[2]
    second_identity = second_group.split("--", 4)[2]
    shared_identity = shared_group.split("--", 4)[2]

    assert first_group.startswith(f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime.")
    assert second_group.startswith(f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime.")
    assert first_identity != second_identity
    assert first_identity == shared_identity


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


@pytest.mark.parametrize("payload", [None, [], 1, "valid JSON"])
def test_duration_history_ignores_non_object_json(
    tmp_path: Path, payload: object
) -> None:
    path = tmp_path / "durations.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    history = DurationHistory(path)

    assert history.tests == {}


def test_duration_history_ignores_invalid_numeric_entries(tmp_path: Path) -> None:
    path = tmp_path / "durations.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "tests": {
                    "valid": {"seconds": 2.0, "samples": 1},
                    "boolean-seconds": {"seconds": True, "samples": 1},
                    "infinite-seconds": {"seconds": float("inf"), "samples": 1},
                    "overflowing-seconds": {"seconds": 10**1000, "samples": 1},
                    "boolean-samples": {"seconds": 1.0, "samples": True},
                },
            }
        ),
        encoding="utf-8",
    )

    history = DurationHistory(path)

    assert history.tests == {"valid": {"seconds": 2.0, "samples": 1}}


def test_duration_history_save_failure_is_non_fatal(tmp_path: Path) -> None:
    history = DurationHistory(tmp_path / "durations.json")
    history.observe("test_a.py::test_case", 1.0)

    with patch("e2e_scheduler.tempfile.mkstemp", side_effect=OSError("cache full")):
        error = history.save()

    assert isinstance(error, OSError)
    assert str(error) == "cache full"


def test_duration_history_merges_observations_from_stale_writers(
    tmp_path: Path,
) -> None:
    path = tmp_path / "durations.json"
    first = DurationHistory(path)
    second = DurationHistory(path)
    first.observe("test_a.py::test_shared", 1.0)
    first.observe("test_a.py::test_first", 2.0)
    second.observe("test_a.py::test_shared", 3.0)
    second.observe("test_a.py::test_second", 4.0)

    assert first.save() is None
    assert second.save() is None

    reloaded = DurationHistory(path)
    assert reloaded.tests == {
        "test_a.py::test_first": {"seconds": 2.0, "samples": 1},
        "test_a.py::test_second": {"seconds": 4.0, "samples": 1},
        "test_a.py::test_shared": {"seconds": 1.6, "samples": 2},
    }


def test_duration_reports_exclude_skips_but_keep_full_executed_protocol(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    e2e_scheduler_module = sys.modules[DurationHistory.__module__]
    history = DurationHistory(tmp_path / "durations.json")
    monkeypatch.setattr(e2e_scheduler_module, "_duration_history", history)
    monkeypatch.setattr(e2e_scheduler_module, "_duration_report_totals", {})
    monkeypatch.setattr(e2e_scheduler_module, "_executed_duration_nodeids", set())

    for when, duration in (("setup", 2.0), ("call", 0.1), ("teardown", 0.2)):
        e2e_scheduler_module.pytest_runtest_logreport(
            SimpleNamespace(
                nodeid="test_optional.py::test_skipped@group",
                duration=duration,
                when=when,
                skipped=when == "call",
            )
        )
    for when, duration in (("setup", 3.0), ("call", 4.0), ("teardown", 1.0)):
        e2e_scheduler_module.pytest_runtest_logreport(
            SimpleNamespace(
                nodeid="test_real.py::test_executed@group",
                duration=duration,
                when=when,
                skipped=False,
            )
        )

    e2e_scheduler_module._flush_duration_reports()

    assert history.observed == {"test_real.py::test_executed": 8.0}
    assert e2e_scheduler_module._duration_report_totals == {}
    assert e2e_scheduler_module._executed_duration_nodeids == set()


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
    scheduler.registered_collections = {owner: [], clean: []}
    scheduler.workqueue = OrderedDict(
        {embedded_scope: {"test_standalone.py::test_next": False}}
    )

    assert scheduler._next_eligible_scope(owner) is None
    assert scheduler._next_eligible_scope(clean) == embedded_scope


def test_session_owner_adds_identity_when_clean_worker_cannot_fit_scope(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 2
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    owner = FakeWorker()
    clean = FakeWorker()
    combined_scope = (
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}embedded_standalone_runtime+"
        "serverless_runtime--module--combined"
    )
    scheduler._persistent_processes = {owner: {"serverless_runtime"}}
    scheduler.assigned_work = {owner: {}, clean: {}}
    scheduler.registered_collections = {owner: [], clean: []}
    scheduler.workqueue = OrderedDict(
        {combined_scope: {"test_combined.py::test_next": False}}
    )

    assert scheduler._next_eligible_scope(owner) == combined_scope
    assert scheduler._next_eligible_scope(clean) is None


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


def test_final_transient_test_hands_its_slot_to_persistent_runway(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    worker = FakeWorker()
    transient_scope = f"{PROCESS_GROUP_PREFIX}test--transient"
    persistent_scope = (
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}serverless_runtime--module--next"
    )
    transient_nodeid = f"test_transient.py::test_current@{transient_scope}"
    persistent_nodeid = f"test_serverless.py::test_next@{persistent_scope}"
    scheduler._retiring_nodes = set()
    scheduler._starting_replacements = set()
    scheduler._collecting_nodes = set()
    scheduler._transient_handoffs = set()
    scheduler._persistent_processes = {}
    scheduler.assigned_work = {
        worker: {transient_scope: {transient_nodeid: False}},
    }
    scheduler.workqueue = OrderedDict({persistent_scope: {persistent_nodeid: False}})
    scheduler.registered_collections = {worker: [transient_nodeid, persistent_nodeid]}

    scheduler._reschedule(worker)

    assert worker.sent == [[1]]
    assert not worker.shutting_down
    assert scheduler._persistent_processes[worker] == {"serverless_runtime"}
    assert worker in scheduler._transient_handoffs
    assert scheduler._reserved_process_slots() == 1

    scheduler.mark_test_complete(worker, 0)

    assert worker not in scheduler._transient_handoffs
    assert scheduler._reserved_process_slots() == 1


def test_transient_handoff_lane_cannot_be_reused_behind_persistent_work(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    worker = FakeWorker()
    current_scope = f"{PROCESS_GROUP_PREFIX}test--current"
    persistent_scope = (
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}serverless_runtime--module--next"
    )
    later_scope = f"{PROCESS_GROUP_PREFIX}test--later"
    current_nodeid = "test_transient.py::test_current"
    persistent_nodeid = "test_serverless.py::test_next"
    later_nodeid = "test_transient.py::test_later"
    scheduler._retiring_nodes = set()
    scheduler._starting_replacements = set()
    scheduler._transient_handoffs = set()
    scheduler._persistent_processes = {}
    scheduler.assigned_work = {
        worker: {current_scope: {current_nodeid: False}},
    }
    scheduler.workqueue = OrderedDict(
        {
            persistent_scope: {persistent_nodeid: False},
            later_scope: {later_nodeid: False},
        }
    )
    scheduler.registered_collections = {
        worker: [current_nodeid, persistent_nodeid, later_nodeid]
    }

    scheduler._reschedule(worker)
    scheduler._reschedule(worker)

    assert worker.sent == [[1]]
    assert list(scheduler.workqueue) == [later_scope]
    assert scheduler._additional_process_slots(worker, later_scope) == 1
    assert scheduler._reserved_process_slots() == 1


def test_last_resource_worker_starts_replacement_before_retirement(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    owner = FakeWorker()
    replacement = FakeWorker()
    cloned: list[FakeWorker] = []

    def clone_node(node: FakeWorker) -> FakeWorker:
        assert node is owner
        cloned.append(replacement)
        return replacement

    controller = SimpleNamespace(_clone_node=clone_node)
    scheduler.config = SimpleNamespace(
        pluginmanager=SimpleNamespace(
            getplugin=lambda name: controller if name == "dsession" else None
        )
    )
    owner_scope = f"{PERSISTENT_PROCESS_GROUP_PREFIX}serverless_runtime--module--owner"
    queued_scope = f"{PROCESS_GROUP_PREFIX}test--queued"
    scheduler._retiring_nodes = set()
    scheduler._starting_replacements = set()
    scheduler._transient_handoffs = set()
    scheduler._persistent_processes = {owner: {"serverless_runtime"}}
    scheduler.assigned_work = {
        owner: {owner_scope: {"test_owner.py::test_current": False}},
    }
    scheduler.registered_collections = {owner: ["test_owner.py::test_current"]}
    scheduler.workqueue = OrderedDict(
        {queued_scope: {"test_transient.py::test_queued": False}}
    )

    scheduler._reschedule(owner)

    assert cloned == [replacement]
    assert replacement in scheduler._starting_replacements
    assert owner in scheduler._retiring_nodes
    assert owner.shutting_down

    scheduler.add_node(replacement)
    assert replacement in scheduler._starting_replacements
    assert replacement in scheduler.assigned_work


def test_replacement_waits_for_collection_before_accepting_work(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    original = FakeWorker()
    replacement = FakeWorker()
    nodeid = "test_process.py::test_queued"
    scope = f"{PROCESS_GROUP_PREFIX}test--queued"
    scheduler.numnodes = 1
    scheduler.collection = [nodeid]
    scheduler._retiring_nodes = set()
    scheduler._starting_replacements = {replacement}
    scheduler._collecting_nodes = set()
    scheduler._transient_handoffs = set()
    scheduler._persistent_processes = {}
    scheduler.assigned_work = {original: {}}
    scheduler.registered_collections = {original: [nodeid]}
    scheduler.workqueue = OrderedDict({scope: {nodeid: False}})

    scheduler.add_node(replacement)
    scheduler._reschedule(replacement)

    assert replacement.sent == []
    assert not replacement.shutting_down
    assert replacement in scheduler._starting_replacements
    assert list(scheduler.workqueue) == [scope]

    original.shutting_down = True
    scheduler.add_node_collection(replacement, [nodeid])
    scheduler.schedule()

    assert replacement not in scheduler._starting_replacements
    assert replacement.sent == [[0]]
    assert not scheduler.workqueue


def test_replacement_collection_mismatch_fails_fast_and_clears_progress(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    original = FakeWorker()
    replacement = FakeWorker()
    original.gateway = SimpleNamespace(id="gw0")
    replacement.gateway = SimpleNamespace(id="gw2")
    original_nodeid = "test_process.py::test_original"
    replacement_nodeid = "test_process.py::test_replacement"
    scheduler.numnodes = 1
    scheduler.collection = [original_nodeid]
    scheduler.log = lambda message: None
    scheduler._retiring_nodes = set()
    scheduler._starting_replacements = {replacement}
    scheduler._collecting_nodes = set()
    scheduler._transient_handoffs = set()
    scheduler._persistent_processes = {}
    scheduler.assigned_work = {original: {}}
    scheduler.registered_collections = {original: [original_nodeid]}
    scheduler.workqueue = OrderedDict()

    scheduler.add_node(replacement)
    with pytest.raises(
        pytest.UsageError,
        match="replacement worker gw2 collected different tests",
    ):
        scheduler.add_node_collection(replacement, [replacement_nodeid])

    assert replacement.shutting_down
    assert replacement not in scheduler.registered_collections
    assert replacement not in scheduler._starting_replacements
    assert replacement not in scheduler._collecting_nodes


def test_global_reschedule_starts_long_persistent_work_on_clean_worker_first(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 2
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    owner = FakeWorker()
    clean = FakeWorker()
    owner_nodeid = "test_a.py::test_done"
    persistent_nodeid = "test_b.py::test_long"
    transient_nodeid = "test_transient.py::test_short"
    owner_scope = f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime_a--module--done"
    persistent_scope = f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime_b--module--long"
    transient_scope = f"{PROCESS_GROUP_PREFIX}test--short"
    scheduler.duration_history.tests = {
        persistent_nodeid: {"seconds": 100.0, "samples": 1},
        transient_nodeid: {"seconds": 1.0, "samples": 1},
    }
    scheduler._retiring_nodes = set()
    scheduler._starting_replacements = set()
    scheduler._collecting_nodes = set()
    scheduler._transient_handoffs = set()
    scheduler._persistent_processes = {owner: {"runtime_a"}}
    scheduler.assigned_work = {
        owner: {owner_scope: {owner_nodeid: True}},
        clean: {},
    }
    collection = [owner_nodeid, persistent_nodeid, transient_nodeid]
    scheduler.registered_collections = {owner: collection, clean: collection}
    scheduler.workqueue = OrderedDict(
        {
            persistent_scope: {persistent_nodeid: False},
            transient_scope: {transient_nodeid: False},
        }
    )

    scheduler._reschedule_all()

    assert owner.sent == []
    assert clean.sent == [[1]]
    assert list(scheduler.workqueue) == [transient_scope]
    assert scheduler._persistent_processes[clean] == {"runtime_b"}
    assert scheduler._reserved_process_slots() == 2


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
    scheduler.registered_collections = {
        owner: ["test_serverless.py::test_done", waiting_nodeid],
        waiter: [waiting_nodeid],
    }

    scheduler._reschedule(waiter)
    assert not waiter.shutting_down
    scheduler._reschedule(owner)
    assert owner.shutting_down
    assert owner in scheduler._retiring_nodes

    assert scheduler.remove_node(owner) is None
    assert waiter.sent == [[0]]


def test_lightweight_runway_preserves_successor_for_session_rotation(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    owner = FakeWorker(exitstatus=0)
    successor = FakeWorker()
    completed_scope = (
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}serverless_runtime--module--done"
    )
    transient_scope = f"{PROCESS_GROUP_PREFIX}test--waiting"
    waiting_nodeid = "test_process.py::test_waiting"
    scheduler._retiring_nodes = set()
    scheduler._persistent_processes = {owner: {"serverless_runtime"}}
    scheduler.assigned_work = {
        owner: {completed_scope: {"test_serverless.py::test_done": True}},
        successor: {"light--test--queued": {"test_light.py::test_queued": False}},
    }
    scheduler.workqueue = OrderedDict({transient_scope: {waiting_nodeid: False}})
    scheduler.registered_collections = {
        successor: ["test_light.py::test_queued", waiting_nodeid]
    }

    scheduler._reschedule(successor)

    assert owner.shutting_down
    assert owner in scheduler._retiring_nodes
    assert not successor.shutting_down

    assert scheduler.remove_node(owner) is None
    assert successor.sent == [[1]]


def test_process_runway_preserves_last_successor_for_queued_work(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 2
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    first = FakeWorker()
    second = FakeWorker()
    first_scope = f"{PROCESS_GROUP_PREFIX}test--first"
    second_scope = f"{PROCESS_GROUP_PREFIX}test--second"
    queued_scope = f"{MIXED_PROCESS_GROUP_PREFIX}new_runtime--module--queued"
    scheduler._retiring_nodes = set()
    scheduler._persistent_processes = {}
    scheduler.assigned_work = {
        first: {first_scope: {"test_process.py::test_first": False}},
        second: {second_scope: {"test_process.py::test_second": False}},
    }
    scheduler.workqueue = OrderedDict(
        {queued_scope: {"test_process.py::test_queued": False}}
    )
    scheduler.registered_collections = {
        first: ["test_process.py::test_first", "test_process.py::test_queued"],
        second: ["test_process.py::test_second", "test_process.py::test_queued"],
    }

    scheduler._reschedule(first)
    scheduler._reschedule(second)

    retiring = [node for node in (first, second) if node.shutting_down]
    successors = [node for node in (first, second) if not node.shutting_down]
    assert len(retiring) == 1
    assert len(successors) == 1
    assert scheduler.workqueue

    retired_workload = scheduler.assigned_work[retiring[0]]
    for work_unit in retired_workload.values():
        for nodeid in work_unit:
            work_unit[nodeid] = True
    scheduler._reschedule(successors[0])

    assert successors[0].sent == [[1]]
    assert not scheduler.workqueue


def test_session_rotation_preserves_owner_needed_by_queued_work(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 2
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    useful_owner = FakeWorker()
    unrelated_owner = FakeWorker(exitstatus=0)
    successor = FakeWorker()
    useful_scope = f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime_a--module--done"
    unrelated_scope = f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime_b--module--done"
    queued_scope = f"{MIXED_PROCESS_GROUP_PREFIX}runtime_a--module--queued"
    queued_nodeid = "test_mixed.py::test_queued"
    scheduler._retiring_nodes = set()
    scheduler._persistent_processes = {
        useful_owner: {"runtime_a"},
        unrelated_owner: {"runtime_b"},
    }
    scheduler.assigned_work = {
        useful_owner: {useful_scope: {"test_a.py::test_done": True}},
        unrelated_owner: {unrelated_scope: {"test_b.py::test_done": True}},
        successor: {"light--test--queued": {"test_light.py::test_queued": False}},
    }
    scheduler.workqueue = OrderedDict({queued_scope: {queued_nodeid: False}})
    scheduler.registered_collections = {
        useful_owner: [queued_nodeid],
        successor: ["test_light.py::test_queued", queued_nodeid],
    }

    scheduler._reschedule(useful_owner)

    assert unrelated_owner.shutting_down
    assert unrelated_owner in scheduler._retiring_nodes
    assert not useful_owner.shutting_down

    assert scheduler.remove_node(unrelated_owner) is None
    assert useful_owner.sent == [[0]]
    assert not scheduler.workqueue


def test_lightweight_runway_waits_for_transient_slot_release(tmp_path: Path) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 2
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    owner = FakeWorker()
    active = FakeWorker()
    successor = FakeWorker()
    scheduler._retiring_nodes = set()
    scheduler._persistent_processes = {owner: {"serverless_runtime"}}
    scheduler.assigned_work = {
        owner: {},
        active: {
            f"{PROCESS_GROUP_PREFIX}test--active": {
                "test_process.py::test_active": False
            }
        },
        successor: {"light--test--queued": {"test_light.py::test_queued": False}},
    }
    scheduler.workqueue = OrderedDict(
        {
            f"{PROCESS_GROUP_PREFIX}test--waiting": {
                "test_process.py::test_waiting": False
            }
        }
    )
    scheduler.registered_collections = {owner: [], active: [], successor: []}

    scheduler._reschedule(successor)

    assert not owner.shutting_down
    assert not successor.shutting_down
    assert scheduler._retiring_nodes == set()


def test_lightweight_runway_waits_when_session_owner_can_continue(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    owner = FakeWorker()
    successor = FakeWorker()
    matching_scope = (
        f"{PERSISTENT_PROCESS_GROUP_PREFIX}serverless_runtime--module--next"
    )
    scheduler._retiring_nodes = set()
    scheduler._persistent_processes = {owner: {"serverless_runtime"}}
    scheduler.assigned_work = {
        owner: {},
        successor: {"light--test--queued": {"test_light.py::test_queued": False}},
    }
    scheduler.workqueue = OrderedDict(
        {matching_scope: {"test_serverless.py::test_next": False}}
    )
    scheduler.registered_collections = {owner: [], successor: []}

    scheduler._reschedule(successor)

    assert not owner.shutting_down
    assert not successor.shutting_down
    assert scheduler._retiring_nodes == set()


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


def test_crash_recovery_preserves_global_duration_priority(tmp_path: Path) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 2
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    owner = FakeWorker()
    clean = FakeWorker()
    crashed = FakeWorker(exitstatus=2)
    owner_nodeid = "test_a.py::test_done"
    persistent_nodeid = "test_b.py::test_long"
    transient_nodeid = "test_transient.py::test_short"
    crash_nodeid = "test_crash.py::test_retry"
    owner_scope = f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime_a--module--done"
    persistent_scope = f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime_b--module--long"
    transient_scope = f"{PROCESS_GROUP_PREFIX}test--short"
    crash_scope = "light--test--crash"
    collection = [
        owner_nodeid,
        persistent_nodeid,
        transient_nodeid,
        crash_nodeid,
    ]
    scheduler.duration_history.tests = {
        persistent_nodeid: {"seconds": 100.0, "samples": 1},
        transient_nodeid: {"seconds": 1.0, "samples": 1},
        crash_nodeid: {"seconds": 0.1, "samples": 1},
    }
    scheduler._retiring_nodes = set()
    scheduler._starting_replacements = set()
    scheduler._collecting_nodes = set()
    scheduler._transient_handoffs = set()
    scheduler._persistent_processes = {owner: {"runtime_a"}}
    scheduler.assigned_work = {
        owner: {owner_scope: {owner_nodeid: True}},
        clean: {},
        crashed: {crash_scope: {crash_nodeid: False}},
    }
    scheduler.registered_collections = {
        owner: collection,
        clean: collection,
        crashed: collection,
    }
    scheduler.workqueue = OrderedDict(
        {
            persistent_scope: {persistent_nodeid: False},
            transient_scope: {transient_nodeid: False},
        }
    )

    assert scheduler.remove_node(crashed) == crash_nodeid

    assert clean.sent == [[1]]
    assert owner.sent == [[3]]
    assert list(scheduler.workqueue) == [transient_scope]
    assert scheduler._persistent_processes[clean] == {"runtime_b"}
    assert scheduler._reserved_process_slots() == 2


def test_completed_worker_removal_immediately_reuses_released_process_slot(
    tmp_path: Path,
) -> None:
    scheduler = IsolationAwareScheduling.__new__(IsolationAwareScheduling)
    scheduler.process_slots = 1
    scheduler.duration_history = DurationHistory(tmp_path / "durations.json")
    owner = FakeWorker(exitstatus=2)
    clean = FakeWorker()
    owner_nodeid = "test_a.py::test_done"
    queued_nodeid = "test_b.py::test_waiting"
    owner_scope = f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime_a--module--done"
    queued_scope = f"{PERSISTENT_PROCESS_GROUP_PREFIX}runtime_b--module--waiting"
    collection = [owner_nodeid, queued_nodeid]
    scheduler._retiring_nodes = set()
    scheduler._starting_replacements = set()
    scheduler._collecting_nodes = set()
    scheduler._transient_handoffs = set()
    scheduler._persistent_processes = {owner: {"runtime_a"}}
    scheduler.assigned_work = {
        owner: {owner_scope: {owner_nodeid: True}},
        clean: {},
    }
    scheduler.registered_collections = {
        owner: collection,
        clean: collection,
    }
    scheduler.workqueue = OrderedDict({queued_scope: {queued_nodeid: False}})

    assert scheduler.remove_node(owner) is None

    assert clean.sent == [[1]]
    assert scheduler.workqueue == {}
    assert scheduler._persistent_processes == {clean: {"runtime_b"}}
    assert scheduler._reserved_process_slots() == 1
