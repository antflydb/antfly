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

"""Isolation-aware, duration-weighted scheduling for Antfly E2E tests."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from pathlib import Path

import pytest
from xdist.remote import Producer
from xdist.scheduler.loadgroup import LoadGroupScheduling
from xdist.workermanage import WorkerController

DURATION_FILE_VERSION = 1
PROCESS_GROUP_PREFIX = "antfly-process--"
PERSISTENT_PROCESS_GROUP_PREFIX = f"{PROCESS_GROUP_PREFIX}persistent--"
DEFAULT_PROCESS_SECONDS = 5.0
DEFAULT_LIGHT_SECONDS = 0.05

# These fixtures own an Antfly process or cluster. Fixture scope determines
# whether tests can be scheduled independently or must remain on one worker.
ANTFLY_PROCESS_FIXTURES = frozenset(
    {
        "auth_api",
        "backup_api",
        "cdc_stateful_api",
        "cli_server",
        "compact_scaling_cluster",
        "_reusable_backup_runtime",
        "_reusable_stateful_runtime",
        "embedded_standalone_api",
        "embedded_standalone_cli",
        "embedded_standalone_runtime",
        "extension_server",
        "ha_cluster",
        "multi_metadata_backup_cluster",
        "multi_node_scaling_cluster",
        "real_clipclap_backup_api",
        "resolution_cluster",
        "serverless_api",
        "serverless_runtime",
        "split_scaling_cluster",
        "split_status_cluster",
        "standalone_lifecycle_server",
        "stateful_api",
        "stateful_auth_api",
        "table_api",
    }
)

# These are the process-owning roots among the session-scoped fixtures above.
# Depending fixtures such as serverless_api share the same worker reservation.
PERSISTENT_ANTFLY_PROCESS_FIXTURES = frozenset(
    {
        "embedded_standalone_runtime",
        "serverless_runtime",
    }
)


def normalize_nodeid(nodeid: str) -> str:
    """Remove the suffix pytest-xdist adds for loadgroup scheduling."""
    if nodeid.rfind("@") > nodeid.rfind("]"):
        return nodeid.rsplit("@", 1)[0]
    return nodeid


def _safe_group_name(prefix: str, identity: str) -> str:
    readable = re.sub(r"[^A-Za-z0-9_.-]+", "-", identity).strip("-")[-80:]
    digest = hashlib.sha1(identity.encode("utf-8")).hexdigest()[:10]
    return f"{prefix}{readable}-{digest}"


def _fixture_scope(item: pytest.Item, fixture_name: str) -> str | None:
    fixture_info = getattr(item, "_fixtureinfo", None)
    definitions = (
        fixture_info.name2fixturedefs.get(fixture_name)
        if fixture_info is not None
        else None
    )
    if not definitions:
        return None
    return str(definitions[-1].scope)


def scheduling_group(item: pytest.Item) -> str:
    """Return a stable group encoding process cost and isolation boundary."""
    nodeid = normalize_nodeid(item.nodeid)
    module_id = nodeid.split("::", 1)[0]
    process_scopes = {
        scope
        for fixture_name in ANTFLY_PROCESS_FIXTURES.intersection(item.fixturenames)
        if (scope := _fixture_scope(item, fixture_name)) is not None
    }
    persistent_processes = sorted(
        fixture_name
        for fixture_name in PERSISTENT_ANTFLY_PROCESS_FIXTURES.intersection(
            item.fixturenames
        )
        if _fixture_scope(item, fixture_name) == "session"
    )
    declared_resources = {
        str(mark.args[0]) for mark in item.iter_markers("e2e_resource") if mark.args
    }
    process_owned = bool(process_scopes) or "antfly_process" in declared_resources
    reuse_process = item.get_closest_marker("reuse_antfly_process") is not None
    force_fresh = item.get_closest_marker("fresh_antfly_process") is not None
    shared_external = item.get_closest_marker("postgres_integration") is not None
    isolation = item.get_closest_marker("e2e_isolation")

    # A reusable runtime or any module/session fixture must stay on one worker.
    # Function-scoped process fixtures already provide complete test isolation.
    shared = reuse_process and not force_fresh
    shared = shared or any(
        scope in {"module", "package", "session"} for scope in process_scopes
    )
    shared = shared or shared_external
    explicit_group: str | None = None
    if isolation is not None:
        policy = str(isolation.args[0]) if isolation.args else "module"
        if policy not in {"test", "module"}:
            raise pytest.UsageError(
                f"{nodeid}: e2e_isolation must be 'test' or 'module', got {policy!r}"
            )
        shared = policy == "module"
        if group := isolation.kwargs.get("group"):
            explicit_group = str(group)
    identity = explicit_group or (module_id if shared else nodeid)
    kind = "module--" if shared else "test--"
    if persistent_processes:
        resource = PERSISTENT_PROCESS_GROUP_PREFIX
    else:
        resource = PROCESS_GROUP_PREFIX if process_owned else "light--"
    return _safe_group_name(f"{resource}{kind}", identity)


class DurationHistory:
    """Persistent exponentially weighted test duration observations."""

    def __init__(self, path: Path):
        self.path = path
        self.tests: dict[str, dict[str, float | int]] = {}
        self.observed: dict[str, float] = {}
        self._load()

    def _load(self) -> None:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return
        if payload.get("version") != DURATION_FILE_VERSION:
            return
        tests = payload.get("tests")
        if not isinstance(tests, dict):
            return
        for nodeid, entry in tests.items():
            if not isinstance(nodeid, str) or not isinstance(entry, dict):
                continue
            seconds = entry.get("seconds")
            samples = entry.get("samples")
            if not isinstance(seconds, (int, float)) or seconds < 0:
                continue
            if not isinstance(samples, int) or samples < 1:
                continue
            self.tests[nodeid] = {
                "seconds": float(seconds),
                "samples": samples,
            }

    def estimate(self, nodeid: str, *, process_owned: bool) -> float:
        entry = self.tests.get(normalize_nodeid(nodeid))
        if entry is not None:
            return float(entry["seconds"])
        return DEFAULT_PROCESS_SECONDS if process_owned else DEFAULT_LIGHT_SECONDS

    def observe(self, nodeid: str, duration: float) -> None:
        normalized = normalize_nodeid(nodeid)
        self.observed[normalized] = self.observed.get(normalized, 0.0) + max(
            0.0, duration
        )

    def save(self) -> None:
        if not self.observed:
            return
        for nodeid, observed_seconds in self.observed.items():
            previous = self.tests.get(nodeid)
            if previous is None:
                seconds = observed_seconds
                samples = 1
            else:
                # Favor established CI history while still adapting to real shifts.
                seconds = 0.7 * float(previous["seconds"]) + 0.3 * observed_seconds
                samples = int(previous["samples"]) + 1
            self.tests[nodeid] = {
                "seconds": round(seconds, 6),
                "samples": samples,
            }

        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": DURATION_FILE_VERSION,
            "tests": dict(sorted(self.tests.items())),
        }
        fd, temporary_path = tempfile.mkstemp(
            prefix=f".{self.path.name}.",
            dir=self.path.parent,
            text=True,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
                handle.write("\n")
            os.replace(temporary_path, self.path)
        finally:
            try:
                os.unlink(temporary_path)
            except FileNotFoundError:
                pass


class IsolationAwareScheduling(LoadGroupScheduling):
    """Longest-first loadgroup scheduling with a separate process budget."""

    def __init__(self, config: pytest.Config, log: Producer | None = None):
        super().__init__(config, log)
        self.process_slots = int(config.getoption("e2e_process_slots"))
        self.duration_history = DurationHistory(
            Path(config.getoption("e2e_duration_file"))
        )
        self._retiring_nodes: set[WorkerController] = set()
        self._persistent_process_workers: set[WorkerController] = set()

    @staticmethod
    def _scope_uses_process(scope: str) -> bool:
        return scope.startswith(PROCESS_GROUP_PREFIX)

    @staticmethod
    def _scope_uses_persistent_process(scope: str) -> bool:
        return scope.startswith(PERSISTENT_PROCESS_GROUP_PREFIX)

    @classmethod
    def _workload_uses_process(cls, workload: dict[str, dict[str, bool]]) -> bool:
        return any(
            cls._scope_uses_process(scope)
            and any(not complete for complete in work_unit.values())
            for scope, work_unit in workload.items()
        )

    def _reserved_process_workers(self) -> int:
        # xdist requires each worker to have a second queued item before it can
        # start the first. Reserve capacity per sequential worker queue rather
        # than per queued work unit. A session fixture keeps its worker's
        # reservation after its own work unit completes.
        return sum(
            1
            for node, workload in self.assigned_work.items()
            if node in self._persistent_process_workers
            or self._workload_uses_process(workload)
        )

    def _scope_duration(self, scope: str, work_unit: dict[str, bool]) -> float:
        process_owned = self._scope_uses_process(scope)
        return sum(
            self.duration_history.estimate(nodeid, process_owned=process_owned)
            for nodeid, complete in work_unit.items()
            if not complete
        )

    def _next_eligible_scope(self, node: WorkerController) -> str | None:
        node_has_process_reservation = (
            node in self._persistent_process_workers
            or self._workload_uses_process(self.assigned_work.get(node, {}))
        )
        process_capacity = (
            node_has_process_reservation
            or self._reserved_process_workers() < self.process_slots
        )
        eligible = [
            (scope, work_unit)
            for scope, work_unit in self.workqueue.items()
            if any(not complete for complete in work_unit.values())
            and (process_capacity or not self._scope_uses_process(scope))
        ]
        if not eligible:
            return None
        return max(
            eligible,
            key=lambda entry: self._scope_duration(entry[0], entry[1]),
        )[0]

    def _assign_work_unit(self, node: WorkerController) -> None:
        scope = self._next_eligible_scope(node)
        if scope is None:
            return
        work_unit = self.workqueue.pop(scope)
        self.assigned_work.setdefault(node, {})[scope] = work_unit
        worker_collection = self.registered_collections[node]
        node.send_runtest_some(
            [
                worker_collection.index(nodeid)
                for nodeid, complete in work_unit.items()
                if not complete
            ]
        )
        if self._scope_uses_persistent_process(scope):
            self._persistent_process_workers.add(node)

    def _reschedule(self, node: WorkerController) -> None:
        if node.shutting_down:
            return
        if not self.workqueue:
            node.shutdown()
            return
        # Keep xdist's two-item runway. Workers fetch their next item before
        # running the current one, so a single queued item without a following
        # item or shutdown command deadlocks the worker protocol.
        if self._pending_of(self.assigned_work[node]) > 2:
            return
        if self._next_eligible_scope(node) is not None:
            self._assign_work_unit(node)
            return
        # This worker cannot reserve an Antfly process slot and no lightweight
        # work remains. A shutdown command lets it finish its queued item while
        # the process-reserved workers drain the remaining queue.
        if self._pending_of(self.assigned_work[node]) <= 1:
            self._retiring_nodes.add(node)
            node.shutdown()

    def mark_test_complete(
        self,
        node: WorkerController,
        item_index: int,
        duration: float = 0,
    ) -> None:
        nodeid = self.registered_collections[node][item_index]
        scope = self._split_scope(nodeid)
        self.assigned_work[node][scope][nodeid] = True
        # Reconsider every idle worker whenever a resource slot is released.
        for candidate in self.nodes:
            self._reschedule(candidate)

    def remove_node(self, node: WorkerController) -> str | None:
        workeroutput = getattr(node, "workeroutput", None)
        exitstatus = workeroutput.get("exitstatus") if workeroutput else None
        retired_cleanly = node in self._retiring_nodes and exitstatus in {0, 1}
        self._retiring_nodes.discard(node)
        self._persistent_process_workers.discard(node)

        if retired_cleanly:
            workload = self.assigned_work.pop(node)
            # Depending on event timing, xdist may stop before its last queued
            # item. Put only genuinely incomplete items back into circulation;
            # the worker confirmed a normal exit after intentional retirement.
            for scope, work_unit in workload.items():
                incomplete = {
                    nodeid: False
                    for nodeid, complete in work_unit.items()
                    if not complete
                }
                if incomplete:
                    self.workqueue.setdefault(scope, {}).update(incomplete)
            for candidate in list(self.assigned_work):
                self._reschedule(candidate)
            return None

        crashitem = super().remove_node(node)
        for scope in list(self.workqueue):
            if all(self.workqueue[scope].values()):
                del self.workqueue[scope]
        return crashitem


_duration_history: DurationHistory | None = None


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("antfly-e2e-scheduler")
    group.addoption(
        "--e2e-process-slots",
        type=int,
        default=int(os.environ.get("ANTFLY_E2E_PROCESS_SLOTS", "2")),
        help="Maximum concurrently scheduled Antfly process or cluster work units.",
    )
    group.addoption(
        "--e2e-duration-file",
        default=os.environ.get(
            "ANTFLY_E2E_DURATION_FILE",
            str(Path(__file__).parent / ".pytest_cache" / "durations-v1.json"),
        ),
        help="Persistent JSON file used for duration-weighted scheduling.",
    )


def pytest_configure(config: pytest.Config) -> None:
    global _duration_history
    slots = int(config.getoption("e2e_process_slots"))
    if slots < 1:
        raise pytest.UsageError("--e2e-process-slots must be a positive integer")
    if not hasattr(config, "workerinput"):
        _duration_history = DurationHistory(Path(config.getoption("e2e_duration_file")))


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        item.add_marker(pytest.mark.xdist_group(scheduling_group(item)))


def pytest_xdist_make_scheduler(
    config: pytest.Config,
    log: Producer,
) -> IsolationAwareScheduling | None:
    if config.getvalue("dist") != "loadgroup":
        return None
    return IsolationAwareScheduling(config, log)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if _duration_history is not None:
        _duration_history.observe(report.nodeid, report.duration)


def pytest_sessionfinish() -> None:
    if _duration_history is not None:
        _duration_history.save()
