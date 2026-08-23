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
import math
import os
import re
import tempfile
from collections.abc import Callable
from fcntl import LOCK_EX, flock
from pathlib import Path
from typing import TypeVar

import pytest
from xdist.remote import Producer
from xdist.scheduler.loadgroup import LoadGroupScheduling
from xdist.workermanage import WorkerController

DURATION_FILE_VERSION = 1
PROCESS_GROUP_PREFIX = "antfly-process--"
PERSISTENT_PROCESS_GROUP_PREFIX = f"{PROCESS_GROUP_PREFIX}persistent--"
MIXED_PROCESS_GROUP_PREFIX = f"{PROCESS_GROUP_PREFIX}mixed--"
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
TRANSIENT_ANTFLY_PROCESS_FIXTURES = ANTFLY_PROCESS_FIXTURES - {
    "embedded_standalone_api",
    "embedded_standalone_cli",
    "embedded_standalone_runtime",
    "serverless_api",
    "serverless_runtime",
    "table_api",
}
# Dynamic fixture dependencies are not present in item.fixturenames. Map the
# collected parameter values that select a session-lived process explicitly.
PERSISTENT_PROCESS_PARAMETERS = {
    ("table_api", "serverless"): "serverless_runtime",
}
RESOURCE_IDENTITY_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
E2E_RESOURCE_ATTRIBUTE = "__antfly_e2e_resources__"
FixtureFunction = TypeVar("FixtureFunction", bound=Callable[..., object])


def e2e_resource(
    name: str,
    *,
    lifetime: str | None = None,
    identity: str | None = None,
) -> Callable[[FixtureFunction], FixtureFunction]:
    """Declare a resource; place this immediately below ``@pytest.fixture``."""

    def decorate(function: FixtureFunction) -> FixtureFunction:
        if getattr(function, "_fixture_function_marker", None) is not None:
            raise TypeError("@e2e_resource must be placed below @pytest.fixture")
        declarations = tuple(getattr(function, E2E_RESOURCE_ATTRIBUTE, ()))
        setattr(
            function,
            E2E_RESOURCE_ATTRIBUTE,
            (*declarations, (name, lifetime, identity)),
        )
        return function

    return decorate


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


def _fixture_resource_declarations(
    item: pytest.Item,
) -> list[tuple[str, object, object, str, str, str]]:
    """Return resource declarations attached to fixtures used by an item."""
    fixture_info = getattr(item, "_fixtureinfo", None)
    if fixture_info is None:
        return []

    declarations: list[tuple[str, object, object, str, str, str]] = []
    for fixture_name in item.fixturenames:
        definitions = fixture_info.name2fixturedefs.get(fixture_name)
        if not definitions:
            continue
        definition = definitions[-1]
        function = getattr(definition, "func", None)
        base_id = str(getattr(definition, "baseid", ""))
        if not base_id:
            module = str(getattr(function, "__module__", ""))
            qualified_name = str(getattr(function, "__qualname__", fixture_name))
            base_id = f"{module}.{qualified_name}"
        provenance = f"{base_id}:{fixture_name}"
        default_identity = (
            f"{fixture_name}."
            f"{hashlib.sha1(provenance.encode('utf-8')).hexdigest()[:10]}"
        )
        for name, lifetime, identity in getattr(function, E2E_RESOURCE_ATTRIBUTE, ()):
            declarations.append(
                (
                    str(name),
                    lifetime,
                    identity,
                    fixture_name,
                    str(definition.scope),
                    default_identity,
                )
            )
    return declarations


def scheduling_group(item: pytest.Item) -> str:
    """Return a stable group encoding process cost and isolation boundary."""
    nodeid = normalize_nodeid(item.nodeid)
    module_id = nodeid.split("::", 1)[0]
    process_scopes = {
        scope
        for fixture_name in ANTFLY_PROCESS_FIXTURES.intersection(item.fixturenames)
        if (scope := _fixture_scope(item, fixture_name)) is not None
    }
    persistent_processes = {
        fixture_name
        for fixture_name in PERSISTENT_ANTFLY_PROCESS_FIXTURES.intersection(
            item.fixturenames
        )
        if _fixture_scope(item, fixture_name) == "session"
    }
    callspec = getattr(item, "callspec", None)
    if callspec is not None:
        for (parameter, value), identity in PERSISTENT_PROCESS_PARAMETERS.items():
            if callspec.params.get(parameter) == value:
                persistent_processes.add(identity)
    declared_process = False
    declared_transient_process = False
    declared_process_scopes: set[str] = set()
    resource_declarations = [
        (
            str(marker.args[0]) if marker.args else "",
            marker.kwargs.get("lifetime"),
            marker.kwargs.get("identity"),
            None,
            None,
            "",
        )
        for marker in item.iter_markers("e2e_resource")
    ]
    resource_declarations.extend(_fixture_resource_declarations(item))
    for (
        resource_name,
        lifetime,
        identity,
        fixture_name,
        fixture_scope,
        default_identity,
    ) in resource_declarations:
        if resource_name != "antfly_process":
            continue
        declaration_context = (
            f" (fixture {fixture_name!r})" if fixture_name is not None else ""
        )
        declared_process = True
        if fixture_scope is not None:
            declared_process_scopes.add(fixture_scope)
        if fixture_scope == "session" and lifetime is None:
            lifetime = "session"
        if lifetime == "session" and identity is None and fixture_name is not None:
            identity = default_identity
        if lifetime is None:
            if identity is not None:
                raise pytest.UsageError(
                    f"{nodeid}{declaration_context}: antfly_process identity requires "
                    "lifetime='session'"
                )
            declared_transient_process = True
            continue
        if lifetime != "session":
            raise pytest.UsageError(
                f"{nodeid}{declaration_context}: antfly_process lifetime must be "
                f"'session', got {lifetime!r}"
            )
        if (
            not isinstance(identity, str)
            or not RESOURCE_IDENTITY_RE.fullmatch(identity)
            or "--" in identity
        ):
            raise pytest.UsageError(
                f"{nodeid}{declaration_context}: a session antfly_process requires "
                "an identity containing only letters, digits, '.', '_' or '-'"
            )
        persistent_processes.add(identity)
    process_scopes.update(declared_process_scopes)
    process_owned = bool(process_scopes) or declared_process
    reuse_process = item.get_closest_marker("reuse_antfly_process") is not None
    force_fresh = item.get_closest_marker("fresh_antfly_process") is not None
    shared_external = item.get_closest_marker("postgres_integration") is not None
    isolation = item.get_closest_marker("e2e_isolation")

    # Preserve fixture sharing at the narrowest safe boundary. Function-scoped
    # process fixtures already provide complete test isolation.
    module_shared = reuse_process and not force_fresh
    module_shared = module_shared or any(
        scope in {"module", "package", "session"} for scope in process_scopes
    )
    module_shared = module_shared or bool(persistent_processes)
    module_shared = module_shared or shared_external
    boundary = (
        "module" if module_shared else "class" if "class" in process_scopes else "test"
    )
    explicit_group: str | None = None
    if isolation is not None:
        policy = str(isolation.args[0]) if isolation.args else "module"
        if policy not in {"test", "module"}:
            raise pytest.UsageError(
                f"{nodeid}: e2e_isolation must be 'test' or 'module', got {policy!r}"
            )
        boundary = policy
        if group := isolation.kwargs.get("group"):
            explicit_group = str(group)
    if explicit_group is not None:
        identity = explicit_group
    elif boundary == "module":
        identity = module_id
    elif boundary == "class":
        identity = nodeid.rsplit("::", 1)[0]
    else:
        identity = nodeid
    kind = f"{boundary}--"
    if persistent_processes:
        identities = "+".join(sorted(persistent_processes))
        transient_process = declared_transient_process or bool(
            TRANSIENT_ANTFLY_PROCESS_FIXTURES.intersection(item.fixturenames)
        )
        prefix = (
            MIXED_PROCESS_GROUP_PREFIX
            if transient_process
            else PERSISTENT_PROCESS_GROUP_PREFIX
        )
        resource = f"{prefix}{identities}--"
    else:
        resource = PROCESS_GROUP_PREFIX if process_owned else "light--"
    return _safe_group_name(f"{resource}{kind}", identity)


def _scheduling_group_parts(group: str) -> tuple[frozenset[str], bool, str]:
    persistent_prefix = next(
        (
            prefix
            for prefix in (
                PERSISTENT_PROCESS_GROUP_PREFIX,
                MIXED_PROCESS_GROUP_PREFIX,
            )
            if group.startswith(prefix)
        ),
        None,
    )
    if persistent_prefix is not None:
        encoded, separator, suffix = group.removeprefix(persistent_prefix).partition(
            "--"
        )
        if not separator or not encoded:
            raise ValueError(f"invalid persistent scheduling group: {group!r}")
        return (
            frozenset(encoded.split("+")),
            persistent_prefix == MIXED_PROCESS_GROUP_PREFIX,
            suffix,
        )
    if group.startswith(PROCESS_GROUP_PREFIX):
        return frozenset(), True, group.removeprefix(PROCESS_GROUP_PREFIX)
    return frozenset(), False, group.removeprefix("light--")


def _consolidate_scheduling_groups(groups: list[str]) -> list[str]:
    """Keep every isolation group together under its strictest resource policy."""
    policies: dict[str, tuple[set[str], bool]] = {}
    for group in groups:
        persistent, transient_process, isolation_suffix = _scheduling_group_parts(group)
        identities, any_transient_process = policies.setdefault(
            isolation_suffix, (set(), False)
        )
        identities.update(persistent)
        policies[isolation_suffix] = (
            identities,
            any_transient_process or transient_process,
        )

    consolidated = []
    for group in groups:
        _, _, isolation_suffix = _scheduling_group_parts(group)
        identities, transient_process = policies[isolation_suffix]
        if identities:
            prefix = (
                MIXED_PROCESS_GROUP_PREFIX
                if transient_process
                else PERSISTENT_PROCESS_GROUP_PREFIX
            )
            resource = f"{prefix}{'+'.join(sorted(identities))}--"
        else:
            resource = PROCESS_GROUP_PREFIX if transient_process else "light--"
        consolidated.append(f"{resource}{isolation_suffix}")
    return consolidated


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
        if not isinstance(payload, dict):
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
            if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
                continue
            try:
                normalized_seconds = float(seconds)
            except (OverflowError, ValueError):
                continue
            if not math.isfinite(normalized_seconds) or normalized_seconds < 0:
                continue
            if isinstance(samples, bool) or not isinstance(samples, int) or samples < 1:
                continue
            self.tests[nodeid] = {
                "seconds": normalized_seconds,
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

    def save(self) -> OSError | None:
        if not self.observed:
            return None

        try:
            self._save_locked()
        except OSError as exc:
            return exc
        return None

    def _save_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self.path.with_name(f".{self.path.name}.lock")
        with lock_path.open("a", encoding="utf-8") as lock_handle:
            # Multiple PR, base, and full jobs share one ARC history file. Reload
            # only after acquiring the advisory lock so no observation is lost
            # to a stale last-writer-wins replacement.
            flock(lock_handle.fileno(), LOCK_EX)
            latest = DurationHistory(self.path)
            merged_tests = dict(self.tests)
            merged_tests.update(latest.tests)
            self._merge_observations(merged_tests)
            self._write(merged_tests)
            self.tests = merged_tests

    def _merge_observations(self, tests: dict[str, dict[str, float | int]]) -> None:
        for nodeid, observed_seconds in self.observed.items():
            previous = tests.get(nodeid)
            if previous is None:
                seconds = observed_seconds
                samples = 1
            else:
                # Favor established CI history while still adapting to real shifts.
                seconds = 0.7 * float(previous["seconds"]) + 0.3 * observed_seconds
                samples = int(previous["samples"]) + 1
            tests[nodeid] = {
                "seconds": round(seconds, 6),
                "samples": samples,
            }

    def _write(self, tests: dict[str, dict[str, float | int]]) -> None:
        payload = {
            "version": DURATION_FILE_VERSION,
            "tests": dict(sorted(tests.items())),
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
            except OSError:
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
        self._persistent_processes: dict[WorkerController, set[str]] = {}

    @staticmethod
    def _scope_uses_process(scope: str) -> bool:
        return scope.startswith(PROCESS_GROUP_PREFIX)

    @staticmethod
    def _scope_persistent_processes(scope: str) -> frozenset[str]:
        prefix = next(
            (
                candidate
                for candidate in (
                    PERSISTENT_PROCESS_GROUP_PREFIX,
                    MIXED_PROCESS_GROUP_PREFIX,
                )
                if scope.startswith(candidate)
            ),
            None,
        )
        if prefix is None:
            return frozenset()
        encoded = scope.removeprefix(prefix).split("--", 1)[0]
        return frozenset(encoded.split("+")) if encoded else frozenset()

    @staticmethod
    def _scope_uses_transient_process(scope: str) -> bool:
        return scope.startswith(MIXED_PROCESS_GROUP_PREFIX) or (
            scope.startswith(PROCESS_GROUP_PREFIX)
            and not scope.startswith(PERSISTENT_PROCESS_GROUP_PREFIX)
        )

    @classmethod
    def _workload_uses_process(cls, workload: dict[str, dict[str, bool]]) -> bool:
        return any(
            cls._scope_uses_process(scope)
            and any(not complete for complete in work_unit.values())
            for scope, work_unit in workload.items()
        )

    @classmethod
    def _workload_uses_transient_process(
        cls, workload: dict[str, dict[str, bool]]
    ) -> bool:
        return any(
            cls._scope_uses_transient_process(scope)
            and any(not complete for complete in work_unit.values())
            for scope, work_unit in workload.items()
        )

    def _reserved_process_slots(self) -> int:
        # A sequential worker needs at most one transient slot, regardless of
        # its runway. Session process identities remain reserved until exit.
        return sum(
            len(self._persistent_processes.get(node, ()))
            + int(self._workload_uses_transient_process(workload))
            for node, workload in self.assigned_work.items()
        )

    def _additional_process_slots(self, node: WorkerController, scope: str) -> int:
        persistent_processes = self._scope_persistent_processes(scope)
        additional = len(
            persistent_processes - self._persistent_processes.get(node, set())
        )
        if not self._scope_uses_process(scope):
            return additional
        if self._scope_uses_transient_process(scope):
            additional += int(
                not self._workload_uses_transient_process(
                    self.assigned_work.get(node, {})
                )
            )
        return additional

    def _scope_duration(self, scope: str, work_unit: dict[str, bool]) -> float:
        process_owned = self._scope_uses_process(scope)
        return sum(
            self.duration_history.estimate(nodeid, process_owned=process_owned)
            for nodeid, complete in work_unit.items()
            if not complete
        )

    def _defer_new_persistent_process(self, node: WorkerController, scope: str) -> bool:
        requested = self._scope_persistent_processes(scope)
        owned = self._persistent_processes.get(node, set())
        if not requested or not owned or requested.issubset(owned):
            return False
        # Keep session processes on separate workers when an idle clean worker
        # is available. This preserves parallel execution without changing the
        # slot bound; consolidation onto one worker remains the fallback.
        return any(
            candidate is not node
            and not candidate.shutting_down
            and not self._persistent_processes.get(candidate)
            and self._pending_of(self.assigned_work[candidate]) == 0
            for candidate in self.nodes
        )

    def _next_eligible_scope(self, node: WorkerController) -> str | None:
        reserved_process_slots = self._reserved_process_slots()
        for scope, work_unit in self.workqueue.items():
            if not any(not complete for complete in work_unit.values()):
                continue
            minimum_slots = len(self._scope_persistent_processes(scope)) + int(
                self._scope_uses_transient_process(scope)
            )
            if minimum_slots > self.process_slots:
                raise pytest.UsageError(
                    f"{scope!r} requires {minimum_slots} Antfly process slots, "
                    f"but --e2e-process-slots is {self.process_slots}"
                )
        eligible = [
            (scope, work_unit)
            for scope, work_unit in self.workqueue.items()
            if any(not complete for complete in work_unit.values())
            and not self._defer_new_persistent_process(node, scope)
            and reserved_process_slots + self._additional_process_slots(node, scope)
            <= self.process_slots
        ]
        if not eligible:
            return None
        owned_processes = self._persistent_processes.get(node, set())
        return max(
            eligible,
            key=lambda entry: (
                bool(self._scope_persistent_processes(entry[0]))
                and self._scope_persistent_processes(entry[0]).issubset(
                    owned_processes
                ),
                self._scope_duration(entry[0], entry[1]),
            ),
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
        persistent_processes = self._scope_persistent_processes(scope)
        if persistent_processes:
            self._persistent_processes.setdefault(node, set()).update(
                persistent_processes
            )

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
        pending = self._pending_of(self.assigned_work[node])
        if pending == 0:
            # An idle worker can safely wait for a slot. If it owns a session
            # process that blocks all remaining work, rotate one such worker at
            # a time so teardown releases the lifetime reservation.
            owns_persistent_process = bool(self._persistent_processes.get(node))
            persistent_retirement_in_flight = any(
                self._persistent_processes.get(candidate)
                for candidate in self._retiring_nodes
            )
            other_live_worker = any(
                candidate is not node and not candidate.shutting_down
                for candidate in self.nodes
            )
            if owns_persistent_process and not persistent_retirement_in_flight:
                if not other_live_worker:
                    raise pytest.UsageError(
                        "all remaining work requires a new session process, but no "
                        "worker is available to replace the current session owner; "
                        "increase --e2e-process-slots or run without xdist"
                    )
                self._retiring_nodes.add(node)
                node.shutdown()
            return
        if pending == 1:
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
        self._persistent_processes.pop(node, None)

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
    groups = _consolidate_scheduling_groups([scheduling_group(item) for item in items])
    for item, group in zip(items, groups, strict=True):
        item.add_marker(pytest.mark.xdist_group(group))


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


def pytest_sessionfinish(session: pytest.Session) -> None:
    if _duration_history is not None:
        error = _duration_history.save()
        if error is not None:
            terminal_reporter = session.config.pluginmanager.get_plugin(
                "terminalreporter"
            )
            if terminal_reporter is not None:
                terminal_reporter.write_line(
                    f"warning: could not update E2E duration history at "
                    f"{_duration_history.path}: {error}"
                )
