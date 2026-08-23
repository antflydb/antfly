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
from collections.abc import Callable, Sequence
from fcntl import LOCK_EX, flock
from pathlib import Path
from typing import TypeVar

import pytest
from xdist.remote import Producer
from xdist.report import report_collection_diff
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


def _scope_name(scope: object) -> str:
    """Normalize pytest's Scope enum and test doubles to their public names."""
    return str(getattr(scope, "value", scope))


def _fixture_definition(item: pytest.Item, fixture_name: str) -> object | None:
    fixture_info = getattr(item, "_fixtureinfo", None)
    definitions = (
        fixture_info.name2fixturedefs.get(fixture_name)
        if fixture_info is not None
        else None
    )
    return definitions[-1] if definitions else None


def _effective_fixture_scope(
    item: pytest.Item,
    fixture_name: str,
    definition: object,
) -> str:
    """Return the cache scope pytest will actually use for this item."""
    callspec = getattr(item, "callspec", None)
    parameter_scopes = getattr(callspec, "_arg2scope", {})
    if fixture_name in parameter_scopes:
        return _scope_name(parameter_scopes[fixture_name])
    return _scope_name(definition.scope)  # type: ignore[attr-defined]


def _fixture_scope(item: pytest.Item, fixture_name: str) -> str | None:
    definition = _fixture_definition(item, fixture_name)
    if definition is None:
        return None
    return _effective_fixture_scope(item, fixture_name, definition)


def _fixture_resource_declarations(
    item: pytest.Item,
) -> list[tuple[str, object, object, str, str, str, str]]:
    """Return resource declarations attached to fixtures used by an item."""
    fixture_info = getattr(item, "_fixtureinfo", None)
    if fixture_info is None:
        return []

    declarations: list[tuple[str, object, object, str, str, str, str]] = []
    for fixture_name in item.fixturenames:
        definitions = fixture_info.name2fixturedefs.get(fixture_name)
        if not definitions:
            continue
        definition = definitions[-1]
        function = getattr(definition, "func", None)
        fixture_base_id = str(getattr(definition, "baseid", ""))
        provenance_base_id = fixture_base_id
        if not provenance_base_id:
            module = str(getattr(function, "__module__", ""))
            qualified_name = str(getattr(function, "__qualname__", fixture_name))
            provenance_base_id = f"{module}.{qualified_name}"
        provenance = f"{provenance_base_id}:{fixture_name}"
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
                    _effective_fixture_scope(item, fixture_name, definition),
                    default_identity,
                    fixture_base_id,
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
    package_boundaries = {
        str(getattr(definition, "baseid", "")) or "."
        for fixture_name in ANTFLY_PROCESS_FIXTURES.intersection(item.fixturenames)
        if (definition := _fixture_definition(item, fixture_name)) is not None
        and _effective_fixture_scope(item, fixture_name, definition) == "package"
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
        fixture_base_id,
    ) in resource_declarations:
        if resource_name != "antfly_process":
            continue
        declaration_context = (
            f" (fixture {fixture_name!r})" if fixture_name is not None else ""
        )
        declared_process = True
        if fixture_scope is not None:
            declared_process_scopes.add(fixture_scope)
            if fixture_scope == "package":
                package_boundaries.add(fixture_base_id or ".")
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
    shared_module_boundary = reuse_process and not force_fresh
    shared_module_boundary = shared_module_boundary or "module" in process_scopes
    shared_module_boundary = shared_module_boundary or shared_external
    module_shared = shared_module_boundary or "session" in process_scopes
    module_shared = module_shared or bool(persistent_processes)
    class_id = (
        nodeid.rsplit("::", 1)[0] if getattr(item, "cls", None) is not None else None
    )
    if package_boundaries:
        boundary = "package"
    elif module_shared:
        boundary = "module"
    elif "class" in process_scopes and class_id is not None:
        boundary = "class"
    else:
        boundary = "test"
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
    elif boundary == "package":
        # The shallowest applicable package owns every nested package fixture
        # too, so grouping at it preserves all package-scoped lifecycles.
        identity = min(
            package_boundaries,
            key=lambda package: (package.count("/"), len(package), package),
        )
    elif boundary == "class":
        assert class_id is not None
        identity = class_id
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
    group_identity = identity
    if (
        persistent_processes
        and isolation is None
        and not package_boundaries
        and not shared_module_boundary
    ):
        # Automatically inferred module isolation preserves each session
        # fixture's users without coupling unrelated session processes in the
        # same module. Explicit isolation groups still consolidate every
        # resource they name, and a single item requesting multiple identities
        # continues to reserve all of them together.
        group_identity = f"{identity}::session={'+'.join(sorted(persistent_processes))}"
    return _safe_group_name(f"{resource}{kind}", group_identity)


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
        self._starting_replacements: set[WorkerController] = set()
        self._collecting_nodes: set[WorkerController] = set()
        self._transient_handoffs: set[WorkerController] = set()
        self._persistent_processes: dict[WorkerController, set[str]] = {}

    def add_node(self, node: WorkerController) -> None:
        super().add_node(node)
        self.__dict__.setdefault("_collecting_nodes", set()).add(node)

    def add_node_collection(
        self,
        node: WorkerController,
        collection: Sequence[str],
    ) -> None:
        super().add_node_collection(node, collection)
        if node in self.registered_collections:
            # workerready only means the process connected. Work cannot be
            # indexed safely until the replacement submits the collection it
            # will execute.
            getattr(self, "_collecting_nodes", set()).discard(node)
            getattr(self, "_starting_replacements", set()).discard(node)
            return

        # LoadScopeScheduling rejects a late collection that differs from the
        # established one without registering the worker. It cannot ever
        # become schedulable, so do not leave it advertised as future progress
        # or retry rotations indefinitely.
        getattr(self, "_collecting_nodes", set()).discard(node)
        getattr(self, "_starting_replacements", set()).discard(node)
        node.shutdown()
        worker_id = str(getattr(getattr(node, "gateway", None), "id", "replacement"))
        reference_node, reference_collection = next(
            iter(self.registered_collections.items())
        )
        collection_diff = report_collection_diff(
            reference_collection,
            collection,
            str(getattr(reference_node.gateway, "id", "established")),
            worker_id,
        )
        raise pytest.UsageError(
            f"replacement worker {worker_id} collected different tests from the "
            f"established E2E collection\n{collection_diff}"
        )

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

    def _worker_reserved_process_slots(self, node: WorkerController) -> int:
        transient_slots = int(
            self._workload_uses_transient_process(self.assigned_work[node])
        )
        # A queued persistent-only scope can reuse the lane occupied by the
        # worker's final transient test: pytest tears down that test's fixture
        # before setting up the next item. Keep the future persistent capacity
        # reserved without double-counting the non-overlapping transition.
        if node in getattr(self, "_transient_handoffs", set()):
            transient_slots -= 1
        return len(self._persistent_processes.get(node, ())) + transient_slots

    def _reserved_process_slots(self) -> int:
        # A sequential worker needs at most one transient slot, regardless of
        # its runway. Session process identities remain reserved until exit.
        return sum(
            self._worker_reserved_process_slots(node) for node in self.assigned_work
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
                or node in getattr(self, "_transient_handoffs", set())
            )
        return additional

    def _scope_duration(self, scope: str, work_unit: dict[str, bool]) -> float:
        process_owned = self._scope_uses_process(scope)
        return sum(
            self.duration_history.estimate(nodeid, process_owned=process_owned)
            for nodeid, complete in work_unit.items()
            if not complete
        )

    def _transient_handoff_fits(
        self,
        node: WorkerController,
        scope: str,
        reserved_process_slots: int,
    ) -> bool:
        """Allow a final transient test to hand its lane to persistent work."""
        if (
            self._pending_of(self.assigned_work[node]) != 1
            or not self._workload_uses_transient_process(self.assigned_work[node])
            or self._scope_uses_transient_process(scope)
        ):
            return False
        requested = self._scope_persistent_processes(scope)
        if not requested:
            return False
        additional_persistent = len(
            requested - self._persistent_processes.get(node, set())
        )
        return reserved_process_slots - 1 + additional_persistent <= self.process_slots

    def _defer_new_persistent_process(self, node: WorkerController, scope: str) -> bool:
        requested = self._scope_persistent_processes(scope)
        owned = self._persistent_processes.get(node, set())
        if not requested or not owned or requested.issubset(owned):
            return False
        # Keep session processes on separate workers when an idle clean worker
        # is available. This preserves parallel execution without changing the
        # slot bound; consolidation onto one worker remains the fallback.
        reserved_process_slots = self._reserved_process_slots()
        return any(
            candidate is not node
            and not candidate.shutting_down
            and candidate in self.registered_collections
            and not self._persistent_processes.get(candidate)
            and self._pending_of(self.assigned_work[candidate]) == 0
            and reserved_process_slots
            + self._additional_process_slots(candidate, scope)
            <= self.process_slots
            for candidate in self.nodes
        )

    def _retirement_score(
        self,
        candidate: WorkerController,
    ) -> tuple[float, float, int]:
        """Prefer retirements that unlock work without discarding useful reuse."""
        released_slots = self._worker_reserved_process_slots(candidate)
        reserved_after_retirement = max(
            0, self._reserved_process_slots() - released_slots
        )
        remaining_nodes = [
            node
            for node in self.nodes
            if node is not candidate
            and not node.shutting_down
            and node in self.registered_collections
        ]
        unlocked_duration = 0.0
        reuse_value = 0.0
        owned = self._persistent_processes.get(candidate, set())
        for scope, work_unit in self.workqueue.items():
            if not any(not complete for complete in work_unit.values()):
                continue
            duration = self._scope_duration(scope, work_unit)
            if any(
                reserved_after_retirement + self._additional_process_slots(node, scope)
                <= self.process_slots
                for node in remaining_nodes
            ):
                unlocked_duration += duration
            requested = self._scope_persistent_processes(scope)
            if requested:
                reuse_value += (
                    duration * len(requested.intersection(owned)) / len(requested)
                )
        return unlocked_duration, -reuse_value, released_slots

    def _replacement_controller(self) -> object | None:
        config = getattr(self, "config", None)
        pluginmanager = getattr(config, "pluginmanager", None)
        if pluginmanager is None:
            return None
        controller = pluginmanager.getplugin("dsession")
        return (
            controller if callable(getattr(controller, "_clone_node", None)) else None
        )

    def _start_replacement(self, node: WorkerController) -> bool:
        """Keep intentional resource rotation from consuming the worker pool."""
        controller = self._replacement_controller()
        if controller is None:
            return False
        # xdist exposes worker replacement only through DSession. This is the
        # same path it uses for crashed workers, without charging the crash
        # restart budget or reporting an intentional teardown as a failure.
        replacement = controller._clone_node(node)  # type: ignore[attr-defined]
        self.__dict__.setdefault("_starting_replacements", set()).add(replacement)
        return True

    def _retire_best_resource_worker(self) -> bool:
        """Retire one drainable resource worker while preserving a successor."""
        live_nodes = [node for node in self.nodes if not node.shutting_down]
        can_replace = self._replacement_controller() is not None
        candidates = [
            node
            for node in live_nodes
            if self._pending_of(self.assigned_work[node]) <= 1
            and (
                self._persistent_processes.get(node)
                or self._workload_uses_process(self.assigned_work[node])
            )
            and (can_replace or any(successor is not node for successor in live_nodes))
        ]
        if not candidates:
            return False
        candidate = max(candidates, key=self._retirement_score)
        replacement_started = self._start_replacement(candidate)
        if not replacement_started and not any(
            successor is not candidate for successor in live_nodes
        ):
            return False
        self._retiring_nodes.add(candidate)
        candidate.shutdown()
        return True

    def _another_worker_will_make_progress(
        self,
        node: WorkerController,
    ) -> bool:
        """Return whether a future worker event can reopen scheduling capacity."""
        if getattr(self, "_starting_replacements", set()):
            return True
        if getattr(self, "_collecting_nodes", set()):
            return True
        if any(
            candidate.shutting_down for candidate in self.nodes if candidate is not node
        ):
            return True
        return any(
            candidate is not node and self._pending_of(workload) > 1
            for candidate, workload in self.assigned_work.items()
        )

    def _another_worker_can_accept_work(self, node: WorkerController) -> bool:
        return any(
            candidate is not node
            and not candidate.shutting_down
            and candidate in self.registered_collections
            and self._next_eligible_scope(candidate) is not None
            for candidate in self.nodes
        )

    def _reschedule_priority(
        self,
        node: WorkerController,
    ) -> tuple[bool, float, bool, int]:
        """Rank safe assignments globally without weakening resource reuse."""
        if (
            node.shutting_down
            or node not in self.registered_collections
            or self._pending_of(self.assigned_work[node]) > 2
        ):
            return False, 0.0, False, 0
        scope = self._next_eligible_scope(node)
        if scope is None:
            return False, 0.0, False, 0
        requested = self._scope_persistent_processes(scope)
        owned = self._persistent_processes.get(node, set())
        reuses_reserved_capacity = (
            bool(requested)
            and requested.issubset(owned)
            and self._additional_process_slots(node, scope) == 0
        )
        return (
            reuses_reserved_capacity,
            self._scope_duration(scope, self.workqueue[scope]),
            not bool(owned),
            -self._pending_of(self.assigned_work[node]),
        )

    def _reschedule_all(self) -> None:
        """Reconsider ready workers in resource- and duration-aware order."""
        candidates = sorted(
            self.nodes,
            key=self._reschedule_priority,
            reverse=True,
        )
        for candidate in candidates:
            self._reschedule(candidate)

    def schedule(self) -> None:
        if self.collection is None:
            super().schedule()
            return
        # Late collections come from replacement workers. Reopen scheduling
        # with the same global priority used for resource-release events.
        self._reschedule_all()

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
            and (
                reserved_process_slots + self._additional_process_slots(node, scope)
                <= self.process_slots
                or self._transient_handoff_fits(node, scope, reserved_process_slots)
            )
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
        transient_handoff = self._transient_handoff_fits(
            node, scope, self._reserved_process_slots()
        )
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
        if transient_handoff:
            self.__dict__.setdefault("_transient_handoffs", set()).add(node)

    def _reschedule(self, node: WorkerController) -> None:
        if node.shutting_down or node not in self.registered_collections:
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
        # work remains. Preserve a live successor whenever a shutdown is needed
        # to drain a one-item worker queue or release a persistent reservation.
        pending = self._pending_of(self.assigned_work[node])
        if pending == 0:
            # An idle worker can safely wait for a slot. If it owns a session
            # process that blocks all remaining work, rotate one such worker at
            # a time so teardown releases the lifetime reservation.
            if self._persistent_processes.get(node):
                if self._another_worker_can_accept_work(
                    node
                ) or self._another_worker_will_make_progress(node):
                    return
                if not self._retire_best_resource_worker():
                    raise pytest.UsageError(
                        "all remaining work requires a new session process, but no "
                        "worker is available to replace the current session owner; "
                        "increase --e2e-process-slots or run without xdist"
                    )
            return
        if pending == 1:
            # A one-item worker can wait when another worker will produce an
            # event or accept queued work. Otherwise rotate the resource worker
            # that unlocks the most work, while retaining one live successor to
            # receive the follow-up assignment that unblocks its current item.
            if self._another_worker_can_accept_work(
                node
            ) or self._another_worker_will_make_progress(node):
                return
            if self._retire_best_resource_worker():
                return
            raise pytest.UsageError(
                "all remaining work requires another process slot, but no worker "
                "is available to release or replace the current process owner; "
                "increase --e2e-process-slots or run without xdist"
            )

    def mark_test_complete(
        self,
        node: WorkerController,
        item_index: int,
        duration: float = 0,
    ) -> None:
        nodeid = self.registered_collections[node][item_index]
        scope = self._split_scope(nodeid)
        self.assigned_work[node][scope][nodeid] = True
        if not self._workload_uses_transient_process(self.assigned_work[node]):
            getattr(self, "_transient_handoffs", set()).discard(node)
        # Reconsider every idle worker whenever a resource slot is released.
        self._reschedule_all()

    def remove_node(self, node: WorkerController) -> str | None:
        workeroutput = getattr(node, "workeroutput", None)
        exitstatus = workeroutput.get("exitstatus") if workeroutput else None
        retired_cleanly = node in self._retiring_nodes and exitstatus in {0, 1}
        self._retiring_nodes.discard(node)
        getattr(self, "_starting_replacements", set()).discard(node)
        getattr(self, "_collecting_nodes", set()).discard(node)
        getattr(self, "_transient_handoffs", set()).discard(node)
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
            self._reschedule_all()
            return None

        workload = self.assigned_work.pop(node)
        if not self._pending_of(workload):
            return None

        # Preserve xdist's crash-reporting contract while delaying assignment
        # until every failed scope is back in the queue. Calling the parent
        # implementation here would immediately reschedule in worker insertion
        # order, bypassing the resource- and duration-aware global priority.
        crashitem = next(
            (
                nodeid
                for work_unit in workload.values()
                for nodeid, complete in work_unit.items()
                if not complete
            ),
            None,
        )
        if crashitem is None:
            raise RuntimeError(
                "Unable to identify crashitem on a workload with pending items"
            )

        self.workqueue.update(workload)
        for scope in list(self.workqueue):
            if all(self.workqueue[scope].values()):
                del self.workqueue[scope]
        self._reschedule_all()
        return crashitem


_duration_history: DurationHistory | None = None
_duration_report_totals: dict[str, float] = {}
_executed_duration_nodeids: set[str] = set()


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
    global _duration_history, _duration_report_totals, _executed_duration_nodeids
    slots = int(config.getoption("e2e_process_slots"))
    if slots < 1:
        raise pytest.UsageError("--e2e-process-slots must be a positive integer")
    if not hasattr(config, "workerinput"):
        _duration_history = DurationHistory(Path(config.getoption("e2e_duration_file")))
        _duration_report_totals = {}
        _executed_duration_nodeids = set()


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
    if _duration_history is None:
        return
    nodeid = normalize_nodeid(report.nodeid)
    duration = max(0.0, float(report.duration))
    if math.isfinite(duration):
        _duration_report_totals[nodeid] = (
            _duration_report_totals.get(nodeid, 0.0) + duration
        )
    if report.when == "call" and (
        not report.skipped or getattr(report, "wasxfail", None) is not None
    ):
        _executed_duration_nodeids.add(nodeid)


def _flush_duration_reports() -> None:
    if _duration_history is not None:
        for nodeid in _executed_duration_nodeids:
            _duration_history.observe(nodeid, _duration_report_totals.get(nodeid, 0.0))
    _duration_report_totals.clear()
    _executed_duration_nodeids.clear()


def pytest_sessionfinish(session: pytest.Session) -> None:
    if _duration_history is not None:
        # Setup and teardown are material E2E costs, but only retain their total
        # when the test reached a real call phase. Environment-dependent skips
        # and setup failures must not train expensive full-CI tests toward zero.
        _flush_duration_reports()
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
