# Copyright 2026 Antfly, Inc.
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

"""Exhaustive, fixture-preserving E2E partitions with a shared measured plan.

The legacy two-recovery-lane selection remains available for trusted workflows
on main checking out a candidate which has not yet landed this workflow update.
"""

import json
import math
import os
import zlib
from pathlib import Path

SHARDS = (
    "all",
    "ordinary",
    "ordinary-0",
    "ordinary-1",
    "ordinary-2",
    "recovery-0",
    "recovery-1",
    "recovery-2",
)
DISTRIBUTED_FIXTURE = "three_by_three_backup_cluster"
SEED = Path(__file__).with_name("antfly_e2e_durations.json")


def canonical_nodeid(nodeid):
    if nodeid.rfind("@") > nodeid.rfind("]"):
        nodeid = nodeid.rsplit("@", 1)[0]
    path, separator, test = nodeid.partition("::")
    return path.replace("\\", "/").rsplit("/", 1)[-1] + separator + test


def shard_for_item(item):
    if DISTRIBUTED_FIXTURE not in item.fixturenames:
        return "ordinary"
    return f"recovery-{zlib.crc32(canonical_nodeid(item.nodeid).encode()) % 2}"


def load_history(path):
    return {
        node: entry["seconds"] for node, entry in load_history_entries(path).items()
    }


def load_history_entries(path):
    data = json.loads(Path(path).read_text())
    if data.get("version") != 1 or not isinstance(data.get("tests"), dict):
        raise ValueError(f"invalid E2E duration history: {path}")
    result = {}
    for node, entry in data["tests"].items():
        if not isinstance(node, str) or not isinstance(entry, dict):
            raise TypeError(f"invalid E2E duration entry: {path}")
        value = entry.get("seconds")
        samples = entry.get("samples")
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
            or isinstance(samples, bool)
            or not isinstance(samples, int)
            or samples < 1
        ):
            raise ValueError(f"invalid E2E duration for {node}")
        normalized = canonical_nodeid(node)
        previous = result.get(normalized)
        if previous is not None and (
            samples < previous["samples"]
            or (samples == previous["samples"] and node != normalized)
        ):
            continue
        result[normalized] = {"seconds": value, "samples": samples}
    return result


def make_plan(items, history):
    # Use the same consolidated isolation groups as the runtime scheduler.
    from e2e_scheduler import _consolidate_scheduling_groups, scheduling_group

    return balance_records(
        [
            (
                canonical_nodeid(item.nodeid),
                group,
                "recovery" if DISTRIBUTED_FIXTURE in item.fixturenames else "ordinary",
                history.get(
                    canonical_nodeid(item.nodeid),
                    5.0 if group.startswith("antfly-process--") else 0.05,
                ),
            )
            for item, group in zip(
                items,
                _consolidate_scheduling_groups([scheduling_group(i) for i in items]),
                strict=True,
            )
        ]
    )


def balance_records(records):
    groups = {}
    nodes = set()
    for node, group, family, seconds in records:
        if node in nodes:
            raise ValueError(f"duplicate E2E identity: {node}")
        nodes.add(node)
        entry = groups.setdefault(
            group, {"nodes": [], "families": set(), "seconds": 0.0}
        )
        entry["nodes"].append(node)
        entry["families"].add(family)
        entry["seconds"] += seconds
    assignments, totals = {}, {}
    node_groups = {
        node: group for group, entry in groups.items() for node in entry["nodes"]
    }
    for family in ("ordinary", "recovery"):
        totals[family] = [0.0] * 3
        selected = []
        for group, entry in groups.items():
            if len(entry["families"]) != 1:
                raise ValueError(f"isolation group crosses E2E families: {group}")
            if family in entry["families"]:
                selected.append((group, entry))
        for group, entry in sorted(
            selected, key=lambda pair: (-pair[1]["seconds"], pair[0])
        ):
            bucket = min(range(3), key=lambda i: (totals[family][i], i))
            totals[family][bucket] += entry["seconds"]
            for node in sorted(entry["nodes"]):
                assignments[node] = f"{family}-{bucket}"
    return {
        "version": 2,
        "assignments": dict(sorted(assignments.items())),
        "estimated_seconds": totals,
        "groups": dict(sorted(node_groups.items())),
    }


def pytest_addoption(parser):
    group = parser.getgroup("antfly-ci")
    group.addoption("--antfly-ci-shard", choices=SHARDS, default="all")
    group.addoption("--antfly-ci-plan", default=os.environ.get("ANTFLY_E2E_SHARD_PLAN"))
    group.addoption("--antfly-ci-plan-output", default=None)


def pytest_collection_modifyitems(config, items):
    output = config.getoption("antfly_ci_plan_output", default=None)
    plan_path = config.getoption("antfly_ci_plan", default=None)
    shard = config.getoption("antfly_ci_shard")
    if output:
        history_path = os.environ.get("ANTFLY_E2E_DURATION_FILE")
        history = load_history(
            history_path if history_path and Path(history_path).exists() else SEED
        )
        plan = make_plan(items, history)
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n")
        return
    if shard == "all":
        return
    plan = None
    if plan_path:
        plan = json.loads(Path(plan_path).read_text())
        if plan.get("version") != 2:
            raise ValueError("unsupported E2E shard plan")
        expected = set(plan["assignments"])
        actual = {canonical_nodeid(i.nodeid) for i in items}
        if expected != actual:
            raise ValueError(
                f"E2E collection differs from shared plan: missing={sorted(expected - actual)}, added={sorted(actual - expected)}"
            )
    elif shard.startswith("ordinary-") or shard == "recovery-2":
        raise ValueError("three-lane E2E selection requires --antfly-ci-plan")
    selected, deselected = [], []
    for item in items:
        assigned = (
            plan["assignments"][canonical_nodeid(item.nodeid)]
            if plan
            else shard_for_item(item)
        )
        keep = assigned == shard or (
            shard == "ordinary" and assigned.startswith("ordinary-")
        )
        (selected if keep else deselected).append(item)
    items[:] = selected
    if deselected:
        config.hook.pytest_deselected(items=deselected)


# Keep planning/merge usable by plain Python. Pytest applies this hook after
# marker deselection and fixture-group decoration.
try:
    import pytest
except ModuleNotFoundError:
    pass
else:
    pytest_collection_modifyitems = pytest.hookimpl(trylast=True)(
        pytest_collection_modifyitems
    )
