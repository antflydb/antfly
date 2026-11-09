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

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import antfly_embedded  # noqa: E402
from antfly_embedded._library import LibraryNotFoundError  # noqa: E402

REQUIRE_LIBRARY = os.environ.get("ANTFLY_LITE_REQUIRE_LIBRARY") == "1"


class BackgroundProgressDeadline:
    """Bound background work by both durable inactivity and total elapsed time."""

    def __init__(self, *, idle_seconds=30, total_seconds=120, clock=time.monotonic):
        self.clock = clock
        self.started = self.last_progress = clock()
        self.idle_seconds = idle_seconds
        self.total_seconds = total_seconds
        self.high_water: dict[tuple[str, ...], int] = {}

    def expired(self, stats, *, completed=0):
        now = self.clock()
        progress: dict[tuple[str, ...], int] = {("result", "completed"): completed}
        for stage in ("enrichment", "resolution", "promotion"):
            progress[(stage, "applied_sequence")] = stats.get(stage, {}).get("applied_sequence", 0)
        for replacement in stats.get("replacements", []):
            for field in ("applied_sequence", "scanned_keys"):
                progress[("replacement", replacement["name"], field)] = replacement.get(field, 0)
        advanced = False
        for key, value in progress.items():
            if value > self.high_water.get(key, 0):
                self.high_water[key] = value
                advanced = True
        if advanced:
            self.last_progress = now
        return now - self.started >= self.total_seconds or now - self.last_progress >= self.idle_seconds


@pytest.fixture
def background_progress_deadline():
    return BackgroundProgressDeadline


def ensure_native_library() -> None:
    """Skip the calling test if libantfly cannot be found, unless
    ANTFLY_LITE_REQUIRE_LIBRARY=1, in which case fail instead."""
    try:
        antfly_embedded.validate_abi()
    except LibraryNotFoundError as exc:
        if REQUIRE_LIBRARY:
            pytest.fail(f"libantfly required (ANTFLY_LITE_REQUIRE_LIBRARY=1) but not found: {exc}")
        pytest.skip(f"libantfly not found: {exc}")


@pytest.fixture(scope="session")
def require_native() -> None:
    ensure_native_library()


@pytest.fixture()
def aflite_path(tmp_path: Path) -> Path:
    return tmp_path / "db.aflite"


def _native_report_root() -> Path | None:
    value = os.environ.get("ANTFLY_SDK_NATIVE_REPORT_DIR")
    if not value:
        return None
    root = Path(value)
    root.mkdir(parents=True, exist_ok=True)
    return root


def pytest_runtest_logstart(nodeid, location):
    del location
    if root := _native_report_root():
        (root / "current-test.txt").write_text(nodeid + "\n")


def pytest_runtest_logreport(report):
    # JUnit is finalized only when pytest exits normally. Preserve completed
    # phases and the active identity even if CI kills a stuck native call.
    if root := _native_report_root():
        event = {
            "nodeid": report.nodeid,
            "phase": report.when,
            "seconds": report.duration,
            "outcome": report.outcome,
        }
        if report.failed:
            event["failure"] = report.longreprtext[-65536:]
        with (root / "timings.jsonl").open("a") as output:
            output.write(json.dumps(event) + "\n")


def native_test_partition(nodeid):
    """Keep each physical-work family and every storage parameter together."""
    module, _, test = nodeid.partition("::")
    if Path(module).name != "test_cypher_performance.py":
        return "core"
    family = test.partition("[")[0]
    return f"cypher-{hashlib.sha256(family.encode()).digest()[0] % 2}"


def pytest_collection_modifyitems(config, items):
    shard = os.environ.get("ANTFLY_SDK_PYTHON_SHARD", "")
    if not shard:
        return
    if shard not in {"core", "cypher-0", "cypher-1"}:
        raise pytest.UsageError(f"Unknown native Python shard: {shard}")
    selected, deselected = [], []
    for item in items:
        (selected if native_test_partition(item.nodeid) == shard else deselected).append(item)
    if not selected:
        raise pytest.UsageError(f"Native Python shard {shard} selected no tests")
    config.hook.pytest_deselected(items=deselected)
    items[:] = selected


def pytest_collection_finish(session):
    if root := _native_report_root():
        (root / "collection.json").write_text(json.dumps([item.nodeid for item in session.items]) + "\n")
