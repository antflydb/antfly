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
