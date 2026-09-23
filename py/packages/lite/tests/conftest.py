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

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import antfly_lite  # noqa: E402
from antfly_lite._library import LibraryNotFoundError  # noqa: E402

REQUIRE_LIBRARY = os.environ.get("ANTFLY_LITE_REQUIRE_LIBRARY") == "1"


def ensure_native_library() -> None:
    """Skip the calling test if libantfly cannot be found, unless
    ANTFLY_LITE_REQUIRE_LIBRARY=1, in which case fail instead."""
    try:
        antfly_lite.validate_abi()
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
