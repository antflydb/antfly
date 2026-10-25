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

"""Fixture admission follows production's minimum, percentage and capped reserve."""

from pathlib import Path
from types import SimpleNamespace

import conftest
import pytest


@pytest.mark.parametrize("capacity_gib,floor_gib", [(10, 1), (100, 5), (1000, 16)])
def test_standalone_storage_headroom_boundaries(monkeypatch, capacity_gib, floor_gib):
    required = floor_gib * 1024**3 + 256 * 1024**2
    usage = SimpleNamespace(total=capacity_gib * 1024**3, free=required)
    monkeypatch.setattr(conftest.shutil, "disk_usage", lambda _: usage)
    conftest.require_standalone_storage_headroom(Path("/fixture"))
    usage.free -= 1
    with pytest.raises(RuntimeError, match=f"required_bytes={required}"):
        conftest.require_standalone_storage_headroom(Path("/fixture"))
