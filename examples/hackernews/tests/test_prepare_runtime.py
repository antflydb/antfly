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

import copy
import hashlib
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
from prepare_runtime import PIN, verify_archive, verify_metadata


def test_metadata_binds_exact_source_and_each_library():
    metadata = {
        "tag": PIN["version"],
        "commit": PIN["source_commit"],
        "artifacts": list(PIN["libraries"].values()),
    }
    verify_metadata(metadata)
    for key in ("tag", "commit"):
        changed = copy.deepcopy(metadata)
        changed[key] = "wrong"
        with pytest.raises(ValueError):
            verify_metadata(changed)
    changed = copy.deepcopy(metadata)
    changed["artifacts"][0]["sha256"] = "0" * 64
    with pytest.raises(ValueError):
        verify_metadata(changed)


def test_corrupt_or_truncated_library_is_rejected(tmp_path):
    archive = tmp_path / "library.tar.gz"
    archive.write_bytes(b"verified bytes")
    asset = {
        "name": archive.name,
        "size": archive.stat().st_size,
        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
    }
    verify_archive(archive, asset)
    archive.write_bytes(b"corrupt! bytes")
    with pytest.raises(ValueError):
        verify_archive(archive, asset)
    archive.write_bytes(b"short")
    with pytest.raises(ValueError):
        verify_archive(archive, asset)
