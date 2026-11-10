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

import base64
from copy import deepcopy
from pathlib import Path
import sys
from types import SimpleNamespace

import google_crc32c
import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
from seed_gcs import stage, validated_objects


def manifest(data=b"parquet fixture"):
    return {
        "version": 1,
        "bucket": "test-hn-archive",
        "objects": [
            {
                "name": "archive/part.parquet",
                "generation": "71",
                "size": len(data),
                "crc32c": base64.b64encode(
                    google_crc32c.Checksum(data).digest()
                ).decode(),
            }
        ],
    }


def test_resume_download_is_generation_bound_and_checksum_verified(tmp_path):
    calls = []
    failure = [True]

    def blob(name, generation):
        assert name == "archive/part.parquet" and generation == 71

        def download(path, **kwargs):
            calls.append(kwargs)
            if failure[0]:
                failure[0] = False
                path.write_bytes(b"partial")
                raise OSError("interrupted")
            path.write_bytes(b"parquet fixture")

        return SimpleNamespace(download_to_filename=download)

    client = SimpleNamespace(bucket=lambda name: SimpleNamespace(blob=blob))
    with pytest.raises(OSError):
        stage(client, manifest(), "test-hn-archive", tmp_path, 1024)
    paths = stage(client, manifest(), "test-hn-archive", tmp_path, 1024)
    assert paths[0].read_bytes() == b"parquet fixture"
    assert stage(client, manifest(), "test-hn-archive", tmp_path, 1024) == paths
    assert calls == [{"if_generation_match": 71, "checksum": "crc32c"}] * 2
    paths[0].write_bytes(b"corrupt fixture")
    stage(client, manifest(), "test-hn-archive", tmp_path, 1024)
    assert len(calls) == 3


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", "archive/../stolen.parquet"),
        ("name", "/archive/part.parquet"),
        ("generation", "0"),
        ("size", -1),
        ("crc32c", "AAAA"),
    ],
)
def test_invalid_inventory_rejected_before_network(tmp_path, field, value):
    bad = deepcopy(manifest())
    bad["objects"][0][field] = value
    with pytest.raises(ValueError):
        stage(None, bad, "test-hn-archive", tmp_path, 1024)


def test_inventory_budget_and_symlinks(tmp_path):
    with pytest.raises(ValueError):
        validated_objects(manifest(), "test-hn-archive", 1)
    (tmp_path / "part-00000.parquet").symlink_to(tmp_path / "outside")
    with pytest.raises(ValueError, match="symlink"):
        stage(None, manifest(), "test-hn-archive", tmp_path, 1024)
