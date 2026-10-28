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

from pathlib import Path
import sys
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
from cloud_plan import management_plan, native_plan


def test_separate_object_tables_and_scoped_cloud_keys():
    archive, current = str(uuid4()), str(uuid4())
    tables = native_plan("antfly-showcases", archive, current)
    for name in ("archive-table.json", "current-table.json"):
        assert tables[name]["storage"] == {"engine": "object"}
        assert "num_shards" not in tables[name]
        assert tables[name]["indexes"]["body_text"]["field"] == "body"
    base = tables["archive-table.json"]["schema"]["base_source"]
    assert base["format"] == "parquet" and "write_policy" not in base
    assert base["credentials"]["ref"] == "cloud-gcs-cred_" + archive.replace("-", "")
    plan = management_plan(
        "antfly-showcases", "ghcr.io/antflydb/antfly@sha256:" + "a" * 64
    )
    assert plan["create-instance.json"]["version_policy"] == "pinned"
    assert plan["gateway-key.json"]["key_type"] == "read_only"
    assert not plan["gateway-key.json"]["browser_access"]
    assert plan["producer-key.json"]["grants"] == [
        {"table_name": "hn_current", "actions": ["table.write"]}
    ]
    with pytest.raises(ValueError):
        management_plan("antfly-showcases", "ghcr.io/antflydb/antfly:latest")
    with pytest.raises(ValueError):
        native_plan("antfly-showcases", "invalid", current)
