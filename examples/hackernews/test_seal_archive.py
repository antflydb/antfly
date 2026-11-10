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

import hashlib
import json
import pytest
from seal_archive import build


def inventory():
    return {
        "bucket": "archive",
        "prefix": "hn/archive",
        "object_count": 2,
        "objects": [
            {
                "key": f"hn/archive/part-{i}.parquet",
                "generation": str(100 + i),
                "bytes": 500,
                "rows": 20,
            }
            for i in range(2)
        ],
    }


def test_sealed_membership_is_version_bound_content_addressed_and_deterministic():
    original = inventory()
    uri, destination, artifacts = build(original, "hn-history", 1)
    assert len(artifacts) == 3
    head = json.loads(artifacts[uri.rsplit("/", 1)[-1]])
    assert head["source_id"] == "hn-history" and head["file_count"] == 2
    for chunk in head["chunks"]:
        body = artifacts[chunk["uri"].rsplit("/", 1)[-1]]
        assert hashlib.sha256(body).hexdigest() == chunk["sha256"]
        assert len(body) == chunk["byte_len"]
        assert json.loads(body)[0]["version_id"] in ("100", "101")
    assert build(original, "hn-history", 1) == (uri, destination, artifacts)
    original["objects"][0]["generation"] = "102"
    assert build(original, "hn-history", 1)[0] != uri


@pytest.mark.parametrize(
    "change",
    [
        lambda x: x["objects"].append(x["objects"][0]),
        lambda x: x["objects"][0].update(generation=""),
        lambda x: x["objects"][0].update(key="elsewhere/x.parquet"),
        lambda x: x["objects"][0].update(key="hn/archive/../x.parquet"),
        lambda x: x["objects"][0].update(rows=-1),
    ],
)
def test_invalid_or_unfenced_inventory_is_rejected(change):
    source = inventory()
    change(source)
    with pytest.raises(ValueError):
        build(source, "hn-history")
