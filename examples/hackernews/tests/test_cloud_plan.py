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

import sys
from pathlib import Path
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
from cloud_plan import RUNTIME, management_plan, native_plan


def test_separate_object_tables_and_scoped_cloud_keys():
    archive, current = str(uuid4()), str(uuid4())
    tables = native_plan("antfly-showcases", archive, current)
    for name in ("archive-table.json", "current-table.json"):
        assert tables[name]["storage"] == {"engine": "object"}
        assert "num_shards" not in tables[name]
        assert tables[name]["indexes"]["body_text"] == {
            "type": "full_text",
            "store_source": True,
        }
        props = tables[name]["schema"]["document_schemas"]["row"]["schema"][
            "properties"
        ]
        assert props["item_type"]["x-antfly-field"]["type"] == "keyword"
    base = tables["archive-table.json"]["schema"]["base_source"]
    assert base["format"] == "parquet" and "write_policy" not in base
    assert base["credentials"]["ref"] == "cloud-gcs-cred_" + archive.replace("-", "")
    relation_names = set()
    for table, filename in (
        ("hn_archive", "archive-table.json"),
        ("hn_current", "current-table.json"),
    ):
        indexes = tables[filename]["schema"]["relational_indexes"]
        names = {index["name"] for index in indexes}
        assert len(names) == len(indexes)
        assert all(name.startswith(table + "_") for name in names)
        assert not relation_names.intersection(names)
        relation_names.update(names)
    plan = management_plan("antfly-showcases", RUNTIME["image"])
    assert plan["create-instance.json"]["version_policy"] == "pinned"
    assert "native_artifacts" not in plan["create-instance.json"]
    assert plan["create-instance.json"]["storage"]["artifacts"] == {
        "protocol": "gcs",
        "bucket": "antfly-showcases-hn-warehouse",
        "prefix": "native-indexes",
    }

    assert plan["gateway-key.json"]["key_type"] == "read_only"
    assert not plan["gateway-key.json"]["browser_access"]
    assert plan["producer-key.json"]["grants"] == [
        {"table_name": "hn_current", "actions": ["table.write"]}
    ]
    with pytest.raises(ValueError):
        management_plan("antfly-showcases", "ghcr.io/antflydb/antfly:latest")
    with pytest.raises(ValueError):
        native_plan("antfly-showcases", "invalid", current)


def test_first_boot_uses_exact_rc8_and_rejects_stale_or_unverified_images():
    request = management_plan("antfly-showcases")["create-instance.json"]
    assert (
        request["tier"] == "standard"
    )  # Starter forbids the supplied custom capacity.
    assert request["initial_runtime"] == {
        "version": "v0.3.0-rc.8",
        "image": RUNTIME["image"],
        "image_digest": RUNTIME["image"].split("@")[1],
    }
    digest = request["initial_runtime"]["image_digest"]
    mirror = (
        "us-central1-docker.pkg.dev/antfly-image-artifacts/containers/antfly@" + digest
    )
    assert (
        management_plan("antfly-showcases", mirror)["create-instance.json"][
            "initial_runtime"
        ]["image"]
        == mirror
    )
    for image in (
        "ghcr.io/antflydb/antfly@sha256:" + "a" * 64,
        "ghcr.io/antflydb/antfly:v0.3.0-rc.5@" + digest,
    ):
        with pytest.raises(ValueError, match="RC.8"):
            management_plan("antfly-showcases", image)


def test_sample_archive_isolated_from_future_full_export():
    archive, current = str(uuid4()), str(uuid4())
    files = native_plan(
        "antfly-showcases", archive, current, "archive/qualification-20261029"
    )
    assert (
        files["archive-table.json"]["schema"]["base_source"]["uri"]
        == "gs://antfly-showcases-hn-archive/archive/qualification-20261029"
    )
    for prefix in (
        "",
        "/archive/sample",
        "archive/../other",
        "archive//sample",
        "archive/sample?token=bad",
        "other/sample",
    ):
        with pytest.raises(ValueError, match="Archive prefix"):
            native_plan("antfly-showcases", archive, current, prefix)
