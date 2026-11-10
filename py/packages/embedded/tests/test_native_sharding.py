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

"""Native CI partitions preserve engine pairs and reject empty/mistyped lanes."""

from types import SimpleNamespace

import pytest
from conftest import native_test_partition
from conftest import pytest_collection_modifyitems as partition_items


def test_native_partition_keeps_parameters_and_models_together():
    lanes = set()
    for i in range(64):
        family = f"tests/test_cypher_performance.py::test_family_{i}"
        lane = native_test_partition(family)
        assert {native_test_partition(family + suffix) for suffix in ("[0]", "[1]", "[0-wide]", "[1-wide]")} == {lane}
        lanes.add(lane)
    assert lanes == {"cypher-0", "cypher-1"}
    assert {
        native_test_partition(f"tests/test_inference.py::test_model[{model}]") for model in ("whisper", "gemma", "qwen")
    } == {"core"}


def test_native_collection_partitions_are_exhaustive_and_disjoint(monkeypatch):
    original = [
        SimpleNamespace(nodeid=f"tests/{module}.py::test_family_{i}[{engine}]")
        for module in ("test_cypher_performance", "test_cypher", "test_native_sharding")
        for i in range(32)
        for engine in (0, 1)
    ]
    assigned = []
    for shard in ("core", "cypher-0", "cypher-1"):
        monkeypatch.setenv("ANTFLY_SDK_PYTHON_SHARD", shard)
        omitted = []
        config = SimpleNamespace(hook=SimpleNamespace(pytest_deselected=lambda items: omitted.extend(items)))
        selected = original.copy()
        partition_items(config, selected)
        assert selected and omitted
        assert {id(x) for x in selected}.isdisjoint(id(x) for x in omitted)
        assert len(selected) + len(omitted) == len(original)
        assigned.extend(id(x) for x in selected)
    assert sorted(assigned) == sorted(id(x) for x in original)


def test_native_collection_rejects_invalid_or_empty_partition(monkeypatch):
    monkeypatch.setenv("ANTFLY_SDK_PYTHON_SHARD", "cyper-0")
    with pytest.raises(pytest.UsageError, match="Unknown native Python shard"):
        partition_items(None, [])
    monkeypatch.setenv("ANTFLY_SDK_PYTHON_SHARD", "core")
    with pytest.raises(pytest.UsageError, match="selected no tests"):
        partition_items(None, [])
