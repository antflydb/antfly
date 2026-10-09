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

"""Lite graph edge mutations and per-index readiness through the C ABI."""

from __future__ import annotations

import base64
import json
import os
import random
import subprocess
import sys
from pathlib import Path

import pytest

import antfly_embedded
from antfly_embedded._library import find_library

pytestmark = pytest.mark.usefixtures("require_native")


def _index(stats: dict, name: str) -> dict:
    return next(item for item in stats["indexes"] if item["name"] == name)


def _targets(db: antfly_embedded.Database) -> list[str]:
    return sorted(
        base64.b64decode(edge["target_b64"]).decode()
        for edge in db.edges("graph", "node:a", "KNOWS", antfly_embedded.GraphDirection.OUT)
    )


def test_single_graph_edge_mutations_preserve_other_edges(aflite_path) -> None:
    with antfly_embedded.create(aflite_path) as db:
        db.add_index({"name": "graph", "kind": "graph", "config_json": "{}"})
        db.batch_json(
            {
                "inserts": {
                    "node:a": {"name": "a", "_edges": {"graph": {"KNOWS": [{"target": "node:b"}]}}},
                    "node:b": {},
                    "node:c": {},
                },
                "sync_level": "full_index",
            }
        )
        assert _targets(db) == ["node:b"]
        db.batch_json(
            {
                "graph_writes": [
                    {
                        "index_name": "graph",
                        "source": "node:a",
                        "target": "node:c",
                        "edge_type": "KNOWS",
                        "metadata_json": '{"uuid":"edge"}',
                    }
                ],
                "sync_level": "full_index",
            }
        )
        assert _targets(db) == ["node:b", "node:c"]

        db.batch_json(
            {
                "graph_deletes": [
                    {
                        "index_name": "graph",
                        "source": "node:a",
                        "target": "node:b",
                        "edge_type": "KNOWS",
                    }
                ],
                "sync_level": "full_index",
            }
        )
        assert _targets(db) == ["node:c"]


def test_index_readiness_visible_on_open_and_status_only_handles(aflite_path) -> None:
    with antfly_embedded.create(aflite_path) as db:
        db.add_index({"name": "graph", "kind": "graph", "config_json": "{}"})
        db.add_index(
            {
                "name": "vec",
                "kind": "dense_vector",
                "config_json": json.dumps(
                    {
                        "field": "embedding",
                        "dims": 4,
                        "metric": "cosine",
                        "external": True,
                    }
                ),
            }
        )
        # Replay targets are per index; create an edge so both configured
        # indexes receive a mutation rather than expecting idle graph replay.
        db.batch_json(
            {
                "inserts": {
                    "node:a": {
                        "name": "a",
                        "embedding": [1, 0, 0, 0],
                        "_edges": {"graph": {"KNOWS": [{"target": "node:b"}]}},
                    }
                }
            }
        )
        for stats in (db.stats(), db.status()["stats"]):
            assert stats["indexes_available"] is True
            for name in ("graph", "vec"):
                index = _index(stats, name)
                assert index["replay_target_sequence"] >= 1
                assert isinstance(index["replay_applied_sequence"], int)
                assert isinstance(index["replay_catch_up_required"], bool)
                assert isinstance(index["catch_up_active"], bool)
                assert isinstance(index["catch_up_phase"], str)
        db.run_until_idle()

    with antfly_embedded.open_status_only(aflite_path) as db:
        for name in ("graph", "vec"):
            index = _index(db.status()["stats"], name)
            assert index["replay_target_sequence"] >= 1
            assert index["replay_applied_sequence"] >= index["replay_target_sequence"]


@pytest.mark.parametrize("wait_mode", ["run_until_idle", "full_index"])
def test_external_dense_index_wait_finishes(aflite_path, wait_mode: str) -> None:
    # The DocStore unit test proves the lock order. Exercise the real Lite
    # workload here with a child-process deadline for each wait mode.
    library = find_library()
    assert library is not None
    env = os.environ.copy()
    env["ANTFLY_LIBRARY"] = str(library)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, (str(Path(__file__).parents[1] / "src"), env.get("PYTHONPATH"))))
    result = subprocess.run(
        [sys.executable, __file__, str(aflite_path), wait_mode],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env=env,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _run_wait_workload(path: str, wait_mode: str) -> None:
    batches = json.loads((Path(__file__).parent / "data" / "index_wait_batches.json").read_text())

    def expand(value: object) -> object:
        if isinstance(value, str) and value.startswith("VEC:"):
            rng = random.Random(value)
            return [rng.uniform(0, 0.9) for _ in range(384)]
        return value

    with antfly_embedded.create(path) as db:
        db.add_index({"name": "graph", "kind": "graph", "config_json": "{}"})
        for field in ("name_embedding", "fact_embedding"):
            db.add_index(
                {
                    "name": field,
                    "kind": "dense_vector",
                    "config_json": json.dumps(
                        {
                            "field": field,
                            "dims": 384,
                            "metric": "cosine",
                            "external": True,
                        }
                    ),
                }
            )
        for batch in batches:
            inserts = {key: {field: expand(value) for field, value in doc.items()} for key, doc in batch.items()}
            request: dict[str, object] = {"inserts": inserts}
            if wait_mode == "full_index":
                request["sync_level"] = "full_index"
            db.batch_json(request)
            if wait_mode == "run_until_idle":
                db.run_until_idle()


if __name__ == "__main__":
    _run_wait_workload(sys.argv[1], sys.argv[2])
