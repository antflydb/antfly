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


def test_public_traversal_retains_each_seed_path(aflite_path) -> None:
    with antfly_embedded.create_hosted(aflite_path) as db:
        db.add_index({"name": "knowledge", "type": "graph", "edge_types": [{"name": "about"}]})
        db.batch_json({
            "inserts": {"claim:a": {"search_text": "embedder"}},
            "graph_writes": [
                {"index_name": "knowledge", "source": "claim:a", "target": seed, "edge_type": "about"}
                for seed in ["concept:a", "concept:b"]
            ],
            "sync_level": "full_index",
        })
        response = db.search({
            "full_text_search": {"match_all": {}},
            "graph_queries": {"q": {"index": "knowledge", "traverse": {
                "start": {"keys": ["concept:a", "concept:b"]},
                "direction": "in", "edge_types": ["about"], "max_depth": 1,
                "include_paths": True, "deduplicate_nodes": False,
            }}},
        })
        nodes = response["responses"][0]["graph_results"]["q"]["nodes"]
        assert [node["key"] for node in nodes] == ["claim:a", "claim:a"]
        assert {node["path"][0]["key"] for node in nodes} == {"concept:a", "concept:b"}


@pytest.mark.parametrize("destination_storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
@pytest.mark.parametrize("restore_method", ["memory", "file", "import"])
def test_query_resolution_uses_durable_aliases_without_promotion(
    aflite_path, tmp_path, destination_storage, restore_method
) -> None:
    options = antfly_embedded.OpenOptions(profile=antfly_embedded.Profile.NATIVE, generated_enrichment_replay=True)
    with antfly_embedded.create_with_options(aflite_path, options) as db:
        config = {
            "name": "knowledge",
            "type": "graph",
            "source": {"artifact": "kg", "format": "extraction_graph", "mention_edge_type": "mentions"},
            "artifact": {
                "name": "kg",
                "kind": "asset",
                "source": {"type": "field", "value": "kg"},
                "content_type": "application/json",
            },
            "edge_types": [{"name": "mentions"}, {"name": "uses"}],
            "resolvers": [
                {
                    "name": "concepts",
                    "table": "default",
                    "promote": False,
                    "source_artifact": "kg",
                    "resolution_artifact": "resolved",
                    "key_template": "concept/{{ uslug _entity.text }}",
                    "aliases": [{"key": "concept/qwen3", "target": "concept/qwen"}],
                    "config_generation": 1,
                }
            ],
        }
        db.add_index(config)
        request = {"extraction": {"entities": [{"id": "q0", "label": "concept", "text": "QWEN3"}]}}
        result = db.resolve_query("concepts", request)
        assert result["resolution"]["entities"][0]["doc_ref"]["key"] == "concept/qwen"
        assert result["resolution"]["entities"][0]["decision"] == "match"
        with pytest.raises(antfly_embedded.NotFoundError):
            db.lookup("concept/qwen")
        with pytest.raises(antfly_embedded.InvalidArgumentError, match="exactly one"):
            db.resolve_query("concepts", {**request, "text": "Qwen3"})
        with pytest.raises(antfly_embedded.InvalidArgumentError, match="configured asset extractor"):
            db.resolve_query("concepts", {"text": "Qwen3"})
        db.batch_json(
            {
                "inserts": {
                    "claim:a": {
                        "kg": {
                            "entities": [
                                {"id": "e0", "label": "concept", "text": "Qwen3"},
                                {"id": "e1", "label": "concept", "text": "Antfly"},
                            ],
                            "relations": [{"type": "uses", "source": "e0", "target": "e1"}],
                        }
                    }
                }
            }
        )
        db.run_until_idle()
        db.merge_resolver_keys("concepts", "concept/qwen", "concept/qwen_embedding")
        db.run_until_idle()
        db.delete_index("knowledge")
        db.add_index(config)
        db.run_until_idle()
        assert (
            db.resolve_query("concepts", request)["resolution"]["entities"][0]["doc_ref"]["key"]
            == "concept/qwen_embedding"
        )
    # Exact consumer pattern: the physical index already exists on reopen.
    with antfly_embedded.open_with_options(aflite_path, options) as db:
        db.add_index(config)
        db.run_until_idle()
        assert (
            db.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})["key"] == "concept/qwen_embedding"
        )
        assert db.resolve_query("concepts", request)["resolution"]["config_generation"] == 2
        assert len(db.edges("knowledge", "concept/qwen_embedding", "uses", 0)) == 1
        assert db.edges("knowledge", "concept/qwen", "uses", 0) == []
        for field, value in (("table", "other"), ("key_template", "other/{{ uslug _entity.text }}")):
            changed = json.loads(json.dumps(config))
            changed["name"] = "conflicting"
            changed["resolvers"][0][field] = value
            with pytest.raises(antfly_embedded.InvalidArgumentError, match="resetting or migrating"):
                db.add_index(changed)
        assert (
            db.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})["key"] == "concept/qwen_embedding"
        )
        renamed = json.loads(json.dumps(config))
        renamed["name"] = "knowledge_v2"
        db.delete_index("knowledge")
        db.add_index(renamed)
        db.run_until_idle()
        assert (
            db.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})["key"] == "concept/qwen_embedding"
        )
        assert len(db.edges("knowledge_v2", "concept/qwen_embedding", "uses", 0)) == 1
        config = renamed
        backup = db.backup()
        backup_file = tmp_path / "resolver.afb"
        db.backup_to_file(backup_file)
    restored_path = tmp_path / (
        "restored.aflite" if destination_storage == antfly_embedded.Storage.LITE else "restored"
    )
    destination_options = antfly_embedded.OpenOptions(
        storage=destination_storage, profile=antfly_embedded.Profile.NATIVE, generated_enrichment_replay=True
    )
    if restore_method == "file":
        antfly_embedded.restore_file(restored_path, backup_file, storage=destination_storage)
    elif restore_method == "import":
        opener = (
            antfly_embedded.create_with_options
            if destination_storage == antfly_embedded.Storage.LITE
            else antfly_embedded.open_with_options
        )
        with opener(restored_path, destination_options) as destination:
            destination.import_backup(backup)
    else:
        antfly_embedded.restore(restored_path, backup, storage=destination_storage)
    with antfly_embedded.open_with_options(restored_path, destination_options) as restored:
        restored.run_until_idle()
        assert (
            restored.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})["key"]
            == "concept/qwen_embedding"
        )
        assert (
            restored.resolve_query("concepts", request)["resolution"]["entities"][0]["doc_ref"]["key"]
            == "concept/qwen_embedding"
        )
        assert len(restored.edges(config["name"], "concept/qwen_embedding", "uses", 0)) == 1
        assert restored.edges(config["name"], "concept/qwen", "uses", 0) == []
        restored.create_table("canonical", {})
        migrated = {
            "table": "canonical",
            "key_template": "topic/{{ uslug _entity.text }}",
            "key_map": [
                {"key": "concept/qwen3", "target": "topic/qwen3"},
                {"key": "concept/qwen", "target": "topic/qwen"},
                {"key": "concept/qwen_embedding", "target": "topic/qwen_embedding"},
            ],
        }
        incomplete = {**migrated, "key_map": migrated["key_map"][:1]}
        with pytest.raises(antfly_embedded.InvalidArgumentError, match="every inline alias endpoint"):
            restored.set_resolver_namespace("concepts", incomplete)
        assert (
            restored.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})["key"]
            == "concept/qwen_embedding"
        )
        with pytest.raises(antfly_embedded.NotFoundError, match="destination table"):
            restored.set_resolver_namespace("concepts", {**migrated, "table": "absent"})
        with pytest.raises(antfly_embedded.InvalidArgumentError, match="exclusively"):
            restored.set_resolver_namespace("concepts", {**migrated, "reset_merges": True})
        with restored.open_table("default") as source_table:
            source_table.set_resolver_namespace("concepts", migrated)
        restored.run_until_idle()
        assert (
            restored.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})["key"]
            == "topic/qwen_embedding"
        )
        assert (
            restored.resolve_query("concepts", request)["resolution"]["entities"][0]["doc_ref"]["table"] == "canonical"
        )
        assert len(restored.edges(config["name"], "topic/qwen_embedding", "uses", 0)) == 1
        assert restored.edges(config["name"], "concept/qwen_embedding", "uses", 0) == []
    with antfly_embedded.open_with_options(restored_path, destination_options) as restored:
        assert (
            restored.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})["key"]
            == "topic/qwen_embedding"
        )
        restored.set_resolver_namespace(
            "concepts",
            {
                "table": "default",
                "key_template": "fresh/{{ uslug _entity.text }}",
                "reset_merges": True,
            },
        )
        restored.run_until_idle()
        assert restored.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})["key"] == "fresh/qwen3"
        assert len(restored.edges(config["name"], "fresh/qwen3", "uses", 0)) == 1
        assert restored.edges(config["name"], "topic/qwen_embedding", "uses", 0) == []
    with antfly_embedded.open_with_options(restored_path, destination_options) as restored:
        assert (
            restored.resolve_query("concepts", request)["resolution"]["entities"][0]["doc_ref"]["key"] == "fresh/qwen3"
        )
        # A reset fences the namespace even though the durable merge list is empty.
        stale = json.loads(json.dumps(config))
        with pytest.raises(antfly_embedded.InvalidArgumentError, match="resetting or migrating"):
            restored.add_index(stale)
        current = json.loads(json.dumps(config))
        current["name"] = "fresh_namespace"
        current["resolvers"][0]["key_template"] = "fresh/{{ uslug _entity.text }}"
        current["resolvers"][0]["aliases"] = []
        before_reapply = restored.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})[
            "config_generation"
        ]
        restored.add_index(current)
        restored.run_until_idle()
        assert restored.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})["key"] == "fresh/qwen3"
        assert (
            restored.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})["config_generation"]
            == before_reapply
        )
        # Even an alias-only stale replacement is rejected.
        current["name"] = "stale_aliases"
        current["resolvers"][0]["aliases"] = [{"key": "fresh/qwen3", "target": "fresh/old"}]
        with pytest.raises(antfly_embedded.InvalidArgumentError, match="aliases.*resetting or migrating"):
            restored.add_index(current)
        restored.set_resolver_namespace(
            "concepts",
            {
                "table": "default",
                "key_template": "fresh/{{ uslug _entity.text }}",
                "reset_merges": True,
                "aliases": [{"key": "fresh/qwen3", "target": "fresh/qwen"}],
            },
        )
        restored.run_until_idle()
        assert restored.render_resolver_key("concepts", {"text": "Qwen3", "label": "concept"})["key"] == "fresh/qwen"
    with antfly_embedded.open_with_options(aflite_path, options) as db:
        db.add_index(config)
        db.run_until_idle()
        assert len(db.edges("knowledge_v2", "concept/qwen_embedding", "uses", 0)) == 1
    with antfly_embedded.open_readonly(aflite_path) as db:
        result = db.resolve_query("concepts", request)
        assert result["resolution"]["config_generation"] == 2
        assert result["resolution"]["entities"][0]["doc_ref"]["key"] == "concept/qwen_embedding"
        assert json.loads(db.resolve_query("concepts", request, raw=True)) == result


def test_resolver_revision_advances_for_unmerged_config_changes(tmp_path):
    import copy

    config = {
        "name": "knowledge",
        "type": "graph",
        "source": {"artifact": "kg", "format": "extraction_graph", "mention_edge_type": "mentions"},
        "artifact": {
            "name": "kg",
            "kind": "asset",
            "source": {"type": "field", "value": "kg"},
            "content_type": "application/json",
        },
        "edge_types": [{"name": "mentions"}],
        "resolvers": [
            {
                "name": "concepts",
                "table": "default",
                "promote": False,
                "source_artifact": "kg",
                "resolution_artifact": "resolved",
                "key_template": "concept/{{ uslug _entity.text }}",
                "config_generation": 1,
            }
        ],
    }
    options = antfly_embedded.OpenOptions(storage=antfly_embedded.Storage.LITE, profile=antfly_embedded.Profile.NATIVE)
    with antfly_embedded.create_with_options(tmp_path / "revisions.aflite", options) as db:
        db.add_index(config)
        mention = {"text": "Qwen3", "label": "concept"}
        assert db.render_resolver_key("concepts", mention)["config_generation"] == 1
        changed = copy.deepcopy(config)
        changed["name"] = "changed"
        changed["resolvers"][0]["min_confidence"] = 0.1
        db.add_index(changed)
        assert db.render_resolver_key("concepts", mention)["config_generation"] == 2
        db.add_index(changed)
        assert db.render_resolver_key("concepts", mention)["config_generation"] == 2
        db.merge_resolver_keys("concepts", "concept/qwen3", "concept/qwen")
        assert db.render_resolver_key("concepts", mention)["config_generation"] == 3
        changed["name"] = "changed_again"
        changed["resolvers"][0]["min_confidence"] = 0.2
        db.add_index(changed)
        assert db.render_resolver_key("concepts", mention)["config_generation"] == 4
        # An explicit reset must clear merges even when namespace and aliases
        # exactly match the current declarative config.
        db.set_resolver_namespace(
            "concepts", {"table": "default", "key_template": "concept/{{ uslug _entity.text }}", "reset_merges": True}
        )
        assert db.render_resolver_key("concepts", mention)["key"] == "concept/qwen3"
        db.merge_resolver_keys("concepts", "concept/qwen3", "concept/qwen")
        db.set_resolver_namespace(
            "concepts", {"table": "default", "key_template": "topic/{{ uslug _entity.text }}", "reset_merges": True}
        )
        backup = db.backup()
    restored_path = tmp_path / "reset-restored.aflite"
    antfly_embedded.restore(restored_path, backup, storage=antfly_embedded.Storage.LITE)
    with antfly_embedded.open_with_options(restored_path, options) as restored:
        with pytest.raises(antfly_embedded.InvalidArgumentError, match="resetting or migrating"):
            restored.add_index(changed)
        assert restored.render_resolver_key("concepts", mention)["key"] == "topic/qwen3"


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_inline_resolver_ownership_and_table_scoped_identity(tmp_path, storage) -> None:
    path = tmp_path / ("scoped.aflite" if storage == antfly_embedded.Storage.LITE else "scoped")
    options = antfly_embedded.OpenOptions(
        storage=storage, profile=antfly_embedded.Profile.NATIVE, generated_enrichment_replay=True
    )
    mention = {"id": "a", "text": "Ada", "label": "person"}
    render_mention = {"text": "Ada", "label": "person"}
    extraction = {
        "entities": [mention, {"id": "b", "text": "Bob", "label": "person"}],
        "relations": [{"type": "knows", "source": "a", "target": "b"}],
    }
    configs = {}
    for suffix in ("alpha", "beta"):
        configs[suffix] = {
            "name": "knowledge",
            "type": "graph",
            "source": {"artifact": "kg", "format": "extraction_graph", "mention_edge_type": "mentions"},
            "artifact": {
                "name": "kg",
                "kind": "asset",
                "source": {"type": "field", "value": "kg"},
                "content_type": "application/json",
            },
            "edge_types": [{"name": "mentions"}, {"name": "knows"}],
            "resolvers": [
                {
                    "name": "people",
                    "table": f"entities_{suffix}",
                    "promote": False,
                    "source_artifact": "kg",
                    "resolution_artifact": "resolved",
                    "key_template": suffix + "/{{ uslug _entity.text }}",
                    "config_generation": 1,
                }
            ],
        }
    opener = (
        antfly_embedded.create_with_options
        if storage == antfly_embedded.Storage.LITE
        else antfly_embedded.open_with_options
    )
    with opener(path, options) as db:
        for suffix in configs:
            db.create_table(f"entities_{suffix}", {})
            db.create_table(f"source_{suffix}", {})
        for suffix, config in configs.items():
            with db.open_table(f"source_{suffix}") as table:
                table.add_index(config)
                table.batch_json({"inserts": {"claim": {"kg": extraction}}})
                table.run_until_idle()
        with db.open_table("source_alpha") as alpha, db.open_table("source_beta") as beta:
            alpha.merge_resolver_keys("people", "alpha/ada", "alpha/canonical")
            alpha.run_until_idle()
            assert alpha.render_resolver_key("people", render_mention)["key"] == "alpha/canonical"
            assert beta.render_resolver_key("people", render_mention)["key"] == "beta/ada"
            assert len(alpha.edges("knowledge", "alpha/canonical", "knows", 0)) == 1
            assert len(beta.edges("knowledge", "beta/ada", "knows", 0)) == 1
            # One resolver may have multiple declaring indexes. Only the last
            # owner retires it; table-local curation never affects its sibling.
            shared = json.loads(json.dumps(configs["alpha"]))
            shared["name"] = "shared"
            alpha.add_index(shared)
            alpha.run_until_idle()
            assert alpha.delete_index("knowledge")
            assert alpha.render_resolver_key("people", render_mention)["key"] == "alpha/canonical"
            assert alpha.delete_index("shared")
            with pytest.raises(antfly_embedded.NotFoundError):
                alpha.render_resolver_key("people", render_mention)
            with pytest.raises(antfly_embedded.NotFoundError):
                alpha.resolve_query("people", {"extraction": extraction})
            assert beta.render_resolver_key("people", render_mention)["key"] == "beta/ada"
        # A backup made while retired must retain identity without reactivating it.
        backup = db.backup()
    restored_path = tmp_path / ("archived.aflite" if storage == antfly_embedded.Storage.LITE else "archived")
    antfly_embedded.restore(restored_path, backup, storage=storage)
    with antfly_embedded.open_with_options(restored_path, options) as restored:
        with restored.open_table("source_alpha") as alpha:
            with pytest.raises(antfly_embedded.NotFoundError):
                alpha.render_resolver_key("people", render_mention)
            # Failed reactivation of a retained namespace cannot lose its merges.
            invalid = json.loads(json.dumps(configs["alpha"]))
            invalid["resolvers"][0]["key_template"] = "wrong/{{ uslug _entity.text }}"
            with pytest.raises(antfly_embedded.InvalidArgumentError, match="resetting or migrating"):
                alpha.add_index(invalid)
            alpha.run_until_idle()
            # Partial registration must restore a previously retired identity.
            invalid = json.loads(json.dumps(configs["alpha"]))
            invalid["resolvers"].append({**invalid["resolvers"][0], "name": "conflicting"})
            with pytest.raises(antfly_embedded.InvalidArgumentError):
                alpha.add_index(invalid)
            alpha.run_until_idle()
            with pytest.raises(antfly_embedded.NotFoundError):
                alpha.render_resolver_key("people", render_mention)
            renamed = json.loads(json.dumps(configs["alpha"]))
            renamed["name"] = "knowledge_v2"
            alpha.add_index(renamed)
            alpha.run_until_idle()
            assert alpha.render_resolver_key("people", render_mention)["key"] == "alpha/canonical"
            assert len(alpha.edges("knowledge_v2", "alpha/canonical", "knows", 0)) == 1
    # Exact consumer pattern on two tables: reopen, reapply identical inline
    # configs on existing indexes, then render/resolve and check relation edges.
    with antfly_embedded.open_with_options(path, options) as db:
        for suffix, config in configs.items():
            with db.open_table(f"source_{suffix}") as table:
                table.add_index(config)
                table.run_until_idle()
    with antfly_embedded.open_with_options(path, options) as db:
        for suffix, config in configs.items():
            with db.open_table(f"source_{suffix}") as table:
                table.add_index(config)
                table.run_until_idle()
                expected = "alpha/canonical" if suffix == "alpha" else "beta/ada"
                rendered = table.render_resolver_key("people", render_mention)
                assert rendered["key"] == expected
                assert rendered["table"] == f"entities_{suffix}"
                result = table.resolve_query("people", {"extraction": extraction})
                endpoint = result["resolution"]["entities"][0]["doc_ref"]
                assert endpoint["key"] == expected
                assert endpoint["table"] == f"entities_{suffix}"
                assert len(table.edges("knowledge", expected, "knows", 0)) == 1
