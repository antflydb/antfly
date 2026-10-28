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
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from subprocess_watchdog import run_with_progress_deadline

import antfly_embedded
from antfly_embedded._library import find_library

pytestmark = pytest.mark.usefixtures("require_native")

# Persisted resolver consumer bindings/work require this feature epoch. Keep
# this independent of the latest supported reader: plain databases remain at 1.
LITE_RESOLVER_CONSUMER_READER_VERSION = 29


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
                        # External vector indexes consume the explicit index
                        # payload; a document field alone is not replay work.
                        "_embeddings": {"vec": [1, 0, 0, 0]},
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
    # workload here with a deadline for each native call, not for all 16
    # batches combined. A slow runner must still detect an actual stalled call.
    library = find_library()
    assert library is not None
    env = os.environ.copy()
    env["ANTFLY_LIBRARY"] = str(library)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, (str(Path(__file__).parents[1] / "src"), env.get("PYTHONPATH"))))
    try:
        result = run_with_progress_deadline(
            [sys.executable, __file__, str(aflite_path), wait_mode],
            env=env,
            progress_prefix=f"{wait_mode}:",
            timeout=30,
        )
    except subprocess.TimeoutExpired as exc:
        # The watchdog kills and joins the child before raising. Preserve its
        # native diagnostics; pytest otherwise reports only the command, hiding
        # whether this was a replay stall, startup panic, or teardown failure.
        def output(value: str | bytes | None) -> str:
            return value.decode(errors="replace") if isinstance(value, bytes) else value or ""

        pytest.fail(
            f"{wait_mode} made no progress for 30 seconds\nstdout:\n{output(exc.stdout)}\nstderr:\n{output(exc.stderr)}",
            pytrace=False,
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
        for ordinal, batch in enumerate(batches):
            print(f"{wait_mode}: batch {ordinal} write", flush=True)
            inserts = {key: {field: expand(value) for field, value in doc.items()} for key, doc in batch.items()}
            request: dict[str, object] = {"inserts": inserts}
            if wait_mode == "full_index":
                request["sync_level"] = "full_index"
            db.batch_json(request)
            if wait_mode == "run_until_idle":
                print(f"{wait_mode}: batch {ordinal} drain", flush=True)
                db.run_until_idle()
            print(f"{wait_mode}: batch {ordinal} complete", flush=True)
        print(f"{wait_mode}: closing", flush=True)
    print(f"{wait_mode}: closed", flush=True)


if __name__ == "__main__":
    _run_wait_workload(sys.argv[1], sys.argv[2])


def test_public_traversal_retains_each_seed_path(aflite_path) -> None:
    with antfly_embedded.create_hosted(aflite_path) as db:
        db.add_index({"name": "knowledge", "type": "graph", "edge_types": [{"name": "about"}]})
        db.batch_json(
            {
                "inserts": {"claim:a": {"search_text": "embedder"}},
                "graph_writes": [
                    {"index_name": "knowledge", "source": "claim:a", "target": seed, "edge_type": "about"}
                    for seed in ["concept:a", "concept:b"]
                ],
                "sync_level": "full_index",
            }
        )
        response = db.search(
            {
                "full_text_search": {"match_all": {}},
                "graph_queries": {
                    "q": {
                        "index": "knowledge",
                        "traverse": {
                            "start": {"keys": ["concept:a", "concept:b"]},
                            "direction": "in",
                            "edge_types": ["about"],
                            "max_depth": 1,
                            "include_paths": True,
                            "deduplicate_nodes": False,
                        },
                    }
                },
            }
        )
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
        assert db.resolve_query("concepts", request)["resolution"]["config_generation"] == 3
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
        # Reopen starts durable replay recovery. This case tests namespace
        # migration and alias persistence, not catalog admission racing a
        # worker (which may correctly return Busy). Settle that owned work
        # before resetting the namespace; keep runtime fencing fail-fast.
        restored.run_until_idle()
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
        assert result["resolution"]["config_generation"] == 4
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
            # Updates while retired must be resolved on reactivation, even if
            # an older resolution artifact has the same declarative generation.
            updated = json.loads(json.dumps(extraction))
            updated["entities"][1]["text"] = "Carol"
            alpha.batch_json({"inserts": {"claim": {"kg": updated}}})
            alpha.run_until_idle()
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
            # Conflicting declarations must fail before reactivating a retired identity.
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
            restored_edges = alpha.edges("knowledge_v2", "alpha/canonical", "knows", 0)
            assert len(restored_edges) == 1
            assert base64.b64decode(restored_edges[0]["target_b64"]).decode() == "alpha/carol"
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
                edges = table.edges("knowledge", expected, "knows", 0)
                assert len(edges) == 1
                expected_target = "alpha/carol" if suffix == "alpha" else "beta/bob"
                assert base64.b64decode(edges[0]["target_b64"]).decode() == expected_target


def _open_graph_test(path, options):
    opener = (
        antfly_embedded.create_with_options
        if options.storage == antfly_embedded.Storage.LITE and not path.exists()
        else antfly_embedded.open_with_options
    )
    return opener(path, options)


def _resolver_graph(name, resolver, *, labels=None, template="entity/{{ uslug _entity.text }}", destination="default"):
    return {
        "name": name,
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
                "name": resolver,
                "table": destination,
                "source_artifact": "kg",
                "resolution_artifact": "resolved_" + resolver,
                "key_template": template,
                "labels": labels or [],
                "promote": False,
            }
        ],
    }


def _stored_resolution(db, name):
    key = b"\x01claim\x00\x00\x20resolution\x00\x00" + name.encode() + b"\x00\x00"
    return json.loads(db.get_raw(key.decode()))


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
@pytest.mark.parametrize("owner_kind,catchall_kind", [("asset", "asset"), ("any", "asset"), ("asset", "any")])
def test_retirement_replays_catchall_routing(tmp_path, storage, owner_kind, catchall_kind):
    options = antfly_embedded.OpenOptions(storage=storage, generated_enrichment_replay=True)
    with _open_graph_test(
        tmp_path / ("routing.aflite" if storage == antfly_embedded.Storage.LITE else "routing"), options
    ) as db:
        # Catch-all initially skips the person mention claimed by its sibling.
        owner = _resolver_graph("people_index", "people", labels=["person"])
        owner["resolvers"][0]["source_artifact_kind"] = owner_kind
        catchall = _resolver_graph("catchall_index", "catchall")
        catchall["resolvers"][0]["source_artifact_kind"] = catchall_kind
        db.add_index(owner)
        db.add_index(catchall)
        extraction = {"entities": [{"id": "a", "text": "Ada", "label": "person"}]}
        db.batch_json({"inserts": {"claim": {"kg": extraction}}})
        db.run_until_idle()
        assert _stored_resolution(db, "resolved_catchall")["entities"] == []
        assert db.delete_index("people_index")
        db.run_until_idle()
        stored = _stored_resolution(db, "resolved_catchall")
        query = db.resolve_query("catchall", {"extraction": extraction})
        assert [e["doc_ref"]["key"] for e in stored["entities"]] == ["entity/ada"]
        for entity in query["resolution"]["entities"]:
            entity["doc_ref"].pop("storage_table", None)
        for entity in stored["entities"]:
            entity["doc_ref"].pop("storage_table", None)
        assert stored["entities"] == query["resolution"]["entities"]
        assert "_resolution_work" not in stored
        # Reactivation restores the label owner and retires the catch-all's
        # previous entity, without changing the catch-all's inline config.
        owner["name"] = "people_index_v2"
        db.add_index(owner)
        db.run_until_idle()
        assert _stored_resolution(db, "resolved_catchall")["entities"] == []


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
@pytest.mark.parametrize("order", ["add_first", "delete_first", "atomic"])
def test_index_upgrade_and_retired_identity_management(tmp_path, storage, order):
    options = antfly_embedded.OpenOptions(storage=storage, generated_enrichment_replay=True)
    config = _resolver_graph("knowledge_v1", "concepts")
    mention = {"text": "Ada", "label": "person"}
    path = tmp_path / ("upgrade.aflite" if storage == antfly_embedded.Storage.LITE else "upgrade")
    with _open_graph_test(path, options) as db:
        db.add_index(config)
        db.merge_resolver_keys("concepts", "entity/ada", "entity/canonical")
        db.run_until_idle()
        reader_version = db.capabilities()["minimum_reader_version"]
        # Durable curation requires epoch 6. Newer persisted features may
        # raise that fence; the observed fence must never weaken on upgrade.
        assert reader_version >= 6
        assert reader_version == db.status()["capabilities"]["minimum_reader_version"]
        with pytest.raises(antfly_embedded.InvalidArgumentError, match="retire"):
            db.drop_resolver_identity("concepts")
        upgraded = json.loads(json.dumps(config))
        upgraded["name"] = "knowledge_v2"
        if order == "delete_first":
            assert db.delete_index("knowledge_v1")
            identities = db.list_resolver_identities()
            assert identities[0]["config"]["retired"] is True
            assert identities[0]["config"].get("merge_state_version", 0) <= 1
            assert db.capabilities()["minimum_reader_version"] == reader_version
            db.set_resolver_namespace(
                "concepts",
                {
                    "table": "default",
                    "key_template": "v2/{{ uslug _entity.text }}",
                    "key_map": [
                        {"key": "entity/ada", "target": "v2/ada"},
                        {"key": "entity/canonical", "target": "v2/canonical"},
                    ],
                },
            )
            assert db.list_resolver_identities()[0]["config"]["retired"] is True
            upgraded["resolvers"][0]["key_template"] = "v2/{{ uslug _entity.text }}"
            db.add_index(upgraded)
            expected = "v2/canonical"
        else:
            if order == "atomic":
                upgraded["replaces"] = "knowledge_v1"
                rejected = dict(upgraded, name="broken", edge_types=False)
                with pytest.raises(antfly_embedded.AntflyError):
                    db.add_index(rejected)
                assert db.render_resolver_key("concepts", mention)["key"] == "entity/canonical"
            db.add_index(upgraded)
            if order == "add_first":
                assert db.delete_index("knowledge_v1")
            expected = "entity/canonical"
        if order == "atomic":
            # Repeating the command after the old index is gone is idempotent.
            db.add_index(upgraded)
        db.run_until_idle()
        assert db.render_resolver_key("concepts", mention)["key"] == expected
        names = {i["name"] for i in db.list_indexes()}
        assert "knowledge_v2" in names
        assert "knowledge_v1" not in names
        upgraded.pop("replaces", None)
    with _open_graph_test(path, options) as db:
        db.add_index(upgraded)
        assert db.render_resolver_key("concepts", mention)["key"] == expected
        assert db.delete_index("knowledge_v2")
        assert db.drop_resolver_identity("concepts")
        assert db.list_resolver_identities() == []
        assert not db.drop_resolver_identity("concepts")


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
@pytest.mark.parametrize("shared", [False, True])
def test_same_name_and_template_across_source_tables(tmp_path, storage, shared):
    options = antfly_embedded.OpenOptions(storage=storage, generated_enrichment_replay=True)
    mention = {"text": "Ada", "label": "person"}
    with _open_graph_test(
        tmp_path / ("same_keys.aflite" if storage == antfly_embedded.Storage.LITE else "same_keys"), options
    ) as db:
        for destination in ["entities"] if shared else ["entities_a", "entities_b"]:
            db.create_table(destination, {})
        for source in ("a", "b"):
            db.create_table(source, {})
            with db.open_table(source) as table:
                cfg = _resolver_graph("knowledge", "people", destination="entities" if shared else "entities_" + source)
                cfg["resolvers"][0]["promote"] = True
                table.add_index(cfg)
                table.batch_json({"inserts": {"claim": {"kg": {"entities": [dict(mention, id="a")]}}}})
                table.run_until_idle()
        with db.open_table("a") as a, db.open_table("b") as b:
            assert (
                a.render_resolver_key("people", mention)["key"]
                == b.render_resolver_key("people", mention)["key"]
                == "entity/ada"
            )
            a.merge_resolver_keys("people", "entity/ada", "entity/canonical")
            a.run_until_idle()
            assert a.render_resolver_key("people", mention)["key"] == "entity/canonical"
            assert b.render_resolver_key("people", mention)["key"] == "entity/ada"
            assert b.list_resolver_identities()[0]["merges"] == []
        with db.open_table("entities" if shared else "entities_b") as entities:
            assert entities.lookup("entity/ada")["canonical_name"] == "Ada"


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_fresh_replacement_and_rejected_target_preserve_identity(tmp_path, storage):
    expected_reader = LITE_RESOLVER_CONSUMER_READER_VERSION if storage == antfly_embedded.Storage.LITE else 3
    options = antfly_embedded.OpenOptions(storage=storage, generated_enrichment_replay=True)
    path = tmp_path / ("replace.aflite" if storage == antfly_embedded.Storage.LITE else "replace")
    with _open_graph_test(path, options) as db:
        assert db.capabilities()["minimum_reader_version"] == 1
        config = _resolver_graph("knowledge_v2", "concepts")
        config["resolvers"][0]["min_confidence"] = 0.2
        config["resolvers"][0]["promote"] = True
        config["replaces"] = "knowledge_v1"
        db.add_index(config)
        # Lite persists resolver pin indexes; directory storage retains
        # the identity catalog's reader requirement without that file format.
        assert db.capabilities()["minimum_reader_version"] == expected_reader
        before = db.list_resolver_identities()
        # Existing-target replacement must reject non-graph old indexes before
        # registering changed inline resolver config, for either storage mode.
        db.add_index({"name": "text_target", "type": "full_text"})
        bad = json.loads(json.dumps(config))
        bad["replaces"] = "text_target"
        bad["resolvers"][0]["min_confidence"] = 0
        with pytest.raises(antfly_embedded.InvalidArgumentError):
            db.add_index(bad)
        assert db.list_resolver_identities() == before
        db.add_index(config)
    with _open_graph_test(path, options) as db:
        assert db.capabilities()["minimum_reader_version"] == expected_reader
        assert db.list_resolver_identities() == before


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_plain_database_reader_version_stays_one(tmp_path, storage):
    options = antfly_embedded.OpenOptions(storage=storage)
    path = tmp_path / ("plain.aflite" if storage == antfly_embedded.Storage.LITE else "plain")
    with _open_graph_test(path, options) as db:
        assert db.capabilities()["minimum_reader_version"] == 1
    with _open_graph_test(path, options) as db:
        assert db.status()["capabilities"]["minimum_reader_version"] == 1


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_concurrent_shared_destination_promotion_preserves_aliases(tmp_path, storage):
    options = antfly_embedded.OpenOptions(storage=storage, generated_enrichment_replay=True)
    path = tmp_path / ("concurrent.aflite" if storage == antfly_embedded.Storage.LITE else "concurrent")
    with _open_graph_test(path, options) as db:
        db.create_table("entities", {})
        for name in ["source_a", "source_b"]:
            db.create_table(name, {})
            with db.open_table(name) as source:
                config = _resolver_graph("knowledge", "people", destination="entities")
                config["resolvers"][0]["promote"] = True
                source.add_index(config)
        barrier = threading.Barrier(2)

        def promote(name, text):
            # Directory storage has one writer owner. Both source tables share
            # that owner and its destination transaction coordinator.
            with db.open_table(name) as source:
                barrier.wait(timeout=30)
                source.batch_json(
                    {"inserts": {"claim": {"kg": {"entities": [{"id": "a", "text": text, "label": "person"}]}}}}
                )
                for attempt in range(20):
                    try:
                        db.run_until_idle()
                        return
                    except antfly_embedded.BusyError:
                        if attempt == 19:
                            raise
                        time.sleep(0.01)

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(promote, name, text) for name, text in [("source_a", "Ada"), ("source_b", "ADA")]
            ]
            for future in futures:
                future.result(timeout=60)
        db.run_until_idle()
        with db.open_table("entities") as entities:
            row = json.loads(entities.get_raw("entity/ada"))
            assert row["canonical_name"] in ["Ada", "ADA"]
            assert set(row["aliases"]) == {"Ada", "ADA"}


def test_previous_reader_opens_plain_database_and_rejects_new_features(tmp_path):
    previous = os.environ.get("ANTFLY_LITE_PREVIOUS_LIBRARY")
    if not previous:
        if os.environ.get("ANTFLY_LITE_REQUIRE_PREVIOUS_READER") == "1":
            pytest.fail("ANTFLY_LITE_PREVIOUS_LIBRARY is required")
        pytest.skip("set ANTFLY_LITE_PREVIOUS_LIBRARY to the pinned pre-PR reader")
    options = antfly_embedded.OpenOptions(storage=antfly_embedded.Storage.LITE)
    plain = tmp_path / "previous-plain.aflite"
    identity = tmp_path / "previous-identity.aflite"
    pending = tmp_path / "previous-pending.aflite"
    with _open_graph_test(plain, options) as db:
        db.create_table("plain_named", {})
        db.create_table("dropped_named", {})
        db.drop_table("dropped_named")
        graph = {"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]}
        db.add_index(graph)
        db.add_index(dict(graph, name="new", replaces="old"))
        db.run_until_idle()
        assert db.capabilities()["minimum_reader_version"] == 1
    with _open_graph_test(identity, options) as db:
        config = _resolver_graph("knowledge", "concepts")
        config["resolvers"][0]["promote"] = True
        db.add_index(config)
        # Durable Lite resolver projection work requires reader epoch 15.
        assert db.capabilities()["minimum_reader_version"] == 15
    # Hosted maintenance leaves the shadow pending deterministically. The old
    # writer must fail open before it can truncate the retained journal.
    with antfly_embedded.create_hosted(pending) as db:
        graph = {"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]}
        db.add_index(graph)
        db.batch_json({"inserts": {"source": {"links": ["before"]}}})
        db.run_until_idle()
        db.add_index(dict(graph, name="new", replaces="old"))
        assert db.pending_work_stats()["replacements"]
        assert db.capabilities()["minimum_reader_version"] == 2
    script = r"""
import ctypes, sys
lib = ctypes.CDLL(sys.argv[1])
lib.antfly_lite_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
lib.antfly_lite_open.restype = ctypes.c_int
lib.antfly_db_close.argtypes = [ctypes.c_void_p]
for path, accepted in [(sys.argv[2], True), (sys.argv[3], False), (sys.argv[4], False)]:
    handle = ctypes.c_void_p()
    code = lib.antfly_lite_open(path.encode(), ctypes.byref(handle))
    assert (code == 0) == accepted, (path, code)
    if handle.value:
        lib.antfly_db_close(handle)
"""
    subprocess.run(
        [sys.executable, "-c", script, previous, str(plain), str(identity), str(pending)], check=True, timeout=60
    )

    # A previously opened Lite writer must also reject mutation after the
    # registration commit, when its connection refreshes the catalog.
    live = tmp_path / "previous-live-writer.aflite"
    graph = {"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]}
    # Seed with the historical writer too: current full-text artifacts use a
    # newer section format. This case tests fencing a writer already open when
    # a new catalog feature is published, not opening newer index bytes.
    writer_script = r"""
import ctypes, json, sys
class Slice(ctypes.Structure):
    _fields_ = [("ptr", ctypes.c_void_p), ("len", ctypes.c_size_t)]
class Buffer(ctypes.Structure):
    _fields_ = [("ptr", ctypes.c_void_p), ("len", ctypes.c_size_t)]
lib = ctypes.CDLL(sys.argv[1])
lib.antfly_lite_create_hosted.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
lib.antfly_lite_create_hosted.restype = ctypes.c_int
lib.antfly_db_batch_json.argtypes = [ctypes.c_void_p, Slice, ctypes.POINTER(Buffer)]
lib.antfly_buffer_free.argtypes = [ctypes.POINTER(Buffer)]
lib.antfly_db_batch_json.restype = ctypes.c_int
lib.antfly_db_add_index_json.argtypes = [ctypes.c_void_p, Slice]
lib.antfly_db_add_index_json.restype = ctypes.c_int
lib.antfly_db_run_until_idle.argtypes = [ctypes.c_void_p]
lib.antfly_db_run_until_idle.restype = ctypes.c_int
lib.antfly_db_close.argtypes = [ctypes.c_void_p]
handle = ctypes.c_void_p()
assert lib.antfly_lite_create_hosted(sys.argv[2].encode(), ctypes.byref(handle)) == 0
def invoke(name, payload):
    data = json.dumps(payload).encode()
    buffer = ctypes.create_string_buffer(data)
    args = [handle, Slice(ctypes.cast(buffer, ctypes.c_void_p), len(data))]
    output = Buffer()
    if name == "antfly_db_batch_json": args.append(ctypes.byref(output))
    try:
        code = getattr(lib, name)(*args)
    finally:
        if output.ptr: lib.antfly_buffer_free(ctypes.byref(output))
    assert code == 0, (name, code)
invoke("antfly_db_add_index_json", {"name":"old", "type":"graph", "edge_types":[{"name":"R", "field":"links"}]})
invoke("antfly_db_batch_json", {"inserts":{"source":{"links":["before"]}}})
assert lib.antfly_db_run_until_idle(handle) == 0
print("ready", flush=True)
assert sys.stdin.readline().strip() == "write"
request = b'{"inserts":{"source":{"links":["obsolete"]}}}'
buffer = ctypes.create_string_buffer(request)
output = Buffer()
try:
    code = lib.antfly_db_batch_json(handle, Slice(ctypes.cast(buffer, ctypes.c_void_p), len(request)), ctypes.byref(output))
finally:
    if output.ptr: lib.antfly_buffer_free(ctypes.byref(output))
assert code != 0, "old writer admitted mutation across the replacement fence"
lib.antfly_db_close(handle)
"""
    writer = subprocess.Popen(
        [sys.executable, "-c", writer_script, previous, str(live)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        import select

        assert writer.stdout is not None
        ready, _, _ = select.select([writer.stdout], [], [], 30)
        line = writer.stdout.readline().strip() if ready else ""
        if line != "ready":
            if writer.poll() is None:
                writer.kill()
            _, stderr = writer.communicate(timeout=30)
            pytest.fail(f"previous reader failed to initialize: exit={writer.returncode}, stdout={line!r}\n{stderr}")
        with antfly_embedded.open_hosted(live) as db:
            db.add_index(dict(graph, name="new", replaces="old"))
            _, stderr = writer.communicate("write\n", timeout=30)
            assert writer.returncode == 0, stderr
            db.batch_json({"inserts": {"source": {"links": ["latest"]}}})
            db.run_until_idle()
            edges = db.edges("new", "source", "R", antfly_embedded.GraphDirection.OUT)
            assert [base64.b64decode(edge["target_b64"]).decode() for edge in edges] == ["latest"]
    finally:
        if writer.poll() is None:
            writer.kill()
        writer.wait(timeout=30)


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_atomic_replacement_rebuilds_changed_graph_field_mapping(tmp_path, storage):
    options = antfly_embedded.OpenOptions(storage=storage)
    path = tmp_path / ("fields.aflite" if storage == antfly_embedded.Storage.LITE else "fields")
    with _open_graph_test(path, options) as db:
        db.add_index({"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]})
        db.batch_json({"inserts": {"a": {"links": ["b"], "new_links": ["c"]}}})
        db.run_until_idle()
        assert [base64.b64decode(e["target_b64"]).decode() for e in db.edges("old", "a", "R", 0)] == ["b"]
        db.add_index(
            {
                "name": "new",
                "type": "graph",
                "replaces": "old",
                "wait": {"timeout_ms": 10000},
                "edge_types": [{"name": "R", "field": "new_links"}],
            }
        )
        assert [base64.b64decode(e["target_b64"]).decode() for e in db.edges("new", "a", "R", 0)] == ["c"]
        db.batch_json({"inserts": {"a": {"links": ["b"], "new_links": ["d"]}}})
        db.run_until_idle()
        assert [base64.b64decode(e["target_b64"]).decode() for e in db.edges("new", "a", "R", 0)] == ["d"]
    with _open_graph_test(path, options) as db:
        assert [base64.b64decode(e["target_b64"]).decode() for e in db.edges("new", "a", "R", 0)] == ["d"]


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_replacement_registration_and_optional_wait(tmp_path, storage):
    options = antfly_embedded.OpenOptions(storage=storage)
    path = tmp_path / ("shadow.aflite" if storage == antfly_embedded.Storage.LITE else "shadow")
    with _open_graph_test(path, options) as db:
        db.add_index({"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]})
        db.batch_json({"inserts": {f"source:{i:04}": {"links": ["old"], "next_links": ["new"]} for i in range(300)}})
        db.run_until_idle()
        request = {
            "name": "new",
            "type": "graph",
            "replaces": "old",
            "edge_types": [{"name": "R", "field": "next_links"}],
        }
        with pytest.raises(antfly_embedded.BusyError):
            db.add_index(dict(request, wait={"timeout_ms": 0}))
        # The timeout follows accepted registration; it must preserve the job.
        progress = db.pending_work_stats()["replacements"]
        assert progress[0]["name"] == "new"
        assert progress[0]["replaces"] == "old"
        assert progress[0]["last_error"] is None
        assert progress[0]["retry_count"] == 0
        assert progress[0]["total_retry_count"] == 0
        assert progress[0]["max_total_retries"] == 64
        db.add_index(request)
        names = {i["name"] for i in db.list_indexes()}
        if "old" in names:
            assert [base64.b64decode(e["target_b64"]).decode() for e in db.edges("old", "source:0000", "R", 0)] == [
                "old"
            ]
        db.add_index(dict(request, wait={"timeout_ms": 10000}))
        assert not db.pending_work_stats()["replacements"]
        assert [base64.b64decode(e["target_b64"]).decode() for e in db.edges("new", "source:0000", "R", 0)] == ["new"]
        db.add_index(dict(request, wait={"timeout_ms": 0}))


def test_directory_open_does_not_open_broken_named_table(tmp_path):
    options = antfly_embedded.OpenOptions(storage=antfly_embedded.Storage.DIRECTORY)
    path = tmp_path / "lazy"
    with _open_graph_test(path, options) as db:
        db.create_table("bad", {})
        db.batch_json({"inserts": {"healthy": {"text": "available"}}})
    table_path = path / "embedded" / "tables" / "2"
    table_path.rename(path / "saved-table")
    table_path.write_bytes(b"not a table directory")
    with _open_graph_test(path, options) as db:
        assert "bad" in db.list_tables()
        assert db.lookup("healthy")["text"] == "available"
        db.status()
        db.capabilities()
        with pytest.raises(antfly_embedded.AntflyError):
            db.open_table("bad")
        db.run_until_idle()
        assert "bad" in db.pending_work_stats()["unavailable_tables"]
        assert db.sql("SELECT 42 AS value")["rows"] == [["42"]]
        with pytest.raises(antfly_embedded.AntflyError):
            db.backup()  # A full archive must not silently lose a table.
        scoped = db.backup(tables=["default"])
    with antfly_embedded.create(tmp_path / "healthy-restore.aflite") as restored:
        restored.import_backup(scoped)
        assert restored.lookup("healthy")["text"] == "available"
        assert "bad" not in restored.list_tables()


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_background_replacement_finishes_under_concurrent_writes(tmp_path, storage):
    options = antfly_embedded.OpenOptions(storage=storage)
    path = tmp_path / ("busy-shadow.aflite" if storage == antfly_embedded.Storage.LITE else "busy-shadow")
    with _open_graph_test(path, options) as db:
        db.add_index({"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]})
        db.batch_json({"inserts": {f"source:{i:04}": {"links": ["old"], "next_links": ["new"]} for i in range(500)}})
        db.run_until_idle()
        stop = threading.Event()
        started = threading.Event()

        def write():
            revisions = 0
            started.set()
            while not stop.is_set():
                db.batch_json({"inserts": {"source:0000": {"links": ["old"], "next_links": [f"v{revisions}"]}}})
                revisions += 1
                time.sleep(0.002)
            return revisions

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(write)
            try:
                assert started.wait(timeout=5)
                request = {
                    "name": "new",
                    "type": "graph",
                    "replaces": "old",
                    "edge_types": [{"name": "R", "field": "next_links"}],
                }
                db.add_index(request)
                db.add_index(request)
                deadline = time.monotonic() + 90
                while "new" not in {i["name"] for i in db.list_indexes()}:
                    assert time.monotonic() < deadline, json.dumps(db.pending_work_stats())
                    time.sleep(0.01)
            finally:
                stop.set()
            assert future.result(timeout=10) > 1
        db.batch_json({"inserts": {"source:0000": {"links": ["old"], "next_links": ["final"]}}})
        db.run_until_idle()
        assert [base64.b64decode(e["target_b64"]).decode() for e in db.edges("new", "source:0000", "R", 0)] == ["final"]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX SIGKILL and pipe-polling fixture")
@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_replacement_recovers_after_process_exit(tmp_path, storage):
    import select
    import signal

    path = tmp_path / ("crash-shadow.aflite" if storage == antfly_embedded.Storage.LITE else "crash-shadow")
    script = r"""
import sys, time, json
import antfly_embedded as af
options = af.OpenOptions(storage=af.Storage(int(sys.argv[2])), busy_timeout=5)
opener = af.create_with_options if options.storage == af.Storage.LITE else af.open_with_options
db = opener(sys.argv[1], options)
db.add_index({"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]})
# Many source pages force a partial build; endpoint rows check both the
# acknowledged prefix and the unbuilt suffix without benchmarking graph size.
db.batch_json({"inserts": {f"source:{i:04}": {"links": ["old"] if i in (0, 9999) else [],
                    "next_links": ["new"] if i in (0, 9999) else []} for i in range(10000)}})
db.run_until_idle()
print("ready", flush=True)
assert sys.stdin.readline().strip() == "build"
while True:
    try:
        db.add_index({"name": "new", "type": "graph", "replaces": "old",
                      "edge_types": [{"name": "R", "field": "next_links"}], "wait": {"timeout_ms": 25}})
    except af.BusyError:
        pass
    else:
        raise AssertionError("replacement finished before the parent observed its crash point")
    progress = db.pending_work_stats()["replacements"]
    if progress and progress[0]["phase"] == "building" and progress[0]["scanned_keys"] > 0:
        assert "old" in {i["name"] for i in db.list_indexes()}
        print(json.dumps(progress[0]), flush=True)
        sys.stdin.readline()  # Parent kills us; never close or flush.
        raise AssertionError("parent did not crash the builder")
    time.sleep(0.05)
"""
    with subprocess.Popen(
        [sys.executable, "-u", "-c", script, str(path), str(int(storage))],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as child:
        try:
            assert child.stdin is not None
            assert child.stdout is not None
            assert child.stderr is not None
            assert select.select([child.stdout], [], [], 60)[0], "child did not initialize"
            assert child.stdout.readline().strip() == "ready"
            child.stdin.write("build\n")
            child.stdin.flush()
            assert select.select([child.stdout], [], [], 30)[0], "builder did not checkpoint"
            line = child.stdout.readline()
            assert line, child.stderr.read()
            observed = json.loads(line)
            assert observed["phase"] == "building" and observed["scanned_keys"] > 0
            child.kill()
            assert child.wait(timeout=10) == -signal.SIGKILL
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
    # Observe the durable marker after the crash and before any writer can
    # resume. This proves the fixture killed a partially built replacement.
    with _open_graph_test(
        path, antfly_embedded.OpenOptions(storage=storage, mode=antfly_embedded.OpenMode.READONLY)
    ) as observer:
        progress = observer.pending_work_stats()["replacements"]
        assert len(progress) == 1 and progress[0]["phase"] == "building"
        assert progress[0]["scanned_keys"] >= observed["scanned_keys"]
    with _open_graph_test(path, antfly_embedded.OpenOptions(storage=storage)) as db:
        db.run_until_idle()
        assert "old" not in {i["name"] for i in db.list_indexes()}
        for key in ("source:0000", "source:9999"):
            assert [base64.b64decode(e["target_b64"]).decode() for e in db.edges("new", key, "R", 0)] == ["new"]


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_pending_replacement_name_is_reserved_and_cancellable(tmp_path, storage):
    options = antfly_embedded.OpenOptions(storage=storage)
    path = tmp_path / ("reserved.aflite" if storage == antfly_embedded.Storage.LITE else "reserved")
    with _open_graph_test(path, options) as db:
        db.add_index({"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]})
        db.batch_json({"inserts": {f"s:{i:04}": {"links": ["old"], "next_links": ["new"]} for i in range(300)}})
        db.run_until_idle()
        config = {"name": "new", "type": "graph", "edge_types": [{"name": "R", "field": "next_links"}]}
        with pytest.raises(antfly_embedded.BusyError):
            db.add_index(dict(config, replaces="old", wait={"timeout_ms": 0}))
        db.add_index(config)  # Codex's plain inline config replay joins the shadow.
        with pytest.raises(antfly_embedded.ReplacementConfigurationMismatchError):
            db.add_index(dict(config, edge_types=[{"name": "R", "field": "wrong"}]))
        assert db.cancel_replacement("new")
        assert not db.cancel_replacement("new")
        assert "old" in {i["name"] for i in db.list_indexes()}
        db.run_until_idle()
        assert not db.pending_work_stats()["replacements"]
        # Cancellation retires the shadow before its asynchronous artifact
        # cleanup releases the name. Draining replay is not that cleanup fence.
        # Retry only the documented retirement debt, without hiding other Busy
        # errors or weakening the final replacement/edge assertions.
        deadline = time.monotonic() + 10
        while True:
            try:
                db.add_index(dict(config, replaces="old"))
                break
            except antfly_embedded.BusyError as error:
                assert "IndexArtifactCleanupPending" in str(error)
                assert time.monotonic() < deadline, str(error)
                time.sleep(0.01)
        db.run_until_idle()
        assert [base64.b64decode(e["target_b64"]).decode() for e in db.edges("new", "s:0000", "R", 0)] == ["new"]


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_plain_table_lifecycle_preserves_reader_version_one(tmp_path, storage):
    options = antfly_embedded.OpenOptions(storage=storage)
    path = tmp_path / ("epoch.aflite" if storage == antfly_embedded.Storage.LITE else "epoch")
    with _open_graph_test(path, options) as db:
        db.create_table("plain", {})
        assert db.capabilities()["minimum_reader_version"] == 1
        db.drop_table("plain")
        assert db.capabilities()["minimum_reader_version"] == 1
    with _open_graph_test(path, options) as db:
        assert db.capabilities()["minimum_reader_version"] == 1


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_plain_replacement_temporarily_fences_older_writers(tmp_path, storage):
    path = tmp_path / ("plain-replace.aflite" if storage == antfly_embedded.Storage.LITE else "plain-replace")
    options = antfly_embedded.OpenOptions(storage=storage)
    with _open_graph_test(path, options) as db:
        db.add_index({"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]})
        db.batch_json({"inserts": {f"s:{i:04}": {"links": ["target"]} for i in range(300)}})
        db.run_until_idle()
        with pytest.raises(antfly_embedded.BusyError):
            db.add_index(
                {
                    "name": "new",
                    "type": "graph",
                    "replaces": "old",
                    "edge_types": [{"name": "R", "field": "links"}],
                    "wait": {"timeout_ms": 0},
                }
            )
        assert db.capabilities()["minimum_reader_version"] == 2
        db.run_until_idle()
        assert db.capabilities()["minimum_reader_version"] == 1
    with _open_graph_test(path, options) as db:
        assert db.capabilities()["minimum_reader_version"] == 1


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_replacement_wait_observes_cancellation(tmp_path, storage):
    import concurrent.futures
    import time

    path = tmp_path / ("cancel-wait.aflite" if storage == antfly_embedded.Storage.LITE else "cancel-wait")
    options = antfly_embedded.OpenOptions(storage=storage)
    with _open_graph_test(path, options) as db:
        db.add_index({"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]})
        db.batch_json({"inserts": {f"s:{i:05}": {"links": ["target"]} for i in range(3000)}})
        db.run_until_idle()
        config = {"name": "new", "type": "graph", "replaces": "old", "edge_types": [{"name": "R", "field": "links"}]}
        with pytest.raises(antfly_embedded.BusyError):
            db.add_index(dict(config, wait={"timeout_ms": 0}))
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            waiter = pool.submit(db.add_index, dict(config, wait={"timeout_ms": 60000}))
            time.sleep(0.02)
            assert db.cancel_replacement("new")
            with pytest.raises(antfly_embedded.CancelledError):
                waiter.result(timeout=10)
        assert "old" in {i["name"] for i in db.list_indexes()}


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
@pytest.mark.parametrize("finish", ["cancel", "swap"])
def test_named_replacement_does_not_permanently_upgrade_root(tmp_path, storage, finish):
    path = tmp_path / ("named-fence.aflite" if storage == antfly_embedded.Storage.LITE else "named-fence")
    options = antfly_embedded.OpenOptions(storage=storage)
    graph = {"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]}
    with _open_graph_test(path, options) as db:
        db.create_table("source", {})
        with db.open_table("source") as table:
            table.add_index(graph)
            table.batch_json({"inserts": {f"s:{i:04}": {"links": ["target"]} for i in range(300)}})
            db.run_until_idle()
            with pytest.raises(antfly_embedded.BusyError):
                table.add_index(dict(graph, name="new", replaces="old", wait={"timeout_ms": 0}))
            assert table.capabilities()["minimum_reader_version"] == 2
            assert db.capabilities()["minimum_reader_version"] == 2
            if finish == "cancel":
                assert table.cancel_replacement("new")
            else:
                db.run_until_idle()
            assert table.capabilities()["minimum_reader_version"] == 1
            assert db.capabilities()["minimum_reader_version"] == 1
    with _open_graph_test(path, options) as db:
        assert db.capabilities()["minimum_reader_version"] == 1
        with db.open_table("source") as table:
            assert table.capabilities()["minimum_reader_version"] == 1
            names = {i["name"] for i in table.list_indexes()}
            assert names & {"old", "new"} == {"old" if finish == "cancel" else "new"}


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_child_permanent_epoch_survives_root_temporary_fence(tmp_path, storage):
    expected_reader = LITE_RESOLVER_CONSUMER_READER_VERSION if storage == antfly_embedded.Storage.LITE else 3
    path = tmp_path / ("aggregate-fence.aflite" if storage == antfly_embedded.Storage.LITE else "aggregate-fence")
    options = antfly_embedded.OpenOptions(storage=storage)
    graph = {"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]}
    with _open_graph_test(path, options) as db:
        db.create_table("source", {})
        db.add_index(graph)
        db.batch_json({"inserts": {f"s:{i:04}": {"links": ["target"]} for i in range(300)}})
        db.run_until_idle()
        with pytest.raises(antfly_embedded.BusyError):
            db.add_index(dict(graph, name="new", replaces="old", wait={"timeout_ms": 0}))
        with db.open_table("source") as table:
            table.add_index(_resolver_graph("knowledge", "concepts", destination="source"))
        assert db.cancel_replacement("new")
        assert db.capabilities()["minimum_reader_version"] == expected_reader
    with _open_graph_test(path, options) as db:
        # Permanent aggregation is durable, even before opening the child.
        assert db.capabilities()["minimum_reader_version"] == expected_reader


@pytest.mark.parametrize("promote", [False, True])
@pytest.mark.parametrize("write_count", [1, 10])
def test_inline_resolver_background_work_settles_after_writes(
    aflite_path, promote, write_count, background_progress_deadline
) -> None:
    options = antfly_embedded.OpenOptions(
        profile=antfly_embedded.Profile.NATIVE, generated_enrichment_replay=True, busy_timeout=5.0
    )
    with antfly_embedded.create_with_options(aflite_path, options) as db:
        schema = {"document_schemas": {"default": {"schema": {"type": "object", "additionalProperties": True}}}}
        db.create_table("concepts", schema)
        db.create_table("docs", schema)
        with db.open_table("docs") as docs:
            request = {
                "name": "kg",
                "kind": "graph",
                "config_json": json.dumps(
                    {
                        "source": {
                            "artifact": "extraction",
                            "path": "$.relations[*]",
                            "format": "extraction_relation",
                            "mention_edge_type": "about",
                        },
                        "artifact": {
                            "name": "extraction",
                            "kind": "asset",
                            "source": {"type": "field", "value": "text"},
                            "content_type": "application/json",
                        },
                        "edge_types": [{"name": "about"}],
                        "resolvers": [
                            {
                                "name": "concepts",
                                "table": "concepts",
                                "source_artifact": "extraction",
                                "resolution_artifact": "resolution",
                                "key_template": "concept/{{ uslug _entity.text }}",
                                "promote": promote,
                            }
                        ],
                    }
                ),
            }
            docs.add_index(request)
            extraction = {"entities": [{"id": "a", "text": "Antfly", "label": "technology"}], "relations": []}
            graph_name = "kg"
            for ordinal in range(write_count):
                docs.batch_json({"inserts": {f"d{ordinal}": {"text": json.dumps(extraction)}}, "sync_level": "write"})
                if ordinal == 5:
                    graph_name = "kg2"
                    docs.add_index({**request, "name": graph_name, "replaces": "kg"})
                if write_count > 1:
                    time.sleep(0.1)
            deadline = background_progress_deadline()
            while True:
                stats = docs.pending_work_stats()
                if (
                    stats["enrichment"]["applied_sequence"] >= stats["enrichment"]["target_sequence"] > 0
                    and not stats["resolution"]["catch_up_required"]
                    and not stats["promotion"]["catch_up_required"]
                    and graph_name in {index["name"] for index in docs.list_indexes()}
                ):
                    break
                if deadline.expired(stats):
                    pytest.fail(f"background resolver work did not settle: {json.dumps(stats, sort_keys=True)}")
                time.sleep(0.1)
            assert docs.edges(graph_name, "d0", "about", antfly_embedded.GraphDirection.OUT)
            sequence = stats["derived_target_sequence"]
            # Let the real worker run several further rounds without a write
            # or a foreground drain that could hide detached publication bugs.
            time.sleep(2)
            after = docs.pending_work_stats()
            assert after["derived_target_sequence"] == sequence, after
            assert not after["resolution"]["catch_up_required"], after
            assert not after["promotion"]["catch_up_required"], after


def test_native_child_replacement_replays_generated_artifacts_beside_idle_root(tmp_path):
    options = antfly_embedded.OpenOptions(
        storage=antfly_embedded.Storage.LITE,
        profile=antfly_embedded.Profile.NATIVE,
        generated_enrichment_replay=True,
    )
    with antfly_embedded.create_with_options(tmp_path / "child-replacement.aflite", options) as db:
        db.create_table("child", {})
        with db.open_table("child") as table:
            table.add_index({"name": "old", "type": "graph", "edge_types": [{"name": "R", "field": "links"}]})
            table.batch_json(
                {
                    "inserts": {
                        "claim": {"links": ["old"], "kg": {"entities": [{"id": "a", "text": "Ada", "label": "person"}]}}
                    }
                }
            )
            table.run_until_idle()
            replacement = _resolver_graph("new", "people", destination="child")
            replacement["replaces"] = "old"
            try:
                table.add_index(dict(replacement, wait={"timeout_ms": 0}))
            except antfly_embedded.BusyError:
                pass
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                names = {index["name"] for index in table.list_indexes()}
                if "new" in names and "old" not in names:
                    break
                time.sleep(0.02)
            else:
                pytest.fail(f"child replacement starved producer replay: {table.pending_work_stats()}")
            assert [
                base64.b64decode(edge["target_b64"]).decode() for edge in table.edges("new", "claim", "mentions", 0)
            ] == ["entity/ada"]


@pytest.mark.parametrize("storage", [antfly_embedded.Storage.LITE, antfly_embedded.Storage.DIRECTORY])
def test_qualified_incoming_graph_starts_preserve_table_and_path(tmp_path, storage):
    path = tmp_path / ("qualified.aflite" if storage == antfly_embedded.Storage.LITE else "qualified")
    with _open_graph_test(path, antfly_embedded.OpenOptions(storage=storage)) as db:
        db.add_index({"name": "graph", "kind": "graph", "config_json": "{}"})
        db.batch_json(
            {
                "inserts": {"a": {}, "b": {}},
                "graph_writes": [
                    {
                        "index_name": "graph",
                        "source": "a",
                        "target": "shared",
                        "edge_type": "about",
                        "metadata_json": '{"target_table":"concepts"}',
                    },
                    {
                        "index_name": "graph",
                        "source": "b",
                        "target": "shared",
                        "edge_type": "about",
                        "metadata_json": '{"target_table":"other_concepts"}',
                    },
                ],
                "sync_level": "full_index",
            }
        )
        local = db.search(
            {
                "graph_queries": {
                    "outgoing": {
                        "index": "graph",
                        "traverse": {
                            "start": {"identities": [{"table": "docs", "key": "a"}]},
                            "direction": "out",
                            "edge_types": ["about"],
                            "max_depth": 1,
                        },
                    }
                }
            }
        )["responses"][0]["graph_results"]["outgoing"]["nodes"]
        assert [(node["table"], node["key"]) for node in local] == [("concepts", "shared")]
        for table, expected in (("concepts", ["a"]), ("other_concepts", ["b"]), ("unknown", [])):
            result = db.search(
                {
                    "graph_queries": {
                        "incoming": {
                            "index": "graph",
                            "traverse": {
                                "start": {"identities": [{"table": table, "key": "shared"}]},
                                "direction": "in",
                                "edge_types": ["about"],
                                "max_depth": 1,
                                "include_paths": True,
                            },
                        }
                    }
                }
            )
            nodes = result["responses"][0]["graph_results"]["incoming"]["nodes"]
            assert [node["key"] for node in nodes] == expected
            if nodes:
                assert nodes[0]["path"][0]["table"] == table
