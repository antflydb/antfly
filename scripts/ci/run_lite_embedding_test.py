#!/usr/bin/env python3
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

"""Required public-path Qwen mention/canonical embedding and ANN regression."""

import argparse
import base64
import json
import tempfile
from pathlib import Path

import antfly_embedded as af


def run(models_dir: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="lite-public-embedding-") as directory:
        options = af.OpenOptions(
            profile=af.Profile.NATIVE,
            local_runtime_configured=True,
            generated_enrichment_replay=True,
            models_dir=models_dir,
        )
        with af.create_with_options(Path(directory) / "graph.aflite", options) as db:
            db.create_table("entities", {})
            with db.open_table("entities") as entities:
                entities.add_index(
                    {
                        "name": "names",
                        "type": "embeddings",
                        "dims": 1024,
                        "field": "canonical_name",
                        "metric": "cosine",
                        "embedder": {
                            "provider": "antfly",
                            "model": "Qwen/Qwen3-Embedding-0.6B-GGUF",
                        },
                    }
                )
                scorer = {
                    "comparisons": [
                        {
                            "name": "embedding",
                            "left": "name_embedding",
                            "right": "name_embedding",
                            "levels": [
                                {"when": "cosine > 0.5", "weight": 8.0},
                                {"else": True, "weight": -6.0},
                            ],
                        }
                    ],
                    "combine": {"bias": -3.0},
                    "decision": {"match": 0.9},
                }
                db.add_index(
                    {
                        "name": "knowledge",
                        "type": "graph",
                        "source": {
                            "artifact": "kg",
                            "format": "extraction_graph",
                            "mention_edge_type": "mentions",
                        },
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
                                "table": "entities",
                                "source_artifact": "kg",
                                "resolution_artifact": "resolved",
                                "key_template": "concept/{{ uslug _entity.text }}",
                                "candidate_search": "ann",
                                "candidate_ann_index": "names",
                                "candidate_limit": 1,
                                "name_embedding": "names",
                                "name_embedding_dims": 1024,
                                "scorer_json": json.dumps(scorer),
                                "config_generation": 1,
                            }
                        ],
                    }
                )
                for key, text in (
                    ("seed", "The approval reviewer"),
                    ("later", "Approval reviewer"),
                ):
                    db.batch_json(
                        {
                            "inserts": {
                                key: {
                                    "kg": {
                                        "entities": [
                                            {
                                                "id": "e0",
                                                "label": "concept",
                                                "text": text,
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    )
                    db.run_until_idle()
                canonical = "concept/the_approval_reviewer"
                entities.lookup(canonical)
                edges = db.edges("knowledge", "later", "mentions", 0)
                assert (
                    len(edges) == 1
                    and base64.b64decode(edges[0]["target_b64"]).decode() == canonical
                ), edges
                query = db.resolve_query(
                    "concepts",
                    {
                        "extraction": {
                            "entities": [
                                {
                                    "id": "q",
                                    "label": "concept",
                                    "text": "Approval reviewer",
                                }
                            ]
                        }
                    },
                )
                resolved = query["resolution"]["entities"][0]
                assert (
                    resolved["decision"] == "match"
                    and resolved["doc_ref"]["key"] == canonical
                ), query
                try:
                    entities.lookup("concept/approval_reviewer")
                except af.NotFoundError:
                    pass
                else:
                    raise AssertionError(
                        "ANN failed to merge the second generated mention embedding"
                    )
    print(
        "public Qwen promotion, mention generation, ANN visibility and query resolution passed"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models-dir", type=Path, required=True)
    run(parser.parse_args().models_dir)
