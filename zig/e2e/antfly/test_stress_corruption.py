# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# Elastic License 2.0 for the specific language governing permissions and
# limitations.

"""Heap-corruption regression stress for the managed chunked-semantic flow.

Repeats the create/index/write/scan/search/backup/delete cycle against a
single swarm server. This loop reproduced the request-allocator identity
corruption (search hits adopting index-owned metadata buffers freed through
a different allocator) within one to three iterations; a crashed server
surfaces as a connection error on the next request.
"""

from __future__ import annotations

import tempfile
import time

import pytest
from helpers import assert_created_index, wait_until
from test_backup_restore import _chunked_doc, _semantic_top_hit

pytestmark = pytest.mark.slow


def test_stress_chunked_semantic_corruption(backup_api, openai_embedder):
    for i in range(30):
        table_name = f"stress_chunked_{i}_{time.time_ns()}"
        backup_id = f"stress-backup-{i}-{time.time_ns()}"

        created = backup_api.create_table(
            table_name, num_shards=1, description="stress"
        )
        assert created["name"] == table_name

        assert_created_index(
            backup_api.create_index(
                table_name,
                "semantic_chunked_idx",
                {
                    "name": "semantic_chunked_idx",
                    "type": "embeddings",
                    "field": "content",
                    "dimension": 3,
                    "embedder": {
                        "provider": "openai",
                        "model": "text-embedding-3-small",
                        "url": openai_embedder,
                    },
                    "chunker": {
                        "provider": "antfly",
                        "model": "fixed-bert-tokenizer",
                        "store_chunks": True,
                        "text": {
                            "target_tokens": 4,
                            "overlap_tokens": 1,
                            "separator": " ",
                        },
                    },
                },
            ),
            "semantic_chunked_idx",
            "embeddings",
        )

        backup_api.wait_index_ready(
            table_name,
            "semantic_chunked_idx",
            timeout_s=30.0,
            interval_s=0.5,
            until="complete",
        )

        batch = backup_api.batch_write(
            table_name,
            inserts={
                "doc:a": {
                    "title": "Alpha backup",
                    "content": "alpha body alpha body alpha body alpha body alpha tail",
                },
                "doc:b": {
                    "title": "Beta backup",
                    "content": "beta body beta body beta body beta tail",
                },
            },
            sync_level="full_index",
        )
        assert batch["inserted"] == 2, f"iteration {i}"

        before_scan = wait_until(
            lambda: _chunked_doc(
                backup_api, table_name, "doc:a", "semantic_chunked_idx_chunks"
            ),
            timeout_s=60.0,
            interval_s=0.5,
        )
        assert before_scan is not None, f"iteration {i}"

        assert wait_until(
            lambda: _semantic_top_hit(
                backup_api, table_name, "alpha concept", "semantic_chunked_idx", "doc:a"
            ),
            timeout_s=60.0,
            interval_s=0.5,
        ), f"iteration {i}"

        with tempfile.TemporaryDirectory(prefix="antfly-stress-backup-") as backup_dir:
            location = f"file://{backup_dir}"
            backup = backup_api.backup_table(
                table_name, backup_id=backup_id, location=location
            )
            assert backup["backup"] == "successful", f"iteration {i}"

            deleted = backup_api.delete_table(table_name)
            assert deleted == {}, f"iteration {i}"
