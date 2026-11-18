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

"""Exercise the storage descriptor pool across the linked standalone runtime."""

import os
import random
import sys
import threading
from pathlib import Path

import pytest
import requests

from bench_descriptor_pressure import (
    call,
    descriptor_reader,
    query,
    wait_visible,
    write_server_wrapper,
)
from conftest import (
    ANTFLY_PUBLIC_API_ROOT,
    DEFAULT_ANTFLY_BIN,
    StandaloneAntflyServer,
    find_free_port,
    resolve_binary_path,
)


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.skipif(
    sys.platform not in ("darwin", "linux"), reason="requires FD sampling"
)
def test_full_index_ingest_and_reopen_with_minimum_file_limit(tmp_path):
    binary = Path(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    )
    if not binary.is_file():
        pytest.skip(f"Antfly binary not found: {binary}")
    wrapper = tmp_path / "antfly-with-file-limit"
    write_server_wrapper(wrapper, binary, 256)
    server = StandaloneAntflyServer(
        str(wrapper),
        "127.0.0.1",
        find_free_port(),
        metrics=True,
        api_root=ANTFLY_PUBLIC_API_ROOT,
    )
    dimensions, batch_size, batches = 384, 128, 128
    rng = random.Random(1744)
    first_vector = None
    failed = True
    try:
        url = server.api_url
        read_descriptors = descriptor_reader(server.proc.pid)
        with requests.Session() as session:
            call(session, url, "POST", "/tables/documents", {"num_shards": 1})
            call(
                session,
                url,
                "POST",
                "/tables/documents/indexes/vec",
                {
                    "type": "embeddings",
                    "dimension": dimensions,
                    "distance_metric": "cosine",
                    "external": True,
                },
            )
            for batch in range(batches):
                offset = batch * batch_size
                rows = {}
                for index in range(offset, offset + batch_size):
                    vector = [rng.uniform(-1, 1) for _ in range(dimensions)]
                    if index == 0:
                        first_vector = vector
                    rows[f"d{index:09d}"] = {
                        "text": f"Generated document {index}",
                        "_embeddings": {"vec": vector},
                    }
                result = call(
                    session,
                    url,
                    "POST",
                    "/tables/documents/batch",
                    {"inserts": rows, "sync_level": "full_index"},
                )
                assert result["status"] == "committed", (batch, result)
                if (batch + 1) % 16 == 0:
                    open_fds = read_descriptors()
                    response = session.get(f"{server.health_url}/metrics", timeout=5)
                    response.raise_for_status()
                    metrics = {
                        name: int(value)
                        for name, value in (
                            line.split()
                            for line in response.text.splitlines()
                            if line.startswith("antfly_storage_native_fd_")
                        )
                    }
                    capacity = metrics["antfly_storage_native_fd_admission_capacity"]
                    assert capacity == 128
                    assert 0 < metrics["antfly_storage_native_fd_admitted"] <= capacity
                    # The health listener serves cached metrics. Bound the
                    # live process sample against capacity and leave 64 slots
                    # for listeners and other non-storage activity.
                    assert open_fds <= capacity + 64, (batch, open_fds, metrics)

        server.pause(crash=True)
        server.resume()
        with requests.Session() as session:
            wait_visible(
                session,
                url,
                batch_size * batches,
                check_health=lambda: None,
                failure_event=threading.Event(),
            )
            for index in range(batch_size - 1, batch_size * batches, batch_size):
                call(session, url, "GET", f"/tables/documents/documents/d{index:09d}")
            query(session, url, first_vector)
        failed = False
    finally:
        server.stop(test_failed=failed)
