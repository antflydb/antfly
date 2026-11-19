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
import shlex
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import requests

from bench_descriptor_pressure import (
    call,
    ProcessMeasurements,
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


@pytest.fixture
def descriptor_test_binary():
    binary = Path(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    )
    if not binary.is_file():
        pytest.skip(f"Antfly binary not found: {binary}")
    return binary


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.skipif(
    sys.platform not in ("darwin", "linux"), reason="requires FD sampling"
)
def test_full_index_ingest_and_reopen_with_minimum_file_limit(
    tmp_path, descriptor_test_binary
):
    wrapper = tmp_path / "antfly-with-file-limit"
    write_server_wrapper(wrapper, descriptor_test_binary, 256)
    measurements = ProcessMeasurements()
    server = None
    query_done = threading.Event()
    failed = True
    try:
        server = StandaloneAntflyServer(
            str(wrapper),
            "127.0.0.1",
            find_free_port(),
            metrics=True,
            api_root=ANTFLY_PUBLIC_API_ROOT,
            on_process_started=measurements.start,
        )
        url = server.api_url
        dimensions, batch_size, batches = 384, 128, 128
        rng = random.Random(1744)
        first_vector = None

        def assert_full_text_hit(session):
            result = call(
                session,
                url,
                "POST",
                "/tables/documents/query",
                {
                    "full_text_search": {"field": "text", "match": "descriptorcanary"},
                    "indexes": ["lexical"],
                    "limit": 2,
                },
            )
            hits = result["responses"][0]["hits"]["hits"]
            assert [hit["_id"] for hit in hits] == ["d000000000"]

        def query_during_ingest(vector):
            count = 0
            with requests.Session() as reader:
                while not query_done.is_set():
                    query(reader, url, vector)
                    count += 1
                    query_done.wait(0.02)
            return count

        with (
            requests.Session() as session,
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
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
            call(
                session,
                url,
                "POST",
                "/tables/documents/indexes/lexical",
                {"type": "full_text", "field": "text"},
            )
            ingest_queries = None
            try:
                for batch in range(batches):
                    measurements.check()
                    if ingest_queries is not None and ingest_queries.done():
                        ingest_queries.result()
                    offset = batch * batch_size
                    rows = {}
                    for index in range(offset, offset + batch_size):
                        vector = [rng.uniform(-1, 1) for _ in range(dimensions)]
                        if index == 0:
                            first_vector = vector
                        rows[f"d{index:09d}"] = {
                            "text": (
                                "descriptorcanary"
                                if index == 0
                                else f"Generated document {index}"
                            ),
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
                    if batch == 0:
                        ingest_queries = executor.submit(
                            query_during_ingest, first_vector
                        )
                assert_full_text_hit(session)
                response = session.get(f"{server.health_url}/metrics", timeout=5)
                response.raise_for_status()
                metrics = dict(
                    line.split()
                    for line in response.text.splitlines()
                    if line.startswith("antfly_storage_native_fd_")
                )
                capacity = int(metrics["antfly_storage_native_fd_admission_capacity"])
                assert capacity == 128
                assert 0 < int(metrics["antfly_storage_native_fd_admitted"]) <= capacity
            finally:
                query_done.set()
            assert ingest_queries.result() > 0

        measurements.finish_phase()
        server.pause(crash=True)
        measurements.phase = "recovery"
        server.resume()
        with requests.Session() as session:
            wait_visible(
                session,
                url,
                batch_size * batches,
                check_health=measurements.check,
                failure_event=measurements.failed,
            )
            for index in range(batch_size - 1, batch_size * batches, batch_size):
                call(session, url, "GET", f"/tables/documents/documents/d{index:09d}")
            query(session, url, first_vector)
            assert_full_text_hit(session)

        measurements.finish_phase()
        for phase, record in measurements.records().items():
            # Health gauges are cached. Compare live samples against the stable
            # capacity with room for the server's non-storage descriptors.
            assert record["peak_fds"] <= capacity + 64, (phase, record["peak_fds"])
        failed = False
    finally:
        try:
            if server is not None:
                server.stop(test_failed=failed)
        finally:
            measurements.close()


@pytest.mark.e2e_resource("antfly_process")
@pytest.mark.skipif(
    sys.platform not in ("darwin", "linux"), reason="requires native open interposition"
)
def test_retry_after_source_vector_wal_open_exhaustion(
    tmp_path, descriptor_test_binary
):
    cc = shutil.which("cc")
    if cc is None:
        pytest.skip("A C compiler is required for the WAL-open fault fixture")
    fixture = Path(__file__).parent / "fixtures" / "source_vector_wal_open_fault.c"
    if sys.platform == "linux":
        readelf = shutil.which("readelf")
        if readelf is None:
            pytest.skip("readelf is required to check Linux preload support")
        program_headers = subprocess.run(
            [readelf, "-l", str(descriptor_test_binary)],
            capture_output=True,
            check=True,
        ).stdout
        if b"ld-linux" not in program_headers:
            pytest.skip("The Linux fault fixture requires a glibc-linked binary")
    darwin = sys.platform == "darwin"
    library = tmp_path / ("wal-open-fault.dylib" if darwin else "wal-open-fault.so")
    loader_env = "DYLD_INSERT_LIBRARIES" if darwin else "LD_PRELOAD"
    subprocess.run(
        [
            cc,
            "-std=c11",
            "-Wall",
            "-Wextra",
            "-Werror",
            *(["-dynamiclib"] if darwin else ["-shared", "-fPIC"]),
            str(fixture),
            "-o",
            str(library),
            *([] if darwin else ["-ldl"]),
        ],
        check=True,
    )
    arm = tmp_path / "fail-next-wal-open"
    # Set the loader variable immediately before exec, after the shell used by
    # the limit wrapper. Only this test's server inherits the interposer.
    runtime = tmp_path / "fault-runtime"
    standalone_arg = (
        "shift\n" if descriptor_test_binary.name == "antfly-standalone" else ""
    )
    runtime.write_text(
        "#!/bin/sh\n"
        f"{standalone_arg}"
        f"export {loader_env}={shlex.quote(str(library))}\n"
        f"export ANTFLY_TEST_WAL_OPEN_FAULT={shlex.quote(str(arm))}\n"
        f'exec {shlex.quote(str(descriptor_test_binary))} "$@"\n'
    )
    runtime.chmod(0o700)
    wrapper = tmp_path / "antfly-with-wal-open-fault"
    write_server_wrapper(wrapper, runtime, 256)
    server = StandaloneAntflyServer(
        str(wrapper), "127.0.0.1", find_free_port(), api_root=ANTFLY_PUBLIC_API_ROOT
    )
    failed = True
    vector = [1.0, 0.5, 0.25]
    batch_path = "/tables/documents/batch"
    payload = {
        "inserts": {
            "d000000000": {"text": "descriptor retry", "_embeddings": {"vec": vector}}
        },
        "sync_level": "full_index",
    }
    try:
        url = server.api_url
        with requests.Session() as session:
            call(session, url, "POST", "/tables/documents", {"num_shards": 1})
            call(
                session,
                url,
                "POST",
                "/tables/documents/indexes/vec",
                {"type": "embeddings", "dimension": len(vector), "external": True},
            )
            arm.touch()
            first = session.post(url + batch_path, json=payload, timeout=30)
            assert not arm.exists(), (
                "source-vector WAL open did not reach the fault fixture"
            )
            assert first.status_code == 503, first.text
            assert first.json()["error"] == "storage_descriptor_pressure"
            assert first.headers["Retry-After"] == "1"
            time.sleep(int(first.headers["Retry-After"]))
            retry = session.post(url + batch_path, json=payload, timeout=30)
            assert retry.status_code == 201, retry.text
            assert retry.json()["status"] == "committed"

        server.pause(crash=True)
        server.resume()
        with requests.Session() as session:
            doc = call(session, url, "GET", "/tables/documents/documents/d000000000")
            assert doc["text"] == "descriptor retry"
            query(session, url, vector)
        failed = False
    finally:
        server.stop(test_failed=failed)
