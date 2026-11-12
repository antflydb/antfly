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

"""Protocol and resource invariants for the local lake network fixture."""

import json
import http.client
from pathlib import Path
import tempfile
import unittest
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from concurrent.futures import ThreadPoolExecutor
import time

from lake_network_store import Network, NetworkStore


class NetworkStoreTest(unittest.TestCase):
    def test_resumable_upload_retains_atomic_generation_preconditions_and_cleanup(self):
        with (
            tempfile.TemporaryDirectory() as root,
            NetworkStore(Path(root) / "store") as service,
        ):

            def begin():
                request = Request(
                    service.url
                    + "/upload/storage/v1/b/lake-benchmark/o?uploadType=resumable&name=object&ifGenerationMatch=0",
                    data=b"{}",
                    method="POST",
                    headers={"X-Upload-Content-Length": "6"},
                )
                with urlopen(request) as response:
                    return response.headers["Location"]

            session = begin()
            with self.assertRaises(HTTPError) as progress:
                urlopen(
                    Request(
                        session,
                        data=b"abc",
                        method="PUT",
                        headers={"Content-Range": "bytes 0-2/6"},
                    )
                )
            self.assertEqual(progress.exception.code, 308)
            with urlopen(
                Request(
                    session,
                    data=b"def",
                    method="PUT",
                    headers={"Content-Range": "bytes 3-5/6"},
                )
            ) as response:
                self.assertEqual(response.status, 200)
            self.assertEqual(service.store.get("object")["size"], "6")
            with self.assertRaises(HTTPError) as error:
                urlopen(
                    Request(
                        begin(),
                        data=b"abcdef",
                        method="PUT",
                        headers={"Content-Range": "bytes 0-5/6"},
                    )
                )
            self.assertEqual(error.exception.code, 412)
            session = begin()
            with urlopen(Request(session, method="DELETE")) as response:
                self.assertEqual(response.status, 204)
            self.assertEqual(service.upload_bytes, 0)
            self.assertEqual(service.uploads, {})

    def test_connection_setup_cost_is_paid_once_on_a_reused_connection(self):
        from http.client import HTTPConnection
        from urllib.parse import urlsplit

        with (
            tempfile.TemporaryDirectory() as root,
            NetworkStore(Path(root) / "store", Network(connection_ms=10)) as service,
        ):
            host = urlsplit(service.url)
            connection = HTTPConnection(host.hostname, host.port, timeout=5)
            try:
                for _ in range(2):
                    connection.request("GET", "/storage/v1/b/lake-benchmark")
                    response = connection.getresponse()
                    self.assertEqual(response.status, 200)
                    response.read()
                service.wait_idle()
                summary = service.summary("setup")
                self.assertEqual(summary["requests"], 2)
                self.assertEqual(summary["connections"], 1)
                self.assertGreaterEqual(summary["connection_setup_ns"], 10_000_000)
            finally:
                connection.close()

    def test_versions_conditional_writes_ranges_and_listing(self):
        with (
            tempfile.TemporaryDirectory() as root,
            NetworkStore(Path(root) / "store") as service,
        ):

            def call(method, key="a/b", data=None, **params):
                path = (
                    "/upload/storage/v1/b/lake-benchmark/o"
                    if method == "POST"
                    else "/storage/v1/b/lake-benchmark/o/" + quote(key, safe="")
                )
                if method == "POST":
                    params.update(uploadType="media", name=key)
                return urlopen(
                    Request(
                        service.url + path + "?" + urlencode(params),
                        data=data,
                        method=method,
                    ),
                    timeout=5,
                )

            first = json.load(call("POST", data=b"original", ifGenerationMatch="0"))
            with self.assertRaises(HTTPError) as error:
                call("POST", data=b"wrong", ifGenerationMatch="0")
            self.assertEqual(error.exception.code, 412)
            second = json.load(
                call("POST", data=b"updated!", ifGenerationMatch=first["generation"])
            )
            self.assertNotEqual(first["generation"], second["generation"])
            self.assertEqual(
                call("GET", alt="media", generation=first["generation"]).read(),
                b"original",
            )
            request = Request(
                service.url + "/storage/v1/b/lake-benchmark/o/a%2Fb?alt=media",
                headers={"Range": "bytes=2-4"},
            )
            with urlopen(request, timeout=5) as result:
                self.assertEqual(result.status, 206)
                self.assertEqual(result.headers["Content-Range"], "bytes 2-4/8")
                self.assertEqual(result.read(), b"dat")
            with self.assertRaises(HTTPError) as error:
                call(
                    "DELETE",
                    generation=first["generation"],
                    ifGenerationMatch=second["generation"],
                )
            self.assertEqual(error.exception.code, 412)
            self.assertEqual(
                call(
                    "DELETE",
                    generation=first["generation"],
                    ifGenerationMatch=first["generation"],
                ).status,
                204,
            )
            self.assertEqual(call("GET", alt="media").read(), b"updated!")
            service.wait_idle()
            report = service.summary("setup")
            self.assertGreater(report["requests"], 5)
            self.assertEqual(report["trace_overflow"], 0)
            self.assertIsNone(report["causal_dependency_depth"])

    def test_shared_bandwidth_and_concurrency_bound(self):
        with (
            tempfile.TemporaryDirectory() as root,
            NetworkStore(
                Path(root) / "store", Network(bandwidth_mib=1, concurrency=2)
            ) as service,
        ):
            service.store.put("payload", b"x" * (128 * 1024))
            url = service.url + "/storage/v1/b/lake-benchmark/o/payload?alt=media"

            def read(_):
                with urlopen(url, timeout=5) as response:
                    return len(response.read())

            start = time.monotonic()
            with ThreadPoolExecutor(max_workers=4) as pool:
                self.assertEqual(list(pool.map(read, range(4))), [128 * 1024] * 4)
            elapsed = time.monotonic() - start
            service.wait_idle()
            report = service.summary("setup")
            self.assertLessEqual(report["peak_inflight"], 2)
            self.assertGreaterEqual(elapsed, 0.45)
            self.assertEqual(report["bytes"], 512 * 1024)

    def test_list_versions_paginates_and_filters(self):
        with (
            tempfile.TemporaryDirectory() as root,
            NetworkStore(Path(root) / "store") as service,
        ):
            service.store.put("prefix/a", b"old")
            service.store.put("prefix/a", b"new")
            service.store.put("prefix/b", b"b")
            service.store.put("other", b"excluded")
            url = (
                service.url
                + "/storage/v1/b/lake-benchmark/o?prefix=prefix%2F&versions=true&maxResults=1"
            )
            values = []
            token = None
            while True:
                with urlopen(
                    url + ("&pageToken=" + token if token is not None else ""),
                    timeout=5,
                ) as response:
                    page = json.load(response)
                values.extend(page["items"])
                token = page.get("nextPageToken")
                if token is None:
                    break
            self.assertEqual(
                [row["name"] for row in values], ["prefix/a", "prefix/a", "prefix/b"]
            )

    def test_truncation_and_disconnect_recover_without_changing_bytes(self):
        for fault in ("truncate", "reset"):
            with (
                self.subTest(fault=fault),
                tempfile.TemporaryDirectory() as root,
                NetworkStore(
                    Path(root) / "store", Network(fault=fault, fault_every=1)
                ) as service,
            ):
                item = service.store.put("x", b"immutable payload")
                url = (
                    service.url
                    + "/storage/v1/b/lake-benchmark/o/x?alt=media&generation="
                    + item["generation"]
                )
                with self.assertRaises(
                    (
                        http.client.IncompleteRead,
                        http.client.RemoteDisconnected,
                        ConnectionResetError,
                    )
                ):
                    with urlopen(url, timeout=5) as response:
                        response.read()
                service.wait_idle()
                service.configure(Network(), "recovery")
                with urlopen(url, timeout=5) as response:
                    self.assertEqual(response.read(), b"immutable payload")
                service.wait_idle()
                self.assertEqual(service.summary("setup")["faults"], 1)
                self.assertEqual(service.summary("recovery")["fixture_errors"], 0)

    def test_phase_switch_preserves_bandwidth_reservations(self):
        with (
            tempfile.TemporaryDirectory() as root,
            NetworkStore(Path(root) / "store", Network(bandwidth_mib=1)) as service,
        ):
            with service.lock:
                reserved = time.monotonic() + 2
                service.next_byte_time = reserved
                service.active = 1
            try:
                service.configure(service.network, "query")
                self.assertEqual(service.next_byte_time, reserved)
                with self.assertRaises(RuntimeError):
                    service.configure(Network(bandwidth_mib=2), "different")
            finally:
                with service.lock:
                    service.active = 0

    def test_faults_do_not_change_generation_authority(self):
        with (
            tempfile.TemporaryDirectory() as root,
            NetworkStore(
                Path(root) / "store", Network(fault="throttle", fault_every=1)
            ) as service,
        ):
            item = service.store.put("x", b"value")
            with self.assertRaises(HTTPError) as error:
                urlopen(
                    service.url + "/storage/v1/b/lake-benchmark/o/x?alt=media",
                    timeout=5,
                )
            self.assertEqual(error.exception.code, 429)
            service.wait_idle()
            service.configure(Network(), "recovery")
            with urlopen(
                service.url
                + "/storage/v1/b/lake-benchmark/o/x?generation="
                + item["generation"]
                + "&alt=media",
                timeout=5,
            ) as response:
                self.assertEqual(response.read(), b"value")
            service.wait_idle()
            self.assertEqual(service.summary("setup")["faults"], 1)
            self.assertEqual(service.summary("recovery")["faults"], 0)


if __name__ == "__main__":
    unittest.main()
