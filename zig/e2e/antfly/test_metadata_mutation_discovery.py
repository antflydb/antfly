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

"""A stalled metadata discovery endpoint must not hide a reachable leader."""

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests
import test_backup_restore as backups

three_by_three_backup_cluster = backups.three_by_three_backup_cluster


@pytest.fixture
def stalled_metadata_discovery(monkeypatch, request):
    stopped = threading.Event()
    observed = threading.Event()
    enabled = threading.Event()
    fault_lock = threading.Lock()
    proxies = []
    threads = []
    stall_all = getattr(request, "param", None) == "all"
    fault_path = "/metadata/v1/status" if stall_all else "/metadata/v1/runtime-topology"

    class Proxy(BaseHTTPRequestHandler):
        def forward(self):
            with fault_lock:
                stall = (
                    enabled.is_set()
                    and (stall_all or not observed.is_set())
                    and self.command == "GET"
                    and self.path == fault_path
                )
                if stall:
                    observed.set()
            if stall:
                # Keep the request pending beyond the complete mutation budget.
                # Other routes continue forwarding normally.
                stopped.wait(30.0)
                self.close_connection = True
                return
            # Descriptor admission is remote I/O, not an index-build quantum.
            # Keep it slower than the 25 ms repair slice so restore must make
            # progress even when catalog requests have ordinary network latency.
            if enabled.is_set() and self.path.startswith("/internal/v2/catalog/"):
                time.sleep(0.05)
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            with requests.request(
                self.command,
                self.server.upstream + self.path,
                data=body,
                headers={
                    key: value
                    for key, value in self.headers.items()
                    if key.lower() not in {"host", "connection", "content-length"}
                },
                timeout=10,
            ) as response:
                self.send_response(response.status_code)
                for key, value in response.headers.items():
                    if key.lower() not in {
                        "connection",
                        "content-length",
                        "content-encoding",
                        "transfer-encoding",
                    }:
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(response.content)))
                self.send_header("Connection", "close")
                try:
                    self.end_headers()
                    self.wfile.write(response.content)
                except (BrokenPipeError, ConnectionResetError):
                    # Bounded/fanned-out catalog callers may abandon a response.
                    self.close_connection = True

        do_GET = forward
        do_POST = forward
        do_PUT = forward
        do_DELETE = forward

        def log_message(self, *args):
            pass

    original_command = backups.ThreeByThreeBackupCluster._data_command

    def command_with_stalled_discovery(cluster, index):
        command = original_command(cluster, index)
        if index != 0:
            return command
        # Wrap every configured route so leader affinity cannot bypass the
        # fault. By default stall the first topology request after activation;
        # the all variant stalls every diagnostic status request.
        endpoints = []
        for upstream in cluster.metadata_admin_urls:
            proxy = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
            proxy.upstream = upstream
            proxy.daemon_threads = False
            thread = threading.Thread(target=proxy.serve_forever, daemon=True)
            thread.start()
            proxies.append(proxy)
            threads.append(thread)
            endpoints.append(f"http://127.0.0.1:{proxy.server_port}")
        command = command[: command.index("--metadata-api")]
        for url in endpoints:
            command.extend(["--metadata-api", url])
        return command

    monkeypatch.setattr(
        backups.ThreeByThreeBackupCluster,
        "_data_command",
        command_with_stalled_discovery,
    )
    try:
        yield enabled, observed
    finally:
        stopped.set()
        for proxy in proxies:
            proxy.shutdown()
            proxy.server_close()
        for thread in threads:
            thread.join()


@pytest.fixture
def stalled_discovery_backup_cluster(
    stalled_metadata_discovery, three_by_three_backup_cluster
):
    # Isolate mutation discovery from the fixture's initial bootstrap reads.
    stalled_metadata_discovery[0].set()
    return three_by_three_backup_cluster


def test_backup_restore_discovers_leader_past_stalled_topology(
    stalled_discovery_backup_cluster, stalled_metadata_discovery
):
    backups.test_three_by_three_cluster_backup_restore_through_metadata_public_api(
        stalled_discovery_backup_cluster
    )
    assert stalled_metadata_discovery[1].is_set()


@pytest.mark.parametrize("stalled_metadata_discovery", ["all"], indirect=True)
def test_catalog_mutations_do_not_wait_for_diagnostic_status(
    stalled_discovery_backup_cluster,
):
    cluster = stalled_discovery_backup_cluster
    # All diagnostic status requests remain stalled beyond the mutation budget.
    # The compact topology route and actual mutations continue serving normally.
    for index in range(3):
        response = requests.post(
            cluster.data_api_urls[0] + f"/databases/compact_discovery_{index}",
            json={},
            timeout=15,
        )
        assert response.ok, (response.status_code, response.text)
