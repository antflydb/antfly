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

"""A stalled metadata status endpoint must not hide a reachable leader."""

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests
import test_backup_restore as backups

three_by_three_backup_cluster = backups.three_by_three_backup_cluster


@pytest.fixture
def stalled_metadata_status(monkeypatch):
    stopped = threading.Event()
    observed = threading.Event()
    enabled = threading.Event()
    upstream = [None]

    class Proxy(BaseHTTPRequestHandler):
        def forward(self):
            if (
                enabled.is_set()
                and self.command == "GET"
                and self.path == "/metadata/v1/status"
            ):
                observed.set()
                # Keep the request pending beyond the complete mutation budget.
                # Every direct metadata node address also remains reachable.
                stopped.wait(30.0)
                self.close_connection = True
                return
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            with requests.request(
                self.command,
                upstream[0] + self.path,
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
                self.end_headers()
                self.wfile.write(response.content)

        do_GET = forward
        do_POST = forward
        do_PUT = forward
        do_DELETE = forward

        def log_message(self, *args):
            pass

    proxy = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    proxy.daemon_threads = False
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    original_command = backups.ThreeByThreeBackupCluster._data_command

    def command_with_stalled_status(cluster, index):
        command = original_command(cluster, index)
        if index != 0:
            return command
        # Keep every direct node address reachable even if leadership changes.
        # The first address is an alternate route with a stalled status handler.
        upstream[0] = cluster.metadata_admin_urls[0]
        endpoints = [
            f"http://127.0.0.1:{proxy.server_port}",
            *cluster.metadata_admin_urls,
        ]
        command = command[: command.index("--metadata-api")]
        for url in endpoints:
            command.extend(["--metadata-api", url])
        return command

    monkeypatch.setattr(
        backups.ThreeByThreeBackupCluster, "_data_command", command_with_stalled_status
    )
    try:
        yield enabled, observed
    finally:
        stopped.set()
        proxy.shutdown()
        proxy.server_close()
        thread.join()


@pytest.fixture
def stalled_status_backup_cluster(
    stalled_metadata_status, three_by_three_backup_cluster
):
    # Isolate mutation discovery from the fixture's initial bootstrap reads.
    stalled_metadata_status[0].set()
    return three_by_three_backup_cluster


def test_backup_restore_discovers_leader_past_stalled_status(
    stalled_status_backup_cluster, stalled_metadata_status
):
    backups.test_three_by_three_cluster_backup_restore_through_metadata_public_api(
        stalled_status_backup_cluster
    )
    assert stalled_metadata_status[1].is_set()
