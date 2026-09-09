# Copyright 2026 Antfly, Inc.
#
# Licensed under the Elastic License 2.0 (ELv2); you may not use this file
# except in compliance with the Elastic License 2.0. You may obtain a copy of
# the Elastic License 2.0 at
#
#     https://www.antfly.io/licensing/ELv2-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Elastic License 2.0 for the specific language governing permissions
# and limitations.

"""Replication startup must complete before expecting synchronous write success."""

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests
import test_standby as standby_tests

ha_cluster = standby_tests.ha_cluster
pytestmark = pytest.mark.ha_standby


def test_standby_waits_for_delayed_first_replication(ha_cluster):
    upstream = ha_cluster.primary.url
    delayed = threading.Event()

    class Proxy(BaseHTTPRequestHandler):
        def forward(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if (
                self.path == "/internal/v1/ha/replication/start"
                and not delayed.is_set()
            ):
                delayed.set()
                # Longer than the runtime's two-second synchronous ACK budget.
                # HTTP readiness and restored checkpoint LSNs are already
                # available while this first upstream exchange is pending.
                time.sleep(3.0)
            response = requests.request(
                self.command,
                upstream + self.path,
                data=body,
                headers={
                    "Content-Type": self.headers.get(
                        "Content-Type", "application/json"
                    ),
                    "Authorization": self.headers.get("Authorization", ""),
                    "Connection": "close",
                },
                timeout=10,
            )
            self.send_response(response.status_code)
            self.send_header(
                "Content-Type", response.headers.get("Content-Type", "application/json")
            )
            self.send_header("Content-Length", str(len(response.content)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(response.content)

        do_GET = forward
        do_POST = forward

        def log_message(self, *args):
            pass

    proxy = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()
    ha_cluster.standby.upstream_url = f"http://127.0.0.1:{proxy.server_port}"
    try:
        standby_tests.test_standby_streams_public_writes_restarts_and_rejects_writes(
            ha_cluster
        )
    finally:
        proxy.shutdown()
        proxy.server_close()
        thread.join()
    assert delayed.is_set()
