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


def test_standby_waits_for_delayed_first_replication(ha_cluster, monkeypatch):
    _exercise_delayed_replication(ha_cluster, monkeypatch, withhold_write_ack=False)


def test_standby_reconciles_withheld_write_ack_without_replay(ha_cluster, monkeypatch):
    _exercise_delayed_replication(ha_cluster, monkeypatch, withhold_write_ack=True)


def _exercise_delayed_replication(ha_cluster, monkeypatch, *, withhold_write_ack):
    upstream = ha_cluster.primary.url
    delayed = threading.Event()
    hold_ack = threading.Event()
    ack_withheld = threading.Event()
    release_ack = threading.Event()
    original_write = ha_cluster.primary.batch_write_response

    def write_once(table, inserts):
        if not withhold_write_ack or "doc:first" not in inserts:
            return original_write(table, inserts)
        hold_ack.set()
        try:
            response = original_write(table, inserts)
            assert ack_withheld.is_set()
            assert response.status_code == 503
            assert response.text == (
                "write committed locally; standby durability acknowledgment pending"
            )
            return response
        finally:
            hold_ack.clear()
            release_ack.set()

    monkeypatch.setattr(ha_cluster.primary, "batch_write_response", write_once)

    class Proxy(BaseHTTPRequestHandler):
        def forward(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if (
                self.path
                in (
                    "/internal/v1/standby/replication/start",
                    "/internal/v1/ha/replication/start",
                )
                and not delayed.is_set()
            ):
                delayed.set()
                # Longer than the runtime's two-second synchronous ACK budget.
                # HTTP readiness and restored checkpoint LSNs are already
                # available while this first upstream exchange is pending.
                time.sleep(3.0)
            if self.path.endswith("/replication/status") and hold_ack.is_set():
                ack_withheld.set()
                assert release_ack.wait(timeout=15), (
                    "write response never released ACK gate"
                )
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
        release_ack.set()
        proxy.shutdown()
        proxy.server_close()
        thread.join()
    assert delayed.is_set()
    assert ack_withheld.is_set() == withhold_write_ack


@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize("acknowledged", [False, True])
def test_startup_write_reconciles_without_resubmission(
    monkeypatch, pending, acknowledged
):
    from types import SimpleNamespace

    response = standby_tests._test_response(
        503 if pending else 200,
        b"write committed locally; standby durability acknowledgment pending"
        if pending
        else b"{}",
    )
    sends = []
    observations = []
    node = SimpleNamespace(
        batch_write_response=lambda *args: sends.append(args) or response,
        _check=lambda result: result.raise_for_status(),
    )
    cluster = SimpleNamespace(primary=node)
    monkeypatch.setattr(standby_tests, "_primary_lsn", lambda _: 17)

    def applied(owner, lsn, *, timeout_s):
        assert owner is cluster and lsn == 17 and 0 < timeout_s <= 20
        observations.append("standby")
        return {"applied_lsn": 17}

    def ack(owner, slot, lsn, *, timeout_s):
        assert owner is cluster and slot == "standby-a" and lsn == 17
        assert 0 < timeout_s <= 20
        observations.append("primary")
        if not acknowledged:
            raise AssertionError("ACK did not arrive")

    monkeypatch.setattr(standby_tests, "_wait_for_standby_applied", applied)
    monkeypatch.setattr(standby_tests, "_wait_for_primary_slot_applied", ack)
    if acknowledged:
        assert standby_tests._write_and_wait_for_standby_durability(
            cluster, "docs", {}
        ) == (17, {"applied_lsn": 17})
    else:
        with pytest.raises(AssertionError, match="ACK did not arrive"):
            standby_tests._write_and_wait_for_standby_durability(cluster, "docs", {})
    assert sends == [("docs", {})]
    assert observations == ["standby", "primary"]


def test_startup_write_rejects_other_unavailable_outcomes():
    from types import SimpleNamespace

    response = standby_tests._test_response(503, b"write unavailable")
    sends = []
    cluster = SimpleNamespace(
        primary=SimpleNamespace(
            batch_write_response=lambda *args: sends.append(args) or response,
            _check=lambda result: result.raise_for_status(),
        )
    )
    with pytest.raises(requests.HTTPError):
        standby_tests._write_and_wait_for_standby_durability(cluster, "docs", {})
    assert sends == [("docs", {})]
