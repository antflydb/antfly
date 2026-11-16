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

import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
import qualify_gateway


def test_capture_resume_and_failed_transport_retain_evidence(tmp_path, monkeypatch):
    capture, resume = tmp_path / "capture.json", tmp_path / "resume.json"
    monkeypatch.setattr(qualify_gateway.time, "sleep", lambda _: None)

    def response(base, route, payload, authorization=None):
        if payload is None:
            return {"ok": True}, {"status": 200, "seconds": 0.01}
        row = 2 if payload.get("cursor") else 1
        return {
            "hits": {"hits": [{"_source": {"hn_id": row}}]},
            "cursor": "retained",
            "recent_since": 10 if payload["view"] == "recent" else None,
        }, {"status": 200, "seconds": 0.01}

    monkeypatch.setattr(qualify_gateway, "probe", response)
    base = [
        "qualify_gateway.py",
        "--gateway",
        "http://127.0.0.1:8080",
        "--runtime-image",
        "image@sha256:" + "a" * 64,
    ]
    monkeypatch.setattr(sys, "argv", base + ["--output", str(capture)])
    qualify_gateway.main()
    original = json.loads(capture.read_text())
    assert original["smoke_passed"] and not original["archive_qualified"]
    assert capture.stat().st_mode & 0o777 == 0o600
    monkeypatch.setattr(
        sys, "argv", base + ["--output", str(resume), "--resume-from", str(capture)]
    )
    qualify_gateway.main()
    result = json.loads(resume.read_text())
    assert result["smoke_passed"] and result["phase"] == "resume"
    assert result["checks"][3]["request"]["recent_since"] == 10

    def failure(*args):
        return None, {"transport_error": "TimeoutError", "seconds": 30}

    monkeypatch.setattr(qualify_gateway, "probe", failure)
    failed = tmp_path / "failed.json"
    monkeypatch.setattr(sys, "argv", base + ["--output", str(failed)])
    with pytest.raises(SystemExit):
        qualify_gateway.main()
    evidence = json.loads(failed.read_text())
    assert not evidence["smoke_passed"]
    assert evidence["checks"][0]["transport_error"] == "TimeoutError"


def test_search_first_keeps_slow_results_and_fails_latency_gate(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(qualify_gateway.time, "sleep", lambda _: None)

    def response(base, route, payload, authorization=None):
        calls.append(route)
        if payload is None:
            return {"ok": True}, {"status": 200, "seconds": 0.01}
        return {"hits": {"hits": [{"_source": {"hn_id": 1}}]}, "cursor": "retained"}, {
            "status": 200,
            "seconds": 12,
            "backend_seconds": 2,
            "outside_backend_seconds": 10,
        }

    monkeypatch.setattr(qualify_gateway, "probe", response)
    output = tmp_path / "slow.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "qualify_gateway.py",
            "--gateway",
            "http://127.0.0.1:8080",
            "--runtime-image",
            "image@sha256:" + "a" * 64,
            "--output",
            str(output),
            "--search-first",
            "--max-search-seconds",
            "5",
        ],
    )
    with pytest.raises(SystemExit) as error:
        qualify_gateway.main()
    assert error.value.code == 1
    record = json.loads(output.read_text())
    assert record["smoke_passed"] and not record["latency_passed"]
    assert not record["archive_qualified"]
    assert calls == ["/api/search"] * 3 + ["/health", "/ready"]
    assert record["checks"][0]["ids"] == ["1"]
    assert record["checks"][0]["outside_backend_seconds"] == 10


@pytest.mark.parametrize(
    "header",
    [
        "",
        "secret",
        "antfly_upstream;dur=NaN",
        "antfly_upstream;dur=-1",
        "antfly_upstream;dur=999999",
        "antfly_upstream;dur=1;desc=private",
    ],
)
def test_timing_ignores_invalid_or_unrelated_headers(header):
    assert qualify_gateway.timing_evidence({"Server-Timing": header}, 2) == {}


def test_timing_separates_upstream_from_end_to_end_duration():
    assert qualify_gateway.timing_evidence(None, 12) == {}
    assert qualify_gateway.timing_evidence(
        {"Server-Timing": "antfly_upstream;dur=2000.000"}, 12
    ) == {
        "backend_seconds": 2,
        "outside_backend_seconds": 10,
    }


def test_probe_includes_a_slow_connection_close(monkeypatch):
    clock = [0.0]

    class Response:
        status = 200
        headers = {"Server-Timing": "antfly_upstream;dur=10.000"}

        def __enter__(self):
            clock[0] += 0.02
            return self

        def read(self, _limit):
            clock[0] += 0.01
            return b'{"ok":true}'

        def __exit__(self, *_):
            clock[0] += 20

    monkeypatch.setattr(qualify_gateway, "open_request", lambda *a, **k: Response())
    monkeypatch.setattr(qualify_gateway.time, "monotonic", lambda: clock[0])
    result, evidence = qualify_gateway.probe("http://127.0.0.1", "/health", None)
    assert result == {"ok": True}
    assert evidence["seconds"] == pytest.approx(20.03)
    assert evidence["headers_seconds"] == pytest.approx(0.02)
    assert evidence["body_seconds"] == pytest.approx(0.01)
    assert evidence["close_seconds"] == pytest.approx(20)
    assert evidence["outside_backend_seconds"] == pytest.approx(20.02)
