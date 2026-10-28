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
