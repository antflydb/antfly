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

"""Browser privacy controls use the host's default CORS header contract."""

import json
import os
from pathlib import Path

import pytest
import requests

from conftest import (
    DEFAULT_ANTFLY_BIN,
    StandaloneAntflyServer,
    find_free_port,
    resolve_binary_path,
)


@pytest.mark.e2e_resource("antfly_process")
def test_privacy_default_cors_supports_conditional_browser_saves(tmp_path):
    binary = resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    if not Path(binary).is_file():
        pytest.skip("Public API binary not found")
    origin = "https://privacy.example"
    config = tmp_path / "cors.json"
    config.write_text(json.dumps({"cors": {"allowed_origins": [origin]}}))
    server = StandaloneAntflyServer(
        binary, "127.0.0.1", find_free_port(), config_path=config
    )
    try:
        base = server.api_url
        headers = {"Origin": origin}
        created = requests.post(base + "/tables/browser_privacy", json={}, timeout=30)
        created.raise_for_status()
        path = base + "/tables/browser_privacy/privacy"
        preflight = requests.options(
            path,
            headers={
                **headers,
                "Access-Control-Request-Method": "PUT",
                "Access-Control-Request-Headers": "content-type, if-match, authorization",
            },
            timeout=30,
        )
        assert preflight.status_code == 204, preflight.text
        allowed = {
            item.strip().lower()
            for item in preflight.headers["Access-Control-Allow-Headers"].split(",")
        }
        assert {"content-type", "if-match", "authorization"} <= allowed
        policy = {"mode": "monitor", "fields": ["body"]}
        saved = requests.put(path, json=policy, headers=headers, timeout=30)
        assert saved.status_code == 202, saved.text
        etag = saved.headers["ETag"]
        policy["fields"] = ["body", "subject"]
        replaced = requests.put(
            path, json=policy, headers={**headers, "If-Match": etag}, timeout=30
        )
        assert replaced.status_code == 202, replaced.text
        assert (
            replaced.json()["resource_version"] == saved.json()["resource_version"] + 1
        )
        current = requests.get(path, headers=headers, timeout=30)
        assert current.status_code == 200, current.text
        assert current.headers["ETag"] == replaced.headers["ETag"]
        for response in (saved, replaced, current):
            assert response.headers["Access-Control-Allow-Origin"] == origin
            exposed = {
                item.strip().lower()
                for item in response.headers["Access-Control-Expose-Headers"].split(",")
            }
            assert {"etag", "retry-after", "x-antfly-raft-mutation-outcome"} <= exposed
        rejected = requests.options(
            path,
            headers={
                "Origin": "https://denied.example",
                "Access-Control-Request-Method": "PUT",
                "Access-Control-Request-Headers": "content-type, if-match",
            },
            timeout=30,
        )
        assert rejected.status_code == 403
    finally:
        server.stop()
