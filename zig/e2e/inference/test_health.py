# Copyright 2026 Antfly, Inc.
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

"""Tests for operational health endpoints."""

import requests


def test_healthz(base_url):
    r = requests.get(f"{base_url}/healthz", timeout=5)
    r.raise_for_status()
    assert r.json() == {"status": "ok"}


def test_readyz(base_url):
    r = requests.get(f"{base_url}/readyz", timeout=5)
    assert r.status_code in (200, 503), r.text
    payload = r.json()
    assert payload.get("status") in ("ready", "not_ready"), payload
    models = payload.get("models")
    assert isinstance(models, dict), payload
    for key in (
        "embedders",
        "rerankers",
        "chunkers",
        "generators",
        "recognizers",
        "classifiers",
        "rewriters",
        "readers",
        "transcribers",
        "extractors",
    ):
        assert isinstance(models.get(key), int), payload
