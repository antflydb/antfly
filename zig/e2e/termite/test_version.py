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

"""Tests for /ml/v1/version endpoint."""


def test_version_returns_runtime(api):
    resp = api.version()
    assert "version" in resp
    assert isinstance(resp["version"], str)
    assert len(resp["version"]) > 0
    assert "git_commit" in resp
    assert isinstance(resp["git_commit"], str)
    assert "build_time" in resp
    assert isinstance(resp["build_time"], str)
    assert "go_version" in resp
    assert isinstance(resp["go_version"], str)
    assert "allow_downloads" in resp
    assert isinstance(resp["allow_downloads"], bool)
    assert "runtime" in resp
    assert isinstance(resp["runtime"], str)
    assert len(resp["runtime"]) > 0
    assert "backends" in resp
    assert isinstance(resp["backends"], dict)
    for name in ("native", "onnx", "mlx", "wasm"):
        assert name in resp["backends"]
        assert isinstance(resp["backends"][name], bool)
