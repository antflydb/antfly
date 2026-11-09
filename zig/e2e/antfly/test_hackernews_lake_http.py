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

"""Enroll the assembled HN lake hierarchy regression in full native E2E CI."""

import importlib.util
import os
from pathlib import Path
import sys

import pytest

from conftest import DEFAULT_ANTFLY_BIN, resolve_binary_path


pytestmark = [
    pytest.mark.iceberg_integration,
    pytest.mark.fresh_antfly_process,
    pytest.mark.e2e_resource("antfly_process"),
]


def test_hackernews_managed_lake_pending_hierarchy_and_restart(tmp_path, monkeypatch):
    # Optional lake dependencies are imported only after marker selection. Full
    # lake CI installs them; missing dependencies or artifacts must fail rather
    # than silently skip this assembled-server regression.
    binary = Path(
        resolve_binary_path(os.environ.get("ANTFLY_BIN", str(DEFAULT_ANTFLY_BIN)))
    )
    assert binary.is_file(), f"Native E2E binary missing: {binary}"
    library_name = "libantfly.dylib" if sys.platform == "darwin" else "libantfly.so"
    library = Path(
        os.environ.get(
            "ANTFLY_LIBRARY", str(binary.parent.parent / "lib" / library_name)
        )
    ).resolve()
    assert library.is_file(), f"Matching native E2E Lite library missing: {library}"
    monkeypatch.setenv("ANTFLY_NATIVE_BINARY", str(binary))
    monkeypatch.setenv("ANTFLY_LIBRARY", str(library))

    example = Path(__file__).resolve().parents[3] / "examples" / "hackernews"
    monkeypatch.syspath_prepend(str(example))
    name = "_antfly_hackernews_lake_http_ci"
    spec = importlib.util.spec_from_file_location(
        name, example / "tests" / "test_catalog_http.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    # One local managed case covers archived and pending enrichment members,
    # text projections, provenance/revisions, publication and restart. The
    # independent HTTP object/catalog oracles require no cloud credentials.
    module.test_native_catalog_file_commit_read_and_restart(
        tmp_path, "managed", "overlay"
    )
