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

"""The qualification must reject incomplete commits and invalid query evidence."""

import os
from types import SimpleNamespace

import pytest

from bench_descriptor_pressure import (
    call,
    corpus,
    descriptor_reader,
    distribution,
    query,
)


def test_latency_percentiles_use_observed_tail_and_reject_empty_samples():
    values = [i / 1000 for i in range(1, 101)]
    result = distribution(reversed(values))
    assert result["p95_ms"] == 95
    assert result["p99_ms"] == 99
    assert result["max_ms"] == 100
    assert result["count"] == 100
    with pytest.raises(ValueError):
        distribution([])


def test_synthetic_rows_are_reproducible_varied_and_have_eight_permission_bits():
    first, second = corpus(None, 256), corpus(None, 256)
    assert first(0) == second(0)
    assert first(0) != first(1)
    assert len(first(0)[0]) == 768
    assert all(0 <= first(i)[1] < 256 for i in range(256))


def test_http_failure_is_not_retried_or_counted_as_a_commit():
    class Session:
        calls = 0

        def request(self, *_args, **_kwargs):
            self.calls += 1
            return SimpleNamespace(
                ok=False, status_code=503, text="storage_descriptor_pressure"
            )

    session = Session()
    with pytest.raises(RuntimeError, match="503.*storage_descriptor_pressure"):
        call(session, "http://unused", "POST", "/batch", {})
    assert session.calls == 1


def test_query_requires_the_ingested_self_match():
    class Session:
        def request(self, *_args, **_kwargs):
            return SimpleNamespace(
                ok=True,
                json=lambda: {"responses": [{"hits": {"hits": [{"_id": "wrong"}]}}]},
            )

    with pytest.raises(RuntimeError, match="self-query"):
        query(Session(), "http://unused", [1, 0])


def test_process_descriptor_sampler_counts_live_descriptors():
    read = descriptor_reader(os.getpid())
    assert read() >= 3


def test_linux_mapping_rss_includes_deleted_vectors_and_excludes_other_maps(
    tmp_path, monkeypatch
):
    import bench_descriptor_pressure as bench

    smaps = tmp_path / "smaps"
    smaps.write_text(
        "1000-2000 r--s 00000000 00:00 1 /blocks/a.afvb\nRss: 12 kB\n"
        "2000-3000 r--s 00000000 00:00 2 /blocks/b.afvb (deleted)\nRss: 8 kB\n"
        "3000-4000 rw-p 00000000 00:00 0 [heap]\nRss: 100 kB\n"
    )
    monkeypatch.setattr(bench.sys, "platform", "linux")
    monkeypatch.setattr(bench, "Path", lambda _: smaps)
    assert bench.vector_mapping_rss(123) == 20 * 1024
