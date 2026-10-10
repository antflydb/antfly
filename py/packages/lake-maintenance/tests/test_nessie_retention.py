# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import pytest

from antfly_lake_maintenance.controller import Controller
from antfly_lake_maintenance.provider import Provider
from antfly_lake_maintenance.store import Store, Unavailable, encode


def test_nessie_retention_keeps_unchanged_heads_tags_and_recent_content():
    provider = object.__new__(Provider)
    provider.config = {"provider": "nessie"}

    def pages(path, field, native=False):
        assert native
        if path == "/trees":
            return iter(
                [{"name": "main", "hash": "a"}, {"name": "release", "hash": "b"}]
            )
        if field == "entries":
            uri = "unchanged-head" if "main" in path else "tag-head"
            return iter(
                [{"content": {"type": "ICEBERG_TABLE", "metadataLocation": uri}}]
            )
        return iter(
            [
                {
                    "commitMeta": {"commitTime": "2026-10-09T12:00:00Z"},
                    "operations": [
                        {
                            "content": {
                                "type": "ICEBERG_TABLE",
                                "metadataLocation": "recent",
                            }
                        }
                    ],
                },
                {
                    "commitMeta": {"commitTime": "2020-01-01T00:00:00Z"},
                    "operations": [
                        {
                            "content": {
                                "type": "ICEBERG_TABLE",
                                "metadataLocation": "old",
                            }
                        }
                    ],
                },
            ]
        )

    provider._pages = pages
    roots = set(provider.all_metadata(history_floor_ms=1_700_000_000_000))
    assert roots == {"unchanged-head", "tag-head", "recent"}
    assert "old" in set(provider.all_metadata())


def test_missing_nessie_commit_timestamp_fails_closed():
    provider = object.__new__(Provider)
    provider.config = {"provider": "nessie"}
    provider._pages = lambda path, field, **kwargs: iter(
        [{"name": "main", "hash": "a"}]
        if field == "references"
        else [{}]
        if field == "logEntries"
        else []
    )
    with pytest.raises(Unavailable):
        list(provider.all_metadata(history_floor_ms=1))


def test_persisted_history_policy_blocks_native_historical_reads_after_restart(
    tmp_path,
):
    controller = object.__new__(Controller)
    controller.config = {"authority_uri": tmp_path.as_uri() + "/"}
    controller.store = Store()
    controller.allow_native_read("/trees/main@hash/history?fetch=ALL")
    controller.store.immutable(
        controller.key("nessie-history-policy.json"),
        encode({"protocol": 1, "retention_ms": 600_000}),
    )
    controller.store = Store()
    for path in (
        "/trees/main@hash/history",
        "/trees/main/entries",
        "/trees/@hash/contents",
        "/trees/main/diff",
    ):
        with pytest.raises(PermissionError):
            controller.allow_native_read(path)
    controller.allow_native_read("/trees?page-token=next")
    controller.allow_native_read("/config")
