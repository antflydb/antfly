# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from native_catalog import NativeCatalog
from lite_state import State
import ingest


def test_pending_commit_reopens_with_identical_request(tmp_path, monkeypatch):
    path = tmp_path / "ingestion.aflite"
    state = State(path)
    catalog = NativeCatalog(state, tmp_path.as_uri(), "http://localhost/db/v1", "hn")
    calls = []

    def lost_response(method, route, body):
        calls.append((method, route, body.copy()))
        raise TimeoutError("successful remote commit response was lost")

    monkeypatch.setattr(catalog, "_request", lost_response)
    with pytest.raises(TimeoutError):
        catalog._commit(
            "/lake/commits",
            {
                "requirements": [
                    {
                        "type": "assert-ref-snapshot-id",
                        "ref": "main",
                        "snapshot-id": None,
                    }
                ],
                "updates": [],
                "expected_metadata_location": "file:///old.json",
            },
        )
    intent = json.loads(state.get("native_catalog_request", ""))
    state.db.close()
    reopened = State(path)
    try:
        resumed = NativeCatalog(
            reopened, tmp_path.as_uri(), "http://localhost/db/v1", "hn"
        )
        monkeypatch.setattr(
            resumed,
            "_request",
            lambda method, route, body: (
                calls.append((method, route, body.copy()))
                or {"state": "lake_committed"}
            ),
        )
        assert resumed._finish_pending() == {"state": "lake_committed"}
        assert calls[0] == calls[1]
        assert intent["body"]["requirements"][0]["snapshot-id"] is None
        assert reopened.get("native_catalog_request", "") == ""
    finally:
        reopened.db.close()


def test_pending_commit_cannot_switch_native_table(tmp_path, monkeypatch):
    state = State(tmp_path / "ingestion.aflite")
    try:
        first = NativeCatalog(state, tmp_path.as_uri(), "http://localhost/db/v1", "hn")
        monkeypatch.setattr(
            first, "_request", lambda *args: (_ for _ in ()).throw(TimeoutError())
        )
        with pytest.raises(TimeoutError):
            first._commit("/lake/commits", {"updates": [], "requirements": []})
        other = NativeCatalog(
            state, tmp_path.as_uri(), "http://localhost/db/v1", "other"
        )
        with pytest.raises(RuntimeError, match="different native table"):
            other._finish_pending()
        assert state.get("native_catalog_request", "")
    finally:
        state.db.close()


def test_native_checkpoint_uses_catalog_authority(monkeypatch):
    monkeypatch.setattr(
        NativeCatalog,
        "_request",
        lambda *args: {
            "metadata_location": "gs://archive/metadata/immutable.json",
            "metadata": {"table-uuid": "incarnation"},
        },
    )
    monkeypatch.setattr(
        ingest.Publisher, "read", lambda *args: pytest.fail("legacy pointer consulted")
    )
    assert ingest.archive_authority(
        "gs://archive", native_endpoint="http://localhost/db/v1"
    ) == (
        b"gs://archive/metadata/immutable.json",
        "incarnation",
    )


def test_pending_commit_survives_permission_loss(tmp_path, monkeypatch):
    from urllib.error import HTTPError
    from io import BytesIO

    state = State(tmp_path / "ingestion.aflite")
    try:
        catalog = NativeCatalog(
            state, tmp_path.as_uri(), "http://localhost/db/v1", "hn"
        )

        def denied(*args):
            raise HTTPError(
                "http://localhost",
                403,
                "Forbidden",
                {},
                BytesIO(b'{"error":"Forbidden"}'),
            )

        monkeypatch.setattr(catalog, "_request", denied)
        with pytest.raises(HTTPError):
            catalog._commit("/lake/commits", {"updates": [], "requirements": []})
        assert state.get("native_catalog_request", "")
    finally:
        state.db.close()
