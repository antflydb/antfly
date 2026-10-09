# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lite_state import State
import native_ingest


class Accepted:
    status = 202

    def __init__(self, lsn):
        self.data = json.dumps(
            {"state": "accepted", "wal_lsn": lsn, "searchable": False}
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read(self, *args):
        return self.data


def test_native_changes_restart_replays_exact_request_without_losing_newer_rows(
    tmp_path, monkeypatch
):
    state = State(tmp_path / "state.aflite")
    state.put({"id": 1, "type": "story", "title": "first", "time": 1})
    sent = []

    def lost(request, **kwargs):
        sent.append(request.data)
        raise OSError("lost successful acceptance response")

    monkeypatch.setattr(native_ingest, "urlopen", lost)
    with pytest.raises(OSError):
        native_ingest.publish(state, "http://antfly/db/v1", "hn")
    before = json.loads(sent[0])
    assert before["expected_checkpoint"] is None
    state.db.close()
    state = State(tmp_path / "state.aflite")
    state.put({"id": 1, "type": "story", "title": "newer", "time": 1})

    def success(request, **kwargs):
        sent.append(request.data)
        return Accepted(1 if len(sent) == 2 else 2)

    monkeypatch.setattr(native_ingest, "urlopen", success)
    assert native_ingest.publish(state, "http://antfly/db/v1", "hn")["wal_lsn"] == 1
    assert sent[1] == sent[0]
    assert list(state.db.entries("change:"))
    state.put({"id": 2, "type": "comment", "deleted": True, "time": 1})
    assert native_ingest.publish(state, "http://antfly/db/v1", "hn")["wal_lsn"] == 2
    next_batch = json.loads(sent[2])
    assert next_batch["expected_checkpoint"] == before["checkpoint"]
    assert next_batch["epoch"] == before["epoch"]
    assert next_batch["changes"][0]["row"]["title"] == "newer"
    assert next_batch["changes"][1] == {"op": "delete", "row": {"hn_id": 2}}
    assert not list(state.db.entries("change:"))
    state.db.close()


def test_native_changes_keeps_pending_transaction_when_endpoint_changes(
    tmp_path, monkeypatch
):
    state = State(tmp_path / "state.aflite")
    state.put({"id": 1, "type": "story", "time": 1})
    monkeypatch.setattr(
        native_ingest,
        "urlopen",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("timeout")),
    )
    with pytest.raises(OSError):
        native_ingest.publish(state, "http://antfly/db/v1", "hn")
    pending = state.get("native_changes_request")
    with pytest.raises(RuntimeError, match="different native table"):
        native_ingest.publish(state, "http://other/db/v1", "hn")
    assert state.get("native_changes_request") == pending
    state.db.close()


def test_native_cohorts_route_edits_and_deletes_by_retained_creation_time(
    tmp_path, monkeypatch
):
    state = State(tmp_path / "history.aflite")
    state.put({"id": 1, "type": "story", "title": "old", "time": 10})
    state.put({"id": 2, "type": "story", "title": "recent", "time": 20})
    sent = []

    def success(request, **kwargs):
        sent.append(json.loads(request.data))
        return Accepted(len(sent))

    monkeypatch.setattr(native_ingest, "urlopen", success)
    native_ingest.publish(state, "http://antfly/db/v1", "history", created_before=20)
    assert [change["row"]["hn_id"] for change in sent[0]["changes"]] == [1]
    assert not list(state.db.entries("change:"))
    state.put({"id": 1, "deleted": True})
    state.put({"id": 2, "points": 10})
    native_ingest.publish(state, "http://antfly/db/v1", "history", created_before=20)
    assert sent[1]["changes"] == [{"op": "delete", "row": {"hn_id": 1}}]
    with pytest.raises(RuntimeError, match="boundaries cannot change"):
        native_ingest.publish(
            state, "http://antfly/db/v1", "history", created_before=21
        )
    state.db.close()


def test_native_cohort_consumes_only_foreign_markers_without_a_false_checkpoint(
    tmp_path, monkeypatch
):
    state = State(tmp_path / "current.aflite")
    state.put({"id": 1, "type": "story", "time": 10})
    monkeypatch.setattr(
        native_ingest,
        "urlopen",
        lambda *args, **kwargs: pytest.fail(
            "foreign cohort must not submit a transaction"
        ),
    )
    assert (
        native_ingest.publish(state, "http://antfly/db/v1", "current", created_after=20)
        is None
    )
    assert not list(state.db.entries("change:"))
    assert not state.get("native_changes_checkpoint", "")
    state.put({"id": 2, "deleted": True})
    with pytest.raises(RuntimeError, match="original creation time"):
        native_ingest.publish(state, "http://antfly/db/v1", "current", created_after=20)
    assert list(state.db.entries("change:"))
    state.db.close()
