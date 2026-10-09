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
