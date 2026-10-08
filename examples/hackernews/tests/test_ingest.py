# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ingest
from lite_state import number


def test_run_batches_publication_independently_of_polling(tmp_path, monkeypatch):
    ticks = iter([0, 0, 60, 3600, 3600])
    polls, publications = [], []
    monkeypatch.setattr(ingest.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(ingest.State, "poll", lambda *args: polls.append(True) or 1)
    monkeypatch.setattr(ingest, "publish", lambda *args: publications.append(True))

    def sleep(_):
        if len(polls) == 3:
            raise InterruptedError("end test run")

    monkeypatch.setattr(ingest.time, "sleep", sleep)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ingest.py",
            "--state",
            str(tmp_path),
            "--warehouse",
            tmp_path.as_uri(),
            "--interval",
            "60",
            "--publish-interval",
            "3600",
            "run",
        ],
    )
    with pytest.raises(InterruptedError, match="end test run"):
        ingest.main()
    assert len(polls) == 3
    assert len(publications) == 2


def test_backfill_replay_edit_delete_and_late_parent(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    source = tmp_path / "source.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "hn_id": 12,
                    "parent_id": 11,
                    "item_type": "comment",
                    "created_at": 1704067200,
                    "text_html": "hello &amp; world",
                },
                {
                    "hn_id": 11,
                    "parent_id": 10,
                    "item_type": "comment",
                    "created_at": 1704067200,
                    "text_html": "parent",
                },
            ]
        ),
        source,
    )
    state = ingest.State(tmp_path / "ingestion.aflite")
    state.backfill([source], batch_size=1)
    state.backfill([source], batch_size=2)
    assert len(list(state.items())) == 2
    assert state.missing_parents(100) == [10]
    assert int(state.get("maxitem")) == 12
    with state.transaction():
        state.put({"id": 10, "type": "story", "time": 1701388800, "title": "root"})
    state.resolve_roots()
    rows = [
        row
        for batch in state.batches(["2024-01"], ingest.arrow_schema(), 1)
        for row in batch.to_pylist()
    ]
    assert [r["root_story_id"] for r in rows] == [10, 10]
    assert rows[1]["body"] == "hello & world"
    with state.transaction():
        state.put({"id": 12, "text": "edited", "score": 20})
        state.put({"id": 11, "deleted": True})
    state.resolve_roots()
    state.db.close()
    state = ingest.State(tmp_path / "ingestion.aflite")
    rows = [
        row
        for batch in state.batches(["2024-01"], ingest.arrow_schema())
        for row in batch.to_pylist()
    ]
    assert [(r["hn_id"], r["body"], r["points"], r["root_story_id"]) for r in rows] == [
        (12, "edited", 20, 10)
    ]
    state.db.close()


def test_poll_persists_null_retries_and_reconciles_missed_updates(tmp_path):
    state = ingest.State(tmp_path / "ingestion.aflite")
    with state.transaction():
        state.put({"id": 1, "type": "story", "time": 1704067200, "title": "before"})
        state.set("maxitem", 1)
    responses = {
        "maxitem": 3,
        "updates": {"items": []},
        "item/1": {"id": 1, "deleted": True},
        "item/2": None,
        "item/3": {"id": 3, "type": "story", "time": 1704067200},
    }
    assert state.poll(responses.__getitem__, batch_size=10) == 2
    assert state.get("maxitem") == "3"
    assert [r["id"] for _, r in state.db.entries("pending:")] == [2]
    state.db.close()
    state = ingest.State(tmp_path / "ingestion.aflite")
    assert state.item(1)["payload"]["deleted"]
    with state.transaction():
        pending = state.db.get("pending:" + number(2))
        state.db.delete(state.schedule_key(pending))
        pending["retry_at"] = 0
        state.db.set("pending:" + number(2), pending)
        state.db.set(state.schedule_key(pending), 2)
    responses["item/2"] = {"id": 2, "type": "comment", "parent": 1, "time": 1704067200}
    state.poll(responses.__getitem__, batch_size=10)
    assert list(state.db.entries("pending:")) == []
    assert state.item(2)["root"] == 1
    state.db.close()


def test_ancestry_cycles_are_unresolved(tmp_path):
    state = ingest.State(tmp_path / "ingestion.aflite")
    with state.transaction():
        state.put({"id": 1, "type": "comment", "parent": 2})
        state.put({"id": 2, "type": "comment", "parent": 1})
    state.resolve_roots()
    assert [r["root"] for r in state.items()] == [None, None]
    state.db.close()


def test_iceberg_partition_replacement_and_publication_recovery(tmp_path, monkeypatch):

    state = ingest.State(tmp_path / "ingestion.aflite")
    warehouse = (tmp_path / "warehouse").as_uri()
    with state.transaction():
        state.put({"id": 1, "type": "story", "time": 1701388800, "title": "December"})
        state.put({"id": 2, "type": "story", "time": 1704067200, "title": "January"})
    first = ingest.publish(state, tmp_path, warehouse)
    assert first["version"] == 1
    original = ingest.Publisher.put
    failed = False

    def fail_once(self, name, body, expected):
        nonlocal failed
        if name.endswith("version-hint.text") and not failed:
            failed = True
            raise OSError("pointer write failed")
        return original(self, name, body, expected)

    monkeypatch.setattr(ingest.Publisher, "put", fail_once)
    with state.transaction():
        state.put({"id": 2, "deleted": True})
        state.put(
            {"id": 3, "type": "story", "time": 1704067200, "title": "replacement"}
        )
    with pytest.raises(OSError):
        ingest.publish(state, tmp_path, warehouse)
    assert state.get("publication", "")
    assert (tmp_path / "warehouse/metadata/version-hint.text").read_text() == "1\n"
    state.db.close()
    state = ingest.State(tmp_path / "ingestion.aflite")
    second = ingest.publish(state, tmp_path, warehouse)
    assert second["version"] == 2
    assert not state.get("publication", "")
    catalog = ingest.HackernewsCatalog(state, warehouse)
    table = catalog.load_table("hackernews.items")
    rows = table.scan().to_arrow().to_pylist()
    assert sorted((r["hn_id"], r["title"]) for r in rows) == [
        (1, "December"),
        (3, "replacement"),
    ]
    assert ingest.publish(state, tmp_path, warehouse) is None
    publisher = ingest.Publisher(warehouse)
    content = (tmp_path / "warehouse/metadata/v2.metadata.json").read_bytes()
    # Replay a successful pointer write whose response was lost.
    assert publisher.commit(content, "0") == 2
    with pytest.raises(RuntimeError, match="another publisher"):
        publisher.commit(b"other writer", "0")
    state.db.close()


def test_consistent_checkpoint_restore_and_reject_archive_rollback(tmp_path):
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    state = ingest.State(state_dir / "ingestion.aflite")
    warehouse = (tmp_path / "warehouse").as_uri()
    backups = (tmp_path / "backups").as_uri()
    with state.transaction():
        state.put({"id": 1, "type": "story", "title": "durable", "time": 1704067200})
    ingest.publish(state, state_dir, warehouse)
    ingest.backup_state(state, state_dir, backups, warehouse)
    restored = tmp_path / "restored"
    restored.mkdir()
    ingest.restore_state(restored, backups, warehouse)
    recovered = ingest.State(restored / "ingestion.aflite")
    assert [r["id"] for r in recovered.items()] == [1]
    assert recovered.get("published_version") == "1"
    recovered.db.close()
    with pytest.raises(RuntimeError, match="empty state"):
        ingest.restore_state(restored, backups, warehouse)
    with state.transaction():
        state.put({"id": 2, "type": "story", "time": 1704067200})
    ingest.publish(state, state_dir, warehouse)
    unsafe = tmp_path / "unsafe"
    unsafe.mkdir()
    with pytest.raises(RuntimeError, match="archive advanced"):
        ingest.restore_state(unsafe, backups, warehouse)
    state.db.close()


def test_live_record_clears_prior_moderation_flag(tmp_path):
    state = ingest.State(tmp_path / "ingestion.aflite")
    with state.transaction():
        state.put({"id": 1, "type": "story", "dead": True, "time": 1704067200})
        state.put({"id": 1, "type": "story", "title": "restored", "time": 1704067200})
    rows = [
        r
        for b in state.batches(["2024-01"], ingest.arrow_schema())
        for r in b.to_pylist()
    ]
    assert rows[0]["title"] == "restored"
    state.db.close()


def test_live_items_and_archive_reconciliation_share_capacity(tmp_path):
    state = ingest.State(tmp_path / "ingestion.aflite")
    with state.transaction():
        for item_id in range(1, 21):
            state.put({"id": item_id, "type": "story", "time": 1704067200})
        state.set("maxitem", 20)
    visited = []

    def fetch(path):
        if path == "maxitem":
            return 30
        if path == "updates":
            return {"items": []}
        item_id = int(path.split("/")[1])
        visited.append(item_id)
        return {"id": item_id, "type": "story", "time": 1704067200}

    state.poll(fetch, batch_size=10)
    assert len(visited) == 10
    assert any(item_id > 20 for item_id in visited), "sweep starved new items"
    assert any(item_id <= 20 for item_id in visited), "catch-up starved reconciliation"
    state.db.close()


def test_repeated_updates_do_not_starve_pending_new_items(tmp_path):
    state = ingest.State(tmp_path / "ingestion.aflite")
    with state.transaction():
        for item_id in range(1, 21):
            state.put({"id": item_id, "type": "story", "time": 1704067200})
        state.set("maxitem", 20)
    visited = set()

    def fetch(path):
        if path == "maxitem":
            return 30
        if path == "updates":
            return {"items": list(range(1, 21))}
        item_id = int(path.split("/")[1])
        visited.add(item_id)
        return {"id": item_id, "type": "story", "time": 1704067200}

    for _ in range(10):
        state.poll(fetch, batch_size=10)
    assert set(range(1, 31)).issubset(visited)
    state.db.close()


def test_lite_batch_rolls_back_items_and_progress_together(tmp_path):
    state = ingest.State(tmp_path / "ingestion.aflite")
    with pytest.raises(ValueError):
        with state.transaction():
            state.put({"id": 1, "type": "story", "time": 1704067200})
            state.set("maxitem", 1)
            raise ValueError("interrupted before commit")
    state.db.close()
    state = ingest.State(tmp_path / "ingestion.aflite")
    assert state.item(1) is None
    assert not state.get("maxitem", "")
    assert state.dirty_months() == []
    state.db.close()


def test_lite_ranges_page_and_merge_uncommitted_changes(tmp_path):
    state = ingest.State(tmp_path / "ingestion.aflite")
    with state.transaction():
        for item_id in range(1, 1101):
            state.db.set("test:" + number(item_id), item_id)
    with state.transaction():
        state.db.delete("test:" + number(64))
        state.db.set("test:" + number(1101), 1101)
        state.db.set("test:" + number(65), "replacement")
        rows = list(state.db.entries("test:", page_size=64))
        assert len(rows) == 1100
        assert [k for k, _ in rows] == sorted(k for k, _ in rows)
        assert rows[63][1] == "replacement"
        assert rows[-1][1] == 1101
    state.db.close()


def test_lite_reparenting_invalidates_descendants_and_preserves_cycles(tmp_path):
    state = ingest.State(tmp_path / "ingestion.aflite")
    with state.transaction():
        for item in [
            dict(id=1, type="story"),
            dict(id=2, type="story"),
            dict(id=3, type="comment", parent=1),
            dict(id=4, type="comment", parent=3),
        ]:
            state.put(item | {"time": 1704067200})
    state.resolve_roots()
    assert state.item(4)["root"] == 1
    with state.transaction():
        state.put({"id": 3, "parent": 2})
    state.resolve_roots()
    assert state.item(3)["root"] == state.item(4)["root"] == 2
    with state.transaction():
        state.put({"id": 3, "parent": 4})
    state.resolve_roots()
    assert state.item(3)["root"] is None and state.item(4)["root"] is None
    state.db.close()
