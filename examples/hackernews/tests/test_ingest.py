# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ingest


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
    state = ingest.State(tmp_path / "items.sqlite")
    state.backfill([source], batch_size=1)
    state.backfill([source], batch_size=2)
    assert state.db.execute("SELECT count(*) FROM items").fetchone()[0] == 2
    assert state.missing_parents(100) == [10]
    assert int(state.get("maxitem")) == 12
    with state.db:
        state.put({"id": 10, "type": "story", "time": 1701388800, "title": "root"})
        state.resolve_roots()
    rows = [
        row
        for batch in state.batches(["2024-01"], ingest.arrow_schema(), 1)
        for row in batch.to_pylist()
    ]
    assert [r["root_story_id"] for r in rows] == [10, 10]
    assert rows[1]["body"] == "hello & world"
    with state.db:
        state.put({"id": 12, "text": "edited", "score": 20})
        state.put({"id": 11, "deleted": True})
        state.resolve_roots()
    state.db.close()
    state = ingest.State(tmp_path / "items.sqlite")
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
    state = ingest.State(tmp_path / "items.sqlite")
    with state.db:
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
    assert state.db.execute("SELECT id FROM pending").fetchall() == [(2,)]
    state.db.close()
    state = ingest.State(tmp_path / "items.sqlite")
    assert json.loads(
        state.db.execute("SELECT payload FROM items WHERE id=1").fetchone()[0]
    )["deleted"]
    with state.db:
        state.db.execute("UPDATE pending SET retry_at=0")
    responses["item/2"] = {"id": 2, "type": "comment", "parent": 1, "time": 1704067200}
    state.poll(responses.__getitem__, batch_size=10)
    assert state.db.execute("SELECT id FROM pending").fetchall() == []
    assert state.db.execute("SELECT root FROM items WHERE id=2").fetchone()[0] == 1
    state.db.close()


def test_ancestry_cycles_are_unresolved(tmp_path):
    state = ingest.State(tmp_path / "items.sqlite")
    with state.db:
        state.put({"id": 1, "type": "comment", "parent": 2})
        state.put({"id": 2, "type": "comment", "parent": 1})
        state.resolve_roots()
    assert state.db.execute("SELECT root FROM items").fetchall() == [(None,), (None,)]
    state.db.close()


def test_iceberg_partition_replacement_and_publication_recovery(tmp_path, monkeypatch):
    from pyiceberg.catalog.sql import SqlCatalog

    state = ingest.State(tmp_path / "items.sqlite")
    warehouse = (tmp_path / "warehouse").as_uri()
    with state.db:
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
    with state.db:
        state.put({"id": 2, "deleted": True})
        state.put(
            {"id": 3, "type": "story", "time": 1704067200, "title": "replacement"}
        )
    with pytest.raises(OSError):
        ingest.publish(state, tmp_path, warehouse)
    assert state.get("publication", "")
    assert (tmp_path / "warehouse/metadata/version-hint.text").read_text() == "1\n"
    state.db.close()
    state = ingest.State(tmp_path / "items.sqlite")
    second = ingest.publish(state, tmp_path, warehouse)
    assert second["version"] == 2
    assert not state.get("publication", "")
    catalog = SqlCatalog(
        "hn", uri=f"sqlite:///{tmp_path / 'catalog.sqlite'}", warehouse=warehouse
    )
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
    state = ingest.State(state_dir / "items.sqlite")
    warehouse = (tmp_path / "warehouse").as_uri()
    backups = (tmp_path / "backups").as_uri()
    with state.db:
        state.put({"id": 1, "type": "story", "title": "durable", "time": 1704067200})
    ingest.publish(state, state_dir, warehouse)
    ingest.backup_state(state, state_dir, backups, warehouse)
    restored = tmp_path / "restored"
    restored.mkdir()
    ingest.restore_state(restored, backups, warehouse)
    recovered = ingest.State(restored / "items.sqlite")
    assert recovered.db.execute("SELECT id FROM items").fetchall() == [(1,)]
    assert recovered.get("published_version") == "1"
    recovered.db.close()
    with pytest.raises(RuntimeError, match="empty state"):
        ingest.restore_state(restored, backups, warehouse)
    with state.db:
        state.put({"id": 2, "type": "story", "time": 1704067200})
    ingest.publish(state, state_dir, warehouse)
    unsafe = tmp_path / "unsafe"
    unsafe.mkdir()
    with pytest.raises(RuntimeError, match="archive advanced"):
        ingest.restore_state(unsafe, backups, warehouse)
    state.db.close()


def test_live_record_clears_prior_moderation_flag(tmp_path):
    state = ingest.State(tmp_path / "items.sqlite")
    with state.db:
        state.put({"id": 1, "type": "story", "dead": True, "time": 1704067200})
        state.put({"id": 1, "type": "story", "title": "restored", "time": 1704067200})
    rows = [
        r
        for b in state.batches(["2024-01"], ingest.arrow_schema())
        for r in b.to_pylist()
    ]
    assert rows[0]["title"] == "restored"
    state.db.close()
