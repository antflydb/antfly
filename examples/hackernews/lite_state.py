# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Bounded ingestion state and secondary key ranges in one Antfly Lite file."""

import base64
from contextlib import contextmanager
from datetime import datetime, timezone
import heapq
import hashlib
from itertools import islice
import json
from pathlib import Path
import time

import antfly_embedded as antfly
from normalize import plain


def number(value):
    return f"{int(value):020d}"


class LiteStore:
    def __init__(self, path):
        self.path = Path(path)
        self.native = antfly.open(path) if self.path.exists() else antfly.create(path)
        # Bookkeeping uses native key ranges; the serving instance owns text indexes.
        self.native.delete_index("full_text_index_v0")
        self.staged = None
        self.timestamp = int(self.get("clock", 0))

    def get(self, key, default=None):
        if self.staged is not None and key in self.staged:
            value = self.staged[key]
            return default if value is None else value
        try:
            return self.native.lookup(key)["value"]
        except antfly.NotFoundError:
            return default

    def set(self, key, value):
        if self.staged is None:
            with self.transaction():
                self.set(key, value)
        else:
            self.staged[key] = value

    def delete(self, key):
        self.set(key, None)

    @contextmanager
    def transaction(self):
        if self.staged is not None:
            raise RuntimeError("nested ingestion transaction")
        self.staged = {}
        try:
            yield
            if self.staged:
                self.timestamp = max(time.time_ns(), self.timestamp + 1)
                self.staged["clock"] = self.timestamp
                writes = [
                    antfly.WriteIntent(
                        key=k,
                        delete=v is None,
                        value=None
                        if v is None
                        else json.dumps({"value": v}, separators=(",", ":")).encode(),
                    )
                    for k, v in self.staged.items()
                ]
                deadline = time.monotonic() + 30
                while True:
                    try:
                        self.native.batch(writes, timestamp=self.timestamp)
                        break
                    except antfly.BusyError:
                        # Lite rejected admission before publication. Release the
                        # handle between calls so retirement/vacuum can progress.
                        # OutcomeUnknown and other failures must never be retried.
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.1)
        finally:
            self.staged = None

    def entries(self, prefix, after="", page_size=512):
        """Ordered bounded scans, including this batch's pending writes."""
        start = max(prefix, after)
        stop = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        overlay = self.staged or {}

        def persisted():
            cursor = start
            inclusive = not after or after < prefix
            while True:
                result = self.native.scan(
                    {
                        "from_key_b64": base64.b64encode(cursor.encode()).decode(),
                        "to_key_b64": base64.b64encode(stop.encode()).decode(),
                        "inclusive_from": inclusive,
                        "exclusive_to": True,
                        "include_documents": True,
                        "limit": page_size,
                    }
                )
                documents = result["documents"]
                if not documents:
                    return
                for document in documents:
                    key = base64.b64decode(document["id_b64"]).decode()
                    cursor = key
                    if key not in overlay:
                        value = document["json"]
                        value = json.loads(value) if isinstance(value, str) else value
                        yield key, value["value"]
                inclusive = False

        staged = sorted(
            (k, v)
            for k, v in overlay.items()
            if k.startswith(prefix) and k > after and v is not None
        )
        yield from heapq.merge(persisted(), staged, key=lambda entry: entry[0])

    def close(self):
        self.native.close()


class State:
    def __init__(self, path):
        self.db = LiteStore(path)

    def transaction(self):
        return self.db.transaction()

    def get(self, key, default="0"):
        return self.db.get("checkpoint:" + key, default)

    def set(self, key, value):
        self.db.set("checkpoint:" + key, str(value))

    def item(self, item_id):
        return self.db.get("item:" + number(item_id))

    def items(self, after=0):
        for _, value in self.db.entries("item:", "item:" + number(after)):
            yield value

    def dirty_months(self):
        return [key.split(":", 1)[1] for key, _ in self.db.entries("dirty:")]

    def mark_changed(self, row):
        # Pending markers stay small during a large BigQuery backfill. The
        # exact bounded outgoing transaction owns its own row images.
        digest = hashlib.sha256(
            json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        self.db.set("change:" + number(row["id"]), digest)

    def put(self, item):
        item = dict(item)
        if "type" in item or "item_type" in item:
            item.setdefault("dead", False)
            item.setdefault("deleted", False)
        item_id = int(item.get("id", item.get("hn_id")))
        old = self.item(item_id)
        if old:
            item = old["payload"] | item
        item["id"] = item_id
        if old and item == old["payload"]:
            return
        month = datetime.fromtimestamp(
            int(item.get("time", item.get("created_at", 0)) or 0), timezone.utc
        ).strftime("%Y-%m")
        parent = int(item.get("parent", item.get("parent_id", 0)) or 0)
        kind = item.get("type", item.get("item_type"))
        ancestor = self.item(parent) if parent else None
        root = (
            item_id
            if kind in ("story", "job", "poll")
            else ancestor["root"]
            if ancestor
            else None
        )
        if old:
            self.db.delete(f"month:{old['month']}:{number(item_id)}")
            self.db.delete(f"child:{number(old['parent'])}:{number(item_id)}")
            self.db.set("dirty:" + old["month"], True)
            if old["parent"] != parent:
                self.db.set("invalidate:" + number(item_id), True)
        value = dict(id=item_id, parent=parent, root=root, month=month, payload=item)
        self.db.set("item:" + number(item_id), value)
        self.mark_changed(value)
        self.db.set(f"month:{month}:{number(item_id)}", item_id)
        if parent:
            self.db.set(f"child:{number(parent)}:{number(item_id)}", item_id)
        self.db.set("dirty:" + month, True)
        if root is None:
            self.db.set("unresolved:" + number(item_id), item_id)
        else:
            self.db.delete("unresolved:" + number(item_id))
            self.db.set("root_work:" + number(item_id), item_id)
        self.set("maximum_id", max(int(self.get("maximum_id")), item_id))

    def resolve_roots(self):
        # Persistent bounded frontier: interruption cannot lose descendant work.
        while pending := list(islice(self.db.entries("invalidate:"), 128)):
            with self.transaction():
                for key, _ in pending:
                    item_id = int(key.split(":")[1])
                    row = self.item(item_id)
                    if row is not None:
                        row["root"] = None
                        self.db.set("item:" + number(item_id), row)
                        self.mark_changed(row)
                        self.db.set("unresolved:" + number(item_id), item_id)
                        self.db.delete("root_work:" + number(item_id))
                        self.db.set("dirty:" + row["month"], True)
                        # A cursor bounds high-fanout invalidation transactions.
                        cursor_key = "invalidate_cursor:" + number(item_id)
                        cursor = self.db.get(cursor_key, "")
                        children = list(
                            islice(
                                self.db.entries(
                                    "child:" + number(item_id) + ":", cursor
                                ),
                                128,
                            )
                        )
                        for child_key, child_id in children:
                            child = self.item(child_id)
                            if child["root"] is not None:
                                self.db.set("invalidate:" + number(child_id), True)
                        if len(children) == 128:
                            self.db.set(cursor_key, children[-1][0])
                            continue
                        self.db.delete(cursor_key)
                    self.db.delete(key)
        # Re-seed unresolved children whose parent is now known. No graph in RAM.
        for key, item_id in self.db.entries("unresolved:"):
            row = self.item(item_id)
            parent = self.item(row["parent"])
            kind = row["payload"].get("type", row["payload"].get("item_type"))
            root = (
                item_id
                if kind in ("story", "job", "poll")
                else parent["root"]
                if parent
                else None
            )
            if root is not None:
                with self.transaction():
                    row["root"] = root
                    self.db.set("item:" + number(item_id), row)
                    self.mark_changed(row)
                    self.db.delete(key)
                    self.db.set("root_work:" + number(item_id), item_id)
                    self.db.set("dirty:" + row["month"], True)
        while pending := list(islice(self.db.entries("root_work:"), 32)):
            with self.transaction():
                for key, item_id in pending:
                    parent = self.item(item_id)
                    cursor_key = "root_cursor:" + number(item_id)
                    cursor = self.db.get(cursor_key, "")
                    children = list(
                        islice(
                            self.db.entries("child:" + number(item_id) + ":", cursor),
                            64,
                        )
                    )
                    for _, child_id in children:
                        child = self.item(child_id)
                        if child["root"] is None and parent["root"] is not None:
                            child["root"] = parent["root"]
                            self.db.set("item:" + number(child_id), child)
                            self.mark_changed(child)
                            self.db.delete("unresolved:" + number(child_id))
                            self.db.set("root_work:" + number(child_id), child_id)
                            self.db.set("dirty:" + child["month"], True)
                    if len(children) == 64:
                        self.db.set(cursor_key, children[-1][0])
                    else:
                        self.db.delete(cursor_key)
                        self.db.delete(key)

    def missing_parents(self, limit):
        result = set()
        for _, item_id in self.db.entries("unresolved:"):
            parent = self.item(item_id)["parent"]
            if parent and self.item(parent) is None:
                result.add(parent)
                if len(result) >= limit:
                    break
        return sorted(result)

    @staticmethod
    def schedule_key(value):
        return f"schedule:{value['priority']}:{number(int(value['retry_at'] * 1000))}:{number(value['order'])}:{number(value['id'])}"

    def queue(self, ids, priority=0):
        serial = int(self.get("queue_order"))
        for item_id in ids:
            item_id = int(item_id)
            key = "pending:" + number(item_id)
            old = self.db.get(key)
            serial += 1
            value = old or dict(id=item_id, retry_at=0, priority=priority, order=serial)
            if old:
                self.db.delete(self.schedule_key(old))
                value = value | {"priority": min(value["priority"], priority)}
            self.db.set(key, value)
            self.db.set(self.schedule_key(value), item_id)
        self.set("queue_order", serial)

    def due(self, priority, limit, now):
        result = []
        for key, item_id in self.db.entries(f"schedule:{priority}:"):
            if int(key.split(":")[2]) > int(now * 1000):
                break
            result.append(item_id)
            if len(result) >= limit:
                break
        return result

    def poll(self, fetch, batch_size=1000, parent_limit=1000):
        if not self.get("maxitem", ""):
            raise RuntimeError("backfill first to establish the new-item watermark")
        newest, updates = int(fetch("maxitem")), fetch("updates").get("items", [])
        with self.transaction():
            cursor = int(self.get("maxitem"))
            stop = max(cursor, min(newest, cursor + batch_size))
            self.queue(range(cursor + 1, stop + 1))
            self.set("maxitem", stop)
            self.queue(updates)
            ids = [
                row["id"]
                for row in islice(self.items(int(self.get("sweep"))), batch_size)
            ]
            self.queue(ids, priority=2)
            self.set("sweep", ids[-1] if ids else 0)
            self.queue(self.missing_parents(parent_limit), priority=1)
        now, todo = time.time(), []
        quotas = [
            max(1, batch_size * 3 // 5),
            max(1, batch_size // 5),
            max(1, batch_size // 5),
        ]
        round_number = int(self.get("poll_round"))
        order = (
            list(range(3))
            if batch_size >= 5
            else [(round_number + i) % 3 for i in range(3)]
        )
        for priority in order:
            remaining = batch_size - len(todo)
            if remaining:
                todo.extend(self.due(priority, min(remaining, quotas[priority]), now))
        seen = set(todo)
        for priority in order:
            for item_id in self.due(priority, batch_size, now):
                if len(todo) < batch_size and item_id not in seen:
                    todo.append(item_id)
                    seen.add(item_id)
        with self.transaction():
            self.set("poll_round", round_number + 1)
        done = 0
        for item_id in todo:
            key = "pending:" + number(item_id)
            pending = self.db.get(key)
            try:
                item = fetch(f"item/{item_id}")
                if item is None or int(item.get("id", -1)) != item_id:
                    raise ValueError("item unavailable or mismatched ID")
            except (OSError, ValueError):
                with self.transaction():
                    self.db.delete(self.schedule_key(pending))
                    pending = pending | {"retry_at": time.time() + 60}
                    self.db.set(key, pending)
                    self.db.set(self.schedule_key(pending), item_id)
                continue
            with self.transaction():
                self.put(item)
                self.db.delete(key)
                self.db.delete(self.schedule_key(pending))
            done += 1
        self.resolve_roots()
        return done

    def backfill(self, filenames, batch_size=4096):
        import hashlib
        import pyarrow.parquet as pq

        for filename in filenames:
            digest = hashlib.sha256()
            with Path(filename).open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            key = "backfill:" + digest.hexdigest()
            completed, offset = int(self.get(key)), 0
            for batch in pq.ParquetFile(filename).iter_batches(batch_size=batch_size):
                end = offset + len(batch)
                if end > completed:
                    with self.transaction():
                        for item in batch.to_pylist()[max(0, completed - offset) :]:
                            self.put(item)
                        self.set(key, end)
                offset = end
        self.resolve_roots()
        with self.transaction():
            self.set(
                "maxitem", max(int(self.get("maxitem")), int(self.get("maximum_id")))
            )

    @staticmethod
    def serving_row(item):
        from urllib.parse import urlsplit

        row, root, month = item["payload"], item["root"], item["month"]
        kind = row.get("type", row.get("item_type"))
        if row.get("deleted") or row.get("dead") or kind not in ("story", "comment"):
            return None
        title, text, url = (
            plain(row.get("title", "")),
            row.get("text", row.get("text_html", "")) or "",
            row.get("url", "") or "",
        )
        return dict(
            hn_id=row["id"],
            title=title,
            url=url,
            text_html=text,
            body="\n".join(filter(None, (title, plain(text)))),
            author=row.get("by", row.get("author", "")) or "",
            points=int(row.get("score", row.get("points", 0)) or 0),
            created_at=int(row.get("time", row.get("created_at", 0)) or 0),
            item_type=kind,
            parent_id=int(row.get("parent", row.get("parent_id", 0)) or 0),
            comment_count=int(row.get("descendants", row.get("comment_count", 0)) or 0),
            domain=urlsplit(url).hostname or "",
            root_story_id=root,
            created_month=month,
        )

    def batches(self, months, schema, batch_size=4096):
        import pyarrow as pa

        for month in months:
            records = []
            for _, item_id in self.db.entries("month:" + month + ":"):
                row = self.serving_row(self.item(item_id))
                if row is None:
                    continue
                records.append(row)
                if len(records) >= batch_size:
                    yield pa.RecordBatch.from_pylist(records, schema=schema)
                    records = []
            if records:
                yield pa.RecordBatch.from_pylist(records, schema=schema)
