# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""HN row CDC producer; Antfly owns WAL, Parquet writing and search publication."""

from itertools import islice
import json
import os
from urllib.parse import quote
from urllib.request import Request, urlopen
import uuid


def publish(
    state,
    endpoint,
    table_name,
    batch_size=1000,
    created_before=None,
    created_after=None,
):
    if not 1 <= batch_size <= 16384:
        raise ValueError("native batch size must be between 1 and 16384")
    uri = (
        endpoint.rstrip("/") + "/tables/" + quote(table_name, safe="") + "/lake/changes"
    )
    if (
        created_before is not None
        and created_before < 1
        or created_after is not None
        and created_after < 0
    ):
        raise ValueError("creation-time boundaries must be nonnegative epoch seconds")
    if (
        created_before is not None
        and created_after is not None
        and created_after >= created_before
    ):
        raise ValueError("creation-time cohort must have a nonempty interval")
    cohort = json.dumps(
        {"before": created_before, "after": created_after}, sort_keys=True
    )
    saved = state.get("native_changes_request", "")
    if saved:
        intent = json.loads(saved)
        if intent["endpoint"] != uri:
            raise RuntimeError("pending changes belong to a different native table")
    bound = state.get("native_changes_cohort", "")
    if (
        bound
        and bound != cohort
        or not bound
        and (created_before is not None or created_after is not None)
        and (saved or state.get("native_changes_checkpoint", ""))
    ):
        raise RuntimeError(
            "cohort boundaries cannot change on an existing writer; migrate with a fresh state directory"
        )
    state.set("native_changes_cohort", cohort)
    if not saved:
        pending = list(islice(state.db.entries("change:"), batch_size))
        if not pending:
            return None
        epoch = state.get("native_changes_epoch", "") or uuid.uuid4().hex
        previous = state.get("native_changes_checkpoint", "")
        checkpoint = str(int(previous or "0") + 1)
        changes, selected, ignored, size = [], [], [], 0
        for key, marker in pending:
            item = state.item(int(key.split(":", 1)[1]))
            if item is None:
                raise RuntimeError("pending HN change has no durable source row")
            if created_before is not None or created_after is not None:
                payload = item["payload"]
                created = int(payload.get("time", payload.get("created_at", 0)) or 0)
                if created <= 0:
                    raise RuntimeError(
                        "cannot assign a HN change without its original creation time; reconcile the source row first"
                    )
                if (
                    created_before is not None
                    and created >= created_before
                    or created_after is not None
                    and created < created_after
                ):
                    ignored.append((key, marker))
                    continue
            row = state.serving_row(item)
            change = (
                {"op": "upsert", "row": row}
                if row is not None
                else {
                    "op": "delete",
                    "row": {"hn_id": item["id"]},
                }
            )
            encoded_size = len(json.dumps(change).encode())
            if encoded_size > 3 * 1024 * 1024:
                raise ValueError("HN row exceeds the native transaction byte limit")
            if size + encoded_size > 3 * 1024 * 1024:
                break
            size += encoded_size
            changes.append(change)
            selected.append((key, marker))
        # A separate worker/state directory owns the complementary cohort.
        # Retain source rows so later moderation updates keep their original time.
        with state.transaction():
            for key, marker in ignored:
                if state.db.get(key) == marker:
                    state.db.delete(key)
        if not selected:
            return None
        pending = selected
        body = {
            "batch_id": uuid.uuid4().hex,
            "source": "hackernews.firebase",
            "epoch": epoch,
            "checkpoint": checkpoint,
            "expected_checkpoint": previous or None,
            "key_fields": ["hn_id"],
            "changes": changes,
        }
        intent = {"endpoint": uri, "body": body, "pending": pending}
        # Save exact transaction before network I/O. An ambiguous response is
        # retried byte-for-byte, even if source rows have subsequently changed.
        with state.transaction():
            state.set("native_changes_epoch", epoch)
            state.set("native_changes_request", json.dumps(intent))
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token := os.environ.get("ANTFLY_API_KEY"):
        headers["Authorization"] = "Bearer " + token
    request = Request(uri, json.dumps(intent["body"]).encode(), headers, method="POST")
    with urlopen(request, timeout=60) as response:
        result = json.load(response)
        if (
            response.status != 202
            or result.get("state") != "accepted"
            or not isinstance(result.get("wal_lsn"), int)
        ):
            raise RuntimeError("native changes were not durably accepted")
    with state.transaction():
        for key, submitted in intent["pending"]:
            if state.db.get(key) == submitted:
                state.db.delete(key)
        state.set("native_changes_checkpoint", intent["body"]["checkpoint"])
        state.set("native_changes_request", "")
    return result
