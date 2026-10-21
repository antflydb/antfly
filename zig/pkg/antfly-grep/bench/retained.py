#!/usr/bin/env python3
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

"""Reference consumer for registered lexical views; no third-party dependencies.

One live view is retained. Pinned-revision reads can omit the server request;
call refresh explicitly before requiring current filesystem observations. This
serial client owns the protocol: invalidate/failed refresh, register, forget,
trim and close discard reuse. Delta application stages only changed payloads;
materialize() is an explicit O(full output) operation for consumers needing rows.
The payload allowance bounds retained serialized bytes, not Python object RSS.
"""

import base64
import io
import json
import time
from collections import OrderedDict
from dataclasses import dataclass, field

from registered import Client


# Private retained storage, not a producer wire format. Frames are JSONL,
# already validated before publication, and can be decoded without copying or
# materializing a complete file. Legacy compact JSON arrays remain readable.
FRAME_MAGIC = b"AGVIEW1\n"


def file_rows(payload):
    if not payload.startswith(FRAME_MAGIC):
        yield from json.loads(payload)
        return
    cursor = len(FRAME_MAGIC)
    while cursor < len(payload):
        end = payload.find(b"\n", cursor)
        if end < 0:
            raise RuntimeError("incomplete retained frame")
        yield json.loads(payload[cursor:end])
        cursor = end + 1


def path_bytes(value):
    if "text" in value:
        return value["text"].encode("utf-8")
    return base64.b64decode(value["bytes"], validate=True)


def validate_batch(row, encoded_size):
    if row.get("result_schema_version") == 4:
        rows = row.get("rows")
        if (
            set(row) != {"type", "result_schema_version", "file_id", "rows"}
            or not isinstance(rows, list)
            or not 1 <= len(rows) <= 64
            or encoded_size > 33000
        ):
            raise RuntimeError("invalid batch envelope")
        for event in rows:
            if (
                not isinstance(event, list)
                or len(event) != 5
                or type(event[0]) is not int
                or event[0] not in (0, 1)
                or type(event[1]) is not int
                or event[1] < 1
                or type(event[2]) is not int
                or event[2] < 0
                or not isinstance(event[3], dict)
                or set(event[3]) not in ({"text"}, {"bytes"})
                or not isinstance(next(iter(event[3].values())), str)
                or not isinstance(event[4], list)
            ):
                raise RuntimeError("invalid batch tuple")
        return
    events = row.get("events")
    if (
        set(row) != {"type", "result_schema_version", "file_id", "events"}
        or row.get("result_schema_version") != 3
        or not isinstance(events, list)
        or not 1 <= len(events) <= 64
        or encoded_size > 33000
    ):
        raise RuntimeError("invalid batch envelope")
    if any(
        not isinstance(event, dict)
        or event.get("type") not in ("match", "context")
        or not isinstance(event.get("data"), dict)
        or "file_id" in event
        or "result_schema_version" in event
        or "source" in event
        for event in events
    ):
        raise RuntimeError("invalid batch event")


class Payloads:
    """Stage validated original frames without keeping decoded per-match trees."""

    def __init__(self):
        self.identities = {}
        self.files = {}
        self.open = {}

    def consume(self, row, encoded):
        kind = row["type"]
        if kind in ("begin", "match", "context", "end", "batch") and b"\n" in encoded:
            raise RuntimeError("multiline retained frame")
        if kind == "begin":
            path = path_bytes(row["data"]["path"])
            identity = row["file_id"]
            if identity in self.identities or path in self.files or path in self.open:
                raise RuntimeError("duplicate file identity")
            self.identities[identity] = path
            buffer = self.open[path] = io.BytesIO()
            buffer.write(FRAME_MAGIC)
            buffer.write(encoded)
        elif kind in ("match", "context", "end", "batch"):
            path = self.identities.get(row["file_id"])
            if path not in self.open:
                raise RuntimeError("unbound file identity")
            if kind == "batch":
                validate_batch(row, len(encoded))
            buffer = self.open[path]
            buffer.write(b"\n")
            buffer.write(encoded)
            if kind == "end":
                buffer.write(b"\n")
                self.files[path] = buffer.getvalue()
                del self.open[path]
        else:
            return False
        return True

    def finish(self):
        if self.open:
            raise RuntimeError("incomplete file payload")
        return self.files


@dataclass
class View:
    handle: str
    revision: str = ""
    token: str = ""
    observed_at: int = 0
    exit_code: int = 1
    files: dict = field(default_factory=dict)
    payload_bytes: int = 0

    def iter_file(self, path):
        """Decode consumed frames only; untouched files and tails stay serialized.

        Iteration pins this file payload and the provenance observation before
        yielding, so a subsequent refresh cannot mix generations within a file.
        """
        if not self.revision or not self.token:
            raise RuntimeError("view requires a complete transaction")
        payload = self.files[path]
        revision, observed_at = self.revision, self.observed_at
        for row in file_rows(payload):
            if row["type"] == "begin":
                row["source"]["snapshot"] = revision
                row["source"]["observed_at"] = observed_at
            if row["type"] == "batch":
                if row["result_schema_version"] == 4:
                    for kind, line, offset, text, matches in row["rows"]:
                        yield {
                            "type": "match" if kind == 0 else "context",
                            "data": {
                                "lines": text,
                                "line_number": line,
                                "absolute_offset": offset,
                                "submatches": matches,
                            },
                            "probability": None,
                            "scope": "line",
                            "file_id": row["file_id"],
                            "result_schema_version": 4,
                        }
                else:
                    for event in row["events"]:
                        yield {
                            **event,
                            "file_id": row["file_id"],
                            "result_schema_version": 3,
                        }
            else:
                yield row

    def materialize_file(self, path):
        return list(self.iter_file(path))

    def materialize(self):
        if not self.revision or not self.token:
            raise RuntimeError("view requires a complete transaction")
        return [row for path in sorted(self.files) for row in self.iter_file(path)]

    def apply(self, rows, revision, observed_at, *, payloads=None):
        terminal = rows[-1]
        if payloads is not None:
            payloads.finish()
        if terminal.get("exit_code") not in (0, 1):
            raise RuntimeError("incomplete workspace transaction")
        if terminal["type"] == "workspace_not_modified":
            if (
                len(rows) != 1
                or (payloads is not None and payloads.files)
                or not terminal.get("complete")
                or terminal["revision"] != revision
                or terminal["result_token"] != self.token
                or self.revision != revision
            ):
                raise RuntimeError("invalid minimal acknowledgement")
            self.observed_at = terminal["observed_at"]
            return
        summary = next(r for r in rows if r["type"] == "summary")
        if not summary["stats"]["complete"] or terminal.get("revision") != revision:
            raise RuntimeError("incomplete workspace transaction")
        unchanged = next((r for r in rows if r["type"] == "workspace_unchanged"), None)
        if unchanged is not None:
            if (
                (payloads is not None and payloads.files)
                or unchanged["result_token"] != self.token
                or self.revision != revision
            ):
                raise RuntimeError("unknown unchanged baseline")
            self.observed_at = unchanged["observed_at"]
            return
        token = next(r["result_token"] for r in rows if r["type"] == "result_token")
        patch = next((r for r in rows if r["type"] == "workspace_lexical_delta"), None)
        if patch is not None and (
            patch["base_token"] != self.token
            or patch["base_revision"] != self.revision
            or patch["revision"] != revision
        ):
            raise RuntimeError("unknown delta baseline")
        if payloads is None:
            files, staged, ended = {}, {}, set()
            for row in rows:
                kind = row["type"]
                if kind == "begin":
                    path = path_bytes(row["data"]["path"])
                    identity = row["file_id"]
                    if identity in files or path in staged:
                        raise RuntimeError("duplicate file identity")
                    files[identity] = path
                    staged[path] = [row]
                elif kind in ("match", "context", "end", "batch"):
                    identity = row["file_id"]
                    if identity not in files or identity in ended:
                        raise RuntimeError("unbound file identity")
                    if kind == "batch":
                        validate_batch(
                            row,
                            len(
                                json.dumps(
                                    row, ensure_ascii=False, separators=(",", ":")
                                ).encode()
                            ),
                        )
                    staged[files[identity]].append(row)
                    if kind == "end":
                        ended.add(identity)
            if len(ended) != len(files):
                raise RuntimeError("incomplete file payload")
            upserts = {
                path: FRAME_MAGIC
                + b"\n".join(
                    json.dumps(row, separators=(",", ":")).encode() for row in value
                )
                + b"\n"
                for path, value in staged.items()
            }
        else:
            upserts = payloads.finish()
        removed = [path_bytes(p) for p in patch["removed"]] if patch else []
        # All parsing and validation precedes publication. Successful deltas
        # mutate this owned live view in O(changed paths), without copying the
        # unchanged map or retaining a chain of old generations.
        # Invalidate the certificate before mutable-map publication. A local
        # allocation failure cannot leave a partially changed view reusable.
        # Protocol/validation failures above preserve the prior live view.
        self.revision = self.token = ""
        if patch is None:
            self.files = upserts
            self.payload_bytes = sum(len(p) + len(v) for p, v in upserts.items())
        else:
            for path in removed:
                prior = self.files.pop(path, None)
                if prior is not None:
                    self.payload_bytes -= len(path) + len(prior)
            for path, value in upserts.items():
                prior = self.files.get(path)
                self.payload_bytes += (
                    len(value) - len(prior)
                    if prior is not None
                    else len(path) + len(value)
                )
                self.files[path] = value
        self.revision, self.token = revision, token
        self.observed_at = patch["observed_at"] if patch else observed_at
        self.exit_code = terminal["exit_code"]


class RetainedClient(Client):
    def __init__(
        self,
        *args,
        max_retained_payload=8 * 1024 * 1024,
        max_response_bytes=None,
        **kwargs,
    ):
        self.view = None
        self.handles = OrderedDict()
        self.valid = False
        if max_retained_payload < 0 or (
            max_response_bytes is not None and max_response_bytes < 1
        ):
            raise ValueError("invalid workspace client allowance")
        self.max_retained_payload = max_retained_payload
        self.max_response_bytes = (
            max_response_bytes
            if max_response_bytes is not None
            else max(65536, max_retained_payload * 2 + 65536)
        )
        super().__init__(*args, **kwargs)
        self.valid = True

    def exchange(self, op, **fields):
        if op in ("invalidate", "refresh", "close"):
            self.valid = False
        if op in ("register", "forget", "trim", "close"):
            self.view = None
        handle = fields.get("query_handle")
        if op == "search" and handle in self.handles:
            self.handles.move_to_end(handle)
        if op == "forget":
            self.handles.pop(handle, None)
        if op in ("trim", "close"):
            self.handles.clear()
        try:
            result, rows = super().exchange(op, **fields)
        except Exception:
            # A streaming consumer failure may leave unread transaction frames.
            # Retire the owned stream rather than reuse a desynchronized client.
            if fields.get("consume") is not None and self.process.poll() is None:
                self.process.kill()
                self.process.wait(timeout=5)
            if self.process.poll() is not None:
                self.valid = False
                self.view = None
            raise
        if op == "register":
            row = rows[-1]
            self.handles[row["query_handle"]] = row.get(
                "supports_lexical_delta", False
            ) and row["result_schema_version"] in (2, 3, 4)
            if len(self.handles) > 8:
                self.handles.popitem(last=False)
        if op == "refresh":
            self.valid = True
        return result, rows

    def search_view(self, handle, *, revalidate=False):
        started = time.monotonic()
        if not self.valid:
            raise RuntimeError(
                "refresh required before reading an invalidated workspace"
            )
        if not self.handles.get(handle):
            raise ValueError("a live compact lexical-row registration is required")
        revision = self.ready["revision"]
        view = self.view
        if (
            view is not None
            and view.handle == handle
            and view.revision == revision
            and not revalidate
        ):
            view.observed_at = self.ready["observed_at"]
            return view, {
                "requests": 0,
                "response_bytes": 0,
                "seconds": time.monotonic() - started,
            }
        if view is None or view.handle != handle:
            view = View(handle)
        payloads = Payloads()
        result, rows = self.exchange(
            "search",
            consume=payloads.consume,
            query_handle=handle,
            revision=revision,
            if_result_token=view.token or "0" * 64,
            delta=True,
            acknowledgement="minimal",
        )
        try:
            view.apply(rows, revision, self.ready["observed_at"], payloads=payloads)
        except Exception:
            self.view = None
            raise
        self.view = view if view.payload_bytes <= self.max_retained_payload else None
        result["requests"] = 1
        return view, result

    def close(self):
        self.valid = False
        self.view = None
        self.handles.clear()
        super().close()
