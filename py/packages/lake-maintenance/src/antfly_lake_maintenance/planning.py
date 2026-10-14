# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Epoch-scoped, bounded reachability marking without a process-sized live set."""

import json
from collections import OrderedDict

from .provider import Reachability
from .store import Conflict, Unavailable, digest, encode


class PlanningPending(Exception):
    """A durable planning turn exhausted its budget; admission stays fenced."""


class DurableSet:
    """Monotone radix pages. Splits publish children before their parent CAS.

    A lost response or concurrent split may leave extra marks, never omit marks.
    Each leaf is bounded; no catalog-sized JSON document or SQLite is involved.
    """

    def __init__(self, store, prefix, *, leaf_size=256):
        self.store, self.prefix, self.leaf_size = store, prefix, leaf_size

    def _uri(self, path):
        return self.prefix + (path or "root") + ".json"

    def __contains__(self, key):
        hashed, path = digest(key.encode()), ""
        while True:
            saved = self.store.get(self._uri(path))
            if saved is None:
                return False
            node = json.loads(saved[0])
            if not node.get("split"):
                return key in node["keys"]
            if len(path) >= len(hashed):
                raise Unavailable("reachability radix exhausted")
            path += hashed[len(path)]

    def add(self, key, _path=""):
        hashed, path = digest(key.encode()), _path
        for _ in range(128):
            uri = self._uri(path)
            saved = self.store.get(uri)
            node = json.loads(saved[0]) if saved else {"keys": []}
            if node.get("split"):
                if len(path) >= len(hashed):
                    raise Unavailable("reachability radix exhausted")
                path += hashed[len(path)]
                continue
            if key in node["keys"]:
                return
            keys = sorted(set(node["keys"]) | {key})
            if len(keys) > self.leaf_size:
                children = {}
                for value in keys:
                    child = path + digest(value.encode())[len(path)]
                    children.setdefault(child, []).append(value)
                for child, values in children.items():
                    for value in values:
                        self.add(value, child)
                replacement = {"split": True}
            else:
                replacement = {"keys": keys}
            try:
                self.store.put(
                    uri,
                    encode(replacement),
                    version=saved[1] if saved else None,
                    absent=saved is None,
                )
                return
            except Conflict:
                continue
        raise Conflict("reachability page contention")


class DurableReachability(Reachability):
    def __init__(self, provider, warehouse, *, prefix, max_files, max_bytes):
        super().__init__(provider, warehouse, max_files=max_files, max_bytes=max_bytes)
        self.files = DurableSet(provider.store, prefix + "files/")
        self.marked_manifests = DurableSet(provider.store, prefix + "manifests/")
        self.live_snapshots = DurableSet(provider.store, prefix + "live-snapshots/")
        self.completed_roots = DurableSet(provider.store, prefix + "roots/")
        self.seen_metadata = DurableSet(provider.store, prefix + "metadata/")
        self.metadata = OrderedDict()
        self.seen_inputs = DurableSet(provider.store, prefix + "inputs/")
        self.new_files = 0

    def add(self, uri):
        self.check_uri(uri)
        if uri in self.files:
            return
        if self.new_files >= self.max_files:
            raise PlanningPending()
        self.files.add(uri)
        self.new_files += 1

    def read_metadata(self, uri):
        # Bound memory independently of total catalog/history size. Replays
        # charge newly reached metadata; completed roots remain durable marks.
        if uri in self.metadata:
            self.metadata.move_to_end(uri)
            return self.metadata[uri]
        previous = self.bytes
        seen = uri in self.seen_metadata
        if not seen and self.bytes >= self.max_bytes:
            raise PlanningPending()
        # One individual metadata object still must fit the configured budget.
        old_limit = self.max_bytes
        self.max_bytes = previous + old_limit
        try:
            result = super().read_metadata(uri)
        finally:
            self.max_bytes = old_limit
        if seen:
            self.bytes = previous
        else:
            self.seen_metadata.add(uri)
        while len(self.metadata) > 128:
            self.metadata.popitem(last=False)
        return result

    def manifest_input(self, uri):
        source = self.provider.io.new_input(uri)
        size = len(source)
        if size > self.max_bytes:
            raise Unavailable("individual manifest exceeds planning budget")
        if uri not in self.seen_inputs:
            if size > self.max_bytes - self.bytes:
                raise PlanningPending()
            self.seen_inputs.add(uri)
            self.bytes += size
        return source

    def mark_values(self, metadata, *, snapshots=None, root=None):
        # Global marking needs file reachability, not an in-memory index of
        # every snapshot in every historical catalog root. Target/native reader
        # history is indexed explicitly by history() before this traversal.
        return super().mark_values(metadata, snapshots=snapshots, root=None)

    def history(self, uri):
        # File work per turn is independent of the metadata-history safety
        # ceiling; a table with many data files is not a huge metadata graph.
        per_turn = self.max_files
        self.max_files = (
            self.provider.config.get("max_metadata_roots", 100_000)
            if hasattr(self.provider, "config")
            else 100_000
        )
        try:
            super().history(uri)
        finally:
            self.max_files = per_turn
