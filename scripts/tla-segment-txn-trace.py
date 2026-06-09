#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

"""Split filtered transaction traces into key-connected components.

TraceAntflyTransaction builds several cross-product functions over Txns and
Keys. A full unit-test trace can contain many unrelated transactions, which
makes TLC spend the CI budget on unrelated Txns x Keys state. Transactions that
do not share write, delete, or predicate keys cannot affect each other's OCC or
LWW behavior in the trace model, so validate each connected component
independently.

Usage:
  python3 tla-segment-txn-trace.py filtered.ndjson out-dir
"""

import json
import os
import sys
from collections import Counter, defaultdict

KEY_FIELDS = ("writeKeys", "deleteKeys", "predicateKeys")


class UnionFind:
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def add(self, item):
        if item not in self.parent:
            self.parent[item] = item
            self.rank[item] = 0

    def find(self, item):
        parent = self.parent[item]
        if parent != item:
            self.parent[item] = self.find(parent)
        return self.parent[item]

    def union(self, left, right):
        self.add(left)
        self.add(right)
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        if self.rank[left_root] < self.rank[right_root]:
            left_root, right_root = right_root, left_root
        self.parent[right_root] = left_root
        if self.rank[left_root] == self.rank[right_root]:
            self.rank[left_root] += 1


def parse_line(line):
    line = line.strip()
    if not line:
        return None
    start = line.find("{")
    if start > 0:
        line = line[start:]
    obj = json.loads(line)
    if obj.get("tag") != "antfly-trace":
        return None
    return obj


def event_keys(event):
    state = event.get("state") or {}
    keys = set()
    for field in KEY_FIELDS:
        values = state.get(field)
        if isinstance(values, list):
            keys.update(str(value) for value in values)
    return keys


def write_segment(path, events):
    with open(path, "w", encoding="utf-8") as out:
        for obj in events:
            out.write(json.dumps(obj, separators=(",", ":")))
            out.write("\n")


def main():
    if len(sys.argv) != 3:
        print(
            "usage: tla-segment-txn-trace.py filtered.ndjson out-dir",
            file=sys.stderr,
        )
        return 2

    trace_path = sys.argv[1]
    out_dir = sys.argv[2]
    os.makedirs(out_dir, exist_ok=True)

    events = []
    txns_by_key = defaultdict(list)
    uf = UnionFind()

    with open(trace_path, "r", encoding="utf-8") as trace_file:
        for line in trace_file:
            obj = parse_line(line)
            if obj is None:
                continue
            txn_id = obj["event"]["txnId"]
            uf.add(txn_id)
            events.append(obj)
            for key in event_keys(obj["event"]):
                txns_by_key[key].append(txn_id)

    for txns in txns_by_key.values():
        first = txns[0]
        for txn_id in txns[1:]:
            uf.union(first, txn_id)

    components = defaultdict(list)
    for event_index, obj in enumerate(events):
        txn_id = obj["event"]["txnId"]
        components[uf.find(txn_id)].append((event_index, obj))

    ordered_components = sorted(
        components.values(),
        key=lambda segment: segment[0][0],
    )

    max_events = 0
    max_txns = 0
    max_keys = 0
    for index, segment in enumerate(ordered_components, start=1):
        segment_events = [obj for _, obj in segment]
        segment_txns = {obj["event"]["txnId"] for obj in segment_events}
        segment_keys = set()
        event_counts = Counter()
        for obj in segment_events:
            event = obj["event"]
            event_counts[event["name"]] += 1
            segment_keys.update(event_keys(event))

        max_events = max(max_events, len(segment_events))
        max_txns = max(max_txns, len(segment_txns))
        max_keys = max(max_keys, len(segment_keys))

        name = f"txn-segment-{index:04d}.ndjson"
        write_segment(os.path.join(out_dir, name), segment_events)
        print(
            f"Wrote {name} "
            f"({len(segment_events)} events, {len(segment_txns)} txns, "
            f"{len(segment_keys)} keys)",
            file=sys.stderr,
        )

    print(
        f"Segmented {len(events)} events into {len(ordered_components)} trace(s); "
        f"max events={max_events}, max txns={max_txns}, max keys={max_keys}",
        file=sys.stderr,
    )
    return 0 if ordered_components else 1


if __name__ == "__main__":
    raise SystemExit(main())
