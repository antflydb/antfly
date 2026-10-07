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

"""Filter antfly-trace ndjson to keep only transactions with TLA+ spec-compatible lifecycles.

The AntflyTransaction TLA+ spec models these txnStatus transitions:
  idle → preparing (InitTransaction)
  preparing → predicatesChecked (CheckPredicates)
  predicatesChecked → predicatesChecked (WriteIntentOnShard, no status change)
  predicatesChecked → aborting (WriteIntentFails)
  predicatesChecked → committed (CommitTransaction)
  aborting → aborted (AbortTransaction)
  committed/aborted → (ResolveIntentsOnShard, CleanupTxnRecord)

Events from recovery tests, retries, or external aborts don't match the spec.
They and every transaction in their key-connected component are dropped: a
modeled transaction can otherwise depend on an omitted writer or live intent.
Independent incomplete-but-valid prefixes are kept (CHECK_DEADLOCK FALSE).

Usage:
  python3 tla-filter-txn-trace.py < trace.ndjson > filtered.ndjson
"""

import json
import sys
from collections import defaultdict, deque

KEY_FIELDS = ("writeKeys", "deleteKeys", "predicateKeys")

# Valid next events for each TLA+ txnStatus state.
# AbortTransaction from predicatesChecked/preparing is allowed (DirectAbort).
VALID_TRANSITIONS = {
    "idle": {"InitTransaction"},
    "preparing": {"CheckPredicates", "AbortTransaction"},
    "predicatesChecked": {
        "WriteIntentOnShard",
        "WriteIntentFails",
        "CommitTransaction",
        "AbortTransaction",
    },
    "aborting": {"AbortTransaction"},
    "committed": {"ResolveIntentsOnShard"},
    "aborted": {"ResolveIntentsOnShard"},
    "resolving": {"ResolveIntentsOnShard", "CleanupTxnRecord"},
    "done": {"CleanupTxnRecord"},
}

# State transitions caused by each event
NEXT_STATE = {
    "InitTransaction": "preparing",
    "CheckPredicates": "predicatesChecked",
    "WriteIntentOnShard": "predicatesChecked",  # no change
    "WriteIntentFails": "aborting",
    "CommitTransaction": "committed",
    "AbortTransaction": "aborted",
    "ResolveIntentsOnShard": "resolving",
    "CleanupTxnRecord": "done",
}


def main():
    # Collect events per transaction, preserving global order
    all_events = []
    events_by_txn = defaultdict(list)
    keys_by_txn = defaultdict(set)
    txns_by_key = defaultdict(set)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if obj.get("tag") != "antfly-trace":
            continue
        txn_id = obj["event"]["txnId"]
        idx = len(all_events)
        all_events.append((idx, obj))
        events_by_txn[txn_id].append((idx, obj))
        state = obj["event"].get("state") or {}
        for field in KEY_FIELDS:
            for key in state.get(field, []):
                keys_by_txn[txn_id].add(key)
                txns_by_key[key].add(txn_id)

    # Check each transaction's lifecycle
    valid_txns = set()
    for txn_id, events in events_by_txn.items():
        state = "idle"
        valid = True
        for _, obj in events:
            name = obj["event"]["name"]
            # RecoveryResolve/RecoveryAutoAbort are recovery-only events
            if name in ("RecoveryResolve",):
                valid = False
                break
            allowed = VALID_TRANSITIONS.get(state, set())
            if name not in allowed:
                valid = False
                break
            state = NEXT_STATE.get(name, state)
        if valid:
            valid_txns.add(txn_id)

    # A dropped transaction may still have held an intent or changed the
    # committed version seen by another transaction. Project only complete
    # key-connected components onto the single-shard model; otherwise a real
    # IntentConflict can look impossible after its writer was filtered out.
    tainted_txns = set(events_by_txn) - valid_txns
    pending = deque(tainted_txns)
    while pending:
        txn_id = pending.popleft()
        for key in keys_by_txn[txn_id]:
            for neighbor in txns_by_key.pop(key, ()):
                if neighbor not in tainted_txns:
                    tainted_txns.add(neighbor)
                    pending.append(neighbor)

    # Output independent, model-compatible components in original order.
    retained_txns = valid_txns - tainted_txns
    for _, obj in all_events:
        if obj["event"]["txnId"] in retained_txns:
            print(json.dumps(obj, separators=(",", ":")))


if __name__ == "__main__":
    main()
