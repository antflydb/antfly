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

"""Filter antfly-trace ndjson to keep only transactions with TLA+ spec-compatible lifecycles.

The AntflyTransaction TLA+ spec models these txnStatus transitions:
  idle → preparing (InitTransaction)
  preparing → predicatesChecked (CheckPredicates)
  predicatesChecked → predicatesChecked (WriteIntentOnShard, no status change)
  predicatesChecked → aborting (WriteIntentFails)
  predicatesChecked → committed (CommitTransaction)
  aborting → aborted (AbortTransaction)
  committed/aborted → (ResolveIntentsOnShard, CleanupTxnRecord)

Events from recovery tests, retries, or external aborts don't match the spec
and are dropped. Incomplete-but-valid prefixes are kept (CHECK_DEADLOCK FALSE).

Usage:
  python3 tla-filter-txn-trace.py < trace.ndjson > filtered.ndjson
"""

import json
import sys
from collections import defaultdict

# Valid next events for each TLA+ txnStatus state.
# AbortTransaction from predicatesChecked/preparing is allowed (DirectAbort).
VALID_TRANSITIONS = {
    "idle":              {"InitTransaction"},
    "preparing":         {"CheckPredicates", "AbortTransaction"},
    "predicatesChecked": {"WriteIntentOnShard", "WriteIntentFails", "CommitTransaction", "AbortTransaction"},
    "aborting":          {"AbortTransaction"},
    "committed":         {"ResolveIntentsOnShard"},
    "aborted":           {"ResolveIntentsOnShard"},
    "resolving":         {"ResolveIntentsOnShard", "CleanupTxnRecord"},
    "done":              {"CleanupTxnRecord"},
}

# State transitions caused by each event
NEXT_STATE = {
    "InitTransaction":       "preparing",
    "CheckPredicates":       "predicatesChecked",
    "WriteIntentOnShard":    "predicatesChecked",  # no change
    "WriteIntentFails":      "aborting",
    "CommitTransaction":     "committed",
    "AbortTransaction":      "aborted",
    "ResolveIntentsOnShard": "resolving",
    "CleanupTxnRecord":      "done",
}


def main():
    # Collect events per transaction, preserving global order
    all_events = []
    events_by_txn = defaultdict(list)

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

    # Output events for valid transactions in original order
    for _, obj in all_events:
        if obj["event"]["txnId"] in valid_txns:
            print(json.dumps(obj, separators=(",", ":")))


if __name__ == "__main__":
    main()
