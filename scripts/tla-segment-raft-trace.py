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

"""Segment a multi-run raft ndjson trace into per-cluster-lifecycle segments.

When multiple raft test scenarios write to a single ndjson file, each test
creates a fresh cluster whose nodes all emit InitState. This script splits
the concatenated trace at cluster initialization boundaries so each segment
can be validated independently against Traceetcdraft.tla.
"""
import json, sys, os

BOOTSTRAP_EVENTS = {"InitState", "BecomeFollower", "ApplyConfChange"}


def is_state_reset(current, obj):
    """Detect a new test run by state regression.

    Returns True when a node's commit/log drop to 0 from a higher value,
    indicating the raft engine was re-initialized without InitState events.
    """
    nid = obj["event"]["nid"]
    commit = obj["event"]["state"]["commit"]
    log = obj["event"]["log"]

    # Only trigger on reset to zero
    if commit != 0 or log != 0:
        return False

    # Check if this node previously had non-zero state in the current segment
    for l in reversed(current):
        prev = json.loads(l)
        if prev["event"]["nid"] == nid:
            if prev["event"]["state"]["commit"] > 0 or prev["event"]["log"] > 0:
                return True
            break  # found same node, state was already zero
    return False


def segment_trace(lines):
    """Split trace lines into segments at cluster initialization boundaries.

    A new segment starts when:
    1. We see an InitState for a node that already had InitState in the
       current segment, OR
    2. A node's state regresses (commit/log drop to 0) without InitState,
       indicating the raft engine was re-initialized for a new test.

    Trailing bootstrap events at the end of the previous segment that belong
    to bootstrap-only nodes are pulled forward into the new segment.
    """
    segments = []
    current = []
    init_nodes = set()  # nodes with InitState in current segment

    for line in lines:
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if obj.get("tag") != "trace":
            continue

        name = obj["event"]["name"]
        nid = obj["event"]["nid"]

        new_run = False
        if name == "InitState" and nid in init_nodes:
            new_run = True
        elif name not in BOOTSTRAP_EVENTS and current and is_state_reset(current, obj):
            new_run = True

        if new_run:
            # Pull back trailing bootstrap events that belong to the new run.
            overflow = []
            while current:
                prev = json.loads(current[-1])
                if prev["event"]["name"] in BOOTSTRAP_EVENTS:
                    pnid = prev["event"]["nid"]
                    has_activity = any(
                        json.loads(l)["event"]["nid"] == pnid
                        and json.loads(l)["event"]["name"] not in BOOTSTRAP_EVENTS
                        for l in current
                    )
                    if not has_activity:
                        overflow.insert(0, current.pop())
                    else:
                        break
                else:
                    break

            if current:
                segments.append(current)
            current = overflow + [line]
            init_nodes = set()
            for l in current:
                o = json.loads(l)
                if o["event"]["name"] == "InitState":
                    init_nodes.add(o["event"]["nid"])
        else:
            if name == "InitState":
                init_nodes.add(nid)
            current.append(line)

    if current:
        segments.append(current)
    return segments


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <trace.ndjson> <output-dir>", file=sys.stderr)
        sys.exit(1)

    trace_file = sys.argv[1]
    output_dir = sys.argv[2]
    os.makedirs(output_dir, exist_ok=True)

    with open(trace_file) as f:
        lines = [l.strip() for l in f if l.strip()]

    segments = segment_trace(lines)
    for i, seg in enumerate(segments):
        # Skip segments with no non-bootstrap events (empty runs)
        has_activity = any(
            json.loads(l)["event"]["name"] not in BOOTSTRAP_EVENTS for l in seg
        )
        if not has_activity:
            continue
        # Skip segments that don't start with InitState (partial test runs
        # that were split mid-trace and can't be validated by the TLA+ spec)
        first_event = json.loads(seg[0])["event"]["name"]
        if first_event != "InitState":
            print(f"  SKIP nseg-{i} ({len(seg)} events, starts with {first_event})")
            continue
        # Skip segments where the bootstrap config references nodes not in
        # the trace. The TLA+ spec (BootstrappedConfig) uses the LAST
        # bootstrap event per node to derive the initial config.
        nids = set()
        last_bootstrap_conf = {}  # nid -> conf from last bootstrap event
        for l in seg:
            o = json.loads(l)
            nids.add(o["event"]["nid"])
            if o["event"]["name"] in BOOTSTRAP_EVENTS:
                last_bootstrap_conf[o["event"]["nid"]] = o["event"].get("conf", [[], []])
        # Derive the bootstrapped config for the first node (matches TLA+ TraceInitServer)
        first_nid = json.loads(seg[0])["event"]["nid"]
        if first_nid in last_bootstrap_conf:
            bootstrap_nids = set()
            for group in last_bootstrap_conf[first_nid]:
                for n in group:
                    bootstrap_nids.add(n)
            if not bootstrap_nids.issubset(nids):
                missing = bootstrap_nids - nids
                print(f"  SKIP nseg-{i} ({len(seg)} events, missing nodes {missing})")
                continue
        # Every node's first bootstrap event must come before its first
        # non-bootstrap event. TLA+ BootstrapLogIndicesForServer(i) requires
        # a bootstrap prefix for each node.
        first_bootstrap = {}
        first_non_bootstrap = {}
        for j, l in enumerate(seg):
            o = json.loads(l)
            nid = o["event"]["nid"]
            is_boot = o["event"]["name"] in BOOTSTRAP_EVENTS
            if is_boot and nid not in first_bootstrap:
                first_bootstrap[nid] = j
            if not is_boot and nid not in first_non_bootstrap:
                first_non_bootstrap[nid] = j
        bad_nodes = set()
        for nid in nids:
            if nid not in first_bootstrap:
                bad_nodes.add(nid)
            elif nid in first_non_bootstrap and first_bootstrap[nid] > first_non_bootstrap[nid]:
                bad_nodes.add(nid)
        if bad_nodes:
            print(f"  SKIP nseg-{i} ({len(seg)} events, nodes {bad_nodes} lack bootstrap prefix)")
            continue
        # Skip segments with ReceiveSnapshot but no BecomeLeader — the
        # snapshot came from a leader in another segment, so QuorumLogInv
        # can't be satisfied (only the receiver has the snapshot entries).
        event_names = [json.loads(l)["event"]["name"] for l in seg]
        if "ReceiveSnapshot" in event_names and "BecomeLeader" not in event_names:
            print(f"  SKIP nseg-{i} ({len(seg)} events, snapshot without election)")
            continue
        path = os.path.join(output_dir, f"nseg-{i}.ndjson")
        with open(path, "w") as f:
            f.write("\n".join(seg) + "\n")
        print(f"  Wrote {path} ({len(seg)} events)")


if __name__ == "__main__":
    main()
