#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compare completed live incremental/replay reconstruction and model reports.

Run the same binary, checkpoint and fixture with incremental_validation enabled
and disabled. Counts describe successful native reconstruction, excluding prefix
hashing and transport reads. Shared-host timings remain diagnostics.
"""

import argparse
import json
from pathlib import Path

import video_rolling_qualification


def validate(after, before, oracle):
    after_agreement = video_rolling_qualification.validate(after, oracle)
    before_agreement = video_rolling_qualification.validate(before, oracle)
    if after["backend"] != before["backend"]:
        raise ValueError("matched backend required")
    for new, old in zip(after["cases"], before["cases"]):
        if new["vector"] != old["vector"]:
            raise ValueError("incremental/replay model vector drift")
        if (
            new["coordinator_validation_walks"] != old["coordinator_validation_walks"]
            or new["coordinator_metadata_snapshots"]
            != old["coordinator_metadata_snapshots"]
        ):
            raise ValueError("matched coordinator operations required")
    if after["queries"] != before["queries"]:
        raise ValueError("incremental/replay query drift")
    work = {}
    for final in (5, 7):
        new, old = after["cases"][final], before["cases"][final]
        row = {}
        for field in (
            "coordinator_validation_packets",
            "coordinator_validation_payload_bytes",
        ):
            if not 0 < new[field] < old[field]:
                raise ValueError("native reconstruction work did not decrease")
            row[field] = {"replay": old[field], "incremental": new[field]}
        work[new["case"]] = row
    return {
        "status": "pass",
        "all_eight_vectors_exact_replay": True,
        "work": work,
        "incremental_reference_agreement": after_agreement,
        "replay_reference_agreement": before_agreement,
        "timing_scope": "Shared-host diagnostics; no sustained throughput or RSS claim.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--incremental", type=Path, required=True)
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = validate(
        json.loads(args.incremental.read_text()),
        json.loads(args.replay.read_text()),
        json.loads(args.reference.read_text()),
    )
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
