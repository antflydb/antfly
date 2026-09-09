#!/usr/bin/env python3
"""Keep qualification outside the native generation builder's disk reserve."""

import argparse
import json
import os
from pathlib import Path
import time

GIB = 1024**3
RAW_PAYLOAD_BYTES = {
    "Performance1536D50K": 50_000 * 1536 * 4,
    "Performance768D1M": 1_000_000 * 768 * 4,
}


def capacity_requirement(total_bytes, case=None):
    # ResourceManager's default policy; candidate growth is reserved separately.
    safety_floor = max(GIB, min(total_bytes // 20, 16 * GIB))
    # Fresh qualification includes load, churn, restart and reclamation clones.
    # This is a conservative experiment allowance, not a production limit.
    growth = GIB + 4 * RAW_PAYLOAD_BYTES[case] if case else 256 * 1024**2
    return safety_floor, growth


def observe(path, case=None):
    stat = os.statvfs(path)
    fragment = stat.f_frsize or stat.f_bsize
    total, available = stat.f_blocks * fragment, stat.f_bavail * fragment
    floor, growth = capacity_requirement(total, case)
    return {
        "observed_at": time.time(),
        "path": str(Path(path).resolve()),
        "capacity_bytes": total,
        "available_bytes": available,
        "safety_floor_bytes": floor,
        "growth_allowance_bytes": growth,
        "required_available_bytes": floor + growth,
        "sufficient": available >= floor + growth,
    }


def require_capacity(observation):
    if not observation["sufficient"]:
        raise RuntimeError(
            "qualification deferred for disk capacity: "
            f"{observation['available_bytes'] / GIB:.2f} GiB available; "
            f"{observation['required_available_bytes'] / GIB:.2f} GiB required "
            "including the native repair safety reserve. Free space before retrying; "
            "a backfill timeout here would not measure storage performance."
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--case", choices=RAW_PAYLOAD_BYTES)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = observe(args.path, args.case)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    try:
        require_capacity(result)
    except RuntimeError as exc:
        parser.exit(1, str(exc) + "\n")


if __name__ == "__main__":
    main()
