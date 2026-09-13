#!/usr/bin/env python3
"""Inventory every file under a table/run data root, including journals.

Logical and filesystem-allocated bytes are distinct. Hard links count once in
totals. Embedded replay/transaction records remain included in primary bytes;
this file inventory cannot attribute individual keys inside an SSTable.
"""

import argparse
from collections import defaultdict
import json
from pathlib import Path


def inventory(root: Path) -> dict:
    groups = defaultdict(lambda: {"files": 0, "logical_bytes": 0, "allocated_bytes": 0})
    seen = set()
    files = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        stat = path.stat()
        identity = (stat.st_dev, stat.st_ino)
        if identity in seen:
            continue
        seen.add(identity)
        rel = path.relative_to(root)
        if "source-vectors" in rel.parts:
            owner = "source_vectors"
        elif "vector-blocks" in rel.parts:
            owner = "serving_vectors"
        elif any(
            "journal" in part or "replay" in part or "transaction" in part
            for part in rel.parts
        ):
            owner = "separate_journals"
        elif "indexes" in rel.parts:
            owner = "ann_and_other_indexes"
        else:
            owner = "primary_and_metadata"
        kind = (
            "directory"
            if path.name == "SOURCE_DIRECTORY"
            else "wal"
            if "wal" in path.name.lower() or path.suffix == ".afvw"
            else "persisted"
        )
        group = groups[f"{owner}.{kind}"]
        group["files"] += 1
        group["logical_bytes"] += stat.st_size
        group["allocated_bytes"] += stat.st_blocks * 512
        files.append(
            {
                "path": str(rel),
                "owner": owner,
                "kind": kind,
                "logical_bytes": stat.st_size,
                "allocated_bytes": stat.st_blocks * 512,
            }
        )
    return {
        "root": str(root.resolve()),
        "groups": dict(groups),
        "files": files,
        "total_logical_bytes": sum(g["logical_bytes"] for g in groups.values()),
        "total_allocated_bytes": sum(g["allocated_bytes"] for g in groups.values()),
        "includes_embedded_primary_journals": True,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    if not args.root.is_dir():
        parser.error("data root does not exist")
    args.output.write_text(json.dumps(inventory(args.root), indent=2) + "\n")
