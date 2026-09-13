#!/usr/bin/env python3
"""Prepare an isolated VectorDBBench client with explicit table storage selection.

The source checkout and its virtualenv are left intact. Fail closed if the
adapter's creation contract changes: an A/B run must never silently use default
storage for both arms.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil


def prepare(source: Path, output: Path) -> None:
    adapter = Path("vectordb_bench/backend/clients/antfly/antfly.py")
    original = (source / adapter).read_text()
    before = 'r = client.post(f"/tables/{self.collection_name}", json={"num_shards": num_shards})'
    after = """mode = os.environ["ANTFLY_VDBBENCH_DENSE_EMBEDDINGS"]
                if mode not in ("primary_lsm", "vector_store"):
                    raise ValueError(f"Invalid dense embedding storage: {mode}")
                r = client.post(f"/tables/{self.collection_name}", json={
                    "num_shards": num_shards,
                    "storage": {"dense_embeddings": mode},
                })"""
    check_before = "            self._wait_for_shard_ready(client)"
    check_after = """            effective = self._get_table_status_or_none(client)
            actual = (effective or {}).get("storage", {}).get("dense_embeddings")
            expected = os.environ["ANTFLY_VDBBENCH_DENSE_EMBEDDINGS"]
            if actual != expected:
                raise RuntimeError(f"Storage mismatch: requested {expected}, got {actual}")
            self._wait_for_shard_ready(client)"""
    if original.count(before) != 1 or original.count(check_before) != 1:
        raise RuntimeError(
            "VectorDBBench adapter changed; review the creation/status contract"
        )
    patched = original.replace(before, after).replace(check_before, check_after)
    compile(patched, str(adapter), "exec")
    output.mkdir(parents=True, exist_ok=False)
    shutil.copytree(
        source / "vectordb_bench",
        output / "vectordb_bench",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.log"),
    )
    (output / adapter).write_text(patched)
    (output / "adapter-receipt.json").write_text(
        json.dumps(
            {
                "source": str(source.resolve()),
                "original_sha256": hashlib.sha256(original.encode()).hexdigest(),
                "patched_sha256": hashlib.sha256(patched.encode()).hexdigest(),
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    prepare(args.source, args.output)
