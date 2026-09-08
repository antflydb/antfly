#!/usr/bin/env python3
"""Measure version-changing updates/deletes, then restore VectorDBBench truth.

This is a separate phase from the query recall measurements. A failed restore
fails qualification; never compare query recall against changed ground truth.
"""

import argparse
import json
import re
from pathlib import Path
import time

import httpx
import pyarrow.parquet as pq


def parse_batch_profiles(text, rows, batch):
    profiles = [dict(re.findall(r"(\w+)=([^ ]+)", line.split("antfly_bench_batch ", 1)[1]))
                for line in text.splitlines() if "antfly_bench_batch " in line]
    if (len(profiles) != (rows + batch - 1) // batch
            or sum(int(p["writes"]) + int(p["deletes"]) for p in profiles) != rows
            or sum(p["sync"] == "full_index" for p in profiles) != 1):
        raise RuntimeError("incomplete or unrelated batch profiles; sync timing cannot be attributed")
    return profiles


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--rows", type=int, default=2000)
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--server-log", type=Path, help="ANTFLY_BENCH_METRICS log for separate sync wait accounting")
    args = parser.parse_args()
    inputs = []
    parquet = pq.ParquetFile(args.dataset / "shuffle_train.parquet")
    for batch in parquet.iter_batches(batch_size=args.batch, columns=["id", "emb"]):
        inputs.extend(zip(batch.column(0).to_pylist(), batch.column(1).to_pylist()))
        if len(inputs) >= args.rows:
            break
    inputs = inputs[: args.rows]
    observations = []
    base = f"http://127.0.0.1:{args.port}/db/v1/tables/vdbbench"
    with httpx.Client(timeout=180) as client:

        def mutate(label, rows, delete=False, round_number=0):
            before = client.get(base)
            before.raise_for_status()
            log_offset = args.server_log.stat().st_size if args.server_log else 0
            started = time.perf_counter()
            for offset in range(0, len(rows), args.batch):
                selected = rows[offset : offset + args.batch]
                if delete:
                    payload = {"deletes": [f"key:{i}" for i, _ in selected]}
                else:
                    inserts = {}
                    for i, original in selected:
                        vector = list(original)
                        if round_number:
                            vector[(round_number - 1) % len(vector)] += (
                                0.01 * round_number
                            )
                        inserts[f"key:{i}"] = {
                            "id": i,
                            "metadata": i,
                            "vec_data": str(i),
                            "_embeddings": {"vec": vector},
                        }
                    payload = {"inserts": inserts}
                payload["sync_level"] = "full_index" if offset + args.batch >= len(rows) else "write"
                response = client.post(base + "/batch", json=payload)
                response.raise_for_status()
            mutations_finished = time.perf_counter()
            status = client.get(base)
            status.raise_for_status()
            status_finished = time.perf_counter()
            timing = {"mutation_and_sync_s": mutations_finished - started,
                      "status_read_s": status_finished - mutations_finished}
            if args.server_log:
                with args.server_log.open("rb") as log:
                    log.seek(log_offset)
                    profiles = parse_batch_profiles(log.read().decode(errors="replace"), len(rows), args.batch)
                sync_wait = sum(float(p["sync_wait_ms"]) for p in profiles) / 1000
                timing.update(index_sync_s=sync_wait,
                              mutation_s=max(0, mutations_finished - started - sync_wait),
                              sync_timing_source="server sync_wait_ms; integer milliseconds per batch",
                              batch_profiles=profiles)
            observations.append(
                {
                    "phase": label,
                    "rows": len(rows),
                    "elapsed_s": status_finished - started,
                    **timing,
                    "table_before": before.json(),
                    "table": status.json(),
                }
            )
            args.output.write_text(json.dumps(observations, indent=2) + "\n")

        for round_number in range(1, args.rounds + 1):
            mutate(f"update-{round_number}", inputs, round_number=round_number)
            mutate(f"delete-{round_number}", inputs[::2], delete=True)
            mutate(f"restore-{round_number}", inputs)


if __name__ == "__main__":
    main()
