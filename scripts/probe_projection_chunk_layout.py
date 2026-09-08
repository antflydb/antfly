"""Replay actual projection batches against source-only single-copy chunk layouts.

This is a physical-work screen, NOT a live serving or recall qualification.
Inputs are authenticated source rows and actual read traces exported by the
Zig diagnostic. Training sees only source vectors, never queries/neighbors.
No files in the source database are changed. Each row gets one placement.
"""

import argparse
import hashlib
import json
from collections import OrderedDict
from pathlib import Path

import numpy as np
from threadpoolctl import threadpool_limits


def source_order(vectors, chunk_rows):
    """Balanced spherical splits: bounded four-iteration source-only trainer."""
    if vectors.ndim != 2 or not len(vectors) or chunk_rows < 1:
        raise ValueError("nonempty matrix and positive chunk rows required")
    if not np.isfinite(vectors).all():
        raise ValueError("nonfinite source vector")
    # Hints affect only placement; original payloads/residuals are unchanged.
    norms = np.linalg.norm(vectors, axis=1)
    normalized = vectors / np.maximum(norms[:, None], np.finfo(np.float32).tiny)
    result = []
    pending = [np.arange(len(vectors))]
    while pending:
        ids = pending.pop()
        if len(ids) <= chunk_rows:
            result.extend(sorted(ids.tolist()))
            continue
        rows = normalized[ids]
        left = rows[0]
        right = rows[np.argmin(rows @ left)]
        middle = len(ids) // 2
        for _ in range(4):
            order = np.lexsort((ids, rows @ (right - left)))
            left = rows[order[:middle]].mean(axis=0)
            right = rows[order[middle:]].mean(axis=0)
            left /= max(float(np.linalg.norm(left)), np.finfo(np.float32).tiny)
            right /= max(float(np.linalg.norm(right)), np.finfo(np.float32).tiny)
        pending.append(ids[order[middle:]])
        pending.append(ids[order[:middle]])
    return np.asarray(result, dtype=np.int64)


def packed_locations(order, lengths, chunk_bytes):
    if sorted(order) != list(range(len(lengths))) or chunk_bytes < 1:
        raise ValueError("layout must place every source row exactly once")
    locations = [None] * len(lengths)
    chunk = offset = 0
    for row in order:
        length = int(lengths[row])
        if length <= 0 or length > chunk_bytes:
            raise ValueError("projection does not fit chunk")
        if offset + length > chunk_bytes:
            chunk += 1
            offset = 0
        locations[row] = (chunk, offset, length)
        offset += length
    return locations


def replay(batches, locations, page_bytes=16384, cache_bytes=0):
    """Count bounded sequential read spans; LRU is an optimistic cache model.

    Each batch is separately scheduled. Cross-page vectors are split into
    touched-page spans. Unlike a full-page-only model, cold misses fetch only
    the selected range; cache admission charges/reads the complete page.
    """
    if page_bytes < 1 or cache_bytes < 0:
        raise ValueError("invalid page/cache budget")
    cache = OrderedDict()
    capacity = cache_bytes // page_bytes
    reads = physical_bytes = hits = requested_bytes = 0
    per_batch = []
    for batch in batches:
        spans = {}
        for row in batch:
            file, offset, length = locations[row]
            requested_bytes += length
            while length:
                page = offset // page_bytes
                start = offset % page_bytes
                count = min(length, page_bytes - start)
                key = (file, page)
                old_start, old_end = spans.get(key, (page_bytes, 0))
                spans[key] = (min(start, old_start), max(start + count, old_end))
                offset += count
                length -= count
        batch_reads = 0
        for key, (start, end) in sorted(spans.items()):
            if key in cache:
                hits += 1
                cache.move_to_end(key)
                continue
            reads += 1
            batch_reads += 1
            physical_bytes += page_bytes if capacity else end - start
            if capacity:
                cache[key] = True
                if len(cache) > capacity:
                    cache.popitem(last=False)
        per_batch.append(batch_reads)
    return {
        "physical_reads": reads,
        "physical_bytes": physical_bytes,
        "requested_bytes": requested_bytes,
        "page_hits": hits,
        "reads_per_batch": float(np.mean(per_batch)) if per_batch else 0,
        "cache_model": "bounded LRU; optimistic, not live direct-mapped cache",
        "resident_limit_bytes": capacity * page_bytes,
    }


def replay_contiguous(batches, locations):
    """Join adjacent/overlapping spans, never fetch gaps or duplicate bytes.

    Packed file IDs denote bounded chunks. This proposed scheduler is separate
    from today's scalar reads and the page-cache model above.
    """
    reads = physical_bytes = 0
    for batch in batches:
        current_file = None
        begin = end = 0
        for file, offset, length in sorted(locations[row] for row in batch):
            if file == current_file and offset <= end:
                end = max(end, offset + length)
                continue
            if current_file is not None:
                reads += 1
                physical_bytes += end - begin
            current_file, begin, end = file, offset, offset + length
        if current_file is not None:
            reads += 1
            physical_bytes += end - begin
    return {"physical_reads": reads, "physical_bytes": physical_bytes}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "source", type=Path, help="NPZ: vectors, lengths, file_ids, offsets"
    )
    parser.add_argument("trace", type=Path, help="JSON: batches of source-row IDs")
    parser.add_argument("output", type=Path)
    parser.add_argument("--chunk-bytes", type=int, default=64 * 1024)
    args = parser.parse_args()
    hashes = {
        str(p.resolve()): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (args.source, args.trace, Path(__file__))
    }
    source = np.load(args.source, allow_pickle=False)
    vectors, lengths = source["vectors"], source["lengths"]
    batches = json.loads(args.trace.read_text())["batches"]
    if not batches or any(
        not b or any(type(i) is not int or not 0 <= i < len(vectors) for i in b)
        for b in batches
    ):
        raise ValueError("trace must contain nonempty real in-range request batches")
    current = list(
        zip(source["file_ids"].tolist(), source["offsets"].tolist(), lengths.tolist())
    )
    with threadpool_limits(limits=1):
        order = source_order(
            vectors, max(1, min(16384, args.chunk_bytes) // int(max(lengths)))
        )
    packed = packed_locations(order.tolist(), lengths, args.chunk_bytes)
    results = {}
    for name, locations in (("current", current), ("packed", packed)):
        results[f"{name}_contiguous"] = replay_contiguous(batches, locations)
        for cache_bytes in (0, 32 * 1024 * 1024):
            results[f"{name}_cache_{cache_bytes}"] = replay(
                batches, locations, cache_bytes=cache_bytes
            )
    results["current_scalar"] = {
        "physical_reads": sum(map(len, batches)),
        "physical_bytes": sum(current[row][2] for batch in batches for row in batch),
    }
    baseline, candidate = results["current_scalar"], results["packed_contiguous"]
    passes = (
        candidate["physical_reads"] <= 0.8 * baseline["physical_reads"]
        and candidate["physical_bytes"] <= baseline["physical_bytes"]
    )
    if any(
        hashlib.sha256(Path(p).read_bytes()).hexdigest() != h for p, h in hashes.items()
    ):
        raise RuntimeError("input changed during experiment")
    report = {
        "diagnostic_only": True,
        "inputs_sha256": hashes,
        "training": "source vectors only; never queries or neighbor IDs",
        "source_rows": len(vectors),
        "single_copy_payload_bytes": int(sum(lengths)),
        "chunk_bytes": args.chunk_bytes,
        "results": results,
        "physical_work_gate_passed": passes,
        "caveat": "Excludes residual/directory I/O and metadata. Does not establish serving latency, recall, durability, or RSS.",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
