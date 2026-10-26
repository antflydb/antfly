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

"""Seal and inspect native Metal whole-head diagnostics using only stdlib.

This tool never labels a head diagnostic as serving or total-context memory
qualification. Training authority must explicitly include the query capture.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import struct
from context_release_build_adapter import validate as validate_build

HEADER = struct.Struct("<8s12I2f")
SCHEMA = "antfly.context.native.head-input.v2"
RECIPE = "native-fp64-residual-omp-active-nnls-ridge-qr-whole-head-v2"


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def checked(path, expected, size=None):
    """Hash the same regular-file bytes consumed, with no final symlink."""
    require(isinstance(expected, str) and len(expected) == 64, "missing SHA-256")
    int(expected, 16)
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode), "input is not a regular file")
        require(before.st_size <= 512 * 1024 * 1024, "input exceeds bounded file size")
        if size is not None:
            require(before.st_size == size, "input byte count mismatch")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            data = stream.read()
        after = os.fstat(fd)
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                "input changed while reading")
        require(sha(data) == expected, "input SHA-256 mismatch")
        return data
    finally:
        os.close(fd)


def local(base, name):
    require(isinstance(name, str) and name and Path(name).name == name, "artifact must be a local filename")
    return base / name


def select_head_rows(data, rows, heads, dim, kv_head, element_bytes):
    """Deinterleave canonical [logical row][KV head][head dim] bytes."""
    require(type(heads) is int and heads > 0 and type(kv_head) is int and 0 <= kv_head < heads,
            "invalid physical KV head")
    head_bytes = dim * element_bytes
    row_bytes = heads * head_bytes
    require(len(data) == rows * row_bytes, "source tensor byte count mismatch")
    return b"".join(data[row * row_bytes + kv_head * head_bytes:
                         row * row_bytes + (kv_head + 1) * head_bytes]
                    for row in range(rows))


def write_new(path, data):
    with Path(path).open("xb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def authorized_head(*, source_index, source_sha256, query_manifest, query_sha256,
                    training_authority, authority_sha256, source_build_receipt, owner,
                    continuation_rows=32, retained_row_cap=6144, staging_mib=512,
                    min_recent_rows=2048, kv_head=0):
    """Bind complete KV to an authorized training document before reading Q."""
    source_index, query_manifest, training_authority = map(Path,
        (source_index, query_manifest, training_authority))
    # Authority and metadata are checked before any tensor is opened.
    authority = json.loads(checked(training_authority, authority_sha256))
    require(authority.get("training_source_authority") is True and
            authority.get("held_tensors_loaded") is False and
            authority.get("release_authority") is False, "source has no training authority")
    cells = [cell for cell in authority.get("cells", [])
             if cell.get("owner_layer") == owner and cell.get("manifest", {}).get("sha256") == query_sha256]
    require(len(cells) == 1, "query manifest is not uniquely covered by training authority")
    query = json.loads(checked(query_manifest, query_sha256))
    require(query.get("schema_version") == 2 and query.get("purpose") == "calibration", "query source is not a calibration capture")
    index = json.loads(checked(source_index, source_sha256))
    require(index.get("schema") == "antfly.context.native.full-head-source.v2" and
            index.get("diagnostic_only") is True and index.get("serving_qualified") is False,
            "not a full-head diagnostic source")
    build = validate_build(Path(source_build_receipt))
    require(build["receipt_sha256"] == index["source_build_sha256"] and
            build["binary_sha256"] == index["binary_sha256"], "full-head source build provenance mismatch")
    matches = [entry for entry in index.get("descriptors", []) if entry.get("owner_layer") == owner]
    require(len(matches) == 1 and matches[0].get("block_start") == 0, "missing or duplicate complete owner")
    descriptor = matches[0]
    head = json.loads(checked(local(source_index.parent, descriptor["path"]), descriptor["sha256"]))
    capture, model = query["capture"], query["model"]
    require(head.get("prefix_storage") == "packed_exact" and head.get("tensor_dtype") == "f16_le" and
            head.get("tensor_order") == "logical_row_kv_head_head_dim", "unsupported source KV representation")
    require(head["owner_layer"] == capture["owner_layer"] == owner and head["block_start"] == 0 and
            head["rows"] == index["prompt_tokens"] == head["prompt"]["tokens"], "source is not a complete head")
    require(head["prompt"]["sha256"] == index["prompt_tokens_sha256"], "source prompt identity mismatch")
    require(head["document"] == query["document"], "query and KV documents differ")
    for key in ("artifact", "tokenizer", "template", "layout"):
        expected = model["sha256" if key == "artifact" else key + "_sha256"]
        require(head["loaded_identity"][key + "_sha256"] == expected, "loaded model identity mismatch")
    require(index["artifact_sha256"] == model["sha256"] and
            head["loaded_identity"]["load_generation"] == index["load_generation"] and
            head["provenance"]["binary_sha256"] == index["binary_sha256"] and
            head["provenance"]["source_build_sha256"] == index["source_build_sha256"], "source provenance mismatch")
    heads = head.get("heads")
    require(type(heads) is int and 0 < heads <= 64 and capture.get("heads") == heads and
            head["dim"] == capture["dim"] and type(kv_head) is int and 0 <= kv_head < heads,
            "unsupported head geometry")
    require(capture["query_origin"] == "multi_branch_v2" and
            capture["query_sampling"] == "source_uniform_partition_v3", "unsupported real-query provenance")
    strata = capture["strata"]
    tuples = [(s["consumer_layer"], s["query_head"], s["kv_head"]) for s in strata]
    expected_tuples = [(s["consumer_layer"], s["query_head"], s["kv_head"]) for s in head["strata"]]
    require(tuples and len(set(tuples)) == len(tuples) and tuples == expected_tuples,
            "missing, duplicate, or reordered physical consumer")
    train, held, dim, rows = (capture["training_rows"], capture["validation_rows"], head["dim"], head["rows"])
    by_head = [[s for s in strata if s.get("kv_head") == h] for h in range(heads)]
    counts = {len(part) for part in by_head}
    require(len(counts) == 1 and next(iter(counts)) > 0 and
            train > 0 and held > 0 and train + held <= 4096 and 0 < dim <= 512 and dim % 2 == 0,
            "invalid or unbalanced queries")
    strata_per_head = next(iter(counts))
    require(train % strata_per_head == held % strata_per_head == 0, "invalid or unbalanced queries")
    train_group, held_group = train // strata_per_head, held // strata_per_head
    for physical_head, part in enumerate(by_head):
        for i, s in enumerate(part):
            require(s["kv_head"] == physical_head and
                    s["training_start"] == i * train_group and s["training_count"] == train_group and
                    s["validation_start"] == i * held_group and s["validation_count"] == held_group,
                    "consumer rows are not balanced head-local contiguous strata")
            ranges = s["query_source_ranges"]
            require({r["source_id"] for r in ranges["training"]}.isdisjoint(
                    {r["source_id"] for r in ranges["validation"]}),
                    "training and validation sources overlap")
    source_blocks = index["eligible_block_starts"]
    require(source_blocks and source_blocks == list(range(source_blocks[0], source_blocks[-1] + 512, 512)),
            "first native head profile requires contiguous eligible 512-row blocks")
    require(type(min_recent_rows) is int and 0 < min_recent_rows < rows, "invalid minimum recent window")
    blocks = [start for start in source_blocks if start + 512 <= rows - min_recent_rows]
    require(blocks, "no complete eligible block outside the protected recent window")
    prefix, recent = blocks[0], rows - blocks[-1] - 512
    require(prefix >= 0 and recent > 0 and 0 < continuation_rows < retained_row_cap and
            retained_row_cap <= 131072 and 0 < staging_mib <= 1024, "invalid protected rows or budget")
    require(capture["block_start"] in source_blocks and capture["rows"] == 512, "query witness has wrong block geometry")
    require(math.isfinite(capture["scale"]) and capture["scale"] > 0 and
            struct.pack("<f", capture["scale"]) == struct.pack("<f", head["scale"]), "attention scale mismatch")
    row_bytes = heads * dim * 2
    all_keys = checked(local(source_index.parent, head["keys"]["path"]), head["keys"]["sha256"], rows * row_bytes)
    all_values = checked(local(source_index.parent, head["values"]["path"]), head["values"]["sha256"], rows * row_bytes)
    # Schema v2 calibration captures store original K/V as little-endian f16.
    # Compare the exact stored bytes before reading Q; widening would check a
    # different representation from the capture's sealed tensor hashes.
    start = capture["block_start"] * row_bytes
    for name, data in (("keys", all_keys), ("values", all_values)):
        witness = query["tensors"][name]
        require(witness["bytes"] == 512 * row_bytes and
                sha(data[start:start + 512 * row_bytes]) == witness["sha256"],
                "query capture and full-head KV witness differ")
    keys = select_head_rows(all_keys, rows, heads, dim, kv_head, 2)
    values = select_head_rows(all_values, rows, heads, dim, kv_head, 2)
    return index, head, query, keys, values, build, prefix, recent, heads, by_head[kv_head]


def prepare(*, source_index, source_sha256, query_manifest, query_sha256,
            training_authority, authority_sha256, source_build_receipt, owner, output,
            continuation_rows=32, retained_row_cap=6144, staging_mib=512,
            min_recent_rows=2048, kv_head=0):
    index, head, query, keys, values, build, prefix, recent, source_heads, selected_strata = authorized_head(
        source_index=source_index, source_sha256=source_sha256, query_manifest=query_manifest,
        query_sha256=query_sha256, training_authority=training_authority, authority_sha256=authority_sha256,
        source_build_receipt=source_build_receipt, owner=owner, continuation_rows=continuation_rows,
        retained_row_cap=retained_row_cap, staging_mib=staging_mib,
        min_recent_rows=min_recent_rows, kv_head=kv_head)
    source_index, query_manifest, training_authority, output = map(Path,
        (source_index, query_manifest, training_authority, output))
    capture = query["capture"]
    train, held, dim, rows = capture["training_rows"], capture["validation_rows"], head["dim"], head["rows"]
    strata = selected_strata
    held_group = held // len(strata)
    q_entry = query["tensors"]["queries"]
    total_queries = train + held + capture.get("evaluation_rows", 0)
    require(q_entry["bytes"] == source_heads * total_queries * dim * 4,
            "query tensor geometry mismatch")
    all_queries = checked(local(query_manifest.parent, q_entry["path"]), q_entry["sha256"], q_entry["bytes"])
    plane_bytes = total_queries * dim * 4
    queries = all_queries[kv_head * plane_bytes:(kv_head + 1) * plane_bytes]
    # Copy bytes only. Selection/fitting never receives held query rows.
    payload = HEADER.pack(b"AMHEAD02", rows, dim, train, held, held_group, prefix, recent,
                          4, 2, continuation_rows, retained_row_cap, staging_mib, capture["scale"], 0)
    payload += keys + values + queries[:(train + held) * dim * 4]
    manifest = {
        "schema": SCHEMA, "diagnostic_only": True, "recipe": RECIPE,
        "evaluation_role": "internal_development", "fresh_unseen_qualified": False,
        "held_tensor_bytes_copied": True, "held_numerics_used_for_selection": False,
        "serving_qualified": False, "full_context_memory_qualified": False,
        "preparer_sha256": sha(Path(__file__).read_bytes()),
        "kv_witness_dtype": "f16_le",
        "source_tensor_order": "logical_row_kv_head_head_dim",
        "source_kv_heads": source_heads, "kv_head": kv_head,
        "input": {"path": str(output), "bytes": len(payload), "sha256": sha(payload)},
        "sources": {"full_head_index": {"path": str(source_index), "sha256": source_sha256},
                    "query_manifest": {"path": str(query_manifest), "sha256": query_sha256},
                    "source_build_receipt": {"path": str(source_build_receipt), "sha256": build["receipt_sha256"]},
                    "training_authority": {"path": str(training_authority), "sha256": authority_sha256}},
        "owner_layer": owner, "consumer_groups": len(strata),
        "source_rows": rows, "head_dim": dim, "training_rows": train, "validation_rows": held,
        "prefix_rows": prefix, "recent_rows": recent, "continuation_rows": continuation_rows,
        "minimum_recent_rows": min_recent_rows,
        "retained_row_cap": retained_row_cap, "retention_divisors": [4, 3, 2],
        "limits": {"max_mass_error": 0.05, "max_output_rmse": 0.10, "max_consumer_output_rmse": 0.10},
    }
    manifest_path = Path(str(output) + ".json")
    require(not output.exists() and not manifest_path.exists(), "output already exists")
    write_new(output, payload)
    write_new(manifest_path, (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode())
    return manifest


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("source-index", "source-sha256", "query-manifest", "query-sha256",
                 "training-authority", "authority-sha256", "source-build-receipt", "output"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--owner", type=int, required=True)
    p.add_argument("--continuation-rows", type=int, default=32)
    p.add_argument("--retained-row-cap", type=int, default=6144)
    p.add_argument("--staging-mib", type=int, default=512)
    p.add_argument("--min-recent-rows", type=int, default=2048)
    p.add_argument("--kv-head", type=int, default=0)
    args = vars(p.parse_args())
    print(json.dumps(prepare(**args), sort_keys=True))


if __name__ == "__main__":
    main()
