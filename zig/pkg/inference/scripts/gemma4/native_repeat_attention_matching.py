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

"""Seal a distinct, history-only representative replay development input.

The old five-source validation population keeps its original role. This adapter
does not read its query tensor. New train/held rows come from disjoint positions
in the captured representative replay and never confer serving authority.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import struct

import native_attention_matching as native
import native_repeat_training_authority as repeat_authority

require = native.require


def held_position(position):
    value = position // 4
    value = ((value ^ (value >> 16)) * 0x7FEB352D) & 0xFFFFFFFF
    value = ((value ^ (value >> 15)) * 0x846CA68B) & 0xFFFFFFFF
    value ^= value >> 16
    return position % 4 == value % 4


def expected_positions(start, rows, held, count):
    population = [p for p in range(start, start + rows) if held_position(p) == held]
    require(type(count) is int and 0 < count <= len(population), "invalid repeat sampling quota")
    if count == 1:
        return [population[(len(population) - 1) // 2]]
    return [population[i * (len(population) - 1) // (count - 1)] for i in range(count)]


def tensor(root, ref, size):
    require(type(ref) is dict and ref.get("bytes") == size, "repeat tensor byte count mismatch")
    return native.checked(native.local(root, ref["path"]), ref["sha256"], size)


def capture_binding(resource_path, resource_sha256, repeat_source, repeat_sha256,
                    repeat, source_index, capture_inputs, build):
    resource = json.loads(native.checked(resource_path, resource_sha256))
    require(resource.get("pass") is True and resource.get("returncode") == 0 and
            resource.get("violation") is None and resource.get("swapout_growth_mib") == 0 and
            resource.get("remaining_child_group_live_processes") == 0,
            "repeat capture did not complete under clean resource guards")
    limits = resource.get("limits", {})
    require(0 < limits.get("max_rss_mib", 0) <= 8192 and limits.get("max_swap_growth_mib") == 0 and
            limits.get("min_free_percent", 0) >= 15 and limits.get("min_disk_free_mib", 0) >= 4096,
            "repeat capture limits changed")
    command = resource.get("command", [])
    require(len(command) >= 4 and command[0] == "/usr/bin/env" and
            command[-3:] == [str(build["binary_path"]), "--test-filter",
                             "context Metal installed Gemma production query campaign capture"],
            "repeat capture executable mismatch")
    assignments = command[1:-3]
    require(all(isinstance(item, str) and "=" in item for item in assignments), "invalid capture environment")
    env = dict(item.split("=", 1) for item in assignments)
    require(len(env) == len(assignments), "duplicate capture environment")
    for key in ("MTL_DEBUG_LAYER", "ANTFLY_CONTEXT_CAMPAIGN_CAPTURE",
                "ANTFLY_CONTEXT_CAPTURE_NATIVE_FULL_HEADS", "ANTFLY_CONTEXT_CAPTURE_NATIVE_REPEAT"):
        require(env.get(key) == "1", "not a native representative replay capture")
    require(env.get("ANTFLY_CONTEXT_CAPTURE_NATIVE_PREFILL_ONLY", "0") == "0" and
            env.get("ANTFLY_CONTEXT_CAMPAIGN_PEAK_BYTES") == "8589934592" and
            env.get("ANTFLY_CONTEXT_CAPTURE_CAMPAIGN_DIR") == str(source_index.parent) and
            env.get("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST") == capture_inputs.name and
            env.get("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST_SHA256") == repeat["capture_inputs_manifest_sha256"],
            "repeat capture input binding mismatch")
    stem = env.get("ANTFLY_CONTEXT_CAMPAIGN_CAPTURE_STEM", "")
    require(stem and repeat["full_head_source_index"]["path"] == stem + "-native-head-index.json",
            "repeat capture output binding mismatch")
    expected_repeat_name = stem + "-native-repeat-index.json"
    require(repeat_source.name == expected_repeat_name,
            "repeat capture output binding mismatch")
    receipt_name = env.get("ANTFLY_CONTEXT_SOURCE_BUILD_RECEIPT", "")
    native.checked(native.local(source_index.parent, receipt_name), build["receipt_sha256"])
    stderr_path = Path(resource_path).with_suffix(".stderr")
    stdout_path = Path(resource_path).with_suffix(".stdout")
    stderr = stderr_path.read_bytes()
    stdout = stdout_path.read_bytes()
    artifact_record = ("context_native_repeat_artifact: index=" + expected_repeat_name +
                       " sha256=" + repeat_sha256).encode()
    records = [line for raw in (stdout, stderr) for line in raw.splitlines()
               if line.startswith(b"context_native_repeat_artifact:")]
    require(b"Metal API Validation Enabled" in stderr and
            b"context_native_repeat_capture: repeat_tokens=512 original_unchanged=true source_kv_readback_bytes=0 serving_qualified=false" in stderr and
            b"1 selected; 1 passed; 0 skipped." in stderr and records == [artifact_record],
            "repeat capture completion evidence missing")
    return native.sha(stdout), native.sha(stderr)


def _prepare_legacy(*, repeat_source, repeat_sha256, capture_inputs, capture_resource, resource_sha256,
                    source_index, source_sha256, query_manifest, query_sha256, training_authority,
                    authority_sha256, source_build_receipt, owner, output, continuation_rows=32,
                    retained_row_cap=6144, staging_mib=512, min_recent_rows=2048, kv_head=0):
    repeat_source, source_index, capture_inputs, output = map(Path,
        (repeat_source, source_index, capture_inputs, output))
    repeat = json.loads(native.checked(repeat_source, repeat_sha256))
    schema = repeat.get("schema")
    require(schema in {"antfly.context.native.repeat-source.v1", "antfly.context.native.repeat-source.v2"} and
            repeat.get("diagnostic_only") is True and repeat.get("evaluation_role") == "internal_development" and
            repeat.get("serving_qualified") is False and repeat.get("fresh_unseen_qualified") is False and
            repeat.get("query_sampling") == "representative_repeat_partition_v1" and
            repeat.get("selection") == "history_only_cuda_study_v1" and
            repeat.get("position_partition") == "hashed_quartet_v1" and
            repeat.get("source_kv_readback_bytes") == 0, "unsupported representative replay protocol")
    require(repeat_source.parent == source_index.parent == capture_inputs.parent and
            repeat["full_head_source_index"] == {"path": source_index.name, "sha256": source_sha256},
            "repeat and complete head source differ")
    index, head, old_query, keys, values, build, prefix, recent, source_heads, selected_strata = native.authorized_head(
        source_index=source_index, source_sha256=source_sha256, query_manifest=query_manifest,
        query_sha256=query_sha256, training_authority=training_authority, authority_sha256=authority_sha256,
        source_build_receipt=source_build_receipt, owner=owner, continuation_rows=continuation_rows,
        retained_row_cap=retained_row_cap, staging_mib=staging_mib,
        min_recent_rows=min_recent_rows, kv_head=kv_head)
    require(repeat["binary_sha256"] == index["binary_sha256"] and
            repeat["source_build_sha256"] == index["source_build_sha256"] and
            repeat["load_generation"] == index["load_generation"] and
            repeat["repeat_start"] == head["rows"] and repeat["repeat_rows"] == 512,
            "repeat loaded source provenance mismatch")
    stdout_sha, stderr_sha = capture_binding(capture_resource, resource_sha256, repeat_source,
        repeat_sha256, repeat, source_index, capture_inputs, build)
    inputs = json.loads(native.checked(capture_inputs, repeat["capture_inputs_manifest_sha256"]))
    require(inputs.get("completed") is True and inputs.get("schema_version") == 1 and
            inputs.get("stage") == "context_numerical_capture_inputs" and
            inputs["loaded_identity"] == head["loaded_identity"] and inputs["provenance"] == head["provenance"] and
            inputs["prompt"]["sha256"] == index["prompt_tokens_sha256"] and
            inputs["prompt"]["count"] == head["rows"], "repeat prompt provenance mismatch")
    prompt_data = native.checked(native.local(capture_inputs.parent, inputs["prompt"]["path"]),
                                 inputs["prompt"]["sha256"], head["rows"] * 8)
    prompt = [v for (v,) in struct.iter_unpack("<q", prompt_data)]
    selected = [v for (v,) in struct.iter_unpack("<q", tensor(repeat_source.parent, repeat["repeat_tokens"], 512 * 8))]
    iterator = iter(prompt)
    require(all(any(candidate == value for candidate in iterator) for value in selected) and
            selected[-384:] == prompt[-384:], "repeat is not a history subsequence with exact recent suffix")
    def entry_head(entry):
        return 0 if schema.endswith(".v1") else entry.get("kv_head")
    owners = [entry for entry in repeat["owners"]
              if entry.get("owner_layer") == owner and entry_head(entry) == kv_head]
    require(len(owners) == 1, "missing or duplicate repeat owner")
    captured = owners[0]
    captured_tuples = [(s.get("consumer_layer"), s.get("query_head"), s.get("kv_head"))
                       for s in captured.get("strata", [])]
    selected_tuples = [(s["consumer_layer"], s["query_head"], s["kv_head"])
                       for s in selected_strata]
    require((schema.endswith(".v1") and source_heads == 1 and kv_head == 0 or
             schema.endswith(".v2") and captured.get("source_heads") == source_heads and
             captured.get("kv_head") == kv_head) and
            captured_tuples == selected_tuples and captured["head_dim"] == head["dim"],
            "repeat consumer coverage differs")
    groups, dim = len(selected_strata), head["dim"]
    train, held = captured["training_rows"], captured["validation_rows"]
    require(all(type(n) is int and n > 0 for n in (groups, train, held)) and train + held <= 4096 and
            train == groups * captured["training_group_rows"] and held == groups * captured["validation_group_rows"],
            "invalid repeat query geometry")
    for name, rows, held_partition in (("training", train, False), ("validation", held, True)):
        group_rows = captured[name + "_group_rows"]
        positions = [v for (v,) in struct.iter_unpack("<I", tensor(repeat_source.parent, captured[name + "_positions"], rows * 4))]
        expected = expected_positions(head["rows"], 512, held_partition, group_rows)
        require(all(positions[g * group_rows:(g + 1) * group_rows] == expected for g in range(groups)),
                "repeat positions violate the disjoint uniform partition")
    # Query bytes are opened only after all role, source, consumer and position
    # checks. The old five-source Q tensor is deliberately never opened.
    training = tensor(repeat_source.parent, captured["training"], train * dim * 4)
    validation = tensor(repeat_source.parent, captured["validation"], held * dim * 4)
    payload = native.HEADER.pack(b"AMHEAD02", head["rows"], dim, train, held, captured["validation_group_rows"],
        prefix, recent, 4, 2, continuation_rows, retained_row_cap, staging_mib, head["scale"], 0)
    payload += keys + values + training + validation
    manifest = {
        "schema": native.SCHEMA, "recipe": native.RECIPE, "diagnostic_only": True,
        "evaluation_role": "internal_development", "query_sampling": repeat["query_sampling"],
        "fresh_unseen_qualified": False, "serving_qualified": False, "full_context_memory_qualified": False,
        "old_validation_queries_used_for_fit": False, "held_numerics_used_for_selection": False,
        "preparer_sha256": native.sha(Path(__file__).read_bytes()),
        "source_preparer_sha256": native.sha(Path(native.__file__).read_bytes()),
        "source_tensor_order": "logical_row_kv_head_head_dim",
        "source_kv_heads": source_heads, "kv_head": kv_head,
        "input": {"path": str(output), "bytes": len(payload), "sha256": native.sha(payload)},
        "owner_layer": owner, "consumer_groups": groups, "head_dim": dim, "source_rows": head["rows"],
        "training_rows": train, "validation_rows": held, "prefix_rows": prefix, "recent_rows": recent,
        "minimum_recent_rows": min_recent_rows, "continuation_rows": continuation_rows,
        "retained_row_cap": retained_row_cap, "retention_divisors": [4, 3, 2],
        "limits": {"max_mass_error": 0.05, "max_output_rmse": 0.10, "max_consumer_output_rmse": 0.10},
        "sources": {name: {"path": str(path), "sha256": digest} for name, path, digest in (
            ("repeat", repeat_source, repeat_sha256), ("full_head_index", source_index, source_sha256),
            ("training_authority", training_authority, authority_sha256), ("training_source_witness", query_manifest, query_sha256),
            ("source_build_receipt", source_build_receipt, build["receipt_sha256"]),
            ("capture_inputs", capture_inputs, repeat["capture_inputs_manifest_sha256"]),
            ("capture_resource", capture_resource, resource_sha256),
            ("capture_stdout", Path(capture_resource).with_suffix(".stdout"), stdout_sha),
            ("capture_stderr", Path(capture_resource).with_suffix(".stderr"), stderr_sha))},
    }
    manifest_path = Path(str(output) + ".json")
    require(not output.exists() and not manifest_path.exists(), "output already exists")
    native.write_new(output, payload)
    native.write_new(manifest_path, (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode())
    return manifest


def _protected_rows(index, head, min_recent_rows, continuation_rows,
                    retained_row_cap, staging_mib):
    rows = head["rows"]
    source_blocks = index["eligible_block_starts"]
    require(type(source_blocks) is list and source_blocks and
            all(type(start) is int and 0 <= start and start + 512 <= rows
                for start in source_blocks) and
            source_blocks == list(range(source_blocks[0], source_blocks[-1] + 512, 512)),
            "first native head profile requires contiguous eligible 512-row blocks")
    require(type(min_recent_rows) is int and 0 < min_recent_rows < rows,
            "invalid minimum recent window")
    blocks = [start for start in source_blocks if start + 512 <= rows - min_recent_rows]
    require(blocks, "no complete eligible block outside the protected recent window")
    prefix, recent = blocks[0], rows - blocks[-1] - 512
    require(prefix >= 0 and recent > 0 and 0 < continuation_rows < retained_row_cap and
            retained_row_cap <= 131072 and 0 < staging_mib <= 1024,
            "invalid protected rows or budget")
    return prefix, recent


def _prepare_authority(*, repeat_training_authority, repeat_authority_sha256,
                       owner, output, continuation_rows=32, retained_row_cap=6144,
                       staging_mib=512, min_recent_rows=2048, kv_head=0):
    """Prepare one physical head from a predeclared same-run authority graph."""
    binding = repeat_authority.validate_authority(
        repeat_training_authority, repeat_authority_sha256)
    coordinate = (owner, kv_head)
    require(type(owner) is int and type(kv_head) is int and
            coordinate in binding["descriptors"],
            "requested physical head lacks repeat training authority")
    selected = binding["descriptors"][coordinate]
    index, head = binding["source_index"], selected["source_descriptor"]
    captured = selected["repeat_owner"]
    heads, dim, rows = head["heads"], head["dim"], head["rows"]
    require(type(heads) is int and 0 < heads <= 64 and
            type(dim) is int and 0 < dim <= 512 and dim % 2 == 0 and
            type(rows) is int and 512 < rows <= 131072 and
            0 <= kv_head < heads and captured["source_heads"] == heads and
            captured["kv_head"] == kv_head and captured["owner_layer"] == owner,
            "authorized physical head geometry differs")
    tuples = [(s["consumer_layer"], s["query_head"], s["kv_head"])
              for s in head["strata"]]
    require(tuples and len(set(tuples)) == len(tuples),
            "missing or duplicate physical consumer")
    by_head = [[s for s in head["strata"] if s["kv_head"] == h]
               for h in range(heads)]
    counts = {len(part) for part in by_head}
    require(len(counts) == 1 and next(iter(counts)) > 0 and
            captured["strata"] == by_head[kv_head],
            "repeat consumer coverage differs")
    groups = len(by_head[kv_head])
    train, held = captured["training_rows"], captured["validation_rows"]
    train_group = captured["training_group_rows"]
    held_group = captured["validation_group_rows"]
    require(all(type(value) is int and value > 0
                for value in (groups, train, held, train_group, held_group)) and
            train + held <= 4096 and train == groups * train_group and
            held == groups * held_group,
            "invalid repeat query geometry")
    require(type(head.get("scale")) is float and
            math.isfinite(head["scale"]) and head["scale"] > 0,
            "attention scale mismatch")
    prefix, recent = _protected_rows(index, head, min_recent_rows,
        continuation_rows, retained_row_cap, staging_mib)

    # The validator proves the complete plan/capture/resource graph and hashes
    # every source tensor before this selected head is materialized. Re-open the
    # whole canonical interleaved K/V tensors by their sealed hashes, then
    # deinterleave only the requested physical head.
    root = binding["paths"]["source_index"].parent
    row_bytes = heads * dim * 2
    all_keys = native.checked(native.local(root, head["keys"]["path"]),
                              head["keys"]["sha256"], rows * row_bytes)
    all_values = native.checked(native.local(root, head["values"]["path"]),
                                head["values"]["sha256"], rows * row_bytes)
    keys = native.select_head_rows(all_keys, rows, heads, dim, kv_head, 2)
    values = native.select_head_rows(all_values, rows, heads, dim, kv_head, 2)

    # Metadata, lineage, resource evidence, all positions, and complete raw
    # tensor hashes have already been validated. Numeric held values are not
    # interpreted or used for selection; bytes are copied into AMHEAD02 only.
    training = tensor(root, captured["training"], train * dim * 4)
    validation = tensor(root, captured["validation"], held * dim * 4)
    payload = native.HEADER.pack(b"AMHEAD02", rows, dim, train, held, held_group,
        prefix, recent, 4, 2, continuation_rows, retained_row_cap, staging_mib,
        head["scale"], 0)
    payload += keys + values + training + validation
    output = Path(output)
    sources = {name: {"path": str(path), "sha256": binding["digests"][name]}
               for name, path in binding["paths"].items()
               if name in binding["digests"]}
    sources["source_descriptor"] = {
        "path": str(selected["source_descriptor_path"]),
        "sha256": selected["source_descriptor_ref"]["sha256"],
    }
    manifest = {
        "schema": native.SCHEMA, "recipe": native.RECIPE, "diagnostic_only": True,
        "evaluation_role": "internal_development",
        "query_sampling": binding["repeat"]["query_sampling"],
        "fresh_unseen_qualified": False, "serving_qualified": False,
        "full_context_memory_qualified": False,
        "old_validation_queries_used_for_fit": False,
        "held_numerics_used_for_selection": False,
        "training_authority_mode": "native_repeat_v1",
        "preparer_sha256": native.sha(Path(__file__).read_bytes()),
        "source_preparer_sha256": native.sha(Path(native.__file__).read_bytes()),
        "authority_sealer_sha256": binding["authority"]["sealer_sha256"],
        "source_tensor_order": "logical_row_kv_head_head_dim",
        "source_kv_heads": heads, "kv_head": kv_head,
        "input": {"path": str(output), "bytes": len(payload), "sha256": native.sha(payload)},
        "owner_layer": owner, "consumer_groups": groups, "head_dim": dim,
        "source_rows": rows, "training_rows": train, "validation_rows": held,
        "prefix_rows": prefix, "recent_rows": recent,
        "minimum_recent_rows": min_recent_rows,
        "continuation_rows": continuation_rows, "retained_row_cap": retained_row_cap,
        "retention_divisors": [4, 3, 2],
        "limits": {"max_mass_error": 0.05, "max_output_rmse": 0.10,
                   "max_consumer_output_rmse": 0.10},
        "sources": sources,
    }
    manifest_path = Path(str(output) + ".json")
    require(not output.exists() and not manifest_path.exists(), "output already exists")
    native.write_new(output, payload)
    native.write_new(manifest_path,
        (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode())
    return manifest


def prepare(*, owner, output, repeat_training_authority=None,
            repeat_authority_sha256=None, repeat_source=None, repeat_sha256=None,
            capture_inputs=None, capture_resource=None, resource_sha256=None,
            source_index=None, source_sha256=None, query_manifest=None,
            query_sha256=None, training_authority=None, authority_sha256=None,
            source_build_receipt=None, continuation_rows=32,
            retained_row_cap=6144, staging_mib=512, min_recent_rows=2048,
            kv_head=0):
    legacy = {"repeat_source": repeat_source, "repeat_sha256": repeat_sha256,
        "capture_inputs": capture_inputs, "capture_resource": capture_resource,
        "resource_sha256": resource_sha256, "source_index": source_index,
        "source_sha256": source_sha256, "query_manifest": query_manifest,
        "query_sha256": query_sha256, "training_authority": training_authority,
        "authority_sha256": authority_sha256,
        "source_build_receipt": source_build_receipt}
    new_mode = repeat_training_authority is not None or repeat_authority_sha256 is not None
    if new_mode:
        require(repeat_training_authority is not None and repeat_authority_sha256 is not None and
                all(value is None for value in legacy.values()),
                "mixed or incomplete repeat training authority mode")
        return _prepare_authority(repeat_training_authority=repeat_training_authority,
            repeat_authority_sha256=repeat_authority_sha256, owner=owner, output=output,
            continuation_rows=continuation_rows, retained_row_cap=retained_row_cap,
            staging_mib=staging_mib, min_recent_rows=min_recent_rows, kv_head=kv_head)
    require(all(value is not None for value in legacy.values()),
            "incomplete legacy repeat authority mode")
    return _prepare_legacy(owner=owner, output=output,
        continuation_rows=continuation_rows, retained_row_cap=retained_row_cap,
        staging_mib=staging_mib, min_recent_rows=min_recent_rows, kv_head=kv_head,
        **legacy)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("repeat-source", "repeat-sha256", "capture-inputs", "capture-resource", "resource-sha256",
                 "source-index", "source-sha256", "query-manifest", "query-sha256", "training-authority",
                 "authority-sha256", "source-build-receipt", "output"):
        parser.add_argument("--" + name, required=name == "output")
    parser.add_argument("--repeat-training-authority")
    parser.add_argument("--repeat-authority-sha256")
    parser.add_argument("--owner", type=int, required=True)
    parser.add_argument("--min-recent-rows", type=int, default=2048)
    parser.add_argument("--kv-head", type=int, default=0)
    args = vars(parser.parse_args())
    print(json.dumps(prepare(**args), sort_keys=True))


if __name__ == "__main__":
    main()
