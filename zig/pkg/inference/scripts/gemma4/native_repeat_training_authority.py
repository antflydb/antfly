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

"""Predeclare and terminally seal fresh native-repeat training authority."""
from __future__ import annotations
import argparse, json, math, os, re, stat, struct
from pathlib import Path
import native_attention_matching as native

PLAN_SCHEMA = "antfly.context.native.repeat-training-plan.v1"
AUTHORITY_SCHEMA = "antfly.context.native.repeat-training-authority.v1"
SOURCE_SCHEMA = "antfly.context.native.full-head-source.v2"
REPEAT_SCHEMA = "antfly.context.native.repeat-source.v2"
require = native.require

def _validate_build(path, expected_binary=None):
    from context_release_build_adapter import validate
    return validate(path, expected_binary)

def _digest(value, label="SHA-256"):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "invalid " + label)
    return value

def _json(path, digest):
    return json.loads(native.checked(path, _digest(digest)))

def _ref(root, value, label, size=None):
    require(type(value) is dict and set(value) >= {"path", "sha256"}, "invalid " + label + " reference")
    path = native.local(root, value["path"])
    data = native.checked(path, _digest(value["sha256"], label + " SHA-256"), size)
    if "bytes" in value: require(type(value["bytes"]) is int and value["bytes"] == len(data), label + " byte count mismatch")
    return path, data

def _write(path, value):
    native.write_new(path, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode())
    return value

def _coverage(inputs_path, inputs):
    path, raw = _ref(inputs_path.parent, inputs.get("coverage_receipt"), "coverage receipt")
    value = json.loads(raw)
    require(value.get("schema_version") == 2 and value.get("stage") == "context_payload_block_coverage" and
            value.get("completed") is True and value.get("cancelled") is False and value.get("kv_readback_bytes") == 0 and
            value.get("loaded_identity") == inputs.get("loaded_identity") and value.get("provenance") == inputs.get("provenance") and
            value.get("request") == inputs.get("request") and value.get("prompt_tokens") == inputs.get("prompt", {}).get("count") and
            value.get("prompt_tokens_sha256") == inputs.get("prompt", {}).get("sha256"), "invalid or mismatched coverage receipt")
    document = value.get("document")
    require(type(document) is dict and set(document) == {"id", "sha256", "bucket"} and document["id"] and document["bucket"] and
            re.fullmatch(r"[0-9a-f]{64}", document["sha256"]), "invalid training document identity")
    return path, value, document

def create_plan(*, source_build_receipt, capture_inputs, capture_inputs_sha256, output_stem, owners, output):
    source_build_receipt, capture_inputs, output = map(Path, (source_build_receipt, capture_inputs, output))
    require(source_build_receipt.parent == capture_inputs.parent == output.parent, "training plan evidence must share one campaign directory")
    require(isinstance(output_stem, str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", output_stem), "invalid capture output stem")
    inputs = _json(capture_inputs, capture_inputs_sha256)
    require(inputs.get("schema_version") == 1 and inputs.get("stage") == "context_numerical_capture_inputs" and
            inputs.get("completed") is True and inputs.get("diagnostic_only") is False, "capture inputs lack completed bound coverage")
    identity, provenance, prompt = inputs.get("loaded_identity"), inputs.get("provenance"), inputs.get("prompt")
    ikeys = {"load_generation", "artifact_sha256", "tokenizer_sha256", "template_sha256", "layout_sha256", "implementation_sha256"}
    require(type(identity) is dict and set(identity) == ikeys and type(identity["load_generation"]) is int and identity["load_generation"] > 0 and
            all(re.fullmatch(r"[0-9a-f]{64}", identity[k]) for k in ikeys - {"load_generation"}), "invalid capture model identity")
    require(type(provenance) is dict and set(provenance) == {"binary_sha256", "source_build_sha256"} and
            all(re.fullmatch(r"[0-9a-f]{64}", v) for v in provenance.values()), "invalid capture provenance")
    require(type(prompt) is dict and set(prompt) >= {"path", "sha256", "count"} and type(prompt["count"]) is int and
            512 < prompt["count"] <= 131072,
            "invalid capture prompt")
    _ref(capture_inputs.parent, prompt, "prompt tokens", prompt["count"] * 8)
    coverage_path, coverage, document = _coverage(capture_inputs, inputs)
    build = _validate_build(source_build_receipt)
    require(build["receipt_sha256"] == provenance["source_build_sha256"] and build["binary_sha256"] == provenance["binary_sha256"],
            "capture build provenance mismatch")
    require(type(owners) is list and owners, "empty expected owner geometry")
    normalized, coordinates, prior = [], [], -1
    for owner in owners:
        require(type(owner) is dict and set(owner) == {"owner_layer", "source_heads"} and type(owner["owner_layer"]) is int and
                owner["owner_layer"] > prior and type(owner["source_heads"]) is int and 0 < owner["source_heads"] <= 64,
                "invalid or unordered expected owner geometry")
        prior = owner["owner_layer"]; normalized.append(dict(owner))
        coordinates.extend({"owner_layer": owner["owner_layer"], "kv_head": h} for h in range(owner["source_heads"]))
    plan = {"schema": PLAN_SCHEMA, "purpose": "calibration", "training_source_only": True,
        "held_query_numerics_loaded": False, "release_authority": False, "serving_qualified": False, "fresh_unseen_qualified": False,
        "protocol": {"query_sampling": "representative_repeat_partition_v1", "selection": "history_only_cuda_study_v1",
                     "position_partition": "hashed_quartet_v1", "repeat_rows": 512},
        "output_stem": output_stem, "expected_outputs": {"source_index": output_stem + "-native-head-index.json",
        "repeat_source": output_stem + "-native-repeat-index.json"}, "owners": normalized, "coordinates": coordinates,
        "loaded_identity": identity, "provenance": provenance, "document": document, "request": inputs.get("request"), "prompt": prompt,
        "sources": {"source_build_receipt": {"path": source_build_receipt.name, "sha256": build["receipt_sha256"]},
                    "capture_inputs": {"path": capture_inputs.name, "sha256": capture_inputs_sha256},
                    "coverage_receipt": {"path": coverage_path.name, "sha256": inputs["coverage_receipt"]["sha256"]}},
        "coverage": {"qualified_context_tokens": coverage.get("qualified_context_tokens"),
                     "reserved_output_tokens": coverage.get("reserved_output_tokens"), "eligible_blocks": coverage.get("eligible_blocks")}}
    require(plan["request"] == coverage["request"], "missing bound training request")
    return _write(output, plan)

def _held(position):
    value = position // 4; value = ((value ^ (value >> 16)) * 0x7FEB352D) & 0xffffffff
    value = ((value ^ (value >> 15)) * 0x846CA68B) & 0xffffffff; value ^= value >> 16
    return position % 4 == value % 4

def _positions(start, held, count):
    population = [p for p in range(start, start + 512) if _held(p) == held]
    require(type(count) is int and 0 < count <= len(population), "invalid repeat sampling quota")
    return ([population[(len(population)-1)//2]] if count == 1 else
            [population[i * (len(population)-1)//(count-1)] for i in range(count)])

def _log(resource_path, suffix):
    path = Path(resource_path).with_suffix(suffix)
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and before.st_size <= 16*1024*1024, "invalid capture log")
        with os.fdopen(fd, "rb", closefd=False) as stream: data = stream.read()
        after = os.fstat(fd)
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                "capture log changed while reading")
        return path, data
    finally:
        os.close(fd)

def _capture(root, resource_path, resource_sha, build, plan, plan_sha, inputs_path, repeat_path, repeat_sha):
    resource = _json(resource_path, resource_sha)
    require(resource.get("pass") is True and resource.get("returncode") == 0 and resource.get("violation") is None and
            resource.get("swapout_growth_mib") == 0 and resource.get("remaining_child_group_live_processes") == 0,
            "repeat capture did not complete under clean resource guards")
    limits = resource.get("limits", {})
    require(0 < limits.get("max_rss_mib", 0) <= 8192 and limits.get("max_swap_growth_mib") == 0 and
            limits.get("min_free_percent", 0) >= 15 and limits.get("min_disk_free_mib", 0) >= 4096,
            "repeat capture limits changed")
    command = resource.get("command", [])
    require(len(command) >= 4 and command[0] == "/usr/bin/env" and command[-3:] == [str(build["binary_path"]), "--test-filter",
            "context Metal installed Gemma production query campaign capture"], "repeat capture executable mismatch")
    assignments = command[1:-3]
    require(all(isinstance(x, str) and "=" in x for x in assignments), "invalid capture environment")
    env = dict(x.split("=", 1) for x in assignments); require(len(env) == len(assignments), "duplicate capture environment")
    for key in ("MTL_DEBUG_LAYER", "ANTFLY_CONTEXT_CAMPAIGN_CAPTURE", "ANTFLY_CONTEXT_CAPTURE_NATIVE_FULL_HEADS",
                "ANTFLY_CONTEXT_CAPTURE_NATIVE_REPEAT"):
        require(env.get(key) == "1", "not a native representative replay capture")
    require(env.get("ANTFLY_CONTEXT_CAPTURE_NATIVE_PREFILL_ONLY", "0") == "0" and
            env.get("ANTFLY_CONTEXT_CAMPAIGN_PEAK_BYTES") == "8589934592" and
            env.get("ANTFLY_CONTEXT_CAPTURE_CAMPAIGN_DIR") == str(root) and
            env.get("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST") == inputs_path.name and
            env.get("ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST_SHA256") == plan["sources"]["capture_inputs"]["sha256"] and
            env.get("ANTFLY_CONTEXT_CAMPAIGN_CAPTURE_STEM") == plan["output_stem"] and
            env.get("ANTFLY_CONTEXT_SOURCE_BUILD_RECEIPT") == plan["sources"]["source_build_receipt"]["path"] and
            env.get("ANTFLY_CONTEXT_NATIVE_REPEAT_TRAINING_PLAN_SHA256") == plan_sha,
            "repeat capture training plan binding mismatch")
    stdout_path, stdout = _log(resource_path, ".stdout"); stderr_path, stderr = _log(resource_path, ".stderr")
    record = ("context_native_repeat_artifact: index=" + repeat_path.name + " sha256=" + repeat_sha).encode()
    records = [line for raw in (stdout, stderr) for line in raw.splitlines() if line.startswith(b"context_native_repeat_artifact:")]
    require(b"Metal API Validation Enabled" in stderr and
            b"context_native_repeat_capture: repeat_tokens=512 original_unchanged=true source_kv_readback_bytes=0 serving_qualified=false" in stderr and
            b"1 selected; 1 passed; 0 skipped." in stderr and records == [record], "repeat capture completion evidence missing")
    return resource, stdout_path, native.sha(stdout), stderr_path, native.sha(stderr)

def _graph(*, plan_path, plan_sha, source_index_path, source_sha, repeat_path, repeat_sha, resource_path, resource_sha):
    plan_path, source_index_path, repeat_path, resource_path = map(Path,
        (plan_path, source_index_path, repeat_path, resource_path)); root = plan_path.parent
    require(all(p.parent == root for p in (source_index_path, repeat_path, resource_path)),
            "authority evidence must share one campaign directory")
    plan = _json(plan_path, plan_sha)
    require(plan.get("schema") == PLAN_SCHEMA and plan.get("purpose") == "calibration" and
            plan.get("training_source_only") is True and plan.get("held_query_numerics_loaded") is False and
            plan.get("release_authority") is False and plan.get("serving_qualified") is False and
            plan.get("fresh_unseen_qualified") is False, "invalid native repeat training plan")
    exact_protocol = {"query_sampling": "representative_repeat_partition_v1", "selection": "history_only_cuda_study_v1",
                      "position_partition": "hashed_quartet_v1", "repeat_rows": 512}
    require(isinstance(plan.get("output_stem"), str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,96}", plan["output_stem"]) and
            plan.get("protocol") == exact_protocol and type(plan.get("owners")) is list and plan["owners"],
            "invalid frozen training plan protocol")
    previous = -1
    for owner in plan["owners"]:
        require(type(owner) is dict and set(owner) == {"owner_layer", "source_heads"} and
                type(owner["owner_layer"]) is int and owner["owner_layer"] > previous and
                type(owner["source_heads"]) is int and 0 < owner["source_heads"] <= 64,
                "invalid frozen owner geometry")
        previous = owner["owner_layer"]
    require(type(plan.get("prompt")) is dict and type(plan["prompt"].get("count")) is int and
            512 < plan["prompt"]["count"] <= 131072, "invalid frozen prompt geometry")
    require(source_index_path.name == plan.get("expected_outputs", {}).get("source_index") and
            repeat_path.name == plan.get("expected_outputs", {}).get("repeat_source"), "training plan output binding mismatch")
    sources = plan["sources"]; inputs_path = native.local(root, sources["capture_inputs"]["path"])
    build_path = native.local(root, sources["source_build_receipt"]["path"])
    inputs = _json(inputs_path, sources["capture_inputs"]["sha256"]); build = _validate_build(build_path)
    require(build["receipt_sha256"] == sources["source_build_receipt"]["sha256"] and
            build["binary_sha256"] == plan["provenance"]["binary_sha256"] and
            build["receipt_sha256"] == plan["provenance"]["source_build_sha256"], "training plan build provenance mismatch")
    coverage_path, coverage, document = _coverage(inputs_path, inputs)
    frozen_coverage = {"qualified_context_tokens": coverage.get("qualified_context_tokens"),
        "reserved_output_tokens": coverage.get("reserved_output_tokens"), "eligible_blocks": coverage.get("eligible_blocks")}
    require(plan["loaded_identity"] == inputs["loaded_identity"] and plan["provenance"] == inputs["provenance"] and
            plan["prompt"] == inputs["prompt"] and plan["request"] == inputs["request"] and plan["document"] == document and
            plan.get("coverage") == frozen_coverage and
            sources["coverage_receipt"] == {"path": coverage_path.name, "sha256": inputs["coverage_receipt"]["sha256"]},
            "training plan capture lineage changed")
    index = _json(source_index_path, source_sha)
    require(index.get("schema") == SOURCE_SCHEMA and index.get("diagnostic_only") is True and index.get("serving_qualified") is False and
            index.get("prompt_tokens") == plan["prompt"]["count"] and index.get("prompt_tokens_sha256") == plan["prompt"]["sha256"] and
            index.get("artifact_sha256") == plan["loaded_identity"]["artifact_sha256"] and
            index.get("load_generation") == plan["loaded_identity"]["load_generation"] and
            index.get("binary_sha256") == plan["provenance"]["binary_sha256"] and
            index.get("source_build_sha256") == plan["provenance"]["source_build_sha256"],
            "full-head source identity differs from training plan")
    eligible = plan["coverage"]["eligible_blocks"]
    require(type(eligible) is list and eligible and all(type(block) is dict and
            type(block.get("start")) is int and type(block.get("end")) is int and
            0 <= block["start"] < block["end"] <= plan["prompt"]["count"] and block["end"] - block["start"] == 512
            for block in eligible), "invalid pre-capture eligible blocks")
    starts = [block["start"] for block in eligible]
    require(starts == sorted(set(starts)) and type(index.get("eligible_block_starts")) is list and
            index["eligible_block_starts"] == starts,
            "full-head eligible blocks differ from pre-capture coverage")
    coords = [(x["owner_layer"], x["kv_head"]) for x in plan.get("coordinates", [])]
    expected = [(o["owner_layer"], h) for o in plan.get("owners", []) for h in range(o["source_heads"])]
    require(coords == expected and len(set(coords)) == len(coords), "invalid planned physical-head coordinates")
    by_owner = {}; entries = index.get("descriptors", [])
    for owner in plan["owners"]:
        matches = [x for x in entries if x.get("owner_layer") == owner["owner_layer"]]
        require(len(matches) == 1 and matches[0].get("block_start") == 0, "missing or duplicate complete owner")
        entry = matches[0]; descriptor_path, raw = _ref(root, entry, "source descriptor"); head = json.loads(raw)
        require(head.get("prefix_storage") == "packed_exact" and head.get("tensor_dtype") == "f16_le" and
                head.get("tensor_order") == "logical_row_kv_head_head_dim" and head.get("owner_layer") == owner["owner_layer"] and
                head.get("heads") == owner["source_heads"] and head.get("block_start") == 0 and head.get("rows") == index["prompt_tokens"] and
                head.get("prompt") == {"tokens": plan["prompt"]["count"], "sha256": plan["prompt"]["sha256"]} and
                head.get("document") == plan["document"] and head.get("loaded_identity") == plan["loaded_identity"] and
                head.get("provenance") == plan["provenance"], "source descriptor differs from training plan")
        dim = head.get("dim"); tuples = [(s.get("consumer_layer"), s.get("query_head"), s.get("kv_head")) for s in head.get("strata", [])]
        require(type(head.get("rows")) is int and 512 < head["rows"] <= 131072 and
                type(dim) is int and 0 < dim <= 512 and dim % 2 == 0 and type(head.get("scale")) is float and
                math.isfinite(head["scale"]) and
                head["scale"] > 0 and tuples and len(set(tuples)) == len(tuples) and
                {x[2] for x in tuples} == set(range(owner["source_heads"])), "incomplete source physical-head strata")
        size = head["rows"] * head["heads"] * dim * 2
        _ref(root, head.get("keys"), "source keys", size); _ref(root, head.get("values"), "source values", size)
        by_owner[owner["owner_layer"]] = (entry, descriptor_path, head)
    require(len(entries) == len(plan["owners"]), "unexpected complete-head source owner")
    repeat = _json(repeat_path, repeat_sha); protocol = plan["protocol"]
    require(repeat.get("schema") == REPEAT_SCHEMA and repeat.get("diagnostic_only") is True and
            repeat.get("evaluation_role") == "internal_development" and repeat.get("serving_qualified") is False and
            repeat.get("fresh_unseen_qualified") is False and repeat.get("query_sampling") == protocol["query_sampling"] and
            repeat.get("selection") == protocol["selection"] and repeat.get("position_partition") == protocol["position_partition"] and
            repeat.get("repeat_rows") == 512 and repeat.get("training_plan_sha256") == plan_sha and
            repeat.get("source_kv_readback_bytes") == 0 and repeat.get("binary_sha256") == index["binary_sha256"] and
            repeat.get("source_build_sha256") == index["source_build_sha256"] and repeat.get("load_generation") == index["load_generation"] and
            repeat.get("repeat_start") == index["prompt_tokens"] and repeat.get("capture_inputs_manifest_sha256") == sources["capture_inputs"]["sha256"] and
            repeat.get("full_head_source_index") == {"path": source_index_path.name, "sha256": source_sha},
            "repeat source differs from predeclared training plan")
    # Reject failed, stale, or unbound capture evidence before opening any
    # training or held query tensor bytes.
    resource, stdout_path, stdout_sha, stderr_path, stderr_sha = _capture(root, resource_path, resource_sha,
        build, plan, plan_sha, inputs_path, repeat_path, repeat_sha)
    prompt_path, prompt_raw = _ref(root, plan["prompt"], "prompt tokens", plan["prompt"]["count"] * 8)
    repeat_tokens_path, repeat_raw = _ref(root, repeat.get("repeat_tokens"), "repeat tokens", 512 * 8)
    prompt_tokens = [v for (v,) in struct.iter_unpack("<q", prompt_raw)]
    repeat_tokens = [v for (v,) in struct.iter_unpack("<q", repeat_raw)]; iterator = iter(prompt_tokens)
    require(all(any(candidate == value for candidate in iterator) for value in repeat_tokens) and
            repeat_tokens[-384:] == prompt_tokens[-384:], "repeat is not a history subsequence with exact recent suffix")
    rows = repeat.get("owners", []); actual_coords = [(x.get("owner_layer"), x.get("kv_head")) for x in rows]
    require(actual_coords == coords and len(set(actual_coords)) == len(coords),
            "missing, duplicate, or reordered repeat physical head")
    descriptors = {}
    for captured in rows:
        coordinate = (captured["owner_layer"], captured["kv_head"])
        entry, descriptor_path, head = by_owner[captured["owner_layer"]]
        expected_strata = [s for s in head["strata"] if s["kv_head"] == captured["kv_head"]]
        require(captured.get("source_heads") == head["heads"] and captured.get("head_dim") == head["dim"] and
                captured.get("strata") == expected_strata and expected_strata, "repeat consumer coverage differs from source")
        groups = len(expected_strata); train, held = captured.get("training_rows"), captured.get("validation_rows")
        tg, hg = captured.get("training_group_rows"), captured.get("validation_group_rows")
        require(all(type(v) is int and v > 0 for v in (train, held, tg, hg)) and train + held <= 4096 and
                train == groups * tg and held == groups * hg, "invalid repeat query geometry")
        for name, count, group, is_held in (("training", train, tg, False), ("validation", held, hg, True)):
            # Hash all f32 bytes, but never unpack validation numeric values.
            _ref(root, captured.get(name), name + " query", count * head["dim"] * 4)
            _, raw = _ref(root, captured.get(name + "_positions"), name + " positions", count * 4)
            positions = [v for (v,) in struct.iter_unpack("<I", raw)]; expected_positions = _positions(head["rows"], is_held, group)
            require(all(positions[g*group:(g+1)*group] == expected_positions for g in range(groups)),
                    "repeat positions violate the disjoint uniform partition")
        descriptors[coordinate] = {"source_descriptor": head, "source_descriptor_ref": entry,
            "source_descriptor_path": descriptor_path, "repeat_owner": captured}
    paths = {"plan": plan_path, "source_index": source_index_path, "repeat_source": repeat_path,
        "capture_resource": resource_path, "capture_stdout": stdout_path, "capture_stderr": stderr_path,
        "source_build_receipt": build_path, "capture_inputs": inputs_path,
        "prompt_tokens": prompt_path, "repeat_tokens": repeat_tokens_path}
    digests = {"plan": plan_sha, "source_index": source_sha, "repeat_source": repeat_sha,
        "capture_resource": resource_sha, "capture_stdout": stdout_sha, "capture_stderr": stderr_sha,
        "source_build_receipt": build["receipt_sha256"], "capture_inputs": sources["capture_inputs"]["sha256"]}
    return plan, index, repeat, resource, paths, descriptors, digests

def seal_authority(*, plan, plan_sha256, source_index, source_sha256, repeat_source, repeat_sha256,
                   capture_resource, resource_sha256, output=None):
    plan_obj, index, repeat, resource, paths, descriptors, digests = _graph(plan_path=plan, plan_sha=plan_sha256,
        source_index_path=source_index, source_sha=source_sha256, repeat_path=repeat_source, repeat_sha=repeat_sha256,
        resource_path=capture_resource, resource_sha=resource_sha256)
    cells = []
    for cell in plan_obj["coordinates"]:
        owner, head = cell["owner_layer"], cell["kv_head"]; item = descriptors[(owner, head)]
        source, captured = item["source_descriptor"], item["repeat_owner"]
        cells.append({"owner_layer": owner, "kv_head": head, "source_heads": source["heads"], "head_dim": source["dim"],
            "source_descriptor": {"path": item["source_descriptor_path"].name, "sha256": item["source_descriptor_ref"]["sha256"]},
            "source_keys": source["keys"], "source_values": source["values"],
            "repeat_training": captured["training"], "repeat_validation": captured["validation"],
            "training_positions": captured["training_positions"], "validation_positions": captured["validation_positions"]})
    authority = {"schema": AUTHORITY_SCHEMA, "purpose": "calibration", "training_source_authority": True,
        "training_source_only": True, "held_query_numerics_loaded": False, "release_authority": False,
        "serving_qualified": False, "fresh_unseen_qualified": False, "loaded_identity": plan_obj["loaded_identity"],
        "document": plan_obj["document"], "request": plan_obj["request"], "prompt": plan_obj["prompt"],
        "protocol": plan_obj["protocol"], "cells": cells,
        "sources": {name: {"path": paths[name].name, "sha256": digests[name]} for name in
                    ("plan", "source_index", "repeat_source", "capture_resource", "capture_stdout", "capture_stderr",
                     "source_build_receipt", "capture_inputs")},
        "sealer_sha256": native.sha(Path(__file__).read_bytes())}
    if output is not None:
        output = Path(output); require(output.parent == Path(plan).parent, "authority output must share campaign directory")
        _write(output, authority)
    return authority

def validate_authority(authority_path, authority_sha256):
    authority_path = Path(authority_path); authority = _json(authority_path, authority_sha256)
    require(authority.get("schema") == AUTHORITY_SCHEMA and authority.get("purpose") == "calibration" and
            authority.get("training_source_authority") is True and authority.get("training_source_only") is True and
            authority.get("held_query_numerics_loaded") is False and authority.get("release_authority") is False and
            authority.get("serving_qualified") is False and authority.get("fresh_unseen_qualified") is False,
            "invalid native repeat training authority")
    names = ("plan", "source_index", "repeat_source", "capture_resource", "capture_stdout", "capture_stderr",
             "source_build_receipt", "capture_inputs"); refs = authority.get("sources", {})
    require(type(refs) is dict and set(refs) == set(names), "incomplete authority source graph")
    root = authority_path.parent; paths = {name: native.local(root, refs[name]["path"]) for name in names}
    regenerated = seal_authority(plan=paths["plan"], plan_sha256=refs["plan"]["sha256"],
        source_index=paths["source_index"], source_sha256=refs["source_index"]["sha256"],
        repeat_source=paths["repeat_source"], repeat_sha256=refs["repeat_source"]["sha256"],
        capture_resource=paths["capture_resource"], resource_sha256=refs["capture_resource"]["sha256"])
    require(authority == regenerated, "authority does not match its complete binding graph")
    values = _graph(plan_path=paths["plan"], plan_sha=refs["plan"]["sha256"], source_index_path=paths["source_index"],
        source_sha=refs["source_index"]["sha256"], repeat_path=paths["repeat_source"], repeat_sha=refs["repeat_source"]["sha256"],
        resource_path=paths["capture_resource"], resource_sha=refs["capture_resource"]["sha256"])
    plan, index, repeat, resource, graph_paths, descriptors, digests = values
    graph_paths["authority"] = authority_path; digests["authority"] = authority_sha256
    return {"authority": authority, "plan": plan, "source_index": index, "repeat": repeat, "resource": resource,
            "paths": graph_paths, "descriptors": descriptors, "digests": digests}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    create = commands.add_parser("create-plan")
    for name in ("source-build-receipt", "capture-inputs", "capture-inputs-sha256", "output-stem",
                 "owners-json", "owners-sha256", "output"):
        create.add_argument("--" + name, required=True)
    seal = commands.add_parser("seal")
    for name in ("plan", "plan-sha256", "source-index", "source-sha256", "repeat-source", "repeat-sha256",
                 "capture-resource", "resource-sha256", "output"):
        seal.add_argument("--" + name, required=True)
    check = commands.add_parser("validate")
    check.add_argument("--authority", required=True); check.add_argument("--authority-sha256", required=True)
    args = vars(parser.parse_args()); command = args.pop("command")
    if command == "create-plan":
        owners_path = args.pop("owners_json"); owners_sha = args.pop("owners_sha256")
        args["owners"] = json.loads(native.checked(owners_path, owners_sha))
        result = create_plan(**args)
    elif command == "seal":
        result = seal_authority(**args)
    else:
        result = validate_authority(args["authority"], args["authority_sha256"])["authority"]
    print(json.dumps(result, sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
