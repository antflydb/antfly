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

"""Deterministic source packing and exact loaded-tokenizer receipt finalization."""
import argparse
from collections import Counter
import hashlib
import heapq
import json
import os
from pathlib import Path

LENGTHS = (8192, 32768)
BUCKETS = ("document_qa", "exact_retrieval", "code", "structured_tool", "multiturn")
CASES_PER_CELL = 100


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(path):
    def pairs(rows):
        result = {}
        for key, value in rows:
            require(key not in result, "duplicate JSON field: " + key)
            result[key] = value
        return result
    return json.loads(Path(path).read_bytes(), object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def source_bytes(root, row):
    exact = {"authorship", "clone_cluster_id", "content_type", "family_id", "family_sha256",
             "license", "normalized_sha256", "partition", "path", "repository", "revision", "sha256"}
    require(type(row) is dict and set(row) == exact and row["partition"] == "quality_evaluation",
            "invalid evaluation source")
    relative = Path(row["path"])
    require(not relative.is_absolute() and ".." not in relative.parts, "source path escapes snapshot")
    raw = (Path(root) / relative).read_bytes()
    require(sha(raw) == row["sha256"] and raw.decode("utf-8").encode() == raw,
            "source bytes do not match sealed UTF-8 snapshot")
    return raw


def assign_cell(cases, sources, sizes, seed, length, bucket):
    """Assign every non-primary family exactly once, balancing sealed bytes."""
    cases = sorted(cases, key=lambda row: row["id"])
    require(len(cases) == CASES_PER_CELL, "wrong frozen case count")
    primary = [row["primary_source_family"] for row in cases]
    require(len(set(primary)) == len(primary), "primary families are not independent within cell")
    require(all(family in sources for family in primary), "unknown primary source family")
    assignments = [[] for _ in cases]
    queue = [(sizes[family], row["id"], index) for index, (family, row) in
             enumerate(zip(primary, cases))]
    heapq.heapify(queue)
    remaining = [family for family in sources if family not in set(primary)]
    remaining.sort(key=lambda family: (-sizes[family],
        sha(bytes.fromhex(seed) + b"\0" + str(length).encode() + b"\0" + bucket.encode() +
            b"\0" + family.encode()), family))
    for family in remaining:
        total, case_id, index = heapq.heappop(queue)
        assignments[index].append(family)
        heapq.heappush(queue, (total + sizes[family], case_id, index))
    used = primary + [family for values in assignments for family in values]
    require(len(used) == len(set(used)) == len(sources), "source family assignment is not independent")
    return [{"case": row, "primary_family": primary[index],
             "distractor_families": assignments[index],
             "available_source_bytes": sizes[primary[index]] + sum(sizes[f] for f in assignments[index])}
            for index, row in enumerate(cases)]


def initialize(blueprints, source_manifest, source_root, seed):
    require(type(blueprints) is dict and blueprints.get("schema_version") == 3 and
            blueprints.get("stage") == "quality_source_task_blueprints_historical_tool_response_v1" and
            blueprints.get("model_outputs_observed") is False, "invalid frozen blueprint artifact")
    require(type(source_manifest) is dict and source_manifest.get("model") == blueprints.get("model"),
            "source/blueprint model mismatch")
    require(seed == blueprints.get("selection_seed") == source_manifest.get("selection_seed"),
            "packing seed does not match frozen source selection")
    sources = {}
    sizes = {}
    source_rows = {}
    for row in source_manifest.get("sources", []):
        raw = source_bytes(source_root, row)
        family = row["family_id"]
        require(family not in sources, "duplicate source family")
        sources[family] = row["path"]
        source_rows[family] = row
        sizes[family] = len(raw)
    counts = Counter(row.get("bucket") for row in blueprints.get("cases", []))
    require(counts == Counter({bucket: CASES_PER_CELL for bucket in BUCKETS}),
            "frozen blueprint cells are incomplete")
    cells = []
    for length in LENGTHS:
        for bucket in BUCKETS:
            rows = [row for row in blueprints["cases"] if row["bucket"] == bucket]
            assigned = assign_cell(rows, sources, sizes, seed, length, bucket)
            cells.append({"context_tokens": length, "bucket": bucket, "cases": assigned})
    return {"schema_version": 1, "stage": "quality_loaded_tokenizer_pack_work",
            "completed": False, "model_outputs_observed": False, "model": blueprints["model"],
            "selection_seed": seed, "blueprints_sha256": None,
            "source_manifest_sha256": None, "source_rows": source_rows, "cells": cells}


def _utf8_prefix(raw, limit):
    limit = min(max(limit, 0), len(raw))
    while limit > 0:
        try:
            raw[:limit].decode("utf-8")
            return raw[:limit]
        except UnicodeDecodeError as exc:
            limit = exc.start
    return b""


def _utf8_evidence_excerpt(raw, start, end, budget, prefix_limit=None):
    require(type(start) is int and type(end) is int and 0 <= start < end <= len(raw),
            "evidence range escapes primary source")
    require(raw[start:end].decode("utf-8").encode() == raw[start:end],
            "evidence range is not a UTF-8 boundary")
    budget = max(budget, end - start)
    require(prefix_limit is None or type(prefix_limit) is int and 0 <= prefix_limit <= 4096,
            "invalid primary prefix limit")
    context_before = (budget - (end - start)) // 2
    if prefix_limit is not None:
        context_before = min(context_before, prefix_limit)
        budget = min(budget, len(raw) - start + prefix_limit)
    left = max(0, start - context_before)
    right = min(len(raw), left + budget)
    left = max(0, right - budget)
    while left < start:
        try:
            raw[left:right].decode("utf-8")
            break
        except UnicodeDecodeError as exc:
            left += max(1, exc.start + 1)
    while right > end:
        try:
            raw[left:right].decode("utf-8")
            break
        except UnicodeDecodeError as exc:
            right = left + exc.start
    require(left <= start < end <= right and raw[left:right].decode("utf-8").encode() == raw[left:right],
            "cannot form UTF-8 primary excerpt")
    return left, right, raw[left:right]

def _utf8_evidence_excerpt_within(raw,start,end,budget,lower,upper):
    require(0<=lower<=start<end<=upper<=len(raw) and
            raw[:lower].decode("utf-8").encode()==raw[:lower] and
            raw[:upper].decode("utf-8").encode()==raw[:upper],
            "invalid bounded UTF-8 evidence range")
    local_start,local_end,excerpt=_utf8_evidence_excerpt(
        raw[lower:upper],start-lower,end-lower,min(budget,upper-lower))
    return lower+local_start,lower+local_end,excerpt

def _utf8_boundary_before(raw,index):
    index=min(max(index,0),len(raw))
    while index>0:
        try:
            raw[:index].decode("utf-8")
            return index
        except UnicodeDecodeError as exc:
            index=exc.start
    return 0


def compose_candidate(work_case, source_rows, source_root, byte_budget, insertion_index=None, padding_units=0, prefix_padding_units=0, suffix_padding_units=0, primary_prefix_limit=None, insertion_subpart_units=0):
    """Build one deterministic candidate; loaded receipts, never bytes, finalize it."""
    case = work_case["case"]
    families = list(work_case["distractor_families"])
    families.sort(key=lambda family: (sha(case["distractor_order_seed"].encode() + b"\0" + family.encode()), family))
    primary = work_case["primary_family"]
    require(case["source"]["path"] == source_rows[primary]["path"] and
            case["source"]["sha256"] == source_rows[primary]["sha256"],
            "primary evidence source does not match sealed family")
    default_insertion = min(len(families), ((2 * case["intended_compactable_quartile"] + 1) *
                                            (len(families) + 1)) // 8)
    insertion = default_insertion if insertion_index is None else insertion_index
    require(type(insertion) is int and 0 <= insertion <= len(families),
            "invalid primary source insertion")
    families.insert(insertion, primary)
    require(0 < byte_budget <= work_case["available_source_bytes"], "invalid candidate byte budget")
    evidence_ranges = [(case["source"]["byte_start"], case["source"]["byte_end"])]
    if case.get("bucket") == "multiturn":
        require(type(case.get("messages")) is list and len(case["messages"]) == 3 and
                type(case.get("earlier_source_byte_start")) is int and
                type(case.get("earlier_source_byte_end")) is int,
                "invalid frozen multiturn case")
        evidence_ranges.append((case["earlier_source_byte_start"],
                                case["earlier_source_byte_end"]))
    evidence_lower = min(start for start, _ in evidence_ranges)
    evidence_upper = max(end for _, end in evidence_ranges)
    primary_minimum = max(1024, evidence_upper - evidence_lower)
    sizes = {family: Path(source_root, source_rows[family]["path"]).stat().st_size
             for family in families}
    require(primary_prefix_limit is None or type(primary_prefix_limit) is int and
            0 <= primary_prefix_limit <= 4096, "invalid primary prefix limit")
    if primary_prefix_limit is not None:
        require(case.get("bucket") == "structured_tool", "primary prefix limit requires tool history")
        # Preserve the cited span while keeping its context after the span.
        # Mandatory tool-call metadata precedes the response, so a centered
        # excerpt can make the first eligible quartile unreachable. Spare
        # bytes still go to the same assigned distractor source reservoirs.
        sizes[primary] = min(sizes[primary], sizes[primary] - evidence_lower + primary_prefix_limit)
    # Placement search must reorder the same excerpts. A greedy first-part
    # allocation changes both content and evidence position when it moves a
    # family, and can make an otherwise reachable quartile impossible.
    allocations = {family: min(size, primary_minimum if family == primary else 4)
                   for family, size in sizes.items()}
    remaining = byte_budget - sum(allocations.values())
    require(remaining >= 0, "candidate source budget cannot retain mandatory evidence")

    def distribute(capacities):
        nonlocal remaining
        while remaining:
            active = sorted(family for family in families
                            if allocations[family] < capacities[family])
            if not active:
                return
            share = max(1, remaining // len(active))
            for family in active:
                take = min(share, remaining, capacities[family] - allocations[family])
                allocations[family] += take
                remaining -= take
                if not remaining:
                    return

    preferred = {family: min(size, primary_minimum if family == primary else 1024)
                 for family, size in sizes.items()}
    distribute(preferred)
    # Keep the evidence window bounded, including evidence at a file's end.
    # Use the already assigned distractor reservoirs for the long context;
    # grow the primary only if those reservoirs are fully consumed.
    distractor_capacity = {family: (allocations[family] if family == primary else size)
                           for family, size in sizes.items()}
    distribute(distractor_capacity)
    distribute(sizes)
    require(remaining == 0, "candidate source reservoir cannot meet byte budget")
    require(type(insertion_subpart_units) is int and 0 <= insertion_subpart_units < 65536,
            "invalid fractional source placement")
    split_family = None
    split_excerpt = None
    split_cut = 0
    if insertion_subpart_units:
        primary_index = families.index(primary)
        require(primary_index + 1 < len(families), "fractional placement has no following source")
        split_family = families[primary_index + 1]
        split_excerpt = _utf8_prefix(source_bytes(source_root, source_rows[split_family]), allocations[split_family])
        split_cut = _utf8_boundary_before(split_excerpt, (len(split_excerpt) * insertion_subpart_units) // 65536)
        require(0 < split_cut < len(split_excerpt), "source fragment cannot be split at this UTF-8 boundary")
    first_parts, final_parts, segments = [], [], []
    primary_label = None
    evidence_text_start = evidence_text_end = None
    next_label=1
    before_primary=True
    answer_part=None
    def append_source(target,family,row,source_start,source_end,excerpt,kind=None):
        nonlocal next_label
        header = (f"SOURCE family={family} path={row['path']} bytes={source_start}:{source_end}\n").encode()
        # Loaded Gemma templates trim each content part. Keep framing edges
        # non-whitespace so the selected part survives rendering byte-for-byte.
        footer = b"\nEND SOURCE"; text=header+excerpt+footer
        part_index=len(target);label=next_label;next_label+=1
        target.append({"text":text.decode("utf-8"),"selected":True})
        segment={"message_index":0 if target is first_parts else 2,"part_index":part_index,
                 "label":label,"family_id":family,"source_path":row["path"],
                 "source_sha256":row["sha256"],"source_byte_start":source_start,
                 "source_byte_end":source_end,"part_byte_start":len(header),
                 "part_byte_end":len(header)+len(excerpt)}
        if kind is not None: segment["kind"]=kind
        segments.append(segment)
        return segment,text
    for family in families:
        row = source_rows[family]
        raw = source_bytes(source_root, row)
        if family != primary:
            excerpt = split_excerpt[split_cut:] if family == split_family else _utf8_prefix(raw, allocations[family])
            source_start = split_cut if family == split_family else 0
            source_end = source_start + len(excerpt)
            require(excerpt,"empty source excerpt")
            append_source(first_parts if before_primary else final_parts,
                          family,row,source_start,source_end,excerpt,
                          "split_after_primary_v1" if family == split_family else None)
            continue
        if split_family is not None:
            append_source(first_parts,split_family,source_rows[split_family],0,split_cut,
                          split_excerpt[:split_cut],"split_before_primary_v1")
        if case.get("bucket") == "multiturn":
            earlier_start,earlier_end=evidence_ranges[1];later_start,later_end=evidence_ranges[0]
            require(earlier_end<=later_start,"multiturn cited ranges are not chronological")
            split=_utf8_boundary_before(raw,(earlier_end+later_start)//2)
            require(earlier_end<=split<=later_start,"cannot split multiturn evidence ranges")
            total=allocations[family];min_first=earlier_end-earlier_start;min_final=later_end-later_start
            first_take=min(split,max(min_first,total//2));final_take=total-first_take
            if final_take<min_final: first_take-=min_final-final_take;final_take=min_final
            if final_take>len(raw)-split:
                moved=final_take-(len(raw)-split);first_take+=moved;final_take-=moved
            require(min_first<=first_take<=split and min_final<=final_take<=len(raw)-split,
                    "multiturn primary source reservoir cannot meet byte budget")
            es,ee,early=_utf8_evidence_excerpt_within(
                raw,earlier_start,earlier_end,first_take,0,split)
            early_segment,_=append_source(first_parts,family,row,es,ee,early,"earlier")
            early_segment["evidence_part_byte_start"]=early_segment["part_byte_start"]+earlier_start-es
            early_segment["evidence_part_byte_end"]=early_segment["part_byte_start"]+earlier_end-es
            ls,le,late=_utf8_evidence_excerpt_within(
                raw,later_start,later_end,final_take,split,len(raw))
            late_segment,late_text=append_source(final_parts,family,row,ls,le,late,"later")
            primary_label=late_segment["label"];answer_part=late_text
            evidence_text_start=late_segment["part_byte_start"]+later_start-ls
            evidence_text_end=late_segment["part_byte_start"]+later_end-ls
            late_segment["evidence_part_byte_start"]=evidence_text_start
            late_segment["evidence_part_byte_end"]=evidence_text_end
        else:
            source_start,source_end,excerpt=_utf8_evidence_excerpt(
                raw,evidence_lower,evidence_upper,allocations[family],primary_prefix_limit)
            segment,text=append_source(first_parts,family,row,source_start,source_end,excerpt)
            primary_label=segment["label"];answer_part=text
            evidence_text_start=segment["part_byte_start"]+case["source"]["byte_start"]-source_start
            evidence_text_end=segment["part_byte_start"]+case["source"]["byte_end"]-source_start
            segment["evidence_part_byte_start"]=evidence_text_start
            segment["evidence_part_byte_end"]=evidence_text_end
        before_primary=False
    require(type(padding_units) is int and 0 <= padding_units <= 256,
            "invalid neutral padding units")
    neutral_padding = None
    if padding_units and case.get("bucket") == "structured_tool":
        neutral_padding = (" neutral" * padding_units) + "\n"
    elif padding_units:
        padding = (" neutral" * padding_units) + "\n"
        target=final_parts if case.get("bucket")=="multiturn" else first_parts
        label=next_label;next_label+=1;part_index=len(target)
        target.append({"text":padding,"selected":True})
        segments.append({"message_index":2 if target is final_parts else 0,
                         "part_index":part_index,"label":label,
                         "kind":"generated_neutral_padding_v1","units":padding_units,
                         "sha256":sha(padding.encode("utf-8"))})
    if case.get("bucket") == "multiturn":
        messages_rows=[]
        for index,row in enumerate(case["messages"]):
            require(type(row) is dict and set(row)=={"role","content"} and
                    row["role"] in ("user","assistant") and
                    isinstance(row["content"],str) and row["content"],
                    "invalid frozen multiturn message")
            message_parts=([*first_parts] if index==0 else [*final_parts] if index==len(case["messages"])-1 else [])
            message_parts.append({"text":row["content"],"selected":False})
            messages_rows.append({"role":row["role"],"parts":message_parts})
        messages={"schema_version":1,"messages":messages_rows}
    elif case.get("bucket") == "structured_tool":
        history = case.get("tool_history")
        require(type(history) is dict and set(history) == {
                    "schema_version", "initial_request_utf8", "assistant_call",
                    "tool_call_id", "final_request_utf8"} and
                history["schema_version"] == 1 and
                isinstance(history["initial_request_utf8"], str) and history["initial_request_utf8"] and
                isinstance(history["final_request_utf8"], str) and history["final_request_utf8"] and
                type(history["assistant_call"]) is dict and
                history["tool_call_id"] == history["assistant_call"].get("id"),
                "invalid historical tool-response blueprint")
        call = history["assistant_call"]
        function = call.get("function")
        require(set(call) == {"id", "type", "function"} and call["type"] == "function" and
                type(function) is dict and set(function) == {"name", "arguments_contract"} and
                function["name"] == "read_source_ranges" and
                function["arguments_contract"] == "packed_selected_source_ranges_v1",
                "invalid historical tool call contract")
        ranges = [{
            "byte_end": segment["source_byte_end"],
            "byte_start": segment["source_byte_start"],
            "path": segment["source_path"],
            "sha256": segment["source_sha256"],
        } for segment in segments if "source_path" in segment]
        require(ranges, "historical tool call has no selected source ranges")
        arguments_json = json.dumps({"ranges": ranges}, allow_nan=False,
                                    sort_keys=True, separators=(",", ":"))
        require(len(arguments_json.encode("utf-8")) <= 4 * 1024,
                "packed tool-call arguments exceed ordered-message bound")
        concrete_call = {"id": call["id"], "type": "function", "function": {
            "name": function["name"],
            "arguments_json": arguments_json,
        }}
        first_count = len(first_parts)
        for segment in segments:
            if segment.get("message_index") == 2:
                segment["part_index"] += first_count
            segment["message_index"] = 2
        tool_parts = first_parts + final_parts
        require(tool_parts and all(part.get("selected") is True for part in tool_parts),
                "historical tool response must contain every selected packed source part")
        messages = {"schema_version": 2, "messages": [
            {"role": "user", "parts": [
                {"text": history["initial_request_utf8"], "selected": False},
            ] + ([] if neutral_padding is None else [
                {"text": neutral_padding, "selected": False},
            ])},
            {"role": "assistant", "parts": [], "content_is_null": True,
             "tool_calls": [concrete_call]},
            {"role": "tool", "parts": tool_parts,
             "tool_call_id": history["tool_call_id"]},
            {"role": "user", "parts": [
                {"text": history["final_request_utf8"], "selected": False},
            ]},
        ]}
    else:
        require(isinstance(case.get("question_utf8"),str) and case["question_utf8"],
                "invalid frozen question")
        # Ordinary single-message tasks retain the sources on both sides of
        # the primary evidence. Their segment locations follow the merge.
        first_count = len(first_parts)
        for segment in segments:
            if segment.get("message_index") == 2:
                segment["message_index"] = 0
                segment["part_index"] += first_count
        first_parts.extend(final_parts)
        first_parts.append({"text": case["question_utf8"], "selected": False})
        messages = {"schema_version": 1, "messages": [{"role": "user", "parts": first_parts}]}
    for count in (prefix_padding_units, suffix_padding_units):
        require(type(count) is int and 0 <= count <= 768, "invalid eligibility guard padding")
    # Extend existing framing rather than exceeding the ordered-message part
    # bound. Prefix padding can cover only synthetic framing inside the
    # protected prefix; source-byte offsets continue to name the original file.
    if prefix_padding_units:
        prefix = "NEUTRAL PREFIX BEGIN" + (" neutral" * prefix_padding_units) + " NEUTRAL PREFIX END\n"
        part = messages["messages"][0]["parts"][0]
        part["text"] = prefix + part["text"]
        prefix_bytes = len(prefix.encode())
        for segment in segments:
            if segment["message_index"] == 0 and segment["part_index"] == 0:
                segment["prefix_padding_units"] = prefix_padding_units
                for name in ("part_byte_start", "part_byte_end", "evidence_part_byte_start", "evidence_part_byte_end"):
                    if name in segment:
                        segment[name] += prefix_bytes
                if segment["label"] == primary_label:
                    answer_part = part["text"].encode()
                    evidence_text_start += prefix_bytes
                    evidence_text_end += prefix_bytes
    if suffix_padding_units:
        suffix = "NEUTRAL SUFFIX BEGIN" + (" neutral" * suffix_padding_units) + " NEUTRAL SUFFIX END\n"
        part = messages["messages"][-1]["parts"][-1]
        require(part["selected"] is False, "suffix guard requires protected final instructions")
        part["text"] = suffix + part["text"]
    return {"messages": messages, "segments": segments, "primary_label": primary_label,
            "evidence_part_utf8": answer_part,
            "evidence_part_byte_start": evidence_text_start,
            "evidence_part_byte_end": evidence_text_end,
            "reference_utf8": case["reference_utf8"], "byte_budget": byte_budget,
            "padding_units": padding_units, "prefix_padding_units": prefix_padding_units,
            "suffix_padding_units": suffix_padding_units}



def evidence_quartile(start, end, prompt_tokens, protected_prefix=0, protected_recent=0):
    """Quartiles of the actual declared eligible interval, without model outputs."""
    require(all(type(x) is int for x in (start, end, prompt_tokens, protected_prefix, protected_recent)) and
            0 <= protected_prefix < prompt_tokens - protected_recent <= prompt_tokens and
            0 <= start < end <= prompt_tokens, "invalid evidence quartile interval")
    width = prompt_tokens - protected_prefix - protected_recent
    return min(3, max(0, (2 * (start + end - 2 * protected_prefix)) // width))


def next_candidate_control(control, observed_prompt_tokens, observed_quartile, *, evidence_eligible=True, evidence_token_start=None, evidence_token_end=None, protected_prefix=512, protected_recent=512):
    """Advance bounded source-byte then neutral-padding search without outputs."""
    required = {"target_prompt_tokens", "intended_quartile", "byte_budget",
        "maximum_byte_budget", "insertion_index", "maximum_insertion_index",
        "phase", "padding_units", "samples"}
    optional = {"prefix_padding_units", "suffix_padding_units", "insertion_subpart_units"}
    require(type(control) is dict and required.issubset(control) and set(control) <= required | optional,
            "invalid packing search control")
    require(type(protected_prefix) is int and type(protected_recent) is int and
            0 <= protected_prefix < observed_prompt_tokens - protected_recent <= observed_prompt_tokens,
            "invalid protected packing windows")
    native_windows = protected_prefix == 128 and protected_recent == 2048
    subpart = control.get("insertion_subpart_units", 0)
    require(type(subpart) is int and 0 <= subpart < 65536, "invalid fractional search placement")
    # Establish the exact prompt length before reserving native eligibility
    # guards. Shrinking an oversized source moves the evidence too; using its
    # old position against the final length can exhaust the guard prematurely.
    if not evidence_eligible:
        require(type(evidence_token_start) is int and type(evidence_token_end) is int and
                0 <= evidence_token_start < evidence_token_end <= observed_prompt_tokens,
                "invalid eligibility interval")
        require(evidence_token_start < protected_prefix or
                evidence_token_end > observed_prompt_tokens - protected_recent,
                "evidence eligibility failure is outside protected windows")
    native_tail = native_windows and not evidence_eligible and evidence_token_end > observed_prompt_tokens - protected_recent
    if not evidence_eligible and (not native_windows or
            observed_prompt_tokens == control["target_prompt_tokens"] and not native_tail):
        require(type(evidence_token_start) is int and type(evidence_token_end) is int and
                0 <= evidence_token_start < evidence_token_end <= observed_prompt_tokens,
                "invalid eligibility interval")
        prefix = control.get("prefix_padding_units", 0)
        suffix = control.get("suffix_padding_units", 0)
        margin = 128 if native_windows else 16
        final_prompt = min(observed_prompt_tokens, control["target_prompt_tokens"]) if native_windows else observed_prompt_tokens
        require(all(type(value) is int and 0 <= value <= 768 for value in (prefix, suffix)),
                "invalid eligibility guard state")
        if evidence_token_start < protected_prefix:
            prefix += protected_prefix - evidence_token_start + margin
        elif evidence_token_end > observed_prompt_tokens - protected_recent:
            suffix += evidence_token_end - (final_prompt - protected_recent) + margin
        else:
            raise ValueError("evidence eligibility failure is outside protected windows")
        require(max(prefix, suffix) <= 768, "eligibility guard search is exhausted")
        return {**control, "prefix_padding_units": prefix, "suffix_padding_units": suffix,
                "padding_units": 0, "phase": "source", "samples": []}
    target, budget, maximum = (control[k] for k in
                               ("target_prompt_tokens", "byte_budget", "maximum_byte_budget"))
    insertion, padding = control["insertion_index"], control["padding_units"]
    require(control["phase"] in ("source", "padding") and
            all(type(v) is int for v in (target,budget,maximum,insertion,padding,
                                         observed_prompt_tokens,observed_quartile)) and
            0 < budget <= maximum and 0 <= padding <= 256 and
            0 <= insertion <= control["maximum_insertion_index"] and 0 <= observed_quartile <= 3,
            "invalid packing search observation")
    samples=list(control["samples"])
    keys={"bytes","tokens","insertion_index","padding_units"}
    native_keys=keys|{"quartile","insertion_subpart_units"}
    require(len(samples)<128 and all(type(row) is dict and set(row) in (keys,native_keys) for row in samples),
            "invalid packing search history")
    require(not any(row["bytes"]==budget and row["insertion_index"]==insertion and
                    row["padding_units"]==padding and row.get("insertion_subpart_units",0)==subpart
                    for row in samples), "repeated packing candidate")
    sample={"bytes":budget,"tokens":observed_prompt_tokens,
            "insertion_index":insertion,"padding_units":padding}
    # An overlap with the protected tail is above every eligible quartile,
    # including quartile 3. Move/split source parts before consuming a guard.
    placement_quartile = 4 if native_tail and observed_prompt_tokens == target else observed_quartile
    if native_windows:sample.update(quartile=placement_quartile,insertion_subpart_units=subpart)
    samples.append(sample)
    intended=control["intended_quartile"]
    if observed_prompt_tokens==target and placement_quartile==intended and evidence_eligible:
        return {**control,"samples":samples,"completed":True}
    if observed_prompt_tokens==target:
        proposed = insertion + (1 if placement_quartile<intended else -1)
        crossed = native_windows and any(row["insertion_index"]==proposed and
            row.get("insertion_subpart_units",0)==0 and row.get("quartile",intended)!=intended and
            (row["quartile"]<intended)!=(placement_quartile<intended) for row in samples)
        if native_windows and (subpart or crossed):
            if not subpart:
                insertion=min(insertion,proposed);proposal=32768
            else:
                current=[row for row in samples if row["insertion_index"]==insertion and
                         "quartile" in row and row["tokens"]==target]
                lower=max((row.get("insertion_subpart_units",0) for row in current
                           if row["quartile"]<intended),default=0)
                upper=min((row.get("insertion_subpart_units",0) for row in current
                           if row["quartile"]>intended),default=65536)
                require(lower<upper,"fractional placement bounds conflict")
                proposal=(lower+upper)//2
            require(0<proposal<65536 and proposal!=subpart,"fractional placement search is exhausted")
            return {**control,"insertion_index":insertion,"insertion_subpart_units":proposal,
                    "phase":"source","padding_units":0,"samples":samples}
        insertion=proposed
        require(0<=insertion<=control["maximum_insertion_index"],
                "source placement cannot reach intended quartile")
        return {**control,"insertion_index":insertion,"phase":"source",
                "padding_units":0,"samples":samples}
    if control["phase"]=="source" and observed_prompt_tokens<target and             target-observed_prompt_tokens<=256:
        return {**control,"phase":"padding","padding_units":target-observed_prompt_tokens,
                "samples":samples}
    if control["phase"]=="padding":
        current=[r for r in samples if r["bytes"]==budget and r["insertion_index"]==insertion and
                 r.get("insertion_subpart_units",0)==subpart]
        lower=max((r for r in current if r["tokens"]<target),key=lambda r:(r["tokens"],r["padding_units"]),default=None)
        upper=min((r for r in current if r["tokens"]>target),key=lambda r:(r["tokens"],r["padding_units"]),default=None)
        if lower and upper:
            require(lower["padding_units"]<upper["padding_units"],"padding token counts are not monotonic")
            proposal=lower["padding_units"]+((target-lower["tokens"])*(upper["padding_units"]-lower["padding_units"]))//(upper["tokens"]-lower["tokens"])
            proposal=min(upper["padding_units"]-1,max(lower["padding_units"]+1,proposal))
        elif lower:
            proposal=min(256, padding + max(1, target-observed_prompt_tokens))
        else:
            proposal=max(0, padding - max(1, observed_prompt_tokens-target))
        require(proposal!=padding and not any(r["padding_units"]==proposal for r in current),
                "neutral padding search is exhausted")
        return {**control,"padding_units":proposal,"samples":samples}
    current=[r for r in samples if r["insertion_index"]==insertion and r["padding_units"]==0 and
             r.get("insertion_subpart_units",0)==subpart]
    lower=max((r for r in current if r["tokens"]<target),key=lambda r:(r["tokens"],r["bytes"]),default=None)
    upper=min((r for r in current if r["tokens"]>target),key=lambda r:(r["tokens"],r["bytes"]),default=None)
    if lower and upper:
        require(lower["bytes"]<upper["bytes"],"loaded token counts are not monotonic")
        proposal=lower["bytes"]+((target-lower["tokens"])*(upper["bytes"]-lower["bytes"]))//(upper["tokens"]-lower["tokens"])
        proposal=min(upper["bytes"]-1,max(lower["bytes"]+1,proposal))
    elif lower:
        proposal=min(maximum,max(budget+1,(budget*target)//max(1,observed_prompt_tokens)))
    else:
        proposal=max(1,min(budget-1,(budget*target)//max(1,observed_prompt_tokens)))
    if proposal==budget or any(r["bytes"]==proposal for r in current):
        candidates=[]
        lo=lower["bytes"]+1 if lower else 1; hi=upper["bytes"]-1 if upper else maximum
        for delta in range(1,257):
            for value in (proposal-delta,proposal+delta):
                if lo<=value<=hi and not any(r["bytes"]==value for r in current): candidates.append(value)
            if candidates: break
        require(candidates,"loaded-tokenizer source endpoint search is exhausted")
        proposal=min(candidates)
    return {**control,"byte_budget":proposal,"samples":samples}


def _write_exclusive(path, raw):
    with Path(path).open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())

def minimal_evidence_interval(offsets, rendered_start, rendered_end):
    require(type(rendered_start) is int and type(rendered_end) is int and
            0 <= rendered_start < rendered_end, "invalid rendered evidence interval")
    selected = []
    previous_start = previous_end = 0
    for index, pair in enumerate(offsets):
        require(type(pair) in (list, tuple) and len(pair) == 2 and
                type(pair[0]) is int and type(pair[1]) is int, "invalid token offset")
        start, end = pair
        require((start == 0 and end == 0) or
                (start <= end and start >= previous_start and end >= previous_end),
                "backward token offset")
        if start or end:
            previous_start, previous_end = start, end
            if start < rendered_end and end > rendered_start:
                selected.append(index)
    require(selected and selected == list(range(selected[0], selected[-1] + 1)),
            "evidence has no contiguous token interval")
    return selected[0], selected[-1] + 1


def verify_exact_materialization(case, observation):
    required = {"context_tokens", "reserved_output_tokens", "intended_compactable_quartile",
                "evidence_part_utf8", "evidence_part_byte_start", "evidence_part_byte_end",
                "selected_label"}
    require(type(case) is dict and required.issubset(case), "invalid packed case")
    require(type(observation) is dict and set(observation) == {
        "rendered_prompt", "prompt_tokens", "prompt_token_offsets", "rendered_labels",
        "prompt_token_labels", "loaded_identity"}, "invalid materialization observation")
    rendered = observation["rendered_prompt"]
    part = case["evidence_part_utf8"]
    require(type(rendered) is bytes and type(part) is bytes and part,
            "invalid rendered/selected-part bytes")
    start = rendered.find(part)
    require(start >= 0 and rendered.find(part, start + 1) < 0,
            "selected evidence part is not unique in rendered prompt")
    evidence_start = start + case["evidence_part_byte_start"]
    evidence_end = start + case["evidence_part_byte_end"]
    require(0 <= evidence_start < evidence_end <= start + len(part),
            "evidence escapes selected part")
    labels = observation["rendered_labels"]
    require(len(labels) == len(rendered) and
            all(value == case["selected_label"] for value in labels[evidence_start:evidence_end]),
            "evidence bytes lack selected provenance")
    prompt = observation["prompt_tokens"]
    offsets = observation["prompt_token_offsets"]
    token_labels = observation["prompt_token_labels"]
    require(len(prompt) == len(offsets) == len(token_labels) and
            len(prompt) + case["reserved_output_tokens"] == case["context_tokens"],
            "materialized prompt is not exact target length")
    token_start, token_end = minimal_evidence_interval(offsets, evidence_start, evidence_end)
    require(all(value == case["selected_label"] for value in token_labels[token_start:token_end]),
            "evidence tokens lack selected provenance")
    quartile = min(3, (2 * (token_start + token_end)) // len(prompt))
    require(quartile == case["intended_compactable_quartile"],
            "actual evidence tokens are in the wrong quartile")
    return {"prompt_tokens": len(prompt), "evidence_token_start": token_start,
            "evidence_token_end": token_end, "actual_quartile": quartile,
            "loaded_identity": observation["loaded_identity"]}


def emit_requests(work, source_root, authority, campaign_root, length, bucket):
    import quality_ordered_messages as ordered
    require(type(authority) is dict and set(authority) == {
        "schema_version", "session_id", "loaded_identity", "qualified_context_tokens",
        "reserved_output_tokens", "settings_sha256", "variant_settings_sha256"} and
        authority["schema_version"] == 1, "invalid materializer authority")
    cell = next((value for value in work["cells"]
                 if value["context_tokens"] == length and value["bucket"] == bucket), None)
    require(cell is not None and len(cell["cases"]) == CASES_PER_CELL, "packing cell missing")
    root = Path(campaign_root)
    root.mkdir(parents=True, exist_ok=False)
    cases, batch = [], []
    for index, item in enumerate(cell["cases"]):
        target_prompt = length - authority["reserved_output_tokens"]
        require(target_prompt > 0, "reserved output exceeds target context")
        # This is only a deterministic first proposal. Exact loaded counts,
        # never this byte heuristic, decide every subsequent step and finality.
        byte_budget = min(item["available_source_bytes"], max(8192, target_prompt * 3))
        candidate = compose_candidate(item, work["source_rows"], source_root, byte_budget)
        messages_raw = (json.dumps(candidate["messages"], ensure_ascii=False, allow_nan=False,
                                   separators=(",", ":"), sort_keys=True) + "\n").encode()
        reference_raw = candidate["reference_utf8"].encode("utf-8")
        messages_name = f"packing-{length}-{bucket}-{index:03d}.messages.json"
        reference_name = f"packing-{length}-{bucket}-{index:03d}.reference.txt"
        _write_exclusive(root / messages_name, messages_raw)
        _write_exclusive(root / reference_name, reference_raw)
        messages_value = ordered.loads(messages_raw)
        case = item["case"]
        cases.append({"case_id": case["id"], "messages_file": messages_name,
            "messages_sha256": sha(messages_raw),
            "request_content_sha256": ordered.semantic_digest(messages_value),
            "source_sha256": ordered.selected_source_digest(messages_value),
            "selected_part_sha256s": ordered.selected_part_digests(messages_value),
            "reference_file": reference_name, "reference_sha256": sha(reference_raw),
            "qualified_context_tokens": authority["qualified_context_tokens"],
            "reserved_output_tokens": authority["reserved_output_tokens"],
            "settings_sha256": authority["settings_sha256"],
            "variant_settings_sha256": authority["variant_settings_sha256"]})
        batch.append({"request_index": index, "case_id": case["id"],
            "context_tokens": length, "target_prompt_tokens": target_prompt,
            "intended_compactable_quartile": case["intended_compactable_quartile"],
            "candidate": {"messages_file": messages_name, "messages_sha256": sha(messages_raw),
                "segments": candidate["segments"], "primary_label": candidate["primary_label"],
                "evidence_part_byte_start": candidate["evidence_part_byte_start"],
                "evidence_part_byte_end": candidate["evidence_part_byte_end"],
                "byte_budget": candidate["byte_budget"]}})
    return ({"schema_version": 1, "session_id": authority["session_id"],
             "loaded_identity": authority["loaded_identity"], "cases": cases},
            {"schema_version": 1, "stage": "quality_loaded_tokenizer_pack_batch",
             "completed": False, "model_outputs_observed": False, "model": work["model"],
             "context_tokens": length, "bucket": bucket, "cases": batch})


def primary_selected_part(messages, segments, label):
    primary = next((segment for segment in segments if segment.get("label") == label), None)
    require(type(primary) is dict and type(primary.get("message_index")) is int and
            type(primary.get("part_index")) is int and
            0 <= primary["message_index"] < len(messages["messages"]),
            "invalid primary selected segment")
    message = messages["messages"][primary["message_index"]]
    require(0 <= primary["part_index"] < len(message["parts"]) and
            message["parts"][primary["part_index"]].get("selected") is True,
            "primary segment does not resolve to selected source text")
    return message["parts"][primary["part_index"]]


def finalize_batch(plan, batch, journal_path, campaign_root):
    import quality_ordered_messages as ordered
    import quality_tokenizer_materialize as materialize
    require(type(plan) is dict and type(batch) is dict and
            batch.get("stage") == "quality_loaded_tokenizer_pack_batch",
            "invalid packing batch")
    raw_lines = Path(journal_path).read_bytes().splitlines()
    reports = [materialize._strict_json(line, "materialization journal") for line in raw_lines]
    require(len(reports) == len(plan["cases"]) == len(batch["cases"]),
            "materialization journal is incomplete")
    loaded_identity = plan["loaded_identity"]
    finalized = []
    for index, (case, packed, report) in enumerate(zip(plan["cases"], batch["cases"], reports)):
        expected = dict(case, session_id=plan["session_id"], request_index=index,
                        loaded_identity=loaded_identity)
        materialize.report_contract(report, campaign_root, expected)
        require(type(report["schema_version"]) is int and report["schema_version"] == 2,
                "packing requires labeled materialization schema 2")
        messages_raw = materialize._artifact(
            campaign_root, {"path": packed["candidate"]["messages_file"],
                            "sha256": packed["candidate"]["messages_sha256"], "count": 1},
            "packed messages", 1, 8 * 1024 * 1024)
        messages = ordered.loads(messages_raw)
        label = packed["candidate"]["primary_label"]
        evidence_part = primary_selected_part(
            messages, packed["candidate"]["segments"], label)["text"].encode("utf-8")
        rendered = materialize._artifact(campaign_root, report["rendered_prompt"],
                                         "rendered prompt", 32 * 1024 * 1024, 32 * 1024 * 1024)
        prompt_raw = materialize._artifact(campaign_root, report["prompt_tokens"],
                                           "prompt tokens", materialize.MAX_PROMPT_TOKENS,
                                           materialize.MAX_PROMPT_TOKENS * 8)
        prompt = materialize._i64(prompt_raw, report["prompt_tokens"]["count"], "prompt")
        offsets_raw = materialize._artifact(campaign_root, report["prompt_token_offsets"],
                                            "prompt token offsets", materialize.MAX_PROMPT_TOKENS,
                                            materialize.MAX_PROMPT_TOKENS * 8)
        offsets = materialize._offsets(offsets_raw, len(prompt), len(rendered))
        rendered_label_raw = materialize._artifact(campaign_root, report["rendered_labels"],
                                                    "rendered labels", 32 * 1024 * 1024,
                                                    32 * 1024 * 1024 * 4)
        rendered_labels = materialize._u32(rendered_label_raw,
                                           report["rendered_labels"]["count"], "rendered labels")
        prompt_label_raw = materialize._artifact(campaign_root, report["prompt_token_labels"],
                                                 "prompt token labels", materialize.MAX_PROMPT_TOKENS,
                                                 materialize.MAX_PROMPT_TOKENS * 4)
        prompt_labels = materialize._u32(prompt_label_raw,
                                         report["prompt_token_labels"]["count"], "prompt token labels")
        proof = verify_exact_materialization({
            "context_tokens": packed["context_tokens"],
            "reserved_output_tokens": case["reserved_output_tokens"],
            "intended_compactable_quartile": packed["intended_compactable_quartile"],
            "evidence_part_utf8": evidence_part,
            "evidence_part_byte_start": packed["candidate"]["evidence_part_byte_start"],
            "evidence_part_byte_end": packed["candidate"]["evidence_part_byte_end"],
            "selected_label": label,
        }, {"rendered_prompt": rendered, "prompt_tokens": prompt,
            "prompt_token_offsets": offsets, "rendered_labels": rendered_labels,
            "prompt_token_labels": prompt_labels, "loaded_identity": report["loaded_identity"]})
        finalized.append({"case_id": packed["case_id"], "request_index": index,
                          "messages_sha256": case["messages_sha256"],
                          "reference_sha256": case["reference_sha256"], **proof})
    require(len(finalized) == CASES_PER_CELL, "final packing cell is incomplete")
    return {"schema_version": 1, "stage": "quality_loaded_tokenizer_packed_cell",
            "completed": True, "model_outputs_observed": False, "model": batch["model"],
            "context_tokens": batch["context_tokens"], "bucket": batch["bucket"],
            "loaded_identity": loaded_identity, "cases": finalized}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init")
    init.add_argument("--blueprints", required=True)
    init.add_argument("--sources", required=True)
    init.add_argument("--source-root", required=True)
    init.add_argument("--seed", required=True)
    init.add_argument("--out", required=True)
    requests = sub.add_parser("requests")
    requests.add_argument("--work", required=True)
    requests.add_argument("--source-root", required=True)
    requests.add_argument("--authority", required=True)
    requests.add_argument("--campaign-root", required=True)
    requests.add_argument("--length", required=True, type=int, choices=LENGTHS)
    requests.add_argument("--bucket", required=True, choices=BUCKETS)
    requests.add_argument("--plan-out", required=True)
    requests.add_argument("--batch-out", required=True)
    finalize = sub.add_parser("finalize")
    finalize.add_argument("--plan", required=True)
    finalize.add_argument("--batch", required=True)
    finalize.add_argument("--journal", required=True)
    finalize.add_argument("--campaign-root", required=True)
    finalize.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    if args.command == "init":
        blueprints = strict_json(args.blueprints)
        sources = strict_json(args.sources)
        result = initialize(blueprints, sources, args.source_root, args.seed)
        result["blueprints_sha256"] = sha(Path(args.blueprints).read_bytes())
        result["source_manifest_sha256"] = sha(Path(args.sources).read_bytes())
        _write_exclusive(args.out, (json.dumps(result, allow_nan=False,
                                                separators=(",", ":"), sort_keys=True) + "\n").encode())
    elif args.command == "requests":
        plan, batch = emit_requests(strict_json(args.work), args.source_root,
                                    strict_json(args.authority), args.campaign_root,
                                    args.length, args.bucket)
        _write_exclusive(args.plan_out, (json.dumps(plan, allow_nan=False,
                                                     separators=(",", ":"), sort_keys=True) + "\n").encode())
        _write_exclusive(args.batch_out, (json.dumps(batch, allow_nan=False,
                                                      separators=(",", ":"), sort_keys=True) + "\n").encode())
    elif args.command == "finalize":
        result = finalize_batch(strict_json(args.plan), strict_json(args.batch),
                                args.journal, args.campaign_root)
        _write_exclusive(args.out, (json.dumps(result, allow_nan=False,
                                                separators=(",", ":"), sort_keys=True) + "\n").encode())


if __name__ == "__main__":
    main()
