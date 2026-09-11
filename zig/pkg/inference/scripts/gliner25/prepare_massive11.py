#!/usr/bin/env python3
"""Prepare pinned multilingual slots and classification; never run a model.

The full original test is preserved. Exact source IDs join translations, and
normalized duplicate text joins IDs before lower-priority split exclusions.
Chinese/Japanese annotation spacing is projected onto immutable source bytes.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter, defaultdict
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import unicodedata

import evaluation_contract as evaluation
import oracle

MANIFEST = Path(__file__).with_name("massive11_manifest.json")
TAG = re.compile(r"\[([a-z_]+) : (.*?)\]", re.DOTALL)
MAX_ROWS = 100000
MAX_ROW_BYTES = 65536
MAX_SHARD_ROWS = 1024
PRIORITY = {"train": 0, "dev": 1, "test": 2}


def raw_rows(path):
    """The original archive omits its final LF; normalized JSONL does not."""
    with path.open("rb") as stream:
        while line := stream.readline(MAX_ROW_BYTES + 1):
            evaluation.checked(len(line) <= MAX_ROW_BYTES, "raw source row byte cap exceeded")
            try:
                row = json.loads(line, object_pairs_hook=oracle._unique_object,
                    parse_constant=lambda _: (_ for _ in ()).throw(evaluation.EvaluationError("nonfinite source JSON")))
            except (UnicodeError, json.JSONDecodeError) as error:
                raise evaluation.EvaluationError("invalid original source JSONL") from error
            evaluation.checked(isinstance(row, dict), "source row must be an object")
            yield row


def annotation_spans(text, annotated, locale):
    """An ordinal character projection avoids ambiguous repeated-string search."""
    evaluation.checked(isinstance(text, str) and bool(text) and isinstance(annotated, str), "invalid source text")
    evaluation.checked(len(text.encode()) <= MAX_ROW_BYTES and len(annotated.encode()) <= MAX_ROW_BYTES, "source text byte cap exceeded")
    pieces, tags, previous, length = [], [], 0, 0
    for match in TAG.finditer(annotated):
        gap = annotated[previous:match.start()]
        pieces.append(gap); length += len(gap)
        value = match[2]
        evaluation.checked(bool(value) and value.strip() == value, "empty or padded source slot")
        tags.append((match[1], length, length + len(value)))
        pieces.append(value); length += len(value)
        previous = match.end()
    pieces.append(annotated[previous:])
    rendered = "".join(pieces)
    if rendered == text:
        positions = list(range(len(text)))
        policy = "exact"
    else:
        evaluation.checked(locale in ("zh-CN", "ja-JP") and rendered.replace(" ", "") == text.replace(" ", ""),
                           "annotation differs from immutable text beyond permitted CJK ASCII spaces")
        original = [index for index, char in enumerate(text) if char != " "]
        positions, cursor = [], 0
        for char in rendered:
            if char == " ":
                positions.append(None)
            else:
                evaluation.checked(cursor < len(original) and char == text[original[cursor]], "annotation character projection differs")
                positions.append(original[cursor]); cursor += 1
        evaluation.checked(cursor == len(original), "annotation projection lost source characters")
        policy = "cjk_ascii_space_projection"
    byte_offsets = [0]
    for char in text:
        byte_offsets.append(byte_offsets[-1] + len(char.encode()))
    facts = []
    for label, start, end in tags:
        mapped = [index for index in positions[start:end] if index is not None]
        evaluation.checked(bool(mapped), "slot has no source characters")
        first, last = mapped[0], mapped[-1] + 1
        facts.append({"type": label, "span": {"start": byte_offsets[first], "end": byte_offsets[last], "text": text[first:last]}})
    evaluation.checked(len({evaluation.encoded(fact, sorted_keys=True) for fact in facts}) == len(facts), "duplicate exact source slot")
    return facts, policy


def source_rows(corpus, manifest):
    for pin in (manifest["archive"], *manifest["files"]):
        oracle.verify_file(corpus / pin["path"], pin)
    rows, seen, partitions = [], set(), {}
    counts, alignment = Counter(), Counter()
    slots, intents, scenarios, mapping = set(), set(), set(), {}
    for locale in manifest["locales"]:
        for original in raw_rows(corpus / f"data/{locale}.jsonl"):
            identifier, split = original.get("id"), original.get("partition")
            evaluation.checked(isinstance(identifier, str) and identifier.isascii() and identifier.isdigit() and split in PRIORITY,
                               "invalid original source identity or partition")
            evaluation.checked(original.get("locale") == locale and (locale, identifier) not in seen, "duplicate or mismatched locale source ID")
            seen.add((locale, identifier))
            evaluation.checked(partitions.setdefault(identifier, split) == split, "source translation ID crosses partitions")
            facts, alignment_kind = annotation_spans(original["utt"], original["annot_utt"], locale)
            row = {"source_id": identifier, "locale": locale, "partition": split, "text": original["utt"],
                   "facts": facts, "intent": original["intent"], "scenario": original["scenario"], "alignment": alignment_kind}
            evaluation.checked(isinstance(row["intent"], str) and isinstance(row["scenario"], str), "source classification label is invalid")
            rows.append(row)
            evaluation.checked(len(rows) <= MAX_ROWS, "corpus row budget exceeded")
            counts[locale, split] += 1; alignment[locale, alignment_kind] += 1
            if split == "train":
                slots.update(fact["type"] for fact in facts); intents.add(row["intent"]); scenarios.add(row["scenario"])
                evaluation.checked(mapping.setdefault(row["intent"], row["scenario"]) == row["scenario"], "training intent maps to multiple scenarios")
    evaluation.checked(sorted(slots) == manifest["slot_types"] and sorted(intents) == manifest["intents"] and
                       sorted(scenarios) == manifest["scenarios"] and mapping == manifest["intent_to_scenario"], "full training-derived ontology differs")
    for locale in manifest["locales"]:
        evaluation.checked({split: counts[locale, split] for split in PRIORITY} == manifest["source_rows_per_locale"], "source split denominator differs")
        evaluation.checked({row["source_id"] for row in rows if row["locale"] == locale} == set(partitions), "a locale lost a translation family")
    for row in rows:
        evaluation.checked(row["intent"] in intents and row["scenario"] == mapping[row["intent"]] and
                           all(fact["type"] in slots for fact in row["facts"]), "held-out label is absent from the frozen training ontology")
    return rows, {locale: {kind: alignment[locale, kind] for kind in ("exact", "cjk_ascii_space_projection")}
                  for locale in manifest["locales"]}


def split_families(rows):
    """Union translation IDs and text duplicates before choosing any exclusion."""
    parent = {row["source_id"]: row["source_id"] for row in rows}

    def find(value):
        while parent[value] != value:
            parent[value] = parent[parent[value]]; value = parent[value]
        return value

    normalized, partitions = {}, {}
    for row in rows:
        identifier = row["source_id"]
        evaluation.checked(partitions.setdefault(identifier, row["partition"]) == row["partition"], "translation source split differs")
        text = " ".join(unicodedata.normalize("NFC", row["text"]).casefold().split())
        digest = evaluation.digest(text.encode())
        previous = normalized.setdefault(digest, identifier)
        left, right = sorted((find(identifier), find(previous)))
        parent[right] = left
    components = defaultdict(list)
    for identifier in sorted(parent):
        components[find(identifier)].append(identifier)
    excluded, identities, groups = set(), {}, []
    for members in components.values():
        family = "massive/1.1/family/" + evaluation.digest(evaluation.encoded(members))
        retained = max((partitions[identifier] for identifier in members), key=PRIORITY.__getitem__)
        dropped = [identifier for identifier in members if partitions[identifier] != retained]
        excluded.update(dropped)
        for identifier in members:
            identities[identifier] = family
        if len(members) > 1:
            groups.append({"family_id": family, "source_ids": members, "source_partitions": [partitions[i] for i in members],
                           "retained_partition": retained, "excluded_source_ids": dropped})
    audit = {"scope": "gliner25_massive_split_audit/v1", "qualification": False,
        "decision_inputs": "original_translation_id_original_partition_and_normalized_text_only",
        "unicode_version": unicodedata.unidata_version, "official_test_preserved": True,
        "within_test_multiplicity_preserved": True, "source_translation_ids": len(parent), "families": len(components),
        "cross_id_duplicate_components": sorted(groups, key=lambda group: group["family_id"]),
        "excluded_source_ids": sorted(excluded), "cross_split_components": sum(bool(group["excluded_source_ids"]) for group in groups),
        "pretraining_contamination_status": "unknown"}
    return [dict(row, family_id=identities[row["source_id"]]) for row in rows if row["source_id"] not in excluded], audit


def word_patterns(source, expected_sha):
    evaluation.checked(oracle.sha256_file(source) == expected_sha, "pinned word splitter source differs")
    classes = {node.name: node for node in ast.parse(source.read_text()).body if isinstance(node, ast.ClassDef)}
    result = {}
    for name, class_name in (("whitespace", "WhitespaceTokenSplitter"), ("char", "CharLevelSplitter")):
        values = [node.value for node in classes[class_name].body if isinstance(node, ast.Assign)
                  and any(isinstance(target, ast.Name) and target.id == "_PATTERN" for target in node.targets)]
        evaluation.checked(len(values) == 1 and isinstance(values[0], ast.Call), "word pattern declaration differs")
        call = values[0]
        evaluation.checked(isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name) and
                           call.func.value.id == "re" and call.func.attr == "compile" and not call.keywords, "unexpected source pattern expression")
        pattern = ast.literal_eval(call.args[0])
        flags = 0
        if len(call.args) == 2:
            flags_node = call.args[1]
            evaluation.checked(isinstance(flags_node, ast.BinOp) and isinstance(flags_node.op, ast.BitOr), "pattern flags differ")
            for flag in (flags_node.left, flags_node.right):
                evaluation.checked(isinstance(flag, ast.Attribute) and isinstance(flag.value, ast.Name) and flag.value.id == "re"
                                   and flag.attr in ("VERBOSE", "IGNORECASE"), "unexpected regex flag")
                flags |= getattr(re, flag.attr)
        else:
            evaluation.checked(len(call.args) == 1, "pattern arity differs")
        result[name] = re.compile(pattern, flags)
    return result


def profile_definitions(manifest):
    profiles = [{"id": "entities_" + locale, "locale": locale, "task": "entities", "word_splitter": "whitespace"}
                for locale in manifest["locales"]]
    profiles += [{"id": "entities_" + locale + "_char", "locale": locale, "task": "entities", "word_splitter": "char"}
                 for locale in manifest["char_locales"]]
    profiles += [{"id": task + "_en-US", "locale": "en-US", "task": task, "word_splitter": "whitespace"}
                 for task in ("intent", "intent_scenario")]
    return profiles


def schema_and_metrics(profile, manifest):
    if profile["task"] == "entities":
        return {"entities": manifest["slot_types"]}, {
            "entity_exact": {"counting": "set", "definition": "Exact slot type and original UTF-8 occurrence"},
            **{"entity_type/" + name: {"counting": "set", "definition": "Exact source occurrence for " + name} for name in manifest["slot_types"]}}
    schema = {"classifications": [{"name": "intent", "labels": manifest["intents"], "mode": "single", "activation": "softmax"}]}
    metrics = {"intent_exact": {"counting": "set", "definition": "One exact original intent label; micro F1 equals accuracy with complete single-label outputs"},
        **{"intent_type/" + name: {"counting": "set", "definition": "Exact original intent " + name} for name in manifest["intents"]}}
    if profile["task"] == "intent_scenario":
        schema["classifications"].append({"name": "scenario", "labels": manifest["scenarios"], "mode": "single", "activation": "softmax"})
        schema["classification_constraints"] = [{"type": "Implies", "cond": {"type": "LabelRef", "task": "intent", "label": intent},
            "then": {"type": "LabelRef", "task": "scenario", "label": scenario}} for intent, scenario in manifest["intent_to_scenario"].items()]
        metrics.update(scenario_exact={"counting": "set", "definition": "Exact separately predicted original scenario label"},
            joint_intent_scenario={"counting": "set", "definition": "Exact predicted intent/scenario pair"},
            constraints_satisfied={"counting": "set", "definition": "Both outputs are single valid labels and satisfy the frozen train-derived mapping"})
    return schema, metrics


def gold_facts(row, profile, manifest):
    if profile["task"] == "entities":
        return {"entity_exact": row["facts"], **{"entity_type/" + name: [fact for fact in row["facts"] if fact["type"] == name]
                                                      for name in manifest["slot_types"]}}
    fact = {"label": row["intent"]}
    metrics = {"intent_exact": [fact], **{"intent_type/" + name: [fact] if name == row["intent"] else [] for name in manifest["intents"]}}
    if profile["task"] == "intent_scenario":
        metrics.update(scenario_exact=[{"label": row["scenario"]}], joint_intent_scenario=[{"intent": row["intent"], "scenario": row["scenario"]}],
                       constraints_satisfied=[{"valid": True}])
    return metrics


def prediction_facts(request, output, backend, profile, manifest):
    """Interpret actual public outputs without opening a gold annotation."""
    schema, definitions = schema_and_metrics(profile, manifest)
    evaluation.checked(request["schema"] == schema and request["offset_unit"] == "utf8_bytes" and
                       request["options"] == dict(manifest["request_defaults"], word_splitter=profile["word_splitter"]),
                       "request differs from the fixed complete profile")
    evaluation.checked(backend in ("python", "native", "metal"), "unknown prediction backend")
    def confidence(value):
        evaluation.checked(type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1, "invalid confidence")
    if profile["task"] == "entities":
        text = request["text"]; raw = text.encode(); positions = [0]
        for char in text: positions.append(positions[-1] + len(char.encode()))
        boundaries = set(positions)
        if backend == "python":
            groups = output["entities"]
        else:
            groups = {group["name"]: group["values"] for group in output["entities"]}
            evaluation.checked(len(groups) == len(output["entities"]), "duplicate native entity group")
        evaluation.checked(isinstance(groups, dict) and set(groups) == set(manifest["slot_types"]), "full entity query coverage differs")
        facts = []
        for name in manifest["slot_types"]:
            evaluation.checked(isinstance(groups[name], list), "entity output must be a list")
            for value in groups[name]:
                confidence(value["confidence"])
                if backend == "python":
                    start, end = value["start"], value["end"]
                    evaluation.checked(type(start) is int and type(end) is int and 0 <= start < end < len(positions), "invalid Python codepoint span")
                    start, end = positions[start], positions[end]
                else:
                    span = value["source"]
                    evaluation.checked(isinstance(span, dict) and span.get("unit") == "utf8_bytes", "native source units differ")
                    start, end = span["start"], span["end"]
                evaluation.checked(type(start) is int and type(end) is int and start in boundaries and end in boundaries and start < end
                                   and value["text"] == raw[start:end].decode(), "predicted slot differs from exact source bytes")
                facts.append({"type": name, "span": {"start": start, "end": end, "text": value["text"]}})
        result = {"entity_exact": facts, **{"entity_type/" + name: [fact for fact in facts if fact["type"] == name] for name in manifest["slot_types"]}}
    else:
        task_names = [task["name"] for task in schema["classifications"]]
        if backend == "python":
            groups = {name: output[name] for name in task_names}
        else:
            groups = {group["name"]: group["labels"] for group in output["classifications"]}
            evaluation.checked(len(groups) == len(output["classifications"]) and set(groups) == set(task_names), "classification query coverage differs")
        selected = {}
        for task in schema["classifications"]:
            value = groups[task["name"]]
            if backend == "python":
                evaluation.checked(isinstance(value, dict), "classification output shape differs")
                label = value.get("label", value.get("value")); confidence(value["confidence"])
            else:
                evaluation.checked(isinstance(value, list) and len(value) == 1, "single classification output count differs")
                label = value[0]["label"]; confidence(value[0]["confidence"])
            evaluation.checked(isinstance(label, str) and label in task["labels"], "classification output label is undeclared")
            selected[task["name"]] = label
        result = gold_facts({**selected}, profile, manifest)
        if profile["task"] == "intent_scenario":
            result["constraints_satisfied"] = [{"valid": manifest["intent_to_scenario"][selected["intent"]] == selected["scenario"]}]
    evaluation.metric_facts(result, definitions, request["text"])
    return result


def prepare(corpus, output, upstream):
    manifest = oracle.read_json(MANIFEST)
    rows, alignment = source_rows(corpus, manifest)
    retained, audit = split_families(rows)
    patterns = word_patterns(upstream / manifest["source_preprocessing"]["file"], manifest["source_preprocessing"]["sha256"])
    summaries = []
    with oracle.atomic_output_directory(output) as staging:
        for profile in profile_definitions(manifest):
            directory = staging / profile["id"]; directory.mkdir()
            def put(name, data):
                path = directory / name; path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("xb") as target: target.write(data)
                return {"path": name, "size_bytes": len(data), "sha256": evaluation.digest(data)}
            # Hard links preserve exact local bytes without copying the same
            # 66 MiB corpus into each independently admissible profile lock.
            source_pins = []
            for pin in manifest["files"]:
                relative = "source/" + pin["path"]; target = directory / relative; target.parent.mkdir(parents=True, exist_ok=True)
                try: os.link(corpus / pin["path"], target)
                except OSError: shutil.copyfile(corpus / pin["path"], target)
                source_pins.append(dict(pin, path=relative))
            schema, metrics = schema_and_metrics(profile, manifest)
            schema_pin = put("schema.json", evaluation.encoded(schema) + b"\n")
            metrics_pin = put("metrics.json", evaluation.encoded(metrics) + b"\n")
            adapter_pin = put("adapter.py", Path(__file__).read_bytes())
            manifest_pin = put("corpus_manifest.json", MANIFEST.read_bytes())
            audit_pin = put("split_audit.json", evaluation.encoded(audit) + b"\n")
            splitter_pin = put("word_splitter.py", (upstream / manifest["source_preprocessing"]["file"]).read_bytes())
            split_files, statistics = [], {}
            for source_split, split in (("train", "train"), ("dev", "calibration"), ("test", "test")):
                selected = sorted((row for row in retained if row["locale"] == profile["locale"] and row["partition"] == source_split),
                                  key=lambda row: int(row["source_id"]))
                evaluation.checked(bool(selected), "a profile split became empty")
                data = bytearray(); word_counts, not_representable = [], 0
                for row in selected:
                    facts = gold_facts(row, profile, manifest)
                    evaluation.metric_facts(facts, metrics, row["text"])
                    normalized = {"id": f"massive/1.1/{row['locale']}/{row['partition']}/{row['source_id']}",
                        "family_id": row["family_id"], "language": row["locale"], "schema_id": profile["id"], "text": row["text"], "gold": facts}
                    data.extend(evaluation.encoded(normalized) + b"\n")
                    text = row["text"]
                    matches = list(patterns[profile["word_splitter"]].finditer(text if text.endswith((".", "!", "?")) else text + "."))
                    word_counts.append(len(matches))
                    starts = {len(text[:match.start()].encode()) for match in matches if match.start() < len(text)}
                    ends = {len(text[:min(match.end(), len(text))].encode()) for match in matches if match.start() < len(text)}
                    not_representable += sum(fact["span"]["start"] not in starts or fact["span"]["end"] not in ends for fact in row["facts"])
                file_pin = put(split + ".jsonl", bytes(data))
                split_files.append({"split": split, "records": len(selected), "file": file_pin})
                statistics[split] = {"source_records": manifest["source_rows_per_locale"][source_split], "records": len(selected),
                    "slots": sum(len(row["facts"]) for row in selected), "maximum_utf8_bytes": max(len(row["text"].encode()) for row in selected),
                    "maximum_processor_words_including_terminal": max(word_counts), "processor_words_over_128": sum(count > 128 for count in word_counts),
                    "gold_slot_occurrences_not_representable_by_splitter": not_representable}
            request_options = dict(manifest["request_defaults"], word_splitter=profile["word_splitter"])
            lock = {"scope": evaluation.SCOPE, "status": "locked", "qualification": False,
                "upstream_commit": oracle.UPSTREAM_COMMIT, "unicode_version": unicodedata.unidata_version,
                "harness_sha256": oracle.sha256_file(Path(evaluation.__file__)), "schema_selection": "fixed_before_test", "test_used_for_tuning": False,
                "adapter_file": adapter_pin, "adapter_sha256": adapter_pin["sha256"], "metric_contract_file": metrics_pin,
                "metric_contract_sha256": metrics_pin["sha256"], "schemas": [{"id": profile["id"], "origin": "training_only", "file": schema_pin}],
                "request_options": request_options, "offset_unit": "utf8_bytes", "source_files": source_pins,
                "supporting_files": [manifest_pin, audit_pin, splitter_pin], "metrics": metrics, "splits": split_files,
                "dataset": {"id": "massive11", "profile": profile, "source_manifest_sha256": manifest_pin["sha256"],
                    "split_policy": manifest["split_policy"], "split_audit_sha256": audit_pin["sha256"], "text_profile": manifest["text_policy"],
                    "all_test_rows_retained": True, "encoded_token_capacity_check": "required_at_execution_no_silent_drop_or_truncation",
                    "pretraining_contamination_status": "unknown"}}
            oracle.write_json(directory / "lock.json", lock)
            admitted = evaluation.audit(directory / "lock.json")["summary"]
            blinded = evaluation.prepare(directory / "lock.json", directory / "prepared")
            requests = list(evaluation.rows(directory / "prepared/requests.jsonl"))
            shards = []
            for start in range(0, len(requests), MAX_SHARD_ROWS):
                subset = requests[start:start + MAX_SHARD_ROWS]
                body = {"scope": "gliner25_blinded_transport_shard/v1", "qualification": False,
                    "lock_sha256": admitted["lock_sha256"], "prepared_sha256": oracle.sha256_file(directory / "prepared/prepared.json"),
                    "global_records": len(requests), "index": len(shards), "start": start, "end": start + len(subset), "requests": subset}
                pin = put(f"prepared/shard_{len(shards):03d}.json", evaluation.encoded(body) + b"\n")
                shards.append({"index": len(shards), "start": start, "end": start + len(subset), "file": pin})
            transport = {"scope": "gliner25_transport_manifest/v1", "qualification": False, "records": len(requests),
                "request_ids_sha256": evaluation.digest(evaluation.encoded([row["request_id"] for row in requests])),
                "request_sha256s_sha256": evaluation.digest(evaluation.encoded([row["request_sha256"] for row in requests])), "shards": shards,
                "completion_policy": "exact_global_id_coverage_and_order_all_errors_retained_atomically_aggregate_before_quality_report"}
            transport_pin = put("transport.json", evaluation.encoded(transport) + b"\n")
            summaries.append({"profile": profile, "statistics": statistics, "admission": admitted, "prepared": blinded,
                              "transport": transport_pin, "lock_sha256": oracle.sha256_file(directory / "lock.json"), "qualification": False})
        summary = {"scope": "gliner25_massive11_preparation/v1", "qualification": False, "no_model_execution": True,
            "source_manifest_sha256": oracle.sha256_file(MANIFEST), "adapter_sha256": oracle.sha256_file(Path(__file__)),
            "harness_sha256": oracle.sha256_file(Path(evaluation.__file__)), "source_alignment_counts": alignment,
            "split_audit": audit, "profiles": summaries, "worker_support": "pending_explicit_profile_and_transport_integration"}
        oracle.write_json(staging / "preparation.json", summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    args = parser.parse_args()
    result = prepare(args.corpus_dir, args.output_dir, args.upstream)
    print("prepared", len(result["profiles"]), "profiles; excluded original source IDs", len(result["split_audit"]["excluded_source_ids"]))
    for row in result["profiles"]:
        print(row["profile"]["id"], row["statistics"]["test"], row["lock_sha256"])


if __name__ == "__main__":
    main()
