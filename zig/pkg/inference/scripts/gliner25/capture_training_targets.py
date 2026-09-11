#!/usr/bin/env python3
"""Capture pinned target preprocessing without a tokenizer or model load.

The real schema transformer builds word-level labels; the real boundary target
compiler packs them. Native annotations independently specify exact original
offsets and declared enum identities. Attribute gold is explicitly lowered to
hidden entity labels because the pinned training helper has no attribute API.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import subprocess
import sys

PIN = "3c913c7369301133d3b7699252074c4303ada50e"


def source(start, end, unit="utf8_bytes"):
    return dict(start=start, end=end, unit=unit)


def field(index, *values):
    return dict(field=index, values=list(values))


def document(*spans):
    return dict(document=list(spans))


def sample(text, schema, upstream, **annotations):
    return dict(text=text, schema=schema, upstream=upstream,
                annotations=dict(schema_fingerprint=[0] * 32, **annotations))


def fixtures():
    meta = {"deal": {"mode": "natural", "anchor": "party", "fields": {
        "party": {"cardinality": "required_one"}, "state": {"cardinality": "optional_one"},
    }}}
    rich = sample(
        "Ada met Acme. Bob paid €12.",
        {"entities": ["person", "company"], "entity_attributes": {
            "tone": {"labels": ["positive", "negative"], "applies_to": ["person"]},
        }, "classifications": [{"name": "topics", "labels": ["meeting", "billing"], "mode": "multi"}],
         "structures": {"deal": {"mode": "natural", "anchor": "party", "fields": {
             "party": {"dtype": "str", "cardinality": "required_one"},
             "state": {"dtype": "str", "choices": ["paid", "due"], "cardinality": "optional_one"},
         }}}, "relations": [{"type": "met"}]},
        {"json_structures": [
            {"deal": {"party": "Ada", "state": {"value": "paid", "choices": ["paid", "due"]}}},
            {"deal": {"party": "Bob", "state": "due"}},
        ], "entities": {"person": ["Ada", "Bob"], "company": ["Acme"], "negative": ["Bob"], "positive": ["Ada"]},
         "relations": [{"met": {"head": "Ada", "tail": "Acme"}}],
         "classifications": [{"task": "topics", "labels": ["meeting", "billing"], "true_label": ["billing"], "multi_label": True}],
         "record_metadata": meta},
        entities=[
            dict(entity_type=0, source=source(0, 3), attributes=[dict(group=0, labels=[0])]),
            dict(entity_type=1, source=source(8, 12)),
            dict(entity_type=0, source=source(14, 17), attributes=[dict(group=0, labels=[1])]),
        ],
        classifications=[dict(task=0, labels=[1])],
        records=[dict(structure=0, id="0:0", fields=[field(0, document(source(0, 3))), field(1, {"choice": 0})]),
                 dict(structure=0, id="0:1", fields=[field(0, document(source(14, 17))), field(1, {"choice": 1})])],
        relations=[dict(relation_type=0, head={"entity": 0}, tail={"entity": 1})],
    )
    plain = sample("Bob.", {"entities": ["person"]}, {"entities": {"person": ["Bob"]}},
                   entities=[dict(entity_type=0, source=source(0, 3))])
    latent = sample(
        "Ada Ada blue red", {"structures": {"event": {"mode": "latent", "occurrence_policy": "latent_all", "fields": {
            "actor": {"dtype": "str", "cardinality": "required_one"}, "tags": {"dtype": "list", "cardinality": "zero_or_more"},
        }}}},
        {"json_structures": [{"event": {"actor": "Ada", "tags": ["blue", "red"]}}], "record_metadata": {
            "event": {"mode": "latent", "occurrence_policy": "latent_all", "fields": {
                "actor": {"cardinality": "required_one"}, "tags": {"cardinality": "zero_or_more"},
            }},
        }},
        records=[dict(structure=0, id="0:0", fields=[
            field(0, document(source(0, 3), source(4, 7))),
            field(1, document(source(8, 12)), document(source(13, 16))),
        ])],
    )
    anchorless = sample(
        "blue", {"structures": {"event": {"mode": "anchorless", "fields": {
            "actor": {"dtype": "str", "cardinality": "optional_one"}, "tags": {"dtype": "list", "cardinality": "one_or_more"},
        }}}},
        {"json_structures": [{"event": {"actor": None, "tags": ["blue"]}}], "record_metadata": {
            "event": {"mode": "anchorless", "fields": {
                "actor": {"cardinality": "optional_one"}, "tags": {"cardinality": "one_or_more"},
            }},
        }}, records=[dict(structure=0, id="0:0", fields=[field(1, document(source(0, 4)))])],
    )
    negative = sample("No matches.", {"entities": ["person"]}, {"entities": {"person": []}})
    classification = sample(
        "Happy.", {"classifications": [{"name": "sentiment", "labels": ["good", "bad"]}]},
        {"classifications": [{"task": "sentiment", "labels": ["good", "bad"], "true_label": ["good"]}]},
        classifications=[dict(task=0, labels=[0])],
    )
    unicode_sample = sample(
        "😀Ada café", {"entities": ["word"], "entity_attributes": {"status": {"labels": ["yes", "no"]}}},
        {"entities": {"word": ["café"], "no": [], "yes": ["café"]}},
        entities=[dict(entity_type=0, source=source(5, 9, "unicode_codepoints"), attributes=[dict(group=0, labels=[0])])],
    )
    return [
        dict(id="mixed", samples=[rich]), dict(id="ragged", samples=[rich, plain]),
        dict(id="fixed_capacity", samples=[rich, plain], capacity=4),
        dict(id="latent_alternatives", samples=[latent]), dict(id="anchorless_absent", samples=[anchorless]),
        dict(id="negative_only", samples=[negative]), dict(id="classification_only", samples=[classification]),
        dict(id="unicode_whitespace", samples=[unicode_sample]),
        dict(id="unicode_char", samples=[unicode_sample], word_splitter="char"),
        dict(id="capacity_error", samples=[rich], capacity=1),
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, default=Path(__file__).parents[2] / "testdata/gliner25/training_targets.json")
    args = parser.parse_args()
    source_files = ("gliner2/processor.py", "gliner2/processing/boundary_preprocessing.py", "gliner2/processing/records.py",
                    "gliner2/processing/targets.py", "gliner2/processing/layouts.py", "gliner2/processing/word_splitter.py")
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=args.upstream, text=True).strip()
    if actual != PIN:
        raise SystemExit(f"wrong upstream revision: {actual}")
    if subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no", "--", *source_files], cwd=args.upstream, text=True):
        raise SystemExit("pinned target sources have local modifications")
    sys.path.insert(0, str(args.upstream))
    import torch
    from gliner2.processor import SamplingConfig, SchemaTransformer
    from gliner2.processing.boundary_preprocessing import build_boundary_batch_metadata
    from gliner2.processing.targets import TargetCapacityError
    from gliner2.processing.word_splitter import resolve_word_splitter

    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    rows = []
    for case in fixtures():
        transformer = SchemaTransformer.__new__(SchemaTransformer)
        transformer.is_training = False  # Full declared schemas; no gold-dependent augmentation.
        transformer.sampling_config = SamplingConfig()
        transformer.word_splitter = resolve_word_splitter(case.get("word_splitter", "whitespace"))
        transformed, lengths, words_out, prefixes = [], [], [], []
        for item in case["samples"]:
            raw = copy.deepcopy(item["upstream"])
            prefix = transformer._build_classification_prefix(raw)
            if prefix:
                transformer._wrap_classification_fields(raw, prefix)
            text = item["text"]
            if not text.endswith((".", "!", "?")):
                text += "."
            words = prefix + [word for word, _, _ in transformer.word_splitter(text, lower=True)]
            result = transformer._build_outputs(transformer._infer_from_json(raw), raw, words, len(prefix))
            transformed.append(result)
            lengths.append(len(words))
            words_out.append(words)
            prefixes.append(len(prefix))
        args_ = dict(schema_tokens_list=[[r["schema_tokens"] for r in results] for results in transformed],
                     task_types=[[r["task_type"] for r in results] for results in transformed],
                     structure_labels=[[r["output"] for r in results] for results in transformed],
                     text_lengths=lengths, is_training=False, build_targets=True,
                     max_gold_per_query=case.get("capacity"), on_capacity_exceeded="raise",
                     record_metadata_list=[item["upstream"].get("record_metadata") for item in case["samples"]])
        entry = dict(case, upstream_word_tokens=words_out, upstream_prefix_counts=prefixes)
        try:
            layouts, targets, _ = build_boundary_batch_metadata(**args_)
        except TargetCapacityError:
            entry["capacity_error"] = True
            rows.append(entry)
            continue
        batch, query_width, capacity = targets.mention_mask.shape
        records = []
        for sample_records in targets.records or [[] for _ in range(batch)]:
            records.append([dict(id=record.instance_id, group=record.task_index, anchor_query=record.anchor_query_id,
                                 fields=[dict(query=f.query_id, values=[[dict(start=s, end=e) for s, e in alternatives]
                                                                       for alternatives in f.values]) for f in record.fields])
                            for record in sample_records])
        class_labels = [[label for r in results if r["task_type"] == "classifications" for label in r["output"]]
                        for results in transformed]
        class_width = max(map(len, class_labels), default=0)
        entry["expected"] = dict(
            word_width=max(lengths), query_width=query_width, classification_width=class_width, gold_capacity=capacity,
            mention_pairs=[dict(start=s, end=e) for s, e in targets.mention_pairs.reshape(-1, 2).tolist()],
            mention_mask=targets.mention_mask.reshape(-1).tolist(),
            query_mask=[q < layout.extractive_count() for layout in layouts for q in range(query_width)],
            classification_targets=[value for labels in class_labels for value in labels + [0] * (class_width - len(labels))],
            classification_mask=[q < len(labels) for labels in class_labels for q in range(class_width)], records=records,
        )
        rows.append(entry)
    report = dict(format_version=1, scope="word_level_training_targets_without_model_or_subword_tokenizer",
                  provenance=dict(upstream_commit=PIN, python=platform.python_version(), torch=importlib.metadata.version("torch"),
                                  source_sha256={name: hashlib.sha256((args.upstream / name).read_bytes()).hexdigest() for name in source_files}),
                  training_qualified=False, cases=rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n", encoding="utf-8")
    print(f"wrote {len(rows)} target cases to {args.output}")


if __name__ == "__main__":
    main()
