#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Audit untouched pinned 1B extraction with upstream APIs and schema variants.

Count and span logits are observed without modifying weights or outputs.
Slot-zero probabilities are diagnostic even when the upstream count gate is zero.
"""
import argparse
import inspect
import importlib.metadata
import json
import sys
from pathlib import Path
from family_oracle import verify_model, UPSTREAM_COMMIT
from source_pin import verify_source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--upstream', type=Path, required=True)
    parser.add_argument('--model-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    verify_source(args.upstream)
    pin = verify_model(args.model_dir, 'decide_1b')
    sys.path.insert(0, str(args.upstream.resolve()))
    import torch
    from gliner2 import AutoExtractor, Schema
    torch.set_num_threads(4)
    model = AutoExtractor.from_pretrained(str(args.model_dir), map_location='cpu').eval()
    probes = [
        ('readme', 'Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.', ['company', 'person', 'product', 'location'], 0.5),
        ('readme_low_threshold', 'Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.', ['company', 'person', 'product', 'location'], 0.01),
        ('descriptions', 'Apple CEO Tim Cook announced iPhone 15 in Cupertino yesterday.', {'company': 'A named company or organization', 'person': 'A named person', 'product': 'A named product', 'location': 'A named place'}, 0.5),
        ('conll_labels', 'John Smith works at Microsoft in London.', ['PER', 'ORG', 'LOC'], 0.5),
        ('single_label', 'Barack Obama was born in Hawaii.', ['person'], 0.5),
        ('combined_relation', 'Ada Lovelace works at Antfly in Paris.', ['person', 'organization', 'location'], 0.5),
    ]
    results = []
    original = model._extract_span_result
    observed = []
    def observe(*positional, **keyword):
        bound = inspect.signature(original).bind(*positional, **keyword).arguments
        embs = bound['embs']
        logits = model.count_pred(embs[:1]).flatten()
        row = dict(task_type=bound['task_type'], schema_tokens=bound['schema_tokens'],
                   count_logits=logits.tolist(), predicted_count=int(logits.argmax().item()))
        if bound['span_info'] is not None:
            projected = model.count_embed(embs[1:], 1)
            scores = torch.einsum('lkd,bpd->bplk', bound['span_info']['span_rep'], projected)
            row['slot_zero_max_probability'] = float(scores.sigmoid().max().item())
            row['span_logits_range'] = [float(scores.min().item()), float(scores.max().item())]
            row['span_rep_range'] = [float(bound['span_info']['span_rep'].min().item()), float(bound['span_info']['span_rep'].max().item())]
            row['projection_range'] = [float(projected.min().item()), float(projected.max().item())]
        observed.append(row)
        return original(*positional, **keyword)
    model._extract_span_result = observe
    with torch.inference_mode():
        for name, text, labels, threshold in probes:
            observed.clear()
            if name == 'combined_relation':
                schema = Schema().entities(labels).relations(['works_at'])
                output = model.extract(text, schema, threshold=threshold, include_confidence=True, include_spans=True)
            else:
                output = model.extract_entities(text, labels, threshold=threshold, include_confidence=True, include_spans=True)
            result = dict(id=name, text=text, labels=labels, threshold=threshold, output=output, heads=list(observed))
            results.append(result)
            print(json.dumps(result), flush=True)
        control = model.classify_text('Please refund this duplicate charge.', {'intent': ['refund', 'support', 'cancel']})
    weights = {name: dict(shape=list(value.shape), minimum=float(value.min().item()), maximum=float(value.max().item()), mean=float(value.float().mean().item())) for name, value in model.state_dict().items() if name.startswith(('span_rep.', 'count_embed.', 'count_pred.'))}
    args.output.write_text(json.dumps(dict(model=pin, upstream_commit=UPSTREAM_COMMIT, probes=results, classification_control=control, head_weights=weights, runtime={name: importlib.metadata.version(name) for name in ('torch', 'transformers', 'tokenizers')}), indent=2) + '\n')
    print('classification_control', control, flush=True)

if __name__ == '__main__':
    main()
