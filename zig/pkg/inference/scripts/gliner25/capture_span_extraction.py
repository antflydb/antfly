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

"""Capture pinned ModernBERT span outputs independently of Antfly inference."""
import argparse
import hashlib
import importlib.metadata
import inspect
import json
from pathlib import Path
import sys
from family_oracle import UPSTREAM_COMMIT, verify_model
from source_pin import verify_source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--upstream', type=Path, required=True)
    parser.add_argument('--model-dir', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    verify_source(args.upstream)
    sys.path.insert(0, str(args.upstream.resolve()))
    import torch
    from gliner2 import AutoExtractor
    from oracle import build_extract_schema
    from benchmark_family_python import canonical_expected

    pin = verify_model(args.model_dir, 'decide_1b')
    torch.set_num_threads(4)
    torch.use_deterministic_algorithms(True)
    model = AutoExtractor.from_pretrained(str(args.model_dir), map_location='cpu').eval()
    tasks = [{'task': 'intent', 'labels': ['refund', 'support', 'cancel']}]
    specs = [
        ('entities_relations', 'Ada Lovelace works at Antfly in Paris.',
         {'entities': ['person', 'organization', 'location'], 'relations': ['works_at']}),
        ('unicode', 'Café 東京: Ada works for Antfly.',
         {'entities': ['person', 'organization', 'location']}),
        ('mixed', 'Please refund the charge from Antfly to Ada.',
         {'entities': ['person', 'organization'], 'relations': ['works_at'], 'classifications': tasks}),
        ('decision_1024', 'Please refund this duplicate charge. ' + 'word ' * 1001 + '.',
         {'classifications': tasks}),
        ('extraction_1024', 'Ada works at Antfly. ' + 'word ' * 992 + '.',
         {'entities': ['person', 'organization'], 'relations': ['works_at']}),
    ]
    cases = []
    for name, text, spec in specs:
        schema = build_extract_schema(spec)
        batch = model.processor.collate_fn_inference(
            [(text, schema.build())], max_len=2048, architecture='span',
            error_policy='raise', build_targets=False, on_capacity_exceeded='raise')
        ids = batch.input_ids.tolist()[0]
        assert 0 < len(ids) <= 1024, (name, len(ids))
        heads = []
        original = model._extract_span_result
        def observe(*positional, **keyword):
            bound = inspect.signature(original).bind(*positional, **keyword).arguments
            if len(text) < 128 and bound['span_info'] is not None:
                embeddings = bound['embs']
                projected = model.count_embed(embeddings[1:], 3)
                scores = torch.einsum('lkd,bpd->bplk', bound['span_info']['span_rep'], projected)
                heads.append({'counts': model.count_pred(embeddings[:1]).flatten().tolist(),
                              'projected': projected.flatten().tolist(),
                              'scores': scores.flatten().tolist(), 'span_prefix': bound['span_info']['span_rep'].flatten().tolist()[:embeddings.shape[-1]]})
            return original(*positional, **keyword)
        model._extract_span_result = observe
        with torch.inference_mode():
            output = model.extract(text, schema, include_confidence=True, include_spans=True)
        model._extract_span_result = original
        native = {'entities': spec.get('entities', []),
                  'relations': [{'type': relation} for relation in spec.get('relations', [])],
                  'classifications': [{'name': task['task'], 'labels': task['labels']}
                                      for task in spec.get('classifications', [])]}
        cases.append({'id': name, 'text': text, 'native_schema_json': json.dumps(native),
                      'input_ids': ids, 'heads': heads, 'expected': canonical_expected({}, native, output)})
        print(name, len(ids), output, flush=True)
    source_hash = hashlib.sha256()
    for file in sorted((args.upstream / 'gliner2').rglob('*.py')):
        source_hash.update(str(file.relative_to(args.upstream)).encode())
        source_hash.update(b'\0' + file.read_bytes() + b'\0')
    args.output.write_text(json.dumps({'format_version': 1, 'model': pin,
        'upstream_commit': UPSTREAM_COMMIT, 'source_sha256': source_hash.hexdigest(),
        'runtime': {name: importlib.metadata.version(name) for name in
                    ('torch', 'transformers', 'tokenizers', 'pydantic')},
        'cases': cases}, indent=2) + '\n')


if __name__ == '__main__':
    main()
