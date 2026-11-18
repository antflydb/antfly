#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Upstream eager Transformers CUDA reference for prepared GLiNER2.5 inputs.

Run this in a separate sampled process. Timing includes input upload and
synchronized FP32 output readback; excludes tokenization, loading and warmup.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.metadata
import inspect
import json
from pathlib import Path
import time


def digest(path: Path) -> str:
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('model', 'manifest', 'out'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--precision', choices=('fp32', 'bf16'), default='bf16')
    parser.add_argument('--warmups', type=int, default=3)
    parser.add_argument('--iterations', type=int, default=8)
    parser.add_argument('--threads', type=int, default=4)
    args = parser.parse_args()
    if args.out.exists() or args.warmups < 0 or not 1 <= args.iterations <= 100:
        parser.error('Use a fresh output and 1..100 measured iterations')
    import torch
    from gliner2 import AutoExtractor
    from prepare_gliner25 import Core, EntityCore
    from transformers.models.deberta_v2 import modeling_deberta_v2

    torch.set_num_threads(args.threads)
    torch.set_float32_matmul_precision('highest')
    torch.cuda.init()
    manifest = json.loads(args.manifest.read_text())
    checkpoint_hash = digest(args.model / 'model.safetensors')
    if checkpoint_hash != manifest['model_sha256']:
        raise ValueError('Checkpoint differs from prepared reference')
    started = time.perf_counter()
    model = AutoExtractor.from_pretrained(str(args.model), local_files_only=True).eval()
    model.requires_grad_(False)
    # Give BF16 a minimal GPU load footprint: lower on CPU before upload.
    model.to(dtype=torch.bfloat16 if args.precision == 'bf16' else torch.float32)
    model.to('cuda')
    torch.cuda.synchronize()
    load_s = time.perf_counter() - started
    core, entity_core = Core(model).eval(), EntityCore(model).eval()
    rows = []
    with torch.inference_mode():
        for fixture in manifest['cases']:
            tensors = [torch.tensor(x['data'], dtype=torch.int64).reshape(x['shape']) for x in fixture['inputs']]
            fn = entity_core if fixture.get('kind') == 'entities' else core
            samples, gpu_samples, logits = [], [], []
            for repetition in range(args.warmups + args.iterations):
                torch.cuda.synchronize()
                start_event, end_event = torch.cuda.Event(enable_timing=True), torch.cuda.Event(enable_timing=True)
                started = time.perf_counter()
                inputs = [x.to('cuda') for x in tensors]
                start_event.record()
                result = fn(*inputs)
                end_event.record()
                output = result.detach().float().cpu().reshape(-1)
                torch.cuda.synchronize()
                elapsed = time.perf_counter() - started
                if not torch.isfinite(output).all():
                    raise ValueError('Nonfinite reference output')
                if repetition >= args.warmups:
                    samples.append(elapsed)
                    gpu_samples.append(start_event.elapsed_time(end_event))
                    logits.append(output.tolist())
            rows.append({'id': fixture['id'], 'samples_seconds': samples, 'cuda_event_ms': gpu_samples, 'logits': logits})
    report = {
        'scope': 'upstream eager Transformers; prepared encoder/task head; includes input upload and synchronized output readback; excludes tokenization/loading',
        'precision': args.precision,
        'model_sha256': checkpoint_hash,
        'manifest_sha256': digest(args.manifest),
        'producer_sha256': digest(Path(__file__)),
        'core_source_sha256': digest(Path(__file__).with_name('prepare_gliner25.py')),
        'deberta_source_sha256': digest(Path(inspect.getfile(modeling_deberta_v2))),
        'versions': {name: importlib.metadata.version(name) for name in ('torch', 'transformers', 'gliner2', 'numpy')},
        'device': torch.cuda.get_device_name(),
        'dtype': str(next(model.parameters()).dtype),
        'attention_implementation': str(getattr(model.encoder.config, '_attn_implementation', None)),
        'matmul_settings': {
            'float32_precision': torch.get_float32_matmul_precision(),
            'allow_bf16_reduced_precision_reduction': torch.backends.cuda.matmul.allow_bf16_reduced_precision_reduction,
        },
        'inference_mode': True, 'autocast': False,
        'load_seconds': load_s,
        'cuda_max_memory_allocated_bytes': torch.cuda.max_memory_allocated(),
        'cuda_max_memory_reserved_bytes': torch.cuda.max_memory_reserved(),
        'cases': rows,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, allow_nan=False) + '\n')


if __name__ == '__main__':
    main()
