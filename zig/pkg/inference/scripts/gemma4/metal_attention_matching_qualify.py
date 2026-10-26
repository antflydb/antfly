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

"""Qualify the real Metal fitter against sealed, selected FP64 candidates.

Candidate readback is explicit test-only evidence, never a serving path. Passing
this driver cannot qualify the model lifecycle or enable runtime capabilities.
"""
from __future__ import annotations
import os
for _name in ('OPENBLAS_NUM_THREADS', 'OMP_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS'):
    os.environ[_name] = '1'
import argparse
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys

import numpy as np
import attention_matching_calibrate as am
import context_release_build_adapter as release_build
from release_attention_matching import read_json, require, sha, write_json, number
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from benchmark_resources import run_guarded

HEADER = struct.Struct('<8s6If')
REPORT = struct.Struct('<IIfffIfI')
METRIC_PARITY_TOLERANCE = .002
REPORT_PARITY_TOLERANCE = .0002
# Mirrors runtime/kv/attention_matching.zig and context_compaction.Request.
# The larger offline schema-2 envelope is not a serving fitter capability.
MAX_FITTER_QUERY_ROWS = 4096


def input_parts(meta, arrays, result):
    """Canonical fitter input; independent teacher queries never enter it."""
    c, head = meta['capture'], result['head']
    require(result['accepted'] is True and not result['exact'] and result['recipe'] == 'relative_cd_ridge_rms', 'unsupported or rejected candidate')
    require(0 <= head < c['heads'] and 0 < result['retained_rows'] < c['rows'], 'invalid candidate shape')
    require(0 < c['training_rows'] < MAX_FITTER_QUERY_ROWS and
            0 < c['validation_rows'] <= MAX_FITTER_QUERY_ROWS - c['training_rows'],
            'query campaign exceeds the Metal fitter row limit')
    strata = [s for s in c['strata'] if s['kv_head'] == head]
    sizes = {s['validation_count'] for s in strata}
    require(len(sizes) == 1 and sum(s['validation_count'] for s in strata) == c['validation_rows'], 'Metal fitter requires equal validation strata')
    expected_start = 0
    for s in strata:
        require(s['validation_start'] == expected_start, 'noncontiguous consumer strata')
        expected_start += s['validation_count']
    header = HEADER.pack(b'AMFIT001', c['rows'], c['dim'], c['training_rows'], c['validation_rows'], sizes.pop(), result['retained_rows'], c['scale'])
    yield header
    for tensor in (arrays['keys'][:, head], arrays['values'][:, head],
                   arrays['queries'][head, :c['training_rows']], arrays['queries'][head, c['training_rows']:c['training_rows'] + c['validation_rows']]):
        yield np.ascontiguousarray(tensor).tobytes()


def input_digest(meta, arrays, result):
    digest = hashlib.sha256()
    for part in input_parts(meta, arrays, result):
        digest.update(part)
    return digest.hexdigest()


def export_input(path, meta, arrays, result):
    parts = iter(input_parts(meta, arrays, result))
    header = next(parts)  # Validate before creating a destination artifact.
    with Path(path).open('xb') as stream:
        stream.write(header)
        for part in parts:
            stream.write(part)


def evaluate_candidate(meta, arrays, result, keys, values, bias, report):
    c, head, retained = meta['capture'], result['head'], result['retained_rows']
    require(keys.shape == values.shape == (retained, c['dim']) and bias.shape == (retained,), 'invalid stored candidate shape')
    require(all(np.isfinite(x).all() for x in (keys, values, bias)), 'nonfinite stored candidate')
    require(np.array_equal(keys, arrays['keys'][result['selected_source_rows'], head]), 'GPU/FP64 selected keys differ')
    require(report['failure_flags'] == 0 and report['evaluated'] == 7 and report['iterations'] == 128, 'incomplete or rejected GPU report')
    queries = arrays['queries'][head].astype(float)
    scores = queries @ keys.astype(float).T * c['scale'] + bias.astype(float)
    maximum = scores.max(axis=1)
    weights = np.exp(scores - maximum[:, None])
    partition = weights.sum(axis=1)
    mass = np.exp(maximum + np.log(partition) - arrays['exact_log_partition'][head])
    output = (weights / partition[:, None]) @ values.astype(float)
    require(np.isfinite(mass).all() and np.isfinite(output).all(), 'nonfinite oracle result')
    target = arrays['exact_outputs'][head]
    train = c['training_rows']
    gate_end = train + c['validation_rows']
    measured = {'training': am.errors(mass[:train], output[:train], target[:train]),
                'validation': am.errors(mass[train:gate_end], output[train:gate_end], target[train:gate_end]), 'consumers': []}
    if c.get('evaluation_rows', 0):
        measured['evaluation'] = am.errors(mass[gate_end:], output[gate_end:], target[gate_end:])
        require(am.passes(measured['evaluation']), 'GPU candidate fails independent teacher evaluation')
        measured['evaluation_consumers'] = []
    for s in c['strata']:
        if s['kv_head'] != head:
            continue
        sl = slice(train + s['validation_start'], train + s['validation_start'] + s['validation_count'])
        consumer = {'consumer_layer': s['consumer_layer'], 'query_head': s['query_head'], **am.errors(mass[sl], output[sl], target[sl])}
        require(am.passes(consumer), 'GPU candidate fails a consumer gate')
        measured['consumers'].append(consumer)
        if c.get('evaluation_rows', 0):
            evaluation_slice = slice(gate_end + s['evaluation_start'],
                                     gate_end + s['evaluation_start'] + s['evaluation_count'])
            evaluation_consumer = {'consumer_layer': s['consumer_layer'], 'query_head': s['query_head'],
                                   **am.errors(mass[evaluation_slice], output[evaluation_slice], target[evaluation_slice])}
            require(am.passes(evaluation_consumer), 'GPU candidate fails independent teacher consumer gate')
            measured['evaluation_consumers'].append(evaluation_consumer)
    require(measured['training']['mass_max'] <= am.MASS_LIMIT and am.passes(measured['validation']), 'GPU candidate fails independent numerical gates')
    expected_report = {
        'training_mass_relative_max_error': measured['training']['mass_max'],
        'mass_relative_max_error': measured['validation']['mass_max'],
        'output_relative_rmse': measured['validation']['output_relative_rmse'],
        'max_consumer_output_relative_rmse': max(x['output_relative_rmse'] for x in measured['consumers']),
    }
    require(report['validation_groups'] == len(measured['consumers']), 'GPU report consumer count differs')
    for name, expected in expected_report.items():
        require(np.isfinite(report[name]) and abs(report[name] - expected) <= REPORT_PARITY_TOLERANCE, 'GPU report/oracle mismatch: ' + name)
    for split, metrics in (('training', ('mass_max',)), ('validation', ('mass_max', 'output_relative_rmse'))):
        for metric in metrics:
            require(abs(measured[split][metric] - result[split][metric]) <= METRIC_PARITY_TOLERANCE, 'Metal/FP64 fit metric mismatch')
    for actual, expected in zip(measured['consumers'], result['consumers'], strict=True):
        require((actual['consumer_layer'], actual['query_head']) == (expected['consumer_layer'], expected['query_head']), 'consumer ordering differs')
        require(abs(actual['output_relative_rmse'] - expected['validation']['output_relative_rmse']) <= METRIC_PARITY_TOLERANCE, 'Metal/FP64 consumer metric mismatch')
    if c.get('evaluation_rows', 0):
        for metric in ('mass_max', 'output_relative_rmse'):
            require(abs(measured['evaluation'][metric] - result['evaluation'][metric]) <= METRIC_PARITY_TOLERANCE,
                    'Metal/FP64 independent evaluation metric mismatch')
        for actual, expected in zip(measured['evaluation_consumers'], result['consumers'], strict=True):
            require((actual['consumer_layer'], actual['query_head']) == (expected['consumer_layer'], expected['query_head']),
                    'independent evaluation consumer ordering differs')
            for metric in ('mass_max', 'output_relative_rmse'):
                require(abs(actual[metric] - expected['evaluation'][metric]) <= METRIC_PARITY_TOLERANCE,
                        'Metal/FP64 independent consumer evaluation metric mismatch')
    return measured


def qualify(args):
    number(args.timeout, 'timeout', .001)
    number(args.max_rss_mib, 'RSS limit', 1)
    build = release_build.validate(args.build_receipt, args.binary)
    require(sha(args.binary) == build['binary_sha256'], 'binary/build mismatch')
    numerical = read_json(args.numerical)
    require(numerical.get('stage') == 'offline_numerical' and numerical.get('script_sha256') == am.script_identity(), 'unsupported or stale numerical receipt')
    captures = {x['sha256']: x for x in numerical['captures']}
    selected = [x for x in numerical['results'] if x['selected_attempt'] and x['accepted'] and not x['exact']]
    require(selected, 'no selected lossy candidates to qualify')
    args.output_dir.mkdir(parents=True, exist_ok=False)
    terminal = {'schema_version': 1, 'stage': 'metal_fitter_parity', 'serving_qualified': False,
                'binary_sha256': build['binary_sha256'], 'build_receipt_sha256': build['receipt_sha256'],
                'numerical_receipt_sha256': sha(args.numerical), 'driver_sha256': sha(__file__),
                'metric_parity_tolerance': METRIC_PARITY_TOLERANCE, 'report_parity_tolerance': REPORT_PARITY_TOLERANCE,
                'passed': False, 'candidates': []}
    try:
        for index, result in enumerate(selected):
            case = {'capture_sha256': result['capture_sha256'], 'owner_layer': result['owner_layer'], 'head': result['head'], 'retained_rows': result['retained_rows'], 'passed': False}
            terminal['candidates'].append(case)
            capture = captures[result['capture_sha256']]
            require(sha(capture['path']) == result['capture_sha256'], 'capture identity changed')
            meta, arrays = am.load_capture(capture['path'])
            require(result['owner_layer'] == meta['capture']['owner_layer'], 'candidate owner mismatch')
            recomputed = am.fit_head(meta, arrays, result['head'], result['retained_rows'], result['recipe'])
            require(recomputed['accepted'] and all(recomputed[key] == result[key] for key in ('selected_source_rows', 'training', 'validation', 'consumers')), 'CPU candidate receipt changed')
            directory = args.output_dir / f'case-{index:03d}'; directory.mkdir()
            path = directory / 'input.bin'; prefix = directory / 'candidate'
            export_input(path, meta, arrays, result)
            command = ['/usr/bin/env']
            for name in os.environ:
                if name.startswith(('ANTFLY_CONTEXT_', 'TERMITE_METAL_')):
                    command.extend(['-u', name])
            command.extend(['MTL_DEBUG_LAYER=1', 'ANTFLY_CONTEXT_FIT_INPUT=' + str(path), 'ANTFLY_CONTEXT_FIT_OUTPUT=' + str(prefix), str(args.binary), '--test-filter', 'context Metal sealed capture fitter qualification'])
            resources_path = directory / 'resources.json'
            resources = run_guarded(command, resources_path, timeout=args.timeout, max_rss_mib=args.max_rss_mib, min_free_percent=15, max_swap_growth_mib=0, min_disk_free_mib=4096)
            case.update(input_sha256=sha(path), resource_receipt_sha256=sha(resources_path))
            require(resources['pass'], 'Metal fitter test or resource guard failed; see ' + str(directory))
            require('1 selected; 1 passed; 0 skipped.' in (directory / 'resources.stderr').read_text(), 'Metal qualification test did not run')
            retained, dim = result['retained_rows'], meta['capture']['dim']
            keys = am.raw_array(str(prefix) + '-keys.f16', '<f2', (retained, dim))
            values = am.raw_array(str(prefix) + '-values.f16', '<f2', (retained, dim))
            bias = am.raw_array(str(prefix) + '-bias.f32', '<f4', (retained,))
            raw = Path(str(prefix) + '-report.bin').read_bytes()
            require(len(raw) == REPORT.size, 'invalid GPU status size')
            fields = ('failure_flags', 'iterations', 'mass_relative_max_error', 'output_relative_rmse', 'training_mass_relative_max_error', 'evaluated', 'max_consumer_output_relative_rmse', 'validation_groups')
            report = dict(zip(fields, REPORT.unpack(raw)))
            case['gpu_report'] = report
            case['oracle'] = evaluate_candidate(meta, arrays, result, keys, values, bias, report)
            case['outputs'] = {suffix: sha(str(prefix) + '-' + suffix) for suffix in ('keys.f16', 'values.f16', 'bias.f32', 'report.bin')}
            case['passed'] = True
            print(json.dumps({key: case[key] for key in ('owner_layer', 'head', 'retained_rows', 'passed')}), flush=True)
        terminal['passed'] = True
    except Exception as error:
        terminal['error'] = str(error)
        raise
    finally:
        write_json(args.output_dir / 'receipt.json', terminal)
    return terminal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'build-receipt', 'numerical', 'output-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--timeout', type=float, default=300)
    parser.add_argument('--max-rss-mib', type=float, default=2048)
    args = parser.parse_args()
    for name in ('binary', 'build_receipt', 'numerical', 'output_dir'):
        setattr(args, name, getattr(args, name).resolve())
    qualify(args)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as error:
        print('Metal attention qualification: ' + str(error), file=sys.stderr)
        sys.exit(2)
