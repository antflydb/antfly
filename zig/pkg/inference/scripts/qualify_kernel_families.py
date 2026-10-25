#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Retained baseline/candidate model and CUDA-code qualification receipts.

Model configs contain cases [{name, args, timing_prefix?}], arm_order and
optional environment/cpu_affinity. Args are literal argv (no shell); --output
is supplied by this runner. All arms must produce finite, byte-identical F32
outputs. Independent operator references remain separate required gates.
"""
import argparse
import array
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import struct
import subprocess


def digest(data):
    return hashlib.sha256(data).hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def models(args):
    config = json.loads(args.config.read_text())
    order = config.get('arm_order', 'ABBABAABABBAABBA')
    if set(order) != {'A', 'B'} or order.count('A') != order.count('B'):
        raise ValueError('arm_order must have balanced A/B arms')
    binaries = {'A': args.baseline.resolve(), 'B': args.candidate.resolve()}
    env = dict(os.environ, **config.get('environment', {}))
    affinity = config.get('cpu_affinity')
    if affinity and not hasattr(os, 'sched_setaffinity'):
        raise ValueError('CPU affinity requires a supported host')
    receipt = {'config': config, 'binaries': {k: {'path': str(v), 'sha256': digest(v.read_bytes())}
               for k, v in binaries.items()}, 'cases': []}
    passed = True
    for case in config['cases']:
        if not re.fullmatch(r'[\w.-]+', case['name']):
            raise ValueError('case name must be a safe filename')
        current = {'name': case['name'], 'arms': []}
        receipt['cases'].append(current)
        reference = None
        for index, arm in enumerate(order):
            stem = args.results / (case['name'] + '-%02d-' % index + arm)
            output = Path(str(stem) + '.f32')
            log = Path(str(stem) + '.log')
            if output.exists():
                output.unlink()
            command = [str(binaries[arm])] + case['args'] + ['--output', str(output)]
            prepare = (lambda: os.sched_setaffinity(0, affinity)) if affinity else None
            run = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                 env=env, preexec_fn=prepare)
            log.write_bytes(run.stdout)
            row = {'arm': arm, 'returncode': run.returncode, 'command': command, 'log': str(log)}
            prefix = case.get('timing_prefix', 'unified_execution') + ' '
            line = next((x for x in run.stdout.decode(errors='replace').splitlines()
                         if x.startswith(prefix)), '')
            row['timing'] = dict(re.findall(r'(\w+)=([^ ]+)', line))
            setup = next((x for x in run.stdout.decode(errors='replace').splitlines()
                          if x.startswith('unified_setup ')), '')
            if setup:
                row['setup'] = {key: int(value) for key, value in
                                re.findall(r'(\w+)=(\d+)', setup)}
            if run.returncode == 0 and output.exists():
                raw = output.read_bytes()
                values = array.array('f')
                if len(raw) % 4:
                    row['invalid_output_bytes'] = len(raw)
                else:
                    values.frombytes(raw)
                    row['finite'] = bool(values) and all(math.isfinite(x) for x in values)
                    reference = raw if reference is None else reference
                    row.update(sha256=digest(raw), byte_identical=raw == reference)
            current['arms'].append(row)
            save(args.results / 'receipt.json', receipt)
            print(case['name'], index, arm, row['returncode'], row['timing'], flush=True)
        summary = {'numerical_pass': all(x.get('finite') and x.get('byte_identical')
                                         and x['returncode'] == 0 for x in current['arms'])}
        for key, limit in [('p50_ns', 1.05), ('p95_ns', 1.10)]:
            values = {arm: [int(x['timing'][key]) for x in current['arms']
                           if x['arm'] == arm and key in x['timing']] for arm in 'AB'}
            if any(len(v) != len(order) // 2 or any(x <= 0 for x in v) for v in values.values()):
                summary[key] = {'pass': False}
            else:
                old, new = (statistics.median(values[arm]) for arm in 'AB')
                summary[key] = {'baseline': old, 'candidate': new, 'ratio': new / old,
                                'pass': new / old <= limit}
        # First warmup includes the cold execution, rather than isolating host
        # preparation. Keep it separate from steady-state acceptance timings.
        for key in ('load_ns', 'inputs_ns', 'first_warmup_ns'):
            values = {arm: [x.get('setup', {}).get(key) for x in current['arms']
                            if x['arm'] == arm] for arm in 'AB'}
            if all(v and all(x is not None and x > 0 for x in v) for v in values.values()):
                old, new = (statistics.median(values[arm]) for arm in 'AB')
                summary[key] = {'baseline': old, 'candidate': new, 'ratio': new / old}
        summary['pass'] = bool(summary['numerical_pass'] and summary['p50_ns']['pass']
                               and summary['p95_ns']['pass'])
        current['summary'] = summary
        passed &= summary['pass']
        save(args.results / 'receipt.json', receipt)
        print('case_summary', case['name'], summary, flush=True)
    return passed


def cuda_static(args):
    receipt = []
    for arch in ('sm_75', 'sm_80', 'sm_89', 'sm_90', 'sm_100', 'sm_110', 'sm_120'):
        versions = {}
        for label, path in [('baseline', args.baseline), ('candidate', args.candidate)]:
            text = subprocess.check_output([str(args.cuobjdump), '--dump-sass',
                                            '--gpu-architecture', arch, str(path)], text=True)
            usage = subprocess.check_output([str(args.cuobjdump), '--dump-resource-usage',
                                             '--gpu-architecture', arch, str(path)], text=True)
            resources = dict(re.findall(r' Function (\S+):\n([^\n]+)', usage))
            versions[label] = {m.group(1): {'sha256': digest(m.group(2).strip().encode()),
                                'resources': resources.get(m.group(1), '').strip(),
                                'instructions': len(re.findall(r'/\*[0-9a-f]+\*/', m.group(2)))}
                              for m in re.finditer(r'Function\s*:\s*(\S+)\n(.*?)(?=\n\s*Function\s*:|\Z)', text, re.S)}
            if not versions[label] or any(not r['resources'] for r in versions[label].values()):
                raise ValueError('Missing functions/resources for ' + arch)
        old, new = versions['baseline'], versions['candidate']
        changed = [{'name': name, 'baseline': old.get(name), 'candidate': new.get(name)}
                   for name in sorted(set(old) | set(new)) if old.get(name) != new.get(name)]
        receipt.append({'arch': arch, 'baseline_count': len(old), 'candidate_count': len(new),
                        'changed': changed})
        save(args.results / 'cuda-static.json', receipt)
        print(arch, 'changed', len(changed), flush=True)
    # Differences are review evidence, not automatically a failed qualification.
    return True


def source_ledger(args):
    root = args.root.resolve()
    subprocess.run(['git', 'rev-parse', '--verify', args.revision + '^{commit}'], cwd=root,
                   stdout=subprocess.DEVNULL, check=True)
    tracked = subprocess.check_output(['git', 'ls-files', 'zig/pkg/inference/src'], cwd=root,
                                      text=True).splitlines()
    added = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard',
                                     'zig/pkg/inference/src'], cwd=root, text=True).splitlines()
    receipt = {'baseline_revision': args.revision, 'files': [], 'authored': {}, 'generated': {}}
    totals = {kind: [0, 0, 0, 0] for kind in ('authored', 'generated')}
    def regions(data):
        authored, generated = [], []
        inside = False
        for line in data.splitlines(keepends=True):
            if re.match(rb'\s*// quant-kernel-codegen:begin', line):
                inside = True
            (generated if inside else authored).append(line)
            if re.match(rb'\s*// quant-kernel-codegen:end', line):
                inside = False
        return {'authored': b''.join(authored), 'generated': b''.join(generated)}
    for name in sorted(set(tracked + added)):
        if Path(name).suffix not in {'.zig', '.m', '.cu', '.cuh', '.h', '.metal'}:
            continue
        old = subprocess.run(['git', 'show', args.revision + ':' + name], cwd=root,
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout
        current = root / name
        new = current.read_bytes() if current.is_file() else b''
        if old == new:
            continue
        if '/generated/' in name:
            parts = ({'authored': b'', 'generated': old}, {'authored': b'', 'generated': new})
        else:
            parts = (regions(old), regions(new))
        entry = {'path': name, 'baseline_sha256': digest(old), 'candidate_sha256': digest(new)}
        for kind in totals:
            a, b = parts[0][kind], parts[1][kind]
            count = [len(a.splitlines()), len(b.splitlines()), len(a), len(b)]
            totals[kind] = [x + y for x, y in zip(totals[kind], count)]
            entry[kind] = {'lines_removed': count[0] - count[1], 'bytes_removed': count[2] - count[3]}
        receipt['files'].append(entry)
    for kind, (a, b, x, y) in totals.items():
        receipt[kind] = {'lines_removed': a - b, 'bytes_removed': x - y}
    save(args.results / 'source-ledger.json', receipt)
    print(json.dumps({k: receipt[k] for k in totals}, indent=2))
    return True


def pack_cuda(args):
    config = json.loads(args.config.read_text())
    tolerant = any(arg['kind'] == 'reference_output_f32' for case in config['cases'] for arg in case['arguments'])
    reference = any(arg['kind'] == 'reference_output' for case in config['cases'] for arg in case['arguments'])
    data = bytearray(struct.pack('<II', 0x4b464333 if tolerant else 0x4b464332 if reference else 0x4b464331, len(config['cases'])))
    def string(value, pattern):
        if len(value) > 256 or not re.fullmatch(pattern, value):
            raise ValueError('Invalid CUDA case/kernel name')
        raw = value.encode()
        return struct.pack('<I', len(raw)) + raw
    for case in config['cases']:
        data += string(case['name'], r'[\w.-]+')
        data += string(case['kernel'], r'[A-Za-z_]\w*')
        data += struct.pack('<IIIIIIII', *(case['grid'] + case['threads'] +
                            [case.get('shared_bytes', 0), len(case['arguments'])]))
        for arg in case['arguments']:
            raw = (args.config.parent / arg['file']).read_bytes()
            role = {'value': 0, 'buffer': 1, 'output': 2, 'reference_output': 3, 'reference_output_f32': 4}[arg['kind']]
            written = arg.get('written_bytes', 0)
            if not raw or written < 0 or written > len(raw) or (role >= 2 and written == 0):
                raise ValueError('Invalid fixture extent')
            absolute = arg.get('absolute_tolerance', 0)
            relative = arg.get('relative_tolerance', 0)
            if role == 4 and (written % 4 or not all(math.isfinite(x) and x >= 0 for x in (absolute, relative))):
                raise ValueError('Invalid F32 reference precision contract')
            data += struct.pack('<IQQ', role, written, len(raw))
            if tolerant:
                data += struct.pack('<dd', absolute if role == 4 else 0, relative if role == 4 else 0)
            data += raw
    (args.results / 'cuda-cases.bin').write_bytes(data)
    save(args.results / 'cuda-manifest.json', config)
    return True



def matched_metal_arm_ratio(arms):
    """Compare adjacent opposite arms before reducing the clock-state mixture.

    An independent median for each version can land in different frequency
    modes even under A/A. Adjacent AB/BA pairs share the local clock state;
    their median preserves a multiplicative source effect without that jump.
    """
    if len(arms) != 32:
        raise ValueError('Metal pairing requires the declared 32-arm protocol')
    ratios = []
    for first, second in zip(arms[::2], arms[1::2]):
        if {first['arm'], second['arm']} != {0, 1}:
            raise ValueError('Metal timing pairs must contain opposite arms')
        values = {arm['arm']: arm['ns'] for arm in (first, second)}
        if not all(math.isfinite(value) and value > 0 for value in values.values()):
            raise ValueError('Metal timing arms must be finite and positive')
        ratios.append(values[1] / values[0])
    return statistics.median(ratios)


def metal_pair(args):
    versions = [json.loads(path.read_text()) for path in (args.ab, args.ba)]
    if not versions[0] or len(versions[0]) != len(versions[1]):
        raise ValueError('Metal creation-order receipt extents differ')
    rows = []
    seen = set()
    for ab, ba in zip(*versions):
        if (ab['name'], ab['kernel']) != (ba['name'], ba['kernel']) or ab['name'] in seen:
            raise ValueError('Metal creation-order case identities differ')
        if ab['pipeline_order'] != 'AB' or ba['pipeline_order'] != 'BA':
            raise ValueError('Both Metal pipeline creation orders are required')
        seen.add(ab['name'])
        ratios = [matched_metal_arm_ratio(row['arms']) for row in (ab, ba)]
        ratio = math.sqrt(ratios[0] * ratios[1])
        correct = all(row[key] is True for row in (ab, ba)
                      for key in ('exact', 'guards', 'resources_pass'))
        # Older unchanged profiles use exact baseline parity only. A null
        # reference flag is legal only when no independent output was supplied.
        correct &= all((row['reference_outputs'] > 0 and row['reference_pass'] is True)
                       or (row['reference_outputs'] == 0 and row['reference_pass'] is None)
                       for row in (ab, ba))
        rows.append(dict(name=ab['name'], kernel=ab['kernel'], ratio=ratio,
                         pass_=correct and ratio <= 1.05,
                         estimator='median-adjacent-AB-BA-ratios-v1',
                         creation_ratios=ratios, creation_AB=ab, creation_BA=ba))
    save(args.results / 'metal-paired.json', rows)
    print('Metal paired', len(rows), 'failed', sum(not row['pass_'] for row in rows),
          'maxratio', max(row['ratio'] for row in rows), flush=True)
    return all(row['pass_'] for row in rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    for command in ('models', 'cuda-static'):
        p = sub.add_parser(command)
        p.add_argument('--baseline', type=Path, required=True)
        p.add_argument('--candidate', type=Path, required=True)
        p.add_argument('--results', type=Path, required=True)
        if command == 'models':
            p.add_argument('--config', type=Path, required=True)
        else:
            p.add_argument('--cuobjdump', type=Path, required=True)
    p = sub.add_parser('pack-cuda')
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--results', type=Path, required=True)
    p = sub.add_parser('metal-pair')
    p.add_argument('--ab', type=Path, required=True)
    p.add_argument('--ba', type=Path, required=True)
    p.add_argument('--results', type=Path, required=True)
    p = sub.add_parser('source-ledger')
    p.add_argument('--root', type=Path, default=Path.cwd())
    p.add_argument('--revision', required=True)
    p.add_argument('--results', type=Path, required=True)
    args = parser.parse_args()
    args.results.mkdir(parents=True, exist_ok=True)
    return 0 if {'models': models, 'cuda-static': cuda_static,
                 'source-ledger': source_ledger, 'pack-cuda': pack_cuda, 'metal-pair': metal_pair}[args.action](args) else 1


if __name__ == '__main__':
    raise SystemExit(main())
