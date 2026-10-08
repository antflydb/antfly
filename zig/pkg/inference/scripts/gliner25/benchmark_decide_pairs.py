#!/usr/bin/env python3
"""Serial AB/BA Decide-1B campaign, retaining every success and failure receipt."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import compare_decide_performance as compare
import run_family_performance as perf

HERE = Path(__file__).resolve().parent


def run_python(args, output):
    output.mkdir(parents=True, exist_ok=False)
    command = ['/usr/bin/time', '-l', '-o', str(output / 'time.txt'), str(args.python),
               str(HERE / 'benchmark_family_python.py'), '--profile', 'decide_1b', '--device',
               'mps' if args.backend == 'metal' else 'cpu', '--task', 'decide', '--short-holdout',
               '--model-dir', str(args.model_dir), '--upstream', str(args.upstream),
               '--runtime-dir', str(args.runtime_dir), '--output', str(output / 'report.json')]
    env = {**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'}
    source = perf.source_identity()
    receipt = dict(source=source, command=command, status='running', started_utc=perf.utc_now(), host_before=perf.host_context())
    path = output / 'receipt.json'
    path.write_text(json.dumps(receipt, indent=2) + '\n')
    started = time.monotonic()
    try:
        receipt['exit_code'] = perf.run_child(command, env, output / 'process.log', args.timeout)
        receipt['resources'] = perf.parse_time((output / 'time.txt').read_text())
        if receipt['exit_code'] != 0:
            raise RuntimeError('Python benchmark process failed')
        receipt['report'] = json.loads((output / 'report.json').read_text())
        receipt['passed'] = True
    except Exception as exc:
        receipt.update(passed=False, error=repr(exc))
    receipt.update(status='finished', finished_utc=perf.utc_now(), host_after=perf.host_context(),
                   wall_seconds=time.monotonic() - started, source_unchanged=source == perf.source_identity())
    receipt['passed'] &= receipt['source_unchanged']
    path.write_text(json.dumps(receipt, indent=2) + '\n')
    return receipt


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('binary', 'build-receipt', 'model-dir', 'upstream', 'runtime-dir', 'output'):
        p.add_argument('--' + name, required=True, type=Path)
    p.add_argument('--python', type=Path, default=Path(sys.executable))
    p.add_argument('--backend', required=True, choices=('metal', 'native'))
    p.add_argument('--blocks', type=int, choices=range(1, 7), default=6)
    p.add_argument('--timeout', type=int, default=300)
    a = p.parse_args()
    for name in ('binary', 'build_receipt', 'model_dir', 'upstream', 'runtime_dir', 'output', 'python'):
        setattr(a, name, getattr(a, name).resolve())
    a.output.mkdir(parents=True, exist_ok=False)
    pairs = []
    for block in range(a.blocks):
        results = {}
        for name in (('antfly', 'python') if block % 2 == 0 else ('python', 'antfly')):
            destination = a.output / f'block-{block:02d}-{name}'
            print(f'block={block} runtime={name}', flush=True)
            if name == 'python':
                results[name] = run_python(a, destination)
            else:
                command = [sys.executable, str(HERE / 'benchmark_decide_service.py'), '--binary', str(a.binary),
                           '--build-receipt', str(a.build_receipt), '--model-dir', str(a.model_dir), '--backend', a.backend, '--output', str(destination),
                           '--timeout', str(a.timeout)]
                subprocess.run(command, check=False)
                results[name] = json.loads((destination / 'receipt.json').read_text())
            if not results[name]['passed']:
                # Retain the failed block and stop; never quietly retry/select.
                print(f'Failed {name} process: {destination / "receipt.json"}', flush=True)
                return 1
        pairs.append((results['antfly'], results['python']))
        result = compare.evaluate(pairs)
        (a.output / 'comparison.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
    return 0 if result['passed'] or a.blocks < 6 else 1


if __name__ == '__main__':
    raise SystemExit(main())
