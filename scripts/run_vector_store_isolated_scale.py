#!/usr/bin/env python3
"""Run isolated 50K comparisons sequentially, gating on each lifecycle result.

Use a previously validated, pinned executable. This driver freezes measurement
scripts in the worktree and does not select a winner or start 1M automatically.
"""

import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--vdbbench-root', type=Path, required=True)
    parser.add_argument('--vdbbench-python', type=Path, required=True)
    parser.add_argument('--measurement-scripts', type=Path, help='Freeze an already pinned harness instead of current scripts.')
    parser.add_argument('--port', type=int, default=18138)
    parser.add_argument('--experiments', nargs='+', default=['adaptive_cache', 'payload_segments', 'selective_gc'],
                        choices=['adaptive_cache', 'payload_segments', 'selective_gc', 'ownership', 'snapshot_reads', 'segment_gc'])
    args = parser.parse_args()
    worktree = Path(__file__).resolve().parents[1]
    root = args.root.absolute()
    if not root.is_relative_to(worktree):
        parser.error('root must be inside the worktree')
    root.mkdir(parents=True, exist_ok=False)
    snapshot = root / 'harness/scripts'
    snapshot.mkdir(parents=True)
    for source in (args.measurement_scripts or worktree / 'scripts').iterdir():
        if source.is_file() and source.suffix in {'.py', '.sh'}:
            shutil.copy2(source, snapshot / source.name)
    phases = []
    for experiment in args.experiments:
        output = root / experiment
        command = [str(args.vdbbench_python.absolute()), '-B', str(snapshot / 'run_vector_store_ab.py'),
                   str(output), '--binary', str(args.binary.resolve(strict=True)),
                   '--refinement', experiment, '--pairs', '2', '--port', str(args.port),
                   '--health-port', str(args.port + 1), '--vdbbench-root', str(args.vdbbench_root.absolute()),
                   '--vdbbench-python', str(args.vdbbench_python.absolute())]
        phase = {'experiment': experiment, 'command': command, 'started_at': time.time()}
        phases.append(phase)
        receipt = root / 'phases.json'
        receipt.write_text(json.dumps(phases, indent=2) + '\n')
        print('started', experiment, flush=True)
        with (root / (experiment + '.log')).open('w') as log:
            completed = subprocess.run(command, cwd=worktree, stdout=log, stderr=subprocess.STDOUT)
            phase.update(exit_code=completed.returncode, finished_at=time.time())
            receipt.write_text(json.dumps(phases, indent=2) + '\n')
            if (output / 'ab-runs.json').exists():
                summary = subprocess.run([sys.executable, '-B', str(snapshot / 'summarize_vector_store_refinement_scale.py'),
                                          str(output)], stdout=log, stderr=subprocess.STDOUT)
                phase['summary_exit_code'] = summary.returncode
                receipt.write_text(json.dumps(phases, indent=2) + '\n')
        print('finished', experiment, completed.returncode, flush=True)
        if completed.returncode or phase.get('summary_exit_code') != 0:
            raise SystemExit('qualification failed; later comparisons are gated; inspect ' + str(receipt))


if __name__ == '__main__':
    main()
