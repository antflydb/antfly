#!/usr/bin/env python3
"""Compare native allocation benchmarks; both trees must use the same harness.

Example:
  python3 tools/measure_allocation_reuse.py \
    --baseline-bin ../.worktrees/baseline/zig/zig-out/bin \
    --candidate-bin zig-out/bin --output /tmp/allocation-comparison

Replay timings include diagnostic counter overhead. Vector timing runs disable
counting and retain the benchmark's normal allocator. Heap counts are requested
bytes through the benchmark allocator, not RSS or complete process allocation.
"""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-bin', type=Path, required=True)
    parser.add_argument('--candidate-bin', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--documents', type=int, default=50000)
    parser.add_argument('--dimensions', type=int, default=1536)
    parser.add_argument('--samples', type=int, default=7)
    parser.add_argument('--vector-samples', type=int, default=3)
    parser.add_argument('--index-kinds', nargs='+', choices=['full_text', 'algebraic'], default=['full_text', 'algebraic'])
    args = parser.parse_args()
    if min(args.documents, args.dimensions, args.samples, args.vector_samples) <= 0:
        parser.error('counts and sample sizes must be positive')
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {'baseline': args.baseline_bin.resolve(), 'changed': args.candidate_bin.resolve()}
    results = []
    env = os.environ.copy()
    env.pop('ANTFLY_SOURCE_VECTOR_BACKGROUND_CHECKPOINT', None)

    def run(label, command, child_env):
        result = subprocess.run([str(x) for x in command], env=child_env, capture_output=True, text=True)
        (args.output / (label + '.log')).write_text(result.stdout + result.stderr)
        result.check_returncode()
        return result.stderr.splitlines()

    def save():
        (args.output / 'results.json').write_text(json.dumps(results, indent=2) + '\n')

    for kind in args.index_kinds:
        for records in [1, 128]:
            for budgeted in [False, True]:
                for batch in [256, 1024]:
                    for pair in range(args.samples):
                        for variant in (['baseline', 'changed'] if pair % 2 == 0 else ['changed', 'baseline']):
                            label = f'replay-{kind}-{records}-{budgeted}-{batch}-{pair}-{variant}'
                            lines = run(label, [binaries[variant] / 'replay-allocation-bench', args.documents,
                                               batch, 1, 'budgeted' if budgeted else 'unbudgeted', records, kind], env)
                            data = json.loads(next(x for x in lines if x.startswith('{')))
                            results.append(dict(workload='replay', variant=variant, pair=pair, **data))
                            print(label, data['elapsed_ns'], flush=True)
                            save()
    for mode in ['counted', 'timing']:
        child_env = env.copy()
        if mode == 'counted':
            child_env['ANTFLY_COUNT_BENCH_ALLOCATIONS'] = '1'
        else:
            child_env.pop('ANTFLY_COUNT_BENCH_ALLOCATIONS', None)
        for pair in range(args.vector_samples):
            for variant in (['baseline', 'changed'] if pair % 2 == 0 else ['changed', 'baseline']):
                label = f'vector-{mode}-{pair}-{variant}'
                with tempfile.TemporaryDirectory(prefix='antfly-allocation-') as directory:
                    root = Path(directory) / 'source'
                    lines = run(label, [binaries[variant] / 'vector-payload-bench', 'ingest', root,
                                        args.documents, args.dimensions], child_env)
                    data = json.loads(next(x[len('payload_bench '):] for x in lines if x.startswith('payload_bench ')))
                    counts = next((json.loads(x[len('allocation_bench '):]) for x in lines
                                   if x.startswith('allocation_bench ')), {})
                    run(label + '-verify', [binaries[variant] / 'vector-payload-bench', 'read', root,
                                           args.documents, args.dimensions], child_env)
                    results.append(dict(workload='vector', variant=variant, pair=pair,
                                        measurement=mode, **data, **counts))
                    print(label, data['run_ns'], flush=True)
                    save()
    summary = {}
    for workload in ['replay', 'vector']:
        groups = sorted({(x.get('index_kind', 'none'), x.get('documents_per_record', 1), x.get('batch', 0),
                          x.get('measurement', 'budgeted' if x.get('budgeted') else 'unbudgeted'))
                         for x in results if x['workload'] == workload})
        for kind, records, batch, mode in groups:
            group = {v: [x for x in results if x['workload'] == workload and x['variant'] == v
                         and x.get('index_kind', 'none') == kind and x.get('documents_per_record', 1) == records and x.get('batch', 0) == batch
                         and x.get('measurement', 'budgeted' if x.get('budgeted') else 'unbudgeted') == mode]
                     for v in binaries}
            fields = ['elapsed_ns', 'allocations', 'allocated_bytes', 'peak_live_bytes'] if workload == 'replay' else [
                'run_ns', 'final_checkpoint_ns', 'max_batch_ns', 'allocations', 'allocated_bytes', 'peak_additional_live_bytes']
            stats = {f: {v: statistics.median(x[f] for x in xs) for v, xs in group.items()}
                     for f in fields if all(f in x for xs in group.values() for x in xs)}
            for values in stats.values():
                values['change_percent'] = 100 * (values['changed'] / values['baseline'] - 1)
            field = 'elapsed_ns' if workload == 'replay' else 'run_ns'
            ratios = [100 * (next(x[field] for x in group['changed'] if x['pair'] == pair) /
                            next(x[field] for x in group['baseline'] if x['pair'] == pair) - 1)
                      for pair in range(len(group['baseline']))]
            stats['paired_elapsed_change_percent_median'] = statistics.median(ratios)
            summary[f'{workload}-{kind}-{records}-{batch}-{mode}'] = stats
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
