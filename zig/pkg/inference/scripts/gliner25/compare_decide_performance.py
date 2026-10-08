#!/usr/bin/env python3
"""Evaluate paired process blocks; never treat individual requests as independent runs."""
from __future__ import annotations
import argparse
import json
import math
from pathlib import Path
import random
import re
import statistics

PRIMARY = ('described_prompt_choice', 'choice_score_noul')
CASES = (*PRIMARY, 'binary_short', 'described_four_labels', 'score_and_noul', 'mixed_longer')


def percentile(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, math.ceil(len(ordered) * p) - 1))]


def swap_used(context):
    match = re.search(r'used\s*=\s*([\d.]+)M', context.get('swap', ''))
    return float(match.group(1)) if match else None


def power_source(context):
    match = re.search(r"drawing from '([^']+)'", context.get('power', ''))
    return match.group(1) if match else None


def evaluate(pairs):
    cases = {}
    failures = []
    if len(pairs) != 6:
        failures.append('Acceptance requires six paired process blocks (three AB/BA cycles).')
    backend = None
    identity = None
    python_identity = None
    for block, (antfly, python) in enumerate(pairs):
        if not antfly.get('passed') or not python.get('passed'):
            failures.append(f'Block {block}: failed or incomplete process.')
            continue
        current = (antfly['binary_sha256'], antfly['source'])
        if identity is not None and current != identity:
            failures.append(f'Block {block}: Antfly source or binary changed.')
        identity = current
        power = {power_source(r[c]) for r in (antfly, python) for c in ('host_before', 'host_after')}
        if len(power) != 1 or None in power:
            failures.append(f'Block {block}: power source changed or telemetry is missing.')
        for name, receipt in [('antfly', antfly), ('python', python)]:
            if receipt.get('diagnostic_only') or receipt.get('performance_qualification') is False:
                failures.append(f'Block {block} {name}: diagnostic run cannot qualify performance.')
            before, after = swap_used(receipt['host_before']), swap_used(receipt['host_after'])
            # Conservative: until per-timed-phase telemetry is available, even
            # load-time swap growth disqualifies a block. Never silently drop it.
            if before is None or after is None or after > before:
                failures.append(f'Block {block} {name}: swap telemetry missing or swap grew.')
            for context in ('host_before', 'host_after'):
                thermal = receipt[context].get('thermal', '')
                if not thermal or re.search(r'CPU_Speed_Limit\s*=\s*(?:[0-9]|[1-9][0-9])(?:\D|$)', thermal):
                    failures.append(f'Block {block} {name}: thermal telemetry unavailable or throttled.')
        py = python['report']
        current_python = tuple(py.get(key) for key in ('model', 'source', 'runtime', 'capture', 'short_holdout_sha256'))
        if not all(current_python) or (python_identity is not None and current_python != python_identity):
            failures.append(f'Block {block}: Python artifact, source, runtime or fixture identity missing or changed.')
        python_identity = current_python
        if python.get('source') != antfly.get('source'):
            failures.append(f'Block {block}: benchmark source differs between paired processes.')
        if not antfly.get('source_unchanged') or not python.get('source_unchanged') or not antfly.get('binary_unchanged'):
            failures.append(f'Block {block}: source changed during measurement.')
        reports = [r for r in antfly['reports'] if r['path'] == 'http_handler']
        for name, rows in (('antfly', reports), ('python', py['cases'])):
            if sorted(r['case_id'] for r in rows) != sorted(CASES):
                failures.append(f'Block {block} {name}: exact six-case inventory required without duplicates.')
        pycases = {r['case_id']: r for r in py['cases']}
        for report in reports:
            if backend is not None and backend != report['backend']:
                failures.append(f'Block {block}: backend changed during campaign.')
            backend = report['backend']
            if (backend, py['device']) not in (('metal', 'mps'), ('native', 'cpu')):
                failures.append(f'Block {block}: backend mismatch.')
            case = report['case_id']
            reference = pycases.get(case)
            if reference is None or reference['prepared_tokens'] != report['prepared_tokens']:
                failures.append(f'Block {block}: missing or mismatched case {case}.')
                continue
            if len(report['samples_ns']) != 20 or len(reference['samples_ns']) != 20 or any(not math.isfinite(x) or x <= 0 for x in report['samples_ns'] + reference['samples_ns']):
                failures.append(f'Block {block} {case}: invalid sample inventory.')
                continue
            model_digest = report.get('model', {}).get('sha256')
            if not model_digest or model_digest != py.get('model', {}).get('model_sha256'):
                failures.append(f'Block {block} {case}: model artifact mismatch.')
            capture_digest = py.get('capture', {}).get('sha256') if case in PRIMARY else py.get('short_holdout_sha256')
            if not capture_digest or report.get('capture_sha256') != capture_digest:
                failures.append(f'Block {block} {case}: capture artifact mismatch.')
            a = [x / 1e6 for x in report['samples_ns']]
            p = [x / 1e6 for x in reference['samples_ns']]
            row = cases.setdefault(case, {'ratios': [], 'antfly_ms': [], 'python_ms': []})
            row['ratios'].append(statistics.median(a) / statistics.median(p))
            row['antfly_ms'].extend(a)
            row['python_ms'].extend(p)
    rng = random.Random(20261007)
    result = {}
    for case, row in cases.items():
        ratios = row['ratios']
        boot = [statistics.mean(rng.choices(ratios, k=len(ratios))) for _ in range(10000)]
        upper = percentile(boot, .975)
        median_ratio = statistics.median(row['antfly_ms']) / statistics.median(row['python_ms'])
        p95_ratio = percentile(row['antfly_ms'], .95) / percentile(row['python_ms'], .95)
        result[case] = dict(paired_median_ratios=ratios, mean_ratio_ci95=[percentile(boot, .025), upper],
                            antfly_median_ms=statistics.median(row['antfly_ms']), python_median_ms=statistics.median(row['python_ms']),
                            pooled_p95_ratio=p95_ratio, pooled_median_ratio=median_ratio)
        if len(ratios) != 6:
            failures.append(f'{case}: incomplete paired blocks.')
        if case in PRIMARY and (not all(r < 1 for r in ratios) or upper >= 1):
            failures.append(f'{case}: not a repeatable service-latency win.')
        if p95_ratio > 1.05 or (case not in PRIMARY and median_ratio > 1.05):
            failures.append(f'{case}: exceeds the 5% regression ceiling.')
    if not set(PRIMARY).issubset(cases):
        failures.append('Primary cases missing.')
    if len(set(cases) - set(PRIMARY)) != 4:
        failures.append('Four held-out cases required.')
    return dict(passed=not failures, backend=backend, paired_blocks=len(pairs), cases=result, failures=failures,
                scope='Antfly production allocator HTTP handler vs pinned Python loaded pipeline; excludes socket transport',
                telemetry_scope='Whole process before/after; stricter than timed-phase-only swap gating')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--pair', nargs=2, type=Path, action='append', required=True, metavar=('ANTFLY_RECEIPT', 'PYTHON_RECEIPT'))
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    result = evaluate([(json.loads(x.read_text()), json.loads(y.read_text())) for x, y in a.pair])
    a.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
