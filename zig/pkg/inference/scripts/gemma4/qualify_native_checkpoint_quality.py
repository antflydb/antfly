#!/usr/bin/env python3
"""Score the fixed quality suite through the public native-checkpoint SDK.

Run with the SDK venv. Unchanged fallback is reported separately from successful
compaction. This harness does not establish the separate sampling/stress gates.
"""
import argparse
import copy
import json
import math
import sys
import time
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(ROOT / 'py/packages/sdk/src'))
from antfly import AntflyClient
from antfly.client_generated.models import InferenceGenerateRequest
from native_quality_score import evaluate, score
from qualify_native_checkpoint_http import server, generate, git_provenance, sha256_file


def coverage(cases, baseline, resumed, checkpoints):
    if not (len(cases) == len(baseline) == len(resumed) == len(checkpoints)):
        raise ValueError('incomplete quality coverage')
    categories = {}
    for case, before, after, checkpoint in zip(cases, baseline, resumed, checkpoints):
        if checkpoint['case_id'] != case['case_id']:
            raise ValueError('changed or reordered checkpoint coverage')
        metadata = checkpoint['metadata']
        if not math.isfinite(metadata['global_savings_ratio']):
            raise ValueError('nonfinite savings ratio')
        outcome = metadata['outcome']
        if outcome not in ('compacted', 'unchanged_insufficient_savings'):
            raise ValueError('unknown checkpoint outcome')
        if outcome == 'compacted' and (metadata['global_savings_ratio'] < 1.5 or metadata['validated_heads'] < 1):
            raise ValueError('claimed compaction failed savings/validation threshold')
        if outcome == 'unchanged_insufficient_savings' and (metadata['global_savings_ratio'] != 1 or metadata['validated_heads'] != 0):
            raise ValueError('fallback incorrectly claims compaction')
        group = categories.setdefault(case['category'], {'compacted': 0, 'unchanged_insufficient_savings': 0,
            'compacted_baseline_correct': 0, 'compacted_resume_correct': 0, 'compacted_case_ids': [], 'fallback_case_ids': []})
        group[outcome] += 1
        if outcome == 'compacted':
            group['compacted_baseline_correct'] += score(case, before['response'])
            group['compacted_resume_correct'] += score(case, after['response'])
            group['compacted_case_ids'].append(case['case_id'])
        else:
            group['fallback_case_ids'].append(case['case_id'])
    return {'categories': categories,
        'outcomes': dict(Counter(c['metadata']['outcome'] for c in checkpoints)),
        'every_category_exercised_compaction': all(g['compacted'] > 0 for g in categories.values())}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--server-kind', choices=['inference', 'standalone'], default='inference')
    parser.add_argument('--models-dir', type=Path, default=Path('.models'))
    parser.add_argument('--suite', type=Path, default=Path('.cache/native-compaction/native-quality-suite-v1.jsonl'))
    parser.add_argument('--cases', nargs='+')
    parser.add_argument('--port', type=int, default=8506)
    parser.add_argument('--timeout', type=float, default=1800)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--checkpoint', type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists() or args.checkpoint.exists():
        raise FileExistsError('use new evidence/checkpoint paths; preserve prior results')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.checkpoint.parent.mkdir(parents=True, exist_ok=True)
    cases = [json.loads(line) for line in args.suite.read_text().splitlines() if line.strip()]
    if len({c['case_id'] for c in cases}) != len(cases):
        raise ValueError('duplicate case IDs')
    if args.cases:
        wanted = set(args.cases)
        cases = [c for c in cases if c['case_id'] in wanted]
        if {c['case_id'] for c in cases} != wanted: raise ValueError('unknown case')
    if not cases: raise ValueError('empty suite')
    evidence = {'passed': False, 'production_ready': False,
        'scope': 'Public API/SDK quality with explicit unchanged fallback; excludes sustained and latency sampling gates',
        'server_kind': args.server_kind, 'source': git_provenance(ROOT), 'binary_sha256': sha256_file(args.server),
        'suite_sha256': sha256_file(args.suite), 'case_ids': [c['case_id'] for c in cases],
        'sources': {str(p): sha256_file(p) for p in [Path(__file__), Path(__file__).with_name('native_quality_score.py'),
            Path(__file__).with_name('qualify_native_checkpoint_http.py')]},
        'baseline': [], 'resumed': [], 'checkpoints': []}
    def publish():
        temporary = args.output.with_suffix('.json.part')
        temporary.write_text(json.dumps(evidence, indent=2) + '\n')
        temporary.replace(args.output)
    try:
        with server(args, 'worker', evidence) as record:
            client = AntflyClient(f'http://127.0.0.1:{args.port}', timeout=args.timeout)
            http = client._client.get_httpx_client()
            def attest(response):
                if response.status_code == 200:
                    if response.headers.get('x-antfly-inference-backend') != 'cuda': raise AssertionError('missing CUDA backend')
                    if int(response.headers.get('x-antfly-inference-pid', '0')) != record['worker_pid']: raise AssertionError('wrong worker PID')
            http.event_hooks['response'].append(attest)
            try:
                for case in cases:
                    started = time.monotonic()
                    evidence['active_case'] = case['case_id']; publish()
                    print(json.dumps({'case': case['case_id'], 'phase': 'baseline'}), flush=True)
                    baseline = generate(args, case['body'], record)
                    evidence['baseline'].append({'fixture': case['case_id'], 'body': case['body'], 'response': baseline})
                    body = copy.deepcopy(case['body']); body.pop('context_management', None)
                    request = InferenceGenerateRequest.from_dict(body)
                    print(json.dumps({'case': case['case_id'], 'phase': 'prepare'}), flush=True)
                    prepare_start = time.monotonic()
                    checkpoint = client.prepare_native_kv_checkpoint(request, args.checkpoint)
                    if checkpoint.metadata['logical_tokens'] != baseline['usage']['prompt_tokens']:
                        raise AssertionError('checkpoint and baseline have different prompt token counts')
                    evidence['checkpoints'].append({'case_id': case['case_id'], 'metadata': dict(checkpoint.metadata),
                        'identity': dict(checkpoint.identity), 'sha256': checkpoint.sha256,
                        'prepare_wall_seconds': time.monotonic() - prepare_start, 'wire_request': body})
                    print(json.dumps({'case': case['case_id'], 'phase': 'resume', 'outcome': checkpoint.metadata['outcome']}), flush=True)
                    resume_start = time.monotonic()
                    result = client.resume_native_kv_checkpoint(request, checkpoint).to_dict()
                    if result['usage']['cached_prompt_tokens'] != checkpoint.metadata['logical_tokens']:
                        raise AssertionError('resume did not attest saved prefix reuse')
                    evidence['resumed'].append({'fixture': case['case_id'], 'body': case['body'], 'response': result,
                        'resume_wall_seconds': time.monotonic() - resume_start})
                    # Reuse bounded disk space. Digests and model identities remain in evidence.
                    args.checkpoint.unlink()
                    evidence['completed_cases'] = len(evidence['resumed']); publish()
                    print(json.dumps({'case': case['case_id'], 'phase': 'complete', 'seconds': time.monotonic()-started,
                        'baseline_correct': score(case, baseline), 'resume_correct': score(case, result)}), flush=True)
            finally:
                http.close()
        evidence['quality'] = evaluate(cases, evidence['baseline'], evidence['resumed'])
        evidence['coverage'] = coverage(cases, evidence['baseline'], evidence['resumed'], evidence['checkpoints'])
        memory = evidence['worker']['memory']
        if memory['samples'] < 2 or memory['errors'] or not memory['gpu_uuids']:
            raise AssertionError('incomplete memory evidence')
        for key, cap in [('peak_host_rss_bytes', 8*1024**3), ('peak_gpu_memory_bytes', 18*1024**3)]:
            if memory[key] is None or memory[key] > cap: raise AssertionError(f'{key} exceeds qualification cap')
        if git_provenance(ROOT) != evidence['source']: raise AssertionError('source changed')
        if sha256_file(args.server) != evidence['binary_sha256'] or sha256_file(args.suite) != evidence['suite_sha256']:
            raise AssertionError('binary or suite changed')
        for path, digest in evidence['sources'].items():
            if sha256_file(Path(path)) != digest: raise AssertionError('harness changed')
        evidence['passed'] = evidence['quality']['passed']
        evidence.pop('active_case', None)
    except Exception as error:
        evidence['error'] = f'{type(error).__name__}: {error}'
    finally:
        publish()
    print(json.dumps({'passed': evidence['passed'], 'error': evidence.get('error'), 'output': str(args.output)}), flush=True)
    return 0 if evidence['passed'] else 1


if __name__ == '__main__': raise SystemExit(main())
