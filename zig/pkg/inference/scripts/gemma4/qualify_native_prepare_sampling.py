"""Sample real checkpoint preparation at 8K or 32K on the selected server host.

Full request wall time includes ingestion, model setup and client file validation.
The separate query/snapshot/fitting/export-write intervals exclude those costs;
their sum is not complete compaction latency. Export-write excludes terminal
close and owner retirement. Every successful observation still requires a fully
validated checksum/EOF and bytes identical to a qualified portable checkpoint.
"""
import argparse
import copy
import json
import math
from pathlib import Path
import shutil
import sys

ROOT = Path.cwd()
BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT/'zig/pkg/inference/scripts/gemma4'))
from qualify_native_checkpoint_http import git_provenance, prepare, requests, server, sha256_file
from qualify_context_management import cuda_device_evidence
from qualify_matched_llama import percentiles, require_idle
from native_benchmark_fixture import qualified_fixture
from native_phase_timing import PREPARE_PHASES, prepare_timing, request_log


def validate_export(actual, expected, logical_tokens):
    if actual['metadata']['logical_tokens'] != logical_tokens or actual['metadata']['outcome'] != 'compacted':
        raise AssertionError('wrong logical length or unchanged fallback')
    for field in ('metadata', 'identity', 'file_sha256'):
        if actual[field] != expected[field]: raise AssertionError(f'repeated export changed {field}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--server-kind', choices=['inference', 'standalone'], default='standalone')
    parser.add_argument('--encoding', choices=['f32', 'f16', 'fp8', 'rotated4'], default='f16')
    parser.add_argument('--experimental-rotated4', action='store_true')
    parser.add_argument('--models-dir', type=Path, default=Path('.models'))
    parser.add_argument('--model', default='google/gemma-4-E4B-it-qat-q4_0-gguf')
    parser.add_argument('--reference', type=Path, required=True)
    parser.add_argument('--reference-checkpoint', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--size', choices=['8k', '32k'], required=True)
    parser.add_argument('--port', type=int, default=8526)
    parser.add_argument('--timeout', type=float, default=1800)
    parser.add_argument('--warmups', type=int, default=5)
    parser.add_argument('--samples', type=int, default=100)
    parser.add_argument('--smoke', action='store_true')
    args = parser.parse_args()
    if args.warmups < 0 or args.samples < 1 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error('invalid sampling counts or timeout')
    if not args.smoke and (args.warmups < 5 or args.samples < 100):
        parser.error('release sampling requires five warmups and 100 measured requests')
    args.expected_outcome = 'compacted'
    args.minimum_savings = 1.5 if args.size == '8k' else 2
    args.checkpoint = args.output.with_suffix('.working.kv')
    if args.output.exists() or args.checkpoint.exists(): raise FileExistsError(args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    evidence = {'passed': False, 'production_ready': False, 'sampling_qualified': False,
                'scope': __doc__, 'size': args.size, 'smoke': args.smoke, 'encoding': args.encoding, 'server_kind': args.server_kind,
                'warmups_requested': args.warmups, 'samples_requested': args.samples,
                'source': git_provenance(ROOT), 'binary_sha256': sha256_file(args.server),
                'observations': [], 'retries': 0,
                'timing_limits': 'Fitting combines selection, mass/value solvers and validation. Upload/restore and server-side ingestion are not separately measured by this runner.'}
    def publish():
        temporary = args.output.with_suffix('.json.part')
        temporary.write_text(json.dumps(evidence, indent=2)+'\n'); temporary.replace(args.output)
    try:
        require_idle()
        reference, fixture = qualified_fixture(args.reference, args.reference_checkpoint, args.size)
        fixture.pop('token_ids')
        if reference['source'] != evidence['source'] or reference['binary_sha256'] != evidence['binary_sha256']:
            raise AssertionError('reference must qualify this exact source and binary')
        expected = next(row['result'] for row in reference['exporter']['requests'] if row['kind'] == 'prepare')
        if expected['metadata'].get('encoding', 'f32') != args.encoding:
            raise AssertionError('reference encoding differs from sampling campaign')
        if args.encoding == 'rotated4':
            from qualify_attention_match_queries import validate_quantization_evidence
            validate_quantization_evidence([expected['metadata']], 'rotated4')
        if args.encoding == 'fp8' and expected['metadata'].get('fp8_heads', 0) < 1:
            raise AssertionError('FP8 sampling requires actually quantized state')
        pinned = {args.server: evidence['binary_sha256'], args.reference: sha256_file(args.reference),
                  args.reference_checkpoint: fixture['checkpoint_sha256_to_reverify'],
                  Path(__file__): sha256_file(Path(__file__))}
        for name in ['native_phase_timing.py', 'native_benchmark_fixture.py', 'qualify_matched_llama.py']:
            pinned[BASE/name] = sha256_file(BASE/name)
        model = args.models_dir/args.model/'gemma-4-E4B_q4_0-it.gguf'
        pinned[model] = '676c35070db6dbe52f93e9c864ee0fba4eddea94b9c875d9cb10daff453fbaee'
        for path, digest in pinned.items():
            if sha256_file(path) != digest: raise AssertionError(f'input changed: {path}')
        hardware = cuda_device_evidence()
        if len(hardware) != 1 or hardware[0]['name'] != 'NVIDIA L4': raise AssertionError('expected one designated L4')
        body = copy.deepcopy(next(iter(requests(args.model, 'f16', [742 if args.size == '8k' else 2975], False, 'fact'))))
        body.pop('context_management'); body.pop('cache_dtype'); body['prompt_cache'] = False
        evidence.update(fixture=fixture, hardware=hardware, request=body,
                        input_sha256={str(p): d for p, d in pinned.items()})
        require_idle(); publish()
        with server(args, 'worker', evidence) as record:
            log_path = args.output.with_suffix('.worker.log')
            for index in range(1+args.warmups+args.samples):
                if shutil.disk_usage(args.output.parent).free < 2*fixture['checkpoint_bytes']+256*1024**2:
                    raise RuntimeError('insufficient disk reserve for atomic checkpoint replacement')
                row = {'index': index, 'cold': index == 0, 'warmup': 0 < index <= args.warmups, 'passed': False}
                evidence['observations'].append(row); publish()
                offset = log_path.stat().st_size
                actual = prepare(args, body, record)
                seconds = record['requests'][-1]['seconds']
                validate_export(actual, expected, fixture['logical_tokens'])
                phases = prepare_timing(request_log(log_path, offset), fixture['logical_tokens'],
                                        actual['metadata']['checkpoint_bytes'], seconds)
                row.update(passed=True, total_seconds=seconds, work_timing=phases,
                           checkpoint_sha256=actual['file_sha256'])
                publish()
                print(json.dumps({'index': index, 'size': args.size, 'seconds': seconds}), flush=True)
        memory = evidence['worker']['memory']
        if memory['samples'] < 2 or memory['errors'] or memory['gpu_uuids'] != [hardware[0]['uuid']]:
            raise AssertionError('incomplete or misattributed memory sampling')
        if not 0 < memory['peak_host_rss_bytes'] <= 8*1024**3 or not 0 < memory['peak_gpu_memory_bytes'] <= 18*1024**3:
            raise AssertionError('memory cap exceeded')
        if git_provenance(ROOT) != evidence['source']: raise AssertionError('source changed during campaign')
        for path, digest in pinned.items():
            if sha256_file(path) != digest: raise AssertionError(f'input changed: {path}')
        if sha256_file(args.checkpoint) != fixture['checkpoint_sha256_to_reverify']:
            raise AssertionError('working checkpoint changed')
        measured = [row for row in evidence['observations'] if not row['cold'] and not row['warmup']]
        evidence['total_seconds'] = percentiles([row['total_seconds'] for row in measured])
        evidence['work_timing_ns'] = {key: percentiles([row['work_timing'][key] for row in measured])
                                      for key in PREPARE_PHASES+('measured_work_sum_ns',)}
        evidence.update(passed=True, sampling_qualified=not args.smoke and len(measured) >= 100 and args.warmups >= 5,
                        measured_actual_compactions=len(measured))
    except Exception as error: evidence['error'] = f'{type(error).__name__}: {error}'
    finally: publish()
    print(json.dumps({'passed': evidence['passed'], 'error': evidence.get('error')}), flush=True)
    return 0 if evidence['passed'] else 1


if __name__ == '__main__': raise SystemExit(main())
