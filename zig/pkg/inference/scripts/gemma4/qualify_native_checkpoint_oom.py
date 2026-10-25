#!/usr/bin/env python3
"""Exercise physical CUDA OOM and recovery on a loaded public checkpoint worker.

This does not qualify live replacement during fitting, sustained pressure,
or other production gates. Run serially on the GPU.
"""
import argparse
import copy
import http.client
import json
import re
from pathlib import Path

from cuda_device_pressure import CudaDriver, device_pressure
from native_checkpoint_transport import send_resume
from qualify_native_checkpoint_http import REPO, canonical_choices, git_provenance, requests, resume, server, sha256_file


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--server-kind', choices=['inference', 'standalone'], default='inference')
    parser.add_argument('--models-dir', type=Path, default=Path('.models'))
    parser.add_argument('--model', default='google/gemma-4-E4B-it-qat-q4_0-gguf')
    parser.add_argument('--checkpoint', type=Path, required=True)
    parser.add_argument('--reference', type=Path, required=True, help='passing public HTTP roundtrip evidence for this checkpoint')
    parser.add_argument('--padding-lines', type=int, default=742)
    parser.add_argument('--port', type=int, default=8508)
    parser.add_argument('--timeout', type=float, default=1800)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.fixture = 'fact'
    if args.output.exists(): raise FileExistsError(args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    evidence = {'passed': False, 'production_ready': False, 'full_model_restore_recovery_qualified': False,
                'live_cache_replacement_oom_qualified': False, 'server_kind': args.server_kind, 'source': git_provenance(REPO),
                'binary_sha256': sha256_file(args.server), 'checkpoint_sha256': sha256_file(args.checkpoint),
                'reference_sha256': sha256_file(args.reference),
                'scope': 'Physical CUDA allocation failure and subsequent public checkpoint restore on the same loaded worker',
                'helper_sources': {str(p): sha256_file(p) for p in (Path(__file__), Path(__file__).with_name('cuda_device_pressure.py'))}}
    try:
        reference = json.loads(args.reference.read_text())
        if reference.get('passed') is not True: raise AssertionError('reference qualification did not pass')
        exports = [r for r in reference['exporter']['requests'] if r['kind'] == 'prepare']
        if len(exports) != 1: raise AssertionError('ambiguous reference checkpoint')
        prepared = exports[0]['result']
        if prepared['file_sha256'] != evidence['checkpoint_sha256']: raise AssertionError('checkpoint differs from reference')
        body = copy.deepcopy(next(iter(requests(args.model, 'f16', [args.padding_lines], False, args.fixture))))
        body.pop('context_management'); body.pop('cache_dtype')
        with server(args, 'worker', evidence) as record:
            before = resume(args, body, record, prepared, 'before_pressure')
            log_path = args.output.with_suffix('.worker.log')
            offset = log_path.stat().st_size
            # CUDA is initialized only after the model has loaded and restored.
            # The temporary allocation is external to its accounting ledger,
            # forcing a real driver failure instead of a configured budget error.
            driver = CudaDriver()
            evidence['pressure_device_uuid'] = driver.uuid
            with device_pressure(driver) as pressure:
                evidence['pressure'] = pressure
                connection = http.client.HTTPConnection('127.0.0.1', args.port, timeout=args.timeout)
                try:
                    response = send_resume(connection, '/ai/v1/generate/context/resume', body, args.checkpoint)
                    payload = response.read(65536)
                    evidence['fault_response'] = {'status': response.status, 'body': payload.decode(errors='replace')}
                    if response.status != 500 or json.loads(payload).get('message') not in ('CudaDriverError', 'CudaMemoryLimitExceeded'):
                        raise AssertionError('request did not reach a physical CUDA allocation failure')
                finally:
                    connection.close()
                with log_path.open('rb') as log:
                    log.seek(offset); fault_log = log.read().decode(errors='replace')
                evidence['fault_log'] = fault_log
                failures = re.findall(r'CUDA cuMemAlloc failed: bytes=(\d+) error=CUDA_ERROR_OUT_OF_MEMORY', fault_log)
                if not failures: raise AssertionError('missing physical CUDA OOM log during the pressured request')
                evidence['failed_allocation_bytes'] = [int(n) for n in failures]
            if not pressure['released']: raise AssertionError('pressure context did not retire')
            # Immediate recovery: no retry is allowed to mask leaked admission.
            after = resume(args, body, record, prepared, 'after_pressure')
            if canonical_choices(before['choices']) != canonical_choices(after['choices']) or before['usage'] != after['usage']:
                raise AssertionError('post-OOM restored continuation changed')
            stream = copy.deepcopy(body); stream['stream'] = True
            resume(args, stream, record, prepared, 'stream_after_pressure')
        memory = evidence['worker']['memory']
        if memory['gpu_uuids'] != [evidence['pressure_device_uuid']]:
            raise AssertionError('pressure and model worker did not use the same GPU')
        if memory['samples'] < 2 or memory['errors'] or not memory['gpu_uuids']:
            raise AssertionError('incomplete model-worker memory evidence')
        if not 0 < memory['peak_host_rss_bytes'] <= 8 * 1024**3 or not 0 < memory['peak_gpu_memory_bytes'] <= 18 * 1024**3:
            raise AssertionError('model worker exceeded memory cap')
        if git_provenance(REPO) != evidence['source']: raise AssertionError('source changed during qualification')
        for path, digest in [(args.server, evidence['binary_sha256']), (args.checkpoint, evidence['checkpoint_sha256']),
                             (args.reference, evidence['reference_sha256']), *evidence['helper_sources'].items()]:
            if sha256_file(Path(path)) != digest: raise AssertionError(f'evidence changed: {path}')
        evidence.update(passed=True, full_model_restore_recovery_qualified=True)
    except Exception as error:
        evidence['error'] = f'{type(error).__name__}: {error}'
    finally:
        args.output.write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps({'passed': evidence['passed'], 'error': evidence.get('error'), 'output': str(args.output)}), flush=True)
    return 0 if evidence['passed'] else 1


if __name__ == '__main__': raise SystemExit(main())
