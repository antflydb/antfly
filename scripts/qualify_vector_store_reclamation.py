#!/usr/bin/env python3
"""Verify idle reclamation on copies of completed, pinned benchmark databases.

Run after timed A/B arms. These supplemental observations do not replace their
original end-of-run accounting. macOS clones preserve the original evidence.
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

from run_vector_store_ab import digest
from vector_store_experiment_settings import configure


def get_json(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('qualification_root', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--port', type=int, default=18138)
    parser.add_argument('--timeout', type=float, default=300)
    parser.add_argument('--recovery-binary', type=Path, help='Test a correction against cloned saved data; preserve the original measurement binary receipt.')
    args = parser.parse_args()
    recovery_binary = args.recovery_binary.resolve(strict=True) if args.recovery_binary else None
    recovery_hash = digest(recovery_binary) if recovery_binary else None
    arms = json.loads((args.qualification_root / 'ab-runs.json').read_text())
    if len(arms) < 4 or any(a.get('exit_code') != 0 or a.get('invalid_reason') for a in arms):
        parser.error('all timed qualification arms must complete successfully first')
    args.output.mkdir(parents=True, exist_ok=False)
    results = []
    for arm in arms:
        name = f"{arm['case']}-{arm['pair']}-{arm['mode']}"
        source = args.qualification_root / name
        config = json.loads((source / 'run-config.json').read_text())
        import pyarrow.parquet as pq
        query_batch = next(pq.ParquetFile(Path(config['public_profile_dataset']) / 'test.parquet')
                           .iter_batches(batch_size=1, columns=['emb']))
        query_vector = query_batch.column(0)[0].as_py()
        measurement_binary = Path(config['antfly_bin'])
        if digest(measurement_binary) != arm['binary_sha256']:
            raise RuntimeError('pinned measurement binary changed')
        binary = recovery_binary or measurement_binary
        binary_hash = recovery_hash or arm['binary_sha256']
        if digest(binary) != binary_hash:
            raise RuntimeError('pinned recovery binary changed')
        output = args.output / name
        output.mkdir()
        print('reclamation start', name, flush=True)
        if sys.platform == 'darwin':
            subprocess.run(['cp', '-cR', str(source / 'data'), str(output / 'data')], check=True)
        else:
            shutil.copytree(source / 'data', output / 'data')
        environment = configure(os.environ.copy(), arm['refinement'], arm['mode'] == 'candidate')
        if {k: environment.get(k) for k in arm['refinement_environment']} != arm['refinement_environment']:
            raise RuntimeError('source-store environment differs from timed arm')
        environment.update(ANTFLY_HBC_POSTING_SIDECAR='1', ANTFLY_HBC_POSTING_WAL_STORE='1',
                           ANTFLY_HBC_VECTOR_BLOCK_STORE='1', ANTFLY_HBC_VECTOR_BLOCK_ENCODING='float32')
        expected = 1000000 if '1M' in arm['case'] else 50000
        dimensions = 768 if '1M' in arm['case'] else 1536
        command = [str(binary), 'standalone', '--host', '127.0.0.1', '--port', str(args.port),
                   '--health-port', str(args.port + 1), '--auth', 'false',
                   '--process-memory-budget-mb', str(config['process_memory_budget_mb']),
                   '--data-dir', str((output / 'data').resolve())]
        result = {'arm': name, 'binary_sha256': binary_hash,
                  'measurement_binary_sha256': arm['binary_sha256'], 'command': command,
                  'refinement_environment': arm['refinement_environment'], 'qualified': False}
        results.append(result)
        receipt = args.output / 'reclamation.json'
        receipt.write_text(json.dumps(results, indent=2) + '\n')
        started = time.monotonic()
        with (output / 'server.log').open('w') as log, (output / 'observations.jsonl').open('w') as observations:
            process = subprocess.Popen(command, env=environment, stdout=log, stderr=subprocess.STDOUT)
            resident_at = None
            try:
                while time.monotonic() - started < args.timeout:
                    if process.poll() is not None:
                        raise RuntimeError('reclamation server exited: ' + name)
                    try:
                        if resident_at is None:
                            # Metadata probes can use ephemeral handles. Admit
                            # the normal resident query runtime once, then leave
                            # it idle so its background GC owner can progress.
                            request = urllib.request.Request(
                                f'http://127.0.0.1:{args.port}/db/v1/tables/vdbbench/query',
                                data=json.dumps({'embeddings': {'vec': query_vector},
                                                 'limit': 1, 'fields': []}).encode(),
                                headers={'Content-Type': 'application/json'})
                            query = get_json(request, timeout=180)
                            (output / 'resident-query.json').write_text(json.dumps(query, indent=2) + '\n')
                            resident_at = time.monotonic()
                            result['resident_ready_seconds'] = resident_at - started
                        table = get_json(f'http://127.0.0.1:{args.port}/db/v1/tables/vdbbench')
                    except (urllib.error.URLError, TimeoutError):
                        time.sleep(1)
                        continue
                    source_stats = table.get('storage_status', {}).get('source_vectors') or {}
                    elapsed = time.monotonic() - started
                    observations.write(json.dumps({'elapsed_s': elapsed, 'source_vectors': source_stats}) + '\n')
                    observations.flush()
                    clean = (source_stats.get('retained_payloads') == expected
                             and source_stats.get('retained_payload_bytes') == expected * dimensions * 4
                             and source_stats.get('collection_pending_bytes') == 0
                             and source_stats.get('unreferenced_payload_bytes_at_collection') == 0
                             and source_stats.get('unresolved_primary_commits') == 0
                             and source_stats.get('active_sessions') == 0)
                    if clean:
                        # This inventory is captured under the source lock and
                        # there are no concurrent writes. Subsequent metadata
                        # probes may omit stats after resident-cache eviction;
                        # prove serving across GC instead of requiring repeated
                        # best-effort observations of the same atomic snapshot.
                        query = get_json(request, timeout=180)
                        (output / 'post-gc-query.json').write_text(json.dumps(query, indent=2) + '\n')
                        index = get_json(f'http://127.0.0.1:{args.port}/db/v1/tables/vdbbench/indexes/vec')
                        status = index.get('status') or {}
                        if (status.get('rebuilding') is not False
                                or status.get('searchable_vectors') != expected
                                or not status.get('publication', {}).get('complete')):
                            time.sleep(1)
                            continue
                        (output / 'index.json').write_text(json.dumps(index, indent=2) + '\n')
                        (output / 'table.json').write_text(json.dumps(table, indent=2) + '\n')
                        result.update(qualified=True, settled_seconds=elapsed,
                                      idle_settled_seconds=time.monotonic() - resident_at,
                                      source_vectors=source_stats)
                        break
                    time.sleep(1)
                if not result['qualified']:
                    result['error'] = 'reclamation did not settle before timeout'
            finally:
                process.terminate()
                try:
                    process.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    result.update(qualified=False, error='shutdown exceeded 60 seconds')
                result['server_exit_code'] = process.returncode
                if process.returncode not in (0, -15):
                    result.update(qualified=False, error='unexpected server exit')
                if digest(binary) != binary_hash:
                    result.update(qualified=False, error='recovery binary changed during run')
                receipt.write_text(json.dumps(results, indent=2) + '\n')
        subprocess.run([sys.executable, '-B', str(Path(__file__).with_name('vector_store_disk_accounting.py')),
                        str(output / 'data'), str(output / 'disk.json')], check=True)
        print('reclamation finished', name, result['qualified'], result.get('settled_seconds'), flush=True)
        if not result['qualified']:
            raise SystemExit('reclamation qualification failed: ' + str(receipt))


if __name__ == '__main__':
    main()
