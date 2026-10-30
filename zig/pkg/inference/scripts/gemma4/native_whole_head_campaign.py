# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Authenticate whole-head v2 batch observations; never authorize serving alone.

This contract deliberately does not accept the independent-block v1 release
schema. Physical serving, performance, reliability and persistent-owner soak
are separate evidence gates even when every private-owner quality pair passes.
"""
from pathlib import Path
import hashlib
import math
import os
import re
import stat

from release_attention_matching import decode_json
from native_whole_head_quality import case_contract, configuration_contract
from native_whole_head_quality_metrics import evaluate

ROLES = ('messages', 'rendered_prompt', 'prompt_tokens', 'reference_utf8', 'reference_tokens')
MODELS = {
    'gemma4-e2b': ('google/gemma-4-E2B-it-qat-q4_0-gguf', 35, 15, 1, (4, 9, 14, 19, 24, 29, 34)),
    'gemma4-e4b': ('google/gemma-4-E4B-it-qat-q4_0-gguf', 42, 24, 2, (5, 11, 17, 23, 29, 35, 41)),
}
ENCODINGS = frozenset(('f16', 'fp8', 'rotated4'))


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def digest(value, label):
    require(isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value) and value != '0' * 64,
            'invalid ' + label + ' digest')
    return value


def regular(root, relative, label):
    root = Path(root)
    require(root.is_absolute() and root.is_dir() and not root.is_symlink(), 'invalid evidence root')
    require(isinstance(relative, str) and relative and not relative.startswith('/') and
            all(part not in ('', '.', '..') for part in relative.split('/')), 'unsafe ' + label + ' path')
    current = root
    for index, part in enumerate(relative.split('/')):
        current = current / part
        mode = current.lstat().st_mode
        require(not stat.S_ISLNK(mode), 'symlinked ' + label)
        require(stat.S_ISREG(mode) if index == len(relative.split('/')) - 1 else stat.S_ISDIR(mode),
                'nonregular ' + label)
    require(current.resolve(strict=True).is_relative_to(root.resolve(strict=True)), 'escaped ' + label)
    return current


def read_ref(root, ref, label, maximum=16 * 1024 * 1024):
    require(type(ref) is dict and set(ref) == {'path', 'sha256'}, 'invalid ' + label + ' reference')
    path = regular(root, ref['path'], label)
    require(path.stat().st_size <= maximum, 'oversized ' + label)
    # O_NOFOLLOW also protects the final component between lstat and open.
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, 'rb') as stream:
        require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), 'nonregular ' + label)
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum and hashlib.sha256(raw).hexdigest() == digest(ref['sha256'], label),
            'changed ' + label)
    return raw


def _finite_json(value):
    if isinstance(value, float):
        require(math.isfinite(value), 'nonfinite JSON number')
    elif isinstance(value, dict):
        for item in value.values():
            _finite_json(item)
    elif isinstance(value, list):
        for item in value:
            _finite_json(item)


def json_ref(root, ref, label, maximum=16 * 1024 * 1024):
    value = decode_json(read_ref(root, ref, label, maximum))
    _finite_json(value)
    return value


def expected_case(manifest, case, index, batch, configuration, implementation, sampling):
    require(batch['model'] in MODELS and batch['encoding'] in ENCODINGS, 'unknown native quality cell')
    model, layers, donors, kv_heads, consumers = MODELS[batch['model']]
    require(manifest['model'] == model and type(batch['context_tokens']) is int and
            batch['context_tokens'] in (8192, 32768),
            'model or context differs from native cell')
    require(case['id'] == sampling['id'] and sampling['model'] == batch['model'] and
            type(sampling['context_tokens']) is int and sampling['context_tokens'] == batch['context_tokens'] and
            case['bucket'] == sampling['bucket'] == batch['bucket'], 'sampling membership changed')
    return {
        'case_index': index, 'encoding': batch['encoding'], 'native_identity': manifest['native_identity'],
        'implementation_sha256': digest(implementation, 'implementation'),
        'global_consumers': [{'layer': layer, 'donor': min(layer, donors - 1),
                             'query_heads': 8, 'kv_heads': kv_heads, 'dim': 512} for layer in consumers],
        'layers': layers, 'donors': donors, 'blueprint_id': sampling['blueprint_id'],
        'primary_family': sampling['primary_family'], 'bucket': sampling['bucket'],
        'observed': configuration['observed'], 'execution': configuration['execution'],
        'output_tokens': 256, 'ordinary_greedy_route': 'backend_owned', 'source_serving_prefill': False,
    }


def audit_batch(input_root, manifest_ref, output_root, output_refs, batch, configuration,
                implementation, sampling_rows):
    """Audit one immutable complete batch against authenticated campaign metadata.

    The caller authenticates source, executable, native model artifacts, job,
    resource receipt and configuration before calling this numerical contract.
    No backend pass bit substitutes for checking raw observations and inputs.
    """
    raw_manifest = read_ref(input_root, manifest_ref, 'native batch manifest')
    manifest = decode_json(raw_manifest)
    require(type(manifest) is dict and manifest.get('schema') == 'antfly.native-checkpoint-quality.input.v1',
            'unsupported native batch manifest')
    require(type(batch.get('cases')) is int and batch['cases'] == 100 and
            len(manifest['cases']) == batch['cases'], 'incomplete native batch cohort')
    require(type(output_refs) is list and len(output_refs) == len(manifest['cases']) and
            len({ref['path'] for ref in output_refs}) == len(output_refs), 'missing or repeated observation file')
    require(type(sampling_rows) is list and len({row['id'] for row in sampling_rows}) == len(sampling_rows),
            'repeated sampling case')
    sampling = {row['id']: row for row in sampling_rows}
    require(len({case['id'] for case in manifest['cases']}) == len(manifest['cases']), 'repeated manifest case')
    configuration_contract(configuration, configuration)
    rows, coverage, phases = [], [], {}
    owners = set()
    for index, (case, ref) in enumerate(zip(manifest['cases'], output_refs)):
        require(case['id'] in sampling, 'case outside frozen scoring cohort')
        expected = expected_case(manifest, case, index, batch, configuration, implementation, sampling[case['id']])
        report = json_ref(output_root, ref, 'native observation')
        inputs = {'manifest': raw_manifest, **{role: read_ref(input_root, case[role], role) for role in ROLES}}
        rows.append(case_contract(report, case, inputs, expected))
        require(rows[-1]['context_tokens'] == batch['context_tokens'], 'observed context changed')
        case_owners = {report['source_owner_id'], report['compacted_owner_id'],
                       *(report[role]['owner_id'] for role in ('exact_greedy', 'compacted_greedy', 'exact_teacher', 'compacted_teacher'))}
        require(owners.isdisjoint(case_owners), 'owner identity replayed between quality cases')
        owners.update(case_owners)
        frozen = report['frozen']
        coverage.append({key: frozen[key] for key in ('validated_heads', 'preserved_heads', 'fp8_heads',
                                                    'fp8_fallback_heads', 'rotated4_heads', 'rotated4_fallback_heads')})
        for phase, nanos in report['phase_durations_ns'].items():
            phases[phase] = phases.get(phase, 0) + nanos
    return {'schema': 'antfly.native-whole-head.batch-audit.v2', 'collection_complete': True,
            'rows': rows, 'codec_coverage': coverage, 'phase_durations_ns': phases,
            'private_owner_count': len(owners), 'full_serving_owner_qualified': False,
            'production_ready': False, 'release_authority': False}


def evaluate_model_encoding(batches, model, encoding, seed):
    require(model in MODELS and encoding in ENCODINGS and type(batches) is list,
            'unknown quality release cell')
    expected = {(context, bucket) for context in (8192, 32768)
                for bucket in ('document_qa', 'exact_retrieval', 'code', 'structured_tool', 'multiturn')}
    require(len(batches) == len(expected), 'incomplete model/encoding quality matrix')
    cells, rows = set(), []
    for batch, audit in batches:
        require(batch['model'] == model and batch['encoding'] == encoding and
                (batch['context_tokens'], batch['bucket']) not in cells, 'duplicate or substituted quality cell')
        cells.add((batch['context_tokens'], batch['bucket']))
        require(audit.get('schema') == 'antfly.native-whole-head.batch-audit.v2' and
                audit.get('collection_complete') is True and len(audit['rows']) == 100 and
                audit.get('full_serving_owner_qualified') is False and audit.get('release_authority') is False,
                'unaudited whole-head quality batch')
        require(all(row['context_tokens'] == batch['context_tokens'] and row['bucket'] == batch['bucket']
                    for row in audit['rows']), 'cross-cell observation substitution')
        rows.extend(audit['rows'])
    require(cells == expected and len(rows) == 1000, 'incomplete native quality cohort')
    return evaluate(rows, seed + ':' + model + ':' + encoding)


def combined_resource_contract(receipt):
    """Recompute the model-campaign guard verdict from its measurements."""
    require(type(receipt) is dict and receipt.get('schema') ==
            'antfly.attention-matching.combined-resource-guard.v1' and
            receipt.get('rss_scope') == 'campaign_parent_plus_active_child_pgid',
            'wrong combined model resource scope')
    require(receipt.get('pass') is True and type(receipt.get('returncode')) is int and
            receipt['returncode'] == 0 and receipt.get('violation') is None and
            'cleanup_error' not in receipt, 'failed model resource guard')
    for key in ('campaign_parent_pid', 'child_pgid'):
        require(type(receipt.get(key)) is int and receipt[key] > 0, 'missing guard process identity')
    limits = receipt['limits']
    for key in ('timeout_seconds', 'max_rss_mib', 'min_free_percent', 'max_swap_growth_mib', 'min_disk_free_mib'):
        require(type(limits.get(key)) in (int, float) and math.isfinite(limits[key]), 'invalid resource limit')
    require(0 < limits['max_rss_mib'] <= 8192 and limits['min_free_percent'] >= 15 and
            limits['max_swap_growth_mib'] == 0 and limits['min_disk_free_mib'] >= 4096 and
            limits['timeout_seconds'] > 0, 'model resource limits were weakened')
    for key in ('elapsed_seconds', 'min_free_percent', 'min_disk_free_mib', 'swapout_growth_mib',
                'peak_combined_rss_mib', 'peak_parent_rss_mib', 'peak_child_group_rss_mib'):
        require(type(receipt.get(key)) in (int, float) and math.isfinite(receipt[key]) and receipt[key] >= 0,
                'invalid measured resource value')
    require(receipt['elapsed_seconds'] <= limits['timeout_seconds'] and
            receipt['min_free_percent'] >= limits['min_free_percent'] and
            receipt['min_disk_free_mib'] >= limits['min_disk_free_mib'] and receipt['swapout_growth_mib'] == 0 and
            0 < receipt['peak_combined_rss_mib'] <= limits['max_rss_mib'], 'model resource measurements failed')
    require(type(receipt.get('remaining_child_group_live_processes')) is int and
            receipt['remaining_child_group_live_processes'] == 0 and
            type(receipt.get('remaining_child_group_rss_mib')) in (int, float) and
            receipt['remaining_child_group_rss_mib'] == 0, 'model child group did not drain')
    rows = receipt['rss_samples']
    require(type(rows) is list and rows and type(receipt.get('samples')) is int and
            receipt['samples'] == len(rows), 'incomplete combined RSS samples')
    peaks = {key: 0. for key in ('parent_rss_mib', 'child_group_rss_mib', 'combined_rss_mib')}
    prior = -1.
    for row in rows:
        for key in (*peaks, 'elapsed_seconds'):
            require(type(row.get(key)) in (int, float) and math.isfinite(row[key]) and row[key] >= 0,
                    'invalid combined RSS sample')
        require(prior <= row['elapsed_seconds'] and row['parent_rss_mib'] > 0 and
                math.isclose(row['combined_rss_mib'], row['parent_rss_mib'] + row['child_group_rss_mib'],
                             rel_tol=0, abs_tol=1e-9) and row['combined_rss_mib'] <= limits['max_rss_mib'],
                'combined RSS samples do not reconcile')
        prior = row['elapsed_seconds']
        for key in peaks:
            peaks[key] = max(peaks[key], row[key])
    require(all(peaks[key] == receipt['peak_' + key] for key in peaks), 'forged combined RSS peak')
    return {'peak_combined_rss_mib': receipt['peak_combined_rss_mib'],
            'elapsed_seconds': receipt['elapsed_seconds'], 'samples': len(rows), 'swapout_growth_mib': 0}


def performance_contract(report, expected, worker_log):
    """Audit authenticated public performance observations for one release cell.

    The caller binds the producer job, environment, request, build, artifacts,
    outer combined guard and probe. This function recomputes the owner ledgers,
    balanced schedule, token coverage, confidence intervals and fixed gates.
    """
    import qualify_metal_native_checkpoint as campaign
    import qualify_metal_native_checkpoint_performance as performance
    from metal_checkpoint_qualification_ledger import generation_contract
    model, encoding, context = expected['model'], expected['encoding'], expected['context_tokens']
    require(model in MODELS and encoding in ENCODINGS and type(context) is int and context in (8192, 32768),
            'unknown performance cell')
    require(type(report) is dict and report.get('schema') == performance.SCHEMA and
            report.get('backend') == 'metal' and report.get('server_kind') == 'metal-test' and
            report.get('qualification_profile') == 'metal_test_native_checkpoint_v1' and
            report.get('execution_mode') == 'eager' and report.get('production_ready') is False,
            'wrong public performance evidence scope')
    require(report.get('source') == expected['source'] and
            report.get('binary_sha256') == digest(expected['binary']['sha256'], 'binary') and
            report.get('request') == expected['request'] and
            report.get('encoding') == encoding and report.get('declared_context_tokens') == context,
            'performance source, executable, request or cell changed')
    require(type(report.get('warmups')) is int and report['warmups'] == 5 and
            type(report.get('pairs_requested')) is int and report['pairs_requested'] == 30 and
            report.get('qualification_sized') is True and report.get('preparation_reused') is False,
            'diagnostic or undersized performance evidence')
    request_model = MODELS[model][0]
    campaign.validate_metal_resources(report)
    campaign.validate_loaded_model(report, {'request_model': request_model, 'loaded_identity': expected['loaded_identity']})
    worker = report['worker']
    require(worker.get('command') == [expected['binary']['path'], '--test-filter',
                                     'context Metal native checkpoint qualification server'], 'different performance worker')
    cell = report['release_cell']
    output = 64 if context == 8192 else 256
    prompt = context - output
    require(cell == {'declared_context_tokens': context, 'reserved_output_tokens': output,
                     'rendered_prompt_tokens': prompt, 'context_cell_matched': True,
                     'output_reservation_filled': True}, 'changed frozen performance workload')
    exact, candidate = report['exact_control'], report['physical_ledger']
    require(exact.get('encoding') == 'f16' and exact.get('logical_tokens') == prompt and
            exact.get('validated_heads') == 0 and exact.get('global_savings_ratio') == 1 and
            candidate.get('encoding') == encoding and candidate.get('logical_tokens') == prompt and
            candidate.get('outcome') == 'compacted' and type(candidate.get('validated_heads')) is int and
            candidate['validated_heads'] > 0 and type(candidate.get('global_savings_ratio')) in (int, float) and
            math.isfinite(candidate['global_savings_ratio']) and candidate['global_savings_ratio'] >= 1.5,
            'unmatched checkpoint preparation')
    pairs = report['observations']
    schedule = list(performance.paired_schedule(30))
    require(type(pairs) is list and len(pairs) == 30 and
            all(type(pair.get('index')) is int and pair['index'] == index and
                pair.get('order') == list(schedule[index]) for index, pair in enumerate(pairs)),
            'unbalanced or incomplete measured triplets')
    attempts = worker['performance_observations']
    modes = ['full_replay'] + ['ordinary', 'exact', 'compacted'] * 5 + [mode for order in schedule for mode in order]
    require(type(attempts) is list and len(attempts) == len(modes), 'missing raw warmup or measured attempts')
    require(type(worker_log) is bytes and worker_log, 'missing immutable performance worker log')
    hashes, owners = set(), set()
    for index, (attempt, mode) in enumerate(zip(attempts, modes)):
        require(type(attempt.get('attempt_index')) is int and attempt['attempt_index'] == index and
                attempt.get('mode') == mode and attempt.get('validated') is True and
                'validation_error' not in attempt, 'failed or reordered raw performance attempt')
        require_binding = mode in ('exact', 'compacted')
        route = 'checkpoint_resume' if require_binding else 'exact'
        require(attempt.get('route') == route, 'performance route changed')
        representation = encoding if mode == 'compacted' else 'f16' if require_binding else None
        rows = attempt['ledger_rows']
        begin = attempt.get('log_offset')
        end = attempts[index + 1]['log_offset'] if index + 1 < len(attempts) else len(worker_log)
        require(type(begin) is int and type(end) is int and 0 <= begin < end <= len(worker_log) and
                (begin == 0 or worker_log[begin - 1:begin] == b'\n'), 'invalid performance log interval')
        parsed_rows = []
        for line in worker_log[begin:end].splitlines():
            marker = line.find(b'metal_checkpoint_qualification ')
            if marker >= 0:
                parsed_rows.append(decode_json(line[marker + len(b'metal_checkpoint_qualification '):]))
        require(parsed_rows == rows, 'raw performance ledgers differ from worker log')
        if require_binding:
            # The legacy parser supports older boundary-less records. Native
            # v2 release evidence always requires its explicit current proof.
            require(all(row.get('kv_boundary_proven') is True for row in rows
                        if row.get('phase') in ('resumed_ready', 'generation_committed')),
                    'native performance lacks materialized-token proof')
        ledger = generation_contract(rows, route, require_binding=require_binding,
                                     expected_representation=representation, expected_model=request_model,
                                     expected_encoded_heads=candidate.get('rotated4_heads') if mode == 'compacted' else None,
                                     expected_encoded_rows=candidate.get('rotated4_compressed_rows') if mode == 'compacted' else None)
        require(ledger == attempt['owner_ledger'], 'forged derived owner ledger')
        release = ledger['release']
        require(all(type(release.get(key)) is int and release[key] >= 0 for key in
                    ('initial_live_bytes', 'committed_live_bytes', 'final_live_bytes',
                     'initial_peak_bytes', 'committed_peak_bytes', 'final_peak_bytes')) and
                release['final_live_bytes'] == release['initial_live_bytes'] and
                release['committed_live_bytes'] == ledger['tracked_combined_live_bytes'] and
                release['committed_peak_bytes'] >= release['committed_live_bytes'] and
                release['final_peak_bytes'] >= release['committed_peak_bytes'],
                'incomplete current public-owner release measurement')
        owner = ledger['committed']['owner_id']
        require(owner not in owners, 'performance owner replayed')
        owners.add(owner)
        if require_binding:
            require(ledger['committed_context_bytes'] >= ledger['committed']['binding_allocated_bytes'],
                    'context savings omit resident checkpoint backing')
        observed = attempt['observation']
        require(observed.get('prompt_tokens') == prompt and observed.get('completion_tokens') == output and
                observed.get('cached_prompt_tokens') == (prompt if require_binding else 0) and
                observed.get('finish_reason') == 'length', 'incomplete or unmatched performance token accounting')
        hashes.add(digest(observed.get('content_sha256'), 'greedy output'))
        for key in ('decode_ms', 'ttft_ms', 'e2e_ms'):
            require(type(observed.get(key)) in (int, float) and math.isfinite(observed[key]) and observed[key] > 0,
                    'invalid observed public timing')
        require(observed['e2e_ms'] >= observed['ttft_ms'] + observed['decode_ms'] - 1e-6,
                'inconsistent public phase clocks')
        if index >= 16:
            pair = pairs[(index - 16) // 3]
            require(pair[mode] == {**observed, 'owner_ledger': ledger}, 'measured timing differs from raw attempt')
    require(len(hashes) == 1, 'public greedy outputs differ')
    transformations = {
        'ttft_ratio': lambda pair: pair['compacted']['ttft_ms'] / pair['exact']['ttft_ms'],
        'ordinary_decode_ratio': lambda pair: pair['ordinary']['decode_ms'] / pair['compacted']['decode_ms'],
        'exact_decode_ratio': lambda pair: pair['exact']['decode_ms'] / pair['compacted']['decode_ms'],
        'committed_context_savings': lambda pair: 1 - pair['compacted']['owner_ledger']['committed_context_bytes'] /
                                                 pair['exact']['owner_ledger']['committed_context_bytes'],
    }
    # Preserve the matched collector's frozen statistical procedure and seeds.
    seeds = {'ordinary_decode_ratio': 'ordinary-decode-throughput',
             'exact_decode_ratio': 'decode-throughput', 'committed_context_savings': 'committed-savings'}
    confidence = {key: performance.paired_transform_interval(pairs, transformations[key], f'{encoding}:{context}:{seed}')
                  for key, seed in seeds.items()}
    confidence['ttft_ratio'] = performance.paired_ratio_interval(pairs, 'ttft_ms', f'{encoding}:{context}:ttft_ms')
    passed = (confidence['ttft_ratio']['upper_95'] <= 1.25 and
              confidence['ordinary_decode_ratio']['lower_95'] >= .90 and
              confidence['exact_decode_ratio']['lower_95'] >= .90 and
              confidence['committed_context_savings']['lower_95'] >= .25)
    return {'schema': 'antfly.native-whole-head.public-performance-audit.v2', 'performance_gate_passed': passed,
            'model': model, 'encoding': encoding, 'context_tokens': context, 'confidence': confidence,
            'measured_triplets': 30, 'untimed_warmups_per_route': 5, 'full_public_owners_released': len(owners),
            'quality_gate_passed': False, 'production_ready': False, 'release_authority': False}
