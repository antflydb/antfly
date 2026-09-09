"""Summarize completed scale arms with medians and paired candidate/control ratios."""

import argparse
import json
from pathlib import Path
import statistics

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('root', type=Path)
args = parser.parse_args()
receipts = json.loads((args.root / 'ab-runs.json').read_text())


def valid_receipt(receipt):
    return (receipt.get('exit_code') == 0 and not receipt.get('invalid_reason')
            and receipt.get('resource_sampler_exit_code', 0) == 0)


def resource_totals(path):
    if not path.exists():
        return {}
    lifetimes = {}
    peak_footprint = 0
    for line in path.read_text().splitlines():
        row = json.loads(line)
        peak_footprint = max(peak_footprint, row.get('phys_footprint_bytes', 0),
                             row.get('lifetime_max_phys_footprint_bytes', 0))
        key = (row['pid'], row['start_abstime'])
        current = lifetimes.setdefault(key, {})
        for name in ('disk_read_bytes', 'disk_written_bytes', 'logical_written_bytes'):
            if name in row:
                current[name] = max(current.get(name, 0), row[name])
    # Per-process cumulative counters already include work before the first
    # sample. Maxima summed across restart lifetimes are sampled lower bounds;
    # physical I/O also depends on OS writeback and is distinct from logical I/O.
    return {'process_sampled_' + name: sum(row.get(name, 0) for row in lifetimes.values())
            for name in ('disk_read_bytes', 'disk_written_bytes', 'logical_written_bytes')} | {
                'process_peak_phys_footprint_bytes': peak_footprint}


def phase_resource_peaks(path, phases_path):
    if not path.exists() or not phases_path.exists():
        return {}
    phases = {row['phase']: row['wall_time'] for row in
              (json.loads(line) for line in phases_path.read_text().splitlines())}
    samples = [json.loads(line) for line in path.read_text().splitlines()]
    result = {}
    for label, begin, end in [('mixed', 'mixed_profile_start', 'mixed_profile_end'),
                              ('read_only', 'public_profile_start', 'public_profile_end')]:
        if begin not in phases or end not in phases:
            continue
        selected = [row for row in samples if phases[begin] <= row['wall_time_s'] <= phases[end]]
        if not selected or len({(row['pid'], row['start_abstime']) for row in selected}) != 1:
            continue
        # A phase peak must use instantaneous demand, not the process's lifetime
        # high-water mark, which can belong to a different workload phase.
        for key in ('rss_bytes', 'phys_footprint_bytes'):
            if all(key in row for row in selected):
                result[label + '_sampled_' + key] = max(row[key] for row in selected)
        result[label + '_resource_samples'] = len(selected)
    return result


def churn_resource_envelope(path, phases_path):
    if not path.exists() or not phases_path.exists():
        return {}
    phases = {row['phase']: row['wall_time'] for row in
              (json.loads(line) for line in phases_path.read_text().splitlines())}
    if 'source_churn_begin' not in phases or 'source_churn_end' not in phases:
        return {}
    start, end = phases['source_churn_begin'], phases['source_churn_end']
    samples = [json.loads(line) for line in path.read_text().splitlines()]
    before = next((row for row in reversed(samples) if row['wall_time_s'] <= start), None)
    after = next((row for row in samples if row['wall_time_s'] >= end), None)
    if (before is None or after is None or
            (before['pid'], before['start_abstime']) != (after['pid'], after['start_abstime'])):
        return {}
    result = {'churn_envelope_start_margin_s': start - before['wall_time_s'],
              'churn_envelope_end_margin_s': after['wall_time_s'] - end}
    for key in ('logical_written_bytes', 'disk_written_bytes', 'disk_read_bytes'):
        if key in before and key in after and after[key] >= before[key]:
            result['churn_envelope_process_' + key] = after[key] - before[key]
    return result


def churn_source_envelope(phases):
    if not phases:
        return {}
    before = phases[0].get('table_before', {}).get('storage_status', {}).get('source_vectors') or {}
    after = phases[-1].get('table', {}).get('storage_status', {}).get('source_vectors') or {}
    # Endpoints can remain valid when an intermediate best-effort probe is
    # missing. A restarted/fallback owner must not charge ingest to churn.
    minimum = sum(p['rows'] for p in phases if p['phase'].startswith('update-'))
    maximum = sum(p['rows'] for p in phases)
    delta = after.get('prepared_payloads', -maximum) - before.get('prepared_payloads', maximum)
    invalid = {'churn_envelope_source_counters_valid': False}
    if not minimum <= delta <= maximum:
        return invalid
    result = {}
    for key in ('outer_db_batch_lock_wait_ns', 'prepare_lock_wait_ns', 'collection_mark_ns',
                'collection_plan_ns', 'collection_mark_steps', 'collection_mark_rows',
                'directory_bytes_written', 'directory_publications', 'directory_publication_deferrals',
                'wal_bytes_written', 'checkpoint_bytes_read', 'checkpoint_bytes_written',
                'collection_bytes_read', 'collection_bytes_written'):
        if key not in before or key not in after or after[key] < before[key]:
            return invalid
        result['churn_envelope_source_' + key] = after[key] - before[key]
    for key in ('collection_mark_budget_yields', 'collection_mark_busy_deferrals',
                'collection_mark_outside_lock_ns', 'collection_mark_merge_ns',
                    'collection_locked_ns', 'collection_setup_ns', 'collection_copy_ns',
                    'collection_publish_ns', 'collection_active_scan_turns',
                    'collection_active_scan_pause_ns', 'collection_rescued_payloads',
                    'deduplicated_reappend_payloads', 'deduplicated_reappend_bytes'):
        result['churn_envelope_source_' + key] = (
            after[key] - before[key] if key in before and key in after and after[key] >= before[key] else None)
    return result | {'churn_envelope_source_counters_valid': True}


def churn_counters(phases):
    result = {}
    keys = ('prepare_lock_wait_ns', 'directory_bytes_written', 'wal_bytes_written',
            'checkpoint_bytes_written', 'checkpoint_bytes_read', 'collection_bytes_written',
            'collection_bytes_read', 'collection_mark_ns', 'collection_plan_ns', 'snapshot_read_ns',
            'outer_db_batch_lock_wait_ns', 'collection_mark_steps', 'collection_mark_rows',
            'directory_publications', 'directory_publication_deferrals',
            'ownership_index_entries_scanned', 'checkpoint_receipt_bytes_written')
    for phase in phases:
        before = phase.get('table_before', {}).get('storage_status', {}).get('source_vectors')
        after = phase.get('table', {}).get('storage_status', {}).get('source_vectors')
        if before is None or after is None:
            return {'fixed_churn_source_counter_samples_valid': False}
        # A best-effort status probe may fall back to a read-only handle with
        # zero volatile counters. Do not turn such an observation change into
        # a negative delta or charge initial ingest to one small churn phase.
        prepared_delta = after.get('prepared_payloads', 0) - before.get('prepared_payloads', 0)
        if not 0 <= prepared_delta <= phase['rows']:
            return {'fixed_churn_source_counter_samples_valid': False}
        if phase['phase'].startswith('update-') and prepared_delta != phase['rows']:
            return {'fixed_churn_source_counter_samples_valid': False}
        for key in keys:
            delta = after.get(key, 0) - before.get(key, 0)
            if delta < 0:
                return {'fixed_churn_source_counter_samples_valid': False}
            result[key] = result.get(key, 0) + delta
        for key in ('collection_mark_budget_yields', 'collection_mark_busy_deferrals',
                    'collection_mark_outside_lock_ns', 'collection_mark_merge_ns',
                    'collection_locked_ns', 'collection_setup_ns', 'collection_copy_ns',
                    'collection_publish_ns', 'collection_active_scan_turns',
                    'collection_active_scan_pause_ns', 'collection_rescued_payloads',
                    'deduplicated_reappend_payloads', 'deduplicated_reappend_bytes'):
            previous = result.get(key, 0)
            result[key] = (previous + after[key] - before[key]
                           if previous is not None and key in before and key in after and after[key] >= before[key]
                           else None)
    return {'fixed_churn_' + key: value for key, value in result.items()} | {
        'fixed_churn_source_counter_samples_valid': True}


for case in sorted({r['case'] for r in receipts}):
    case_receipts = [r for r in receipts if r['case'] == case]
    arms = []
    for receipt in receipts:
        if receipt['case'] != case or not valid_receipt(receipt):
            continue
        root = args.root / f"{case}-{receipt['pair']}-{receipt['mode']}"
        load = lambda name: json.loads((root / name).read_text())
        summary = load('qualification-summary.json')
        live = next(r for r in summary['runs'] if r['label'].endswith('online-live'))
        cold = next(r for r in summary['runs'] if r['label'].endswith('reopened-cold'))
        warm = next(r for r in summary['runs'] if r['label'].endswith('reopened-warm'))
        mixed = summary['public_mixed_profiles']['public-mixed-profile']
        profile = summary['public_query_profiles']['public-query-profile']
        disk = load('disk-after-restart.json')
        table = load('table-after-restart.json')
        source = table.get('storage_status', {}).get('source_vectors') or {}
        source_before = load('table-before-restart.json').get('storage_status', {}).get('source_vectors') or {}
        churn = load('source-churn.json')
        arm = {'pair': receipt['pair'], 'mode': receipt['mode'],
            'ready_seconds': live['ready_seconds'], 'peak_qps': live['max_qps'],
            'c1_qps': live['concurrent_qps'][0], 'c30_qps': live['concurrent_qps'][-1],
            'c1_p99_ms': live['concurrent_latency_ms']['p99'][0],
            'c30_p99_ms': live['concurrent_latency_ms']['p99'][-1],
            'recall': live['recall'], 'cold_recall': cold['recall'], 'warm_recall': warm['recall'],
            'cold_p99_ms': cold['serial_latency_ms']['p99'],
            'warm_p99_ms': warm['serial_latency_ms']['p99'],
            'disk_bytes': disk['total_logical_bytes'], 'allocated_bytes': disk['total_allocated_bytes'],
            'source_disk_bytes': sum(group['logical_bytes'] for name, group in disk['groups'].items()
                                     if name.startswith('source_vectors.')),
            'read_only_rss_bytes': summary['phase_rss_profiles']['read_only']['peak_rss_bytes'],
            'mixed_rss_bytes': summary['phase_rss_profiles']['mixed']['peak_rss_bytes'],
            'mixed_qps': mixed['query_qps'], 'mixed_p99_ms': mixed['query_latency']['p99_ms'],
            'write_rows_s': mixed['write_rows_per_second'], 'catchup_seconds': mixed['catchup_seconds'],
            'profile_p99_ms': profile['p99_ms'], 'profile_recall': profile['recall'],
            'errors': mixed['errors'], 'source_after_restart': source,
            'source_before_restart': source_before,
            # These are source-store counters, not total database write I/O.
            # Primary ownership rows and journals remain in the other receipts.
            'source_reported_write_bytes_before_restart': sum(source_before.get(k, 0) for k in (
                'wal_bytes_written', 'checkpoint_bytes_written',
                'collection_bytes_written', 'directory_bytes_written')) if source_before else None,
            'fixed_churn_seconds': sum(phase['elapsed_s'] for phase in churn),
            'fixed_churn_rows': sum(phase['rows'] for phase in churn),
            **{'fixed_churn_' + key: sum(phase[key] for phase in churn)
               for key in ('mutation_s', 'index_sync_s', 'status_read_s')
               if all(key in phase for phase in churn)},
            'fixed_churn': churn,
            'source_segments_before_restart': source_before.get('source_segments'),
            'source_prepare_lock_wait_ns': source_before.get('prepare_lock_wait_ns'),
            'source_directory_bytes_written': source_before.get('directory_bytes_written'),
            'source_location_cache_bytes': source_before.get('location_cache_bytes'),
            **churn_counters(churn),
            **churn_source_envelope(churn),
            **churn_resource_envelope(args.root / (root.name + '-resources.jsonl'), root / 'phases.jsonl'),
            **phase_resource_peaks(args.root / (root.name + '-resources.jsonl'), root / 'phases.jsonl'),
            **resource_totals(args.root / (root.name + '-resources.jsonl'))}
        arms.append(arm)
    if not arms:
        continue
    # Optional best-effort counters can be present with null values. Keep that
    # absence in each arm and exclude the metric from paired aggregation.
    numeric = [k for k in arms[0] if k != 'pair' and all(
        isinstance(arm.get(k), (float, int)) and not isinstance(arm.get(k), bool)
        for arm in arms)]
    modes = sorted({a['mode'] for a in arms})
    medians = {m: {k: statistics.median(a[k] for a in arms if a['mode'] == m) for k in numeric} for m in modes}
    ratios = []
    for pair in sorted({a['pair'] for a in arms}):
        by_mode = {a['mode']: a for a in arms if a['pair'] == pair}
        if {'control', 'candidate'} <= by_mode.keys():
            ratios.append({'pair': pair, **{k: by_mode['candidate'][k] / by_mode['control'][k]
                if by_mode['control'][k] else None for k in numeric}})
    result = {'case': case, 'completed_arms': len(arms),
        'qualified': len(arms) >= 4 and len(arms) == len(case_receipts),
        'failed_or_incomplete_arms': [
            {'pair': r['pair'], 'mode': r['mode'], 'exit_code': r.get('exit_code'),
             'invalid_reason': r.get('invalid_reason')}
            for r in case_receipts if not valid_receipt(r)],
        'complete_pairs': len(ratios),
        'arms': arms, 'medians': medians,
        'paired_ratios': ratios, 'median_paired_ratios': {k: statistics.median(r[k] for r in ratios if r[k] is not None)
            for k in numeric if any(r[k] is not None for r in ratios)}}
    path = args.root / (case + '-comparison.json')
    path.write_text(json.dumps(result, indent=2) + '\n')
    print(case, len(arms), 'qualified=' + str(result['qualified']), json.dumps(result['median_paired_ratios']))
