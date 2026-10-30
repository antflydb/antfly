# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Independent private-owner quality observations, without release authority.

A caller must authenticate the binary, loaded artifact, job journal, source,
configuration, and input files before using this contract. These checks never
replace public serving, physical savings, failure, performance, or soak gates.
"""
import hashlib
import json
import math
import struct
import unicodedata


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def digest(value, label):
    require(isinstance(value, list) and len(value) == 32 and
            all(type(x) is int and 0 <= x <= 255 for x in value) and any(value),
            'invalid ' + label)
    return bytes(value)


def integer(value, label, minimum=0):
    require(type(value) is int and minimum <= value <= 2**64 - 1, 'invalid ' + label)
    return value


def number(value, label, minimum=0):
    require(type(value) in (int, float) and math.isfinite(value) and value >= minimum,
            'invalid ' + label)
    return value


def h(domain, values):
    return hashlib.sha256(domain.encode() + b''.join(struct.pack('<Q', x) for x in values)).digest()


EXECUTION_FIELDS = ['forward_frame', 'precomputed_tail', 'retain_hidden', 'matrix_stage',
                    'coalesced_stage', 'ple_tail', 'resource_barriers', 'concurrent_frame']


def configuration_contract(frozen, expected):
    execution, observed = frozen['execution'], frozen['observed']
    require(type(execution) is dict and set(execution) == set(EXECUTION_FIELDS) and
            all(type(execution[key]) is bool for key in EXECUTION_FIELDS) and
            execution == expected['execution'] and observed == expected['observed'],
            'destination or execution differs from the pinned configuration')
    require(execution['forward_frame'] and execution['precomputed_tail'] and execution['retain_hidden'] and
            (not execution['concurrent_frame'] or execution['resource_barriers']) and
            (not execution['ple_tail'] or execution['precomputed_tail']), 'unsupported native execution configuration')
    destination = observed['destination']
    require(set(destination) == {'device_registry_id', 'physical_memory_bytes', 'device_name_sha256',
                                'compiler_identity_sha256'}, 'invalid destination binding')
    integer(destination['device_registry_id'], 'device registry id', 1)
    integer(destination['physical_memory_bytes'], 'physical memory', 1)
    digest(destination['device_name_sha256'], 'device name')
    digest(destination['compiler_identity_sha256'], 'compiler and OS identity')
    environment = digest(observed['backend_environment_sha256'], 'backend environment')
    execution_sha = hashlib.sha256(b'antfly.native-whole-head.execution.v2\n' +
                                   bytes(execution[key] for key in EXECUTION_FIELDS)).digest()
    configuration_sha = hashlib.sha256(b'antfly.native-whole-head.configuration.v2\n' +
                                       execution_sha + environment).digest()
    require(digest(frozen['configuration_sha256'], 'configuration') == configuration_sha,
            'configuration digest mismatch')


def calibration_contract(seal, consumers, expected, prompt_count):
    require(consumers == expected, 'shared consumer coverage changed')
    require(seal['source_length'] == prompt_count and seal['repeat_length'] == 512 and
            seal['train_positions'] == 256 and seal['heldout_positions'] == 64 and
            seal['consumer_count'] == len(expected) and
            seal['batch_count'] == sum(c['query_heads'] for c in expected), 'calibration shape changed')
    width = sum(c['query_heads'] * c['dim'] for c in expected)
    require(seal['train_elements'] == width * 256 and seal['heldout_elements'] == width * 64,
            'calibration element count changed')
    geometry = [prompt_count, 512] + [c[k] for c in consumers for k in
                ['layer', 'donor', 'query_heads', 'kv_heads', 'dim']]
    require(digest(seal['geometry_sha256'], 'calibration geometry') ==
            h('antfly.native-calibration-geometry.v1\n', geometry), 'geometry digest mismatch')
    positions = [prompt_count + i * 511 // 319 for i in range(320)]
    require(digest(seal['train_positions_sha256'], 'training positions') ==
            h('antfly.native-calibration-train-positions.v1\n', [x for i, x in enumerate(positions) if i % 5]),
            'training position digest mismatch')
    require(digest(seal['heldout_positions_sha256'], 'held-out positions') ==
            h('antfly.native-calibration-heldout-positions.v1\n', [x for i, x in enumerate(positions) if i % 5 == 0]),
            'held-out position digest mismatch')
    fields = ['source_length', 'repeat_length', 'consumer_count', 'batch_count',
              'train_positions', 'heldout_positions', 'train_elements', 'heldout_elements']
    raw = b'antfly.native-calibration-seal.v1\n' + b''.join(
        struct.pack('<Q', integer(seal[k], k)) for k in fields)
    for key in ['geometry_sha256', 'train_positions_sha256', 'heldout_positions_sha256',
                'train_values_sha256', 'heldout_values_sha256']:
        raw += digest(seal[key], key)
    require(digest(seal['sha256'], 'calibration seal') == hashlib.sha256(raw).digest(),
            'calibration seal mismatch')


def release_contract(value, owner_id):
    require(value['released'] is True and type(value['post_release_live_bytes']) is int and
            value['post_release_live_bytes'] == 0 and
            value['context_allocation_scope_complete'] is False and
            value['full_owner_physical_available'] is False, 'private release scope or leak')
    integer(owner_id, 'owner id', 1)
    snapshots = [value[key] for key in ['initial', 'committed', 'final', 'allocation']]
    for snapshot in snapshots:
        require(snapshot['available'] is True and snapshot['physical_available'] is True and
                snapshot['tracked_combined_physical_available'] is True and
                integer(snapshot['owner_identity'], 'snapshot owner', 1) == owner_id,
                'allocation snapshot has a different owner or unavailable backing')
        for field in ['logical', 'physical', 'shared_backing', 'private_device', 'tracked_combined']:
            live = integer(snapshot[field + '_live_bytes'], field + ' live')
            peak = integer(snapshot[field + '_peak_bytes'], field + ' peak')
            require(peak >= live, 'allocation peak below live bytes')
        require(snapshot['tracked_combined_live_bytes'] == sum(snapshot[field + '_live_bytes']
                for field in ['physical', 'shared_backing', 'private_device']),
                'allocation components do not sum')
    initial, committed, final, allocation = snapshots
    require(all(initial[field + '_live_bytes'] == final[field + '_live_bytes'] for field in
                ['logical', 'physical', 'shared_backing', 'private_device', 'tracked_combined']) and
            final == allocation, 'private owner did not return to its baseline')
    expected = {
        'available': True,
        'committed_live_bytes': committed['tracked_combined_live_bytes'] - initial['tracked_combined_live_bytes'],
        'request_peak_bytes': max(committed['tracked_combined_peak_bytes'], final['tracked_combined_peak_bytes']) - initial['tracked_combined_live_bytes'],
        'released_baseline_bytes': final['tracked_combined_live_bytes'] - initial['tracked_combined_live_bytes'],
    }
    m = value['measurement']
    require(m == expected and m['available'] is True and
            integer(m['committed_live_bytes'], 'committed', 1) > 0 and
            integer(m['request_peak_bytes'], 'peak') >= m['committed_live_bytes'] and
            type(m['released_baseline_bytes']) is int and m['released_baseline_bytes'] == 0,
            'private measurement does not reconcile with owner snapshots')


def route_contract(row, compacted, layers, ordinary_prepared=False):
    for key in ['forward_steps', 'prepared_forward_steps', 'serving_forward_steps',
                'cold_staged_forward_steps', 'prepared_layer_blocks']:
        integer(row[key], key)
    require(row['forward_steps'] == row['count'] - 1, 'forward step count mismatch')
    if compacted:
        require(row['serving_forward_steps'] == row['forward_steps'] and
                row['prepared_forward_steps'] + row['cold_staged_forward_steps'] == row['forward_steps'] and
                row['cold_staged_forward_steps'] <= 1 and
                row['prepared_layer_blocks'] == layers * row['prepared_forward_steps'],
                'changed serving cold-to-prepared route')
    else:
        require(all(row[k] == 0 for k in ['prepared_forward_steps', 'serving_forward_steps',
                                         'cold_staged_forward_steps', 'prepared_layer_blocks']),
                'unproven ordinary-clone prepared route')
    ordinary_steps = integer(row['ordinary_serving_forward_steps'], 'ordinary serving steps')
    ordinary_frames = integer(row['ordinary_backend_frames'], 'ordinary backend frames')
    require(ordinary_steps == ordinary_frames == (row['forward_steps'] if ordinary_prepared else 0),
            'ordinary serving route or backend frame count mismatch')
    release_contract(row['release'], row['owner_id'])


def tokens(raw, count, label):
    require(type(count) is int and count > 0 and len(raw) == count * 8, 'invalid ' + label)
    ids = list(struct.unpack('<' + 'q' * count, raw))
    require(all(0 <= x < 2**31 for x in ids), 'invalid token id in ' + label)
    return ids


def case_contract(report, case, inputs, expected):
    """Check one already authenticated observation against its sealed inputs.

    inputs contains exact manifest bytes and the five file-role byte strings.
    expected supplies pinned model identity, implementation, layer geometry,
    manifest case index and sampling metadata; no backend pass bit is trusted.
    """
    require(report['schema'] == 'antfly.native-checkpoint-quality.case.v2' and
            report['allocation_scope'] == 'private_native_kv_owners' and
            report['full_serving_owner_qualified'] is False and report['release_eligible'] is False and
            report['collection_complete'] is True, 'invalid observation scope')
    require(report['case'] == case and integer(report['case_index'], 'case index') == expected['case_index'] and
            integer(report['load_generation'], 'load generation', 1) > 0 and
            digest(report['manifest_sha256'], 'manifest') == hashlib.sha256(inputs['manifest']).digest(),
            'changed manifest or case')
    manifest = json.loads(inputs['manifest'])
    require(type(manifest) is dict and manifest.get('schema') == 'antfly.native-checkpoint-quality.input.v1' and
            type(manifest.get('cases')) is list and report['case_index'] < len(manifest['cases']) and
            manifest['cases'][report['case_index']] == case and
            manifest['native_identity'] == expected['native_identity'],
            'case differs from sealed manifest')
    for field in ['messages', 'rendered_prompt', 'prompt_tokens', 'reference_utf8', 'reference_tokens']:
        require(hashlib.sha256(inputs[field]).hexdigest() == case[field]['sha256'], 'changed input: ' + field)
    output_limit = integer(expected.get('output_tokens', 256), 'output limit', 2)
    require(output_limit <= 256, 'output limit exceeds frozen capacity')
    ordinary_route = expected.get('ordinary_greedy_route', 'generic')
    require(ordinary_route in ('generic', 'backend_owned'), 'unknown ordinary baseline route')
    prompt_count = case['prompt_count']
    require(type(prompt_count) is int and prompt_count in (7936, 32512) and
            case['request'] == {'encoding': expected['encoding'], 'output_tokens': output_limit,
                                'minimum_savings_ratio': 1.5}, 'changed native request')
    tokens(inputs['prompt_tokens'], prompt_count, 'prompt')
    reference = tokens(inputs['reference_tokens'], case['reference_count'], 'reference')
    require(2 <= len(reference) <= output_limit, 'invalid reference continuation')
    reference_sha = hashlib.sha256(b''.join(struct.pack('<I', x) for x in reference)).digest()
    require(case['reference_sequence_sha256'] == reference_sha.hex() and
            digest(report['reference_sha256'], 'reference sequence') == reference_sha,
            'reference sequence changed')
    f = report['frozen']
    configuration_contract(f, expected)
    require(type(report.get('source_serving_prefill', False)) is bool and
            report.get('source_serving_prefill', False) == expected.get('source_serving_prefill', False),
            'source prefill route differs from the pinned campaign')
    require(type(f['identity']) is dict and set(f['identity']) == {'weights', 'tokenizer', 'template', 'layout'} and
            all(digest(f['identity'][key], key) == digest(expected['native_identity'][key], 'pinned ' + key) for key in f['identity']) and
            digest(f['implementation_sha256'], 'implementation') == bytes.fromhex(expected['implementation_sha256']) and
            digest(f['prompt_sha256'], 'prompt tokens') == hashlib.sha256(inputs['prompt_tokens']).digest(),
            'model, implementation or prompt binding mismatch')
    require(digest(f['checkpoint_sha256'], 'checkpoint') == digest(f['installed_sha256'], 'installed checkpoint'),
            'installed checkpoint changed')
    digest(f['source_checkpoint_sha256'], 'source checkpoint')
    require(integer(f['source_revision'], 'source revision') < 2**64 - 1 and
            integer(f['destination_revision_before_install'], 'destination revision') < 2**64 - 1 and
            integer(f['installed_revision'], 'installed revision', 1) == f['destination_revision_before_install'] + 1 and
            f['logical_tokens'] == prompt_count and f['protected_prefix_rows'] == 128 and
            f['protected_recent_rows'] == 2048 and integer(f['completed_chunks'], 'chunks', 1) > 0,
            'publication boundary or protected policy changed')
    calibration_contract(f['calibration'], report['calibration_consumers'], expected['global_consumers'], prompt_count)
    owner_heads = {(c['donor'], head) for c in expected['global_consumers'] for head in range(c['kv_heads'])}
    require(integer(f['validated_heads'], 'validated heads', 1) +
            integer(f['preserved_heads'], 'preserved heads') == len(owner_heads),
            'global owner validation coverage incomplete')
    fp8, rotated = integer(f['fp8_heads'], 'FP8 heads'), integer(f['rotated4_heads'], 'rotated heads')
    require(fp8 <= len(owner_heads) and rotated <= len(owner_heads) and
            ((expected['encoding'] == 'f16' and fp8 == rotated == 0) or
             (expected['encoding'] == 'fp8' and fp8 > 0 and rotated == 0) or
             (expected['encoding'] == 'rotated4' and rotated > 0 and fp8 == 0)),
            'actual resident encoding coverage missing')
    fp8_fallback = integer(f['fp8_fallback_heads'], 'FP8 fallback heads')
    rotated_fallback = integer(f['rotated4_fallback_heads'], 'rotated fallback heads')
    require(fp8 + rotated <= f['validated_heads'] and
            ((expected['encoding'] == 'f16' and fp8_fallback == rotated_fallback == 0) or
             (expected['encoding'] == 'fp8' and fp8 + fp8_fallback == f['validated_heads'] and rotated_fallback == 0) or
             (expected['encoding'] == 'rotated4' and rotated + rotated_fallback == f['validated_heads'] and fp8_fallback == 0)),
            'resident codec and fallback coverage do not reconcile')
    require(integer(report['reference_provider_calls'], 'reference provider calls') == 1 and
            report['reference_revealed_after_both_greedy_branches'] is True and
            report['source_unchanged'] is True and report['query_scope_released'] is True and
            report['query_scope_delta_bytes'] == 0, 'opaque-reference or immutable-source contract failed')
    require(report['model_slots_prepared_without_kv_allocation'] is True and
            report['model_slot_runtime_kv_bytes_before'] == report['model_slot_runtime_kv_bytes_after'],
            'model-slot preparation changed ordinary KV')
    for phase in ['compacted_greedy', 'compacted_teacher']:
        require(report[phase + '_runtime_kv_bytes_before'] == report[phase + '_runtime_kv_bytes_after'],
                'compacted branch changed ordinary KV')
    require(report['continuation_runtime_kv_bytes'] == report['compacted_teacher_runtime_kv_bytes_after'],
            'final runtime KV accounting mismatch')
    require(integer(report['admitted_peak_bytes'], 'admitted peak') <= 8 * 1024**3 and
            number(report['max_head_mass_error'], 'mass error') <= .05 and
            number(report['max_head_output_rmse'], 'output error') <= .05 and
            integer(report['source_global_bytes'], 'source global', 1) >
            integer(report['candidate_global_bytes'], 'candidate global', 1), 'unchanged fit gates failed')
    owners = [report['source_owner_id'], report['compacted_owner_id']]
    release_contract(report['source_release'], report['source_owner_id'])
    release_contract(report['compacted_release'], report['compacted_owner_id'])
    for name in ['exact_greedy', 'compacted_greedy', 'exact_teacher', 'compacted_teacher']:
        row = report[name]; owners.append(integer(row['owner_id'], 'owner id', 1))
        require(type(row['count']) is int and 1 <= row['count'] <= 256, 'invalid branch count')
        route_contract(row, name.startswith('compacted'), expected['layers'],
                       ordinary_prepared=name == 'exact_greedy' and ordinary_route == 'backend_owned')
        if name.endswith('teacher'):
            require(row['count'] == len(reference), 'incomplete teacher continuation')
            probabilities = row['log_probabilities']
            require(len(probabilities) == 256 and all(type(x) in (int, float) and math.isfinite(x) and x <= 0 for x in probabilities[:row['count']]) and
                    all(x == 0 for x in probabilities[row['count']:]), 'invalid raw log probabilities')
            require(math.isclose(-math.fsum(probabilities[:row['count']]), row['nll'], rel_tol=1e-12, abs_tol=1e-12),
                    'NLL does not sum observed probabilities')
        else:
            require(type(row['hit_eos']) is bool and (row['hit_eos'] or row['count'] == output_limit) and
                    isinstance(row['public_text'], str) and len(row['tokens']) == 256 and
                    all(type(x) is int and 0 <= x < 2**31 for x in row['tokens'][:row['count']]) and
                    all(x == 0 for x in row['tokens'][row['count']:]), 'incomplete greedy answer')
    require(len(set(owners)) == 6 and all(type(x) is int and x > 0 for x in owners), 'branches share an owner')
    require(report['exact_teacher']['log_probabilities'][0] == report['compacted_teacher']['log_probabilities'][0],
            'first prompt-bound score changed')
    ct = report['compacted_teacher']
    require(ct['attention_calls'] == ct['forward_steps'] * expected['layers'] and
            ct['donor_appends'] == ct['forward_steps'] * expected['donors'], 'teacher skipped installed KV')
    durations = report['phase_durations_ns']
    require(set(durations) == {'exact_prefill', 'query_collection', 'fit', 'transport', 'install',
            'model_slots', 'exact_greedy', 'compacted_greedy', 'reference_reveal', 'exact_teacher',
            'compacted_teacher', 'teardown'} and all(type(x) is int and x > 0 for x in durations.values()) and
            report['collection_elapsed_ns'] >= sum(durations.values()), 'invalid parent phase timing')
    normalize = lambda s: unicodedata.normalize('NFC', s).strip()
    gold = normalize(inputs['reference_utf8'].decode('utf-8', errors='strict'))
    return {'id': case['id'], 'blueprint_id': expected['blueprint_id'], 'primary_family': expected['primary_family'],
            'bucket': expected['bucket'], 'context_tokens': prompt_count + output_limit,
            'exact_correct': normalize(report['exact_greedy']['public_text']) == gold,
            'candidate_correct': normalize(report['compacted_greedy']['public_text']) == gold,
            'scored_tokens': len(reference), 'exact_nll_sum': report['exact_teacher']['nll'],
            'candidate_nll_sum': ct['nll'], 'full_serving_owner_qualified': False, 'release_authority': False}
