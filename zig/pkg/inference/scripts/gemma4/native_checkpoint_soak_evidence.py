# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Independently reconcile native persistent-owner activity and parent clocks."""
import hashlib
import math

from native_whole_head_campaign import MODELS, combined_resource_contract, require, _finite_json
from native_whole_head_quality import configuration_contract, release_contract
from release_attention_matching import decode_json

FORMAL_SECONDS = 4 * 60 * 60
STEPS_PER_BATCH = 64


def integer(value, label, minimum=0):
    require(type(value) is int and value >= minimum, 'invalid soak ' + label)
    return value


def digest_array(value):
    require(type(value) is list and len(value) == 32 and
            all(type(byte) is int and 0 <= byte <= 255 for byte in value) and any(value),
            'invalid soak digest')
    return bytes(value).hex()


def activity(value, owner, prefix, live):
    require(type(value) is dict and set(value) == {'owner_id', 'revision', 'logical_tokens', 'attention_calls',
                                                'donor_appends', 'live_bytes', 'prepared_layer_blocks'},
            'incomplete soak activity')
    for key in value:
        integer(value[key], key, 1 if key in ('owner_id', 'revision', 'live_bytes') else 0)
    require(value['owner_id'] == owner and value['logical_tokens'] == prefix and value['live_bytes'] == live,
            'soak owner, logical prefix or live backing changed')


def audit(report, journal, resource, expected, formal=True):
    """Caller authenticates source, binary, model, configuration, input and producer refs."""
    _finite_json(report)
    combined_resource_contract(resource)
    model = expected['model']
    require(model in MODELS and type(formal) is bool and type(journal) is bytes,
            'invalid persistent soak audit input')
    model_name, layers, donors, _, _ = MODELS[model]
    require(report.get('schema') == 'antfly.native-checkpoint-soak.report.v2' and report.get('model') == model_name and
            report.get('encoding') == expected['encoding'] and report.get('model_directory') == expected['model_directory'] and
            digest_array(report.get('input_sha256')) == expected['input_sha256'] and
            digest_array(report.get('main_gguf_sha256')) == expected['artifact_sha256'] and
            digest_array(report.get('implementation_sha256')) == expected['implementation_sha256'] and
            report.get('model_identity') == expected['native_identity'], 'soak runtime or model binding changed')
    configuration_contract(report, expected['configuration'])
    generation = integer(report.get('load_generation'), 'model generation', 1)
    raw = report['report']
    require(raw.get('collection_complete') is True and raw.get('production_ready') is False and
            raw.get('release_authority') is False and raw.get('load_generation') == generation,
            'incomplete soak or model reload')
    duration = integer(expected['duration_seconds'], 'requested duration', 1)
    require(duration <= FORMAL_SECONDS and type(raw.get('diagnostic_only')) is bool and
            raw['diagnostic_only'] == (duration < FORMAL_SECONDS), 'changed soak duration scope')
    active = integer(raw.get('active_ns'), 'active duration', 1)
    elapsed = integer(raw.get('window_elapsed_ns'), 'window duration', 1)
    require(active >= duration * 10**9 and active <= elapsed <= resource['elapsed_seconds'] * 10**9 and
            active / elapsed >= .9, 'soak window duration or active fraction failed')
    require(not formal or (duration == FORMAL_SECONDS and not raw['diagnostic_only'] and
                           active / (resource['elapsed_seconds'] * 10**9) >= .9),
            'four-hour active soak or independent parent-clock fraction failed')
    require(digest_array(raw.get('journal_sha256')) == hashlib.sha256(journal).hexdigest() and
            journal and journal.endswith(b'\n'), 'soak activity journal changed')
    require(len(raw['initial']) == len(raw['final']) == len(raw['output_tokens']) == len(raw['release']) == 2,
            'missing persistent owner')
    initial = raw['initial']
    owners = [integer(row['owner_id'], 'owner', 1) for row in initial]
    require(owners[0] != owners[1], 'same soak owner substituted for both contexts')
    prefixes = [integer(row['logical_tokens'], 'logical prefix', 1) for row in initial]
    require(prefixes == expected['prefix_tokens'] and prefixes[0] in (7936, 8128) and prefixes[1] == 32512,
            'soak omitted 8K or 32K context')
    for value, owner, prefix in zip(initial, owners, prefixes):
        activity(value, owner, prefix, value['live_bytes'])
    require(type(raw['output_tokens']) is list and all(type(token) is int and token >= 0 for token in raw['output_tokens']),
            'invalid soak output token')
    previous, summed_active, previous_elapsed, count = initial, 0, 0, 0
    for index, line in enumerate(journal.splitlines(keepends=True)):
        row = decode_json(line)
        _finite_json(row)
        require(type(row) is dict and set(row) == {'index', 'steps', 'start', 'finish', 'active_ns',
                                                 'elapsed_ns', 'output_tokens'} and
                type(row['index']) is int and row['index'] == index and row['steps'] == [32, 32] and
                all(type(step) is int for step in row['steps']) and row['start'] == previous and
                row['output_tokens'] == raw['output_tokens'], 'incomplete, reordered or replayed soak batch')
        batch_active = integer(row['active_ns'], 'batch activity', 1)
        batch_elapsed = integer(row['elapsed_ns'], 'batch clock', 1)
        summed_active += batch_active
        require(batch_elapsed > previous_elapsed and batch_active <= batch_elapsed - previous_elapsed and
                summed_active <= batch_elapsed <= elapsed, 'inconsistent soak activity clocks')
        require(type(row['finish']) is list and len(row['finish']) == 2, 'missing soak owner batch')
        for i, (before, after) in enumerate(zip(previous, row['finish'])):
            activity(after, owners[i], prefixes[i], initial[i]['live_bytes'])
            require(after['revision'] > before['revision'] and
                    after['attention_calls'] == before['attention_calls'] + 32 * layers and
                    after['donor_appends'] == before['donor_appends'] + 32 * donors and
                    after['prepared_layer_blocks'] == before['prepared_layer_blocks'] + 32 * layers,
                    'soak batch skipped actual attention, donor append or prepared layer')
        previous = row['finish']
        previous_elapsed = batch_elapsed
        count += 1
    require(raw['final'] == previous and integer(raw['batches'], 'batch count', 1) == count and
            integer(raw['forward_steps'], 'forward count', 1) == count * STEPS_PER_BATCH and
            summed_active == active, 'incomplete soak journal totals')
    require(raw.get('prefix_sha256_before') == raw.get('prefix_sha256_after') and
            [digest_array(value) for value in raw['prefix_sha256_before']] == expected['checkpoint_sha256'],
            'persistent soak prefix was mutated')
    require(integer(raw.get('runtime_kv_bytes_before'), 'runtime KV before') ==
            integer(raw.get('runtime_kv_bytes_after'), 'runtime KV after'), 'soak grew ordinary runtime KV')
    for receipt, owner in zip(raw['release'], owners):
        release_contract(receipt, owner)
    return {'schema': 'antfly.native-checkpoint-soak.audit.v2', 'raw_activity_audited': True,
            'persistent_owner_soak_qualified': formal, 'production_ready': False, 'release_authority': False,
            'model': model, 'encoding': expected['encoding'], 'persistent_owners': owners,
            'forward_steps': count * STEPS_PER_BATCH, 'active_seconds': active / 10**9,
            'parent_elapsed_seconds': resource['elapsed_seconds'],
            'parent_active_fraction': active / (resource['elapsed_seconds'] * 10**9),
            'post_release_live_bytes': [receipt['post_release_live_bytes'] for receipt in raw['release']]}
