#!/usr/bin/env python3
"""Create private configuration/protocol artifacts; never launch a model."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import struct

ROOT = Path(__file__).resolve().parent
REPO = Path('/Users/timkaye/Documents/af/antfly')
HISTORICAL = Path('/private/tmp/antfly-gliner25-training-inactive-published-small-v1')
MIB = 1024**2
GIB = 1024**3


def read(path, maximum):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        initial = os.fstat(fd)
        if not stat.S_ISREG(initial.st_mode) or not 0 < initial.st_size <= maximum:
            raise ValueError(f'not a bounded regular file: {path}')
        with os.fdopen(fd, 'rb') as stream:
            fd = None
            raw = stream.read(maximum + 1)
            final = os.fstat(stream.fileno())
        if len(raw) != initial.st_size or (initial.st_size, initial.st_mtime_ns, initial.st_ctime_ns) != (final.st_size, final.st_mtime_ns, final.st_ctime_ns):
            raise ValueError(f'file changed: {path}')
        return raw
    finally:
        if fd is not None:
            os.close(fd)


def pin(raw):
    return {'size_bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}


def save(name, value):
    raw = (json.dumps(value, indent=2, allow_nan=False) + '\n').encode()
    with (ROOT / name).open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return pin(raw)


def main():
    old_raw = read(HISTORICAL / 'preparation.json', 64 * 1024)
    old = json.loads(old_raw)
    assert pin(old_raw)['sha256'] == 'e49ca907daf093303dd3919b56ab61f36983eb9d9174e3d3ccd7993ac21e83b9'
    rows = read(Path(old['data']['path']), MIB)
    assert pin(rows) == {key: old['data'][key] for key in ('size_bytes', 'sha256')}
    source = Path(old['source_dir'])
    for name, expected in zip(old['source_file_names'][1:], old['source']['sidecars']):
        assert pin(read(source / name, 16 * MIB)) == expected
    # Only the immutable file header is read here, never the weight payload.
    fd = os.open(source / 'model.safetensors', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        assert stat.S_ISREG(before.st_mode) and before.st_size == old['source']['weight']['size_bytes']
        prefix = os.pread(fd, 8, 0)
        count, = struct.unpack('<Q', prefix)
        assert 0 < count <= 4 * MIB
        raw_header = os.pread(fd, count, 8)
        after = os.fstat(fd)
        assert len(raw_header) == count and (before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (after.st_size, after.st_mtime_ns, after.st_ctime_ns)
    finally:
        os.close(fd)
    header = json.loads(raw_header)
    assert len([name for name in header if name != '__metadata__']) == 334
    modules = []
    for name, item in sorted(header.items()):
        if name == '__metadata__' or not name.endswith('.weight') or len(item['shape']) != 2:
            continue
        module = name[:-len('.weight')]
        out_dim, in_dim = item['shape']
        bias = header.get(module + '.bias')
        if bias is None or bias['shape'] != [out_dim]:
            continue
        family = module.split('.', 1)[0]
        encoder = family == 'encoder' and any(pattern in module.rsplit('.', 1)[-1] for pattern in ('query', 'key', 'value', 'dense'))
        if not encoder and family not in ('boundary_head', 'classifier', 'record_decoder', 'relation_scorer'):
            continue
        assert item['dtype'] == bias['dtype'] == 'F32'
        modules.append({'module': module, 'base_weight': name, 'in_dim': in_dim, 'out_dim': out_dim, 'family': family})
    assert len(modules) == 131 and sum(m['family'] == 'encoder' for m in modules) == 72
    prior_layout = Path('/private/tmp/antfly-gliner25-training-runtime-artifact-v1/lora-all-host128-backend384-uninterrupted/model/adapter_config.json')
    prior_layout_raw = read(prior_layout, 128 * 1024)
    assert json.loads(prior_layout_raw)['target_modules'] == [m['module'] for m in modules]
    inventory = {'format': 'antfly.gliner25-regional-all-small-inventory/v1', 'backbone': 'small', 'source': old['source'], 'source_weight_header': pin(raw_header), 'source_parameter_count': 334, 'targets': ['encoder', 'all_task_heads'], 'module_count': 131, 'modules': modules, 'modes': {}}
    disk = {}
    for mode in ('lora', 'dora'):
        slots = []
        for module in modules:
            prefix = 'base_model.model.' + module['module']
            for component, shape, saved in (
                ('lora_A.default.weight', [2, module['in_dim']], 'lora_A.weight'),
                ('lora_B.default.weight', [module['out_dim'], 2], 'lora_B.weight'),
                *((('lora_magnitude_vector.default.weight', [module['out_dim']], 'lora_magnitude_vector'),) if mode == 'dora' else ()),
            ):
                slots.append({'name': prefix + '.' + component, 'saved_key': prefix + '.' + saved, 'shape': shape, 'family': module['family'], 'group': 0})
        payload = sum(math.prod(slot['shape']) * 4 for slot in slots)
        header_bound = 71 + sum(256 + len(slot['shape']) * 24 + len(slot['saved_key'].encode()) * 2 for slot in slots)
        # Exact job checkpoint formula and conservative export formula. The
        # export preflight uses actual Zig config JSON bytes; here the declared
        # maximum is substituted only to derive a safe upper bound.
        checkpoint = 8 * MIB + 4 * payload
        export_bound = payload + header_bound + 8 + 2 * 64 * 1024 + 128 * 1024 + 64 * 1024
        assert export_bound <= 4 * MIB and payload < 16 * MIB - 512 * 1024
        inventory['modes'][mode] = {'slot_count': len(slots), 'parameter_payload_bytes': payload, 'slots': slots}
        disk[mode] = {'checkpoint_job_upper_bound_bytes': checkpoint, 'adapter_tensor_payload_bytes': payload, 'export_header_upper_bound_bytes': header_bound, 'export_upper_bound_using_declared_config_cap_bytes': export_bound, 'per_invocation_atomic_job_disk_reservation_bytes': 2 * checkpoint + export_bound, 'three_phase_persistent_checkpoint_and_export_upper_bound_bytes': 3 * checkpoint + 2 * export_bound}
    inventory_pin = save('inventory.json', inventory)
    configs = {}
    base_pins = {}
    for mode in ('lora', 'dora'):
        original = read(HISTORICAL / f'{mode}-paused.json', 64 * 1024)
        base_pins[f'{mode}-paused.json'] = pin(original)
        base = json.loads(original)
        assert base['run']['seed'] == 257713 and base['run']['epochs'] == 1 and base['peft']['alpha'] == 3 and base['peft']['rank'] == 2
        base['attention_profile'] = 'replay_tiled_v1'
        base['activation_profile'] = 'layer_recompute_v1'
        base['peft']['targets'] = ['encoder', 'all_task_heads']
        base['memory'] = {'host_bytes': 768 * MIB, 'backend_bytes': GIB, 'backend_metadata_bytes': 64 * MIB, 'combined_bytes': 3 * GIB, 'optimizer_state_bytes': 32 * MIB, 'optimizer_transaction_bytes': 32 * MIB, 'job_bytes': 32 * MIB}
        base['export_limits'] = {'max_scratch_bytes': 16 * MIB, 'max_output_bytes': 4 * MIB, 'max_adapter_bytes': 2 * MIB, 'max_header_bytes': MIB, 'max_config_bytes': 128 * 1024, 'max_receipt_bytes': 64 * 1024, 'max_slots': 512}
        base['training_limits'] = {
            'version': 1,
            'max_recomputed_batch_scratch_bytes': 64 * MIB,
            'step': {
                'max_step_host_bytes': 128 * MIB,
                'max_total_host_bytes': 768 * MIB,
                'recomputation': {'max_regions': 13, 'max_plan_host_bytes': 128 * MIB, 'max_compile_bytes': 512 * MIB, 'max_checkpoint_bytes': 64 * MIB, 'max_gradient_bytes': 64 * MIB, 'max_backend_bytes': GIB, 'max_host_bytes': 2 * GIB},
                'replay': {'max_host_bytes': 16 * MIB, 'max_mask_bytes': 16 * MIB},
            },
        }
        for phase in ('paused', 'resumed', 'uninterrupted'):
            value = copy.deepcopy(base)
            value['output_dir'] = str(ROOT / f'{mode}-{phase}')
            name = f'{mode}-{phase}.json'
            if phase == 'resumed':
                value['resume_from'] = str(ROOT / f'{mode}-paused/latest.safetensors')
                value['expected_restore_state_sha256'] = 'RESOLVE_FROM_VERIFIED_PAUSED_RESULT_BEFORE_EXECUTION'
                name = f'{mode}-resumed.template.json'
            configs[name] = save(name, value)
    source_files = sum(item['size_bytes'] for item in [old['source']['weight'], *old['source']['sidecars']])
    outer_other = 32 * MIB + 4 * MIB + 768 * MIB + GIB
    persisted = sum(item['three_phase_persistent_checkpoint_and_export_upper_bound_bytes'] for item in disk.values())
    logging = 6 * (4 * MIB + 4 * MIB + MIB + 512 * 1024)
    transient = max(item['checkpoint_job_upper_bound_bytes'] for item in disk.values())
    disk_growth = persisted + logging + transient + MIB
    assert disk_growth < 160 * MIB
    source_locations = {
        'job': 'zig/pkg/inference/src/finetune/gliner_boundary_training_job.zig',
        'source': 'zig/pkg/inference/src/finetune/gliner_boundary_training_source.zig',
        'native_trainer': 'zig/pkg/inference/src/finetune/gliner_boundary_native_trainer.zig',
        'layout': 'zig/pkg/inference/src/finetune/gliner_boundary_adapter_layout.zig',
        'peft': 'zig/pkg/inference/src/finetune/gliner_boundary_peft_graph.zig',
        'adapter': 'zig/pkg/inference/src/finetune/gliner_boundary_adapter.zig',
        'export': 'zig/pkg/inference/src/finetune/gliner_boundary_training_export.zig',
        'controller': 'zig/pkg/inference/src/finetune/seeded_gradient_trainer.zig',
        'regional': 'zig/pkg/inference/src/graph/recomputed_training.zig',
        'wire_limits': 'zig/pkg/inference/src/finetune/gliner_boundary_training_limits.zig',
    }
    resource = {
        'format': 'antfly.gliner25-regional-all-small-resource-plan/v1',
        'model_execution': False, 'actual_graph_admission_measured': False,
        'census_basis': 'Exact pinned source header dimensions and supported matching-bias Linear inventory, independently equal to historical published all131 export targets. No source weight payload copied or loaded.',
        'source_file_bytes': source_files,
        'source_reservation_formula': 'sum(five file sizes) + max_auxiliary_bytes + sizeof(Source) + 3; calculated again by production before source load',
        'source_reservation_historical_observed_bytes': 438134789,
        'source_reservation_historical_scope': 'Original classifier CPU run.json; not a current sizeof(Source) ABI claim.',
        'source_reservation_current_upper_bound_bytes': 512 * MIB,
        'outer_job_native_admission_formula': 'source_reserved + job32MiB + dataset4MiB + host768MiB + backend1024MiB; all host for native',
        'outer_job_admission_with_historical_source_bytes': 438134789 + outer_other,
        'outer_job_admission_using_source_cap_upper_bound_bytes': 512 * MIB + outer_other,
        'combined_cap_bytes': 3 * GIB,
        'outer_sampled_child_tree_rss_cap_bytes': 4 * GIB,
        'rss_distinction': 'Sampled process RSS is not allocator admission; it can count shared pages twice and miss inter-sample peaks. Production live physical-memory reserve and all owners remain enabled.',
        'regional_host_reserved_floor_bytes': (128 + 128 + 64) * MIB,
        'regional_host_formula': 'Source + live parent H minus live regional allocations + full128MiB regional owner + compiled runtime metadata + Step128MiB + caller64MiB + head runtime/bindings + pending optimizer transaction; must fit source_reserved+host768MiB and clamped regional max_host.',
        'regional_backend_formula': 'existing backend allocations +64MiB metadata + selected binding copies + fixed inputs + retained layer boundaries + all gradient accumulators + largest live layer replay/tape/workspace + head tape/outputs/merge copies; <=1GiB, before first forward',
        'regional_geometry_at_declared_B1_S512': {'encoder_layers': 12, 'hidden_size': 384, 'intermediate_size': 1536, 'attention_heads': 6, 'relative_rows': 512, 'encoder_checkpoint_payload_bytes': (13 * 512 * 384 + 512 * 384) * 4, 'one_layer_hidden_relative_dropout_payload_bytes': (3 * 512 * 384) * 4, 'attention_four_sweep_score_elements_all_layers': 4 * 12 * 6 * 512 * 512, 'note': 'Static payload/work components only; not full graph admission, measured peak, FLOPs, latency, or actual dataset token length.'},
        'dora_risk': 'Replay attention removes S-squared materialization, not dense adapted-weight direction/delta/norm intermediates. Exact local program geometry and all overlap remain admitted.',
        'disk': {'modes': disk, 'all_six_phase_persistent_checkpoint_and_export_upper_bound_bytes': persisted, 'all_six_phase_log_receipt_allowance_bytes': logging, 'max_atomic_checkpoint_replacement_overlap_bytes': transient, 'private_directory_misc_allowance_bytes': MIB, 'all_six_phase_growth_upper_bound_bytes': disk_growth, 'required_free_headroom_bytes': 256 * MIB, 'minimum_initial_free_for_entire_plan_bytes': disk_growth + 256 * MIB, 'source_and_original_model_copy_bytes': 0, 'materialized_model_export_bytes': 0, 'note': 'Adapter-only outputs. Existing artifacts remain untouched. Recheck free bytes before each phase; preserve denied/failed outputs and never retry in the same directory.'},
        'code_formula_pins': {key: {'path': value, **pin(read(REPO / value, 2 * MIB))} for key, value in source_locations.items()},
    }
    resource_pin = save('resource_plan.json', resource)
    proposal = {
        'format': 'antfly.gliner25-regional-all-small-native-qualification-proposal/v1',
        'execution_authorized_by_this_file': False,
        'sequence': ['lora paused after1', 'offline validate LoRA pause', 'lora fresh-owner resume', 'lora uninterrupted', 'offline exact LoRA continuity', 'dora paused after1', 'offline validate DoRA pause', 'dora fresh-owner resume', 'dora uninterrupted', 'offline exact DoRA continuity'],
        'binary_binding': 'Require new production non-test public runtime artifact path, exact streamed size/SHA256, and root build receipt. Reject all historical executables; compare executable digest reported by consumed run.json and pre/post hashes. No binary is selected by this preparation.',
        'argv_template': ['NEW_BINARY', 'finetune', 'train', 'gliner25', str(ROOT / 'lora-paused.json'), '--shutdown-grace-seconds', '30', '--stop-after-microbatches', '1'],
        'supervisor': {'dependency': str(HISTORICAL / 'supervision.py'), 'dependency_pin': pin(read(HISTORICAL / 'supervision.py', MIB)), 'python': '/private/tmp/antfly-gliner25-oracle-venv/bin/python', 'psutil': '7.1.3', 'torch_imported': False, 'sampled_tree_rss_limit_bytes': 4 * GIB, 'rss_poll_seconds': 0.05, 'process_identity_limit': 64, 'worker_timeout_seconds': 1800, 'outer_timeout_seconds': 1890, 'stdout_limit_bytes': 4 * MIB, 'stderr_limit_bytes': 4 * MIB, 'parent_grace_seconds': 35, 'kill_wait_seconds': 10, 'worker_parent_loss_grace_seconds': 2, 'lifetime': 'Existing tested creation-identity registration, sampling, failure receipt and bounded cleanup/reap. Require clean child-tree receipt on success; never signal historical PIDs.'},
        'pre_and_post_identity': ['exact raw preparation/inventory/config bytes', 'unchanged authored five-row JSONL', 'all five source files, streamed and same-descriptor regular-file checks', 'new executable and build receipt', 'exact helper bytes', 'paused checkpoint/config/result pins on resume'],
        'strict_input_handling': 'O_NOFOLLOW|O_NONBLOCK, fstat regular files, bounded headers/JSON/depth/payload ranges, duplicate-key rejection, finite values; no untrusted artifact path following.',
        'configuration_validation': 'Exact semantic/config subsets using Zig declared f32 IEEE roundtrip only; all integer resource fields exact, no blanket numerical tolerance. Both new profile strings required in consumed Config. Caps never silently raised.',
        'progress_expectations': {'microbatches': 5, 'optimizer_updates': 3, 'flush_after': [2, 4, 5], 'window_sizes': [2, 2, 1], 'zero_loss_fallback': [False] * 5, 'classification_present': [True, False, True, False, False], 'objective': 'Every real microbatch optimizer.loss equals positive raw terms.total. Classification term positive on rows0/2 and zero otherwise. Final partial-flush gradient norm need not be zero.', 'gold_coverage': 'Exactly one gold mention retained per row; no gold relations/records.', 'first_pause': 'identity0updates/1micro, one accumulated microbatch, no exported model'},
        'checkpoint_validation': {'max_file_bytes': 16 * MIB, 'max_header_bytes': 2 * MIB, 'payload_chunk_bytes': MIB, 'inventory': 'All262/393 ordered exact names/logical shapes, flat F32 optimizer tensors, moments, accumulators and exact counter limbs. Reconstruct Controller contract/state SHA256 independently from raw little-endian payload.', 'presence': 'paused presence bits are exact0/1 and immutable on restore; final bits and gradients cleared. Never infer absence from numeric zero.', 'known_slot_counters_at_final': {'encoder': 3, 'classifier': 2, 'record_decoder': 3, 'relation_scorer': 3}, 'other_boundary_head_slots': 'Keep exact per-slot counters/presence in receipt, validate range0..3 and equality across resume/uninterrupted. Some dormant boundary/proposer paths may legitimately remain None; do not assert all131 modules had gradients.'},
        'export_validation': 'Exactly four adapter files. Exact131 target module list/rank2/alpha3/dropout0/DoRA flag; all262/393 saved keys/shapes/F32/finite values. Bind source/run/data/schema/layout and digest receipts. No merged/full-weight export.',
        'continuity_validation': 'Same new binary and runtime profile; exact semantic progress/decision fingerprints stitched pause+resume vs uninterrupted; byte-exact final result/checkpoint/all four export files. Compare owner peaks separately, not as semantic equality.',
        'bounded_checker_proposal': 'Stream checkpoint headers/payload in <=1MiB chunks; metadata<=2MiB, file<=16MiB. No full tensor-to-Python-float lists, no model/ML imports. First implement fake-fixture rejection tests plus use unchanged supervised child lifetime regressions, then root runs the single LoRA pause.',
        'not_claimed': ['published PyTorch RNG stream equality', 'published PyTorch loss/VJP/update parity', 'CPU vs Metal update bytes', 'long-context qualification', 'quality/convergence', 'performance', 'release gate approval'],
    }
    proposal_pin = save('qualification_plan.json', proposal)
    prep = {'format': 'antfly.gliner25-regional-all-small-native-preparation/v1', 'model_execution': False, 'qualification': False, 'historical_preparation': {'path': str(HISTORICAL / 'preparation.json'), **pin(old_raw)}, 'historical_config_pins': base_pins, 'historical_all131_export_target_inventory': {'path': str(prior_layout), **pin(prior_layout_raw)}, 'source': old['source'], 'source_dir': old['source_dir'], 'source_file_names': old['source_file_names'], 'source_preparation_validation': 'Four exact sidecar hashes, weight size and bounded header only. Full weight SHA is enforced by prospective driver and production Source; no current payload verification or model execution claim.', 'data': old['data'], 'semantic_changes': {'peft_targets': ['encoder', 'all_task_heads'], 'attention_profile': 'replay_tiled_v1', 'activation_profile': 'layer_recompute_v1'}, 'preserved': ['source identity', 'exact train.jsonl bytes/path', 'all run optimizer/scheduler/seed/epoch/batch/accumulation fields', 'PEFT rank/alpha/dropout', 'gold schedule', 'capacities and task weights', 'tokenization bounds and word splitter', 'published source dropout0.1', 'production constructor/inventory', 'global live resource guard'], 'expected_microbatches': 5, 'expected_updates': 3, 'fallback_sequence': [False] * 5, 'inventory': inventory_pin, 'resource_plan': resource_pin, 'qualification_plan': proposal_pin, 'configs': configs, 'status': 'prepared only; checker implementation/current executable binding/first serial phase pending'}
    save('preparation.json', prep)
    print(json.dumps({'root': str(ROOT), 'configs': len(configs), 'inventory': inventory_pin, 'resource': resource_pin, 'disk_growth_upper_bound_bytes': disk_growth, 'minimum_initial_free_bytes': disk_growth + 256 * MIB, 'admission_using_source_cap_upper_bound_bytes': 512 * MIB + outer_other, 'model_execution': False}))


if __name__ == '__main__':
    main()
