"""Small synthetic parser, route, identity and publication regressions only."""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest import mock

import checker as c
import run_phase as runner


SLOTS = [
    {'name': 'base_model.model.encoder.encoder.layer.0.attention.self.query_proj.lora_A.default.weight', 'shape': [2, 2], 'family': 'encoder'},
    {'name': 'base_model.model.classifier.0.lora_B.default.weight', 'shape': [2, 1], 'family': 'classifier'},
    {'name': 'base_model.model.boundary_head.candidate_encoder.lora_A.default.weight', 'shape': [1, 2], 'family': 'boundary_head'},
    {'name': 'base_model.model.record_decoder.projection.lora_B.default.weight', 'shape': [1, 2], 'family': 'record_decoder'},
]


def limbs(value, bits):
    return [(value >> (bits*i)) & ((1 << bits)-1) for i in range(4)]


def payloads(paused=True):
    values = {
        '__trainer_counters': (limbs(1 if paused else 5, 16)+limbs(0 if paused else 3, 16), [8]),
        '__run_fingerprint': (list(range(32)), [32]),
        '__extension.seeded.counters': ([1, int(paused), 2], [3]),
        '__extension.seeded.presence': ([1, 1, 0, 1] if paused else [0]*4, [4]),
    }
    counts = [0]*4 if paused else [3, 2, 0, 3]
    for i, (slot, step) in enumerate(zip(SLOTS, counts)):
        name = slot['name']
        count = 1
        for dimension in slot['shape']:
            count *= dimension
        values['weight::'+name] = ([0.25*(i+1)]*count, [count])
        values['adam_m::'+name] = ([0 if step == 0 else 0.125]*count, [count])
        values['adam_v::'+name] = ([0 if step == 0 else 0.0625]*count, [count])
        values['adam_step::'+name] = ([step], [1])
        values['adam_step_u32::'+name] = (limbs(step, 8), [4])
        # Record is present-zero; dormant is absent-zero. This distinction
        # must survive even though their accumulator payload bytes agree.
        values['__extension.seeded.gradient.'+str(i)] = ([0.125 if paused and i == 0 else 0]*count, [count])
    return values


def tensor_file(path, values, mutate_header=None, suffix=b''):
    header = {}
    body = bytearray()
    for key, (data, shape) in values.items():
        raw = struct.pack('<'+'f'*len(data), *data)
        start = len(body); body.extend(raw)
        header[key] = {'dtype': 'F32', 'shape': shape, 'data_offsets': [start, len(body)]}
    if mutate_header:
        mutate_header(header)
    raw_header = json.dumps(header, separators=(',', ':')).encode()
    raw_header += b' '*((-len(raw_header)) % 8)
    path.write_bytes(struct.pack('<Q', len(raw_header))+raw_header+body+suffix)


class CheckerTests(unittest.TestCase):
    def test_strict_json_and_declared_numeric_identity(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}', b'{"x":18446744073709551616}', b'['*33+b'0'+b']'*33):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                c.loads(raw)
        actual, = struct.unpack('<f', struct.pack('<f', 0.01))
        c.config_subset({'run': {'weight_decay': 0.01}}, {'run': {'weight_decay': actual}})
        with self.assertRaises(ValueError):
            c.config_subset({'run': {'weight_decay': 0.01}}, {'run': {'weight_decay': actual+1e-12}})
        for changed in (True, 13.0, '13', 14):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                c.config_subset({'training_limits': {'step': {'recomputation': {'max_regions': 13}}}}, {'training_limits': {'step': {'recomputation': {'max_regions': changed}}}})

    def test_exact_large_counter_limbs_and_boolean_rejection(self):
        value = 2**48+2**24+3
        self.assertEqual(value, c.decode_limbs([float(x) for x in limbs(value, 16)], 16))
        self.assertEqual(2**24+3, c.decode_limbs([float(x) for x in limbs(2**24+3, 8)], 8))
        for bad in ([True, 0., 0., 0.], [0.5, 0., 0., 0.], [-1., 0., 0., 0.], [256., 0., 0., 0.]):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                c.decode_limbs(bad, 8)
        with self.assertRaises(ValueError):
            c.validate_identity({'optimizer_step': True, 'microbatch_step': 5}, {'optimizer_step': 1, 'microbatch_step': 5})

    def test_chunked_parser_ranges_dtypes_truncation_and_file_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'tensor.safetensors'
            values = {'tensor': ([0.25]*1024, [1024])}
            tensor_file(path, values)
            with mock.patch.object(c, 'CHUNK', 64), c.TensorFile(path) as f:
                self.assertEqual(hashlib.sha256(struct.pack('<1024f', *([0.25]*1024))).hexdigest(), f.scan('tensor'))
                self.assertLessEqual(f.max_read_bytes, 64)
                self.assertEqual(c.digest(path), f.digest())
            mutations = (
                lambda h: h['tensor'].update(dtype='I32'),
                lambda h: h['tensor'].update(shape=[True]),
                lambda h: h['tensor'].update(data_offsets=[4, 4100]),
                lambda h: h['tensor'].update(shape=[1023]),
                lambda h: h.update(extra={'dtype': 'F32', 'shape': [1024], 'data_offsets': [0, 4096]}),
            )
            for mutate in mutations:
                tensor_file(path, values, mutate)
                with self.assertRaises(ValueError):
                    c.TensorFile(path)
            tensor_file(path, values, suffix=b'x')
            with self.assertRaises(ValueError):
                c.TensorFile(path)
            tensor_file(path, values)
            with self.assertRaises(ValueError):
                with c.TensorFile(path) as f:
                    with path.open('r+b') as stream:
                        stream.seek(-4, 2); stream.write(struct.pack('<f', 0.5))
                    f.scan('tensor')
            path.write_bytes(struct.pack('<Q', 1000)+b'{}')
            with self.assertRaises(ValueError):
                c.TensorFile(path)

    def test_metadata_failure_closes_exact_owned_descriptor(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'bad'
            path.write_bytes(struct.pack('<Q', 2000)+b'{}')
            original = c.regular
            seen = []
            def capture(*args, **kwargs):
                fd, info = original(*args, **kwargs); seen.append(fd); return fd, info
            with mock.patch.object(c, 'regular', capture), self.assertRaises(ValueError):
                c.TensorFile(path)
            self.assertEqual(len(seen), 1)
            with self.assertRaises(OSError):
                os.fstat(seen[0])

    def test_all131_exact_route_inventory_and_saved_key_spelling(self):
        _, inventory, _ = c.preparation()
        for mode in ('lora', 'dora'):
            slots = inventory['modes'][mode]['slots']
            width = 2 if mode == 'lora' else 3
            self.assertEqual(131*width, len(slots))
            self.assertEqual(14*width, sum(c.dormant(s) for s in slots))
            self.assertEqual(117*width, sum(c.route(s, True)[1] for s in slots))
            self.assertEqual({0: 14*width, 2: 2*width, 3: 115*width}, {step: sum(c.route(s, False)[0] == step for s in slots) for step in (0, 2, 3)})
        bad = copy.deepcopy(inventory)
        bad['modes']['dora']['slots'][2]['saved_key'] += '.weight'
        with self.assertRaises(ValueError):
            c.validate_inventory(bad)
        bad = copy.deepcopy(inventory)
        bad['modules'][1] = bad['modules'][0]
        with self.assertRaises(ValueError):
            c.validate_inventory(bad)

    def test_checkpoint_none_zero_counter_and_raw_state_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'state'
            for paused in (True, False):
                values = payloads(paused)
                tensor_file(path, values)
                result = c.validate_checkpoint(path, SLOTS, paused, bytes(range(32)))
                c.validate_checkpoint(path, SLOTS, paused, bytes(range(32)), result['state_sha256'])
                self.assertEqual([x['adam_step'] for x in result['slots']], [0]*4 if paused else [3, 2, 0, 3])
                # Same numeric zero, different IEEE bytes must change the
                # independent state identity without changing scalar validity.
                values['__extension.seeded.gradient.1'] = ([-0.0]*2, [2])
                tensor_file(path, values)
                with self.assertRaisesRegex(ValueError, 'state digest'):
                    c.validate_checkpoint(path, SLOTS, paused, bytes(range(32)), result['state_sha256'])
            values = payloads(True)
            for key, replacement in (
                ('__extension.seeded.presence', ([1, 1, 1, 1], [4])),
                ('__extension.seeded.gradient.2', ([1, 0], [2])),
                ('adam_step_u32::'+SLOTS[2]['name'], ([1, 0, 0, 0], [4])),
                ('adam_m::'+SLOTS[2]['name'], ([0.5, 0], [2])),
                ('weight::'+SLOTS[0]['name'], ([float('nan')]*4, [4])),
                ('__run_fingerprint', ([0]*32, [32])),
            ):
                bad = copy.deepcopy(values); bad[key] = replacement; tensor_file(path, bad)
                with self.subTest(key=key), self.assertRaises(ValueError):
                    c.validate_checkpoint(path, SLOTS, True, bytes(range(32)))
            values = payloads(False)
            values['adam_v::'+SLOTS[0]['name']] = ([-0.1]*4, [4])
            tensor_file(path, values)
            with self.assertRaisesRegex(ValueError, 'negative second moment'):
                c.validate_checkpoint(path, SLOTS, False, bytes(range(32)))

    def test_controller_hash_preserves_original_numeric_tokens_and_order(self):
        run = {'mode': 'lora', 'epochs': 1, 'batch_size': 1, 'accumulation': 2, 'scheduler': 'constant', 'warmup_steps': 0, 'max_optimizer_steps': None, 'beta1': 0.9, 'beta2': 0.999, 'epsilon': 1e-8, 'weight_decay': 0.01, 'task_lr': 0.0005, 'max_grad_norm': 0.7}
        manifest = {'backend': 'native', 'config': {'execution': 'native', 'run': run}, 'run_fingerprint': list(range(32))}
        raw = json.dumps(manifest, separators=(',', ':')).encode()
        actual = c.controller_fingerprint(raw, SLOTS)
        # Independent canonical wire prefix and entry sequence (no use of
        # checker compact_numbers) supplies an exact expected hash.
        settings = b'{"groups":[{"optimizer":{"beta1":0.9,"beta2":0.999,"eps":1e-08,"weight_decay":0.01},"schedule":{"warmup_constant":{"initial_lr":0.0005,"warmup_steps":0,"total_steps":3}}}],"grad_accum_steps":2,"max_grad_norm":0.7,"partial_window":"actual_microbatches"}'
        expected = hashlib.sha256(b'antfly.seeded-gradient-trainer.v1'+bytes(range(32))+settings)
        for slot in SLOTS:
            expected.update(json.dumps({'name': slot['name'], 'dimensions': slot['shape'], 'group': 0}, separators=(',', ':')).encode())
        self.assertEqual(expected.digest(), actual)
        self.assertNotEqual(actual, c.controller_fingerprint(raw, list(reversed(SLOTS))))
        changed = raw.replace(b'1e-08', b'0.00000001')
        self.assertNotEqual(actual, c.controller_fingerprint(changed, SLOTS))

    def test_partial_flush_requires_live_encoder_loss_without_classifier_fallback(self):
        reports = []
        for row in range(5):
            terms = {key: 0 for key in c.TERM_NAMES}; terms['total'] = 2; terms['classification'] = int(row in (0, 2))
            reports.append({'epoch': 0, 'batch': row, 'examples': 1, 'terms': terms, 'coverage': {'gold_mentions': 1, 'proposed_gold_mentions': 1, 'gold_relations': 0, 'proposed_gold_relations': 0, 'matched_records': 0}, 'optimizer': {'identity': {'optimizer_step': (row+1)//2, 'microbatch_step': row+1}, 'optimizer_stepped': row in (1, 3), 'accumulated_microbatches': (row+1)%2, 'loss': 2, 'grad_norm': 0.25}, 'decision_fingerprint': list(range(32)), 'zero_loss_fallback': False})
        reports.append({'examples': 0, 'terms': None, 'optimizer': {'identity': {'optimizer_step': 3, 'microbatch_step': 5}, 'optimizer_stepped': True, 'accumulated_microbatches': 0, 'loss': None, 'grad_norm': 0.5}})
        c.validate_reports(reports, 'uninterrupted')
        changed = copy.deepcopy(reports); changed[1]['zero_loss_fallback'] = True; changed[1]['optimizer']['loss'] = 0
        with self.assertRaises(ValueError):
            c.validate_reports(changed, 'uninterrupted')
        changed = copy.deepcopy(reports); changed[-1]['optimizer']['identity']['optimizer_step'] = 2
        with self.assertRaises(ValueError):
            c.validate_reports(changed, 'uninterrupted')
        changed = copy.deepcopy(reports); changed[0]['coverage']['gold_mentions'] = True
        with self.assertRaises(ValueError):
            c.validate_reports(changed, 'uninterrupted')

    def test_exact_argv_and_atomic_no_overwrite_cleanup(self):
        proposal = c.load(c.ROOT/'qualification_plan.json', 64*1024)
        self.assertEqual(proposal['supervisor']['outer_timeout_seconds'], runner.TIMEOUT)
        self.assertEqual(proposal['supervisor']['sampled_tree_rss_limit_bytes'], runner.RSS_LIMIT)
        path = Path('/private/tmp/literal spaces/config.json')
        standalone = {'entrypoint': 'standalone', 'binary': {'path': '/private/tmp/literal binary'}}
        self.assertEqual(['/private/tmp/literal binary', str(path), '--shutdown-grace-seconds', '30', '--stop-after-microbatches', '1'], runner.command_for(standalone, path, 'paused'))
        public = copy.deepcopy(standalone); public['entrypoint'] = 'public-runtime'
        self.assertEqual(runner.command_for(public, path, 'resumed')[1:4], ['finetune', 'train', 'gliner25'])
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'receipt.json'
            c.write_new(path, {'before': 1})
            with self.assertRaises(FileExistsError):
                c.write_new(path, {'after': 2})
            self.assertEqual(c.load(path), {'before': 1})
            with mock.patch.object(c.os, 'link', side_effect=OSError('injected publication failure')), self.assertRaises(OSError):
                c.write_new(Path(temporary)/'new.json', {'value': 3})
            self.assertEqual({p.name for p in Path(temporary).iterdir()}, {'receipt.json'})


if __name__ == '__main__':
    unittest.main()
