"""Pure probe contract tests; no model, GPU, or child launch."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import metal_pause as probe

CPU = Path('/private/tmp/antfly-gliner25-training-inactive-published-small-v1')


def config(mode='lora'):
    return probe.reference.load(probe.ROOT / f'{mode}-paused.json')


def resource_report():
    return {'host_peak_bytes': 20 * probe.MIB, 'backend_peak_bytes': 8 * probe.MIB,
            'resident_device_upper_bound_bytes': 600 * probe.MIB,
            'resident_gradient_control_bytes': 32,
            'transfers': {'bytes': {'upload_bytes': 256, 'proposal_logits': 32,
                                   'proposal_features': 128, 'loss_logits': 32,
                                   'finite_control': 16}, 'upload_calls': 5, 'readback_calls': 4}}


class MetalPauseTests(unittest.TestCase):
    def test_preparation_pins_and_changes_are_resource_backend_and_locations_only(self):
        prep = probe.preparation()
        self.assertEqual(prep['outer_supervision']['sampled_child_tree_rss_bytes'], 3 * 1024**3)
        self.assertEqual(probe.reference.read(probe.ROOT / 'train.jsonl'), probe.reference.read(CPU / 'train.jsonl'))
        for mode in ('lora', 'dora'):
            expected = probe.reference.load(CPU / f'{mode}-paused.json')
            expected.update(execution='resident_metal', train_file=str(probe.ROOT / 'train.jsonl'), output_dir=str(probe.ROOT / f'{mode}-paused'))
            expected['memory']['backend_bytes'] = 1024**3
            expected['memory']['combined_bytes'] = 2 * 1024**3
            self.assertEqual(config(mode), expected)

    def test_resident_hash_has_exact_additive_execution_domain(self):
        raw = probe.reference.read(CPU / 'lora-paused' / 'run.json')
        native = probe.reference.checkpoint_contract_fingerprint('lora', raw)
        parsed = probe.reference.loads(raw)
        parsed['backend'] = 'metal'
        parsed['config']['execution'] = 'resident_metal'
        # Preserve original production numeric lexemes by a narrow byte edit.
        resident_raw = raw.replace(b'"backend": "native"', b'"backend": "metal"').replace(b'"execution": "native"', b'"execution": "resident_metal"')
        actual = probe.controller_fingerprint('lora', resident_raw)
        original_hashlib = hashlib.sha256
        captured = []
        def capture(data=b''):
            captured.append(data)
            return original_hashlib(data)
        with mock.patch.object(probe.hashlib, 'sha256', side_effect=capture):
            self.assertEqual(probe.controller_fingerprint('lora', resident_raw), actual)
        with mock.patch.object(probe.hashlib, 'sha256', side_effect=capture):
            self.assertEqual(probe.reference.checkpoint_contract_fingerprint('lora', raw), native)
        marker = b'\x00resident_f32_optimizer_v1\x00'
        self.assertEqual(captured[0].replace(marker, b'', 1), captured[1])
        self.assertNotEqual(actual, native)
        with self.assertRaisesRegex(ValueError, 'not resident'):
            probe.controller_fingerprint('lora', raw)
        mutated = resident_raw.replace(b'"run_fingerprint": [\n    90,', b'"run_fingerprint": [\n    91,')
        self.assertNotEqual(probe.controller_fingerprint('lora', mutated), actual)

    def test_admission_distinguishes_metadata_device_and_sampled_rss(self):
        report = resource_report()
        probe.validate_resource_report(report, config())
        for key, value in (('host_peak_bytes', 128 * probe.MIB + 1), ('backend_peak_bytes', 64 * probe.MIB + 1),
                           ('resident_device_upper_bound_bytes', 0), ('resident_device_upper_bound_bytes', 1024**3 + 1),
                           ('host_peak_bytes', True), ('resident_gradient_control_bytes', -1)):
            changed = copy.deepcopy(report)
            changed[key] = value
            with self.assertRaises(ValueError):
                probe.validate_resource_report(changed, config())

    def test_transfer_categories_and_exact_default_caps(self):
        report = resource_report()
        for key, value in (('upload_bytes', 256 * probe.MIB + 1), ('loss_logits', 128 * probe.MIB), ('finite_control', False)):
            changed = copy.deepcopy(report)
            changed['transfers']['bytes'][key] = value
            with self.assertRaises(ValueError):
                probe.validate_resource_report(changed, config())
        changed = copy.deepcopy(report)
        changed['transfers']['bytes']['encoder_activations'] = 4
        with self.assertRaisesRegex(ValueError, 'unknown transfer'):
            probe.validate_resource_report(changed, config())

    def test_paused_semantics_are_active_first_row_without_optimizer_update(self):
        reports = [probe.reference.loads(line) for line in probe.reference.read(CPU / 'lora-paused' / 'progress.jsonl').splitlines()]
        probe.reference.validate_reports(reports, 'paused')
        for path, value in ((('zero_loss_fallback',), True), (('optimizer', 'optimizer_stepped'), True), (('optimizer', 'identity', 'optimizer_step'), 1)):
            changed = copy.deepcopy(reports)
            current = changed[0]
            for key in path[:-1]:
                current = current[key]
            current[path[-1]] = value
            with self.assertRaises(ValueError):
                probe.reference.validate_reports(changed, 'paused')

    def test_output_publication_is_exclusive_and_supervisor_is_frozen(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'validation.json'
            probe.reference.write_new(path, {'value': 1})
            original = path.read_bytes()
            with self.assertRaises(FileExistsError):
                probe.reference.write_new(path, {'value': 2})
            self.assertEqual(path.read_bytes(), original)
        self.assertEqual(probe.reference.digest(Path(probe.supervision.__file__))['sha256'], 'a937237975be2ed879f62afd285494ccb51a7c1d7cc1dff7160621fb26b87332')


if __name__ == '__main__':
    unittest.main()
