# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Hermetic checks for managed publication and private negative artifacts."""
from pathlib import Path
import hashlib
import json
import struct
import tempfile
from contextlib import contextmanager
import unittest

import qualify_artifact_drift as drift
import qualify_managed_install as install


@contextmanager
def fixture_directory():
    root = install.REPO / '.specialty-artifacts/production-artifact-unit-fixtures'
    root.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(dir=root) as directory:
            yield directory
    finally:
        try:
            root.rmdir()
        except OSError:
            pass


class ManagedPublicationTests(unittest.TestCase):
    def published(self, root, profile='pii'):
        target = root / 'published'
        target.mkdir()
        files = {}
        for name in ('config.json', 'encoder_config/config.json', 'tokenizer.json', 'tokenizer_config.json', 'model.safetensors'):
            data = (name + ':original').encode()
            path = target / name
            path.parent.mkdir(exist_ok=True, parents=True)
            path.write_bytes(data)
            files[name] = {'size_bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}
        pin = {'files': files, 'repository': 'fastino/example', 'revision': '1' * 40}
        generated = target / 'model_manifest.json'
        generated.write_text(json.dumps(install.expected_manifest(profile)))
        artifacts = [{'path': name, 'size': value['size_bytes'], 'sha256': value['sha256']} for name, value in files.items()]
        artifacts.append({'path': 'model_manifest.json', 'size': generated.stat().st_size, 'sha256': install.sha256(generated)})
        receipt = {'version': 2, 'source': {'owner': 'fastino', 'name': 'example', 'variant': 'auto',
                   'requested_revision': pin['revision'], 'resolved_revision': pin['revision'], 'selected_format': 'safetensors'},
                   'artifacts': artifacts}
        (target / install.COMPLETE).write_text(json.dumps(receipt))
        return target, pin, receipt

    def test_published_contract_rejects_wrong_source_or_unfinished_publication(self):
        with fixture_directory() as temporary:
            target, pin, receipt = self.published(Path(temporary))
            install.verify_published(target, 'pii', pin)
            receipt['source']['resolved_revision'] = '0' * 40
            (target / install.COMPLETE).write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, 'immutable source'):
                install.verify_published(target, 'pii', pin)
            (target / '.antfly-download-in-progress').write_text('{}')
            with self.assertRaisesRegex(ValueError, 'unfinished managed'):
                install.verify_published(target, 'pii', pin)

    def test_same_size_payload_or_capability_drift_is_not_accepted(self):
        with fixture_directory() as temporary:
            target, pin, receipt = self.published(Path(temporary), 'gliguard')
            payload = target / 'model.safetensors'
            data = payload.read_bytes()
            payload.write_bytes(b'X' + data[1:])
            with self.assertRaisesRegex(ValueError, 'artifact drift'):
                install.verify_published(target, 'gliguard', pin)
            payload.write_bytes(data)
            manifest = install.expected_manifest('gliguard')
            manifest['capabilities'].append('typed_decisions')
            (target / 'model_manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'generated specialty manifest'):
                install.verify_published(target, 'gliguard', pin)

    def test_valid_managed_extraction_control_publishes_assertion_and_receipt(self):
        with fixture_directory() as temporary:
            root = Path(temporary)
            source, pin, _ = self.published(root)
            clones = root / 'clones'
            clones.mkdir()
            prepared = drift.prepare(source, clones, 'pii', 'managed_valid', pin)
            target = clones / prepared['model']
            install.verify_published(target, 'pii', pin)
            self.assertEqual((source / 'model.safetensors').stat().st_ino,
                             (target / 'model.safetensors').stat().st_ino)
            self.assertNotEqual((source / 'config.json').stat().st_ino,
                                (target / 'config.json').stat().st_ino)
            self.assertEqual(json.loads((target / 'model_manifest.json').read_text())['gliner_specialty_profile'], 'pii')

    def test_generated_manifest_receipt_must_be_hashed(self):
        with fixture_directory() as temporary:
            target, pin, receipt = self.published(Path(temporary))
            del receipt['artifacts'][-1]['sha256']
            (target / install.COMPLETE).write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, 'lacks a receipt hash'):
                install.verify_published(target, 'pii', pin)


class PrivateWeightProbeTests(unittest.TestCase):
    def original(self, root):
        header = json.dumps({'weight': {'dtype': 'F32', 'shape': [2, 2], 'data_offsets': [0, 16]}}).encode()
        header += b' ' * (-len(header) % 8)
        original = root / 'original.safetensors'
        original.write_bytes(struct.pack('<Q', len(header)) + header + struct.pack('<4f', 1, 2, 3, 4))
        return original

    def header(self, path):
        with path.open('rb') as stream:
            size = struct.unpack('<Q', stream.read(8))[0]
            return json.loads(stream.read(size)), 8 + size

    def test_sparse_negative_preserves_original_and_is_not_a_hard_link(self):
        with fixture_directory() as temporary:
            root = Path(temporary)
            original = self.original(root)
            before = original.read_bytes()
            target = root / 'negative.safetensors'
            result = drift.sparse_weights(original, target)
            self.assertEqual(before, original.read_bytes())
            self.assertEqual(original.stat().st_size, target.stat().st_size)
            self.assertNotEqual(original.stat().st_ino, target.stat().st_ino)
            self.assertNotEqual(drift.sha256(original), result['sha256'])
            self.assertEqual('F32', self.header(target)[0]['weight']['dtype'])

    def test_private_fp16_probe_has_valid_half_size_tensor_layout(self):
        with fixture_directory() as temporary:
            root = Path(temporary)
            original = self.original(root)
            before = original.read_bytes()
            target = root / 'converted.safetensors'
            drift.sparse_weights(original, target, fp16=True)
            header, payload_start = self.header(target)
            self.assertEqual(before, original.read_bytes())
            self.assertEqual('F16', header['weight']['dtype'])
            self.assertEqual([0, 8], header['weight']['data_offsets'])
            self.assertEqual(payload_start + 8, target.stat().st_size)
            self.assertEqual(0, target.stat().st_mode & 0o222)


if __name__ == '__main__':
    unittest.main()
