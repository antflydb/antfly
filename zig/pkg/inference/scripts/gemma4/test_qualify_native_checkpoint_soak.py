# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import qualify_native_checkpoint_soak as collector
from test_qualify_native_whole_head_quality import FrozenCollection
from test_native_whole_head_campaign import ResourceContract


class SoakCollection(unittest.TestCase):
    def setUp(self):
        self.fixture = FrozenCollection()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        checkpoint = self.root / 'consumed.kv'
        checkpoint.write_bytes(b'consumed synthetic checkpoint, never imported')
        self.checkpoint = {'path': checkpoint.name, 'sha256': collector.campaign.sha(checkpoint)}
        report = {'worker': {'requests': [{'kind': 'prepare', 'result': {}},
                                           {'kind': 'prepare', 'result': {'file_sha256': self.checkpoint['sha256']}}]}}
        report_ref = self.fixture.put('soak-performance.json', report)
        for entry in self.fixture.frozen['performance']:
            entry['checkpoint'] = self.checkpoint
            entry['observations'] = report_ref
        self.fixture.refresh()
        self.args = SimpleNamespace(root=self.root, campaign_root=self.root,
                                    freeze=Path(self.fixture.freeze_ref['path']),
                                    freeze_sha256=self.fixture.freeze_ref['sha256'], model='gemma4-e2b', encoding='f16',
                                    duration_seconds=1, diagnostic=True, output=self.root / 'soak-output')
        model = patch.object(collector.campaign, 'model_artifact', return_value=(self.root, self.root))
        model.start()
        self.addCleanup(model.stop)

    def guard(self, command, path, **limits):
        self.assertEqual(limits['max_rss_mib'], 8192)
        self.assertEqual(limits['max_swap_growth_mib'], 0)
        self.assertIn('--worker-spec-sha256', command)
        output = Path(path).parent
        (output / 'report.json').write_text('{}\n')
        (output / 'journal.jsonl').write_text('{}\n')
        receipt = ResourceContract().report()
        Path(path).write_text(json.dumps(receipt))
        return receipt

    def test_diagnostic_collection_seals_raw_evidence_without_release_authority(self):
        with patch.object(collector.campaign, 'run_guarded_combined', side_effect=self.guard), \
             patch.object(collector.evidence, 'audit', return_value={'persistent_owner_soak_qualified': False}) as audit:
            self.assertEqual(collector.collect(self.args), 0)
        self.assertFalse(audit.call_args.kwargs['formal'])
        seal = json.loads((self.args.output / 'seal.json').read_bytes())
        self.assertTrue(seal['collection_complete'])
        self.assertFalse(seal['release_authority'])
        self.assertFalse(seal['production_ready'])

    def test_short_unmarked_run_and_changed_checkpoint_prevent_launch(self):
        with patch.object(collector.campaign, 'run_guarded_combined') as guard:
            self.args.diagnostic = False
            with self.assertRaisesRegex(ValueError, 'diagnostic scope'):
                collector.collect(self.args)
            self.args.diagnostic = True
            (self.root / self.checkpoint['path']).write_bytes(b'changed')
            with self.assertRaisesRegex(ValueError, 'checkpoint changed'):
                collector.collect(self.args)
        guard.assert_not_called()

    def test_checkpoint_must_come_from_the_fresh_public_preparation(self):
        wrong = self.fixture.put('wrong-preparation.json', {'worker': {'requests': [
            {'kind': 'prepare', 'result': {}}, {'kind': 'prepare', 'result': {'file_sha256': 'f' * 64}}]}})
        for entry in self.fixture.frozen['performance']:
            entry['observations'] = wrong
        self.fixture.refresh()
        self.args.freeze_sha256 = self.fixture.freeze_ref['sha256']
        with patch.object(collector.campaign, 'run_guarded_combined') as guard:
            with self.assertRaisesRegex(ValueError, 'fresh public preparation'):
                collector.collect(self.args)
        guard.assert_not_called()

    def test_postrun_checkpoint_mutation_remains_failed_and_skips_audit(self):
        def changed(command, path, **limits):
            receipt = self.guard(command, path, **limits)
            (self.root / self.checkpoint['path']).write_bytes(b'changed after execution')
            return receipt
        with patch.object(collector.campaign, 'run_guarded_combined', side_effect=changed), \
             patch.object(collector.evidence, 'audit') as audit:
            self.assertEqual(collector.collect(self.args), 1)
        audit.assert_not_called()
        seal = json.loads((self.args.output / 'seal.json').read_bytes())
        self.assertFalse(seal['passed'])
        self.assertIn('checkpoint changed', seal['error'])


if __name__ == '__main__':
    unittest.main()
