# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
import unittest
from unittest.mock import patch

import freeze_native_whole_head_campaign as builder
from test_qualify_native_whole_head_quality import FrozenCollection


class FreezePublication(unittest.TestCase):
    def setUp(self):
        self.fixture = FrozenCollection()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.root = self.fixture.root
        self.output = self.root / 'published-freeze.json'
        self.spec = {'schema': 'antfly.native-whole-head.freeze-input.v2',
                     **{key: self.fixture.frozen[key] for key in ('build', 'configuration', 'configuration_audit',
                                                                 'input_index', 'models', 'performance')}}
        self.spec_ref = self.fixture.put('freeze-spec.json', self.spec)

    def freeze(self):
        return builder.freeze(self.root, self.root, self.spec_ref, self.output)

    def test_publish_exact_validated_bytes_and_fixed_statistical_seed(self):
        with patch.object(builder.campaign, 'model_artifact', return_value=(self.root, self.root)) as artifacts:
            ref = self.freeze()
        self.assertEqual(ref['sha256'], builder.campaign.sha(self.output))
        value = json.loads(self.output.read_bytes())
        self.assertTrue(value['runtime_frozen'])
        self.assertFalse(value['release_authority'])
        self.assertEqual(artifacts.call_count, 2)
        self.assertEqual(len(value['statistical_seed']), 64)
        self.assertFalse(self.output.with_name(self.output.name + '.pending').exists())

    def test_failed_prerequisite_retains_pending_bytes_without_publication(self):
        with patch.object(builder.campaign.contract, 'performance_contract', return_value={'performance_gate_passed': False}), \
             patch.object(builder.campaign, 'model_artifact') as artifacts:
            with self.assertRaisesRegex(ValueError, 'prerequisite failed'):
                self.freeze()
        self.assertFalse(self.output.exists())
        self.assertTrue(self.output.with_name(self.output.name + '.pending').exists())
        artifacts.assert_not_called()

    def test_existing_freeze_and_changed_inputs_are_not_overwritten(self):
        self.output.write_bytes(b'immutable existing freeze')
        with self.assertRaisesRegex(ValueError, 'existing campaign freeze'):
            self.freeze()
        self.assertEqual(self.output.read_bytes(), b'immutable existing freeze')
        self.output.unlink()
        (self.root / self.spec['configuration']['path']).write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'configuration probe'):
            self.freeze()
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
