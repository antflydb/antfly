"""Small immutable source-control fixtures; no ML imports or model execution."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import struct
import sys
import unittest

import capture_training_inactive_adapters as capture

DIRECTORY=capture.oracle.FIXTURES/'training_inactive_adapters'
PINS={'capture.json':(864250,'f7bb532ad38f54c105b44e703f0af8bb837b02a7ec1850679b2da17b2ecdbfc0'),
      'tensors.safetensors':(591162,'4607cc7ff24066eb53e18936d95b20b17b5fc4e0281a23c565b357d8b60f1f2f')}


class InactiveAdapterSourceContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.metadata=capture.oracle.read_json(DIRECTORY/'capture.json')
        cls.raw=(DIRECTORY/'tensors.safetensors').read_bytes()
        length=struct.unpack('<Q',cls.raw[:8])[0]
        if length>1024**2 or 8+length>len(cls.raw):raise ValueError('invalid fixture header')
        cls.header=json.loads(cls.raw[8:8+length]);cls.start=8+length

    @classmethod
    def floats(cls,key):
        item=cls.header[key]
        if item['dtype']!='F32':raise ValueError('expected fixture float32')
        start,end=item['data_offsets'];count=math.prod(item['shape'])
        if end-start!=count*4:raise ValueError('invalid fixture tensor geometry')
        return struct.unpack('<'+str(count)+'f',cls.raw[cls.start+start:cls.start+end])

    def test_pins_full_tensor_inventory_and_provenance_have_no_numerical_import(self):
        for name,(size,pin) in PINS.items():
            raw=(DIRECTORY/name).read_bytes();self.assertEqual(size,len(raw));self.assertEqual(pin,hashlib.sha256(raw).hexdigest())
        value=self.metadata;contract=capture.oracle.read_json(capture.CONTRACT)
        self.assertEqual(capture.SCOPE,value['scope']);self.assertFalse(value['qualification'])
        self.assertEqual(capture.digest(capture.CONTRACT),value['contract'])
        self.assertEqual(capture.digest(Path(capture.__file__)),value['generator'])
        self.assertEqual(capture.oracle.UPSTREAM_COMMIT,value['source_commit'])
        self.assertEqual(contract['trainer_methods'],value['trainer_methods'])
        self.assertEqual(set(capture.METHODS),set(value['trainer_methods']))
        for name,pin in value['baseline'].items():self.assertEqual(pin,capture.digest(capture.BASELINE/name))
        self.assertEqual(set(self.header),set(value['tensors']['tensors']))
        for name,item in self.header.items():
            self.assertEqual(item['shape'],value['tensors']['tensors'][name]['shape'])
            start,end=item['data_offsets'];self.assertLessEqual(0,start);self.assertLessEqual(start,end)
            self.assertLessEqual(self.start+end,len(self.raw))
            if item['dtype']=='F32':self.assertTrue(all(math.isfinite(x) for x in self.floats(name)))
        self.assertFalse(any(name in sys.modules for name in ('torch','gliner2','peft')))

    def test_classifier_fallback_is_zero_present_and_zero_window_advances_all_slots(self):
        for profile in self.metadata['profiles']:
            if not profile['id'].endswith('.classifier_only'):continue
            self.assertEqual([2,3,5],profile['flush_after']);self.assertEqual(5,len(profile['microbatches']))
            self.assertTrue(profile['fresh_owner_mid_window_resume_exact'])
            for micro in profile['microbatches']:
                self.assertGreater(micro['model_loss'],0)
                self.assertEqual(micro['case']=='inactive',micro['fallback'])
                if micro['case']!='inactive':continue
                self.assertFalse(micro['model_loss_requires_grad']);self.assertEqual(0,micro['reported_loss'])
                for key in micro['gradients_scaled'].values():
                    self.assertIsNotNone(key);self.assertTrue(all(x==0 for x in self.floats(key)))
            self.assertEqual([2,1,2],[item['microbatches'] for item in profile['flushes']])
            self.assertEqual([1,2,1],[item['partial_renormalization'] for item in profile['flushes']])
            for index,flush in enumerate(profile['flushes'],1):
                self.assertEqual(index,flush['global_step']);self.assertEqual(index,flush['scheduler_last_epoch'])
                for slot in flush['parameters'].values():
                    self.assertTrue(slot['state_present']);self.assertEqual(index,slot['step'])
            self.assertEqual(0,profile['flushes'][-1]['grad_norm'])
            before,after=profile['flushes'][-2:]
            self.assertTrue(any(self.floats(before['parameters'][name]['weight'])!=self.floats(slot['weight'])
                for name,slot in after['parameters'].items()))
            self.assertTrue(any(self.floats(before['parameters'][name]['exp_avg'])!=self.floats(slot['exp_avg'])
                for name,slot in after['parameters'].items()))

    def test_optional_head_touch_keeps_nonzero_objective_and_present_zero_slots(self):
        selected=[p for p in self.metadata['profiles'] if p['id'].endswith(('.record_only','.relation_only'))]
        self.assertEqual(4,len(selected))
        for profile in selected:
            micro=profile['microbatches'][0]
            self.assertFalse(micro['fallback']);self.assertTrue(micro['model_loss_requires_grad'])
            self.assertGreater(micro['reported_loss'],0);self.assertEqual(micro['model_loss'],micro['reported_loss'])
            for key in micro['gradients_unscaled'].values():
                self.assertIsNotNone(key);self.assertTrue(all(x==0 for x in self.floats(key)))
            for slot in profile['flushes'][0]['parameters'].values():self.assertEqual(1,slot['step'])

    def test_other_live_path_keeps_unsupervised_classifier_absent_without_losing_prior_accumulation(self):
        for profile in self.metadata['profiles']:
            if not profile['id'].endswith('.encoder_classifier'):continue
            micro=profile['microbatches'][1]
            self.assertFalse(micro['fallback']);self.assertTrue(micro['model_loss_requires_grad'])
            self.assertEqual(micro['model_loss'],micro['reported_loss'])
            absent=[name for name,key in micro['gradients_unscaled'].items() if key is None]
            self.assertEqual(4 if profile['mode']=='lora' else 6,len(absent))
            self.assertTrue(all('.classifier.' in name for name in absent))
            for name in absent:
                self.assertIsNotNone(micro['accumulated_gradients'][name])
                self.assertTrue(profile['flushes'][0]['parameters'][name]['state_present'])
            self.assertTrue(profile['frozen_parameters_unchanged'])


if __name__=='__main__':unittest.main()
