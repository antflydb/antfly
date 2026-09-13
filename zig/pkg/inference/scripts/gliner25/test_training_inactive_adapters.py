"""Small immutable source-control fixtures; no ML imports or model execution."""
from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path
import struct
import sys
import unittest

import fixture_support
import capture_training_inactive_adapters as capture

DIRECTORY=capture.oracle.FIXTURES/'training_inactive_adapters'
PINS={'capture.json':(420293,'d0d5f8dbbd39446cf8050021623f2eda7db6992447bd65029970e93a3a1eff53'),
      'tensors.safetensors':(207375,'9394123bccbaa959e6aa2b4a32905208e3f2df5917626824edff7af741e0192a')}


def tensor_header(raw):
    """Validate the bounded golden file's own header, including its entire payload."""
    def unique_object(pairs):
        value = dict(pairs)
        if len(value) != len(pairs):
            raise ValueError('duplicate fixture header key')
        return value

    if not 8 <= len(raw) <= 8 * 1024**2:
        raise ValueError('invalid fixture size')
    length = struct.unpack('<Q', raw[:8])[0]
    if not 0 < length <= 1024**2 or 8 + length > len(raw):
        raise ValueError('invalid fixture header size')
    header = json.loads(raw[8:8 + length], object_pairs_hook=unique_object)
    if not isinstance(header, dict) or not 0 < len(header) <= 8192:
        raise ValueError('invalid fixture tensor count')
    payload = len(raw) - 8 - length
    widths = {'F32': 4, 'I64': 8, 'I32': 4, 'BOOL': 1}
    ranges = []
    for name, item in header.items():
        if not name or not isinstance(item, dict) or set(item) != {'dtype', 'shape', 'data_offsets'}:
            raise ValueError('invalid fixture tensor entry')
        dtype, shape, offsets = item['dtype'], item['shape'], item['data_offsets']
        if not isinstance(dtype, str) or dtype not in widths or not isinstance(shape, list) or len(shape) > 8:
            raise ValueError('invalid fixture tensor dtype or rank')
        if any(type(dim) is not int or not 0 <= dim <= len(raw) for dim in shape):
            raise ValueError('invalid fixture tensor dimension')
        if (not isinstance(offsets, list) or len(offsets) != 2 or
                any(type(offset) is not int for offset in offsets) or
                not 0 <= offsets[0] <= offsets[1] <= payload or
                offsets[1] - offsets[0] != math.prod(shape) * widths[dtype]):
            raise ValueError('invalid fixture tensor geometry')
        ranges.append(tuple(offsets))
    end = 0
    for start, stop in sorted(ranges):
        if start != end:
            raise ValueError('fixture tensor gap or overlap')
        end = stop
    if end != payload:
        raise ValueError('unclaimed fixture payload')
    return header, 8 + length


class InactiveAdapterSourceContract(unittest.TestCase):
    def load_fixture(self):
        fixture_support.require_fixtures("training_inactive_adapters/capture.json", "training_inactive_adapters/tensors.safetensors")
        self.metadata=capture.step_capture.expand_metadata(capture.oracle.read_json(DIRECTORY/'capture.json'))
        with (DIRECTORY/'tensors.safetensors').open('rb') as stream:
            self.raw=stream.read(8 * 1024**2 + 1)
        self.header,self.start=tensor_header(self.raw)

    def floats(self,key):
        item=self.header[key]
        if item['dtype']!='F32':raise ValueError('expected fixture float32')
        start,end=item['data_offsets'];count=math.prod(item['shape'])
        if end-start!=count*4:raise ValueError('invalid fixture tensor geometry')
        return struct.unpack('<'+str(count)+'f',self.raw[self.start+start:self.start+end])

    def test_pins_tensor_headers_and_provenance_have_no_numerical_import(self):
        self.load_fixture()
        fixture_support.require_fixtures("training_step/capture.json", "training_step/tensors.safetensors")
        for name,(size,pin) in PINS.items():
            raw=(DIRECTORY/name).read_bytes();self.assertEqual(size,len(raw));self.assertEqual(pin,hashlib.sha256(raw).hexdigest())
        value=self.metadata;contract=capture.oracle.read_json(capture.CONTRACT)
        self.assertEqual(capture.SCOPE,value['scope']);self.assertFalse(value['qualification'])
        self.assertEqual(capture.digest(capture.CONTRACT),value['contract'])
        self.assertEqual(capture.digest(Path(capture.__file__)),value['generator'])
        self.assertEqual(capture.digest(Path(capture.step_capture.__file__)),value['tensor_storage_helper'])
        self.assertEqual(contract['tensor_storage_helper'],value['tensor_storage_helper'])
        self.assertEqual(capture.oracle.UPSTREAM_COMMIT,value['source_commit'])
        self.assertEqual(contract['trainer_methods'],value['trainer_methods'])
        self.assertEqual(set(capture.METHODS),set(value['trainer_methods']))
        for name,pin in value['baseline'].items():self.assertEqual(pin,capture.digest(capture.BASELINE/name))
        self.assertEqual({'file','sha256','size_bytes'},set(value['tensors']))
        self.assertEqual(capture.digest(DIRECTORY/value['tensors']['file']),
                         {key:value['tensors'][key] for key in ('size_bytes','sha256')})
        for name,item in self.header.items():
            if item['dtype']=='F32':self.assertTrue(all(math.isfinite(x) for x in self.floats(name)))
        for profile in value['profiles']:
            shapes={parameter['name']:parameter['shape'] for parameter in profile['parameters']}
            slots=[profile['initial']]
            for micro in profile['microbatches']:
                self.assertTrue(all(key in self.header for key in micro['inputs'].values()))
                slots.extend(micro[name] for name in ('gradients_scaled','gradients_unscaled','accumulated_gradients'))
            for flush in profile['flushes']:
                slots.append(flush['gradients_before_clip'])
                for name,state in flush['parameters'].items():
                    slots.extend({name:state[key]} for key in ('weight','exp_avg','exp_avg_sq') if state[key] is not None)
            for slot in slots:
                for name,key in slot.items():
                    if key is not None:
                        self.assertEqual('F32',self.header[key]['dtype'])
                        self.assertEqual(shapes[name],self.header[key]['shape'])
        self.assertFalse(any(name in sys.modules for name in ('torch','gliner2','peft')))

    def test_compact_capture_bindings_resolve_to_bounded_tensor_headers(self):
        for family in ('tasks','training_step','training_step_dropout',
                       'training_inactive_adapters','training_inactive_native_epoch'):
            with self.subTest(family=family):
                names = [name for name in fixture_support.external_inventory() if name.startswith(family + "/")]
                fixture_support.require_fixtures(*names)
                directory=capture.oracle.FIXTURES/family
                metadata=capture.oracle.read_json(directory/'capture.json')
                for name in ('weights','tensors'):
                    if name not in metadata:continue
                    binding=metadata[name]
                    self.assertEqual({'file','sha256','size_bytes'},set(binding))
                    with (directory/binding['file']).open('rb') as stream:raw=stream.read(8 * 1024**2 + 1)
                    self.assertEqual(binding['size_bytes'],len(raw))
                    self.assertEqual(binding['sha256'],hashlib.sha256(raw).hexdigest())
                    tensor_header(raw)

    def test_tensor_header_rejects_invalid_geometry_and_duplicate_keys(self):
        def encoded(value,payload=b'\0'*8):
            header=json.dumps(value,separators=(',',':')).encode()
            return struct.pack('<Q',len(header))+header+payload

        valid={'a':{'dtype':'F32','shape':[1],'data_offsets':[0,4]},
               'b':{'dtype':'I32','shape':[1],'data_offsets':[4,8]}}
        self.assertEqual(valid,tensor_header(encoded(valid))[0])
        for key,value in (('dtype','F64'),('shape',[True]),('shape',[2]),
                          ('data_offsets',[0,4]),('data_offsets',[4,12])):
            malformed=copy.deepcopy(valid);malformed['b'][key]=value
            with self.subTest(key=key,value=value),self.assertRaises(ValueError):
                tensor_header(encoded(malformed))
        duplicate=b'{"a":{"dtype":"F32","dtype":"I32","shape":[1],"data_offsets":[0,4]}}'
        for raw in (b'',struct.pack('<Q',1024**2+1),encoded(valid,b'\0'*9),
                    struct.pack('<Q',len(duplicate))+duplicate+b'\0'*4):
            with self.subTest(size=len(raw)),self.assertRaises(ValueError):tensor_header(raw)

    def test_shared_metadata_preserves_absence_and_rejects_invalid_references(self):
        value={'profiles':[{'initial_ref':0,'microbatches':[{'inputs_ref':0,
                'gradients_unscaled':{'weight':None},'loss':-0.0,'resumed':True}]}],
               'shared_metadata':{'version':1,'bindings':[{'weight':'exact.tensor'}]}}
        resolved=capture.step_capture.expand_metadata(value)
        micro=resolved['profiles'][0]['microbatches'][0]
        self.assertEqual({'weight':'exact.tensor'},micro['inputs'])
        self.assertIsNone(micro['gradients_unscaled']['weight'])
        self.assertEqual(struct.pack('<d',-0.0),struct.pack('<d',micro['loss']))
        self.assertIs(True,micro['resumed'])
        for index in (-1,1,True,None):
            changed=copy.deepcopy(value);changed['profiles'][0]['initial_ref']=index
            with self.subTest(index=index),self.assertRaises(capture.oracle.ContractError):
                capture.step_capture.expand_metadata(changed)
        for change in ('ambiguous','missing_pool','oversize','version','nested'):
            changed=copy.deepcopy(value);shared=changed['shared_metadata']
            if change=='ambiguous':changed['profiles'][0]['initial']={}
            elif change=='missing_pool':shared.pop('bindings')
            elif change=='oversize':shared['bindings']*=capture.step_capture.MAX_SHARED_METADATA_ENTRIES+1
            elif change=='version':shared['version']=True
            else:shared['bindings'][0]['weight']={'ref':0}
            with self.subTest(change=change),self.assertRaises(capture.oracle.ContractError):
                capture.step_capture.expand_metadata(changed)

    def test_classifier_fallback_is_zero_present_and_zero_window_advances_all_slots(self):
        self.load_fixture()
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
        self.load_fixture()
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
        self.load_fixture()
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
