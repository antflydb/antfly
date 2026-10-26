# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib, json, struct, tempfile, unittest
from pathlib import Path

import release_attention_matching as release

def h(b): return hashlib.sha256(b).hexdigest()
def ref(root,name,data):
    p=root/name; p.write_bytes(data); return {'path':name,'sha256':h(data)}
def template_hash(source, fields):
    d=hashlib.sha256(); d.update(b'antfly.chat-template.v1\0')
    for value in [source,*fields]: d.update(struct.pack('<Q',len(value))); d.update(value)
    return d.hexdigest()
def policy_hash(parsed,add,bos):
    d=hashlib.sha256(); d.update(b'antfly.loaded-tokenizer-policy.v1\0'); d.update(bytes.fromhex(parsed)); d.update(bytes((int(add),))); d.update(struct.pack('<Q',len(bos))); d.update(bos); return d.hexdigest()

class LoadedProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name)
        tok=b'{"version":"1.0"}'; template=b'{{ bos_token }}{{ messages }}'
        parsed=h(tok); bos=b'<bos>'; special=[bos,b'<eos>',b'<unk>',b'<pad>']
        tokenizer=policy_hash(parsed,True,bos); tmpl=template_hash(template,special)
        self.identity={k:(h(k.encode())) for k in ('artifact','layout','profile','implementation')}
        self.identity.update(tokenizer=tokenizer,template=tmpl)
        self.origin={'binary_sha256':h(b'binary'),'source_build_sha256':h(b'source'),'device_sha256':h(b'device'),'os_sha256':h(b'os')}
        profile={'runtime_candidate':{'profile_semantic_sha256':h(b'semantic')}}
        self.profile_ref=ref(self.root,'profile.json',json.dumps(profile,sort_keys=True,separators=(',',':')).encode())
        self.identity['profile']=self.profile_ref['sha256']
        receipt={
          'schema_version':1,'stage':'loaded_compaction_provenance','completed':True,'request':{'session_id':'a'*64,'request_index':0},
          'semantic_identity':self.identity,'provenance':self.origin,
          'loaded_identity':{'load_generation':7,**{k+'_sha256':self.identity[k] for k in ('artifact','tokenizer','template','layout','profile','implementation')}},
          'tokenizer':{'kind':'hf_json','source':ref(self.root,'tokenizer.json',tok),'parsed_source_sha256':parsed,'gguf_metadata':None,'policy':{'add_bos_token':True,'bos_token':'<bos>'},'loaded_sha256':tokenizer},
          'template':{'source':ref(self.root,'template.jinja',template),'bos_token':'<bos>','eos_token':'<eos>','unk_token':'<unk>','pad_token':'<pad>','loaded_sha256':tmpl},
          'profile':{'artifact_sha256':self.identity['profile'],'semantic_sha256':profile['runtime_candidate']['profile_semantic_sha256']},
        }
        self.receipt_path=self.root/'loaded.json'; release.write_json(self.receipt_path,receipt)
        self.entry={'path':'loaded.json','sha256':release.sha(self.receipt_path)}
    def tearDown(self): self.tmp.cleanup()
    def validate(self): return release.loaded_provenance_contract(self.entry,self.root.resolve(),self.identity,self.origin,self.profile_ref)
    def rewrite(self,mutate):
        data=release.read_json(self.receipt_path); mutate(data); self.receipt_path.unlink(); release.write_json(self.receipt_path,data); self.entry['sha256']=release.sha(self.receipt_path)
    def test_reconstructs_loaded_hf_tokenizer_template_and_profile(self):
        got=self.validate(); self.assertEqual(got['load_generation'],7); self.assertEqual(got['tokenizer_kind'],'hf_json'); self.assertEqual(got['session_id'],'a'*64)
    def test_rejects_unbound_request(self):
        self.rewrite(lambda r:r['request'].update(session_id='caller-echo'))
        with self.assertRaisesRegex(ValueError,'request binding'): self.validate()
    def test_compact_reply_binds_same_loaded_receipt(self):
        report={'schema_version':2,'completed':True,'operation':'describe_provenance','session_id':'a'*64,
                'request_index':0,'loaded_identity':release.read_json(self.receipt_path)['loaded_identity'],
                'loaded_provenance':self.entry}
        got=release.loaded_provenance_report_contract(report,self.root.resolve(),self.identity,self.origin,
                                                       self.profile_ref,'a'*64,0)
        self.assertEqual(got['audited']['load_generation'],7)
    def test_compact_reply_rejects_different_request(self):
        report={'schema_version':2,'completed':True,'operation':'describe_provenance','session_id':'a'*64,
                'request_index':1,'loaded_identity':release.read_json(self.receipt_path)['loaded_identity'],
                'loaded_provenance':self.entry}
        with self.assertRaisesRegex(ValueError,'receipt/reply mismatch'):
            release.loaded_provenance_report_contract(report,self.root.resolve(),self.identity,self.origin,
                                                       self.profile_ref,'a'*64,1)
    def test_v2_completion_stays_false_with_loaded_provenance_blocker(self):
        got=release._finish_v2_audit([{'incomplete_gates':['loaded_tokenizer_template_profile_provenance']}])
        self.assertFalse(got['evidence_passed']); self.assertTrue(got['diagnostic_only'])
    def test_v2_completion_only_passes_with_no_remaining_gates(self):
        got=release._finish_v2_audit([{'incomplete_gates':[]}])
        self.assertTrue(got['evidence_passed']); self.assertTrue(got['serving_qualified']); self.assertIsNone(got['reason'])
    def test_rejects_tokenizer_policy_echo(self):
        self.rewrite(lambda r:r['tokenizer']['policy'].update(add_bos_token=False))
        with self.assertRaisesRegex(ValueError,'tokenizer policy'): self.validate()
    def test_rejects_template_special_token_echo(self):
        self.rewrite(lambda r:r['template'].update(bos_token='<different>'))
        with self.assertRaisesRegex(ValueError,'template identity'): self.validate()
    def test_rejects_profile_semantic_echo(self):
        self.rewrite(lambda r:r['profile'].update(semantic_sha256='0'*64))
        with self.assertRaisesRegex(ValueError,'profile semantic'): self.validate()
    def native_profile(self):
        # Reuse the real native profile/freezer fixture. Loaded provenance
        # validates its standalone runtime projection; the numerical auditor
        # separately rewalks the calibration graph.
        from test_native_repeat_profile import NativeRepeatProfileTests
        fixture = NativeRepeatProfileTests()
        fixture.setUp()
        try:
            frozen = fixture.invoke()
        finally:
            fixture.doCleanups()
        frozen['identity'] = {name + '_sha256': self.identity[name] for name in
                              ('artifact','tokenizer','template','layout','implementation')}
        path = self.root / 'native-profile.json'
        release.write_json(path, frozen)
        self.profile_ref = {'path':path.name,'sha256':release.sha(path)}
        self.identity['profile'] = self.profile_ref['sha256']
        data = release.read_json(self.receipt_path)
        data['semantic_identity'] = dict(self.identity)
        data['loaded_identity']['profile_sha256'] = self.identity['profile']
        data['profile'] = {'artifact_sha256':self.identity['profile'],
                           'semantic_sha256':frozen['profile_semantic_sha256']}
        self.receipt_path.unlink()
        release.write_json(self.receipt_path,data)
        self.entry['sha256'] = release.sha(self.receipt_path)
        return path, frozen
    def test_reconstructs_native_profile_runtime_projection(self):
        _, frozen = self.native_profile()
        got = self.validate()
        self.assertEqual(got['profile_semantic_sha256'],frozen['profile_semantic_sha256'])
    def test_native_profile_semantics_are_recomputed_before_loaded_claim(self):
        path, frozen = self.native_profile()
        frozen['owners'][0]['retained'][0] += 1
        path.unlink()
        release.write_json(path,frozen)
        self.profile_ref['sha256'] = release.sha(path)
        self.identity['profile'] = self.profile_ref['sha256']
        def rebind(data):
            data['semantic_identity']['profile'] = self.identity['profile']
            data['loaded_identity']['profile_sha256'] = self.identity['profile']
            data['profile']['artifact_sha256'] = self.identity['profile']
        self.rewrite(rebind)
        with self.assertRaisesRegex(ValueError,'semantic'):
            self.validate()
    def make_gguf(self):
        data=release.read_json(self.receipt_path); source=(self.root/'tokenizer.json').read_bytes()
        metadata={'bos_token_id':2,'eos_token_id':3,'padding_token_id':None,'unknown_token_id':0,'add_bos_token':True,'add_eos_token':False}
        d=hashlib.sha256(); d.update(b'antfly.hf-tokenizer.gguf.v1\0'); d.update(struct.pack('<Q',len(source))); d.update(source)
        for key in ('bos_token_id','eos_token_id','padding_token_id','unknown_token_id'):
            value=metadata[key]; d.update(b'\0' if value is None else b'\1'+struct.pack('<i',value))
        for key in ('add_bos_token','add_eos_token'):
            value=metadata[key]; d.update(b'\0' if value is None else b'\2' if value else b'\1')
        parsed=d.hexdigest(); loaded=policy_hash(parsed,True,b'<bos>')
        data['tokenizer'].update(kind='gguf_derived',parsed_source_sha256=parsed,gguf_metadata=metadata,loaded_sha256=loaded)
        data['semantic_identity']['tokenizer']=loaded; data['loaded_identity']['tokenizer_sha256']=loaded; self.identity['tokenizer']=loaded
        self.receipt_path.unlink(); release.write_json(self.receipt_path,data); self.entry['sha256']=release.sha(self.receipt_path)
    def test_reconstructs_gguf_derived_tokenizer_preimage(self):
        self.make_gguf(); self.assertEqual(self.validate()['tokenizer_kind'],'gguf_derived')
    def test_rejects_gguf_metadata_mutation(self):
        self.make_gguf(); self.rewrite(lambda r:r['tokenizer']['gguf_metadata'].update(bos_token_id=9))
        with self.assertRaisesRegex(ValueError,'parsed identity'): self.validate()
    def test_rejects_changed_exported_source(self):
        (self.root/'tokenizer.json').write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'digest mismatch'): self.validate()

if __name__=='__main__': unittest.main()
