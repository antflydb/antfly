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

import hashlib,importlib.util,json,os,sys,tempfile,types,unittest
from pathlib import Path
module_path=Path(__file__).with_name('context_release_build_adapter.py')
spec=importlib.util.spec_from_file_location('context_release_build_adapter_v2',module_path)
adapter=importlib.util.module_from_spec(spec);spec.loader.exec_module(adapter)
H=lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
class Adapter(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
  self.binary=self.root/'context-tests';self.binary.write_bytes(b'bin');self.binary.chmod(0o755)
  self.receipt=self.root/'full-release-build.json'
  
  for name,data in [('resource',b'r'),('stdout',b''),('stderr',b's'),('source.zip',b'z')]: (self.root/name).write_bytes(data)
  self.value={'schema':'antfly.context-release-build.v1','evidence_scope':'full_context_release_build','release_eligible':True,'serving_qualified':False,'source_verified_after_build':True,'binary':{'path':'context-tests','sha256':H(self.binary),'bytes':3},'source':{'files':{'zig/a.zig':'0'*64}},'resource':{'path':'resource','sha256':H(self.root/'resource')},'stdout':{'path':'stdout','sha256':H(self.root/'stdout')},'stderr':{'path':'stderr','sha256':H(self.root/'stderr')},'source_archive':{'path':'source.zip','sha256':H(self.root/'source.zip')}}
  self.receipt.write_text(json.dumps(self.value))
  self.old=sys.modules.get('release_attention_matching')
  module=types.ModuleType('release_attention_matching')
  module.full_release_build_contract=lambda root,path,expected:{'release_build_evidence':True,'head':'a'*40,'binary_sha256':expected['sha256'],'source_archive_sha256':'b'*64}
  sys.modules['release_attention_matching']=module
 def tearDown(self):
  if self.old is None:sys.modules.pop('release_attention_matching',None)
  else:sys.modules['release_attention_matching']=self.old
  self.tmp.cleanup()
 def test_validated_fields_are_derived(self):
  got=adapter.validate(self.receipt,self.binary,self.root)
  self.assertEqual(got['binary_sha256'],H(self.binary));self.assertEqual(got['receipt_sha256'],H(self.receipt))
 def test_old_binary_only_receipt_is_not_upgraded(self):
  self.receipt.write_text(json.dumps({'binary_sha256':H(self.binary),'source_verified_after_build':True}))
  with self.assertRaisesRegex(ValueError,'historical, scoped'):adapter.validate(self.receipt)
 def test_scoped_receipt_is_not_upgraded(self):
  self.value['release_eligible']=False;self.value['evidence_scope']='numerical_capture_and_metal_lifecycle_only';self.receipt.write_text(json.dumps(self.value))
  with self.assertRaisesRegex(ValueError,'historical, scoped'):adapter.validate(self.receipt)
 def test_campaign_root_must_contain_complete_evidence(self):
  other=self.root/'capture';other.mkdir()
  with self.assertRaisesRegex(ValueError,'share one sealed root'):adapter.validate(self.receipt,require_campaign_root=other)
 def test_selected_binary_must_be_exact_receipt_binary(self):
  other=self.root/'other';other.write_bytes(b'bin');other.chmod(0o755)
  with self.assertRaisesRegex(ValueError,'not the full release executable'):adapter.validate(self.receipt,other)
 def test_mutated_binary_is_rejected_after_parent_audit(self):
  self.binary.write_bytes(b'changed')
  with self.assertRaisesRegex(ValueError,'executable changed'):adapter.validate(self.receipt)

 def test_receipt_symlink_is_rejected(self):
  link=self.root/'receipt-link';link.symlink_to(self.receipt)
  with self.assertRaisesRegex(ValueError,'receipt path'):adapter.validate(link)

 def test_materialize_copies_exact_closed_graph(self):
  destination=self.root/'capture';destination.mkdir()
  got=adapter.materialize_bundle(self.receipt,destination)
  self.assertEqual(got['receipt_sha256'],H(self.receipt))
  self.assertEqual((destination/'full-release-build.json').read_bytes(),self.receipt.read_bytes())
  for name in ('context-tests','resource','stdout','stderr','source.zip'):self.assertTrue((destination/name).is_file())
 def test_materialize_rejects_collision_without_rewriting_receipt(self):
  destination=self.root/'capture';destination.mkdir();(destination/'resource').write_bytes(b'x')
  with self.assertRaisesRegex(ValueError,'collision'):adapter.materialize_bundle(self.receipt,destination)
  self.assertFalse((destination/'full-release-build.json').exists())

 def test_materialize_rejects_symlink_parent_even_inside_root(self):
  source_alias=self.root/'alias';source_alias.mkdir()
  self.value['resource']['path']='alias/resource'
  (source_alias/'resource').write_bytes((self.root/'resource').read_bytes())
  self.value['resource']['sha256']=H(source_alias/'resource');self.receipt.write_text(json.dumps(self.value))
  destination=self.root/'capture';destination.mkdir();(destination/'nested').mkdir()
  (destination/'alias').symlink_to(destination/'nested',target_is_directory=True)
  with self.assertRaisesRegex(ValueError,'destination parent'):
   adapter.materialize_bundle(self.receipt,destination)
  self.assertFalse((destination/'full-release-build.json').exists())

 def test_partial_copy_is_removed_on_write_failure(self):
  destination=self.root/'capture';destination.mkdir()
  original=adapter._copy_exact
  calls=0
  def fail_second(source,target,copied):
   nonlocal calls
   calls+=1
   if calls==2:
    out=target.open('xb');copied.append(target);out.write(b'partial');out.close()
    raise OSError('injected copy failure')
   return original(source,target,copied)
  adapter._copy_exact=fail_second
  try:
   with self.assertRaisesRegex(OSError,'injected copy failure'):
    adapter.materialize_bundle(self.receipt,destination)
  finally: adapter._copy_exact=original
  self.assertEqual(list(destination.iterdir()),[])

if __name__=='__main__':unittest.main()
