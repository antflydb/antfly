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

import hashlib, json, os, shutil, subprocess, tempfile, unittest, zipfile
from pathlib import Path
import release_attention_matching as release

H=lambda b: hashlib.sha256(b).hexdigest()
def sha(path): return H(Path(path).read_bytes())

class FullReleaseBuild(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name)
        (self.root/'bin').write_bytes(b'binary'); (self.root/'bin').chmod(0o755)
        (self.root/'stdout').write_bytes(b'')
        (self.root/'stderr').write_text('12 selected; 10 passed; 2 skipped.\n')
        self.command=['/usr/bin/env','ANTFLY_ZIG_MAX_RSS=8589934592','python3','tools/run_bounded_zig_build.py',
                      'build','inference-test-context','--verbose','-j1','-Dmetal=true','-Dcuda=false','-Donnx=false','-Doptimize=ReleaseFast']
        self.resource={'schema':'antfly.benchmark_resources.v1','command':self.command,'elapsed_seconds':2,
          'initial_free_percent':70,'limits':{'timeout_seconds':1800,'max_rss_mib':8192,'min_free_percent':15,
          'max_swap_growth_mib':512,'min_disk_free_mib':4096},'min_disk_free_mib':5000,'min_free_percent':50,
          'pass':True,'peak_group_rss_mib':100,'pid':123,'returncode':0,'samples':4,'swapout_growth_mib':2,'violation':None}
        self.write('resource',self.resource)
        self.source_name='zig/pkg/inference/src/new.zig'; self.source_bytes=b'const release = true;\n'
        self.source={'contract':release.FULL_RELEASE_SOURCE_NOTE,
          'files':{self.source_name:H(self.source_bytes)},'deleted':['zig/pkg/inference/src/old.zig'],'gitlinks':{}}
        self.build={'cwd':'zig','command':self.command,'target':'inference-test-context','environment':release.FULL_RELEASE_ENVIRONMENT,'optimize':'ReleaseFast',
                    'metal':True,'cuda':False,'onnx':False}
        self.tests={'selected':12,'passed':10,'skipped':2,'failed':0,'leaked':0}
        self.tools={}
        for name in ('python3','zig'):
            path=Path(shutil.which(name)).resolve(); command=[str(path),'--version'] if name=='python3' else [str(path),'version']
            self.tools[name]={'path':str(path),'sha256':sha(path),'bytes':path.stat().st_size,
                              'version':subprocess.check_output(command,text=True).strip()}
        self.archive=self.root/'source.zip'; self.write_archive()
        self.value={'schema':'antfly.context-release-build.v1','evidence_scope':'full_context_release_build',
          'release_eligible':True,'serving_qualified':False,'created_utc':'2026-10-19T00:00:00Z',
          'sealed_utc':'2026-10-19T00:01:00Z','head':'a'*40,'zig_version':self.tools['zig']['version'],'tools':self.tools,
          'binary':{'path':'bin','sha256':sha(self.root/'bin'),'bytes':(self.root/'bin').stat().st_size},
          'build':self.build,'resource':{'path':'resource','sha256':sha(self.root/'resource')},
          'stdout':{'path':'stdout','sha256':sha(self.root/'stdout')},
          'stderr':{'path':'stderr','sha256':sha(self.root/'stderr')},'tests':self.tests,'source':self.source,
          'source_archive':{'path':'source.zip','sha256':sha(self.archive),'bytes':self.archive.stat().st_size,'files':1},
          'source_verified_after_build':True}
        self.write('receipt',self.value)
    def tearDown(self): self.tmp.cleanup()
    def write(self,name,value): (self.root/name).write_text(json.dumps(value,sort_keys=True)+'\n')
    def write_archive(self, source=None, include=True):
        manifest={'head':'a'*40,'source':self.source,'build':self.build,'tools':self.tools}
        with zipfile.ZipFile(self.archive,'w',compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr(release.FULL_RELEASE_ARCHIVE_MANIFEST,json.dumps(manifest,indent=2,sort_keys=True)+'\n')
            if include:z.writestr(self.source_name,self.source_bytes if source is None else source)
    def audit(self):
        return release.full_release_build_contract(self.root.resolve(),self.root/'receipt',
                  {'path':'bin','sha256':sha(self.root/'bin')})
    def refresh_receipt(self): self.write('receipt',self.value)
    def test_valid_full_release_build_evidence(self):
        self.assertTrue(self.audit()['release_build_evidence'])
    def test_scoped_release_ineligible_capture_is_rejected(self):
        self.write('receipt',{'schema_version':1,'evidence_scope':'numerical_capture_and_metal_lifecycle_only',
                              'release_eligible':False,'binary_sha256':sha(self.root/'bin')})
        with self.assertRaisesRegex(ValueError,'release-ineligible or scoped'):
            self.audit()
    def test_binary_only_receipt_is_rejected(self):
        self.write('receipt',{'binary_sha256':sha(self.root/'bin')})
        with self.assertRaisesRegex(ValueError,'release-ineligible or scoped'):
            self.audit()
    def test_failed_tests_are_rejected_after_rehash(self):
        self.value['tests']={'selected':12,'passed':9,'skipped':2,'failed':1,'leaked':0}; self.refresh_receipt()
        with self.assertRaisesRegex(ValueError,'compiled tests'):
            self.audit()
    def test_all_skipped_is_rejected_after_rehash(self):
        self.value['tests']={'selected':12,'passed':0,'skipped':12,'failed':0,'leaked':0}
        (self.root/'stderr').write_text('12 selected; 0 passed; 12 skipped.\n')
        self.value['stderr']['sha256']=sha(self.root/'stderr'); self.refresh_receipt()
        with self.assertRaisesRegex(ValueError,'compiled tests'):
            self.audit()
    def test_failed_resource_guard_is_rejected_after_rehash(self):
        self.resource['pass']=False; self.resource['returncode']=1; self.write('resource',self.resource)
        self.value['resource']['sha256']=sha(self.root/'resource'); self.refresh_receipt()
        with self.assertRaisesRegex(ValueError,'resource guard'):
            self.audit()
    def test_filtered_build_is_rejected_even_with_rehashed_evidence(self):
        self.value['build']['command'].append('-Dtest-filter=context')
        self.resource['command']=self.value['build']['command']; self.write('resource',self.resource)
        self.value['resource']['sha256']=sha(self.root/'resource'); self.refresh_receipt()
        with self.assertRaisesRegex(ValueError,'guarded command'):
            self.audit()
    def test_contradictory_build_option_is_rejected_even_with_rehashed_evidence(self):
        self.value['build']['command'].append('-Dmetal=false')
        self.resource['command']=self.value['build']['command']; self.write('resource',self.resource)
        self.value['resource']['sha256']=sha(self.root/'resource'); self.refresh_receipt()
        with self.assertRaisesRegex(ValueError,'guarded command'):
            self.audit()
    def test_unknown_build_option_is_rejected_even_with_rehashed_evidence(self):
        self.value['build']['command'].append('-Dfoo=true')
        self.resource['command']=self.value['build']['command']; self.write('resource',self.resource)
        self.value['resource']['sha256']=sha(self.root/'resource'); self.refresh_receipt()
        with self.assertRaisesRegex(ValueError,'guarded command'):
            self.audit()
    def test_environment_sanitization_policy_cannot_be_weakened(self):
        self.value['build']['environment']={'inherit_non_runtime_environment':True,'cleared_prefixes':['TERMITE_'],
                                            'set_by_command':{'ANTFLY_ZIG_MAX_RSS':'8589934592'}}
        self.refresh_receipt()
        with self.assertRaisesRegex(ValueError,'build configuration'):
            self.audit()
    def test_forged_tool_identity_is_rejected(self):
        self.value['tools']['zig']['sha256']='0'*64
        self.refresh_receipt()
        with self.assertRaisesRegex(ValueError,'build tool changed'):
            self.audit()
    def test_compiler_error_is_rejected_after_rehash(self):
        (self.root/'stderr').write_text('error: failed\n12 selected; 10 passed; 2 skipped.\n')
        self.value['stderr']['sha256']=sha(self.root/'stderr'); self.refresh_receipt()
        with self.assertRaisesRegex(ValueError,'compiler log'):
            self.audit()
    def test_missing_source_archive_member_is_rejected_after_rehash(self):
        self.archive.unlink(); self.write_archive(include=False)
        self.value['source_archive'].update(sha256=sha(self.archive),bytes=self.archive.stat().st_size); self.refresh_receipt()
        with self.assertRaisesRegex(ValueError,'membership'):
            self.audit()
    def test_changed_dirty_source_is_rejected_after_rehash(self):
        self.archive.unlink(); self.write_archive(source=b'forged\n')
        self.value['source_archive'].update(sha256=sha(self.archive),bytes=self.archive.stat().st_size); self.refresh_receipt()
        with self.assertRaisesRegex(ValueError,'archived source digest'):
            self.audit()

if __name__=='__main__': unittest.main()
