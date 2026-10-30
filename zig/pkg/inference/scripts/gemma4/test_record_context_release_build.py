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

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile

import record_context_release_build as recorder


class FullReleaseRecorder(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name)/'repo'; self.root.mkdir()
        (self.root/'zig/tools').mkdir(parents=True)
        (self.root/'zig/pkg/inference/src').mkdir(parents=True)
        (self.root/'zig/build.zig').write_text('const std = @import("std");\n')
        (self.root/'zig/tools/run_bounded_zig_build.py').write_text('print("build")\n')
        (self.root/'zig/pkg/inference/src/root.zig').write_text('test {}\n')
        (self.root/'zig/config.json').write_text('{"generated":true}\n')
        (self.root/'zig/ignored.md').write_text('not a compiled/config input\n')
        (self.root/'zig/.gitignore').write_text('.zig-cache/\nzig-out/\n')
        subprocess.run(['git','init','-q'],cwd=self.root,check=True)
        subprocess.run(['git','add','zig'],cwd=self.root,check=True)
        self.original_git=recorder.git
        def uncommitted_index(root,*args):
            if args == ('rev-parse','HEAD'): return b'a'*40+b'\n'
            return subprocess.check_output(['git',*args],cwd=root)
        recorder.git=uncommitted_index
        (self.root/'zig/generated.json').write_text('{"untracked":true}\n')
        (self.root/'zig/.zig-cache').mkdir(); (self.root/'zig/.zig-cache/ignored.bin').write_bytes(b'cache')
        self.campaign=Path(self.tmp.name)/'campaign'; self.campaign.mkdir()
        self.tools=recorder.tool_identity()
    def tearDown(self): recorder.git=self.original_git; self.tmp.cleanup()
    def test_snapshot_archives_complete_git_visible_source_closure_before_build(self):
        head,source=recorder.source_snapshot(self.root)
        self.assertIn('zig/build.zig',source['files'])
        self.assertIn('zig/tools/run_bounded_zig_build.py',source['files'])
        self.assertIn('zig/config.json',source['files'])
        self.assertIn('zig/generated.json',source['files'])
        self.assertIn('zig/ignored.md',source['files'])
        self.assertNotIn('zig/.zig-cache/ignored.bin',source['files'])
        archive=recorder.freeze_archive(self.root,self.campaign,head,source,self.tools)
        with zipfile.ZipFile(archive) as bundle:
            manifest=json.loads(bundle.read(recorder.MANIFEST))
            self.assertEqual(set(manifest),{'head','source','build','tools'})
            self.assertNotIn('tests',manifest)
            self.assertEqual(set(bundle.namelist()),{recorder.MANIFEST,*source['files']})
        self.assertNotIn('-Dtest-filter', ' '.join(recorder.COMMAND))
        self.assertEqual(recorder.COMMAND.count('-Dmetal=true'),1)
    def test_tracked_deletion_is_bound(self):
        (self.root/'zig/config.json').unlink()
        _,source=recorder.source_snapshot(self.root)
        self.assertIn('zig/config.json',source['deleted'])
        self.assertNotIn('zig/config.json',source['files'])
    def test_gitlink_identity_is_bound_without_archiving_a_directory(self):
        subprocess.run(['git','update-index','--add','--cacheinfo',
                        '160000,'+'b'*40+',zig/deps/submodule'],cwd=self.root,check=True)
        _,source=recorder.source_snapshot(self.root)
        self.assertEqual(source['gitlinks'],{'zig/deps/submodule':'b'*40})
    def test_nonempty_uninitialized_gitlink_is_rejected(self):
        subprocess.run(['git','update-index','--add','--cacheinfo',
                        '160000,'+'b'*40+',zig/deps/submodule'],cwd=self.root,check=True)
        path=self.root/'zig/deps/submodule'; path.mkdir(parents=True); (path/'dirty.txt').write_text('dirty\n')
        with self.assertRaisesRegex(RuntimeError,'uninitialized gitlink worktree is not empty'):
            recorder.source_snapshot(self.root)
    def test_runtime_and_diagnostic_environment_is_cleared_and_restored(self):
        names={'ANTFLY_INFERENCE_TEST_RUNTIME_LIMIT':'1','ANTFLY_CONTEXT_MODEL_DIR':'/model',
               'TERMITE_METAL_TRACE_TO_HOST_LIMIT':'20','MTL_DEBUG_LAYER':'1','ZIG_LOCAL_CACHE_DIR':'/tmp/cache'}
        old={name:os.environ.get(name) for name in names}
        try:
            os.environ.update(names)
            with recorder.sanitized_environment():
                self.assertTrue(all(name not in os.environ for name in names))
            self.assertEqual({name:os.environ.get(name) for name in names},names)
        finally:
            for name,value in old.items():
                if value is None: os.environ.pop(name,None)
                else: os.environ[name]=value
    def test_symlinked_untracked_source_is_rejected(self):
        os.symlink(self.root/'zig/build.zig',self.root/'zig/link.zig')
        with self.assertRaisesRegex(RuntimeError,'symlink'):
            recorder.source_snapshot(self.root)
    def test_changed_source_cannot_be_sealed(self):
        head,source=recorder.source_snapshot(self.root)
        archive=recorder.freeze_archive(self.root,self.campaign,head,source,self.tools)
        (self.root/'zig/pkg/inference/src/root.zig').write_text('test { return error.Bad; }\n')
        resource=self.make_successful_evidence()
        with self.assertRaisesRegex(RuntimeError,'changed during build'):
            recorder.seal(self.root,self.campaign,'2026-10-19T00:00:00Z',head,source,archive,resource,self.tools)
    def test_successful_evidence_is_sealed_with_executed_binary(self):
        head,source=recorder.source_snapshot(self.root)
        archive=recorder.freeze_archive(self.root,self.campaign,head,source,self.tools)
        resource=self.make_successful_evidence()
        receipt=recorder.seal(self.root,self.campaign,'2026-10-19T00:00:00Z',head,source,archive,resource,self.tools)
        value=json.loads(receipt.read_text())
        self.assertTrue(value['release_eligible'])
        self.assertFalse(value['serving_qualified'])
        self.assertEqual(value['tests'],{'selected':12,'passed':10,'skipped':2,'failed':0,'leaked':0})
        self.assertEqual((self.campaign/value['binary']['path']).read_bytes(),b'executable')
    def test_verbose_inference_relative_binary_path_is_resolved(self):
        cache=self.root/'zig/.zig-cache/o'/('b'*32); cache.mkdir(parents=True)
        binary=cache/'context-tests'; binary.write_bytes(b'executable'); binary.chmod(0o755)
        log=f'cd pkg/inference/. && ./../../.zig-cache/o/{"b"*32}/context-tests\n'
        self.assertEqual(recorder.executed_binary(log,self.root/'zig'),binary.resolve())
    def test_larger_watchdog_is_bound_in_sealed_resource_receipt(self):
        recorder.configure_profile(True, 16384)
        try:
            head,source=recorder.source_snapshot(self.root)
            archive=recorder.freeze_archive(self.root,self.campaign,head,source,self.tools)
            resource=self.make_successful_evidence()
            receipt=recorder.seal(self.root,self.campaign,'2026-10-19T00:00:00Z',head,source,archive,resource,self.tools)
            value=json.loads(receipt.read_text())
            self.assertEqual(value['schema'],recorder.SCHEMA_V4)
            guard=json.loads((self.campaign/value['resource']['path']).read_text())
            self.assertEqual(guard['limits']['max_rss_mib'],16384)
            self.assertEqual(value['resource']['sha256'],recorder.digest(resource))
        finally:
            recorder.configure_profile(False)
    def test_all_skipped_is_not_a_successful_full_build(self):
        with self.assertRaisesRegex(RuntimeError,'compiled tests did not pass'):
            recorder.parse_tests('12 selected; 0 passed; 12 skipped.\n')
    def make_successful_evidence(self):
        cache=self.root/'zig/.zig-cache/o'/('a'*32); cache.mkdir(parents=True)
        binary=cache/'context-tests'; binary.write_bytes(b'executable'); binary.chmod(0o755)
        resource=self.campaign/'build-resource.json'
        values={'schema':'antfly.benchmark_resources.v1','command':recorder.COMMAND,'pass':True,
          'returncode':0,'violation':None,'limits':{'timeout_seconds':3600,'max_rss_mib':recorder.LIMITS['max_rss_mib'],
          'min_free_percent':15,'max_swap_growth_mib':512,'min_disk_free_mib':4096},'samples':2,
          'pid':123,'elapsed_seconds':1,'peak_group_rss_mib':100,'initial_free_percent':70,
          'min_free_percent':60,'swapout_growth_mib':0,'min_disk_free_mib':5000}
        resource.write_text(json.dumps(values)+'\n'); resource.with_suffix('.stdout').write_bytes(b'')
        profile = '' if recorder.SCHEMA == recorder.SCHEMA_V1 else 'context_compiler_profile: {"schema_version":1,"strip_debug_info":true,"have_error_return_tracing":false}\n'
        resource.with_suffix('.stderr').write_text(profile+f'{binary}\n12 selected; 10 passed; 2 skipped.\n')
        return resource


if __name__=='__main__': unittest.main()
