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

import hashlib, tempfile, unittest
from types import SimpleNamespace
from pathlib import Path
import context_serving_routes_qualify as q

H=lambda b: hashlib.sha256(b).hexdigest()
class Routes(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name)
        (self.root/'d').write_bytes(b'doc'); (self.root/'q').write_bytes(b'question')
        self.runtime={"model":"gemma4-e2b","max_context_tokens":8192,"max_kv_tokens":6144,
          "keep_prefix_tokens":128,"keep_recent_tokens":256,"optimize_decode":True,
          "optimize_prefill":True,"implementation_sha256":"5"*64,
          "profile_artifact_sha256":"6"*64,"profile_semantic_sha256":"b"*64}
        self.single={"route":"native","variant":"attention_matching","document_file":"d","document_sha256":H(b'doc'),
          "question_file":"q","question_sha256":H(b'question'),
          "request_content_sha256":q.structured_request_identity(b'doc',b'question'),"prompt_sha256":"2"*64,
          "prompt_tokens":100,"reserved_output_tokens":8,"settings_sha256":q.generation_settings(8),
          "variant_settings_sha256":q.variant_settings("attention_matching",self.runtime)}
        self.assertEqual("22cbd1044f5b33809b648c00a711e9c06264e4b0dd1136d58a3b7c525e9381b6",
            q.cli_request_identity({"document_file":"document.txt","document_sha256":"1"*64,
                "question_file":"question.txt","question_sha256":"2"*64,
                "reserved_output_tokens":32}, self.runtime))
    def tearDown(self): self.tmp.cleanup()
    def test_plan_requires_all_routes_and_real_mixed_membership(self):
        rows=[]
        for route in ("native","cli","http","stream"):
            row=dict(self.single); row['route']=route; rows.append(row)
        exact={k:v for k,v in self.single.items() if k!='route'}; exact['variant']='exact'
        exact['variant_settings_sha256']=q.variant_settings('exact',self.runtime)
        am={k:v for k,v in self.single.items() if k!='route'}
        rows.append({"route":"serial_mixed_batch","settings_sha256":q.generation_settings(8),"siblings":[exact,am]})
        plan=q.plan_contract({"schema_version":2,"cli_binary_sha256":"a"*64,"cli_source_build_sha256":"c"*64,
                              "runtime":self.runtime,"requests":rows},self.root)
        self.assertEqual(5,len(q.requests(plan)))
        rows[-1]['siblings']=[am,am]
        with self.assertRaisesRegex(ValueError,"one AM singleton"):
            q.plan_contract({"schema_version":2,"cli_binary_sha256":"a"*64,"cli_source_build_sha256":"c"*64,
                             "runtime":self.runtime,"requests":rows},self.root)
    def test_missing_route_rejected(self):
        with self.assertRaisesRegex(ValueError,"misses"):
            q.plan_contract({"schema_version":2,"cli_binary_sha256":"a"*64,"cli_source_build_sha256":"c"*64,
                             "runtime":self.runtime,"requests":[self.single]*5},self.root)

    def test_rehashed_policy_or_structured_input_cannot_reuse_settings(self):
        rows=[]
        for route in ("native","cli","http","stream"):
            rows.append(self.single|{"route":route})
        exact={k:v for k,v in self.single.items() if k!='route'}|{"variant":"exact"}
        exact["variant_settings_sha256"]=q.variant_settings("exact",self.runtime)
        am={k:v for k,v in self.single.items() if k!='route'}
        rows.append({"route":"serial_mixed_batch","settings_sha256":q.generation_settings(8),
                     "siblings":[exact,am]})
        drift={**self.runtime,"keep_recent_tokens":self.runtime["keep_recent_tokens"]+1}
        with self.assertRaisesRegex(ValueError,"settings identity"):
            q.plan_contract({"schema_version":2,"cli_binary_sha256":"a"*64,"cli_source_build_sha256":"c"*64,
                             "runtime":drift,"requests":rows},self.root)
        rows[0]={**rows[0],"request_content_sha256":"f"*64}
        with self.assertRaisesRegex(ValueError,"structured request identity"):
            q.plan_contract({"schema_version":2,"cli_binary_sha256":"a"*64,"cli_source_build_sha256":"c"*64,
                             "runtime":self.runtime,"requests":rows},self.root)

    def test_cli_report_binds_separate_binary_exact_argv_and_raw_logs(self):
        public=b'visible output'
        (self.root/'out').write_bytes(public+b'\n')
        (self.root/'err').write_bytes(b'')
        expected=self.single|{"route":"cli"}
        argv=q.expected_cli_argv(expected,self.runtime,"/qualified/antfly","http://127.0.0.1:43123")
        argv_bytes=q.cli_argv_preimage(argv)
        (self.root/'argv').write_bytes(argv_bytes)
        self.assertEqual("c9b045a10fa39207bf5ea77ca6ff4e77d0ff9546c740f892db35713df3ecf422",
                         H(q.cli_argv_preimage(["/qualified/antfly","inference","generate","gemma4-e2b","-","--server","http://127.0.0.1:43123","--stream"])))
        report={**self.single,"route":"cli","elapsed_ns":20,"live_bytes_before":7,
                "cli_process":{"separate_process":True,
                "binary_sha256":"a"*64,"argv_sha256":H(argv_bytes),
                "argv":{"path":"argv","sha256":H(argv_bytes)},
                "request_sha256":q.cli_request_identity(expected,self.runtime),
                "exit_code":0,"elapsed_ns":30,
                "public_output_sha256":H(public),"public_output_bytes":len(public),
                "public_output_match":True,
                "stdout":{"path":"out","sha256":H(public+b'\n')},
                "stderr":{"path":"err","sha256":H(b'')}}}
        release=SimpleNamespace(report_contract_v2=lambda *args:None,
                                loaded_serving_identity=lambda *args:11)
        audited=q.report_contract(report,expected,{},{},self.runtime,"a"*64,
                                  self.root,release)
        self.assertEqual(11,audited["load_generation"])
        forged=dict(report); forged["cli_process"]=dict(report["cli_process"])
        forged["cli_process"]["binary_sha256"]="d"*64
        with self.assertRaisesRegex(ValueError,"CLI child"):
            q.report_contract(forged,expected,{},{},self.runtime,"a"*64,self.root,release)

        # Rehashing a different actual argv cannot retain the sealed request.
        changed=list(argv); changed[changed.index("1")+0]="2"
        changed_bytes=q.cli_argv_preimage(changed)
        (self.root/'changed-argv').write_bytes(changed_bytes)
        forged=dict(report); forged["cli_process"]=dict(report["cli_process"])
        forged["cli_process"]["argv_sha256"]=H(changed_bytes)
        forged["cli_process"]["argv"]={"path":"changed-argv","sha256":H(changed_bytes)}
        with self.assertRaisesRegex(ValueError,"does not match sealed request"):
            q.report_contract(forged,expected,{},{},self.runtime,"a"*64,self.root,release)

        external=list(argv); external[6]="http://example.com:43123"
        external_bytes=q.cli_argv_preimage(external)
        (self.root/'external-argv').write_bytes(external_bytes)
        forged["cli_process"]["argv_sha256"]=H(external_bytes)
        forged["cli_process"]["argv"]={"path":"external-argv","sha256":H(external_bytes)}
        with self.assertRaisesRegex(ValueError,"loopback"):
            q.report_contract(forged,expected,{},{},self.runtime,"a"*64,self.root,release)

        for payload, message in ((public, "plus newline"),
                                 (public+b'\r\n', "plus newline"),
                                 (public+b'\n\n', "plus newline"),
                                 (b'reasoning'+public+b'\n', "plus newline")):
            (self.root/'forged-out').write_bytes(payload)
            forged=dict(report); forged["cli_process"]=dict(report["cli_process"])
            forged["cli_process"]["stdout"]={"path":"forged-out","sha256":H(payload)}
            with self.assertRaisesRegex(ValueError,message):
                q.report_contract(forged,expected,{},{},self.runtime,"a"*64,self.root,release)

        forged=dict(report); forged["cli_process"]=dict(report["cli_process"])
        forged["cli_process"]["public_output_sha256"]="f"*64
        with self.assertRaisesRegex(ValueError,"digest mismatch"):
            q.report_contract(forged,expected,{},{},self.runtime,"a"*64,self.root,release)

        forged=dict(report); forged["cli_process"]=dict(report["cli_process"])
        forged["cli_process"]["public_output_match"]=False
        with self.assertRaisesRegex(ValueError,"CLI child"):
            q.report_contract(forged,expected,{},{},self.runtime,"a"*64,self.root,release)
if __name__=='__main__': unittest.main()
