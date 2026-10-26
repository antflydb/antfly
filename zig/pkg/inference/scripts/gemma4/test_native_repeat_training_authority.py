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

import json, os, struct, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
import native_attention_matching as native
import native_repeat_training_authority as authority
import native_repeat_attention_matching as repeat_prepare

class RepeatAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup); self.root = Path(self.tmp.name)
        self.binary = self.root / "context-tests"; self.binary.write_bytes(b"bin"); self.binary.chmod(0o755)
        self.build = self.root / "build.json"; self.build.write_bytes(b"build")
        self.build_sha = native.sha(b"build"); self.binary_sha = native.sha(b"bin")
        self.patcher = patch.object(authority, "_validate_build", return_value={"receipt_sha256": self.build_sha,
            "binary_sha256": self.binary_sha, "binary_path": self.binary})
        self.patcher.start(); self.addCleanup(self.patcher.stop)
        self.identity = {"load_generation": 7, "artifact_sha256": "1"*64, "tokenizer_sha256": "2"*64,
            "template_sha256": "3"*64, "layout_sha256": "4"*64, "implementation_sha256": "5"*64}
        self.provenance = {"binary_sha256": self.binary_sha, "source_build_sha256": self.build_sha}
        self.document = {"id": "e4b-training", "sha256": "8"*64, "bucket": "document_qa"}
        self.request = {"session_id": "9"*64, "request_index": 0, "question_sha256": "a"*64,
                        "request_content_sha256": "b"*64}
        self.prompt_values = list(range(1025)); self.prompt = struct.pack("<1025q", *self.prompt_values)
        self.file("prompt.i64", self.prompt)
        coverage = {"schema_version": 2, "stage": "context_payload_block_coverage", "completed": True,
            "cancelled": False, "kv_readback_bytes": 0, "loaded_identity": self.identity, "provenance": self.provenance,
            "request": self.request, "document": self.document, "prompt_tokens": 1025,
            "prompt_tokens_sha256": native.sha(self.prompt), "qualified_context_tokens": 1057,
            "reserved_output_tokens": 32, "eligible_blocks": [{"start": 1, "end": 513}]}
        coverage_ref = self.json("coverage.json", coverage)
        inputs = {"schema_version": 1, "stage": "context_numerical_capture_inputs", "completed": True,
            "diagnostic_only": False, "loaded_identity": self.identity, "provenance": self.provenance,
            "request": self.request, "prompt": {"path": "prompt.i64", "sha256": native.sha(self.prompt), "count": 1025},
            "coverage_receipt": coverage_ref}
        self.inputs_ref = self.json("inputs.json", inputs)
        self.plan_path = self.root / "fixture-training-plan.json"
        authority.create_plan(source_build_receipt=self.build, capture_inputs=self.root/"inputs.json",
            capture_inputs_sha256=self.inputs_ref["sha256"], output_stem="fixture", owners=[{"owner_layer": 5, "source_heads": 2}],
            output=self.plan_path)
        self.plan_sha = native.sha(self.plan_path.read_bytes())
        self.make_capture()

    def file(self, name, data):
        path = self.root/name; path.write_bytes(data); return {"path": name, "sha256": native.sha(data), "bytes": len(data)}
    def json(self, name, value):
        data = json.dumps(value, sort_keys=True).encode(); self.file(name, data)
        return {"path": name, "sha256": native.sha(data)}
    def rewrite_json(self, name, value):
        path=self.root/name; data=json.dumps(value, sort_keys=True).encode(); path.write_bytes(data); return native.sha(data)

    def make_capture(self):
        strata=[{"consumer_layer":5,"query_head":q,"kv_head":q//4} for q in range(8)]
        keys=self.file("keys.f16",struct.pack("<4100e",*range(4100)))
        values=self.file("values.f16",struct.pack("<4100e",*range(4100)))
        head={"prefix_storage":"packed_exact","tensor_dtype":"f16_le","tensor_order":"logical_row_kv_head_head_dim",
            "owner_layer":5,"heads":2,"dim":2,"rows":1025,"block_start":0,"scale":.5,
            "prompt":{"tokens":1025,"sha256":native.sha(self.prompt)},"document":self.document,
            "loaded_identity":self.identity,"provenance":self.provenance,"strata":strata,"keys":keys,"values":values}
        href=self.json("head.json",head); href.update(owner_layer=5,block_start=0)
        index={"schema":authority.SOURCE_SCHEMA,"diagnostic_only":True,"serving_qualified":False,
            "prompt_tokens":1025,"prompt_tokens_sha256":native.sha(self.prompt),"artifact_sha256":self.identity["artifact_sha256"],
            "load_generation":7,"binary_sha256":self.binary_sha,"source_build_sha256":self.build_sha,
            "eligible_block_starts":[1],"descriptors":[href]}
        self.source_sha=self.rewrite_json("fixture-native-head-index.json",index)
        repeat_tokens=self.prompt_values[0:128]+self.prompt_values[-384:]
        token_ref=self.file("repeat.i64",struct.pack("<512q",*repeat_tokens)); owners=[]
        for h in range(2):
            row={"owner_layer":5,"kv_head":h,"source_heads":2,"head_dim":2,
                 "strata":[s for s in strata if s["kv_head"]==h],"training_rows":24,"validation_rows":8,
                 "training_group_rows":6,"validation_group_rows":2}
            for name,count,group,held in (("training",24,6,False),("validation",8,2,True)):
                raw=os.urandom(count*2*4); row[name]=self.file(f"{name}-h{h}.f32",raw)
                positions=authority._positions(1025,held,group)*4
                row[name+"_positions"]=self.file(f"{name}-h{h}.u32",struct.pack("<"+str(count)+"I",*positions))
            owners.append(row)
        repeat={"schema":authority.REPEAT_SCHEMA,"diagnostic_only":True,"evaluation_role":"internal_development",
            "serving_qualified":False,"fresh_unseen_qualified":False,"query_sampling":"representative_repeat_partition_v1",
            "selection":"history_only_cuda_study_v1","position_partition":"hashed_quartet_v1","training_plan_sha256":self.plan_sha,
            "source_kv_readback_bytes":0,"repeat_start":1025,"repeat_rows":512,"repeat_tokens":token_ref,"load_generation":7,
            "binary_sha256":self.binary_sha,"source_build_sha256":self.build_sha,"capture_inputs_manifest_sha256":self.inputs_ref["sha256"],
            "full_head_source_index":{"path":"fixture-native-head-index.json","sha256":self.source_sha},"owners":owners}
        self.repeat=repeat; self.repeat_sha=self.rewrite_json("fixture-native-repeat-index.json",repeat)
        command=["/usr/bin/env","MTL_DEBUG_LAYER=1","ANTFLY_CONTEXT_CAMPAIGN_CAPTURE=1","ANTFLY_CONTEXT_CAPTURE_NATIVE_FULL_HEADS=1",
            "ANTFLY_CONTEXT_CAPTURE_NATIVE_REPEAT=1","ANTFLY_CONTEXT_CAMPAIGN_PEAK_BYTES=8589934592",
            "ANTFLY_CONTEXT_CAPTURE_CAMPAIGN_DIR="+str(self.root),"ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST=inputs.json",
            "ANTFLY_CONTEXT_CAPTURE_INPUT_MANIFEST_SHA256="+self.inputs_ref["sha256"],"ANTFLY_CONTEXT_CAMPAIGN_CAPTURE_STEM=fixture",
            "ANTFLY_CONTEXT_SOURCE_BUILD_RECEIPT=build.json","ANTFLY_CONTEXT_NATIVE_REPEAT_TRAINING_PLAN_SHA256="+self.plan_sha,
            str(self.binary),"--test-filter","context Metal installed Gemma production query campaign capture"]
        resource={"pass":True,"returncode":0,"violation":None,"swapout_growth_mib":0,"remaining_child_group_live_processes":0,
            "limits":{"max_rss_mib":8192,"max_swap_growth_mib":0,"min_free_percent":15,"min_disk_free_mib":4096},"command":command}
        self.resource_sha=self.rewrite_json("resource.json",resource); self.file("resource.stdout",b"")
        stderr=("Metal API Validation Enabled\ncontext_native_repeat_capture: repeat_tokens=512 original_unchanged=true "
            "source_kv_readback_bytes=0 serving_qualified=false\n1 selected; 1 passed; 0 skipped.\n"
            f"context_native_repeat_artifact: index=fixture-native-repeat-index.json sha256={self.repeat_sha}\n").encode()
        self.file("resource.stderr",stderr)

    def kwargs(self):
        return dict(plan=self.plan_path,plan_sha256=self.plan_sha,source_index=self.root/"fixture-native-head-index.json",
            source_sha256=self.source_sha,repeat_source=self.root/"fixture-native-repeat-index.json",repeat_sha256=self.repeat_sha,
            capture_resource=self.root/"resource.json",resource_sha256=self.resource_sha)
    def refresh_artifact_log(self):
        stderr=self.root/"resource.stderr"; base=stderr.read_text().split("context_native_repeat_artifact:")[0]
        stderr.write_text(base+f"context_native_repeat_artifact: index=fixture-native-repeat-index.json sha256={self.repeat_sha}\n")
    def seal(self):
        out=self.root/"authority.json"; value=authority.seal_authority(**self.kwargs(),output=out)
        return out,native.sha(out.read_bytes()),value

    def test_canonical_seal_and_complete_validate_shape(self):
        path,digest,value=self.seal(); graph=authority.validate_authority(path,digest)
        self.assertEqual(set(graph),{"authority","plan","source_index","repeat","resource","paths","descriptors","digests"})
        self.assertEqual(set(graph["descriptors"]),{(5,0),(5,1)}); self.assertFalse(value["held_query_numerics_loaded"])
        self.assertEqual(value["cells"][1]["kv_head"],1); self.assertEqual(graph["paths"]["authority"],path)
        with self.assertRaises(FileExistsError): authority.seal_authority(**self.kwargs(),output=path)

    def test_authorized_preparer_materializes_exact_second_head(self):
        path,digest,_=self.seal(); output=self.root/"head1.bin"
        manifest=repeat_prepare.prepare(repeat_training_authority=path,repeat_authority_sha256=digest,
            owner=5,kv_head=1,output=output,min_recent_rows=512)
        payload=output.read_bytes()[native.HEADER.size:]
        all_keys=(self.root/"keys.f16").read_bytes(); all_values=(self.root/"values.f16").read_bytes()
        expected_keys=native.select_head_rows(all_keys,1025,2,2,1,2)
        expected_values=native.select_head_rows(all_values,1025,2,2,1,2)
        expected_queries=(self.root/"training-h1.f32").read_bytes()+(self.root/"validation-h1.f32").read_bytes()
        self.assertEqual(payload,expected_keys+expected_values+expected_queries)
        self.assertEqual(native.HEADER.unpack_from(output.read_bytes())[:6],(b"AMHEAD02",1025,2,24,8,2))
        self.assertEqual(manifest["training_authority_mode"],"native_repeat_v1")

    def test_preparer_rejects_mixed_authority_modes(self):
        path,digest,_=self.seal()
        with self.assertRaisesRegex(ValueError,"mixed or incomplete"):
            repeat_prepare.prepare(repeat_training_authority=path,repeat_authority_sha256=digest,
                repeat_source=self.root/"fixture-native-repeat-index.json",owner=5,kv_head=1,output=self.root/"mixed.bin")

    def test_missing_physical_head_rejected(self):
        self.repeat["owners"].pop(); self.repeat_sha=self.rewrite_json("fixture-native-repeat-index.json",self.repeat)
        self.refresh_artifact_log()
        with self.assertRaisesRegex(ValueError,"physical head"): authority.seal_authority(**self.kwargs())

    def test_stale_unbound_plan_rejected(self):
        self.repeat["training_plan_sha256"]="0"*64; self.repeat_sha=self.rewrite_json("fixture-native-repeat-index.json",self.repeat)
        with self.assertRaisesRegex(ValueError,"predeclared training plan"): authority.seal_authority(**self.kwargs())

    def test_capture_command_missing_plan_binding_rejected(self):
        resource=json.loads((self.root/"resource.json").read_text())
        resource["command"]=[x for x in resource["command"] if not x.startswith("ANTFLY_CONTEXT_NATIVE_REPEAT_TRAINING_PLAN_SHA256=")]
        self.resource_sha=self.rewrite_json("resource.json",resource)
        with self.assertRaisesRegex(ValueError,"training plan binding"): authority.seal_authority(**self.kwargs())

    def test_failed_resource_rejects_before_query_file_read(self):
        resource=json.loads((self.root/"resource.json").read_text()); resource["pass"]=False
        self.resource_sha=self.rewrite_json("resource.json",resource); (self.root/"training-h0.f32").unlink()
        with self.assertRaisesRegex(ValueError,"clean resource guards"): authority.seal_authority(**self.kwargs())

    def test_raw_source_hash_corruption_rejected(self):
        path=self.root/"keys.f16"; path.write_bytes(path.read_bytes()[:-1]+b"x")
        with self.assertRaisesRegex(ValueError,"SHA-256 mismatch"): authority.seal_authority(**self.kwargs())

    def test_held_query_nan_bytes_are_hashed_not_interpreted(self):
        raw=struct.pack("<16I",*([0x7fc00000]*16)); path=self.root/"validation-h1.f32"; path.write_bytes(raw)
        self.repeat["owners"][1]["validation"]={"path":path.name,"sha256":native.sha(raw),"bytes":len(raw)}
        self.repeat_sha=self.rewrite_json("fixture-native-repeat-index.json",self.repeat)
        # refresh the required artifact digest record only; no numeric validation occurs.
        stderr=self.root/"resource.stderr"; data=stderr.read_text().split("context_native_repeat_artifact:")[0]
        stderr.write_text(data+f"context_native_repeat_artifact: index=fixture-native-repeat-index.json sha256={self.repeat_sha}\n")
        result=authority.seal_authority(**self.kwargs()); self.assertFalse(result["held_query_numerics_loaded"])
        auth_path=self.root/"nan-authority.json"; authority.seal_authority(**self.kwargs(),output=auth_path)
        manifest=repeat_prepare.prepare(repeat_training_authority=auth_path,
            repeat_authority_sha256=native.sha(auth_path.read_bytes()),owner=5,kv_head=1,
            output=self.root/"nan-head.bin",min_recent_rows=512)
        self.assertFalse(manifest["held_numerics_used_for_selection"])

    def test_bad_positions_rejected(self):
        path=self.root/"validation-h0.u32"; raw=bytearray(path.read_bytes()); raw[:4]=struct.pack("<I",0); path.write_bytes(raw)
        self.repeat["owners"][0]["validation_positions"]["sha256"]=native.sha(raw)
        self.repeat_sha=self.rewrite_json("fixture-native-repeat-index.json",self.repeat)
        self.refresh_artifact_log()
        with self.assertRaisesRegex(ValueError,"disjoint uniform partition"): authority.seal_authority(**self.kwargs())

    def test_authority_digest_and_content_tamper_rejected(self):
        path,digest,value=self.seal()
        with self.assertRaisesRegex(ValueError,"SHA-256 mismatch"): authority.validate_authority(path,"0"*64)
        value["cells"].pop(); changed=json.dumps(value,indent=2,sort_keys=True).encode()+b"\n"; path.write_bytes(changed)
        with self.assertRaisesRegex(ValueError,"complete binding graph"): authority.validate_authority(path,native.sha(changed))

if __name__ == "__main__": unittest.main()
