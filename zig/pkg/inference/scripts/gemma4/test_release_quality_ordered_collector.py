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

import hashlib
import json
from pathlib import Path
import sys

import quality_ordered_messages as ordered
import test_release_quality_collector as legacy


class ReleaseOrderedQualityCollectorTests(legacy.ReleaseQualityCollectorTests):
    def setUp(self):
        super().setUp()
        for case in self.plan["cases"]:
            document = (self.inputs / case["document_file"]).read_text()
            question = (self.inputs / case["question_file"]).read_text()
            artifact = {"schema_version": 1, "messages": [
                {"role": "user", "parts": [{"text": document, "selected": True}]},
                {"role": "assistant", "parts": [{"text": "stored"}]},
                {"role": "user", "parts": [{"text": question}]},
            ]}
            raw = json.dumps(artifact, sort_keys=True, separators=(",", ":")).encode()
            path = (self.inputs / case["document_file"]).parent / "messages.json"
            path.write_bytes(raw)
            for field in ("document_file", "document_sha256", "question_file", "question_sha256"):
                del case[field]
            case.update(messages_file=str(path.relative_to(self.inputs)),
                        messages_sha256=hashlib.sha256(raw).hexdigest(),
                        source_sha256=ordered.selected_source_digest(artifact),
                        selected_part_sha256s=ordered.selected_part_digests(artifact),
                        request_content_sha256=ordered.semantic_digest(artifact))
        self.plan["schema_version"] = 4

    def command(self, elapsed_ns=1000):
        base = {"schema_version": 2, "semantic_identity": self.identity, "provenance": self.origin,
                "loaded_identity": self.loaded, "completed": True, "exact_fallback": False,
                "elapsed_ns": elapsed_ns, "committed_before_bytes": 1000, "context_peak_bytes": 1200,
                "live_bytes_before": 10, "live_bytes_after_release": 10,
                "context_allocation_scope": legacy.release.CONTEXT_ALLOCATION_SCOPE,
                "context_allocation_scope_version": legacy.release.CONTEXT_ALLOCATION_SCOPE_VERSION,
                "context_allocation_scope_complete": True, "full_owner_physical_available": True,
                "model_execution_pool_admission_version": legacy.release.CONTEXT_EXECUTION_POOL_ADMISSION_VERSION,
                "model_execution_pool_admitted_backend_bytes": legacy.release.CONTEXT_EXECUTION_POOL_ADMITTED_BACKEND_BYTES,
                "model_execution_pool_shared_residency": True, "owner_released": True,
                "owner_post_release_live_bytes": 0, "backend_kv_readback_bytes": 0}
        code = (
            "import sys,json,hashlib,struct,pathlib,os\n"
            "sys.path.insert(0," + repr(str(Path(__file__).resolve().parent)) + ")\n"
            "import quality_ordered_messages as ordered\n"
            "base=json.loads(" + repr(json.dumps(base)) + ")\n"
            "for line in sys.stdin:\n"
            " c=json.loads(line)\n"
            " if c['command']=='stop': break\n"
            " root=pathlib.Path(os.environ['ANTFLY_CONTEXT_CAMPAIGN_DIR'])\n"
            " assert c.get('prompt_file') and (root/c['prompt_file']).is_file()\n"
            " raw=(root/c['messages_file']).read_bytes()\n"
            " assert hashlib.sha256(raw).hexdigest()==c['messages_sha256']\n"
            " artifact=ordered.loads(raw)\n"
            " assert ordered.semantic_digest(artifact)==c['request_content_sha256']\n"
            " tids=json.loads((root/c['reference_tokens_file']).read_text())\n"
            " out=[1,2]; decoded='café\\n'\n"
            " r={**base,**c,'document_sha256':None,'question_sha256':None,"
            "'source_sha256':ordered.selected_source_digest(artifact),"
            "'published':c['variant']=='attention_matching',"
            "'committed_after_bytes':700 if c['variant']=='attention_matching' else 1000,"
            "'committed_context_bytes':700 if c['variant']=='attention_matching' else 1000,"
            "'output_token_ids':out,'output_sha256':hashlib.sha256(b''.join(struct.pack('<I',x) for x in out)).hexdigest(),"
            "'output_utf8':decoded,'output_utf8_sha256':hashlib.sha256(decoded.encode()).hexdigest(),"
            "'reference_sequence_sha256':hashlib.sha256(b''.join(struct.pack('<I',x) for x in tids)).hexdigest(),"
            "'continuation_forward_steps':len(tids)-1,"
            "'published_compaction_forward_steps':len(tids)-1 if c['variant']=='attention_matching' else 0,"
            "'teacher_forced':[{'sequence_position':i,'token_id':x,'log_probability':p,"
            "'conditioned_on_token_id':tids[i-1] if i else None,"
            "'source':'prefill_final_row' if i==0 else 'published_compacted_context' if c['variant']=='attention_matching' else 'exact_context_continuation'}"
            " for i,(x,p) in enumerate(zip(tids,[-.1,-.2]))]}\n"
            " print(json.dumps(r),flush=True)\n")
        return [sys.executable, "-c", code]
