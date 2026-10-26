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

import hashlib, json, os, tempfile, unittest, zipfile
from pathlib import Path
import release_attention_matching as release


def digest(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class CliBuildEvidence(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)
        self.prefix = self.root / "prefix"; (self.prefix / "bin").mkdir(parents=True)
        self.binary = self.prefix / "bin" / "antfly"; self.binary.write_bytes(b"binary"); self.binary.chmod(0o755)
        self.stdout = self.root / "build.stdout"; self.stdout.write_bytes(b"")
        self.stderr = self.root / "build.stderr"; self.stderr.write_bytes(b"compiled\n")
        self.resource = self.root / "resource.json"
        self.archive = self.root / "source.zip"; self.receipt = self.root / "receipt.json"
        self.source_name = "zig/pkg/inference/src/example.zig"; self.source_bytes = b"const x = 1;\n"
        self.command = ["/usr/bin/env", "ANTFLY_ZIG_MAX_RSS=8589934592", "python3",
            "tools/run_bounded_zig_build.py", "build", "antfly", "-j1", "-Dmetal=true",
            "-Dcuda=false", "-Donnx=false", "-Doptimize=ReleaseFast",
            "-Dantfly-version=context-test", "--prefix", str(self.prefix)]
        self.resource_value = {"schema":"antfly.benchmark_resources.v1","command":self.command,
            "elapsed_seconds":1.0,"initial_free_percent":60,"limits":dict(release.CLI_BUILD_LIMITS),
            "min_disk_free_mib":5000,"min_free_percent":50,"pass":True,"peak_group_rss_mib":2,
            "pid":123,"returncode":0,"samples":2,"swapout_growth_mib":0,"violation":None}
        self.write_resource()
        self.snapshot = {"schema_version":1,"evidence_scope":"build_only","created_utc":"2026-10-19T00:00:00+00:00",
            "head":"a"*40,"source":{"contract":"Git HEAD binds unchanged tracked inputs; files/deleted bind the dirty and untracked Zig build-input superset.",
                "files":{self.source_name:hashlib.sha256(self.source_bytes).hexdigest()},"deleted":[]},
            "build":{"cwd":"zig","command":self.command,"output":str(self.binary),"target":"antfly",
                "optimize":"ReleaseFast","metal":True,"cuda":False,"onnx":False},
            "zig_version":"0.17.0-dev","tools":{"orchestrator_sha256":"1"*64,"bounded_build_sha256":"2"*64,
                "resource_guard_sha256":"3"*64},"diagnostic_only":True,"serving_qualified":False}
        self.write_archive()
        self.value = dict(self.snapshot)
        self.value.update({"sealed_utc":"2026-10-19T00:01:00+00:00","build_completed":True,"sealed":True,
            "binary":str(self.binary),"binary_sha256":digest(self.binary),"binary_bytes":self.binary.stat().st_size,
            "build_resource_receipt":str(self.resource),"build_resource_receipt_sha256":digest(self.resource),
            "build_stdout":str(self.stdout),"build_stdout_sha256":digest(self.stdout),
            "build_stderr":str(self.stderr),"build_stderr_sha256":digest(self.stderr),
            "source_verified_after_build":True,"source_archive":str(self.archive),
            "source_archive_sha256":digest(self.archive),
            "source_archive_note":"Archive contains the source snapshot and every dirty/untracked build input; Git HEAD supplies unchanged tracked inputs."})
        self.write_receipt()
    def tearDown(self): self.tmp.cleanup()
    def write_resource(self): self.resource.write_text(json.dumps(self.resource_value,sort_keys=True)+"\n")
    def write_archive(self, source_bytes=None, include_source=True):
        with zipfile.ZipFile(self.archive,"w",compression=zipfile.ZIP_DEFLATED) as z:
            z.writestr("SOURCE-SNAPSHOT.json",json.dumps(self.snapshot,indent=2,sort_keys=True)+"\n")
            if include_source: z.writestr(self.source_name,self.source_bytes if source_bytes is None else source_bytes)
    def write_receipt(self): self.receipt.write_text(json.dumps(self.value,sort_keys=True)+"\n")
    def audit(self):
        return release.cli_build_evidence_contract(self.receipt,digest(self.receipt),digest(self.binary),self.binary)
    def test_valid_build_evidence_is_build_only(self):
        self.assertEqual({"build_evidence_only":True,"binary_sha256":digest(self.binary),
                          "source_archive_sha256":digest(self.archive),"head":"a"*40},self.audit())
    def test_missing_completion_cannot_be_rehashed(self):
        del self.value["build_completed"]; self.write_receipt()
        with self.assertRaisesRegex(ValueError,"scope"):
            self.audit()
    def test_binary_digest_only_json_is_not_build_evidence(self):
        self.receipt.write_text(json.dumps({"binary_sha256":digest(self.binary)})+"\n")
        with self.assertRaisesRegex(ValueError,"scope"):
            self.audit()
    def test_changed_compiler_mode_cannot_be_rehashed(self):
        self.value["build"]["optimize"]="Debug"; self.write_receipt()
        with self.assertRaisesRegex(ValueError,"configuration"):
            self.audit()
    def test_failed_guard_cannot_be_rehashed(self):
        self.resource_value["pass"]=False; self.resource_value["returncode"]=1; self.write_resource()
        self.value["build_resource_receipt_sha256"]=digest(self.resource); self.write_receipt()
        with self.assertRaisesRegex(ValueError,"resource guard failed"):
            self.audit()
    def test_missing_archive_member_cannot_be_rehashed(self):
        self.archive.unlink(); self.write_archive(include_source=False)
        self.value["source_archive_sha256"]=digest(self.archive); self.write_receipt()
        with self.assertRaisesRegex(ValueError,"membership"):
            self.audit()
    def test_changed_archived_source_cannot_be_rehashed(self):
        self.archive.unlink(); self.write_archive(source_bytes=b"forged\n")
        self.value["source_archive_sha256"]=digest(self.archive); self.write_receipt()
        with self.assertRaisesRegex(ValueError,"file digest"):
            self.audit()
    def test_compiler_error_log_cannot_be_rehashed(self):
        self.stderr.write_bytes(b"error: compile failed\n")
        self.value["build_stderr_sha256"]=digest(self.stderr); self.write_receipt()
        with self.assertRaisesRegex(ValueError,"compiler log"):
            self.audit()

if __name__ == "__main__": unittest.main()
