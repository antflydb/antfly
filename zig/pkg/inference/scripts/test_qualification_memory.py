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

"""Memory evidence must fail closed, including phase coverage and attribution."""
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch
import qualify_read_pool_40g as q

class MemoryEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.args = SimpleNamespace(output=self.root / "out", cgroup=self.root / "cgroup", url="unused", pressure=False)
        self.args.cgroup.mkdir()
        for name, value in {"memory.current":"100", "memory.peak":"200", "memory.swap.current":"0", "cgroup.procs":"123"}.items():
            (self.args.cgroup / name).write_text(value)
        self.raw = "\n".join(f"antfly_inference_cuda_managed_{name}_bytes {value}" for name,value in {"live":10,"peak":20,"limit":38*(1<<30)}.items())
        self.gpu = SimpleNamespace(sample=lambda: {"gpu_util_percent":50,"nvml_compute_processes":[{"pid":123,"bytes":1000}]})
        self.gpu_patch = patch.object(q, "Gpu", return_value=self.gpu)
        self.gpu_patch.start(); self.addCleanup(self.gpu_patch.stop)
        self.http_patch = patch.object(q, "http", side_effect=lambda *a,**k: (200,self.raw,0.01))
        self.http_patch.start(); self.addCleanup(self.http_patch.stop)
        self.c = q.Campaign(self.args)

    def test_empty_evidence_is_incomplete(self):
        with self.assertRaises(q.EvidenceUnavailable): self.c.require_memory_evidence()

    def test_short_phases_each_have_boundary_samples(self):
        self.c.set_phase("warmup"); self.c.set_phase("mixed"); self.c.set_phase("recovery")
        self.c.require_memory_evidence()
        for phase in self.c.workload_phases:
            self.assertGreaterEqual(sum(s["phase"] == phase for s in self.c.samples),2)

    def test_missing_and_nonfinite_managed_metrics_are_incomplete(self):
        original = self.raw
        for raw in ("", original.replace("20", "nan"), original.replace("20", "-1")):
            with self.subTest(raw=raw):
                self.raw = raw
                c = q.Campaign(self.args); c.set_phase("mixed")
                with self.assertRaises(q.EvidenceUnavailable): c.require_memory_evidence()

    def test_physical_limit_breach_fails_instead_of_becoming_missing_evidence(self):
        self.raw = self.raw.replace("peak_bytes 20", f"peak_bytes {39*(1<<30)}")
        self.c.set_phase("mixed")
        with self.assertRaises(AssertionError): self.c.require_memory_evidence()

    def test_service_nvml_is_required_when_attribution_requested(self):
        for processes in (None, [], [{"pid":999,"bytes":1000}], [{"pid":123,"bytes":None}], [{"pid":123,"bytes":0}]):
            with self.subTest(processes=processes):
                self.gpu.sample = lambda: {"gpu_util_percent":None,"nvml_compute_processes":processes}
                c = q.Campaign(self.args); c.set_phase("mixed")
                with self.assertRaises(q.EvidenceUnavailable): c.require_memory_evidence()

    def test_sampling_failure_cannot_be_forgiven_by_later_success(self):
        self.c.set_phase("baseline")
        with patch.object(q,"http",side_effect=TimeoutError("telemetry timeout")):
            self.c.capture_sample()
        self.c.set_phase("recovery")
        with self.assertRaises(q.EvidenceUnavailable): self.c.require_memory_evidence()
        self.assertIn("telemetry timeout",self.c.evidence_errors[0]["error"])

    def test_missing_phase_or_cgroup_is_incomplete(self):
        self.c.set_phase("baseline")
        self.c.workload_phases.add("never-sampled")
        with self.assertRaises(q.EvidenceUnavailable): self.c.require_memory_evidence()
        (self.args.cgroup / "cgroup.procs").unlink()
        self.c.set_phase("mixed")
        with self.assertRaises(q.EvidenceUnavailable): self.c.require_memory_evidence()

    def test_nvml_can_only_be_omitted_when_explicitly_not_requested(self):
        self.args.cgroup = None
        self.gpu.sample = lambda: {"gpu_util_percent":None,"nvml_compute_processes":None}
        self.c.set_phase("mixed"); self.c.require_memory_evidence()

if __name__ == "__main__": unittest.main()
