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

"""Run qualification entry points with recording runtimes, without models or GPUs."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "zig/pkg/inference/scripts/inference_cli.sh"


class InferenceScriptCommandsTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory(prefix="antfly-script-command-")
        self.addCleanup(self.scratch.cleanup)
        self.directory = Path(self.scratch.name)
        self.record = self.directory / "calls.jsonl"
        self.runtime = self.directory / "runtime-template"
        self.runtime.write_text(
            f"#!{sys.executable}\n"
            "import json, os, pathlib, sys\n"
            "with open(os.environ['RECORD_RUNTIME_CALLS'], 'a') as f:\n"
            " f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
            "if '--json-timing' in sys.argv:\n"
            " pathlib.Path(sys.argv[sys.argv.index('--json-timing')+1]).write_text('{}')\n"
        )
        self.runtime.chmod(0o755)
        self.env = {
            **os.environ,
            "RECORD_RUNTIME_CALLS": str(self.record),
            "PROMPT_FILTER": "ants_chat",
            "SPEC_KS": "1",
            "ASSISTANTS_EXTRA": "",
            "OUT_DIR": str(self.directory / "output"),
        }
        for key in ("ANTFLY_BIN", "BIN", "ROOT_DIR"):
            self.env.pop(key, None)

    def run_script(self, script, *args, env=None):
        result = subprocess.run(
            ["bash", str(ROOT / script), *args],
            cwd=self.directory,
            env={**self.env, **(env or {})},
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return [json.loads(line) for line in self.record.read_text().splitlines()]

    def test_gate_builds_and_invokes_default_development_binary(self):
        repo = self.directory / "repo"
        package = repo / "zig/pkg/inference"
        (package / "scripts").mkdir(parents=True)
        shutil.copyfile(HELPER, package / "scripts/inference_cli.sh")
        zig = self.directory / "zig"
        zig.write_text(
            f"#!{sys.executable}\n"
            "import os, pathlib, shutil\n"
            "dest = pathlib.Path('zig-out/bin/antfly-inference-dev')\n"
            "dest.parent.mkdir(parents=True, exist_ok=True)\n"
            "shutil.copyfile(os.environ['RUNTIME_TEMPLATE'], dest)\n"
            "dest.chmod(0o755)\n"
        )
        zig.chmod(0o755)
        calls = self.run_script(
            "scripts/gemma4_cuda_production_gate.sh",
            env={
                "ROOT_DIR": str(repo),
                "ZIG": str(zig),
                "RUNTIME_TEMPLATE": str(self.runtime),
                "RUN_BUILD": "1",
                "RUN_SMOKE": "1",
                "RUN_MICROBENCH": "0",
                "RUN_DEFAULT_POLICY": "0",
                "RUN_TARGET_ONLY": "0",
                "RUN_12B_MTP": "0",
                "RUN_E2B_MTP": "0",
            },
        )
        self.assertEqual(calls, [["cuda-info", "--smoke"]])

    def test_benchmarks_accept_development_and_unified_cli_overrides(self):
        for script in ("scripts/bench_gemma4_mtp.sh", "scripts/bench_gemma4_e2b.sh"):
            for binary in ("antfly-inference-dev", "antfly"):
                with self.subTest(script=script, binary=binary):
                    self.record.unlink(missing_ok=True)
                    runtime = self.directory / binary
                    shutil.copyfile(self.runtime, runtime)
                    runtime.chmod(0o755)
                    args = (
                        ("target-model", "assistant-model")
                        if script.endswith("mtp.sh")
                        else ()
                    )
                    calls = self.run_script(script, *args, env={"BIN": str(runtime)})
                    self.assertTrue(calls)
                    self.assertTrue(
                        all("Write one sentence about ants." in call for call in calls)
                    )
                    self.assertTrue(any("--draft-model" in call for call in calls))
                    prefix = (
                        ["inference", "generate"]
                        if binary == "antfly"
                        else ["generate"]
                    )
                    self.assertTrue(
                        all(call[: len(prefix)] == prefix for call in calls), calls
                    )

    def test_gate_accepts_a_unified_cli_override(self):
        runtime = self.directory / "antfly"
        shutil.copyfile(self.runtime, runtime)
        runtime.chmod(0o755)
        calls = self.run_script(
            "scripts/gemma4_cuda_production_gate.sh",
            env={
                "BIN": str(runtime),
                "RUN_BUILD": "0",
                "RUN_SMOKE": "1",
                "RUN_MICROBENCH": "0",
                "RUN_DEFAULT_POLICY": "0",
                "RUN_TARGET_ONLY": "0",
                "RUN_12B_MTP": "0",
                "RUN_E2B_MTP": "0",
            },
        )
        self.assertEqual(calls, [["inference", "cuda-info", "--smoke"]])

    def test_resolver_finds_the_monorepo_cli(self):
        repo = self.directory / "resolver"
        package = repo / "zig/pkg/inference"
        (repo / "zig/zig-out/bin").mkdir(parents=True)
        cli = repo / "zig/zig-out/bin/antfly"
        shutil.copyfile(self.runtime, cli)
        cli.chmod(0o755)
        result = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; ANTFLY_INFERENCE_ZIG_ROOT="$2/zig"; ANTFLY_INFERENCE_PKG_DIR="$2/zig/pkg/inference"; resolve_antfly_inference_bin',
                "bash",
                str(HELPER),
                str(repo),
            ],
            env=self.env,
            capture_output=True,
            text=True,
            check=True,
        )
        self.assertEqual(result.stdout.strip(), str(cli))


if __name__ == "__main__":
    unittest.main()
