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

"""Exercise contract capabilities and the real workflow payload handoff."""

import json
import os
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from validate_source_contract import SUPPORTED_SCHEMAS, runtime_products

ROOT = Path(__file__).resolve().parents[2]
BUILD = (ROOT / ".github/workflows/antfly-artifact-build.yml").read_text()
PROMOTION = (ROOT / ".github/workflows/antfly-release.yml").read_text()


def step(workflow, name):
    return workflow.split(f"      - name: {name}\n", 1)[1].split("\n      - ", 1)[0]


def script(body):
    return textwrap.dedent(body.split("        run: |\n", 1)[1])


class SourceArtifactHandoffTests(unittest.TestCase):
    def test_every_supported_contract_reaches_the_payload_with_its_source(self):
        producer = BUILD.split("  package-embedded-zig-source:\n", 1)[1].split(
            "\n  package-release-source:", 1
        )[0]
        self.assertIn(
            "if: ${{ needs.release-plan.outputs.embedded_zig_source == 'true' }}",
            producer,
        )
        self.assertIn(
            "embedded_zig_source: ${{ steps.source-contract.outputs.embedded_zig_source }}",
            BUILD,
        )
        download = step(PROMOTION, "Download independently fetchable Apache Zig source")
        self.assertIn(
            "if: ${{ github.event_name == 'workflow_run' && steps.source-capabilities.outputs.embedded_zig_source == 'true' }}",
            download,
        )
        # The producer and consumer must exchange the same named artifact.
        self.assertIn("name: antfly-embedded-zig-source\n", producer)
        self.assertIn("name: antfly-embedded-zig-source\n", download)
        self.assertIn("path: dist/embedded-zig-source\n", download)
        capabilities = step(PROMOTION, "Resolve release source capabilities")
        self.assertIn("id: source-capabilities", capabilities)
        payload = step(PROMOTION, "Build the schema-versioned release payload")
        self.assertIn(
            "EMBEDDED_ZIG_SOURCE: ${{ steps.source-capabilities.outputs.embedded_zig_source }}",
            payload,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            commands = root / "bin"
            commands.mkdir()
            interpreter = root / "interpreter"
            interpreter.mkdir()
            (interpreter / "python").symlink_to(sys.executable)
            stub = commands / "python"
            stub.write_text(
                f"#!{sys.executable}\n"
                "import json,os,sys\n"
                "with open(os.environ['CALLS'], 'a') as f:\n"
                "    f.write(json.dumps(sys.argv[1:]) + '\\n')\n"
            )
            stub.chmod(0o755)
            for schema in SUPPORTED_SCHEMAS:
                with self.subTest(schema=schema):
                    output = root / f"capabilities-{schema}"
                    env = {
                        **os.environ,
                        "BUILD_CONTRACT_SCHEMA": str(schema),
                        "GITHUB_OUTPUT": str(output),
                        "PATH": str(interpreter) + os.pathsep + os.environ["PATH"],
                    }
                    # Execute the exact capability-resolution step with the
                    # authenticated source schema, rather than supplying test
                    # outputs that might diverge from production.
                    result = subprocess.run(
                        ["bash", "-eu", "-c", script(capabilities)],
                        cwd=ROOT,
                        env=env,
                        text=True,
                        capture_output=True,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    values = dict(
                        line.split("=", 1) for line in output.read_text().splitlines()
                    )
                    self.assertEqual(values["build_contract_schema"], str(schema))
                    self.assertEqual(
                        json.loads(values["runtime_products"]),
                        list(runtime_products(schema)),
                    )
                    self.assertEqual(
                        values["embedded_zig_source"], str(schema >= 5).lower()
                    )
                    calls = root / f"calls-{schema}"
                    subprocess.run(
                        ["bash", "-e", "-c", script(payload)],
                        cwd=ROOT,
                        env={
                            **env,
                            "PATH": str(commands) + os.pathsep + env["PATH"],
                            "CALLS": str(calls),
                            "EMBEDDED_ZIG_SOURCE": values["embedded_zig_source"],
                            "RELEASE_TAG": "v0.3.0-rc.3",
                            "RELEASE_VERSION": "0.3.0-rc.3",
                            "RELEASE_COMMIT": "a" * 40,
                            "PROMOTION_CONTROLLER_COMMIT": "b" * 40,
                        },
                        check=True,
                    )
                    arguments = [
                        json.loads(line) for line in calls.read_text().splitlines()
                    ]
                    self.assertEqual(
                        arguments[0][0], "scripts/release/verify_cli_snapshot.py"
                    )
                    build = arguments[1]
                    self.assertEqual(
                        build[0], "scripts/release/build_release_payload.py"
                    )
                    self.assertEqual("--embedded-source-dir" in build, schema >= 5)
                    if schema >= 5:
                        self.assertEqual(
                            build[build.index("--embedded-source-dir") + 1],
                            "dist/embedded-zig-source",
                        )

    def test_unknown_contract_cannot_emit_capabilities(self):
        for schema in (0, max(SUPPORTED_SCHEMAS) + 1):
            with (
                self.subTest(schema=schema),
                tempfile.TemporaryDirectory() as directory,
            ):
                output = Path(directory) / "output"
                result = subprocess.run(
                    [
                        sys.executable,
                        str(ROOT / "scripts/release/validate_source_contract.py"),
                        "--schema",
                        str(schema),
                        "--github-output",
                        str(output),
                    ],
                    text=True,
                    capture_output=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(
                    "unsupported release build contract schema", result.stderr
                )
                self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
