# Copyright 2026 Antfly, Inc.
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

"""Exercise the Zig workflow's actual Git path filter against tracked inputs."""

import json
import os
import re
import shlex
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class ZigValidationScopeTests(unittest.TestCase):
    def test_vopr_qualification_is_an_optional_admitted_suite(self):
        config = json.loads((ROOT / ".github/scripts/pr-ci-config.json").read_text())
        suite = next(suite for suite in config["suites"] if suite["id"] == "vopr")
        self.assertEqual(suite["label"], "ci:vopr")
        self.assertEqual(suite["workflow"], "zig-vopr-pr.yml")
        wrapper = (ROOT / ".github/workflows/zig-vopr-pr.yml").read_text()
        self.assertIn("uses: ./.github/workflows/pr-ci-admission.yml", wrapper)
        self.assertIn("suite: ${{ inputs.full_soak && 'soak' || 'vopr' }}", wrapper)
        self.assertIn("qualification_only: ${{ !inputs.full_soak }}", wrapper)
        workflow = (ROOT / ".github/workflows/zig-tests.yml").read_text()
        self.assertNotIn("vopr-qualification:", workflow)
        self.assertIn("vopr-test", workflow)
        self.assertIn("vopr-build", workflow)

    def test_full_validation_job_admission_and_build_reuse(self):
        workflow = (ROOT / ".github/workflows/zig-tests.yml").read_text()
        blocks = dict(
            re.findall(r"\n  ([\w-]+):\n(.*?)(?=\n  [\w-]+:\n|\Z)", workflow, re.DOTALL)
        )

        def selected(
            job, *, full=False, pr="7", event="workflow_dispatch", **overrides
        ):
            values = {
                "inputs.full_validation": full,
                "inputs.pr_number": pr,
                "inputs.validation_scope": "all",
                "github.run_attempt": 1,
                "github.event_name": event,
                "github.ref": "refs/heads/main",
                "github.event.schedule": "",
                "needs.admission.result": "success",
                "needs.changes.result": "success",
                "needs.changes.outputs.zig_tests": "true",
                "needs.changes.outputs.model_check": "true",
                "needs.changes.outputs.trace_validate": "true",
                "needs.changes.outputs.e2e": "true",
                "needs.e2e-base-build.result": "success" if pr else "skipped",
                "needs.e2e-base-plan.result": "success"
                if pr and not full
                else "skipped",
                "needs.e2e-full-build.result": "skipped" if pr else "success",
            }
            values.update(overrides)
            condition = blocks[job].split("${{", 1)[1].split("}}", 1)[0]
            condition = re.sub(
                r"\b(?:inputs|github|needs)\.[\w.-]+",
                lambda m: repr(values[m[0]]),
                condition,
            )
            condition = (
                condition.replace("!cancelled()", "True")
                .replace("success()", "True")
                .replace("always()", "True")
            )
            condition = (
                re.sub(r"!(?!=)", "not ", condition)
                .replace("&&", " and ")
                .replace("||", " or ")
            )
            return bool(eval(" ".join(condition.split()), {"__builtins__": {}}))

        full_jobs = [
            "zig-full-tests",
            "zig-build-cache-tests",
            "zig-full",
            "e2e-full-tests",
            "e2e-full",
            "arm64-codec",
        ]
        base_jobs = [
            "zig-base-tests",
            "zig-base",
            "e2e-base-plan",
            "e2e-base-tests",
            "e2e-base",
        ]
        for job in full_jobs:
            with self.subTest(job=job):
                self.assertTrue(selected(job, full=True))
                self.assertTrue(selected(job, pr="", event="push"))
                self.assertFalse(
                    selected(job, full=True, **{"needs.admission.result": "failure"})
                )
                self.assertFalse(selected(job, full=True, **{"github.run_attempt": 2}))
                if job != "arm64-codec":
                    self.assertFalse(selected(job))
        for job in base_jobs:
            self.assertTrue(selected(job))
            self.assertFalse(selected(job, full=True))
        self.assertTrue(selected("e2e-base-build", full=True))
        self.assertTrue(selected("e2e-base-low-fd"))
        self.assertTrue(selected("e2e-base-low-fd", full=True))
        self.assertFalse(selected("e2e-base-low-fd", pr="", event="push"))
        self.assertFalse(
            selected("e2e-base-tests", **{"needs.e2e-base-plan.result": "failure"})
        )
        self.assertFalse(selected("e2e-full-build", full=True))
        self.assertTrue(selected("e2e-full-build", pr="", event="push"))
        self.assertFalse(
            selected(
                "e2e-full-tests",
                full=True,
                **{"needs.e2e-base-build.result": "failure"},
            )
        )

        # Evaluate the actual aggregate shell, including independent low-FD
        # coverage. Full validation must not hide a failed specialized lane.
        aggregate = blocks["e2e-full"]
        self.assertIn("e2e-base-low-fd]", aggregate)
        self.assertIn("LOW_FD_RESULT: ${{ needs.e2e-base-low-fd.result }}", aggregate)
        command = textwrap.dedent(aggregate.split("run: |\n", 1)[1])
        for build, tests, low_fd, required, success in (
            ("success", "success", "success", "true", True),
            ("success", "success", "failure", "true", False),
            ("success", "success", "skipped", "true", False),
            ("success", "success", "skipped", "false", True),
            ("failure", "success", "success", "true", False),
            ("success", "failure", "success", "true", False),
        ):
            with self.subTest(
                build=build, tests=tests, low_fd=low_fd, required=required
            ):
                result = subprocess.run(
                    ["bash", "-c", command],
                    env={
                        **os.environ,
                        "BUILD_RESULT": build,
                        "TEST_RESULT": tests,
                        "LOW_FD_RESULT": low_fd,
                        "REQUIRE_LOW_FD": required,
                    },
                    capture_output=True,
                )
                self.assertEqual(result.returncode == 0, success, result.stderr)

    def test_codegen_and_laya_inputs_select_zig_validation(self):
        workflow = (ROOT / ".github/workflows/zig-tests.yml").read_text()
        command = workflow.split('if ! "$helper"', 1)[1].split("\n          then", 1)[0]
        pathspecs = shlex.split(command.split(" -- ", 1)[1].replace("\\\n", " "))
        inputs = {
            "scripts/laya/prepare_laya_finetune.py",
            "scripts/laya/qualify_laya_finetune_quality.py",
            "scripts/laya/test_run_laya_qualification.py",
            "scripts/laya/test_prepare_laya_finetune.py",
            "scripts/laya/test_qualify_laya_finetune_quality.py",
            "scripts/laya/test_benchmark_laya_metal.py",
            "scripts/openapi_inputs.py",
            "scripts/openapi_joiner.py",
            "scripts/join_openapi.py",
            "scripts/join_public_openapi.py",
            "scripts/public_openapi_overlays.py",
            "scripts/yaml_to_json.py",
            "scripts/generate_graph_identifier_policy.py",
            "scripts/generate_mcp_schema_fragments.py",
            "scripts/pyproject.toml",
            "scripts/uv.lock",
            "specs/openapi/new-owner/nested/new-schema.yaml",
            "openapi.yaml",
            "zig/lib/yacc/src/main.zig",
            "zig/tools/test_runtime_cache.py",
            "zig/tools/test_linked_tests.py",
            "zig/tools/test_audit_test_selection.py",
            "scripts/ci/test_zig_validation_scope.py",
        }
        unrelated = {
            "scripts/unrelated.py",
            "docs/guide.md",
            "zig/pkg/antfly/antfarm/index.html",
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(["git", "init", "-q", temporary], check=True)
            for name in inputs | unrelated:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            subprocess.run(["git", "add", "."], cwd=root, check=True)
            selected = subprocess.check_output(
                ["git", "ls-files", "-z", "--", *pathspecs], cwd=root
            )
        self.assertEqual(
            {path.decode() for path in selected.split(b"\0") if path}, inputs
        )


def embedded_helper() -> str:
    """Return the change-filter script exactly as the workflow writes it.

    The workflow runs from the default branch but checks out the PR head, so
    the filter is embedded in the workflow rather than read from the checkout;
    this test exercises that embedded text.
    """
    workflow = (ROOT / ".github/workflows/zig-tests.yml").read_text()
    match = re.search(
        r'cat > "\$helper" <<\'HELPER\'\n(.*?)\n {10}HELPER\n', workflow, re.DOTALL
    )
    assert match, "embedded zig-relevant-changes helper not found in zig-tests.yml"
    lines = [
        line[10:] if line.startswith(" " * 10) else line
        for line in match.group(1).splitlines()
    ]
    return "\n".join(lines) + "\n"


def _relevant(root: Path, base: str, head: str, *pathspecs: str) -> int:
    return subprocess.run(
        [str(SCRIPT), base, head, "--", *pathspecs],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    ).returncode


def _commit(root: Path, message: str) -> None:
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "-q",
            "-m",
            message,
        ],
        cwd=root,
        check=True,
    )


SCRIPT: Path


class ZigRelevantChangesScriptTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global SCRIPT
        cls.script_dir = tempfile.TemporaryDirectory()
        SCRIPT = Path(cls.script_dir.name) / "zig-relevant-changes.sh"
        SCRIPT.write_text(embedded_helper())
        SCRIPT.chmod(0o755)

    @classmethod
    def tearDownClass(cls):
        cls.script_dir.cleanup()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        subprocess.run(["git", "init", "-q", self.temporary.name], check=True)
        for name in (
            "zig/source.zig",
            "zig/DESIGN.md",
            "zig/pkg/inference/testdata/gliner25/README.md",
            "zig/pkg/inference/QUANT_KERNEL_COMPILER.md",
        ):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("v1\n")
        _commit(self.root, "base")

    def tearDown(self):
        self.temporary.cleanup()

    def _change(self, name: str, text: str = "v2\n") -> None:
        (self.root / name).write_text(text)

    def test_markdown_only_change_is_not_relevant(self):
        self._change("zig/DESIGN.md")
        _commit(self.root, "docs")
        self.assertEqual(_relevant(self.root, "HEAD~1", "HEAD", ":(glob)zig/**"), 1)

    def test_code_change_is_relevant(self):
        self._change("zig/source.zig")
        _commit(self.root, "code")
        self.assertEqual(_relevant(self.root, "HEAD~1", "HEAD", ":(glob)zig/**"), 0)

    def test_testdata_readme_is_relevant(self):
        self._change("zig/pkg/inference/testdata/gliner25/README.md")
        _commit(self.root, "fixture policy")
        self.assertEqual(_relevant(self.root, "HEAD~1", "HEAD", ":(glob)zig/**"), 0)

    def test_markdown_read_by_a_test_is_relevant(self):
        self._change("zig/pkg/inference/QUANT_KERNEL_COMPILER.md")
        _commit(self.root, "doc contract")
        self.assertEqual(_relevant(self.root, "HEAD~1", "HEAD", ":(glob)zig/**"), 0)

    def test_renaming_code_to_markdown_is_relevant(self):
        (self.root / "zig/source.zig").rename(self.root / "zig/source.md")
        _commit(self.root, "rename")
        self.assertEqual(_relevant(self.root, "HEAD~1", "HEAD", ":(glob)zig/**"), 0)

    def test_pathspec_outside_the_change_is_not_relevant(self):
        self._change("zig/source.zig")
        _commit(self.root, "code")
        self.assertEqual(_relevant(self.root, "HEAD~1", "HEAD", ":(glob)go/**"), 1)

    def test_git_failure_selects_tests(self):
        self.assertEqual(
            _relevant(self.root, "no-such-revision", "HEAD", ":(glob)zig/**"), 0
        )

    def test_missing_separator_is_a_usage_error(self):
        code = subprocess.run(
            [str(SCRIPT), "HEAD", "HEAD", ":(glob)zig/**"],
            cwd=self.root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
