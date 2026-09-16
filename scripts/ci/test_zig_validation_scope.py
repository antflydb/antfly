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

import shlex
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class ZigValidationScopeTests(unittest.TestCase):
    def test_vopr_changes_select_qualification_and_the_base_gate(self):
        workflow = (ROOT / ".github/workflows/zig-tests.yml").read_text()
        focused = workflow.split("      - name: Detect VOPR qualification changes", 1)[
            1
        ]
        inputs = {
            "zig/lib/vopr/src/runner.zig",
            "zig/pkg/antfly/src/vopr/cli.zig",
            "zig/pkg/antfly/src/storage/hot_standby/vopr.zig",
            "zig/pkg/antfly/src/storage/hot_standby/standby.zig",
            "zig/pkg/antfly/src/raft/transport/http_driver.zig",
            "zig/pkg/antfly/src/raft/transport/http_snapshot.zig",
            "zig/pkg/antfly/src/raft/host.zig",
            "zig/pkg/antfly/src/raft/reconciler.zig",
            "zig/pkg/antfly/src/raft/vopr_harness.zig",
            "zig/pkg/antfly/src/data/runtime.zig",
            "zig/build.zig",
            "scripts/ci/zig_vopr_soak.py",
            "scripts/ci/test_zig_vopr_soak.py",
            "scripts/ci/zig_vopr_qualification.py",
            ".github/workflows/zig-vopr-soak.yml",
        }
        unrelated = {"scripts/unrelated.py", "docs/guide.md"}
        for source in (workflow, focused):
            command = source.split("if ! scripts/ci/zig-relevant-changes.sh", 1)[1].split(
                "\n          then", 1
            )[0]
            pathspecs = shlex.split(command.split(" -- ", 1)[1].replace("\\\n", " "))
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

    def test_codegen_inputs_select_zig_validation(self):
        workflow = (ROOT / ".github/workflows/zig-tests.yml").read_text()
        command = workflow.split("if ! scripts/ci/zig-relevant-changes.sh", 1)[1].split(
            "\n          then", 1
        )[0]
        pathspecs = shlex.split(command.split(" -- ", 1)[1].replace("\\\n", " "))
        inputs = {
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


SCRIPT = ROOT / "scripts/ci/zig-relevant-changes.sh"


def _relevant(root: Path, base: str, head: str, *pathspecs: str) -> int:
    return subprocess.run(
        [str(SCRIPT), base, head, "--", *pathspecs],
        cwd=root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode


def _commit(root: Path, message: str) -> None:
    subprocess.run(["git", "add", "-A"], cwd=root, check=True)
    subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", message],
        cwd=root,
        check=True,
    )


class ZigRelevantChangesScriptTests(unittest.TestCase):
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
        self.assertEqual(_relevant(self.root, "no-such-revision", "HEAD", ":(glob)zig/**"), 0)

    def test_missing_separator_is_a_usage_error(self):
        code = subprocess.run(
            [str(SCRIPT), "HEAD", "HEAD", ":(glob)zig/**"],
            cwd=self.root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
