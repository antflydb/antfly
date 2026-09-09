"""Exercise evidence lifecycle, not model/hardware acceptance thresholds."""

import copy
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import qualify


def gate():
    return {
        "id": "example",
        "layer": "execution",
        "kind": "contract",
        "scope": "test fixture",
        "command": [
            sys.executable,
            "-c",
            'from pathlib import Path; Path(r\'${run}/native.json\').write_text(\'{"schema":"fixture.v1","pass":true}\')',
        ],
        "report": {
            "path": "${run}/native.json",
            "schema": "fixture.v1",
            "pass_field": "pass",
        },
    }


class QualificationRunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def run_gate(self, spec=None):
        return qualify.run_gate(spec or gate(), self.root, self.root / "run", {})

    def test_preserves_native_report_and_verdict(self):
        result = self.run_gate()
        self.assertTrue(result["pass"])
        self.assertEqual(
            qualify.sha256(self.root / "run/native.json"), result["report"]["sha256"]
        )
        self.assertTrue((self.root / "run/command.log").exists())

    def test_native_failure_missing_schema_and_truthy_nonboolean_fail(self):
        for payload in (
            {"schema": "fixture.v1", "pass": False},
            {"pass": True},
            {"schema": "wrong", "pass": True},
            {"schema": "fixture.v1", "pass": "true"},
        ):
            with self.subTest(payload=payload), tempfile.TemporaryDirectory(
                dir=self.root
            ) as folder:
                spec = gate()
                spec["command"][-1] = (
                    "from pathlib import Path; Path(r'${run}/native.json').write_text("
                    + repr(json.dumps(payload))
                    + ")"
                )
                result = qualify.run_gate(spec, self.root, Path(folder) / "gate", {})
                self.assertFalse(result["pass"])

    def test_success_report_cannot_override_nonzero_exit(self):
        spec = gate()
        spec["command"][-1] += "; raise SystemExit(3)"
        result = self.run_gate(spec)
        self.assertEqual(3, result["returncode"])
        self.assertFalse(result["pass"])

    def test_reports_outside_fresh_directory_are_rejected_before_launch(self):
        path = self.root / "stale.json"
        path.write_text('{"schema":"fixture.v1","pass":true}')
        spec = gate()
        spec["report"]["path"] = str(path)
        result = self.run_gate(spec)
        self.assertFalse(result["pass"])
        self.assertNotIn("returncode", result)

    def test_missing_report_or_executable_fails_and_is_retained(self):
        for command in (
            [sys.executable, "-c", "pass"],
            ["/nonexistent/qualification-binary"],
        ):
            with self.subTest(command=command), tempfile.TemporaryDirectory(
                dir=self.root
            ) as folder:
                spec = gate()
                spec["command"] = command
                result = qualify.run_gate(spec, self.root, Path(folder) / "gate", {})
                self.assertFalse(result["pass"])
                self.assertTrue((Path(folder) / "gate/gate.json").exists())

    def test_command_cannot_redirect_native_report_to_stale_evidence(self):
        stale = self.root / "stale.json"
        stale.write_text('{"schema":"fixture.v1","pass":true}')
        spec = gate()
        spec["command"][
            -1
        ] = f"from pathlib import Path; Path(r'${{run}}/native.json').symlink_to({str(stale)!r})"
        result = self.run_gate(spec)
        self.assertFalse(result["pass"])

    @patch.object(qualify, "source_state", return_value={"revision": "fixture"})
    def test_failed_gate_does_not_drop_later_evidence(self, _state):
        failing = gate()
        failing["command"] = [sys.executable, "-c", "raise SystemExit(1)"]
        succeeding = gate()
        succeeding["id"] = "later"
        result = qualify.run_plan(
            {"schema": qualify.PLAN_SCHEMA, "gates": [failing, succeeding]},
            self.root,
            self.root / "evidence",
            {},
        )
        self.assertTrue(result["complete"])
        self.assertFalse(result["pass"])
        self.assertTrue(result["gates"][1]["pass"])

    def test_checked_in_plans_validate(self):
        for path in Path(__file__).parent.glob("*.plan.json"):
            with self.subTest(path=path):
                qualify.validate_plan(qualify.read_json(path))

    def test_timeout_is_a_failed_result(self):
        spec = gate()
        spec["command"] = [sys.executable, "-c", "import time; time.sleep(60)"]
        spec["timeout_seconds"] = 0.05
        result = self.run_gate(spec)
        self.assertTrue(result["timed_out"])
        self.assertFalse(result["pass"])

    def test_cli_interrupts_clean_up_owned_children_and_retain_failure(self):
        repo = Path(__file__).resolve().parents[3]
        for signum in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signum=signum), tempfile.TemporaryDirectory(
                dir=self.root
            ) as temp:
                folder = Path(temp)
                spec = gate()
                spec["command"] = [
                    sys.executable,
                    "-c",
                    "import os,time; from pathlib import Path; marker = Path(r'${run}/ready.tmp'); marker.write_text(str(os.getpid())); marker.rename(marker.with_name('ready')); time.sleep(60)",
                ]
                plan = folder / "plan.json"
                plan.write_text(
                    json.dumps({"schema": qualify.PLAN_SCHEMA, "gates": [spec]})
                )
                output = folder / "evidence"
                # Reproduce a batch launcher inheriting SIGINT=SIG_IGN, without
                # preexec_fn hooks that are unsafe in multithreaded processes.
                command = [
                    sys.executable,
                    "-c",
                    "import os,signal,sys; signal.signal(signal.SIGINT, signal.SIG_IGN); os.execv(sys.executable, sys.argv[1:])",
                    sys.executable,
                    str(Path(qualify.__file__).resolve()),
                    "--repo",
                    str(repo),
                    "--plan",
                    str(plan),
                    "--output",
                    str(output),
                ]
                process = subprocess.Popen(
                    command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
                try:
                    ready = output / "example/ready"
                    deadline = time.monotonic() + 10
                    while (
                        not ready.exists()
                        and process.poll() is None
                        and time.monotonic() < deadline
                    ):
                        time.sleep(0.01)
                    self.assertTrue(ready.exists())
                    child_pid = int(ready.read_text())
                    process.send_signal(signum)
                    self.assertEqual(128 + signum, process.wait(timeout=10))
                    with self.assertRaises(ProcessLookupError):
                        os.kill(child_pid, 0)
                    summary = json.loads((output / "summary.json").read_text())
                    self.assertFalse(summary["pass"])
                    self.assertTrue(summary["interrupted"])
                    self.assertTrue(
                        json.loads((output / "example/gate.json").read_text())[
                            "interrupted"
                        ]
                    )
                finally:
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=10)

    def test_plan_validation_fails_closed(self):
        plan = {"schema": qualify.PLAN_SCHEMA, "gates": [gate()]}
        qualify.validate_plan(plan)
        invalid = []
        duplicate = copy.deepcopy(plan)
        duplicate["gates"].append(gate())
        invalid.append(duplicate)
        for field, value in (
            ("id", "../escape"),
            ("scope", ""),
            ("kind", "hardware"),
            ("timeout_seconds", float("nan")),
            ("command", "echo yes"),
        ):
            bad = copy.deepcopy(plan)
            bad["gates"][0][field] = value
            invalid.append(bad)
        for bad in invalid:
            with self.subTest(plan=bad), self.assertRaises(ValueError):
                qualify.validate_plan(bad)

    @patch.object(qualify, "source_state", return_value={"revision": "fixture"})
    def test_subset_does_not_require_unselected_models_or_pass_whole_plan(self, _state):
        model = gate()
        model.update(id="model", layer="model", kind="hardware", artifacts=["weights"])
        plan = {
            "schema": qualify.PLAN_SCHEMA,
            "artifacts": {"weights": "${unavailable_model}"},
            "gates": [gate(), model],
        }
        result = qualify.run_plan(
            plan, self.root, self.root / "evidence", {}, only="execution"
        )
        self.assertTrue(result["layers"]["execution"]["pass"])
        self.assertFalse(result["pass"])
        self.assertFalse(result["complete"])
        self.assertTrue(result["gates"][1]["not_run"])

    @patch.object(qualify, "source_state", return_value={"revision": "fixture"})
    def test_artifact_drift_disqualifies_successful_native_gate(self, _state):
        artifact = self.root / "binary"
        artifact.write_text("before")
        spec = gate()
        spec["artifacts"] = ["binary"]
        spec["command"][-1] += f"; Path({str(artifact)!r}).write_text('after')"
        plan = {
            "schema": qualify.PLAN_SCHEMA,
            "artifacts": {"binary": str(artifact)},
            "gates": [spec],
        }
        result = qualify.run_plan(plan, self.root, self.root / "evidence", {})
        self.assertTrue(result["gates"][0]["pass"])
        self.assertFalse(result["artifacts_unchanged"])
        self.assertFalse(result["pass"])

    @patch.object(qualify, "source_state", return_value={"revision": "fixture"})
    def test_successful_contract_plan_does_not_claim_all_layers(self, _state):
        plan = {"schema": qualify.PLAN_SCHEMA, "gates": [gate()]}
        result = qualify.run_plan(plan, self.root, self.root / "evidence", {})
        self.assertTrue(result["pass"])
        self.assertFalse(result["all_layers_represented"])
        with self.assertRaises(FileExistsError):
            qualify.run_plan(plan, self.root, self.root / "evidence", {})


if __name__ == "__main__":
    unittest.main()
