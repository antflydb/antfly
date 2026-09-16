"""Check production-soak evidence without starting a server or building Zig."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("zig-e2e-regression-loop.sh").resolve()


class RegressionEvidenceTests(unittest.TestCase):
    def run_loop(
        self, *, mode="pass", workers=1, repeats=1, stale=False, autograph=False
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            reports = root / "reports"
            reports.mkdir()
            stub = root / "uv"
            stub.write_text(
                "#!/usr/bin/env python3\n"
                "import os, sys, resource\n"
                "from pathlib import Path\n"
                "path = next(arg.split('=', 1)[1] for arg in sys.argv if arg.startswith('--junitxml='))\n"
                "mode = os.environ['STUB_JUNIT_MODE']\n"
                "if mode != 'missing':\n"
                "    child = '<skipped/>' if mode == 'skip' or (mode == 'normal-skip' and '/normal/' in path) else ''\n"
                "    limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]\n"
                "    Path(path).write_text(f'<testsuites><testsuite><testcase nofile=\"{limit}\">' + child + '</testcase></testsuite></testsuites>')\n"
            )
            stub.chmod(0o755)
            if stale:
                (reports / "worker-1-case-1.xml").write_text("previous run")
            result = subprocess.run(
                [str(SCRIPT.with_name("zig-e2e-autograph-soak.sh"))]
                if autograph
                else [str(SCRIPT), "example.py::test_restore"],
                env={
                    **os.environ,
                    "PATH": f"{root}{os.pathsep}{os.environ['PATH']}",
                    "SKIP_BUILD": "1",
                    "ANTFLY_E2E_ENV_LOADED": "1",
                    "ANTFLY_E2E_REGRESSION_REPEATS": str(repeats),
                    "ANTFLY_E2E_REGRESSION_WORKERS": str(workers),
                    "ANTFLY_E2E_REGRESSION_REPORT_DIR": str(reports),
                    "STUB_JUNIT_MODE": mode,
                },
                check=False,
                capture_output=True,
                text=True,
                timeout=20,
            )
            return result, {
                str(p.relative_to(reports)): p.read_text()
                for p in reports.rglob("*.xml")
            }

    def test_every_worker_and_repetition_retains_distinct_evidence(self):
        result, reports = self.run_loop(workers=2, repeats=2)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(reports), 4)

    def test_skipped_case_does_not_qualify_a_soak(self):
        result, _ = self.run_loop(mode="skip")
        self.assertEqual(result.returncode, 1)

    def test_missing_report_does_not_qualify_a_soak(self):
        result, _ = self.run_loop(mode="missing")
        self.assertEqual(result.returncode, 1)

    def test_previous_evidence_is_never_overwritten(self):
        result, reports = self.run_loop(stale=True)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(reports["worker-1-case-1.xml"], "previous run")

    def test_autograph_profiles_keep_distinct_reports_and_constrain_descriptors(self):
        result, reports = self.run_loop(autograph=True, workers=2, repeats=2)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(reports), 16)
        self.assertEqual(sum(path.startswith("normal/") for path in reports), 8)
        for path, body in reports.items():
            if path.startswith("constrained/"):
                self.assertIn('nofile="256"', body)

    def test_autograph_runs_both_profiles_but_keeps_first_failure(self):
        result, reports = self.run_loop(autograph=True, mode="normal-skip")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(len(reports), 4)


if __name__ == "__main__":
    unittest.main()
