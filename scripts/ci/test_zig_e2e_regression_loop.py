"""Check production-soak evidence without starting a server or building Zig."""

import os
import signal
import sys
import time
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("zig-e2e-regression-loop.sh").resolve()


class RegressionEvidenceTests(unittest.TestCase):
    def run_loop(
        self,
        *,
        mode="pass",
        workers=1,
        repeats=1,
        stale=False,
        autograph=False,
        cluster_restore=False,
        profile="",
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
                [str(SCRIPT.with_name("zig-e2e-cluster-restore-soak.sh"))]
                if cluster_restore
                else [str(SCRIPT.with_name("zig-e2e-autograph-soak.sh"))]
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
                    "ANTFLY_E2E_REGRESSION_PROFILE": profile,
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

    def test_cancellation_stops_parallel_workers_and_their_servers(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = root / "server.py"
            child.write_text(
                "import os, signal, time\nfrom pathlib import Path\n"
                "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                "p = Path(os.environ['HEARTBEAT_ROOT']) / str(os.getpid())\n"
                "while True:\n p.write_text(str(time.monotonic())); time.sleep(.01)\n"
            )
            uv = root / "uv"
            uv.write_text(
                f"#!{sys.executable}\nimport subprocess, sys, time\n"
                f"subprocess.Popen([sys.executable, {str(child)!r}])\n"
                "time.sleep(60)\n"
            )
            uv.chmod(0o755)
            heartbeats = root / "heartbeats"
            heartbeats.mkdir()
            process = subprocess.Popen(
                [str(SCRIPT), "example.py::test_restore"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env={
                    **os.environ,
                    "PATH": f"{root}{os.pathsep}{os.environ['PATH']}",
                    "HEARTBEAT_ROOT": str(heartbeats),
                    "SKIP_BUILD": "1",
                    "ANTFLY_E2E_ENV_LOADED": "1",
                    "ANTFLY_E2E_REGRESSION_REPEATS": "1",
                    "ANTFLY_E2E_REGRESSION_WORKERS": "2",
                    "TMPDIR": str(root),
                    "ANTFLY_E2E_REGRESSION_REPORT_DIR": str(root / "reports"),
                },
            )
            try:
                deadline = time.monotonic() + 10
                while (
                    len(list(heartbeats.iterdir())) < 2 and time.monotonic() < deadline
                ):
                    time.sleep(0.02)
                self.assertEqual(len(list(heartbeats.iterdir())), 2)
                process.send_signal(signal.SIGTERM)
                output, _ = process.communicate(timeout=15)
                self.assertEqual(process.returncode, 130, output)
                logs = list(
                    (root / "reports").glob("antfly-e2e-regression.*/worker-*.log")
                )
                self.assertEqual(len(logs), 2)
                self.assertTrue(
                    all("E2E regression worker=" in log.read_text() for log in logs)
                )
                time.sleep(0.1)
                stopped = {p: p.read_text() for p in heartbeats.iterdir()}
                time.sleep(0.1)
                self.assertEqual(
                    {p: p.read_text() for p in heartbeats.iterdir()}, stopped
                )
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
                for path in heartbeats.iterdir():
                    try:
                        os.kill(int(path.name), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_cluster_profile_selection_preserves_each_case_once(self):
        for profile in ("normal", "constrained"):
            result, reports = self.run_loop(
                cluster_restore=True, profile=profile, workers=2, repeats=2
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(len(reports), 8)
            self.assertTrue(all(name.startswith(profile + "/") for name in reports))

    def test_cluster_rejects_unknown_profile_before_running_tests(self):
        result, reports = self.run_loop(cluster_restore=True, profile="typo")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(reports, {})

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

    def test_cluster_restore_profiles_retain_every_case_and_constrain_descriptors(self):
        result, reports = self.run_loop(cluster_restore=True, workers=2, repeats=2)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(len(reports), 16)
        self.assertEqual(sum(path.startswith("normal/") for path in reports), 8)
        for path, body in reports.items():
            if path.startswith("constrained/"):
                self.assertIn('nofile="256"', body)

    def test_cluster_restore_runs_both_profiles_but_keeps_first_failure(self):
        result, reports = self.run_loop(cluster_restore=True, mode="normal-skip")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(len(reports), 4)


if __name__ == "__main__":
    unittest.main()
