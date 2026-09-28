"""Check the approved-PR workflow's legacy E2E resource setting."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("zig-antfly-e2e-pytest.sh")


class E2EWrapperTests(unittest.TestCase):
    def run_wrapper(
        self, *, github_actions, slots, process_workers=None, shard="ordinary"
    ):
        with tempfile.TemporaryDirectory() as directory:
            stub = Path(directory) / "uv"
            stub.write_text(
                f"#!{sys.executable}\n"
                "import json, os, sys\n"
                "print(json.dumps({'args': sys.argv[1:], "
                "'process_workers': os.environ.get('ANTFLY_E2E_PROCESS_WORKERS')}))\n"
            )
            stub.chmod(0o755)
            env = {
                **os.environ,
                "PATH": f"{directory}{os.pathsep}{os.environ['PATH']}",
                "GITHUB_ACTIONS": "true" if github_actions else "false",
                "ANTFLY_E2E_SUITE": "antfly",
                "ANTFLY_E2E_WORKERS": "4",
                "ANTFLY_E2E_PROCESS_SLOTS": str(slots),
            }
            env.pop("ANTFLY_E2E_SHARD", None)
            if shard is not None:
                env["ANTFLY_E2E_SHARD"] = shard
            env.pop("ANTFLY_E2E_PROCESS_WORKERS", None)
            if process_workers is not None:
                env["ANTFLY_E2E_PROCESS_WORKERS"] = str(process_workers)
            result = subprocess.run(
                [str(SCRIPT), "example.py::test_mixed"],
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
            payload = json.loads(result.stdout)
            args = payload["args"]
            return args[args.index("--e2e-process-slots") + 1], payload[
                "process_workers"
            ]

    def test_legacy_approved_pr_ci_uses_one_worker_with_two_slots(self):
        self.assertEqual(self.run_wrapper(github_actions=True, slots=1), ("2", "1"))

    def test_explicit_local_one_slot_limit_is_preserved(self):
        self.assertEqual(self.run_wrapper(github_actions=False, slots=1), ("1", None))

    def test_unsharded_ci_one_slot_limit_is_preserved(self):
        self.assertEqual(
            self.run_wrapper(github_actions=True, slots=1, shard=None),
            ("1", None),
        )

    def test_new_ci_setting_is_preserved(self):
        self.assertEqual(
            self.run_wrapper(github_actions=True, slots=2, process_workers=1),
            ("2", "1"),
        )


if __name__ == "__main__":
    unittest.main()
