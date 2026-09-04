#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import tempfile
import textwrap
import unittest


GATE = pathlib.Path(__file__).resolve().with_name("gemma4_cuda_production_gate.sh")


class Gemma4CudaProductionGateTest(unittest.TestCase):
    def test_mtp_is_off_by_default(self) -> None:
        gate = GATE.read_text(encoding="utf-8")
        self.assertIn(
            "RUN_MTP                       auto|required|off (default: off; --mtp-only: required)",
            gate,
        )
        self.assertIn('default_run_mtp="off"', gate)

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = pathlib.Path(self.temp_dir.name)
        self.target = self.root / "target.gguf"
        self.draft = self.root / "draft.gguf"
        self.target.write_bytes(b"target")
        self.draft.write_bytes(b"draft")
        self.antfly = self.root / "fake-antfly.py"
        self.antfly.write_text(
            textwrap.dedent("""
            #!/usr/bin/env python3
            import json
            import os
            import pathlib
            import sys

            if "--print-token-ids" not in sys.argv:
                raise SystemExit("missing --print-token-ids")
            is_mtp = "--draft-model" in sys.argv
            raw_ids = os.environ["FAKE_MTP_IDS" if is_mtp else "FAKE_TARGET_IDS"]
            token_ids = [int(value) for value in raw_ids.split(",")]
            payload = {
                "tokens": len(token_ids),
                "token_ids": token_ids,
                "decode_tok_per_s": 120.0 if is_mtp else 100.0,
            }
            if is_mtp:
                payload["speculative"] = {
                    "speculation_policy": "auto",
                    "speculation_calibration": "probe",
                    "speculation_policy_decision": "active",
                    "mtp_enabled": True,
                    "rounds": 2,
                    "drafted": 2,
                }
            output = pathlib.Path(sys.argv[sys.argv.index("--json-timing") + 1])
            output.write_text(json.dumps(payload), encoding="utf-8")
            print("token_ids:", *token_ids)
        """).lstrip(),
            encoding="utf-8",
        )
        self.antfly.chmod(0o755)

    def run_gate(self, mtp_ids: str) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.pop("RUN_MTP", None)
        env.update(
            {
                "ANTFLY_BIN": str(self.antfly),
                "OUT_DIR": str(self.root / f"out-{mtp_ids.replace(',', '-')}"),
                "RUN_RESIDENT": "off",
                "MTP_TARGET_MODEL": str(self.target),
                "MTP_DRAFT_MODEL": str(self.draft),
                "MTP_TOKENS": "3",
                "MTP_MIN_ACTIVE_SPEED_RATIO": "0",
                "ANTFLY_GEMMA4_MTP_VERIFY_DEVICE_RESULT": "0",
                "FAKE_TARGET_IDS": "11,22,33",
                "FAKE_MTP_IDS": mtp_ids,
            }
        )
        return subprocess.run(
            [str(GATE), "--mtp-only"],
            env=env,
            text=True,
            capture_output=True,
            timeout=30,
        )

    def test_accepts_exact_target_mtp_token_parity(self) -> None:
        completed = self.run_gate("11,22,33")
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertIn("PASS mtp_policy", completed.stdout)

    def test_rejects_mtp_token_divergence(self) -> None:
        completed = self.run_gate("11,22,44")
        self.assertEqual(1, completed.returncode)
        self.assertIn(
            "MTP token_ids differ from target-only greedy token_ids", completed.stderr
        )


if __name__ == "__main__":
    unittest.main()
