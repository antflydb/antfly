import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
SCRIPTS = Path.cwd() / "zig/pkg/inference/scripts/gemma4"
sys.path.insert(0, str(SCRIPTS))
import qualify_native_cancellation as q

MARKER = "cuda_gqa_prefill_profile: status=active profile=required-fast route=prefill-fast batch=1 q_seq_len=1024 num_heads=8 num_kv_heads=2 head_dim=256"
ERROR = "attention_match_fit_error error=Cancelled"
PHASE = "attention_match_fit_rejected phase=fit elapsed_ns=100 attempted_chunks=4 completed_chunks=1"


class LogFormatTests(unittest.TestCase):
    def test_structured_plain_and_scoped_stderr_records(self):
        for data in [
            f"info: {MARKER}\n".encode(),
            json.dumps({"msg": MARKER}).encode(),
            f"info(cuda): {MARKER}\r\n".encode(),
        ]:
            with self.subTest(data=data):
                self.assertEqual(q.diagnostic_messages(data), [MARKER])

    def test_bare_payload_malformed_json_and_nonstring_messages_do_not_count(self):
        for data in [
            MARKER.encode(),
            b'{"msg":',
            b'{"msg":null}',
            b'{"msg":123}',
            b"[]",
            b"null",
            json.dumps("info: " + MARKER).encode(),
            b"\xff",
        ]:
            with self.subTest(data=data):
                self.assertEqual(q.diagnostic_messages(data), [])

    def test_plain_fitting_diagnostics_keep_error_and_chunk_validation(self):
        data = f"error: {ERROR}\ninfo: {PHASE}\n".encode()
        self.assertEqual(q.fitting_cancellation_evidence(data)["attempted_chunks"], 4)
        for invalid in [
            data.replace(b"Cancelled", b"CudaDriverError"),
            data.replace(b"attempted_chunks=4", b"attempted_chunks=0"),
            data + b"error: " + ERROR.encode() + b"\n",
        ]:
            with self.subTest(invalid=invalid), self.assertRaises(AssertionError):
                q.fitting_cancellation_evidence(invalid)

    def test_fresh_offset_ignores_stale_marker_and_embedded_message(self):
        with tempfile.TemporaryDirectory(dir=HERE) as directory:
            log = Path(directory) / "worker.log"
            log.write_text("info: " + MARKER + "\n")
            offset = log.stat().st_size
            with log.open("a") as f:
                f.write("info: request payload " + MARKER + "\n")
            with (
                patch.object(q.time, "monotonic", side_effect=[0, 0, 2]),
                patch.object(q.time, "sleep"),
                self.assertRaises(TimeoutError),
            ):
                q.wait_prefill_route(log, offset, 1)
            with log.open("a") as f:
                f.write("info: " + MARKER + "\n")
            self.assertEqual(q.wait_prefill_route(log, offset, 1), MARKER)

    def test_log_truncation_does_not_reuse_old_request(self):
        with tempfile.TemporaryDirectory(dir=HERE) as directory:
            log = Path(directory) / "worker.log"
            log.write_text("")
            with self.assertRaisesRegex(RuntimeError, "truncated"):
                q.wait_prefill_route(log, 100, 1)

    def test_observed_standalone_log_reproduces_the_fixed_parse(self):
        # Exact format observed in the failed standalone qualification log.
        data = (
            "info: prefill_chunk_plan: status=execute prompt_tokens=8189\n"
            "info: " + MARKER + "\n"
        ).encode()
        self.assertIn(MARKER, q.diagnostic_messages(data))
        old_messages = []
        for line in data.splitlines():
            try:
                record = json.loads(line)
            except (ValueError, UnicodeError):
                continue
            if isinstance(record, dict):
                old_messages.append(record.get("msg"))
        self.assertNotIn(MARKER, old_messages)


if __name__ == "__main__":
    unittest.main()
