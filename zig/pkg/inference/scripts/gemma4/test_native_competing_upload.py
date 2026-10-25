import json
from pathlib import Path
import socket
import struct
import tempfile
import threading
from types import SimpleNamespace
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from qualify_native_concurrency import competing_request, parse_admission_metrics


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_POST(self):
        header = self.rfile.read(40)
        magic, metadata_bytes, count, flags, mime_bytes, attachment_flags, size = (
            struct.unpack("<8sQIIIIQ", header)
        )
        self.server.framing = (magic, count, flags, attachment_flags, size)
        self.server.metadata = json.loads(self.rfile.read(metadata_bytes))
        self.rfile.read(mime_bytes)
        self.connection.settimeout(0.1)
        try:
            self.server.payload = self.rfile.read(1)
        except socket.timeout:
            self.server.payload = b""
        body = json.dumps({"error": self.server.error_code}).encode()
        self.send_response(self.server.response_status)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Content-Type", "application/json")
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.close_connection = True


class CompetingUploadTests(unittest.TestCase):
    def exercise(self, status, error):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.response_status = status
        server.error_code = error
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory:
                checkpoint = Path(directory) / "checkpoint.kv"
                with checkpoint.open("wb") as file:
                    file.truncate(1024 * 1024)
                args = SimpleNamespace(port=server.server_port, checkpoint=checkpoint)
                body = {
                    "model": "fixture",
                    "messages": [{"role": "user", "content": "hello"}],
                }
                if status == 503 and error == "MODEL_RESOURCE_BUSY":
                    result = competing_request(args, body, "resume")
                    self.assertEqual(result["checkpoint_payload_bytes_sent"], 0)
                    self.assertEqual(server.payload, b"")
                    self.assertEqual(server.metadata, body)
                    self.assertEqual(
                        server.framing, (b"AFATT001", 1, 0, 0, 1024 * 1024)
                    )
                else:
                    with self.assertRaises(AssertionError):
                        competing_request(args, body, "resume")
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_valid_envelope_is_rejected_without_sending_checkpoint(self):
        self.exercise(503, "MODEL_RESOURCE_BUSY")

    def test_validation_error_does_not_count_as_busy(self):
        self.exercise(400, "INVALID_KV_CHECKPOINT")

    def test_unrelated_capacity_response_does_not_count(self):
        self.exercise(503, "UNRELATED_FAILURE")


class AdmissionEvidenceTests(unittest.TestCase):
    def fixture(self):
        return (
            "\n".join(
                "antfly_admission_inference_" + field + " " + str(value)
                for field, value in [
                    ("capacity_units", 48),
                    ("in_flight_units", 24),
                    ("available_units", 24),
                    ("in_flight_requests", 1),
                ]
            )
            + "\n"
        )

    def test_actual_held_upload_reservation_is_visible(self):
        row = parse_admission_metrics("# HELP unrelated\n" + self.fixture())
        self.assertEqual(row["available_units"], 24)
        self.assertEqual(row["in_flight_requests"], 1)

    def test_incomplete_duplicate_or_inconsistent_counters_fail(self):
        text = self.fixture()
        for altered in (
            "\n".join(text.splitlines()[:-1]),
            text + text.splitlines()[0] + "\n",
            text.replace("available_units 24", "available_units 25"),
            text.replace("in_flight_units 24", "in_flight_units -1"),
            text.replace("capacity_units 48", "capacity_units NaN"),
        ):
            with self.subTest(text=altered), self.assertRaises(AssertionError):
                parse_admission_metrics(altered)


if __name__ == "__main__":
    unittest.main()
