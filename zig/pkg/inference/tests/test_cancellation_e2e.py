# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Real HTTP cancellation against production inference routes and supervision.

Only model computation is synthetic, so these tests need no GPU or downloads.
Every wait is bounded; cleanup owns the complete fixture process group.
"""

import http.client
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

FIXTURE = os.path.abspath(sys.argv.pop(1))


class CancellationE2E(unittest.TestCase):
    def start(self, mode):
        temp = tempfile.TemporaryDirectory(prefix="inference-cancellation-")
        self.addCleanup(temp.cleanup)
        model = Path(temp.name).resolve() / "fixture-model"
        model.mkdir()
        (model / "config.json").write_text(
            json.dumps(
                {
                    "model_type": "bert",
                    "hidden_size": 4,
                    "num_hidden_layers": 1,
                    "num_attention_heads": 1,
                    "intermediate_size": 8,
                    "vocab_size": 2,
                    "max_position_embeddings": 16,
                }
            )
        )
        # A minimal safetensors artifact lets the normal compatibility
        # gate discover a native bundle. Computation uses the resident fixture.
        header = json.dumps(
            {"fixture": {"dtype": "F32", "shape": [1], "data_offsets": [0, 4]}}
        ).encode()
        header += b" " * (-len(header) % 8)
        (model / "model.safetensors").write_bytes(
            len(header).to_bytes(8, "little") + header + b"\0" * 4
        )
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]
        self.model = model.name
        env = {
            k: v for k, v in os.environ.items() if not k.startswith("ANTFLY_INFERENCE_")
        }
        env.update(
            FIXTURE_MODEL_DIR=str(model), FIXTURE_PORT=str(self.port), FIXTURE_MODE=mode
        )
        # addCleanup closes the log after terminating the process group.
        self.log = tempfile.TemporaryFile(mode="w+b")  # noqa: SIM115
        self.addCleanup(self.log.close)
        self.proc = subprocess.Popen(
            [FIXTURE, "run"],
            env=env,
            stdout=self.log,
            stderr=self.log,
            start_new_session=True,
        )
        self.addCleanup(self.stop)
        return self.wait_state(lambda state: True)

    def stop(self):
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait(timeout=5)

    def request(self, method, path, body=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=1)
        try:
            conn.request(
                method,
                path,
                json.dumps(body) if body is not None else None,
                {"Content-Type": "application/json"},
            )
            response = conn.getresponse()
            return response.status, json.loads(response.read())
        finally:
            conn.close()

    def wait_state(self, predicate, timeout=10):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                break
            try:
                status, last = self.request("GET", "/_fixture/state")
                if status == 200 and predicate(last):
                    return last
            except (OSError, http.client.HTTPException, ValueError):
                pass
            time.sleep(0.025)
        self.fail(f"state condition timed out; last={last}; log={self.read_log()}")

    def read_log(self):
        # Do not move the file offset shared with the worker's stderr.
        size = os.fstat(self.log.fileno()).st_size
        return os.pread(self.log.fileno(), 12000, max(0, size - 12000)).decode(
            errors="replace"
        )

    def embed(self):
        status, result = self.request(
            "POST", "/ai/v1/embed", {"model": self.model, "input": ["ab"]}
        )
        self.assertEqual(status, 200, result)
        return result

    def exercise(self, mode):
        initial = self.start(mode)
        expected = self.embed()
        baseline = self.wait_state(
            lambda s: s["requests"] == 0 and s["units"] == 0 and s["active"] == 0
        )
        self.assertEqual(baseline["entered"], 1)
        self.assertEqual(self.request("POST", "/_fixture/arm")[0], 200)
        client = socket.create_connection(("127.0.0.1", self.port), timeout=2)
        self.addCleanup(client.close)
        body = json.dumps({"model": self.model, "input": ["ab"]}).encode()
        client.sendall(
            b"POST /ai/v1/embed HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: "
            + str(len(body)).encode()
            + b"\r\n\r\n"
            + body
        )
        running = self.wait_state(lambda s: s["active"] == 1)
        self.assertEqual(running["pid"], initial["pid"])
        self.assertEqual(running["entered"], 2)
        self.assertEqual(running["requests"], 1)
        self.assertGreater(running["units"], 0)
        self.assertGreater(running["scratch"], baseline["scratch"])
        # Abort with RST, not FIN: an orderly TCP half-close is legal while
        # waiting for the response and intentionally does not cancel HTTP/1.
        # Never set a cancellation token or invoke the watchdog directly.
        client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        client.close()
        if mode == "hard":
            recovered = self.wait_state(lambda s: s["pid"] != initial["pid"])
            with self.assertRaises(ProcessLookupError):
                os.kill(initial["pid"], 0)
            # A native crash also restarts a worker; it must not make this pass.
            log = self.read_log()
            self.assertIn("terminating supervised worker err=Cancelled", log)
            self.assertIn(".exited = 86", log)
        else:
            recovered = self.wait_state(
                lambda s: (
                    s["cancelled"] == 1
                    and s["active"] == 0
                    and s["requests"] == 0
                    and s["units"] == 0
                )
            )
            self.assertEqual(recovered["pid"], initial["pid"])
        self.assertEqual(recovered["scratch"], baseline["scratch"])
        self.assertIsNone(self.proc.poll(), "supervisor must survive cancellation")
        self.assertEqual(self.embed(), expected)
        completed = self.wait_state(
            lambda s: (
                s["active"] == 0
                and s["requests"] == 0
                and s["units"] == 0
                and s["scratch"] == baseline["scratch"]
            )
        )
        self.assertEqual(completed["pid"], recovered["pid"])
        self.assertEqual(completed["entered"], 1 if mode == "hard" else 3)

    def test_disconnect_unwinds_cooperative_inference(self):
        self.exercise("cooperative")

    def test_disconnect_replaces_uninterruptible_worker(self):
        self.exercise("hard")


if __name__ == "__main__":
    unittest.main()
