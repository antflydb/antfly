# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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

import hashlib
import io
import json
import struct
import tempfile
import unittest
from pathlib import Path
from native_checkpoint_transport import (
    MIME,
    MAX_CHECKPOINT,
    envelope_prefix,
    receive_checkpoint,
    send_resume,
)


def fixture(version=2, encoding="f32"):
    payload = b"payload for a transport-only fixture"
    header = struct.pack(
        "<8sIIQ", b"ANTFLYKV", version, int(encoding == "fp8"), len(payload)
    ) + bytes(range(128))
    checkpoint = header + payload + hashlib.sha256(header + payload).digest()
    metadata = {
        "version": "attention_match_v1",
        "wire_version": version,
        "encoding": encoding,
        "outcome": "compacted",
        "checkpoint_bytes": len(checkpoint),
    }
    return envelope_prefix(metadata, len(checkpoint)) + checkpoint, checkpoint


class TransportTests(unittest.TestCase):
    def test_resume_upload_completion_hook_precedes_response_wait(self):
        _, checkpoint = fixture()

        class Connection:
            def __init__(self):
                self.events = []

            def putrequest(self, method, path):
                self.events.append(("request", method, path))

            def putheader(self, name, value):
                self.events.append(("header", name, value))

            def endheaders(self):
                self.events.append(("headers_complete",))

            def send(self, data):
                self.events.append(("send", len(data)))

            def getresponse(self):
                self.events.append(("response",))
                return "response"

        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "checkpoint"
            source.write_bytes(checkpoint)
            connection = Connection()
            observed = []
            result = send_resume(
                connection,
                "/resume",
                {"request": "metadata"},
                source,
                on_upload_complete=lambda: (
                    connection.events.append(("upload_complete",)),
                    observed.append(True),
                ),
            )
        self.assertEqual(result, "response")
        self.assertEqual(observed, [True])
        kinds = [event[0] for event in connection.events]
        self.assertLess(kinds.index("upload_complete"), kinds.index("response"))

    def test_valid_bounded_transfer_and_independent_digest(self):
        frame, checkpoint = fixture()

        class ShortReader(io.BytesIO):
            def read(self, n=-1):
                self.assert_bound(n)
                return super().read(min(n, 7))

            @staticmethod
            def assert_bound(n):
                if not 0 < n <= 65536:
                    raise AssertionError(n)

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "checkpoint"
            result = receive_checkpoint(ShortReader(frame), destination)
            self.assertEqual(checkpoint, destination.read_bytes())
            self.assertEqual(
                hashlib.sha256(checkpoint).hexdigest(), result["file_sha256"]
            )
            self.assertEqual(bytes(range(32)).hex(), result["identity"]["weights"])

    def test_v3_encodings_and_metadata_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "checkpoint"
            for encoding in ("f16", "fp8"):
                frame, checkpoint = fixture(3, encoding)
                result = receive_checkpoint(io.BytesIO(frame), destination)
                self.assertEqual(result["metadata"]["encoding"], encoding)
                self.assertEqual(destination.read_bytes(), checkpoint)
                wrong = frame.replace(b'"wire_version":3', b'"wire_version":2', 1)
                with self.assertRaises(ValueError):
                    receive_checkpoint(io.BytesIO(wrong), destination)
                self.assertEqual(destination.read_bytes(), checkpoint)

    def test_v4_requires_optin_and_rejects_corruption_without_replacing_file(self):
        frame, checkpoint = fixture(4, "rotated4")
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "checkpoint"
            destination.write_bytes(b"old")
            with self.assertRaises(ValueError):
                receive_checkpoint(io.BytesIO(frame), destination)
            self.assertEqual(destination.read_bytes(), b"old")
            result = receive_checkpoint(
                io.BytesIO(frame), destination, allow_rotated4=True
            )
            self.assertEqual(result["metadata"]["wire_version"], 4)
            self.assertEqual(destination.read_bytes(), checkpoint)
            for size in range(len(frame)):
                with self.assertRaises(ValueError):
                    receive_checkpoint(
                        io.BytesIO(frame[:size]), destination, allow_rotated4=True
                    )
                self.assertEqual(destination.read_bytes(), checkpoint)
            for bad in (
                frame + b"x",
                frame[:-1] + bytes([frame[-1] ^ 1]),
                frame.replace(b'"rotated4"', b'"wrongfmt"'),
                fixture(4, "fp8")[0],
            ):
                with self.assertRaises(ValueError):
                    receive_checkpoint(
                        io.BytesIO(bad), destination, allow_rotated4=True
                    )
                self.assertEqual(destination.read_bytes(), checkpoint)
                self.assertFalse(destination.with_name("checkpoint.part").exists())

    def test_every_truncation_preserves_previous_file(self):
        frame, _ = fixture()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "checkpoint"
            destination.write_bytes(b"previous-valid-file")
            for length in range(len(frame)):
                with (
                    self.subTest(length=length),
                    self.assertRaises((ValueError, json.JSONDecodeError)),
                ):
                    receive_checkpoint(io.BytesIO(frame[:length]), destination)
                self.assertEqual(b"previous-valid-file", destination.read_bytes())
                self.assertFalse(destination.with_name("checkpoint.part").exists())

    def test_bad_integrity_header_mime_and_trailing_data(self):
        frame, _ = fixture()
        variants = [frame + b"x"]
        for offset in (
            0,
            16,
            20,
            24,
            28,
            frame.index(MIME),
            len(frame) - 33,
            len(frame) - 1,
        ):
            bad = bytearray(frame)
            bad[offset] ^= 1
            variants.append(bytes(bad))
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "checkpoint"
            for bad in variants:
                with self.assertRaises(ValueError):
                    receive_checkpoint(io.BytesIO(bad), destination)
                self.assertFalse(destination.exists())
                self.assertFalse(destination.with_name("checkpoint.part").exists())

    def test_limits_and_existing_staging_are_not_overwritten(self):
        with self.assertRaises(ValueError):
            envelope_prefix({}, MAX_CHECKPOINT + 1)
        frame, _ = fixture()
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "checkpoint"
            partial = destination.with_name("checkpoint.part")
            partial.write_bytes(b"other-transfer")
            with self.assertRaises(FileExistsError):
                receive_checkpoint(io.BytesIO(frame), destination)
            self.assertEqual(b"other-transfer", partial.read_bytes())


if __name__ == "__main__":
    unittest.main()
