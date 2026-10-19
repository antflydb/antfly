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

"""Transactional streaming view staging and malformed producer frames."""

import json
import sys
import unittest
from unittest.mock import Mock, patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bench"))
from retained import FRAME_MAGIC, Payloads, View
from session_compare import Service


class FrameReaderTests(unittest.TestCase):
    def test_fragmented_record_still_enforces_response_allowance(self):
        service = Service.__new__(Service)
        service.pending = bytearray(b'{}\n{"text":"')
        service.cursor = service.wire_bytes = service.response_start = 3
        service.max_response_bytes = 16
        service.process = Mock()
        service.process.stdout.fileno.return_value = 17
        with (
            patch("session_compare.select.select", return_value=([17], [], [])),
            patch("session_compare.os.read", return_value=b"x" * 32),
            self.assertRaisesRegex(RuntimeError, "response budget exceeded"),
        ):
            service.read()
        service.process.kill.assert_called_once()
        service.process.wait.assert_called_once_with(timeout=5)

    def test_fragmented_records_search_each_byte_once_after_compaction(self):
        class CountedBuffer(bytearray):
            searched = 0

            def find(self, needle, start=0):
                self.searched += len(self) - start
                return super().find(needle, start)

        class Output:
            def fileno(self):
                return 17

        service = Service.__new__(Service)
        service.pending = CountedBuffer(b'{}\n{"text":"')
        service.cursor = 3
        service.wire_bytes = 3
        service.process = type("Process", (), {"stdout": Output()})()
        chunks = iter([b"x" * 1024] * 1024 + [b'"}\n{}\n'])
        with (
            patch("session_compare.select.select", return_value=([17], [], [])),
            patch("session_compare.os.read", side_effect=lambda *_: next(chunks)),
        ):
            self.assertEqual(service.read(), {"text": "x" * (1024 * 1024)})
            self.assertEqual(service.read(), {})
        # Includes the bounded suffix of the final read containing two records.
        self.assertLessEqual(service.pending.searched, 1024 * 1024 + 32)
        self.assertEqual(service.wire_bytes, 1024 * 1024 + 18)

    def read(self, raw):
        service = Service.__new__(Service)
        service.pending = bytearray(raw + b"\n")
        service.cursor = 0
        service.wire_bytes = 0
        return service.read()

    def test_utf8_and_byte_paths(self):
        value = {"type": "match", "text": "é雪", "path": {"bytes": "Yf8="}}
        self.assertEqual(
            self.read(json.dumps(value, ensure_ascii=False).encode()), value
        )

    def test_malformed_json_utf8_and_bom_are_rejected(self):
        for raw in (
            b"{",
            b'{"text":"\xff"}',
            b"\xef\xbb\xbf{}",
            b"\xff\xfe{\x00}\x00",
            b"{}{}",
            b"{} {}",
            b"{} trailing}",
        ):
            with self.subTest(raw=raw), self.assertRaises((ValueError, UnicodeError)):
                self.read(raw)

    def test_legal_json_whitespace_is_preserved(self):
        for raw in (b"{}", b" {}", b"\t{}\r", b"{} ", b'{ "text": "} {" }'):
            self.assertEqual(self.read(raw), json.loads(raw))


class StreamingViewTests(unittest.TestCase):
    def test_v3_batches_retain_original_frames_and_expand_at_materialization(self):
        p = Payloads()
        frames = [
            {
                "type": "begin",
                "file_id": "a",
                "data": {"path": {"text": "a"}},
                "source": {},
            },
            {
                "type": "batch",
                "file_id": "a",
                "result_schema_version": 3,
                "events": [
                    {"type": "match", "data": {"text": "é"}},
                    {"type": "context", "data": {"text": "snow"}},
                ],
            },
            {"type": "end", "file_id": "a", "data": {}},
        ]
        for row in frames:
            p.consume(row, json.dumps(row).encode())
        v = View("handle", revision="rev", token="token", files=p.finish())
        self.assertEqual(
            [r["type"] for r in v.materialize()], ["begin", "match", "context", "end"]
        )
        self.assertEqual(v.materialize()[1]["file_id"], "a")
        self.assertIn(b'"batch"', v.files[b"a"])

    def test_v4_tuples_decode_only_the_requested_file(self):
        frames = [
            {
                "type": "begin",
                "file_id": "a",
                "data": {"path": {"text": "a"}},
                "source": {},
            },
            {
                "type": "batch",
                "file_id": "a",
                "result_schema_version": 4,
                "rows": [
                    [0, 2, 9, {"text": "é STOP\n"}, []],
                    [1, 3, 17, {"bytes": "/w=="}, []],
                ],
            },
            {"type": "end", "file_id": "a", "data": {}},
        ]
        p = Payloads()
        for row in frames:
            p.consume(row, json.dumps(row).encode())
        v = View(
            "h",
            revision="rev",
            token="token",
            observed_at=42,
            files={**p.finish(), b"unread": b"deliberately invalid JSON"},
        )
        rows = v.materialize_file(b"a")
        self.assertEqual(
            [r["type"] for r in rows], ["begin", "match", "context", "end"]
        )
        self.assertEqual(rows[1]["data"]["line_number"], 2)
        self.assertEqual(rows[2]["data"]["lines"], {"bytes": "/w=="})
        self.assertEqual(rows[0]["source"], {"snapshot": "rev", "observed_at": 42})
        with self.assertRaises(KeyError):
            v.materialize_file(b"missing")

    def test_file_iteration_does_not_materialize_unread_large_frames(self):
        import tracemalloc

        p = Payloads()
        for row in (
            {
                "type": "begin",
                "file_id": "a",
                "data": {"path": {"text": "a"}},
                "source": {},
            },
            {
                "type": "match",
                "file_id": "a",
                "data": {"text": "x" * (2 * 1024 * 1024)},
            },
            {"type": "end", "file_id": "a", "data": {}},
        ):
            p.consume(row, json.dumps(row).encode())
        v = View("h", revision="rev", token="token", files=p.finish())
        tracemalloc.start()
        try:
            rows = v.iter_file(b"a")
            self.assertEqual(next(rows)["type"], "begin")
            self.assertLess(tracemalloc.get_traced_memory()[1], 128 * 1024)
            rows.close()
        finally:
            tracemalloc.stop()
        # Older serialized array views still expand through the same API.
        v.files[b"legacy"] = b'[{"type":"end","file_id":"old"}]'
        self.assertEqual(v.materialize_file(b"legacy")[0]["file_id"], "old")

    def test_malformed_v4_tuples_fail_before_publication(self):
        from retained import validate_batch

        for event in (
            [],
            [0, 1, 0, {}, []],
            [False, 1, 0, {"text": ""}, []],
            [0, -1, 0, {"text": ""}, []],
            [0, 1, 0, {"text": 4}, []],
            [2, 1, 0, {"text": ""}, []],
        ):
            with (
                self.subTest(event=event),
                self.assertRaisesRegex(RuntimeError, "invalid batch"),
            ):
                validate_batch(
                    {
                        "type": "batch",
                        "result_schema_version": 4,
                        "file_id": "a",
                        "rows": [event],
                    },
                    200,
                )

    def test_malformed_batches_are_rejected_before_staging(self):
        for events in (
            [],
            [{}],
            [{"type": "end"}],
            [{"type": "match", "file_id": "other"}],
            [{"type": "match"}] * 65,
        ):
            p = Payloads()
            p.consume(
                {"type": "begin", "file_id": "a", "data": {"path": {"text": "a"}}},
                b"{}",
            )
            with self.assertRaisesRegex(RuntimeError, "invalid batch"):
                p.consume(
                    {
                        "type": "batch",
                        "file_id": "a",
                        "result_schema_version": 3,
                        "events": events,
                    },
                    b"{}",
                )

    def test_decoded_invalid_batch_cannot_replace_certified_view(self):
        view = View("h", revision="old", token="token", files={b"p": b"[]"})
        rows = [
            {"type": "begin", "file_id": "a", "data": {"path": {"text": "p"}}},
            {
                "type": "batch",
                "result_schema_version": 3,
                "file_id": "a",
                "events": [{"type": "end", "data": {}}],
            },
            {"type": "end", "file_id": "a"},
            {"type": "summary", "stats": {"complete": True}},
            {"type": "result_token", "result_token": "new-token"},
            {"type": "workspace_end", "revision": "new", "exit_code": 0},
        ]
        with self.assertRaisesRegex(RuntimeError, "invalid batch"):
            view.apply(rows, "new", 1)
        self.assertEqual(
            (view.revision, view.token, view.files), ("old", "token", {b"p": b"[]"})
        )

    def test_original_frames_round_trip_unicode_and_byte_paths(self):
        p = Payloads()
        frames = [
            {
                "type": "begin",
                "file_id": "a",
                "data": {"path": {"bytes": "Yf8="}},
                "source": {"snapshot": "old", "observed_at": 0},
            },
            {"type": "match", "file_id": "a", "data": {"text": 'é\n"'}},
            {"type": "end", "file_id": "a", "data": {}},
        ]
        encoded = [json.dumps(row, ensure_ascii=False).encode() for row in frames]
        for row, raw in zip(frames, encoded):
            self.assertTrue(p.consume(row, raw))
        self.assertEqual(
            p.finish()[b"a\xff"], FRAME_MAGIC + b"\n".join(encoded) + b"\n"
        )
        v = View("handle")
        v.apply(
            [
                {"type": "summary", "stats": {"complete": True}},
                {"type": "result_token", "result_token": "token"},
                {"type": "workspace_end", "revision": "new", "exit_code": 0},
            ],
            "new",
            42,
            payloads=p,
        )
        actual = v.materialize()
        self.assertEqual(actual[1], frames[1])
        self.assertEqual(actual[0]["source"], {"snapshot": "new", "observed_at": 42})

    def test_interleaved_dense_files_own_immutable_payloads(self):
        payloads = Payloads()
        expected = {b"a": [], b"b": []}

        def consume(row):
            raw = json.dumps(row, ensure_ascii=False).encode()
            payloads.consume(row, raw)
            expected[row["file_id"].encode()].append(raw)

        for name in ("a", "b"):
            consume(
                {"type": "begin", "file_id": name, "data": {"path": {"text": name}}}
            )
        for number in range(2000):
            for name in ("a", "b"):
                consume(
                    {
                        "type": "match",
                        "file_id": name,
                        "data": {"text": "é" * 64, "line": number},
                    }
                )
        for name in ("a", "b"):
            consume({"type": "end", "file_id": name})
        actual = payloads.finish()
        self.assertFalse(payloads.open)
        for name, frames in expected.items():
            self.assertIsInstance(actual[name], bytes)
            self.assertEqual(actual[name], FRAME_MAGIC + b"\n".join(frames) + b"\n")

    def test_missing_end_and_duplicate_identity_are_rejected(self):
        p = Payloads()
        row = {"type": "begin", "file_id": "a", "data": {"path": {"text": "p"}}}
        p.consume(row, json.dumps(row).encode())
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            p.finish()
        with self.assertRaisesRegex(RuntimeError, "duplicate"):
            p.consume(row, json.dumps(row).encode())
        with self.assertRaisesRegex(RuntimeError, "unbound"):
            p.consume({"type": "end", "file_id": "missing"}, b"{}")

    def test_incomplete_stream_preserves_certified_view(self):
        view = View("h", revision="old", token="token", files={b"p": b"[]"})
        p = Payloads()
        row = {"type": "begin", "file_id": "a", "data": {"path": {"text": "p"}}}
        p.consume(row, json.dumps(row).encode())
        with self.assertRaisesRegex(RuntimeError, "incomplete"):
            view.apply(
                [{"type": "workspace_end", "exit_code": 0}], "new", 1, payloads=p
            )
        self.assertEqual(
            (view.revision, view.token, view.files), ("old", "token", {b"p": b"[]"})
        )

    def test_failed_streaming_consumer_retires_owned_connection(self):
        from unittest.mock import Mock, patch
        from registered import Client
        from retained import RetainedClient

        client = RetainedClient.__new__(RetainedClient)
        client.view = object()
        client.valid = True
        client.handles = {}
        client.process = Mock()
        client.process.poll.side_effect = [None, 1]
        with patch.object(
            Client, "exchange", side_effect=RuntimeError("malformed frame")
        ):
            with self.assertRaisesRegex(RuntimeError, "malformed"):
                client.exchange("search", consume=lambda row, raw: False)
        client.process.kill.assert_called_once()
        client.process.wait.assert_called_once_with(timeout=5)
        self.assertFalse(client.valid)
        self.assertIsNone(client.view)

    def test_minimal_ack_cannot_hide_a_file_payload(self):
        p = Payloads()
        for row in [
            {"type": "begin", "file_id": "a", "data": {"path": {"text": "p"}}},
            {"type": "end", "file_id": "a"},
        ]:
            p.consume(row, json.dumps(row).encode())
        view = View("h", revision="old", token="token")
        with self.assertRaisesRegex(RuntimeError, "invalid minimal"):
            view.apply(
                [
                    {
                        "type": "workspace_not_modified",
                        "revision": "old",
                        "result_token": "token",
                        "complete": True,
                        "exit_code": 0,
                    }
                ],
                "old",
                0,
                payloads=p,
            )


if __name__ == "__main__":
    unittest.main()
