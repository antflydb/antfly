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

import base64
import hashlib
import socket
import struct
import unittest
from unittest.mock import patch

from collect_zig_time_report import Decoder, STAT_NAMES, WebSocket


def compile_message(file_index=0):
    return (
        struct.pack("<BIB4I9QQIII", 7, 0, 1, *range(13), 123, 4, 1, 1)
        + b"llvm"
        + b"source.zig\0"
        + b"worker\0"
        + struct.pack("<IIQQQ", file_index, 2, 10, 20, 30)
    )


class DecoderTests(unittest.TestCase):
    def test_full_compile_report_preserves_stats_and_declarations(self):
        decoder = Decoder()
        decoder.accept(struct.pack("<BBHqII", 0, 2, 1, 5, 1, 4) + b"unit" + b"\x01")
        decoder.accept(compile_message())
        decoder.accept(struct.pack("<BIB", 2, 0, 2))
        decoder.accept(bytes((1, 0)))
        value = decoder.report()
        self.assertEqual(value["steps"][0]["status"], 2)
        compile = value["compiles"][0]
        self.assertEqual(compile["step"], "unit")
        self.assertEqual(compile["ns_total"], 123)
        self.assertEqual(compile["stats"], dict(zip(STAT_NAMES, range(13))))
        self.assertEqual(compile["llvm_pass_timings"], "llvm")
        self.assertEqual(
            compile["declarations"],
            [
                dict(
                    name="worker",
                    file="source.zig",
                    sema_count=2,
                    sema_ns=10,
                    codegen_ns=20,
                    link_ns=30,
                )
            ],
        )

    def test_truncated_or_changed_protocol_rejected(self):
        for message in (
            compile_message()[:-1],
            compile_message() + b"extra",
            compile_message(2),
            b"\x07",
            b"",
            b"\xff",
            struct.pack("<BBHqI", 0, 2, 0, 0, 0),
        ):
            with self.subTest(message=message), self.assertRaises(ValueError):
                Decoder().accept(message)

    def test_unknown_symbol_bytes_are_safe(self):
        decoder = Decoder()
        decoder.accept(compile_message().replace(b"worker", b"work\xffr"))
        self.assertEqual(decoder.compiles[0]["declarations"][0]["name"], "work\ufffdr")


class FakeSocket:
    def __init__(self, chunks):
        self.chunks = iter(chunks)

    def recv(self, size):
        value = next(self.chunks)
        if isinstance(value, Exception):
            raise value
        return value

    def sendall(self, data):
        pass

    def settimeout(self, value):
        pass


class WebSocketTests(unittest.TestCase):
    def connection(self, chunks):
        connection = WebSocket.__new__(WebSocket)
        connection.socket = FakeSocket(chunks)
        connection.buffer = bytearray()
        connection.fragments = bytearray()
        return connection

    def test_real_handshake_header_and_coalesced_first_frame(self):
        nonce = b"x" * 16
        key = base64.b64encode(nonce).decode()
        accept = base64.b64encode(
            hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
            ).digest()
        )
        header = (
            b"HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Accept: "
            + accept
            + b"\r\n\r\n"
        )
        connection = FakeSocket([header + b"\x82\x03abc"])
        with (
            patch("collect_zig_time_report.os.urandom", return_value=nonce),
            patch(
                "collect_zig_time_report.socket.create_connection",
                return_value=connection,
            ),
        ):
            self.assertEqual(WebSocket(1234).message(), b"abc")

    def test_timeout_during_payload_preserves_frame_header(self):
        connection = self.connection([b"\x82\x05ab", socket.timeout(), b"cde"])
        with self.assertRaises(socket.timeout):
            connection.message()
        self.assertEqual(connection.message(), b"abcde")

    def test_extended_size_and_fragmentation(self):
        payload = b"x" * 130
        connection = self.connection(
            [b"\x02\x7e" + struct.pack("!H", len(payload)), payload, b"\x80\x01y"]
        )
        self.assertEqual(connection.message(), payload + b"y")

    def test_masked_server_frame_is_rejected(self):
        with self.assertRaises(ValueError):
            self.connection([b"\x82\x80"]).message()


if __name__ == "__main__":
    unittest.main()
