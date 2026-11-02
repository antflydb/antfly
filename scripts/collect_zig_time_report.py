#!/usr/bin/env python3
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

"""Collect the pinned Zig 0.17 build WebSocket time report, then stop the server.

--time-report forces recompilation. Use a separate diagnostic run, never a warm
cache experiment or primary acceptance timing. Raw binary messages and build
output are retained alongside JSON. No external Python packages are required.
"""

import argparse
import ast
import base64
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import signal
import socket
import struct
import subprocess
import threading
import time

ABI_SHA256 = "bc9fae07984ff637825f19c0f0dd1e9cd3c352eade2a928ce183b6f7ca0ec7af"
READER_SHA256 = "5fd8d0269aed06e6b678815a9034d1f0db823b61977d014507161cc2d1d41b46"
STAT_NAMES = (
    "n_reachable_files",
    "n_imported_files",
    "n_generic_instances",
    "n_inline_calls",
    "cpu_ns_parse",
    "cpu_ns_astgen",
    "cpu_ns_sema",
    "cpu_ns_codegen",
    "cpu_ns_link",
    "real_ns_files",
    "real_ns_decls",
    "real_ns_llvm_emit",
    "real_ns_link_flush",
)


class Decoder:
    def __init__(self):
        self.steps = []
        self.statuses = []
        self.status = None
        self.compiles = {}
        self.generic = {}

    def accept(self, data):
        if not data:
            raise ValueError("empty ABI message")
        tag = data[0]
        if tag == 0:
            if len(data) < 16:
                raise ValueError("truncated hello")
            _, self.status, flags, _, count = struct.unpack_from("<BBHqI", data)
            if not flags & 1:
                raise ValueError("compiler time reporting is disabled")
            position = 16
            lengths = struct.unpack_from(f"<{count}I", data, position)
            position += 4 * count
            for length in lengths:
                stop = position + length
                if stop > len(data):
                    raise ValueError("truncated step name")
                self.steps.append(data[position:stop].decode("utf-8", "replace"))
                position = stop
            if len(data) - position != (count + 3) // 4:
                raise ValueError("invalid step status table")
            self.statuses = [
                (data[position + index // 4] >> (2 * (index % 4))) & 3
                for index in range(count)
            ]
        elif tag == 1:
            if len(data) != 2:
                raise ValueError("invalid status update")
            self.status = data[1]
        elif tag == 2:
            if len(data) != 6:
                raise ValueError("invalid step update")
            index, bits = struct.unpack_from("<IB", data, 1)
            self.statuses[index] = bits & 3
        elif tag == 6:
            if len(data) != 13:
                raise ValueError("invalid generic result")
            index, duration = struct.unpack_from("<IQ", data, 1)
            self.generic[index] = duration
        elif tag == 7:
            self.compile(data)
        elif tag != 8:
            raise ValueError(f"unsupported build ABI tag {tag}")

    def compile(self, data):
        if len(data) < 114:
            raise ValueError("truncated compiler report")
        index, flags = struct.unpack_from("<IB", data, 1)
        stats = dict(zip(STAT_NAMES, struct.unpack_from("<4I9Q", data, 6), strict=True))
        duration, llvm_len, files_len, decls_len = struct.unpack_from("<QIII", data, 94)
        position = 114 + llvm_len
        if position > len(data):
            raise ValueError("truncated LLVM timings")
        llvm = data[114:position].decode("ascii", "replace")

        def string():
            nonlocal position
            stop = data.find(b"\0", position)
            if stop == -1:
                raise ValueError("unterminated report string")
            value = data[position:stop].decode("utf-8", "replace")
            position = stop + 1
            return value

        files = [string() for _ in range(files_len)]
        declarations = []
        for _ in range(decls_len):
            name = string()
            if position + 32 > len(data):
                raise ValueError("truncated declaration")
            file, sema_count, sema, codegen, link = struct.unpack_from(
                "<IIQQQ", data, position
            )
            position += 32
            if file >= len(files):
                raise ValueError("invalid declaration file index")
            declarations.append(
                dict(
                    name=name,
                    file=files[file],
                    sema_count=sema_count,
                    sema_ns=sema,
                    codegen_ns=codegen,
                    link_ns=link,
                )
            )
        if position != len(data) or flags & ~1:
            raise ValueError("compiler ABI layout changed")
        self.compiles[index] = dict(
            step_index=index,
            use_llvm=bool(flags & 1),
            ns_total=duration,
            stats=stats,
            llvm_pass_timings=llvm,
            files=files,
            declarations=declarations,
        )

    def report(self):
        return dict(
            format_version=1,
            protocol_sha256=ABI_SHA256,
            payload_reader_sha256=READER_SHA256,
            status=self.status,
            steps=[
                dict(
                    name=name,
                    status=self.statuses[index],
                    ns_total=self.generic.get(index),
                )
                for index, name in enumerate(self.steps)
            ],
            compiles=[
                dict(step=self.steps[index], **value)
                for index, value in sorted(self.compiles.items())
            ],
        )


class WebSocket:
    def __init__(self, port):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=10)
        self.buffer = bytearray()
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        self.socket.sendall(
            (
                f"GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\n"
                f"Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {key}\r\n\r\n"
            ).encode()
        )
        while b"\r\n\r\n" not in self.buffer:
            block = self.socket.recv(4096)
            if not block:
                raise EOFError("WebSocket handshake disconnected")
            self.buffer.extend(block)
            if len(self.buffer) > 65536:
                raise ValueError("oversized WebSocket handshake")
        header, _, trailing = self.buffer.partition(b"\r\n\r\n")
        self.buffer = bytearray(trailing)
        expected = base64.b64encode(
            hashlib.sha1(
                (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
            ).digest()
        )
        fields = dict(line.split(b":", 1) for line in bytes(header).split(b"\r\n")[1:])
        fields = {key.lower(): value.strip() for key, value in fields.items()}
        if (
            not header.startswith(b"HTTP/1.1 101 ")
            or fields.get(b"sec-websocket-accept") != expected
        ):
            raise ValueError("invalid WebSocket handshake")
        self.socket.settimeout(1)
        self.fragments = bytearray()

    def peek(self, size):
        while len(self.buffer) < size:
            block = self.socket.recv(max(4096, size - len(self.buffer)))
            if not block:
                raise EOFError("WebSocket disconnected")
            self.buffer.extend(block)
        return bytes(self.buffer[:size])

    def message(self):
        while True:
            first, second = self.peek(2)
            offset = 2
            length = second & 127
            if second & 128 or first & 0x70:
                raise ValueError("unsupported WebSocket frame flags")
            if length == 126:
                length = struct.unpack("!H", self.peek(4)[2:])[0]
                offset = 4
            elif length == 127:
                length = struct.unpack("!Q", self.peek(10)[2:])[0]
                offset = 10
            if length > 512 * 1024 * 1024:
                raise ValueError("oversized time-report frame")
            payload = self.peek(offset + length)[offset:]
            del self.buffer[: offset + length]
            opcode = first & 15
            if opcode == 8:
                raise EOFError("WebSocket closed")
            if opcode == 9:
                if length > 125 or not first & 128:
                    raise ValueError("invalid WebSocket ping")
                mask = os.urandom(4)
                self.socket.sendall(
                    bytes((0x8A, 128 | len(payload)))
                    + mask
                    + bytes(
                        value ^ mask[index % 4] for index, value in enumerate(payload)
                    )
                )
                continue
            if opcode not in (0, 2):
                raise ValueError(f"unexpected WebSocket opcode {opcode}")
            self.fragments.extend(payload)
            if first & 128:
                result = bytes(self.fragments)
                self.fragments.clear()
                return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--zig-lib", type=Path, help="Defaults to the installed zig env lib_dir"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=7200)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or any(
        arg.startswith(("--time-report", "--watch", "--webui")) for arg in command
    ):
        parser.error(
            "provide a build command without existing time-report/webui/watch flags"
        )
    if args.zig_lib is None:
        env = subprocess.check_output(["zig", "env"], text=True)
        match = re.search(
            r'^\s*\.lib_dir\s*=\s*("(?:[^"\\]|\\.)*"),\s*$', env, re.MULTILINE
        )
        if match is None:
            parser.error("cannot locate Zig lib_dir; provide --zig-lib")
        args.zig_lib = Path(ast.literal_eval(match[1]))
    abi = args.zig_lib / "std/Build/abi.zig"
    if hashlib.sha256(abi.read_bytes()).hexdigest() != ABI_SHA256:
        parser.error(
            "unsupported Zig build ABI; review the protocol before updating its hash"
        )
    reader = args.zig_lib / "build-web/time_report.zig"
    if hashlib.sha256(reader.read_bytes()).hexdigest() != READER_SHA256:
        parser.error(
            "unsupported time-report payload; review the pinned reader before updating its hash"
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.timeout
    decoder = Decoder()
    lines = queue.Queue()
    process = subprocess.Popen(
        [*command, "--time-report", "--webui=127.0.0.1:0", "--summary", "all"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    connection = None
    port = None

    def drain():
        with args.output.with_suffix(".build.log").open("wb") as log:
            for line in iter(process.stdout.readline, b""):
                log.write(line)
                log.flush()
                lines.put(line)

    thread = threading.Thread(target=drain, daemon=True)
    thread.start()
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(
                    "build exited before report collection; see build log"
                )
            try:
                line = lines.get(timeout=1)
            except queue.Empty:
                continue
            match = re.search(
                rb"web interface listening at http://127\.0\.0\.1:(\d+)/", line
            )
            if match:
                port = int(match[1])
            # This toolchain announces its socket before graph configuration.
            # Opening a WebSocket then dereferences configured=null in Zig.
            # Reports are retained: connect after the build summary instead.
            if port is not None and b"Build Summary:" in line:
                connection = WebSocket(port)
                break
        if connection is None:
            raise TimeoutError("build report server did not start")
        with args.output.with_suffix(".frames.bin").open("wb") as frames:
            while time.monotonic() < deadline:
                try:
                    data = connection.message()
                except socket.timeout:
                    # The server sends retained reports before the final idle
                    # status. A quiet idle connection has drained those reports.
                    if decoder.status == 0 and decoder.steps:
                        break
                    if process.poll() is not None:
                        raise RuntimeError("report server exited during build")
                    continue
                frames.write(struct.pack("<I", len(data)) + data)
                frames.flush()
                decoder.accept(data)
            else:
                raise TimeoutError("time-report build exceeded timeout")
        if not decoder.compiles or 3 in decoder.statuses:
            raise RuntimeError(
                "missing compiler reports or failed build steps; inspect raw log"
            )
    finally:
        if connection is not None:
            connection.socket.close()
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
        thread.join(timeout=10)
        value = decoder.report()
        value.update(command=command, zig_lib=str(args.zig_lib), forced_rebuild=True)
        args.output.write_text(json.dumps(value, indent=2) + "\n")


if __name__ == "__main__":
    main()
