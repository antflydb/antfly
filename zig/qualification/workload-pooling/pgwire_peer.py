"""Bounded pgwire v3 peer for native resource qualification (stdlib only)."""

import os
import socket
import struct


class Peer:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=5)
        try:
            startup = struct.pack("!I", 196608) + b"user\0admin\0database\0default\0\0"
            self.sock.sendall(struct.pack("!I", len(startup) + 4) + startup)
            while True:
                tag, payload = self.message()
                if tag == b"R":
                    mode = struct.unpack("!I", payload[:4])[0]
                    if mode == 3:
                        password = (
                            os.environ["ANTFLY_QUALIFICATION_PGWIRE_PASSWORD"].encode()
                            + b"\0"
                        )
                        self.send(b"p", password)
                    elif mode != 0:
                        raise RuntimeError(
                            f"unsupported pgwire authentication mode {mode}"
                        )
                elif tag == b"E":
                    raise RuntimeError("pgwire authentication rejected")
                elif tag == b"Z":
                    if payload != b"I":
                        raise RuntimeError("unexpected pgwire startup state")
                    break
        except BaseException:
            self.close()
            raise

    def read(self, count):
        chunks = bytearray()
        while len(chunks) < count:
            piece = self.sock.recv(count - len(chunks))
            if not piece:
                raise RuntimeError("pgwire peer closed unexpectedly")
            chunks.extend(piece)
        return bytes(chunks)

    def message(self):
        tag = self.read(1)
        size = struct.unpack("!I", self.read(4))[0]
        if size < 4 or size > 1 << 20:
            raise RuntimeError("pgwire frame exceeds qualification limit")
        return tag, self.read(size - 4)

    def send(self, tag, payload):
        self.sock.sendall(tag + struct.pack("!I", len(payload) + 4) + payload)

    def query(self, sql, state=b"I"):
        self.send(b"Q", sql.encode() + b"\0")
        rows, problem = [], None
        while True:
            tag, payload = self.message()
            if tag == b"E":
                fields = payload.split(b"\0")
                # Only SQLSTATE belongs in the receipt; never copy backend messages.
                code = next(
                    (field[1:].decode() for field in fields if field[:1] == b"C"),
                    "unknown",
                )
                problem = RuntimeError(f"pgwire query failed with SQLSTATE {code}")
            elif tag == b"D":
                columns = struct.unpack("!H", payload[:2])[0]
                offset, row = 2, []
                for _ in range(columns):
                    size = struct.unpack("!i", payload[offset : offset + 4])[0]
                    offset += 4
                    if size == -1:
                        row.append(None)
                    elif size >= 0 and offset + size <= len(payload):
                        row.append(payload[offset : offset + size].decode())
                        offset += size
                    else:
                        raise RuntimeError("invalid pgwire data row")
                if offset != len(payload):
                    raise RuntimeError("invalid pgwire row length")
                rows.append(row)
            elif tag == b"Z":
                if problem:
                    raise problem
                if payload != state:
                    raise RuntimeError("unexpected pgwire transaction state")
                return rows

    def close(self):
        self.sock.close()
