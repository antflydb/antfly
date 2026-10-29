"""Bounded pgwire v3 peer for native resource qualification (stdlib only)."""

import os
import socket
import struct


class Peer:
    def __init__(self, host, port, *, password=None):
        self.sock = socket.create_connection((host, port), timeout=5)
        try:
            startup = struct.pack("!I", 196608) + b"user\0admin\0database\0default\0\0"
            self.sock.sendall(struct.pack("!I", len(startup) + 4) + startup)
            while True:
                tag, payload = self.message()
                if tag == b"R":
                    mode = struct.unpack("!I", payload[:4])[0]
                    if mode == 3:
                        secret = (
                            password
                            if password is not None
                            else os.environ["ANTFLY_QUALIFICATION_PGWIRE_PASSWORD"]
                        )
                        self.send(b"p", secret.encode() + b"\0")
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
        return self.ready(state)

    def ready(self, state=b"I", allow_suspended=False):
        rows, problem = [], None
        suspended = False
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
            elif tag == b"s":
                suspended = True
            elif tag == b"Z":
                if suspended and not allow_suspended:
                    raise RuntimeError("unexpected suspended portal")
                if problem:
                    raise problem
                if payload != state:
                    raise RuntimeError("unexpected pgwire transaction state")
                return rows

    def open_portal(self, statement):
        self.send(
            b"P", b"prepared\0" + statement.encode() + b"\0" + struct.pack("!H", 0)
        )
        self.send(b"B", b"portal\0prepared\0" + struct.pack("!HHH", 0, 0, 0))
        return self.execute_portal(1)

    def execute_portal(self, rows):
        self.send(b"E", b"portal\0" + struct.pack("!I", rows))
        self.send(b"S", b"")
        return self.ready(b"T", allow_suspended=True)

    def close_portal(self):
        self.send(b"C", b"Pportal\0")
        self.send(b"C", b"Sprepared\0")
        self.send(b"S", b"")
        self.ready(b"T")

    def close(self):
        self.sock.close()
