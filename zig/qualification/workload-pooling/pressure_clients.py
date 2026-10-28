"""Retain authenticated sockets, held cursors and unfinished bodies during load."""

import http.client
import select
import socket
from idle_clients import auth_headers
from pgwire_peer import Peer


class PressureClients:
    def __init__(self, target, pgwire_port, manifest):
        self.target = target
        self.port = pgwire_port
        self.rows = manifest["fixture_rows"]
        self.config = manifest.get("retained_pressure", {})
        self.http, self.sql, self.uploads = [], [], []
        for key, value in self.config.items():
            if type(value) is not int or value < 0:
                raise ValueError(
                    "retained pressure counts must be nonnegative integers"
                )
        if self.config.get("held_cursors", 0) > self.config.get(
            "pgwire_connections", 0
        ):
            raise ValueError("held cursor count exceeds pgwire connections")
        if (
            self.config.get("slow_uploads", 0)
            and self.config.get("upload_bytes", 0) < 2
        ):
            raise ValueError("slow uploads require an unfinished body")

    def start(self):
        # Append each owner before sending so failed setup can close everything.
        for _ in range(self.config.get("http_connections", 0)):
            peer = http.client.HTTPConnection(
                self.target.hostname, self.target.port, timeout=5
            )
            self.http.append(peer)
            peer.request("GET", self.target.path, headers=auth_headers())
            response = peer.getresponse()
            response.read()
            if response.status != 200 or peer.sock is None:
                raise RuntimeError("pressure HTTP connection was not retained")
        for index in range(self.config.get("pgwire_connections", 0)):
            peer = Peer(self.target.hostname, self.port)
            self.sql.append(peer)
            if index < self.config.get("held_cursors", 0):
                peer.query("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY", b"T")
                peer.query(
                    "DECLARE held CURSOR WITH HOLD FOR SELECT id FROM pooling_qualification ORDER BY id",
                    b"T",
                )
                if peer.query("FETCH NEXT FROM held", b"T") != [["9007199254740993"]]:
                    raise RuntimeError("pressure cursor lost its exact first row")
                peer.query("COMMIT")
        for _ in range(self.config.get("slow_uploads", 0)):
            peer = socket.create_connection(
                (self.target.hostname, self.target.port), timeout=5
            )
            self.uploads.append(peer)
            headers = dict(
                auth_headers(),
                Host="qualification",
                **{
                    "Content-Type": "application/json",
                    "Content-Length": str(self.config["upload_bytes"]),
                },
            )
            request = "POST /db/v1/sql HTTP/1.1\r\n" + "".join(
                f"{k}: {v}\r\n" for k, v in headers.items()
            )
            peer.sendall((request + "\r\n{").encode())
        return self.verify(check_rows=False)

    def verify(self, *, check_rows=True):
        for peer in self.http:
            original_socket = peer.sock
            if (
                original_socket is None
                or select.select([original_socket], [], [], 0)[0]
            ):
                raise RuntimeError(
                    "retained HTTP connection closed before verification"
                )
            peer.request("GET", self.target.path, headers=auth_headers())
            response = peer.getresponse()
            response.read()
            if response.status != 200 or peer.sock is not original_socket:
                raise RuntimeError("retained HTTP connection failed during pressure")
        for index, peer in enumerate(self.sql):
            if check_rows and index < self.config.get("held_cursors", 0):
                expected = [[str(9007199254740993 + i)] for i in range(1, self.rows)]
                if peer.query("FETCH ALL FROM held") != expected:
                    raise RuntimeError(
                        "pressure cursor lost rows across COMMIT and load"
                    )
                peer.query("CLOSE held")
            elif index >= self.config.get("held_cursors", 0):
                if peer.query("SELECT 1") != [["1"]]:
                    raise RuntimeError("retained pgwire connection failed")
        # Any early response or EOF invalidates the slow-upload pressure. Do not
        # count a TCP connect as retained server-side request ownership.
        if self.uploads and select.select(self.uploads, [], [], 0)[0]:
            raise RuntimeError("unfinished upload was rejected or closed")
        return dict(
            http_connections=len(self.http),
            pgwire_connections=len(self.sql),
            held_cursors=self.config.get("held_cursors", 0),
            slow_uploads=len(self.uploads),
            declared_upload_bytes=len(self.uploads)
            * self.config.get("upload_bytes", 0),
            cursor_rows_verified=check_rows,
        )

    def close(self):
        for peer in self.http + self.sql + self.uploads:
            peer.close()
        self.http.clear()
        self.sql.clear()
        self.uploads.clear()
