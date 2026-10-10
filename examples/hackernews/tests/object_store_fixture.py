# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""S3 wire oracle for retained snapshot transfer and conditional publication."""

import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
from urllib.parse import parse_qs, unquote, urlsplit
from xml.sax.saxutils import escape


class S3Fixture:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir()
        self.lock = threading.RLock()
        self.signed_requests = 0
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def reply(self, status, payload=b"", etag=None, head=False):
                self.send_response(status)
                self.send_header("Content-Length", str(len(payload)))
                if etag:
                    self.send_header("ETag", etag)
                self.end_headers()
                if not head:
                    self.wfile.write(payload)

            def execute(self, method):
                with fixture.lock:
                    if not self.headers.get("Authorization", "").startswith(
                        "AWS4-HMAC-SHA256 "
                    ):
                        return self.reply(403)
                    fixture.signed_requests += 1
                    url = urlsplit(self.path)
                    relative = unquote(url.path).removeprefix("/archive").lstrip("/")
                    if (
                        not url.path.startswith("/archive")
                        or ".." in Path(relative).parts
                    ):
                        return self.reply(403)
                    params = parse_qs(url.query)
                    if method == "GET" and params.get("list-type") == ["2"]:
                        prefix = params.get("prefix", [""])[0]
                        after = params.get(
                            "continuation-token", params.get("start-after", [""])
                        )[0]
                        maximum = int(params.get("max-keys", ["1000"])[0])
                        keys = sorted(
                            str(path.relative_to(fixture.root))
                            for path in fixture.root.rglob("*")
                            if path.is_file()
                        )
                        keys = [
                            key
                            for key in keys
                            if key.startswith(prefix) and key > after
                        ]
                        page = keys[:maximum]
                        contents = "".join(
                            f"<Contents><Key>{escape(key)}</Key><Size>{(fixture.root / key).stat().st_size}</Size></Contents>"
                            for key in page
                        )
                        more = len(keys) > len(page)
                        continuation = (
                            f"<NextContinuationToken>{escape(page[-1])}</NextContinuationToken>"
                            if more and page
                            else ""
                        )
                        return self.reply(
                            200,
                            (
                                '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                                f"<Name>archive</Name><Prefix>{escape(prefix)}</Prefix><KeyCount>{len(page)}</KeyCount>"
                                f"<MaxKeys>{maximum}</MaxKeys><IsTruncated>{str(more).lower()}</IsTruncated>"
                                f"{contents}{continuation}</ListBucketResult>"
                            ).encode(),
                        )
                    if not relative:
                        return self.reply(200, head=method == "HEAD")
                    path = fixture.root / relative
                    exists = path.is_file()
                    body = path.read_bytes() if exists else b""
                    etag = '"' + hashlib.md5(body).hexdigest() + '"' if exists else None
                    if (
                        self.headers.get("If-Match")
                        and self.headers["If-Match"] != etag
                    ):
                        return self.reply(412)
                    if method == "PUT":
                        incoming = self.rfile.read(int(self.headers["Content-Length"]))
                        if self.headers.get("If-None-Match") == "*" and exists:
                            return self.reply(412)
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(incoming)
                        return self.reply(
                            200, etag='"' + hashlib.md5(incoming).hexdigest() + '"'
                        )
                    if method == "DELETE":
                        path.unlink(missing_ok=True)
                        return self.reply(204)
                    if not exists:
                        return self.reply(
                            404,
                            b"<Error><Code>NoSuchKey</Code></Error>",
                            head=method == "HEAD",
                        )
                    return self.reply(200, body, etag, head=method == "HEAD")

            def do_HEAD(self):
                self.execute("HEAD")

            def do_GET(self):
                self.execute("GET")

            def do_PUT(self):
                self.execute("PUT")

            def do_DELETE(self):
                self.execute("DELETE")

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def endpoint(self):
        return f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()
