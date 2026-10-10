# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Authenticated gateway and the controller protocol consumed by Antfly."""

import argparse
import hmac
import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs, urlencode, unquote

from .controller import Controller
from .store import Conflict, Store, encode


def resolve(value):
    if isinstance(value, dict):
        if set(value) == {"env"}:
            return os.environ[value["env"]]
        return {key: resolve(item) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve(item) for item in value]
    return value


class Gateway(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, controller, tokens):
        super().__init__(address, Handler)
        self.controller, self.tokens = controller, tokens
        self.capacity = threading.BoundedSemaphore(32)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        # URLs, bodies and credentials are not written to access logs.
        pass

    def do_GET(self):
        self.handle_request()

    def do_HEAD(self):
        self.handle_request()

    def do_POST(self):
        self.handle_request()

    def do_DELETE(self):
        self.handle_request()

    def do_PUT(self):
        self.handle_request()

    def send(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def error(self, status, message):
        value = {"message": message, "type": "AntflyGatewayError", "code": status}
        self.send(status, encode({"error": value}))

    def handle_request(self):
        self.connection.settimeout(30)
        if not self.server.capacity.acquire(blocking=False):
            self.send(429, encode({"error": "gateway request capacity exhausted"}))
            return
        try:
            self._dispatch()
        except Conflict as error:
            self.error(409, str(error))
        except (ValueError, KeyError, TypeError) as error:
            self.error(400, str(error))
        except PermissionError:
            self.error(403, "gateway permission denied")
        except Exception as error:
            # Vendor errors can contain auth headers/URLs: expose their type,
            # never raw exception text. Durable authority remains fenced.
            logging.warning("gateway operation failed: %s", type(error).__name__)
            self.error(503, "provider operation unavailable")
        finally:
            self.server.capacity.release()

    def _dispatch(self):
        authorization = self.headers.get("Authorization", "")
        role = next(
            (
                role
                for role, token in self.server.tokens.items()
                if hmac.compare_digest(authorization, "Bearer " + token)
            ),
            None,
        )
        if role is None:
            raise PermissionError
        if self.headers.get("Transfer-Encoding"):
            raise ValueError("chunked request bodies are unsupported")
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 <= length <= 4 * 1024 * 1024:
            raise ValueError("request body exceeds budget")
        body = self.rfile.read(length)
        if len(body) != length:
            raise ValueError("incomplete request body")
        controller = self.server.controller
        parsed = urlsplit(self.path)
        path = parsed.path
        if path == "/v1/antfly/maintenance/capabilities" and self.command == "GET":
            if role != "admin":
                raise PermissionError
            self.send(200, encode(controller.capabilities()))
        elif path.startswith("/v1/antfly/maintenance/jobs/") and self.command == "POST":
            if role != "admin":
                raise PermissionError
            identifier = path.rsplit("/", 1)[-1]
            if (
                len(identifier) != 64
                or any(char not in "0123456789abcdef" for char in identifier)
                or self.headers.get("Idempotency-Key") != identifier
            ):
                raise ValueError("invalid maintenance idempotency key")
            result = controller.run_job(identifier, body)
            self.send(200 if result["state"] == "complete" else 202, encode(result))
        elif path == "/v1/antfly/maintenance/recover-writer" and self.command == "POST":
            if role != "admin":
                raise PermissionError
            result = controller.recover_writer()
            self.send(200 if result["complete"] else 202, encode(result))
        elif path == "/v1/antfly/maintenance/status" and self.command == "GET":
            if role != "admin":
                raise PermissionError
            self.send(200, encode(controller.state()))
        elif path == "/v1/antfly/readers" and self.command == "POST":
            value = json.loads(body)
            result = controller.acquire_reader(
                value["namespace"],
                value["name"],
                ttl_ms=value.get("ttl_ms", 120_000),
                input_files=value.get("input_files", ()),
            )
            self.send(200, encode(result))
        elif path.startswith("/v1/antfly/readers/"):
            identifier = path.rsplit("/", 1)[-1]
            if self.command == "DELETE":
                controller.release_reader(identifier)
                self.send(200, b"{}")
            elif self.command == "POST":
                self.send(
                    200,
                    encode(
                        controller.renew_reader(
                            identifier, json.loads(body).get("ttl_ms", 120_000)
                        )
                    ),
                )
            else:
                self.send(405, b"{}")
        elif self.path.startswith("/catalog/") or self.path.startswith("/nessie/"):
            native = self.path.startswith("/nessie/")
            upstream_path = self.path[len("/nessie") if native else len("/catalog") :]
            components = unquote(urlsplit(upstream_path).path).split("/")
            if any(
                part in (".", "..")
                or "%" in part
                or "\\" in part
                or any(ord(char) < 32 for char in part)
                for part in components
            ):
                raise PermissionError
            if native and controller.config["provider"] != "nessie":
                raise ValueError("native API requires Nessie")
            if not native:
                route = urlsplit(upstream_path)
                if route.path == "/v1/config":
                    supplied = parse_qs(route.query).get(
                        "warehouse", [controller.config.get("warehouse")]
                    )[0]
                    if supplied != controller.config.get("warehouse"):
                        raise PermissionError
                    upstream_path = "/v1/config?" + urlencode({"warehouse": supplied})
                else:
                    expected = controller.provider.negotiate()
                    pieces = route.path.split("/")
                    expected_pieces = expected.split("/")
                    if [unquote(part) for part in pieces[: len(expected_pieces)]] != [
                        unquote(part) for part in expected_pieces
                    ]:
                        raise PermissionError
                    if route.path.endswith("/credentials") or "/views" in route.path:
                        raise PermissionError
            if self.command in ("GET", "HEAD"):
                if native:
                    controller.allow_native_read(upstream_path)
                if (
                    not native
                    and self.command == "GET"
                    and (table := controller._table(upstream_path))
                ):
                    if role != "native":
                        result = controller.leased_table(
                            self.headers.get("X-Antfly-Reader-Lease", ""), *table
                        )
                        self.send(200, encode(result))
                        return
                status, data = controller.provider.request(
                    self.command, upstream_path, native=native
                )
                if (
                    not native
                    and upstream_path.startswith("/v1/config")
                    and status == 200
                ):
                    value = json.loads(data)
                    # Vendor tokens, signers and idempotency promises belong
                    # to the private upstream, never to this gateway contract.
                    for field in ("defaults", "overrides"):
                        value[field] = {
                            key: item
                            for key, item in value.get(field, {}).items()
                            if key in ("prefix", "warehouse")
                        }
                    value["overrides"]["uri"] = controller.config["catalog_uri"]
                    value["overrides"].pop("oauth2-server-uri", None)
                    value["overrides"]["rest-metrics-reporting-enabled"] = "false"
                    for key in list(value["overrides"]):
                        if key.startswith("nessie.") and key.endswith("-uri"):
                            del value["overrides"][key]
                    if "endpoints" in value:
                        value["endpoints"] = [
                            endpoint
                            for endpoint in value["endpoints"]
                            if endpoint.startswith(("GET ", "HEAD "))
                            and "/views" not in endpoint
                            and "/credentials" not in endpoint
                            or endpoint
                            in (
                                "POST /v1/{prefix}/namespaces",
                                "POST /v1/{prefix}/namespaces/{namespace}/tables",
                                "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
                            )
                        ]
                    data = encode(value)
                self.send(status, data)
            else:
                if role not in ("admin", "writer", "native"):
                    raise PermissionError
                status, data = controller.proxy_write(
                    self.command,
                    upstream_path,
                    body,
                    native=native,
                    grant_lease=role != "native",
                )
                self.send(status, data)
        else:
            self.send(404, b"{}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8089)
    args = parser.parse_args()
    with open(args.config) as stream:
        config = resolve(json.load(stream))
    tokens = config.pop("tokens")
    if (
        set(tokens) != {"admin", "writer", "native"}
        or len(set(tokens.values())) != 3
        or any(len(token) < 24 for token in tokens.values())
    ):
        raise ValueError(
            "three distinct gateway tokens of at least 24 characters are required"
        )
    controller = Controller(config, Store(config.get("io_properties")))
    server = Gateway((args.host, args.port), controller, tokens)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
