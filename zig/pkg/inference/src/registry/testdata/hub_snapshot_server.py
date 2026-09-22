# Copyright 2026 Antfly, Inc.
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

"""Local Hub fixture: main moves after metadata; bytes must use its resolved SHA."""

import hashlib
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

COMMIT = "0123456789abcdef0123456789abcdef01234567"


def varint(value):
    out = bytearray()
    while value > 127:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def field(number, value):
    return varint(number * 8 + 2) + varint(len(value)) + value


def tensor(location):
    return field(13, field(1, b"location") + field(2, location))


ONNX = field(
    7,
    field(5, tensor(b"weights.bin"))
    + field(1, field(5, field(5, tensor(b"Constant_7_attr__value")))),
)
FILES = {
    "config.json": b'{"hidden_size":4}',
    "model-Q8_0.gguf": b"gguf snapshot payload",
    "model.safetensors.index.json": b'{"weight_map":{"a":"custom.safetensors","b":"custom.safetensors"}}',
    "custom.safetensors": b"referenced shard",
    "model-00001-of-00001.safetensors": b"unreferenced decoy",
    "onnx/model.onnx": ONNX,
    "onnx/weights.bin": b"external weights",
    "onnx/Constant_7_attr__value": b"external constant",
    "onnx/config.json": b'{"hidden_size":8}',
    "onnx/tokenizer.json": b"{}",
}
MODE = sys.argv[2]
if MODE == "onnx-only":
    FILES = {
        k: v for k, v in FILES.items() if k.startswith("onnx/") or k == "config.json"
    }
if MODE == "safetensors-only":
    FILES = {
        k: v
        for k, v in FILES.items()
        if k.endswith(".safetensors")
        or k in {"config.json", "model.safetensors.index.json"}
    }
if MODE == "missing-dependency":
    del FILES["onnx/Constant_7_attr__value"]
if MODE == "unsafe-dependency":
    FILES["onnx/model.onnx"] = field(7, field(5, tensor(b"../escape")))


class Handler(BaseHTTPRequestHandler):
    def respond(self, body=True):
        path = unquote(urlsplit(self.path).path)
        with Path("requests.log").open("a") as log:
            log.write(f"{self.command} {self.path}\n")
        if path.startswith("/api/models/"):
            commit = COMMIT if MODE != "missing-commit" else None
            payload = json.dumps(
                {
                    "sha": commit,
                    "siblings": [
                        {
                            "rfilename": name,
                            "size": len(data),
                            "blobId": hashlib.sha1(
                                b"blob " + str(len(data)).encode() + b"\0" + data
                            ).hexdigest(),
                        }
                        for name, data in FILES.items()
                    ],
                }
            ).encode()
            code = 200
        elif f"/resolve/{COMMIT}/" in path:
            filename = path.split(f"/resolve/{COMMIT}/", 1)[1]
            payload = FILES.get(filename, b"not found")
            code = 200 if filename in FILES else 404
        else:
            # Simulate a branch that changed immediately after metadata lookup.
            payload, code = b"main has moved", 409
        self.send_response(code)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if body:
            self.wfile.write(payload)

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond()

    def log_message(self, *args):
        pass


HTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
