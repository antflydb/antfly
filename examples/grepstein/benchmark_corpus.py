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

"""Measure native PDF extraction and its OCR debt on an explicitly selected sample.

Starts two owned loopback services. No OCR model, production table, or Cloud
credentials are used. Input PDFs must already have been acquired and verified.
"""

import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import quote, unquote


def run(args):
    args.output.mkdir(parents=True, exist_ok=True)
    files = sorted(args.pdf_dir.glob("*.pdf"))
    if not files or len(files) > args.max_documents:
        raise ValueError(
            "select between 1 and max-documents PDFs in the sample directory"
        )
    if sum(path.stat().st_size for path in files) > args.max_bytes:
        raise ValueError("selected sample exceeds max-bytes")
    selected = {path.name: path for path in files}

    class Originals(BaseHTTPRequestHandler):
        def do_GET(self):
            path = selected.get(unquote(self.path.removeprefix("/")))
            if path is None:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/pdf")
            self.send_header("Content-Length", str(path.stat().st_size))
            self.end_headers()
            with path.open("rb") as source:
                while data := source.read(1 << 20):
                    self.wfile.write(data)

        def log_message(self, *_):
            pass

    originals = ThreadingHTTPServer(("127.0.0.1", 0), Originals)
    serving = threading.Thread(target=originals.serve_forever, daemon=True)
    serving.start()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    base = f"http://127.0.0.1:{port}/db/v1"
    config = {
        "remote_content": {
            "security": {
                "block_private_ips": False,
                "allowed_hosts": ["127.0.0.1"],
                "max_download_size_bytes": args.max_bytes,
            }
        },
        "resources": {"memory": {"max_bytes": 8 << 30}},
        "public_inference": {"enabled": False},
        "inference": {"models_dir": str(args.output / "models")},
    }
    config_path = args.output / "config.json"
    config_path.write_text(json.dumps(config))
    log = (args.output / "server.log").open("w")
    process = subprocess.Popen(
        [
            str(args.antfly),
            "standalone",
            "--config",
            str(config_path),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--data-dir",
            str(args.output / "data"),
            "--control-tick-ms",
            "5",
            "--auth",
            "false",
            "--health",
            "false",
        ],
        stdout=log,
        stderr=log,
        env=os.environ.copy(),
    )
    started = time.monotonic()
    report = {
        "scope": "native digital-text extraction and missing-OCR coverage; no GPU OCR or enrichment",
        "documents": [],
        "input_bytes": sum(path.stat().st_size for path in files),
    }

    def request(path, body=None):
        remaining = args.budget_seconds - (time.monotonic() - started)
        if remaining <= 0:
            raise TimeoutError("native sample exceeded its wall-clock budget")
        req = urllib.request.Request(
            base + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=min(10, remaining)) as response:
            return json.loads(response.read())

    try:
        while time.monotonic() - started < min(30, args.budget_seconds):
            if process.poll() is not None:
                raise RuntimeError("owned native server exited; inspect server.log")
            try:
                request("/tables")
                break
            except (OSError, ValueError):
                time.sleep(0.1)
        else:
            raise TimeoutError("native server did not become ready")
        producer = {
            "type": "document_extraction",
            "config": {
                "source": {
                    "filename_field": "filename",
                    "content_type_field": "mime_type",
                    "version_field": "version",
                }
            },
        }
        request(
            "/tables/sample",
            {
                "num_shards": 1,
                "indexes": {
                    "document_text": {
                        "type": "full_text",
                        "field": "text",
                        "artifact_name": "document_chunks_v1",
                        "enrichments": [
                            {
                                "name": "document_units_v1",
                                "kind": "asset",
                                "field": "url",
                                "content_type": "application/json",
                                "producer_json": json.dumps(producer),
                            },
                            {
                                "name": "document_chunks_v1",
                                "kind": "chunk",
                                "field": "text",
                                "source_artifact_name": "document_units_v1",
                                "chunk_size": 512,
                                "chunk_overlap": 50,
                            },
                        ],
                    }
                },
            },
        )
        inserts = {
            str(index): {
                "url": f"http://127.0.0.1:{originals.server_port}/{quote(path.name, safe='')}",
                "filename": path.name,
                "mime_type": "application/pdf",
                "version": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for index, path in enumerate(files)
        }
        submitted = time.monotonic()
        request("/tables/sample/batch", {"inserts": inserts, "sync_level": "write"})
        pending = set(inserts)
        while pending:
            for key in sorted(pending):
                try:
                    artifact = request(
                        f"/tables/sample/documents/{key}/artifacts/document_units_v1?detail=raw"
                    )
                except urllib.error.HTTPError as error:
                    if error.code == 404:
                        continue
                    raise
                if artifact.get("last_error_code"):
                    raise ValueError(artifact["last_error_code"])
                if artifact.get("merge_status") != "converged":
                    continue
                manifest = json.loads(artifact["manifest_json"])
                units, incomplete = (
                    artifact["unit_count"],
                    manifest["incomplete_unit_count"],
                )
                if (
                    not isinstance(units, int)
                    or not isinstance(incomplete, int)
                    or not 0 <= incomplete <= units
                ):
                    raise ValueError("invalid native coverage")
                report["documents"].append(
                    {
                        "file": files[int(key)].name,
                        "pages": units,
                        "incomplete_without_ocr": incomplete,
                        "text_bytes": sum(
                            part.get("text_bytes", 0)
                            for part in artifact.get("child_ranges", [])
                        ),
                    }
                )
                pending.remove(key)
            if pending:
                time.sleep(0.1)
        report["native_seconds"] = time.monotonic() - submitted
        report["pages"] = sum(row["pages"] for row in report["documents"])
        report["incomplete_without_ocr"] = sum(
            row["incomplete_without_ocr"] for row in report["documents"]
        )
    except Exception as error:
        report["error"] = str(error)
    finally:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        log.close()
        originals.shutdown()
        originals.server_close()
        serving.join()
        report["seconds"] = time.monotonic() - started
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report), flush=True)
    return 1 if report.get("error") else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--antfly", type=Path, required=True)
    parser.add_argument("--pdf-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-documents", type=int, default=16)
    parser.add_argument("--max-bytes", type=int, default=64 << 20)
    parser.add_argument("--budget-seconds", type=float, default=120)
    options = parser.parse_args()
    if (
        not 0 < options.budget_seconds <= 600
        or options.max_documents < 1
        or options.max_bytes < 1
    ):
        parser.error(
            "positive document/byte limits and a budget of at most 600 seconds are required"
        )
    raise SystemExit(run(options))
