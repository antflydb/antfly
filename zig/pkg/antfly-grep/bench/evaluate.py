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

"""Evaluate labeled coding searches through the actual CLI and decision protocol.

--fixture validates search plumbing with ground-truth replies; it does not measure
model relevance. Supply --endpoint/--model for a separately authorized provider
measurement. No live provider is contacted by default.
"""

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def metrics(expected, scored):
    ordered = list(scored)  # Preserve actual CLI presentation order.
    selected = set(ordered)
    tp = len(selected & expected)
    dcg = sum(
        (1 if item in expected else 0) / math.log2(i + 2)
        for i, item in enumerate(ordered)
    )
    ideal = sum(1 / math.log2(i + 2) for i in range(len(expected)))
    return {
        "precision": tp / len(selected) if selected else (0 if expected else 1),
        "recall": tp / len(expected) if expected else 1,
        "ndcg": dcg / ideal if ideal else (0 if selected else 1),
        "true_positive": tp,
        "returned": len(selected),
        "expected": len(expected),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(__file__).parents[1] / "tests/relevance.json",
    )
    parser.add_argument("--fixture", action="store_true")
    parser.add_argument("--endpoint")
    parser.add_argument("--model")
    parser.add_argument(
        "--token-env",
        default="ANTFLY_GREP_TOKEN",
        help="Credential environment variable for explicit provider runs",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--min-recall", type=float, default=0.0)
    parser.add_argument("--min-precision", type=float, default=0.0)
    args = parser.parse_args()
    if not args.fixture and (not args.endpoint or not args.model):
        parser.error("select --fixture or explicitly provide --endpoint and --model")
    corpus = json.loads(args.manifest.read_text())
    if corpus["version"] != 1:
        raise ValueError("unsupported evaluation corpus")
    server = None
    endpoint = args.endpoint
    if args.fixture:

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                data = []
                for index, item in enumerate(body["inputs"]):
                    text = item["input"]
                    found = re.search(r"(?:File: |TARGET file: )(.*)", text)
                    path = Path(found[1]).name if found else ""
                    target = set(
                        map(int, re.findall(r"^TARGET (\d+):", text, re.MULTILINE))
                    )
                    span = re.search(r"^TARGET lines (\d+)-(\d+)", text, re.MULTILINE)
                    if span:
                        target.update(range(int(span[1]), int(span[2]) + 1))
                    answers = []
                    for question in body["questions"]:
                        truth = next(
                            (
                                q
                                for q in corpus["questions"]
                                if q["query"] in question["instructions"]
                            ),
                            None,
                        )
                        relevant = (
                            set(truth["relevant"].get(path, [])) if truth else set()
                        )
                        # Deliberately reject declaration regions. Independent
                        # refinement must still recover their relevant lines.
                        score = (
                            0.95
                            if target & relevant
                            and not item["id"].startswith("region-")
                            else 0.05
                        )
                        answers.append(
                            {
                                "name": question["name"],
                                "type": "predicate",
                                "decision_method": "typed",
                                "probability": score,
                            }
                        )
                    data.append(
                        {"input_index": index, "id": item["id"], "answers": answers}
                    )
                encoded = json.dumps({"data": data}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        endpoint = f"http://127.0.0.1:{server.server_port}/decisions"
    binary = str(args.binary.resolve())
    report = {
        "evaluation": "fixture_pipeline" if args.fixture else "provider_relevance",
        "manifest_sha256": hashlib.sha256(args.manifest.read_bytes()).hexdigest(),
        "binary_sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
        "model": "fixture" if args.fixture else args.model,
        "ranking": "CLI file order, then source-line order",
        "token_accounting": "Provider-reported CLI counters; zero may mean omitted usage. Fixture replies omit token usage.",
        "questions": [],
    }
    try:
        with tempfile.TemporaryDirectory(prefix="antfly-grep-eval-") as directory:
            root = Path(directory)
            for path, content in corpus["documents"].items():
                dest = root / path
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text(content)
            env = {
                k: v
                for k, v in os.environ.items()
                if not k.startswith(("ANTFLY_GREP_", "RIPGREP_"))
            }
            env.update(HOME=str(root), XDG_CONFIG_HOME=str(root / "config"))
            if not args.fixture and os.environ.get(args.token_env):
                env["ANTFLY_GREP_TOKEN"] = os.environ[args.token_env]
            for question in corpus["questions"]:
                started = time.perf_counter()
                result = subprocess.run(
                    [
                        binary,
                        "ask",
                        question["query"],
                        "--model",
                        report["model"],
                        "--endpoint",
                        endpoint,
                        "--json",
                        "--broad",
                        "--no-cache",
                        "--no-config",
                        "--concurrency",
                        "1",
                        *corpus["documents"],
                    ],
                    check=False,
                    cwd=root,
                    env=env,
                    capture_output=True,
                    timeout=120,
                )
                if result.returncode not in (0, 1):
                    raise RuntimeError(result.stderr.decode(errors="replace"))
                events = [json.loads(line) for line in result.stdout.splitlines()]
                elapsed = time.perf_counter() - started
                scored = {}
                files = {}
                for event in events:
                    if event["type"] != "file":
                        continue
                    files[event["path"]] = event["relevance"]
                    for line in event["lines"]:
                        if line["matched"]:
                            scored[(event["path"], line["line"])] = (
                                line["p"] if line["p"] is not None else line["region_p"]
                            )
                expected = {
                    (path, line)
                    for path, lines in question["relevant"].items()
                    for line in lines
                }
                summary = next(e for e in events if e["type"] == "summary")
                row = {
                    "id": question["id"],
                    **metrics(expected, scored),
                    "complete": summary["stats"]["complete"],
                    "elapsed_seconds": elapsed,
                    "files": metrics(set(question["relevant"]), files),
                    "requests": summary["requests"],
                    "input_tokens": summary["input_tokens"],
                    "output_tokens": summary["output_tokens"],
                }
                report["questions"].append(row)
    finally:
        if server:
            server.shutdown()
            server.server_close()
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")
    if any(
        not row["complete"]
        or row["recall"] < args.min_recall
        or row["precision"] < args.min_precision
        or row["files"]["recall"] < args.min_recall
        or row["files"]["precision"] < args.min_precision
        for row in report["questions"]
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
