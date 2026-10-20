#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Check immediate SSE follow-ups against a server with admission concurrency 1.

Stop reading at [DONE], close the response, and immediately submit the next
request. Do not sleep, retry 503s, or wait for HTTP EOF: those hide the lifecycle
race. This checks resource release, not native compaction quality or latency.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8091")
    parser.add_argument("--model", required=True)
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.runs < 2:
        parser.error("--runs must be at least 2")
    evidence = {"model": args.model, "runs_requested": args.runs, "cases": [], "passed": False}
    try:
        for index in range(args.runs):
            include_usage = index % 2 == 0
            request = {
                "model": args.model,
                "messages": [{"role": "user", "content": "Reply with the word ready."}],
                "max_tokens": 3,
                "temperature": 0,
                "stream": True,
                "stream_options": {"include_usage": include_usage},
                "chat_template_kwargs": {"enable_thinking": False},
                "context_management": {"mode": "reject"},
            }
            started = time.monotonic()
            case = {"index": index, "include_usage": include_usage, "done": False}
            evidence["cases"].append(case)
            req = urllib.request.Request(
                args.base_url.rstrip("/") + "/ai/v1/generate",
                data=json.dumps(request).encode(),
                headers={"Content-Type": "application/json"},
            )
            try:
                with urllib.request.urlopen(req, timeout=180) as response:
                    case["status"] = response.status
                    case["runtime_headers"] = {
                        name: value for name, value in response.headers.items()
                        if name.lower().startswith("x-antfly-inference")
                    }
                    saw_usage = False
                    for raw in response:
                        line = raw.decode().strip()
                        if line.startswith("event: error"):
                            raise RuntimeError("SSE error event")
                        if not line.startswith("data: "):
                            continue
                        data = line[6:]
                        if data == "[DONE]":
                            case["done"] = True
                            break
                        event = json.loads(data)
                        if event.get("error"):
                            raise RuntimeError(f"SSE error: {event['error']}")
                        saw_usage |= bool(event.get("usage"))
                    case["usage"] = saw_usage
                    if not case["done"] or saw_usage != include_usage:
                        raise RuntimeError("missing DONE or incorrect terminal usage")
            except urllib.error.HTTPError as error:
                case["status"] = error.code
                case["body"] = error.read().decode(errors="replace")
                raise
            finally:
                case["elapsed_ms"] = (time.monotonic() - started) * 1000
        evidence["passed"] = True
    except Exception as error:
        evidence["error"] = str(error)
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps({"passed": evidence["passed"], "cases": len(evidence["cases"]), "output": str(args.output)}))
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
