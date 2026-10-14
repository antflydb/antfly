#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Observe server-side video deadline cleanup and a correct pretrained retry."""

import argparse
import http.client
import json
import re
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request

import compare
import reference
import video_compare
from video_resident_qualification import body

HEADER = "X-Antfly-Inference-Timeout-Ms"


def declared_request(url, payload, budget, duplicate=False):
    encoded = json.dumps(payload).encode()
    if duplicate:
        endpoint = urllib.parse.urlsplit(url)
        connection = http.client.HTTPConnection(
            endpoint.hostname, endpoint.port, timeout=30
        )
        try:
            connection.putrequest("POST", "/ai/v1/embed")
            connection.putheader("Content-Type", "application/json")
            connection.putheader("Content-Length", str(len(encoded)))
            connection.putheader(HEADER, budget)
            connection.putheader(HEADER.lower(), budget)
            connection.endheaders(encoded)
            response = connection.getresponse()
            return response.status, json.loads(response.read(65536))
        finally:
            connection.close()
    request = urllib.request.Request(
        url + "/ai/v1/embed",
        encoded,
        {"Content-Type": "application/json", HEADER: budget},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read(65536))
    except urllib.error.HTTPError as error:
        try:
            return error.code, json.loads(error.read(65536))
        finally:
            error.close()


def restart_generation(text):
    matches = re.findall(
        r"inference worker stopped unexpectedly; restarting generation=(\d+).*?exited\s*=\s*86",
        text,
    )
    if len(matches) != 1 or int(matches[0]) <= 1:
        raise ValueError(
            "one supervised watchdog exit and replacement generation required"
        )
    return int(matches[0])


def run(args):
    reference.verify_checkpoint(args.model_dir)
    data = video_compare.corpus(args.corpus)
    oracle = json.loads(args.reference.read_text())
    if oracle["status"] != "pass" or oracle["corpus_sha256"] != reference.sha256(
        args.corpus
    ):
        raise ValueError("matching completed reference required")
    report = {
        "status": "running",
        "backend": args.backend,
        "binary_sha256": reference.sha256(args.binary),
        "tool_sha256": reference.sha256(Path(__file__)),
        "corpus_sha256": reference.sha256(args.corpus),
        "reference_sha256": reference.sha256(args.reference),
        "retry_timeout_seconds": args.retry_timeout,
    }
    args.url = None
    try:
        with video_compare.server(args) as url:
            query = data["queries"][0]
            warm = compare.post(
                url + "/ai/v1", "/embed", body(args.model_dir.name, query, 768)
            )
            report["warm_query"] = compare.assert_agreement(
                warm["data"][0]["embedding"], oracle["cases"][query["id"]]["vector"]
            )
            before = video_compare.embedding_metrics(url, include_outcomes=True)
            if before["active"] or before["admitted"]:
                raise ValueError("owned server is not idle")
            invalid = []
            for budget, duplicate in [
                ("0", False),
                ("bad", False),
                ("3600001", False),
                ("1,1", False),
                ("1", True),
            ]:
                status, result = declared_request(
                    url, body(args.model_dir.name, query, 768), budget, duplicate
                )
                invalid.append(
                    {
                        "budget": budget,
                        "duplicate": duplicate,
                        "status": status,
                        "response": result,
                    }
                )
                if status != 400 or result["error"] != "INVALID_INFERENCE_TIMEOUT":
                    raise ValueError("invalid deadline metadata reached inference")
            report["invalid_requests"] = invalid
            if video_compare.embedding_metrics(url, include_outcomes=True) != before:
                raise ValueError("invalid deadline metadata changed group lifecycle")
            row = data["documents"][0]
            payload = body(args.model_dir.name, row, 768)
            start = time.monotonic()
            log = args.output.with_suffix(".server.log")
            log_offset = log.stat().st_size
            restart = None
            try:
                status, result = declared_request(url, payload, "1000")
            except (http.client.RemoteDisconnected, ConnectionResetError) as error:
                if args.backend != "metal":
                    raise
                # A process-required driver call is hard-deadline terminated.
                # A plain disconnect never qualifies: require the owned
                # supervisor's exit86 and new generation in this request window.
                limit = time.monotonic() + 5
                while True:
                    try:
                        restart = restart_generation(log.read_text()[log_offset:])
                        break
                    except ValueError:
                        if time.monotonic() >= limit:
                            raise
                        time.sleep(0.05)
                status, result = None, video_compare.failure_details(error)
            elapsed = time.monotonic() - start
            report["deadline_response"] = {
                "status": status,
                "response": result,
                "seconds": elapsed,
                "replacement_generation": restart,
                "outcome": "watchdog_restart"
                if restart is not None
                else "http_timeout",
            }
            compare.publish(args.output, report)
            if (
                restart is None
                and (status != 504 or result["error"] != "INFERENCE_TIMEOUT")
            ) or elapsed > 15:
                raise ValueError(
                    "video deadline did not return a bounded timeout response"
                )
            deadline = time.monotonic() + 15
            while True:
                try:
                    after = video_compare.embedding_metrics(url, include_outcomes=True)
                except (
                    urllib.error.URLError,
                    http.client.RemoteDisconnected,
                    ConnectionResetError,
                ):
                    if restart is None or time.monotonic() >= deadline:
                        raise
                    time.sleep(0.05)
                    continue
                if after["active"] == 0 and after["admitted"] == 0:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError("video deadline cleanup did not finish")
                time.sleep(0.05)
            report["metrics_before"] = before
            report["metrics_after_deadline"] = after
            if restart is None:
                if (
                    after["timed_out"] != before["timed_out"] + 1
                    or after["completed"] != before["completed"]
                    or after["cancelled"] != before["cancelled"]
                    or after["failed"] != before["failed"]
                ):
                    raise ValueError(
                        "server deadline was not attributed after group cleanup"
                    )
            elif any(after.values()):
                raise ValueError(
                    "replacement worker did not begin with fresh lifecycle counters"
                )
            compare.publish(args.output, report)
            start = time.monotonic()
            retry = compare.post(
                url + "/ai/v1", "/embed", payload, timeout=args.retry_timeout
            )
            report["retry_seconds"] = time.monotonic() - start
            report["retry_agreement"] = compare.assert_agreement(
                retry["data"][0]["embedding"], oracle["cases"][row["id"]]["vector"]
            )
            if retry["model_identity"] != warm["model_identity"]:
                raise ValueError("deadline changed model identity")
            final = video_compare.embedding_metrics(url, include_outcomes=True)
            report["metrics_after_retry"] = final
            if (
                final["active"]
                or final["admitted"]
                or final["completed"] != after["completed"] + 1
                or final["timed_out"] != after["timed_out"]
                or final["cancelled"] != after["cancelled"]
                or final["failed"] != after["failed"]
            ):
                raise ValueError("retry did not complete and release admission")
        report["status"] = "pass"
    except Exception as error:
        report["status"] = "failed"
        report["failure"] = video_compare.failure_details(error)
        raise
    finally:
        if hasattr(args, "server_memory"):
            report["server_memory"] = args.server_memory
        compare.publish(args.output, report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["native", "metal"], required=True)
    for name in ("binary", "model-dir", "corpus", "reference", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--retry-timeout", type=float, default=180)
    args = parser.parse_args()
    if not 0 < args.retry_timeout <= 3600:
        parser.error("retry timeout must be positive and at most one hour")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    run(args)


if __name__ == "__main__":
    main()
