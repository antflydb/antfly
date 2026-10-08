#!/usr/bin/env python3
"""Qualify real HTTP embedding and similarity contracts against pinned oracles.

Run against the exact binary being reviewed. This checks numerical and serving
contracts; it deliberately does not claim application quality or production
qualification. Keep the output with server/binary hashes and resource snapshots.
"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
from pathlib import Path
import statistics
import time
import urllib.error
import urllib.request


def post(url, endpoint, body, expected=200):
    raw = json.dumps(body, allow_nan=False).encode()
    request = urllib.request.Request(url.rstrip("/") + endpoint, raw, {"Content-Type": "application/json"})
    start = time.monotonic()
    try:
        response = urllib.request.urlopen(request, timeout=120)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        data = json.loads(response.read(64 * 1024 * 1024))
        if response.status != expected:
            raise AssertionError((endpoint, response.status, data))
    return data, time.monotonic() - start


def compare(actual, reference, limit=1e-4):
    if len(actual) != len(reference) or not all(math.isfinite(x) for x in actual):
        raise AssertionError("invalid embedding shape or nonfinite vector")
    norm = math.sqrt(sum(x*x for x in actual))
    if abs(norm - 1) > 1e-4:
        raise AssertionError("embedding is not normalized")
    difference = max(abs(x-y) for x,y in zip(actual, reference))
    if difference > limit:
        raise AssertionError(f"embedding max_abs={difference} > {limit}")
    return difference


def qualify(args):
    reference = json.loads(args.oracle.read_text())["embeddings"][0]
    identity = None
    errors = {}
    latencies = []
    for dimension in (768, 512, 256, 128):
        result, elapsed = post(args.url, "/embed", {"model": args.model, "input": ["A fox jumps over a log."], "dimensions": dimension})
        latencies.append(elapsed)
        actual_identity = result["model_identity"]
        if len(actual_identity) != 64 or (identity and identity != actual_identity):
            raise AssertionError("identity changed across dimensions")
        identity = actual_identity
        norm = math.sqrt(sum(x*x for x in reference[:dimension]))
        errors[str(dimension)] = compare(result["data"][0]["embedding"], [x/norm for x in reference[:dimension]])
    groups = [{"title": "none", "content": [{"type": "text", "text": "A fox jumps over a log."}]}, {"content": []}, {"content": [{"type": "text", "text": "A fox jumps over a log."}]}]
    partial, _ = post(args.url, "/embed", {"model": args.model, "model_identity": identity, "input": groups, "error_policy": "per_item"})
    if [row["index"] for row in partial["data"]] != [0, 2] or not partial.get("errors"):
        raise AssertionError("partial group indexes or errors missing")
    if partial["errors"][0]["status"] != 400 or partial["errors"][0]["retryable"]:
        raise AssertionError("invalid group must produce a nonretryable client error")
    compare(partial["data"][0]["embedding"], reference)
    flat, _ = post(args.url, "/embed", {"model": args.model, "model_identity": identity, "input": ["A fox jumps over a log.", "A fox jumps over a log."]})
    if [row["index"] for row in flat["data"]] != [0, 1]:
        raise AssertionError("flat batch indexes differ")
    for row in flat["data"]:
        compare(row["embedding"], reference)
    post(args.url, "/embed", {"model": args.model, "model_identity": "a"*64, "input": ["wrong identity"]}, 409)
    body = {"model": args.model, "model_identity": identity, "state": "Reset my password", "questions": {"route": {"type": "choice", "instructions": "Route the request", "criteria": {"account": {"examples": ["Password reset", "Cannot log in"]}, "duplicate": {"examples": ["Password reset", "Cannot log in"]}}}}}
    def decision(_):
        result, elapsed = post(args.url, "/decide", body)
        answer = result["answers"]["route"]
        if answer["choice"] is not None or answer["status"] != "abstained" or answer["abstention_reason"] != "tie" or answer["decision_method"] != "embedding_similarity":
            raise AssertionError("invalid tie abstention")
        if "probabilities" in answer or "confidence" in answer:
            raise AssertionError("raw cosine was exposed as probability/confidence")
        if result["model_identity"] != identity:
            raise AssertionError("decision identity differs")
        return elapsed
    decision(0)
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        latencies.extend(pool.map(decision, range(args.iterations)))
    post(args.url, "/decide", {"model": args.model, "state": "Priority", "questions": {"priority": {"type": "score", "instructions": "Urgency", "criteria": ["Low", "High"]}}}, 400)
    post(args.url, "/decide", {**body, "embedding_options": {"min_margin": 3}}, 400)
    post(args.url, "/decide", {**body, "embedding_options": {"calibration_id": "missing_qualification"}}, 400)
    extraction = {"model": args.model, "schema_version": 2, "inputs": [{"id": "route-1", "content": "Reset my password"}], "schema": {"classifications": [{"name": "route", "prompt": "Route the request", "mode": "multi", "labels": ["account", "duplicate"], "label_definitions": {"account": {"description": "Account access"}, "duplicate": {"description": "Account access"}}, "similarity_thresholds": -1}]}}
    classified, _ = post(args.url, "/extract", extraction)
    item = classified["data"][0]
    result = item["decisions"][0]
    if item["id"] != "route-1" or result["labels"] != ["account", "duplicate"] or result["decision_method"] != "embedding_similarity":
        raise AssertionError("multi-label extraction contract differs")
    if "probabilities" in result or "confidence" in result or any("score" in row or "similarity" not in row for row in item["classifications"]):
        raise AssertionError("extraction mixed similarity and probability contracts")
    post(args.url, "/embed", {"model": args.model, "input": [{"content": []}]}, 400)
    post(args.url, "/embed", {"model": args.model, "task_type": "RETRIEVAL_QUERY", "input": [{"title": "invalid query title", "content": [{"type": "text", "text": "text"}]}]}, 400)
    media = {}
    if args.media_oracle:
        definitions = json.loads(args.media_oracle.read_text())["cases"]
        root = args.media_oracle.parent
        image = {"type": "media", "mime_type": "image/png", "data": base64.b64encode((root/"image.png").read_bytes()).decode()}
        audio = {"type": "media", "mime_type": "audio/wav", "data": base64.b64encode((root/"audio.wav").read_bytes()).decode()}
        content = [[image], [audio], [{"type":"text","text":"Describe this image."},image], [image,{"type":"text","text":"Describe this image."}], [{"type":"text","text":"Describe these inputs."},audio,image]]
        result, _ = post(args.url, "/embed", {"model":args.model,"model_identity":identity,"input":[{"content":parts} for parts in content]})
        for row, expected in zip(result["data"], definitions):
            media[expected["name"]] = compare(row["embedding"], expected["embedding"])
        if len(result["data"]) != 5:
            raise AssertionError("missing media groups")
    return {"version":1,"production_qualified":False,"qualification":"numerical_and_http_contracts","model":args.model,"model_identity":identity,"oracle_sha256":hashlib.sha256(args.oracle.read_bytes()).hexdigest(),"dimension_max_abs":errors,"media_max_abs":media,"requests":len(latencies),"concurrency":args.concurrency,"latency_seconds":{"median":statistics.median(latencies),"maximum":max(latencies)},"remaining_gates":["application retrieval and classification holdouts","qualified deployment calibration","clean-host resource and latency campaign","index migration and retrieval qualification"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--oracle", type=Path, required=True)
    parser.add_argument("--media-oracle", type=Path)
    parser.add_argument("--iterations", type=int, default=32)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.iterations <= 1000 or not 1 <= args.concurrency <= 16:
        parser.error("bounded iterations and concurrency required")
    result = qualify(args)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
