#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Live source-mode API isolation/rejection smoke; never starts a server/model.

Reuse a saved native feature store and raw v1 copper A/B documents. Gold labels
are checked locally after responses, never transmitted. Source alignment is
caller-supplied; model entailment is not formal truth verification. Default
disabled behavior belongs to native unit tests, not live configuration changes.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import datetime
import hashlib
import json
import os
from pathlib import Path
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import quality_cases
import quality_eval
import verified_quality_eval as verified
from http_smoke import IDENTITY, STAGE3_SHA256


class Client:
    def __init__(self, args, events, checkpoint):
        url = urlsplit(args.base_url)
        if (
            url.scheme not in {"http", "https"}
            or not url.netloc
            or url.username
            or url.password
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "use an HTTP(S) base URL without credentials/query/fragment"
            )
        self.args, self.events, self.checkpoint = args, events, checkpoint
        self.lock = threading.Lock()

    def post(
        self,
        body: dict,
        expected: set[int],
        *,
        label: str,
        timeout_ms: int | None = None,
    ):
        deadline = self.args.timeout_ms if timeout_ms is None else timeout_ms
        raw = quality_eval.json_bytes(body)
        headers = {
            "Content-Type": "application/json",
            "X-Antfly-Inference-Timeout-Ms": str(deadline),
        }
        if os.environ.get("ANTFLY_API_KEY"):
            headers["Authorization"] = "Bearer " + os.environ["ANTFLY_API_KEY"]
        request = Request(
            self.args.base_url.rstrip("/") + "/ai/v1/memorilla/generate",
            raw,
            headers,
            method="POST",
        )
        event = {
            "label": label,
            "timeout_ms": deadline,
            "expected_statuses": sorted(expected),
            "request_sha256": hashlib.sha256(raw).hexdigest(),
        }
        started = time.monotonic()
        try:
            try:
                response = urlopen(request, timeout=deadline / 1000 + 30)
            except HTTPError as exc:
                response = exc
            with response:
                status, data = response.status, response.read()
            event.update(
                status=status, response_sha256=hashlib.sha256(data).hexdigest()
            )
            value = json.loads(data)
            event["response"] = value
            if status not in expected:
                raise ValueError(
                    f"{label}: expected {sorted(expected)}, received {status}: {value}"
                )
            if not isinstance(value, dict):
                raise ValueError("native response must be an object")
            return value
        except Exception as exc:
            event["error"] = {"type": type(exc).__name__, "message": str(exc)}
            raise
        finally:
            event["request_wall_seconds"] = time.monotonic() - started
            with self.lock:
                self.events.append(event)
                self.checkpoint()


def load_inputs(features: Path, profile: str, maximum: int):
    if verified.digest(quality_cases.DEFAULT_SUITE) != verified.V1_SHA256:
        raise ValueError("frozen v1 fixture changed")
    suite = quality_cases.load_suite(quality_cases.DEFAULT_SUITE)
    cases = [
        next(c for c in suite["cases"] if c["id"] == name)
        for name in ("copper_badge_a", "copper_badge_b")
    ]
    store = json.loads(features.read_text())
    if store.get("schema") not in {
        quality_eval.FEATURE_SCHEMA,
        verified.FEATURE_SCHEMA,
    }:
        raise ValueError("requires an existing native quality feature store")
    # Existing v1 store validation binds raw text/vector hashes, full width,
    # unit norms, exact native producer identity, and duplicate rows.
    legacy = {**store, "schema": quality_eval.FEATURE_SCHEMA}
    documents = list(dict.fromkeys(t for c in cases for t in c["documents"]))
    rows = quality_eval.validate_store(legacy, documents)
    bodies = []
    for case in cases:
        bodies.append(
            {
                "model": profile,
                "question": case["question"],
                "document_embeddings": [
                    rows[quality_eval.document_key(t)] for t in case["documents"]
                ],
                "encoder_execution_identity": IDENTITY,
                "max_new_tokens": maximum,
                "context_mode": "source_verified",
                "document_texts": case["documents"],
            }
        )
    return cases, bodies


def negative_cases(a: dict) -> list[tuple[str, dict, set[int]]]:
    cases = []

    def add(name, updates=None, *, remove=(), status=400):
        body = copy.deepcopy(a)
        for key in remove:
            body.pop(key, None)
        body.update(updates or {})
        cases.append((name, body, {status}))

    add("source_missing", remove=("document_texts",))
    add("source_null", {"document_texts": None})
    add("source_count_mismatch", {"document_texts": a["document_texts"][:-1]})
    add("source_empty", {"document_texts": []})
    add("source_blank", {"document_texts": [" \t\n", *a["document_texts"][1:]]})
    add("mode_unsupported", {"context_mode": "automatic"})
    add("mode_null", {"context_mode": None})
    add("explicit_latent_with_sources", {"context_mode": "latent"})
    for options, label in [
        ({"stream": False}, "stream_false"),
        ({"stream": True}, "stream_true"),
        ({"temperature": 0}, "temperature"),
        ({"top_p": 1}, "top_p"),
        ({"top_k": 1}, "top_k"),
        ({"images": []}, "images"),
        ({"audio": []}, "audio"),
        ({"assets": []}, "assets"),
        ({"teacher_forced_ids": [42]}, "teacher_forcing"),
    ]:
        add("unsupported_" + label, options)
    add(
        "individual_source_bytes",
        {"document_texts": ["x" * 8193, *a["document_texts"][1:]]},
        status=413,
    )
    # All feature rows remain real, normalized native output. Only source
    # strings change to exercise source-policy bounds, not bad-vector checks.
    add(
        "aggregate_source_bytes",
        {
            "document_texts": ["x" * 7000] * 5,
            "document_embeddings": [a["document_embeddings"][0]] * 5,
        },
        status=413,
    )
    add(
        "source_framing_tokens",
        {"document_texts": ["source " * 1100, *a["document_texts"][1:]]},
        status=413,
    )
    add("producer_identity", {"encoder_execution_identity": "0" * 64}, status=409)
    add("unknown_profile", {"model": "memorilla-unknown-smoke-profile"}, status=404)
    return cases


def run(args, client=None):
    if args.output.exists():
        raise ValueError("preserve previous smoke receipts; choose a new --output")
    if not 1 <= args.timeout_ms <= 3_600_000 or args.max_new_tokens != 48:
        raise ValueError(
            "smoke protocol fixes max_new_tokens48 and requires a finite deadline"
        )
    cases, bodies = load_inputs(args.features, args.profile, args.max_new_tokens)
    feature_digest = verified.digest(args.features)
    record = {
        "schema": "antfly.memorilla.verified-http-smoke.v1",
        "passed": False,
        "rollout_qualified": False,
        "base_url": args.base_url,
        "profile": args.profile,
        "module_sha256": args.module_sha256,
        "feature_store": str(args.features),
        "feature_store_sha256": feature_digest,
        "events": [],
        "checks": {},
        "source_sha256": {
            p.name: verified.digest(p)
            for p in (
                Path(__file__),
                verified.HERE / "verified_quality_eval.py",
                verified.HERE / "quality_eval.py",
                verified.HERE / "quality_cases.py",
                quality_cases.DEFAULT_SUITE,
            )
        },
        "local_binary_sha256": verified.digest(args.native_binary)
        if args.native_binary
        else None,
        "binary_provenance": "controller local artifact fingerprint, not HTTP attestation",
        "protocol": {
            "cases": [c["id"] for c in cases],
            "expected_answers": ["Tovik", "Mivra"],
            "max_new_tokens": 48,
            "isolation": "A/B/A then concurrent A/B then1ms deadline and exact A recovery",
            "feature_text_alignment": "caller_supplied, not authenticated",
            "truth": "model-entailment checked, not guaranteed",
            "negative_cases_use_valid_native_vectors": True,
            "default_disabled": "native unit-test responsibility; no live config mutation",
            "legacy_latent": "execution/schema check only, no factual-quality claim",
        },
        "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    def checkpoint():
        verified.save(args.output, record)

    checkpoint()
    client = client or Client(args, record["events"], checkpoint)
    try:
        for label, body, expected in negative_cases(bodies[0]):
            client.post(body, expected, label=label)
        record["checks"]["negative_requests"] = True

        def positive(index, label):
            body = bodies[index]
            value = client.post(body, {200}, label=label)
            verified.validate_response(
                value,
                profile=args.profile,
                module_sha256=args.module_sha256,
                documents=body["document_texts"],
                maximum=48,
            )
            score = quality_cases.score_answer(value["text"], cases[index])
            if not score["correct"]:
                raise ValueError(
                    f"{label}: source-verified answer is factually wrong: {value['text']}"
                )
            return value

        a = positive(0, "A_first")
        b = positive(1, "B")
        again = positive(0, "A_after_B")
        if again != a:
            raise ValueError("A changed after B")
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [
                pool.submit(positive, i, label)
                for i, label in ((0, "A_concurrent"), (1, "B_concurrent"))
            ]
            together = [job.result() for job in jobs]
        if together != [a, b]:
            raise ValueError("concurrent A/B responses differ")
        client.post(bodies[0], {408, 504}, label="deadline_1ms", timeout_ms=1)
        recovered = positive(0, "A_after_deadline")
        if recovered != a:
            raise ValueError("A changed after deadline cancellation")
        # The legacy omitted mode must retain its old wire contract.
        legacy = {
            k: v
            for k, v in bodies[0].items()
            if k not in {"context_mode", "document_texts"}
        }
        latent = client.post(legacy, {200}, label="legacy_latent")
        quality_eval.validate_response(
            latent,
            profile=args.profile,
            module_sha256=args.module_sha256,
            documents=len(legacy["document_embeddings"]),
            max_new_tokens=48,
        )
        forbidden_fields = {
            "answer_path",
            "verification_status",
            "source_alignment",
            "document_text_sha256s",
        }
        if (
            forbidden_fields & latent.keys()
            or {
                "total_prompt_tokens",
                "total_completion_tokens",
                "verification_calls",
                "source_fallback_used",
            }
            & latent["usage"].keys()
        ):
            raise ValueError("legacy latent response gained source-mode-only fields")
        if verified.digest(args.features) != feature_digest:
            raise ValueError("native feature store changed during smoke")
        record["checks"].update(
            known_answers=True,
            A_B_A_exact=True,
            concurrent_A_B_exact=True,
            deadline_recovery_exact=True,
            legacy_schema_unchanged=True,
            feature_store_unchanged=True,
        )
        record["passed"] = True
    except BaseException as exc:
        record["error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        record["finished_utc"] = datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat()
        checkpoint()
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:18091")
    parser.add_argument("--profile", default="memorilla-stage3")
    parser.add_argument("--module-sha256", default=STAGE3_SHA256)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--native-binary", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-new-tokens", type=int, default=48)
    parser.add_argument("--timeout-ms", type=int, default=300000)
    result = run(parser.parse_args())
    print(
        json.dumps(
            {
                "passed": result["passed"],
                "checks": result["checks"],
                "rollout_qualified": False,
            }
        )
    )


if __name__ == "__main__":
    main()
