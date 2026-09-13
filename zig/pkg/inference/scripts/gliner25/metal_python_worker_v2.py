#!/usr/bin/env python3
"""Versioned wire adapter around the unchanged pinned FP32 reference worker.

Extraction, strict device checks, fallback rejection, clocks and command
handling are the v1 functions. Only the enclosing comparison scope changes.
"""
from __future__ import annotations

import sys

import metal_python_worker as reference
from metal_runtime_contract_v2 import SCOPE


def event_v2(event):
    if "scope" in event and event["scope"] != reference.SCOPE:
        raise reference.WorkerError("reference worker emitted an unexpected scope")
    return {**event, "scope": SCOPE}


def python_worker(args) -> int:
    if args.device not in reference.SYNC_POLICIES or not 1 <= args.max_commands <= 4096:
        raise reference.WorkerError("worker profile or command limit is invalid")
    reference.configure_environment()
    with reference.reject_mps_fallback():
        provenance, torch = reference.oracle.prepare_runtime(args.upstream)
        torch.set_num_interop_threads(1)
        reference.verify_runtime(torch, args.device)
        bundle = reference.oracle.verify_model_dir(args.model, args.model_dir)
        requests, digest = reference.read_requests(reference.oracle.FIXTURES / "requests.json")
        from gliner2 import AutoExtractor
        with reference.reject_mps_fallback():
            model = reference.load_model(AutoExtractor, args.model_dir, args.device)
            reference.synchronize(torch, args.device)
            reference.bench.emit(event_v2(reference.ready_event(args, model, torch, bundle, provenance, digest)))
        return reference.serve_commands(
            args, model, torch, bundle, requests, digest,
            emit=lambda event: reference.bench.emit(event_v2(event)),
        )


def main(argv=None) -> int:
    try:
        return python_worker(reference.parse_args(argv))
    except Exception as error:
        print(f"{type(error).__name__}: {str(error)[:reference.MAX_ERROR_CHARS]}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
