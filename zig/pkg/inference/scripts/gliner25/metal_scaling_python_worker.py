#!/usr/bin/env python3
"""Pinned Fastino true-batch worker for the separate FP32 scaling inputs."""
from __future__ import annotations

import argparse
import contextlib
import json
from pathlib import Path
import sys
import time

import metal_python_worker as reference
import scaling_contract_v1 as contract

bench, oracle = reference.bench, reference.oracle


def execute_batch(model, case, schema_json):
    """Use source batch APIs once; schema compilation stays in the clock."""
    specification = bench.strict_json(schema_json)
    texts = [item["text"] for item in case["items"]]
    if case["kind"] == "extract":
        return model.batch_extract(texts, oracle.build_extract_schema(specification),
            batch_size=len(texts), num_workers=0, threshold=0.5,
            include_confidence=True, include_spans=True, max_len=contract.MAX_WORDS)
    if case["kind"] == "joint_ie":
        from gliner2.joint_ie import JointIE, JointIEConfig, JointSchema
        joint = JointIE(model)
        compiled = joint.compile_schema(JointSchema.from_dict(specification))
        results = joint.batch_extract(texts, compiled,
            config=JointIEConfig(batch_size=len(texts), max_len=contract.MAX_WORDS))
        if any(not result.feasible for result in results):
            raise reference.WorkerError("source batch JointIE did not produce feasible results")
        return [result.to_dict() for result in results]
    raise reference.WorkerError("unsupported fixed scaling template")


def canonical_batch(case, raw):
    if not isinstance(raw, list) or len(raw) != len(case["items"]):
        raise reference.WorkerError("source batch returned an incomplete output list")
    result = [bench.canonical_python({"kind": case["kind"], "schema": case["schema"],
              "text": item["text"], "id": item["id"]}, value)
              for item, value in zip(case["items"], raw)]
    reference.finite_output(result)
    return result


@contextlib.contextmanager
def capture_batch(model, torch, device, case):
    captured = []

    def capture(_module, positional, keyword):
        ids = keyword.get("input_ids", positional[0] if positional else None)
        mask = keyword.get("attention_mask", positional[1] if len(positional) > 1 else None)
        if not torch.is_tensor(ids) or not torch.is_tensor(mask):
            raise reference.WorkerError("validation encoder requires actual IDs and attention mask")
        observed = {}

        def visit(value, path, depth=0):
            if depth > 8 or len(observed) > 128:
                raise reference.WorkerError("encoder metadata exceeds validation bound")
            if torch.is_tensor(value):
                actual = reference.normalized_device(value.device)
                if actual != device or value.is_complex() or (value.is_floating_point() and value.dtype != torch.float32):
                    raise reference.WorkerError("encoder input device or floating dtype differs")
                observed[path] = actual
            elif isinstance(value, (tuple, list)):
                for index, entry in enumerate(value):
                    visit(entry, f"{path}[{index}]", depth + 1)
            elif isinstance(value, dict):
                for name, entry in value.items():
                    visit(entry, f"{path}.{name}", depth + 1)

        visit(positional, "args")
        visit(keyword, "kwargs")
        expected = contract.packet(case["expected_input_ids"])
        if list(ids.shape) != expected["input_shape"] or list(mask.shape) != expected["input_shape"]:
            raise reference.WorkerError("encoder did not execute the declared single true batch")
        result = {"input_shape": list(ids.shape),
                  "input_ids": ids.detach().reshape(-1).cpu().tolist(),
                  "attention_mask": mask.detach().reshape(-1).cpu().tolist(),
                  "input_device": reference.normalized_device(ids.device),
                  "encoder_input_devices": observed}
        contract.check_packet(result, case["expected_input_ids"])
        captured.append(result)
        if len(captured) > 1:
            raise reference.WorkerError("source split the declared batch into multiple encoder calls")

    handle = model.encoder.register_forward_pre_hook(capture, with_kwargs=True)
    try:
        yield captured
    finally:
        handle.remove()


def serve(args, model, torch, bundle, inputs, *, source=None, emit=bench.emit,
          execute=execute_batch, canonical=canonical_batch, clock=time.perf_counter_ns):
    source = sys.stdin.buffer if source is None else source
    cases = inputs["source_cases"]
    schemas = {name: json.dumps(row["schema"], ensure_ascii=False) for name, row in cases.items()}
    validated, previous, count = set(), 0, 0
    arm = f"fastino_{args.device}"
    while line := source.readline(reference.MAX_COMMAND_BYTES + 1):
        if len(line) > reference.MAX_COMMAND_BYTES or not line.endswith(b"\n") or count >= args.max_commands:
            raise reference.WorkerError("worker command byte/count bound exceeded")
        count += 1
        cmd = bench.strict_json(line)
        if (not isinstance(cmd, dict) or not {"op", "request_id"} <= cmd.keys()
                or not cmd.keys() <= {"op", "request_id", "case_id"}
                or type(cmd["request_id"]) is not int or cmd["request_id"] <= previous):
            raise reference.WorkerError("invalid worker command identity")
        previous = cmd["request_id"]
        if cmd["op"] == "stop":
            if cmd.get("case_id", "") != "":
                raise reference.WorkerError("stop cannot select a case")
            if oracle.verify_model_dir(args.model, args.model_dir) != bundle:
                raise reference.WorkerError("model bundle changed during batch extraction")
            oracle.verify_upstream_checkout(args.upstream)
            if contract.load(args.prepared, args.model)["pins"] != inputs["pins"]:
                raise reference.WorkerError("scaling inputs changed during extraction")
            reference.verify_runtime(torch, args.device)
            reference.verify_model_tensors(model, torch, args.device)
            emit({"event": "stopped", "arm": arm, "scope": contract.SCOPE, "request_id": previous,
                  "qualification": False, "requests_sha256": inputs["requests_sha256"]})
            return 0
        name = cmd.get("case_id")
        if cmd["op"] not in ("validate", "run") or not isinstance(name, str) or name not in cases:
            raise reference.WorkerError("unknown scaling case or operation")
        case = cases[name]
        try:
            reference.verify_runtime(torch, args.device)
            if model.strict_extraction is not True:
                raise reference.WorkerError("strict_extraction changed during extraction")
            validating = cmd["op"] == "validate"
            if validating:
                reference.verify_model_tensors(model, torch, args.device)
                validated.discard(name)
            elif name not in validated:
                raise reference.WorkerError("run requires successful actual batch validation")
            capture = capture_batch(model, torch, args.device, case) if validating else contextlib.nullcontext([])
            with reference.reject_mps_fallback(), capture as observed:
                raw, duration = reference.timed_call(torch, args.device,
                    lambda: execute(model, case, schemas[name]), clock=clock)
            if validating and len(observed) != 1:
                raise reference.WorkerError("validation requires exactly one encoder execution")
            outputs = canonical(case, raw)
            del raw
            if validating:
                validated.add(name)
            emit({"event": "result", "arm": arm, "scope": contract.SCOPE,
                "request_id": previous, "case_id": name, "duration_ns": duration,
                "outputs": outputs, "output": outputs[0],
                **(observed[0] if observed else {"input_ids": None, "attention_mask": None,
                    "input_shape": [len(case["items"]), max(map(len, case["expected_input_ids"]))],
                    "input_device": None, "encoder_input_devices": None}),
                "mps_memory": reference.mps_memory(torch, args.device)})
        except Exception as error:
            details = reference.error_details(error, args.device)
            emit({"event": "error", "arm": arm, "scope": contract.SCOPE,
                  "request_id": previous, "case_id": name, **details})
            if not details["recoverable"]:
                return 1
    raise reference.WorkerError("protocol ended without explicit stop")


def worker(args):
    if args.device not in reference.SYNC_POLICIES or not 1 <= args.max_commands <= 4096:
        raise reference.WorkerError("invalid scaling worker profile")
    reference.configure_environment()
    inputs = contract.load(args.prepared, args.model)
    with reference.reject_mps_fallback():
        provenance, torch = oracle.prepare_runtime(args.upstream)
        torch.set_num_interop_threads(1)
        reference.verify_runtime(torch, args.device)
        bundle = oracle.verify_model_dir(args.model, args.model_dir)
        from gliner2 import AutoExtractor
        with reference.reject_mps_fallback():
            model = reference.load_model(AutoExtractor, args.model_dir, args.device)
            reference.synchronize(torch, args.device)
            ready = reference.ready_event(args, model, torch, bundle, provenance, inputs["requests_sha256"])
            ready.update(scope=contract.SCOPE, workload="scaling",
                         cases_sha256=inputs["pins"]["native_inputs"]["sha256"],
                         scaling_input_pins=inputs["pins"], max_batch_items=8, max_text_words=512)
            bench.emit(ready)
        return serve(args, model, torch, bundle, inputs)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--device", choices=("mps", "cpu"), required=True)
    parser.add_argument("--model", choices=contract.VARIANTS, required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--max-commands", type=int, default=2048)
    try:
        return worker(parser.parse_args(argv))
    except Exception as error:
        print(f"{type(error).__name__}: {str(error)[:reference.MAX_ERROR_CHARS]}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
