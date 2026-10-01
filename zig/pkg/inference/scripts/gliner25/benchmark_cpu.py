#!/usr/bin/env python3
"""Supervised, paired GLiNER2.5 CPU/Fastino direct-core benchmark.

Model loading and protocol JSON are excluded. Each timed call parses/compiles
its schema, preprocesses, encodes, scores every requested head, decodes, and
releases temporary state. Never runs workers concurrently or downloads models.
"""
from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
from pathlib import Path
import platform
import selectors
import signal
import subprocess
import sys
import time
from typing import Any

import oracle
import generate_pipeline_cases as adaptation

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import paired_benchmark

HERE = Path(__file__).resolve().parent
SCOPE = "gliner25_direct_core_cpu_fp32"
TIMING_BOUNDARY = "schema_parse_compile+processor+encoder+heads+decode+temporary_cleanup"
THREAD_ENV = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS")
MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class BenchmarkError(RuntimeError):
    pass


def strict_json(text: str | bytes) -> Any:
    def invalid(value):
        raise BenchmarkError(f"non-finite protocol JSON: {value}")
    return json.loads(text, object_pairs_hook=oracle._unique_object, parse_constant=invalid)


def emit(value: Any) -> None:
    text = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    if len(text.encode("utf-8")) > MAX_RESPONSE_BYTES:
        raise BenchmarkError("worker response exceeds byte limit")
    print(text, flush=True)


def canonical_value(item: dict[str, Any]) -> dict[str, Any]:
    source = item.get("source")
    return {"text": item["text"], "confidence": item["confidence"],
            "source": None if source is None else {"start": source["start"], "end": source["end"]},
            "attributes": sorted(({"name": group["name"], "labels": group["labels"]} for group in item.get("attributes", [])), key=lambda group: group["name"])}


def canonical_result(output: dict[str, Any]) -> dict[str, Any]:
    """Strip internal coordinates/diagnostics, retaining all public values."""
    return {
        "entities": [{"name": group["name"], "values": [canonical_value(value) for value in group["values"]]} for group in output["entities"]],
        "classifications": [{"name": group["name"], "labels": group["labels"]} for group in output["classifications"]],
        "structures": [{"name": group["name"], "instances": [{"fields": [{"name": field["name"], "values": [canonical_value(value) for value in field["values"]]} for field in record["fields"]]} for record in group["instances"]]} for group in output["structures"]],
        "relations": [{"name": edge["name"], "head": canonical_value(edge["head"]), "tail": canonical_value(edge["tail"]),
                       "confidence": edge["confidence"], "derived": edge.get("derived", False),
                       "head_entity_type": edge.get("head_entity_type"), "tail_entity_type": edge.get("tail_entity_type")} for edge in output["relations"]],
    }


def canonical_python(request: dict[str, Any], output: dict[str, Any]) -> dict[str, Any]:
    schema = adaptation.schema_for(request)
    result = {"entities": [], "classifications": [], "structures": [], "relations": []}
    if request["kind"] == "joint_ie":
        types = list(request["schema"]["entities"])
        entities = {entity["id"]: entity for entity in output["entities"]}
        if len(entities) != len(output["entities"]):
            raise BenchmarkError("duplicate JointIE entity identity")
        for name in types:
            result["entities"].append({"name": name, "values": adaptation.values([entity for entity in output["entities"] if entity["type"] == name])})
        for edge in output["relations"]:
            head, tail = entities[edge["head"]], entities[edge["tail"]]
            result["relations"].append({"name": edge["type"], "head": adaptation.values(head)[0], "tail": adaptation.values(tail)[0],
                                        "confidence": edge["confidence"], "derived": edge.get("derived", False),
                                        "head_entity_type": types.index(head["type"]), "tail_entity_type": types.index(tail["type"])})
    else:
        for name in schema.get("entities", []):
            result["entities"].append({"name": name, "values": adaptation.values(output["entities"][name], schema.get("entity_attributes", {}))})
        for task in schema.get("classifications", []):
            result["classifications"].append({"name": task["name"], "labels": adaptation.labels(output[task["name"]])})
        for name, structure in schema.get("structures", {}).items():
            instances = [{"fields": [{"name": field, "values": adaptation.values(record[field])} for field in structure["fields"]]} for record in output.get(name, [])]
            if instances:
                result["structures"].append({"name": name, "instances": instances})
        for name, edges in output.get("relation_extraction", {}).items():
            result["relations"].extend({"name": name, "head": adaptation.values(edge["head"])[0], "tail": adaptation.values(edge["tail"])[0], "confidence": edge["head"]["confidence"]} for edge in edges)
    return canonical_result(result)


def require_equal(expected: Any, actual: Any, path: str = "output") -> None:
    """Exact decisions, strings, ordering, and offsets; FP32 confidence only."""
    if isinstance(expected, dict):
        if not isinstance(actual, dict) or expected.keys() != actual.keys():
            raise BenchmarkError(f"{path}: output keys differ")
        for key in expected:
            require_equal(expected[key], actual[key], f"{path}.{key}")
    elif isinstance(expected, list):
        if not isinstance(actual, list) or len(expected) != len(actual):
            raise BenchmarkError(f"{path}: output count differs")
        for i, (left, right) in enumerate(zip(expected, actual)):
            require_equal(left, right, f"{path}[{i}]")
    elif path.endswith(".confidence"):
        if isinstance(actual, bool) or not isinstance(actual, (int, float)) or not math.isfinite(actual) or abs(expected - actual) > 5e-4:
            raise BenchmarkError(f"{path}: confidence differs ({expected!r}, {actual!r})")
    elif type(expected) is not type(actual) or expected != actual:
        raise BenchmarkError(f"{path}: value differs ({expected!r}, {actual!r})")


def execute_python(model: Any, request: dict[str, Any], schema_json: str) -> Any:
    # Payload/protocol parsing is outside the clock on both arms. The schema
    # parser that is part of each implementation's compile API is included.
    specification = strict_json(schema_json)
    if request["kind"] == "extract":
        return model.extract(request["text"], oracle.build_extract_schema(specification), threshold=0.5,
                             include_confidence=True, include_spans=True, max_len=oracle.MAX_WORDS)
    if request["kind"] == "classification":
        from gliner2.classification import Classifier, ClassificationConfig, ClassificationSchema
        classifier = Classifier(model)
        compiled = classifier.compile_schema(ClassificationSchema.from_dict(specification))
        config = ClassificationConfig(on_infeasible="raise", max_len=oracle.MAX_WORDS)
        return classifier.decode(classifier.score(request["text"], compiled, config=config), compiled, config=config).to_dict()
    if request["kind"] == "joint_ie":
        from gliner2.joint_ie import JointIE, JointIEConfig, JointSchema
        joint = JointIE(model)
        compiled = joint.compile_schema(JointSchema.from_dict(specification))
        result = joint.extract(request["text"], compiled, config=JointIEConfig(max_len=oracle.MAX_WORDS))
        if not result.feasible:
            raise BenchmarkError("Python JointIE did not produce a feasible result")
        return result.to_dict()
    raise BenchmarkError("unsupported benchmark request kind")


@contextlib.contextmanager
def capture_encoder_input_ids(model: Any):
    """Observe the actual encoder shared by extract, Classifier and JointIE.

    Pinned ClassificationScorer calls model.encoder directly and bypasses
    _encode_core. A module pre-hook covers both routes without changing the
    executed function. The handle is removed even if execution fails.
    """
    captured = []

    def capture(_module, positional, keyword):
        tensor = keyword.get("input_ids")
        if tensor is None and positional:
            tensor = positional[0]
        if tensor is None:
            raise BenchmarkError("validation encoder did not receive input IDs")
        ids = tensor.detach().cpu().tolist()
        if (not isinstance(ids, list) or len(ids) != 1 or not isinstance(ids[0], list)
                or not ids[0] or len(ids[0]) > oracle.MAX_ENCODED_TOKENS
                or any(type(token) is not int for token in ids[0])):
            raise BenchmarkError("validation encoder batch exceeds bounded benchmark scope")
        captured.append(ids[0])

    handle = model.encoder.register_forward_pre_hook(capture, with_kwargs=True)
    try:
        yield captured
    finally:
        handle.remove()


def python_worker(args: argparse.Namespace) -> None:
    if not 1 <= args.threads <= 32 or not 1 <= args.max_commands <= 4096:
        raise BenchmarkError("worker limits are invalid")
    for name in THREAD_ENV:
        if os.environ.get(name) != str(args.threads):
            raise BenchmarkError(f"thread environment is not explicit: {name}")
    provenance, torch = oracle.prepare_runtime(args.upstream)
    # prepare_runtime establishes the immutable oracle's one-thread profile.
    # This diagnostic harness permits an explicit alternate thread budget,
    # then revalidates its outputs before any measurements are admitted.
    for name in THREAD_ENV:
        os.environ[name] = str(args.threads)
    torch.set_num_threads(args.threads)
    torch.set_num_interop_threads(1)
    provenance["threads"] = torch.get_num_threads()
    bundle = oracle.verify_model_dir(args.model, args.model_dir)
    requests = oracle.read_json(oracle.FIXTURES / "requests.json")["requests"]
    by_id = {request["id"]: request for request in requests}
    schemas = {request["id"]: json.dumps(request["schema"], ensure_ascii=False) for request in requests}
    from gliner2 import AutoExtractor
    model = AutoExtractor.from_pretrained(str(args.model_dir.resolve()), local_files_only=True, map_location="cpu", use_flashdeberta=False).float().eval()
    if model.architecture != "boundary":
        raise BenchmarkError("benchmark did not load the boundary architecture")
    emit({"event": "ready", "arm": "python", "scope": SCOPE, "timing_boundary": TIMING_BOUNDARY,
          "model": args.model, "model_id": bundle["model_id"], "revision": bundle["revision"], "model_files": bundle["files"],
          "dtype": "float32", "threads": torch.get_num_threads(), "interop_threads": torch.get_num_interop_threads(),
          "provenance": provenance, "qualification": False})
    count = 0
    while line := sys.stdin.buffer.readline(2049):
        if len(line) > 2048 or not line.endswith(b"\n") or count >= args.max_commands:
            raise BenchmarkError("worker command limit exceeded")
        count += 1
        command = strict_json(line)
        if command["op"] == "stop":
            if oracle.verify_model_dir(args.model, args.model_dir) != bundle:
                raise BenchmarkError("model bundle changed during benchmark")
            oracle.verify_upstream_checkout(args.upstream)
            emit({"event": "stopped", "request_id": command["request_id"]})
            return
        if command["op"] not in ("validate", "run") or command["case_id"] not in by_id:
            raise BenchmarkError("unknown benchmark command")
        request = by_id[command["case_id"]]
        capture = capture_encoder_input_ids(model) if command["op"] == "validate" else contextlib.nullcontext([])
        with capture as captured:
            with torch.inference_mode():
                started = time.perf_counter_ns()
                output = execute_python(model, request, schemas[request["id"]])
                duration = time.perf_counter_ns() - started
        if command["op"] == "validate" and len(captured) != 1:
            raise BenchmarkError(f"{request['id']}: expected one encoder execution, observed {len(captured)}")
        emit({"event": "result", "arm": "python", "request_id": command["request_id"], "case_id": request["id"],
              "duration_ns": duration, "input_ids": captured[0] if captured else None,
              "output": canonical_python(request, output)})
    raise BenchmarkError("protocol ended without explicit stop")


class ResourceGuard:
    def __init__(self, max_rss_bytes: int):
        import psutil
        self.psutil = psutil
        self.max_rss_bytes = max_rss_bytes
        self.peak_rss_bytes = 0
        self.workers: list[Worker] = []

    def check(self) -> None:
        total = 0
        for worker in self.workers:
            try:
                total += self.psutil.Process(worker.process.pid).memory_info().rss
            except self.psutil.NoSuchProcess:
                continue
            if worker.log_path.stat().st_size > 64 * 1024 * 1024:
                raise BenchmarkError("worker stderr exceeded 64 MiB")
        self.peak_rss_bytes = max(self.peak_rss_bytes, total)
        if total > self.max_rss_bytes:
            raise BenchmarkError(f"combined worker RSS {total} exceeds {self.max_rss_bytes}")


class Worker:
    def __init__(self, arm: str, command: list[str], env: dict[str, str], directory: Path, guard: ResourceGuard):
        self.arm = arm
        self.log_path = directory / f"{arm}.stderr.log"
        self.log = self.log_path.open("wb")
        try:
            self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log,
                                            env=env, bufsize=0, start_new_session=True)
        except BaseException:
            self.log.close()
            raise
        self.guard = guard
        guard.workers.append(self)
        self.buffer = bytearray()
        self.sequence = 0
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ)

    def receive(self, timeout: float) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while b"\n" not in self.buffer:
            self.guard.check()
            if time.monotonic() >= deadline:
                raise BenchmarkError(f"{self.arm}: response deadline exceeded")
            events = self.selector.select(min(0.1, max(0, deadline - time.monotonic())))
            if events:
                data = os.read(self.process.stdout.fileno(), 65536)
                if not data:
                    raise BenchmarkError(f"{self.arm}: exited before response (code {self.process.poll()}); see {self.log_path}")
                self.buffer.extend(data)
                if len(self.buffer) > MAX_RESPONSE_BYTES:
                    raise BenchmarkError(f"{self.arm}: oversized response")
        line, _, rest = self.buffer.partition(b"\n")
        self.buffer = bytearray(rest)
        return strict_json(line)

    def request(self, op: str, case_id: str = "", timeout: float = 60) -> dict[str, Any]:
        self.sequence += 1
        data = json.dumps({"request_id": self.sequence, "op": op, "case_id": case_id}).encode() + b"\n"
        self.process.stdin.write(data)
        self.process.stdin.flush()
        result = self.receive(timeout)
        if result.get("request_id") != self.sequence or result.get("event") != ("stopped" if op == "stop" else "result"):
            raise BenchmarkError(f"{self.arm}: response identity mismatch")
        if op != "stop" and (result.get("arm") != self.arm or result.get("case_id") != case_id or type(result.get("duration_ns")) is not int or result["duration_ns"] <= 0):
            raise BenchmarkError(f"{self.arm}: invalid measured result")
        return result

    def close(self) -> None:
        if self.process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.process.pid, signal.SIGTERM)
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=2)
        self.selector.close()
        self.process.stdin.close()
        self.process.stdout.close()
        self.log.close()


def case_fixture(variant: str) -> Path:
    return oracle.FIXTURES / ("pipeline_cases.json" if variant == "small" else f"pipeline_cases_{variant}.json")


def checked_ready(arm: str, ready: dict[str, Any], bundle: dict[str, Any], cases_path: Path, threads: int) -> None:
    fields = {"event": "ready", "arm": arm, "scope": SCOPE, "timing_boundary": TIMING_BOUNDARY,
              "model_id": bundle["model_id"], "revision": bundle["revision"], "model_files": bundle["files"],
              "dtype": "float32", "threads": threads, "qualification": False}
    for key, expected in fields.items():
        if ready.get(key) != expected:
            raise BenchmarkError(f"{arm} ready contract differs: {key}")
    if arm == "native" and (ready.get("build_mode") != "ReleaseFast" or ready.get("scheduler") != "serial_io" or ready.get("cases_sha256") != oracle.sha256_file(cases_path)):
        raise BenchmarkError("native build, scheduling, or fixture identity is not qualified for timing")
    if arm == "python" and ready.get("interop_threads") != 1:
        raise BenchmarkError("Python interop thread budget differs")


def run_variant(args: argparse.Namespace, variant: str, directory: Path) -> dict[str, Any]:
    directory.mkdir()
    model_dir = args.model_root / variant
    bundle = oracle.verify_model_dir(variant, model_dir)
    case_path = case_fixture(variant)
    fixture = oracle.read_json(case_path)
    regenerated = adaptation.generate(variant, oracle.FIXTURES / f"{variant}_reference", oracle.FIXTURES / "requests.json")
    # Check insertion order too: it controls prompt construction.
    if json.dumps(fixture, ensure_ascii=False) != json.dumps(regenerated, ensure_ascii=False):
        raise BenchmarkError("canonical fixture differs from the verified oracle adaptation")
    all_cases = {case["id"]: case for case in fixture["cases"]}
    selected = args.cases or list(all_cases)
    if len(selected) > 32 or len(set(selected)) != len(selected) or any(case not in all_cases for case in selected):
        raise BenchmarkError("case selection must be unique bounded captured requests")
    command_count = len(selected) * (1 + args.warmup + args.pairs) + 1
    if command_count > 4096:
        raise BenchmarkError("requested benchmark exceeds worker command budget")
    expected = {name: canonical_result(all_cases[name]["expected"]) for name in selected}
    env = dict(os.environ)
    env.update({name: str(args.threads) for name in THREAD_ENV})
    env.update(PYTHONDONTWRITEBYTECODE="1", TOKENIZERS_PARALLELISM="false", HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1")
    env.pop("USE_FLASHDEBERTA", None)
    native_command = [str(args.native_bin), "--model-dir", str(model_dir), "--cases", str(case_path), "--threads", str(args.threads), "--timeout-ms", str(args.timeout_ms), "--max-commands", str(command_count)]
    python_command = [sys.executable, str(Path(__file__).resolve()), "worker", "--model", variant, "--model-dir", str(model_dir), "--upstream", str(args.upstream), "--threads", str(args.threads), "--max-commands", str(command_count)]
    guard = ResourceGuard(args.max_rss_mib * 1024 * 1024)
    workers = {}
    readies = {}
    try:
        # Startup is serial; only model residency overlaps. No timing begins
        # until both workers have independently verified their artifacts.
        for arm, command in (("python", python_command), ("native", native_command)):
            workers[arm] = Worker(arm, command, env, directory, guard)
            ready = workers[arm].receive(args.startup_timeout)
            checked_ready(arm, ready, bundle, case_path, args.threads)
            readies[arm] = ready
        validation = {}
        for name in selected:
            responses = {arm: workers[arm].request("validate", name, args.timeout_ms / 1000 + 5) for arm in ("python", "native")}
            for arm, response in responses.items():
                normalized = canonical_result(response["output"])
                require_equal(expected[name], normalized, f"{variant}.{name}.{arm}")
            left, right = responses["python"]["input_ids"], responses["native"]["input_ids"]
            if not isinstance(left, list) or not left or len(left) > oracle.MAX_ENCODED_TOKENS or any(type(token) is not int for token in left) or left != right:
                raise BenchmarkError(f"{variant}.{name}: encoder token identity differs")
            validation[name] = {"input_ids": left, "outputs_match_oracle": True, "confidence_absolute_tolerance": 5e-4,
                                "expected": expected[name], "python": canonical_result(responses["python"]["output"]),
                                "native": canonical_result(responses["native"]["output"])}
        for iteration in range(args.warmup):
            for name in selected:
                for arm in paired_benchmark.balanced_pair_order(iteration + 1, "native", "python"):
                    response = workers[arm].request("run", name, args.timeout_ms / 1000 + 5)
                    require_equal(expected[name], canonical_result(response["output"]), f"warmup.{variant}.{name}.{arm}")
        rows = []
        for pair in range(1, args.pairs + 1):
            for name in selected:
                row = {"pair": pair, "case_id": name, "order": paired_benchmark.balanced_pair_order(pair, "native", "python")}
                for arm in row["order"]:
                    response = workers[arm].request("run", name, args.timeout_ms / 1000 + 5)
                    require_equal(expected[name], canonical_result(response["output"]), f"measured.{variant}.{name}.{arm}")
                    row[arm] = {"duration_ns": response["duration_ns"]}
                rows.append(row)
        for worker in workers.values():
            worker.request("stop", timeout=args.startup_timeout)
            if worker.process.wait(timeout=5) != 0:
                raise BenchmarkError(f"{worker.arm}: nonzero worker exit after stop")
        if oracle.verify_model_dir(variant, model_dir) != bundle:
            raise BenchmarkError("bundle changed after measured calls")
        oracle.verify_upstream_checkout(args.upstream)
        comparisons = {}
        for name in selected:
            pairs = [(row["native"]["duration_ns"], row["python"]["duration_ns"]) for row in rows if row["case_id"] == name]
            comparisons[name] = {"native_ns": paired_benchmark.distribution(n for n, _ in pairs),
                                 "python_ns": paired_benchmark.distribution(p for _, p in pairs),
                                 "native_over_python_latency": paired_benchmark.paired_log_ratio_ci(pairs, samples=2000)}
        return {"model": variant, "model_bundle": bundle, "workers": readies, "validation": validation, "pairs": rows,
                "comparisons": comparisons, "combined_peak_observed_rss_bytes": guard.peak_rss_bytes,
                "rss_sampling_period_seconds": 0.1, "max_combined_rss_bytes": guard.max_rss_bytes,
                "startup_commands": {"native": native_command, "python": python_command}}
    finally:
        for worker in workers.values():
            worker.close()


def driver(args: argparse.Namespace) -> None:
    if not 1 <= args.threads <= 32 or not 0 <= args.warmup <= 8 or not 2 <= args.pairs <= 64 or not 1 <= args.timeout_ms <= 60000 or not 1 <= args.startup_timeout <= 300 or not 256 <= args.max_rss_mib <= 12288:
        raise BenchmarkError("invalid benchmark resource or sampling limits")
    if args.output.exists() or not args.native_bin.is_file():
        raise BenchmarkError("requires a new output directory and an existing native binary")
    oracle.verify_config_fixtures()
    oracle.verify_reference_fixtures()
    oracle.verify_dependencies()
    provenance = oracle.verify_upstream_checkout(args.upstream)
    args.output.mkdir(parents=True)
    report = {"format_version": 1, "status": "running", "scope": SCOPE, "timing_boundary": TIMING_BOUNDARY,
              "serving_qualified": False, "performance_release_qualified": False,
              "excluded": ["process_startup", "model_loading", "HTTP", "admission", "model_resolution", "wire_JSON_serialization", "returned_result_destruction"],
              "threads": args.threads, "warmup_per_case": args.warmup, "pairs_per_case": args.pairs,
              "native_binary": {"path": str(args.native_bin), "sha256": oracle.sha256_file(args.native_bin)},
              "driver_sha256": oracle.sha256_file(Path(__file__)), "source": provenance,
              "platform": {"system": platform.system(), "machine": platform.machine(), "processor": platform.processor()},
              "models": []}
    try:
        for variant in (("small", "base", "multi") if args.model == "all" else (args.model,)):
            report["models"].append(run_variant(args, variant, args.output / variant))
        report["status"] = "complete"
        report["parity_validated"] = True
        if oracle.sha256_file(args.native_bin) != report["native_binary"]["sha256"]:
            raise BenchmarkError("native executable changed during benchmark")
    except BaseException as error:
        report["status"] = "failed"
        report["parity_validated"] = False
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        oracle.write_json(args.output / "report.json", report)
        paired_benchmark.write_evidence_manifest(args.output)
    print(json.dumps({"status": report["status"], "scope": SCOPE, "report": str(args.output / "report.json")}))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--native-bin", type=Path, required=True)
    run.add_argument("--model", choices=("small", "base", "multi", "all"), default="small")
    run.add_argument("--model-root", type=Path, required=True)
    run.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--cases", nargs="+")
    run.add_argument("--threads", type=int, default=1)
    run.add_argument("--warmup", type=int, default=2)
    run.add_argument("--pairs", type=int, default=6)
    run.add_argument("--timeout-ms", type=int, default=30000)
    run.add_argument("--startup-timeout", type=float, default=120)
    run.add_argument("--max-rss-mib", type=int, default=8192)
    worker = commands.add_parser("worker")
    worker.add_argument("--model", choices=("small", "base", "multi"), required=True)
    worker.add_argument("--model-dir", type=Path, required=True)
    worker.add_argument("--upstream", type=Path, required=True)
    worker.add_argument("--threads", type=int, default=1)
    worker.add_argument("--max-commands", type=int, default=2048)
    args = parser.parse_args()
    for name in ("native_bin", "model_root", "model_dir", "upstream", "output"):
        if hasattr(args, name):
            setattr(args, name, getattr(args, name).expanduser().resolve())
    if args.command == "worker":
        python_worker(args)
    else:
        driver(args)


if __name__ == "__main__":
    main()
