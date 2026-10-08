#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Small, matched F32 native/Metal versus official PyTorch CPU/MPS comparison.

Each runtime runs in its own process, with synchronized GPU timings. Native
timings include HTTP; PyTorch timings exclude HTTP. This is a representative
parity check, not application-quality or production performance qualification.
"""
import argparse
import base64
import hashlib
import json
import math
import os
import re
import resource
import sys
from pathlib import Path
import signal
import socket
import statistics
import subprocess
import time
import urllib.request

import embeddinggemma2_reference as reference


def publish(path, value):
    temporary = path.with_name(path.name + f".{os.getpid()}.partial")
    with temporary.open("x") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")
    temporary.replace(path)


def timing(samples):
    if not samples or any(not math.isfinite(x) or x <= 0 for x in samples):
        raise ValueError("positive finite timing samples required")
    ordered = sorted(samples)
    return {
        "samples_seconds": samples,
        "count": len(samples),
        "p50_seconds": statistics.median(samples),
        "p95_seconds": ordered[math.ceil(0.95 * len(samples)) - 1],
        "mean_seconds": statistics.mean(samples),
    }


def agreement(actual, expected):
    if len(actual) != len(expected) or not actual:
        raise ValueError("embedding shape differs")
    if any(not math.isfinite(x) for x in actual + expected):
        raise ValueError("nonfinite embedding")
    norm_a = math.sqrt(sum(x*x for x in actual))
    norm_b = math.sqrt(sum(x*x for x in expected))
    if not norm_a or not norm_b:
        raise ValueError("zero embedding")
    return {
        "cosine": sum(x*y for x, y in zip(actual, expected)) / (norm_a*norm_b),
        "max_abs": max(abs(x-y) for x, y in zip(actual, expected)),
        "actual_norm": norm_a,
    }


def assert_agreement(actual, expected, max_error=1e-4):
    result = agreement(actual, expected)
    if result["cosine"] < 0.9999 or result["max_abs"] > max_error or abs(result["actual_norm"] - 1) > 1e-4:
        raise ValueError(f"F32 parity failed: {result}")
    return result


def retrieval_result(suite, cases):
    """Retain scores and full rankings for the tiny matched retrieval corpus."""
    spec = suite.get("retrieval")
    if spec is None or not set(spec["queries"] + spec["documents"]).issubset(cases):
        return None
    scores = {}
    rankings = {}
    for query in spec["queries"]:
        vector = cases[query]["vector"]
        scores[query] = {doc: agreement(vector, cases[doc]["vector"])["cosine"]
                         for doc in spec["documents"]}
        rankings[query] = sorted(scores[query], key=lambda doc: (-scores[query][doc], doc))
    return {"purpose": spec["purpose"], "scores": scores, "rankings": rankings}


def compare_reference(actual, expected, max_error=1e-4):
    if actual.get("measurement_mode", "default") != expected.get("measurement_mode", "default"):
        raise ValueError("reference measurement mode differs")
    for field in ("suite_sha256", "checkpoint_receipt_sha256", "precision"):
        if actual[field] != expected[field]:
            raise ValueError(f"reference {field} differs")
    if expected["status"] != "pass" or set(actual["cases"]) != set(expected["cases"]):
        raise ValueError("reference is incomplete or has different cases")
    vectors = {name: assert_agreement(case["vector"], expected["cases"][name]["vector"], max_error)
               for name, case in actual["cases"].items()}
    if actual.get("decision", {}).get("status") == "not_run":
        if expected.get("decision", {}).get("status") != "not_run" or actual.get("measurement_mode") != expected.get("measurement_mode"):
            raise ValueError("reference evaluation scope differs")
        return {"vectors": vectors, "decision_status": "not_run"}
    decision = actual["decision"].get("answer", actual["decision"])
    oracle = expected["decision"].get("answer", expected["decision"])
    if decision["choice"] != oracle["choice"] or set(decision["similarities"]) != set(oracle["similarities"]):
        raise ValueError("reference routing choice or labels differ")
    score_error = max(abs(value - oracle["similarities"][label]) for label, value in decision["similarities"].items())
    margin_error = abs(decision["margin"] - oracle["margin"])
    if score_error > 1e-4 or margin_error > 1e-4:
        raise ValueError("reference routing scores or margin differ")
    retrieval_error = None
    if actual.get("retrieval") is not None or expected.get("retrieval") is not None:
        if actual.get("retrieval") is None or expected.get("retrieval") is None or actual["retrieval"]["rankings"] != expected["retrieval"]["rankings"]:
            raise ValueError("reference retrieval rankings differ")
        retrieval_error = max(abs(score - expected["retrieval"]["scores"][query][doc])
                              for query, scores in actual["retrieval"]["scores"].items() for doc, score in scores.items())
        if retrieval_error > 1e-4:
            raise ValueError("reference retrieval scores differ")
    return {"vectors": vectors, "choice": decision["choice"], "max_routing_score_error": score_error,
            "routing_margin_error": margin_error, "max_retrieval_score_error": retrieval_error}


def vm_counters():
    raw = subprocess.check_output(["vm_stat"], text=True)
    values = []
    for key in ("Pageouts", "Swapins", "Swapouts"):
        match = re.search(r"^" + key + r":\s+(\d+)", raw, re.MULTILINE)
        if not match:
            raise ValueError("missing VM counter " + key)
        values.append(int(match[1]))
    return values


def conditions():
    return {
        "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "hardware": subprocess.check_output(["sysctl", "hw.model", "hw.memsize", "hw.ncpu", "machdep.cpu.brand_string"], text=True),
        "swap": subprocess.check_output(["sysctl", "vm.swapusage"], text=True),
        "vm_stat": subprocess.check_output(["vm_stat"], text=True),
        "threads": {key: os.environ.get(key) for key in ("VECLIB_MAXIMUM_THREADS", "OMP_NUM_THREADS")},
    }


def prepare(model, artifacts, output):
    from tokenizers import Tokenizer
    tokenizer = Tokenizer.from_file(str(model / "tokenizer.json"))
    cases = []
    texts = [("document_short", "A fox jumps over a log.", "RETRIEVAL_DOCUMENT", "title: none | text: "),
             ("query_short", "What animal jumps?", "RETRIEVAL_QUERY", "task: search result | query: ")]
    for size in (128, 512, 8192):
        prefix = "title: none | text: "
        # The pinned tokenizer maps successive alpha words one token each.
        text = "alpha " * (size - len(tokenizer.encode(prefix).ids))
        for _ in range(4):
            delta = size - len(tokenizer.encode(prefix + text).ids)
            if not delta:
                break
            text = "alpha " * (text.count("alpha") + delta)
        if len(tokenizer.encode(prefix + text).ids) != size:
            raise ValueError("cannot construct exact-length natural text")
        texts.append((f"document_{size}", text, "RETRIEVAL_DOCUMENT", prefix))
    for name, text, task, prefix in texts:
        ids = tokenizer.encode(prefix + text).ids
        cases.append({"name": name, "task": task, "text": text, "token_ids": ids})
    media = json.loads((artifacts / "media-oracle.json").read_text())
    for case in media["cases"]:
        if case["name"] in ("image", "audio", "mixed"):
            cases.append({"name": case["name"], "token_ids": case["token_ids"], "oracle": case["embedding"]})
    documents = [("access", "Reset your password to recover access to your account."),
                 ("billing", "View invoices and request a refund for an unexpected charge."),
                 ("delivery", "Track a shipment and report a missing parcel."),
                 ("returns", "Return an unwanted item within thirty days of delivery.")]
    queries = [("forgot_password", "I forgot my password. How can I log in?"),
               ("unexpected_charge", "Why was I charged twice?"),
               ("missing_parcel", "My package has not arrived.")]
    for role, items, task, prefix in [("document", documents, "RETRIEVAL_DOCUMENT", "title: none | text: "),
                                      ("query", queries, "RETRIEVAL_QUERY", "task: search result | query: ")]:
        for label, text in items:
            cases.append({"name": f"retrieval_{role}_{label}", "task": task, "text": text,
                          "token_ids": tokenizer.encode(prefix + text).ids})
    decision = {"state": "Reset my password", "instructions": "Route the support request.",
                "criteria": {"account": "Account access and forgotten passwords", "billing": "Payments, invoices and unexpected charges", "delivery": "Deliveries, shipping and missing parcels"}}
    decision["rendered"] = ["task: clustering | query: Instruction: " + decision["instructions"] + "\nInput: " + decision["state"]]
    decision["rendered"] += ["task: clustering | query: Instruction: " + decision["instructions"] + "\nCategory: " + value for value in decision["criteria"].values()]
    decision["token_ids"] = [tokenizer.encode(text).ids for text in decision["rendered"]]
    result = {"version": 1, "model": reference.MODEL, "revision": reference.REVISION,
              "recipe": "embeddinggemma2-f32-mean-v1", "cases": cases, "decision": decision,
              "retrieval": {"documents": [f"retrieval_document_{x[0]}" for x in documents],
                            "queries": [f"retrieval_query_{x[0]}" for x in queries],
                            "purpose": "Small ranking parity check; no application-quality qualification."},
              "assets": {name: {"path": str((artifacts/name).resolve()), "sha256": reference.sha256(artifacts/name)} for name in ("image.png", "audio.wav")}}
    publish(output, result)
    print("Prepared", [(c["name"], len(c["token_ids"])) for c in cases], flush=True)


def media_parts(suite, name):
    def part(filename, mime):
        raw = Path(suite["assets"][filename]["path"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != suite["assets"][filename]["sha256"]:
            raise ValueError("media asset changed")
        return {"type": "media", "mime_type": mime, "data": base64.b64encode(raw).decode()}
    image = part("image.png", "image/png")
    audio = part("audio.wav", "audio/wav")
    return {"image": [image], "audio": [audio], "mixed": [{"type": "text", "text": "Describe these inputs."}, audio, image]}[name]


def post(url, endpoint, body):
    request = urllib.request.Request(url + endpoint, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=600) as response:
        return json.loads(response.read(16 * 1024 * 1024))


def iterations(case, count, long_count=1):
    return long_count if len(case["token_ids"]) > 512 else min(count, 3) if "text" not in case else count


def select_cases(suite, names, encoder=False):
    known = {case["name"] for case in suite["cases"]}
    if names and (len(set(names)) != len(names) or not set(names).issubset(known)):
        raise ValueError("duplicate or unknown case selection")
    selected = [case for case in suite["cases"] if not names or case["name"] in names]
    if encoder:
        if names and any("text" not in case for case in selected):
            raise ValueError("encoder benchmark requires prepared text")
        selected = [case for case in selected if "text" in case]
    if not selected:
        raise ValueError("empty case selection")
    return dict(suite, cases=selected)


def case_warmups(case, override):
    return override if override is not None else (0 if len(case["token_ids"]) > 512 else 2)


def encoder_run(args, suite, result):
    raw_path = args.output.with_suffix(".encoder.json")
    command = [str(args.binary.resolve()), "--model", str(args.model_dir.resolve()),
               "--suite", str(args.suite.resolve()), "--output", str(raw_path.resolve()),
               "--backend", args.backend, "--iterations", str(args.iterations),
               "--long-iterations", str(args.long_iterations), "--warmups", str(args.warmups if args.warmups is not None else 2)]
    for case in suite["cases"]:
        command.extend(["--case", case["name"]])
    log_path = args.output.with_suffix(".encoder.log")
    with log_path.open("w") as log:
        subprocess.run(command, check=True, stdout=log, stderr=log, timeout=1800)
    result["child_peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * (1 if sys.platform == "darwin" else 1024)
    raw = json.loads(raw_path.read_text())
    result["vm_counters_available"] = raw.get("vm_counters_available", True)
    if raw["backend"] != args.backend or {case["name"] for case in raw["cases"]} != {case["name"] for case in suite["cases"]}:
        raise ValueError("encoder output selection or backend differs")
    for case in raw["cases"]:
        result["cases"][case["name"]] = dict(case, timing=timing(case.pop("samples_seconds")))
    result.update(binary_sha256=reference.sha256(args.binary), encoder_output_sha256=reference.sha256(raw_path),
                  encoder_log_sha256=reference.sha256(log_path), decision={"status": "not_run", "reason": "text encoder timing mode"})


def native(args, suite, result):
    with socket.socket() as endpoint:
        endpoint.bind(("127.0.0.1", 0))
        port = endpoint.getsockname()[1]
    env = dict(os.environ, ANTFLY_INFERENCE_REQUIRED_BACKEND=args.backend)
    log_path = args.output.with_suffix(".server.log")
    url = f"http://127.0.0.1:{port}"
    with log_path.open("w") as log:
        server = subprocess.Popen([str(args.binary.resolve()), "run", "--models-dir", str(args.model_dir.parent.resolve()), "--port", str(port), "--host-budget-mb", "6144", "--backend-budget-mb", "12288", "--combined-budget-mb", "18432", "--scratch-budget-mb", "8192"], env=env, stdout=log, stderr=log, start_new_session=True)
        try:
            for _ in range(300):
                if server.poll() is not None:
                    raise RuntimeError("owned inference server exited")
                try:
                    with urllib.request.urlopen(url + "/healthz", timeout=1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("owned inference server did not become healthy")
            result["binary_sha256"] = reference.sha256(args.binary)
            identity = None
            for case in suite["cases"]:
                body = {"model": args.model_dir.name, "input": [case["text"]] if "text" in case else [{"content": media_parts(suite, case["name"])}]}
                if "task" in case:
                    body["task_type"] = case["task"]
                if identity:
                    body["model_identity"] = identity
                samples = []
                vector = None
                # Long context reuses the already warmed encoder weights.
                warmups = case_warmups(case, args.warmups)
                for i in range(warmups + iterations(case, args.iterations, args.long_iterations)):
                    started = time.perf_counter()
                    response = post(url + "/ai/v1", "/embed", body)
                    elapsed = time.perf_counter() - started
                    actual_identity = response["model_identity"]
                    if identity and identity != actual_identity:
                        raise ValueError("model generation changed")
                    identity = actual_identity
                    vector = response["data"][0]["embedding"]
                    assert_agreement(vector, case.get("oracle", vector))
                    if i == 0 and not result.get("first_request_seconds"):
                        result["first_request_seconds"] = elapsed
                    if i >= warmups:
                        samples.append(elapsed)
                result["cases"][case["name"]] = {"tokens": len(case["token_ids"]), "timing": timing(samples), "vector": vector}
                print(args.backend, case["name"], timing(samples)["p50_seconds"], flush=True)
                publish(args.output, result)
            result["model_identity"] = identity
            d = suite["decision"]
            body = {"model": args.model_dir.name, "model_identity": identity, "state": d["state"], "questions": {"route": {"type": "choice", "instructions": d["instructions"], "criteria": d["criteria"]}}}
            started = time.perf_counter()
            post(url + "/ai/v1", "/decide", body)
            cold = time.perf_counter() - started
            samples = []
            for _ in range(args.iterations):
                started = time.perf_counter()
                response = post(url + "/ai/v1", "/decide", body)
                samples.append(time.perf_counter() - started)
            result["decision"] = {"prototype_cold_seconds": cold, "timing": timing(samples), "answer": response["answers"]["route"]}
            # Repeated warm requests check retained memory without huge datasets.
            rss = []
            for i in range(64):
                post(url + "/ai/v1", "/decide", body)
                if i % 16 == 0 or i == 63:
                    rows = subprocess.check_output(["ps", "-axo", "pid,ppid,rss"], text=True).splitlines()[1:]
                    owned = [(int(x.split()[0]), int(x.split()[2]) * 1024) for x in rows if len(x.split()) == 3 and (int(x.split()[0]) == server.pid or int(x.split()[1]) == server.pid)]
                    rss.append({"request": i+1, "process_rss_bytes": owned})
            result["soak"] = {"requests": 64, "rss": rss}
        finally:
            if server.poll() is None:
                os.killpg(server.pid, signal.SIGTERM)
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(server.pid, signal.SIGKILL)
                server.wait(timeout=10)
    text = log_path.read_text()
    if f"selected backend {args.backend} " not in text:
        raise ValueError("required backend was not confirmed by server log")
    result["server_log_sha256"] = reference.sha256(log_path)


def torch_run(args, suite, result):
    import numpy as np
    import torch
    import wave
    from PIL import Image
    from transformers import AutoModel, AutoProcessor, AutoTokenizer
    reference.verify_reference(args.model_dir)
    torch.set_num_threads(4)
    torch.set_num_interop_threads(1)
    if args.backend == "mps" and not torch.backends.mps.is_available():
        raise RuntimeError("PyTorch MPS unavailable")
    sync = torch.mps.synchronize if args.backend == "mps" else lambda: None
    started = time.perf_counter()
    model = AutoModel.from_pretrained(args.model_dir, dtype=torch.float32, local_files_only=True).eval().to(args.backend)
    sync()
    result["model_load_seconds"] = time.perf_counter() - started
    tokenizer = AutoTokenizer.from_pretrained(args.model_dir, local_files_only=True)
    processor = AutoProcessor.from_pretrained(args.model_dir, local_files_only=True)
    image = Image.open(suite["assets"]["image.png"]["path"]).convert("RGB")
    with wave.open(suite["assets"]["audio.wav"]["path"]) as audio:
        waveform = np.frombuffer(audio.readframes(audio.getnframes()), dtype="<i2").astype(np.float32) / 32768
    def inputs(case):
        if args.timing_mode == "encoder":
            ids = case["token_ids"]
            mask = case.get("attention_mask", [1] * len(ids))
            if len(mask) != len(ids) or not 0 < len(ids) <= 8192 or not any(mask):
                raise ValueError("invalid prepared IDs/mask")
            return {"input_ids": torch.tensor([ids], dtype=torch.long, device=args.backend),
                    "attention_mask": torch.tensor([mask], dtype=torch.long, device=args.backend)}
        if "text" in case:
            prefix = "task: search result | query: " if case["task"] == "RETRIEVAL_QUERY" else "title: none | text: "
            encoded = tokenizer(prefix + case["text"], return_tensors="pt")
        elif case["name"] == "image":
            encoded = processor(text=["<|image|>"], images=[[image]], return_tensors="pt")
        elif case["name"] == "audio":
            encoded = processor(text=["<|audio|>"], audio=[waveform], sampling_rate=16000, return_tensors="pt")
        else:
            encoded = processor(text=["title: none | text: Describe these inputs.\n<|audio|>\n<|image|>"], images=[[image]], audio=[waveform], sampling_rate=16000, return_tensors="pt")
        if encoded["input_ids"][0].tolist() != case["token_ids"]:
            raise ValueError("token IDs differ from matched suite")
        return {k: v.to(args.backend) for k, v in encoded.items()}
    def forward(encoded, dimensions=768):
        hidden = model(**encoded).last_hidden_state
        mask = encoded["attention_mask"].unsqueeze(-1)
        vector = torch.nn.functional.normalize(((hidden * mask).sum(1) / mask.sum(1))[:, :dimensions], dim=-1)
        return vector[0].cpu().tolist()
    with torch.inference_mode():
        for case in suite["cases"]:
            samples = []
            full_samples = []
            warmups = case_warmups(case, args.warmups)
            prepared = inputs(case) if args.timing_mode == "encoder" else None
            if prepared is not None:
                sync()
            vm_before = None
            for i in range(warmups + iterations(case, args.iterations, args.long_iterations)):
                sync()
                if args.timing_mode == "encoder" and i == warmups:
                    vm_before = vm_counters()
                full_started = time.perf_counter()
                encoded = prepared if prepared is not None else inputs(case)
                sync()
                started = time.perf_counter()
                vector = forward(encoded, case.get("dimensions", 768))
                sync()
                elapsed = time.perf_counter() - started
                if i >= warmups:
                    samples.append(elapsed)
                    full_samples.append(time.perf_counter() - full_started)
            assert_agreement(vector, case.get("oracle", vector))
            result["cases"][case["name"]] = {"tokens": len(case["token_ids"]), "timing": timing(samples), "preprocessing_and_inference_timing": timing(full_samples), "vector": vector}
            if args.timing_mode == "encoder":
                result["cases"][case["name"]].update(vm_measured_before=vm_before, vm_measured_after=vm_counters())
            print(args.backend, case["name"], timing(samples)["p50_seconds"], flush=True)
            publish(args.output, result)
        if args.timing_mode == "encoder":
            result["decision"] = {"status": "not_run", "reason": "text encoder timing mode"}
            result["attention_implementation"] = model.config.text_config._attn_implementation
            result["torch"] = torch.__version__
            result["torch_threads"] = torch.get_num_threads()
            result["torch_interop_threads"] = torch.get_num_interop_threads()
            result["peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)
            if args.backend == "mps":
                result["mps_allocated_bytes"] = torch.mps.current_allocated_memory()
                result["mps_driver_bytes"] = torch.mps.driver_allocated_memory()
            return
        vectors = []
        for text, expected in zip(suite["decision"]["rendered"], suite["decision"]["token_ids"]):
            encoded = tokenizer(text, return_tensors="pt")
            if encoded["input_ids"][0].tolist() != expected:
                raise ValueError("decision token IDs differ")
            vectors.append(forward({k: v.to(args.backend) for k, v in encoded.items()}))
        scores = {label: sum(x*y for x, y in zip(vectors[0], vector)) for label, vector in zip(suite["decision"]["criteria"], vectors[1:])}
        ranked = sorted(scores, key=scores.get, reverse=True)
        result["decision"] = {"similarities": scores, "choice": ranked[0], "margin": scores[ranked[0]]-scores[ranked[1]]}
        samples = []
        for _ in range(args.iterations):
            sync()
            started = time.perf_counter()
            encoded = tokenizer(suite["decision"]["rendered"][0], return_tensors="pt")
            state = forward({k: v.to(args.backend) for k, v in encoded.items()})
            current = [sum(x*y for x, y in zip(state, vector)) for vector in vectors[1:]]
            if any(abs(x-y) > 1e-5 for x, y in zip(current, scores.values())):
                raise ValueError("cached decision scores changed")
            sync()
            samples.append(time.perf_counter() - started)
        result["decision"]["timing"] = timing(samples)
    result["torch"] = torch.__version__
    result["torch_threads"] = torch.get_num_threads()
    result["attention_implementation"] = model.config.text_config._attn_implementation
    if args.backend == "mps":
        result["mps_allocated_bytes"] = torch.mps.current_allocated_memory()
        result["mps_driver_bytes"] = torch.mps.driver_allocated_memory()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["prepare", "native", "metal", "cpu", "mps"], required=True)
    parser.add_argument("--model-dir", type=Path, required=True)
    parser.add_argument("--artifacts-dir", type=Path)
    parser.add_argument("--suite", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--reference", type=Path, help="Gate vectors, rankings and routing against a completed matched runtime result")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--case", action="append", help="Select named cases; may be repeated")
    parser.add_argument("--warmups", type=int, help="Override case-specific warmups, including long inputs")
    parser.add_argument("--long-iterations", type=int, default=1)
    parser.add_argument("--timing-mode", choices=["default", "encoder"], default="default")
    parser.add_argument("--max-vector-error", type=float, default=1e-4)
    args = parser.parse_args()
    if not 1 <= args.iterations <= 100 or not 1 <= args.long_iterations <= 100 or (args.warmups is not None and not 0 <= args.warmups <= 100) or not 0 < args.max_vector_error <= 1e-4:
        parser.error("iterations must be 1..100")
    if args.backend == "prepare":
        if not args.artifacts_dir:
            parser.error("prepare requires --artifacts-dir")
        prepare(args.model_dir, args.artifacts_dir, args.output)
        return
    if not args.suite or (args.backend in ("native", "metal") and not args.binary):
        parser.error("runtime requires --suite and native/metal requires --binary")
    suite = json.loads(args.suite.read_text())
    if suite["revision"] != reference.REVISION:
        raise ValueError("suite checkpoint revision differs")
    suite = select_cases(suite, args.case, args.timing_mode == "encoder")
    if args.timing_mode == "encoder" and args.warmups is None:
        args.warmups = 2
    result = {"version": 1, "status": "running", "measurement_mode": args.timing_mode,
              "case_selection": [case["name"] for case in suite["cases"]],
              "warmups": args.warmups, "long_iterations": args.long_iterations,
              "max_vector_error": args.max_vector_error,
              "implementation_controls": {key: value for key, value in os.environ.items() if key.startswith(("ANTFLY_EMBEDDINGGEMMA2_", "TERMITE_METAL_EMBEDDINGGEMMA2_"))},
              "vm_counter_order": ["pageouts", "swapins", "swapouts"], "backend": args.backend,
              "tool_sha256": reference.sha256(Path(__file__)),
              "reference_tool_sha256": reference.sha256(Path(reference.__file__)),
              "checkpoint_receipt_sha256": reference.sha256(args.model_dir / "embeddinggemma2_receipt.json"),
              "production_qualified": False, "precision": "float32", "suite_sha256": reference.sha256(args.suite),
              "timing_scope": "prepared IDs/mask: embedding lookup, encoder, pooling, normalization and synchronized vector readback" if args.timing_mode == "encoder" else "HTTP request including serialization/tokenization and output transfer" if args.backend in ("native", "metal") else "encoder, pooling and output transfer; excludes input preprocessing and HTTP",
              "cases": {}, "before": conditions()}
    try:
        if args.backend in ("native", "metal"):
            (encoder_run if args.timing_mode == "encoder" else native)(args, suite, result)
        else:
            torch_run(args, suite, result)
        result["retrieval"] = retrieval_result(suite, result["cases"])
        if args.reference:
            result["parity"] = compare_reference(result, json.loads(args.reference.read_text()), args.max_vector_error)
            result["reference_result_sha256"] = reference.sha256(args.reference)
        result["status"] = "pass"
    finally:
        result["after"] = conditions()
        if result["status"] != "pass":
            result["status"] = "failed"
        publish(args.output, result)


if __name__ == "__main__":
    main()
