# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Fresh fixed-budget native/PJRT/llama.cpp comparison on common fixtures."""

import argparse
import json
import os
import statistics
import subprocess
from pathlib import Path
from types import SimpleNamespace

import numpy as np
from benchmark_gemma4 import Pjrt, digest, gpu, idle, native_suite, validate_suite
from gemma4_inputs import prepare, processor
from memory import MemorySampler
from validate_gemma4 import compare

REFERENCE_ENV = {
    "ANTFLY_INFERENCE_CUDA_GEMMA4_IMAGE_BICUBIC": "1",
    "ANTFLY_INFERENCE_CUDA_GEMMA4_VIDEO_SWSCALE_RGB": "1",
    "ANTFLY_INFERENCE_CUDA_GEMMA4_PROJECTOR_COMPACT": "0",
    "ANTFLY_INFERENCE_CUDA_GEMMA4_VISION_F16": "0",
    "ANTFLY_INFERENCE_CUDA_Q4_0_PLE_GATE_FUSION": "0",
    "ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1": "0",
}


def check_rows(rows, cases, repeats):
    if len(rows) != len(cases) * repeats:
        raise ValueError("Incomplete measurement coverage")
    for case in cases:
        selected = [r for r in rows if r["id"] == case["id"]]
        if len(selected) != repeats or len({r["repeat"] for r in selected}) != repeats:
            raise ValueError("Incomplete case coverage")
        for row in selected:
            if (
                row["prompt_tokens"] != case["expected_prompt_tokens"]
                or row["completion_tokens"] != case["max_tokens"]
                or len(row["tokens"]) != case["max_tokens"]
                or not np.isfinite(row["ms"])
                or row["ms"] <= 0
                or any(
                    type(token) is not int or not 0 <= token < 262144
                    for token in row["tokens"]
                )
            ):
                raise ValueError("Invalid fixed-budget measurement")
        if len({tuple(r["tokens"]) for r in selected}) != 1:
            raise ValueError("Nondeterministic greedy measurements")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in (
        "native",
        "runner",
        "plugin",
        "gguf",
        "mmproj",
        "tokenizer",
        "template",
        "suite",
        "manifest",
        "cache",
        "processor",
        "llama-runner",
        "llama-library",
        "llama-inputs",
        "out",
    ):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--llama-loader", type=Path)
    p.add_argument("--llama-runtime", type=Path)
    p.add_argument("--cpu", type=Path, action="append", required=True)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--iterations", type=int, default=2)
    a = p.parse_args()
    if not 1 <= a.rounds <= 10 or not 1 <= a.iterations <= 20:
        p.error("Invalid campaign bounds")
    if bool(a.llama_loader) != bool(a.llama_runtime):
        p.error("Specify both compatible loader and runtime")
    a.out.mkdir(parents=True, exist_ok=False)
    suite = json.loads(a.suite.read_text())
    manifest = json.loads(a.manifest.read_text())
    validate_suite(suite, manifest)
    for key in ("gguf", "mmproj"):
        if digest(getattr(a, key)) != manifest["preparation"][key + "_sha256"]:
            raise ValueError("Model source differs")
    for name, info in suite["files"].items():
        path = a.suite.parent / name
        if path.stat().st_size != info["bytes"] or digest(path) != info["sha256"]:
            raise ValueError("Fixture identity differs")
    selected_ids = []
    for line in (a.llama_inputs / "cases.tsv").read_text().splitlines():
        ident, kind, path, count, budget = line.split()
        if ident in selected_ids:
            raise ValueError("Duplicate llama case")
        case = next(c for c in suite["cases"] if c["id"] == ident)
        if (
            kind != case["kind"]
            or int(count) != case["expected_prompt_tokens"]
            or int(budget) != case["max_tokens"]
            or (
                kind != "text"
                and Path(path).resolve() != (a.suite.parent / case["path"]).resolve()
            )
        ):
            raise ValueError("llama/native workload differs")
        selected_ids.append(ident)
    cases = [case for case in suite["cases"] if case["id"] in selected_ids]
    if not cases:
        raise ValueError("No common cases")
    suite = {**suite, "cases": cases}
    refs = {}
    identities = []
    for directory in a.cpu:
        identity = json.loads((directory / "reference.json").read_text())
        if (
            identity["forward_module"] != "transformers.models.gemma4.modeling_gemma4"
            or identity.get("logit_position", "first_step") != "first_step"
        ):
            raise ValueError("Expected official first-step references")
        identities.append(identity)
        for path in directory.glob("*.f32"):
            if path.stem in refs:
                raise ValueError("Duplicate CPU reference")
            refs[path.stem] = np.fromfile(path, dtype="<f4")
    if not set(selected_ids) <= set(refs):
        raise ValueError("Missing CPU references")
    orders = [
        ("native", "pjrt", "llama"),
        ("llama", "pjrt", "native"),
        ("pjrt", "native", "llama"),
    ]
    receipt = {
        "gpu": gpu(),
        "rounds": a.rounds,
        "iterations": a.iterations,
        "warmups": 1,
        "common_case_ids": selected_ids,
        "orders": orders,
        "native_environment": REFERENCE_ENV,
        "files": {
            name: digest(getattr(a, name))
            for name in (
                "native",
                "runner",
                "plugin",
                "gguf",
                "mmproj",
                "tokenizer",
                "template",
                "suite",
                "manifest",
                "llama_runner",
            )
        },
        "llama_libraries": {
            str(path.name): digest(path) for path in a.llama_library.glob("*.so")
        },
        "llama_inputs": {
            str(path.name): digest(path)
            for path in a.llama_inputs.iterdir()
            if path.is_file()
        },
        "reference_identities": identities,
        "scope_difference": "llama starts with a pre-rendered prompt and excludes output text decoding; native includes both; no HTTP or cold load/compile in either timing",
        "winner_guardrail": "native median <=0.95 * competitor median and native p95 <= competitor p95; accuracy reported separately",
    }
    (a.out / "acceptance.json").write_text(json.dumps(receipt, indent=2) + "\n")
    proc = processor(a.processor)
    model_config = json.loads((a.processor / "config.json").read_text())
    modal_ids = [model_config[k + "_token_id"] for k in ("image", "audio", "video")]
    expected_prompts = {}
    for case in cases:
        values, _ = prepare(proc, case, a.suite.parent)
        ids = values["input_ids"][0].numpy().copy()
        ids[np.isin(ids, modal_ids)] = -1
        expected_prompts[case["id"]] = ids.tolist()

    def child(command, environment, directory):
        idle()
        with (directory / "worker.log").open("wb") as log:
            process = subprocess.Popen(command, stdout=log, stderr=log, env=environment)
            sampler = MemorySampler(
                SimpleNamespace(pid=os.getpid()), directory / "memory.jsonl"
            )
            sampler.thread.start()
            try:
                process.wait(timeout=3600)
                if process.returncode:
                    raise RuntimeError("Worker failed: " + str(directory))
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                memory = sampler.stop()
                (directory / "memory.json").write_text(
                    json.dumps(memory, indent=2) + "\n"
                )
        if not memory["valid"]:
            raise ValueError("Invalid memory sampling")

    def native(directory, validation=False):
        directory.mkdir()
        config = native_suite(
            suite,
            a.suite.parent,
            directory,
            1 if validation else a.iterations,
            validation,
        )
        (directory / "suite.json").write_text(json.dumps(config))
        env = {**os.environ, **REFERENCE_ENV}
        child(
            [
                str(x.resolve())
                for x in (
                    a.native,
                    a.gguf,
                    a.tokenizer,
                    a.template,
                    a.mmproj,
                    directory / "suite.json",
                    directory / "result.json",
                )
            ],
            env,
            directory,
        )
        return json.loads((directory / "result.json").read_text())["rows"]

    def llama(directory, validation=False):
        directory.mkdir()
        env = {**os.environ, "GGML_BACKEND_PATH": str(a.llama_library.resolve())}
        command = [
            str(a.llama_runner.resolve()),
            str(a.gguf.resolve()),
            str(a.mmproj.resolve()),
            str(a.llama_inputs.resolve()),
            str((directory / "run").resolve()),
        ]
        if a.llama_loader:
            libs = [
                str(a.llama_runtime.resolve()),
                str(a.llama_library.resolve()),
                os.environ.get("LD_LIBRARY_PATH", ""),
                "/usr/lib/x86_64-linux-gnu",
            ]
            command = [
                str(a.llama_loader.resolve()),
                "--library-path",
                os.pathsep.join(libs),
            ] + command
        if not validation:
            command += ["--measure", "1", str(a.iterations)]
        child(command, env, directory)
        result = json.loads((directory / "run/result.json").read_text())
        if result["measurement"] != (not validation):
            raise ValueError("Wrong llama measurement mode")
        return result["rows"]

    def pjrt(directory, validation=False):
        idle()
        directory.mkdir()
        session = Pjrt(a, manifest, directory)
        rows = []
        try:
            for index, case in enumerate(manifest["cases"]):
                if case["id"] not in selected_ids:
                    continue
                for rep in range(1 if validation else 1 + a.iterations):
                    row, values = session.call(index, proc, a.suite.parent, validation)
                    if validation:
                        values.tofile(directory / (case["id"] + ".f32"))
                    elif rep:
                        row.update(
                            tokens=values.tolist(),
                            repeat=rep - 1,
                            prompt_tokens=case["expected_prompt_tokens"],
                            completion_tokens=len(values),
                        )
                        rows.append(row)
        finally:
            session.close()
        (directory / "result.json").write_text(json.dumps({"rows": rows}))
        if not json.loads((directory / "pjrt.memory.json").read_text())["valid"]:
            raise ValueError("Invalid PJRT memory sampling")
        return rows

    native(a.out / "validation-native", True)
    llama_oracle = llama(a.out / "validation-llama", True)
    pjrt(a.out / "validation-pjrt", True)
    checks = {}
    for engine in ("native", "llama", "pjrt"):
        base = a.out / ("validation-" + engine)
        if engine == "llama":
            base /= "run"
        checks[engine] = [
            {
                "id": c["id"],
                **compare(
                    np.fromfile(base / (c["id"] + ".f32"), dtype="<f4"), refs[c["id"]]
                ),
            }
            for c in cases
        ]
    for case in cases:
        actual = [
            int(v)
            for v in (a.out / "validation-llama/run" / (case["id"] + ".prompt-tokens"))
            .read_text()
            .splitlines()
        ]
        if actual != expected_prompts[case["id"]]:
            raise ValueError("llama/reference prepared tokens differ: " + case["id"])
    (a.out / "validation.json").write_text(json.dumps(checks, indent=2) + "\n")
    if not all(
        x["logit_tolerance_pass"] and x["argmax_matches"] for x in checks["native"]
    ):
        raise ValueError("Native accuracy gate failed before timing")
    all_rows = {name: [] for name in ("native", "pjrt", "llama")}
    for index in range(a.rounds):
        for engine in orders[index % len(orders)]:
            directory = a.out / f"r{index}-{engine}"
            rows = {"native": native, "pjrt": pjrt, "llama": llama}[engine](directory)
            check_rows(rows, cases, a.iterations)
            if engine == "llama":
                canonical = {r["id"]: r["tokens"] for r in llama_oracle}
                if any(r["tokens"] != canonical[r["id"]] for r in rows):
                    raise ValueError("Backend sampling differs from full-logit oracle")
            all_rows[engine].extend(rows)
            print(index, engine, "complete", flush=True)
    summary = []
    for case in cases:
        ident = case["id"]
        for engine, rows in all_rows.items():
            if len({tuple(r["tokens"]) for r in rows if r["id"] == ident}) != 1:
                raise ValueError(
                    "Cross-round greedy sequence changed: " + engine + "/" + ident
                )
        samples = {
            engine: [r["ms"] for r in rows if r["id"] == ident]
            for engine, rows in all_rows.items()
        }
        medians = {
            engine: statistics.median(values) for engine, values in samples.items()
        }
        tails = {
            engine: float(np.percentile(values, 95))
            for engine, values in samples.items()
        }
        summary.append(
            {
                "id": ident,
                "samples_ms": samples,
                "median_ms": medians,
                "p95_ms": tails,
                "native_speedup": {
                    engine: medians[engine] / medians["native"]
                    for engine in ("pjrt", "llama")
                },
                "native_latency_win": {
                    engine: medians["native"] <= 0.95 * medians[engine]
                    and tails["native"] <= tails[engine]
                    for engine in ("pjrt", "llama")
                },
            }
        )
    (a.out / "summary.json").write_text(
        json.dumps(
            {
                "measurement_valid": True,
                "acceptance": receipt,
                "accuracy": checks,
                "results": summary,
            },
            indent=2,
        )
        + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
