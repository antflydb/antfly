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
NATIVE_ENV_PREFIXES = ("ANTFLY_INFERENCE_", "ANTFLY_GEMMA4_")


def native_profile(overrides, inherited=None):
    if not isinstance(overrides, dict) or any(
        not isinstance(key, str)
        or not key.startswith(NATIVE_ENV_PREFIXES)
        or not isinstance(value, str)
        for key, value in overrides.items()
    ):
        raise ValueError("Native profile must map inference/Gemma4 keys to strings")
    return {**(inherited or {}), **REFERENCE_ENV, **overrides}


def native_child_environment(ambient, profile):
    return {
        **{k: v for k, v in ambient.items() if not k.startswith(NATIVE_ENV_PREFIXES)},
        **profile,
    }


def native_campaign_suite(suite, baseline=False):
    return {
        key: value
        for key, value in suite.items()
        if not baseline or key not in ("draft_model", "speculative_k")
    }


def check_native_speculation(rows):
    if not rows:
        raise ValueError("Missing measured speculative rows")
    for row in rows:
        stats = row.get("speculative")
        if (
            not isinstance(stats, dict)
            or stats.get("mtp_enabled") is not True
            or any(
                type(stats.get(name)) is not int or stats[name] <= 0
                for name in ("rounds", "drafted_tokens", "matched_draft_tokens")
            )
        ):
            raise ValueError("Inactive measured speculation: " + row["id"])


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


def check_decisions(rows, references):
    """Reuse official CPU decisions only while their complete prefixes match."""
    actual_ids = {row["id"] for row in rows}
    if actual_ids != set(references):
        raise ValueError("Decision reference coverage differs")
    for row in rows:
        expected = references[row["id"]]
        if len(row["tokens"]) < len(expected):
            raise ValueError("Incomplete decision prefix: " + row["id"])
        for step, (actual, reference) in enumerate(zip(row["tokens"], expected)):
            if actual != reference:
                raise ValueError(
                    f"Transformer decision differs: {row['id']}/{step}: {actual} != {reference}"
                )


def check_cpu_pins(qualification, models, identities, vectors):
    truth = qualification["source_truth"]
    expected_models = qualification["native_reference_profile"][
        "validation_provenance"
    ]["models"]
    if models != expected_models:
        raise ValueError("CPU reference model source differs")
    if any(identity != truth["identity"] for identity in identities):
        raise ValueError("CPU forward identity differs")
    if any(truth["vectors"].get(name) != value for name, value in vectors.items()):
        raise ValueError("CPU reference vector identity differs")


def check_native_activity(
    report, required, media_required=(), cases=(), kind_required=()
):
    by_kind = {}
    for requirement in kind_required:
        kind, separator, name = requirement.partition("=")
        if not separator or kind not in {"text", "image", "audio", "video"} or not name:
            raise ValueError("Expected kind=counter native activity requirement")
        if not any(case.get("kind", "text") == kind for case in cases):
            raise ValueError("Required native activity kind has no cases: " + kind)
        by_kind.setdefault(kind, set()).add(name)
    for name in (
        set(required)
        | set(media_required)
        | {n for names in by_kind.values() for n in names}
    ):
        count = report.get("cuda", {}).get(name)
        if type(count) is not int or count <= 0:
            raise ValueError("Inactive native optimization: " + name)
    kinds = {case["id"]: case.get("kind", "text") for case in cases}
    for row in report.get("rows", []):
        kind = kinds.get(row["id"])
        names = set(media_required) if kind and kind != "text" else set()
        for name in names | by_kind.get(kind, set()):
            count = row.get(name)
            if type(count) is not int or count <= 0:
                raise ValueError(
                    "Inactive measured media optimization: " + row["id"] + "/" + name
                )


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
    p.add_argument("--native-env-file", type=Path)
    p.add_argument("--native-baseline", type=Path)
    p.add_argument(
        "--baseline-env-file",
        type=Path,
        help="Explicit non-speculative baseline profile; otherwise use reference settings",
    )
    p.add_argument(
        "--require-native-speculation",
        action="store_true",
        help="Require active MTP with accepted drafts on every measured native request",
    )
    p.add_argument("--native-decisions-reference", type=Path)
    p.add_argument("--cpu-qualification", type=Path)
    p.add_argument("--require-native-stat", action="append", default=[])
    p.add_argument("--require-native-media-stat", action="append", default=[])
    p.add_argument(
        "--require-native-kind-stat",
        action="append",
        default=[],
        metavar="KIND=COUNTER",
    )
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--iterations", type=int, default=2)
    a = p.parse_args()
    if not 1 <= a.rounds <= 10 or not 1 <= a.iterations <= 20:
        p.error("Invalid campaign bounds")
    if bool(a.llama_loader) != bool(a.llama_runtime):
        p.error("Specify both compatible loader and runtime")
    inherited = {
        key: value
        for key, value in os.environ.items()
        if key.startswith(NATIVE_ENV_PREFIXES)
    }
    if a.baseline_env_file and not a.native_baseline:
        p.error("--baseline-env-file requires --native-baseline")
    try:
        native_environment = native_profile(
            json.loads(a.native_env_file.read_text()) if a.native_env_file else {},
            inherited,
        )
        baseline_environment = native_profile(
            json.loads(a.baseline_env_file.read_text()) if a.baseline_env_file else {}
        )
    except ValueError as error:
        p.error(str(error))
    decision_references = None
    if a.native_decisions_reference:
        reference = json.loads(a.native_decisions_reference.read_text())
        positions = reference["all_native_decisions"]["rows"]
        decision_references = {row["id"]: row["argmax"] for row in positions}
        if len(decision_references) != len(positions) or any(
            not values
            or any(
                type(value) is not int or not 0 <= value < 262144 for value in values
            )
            for values in decision_references.values()
        ):
            p.error("Invalid independent decision reference")
    a.out.mkdir(parents=True, exist_ok=False)
    suite = json.loads(a.suite.read_text())
    draft_model = suite.get("draft_model")
    if a.require_native_speculation and not draft_model:
        p.error("--require-native-speculation requires suite draft_model")
    draft_identity = None
    if draft_model:
        draft_path = Path(draft_model)
        if not draft_path.is_absolute():
            draft_path = a.suite.parent / draft_path
        suite["draft_model"] = str(draft_path.resolve())
        draft_identity = {"path": suite["draft_model"], "sha256": digest(draft_path)}
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
    reference_hashes = {}
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
            reference_hashes[path.stem] = digest(path)
    if not set(selected_ids) <= set(refs):
        raise ValueError("Missing CPU references")
    if a.cpu_qualification:
        check_cpu_pins(
            json.loads(a.cpu_qualification.read_text()),
            {
                key: manifest["preparation"][key + "_sha256"]
                for key in ("gguf", "mmproj")
            },
            identities,
            {name: reference_hashes[name] for name in selected_ids},
        )
    orders = [
        ("native", "pjrt", "llama"),
        ("llama", "pjrt", "native"),
        ("pjrt", "native", "llama"),
    ]
    if a.native_baseline:
        orders = [
            ("native", "pjrt", "baseline", "llama"),
            ("pjrt", "llama", "native", "baseline"),
            ("llama", "baseline", "pjrt", "native"),
            ("baseline", "native", "llama", "pjrt"),
        ]
    runtime_directories = [
        Path(directory)
        for directory in os.environ.get("LD_LIBRARY_PATH", "").split(os.pathsep)
        if directory
    ]
    if a.llama_runtime:
        runtime_directories.append(a.llama_runtime)
    runtime_files = {
        path.resolve()
        for directory in runtime_directories
        for pattern in (
            "ld-linux*.so*",
            "libc.so*",
            "libm.so*",
            "libstdc++.so*",
            "libgcc_s.so*",
            "libgomp.so*",
            "libcublas.so*",
            "libcublasLt.so*",
            "libcudart.so*",
        )
        for path in directory.glob(pattern)
        if path.is_file()
    }
    receipt = {
        "gpu": gpu(),
        "rounds": a.rounds,
        "iterations": a.iterations,
        "warmups": 1,
        "common_case_ids": selected_ids,
        "orders": orders,
        "native_environment": native_environment,
        "harness_sha256": {
            name: digest(Path(__file__).with_name(name))
            for name in (
                "compare_gemma4.py",
                "benchmark_gemma4.py",
                "gemma4_inputs.py",
                "validate_gemma4.py",
                "memory.py",
            )
        },
        "native_draft_model": draft_identity,
        "required_native_speculation": a.require_native_speculation,
        "cpu_vector_sha256": {name: reference_hashes[name] for name in selected_ids},
        "cpu_qualification_sha256": digest(a.cpu_qualification)
        if a.cpu_qualification
        else None,
        "native_baseline": (
            {
                "sha256": digest(a.native_baseline),
                "environment": baseline_environment,
                "draft_model": None,
            }
            if a.native_baseline
            else None
        ),
        "ambient_runtime_environment": {
            key: value
            for key, value in sorted(os.environ.items())
            if key.startswith("ANTFLY_CUDA_ENABLE_")
            or key
            in (
                "CUDA_VISIBLE_DEVICES",
                "LD_LIBRARY_PATH",
                "LD_PRELOAD",
                "NVIDIA_TF32_OVERRIDE",
                "ANTFLY_CUDA_QMATMUL_VARIANT",
                "TERMITE_GEMMA4_AUDIO_HOST_OPS",
                "TERMITE_DISABLE_PLE",
                "TERMITE_DISABLE_SHARED_KV",
                "TERMITE_DISABLE_SLIDING_ATTENTION",
                "TERMITE_DENSE_DECODE_EVAL_STRIDE",
                "TERMITE_GEMMA4_DISABLE_LAYER_OUTPUT_SCALE",
                "TERMITE_GEMMA4_DISABLE_RMSNORM_ADD_FUSION",
            )
        },
        "required_native_stats": a.require_native_stat,
        "required_native_media_stats": a.require_native_media_stat,
        "required_native_kind_stats": a.require_native_kind_stat,
        "native_decisions_reference_sha256": (
            digest(a.native_decisions_reference)
            if a.native_decisions_reference
            else None
        ),
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
        "configured_runtime_libraries_sha256": {
            str(path): digest(path) for path in sorted(runtime_files)
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

    def native(directory, validation=False, baseline=False):
        directory.mkdir()
        config = native_suite(
            native_campaign_suite(suite, baseline),
            a.suite.parent,
            directory,
            1 if validation else a.iterations,
            validation,
        )
        (directory / "suite.json").write_text(json.dumps(config))
        env = native_child_environment(
            os.environ, baseline_environment if baseline else native_environment
        )
        child(
            [
                str(x.resolve())
                for x in (
                    a.native_baseline if baseline else a.native,
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
        report = json.loads((directory / "result.json").read_text())
        if not baseline:
            check_native_activity(
                report,
                a.require_native_stat,
                a.require_native_media_stat,
                cases,
                a.require_native_kind_stat,
            )
        return report["rows"]

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
    if a.native_baseline:
        native(a.out / "validation-baseline", True, baseline=True)
    llama_oracle = llama(a.out / "validation-llama", True)
    pjrt(a.out / "validation-pjrt", True)
    checks = {}
    engines = ("native", "llama", "pjrt") + (("baseline",) if a.native_baseline else ())
    for engine in engines:
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
        x["logit_tolerance_pass"] and x["argmax_matches"]
        for engine in ("native", "baseline")
        if engine in checks
        for x in checks[engine]
    ):
        raise ValueError("Native accuracy gate failed before timing")
    all_rows = {name: [] for name in engines}
    runners = {"native": native, "pjrt": pjrt, "llama": llama}
    if a.native_baseline:
        runners["baseline"] = lambda directory: native(directory, baseline=True)
    for index in range(a.rounds):
        for engine in orders[index % len(orders)]:
            directory = a.out / f"r{index}-{engine}"
            rows = runners[engine](directory)
            check_rows(rows, cases, a.iterations)
            if engine == "native" and a.require_native_speculation:
                check_native_speculation(rows)
            if engine in ("native", "baseline") and decision_references is not None:
                check_decisions(rows, decision_references)
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
                    for engine in engines
                    if engine != "native"
                },
                "native_latency_win": {
                    engine: medians["native"] <= 0.95 * medians[engine]
                    and tails["native"] <= tails[engine]
                    for engine in engines
                    if engine != "native"
                },
            }
        )
    (a.out / "summary.json").write_text(
        json.dumps(
            {
                "measurement_valid": True,
                "acceptance": receipt,
                "accuracy": checks,
                "native_decisions": (
                    {
                        "scope": "official Transformer prefixes through first EOS or fixed budget; every measured native row checked",
                        "unique_positions": sum(map(len, decision_references.values())),
                        "passed": True,
                    }
                    if decision_references is not None
                    else None
                ),
                "results": summary,
            },
            indent=2,
        )
        + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
