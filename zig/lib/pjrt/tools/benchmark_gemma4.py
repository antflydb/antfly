# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Isolated CUDA generation through native Zig and native Zig/PJRT workers."""

import argparse
import copy
import hashlib
import importlib.metadata
import json
import os
import selectors
import statistics
import struct
import subprocess
import time
from pathlib import Path
from types import SimpleNamespace

import numpy as np
from gemma4_inputs import prepare, processor
from gemma4_stream import parameters, write_all
from memory import MemorySampler


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for data in iter(lambda: stream.read(1 << 24), b""):
            h.update(data)
    return h.hexdigest()


def exact(stream, n, timeout=1800):
    result = bytearray()
    deadline = time.monotonic() + timeout
    with selectors.DefaultSelector() as selector:
        selector.register(stream, selectors.EVENT_READ)
        while len(result) < n:
            if not selector.select(max(0, deadline - time.monotonic())):
                raise TimeoutError("Native worker response")
            data = os.read(stream.fileno(), n - len(result))
            if not data:
                raise RuntimeError(
                    "Worker closed response pipe; inspect its stderr log"
                )
            result.extend(data)
    return bytes(result)


def gpu():
    return subprocess.check_output(
        [
            "nvidia-smi",
            "--query-gpu=name,driver_version,compute_cap,memory.total",
            "--format=csv,noheader",
        ],
        text=True,
    ).strip()


def idle():
    active = subprocess.check_output(
        ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True
    ).strip()
    if active:
        raise RuntimeError("Other GPU compute process active: " + active)


def native_suite(suite, base, out, iterations, validate):
    selected = {**copy.deepcopy(suite), "warmups": 1, "repeats": iterations}
    for case in selected["cases"]:
        # The direct Message API accepts explicit placeholders with raw media.
        # Match the upstream template's media-part-then-text rendering exactly.
        if case.get("kind") in ("image", "audio"):
            case["text"] = "<|" + case["kind"] + "|>" + case["text"]
        if case.get("path"):
            case["path"] = os.path.relpath(
                (base / case["path"]).resolve(), out.resolve()
            )
        if validate:
            case["logits_path"] = case["id"] + ".f32"
            case["max_tokens"] = 1
    return selected


def validate_suite(suite, manifest):
    by_id = {case["id"]: case for case in manifest["cases"]}
    if len(by_id) != len(manifest["cases"]) or len(
        {case["id"] for case in suite["cases"]}
    ) != len(suite["cases"]):
        raise ValueError("Duplicate suite/manifest case IDs")
    selected = [case for case in suite["cases"] if case["id"] in by_id]
    if {case["id"] for case in selected} != set(by_id):
        raise ValueError("Suite/manifest coverage differs")
    for case in selected:
        for field in ("kind", "text", "path", "max_tokens", "expected_prompt_tokens"):
            if case.get(field) != by_id[case["id"]].get(field):
                raise ValueError(
                    "Suite/manifest case differs: " + case["id"] + ":" + field
                )
    return selected


def compiler_environment():
    return {
        key: os.environ[key]
        for key in ("XLA_FLAGS", "TF_XLA_FLAGS", "NVIDIA_TF32_OVERRIDE")
        if os.environ.get(key)
    }


class Pjrt:
    def __init__(self, a, manifest, out, prepare_only=False):
        self.a = a
        self.manifest = manifest
        self.out = out
        self.log = (out / "pjrt.log").open("wb")
        identity = {
            "manifest": digest(a.manifest),
            "worker": digest(a.runner),
            "plugin": digest(a.plugin),
            "gpu": gpu(),
            "gguf": manifest["preparation"]["gguf_sha256"],
            "mmproj": manifest["preparation"]["mmproj_sha256"],
        }
        if flags := compiler_environment():
            identity["compiler_environment"] = flags
        key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        self.cache = a.cache / key
        self.cache.mkdir(parents=True, exist_ok=True)
        (self.cache / "identity.json").write_text(json.dumps(identity, indent=2) + "\n")
        paths = [
            a.manifest.parent / case[phase]
            for case in manifest["cases"]
            for phase in ("prefill", "decode", "logits")
        ]
        if not prepare_only and any(
            not (self.cache / (digest(path) + ".pjrt")).exists() for path in paths
        ):
            self.log.close()
            raise RuntimeError("Run --prepare-only before warm measurement")
        self.process = subprocess.Popen(
            [
                str(a.runner.resolve()),
                str(a.plugin.resolve()),
                str(a.manifest.resolve()),
                str(self.cache.resolve()),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.log,
            bufsize=0,
            start_new_session=True,
        )
        self.sampler = MemorySampler(
            SimpleNamespace(pid=os.getpid()), out / "pjrt.memory.jsonl"
        )
        self.sampler.thread.start()
        try:
            parameters(self.process.stdin, manifest, a.gguf, a.mmproj)
            if exact(self.process.stdout, 6) != b"READY\n":
                raise RuntimeError("Unexpected worker startup")
        except BaseException:
            self.close()
            raise

    def call(self, index, proc, base, validate=False):
        case = self.manifest["cases"][index]
        started = time.perf_counter()
        values, metadata = prepare(proc, case, base)
        if metadata != case["processor_metadata"]:
            raise ValueError("Prepared media frame metadata changed")
        if len(values["input_ids"][0]) != case["expected_prompt_tokens"]:
            raise ValueError("Prepared prompt geometry changed")
        self.process.stdin.write(
            struct.pack("<I", index | (0x80000000 if validate else 0))
        )
        for spec in case["inputs"]:
            value = values[spec["name"]].numpy()
            if list(value.shape) != spec["shape"]:
                raise ValueError(("Input geometry", spec["name"]))
            dtype = "<i8" if spec["dtype"] == "int64" else "<f4"
            write_all(self.process.stdin, np.asarray(value, dtype=dtype).tobytes())
        self.process.stdin.flush()
        upload, prefill, decode = struct.unpack("<QQQ", exact(self.process.stdout, 24))
        count = 262144 if validate else case["max_tokens"]
        result = np.frombuffer(
            exact(self.process.stdout, count * (4 if validate else 8)),
            dtype="<f4" if validate else "<i8",
        ).copy()
        elapsed = time.perf_counter() - started
        return {
            "id": case["id"],
            "ms": elapsed * 1000,
            "upload_ms": upload / 1e6,
            "prefill_ms": prefill / 1e6,
            "decode_ms": decode / 1e6,
            "worker_ms": (upload + prefill + decode) / 1e6,
            "decode_tok_s": (case["max_tokens"] - 1) / (decode / 1e9)
            if decode and not validate
            else None,
            "processor_and_ipc_ms": elapsed * 1000 - (upload + prefill + decode) / 1e6,
            "processor_metadata": metadata,
        }, result

    def close(self):
        if hasattr(self, "process"):
            try:
                if self.process.poll() is None:
                    self.process.stdin.write(struct.pack("<I", 0xFFFFFFFF))
                    self.process.stdin.close()
                    self.process.wait(timeout=30)
            except (OSError, subprocess.TimeoutExpired):
                self.process.kill()
                self.process.wait(timeout=30)
            self.process.stdout.close()
        if hasattr(self, "sampler"):
            memory = self.sampler.stop()
            (self.out / "pjrt.memory.json").write_text(
                json.dumps(memory, indent=2) + "\n"
            )
        self.log.close()


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
        "out",
        "cache",
    ):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--prepare-only", action="store_true")
    p.add_argument("--arm", choices=("native", "pjrt", "both"), default="both")
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--iterations", type=int, default=3)
    p.add_argument("--validate", action="store_true")
    p.add_argument("--processor", type=Path)
    a = p.parse_args()
    if not 1 <= a.rounds <= 10 or not 1 <= a.iterations <= 100:
        p.error("Invalid campaign bounds")
    if a.validate and a.iterations != 1:
        p.error(
            "Validation requires --iterations 1 to retain every measured logit vector"
        )
    a.out.mkdir(parents=True, exist_ok=False)
    manifest = json.loads(a.manifest.read_text())
    suite = json.loads(a.suite.read_text())
    suite["cases"] = validate_suite(suite, manifest)
    ids = {c["id"] for c in suite["cases"]}
    for name in ("gguf", "mmproj"):
        if digest(getattr(a, name)) != manifest["preparation"][name + "_sha256"]:
            raise ValueError("Model source hash differs")
    for name, identity in suite["files"].items():
        path = a.suite.parent / name
        if (
            path.stat().st_size != identity["bytes"]
            or digest(path) != identity["sha256"]
        ):
            raise ValueError("Fixture identity differs: " + name)
    processor_dir = a.processor or a.suite.parent.parent / "gemma4-upstream-sidecars"
    provenance = {
        "gpu": gpu(),
        "files": {
            name: digest(getattr(a, name))
            for name in (
                "native",
                "runner",
                "plugin",
                "tokenizer",
                "template",
                "suite",
                "manifest",
            )
        },
        "precision": {
            "native": "QAT Q4_0 / F16 KV",
            "pjrt": "dense BF16 from same GGUF / BF16 KV",
        },
        "cold_compile_excluded": not a.prepare_only,
        "validation_only": a.validate,
        "rounds": a.rounds,
        "iterations": a.iterations,
        "warmups_per_case": 1,
        "sources": {
            path.name: digest(path) for path in Path(__file__).parent.glob("*.py")
        },
        "processor": {
            path.name: digest(path)
            for path in processor_dir.glob("*")
            if path.is_file()
        },
        "versions": {
            name: importlib.metadata.version(name)
            for name in ("torch", "transformers", "numpy", "gguf", "av")
        },
        "models": {
            name: manifest["preparation"][name + "_sha256"]
            for name in ("gguf", "mmproj")
        },
        "fixture_scope": suite["fixture_scope"],
        "video_prompt_convention": "single video marker followed by one space, matching native ChatTemplate",
        "compiler_environment": compiler_environment(),
        "runtime_environment": {
            key: value
            for key, value in sorted(os.environ.items())
            if key.startswith("ANTFLY_INFERENCE_")
            or key
            in (
                "CUDA_VISIBLE_DEVICES",
                "NVIDIA_TF32_OVERRIDE",
                "TERMITE_DISABLE_PLE",
                "TERMITE_DISABLE_SHARED_KV",
                "TERMITE_DISABLE_SLIDING_ATTENTION",
                "TERMITE_DENSE_DECODE_EVAL_STRIDE",
                "TERMITE_GEMMA4_DISABLE_LAYER_OUTPUT_SCALE",
                "TERMITE_GEMMA4_DISABLE_RMSNORM_ADD_FUSION",
            )
        },
    }
    (a.out / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    proc = processor(processor_dir)
    if a.prepare_only:
        idle()
        session = Pjrt(a, manifest, a.out, True)
        session.close()
        print("prepared cache", session.cache, flush=True)
        return
    results = {"native": [], "pjrt": []}
    for round_index in range(a.rounds):
        arms = ["native", "pjrt"] if round_index % 2 == 0 else ["pjrt", "native"]
        for arm in arms:
            if a.arm != "both" and arm != a.arm:
                continue
            idle()
            out = a.out / f"r{round_index}-{arm}"
            out.mkdir()
            if arm == "native":
                selected = native_suite(
                    suite, a.suite.parent, out, a.iterations, a.validate
                )
                config = out / "suite.json"
                config.write_text(json.dumps(selected) + "\n")
                sampler = MemorySampler(
                    SimpleNamespace(pid=os.getpid()), out / "native.memory.jsonl"
                )
                sampler.thread.start()
                try:
                    with (out / "native.log").open("wb") as stderr:
                        subprocess.run(
                            [
                                str(a.native.resolve()),
                                str(a.gguf.resolve()),
                                str(a.tokenizer.resolve()),
                                str(a.template.resolve()),
                                str(a.mmproj.resolve()),
                                str(config.resolve()),
                                str((out / "result.json").resolve()),
                            ],
                            stderr=stderr,
                            stdout=stderr,
                            check=True,
                            timeout=1800,
                        )
                finally:
                    (out / "native.memory.json").write_text(
                        json.dumps(sampler.stop(), indent=2) + "\n"
                    )
                report = json.loads((out / "result.json").read_text())
                rows = report["rows"]
            else:
                session = Pjrt(a, manifest, out)
                try:
                    rows = []
                    for index, case in enumerate(manifest["cases"]):
                        for rep in range(1 + a.iterations):
                            row, values = session.call(
                                index, proc, a.suite.parent, a.validate
                            )
                            if rep:
                                row["repeat"] = rep
                                if a.validate:
                                    values.tofile(out / (case["id"] + f"-{rep}.f32"))
                                else:
                                    row["tokens"] = values.tolist()
                                rows.append(row)
                    (out / "result.json").write_text(
                        json.dumps({"rows": rows}, indent=2) + "\n"
                    )
                finally:
                    session.close()
            results[arm].extend(rows)
            memory = json.loads((out / f"{arm}.memory.json").read_text())
            if not memory["valid"]:
                raise RuntimeError("Invalid memory sampling; inspect " + str(out))
            print(
                round_index,
                arm,
                {
                    case_id: round(
                        statistics.median(r["ms"] for r in rows if r["id"] == case_id),
                        3,
                    )
                    for case_id in ids
                },
                flush=True,
            )
    summary = {
        "scope": "encoded media file read and official preprocessing/native pipelines through fixed-budget generation and synchronized tokens; no HTTP or cold load/compile",
        "results": [
            {
                "id": case_id,
                "ms": {
                    arm: statistics.median(r["ms"] for r in rows if r["id"] == case_id)
                    for arm, rows in results.items()
                    if rows
                },
            }
            for case_id in sorted(ids)
        ],
    }
    (a.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
