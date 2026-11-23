#!/usr/bin/env python3
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

"""Device benchmark for mixed-precision GGUF plans (Metal, CUDA or CPU).

`prepare` builds one model directory per variant: uniform formats and plans
for each fusion target and budget, each a GGUF beside the source's tokenizer
and config sidecars, plus a manifest recording plan, size and the fused
groups each plan splits.

`run` greedily generates from every variant on one backend and reports decode
tokens/s, prefill time, greedy-token agreement with a reference variant, and
the backend's fusion and fallback counters from `generate --json-timing`, so
a slowdown can be traced to the fused kernel it lost.

    bench_variants.py prepare --antfly antfly-inference --model MODEL \\
        --sensitivity kl.json --base-format q4_k --budgets 5.0,5.5 \\
        --targets portable,metal,cuda,untied,metal-qkv-free,cuda-qkv-free --uniform q4_0,q4_k,q5_k,q8_0 \\
        --out bench/
    bench_variants.py run --antfly antfly-inference --out bench/ \\
        --backend metal --reference uniform-q8_0 --max-tokens 128 --repeat 3 \\
        [--extra qat-q4_0=/path/to/gemma-4-e2b-qat-q4_0-dir]

Quality belongs to torch_lm_sensitivity.py (token KL); agreement here is a
cheap on-device cross-check that the runtime matches it.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

WEIGHT_SUFFIXES = (
    ".safetensors",
    ".bin",
    ".onnx",
    ".onnx.data",
    ".gguf",
    ".pt",
    ".pth",
)
DEFAULT_PROMPTS = [
    "What is 17 times 23? Show the steps.",
    "Write a Python function that merges overlapping intervals.",
    "Explain how a hash map handles collisions.",
    "Summarize the causes of the French Revolution in three sentences.",
    "Write a short, friendly reply declining a meeting invitation.",
    "Translate 'The library opens at nine tomorrow' into French and German.",
]
COUNTER_PATTERN = re.compile(
    r"(fallback|pair|qkv|quant_block|direct|fused|split|miss)", re.IGNORECASE
)


def run(cmd: list[str], log: Path | None = None) -> subprocess.CompletedProcess:
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if log is not None:
        log.write_text(proc.stdout + "\n--- stderr ---\n" + proc.stderr)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr[-4000:])
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(cmd[:4])} ...")
    return proc


def copy_sidecars(model: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for entry in model.iterdir():
        if (
            entry.name.endswith(WEIGHT_SUFFIXES)
            or entry.name == "model.safetensors.index.json"
        ):
            continue
        target = out_dir / entry.name
        if entry.is_dir():
            shutil.copytree(entry, target, dirs_exist_ok=True)
        else:
            shutil.copy2(entry, target)


def target_flags(target: str) -> list[str]:
    """`untied`, a fusion target (`portable`, `metal`, `cuda`), or a fusion
    target with free Q/K/V (`metal-qkv-free`): the plan a QKV kernel with
    per-projection formats would allow."""
    if target == "untied":
        return ["--untied"]
    if target.endswith("-qkv-free"):
        return ["--fusion-target", target.removesuffix("-qkv-free"), "--untie", "qkv"]
    return ["--fusion-target", target]


def prepare(args: argparse.Namespace) -> None:
    out = Path(args.out)
    (out / "plans").mkdir(parents=True, exist_ok=True)
    model = Path(args.model)
    variants = []

    def export(name: str, base: str, plan: Path | None) -> None:
        vdir = out / "variants" / name
        if vdir.exists():
            shutil.rmtree(vdir)
        copy_sidecars(model, vdir)
        cmd = [
            args.antfly,
            "export",
            str(model),
            "--target",
            "gguf",
            "--format",
            base,
            "--output",
            str(vdir / "model.gguf"),
        ]
        if plan is not None:
            cmd += ["--quantize-plan", str(plan)]
        log = out / "plans" / f"{name}.export.log"
        run(cmd, log)
        split = [
            line
            for line in log.read_text().splitlines()
            if line.startswith("warning:") and "not fused" in line
        ]
        variants.append(
            {
                "name": name,
                "dir": str(vdir),
                "plan": str(plan) if plan else None,
                "file_bytes": (vdir / "model.gguf").stat().st_size,
                "split_on_metal": sum("Metal" in line for line in split),
                "split_on_cuda": sum("CUDA" in line for line in split),
            }
        )
        print(
            f"{name}: {variants[-1]['file_bytes']} bytes, {len(split)} split groups",
            file=sys.stderr,
        )

    for fmt in filter(None, args.uniform.split(",")):
        export(f"uniform-{fmt}", fmt, None)
    if args.sensitivity:
        for budget in filter(None, args.budgets.split(",")):
            for target in filter(None, args.targets.split(",")):
                name = f"plan-{target}-{budget}"
                plan = out / "plans" / f"{name}.json"
                cmd = [
                    args.antfly,
                    "quantize-plan",
                    "--sensitivity",
                    args.sensitivity,
                    "--bits-per-weight",
                    budget,
                    "--output",
                    str(plan),
                ]
                cmd += target_flags(target)
                if args.formats:
                    cmd += ["--formats", args.formats]
                run(cmd)
                export(name, args.base_format, plan)
    (out / "manifest.json").write_text(
        json.dumps({"model": str(model), "variants": variants}, indent=2)
    )


def counters(section: dict | None) -> dict[str, float]:
    found: dict[str, float] = {}
    if not isinstance(section, dict):
        return found
    for key, value in section.items():
        if isinstance(value, dict):
            for sub, inner in counters(value).items():
                found[f"{key}.{sub}"] = inner
        elif (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and value
            and COUNTER_PATTERN.search(key)
        ):
            found[key] = value
    return found


TOKEN_IDS_LINE = re.compile(r"^token_ids:((?: \d+)*)\s*$", re.MULTILINE)


def generated_token_ids(proc: subprocess.CompletedProcess, timing: dict) -> list[int]:
    """`generate --print-token-ids` prints the ids on a `token_ids:` line
    (stderr); the timing JSON field is used when a build fills it."""
    if timing.get("token_ids"):
        return [int(t) for t in timing["token_ids"]]
    match = TOKEN_IDS_LINE.search(proc.stderr) or TOKEN_IDS_LINE.search(proc.stdout)
    if match is None:
        raise SystemExit("generate printed no token_ids line")
    return [int(t) for t in match.group(1).split()]


def agreement(reference: list[int], tokens: list[int]) -> tuple[int, float]:
    """Tokens before the first divergence, and the matching fraction."""
    prefix = 0
    for a, b in zip(reference, tokens):
        if a != b:
            break
        prefix += 1
    n = max(len(reference), 1)
    return prefix, sum(a == b for a, b in zip(reference, tokens)) / n


def bench(args: argparse.Namespace) -> None:
    out = Path(args.out)
    manifest = (
        json.loads((out / "manifest.json").read_text())
        if (out / "manifest.json").exists()
        else {"variants": []}
    )
    variants = list(manifest["variants"])
    for spec in args.extra or []:
        name, path = spec.split("=", 1)
        variants.append({"name": name, "dir": path, "plan": None})
    if args.only:
        keep = set(args.only.split(","))
        variants = [v for v in variants if v["name"] in keep]
    prompts = DEFAULT_PROMPTS
    if args.prompts:
        prompts = [
            p
            for p in Path(args.prompts).read_text(encoding="utf-8").splitlines()
            if p.strip()
        ]

    results = []
    tokens_by_variant: dict[str, list[list[int]]] = {}
    with tempfile.TemporaryDirectory() as tmp:
        for variant in variants:
            speeds, prefill, load, outputs, totals = [], [], [], [], {}
            for p_index, prompt in enumerate(prompts):
                prompt_file = Path(tmp) / f"p{p_index}.txt"
                prompt_file.write_text(prompt, encoding="utf-8")
                for r in range(args.repeat):
                    timing = Path(tmp) / "timing.json"
                    cmd = [
                        args.antfly,
                        "generate",
                        variant["dir"],
                        "--prompt-file",
                        str(prompt_file),
                    ]
                    cmd += [
                        "--backend",
                        args.backend,
                        "--temperature",
                        "0",
                        "--max-tokens",
                        str(args.max_tokens),
                    ]
                    cmd += [
                        "--ignore-eos",
                        "--print-token-ids",
                        "--json-timing",
                        str(timing),
                    ] + (args.generate_args or [])
                    proc = run(cmd)
                    data = json.loads(timing.read_text())
                    if r == 0:
                        outputs.append(generated_token_ids(proc, data))
                    if r > 0 or args.repeat == 1:
                        speeds.append(float(data.get("decode_tok_per_s") or 0))
                        prefill.append(
                            float(
                                (data.get("timing_ms") or {}).get("prefill_inner") or 0
                            )
                        )
                        load.append(
                            float((data.get("timing_ms") or {}).get("load_model") or 0)
                        )
                    for section in ("metal", "cuda", "generation_decoder_runtime"):
                        for key, value in counters(data.get(section)).items():
                            totals[f"{section}.{key}"] = (
                                totals.get(f"{section}.{key}", 0) + value
                            )
            tokens_by_variant[variant["name"]] = outputs
            results.append(
                {
                    "variant": variant["name"],
                    "file_bytes": variant.get("file_bytes")
                    or sum(
                        f.stat().st_size for f in Path(variant["dir"]).glob("*.gguf")
                    ),
                    "split_on_metal": variant.get("split_on_metal"),
                    "split_on_cuda": variant.get("split_on_cuda"),
                    "decode_tok_per_s_median": statistics.median(speeds)
                    if speeds
                    else None,
                    "prefill_ms_median": statistics.median(prefill)
                    if prefill
                    else None,
                    "load_ms_median": statistics.median(load) if load else None,
                    "counters": totals,
                }
            )
            print(
                json.dumps({k: v for k, v in results[-1].items() if k != "counters"}),
                file=sys.stderr,
                flush=True,
            )

    reference = tokens_by_variant.get(args.reference)
    for row in results:
        if reference is None:
            continue
        pairs = [
            agreement(ref, toks)
            for ref, toks in zip(reference, tokens_by_variant[row["variant"]])
        ]
        row["greedy_prefix_tokens_mean"] = (
            statistics.mean(p for p, _ in pairs) if pairs else None
        )
        row["greedy_match_fraction"] = (
            statistics.mean(m for _, m in pairs) if pairs else None
        )

    report = {
        "backend": args.backend,
        "reference": args.reference,
        "prompts": len(prompts),
        "max_tokens": args.max_tokens,
        "repeat": args.repeat,
        "results": results,
    }
    (out / f"results-{args.backend}.json").write_text(json.dumps(report, indent=2))
    lines = [
        f"| variant ({args.backend}) | MB | split Metal/CUDA | decode tok/s | prefill ms | greedy prefix | greedy match |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in results:

        def fmt(value, spec):
            return "-" if value is None else format(value, spec)

        lines.append(
            f"| {row['variant']} | {row['file_bytes'] / 1e6:.1f} | {fmt(row['split_on_metal'], 'd')}/{fmt(row['split_on_cuda'], 'd')} "
            f"| {fmt(row['decode_tok_per_s_median'], '.1f')} | {fmt(row['prefill_ms_median'], '.0f')} "
            f"| {fmt(row.get('greedy_prefix_tokens_mean'), '.1f')} | {fmt(row.get('greedy_match_fraction'), '.3f')} |"
        )
    table = "\n".join(lines) + "\n"
    (out / f"results-{args.backend}.md").write_text(table)
    print(table)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--antfly", required=True, help="antfly-inference binary")
    p.add_argument(
        "--model", required=True, help="dense source model directory (safetensors)"
    )
    p.add_argument("--out", required=True)
    p.add_argument("--base-format", default="q4_k")
    p.add_argument(
        "--uniform",
        default="q4_0,q4_k,q5_k,q8_0",
        help="csv of uniform formats to export",
    )
    p.add_argument(
        "--sensitivity", help="sensitivity file; plans are skipped without one"
    )
    p.add_argument(
        "--budgets", default="5.0,5.5", help="csv of bits per planned weight"
    )
    p.add_argument(
        "--targets",
        default="portable,metal,cuda,untied,metal-qkv-free,cuda-qkv-free",
        help="csv of fusion targets, untied, or <target>-qkv-free (Q/K/V planned freely)",
    )
    p.add_argument("--formats", help="csv passed to quantize-plan --formats")
    r = sub.add_parser("run")
    r.add_argument("--antfly", required=True)
    r.add_argument("--out", required=True, help="directory written by prepare")
    r.add_argument("--backend", required=True, help="metal, cuda or native")
    r.add_argument(
        "--reference",
        default="uniform-q8_0",
        help="variant whose greedy tokens the others are compared with",
    )
    r.add_argument("--max-tokens", type=int, default=128)
    r.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="runs per prompt; the first is a warm-up when > 1",
    )
    r.add_argument("--prompts", help="UTF-8 file, one prompt per line")
    r.add_argument("--only", help="csv of variant names to run")
    r.add_argument(
        "--extra",
        action="append",
        help="name=model-dir: also run an existing variant, e.g. a QAT GGUF",
    )
    r.add_argument(
        "--generate-args",
        nargs=argparse.REMAINDER,
        help="passed through to generate (put last)",
    )
    args = parser.parse_args()
    {"prepare": prepare, "run": bench}[args.command](args)


if __name__ == "__main__":
    main()
