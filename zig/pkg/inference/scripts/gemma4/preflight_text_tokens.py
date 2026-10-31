#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Untimed, exact prompt-token preflight for Antfly, llama.cpp and vLLM.

Run outside all measured requests. Native capture uses the existing CLI's shared
chat renderer/tokenizer and retains its exact output and source provenance. The
existing CLI still loads a model and can prefill: schedule it separately from GPU
work. Reference engines must match full token IDs, not merely token count.

Native timing can reuse a capture with --native-reference when the benchmark
report proves identical binary/model hashes; log integrity and token IDs are
reverified. No checkpoint capture is attempted: its replay kernels are restricted
to SM89 and are not a portable tokenization interface.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import struct
import subprocess
import urllib.error
import urllib.request

from native_benchmark_fixture import extract_tokens
import qualify_transcription as q


def native_gguf_checkpoint(directory, selected=None):
    """Bind the benchmark's supported local layout to one actual decoder.

    The native manifest loader also supports bundles, managed receipts and
    competing weight formats. Do not guess their resolution in a benchmark:
    require a plain GGUF directory until the server exposes loaded-file proof.
    Projector/head filenames follow models/manifest.zig's GGUF discovery rules.
    """
    directory = Path(directory).resolve(strict=True)
    if not directory.is_dir():
        raise ValueError("native checkpoint binding requires a local model directory")
    overrides = ("antfly_inference_bundle.json", "antfly_inference_variants.json",
                 ".antfly-download-complete.json", ".antfly-download-plan.json", ".antfly-download-in-progress")
    if any((directory / name).exists() for name in overrides):
        raise ValueError("native checkpoint binding does not support bundle/managed artifact overrides; use a plain GGUF benchmark directory")
    decoders = []
    def walk_error(error):
        raise error

    for current, directories, files in os.walk(directory, followlinks=False, onerror=walk_error):
        directories[:] = [name for name in directories if not name.startswith(".")]
        if any((Path(current) / name).is_symlink() for name in directories):
            raise ValueError("native checkpoint binding does not follow nested directory symlinks")
        for name in files:
            if name.startswith("."):
                continue
            path = Path(current) / name
            if path.name.endswith((".safetensors", ".safetensors.index.json", ".onnx")):
                raise ValueError("native checkpoint binding rejects competing weight formats")
            if not path.name.endswith(".gguf"):
                continue
            if not path.is_file():
                raise ValueError("native GGUF checkpoint must be a regular file")
            stem = path.name[:-5]
            projector = stem == "mmproj" or stem.startswith(("mmproj-", "mmproj_")) or stem.endswith(("-mmproj", "_mmproj"))
            head = path.name in ("gliner_head.gguf", "gliner2-head.gguf") or path.name.startswith("gliner2-head.")
            if projector or head:
                continue
            resolved = path.resolve(strict=True)
            if not resolved.is_relative_to(directory):
                raise ValueError("native GGUF checkpoint escapes its model directory")
            if path.parent != directory:
                raise ValueError("native checkpoint binding requires a top-level GGUF decoder, without nested fallback resolution")
            decoders.append(resolved)
    if len(decoders) != 1:
        raise ValueError("native checkpoint binding requires exactly one GGUF decoder; selection is missing or ambiguous")
    checkpoint = decoders[0]
    if selected is not None and not checkpoint.samefile(Path(selected).resolve(strict=True)):
        raise ValueError(f"--model-file differs from the GGUF native will load: {checkpoint}")
    return checkpoint


def prompt_identity(request):
    value = {key: request[key] for key in ("messages", "chat_template_kwargs")}
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def token_identity(tokens):
    if not tokens or any(type(token) is not int or not 0 <= token < 262144 for token in tokens):
        raise ValueError("invalid Gemma4 prompt token IDs")
    return hashlib.sha256(struct.pack("<" + "I" * len(tokens), *tokens)).hexdigest()


def require_match(actual, reference, request):
    if reference.get("engine") != "native" or reference.get("passed") is not True:
        raise ValueError("an authoritative passing native preflight is required")
    if reference.get("prompt_identity_sha256") != prompt_identity(request):
        raise ValueError("reference messages or chat-template arguments differ")
    expected = reference["token_ids"]
    if token_identity(expected) != reference.get("token_bytes_sha256"):
        raise ValueError("native token reference changed")
    if actual != expected:
        different = next((i for i, pair in enumerate(zip(actual, expected)) if pair[0] != pair[1]), min(len(actual), len(expected)))
        raise ValueError(f"prompt token mismatch at index {different}; actual count={len(actual)}, native count={len(expected)}")
    return token_identity(actual)


def reuse_native(reference, request, provenance):
    """Reuse an untuned capture only for the same binary, model and inputs."""
    require_match(reference["token_ids"], reference, request)
    captured = reference.get("source_provenance", {})
    for key in ("binary_sha256", "model_sha256", "model"):
        if not provenance.get(key) or provenance[key] != captured.get(key):
            raise ValueError(f"reused native token capture requires matching {key} provenance")
    if "native_cli_log" in reference:
        log = Path(reference["native_cli_log"])
        if q.file_sha(log) != reference["native_cli_log_sha256"]:
            raise ValueError("reused native CLI log changed")
        tokens, _ = parse_cli_tokens(log.read_text())
        fixture = {"token_ids": tokens, "logical_tokens": len(tokens),
                   "native_cli_log": str(log), "native_cli_log_sha256": q.file_sha(log)}
        for name in ("native_cli_source_root", "native_server_source_root"):
            if prompt_source_contract(Path(reference[name])) != reference["prompt_sources"]:
                raise ValueError("reused native CLI renderer/tokenizer source changed")
    else:
        # Previously captured, integrity-checked SM89 artifacts remain readable;
        # this branch never invokes checkpoint capture or GPU execution.
        fixture = extract_tokens(reference["checkpoint"], reference["checkpoint_sha256_to_reverify"], reference["identity"])
    if fixture["token_ids"] != reference["token_ids"]:
        raise ValueError("reused native checkpoint token IDs changed")
    return {**fixture, "capture_kind": "reused untimed native token evidence; same binary and model",
            "capture_provenance": captured}


def prompt_source_contract(root):
    """Hash the shared rendering/tokenization code, excluding execution kernels."""
    root = Path(root)
    parts = {}
    for directory in ("zig/lib/tokenizer/src", "zig/lib/jinja/src"):
        files = sorted((root / directory).rglob("*.zig"))
        if not files:
            raise ValueError(f"missing prompt source directory: {directory}")
        for path in files:
            parts[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    for name in ("zig/pkg/inference/src/native_generate.zig", "zig/pkg/inference/src/models/manifest.zig", "zig/pkg/inference/src/io/manifest.zig"):
        parts[name] = hashlib.sha256((root / name).read_bytes()).hexdigest()
    source = (root / "zig/pkg/inference/src/pipelines/generation.zig").read_text()
    for marker, end in (("pub const ChatTemplate = struct {", "\n};"),
                        ("pub fn encodePromptForGeneration(", "\n}"),
                        ("pub fn encodeNativeGenerationPrompt(", "\n}"),
                        ("fn shouldAddBosToken(", "\n}")):
        start = source.index(marker)
        finish = source.index(end, start) + len(end)
        parts["generation.zig::" + marker] = hashlib.sha256(source[start:finish].encode()).hexdigest()
    source = (root / "zig/pkg/inference/src/server/model_manager.zig").read_text()
    start = source.index("pub fn loadHuggingFaceTokenizerFromDir(")
    finish = source.index("pub fn loadSentencePieceTokenizerFromDirOrGguf(", start)
    parts["model_manager.zig::GGUF_BPE_loader"] = hashlib.sha256(source[start:finish].encode()).hexdigest()
    return parts


def parse_cli_tokens(output):
    if output.splitlines().count("chat_template=true") != 1:
        raise ValueError("native CLI did not confirm exactly one applied chat template")
    records = re.findall(r"^prompt_token_ids:([^\r\n]*)$", output, re.MULTILINE)
    if len(records) != 1 or not re.fullmatch(r"(?:\s+[0-9]+)+\s*", records[0]):
        raise ValueError("native CLI emitted missing, invalid or ambiguous prompt token IDs")
    tokens = [int(token) for token in records[0].split()]
    token_identity(tokens)
    marker = "prompt:\n"
    start = output.find(marker)
    finish = output.find("\nprompt_token_ids:", start + len(marker))
    if start < 0 or finish < 0:
        raise ValueError("native CLI emitted no rendered prompt")
    return tokens, output[start + len(marker):finish]


def capture_native_cli(args, request, provenance):
    messages, kwargs = request["messages"], request["chat_template_kwargs"]
    if len(messages) != 1 or messages[0].get("role") != "user" or not isinstance(messages[0].get("content"), str) or set(messages[0]) != {"role", "content"}:
        raise ValueError("native CLI preflight supports one plain user message")
    if kwargs != {"enable_thinking": False}:
        raise ValueError("native CLI preflight requires exactly enable_thinking=false")
    if not provenance.get("binary_sha256") or not provenance.get("model_sha256"):
        raise ValueError("native CLI preflight needs benchmark server binary and model SHA provenance")
    cli_sources = prompt_source_contract(args.native_cli_source_root)
    server_sources = prompt_source_contract(args.native_server_source_root)
    if cli_sources != server_sources:
        changed = sorted(key for key in cli_sources.keys() | server_sources.keys() if cli_sources.get(key) != server_sources.get(key))
        raise ValueError(f"native CLI/server prompt source mismatch: {changed}")
    native_gguf_checkpoint(args.model_file.resolve().parent, args.model_file)
    if q.file_sha(args.model_file) != provenance["model_sha256"]:
        raise ValueError("native CLI model file does not match benchmark model SHA")
    prefix, extra = json.loads(args.native_cli_prefix_json), json.loads(args.native_cli_args_json)
    if prefix not in ([], ["inference"]):
        raise ValueError("native CLI prefix must be [] or [inference]")
    allowed = {f"--{kind}-budget-mb" for kind in ("host", "backend", "combined", "kv", "scratch")}
    if not isinstance(extra, list) or len(extra) % 2 or any(extra[i] not in allowed or not isinstance(extra[i + 1], str) or not extra[i + 1].isdigit() for i in range(0, len(extra), 2)):
        raise ValueError("native CLI extra arguments may only set explicit resource budgets")
    command = [str(args.native_cli.resolve()), *prefix, "generate", str(args.model_file.resolve().parent),
               messages[0]["content"], "--backend", args.native_cli_backend, "--max-tokens", "0", "--temperature", "0",
               "--cache-dtype", "f16", "--disable-thinking", "--print-chat-template-status", "--print-prompt", "--print-prompt-token-ids",
               *extra]
    log = args.output.with_suffix(".cli.log")
    with log.open("wb") as stream:
        completed = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, timeout=600, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"native CLI preflight exited {completed.returncode}; inspect {log}")
    tokens, rendered = parse_cli_tokens(log.read_text())
    return {"token_ids": tokens, "logical_tokens": len(tokens), "capture_kind": "native CLI shared prompt renderer/tokenizer",
            "native_cli_command": command, "native_cli_binary_sha256": q.file_sha(args.native_cli),
            "native_cli_source_root": str(args.native_cli_source_root.resolve()),
            "native_server_source_root": str(args.native_server_source_root.resolve()), "prompt_sources": server_sources,
            "rendered_prompt_sha256": hashlib.sha256(rendered.encode()).hexdigest(), "native_cli_log": str(log.resolve()),
            "native_cli_log_sha256": q.file_sha(log),
            "execution_note": "Existing generate CLI loads a model and may prefill despite max_tokens=0; run outside all timed/GPU work"}


def post(base, path, body):
    headers = {"Content-Type": "application/json"}
    request = urllib.request.Request(base.rstrip("/") + path, json.dumps(body).encode(), headers)
    try:
        return urllib.request.urlopen(request, timeout=600)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"preflight HTTP {error.code}: {error.read(65536)!r}") from error


def post_json(base, path, body):
    with post(base, path, body) as response:
        return json.load(response)


def reference_tokens(engine, base, request):
    chat = {"messages": request["messages"], "chat_template_kwargs": request["chat_template_kwargs"], "add_generation_prompt": True}
    if engine == "llama":
        rendered = post_json(base, "/apply-template", chat)["prompt"]
        tokens = post_json(base, "/tokenize", {"content": rendered, "add_special": True, "parse_special": True})["tokens"]
        return tokens, {"rendered_prompt_sha256": hashlib.sha256(rendered.encode()).hexdigest()}
    if engine != "vllm":
        raise ValueError("unknown reference engine")
    # vLLM's chat template includes BOS; its chat tokenizer defaults to false.
    result = post_json(base, "/tokenize", {**chat, "model": request["model"], "add_special_tokens": False})
    if result.get("count") != len(result["tokens"]):
        raise ValueError("vLLM token-count metadata disagrees with token IDs")
    return result["tokens"], {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=("native", "llama", "vllm"), required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--native-reference", type=Path, help="Native: reuse same-binary/model CLI evidence; reference engines: compare full token IDs")
    parser.add_argument("--native-cli", type=Path, help="Capture prompt IDs using an existing generate CLI outside timing (model initialization still occurs)")
    parser.add_argument("--native-cli-prefix-json", default="[]", help="Subcommands before generate, e.g. [\"inference\"] for the antfly executable")
    parser.add_argument("--native-cli-backend", choices=("native", "cuda"), default="native")
    parser.add_argument("--native-cli-args-json", default="[]", help="Explicit resource-budget arguments for the CLI process")
    parser.add_argument("--native-cli-source-root", type=Path)
    parser.add_argument("--native-server-source-root", type=Path)
    parser.add_argument("--model-file", type=Path)
    parser.add_argument("--request-file", type=Path, help="Exact generation body, or a benchmark report containing request")
    parser.add_argument("--model", default=q.DEFAULT_MODEL)
    parser.add_argument("--prompt-file", type=Path)
    parser.add_argument("--tokens", type=int, default=256)
    args = parser.parse_args()
    if args.engine != "native" and args.native_reference is None:
        parser.error("reference engines require --native-reference")
    if args.native_cli and (args.engine != "native" or args.native_reference or not all((args.native_cli_source_root, args.native_server_source_root, args.model_file))):
        parser.error("--native-cli requires native engine, model-file and both source roots; cannot combine with native-reference")
    if args.request_file:
        source = json.loads(args.request_file.read_text())
        request = source.get("request", source)
    else:
        request = q.generation_request(args.model, args.tokens, prompt=args.prompt_file.read_text() if args.prompt_file else None)
        source = {}
    provenance = {key: source.get(key) for key in ("binary_sha256", "model_sha256", "model")}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    evidence = {"engine": args.engine, "base_url": args.base_url, "request": request,
                "prompt_identity_sha256": prompt_identity(request), "timed": False, "passed": False,
                "source_provenance": provenance}
    try:
        if args.engine == "native":
            if args.native_cli:
                fixture = capture_native_cli(args, request, provenance)
            elif args.native_reference:
                reference = json.loads(args.native_reference.read_text())
                fixture = reuse_native(reference, request, provenance)
                evidence["native_reference_sha256"] = hashlib.sha256(args.native_reference.read_bytes()).hexdigest()
            else:
                raise ValueError("native preflight requires --native-cli or --native-reference; portable tokenization cannot use checkpoint capture")
            evidence.update(fixture)
            evidence["token_bytes_sha256"] = token_identity(fixture["token_ids"])
        else:
            tokens, extra = reference_tokens(args.engine, args.base_url, request)
            reference = json.loads(args.native_reference.read_text())
            evidence.update(extra, token_ids=tokens, logical_tokens=len(tokens), token_bytes_sha256=require_match(tokens, reference, request),
                            native_reference_sha256=hashlib.sha256(args.native_reference.read_bytes()).hexdigest())
        evidence["passed"] = True
    except Exception as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    print(json.dumps({key: value for key, value in evidence.items() if key not in ("token_ids", "request", "capture_request")}, indent=2))


if __name__ == "__main__":
    main()
