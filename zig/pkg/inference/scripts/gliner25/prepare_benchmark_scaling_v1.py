#!/usr/bin/env python3
"""Prepare versioned true-batch FP32 exercises using only the pinned tokenizer.

No Torch, model tensors, downloads or extraction. Each original token suffix
must exactly reproduce the saved encoder observation before its prefix is used.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import importlib.util
from pathlib import Path
import platform
import sys
import unicodedata

import scaling_contract_v1 as contract
import oracle


def body_ids(text, splitter, tokenize):
    fragments = list(splitter(text))
    if not 1 <= len(fragments) <= contract.MAX_WORDS:
        raise oracle.ContractError("text exceeds the fixed source word bound")
    result = []
    for word, start, end in fragments:
        if not 0 <= start < end <= len(text) or text[start:end].lower() != word:
            raise oracle.ContractError("word splitter changed original source coordinates")
        ids = tokenize(word)
        if not ids:
            raise oracle.ContractError("source word has no tokenizer output")
        result.extend(ids)
    return result


def expand(original, prefix, width, splitter, tokenize):
    if not original.endswith("."):
        raise oracle.ContractError("exercise template must end in an original terminal period")
    encode = lambda text: prefix + body_ids(text, splitter, tokenize)
    if len(encode(original)) > width:
        raise oracle.ContractError("schema and original text do not fit the declared width")
    filler = next((word for word in contract.POLICY["filler_preference"] if len(tokenize(word)) == 1), None)
    if filler is None:
        raise oracle.ContractError("pinned tokenizer has no declared one-token filler")
    text = original
    while len(encode(text + " " + original)) <= width:
        text += " " + original
    remaining = width - len(encode(text))
    if remaining:
        # Keep every original sentence byte-for-byte, including the smallest
        # ragged member where only one sentence fits. Prefix fillers so the
        # original final period remains intact; punctuation is not one token
        # in every published tokenizer.
        text = " ".join([filler] * remaining) + " " + text
    ids = encode(text)
    if len(ids) != width or len(text.encode()) > 65536:
        raise oracle.ContractError("exact inclusive-prefix width could not be constructed")
    return text, ids


def generate_variant(variant, evidence, splitter, tokenize):
    import generate_pipeline_cases as adaptation
    originals = {row["id"]: row for row in oracle.read_json(oracle.FIXTURES / "requests.json")["requests"]}
    prefixes, minima = {}, {}
    for template in (*contract.REGULAR, *contract.RAGGED):
        original = originals[template]
        body = body_ids(original["text"], splitter, tokenize)
        saved = evidence["validation"][template]["input_ids"]
        if not body or saved[-len(body):] != body or len(saved) <= len(body):
            raise oracle.ContractError(f"{variant}/{template}: original source token suffix differs")
        prefixes[template], minima[template] = saved[:-len(body)], len(saved)
    native_cases, source_cases = [], []
    cache = {}
    for name, template, width, batch, mode in contract.case_specs():
        original, prefix = originals[template], prefixes[template]
        lengths = ([max(minima[template], width * n // 4) for n in (4, 3, 2, 1)]
                   if mode == "ragged" else [width] * batch)
        items, sequences = [], []
        for index, length in enumerate(lengths):
            key = (template, length)
            if key not in cache:
                cache[key] = expand(original["text"], prefix, length, splitter, tokenize)
            text, ids = cache[key]
            items.append({"id": f"{name}.{index}", "text": text})
            sequences.append(ids)
        native_cases.append({"id": name, "schema": adaptation.schema_for(original), "items": items,
                             "expected_encoded_lengths": lengths, "encoded_width": width})
        source_cases.append({"id": name, "template_id": template, "kind": original["kind"],
            "schema": original["schema"], "items": items, "expected_input_ids": sequences,
            "profile": {"width": width, "batch_size": batch, "mode": mode},
            "template_minimum": minima[template]})
    source = {"format_version": 1, "scope": contract.SOURCE_SCOPE, "source_commit": oracle.UPSTREAM_COMMIT,
              "model": variant, "policy": contract.POLICY,
              "original_requests_sha256": oracle.sha256_file(oracle.FIXTURES / "requests.json"),
              "token_evidence_sha256": contract.TOKEN_EVIDENCE_SHA256, "cases": source_cases}
    source_raw = contract.encoded(source)
    native = {"format_version": 1, "scope": contract.SCOPE, "source_commit": oracle.UPSTREAM_COMMIT,
              **contract.identity(variant), "requests_sha256": hashlib.sha256(source_raw).hexdigest(),
              "cases": native_cases}
    contract.validate_pair(native, source, variant, native["requests_sha256"])
    return native, source


def prepare(args):
    if any(name == "torch" or name.startswith("torch.") for name in sys.modules):
        raise oracle.ContractError("scaling preparation cannot import Torch")
    if args.output.exists():
        raise oracle.ContractError("output must be a fresh directory")
    provenance = oracle.verify_upstream_checkout(args.upstream)
    pinned_runtime = oracle.load_manifest()["runtime"]
    if (platform.python_version() != pinned_runtime["python"]
            or unicodedata.unidata_version != pinned_runtime["unicode"]):
        raise oracle.ContractError("Python/Unicode text preparation differs from the pinned oracle")
    if importlib.metadata.version("tokenizers") != oracle.load_manifest()["runtime"]["packages"]["tokenizers"]:
        raise oracle.ContractError("tokenizer package differs from the pinned oracle")
    evidence, pin = contract.read(oracle.FIXTURES / "token_evidence.json")
    if pin["sha256"] != contract.TOKEN_EVIDENCE_SHA256:
        raise oracle.ContractError("original encoder token evidence changed")
    splitter_path = args.upstream / "gliner2/processing/word_splitter.py"
    if oracle.sha256_file(splitter_path) != contract.SPLITTER_SHA256:
        raise oracle.ContractError("pinned source word splitter changed")
    spec = importlib.util.spec_from_file_location("gliner25_scaling_pinned_word_splitter", splitter_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    splitter = module.WhitespaceTokenSplitter()
    from tokenizers import Tokenizer
    rows = {row["model"]: row for row in evidence["models"]}
    args.output.mkdir(parents=True, mode=0o700)
    files, tokenizers = {}, {}
    for variant in contract.VARIANTS:
        path = args.model_root / variant / "tokenizer.json"
        expected = contract.files_for(variant)["tokenizer.json"]
        oracle.verify_file(path, expected)
        tokenizer = Tokenizer.from_file(str(path))
        native, source = generate_variant(variant, rows[variant], splitter,
            lambda word: tokenizer.encode(word, add_special_tokens=False).ids)
        if oracle.verify_file(path, expected) != expected:
            raise oracle.ContractError("tokenizer changed during preparation")
        tokenizers[variant] = expected
        for prefix, value in (("source", source), ("scaling", native)):
            raw = contract.encoded(value)
            if len(raw) > contract.MAX_JSON_BYTES:
                raise oracle.ContractError("prepared inputs exceed bounded JSON profile")
            name = f"{prefix}_{variant}.json"
            with (args.output / name).open("xb") as output:
                output.write(raw)
            files[name] = contract.digest(raw)
        del tokenizer
    oracle.verify_upstream_checkout(args.upstream)
    manifest = {"format_version": 1, "scope": contract.MANIFEST_SCOPE, "source_commit": oracle.UPSTREAM_COMMIT,
        "policy": contract.POLICY, "models": list(contract.VARIANTS), "files": files,
        "cases_per_variant": len(contract.case_specs()), "total_cases": len(contract.case_specs()) * 3,
        "qualification": False, "model_execution": False, "model_weight_files_read": False,
        "provenance": {"upstream": provenance, "tokenizers": tokenizers,
            "tokenizer_package": importlib.metadata.version("tokenizers"),
            "python": platform.python_version(), "unicode": unicodedata.unidata_version,
            "word_splitter_sha256": contract.SPLITTER_SHA256, "token_evidence": pin,
            "helpers": {Path(name).name: contract.digest(Path(name).read_bytes())
                        for name in (__file__, contract.__file__, oracle.__file__)}}}
    (args.output / "manifest.json").write_bytes(contract.encoded(manifest))
    for variant in contract.VARIANTS:
        contract.load(args.output, variant)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-root", type=Path, default=Path("/private/tmp/antfly-gliner25-models"))
    parser.add_argument("--upstream", type=Path, default=Path("/private/tmp/antfly-gliner25-upstream"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        value = prepare(args)
        print(f"Prepared {value['total_cases']} input-only cases; no model execution.")
        return 0
    except Exception as error:
        if args.output.is_dir() and not (args.output / "manifest.json").exists():
            failure = args.output / "preparation_failure.json"
            if not failure.exists():
                failure.write_bytes(contract.encoded({"status": "failed", "model_execution": False,
                    "error": f"{type(error).__name__}: {error}"}))
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
