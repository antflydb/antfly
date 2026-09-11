"""Input-only FP32 scaling profile; imports no ML runtime.

Widths include the complete schema prefix. Generated texts are exercises, with
no inherited expected extraction outputs or held-out quality claim.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat

import benchmark_cpu as cpu
import oracle

SCOPE = "gliner25_fp32_scaling_inputs_v1"
SOURCE_SCOPE = "gliner25_fp32_scaling_source_requests_v1"
MANIFEST_SCOPE = "gliner25_fp32_scaling_manifest_v1"
VARIANTS = ("small", "base", "multi")
WIDTHS = (128, 256, 512)
BATCHES = (1, 2, 4)
REGULAR = ("mixed_tasks", "record_natural", "record_latent", "record_anchorless")
RAGGED = ("unicode_offsets", "joint_ie")
MAX_JSON_BYTES = 8 * 1024**2
MAX_CASES, MAX_BATCH, MAX_WORDS, MAX_TOKENS = 128, 8, 512, 512
TOKEN_EVIDENCE_SHA256 = "b6c8d4d7ff5f6a944eac36e26bb258bd177ad6e08b4171f11583d75cee65fdc8"
SPLITTER_SHA256 = "8c533d4defec1cc469578bdb87244d57a82684ecff57431d0556a54e9a6eefc8"
POLICY = {
    "format_version": 1, "widths": list(WIDTHS), "batch_sizes": list(BATCHES),
    "regular_templates": list(REGULAR), "ragged_templates": list(RAGGED),
    "ragged_batch_size": 4, "batch8_smoke_width": 128,
    "word_splitter": "whitespace", "threshold": 0.5,
    "max_text_words": MAX_WORDS, "max_encoded_tokens": MAX_TOKENS,
    "max_queries": 64, "padding_token_id": 0,
    "confidence_absolute_tolerance": 5e-4,
    "length_unit": "complete_schema_prefix_plus_source_word_fragment_subwords",
    "ragged_lengths": "max(template_minimum,floor(width*[1,3/4,1/2,1/4]))",
    "text_construction": "repeat_complete_original_sentences_then_prepend_one_token_fillers",
    "filler_preference": ["a", "the", "and", "one", "today"],
    "qualification": False,
}


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode()


def digest(raw):
    return {"size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def read(path: Path, expected=None, limit=MAX_JSON_BYTES):
    """Read/hash the same bounded regular descriptor; reject symlink inputs."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= limit:
            raise cpu.BenchmarkError(f"input is not a bounded regular file: {path}")
        raw = source.read(limit + 1)
        after = os.fstat(source.fileno())
    if len(raw) != info.st_size or after.st_size != info.st_size or len(raw) > limit:
        raise cpu.BenchmarkError(f"input changed while reading: {path}")
    pin = digest(raw)
    if expected is not None and pin != expected:
        raise cpu.BenchmarkError(f"input digest differs: {path}")
    return cpu.strict_json(raw), pin


def files_for(variant):
    return {name: {key: item[key] for key in ("size_bytes", "sha256")}
            for name, item in oracle.load_manifest()["models"][variant]["files"].items()}


def identity(variant):
    model = oracle.load_manifest()["models"][variant]
    return {"model": variant, "model_id": model["model_id"], "revision": model["revision"],
            "model_files": files_for(variant)}


def case_specs():
    result = [(name, width, batch, "regular") for name in REGULAR
              for width in WIDTHS for batch in BATCHES]
    result += [(name, width, 4, "ragged") for name in RAGGED for width in WIDTHS]
    result += [("mixed_tasks", 128, 8, "smoke")]
    return [(f"{name}_s{width}_b{batch}_{mode}", name, width, batch, mode)
            for name, width, batch, mode in result]


def packet(sequences):
    if (not isinstance(sequences, list) or not 1 <= len(sequences) <= MAX_BATCH
            or any(not isinstance(ids, list) or not 1 <= len(ids) <= MAX_TOKENS
                   or any(type(token) is not int or not 0 <= token < 2**32 for token in ids)
                   for ids in sequences)):
        raise cpu.BenchmarkError("invalid bounded source token sequences")
    width = max(map(len, sequences))
    return {"input_shape": [len(sequences), width],
            "input_ids": [token for ids in sequences for token in ids + [0] * (width - len(ids))],
            "attention_mask": [bit for ids in sequences for bit in [1] * len(ids) + [0] * (width - len(ids))]}


def check_packet(response, sequences):
    expected = packet(sequences)
    for key, value in expected.items():
        observed = response.get(key)
        if (not isinstance(observed, list) or any(type(item) is not int for item in observed)
                or observed != value):
            raise cpu.BenchmarkError(f"true-batch encoder packet differs: {key}")
    return expected


def ordered_equal(left, right):
    return encoded(left) == encoded(right)


def validate_pair(native, source, variant, source_sha256):
    expected_top = {"format_version", "scope", "source_commit", "model", "model_id",
                    "revision", "model_files", "requests_sha256", "cases"}
    if (not isinstance(native, dict) or set(native) != expected_top
            or type(native["format_version"]) is not int or native["format_version"] != 1
            or native["scope"] != SCOPE or native["source_commit"] != oracle.UPSTREAM_COMMIT
            or native["requests_sha256"] != source_sha256
            or any(native[key] != value for key, value in identity(variant).items())):
        raise cpu.BenchmarkError("scaling native input identity differs")
    if (not isinstance(source, dict) or set(source) != {"format_version", "scope", "source_commit",
            "model", "policy", "original_requests_sha256", "token_evidence_sha256", "cases"}
            or type(source["format_version"]) is not int or source["format_version"] != 1
            or source["scope"] != SOURCE_SCOPE or source["model"] != variant
            or source["source_commit"] != oracle.UPSTREAM_COMMIT or source["policy"] != POLICY
            or source["original_requests_sha256"] != oracle.sha256_file(oracle.FIXTURES / "requests.json")
            or source["token_evidence_sha256"] != TOKEN_EVIDENCE_SHA256):
        raise cpu.BenchmarkError("scaling source input identity differs")
    originals = {row["id"]: row for row in oracle.read_json(oracle.FIXTURES / "requests.json")["requests"]}
    specs = case_specs()
    if (not isinstance(native["cases"], list) or not isinstance(source["cases"], list)
            or len(native["cases"]) != len(specs) or len(source["cases"]) != len(specs)):
        raise cpu.BenchmarkError("scaling matrix is incomplete")
    import generate_pipeline_cases as adaptation
    for native_case, source_case, (name, template, width, batch, mode) in zip(native["cases"], source["cases"], specs):
        if (set(native_case) != {"id", "schema", "items", "expected_encoded_lengths", "encoded_width"}
                or set(source_case) != {"id", "template_id", "kind", "schema", "items",
                                        "expected_input_ids", "profile", "template_minimum"}):
            raise cpu.BenchmarkError("scaling case fields differ")
        original = originals[template]
        if (native_case["id"] != name or source_case["id"] != name
                or source_case["template_id"] != template or source_case["kind"] != original["kind"]
                or not ordered_equal(source_case["schema"], original["schema"])
                or not ordered_equal(native_case["schema"], adaptation.schema_for(original))
                or source_case["profile"] != {"width": width, "batch_size": batch, "mode": mode}
                or native_case["encoded_width"] != width):
            raise cpu.BenchmarkError("scaling schema, order or geometry changed")
        items = source_case["items"]
        if not isinstance(items, list) or len(items) != batch or not ordered_equal(items, native_case["items"]):
            raise cpu.BenchmarkError("scaling samples differ between implementations")
        minimum = source_case["template_minimum"]
        if type(minimum) is not int or not 1 <= minimum <= width:
            raise cpu.BenchmarkError("invalid template token minimum")
        lengths = ([max(minimum, width * n // 4) for n in (4, 3, 2, 1)] if mode == "ragged"
                   else [width] * batch)
        if mode == "ragged" and len(set(lengths)) < 2:
            raise cpu.BenchmarkError("ragged case has no padding")
        if (native_case["expected_encoded_lengths"] != lengths
                or any(type(n) is not int for n in native_case["expected_encoded_lengths"])
                or [len(ids) for ids in source_case["expected_input_ids"]] != lengths):
            raise cpu.BenchmarkError("scaling exact token lengths differ")
        packet(source_case["expected_input_ids"])
        for index, item in enumerate(items):
            if (not isinstance(item, dict) or set(item) != {"id", "text"}
                    or item["id"] != f"{name}.{index}"
                    or not isinstance(item["text"], str) or not 1 <= len(item["text"].encode()) <= 65536
                    or not item["text"].endswith(".") or original["text"] not in item["text"]):
                raise cpu.BenchmarkError("scaling text/identity or original source sentence differs")
    return {row["id"]: row for row in source["cases"]}


def load(prepared: Path, variant: str):
    manifest, manifest_pin = read(prepared / "manifest.json")
    if (manifest.get("scope") != MANIFEST_SCOPE or manifest.get("format_version") != 1
            or manifest.get("policy") != POLICY or manifest.get("source_commit") != oracle.UPSTREAM_COMMIT
            or manifest.get("models") != list(VARIANTS) or manifest.get("qualification") is not False
            or manifest.get("model_execution") is not False
            or set(manifest.get("files", {})) != {f"{prefix}_{name}.json" for name in VARIANTS
                                                for prefix in ("source", "scaling")}):
        raise cpu.BenchmarkError("scaling manifest profile or completeness differs")
    native_path, source_path = prepared / f"scaling_{variant}.json", prepared / f"source_{variant}.json"
    native, native_pin = read(native_path, manifest["files"][native_path.name])
    source, source_pin = read(source_path, manifest["files"][source_path.name])
    cases = validate_pair(native, source, variant, source_pin["sha256"])
    return {"model": variant, "case_path": native_path, "source_path": source_path,
            "prepared": prepared, "cases": list(cases), "source_cases": cases,
            "requests_sha256": source_pin["sha256"], "pins": {
                "manifest": manifest_pin, "native_inputs": native_pin, "source_inputs": source_pin},
            "reference_input_ids": {name: packet(row["expected_input_ids"]) for name, row in cases.items()}}
