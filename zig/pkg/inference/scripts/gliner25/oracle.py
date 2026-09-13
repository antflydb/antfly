#!/usr/bin/env python3
"""Pinned, offline GLiNER2.5 oracle provenance and bounded fixture capture.

Verification uses only the standard library until the explicit runtime check.
Tiny fixtures are diagnostic references, never pretrained-model qualification.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib
import importlib.metadata
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import unicodedata
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Iterator


HERE = Path(__file__).resolve().parent
FIXTURES = HERE.parents[1] / "testdata" / "gliner25"
UPSTREAM_COMMIT = "3c913c7369301133d3b7699252074c4303ada50e"
MAX_ENCODED_TOKENS = 512
MAX_WORDS = 128
MAX_QUERIES = 64
MAX_TENSOR_BYTES = 32 * 1024 * 1024


class ContractError(ValueError):
    """The requested evidence cannot satisfy the pinned oracle contract."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ContractError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(path: Path) -> Any:
    def invalid_constant(value: str) -> None:
        raise ContractError(f"non-finite JSON constant: {value}")

    with path.open(encoding="utf-8") as source:
        return json.load(source, object_pairs_hook=_unique_object,
                         parse_constant=invalid_constant)


def write_json(path: Path, value: Any) -> None:
    def default(item: Any) -> Any:
        if isinstance(item, Mapping):
            return dict(item)
        raise TypeError(f"unsupported JSON capture value: {type(item).__name__}")

    path.write_text(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False,
                               allow_nan=False, default=default) + "\n", encoding="utf-8")


def load_manifest() -> dict[str, Any]:
    manifest = read_json(HERE / "oracle_manifest.json")
    if manifest.get("format_version") != 1:
        raise ContractError("unsupported oracle manifest version")
    if manifest["upstream"]["commit"] != UPSTREAM_COMMIT:
        raise ContractError("manifest does not identify the pinned upstream commit")
    if set(manifest["models"]) != {"base", "multi", "small"}:
        raise ContractError("manifest must identify all three GLiNER2.5 variants")
    return manifest


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_file(path: Path, expected: dict[str, Any]) -> dict[str, Any]:
    if not path.is_file():
        raise ContractError(f"required regular file is missing: {path}")
    size = path.stat().st_size
    if size != expected["size_bytes"]:
        raise ContractError(f"size mismatch for {path}: {size} != {expected['size_bytes']}")
    digest = sha256_file(path)
    if digest != expected["sha256"]:
        raise ContractError(f"SHA256 mismatch for {path}")
    return {"size_bytes": size, "sha256": digest}


def verify_config_fixtures() -> dict[str, Any]:
    files = {}
    for model in load_manifest()["models"].values():
        for expected in model["files"].values():
            if "fixture" in expected:
                relative = expected["fixture"]
                files[relative] = verify_file(FIXTURES / relative, expected)
    return {"scope": "configuration_fixtures", "files": files,
            "real_model_qualified": False, "native_runtime_qualified": False}


def verify_reference_fixtures() -> dict[str, Any]:
    manifest = read_json(FIXTURES / "reference_manifest.json")
    if manifest.get("format_version") != 1 or manifest.get("upstream_commit") != UPSTREAM_COMMIT:
        raise ContractError("reference fixture manifest must identify the pinned upstream")
    files = {}
    for relative, expected in manifest["files"].items():
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ContractError("reference manifest contains an unsafe path")
        files[relative] = verify_file(FIXTURES / path, expected)
    for relative, expected in manifest["generators"].items():
        if Path(relative).name != relative:
            raise ContractError("reference generator must be a file in the oracle directory")
        verify_file(HERE / relative, expected)
    required = {"primitives.json", "tiny/capture.json", "small_reference/capture.json"}
    if not required.issubset(files):
        raise ContractError("reference manifest is missing a required evidence scope")
    for relative in required:
        report = read_json(FIXTURES / relative)
        if report["provenance"]["commit"] != UPSTREAM_COMMIT:
            raise ContractError(f"reference source drift: {relative}")
        if any(report.get(key) is not False for key in (
            "real_model_qualified", "native_runtime_qualified", "training_qualified",
        )):
            raise ContractError("oracle fixture capture must not claim runtime qualification")
    return {"scope": "checked_in_oracle_references", "files": files,
            "real_model_qualified": False, "native_runtime_qualified": False}


def _git(source: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(source), *args], check=False, capture_output=True,
        text=True, timeout=20, env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
    )
    if result.returncode:
        raise ContractError(f"cannot verify upstream Git checkout: {result.stderr.strip()}")
    return result.stdout.strip()


def verify_upstream_checkout(
    source: Path, expected_commit: str = UPSTREAM_COMMIT,
) -> dict[str, Any]:
    source = source.expanduser().resolve()
    if not (source / "gliner2" / "__init__.py").is_file():
        raise ContractError(f"not a GLiNER2 source checkout: {source}")
    if Path(_git(source, "rev-parse", "--show-toplevel")).resolve() != source:
        raise ContractError("upstream path must identify the checkout root")
    commit = _git(source, "rev-parse", "HEAD")
    if commit != expected_commit:
        raise ContractError(f"upstream commit {commit} != pinned {expected_commit}")
    if _git(source, "status", "--porcelain=v1", "--untracked-files=all"):
        raise ContractError("upstream checkout must be clean, including untracked files")
    # Include ignored files too: an ignored Python module or bytecode file can
    # shadow the verified source even when ordinary git status is clean.
    if _git(source, "ls-files", "--others"):
        raise ContractError("upstream contains untracked/ignored files; remove them before capture")
    return {"commit": commit, "checkout": str(source)}


def verify_import_source(imported: Any, checkout: Path) -> str:
    filename = getattr(imported, "__file__", None)
    if filename is None:
        # Upstream gliner2.utils is a namespace package without __init__.py.
        # Every contributing search location must still belong to the pin.
        locations = [Path(path).resolve() for path in getattr(imported, "__path__", ())]
        if not locations or any(not path.is_relative_to(checkout.resolve()) for path in locations):
            raise ContractError("oracle import has no verifiable source path")
        return "namespace:" + ":".join(map(str, locations))
    filename = Path(filename).resolve()
    if not filename.is_relative_to(checkout.resolve()):
        raise ContractError(f"oracle imported from unpinned source: {filename}")
    return str(filename)


def verify_dependencies() -> dict[str, Any]:
    expected = load_manifest()["runtime"]
    actual = {"python": platform.python_version(), "unicode": unicodedata.unidata_version,
              "packages": {}}
    for name in expected["packages"]:
        try:
            actual["packages"][name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            actual["packages"][name] = "<missing>"
    if actual != expected:
        differences = [f"{key}={actual[key]} (expected {expected[key]})"
                       for key in ("python", "unicode") if actual[key] != expected[key]]
        differences.extend(
            f"{name}={actual['packages'][name]} (expected {version})"
            for name, version in expected["packages"].items()
            if actual["packages"][name] != version
        )
        raise ContractError("oracle runtime mismatch: " + "; ".join(differences))
    return actual


def prepare_runtime(source: Path) -> tuple[dict[str, Any], Any]:
    source = source.expanduser().resolve()
    provenance = verify_upstream_checkout(source)
    provenance["runtime"] = verify_dependencies()
    sys.dont_write_bytecode = True
    for key in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "TOKENIZERS_PARALLELISM"):
        os.environ[key] = "false" if key == "TOKENIZERS_PARALLELISM" else "1"
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ.pop("USE_FLASHDEBERTA", None)
    for name, module in tuple(sys.modules.items()):
        if name == "gliner2" or name.startswith("gliner2."):
            verify_import_source(module, source)
    sys.path.insert(0, str(source))
    import torch
    import gliner2
    from gliner2 import AutoExtractor, BoundaryExtractor  # noqa: F401

    if gliner2.__version__ != load_manifest()["upstream"]["package_version"]:
        raise ContractError("imported GLiNER2 package version does not match the manifest")
    imports = {}
    for name, module in tuple(sys.modules.items()):
        if name == "gliner2" or name.startswith("gliner2."):
            imports[name] = verify_import_source(module, source)
    provenance["imports"] = imports
    provenance["platform"] = {"system": platform.system(), "machine": platform.machine()}
    provenance["device"] = "cpu"
    provenance["dtype"] = "float32"
    provenance["threads"] = 1
    torch.set_num_threads(1)
    torch.use_deterministic_algorithms(True)
    torch.set_default_dtype(torch.float32)
    return provenance, torch


def verify_model_dir(variant: str, directory: Path) -> dict[str, Any]:
    model = load_manifest()["models"][variant]
    directory = directory.expanduser().resolve()
    files = {name: verify_file(directory / name, expected)
             for name, expected in model["files"].items()}
    # Reject loader-affecting extras, including tokenizer overrides or Python
    # code. Clean HF snapshots may also contain the known documentation assets.
    allowed = set(files) | {"README.md", ".gitattributes", "GitHub_new.jpg"}
    extras = sorted(str(path.relative_to(directory)) for path in directory.rglob("*")
                    if path.is_file() and str(path.relative_to(directory)) not in allowed)
    if extras:
        raise ContractError(f"unmanifested files in model bundle: {extras}")
    config = read_json(directory / "config.json")
    if (config.get("architecture"), config.get("architecture_version"),
        config.get("token_pooling")) != ("boundary", 1, "first"):
        raise ContractError("model bundle is not the supported boundary checkpoint profile")
    return {"model_id": model["model_id"], "revision": model["revision"],
            "directory": str(directory), "files": files}


@contextlib.contextmanager
def atomic_output_directory(destination: Path) -> Iterator[Path]:
    destination = destination.expanduser().absolute()
    if destination.exists() or destination.is_symlink():
        raise ContractError(f"refusing to overwrite existing output: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}-", dir=destination.parent))
    try:
        yield staging
        if destination.exists():
            raise ContractError(f"output appeared during capture: {destination}")
        staging.rename(destination)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def save_tensors(path: Path, tensors: dict[str, Any], torch: Any) -> dict[str, Any]:
    from safetensors.torch import save_file

    total = sum(tensor.numel() * tensor.element_size() for tensor in tensors.values())
    if total > MAX_TENSOR_BYTES:
        raise ContractError(f"tensor capture exceeds {MAX_TENSOR_BYTES} bytes")
    prepared = {}
    metadata = {}
    for name, tensor in sorted(tensors.items()):
        value = tensor.detach().cpu().contiguous().clone()
        if value.is_floating_point() and not bool(torch.isfinite(value).all()):
            raise ContractError(f"non-finite tensor: {name}")
        prepared[name] = value
        metadata[name] = {"shape": list(value.shape), "dtype": str(value.dtype).removeprefix("torch."),
                          "numel": value.numel()}
    save_file(prepared, str(path))
    return {"file": path.name, "sha256": sha256_file(path), "size_bytes": path.stat().st_size,
            "tensors": metadata}


def tiny_boundary_settings() -> dict[str, Any]:
    settings = read_json(FIXTURES / "models" / "base" / "config.json")["boundary_head"]
    settings.update(boundary_dim=16, pair_dim=16, content_dim=8, record_dim=16,
                    record_instance_queries=4, boundary_attention_window=4,
                    start_top_k=4, end_top_k=4, ends_per_start=2, starts_per_end=2,
                    candidate_budget=12, training_candidate_budget=16,
                    max_gold_per_query=4, end_block_size=8, pool_boundary_top_k=4,
                    pool_size=12, min_pool_per_query=2, relation_heads_per_type=4,
                    relation_tails_per_type=4, relation_pair_cap=8,
                    multihead_pair_compat_heads=4, dropout=0.0)
    return settings


def capture_boundary_head(directory: Path, torch: Any) -> dict[str, Any]:
    from gliner2.configuration import BoundaryHeadSettings
    from gliner2.models.boundary.model import BoundaryHead

    torch.manual_seed(1701)
    settings = tiny_boundary_settings()
    head = BoundaryHead(32, BoundaryHeadSettings(**settings), build_candidate_states=True).eval()
    token_states = torch.linspace(-1.0, 1.0, 2 * 7 * 32).reshape(2, 7, 32)
    text_mask = torch.arange(7)[None, :] < torch.tensor([7, 3])[:, None]
    query_states = torch.linspace(0.75, -0.75, 2 * 3 * 32).reshape(2, 3, 32)
    query_mask = torch.tensor([[True, True, True], [True, False, True]])
    explicit = torch.tensor([[[[0, 1], [0, 7], [1, 3]]] * 3,
                             [[[0, 1], [0, 3], [3, 3]]] * 3])
    with torch.inference_mode():
        encoding = head.boundary_encoder(token_states, text_mask)
        marginal = head.boundary_query_head(encoding.states, encoding.mask, token_states,
                                            text_mask, query_states, query_mask)
        pooled = head.shared_pool_builder(encoding.states, encoding.mask, query_mask,
                                           marginal.start_logits, marginal.end_logits)
        shared_logits, shared_features = head.shared_pool_scorer(
            encoding.states, query_states, query_mask, pooled, marginal.start_logits,
            marginal.end_logits, marginal.inside_prefix, text_mask.sum(-1), token_states,
            text_mask, marginal.inside_prefix_mean,
        )
        output = head(token_states, text_mask, query_states, query_mask)
        explicit_logits = head.score_explicit_spans(token_states, text_mask, query_states,
                                                   query_mask, explicit)
    tensors = {"input.token_states": token_states, "input.text_mask": text_mask,
               "input.query_states": query_states, "input.query_mask": query_mask,
               "input.explicit_spans": explicit, "boundary.states": encoding.states,
               "boundary.mask": encoding.mask, "marginal.start": marginal.start_logits,
               "marginal.end": marginal.end_logits, "marginal.inside": marginal.inside_logits,
               "marginal.inside_prefix": marginal.inside_prefix,
               "marginal.inside_mean": marginal.inside_prefix_mean,
               "pool.indices": pooled.indices, "pool.mask": pooled.mask,
               "pool.compat_logits": pooled.compat_logits,
               "pool.proposal_logits": pooled.proposal_logits,
               "shared.features": shared_features, "shared.logits": shared_logits,
               "candidate.indices": output.candidates.indices,
               "candidate.mask": output.candidates.valid_mask,
               "candidate.logits": output.candidates.pair_logits,
               "candidate.states": output.candidates.candidate_states,
               "query.abstention_logits": output.null_logits,
               "query.count_log_rates": output.count_log_rates,
               "explicit.logits": explicit_logits}
    return {"seed": 1701, "settings": settings,
            "weights": save_tensors(directory / "boundary_weights.safetensors",
                                    {"boundary_head." + name: value for name, value in head.state_dict().items()}, torch),
            "forward": save_tensors(directory / "boundary_tensors.safetensors", tensors, torch)}


def build_tiny_model(source: Path, torch: Any) -> Any:
    from gliner2 import BoundaryExtractor, ExtractorConfig
    from tests.fixtures import tiny_encoder, tiny_tokenizer

    verify_import_source(tiny_encoder, source)
    verify_import_source(tiny_tokenizer, source)
    tokenizer = tiny_tokenizer.build_tiny_tokenizer(extra_words=["alice", "good", "bad", "delete"])
    encoder = tiny_encoder.build_tiny_encoder_config(vocab_size=len(tokenizer))
    config = ExtractorConfig(model_name="tiny-bert-fixture", architecture="boundary",
                             boundary_head=tiny_boundary_settings(), token_pooling="first")
    torch.manual_seed(17)
    return BoundaryExtractor(config, encoder_config=encoder, tokenizer=tokenizer,
                             use_flashdeberta=False).float().cpu().eval()


def build_extract_schema(specification: dict[str, Any]) -> Any:
    from gliner2 import AttributeGroup, Schema

    specification = dict(specification)
    attributes = specification.pop("entity_attributes", {})
    schema = Schema.from_dict(specification)
    if attributes:
        schema.entity_attributes({name: AttributeGroup(**group) for name, group in attributes.items()})
    return schema


def bounded_batch(model: Any, text: str, schema: Any) -> Any:
    words = list(model.processor.word_splitter(text, lower=False))
    if len(words) > MAX_WORDS:
        raise ContractError(f"request exceeds the capture limit of {MAX_WORDS} words")
    raw = schema.build() if hasattr(schema, "build") else schema
    batch = model.processor.collate_fn_inference(
        [(text, raw)], max_len=MAX_WORDS, architecture="boundary", error_policy="raise",
        build_targets=False, on_capacity_exceeded="raise",
    )
    if batch.input_ids.shape[-1] > MAX_ENCODED_TOKENS:
        raise ContractError(f"schema plus text exceeds {MAX_ENCODED_TOKENS} encoded tokens")
    if batch.query_marker_mask.shape[-1] > MAX_QUERIES:
        raise ContractError(f"request exceeds {MAX_QUERIES} boundary queries")
    return batch


def capture_requests(model: Any, requests_path: Path, directory: Path, torch: Any) -> list[dict[str, Any]]:
    from gliner2.classification import Classifier, ClassificationConfig, ClassificationSchema
    from gliner2.joint_ie import JointIE, JointIEConfig, JointSchema

    document = read_json(requests_path)
    if document.get("format_version") != 1 or not 0 < len(document.get("requests", [])) <= 32:
        raise ContractError("expected a version-1 request fixture with 1..32 requests")
    seen = set()
    results = []
    for request in document["requests"]:
        name = request["id"]
        if not isinstance(name, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name) or name in seen:
            raise ContractError("fixture request IDs must be unique safe identifiers")
        seen.add(name)
        kind = request["kind"]
        text = request["text"]
        if not isinstance(text, str) or len(text.encode("utf-8")) > 65536:
            raise ContractError("fixture text must be a string of at most 65536 UTF-8 bytes")
        if kind == "extract":
            schema = build_extract_schema(request["schema"])
            batch = bounded_batch(model, text, schema)
            with torch.inference_mode():
                core = model._encode_core(batch)
                tensors = {"input.ids": batch.input_ids, "input.attention_mask": batch.attention_mask,
                           "encoded.text": core["text_states"], "encoded.text_mask": core["text_mask"],
                           "encoded.query": core["query_states"], "encoded.query_mask": core["query_mask"]}
                if core["query_states"].shape[1]:
                    output = model.boundary_head(core["text_states"], core["text_mask"],
                                                 core["query_states"], core["query_mask"])
                    tensors.update({"marginal.start": output.start_logits, "marginal.end": output.end_logits,
                                    "marginal.inside": output.inside_logits,
                                    "candidate.indices": output.candidates.indices,
                                    "candidate.mask": output.candidates.valid_mask,
                                    "candidate.logits": output.candidates.pair_logits})
                result = model.extract(text, schema, threshold=0.5, include_confidence=True,
                                       include_spans=True, max_len=MAX_WORDS)
            row = {"id": name, "kind": kind, "output": result,
                   "text_tokens": batch.text_tokens, "start_mappings": batch.start_mappings,
                   "end_mappings": batch.end_mappings, "queries": core["ext_specs"],
                   "tensor_capture": save_tensors(directory / f"{name}.safetensors", tensors, torch)}
        elif kind == "classification":
            schema = ClassificationSchema.from_dict(request["schema"])
            classifier = Classifier(model)
            compiled = classifier.compile_schema(schema)
            bounded_batch(model, text, compiled)
            config = ClassificationConfig(on_infeasible="raise", max_len=MAX_WORDS)
            scores = classifier.score(text, compiled, config=config)
            result = classifier.decode(scores, compiled, config=config)
            row = {"id": name, "kind": kind, "output": result.to_dict(), "logits": scores.tasks,
                   "schema_fingerprint": compiled.fingerprint}
        elif kind == "joint_ie":
            schema = JointSchema.from_dict(request["schema"])
            joint = JointIE(model)
            compiled = joint.compile_schema(schema)
            bounded_batch(model, text, compiled)
            result = joint.extract(text, compiled, config=JointIEConfig(max_len=MAX_WORDS))
            if not result.feasible:
                raise ContractError(f"upstream JointIE did not return a feasible result for {name}")
            row = {"id": name, "kind": kind, "output": result.to_dict(), "feasible": result.feasible}
        else:
            raise ContractError(f"unsupported fixture request kind: {kind}")
        results.append(row)
    return results


def capture(args: argparse.Namespace, tiny: bool) -> dict[str, Any]:
    verify_config_fixtures()
    provenance, torch = prepare_runtime(args.upstream)
    bundle = None if tiny else verify_model_dir(args.model, args.model_dir)
    with atomic_output_directory(args.output) as directory:
        report = {"format_version": 1, "status": "captured", "scope": "tiny_diagnostic" if tiny else "pretrained_reference",
                  "real_model_qualified": False, "native_runtime_qualified": False,
                  "training_qualified": False, "provenance": provenance,
                  "generator_sha256": sha256_file(Path(__file__)),
                  "manifest_sha256": sha256_file(HERE / "oracle_manifest.json"),
                  "requests_sha256": sha256_file(args.requests)}
        if tiny:
            report["boundary_head"] = capture_boundary_head(directory, torch)
            model = build_tiny_model(args.upstream.resolve(), torch)
            report["tiny_model"] = {"seed": 17, "parameters": sum(p.numel() for p in model.parameters()),
                                    "config": model.config.to_dict()}
        else:
            from gliner2 import AutoExtractor

            report["model"] = bundle
            model = AutoExtractor.from_pretrained(str(args.model_dir.resolve()), local_files_only=True,
                                                  map_location="cpu", use_flashdeberta=False).float().eval()
            if getattr(model, "architecture", None) != "boundary":
                raise ContractError("AutoExtractor did not load the boundary architecture")
        report["requests"] = capture_requests(model, args.requests, directory, torch)
        # Re-check after import/inference too; neither execution nor a concurrent
        # edit may silently invalidate the source/artifacts behind the result.
        verify_upstream_checkout(args.upstream)
        if not tiny and verify_model_dir(args.model, args.model_dir) != bundle:
            raise ContractError("model provenance changed during fixture capture")
        write_json(directory / "capture.json", report)
    return {"status": "captured", "scope": report["scope"], "output": str(args.output.resolve()),
            "requests": len(report["requests"]), "real_model_qualified": False,
            "native_runtime_qualified": False, "training_qualified": False}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("verify-fixtures", help="verify the nine checked-in config fixtures without ML packages")
    commands.add_parser("verify-references", help="verify checked-in tensor/output fixtures and generator hashes")
    verify = commands.add_parser("verify", help="verify the clean source, exact dependencies and actual imports")
    verify.add_argument("--upstream", type=Path, required=True)
    verify.add_argument("--model", choices=("base", "small", "multi"))
    verify.add_argument("--model-dir", type=Path)
    for name in ("capture-tiny", "capture-model"):
        command = commands.add_parser(name, help="capture bounded CPU/f32 oracle fixtures into a new directory")
        command.add_argument("--upstream", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--requests", type=Path, default=FIXTURES / "requests.json")
        if name == "capture-model":
            command.add_argument("--model", choices=("base", "small", "multi"), required=True)
            command.add_argument("--model-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "verify-fixtures":
            result = {"status": "verified", **verify_config_fixtures()}
        elif args.command == "verify-references":
            result = {"status": "verified", **verify_config_fixtures(), **verify_reference_fixtures()}
        elif args.command == "verify":
            if bool(args.model) != bool(args.model_dir):
                raise ContractError("--model and --model-dir must be supplied together")
            verify_config_fixtures()
            provenance, _ = prepare_runtime(args.upstream)
            result = {"status": "verified", "scope": "oracle_environment", "provenance": provenance,
                      "real_model_qualified": False, "native_runtime_qualified": False}
            if args.model:
                result["model"] = verify_model_dir(args.model, args.model_dir)
        else:
            result = capture(args, tiny=args.command == "capture-tiny")
        print(json.dumps(result, sort_keys=True, allow_nan=False))
        return 0
    except (ContractError, OSError, ImportError, RuntimeError, ValueError, KeyError, TypeError,
            subprocess.TimeoutExpired) as exc:
        print(json.dumps({"status": "error", "error_type": type(exc).__name__, "error": str(exc),
                          "real_model_qualified": False, "native_runtime_qualified": False}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
