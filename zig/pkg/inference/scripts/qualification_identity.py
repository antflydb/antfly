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

"""Content identities for immutable loaded model directories in controlled runs."""

import hashlib
import os
from pathlib import Path
import stat
import json
import re

import qualify_read_pool_40g as q

WEIGHTS = {".gguf", ".safetensors", ".onnx", ".bin", ".pt", ".pth"}
METADATA = {"model_manifest.json", "config.json", "antfly_inference_bundle.json"}


def sha256_file(path):
    """Never trust a cached checksum; reject a file changing while it is read."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise q.EvidenceUnavailable(f"model artifact is not a regular file: {path}")
        for block in iter(lambda: source.read(8 << 20), b""):
            digest.update(block)
        after = os.fstat(source.fileno())

    def signature(value):
        return (
            value.st_dev,
            value.st_ino,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )

    if signature(before) != signature(after) or signature(after) != signature(
        path.stat()
    ):
        raise q.EvidenceUnavailable(f"model artifact changed during hashing: {path}")
    return {"size": after.st_size, "sha256": digest.hexdigest()}


def model_files(directory):
    files = []
    # Follow model-cache symlinks, including shard directories. Detect cycles;
    # do not silently omit a linked weight. Operational checksum/download caches
    # are excluded; manifests, configs, tokenizers and all weights are hashed.
    visited = set()

    def walk_error(error):
        raise q.EvidenceUnavailable(f"cannot enumerate model artifacts: {error}")

    for root, dirs, names in os.walk(directory, followlinks=True, onerror=walk_error):
        root = Path(root)
        identity = root.stat()
        key = identity.st_dev, identity.st_ino
        if key in visited:
            raise q.EvidenceUnavailable(f"cyclic or aliased model directory: {root}")
        visited.add(key)
        dirs.sort()
        for name in sorted(names):
            if (
                name.endswith(".antfly-sha256-cache")
                or name == ".antfly-download-complete.json"
            ):
                continue
            path = root / name
            files.append(
                {"path": path.relative_to(directory).as_posix(), **sha256_file(path)}
            )
    names = {item["path"] for item in files}
    if not names.intersection(METADATA) or not any(
        Path(name).suffix in WEIGHTS for name in names
    ):
        raise q.EvidenceUnavailable(
            f"model lacks manifest/configuration or weight artifacts: {directory}"
        )
    return sorted(files, key=lambda item: item["path"])


TASK_SCOPES = ("embedders", "chunkers", "rerankers", "generators", "classifiers",
               "rewriters", "readers", "transcribers", "extractors")
COMPLETE_RECEIPT = ".antfly-download-complete.json"
BLOCKED_MARKERS = (".antfly-download-in-progress", ".antfly-download-plan.json")


def _stamp(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _safe_artifact(path):
    return (isinstance(path, str) and path and not path.startswith("/")
            and not any(char in path for char in ("\\", ":", "\0"))
            and all(part not in ("", ".", "..") for part in path.split("/")))


def _receipt_object(raw):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate managed receipt key")
            value[key] = item
        return value
    def invalid(value):
        raise ValueError("nonfinite managed receipt value: " + value)
    return json.loads(raw, object_pairs_hook=unique, parse_constant=invalid)


def _canonical_source(source):
    # Exact registry.managedModelSelectionAlloc grammar; derive FROM receipt,
    # never by splitting the public selector into a guessed filesystem path.
    for field in ("owner", "name"):
        part = source[field]
        if not isinstance(part, str) or part in (".", "..") or re.fullmatch(r"[A-Za-z0-9_.-]+", part) is None:
            raise ValueError("invalid managed source component")
    variant = source["variant"]
    if not isinstance(variant, str) or not 0 < len(variant.encode()) <= 512 or variant.count("@") > 1:
        raise ValueError("invalid managed source variant")
    if "@" in variant:
        variant, requested = variant.split("@")
        if (not requested or len(requested.encode()) > 256
                or any(part in ("", ".", "..") or re.fullmatch(r"[A-Za-z0-9_.-]+", part) is None for part in requested.split("/"))):
            raise ValueError("invalid managed requested revision")
        variant = variant or "auto"
    if ("/" in variant or "\\" in variant
            or any(part in ("", ".", "..") or any(ord(char) < 32 or ord(char) == 127 for char in part) for part in variant.split(":"))):
        raise ValueError("invalid managed source variant")
    base = source["owner"] + "/" + source["name"]
    qualified = base if variant == "auto" else base + ":" + variant
    resolved = source.get("resolved_revision")
    for field in ("requested_revision", "resolved_revision", "selected_format"):
        if source.get(field) is not None and not isinstance(source[field], str):
            raise ValueError("invalid managed source optional field")
    if resolved is not None and re.fullmatch(r"[0-9a-fA-F]{40}", resolved):
        return qualified + "@" + resolved
    return qualified


def _managed_publication(directory):
    directory = directory.resolve(strict=True)
    if any(os.path.lexists(directory / name) for name in BLOCKED_MARKERS):
        raise q.EvidenceUnavailable("managed publication is incomplete")
    path = directory / COMPLETE_RECEIPT
    if not os.path.lexists(path):
        return None
    if not path.resolve(strict=True).is_relative_to(directory):
        raise ValueError("managed receipt escapes installation")
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > 16 * 1024 * 1024:
            raise ValueError("invalid managed receipt size/type")
        raw = stream.read(16 * 1024 * 1024 + 1)
        after = os.fstat(stream.fileno())
    if len(raw) > 16 * 1024 * 1024 or _stamp(before) != _stamp(after) or _stamp(after) != _stamp(path.stat()):
        raise ValueError("managed receipt changed while read")
    receipt = _receipt_object(raw)
    if not isinstance(receipt, dict):
        raise ValueError("managed receipt must be an object")
    version = receipt.get("version", 1)
    if type(version) is not int or version not in (1, 2):
        raise ValueError("unsupported managed receipt version")
    source = receipt.get("source")
    canonical = _canonical_source(source) if source is not None else None
    artifacts = receipt["artifacts"]
    if not isinstance(artifacts, list) or not 0 < len(artifacts) <= 65536:
        raise ValueError("invalid managed artifact inventory")
    seen = set()
    stamps = {}
    aliases = {}
    for item in artifacts:
        name, size = item["path"], item["size"]
        if not _safe_artifact(name) or name in seen or type(size) is not int or not 0 <= size < 2**64:
            raise ValueError("unsafe/duplicate managed artifact")
        seen.add(name)
        target = (directory / name).resolve(strict=True)
        if not target.is_relative_to(directory):
            raise ValueError("managed artifact escapes installation")
        observed = target.stat()
        if not stat.S_ISREG(observed.st_mode) or observed.st_size != size:
            raise ValueError("managed artifact size/type differs")
        digest = item.get("sha256")
        if digest is not None and (not isinstance(digest, str) or re.fullmatch(r"[0-9a-fA-F]{64}", digest) is None):
            raise ValueError("invalid managed artifact digest")
        digest = digest.lower() if digest is not None else None
        prior = aliases.get(str(target))
        if prior is not None and digest is not None and prior != digest:
            raise ValueError("conflicting managed artifact alias digests")
        aliases[str(target)] = prior or digest
        stamps[name] = _stamp(observed)
    if not any(name.endswith((".gguf", ".onnx", ".safetensors")) for name in seen):
        raise ValueError("managed receipt lacks supported payload")
    if any(os.path.lexists(directory / name) for name in BLOCKED_MARKERS):
        raise q.EvidenceUnavailable("managed publication changed during validation")
    return {"sha256": hashlib.sha256(raw).hexdigest(), "version": version,
            "source": source, "canonical": canonical if version == 2 else None,
            "artifacts": artifacts, "artifact_stamps": stamps, "receipt_stamp": _stamp(after)}


def _selector_installations(root, model):
    # Registry discovers flat root/owner/model and the same two levels below
    # known task directories. Bound the scan; never descend through payloads.
    candidates = {root}
    visited = 0
    for base in [root, *(root / scope for scope in TASK_SCOPES)]:
        if not base.is_dir():
            continue
        for first in sorted(base.iterdir()):
            if first.name.startswith(".") or not first.is_dir() or base == root and first.name in TASK_SCOPES:
                continue
            visited += 1
            if visited > 4096:
                raise ValueError("managed selector inventory exceeds bounded scan")
            candidates.add(first)
            if (first / COMPLETE_RECEIPT).exists():
                continue
            for second in sorted(first.iterdir()):
                if second.name.startswith(".") or not second.is_dir():
                    continue
                visited += 1
                if visited > 4096:
                    raise ValueError("managed selector inventory exceeds bounded scan")
                candidates.add(second)
    matches = set()
    for directory in candidates:
        path = directory / COMPLETE_RECEIPT
        if not path.is_file() or any(os.path.lexists(directory / name) for name in BLOCKED_MARKERS):
            continue
        # Candidate-local invalid receipts do not hide an unrelated model, as
        # in registry listing. A plausible matching source remains ambiguous.
        with path.open("rb") as stream:
            raw = stream.read(16 * 1024 * 1024 + 1)
        if len(raw) > 16 * 1024 * 1024:
            continue
        try:
            receipt = _receipt_object(raw)
            if not isinstance(receipt, dict) or type(receipt.get("version")) is not int or receipt["version"] != 2:
                continue
            if _canonical_source(receipt["source"]) != model:
                continue
        except (ValueError, TypeError, KeyError):
            continue
        matches.add((str(directory.resolve(strict=True)), hashlib.sha256(raw).hexdigest()))
    return sorted(matches)


def _model_directory(root, model, mapping):
    if mapping is not None:
        if (not isinstance(mapping, dict) or set(mapping) != {"relative_path", "publication_sha256"}
                or not (mapping["relative_path"] == "." or _safe_artifact(mapping["relative_path"]))
                or not isinstance(mapping["publication_sha256"], str)
                or re.fullmatch(r"[0-9a-f]{64}", mapping["publication_sha256"]) is None):
            raise ValueError("invalid controlled managed root mapping")
        directory = (root / mapping["relative_path"]).resolve(strict=True)
        if not directory.is_relative_to(root):
            raise ValueError("controlled model root escapes configured namespace")
        publication = _managed_publication(directory)
        if (publication is None or publication["version"] != 2
                or publication["canonical"] != model
                or publication["sha256"] != mapping["publication_sha256"]):
            raise ValueError("controlled root differs from source/publication selector")
        signature = _selector_installations(root, model)
        if signature != [(str(directory), publication["sha256"])]:
            raise ValueError("managed selector root is ambiguous or changed")
        publication["selector_installations"] = signature
        return directory, publication
    relative = Path(model)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("unsafe loaded model identifier")
    candidates = [root / relative, *(root / scope / relative for scope in TASK_SCOPES)]
    candidates = {str(path.resolve(strict=True)): path for path in candidates if path.is_dir()}
    if len(candidates) != 1:
        raise ValueError("model root unavailable or ambiguous; managed selectors require a controlled receipt mapping")
    directory = next(iter(candidates.values()))
    publication = _managed_publication(directory)
    if publication is not None and publication["canonical"] is not None and publication["canonical"] != model:
        raise ValueError("loaded identifier differs from managed canonical selector")
    return directory, publication


def _inventory_key(inventory):
    return sorted((item["id"], item["model"], item["backend"], item["state"], tuple(sorted(item["tasks"]))) for item in inventory["data"])


def model_artifacts(configuration, url, working_directory, *, model_roots=None):
    try:
        return _model_artifacts(configuration, url, working_directory, model_roots=model_roots)
    except (OSError, KeyError, TypeError, ValueError) as error:
        raise q.EvidenceUnavailable(
            f"model artifact identity unavailable: {error}"
        ) from error


def _model_artifacts(configuration, url, working_directory, *, model_roots=None):
    root = Path(configuration["inference"]["models_dir"])
    if not root.is_absolute():
        root = working_directory / root
    root = root.resolve(strict=True)
    status, inventory, _ = q.http(url, "/models/instances")
    if status != 200 or not inventory.get("data"):
        raise q.EvidenceUnavailable(
            "loaded model inventory unavailable for artifact identity"
        )
    if model_roots is not None and (not isinstance(model_roots, dict)
            or not set(model_roots).issubset({item["model"] for item in inventory["data"]})):
        raise ValueError("controlled roots include an unobserved model")
    if len({item["model"] for item in inventory["data"]}) != len(inventory["data"]):
        raise ValueError("ambiguous loaded model identifiers")
    models = []
    for instance in inventory["data"]:
        model = instance["model"]
        relative = Path(model)
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or instance["state"] != "loaded"
        ):
            raise q.EvidenceUnavailable(f"unresolved loaded model identity: {model}")
        directory, publication = _model_directory(root, model, (model_roots or {}).get(model))
        files = model_files(directory)
        after_directory, after_publication = _model_directory(root, model, (model_roots or {}).get(model))
        if after_directory != directory or after_publication != publication:
            raise q.EvidenceUnavailable("managed publication or artifact stamps changed during hashing")
        row = {"model": model, "backend": instance["backend"],
               "tasks": sorted(instance["tasks"]), "files": files}
        if publication is not None:
            by_path = {item["path"]: item for item in files}
            for artifact in publication["artifacts"]:
                observed = by_path.get(artifact["path"])
                if (observed is None or observed["size"] != artifact["size"]
                        or artifact.get("sha256") is not None and observed["sha256"] != artifact["sha256"].lower()):
                    raise q.EvidenceUnavailable("managed receipt artifact digest/size differs")
            row["publication"] = {key: publication[key] for key in ("sha256", "version", "source")}
        models.append(row)
    status, after, _ = q.http(url, "/models/instances")
    if status != 200 or _inventory_key(after) != _inventory_key(inventory):
        raise q.EvidenceUnavailable("loaded models changed during artifact hashing")
    return {"version": 1, "models": sorted(models, key=lambda item: item["model"])}


def require_matching_artifacts(left, right):
    for identity in (left, right):
        artifacts = identity.get("model_artifacts")
        if (
            not artifacts
            or artifacts.get("version") != 1
            or not artifacts.get("models")
        ):
            raise q.EvidenceUnavailable(
                "controlled evidence lacks model artifact hashes; rerun qualification"
            )
        for model in artifacts["models"]:
            files = model.get("files", [])
            names = {item["path"] for item in files}
            if not names.intersection(METADATA) or not any(
                Path(name).suffix in WEIGHTS for name in names
            ):
                raise q.EvidenceUnavailable(
                    "controlled model evidence lacks metadata or weights"
                )
            for item in files:
                if (
                    item.get("size", -1) < 0
                    or len(item.get("sha256", "")) != 64
                    or any(c not in "0123456789abcdef" for c in item["sha256"])
                ):
                    raise q.EvidenceUnavailable("invalid model artifact hash evidence")
    if left["model_artifacts"] != right["model_artifacts"]:
        raise q.EvidenceUnavailable("controlled model artifacts differ")


def verify_artifacts(identity, args):
    current = {
        "model_artifacts": model_artifacts(
            identity["configuration"], args.url, Path(identity["working_directory"])
        )
    }
    require_matching_artifacts(identity, current)
