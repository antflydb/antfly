"""Explicit, isolated official PEFT export-loader profile. No import-time ML.

The training oracle remains on PEFT 0.17.1. This profile extracts only an exact
official wheel into a private temporary directory and uses normal import-path
selection; it never patches either package or changes the existing environment.
"""
from __future__ import annotations

import contextlib
import hashlib
import importlib
import importlib.metadata
import io
import os
from pathlib import Path, PurePosixPath
import platform
import stat
import sys
import tempfile
import unicodedata
import zipfile

import oracle

PROFILE = "peft-0.18.0-export-v1"
CONTRACT = Path(__file__).with_name("training_export_peft018.json")
MAX_WHEEL = 2 * 1024**2


def require(condition, message):
    if not condition:
        raise oracle.ContractError(message)


def pin(data):
    return {"size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def load_profile():
    require(0 < CONTRACT.stat().st_size <= 128 * 1024, "export loader profile byte limit")
    profile = oracle.read_json(CONTRACT)
    require(profile.get("version") == 1 and profile.get("scope") == "gliner25_training_export_runtime_profile/v1" and
            profile.get("profile") == PROFILE and profile.get("peft_version") == "0.18.0" and
            profile.get("upstream_commit") == oracle.UPSTREAM_COMMIT, "export loader profile identity differs")
    require(profile["original_oracle_runtime"] == oracle.load_manifest()["runtime"],
            "export profile no longer references the exact original oracle runtime")
    return profile


def wheel_bytes(path: Path, profile):
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= MAX_WHEEL, "invalid export loader wheel file")
        with os.fdopen(descriptor, "rb", closefd=False) as source:
            data = source.read(MAX_WHEEL + 1)
        after = os.fstat(descriptor)
        require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) ==
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns), "export loader wheel changed while reading")
    finally:
        os.close(descriptor)
    require(pin(data) == {key: profile["wheel"][key] for key in ("size_bytes", "sha256")}, "export loader wheel hash differs")
    return data


def stage_wheel(data: bytes, destination: Path, profile):
    """Extract only the closed, bounded package inventory from verified bytes."""
    expected = profile["wheel_files"]
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        entries = archive.infolist()
        require(len(entries) == len(expected) and len({entry.filename for entry in entries}) == len(entries) and
                set(archive.namelist()) == set(expected), "export wheel file inventory differs")
        require(sum(entry.file_size for entry in entries) <= profile["max_uncompressed_bytes"], "export wheel expanded byte limit")
        for entry in entries:
            name = PurePosixPath(entry.filename)
            require(not name.is_absolute() and ".." not in name.parts and "\\" not in entry.filename and
                    name.parts[0] in ("peft", "peft-0.18.0.dist-info") and not entry.is_dir() and
                    not stat.S_ISLNK(entry.external_attr >> 16) and
                    0 <= entry.file_size <= profile["max_file_bytes"], "unsafe export wheel entry")
            require(name.suffix not in (".pth", ".pyc") and "__pycache__" not in name.parts, "unapproved wheel import hook")
            raw = archive.read(entry)
            require(pin(raw) == expected[entry.filename], "export wheel entry hash differs: " + entry.filename)
            target = destination / name
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("xb") as output:
                output.write(raw)
    verify_overlay(destination, profile)


def verify_overlay(directory: Path, profile):
    actual = {}
    for path in directory.rglob("*"):
        require(not path.is_symlink(), "export loader overlay contains a symlink")
        if path.is_dir():
            continue
        require(path.is_file(), "export loader overlay contains a nonregular file")
        name = path.relative_to(directory).as_posix()
        require(name in profile["wheel_files"] and path.stat().st_size <= profile["max_file_bytes"], "unapproved export overlay file")
        actual[name] = pin(path.read_bytes())
    require(actual == profile["wheel_files"], "export loader overlay changed")


def verify_dependency_profile(profile):
    expected = profile["original_oracle_runtime"]
    packages = {**expected["packages"], **profile["additional_dependency_pins"], "peft": profile["peft_version"]}
    actual = {"python": platform.python_version(), "unicode": unicodedata.unidata_version,
              "packages": {name: importlib.metadata.version(name) for name in packages}}
    require(actual == {"python": expected["python"], "unicode": expected["unicode"], "packages": packages},
            "export compatibility dependency profile differs")
    # Validate the actual wheel's published dependency requirements too.
    from packaging.requirements import Requirement
    from packaging.specifiers import SpecifierSet
    require(actual["python"] in SpecifierSet(profile["metadata_requires_python"]), "PEFT Python requirement differs")
    distribution = importlib.metadata.distribution("peft")
    require(distribution.metadata.get_all("Requires-Dist") == profile["metadata_requires_dist"], "PEFT requirements metadata differs")
    for encoded in profile["metadata_requires_dist"]:
        requirement = Requirement(encoded)
        if requirement.marker and not requirement.marker.evaluate({"extra": ""}):
            continue
        require(importlib.metadata.version(requirement.name) in requirement.specifier,
                "unsatisfied PEFT requirement: " + encoded)
    return actual


def verify_imports(source: Path, overlay: Path, profile):
    imports = {}
    for name, module in tuple(sys.modules.items()):
        if name == "gliner2" or name.startswith("gliner2."):
            imports[name] = oracle.verify_import_source(module, source)
        if name == "peft" or name.startswith("peft."):
            filename = getattr(module, "__file__", None)
            require(filename is not None, "unverifiable PEFT module origin")
            path = Path(filename).resolve()
            require(path.is_relative_to(overlay.resolve()), "PEFT imported outside isolated wheel")
            relative = path.relative_to(overlay.resolve()).as_posix()
            require(relative in profile["wheel_files"] and pin(path.read_bytes()) == profile["wheel_files"][relative],
                    "PEFT imported source hash differs")
            imports[name] = relative
    return imports


@contextlib.contextmanager
def prepared_runtime(source: Path, output: Path, wheel: Path):
    require(not any(name in sys.modules for name in ("torch", "gliner2", "peft")), "export profile requires a fresh process")
    profile = load_profile()
    profile_sha = oracle.sha256_file(CONTRACT)
    helper_sha = oracle.sha256_file(Path(__file__))
    # Check the original installed runtime before any overlay is selected.
    oracle.verify_dependencies()
    source = source.expanduser().resolve()
    provenance = oracle.verify_upstream_checkout(source)
    data = wheel_bytes(wheel, profile)
    original_path = sys.path[:]
    sys.dont_write_bytecode = True
    for key in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "TOKENIZERS_PARALLELISM"):
        os.environ[key] = "false" if key == "TOKENIZERS_PARALLELISM" else "1"
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ.pop("USE_FLASHDEBERTA", None)
    try:
        with tempfile.TemporaryDirectory(prefix=".peft018-wheel-", dir=output) as temporary:
            overlay = Path(temporary)
            stage_wheel(data, overlay, profile)
            sys.path[:0] = [str(source), str(overlay)]
            importlib.invalidate_caches()
            provenance["runtime"] = verify_dependency_profile(profile)
            distribution = importlib.metadata.distribution("peft")
            require(Path(distribution.locate_file("peft/__init__.py")).resolve() == overlay / "peft/__init__.py",
                    "PEFT metadata selected an unverified distribution")
            import torch
            import gliner2
            import peft
            from gliner2 import AutoExtractor, BoundaryExtractor  # noqa: F401

            require(gliner2.__version__ == oracle.load_manifest()["upstream"]["package_version"] and
                    peft.__version__ == profile["peft_version"], "export runtime package version differs")
            provenance.update({"platform": {"system": platform.system(), "machine": platform.machine()},
                "device": "cpu", "dtype": "float32", "threads": 1, "export_loader_profile": PROFILE,
                "export_loader_contract_sha256": profile_sha, "peft_wheel": profile["wheel"],
                "export_loader_helper_sha256": helper_sha,
                "training_oracle_runtime_unchanged": True})
            torch.set_num_threads(1)
            torch.set_num_interop_threads(1)
            torch.use_deterministic_algorithms(True)
            torch.set_default_dtype(torch.float32)
            provenance["imports"] = verify_imports(source, overlay, profile)
            yield provenance, torch
            provenance["imports"] = verify_imports(source, overlay, profile)
            verify_overlay(overlay, profile)
            verify_dependency_profile(profile)
            require(oracle.sha256_file(CONTRACT) == profile_sha and oracle.sha256_file(Path(__file__)) == helper_sha and
                    wheel_bytes(wheel, profile) == data,
                    "export loader profile changed during runtime")
            oracle.verify_upstream_checkout(source)
    finally:
        sys.path[:] = original_path
        importlib.invalidate_caches()
