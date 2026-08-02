"""Shared release contracts for Zig GLiNER2 and its pinned Python oracle."""

from __future__ import annotations

import hashlib
import inspect
import stat
import subprocess
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


UPSTREAM_COMMIT = "8f3fc399bcc5a00749a62a1565e5c6529f04b574"
CANONICAL_NORMALIZATION = "unicode_nfc_collapsed_whitespace_casefold/v1"
CANONICAL_PYTHON_VERSION = (3, 12)
CANONICAL_UNICODE_VERSION = "15.0.0"
FINGERPRINT_ABSENT_MARKER = b"<absent>"


@dataclass(frozen=True)
class FingerprintEntry:
    relative_path: str
    optional: bool = False


MODEL_FINGERPRINT_ENTRIES = (
    FingerprintEntry("model.safetensors"),
    FingerprintEntry("config.json"),
    FingerprintEntry("encoder_config/config.json"),
    FingerprintEntry("tokenizer.json"),
    FingerprintEntry("tokenizer_config.json"),
    FingerprintEntry("special_tokens_map.json"),
    FingerprintEntry("added_tokens.json"),
    FingerprintEntry("spm.model", optional=True),
)


def clean_text(value: Any) -> str:
    """Return the display-preserving half of the v1 scoring normalizer."""
    if not isinstance(value, str):
        raise ValueError(f"expected string value, got {type(value).__name__}")
    return " ".join(unicodedata.normalize("NFC", value).split())


def canonical_text(value: Any) -> str:
    """Normalize an exact-match atom under the versioned release contract."""
    return clean_text(value).casefold()


def verify_canonical_python_runtime() -> dict[str, str]:
    """Require the Python/Unicode pair used to define normalization v1."""
    actual_python = sys.version_info[:2]
    actual_unicode = unicodedata.unidata_version
    if actual_python != CANONICAL_PYTHON_VERSION or actual_unicode != CANONICAL_UNICODE_VERSION:
        raise ValueError(
            "canonical GLiNER2 normalization requires Python "
            f"{CANONICAL_PYTHON_VERSION[0]}.{CANONICAL_PYTHON_VERSION[1]} / Unicode "
            f"{CANONICAL_UNICODE_VERSION}, found {actual_python[0]}.{actual_python[1]} / {actual_unicode}"
        )
    return {
        "python": f"{actual_python[0]}.{actual_python[1]}",
        "unicode": actual_unicode,
    }


def directory_fingerprint(directory: Path, entries: Iterable[FingerprintEntry]) -> str:
    """Mirror Zig's ordered, present/absent-framed SHA-256 fingerprint."""
    hasher = hashlib.sha256()
    for entry in entries:
        path = directory / entry.relative_path
        hasher.update(entry.relative_path.encode())
        try:
            path_stat = path.stat()
        except FileNotFoundError:
            if not entry.optional:
                raise ValueError(f"required fingerprint file is missing: {path}")
            hasher.update(b"\1")
            hasher.update(FINGERPRINT_ABSENT_MARKER)
            hasher.update(b"\0")
            continue
        except OSError as exc:
            raise ValueError(f"could not stat fingerprint path {path}: {exc}") from exc
        if not stat.S_ISREG(path_stat.st_mode):
            kind = "optional" if entry.optional else "required"
            raise ValueError(f"{kind} fingerprint path is not a regular file: {path}")
        hasher.update(b"\0")
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                hasher.update(chunk)
        hasher.update(b"\0")
    return f"sha256:{hasher.hexdigest()}"


def path_fingerprint(path: Path) -> str:
    """Fingerprint a file or an ordered directory of JSONL shards."""
    path = path.expanduser().resolve()
    if path.is_file():
        hasher = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                hasher.update(chunk)
        return f"sha256:{hasher.hexdigest()}"
    if path.is_dir():
        shards = sorted(path.glob("*.jsonl"))
        if not shards:
            raise ValueError(f"{path}: expected a file or directory containing JSONL files")
        return directory_fingerprint(
            path,
            tuple(FingerprintEntry(shard.name) for shard in shards),
        )
    raise ValueError(f"{path}: expected a file or directory containing JSONL files")


def verify_upstream_checkout(source: Path) -> dict[str, str]:
    """Fail closed unless *source* is a clean checkout of the pinned oracle."""
    source = source.expanduser().resolve()
    if not (source / "gliner2" / "__init__.py").is_file():
        raise ValueError(f"{source}: expected an upstream GLiNER2 checkout")
    head = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    commit = head.stdout.strip() if head.returncode == 0 else ""
    if commit != UPSTREAM_COMMIT:
        raise ValueError(f"upstream commit {commit or 'unknown'} does not match frozen {UPSTREAM_COMMIT}")
    dirty = subprocess.run(
        ["git", "-C", str(source), "status", "--porcelain=v1", "--untracked-files=all"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if dirty.returncode != 0 or dirty.stdout.strip():
        raise ValueError("upstream checkout must be clean")
    return {"commit": commit, "checkout": str(source)}


def verify_import_source(imported: Any, checkout: Path) -> str:
    """Prove an imported GLiNER2 object came from the verified checkout."""
    imported_from = Path(inspect.getfile(imported)).resolve()
    checkout = checkout.expanduser().resolve()
    if not imported_from.is_relative_to(checkout):
        raise ValueError(f"GLiNER2 imported from unpinned source: {imported_from}")
    return str(imported_from)
