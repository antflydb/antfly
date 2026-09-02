from __future__ import annotations

import re
import subprocess
from collections.abc import Callable, Sequence

from .model import Lookup, LookupState, RegistryError

Runner = Callable[..., subprocess.CompletedProcess[str]]
DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")
NOT_FOUND_PATTERNS = (
    "manifest_unknown",
    "name_unknown",
    "manifest unknown",
    "404 not found",
    "not_found",
)


def _run(args: Sequence[str], runner: Runner) -> subprocess.CompletedProcess[str]:
    return runner(args, capture_output=True, text=True, check=False)


def lookup_digest(ref: str, runner: Runner = subprocess.run) -> Lookup:
    result = _run(("crane", "digest", ref), runner)
    if result.returncode == 0:
        digest = result.stdout.strip()
        if not DIGEST_PATTERN.fullmatch(digest):
            raise RegistryError(
                f"container registry returned an invalid digest for {ref}"
            )
        return Lookup.present(digest)
    detail = f"{result.stdout}\n{result.stderr}".lower()
    if any(marker in detail for marker in NOT_FOUND_PATTERNS):
        return Lookup.missing()
    raise RegistryError(
        f"container registry lookup failed for {ref}: {result.stderr.strip() or result.stdout.strip()}"
    )


def _copy(source: str, destination: str, runner: Runner) -> None:
    result = _run(("crane", "copy", source, destination), runner)
    if result.returncode:
        raise RegistryError(
            f"container copy failed from {source} to {destination}: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )


def require_digest(ref: str, runner: Runner = subprocess.run) -> str:
    lookup = lookup_digest(ref, runner)
    if lookup.state is not LookupState.PRESENT:
        raise RegistryError(f"container image is missing: {ref}")
    assert lookup.digest is not None
    return lookup.digest


def optional_digest(ref: str, runner: Runner = subprocess.run) -> str | None:
    lookup = lookup_digest(ref, runner)
    return lookup.digest if lookup.state is LookupState.PRESENT else None


def digest_reference(ref: str, digest: str) -> str:
    """Return a repository reference pinned to an already-resolved digest."""
    repository = ref.split("@", 1)[0]
    last_slash = repository.rfind("/")
    last_colon = repository.rfind(":")
    if last_colon > last_slash:
        repository = repository[:last_colon]
    return f"{repository}@{digest}"


def verify_digest(ref: str, expected: str, runner: Runner = subprocess.run) -> str:
    if not DIGEST_PATTERN.fullmatch(expected):
        raise RegistryError(f"invalid expected container digest: {expected}")
    actual = require_digest(ref, runner)
    if actual != expected:
        raise RegistryError(
            f"container digest differs for {ref}: expected={expected} actual={actual}"
        )
    return actual


def promote_alias(
    source: str, destination: str, runner: Runner = subprocess.run
) -> str:
    expected = require_digest(source, runner)
    # Channel and retention tags are mutable aliases, never immutable records.
    # Resolve once, then copy the digest-pinned source so a concurrent source-tag
    # move cannot change the bytes promoted by this transaction.
    _copy(digest_reference(source, expected), destination, runner)
    actual = lookup_digest(destination, runner)
    if actual.state is not LookupState.PRESENT or actual.digest != expected:
        raise RegistryError(
            f"container alias verification failed for {destination}: expected {expected}"
        )
    return expected


def ensure_version(
    source: str, destination: str, runner: Runner = subprocess.run
) -> str:
    """Create a version tag once, or verify an identical prior publication."""
    expected = require_digest(source, runner)
    existing = optional_digest(destination, runner)
    if existing is not None:
        if existing != expected:
            raise RegistryError(
                f"immutable container version differs for {destination}: "
                f"expected={expected} actual={existing}"
            )
        return expected
    _copy(digest_reference(source, expected), destination, runner)
    return verify_digest(destination, expected, runner)
