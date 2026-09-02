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


def ensure_immutable(
    source: str, destination: str, runner: Runner = subprocess.run
) -> str:
    source_lookup = lookup_digest(source, runner)
    if source_lookup.state is not LookupState.PRESENT:
        raise RegistryError(f"immutable container source is missing: {source}")
    expected = source_lookup.digest
    assert expected is not None

    destination_lookup = lookup_digest(destination, runner)
    if destination_lookup.state is LookupState.PRESENT:
        if destination_lookup.digest != expected:
            raise RegistryError(
                f"immutable container tag differs: {destination}; "
                f"existing={destination_lookup.digest} new={expected}"
            )
        return expected

    _copy(source, destination, runner)
    actual = lookup_digest(destination, runner)
    if actual.state is not LookupState.PRESENT or actual.digest != expected:
        raise RegistryError(
            f"container tag verification failed for {destination}: expected {expected}"
        )
    return expected


def promote_alias(
    source: str, destination: str, runner: Runner = subprocess.run
) -> str:
    source_lookup = lookup_digest(source, runner)
    if source_lookup.state is not LookupState.PRESENT:
        raise RegistryError(f"container alias source is missing: {source}")
    expected = source_lookup.digest
    assert expected is not None
    _copy(source, destination, runner)
    actual = lookup_digest(destination, runner)
    if actual.state is not LookupState.PRESENT or actual.digest != expected:
        raise RegistryError(
            f"container alias verification failed for {destination}: expected {expected}"
        )
    return expected
