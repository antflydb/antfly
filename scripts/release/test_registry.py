#!/usr/bin/env python3
"""Failure-mode tests for release registry adapters."""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from registry.container import lookup_digest, promote_alias, verify_digest
from registry.model import LookupState, RegistryError

DIGEST = f"sha256:{'a' * 64}"


class FakeRunner:
    def __init__(self, responses: list[tuple[int, str, str]]) -> None:
        self.responses = list(responses)
        self.calls: list[list[str]] = []

    def __call__(self, args, **_kwargs):
        self.calls.append(list(args))
        code, stdout, stderr = self.responses.pop(0)
        return subprocess.CompletedProcess(args, code, stdout, stderr)


class ContainerRegistryTests(unittest.TestCase):
    def test_only_explicit_not_found_is_missing(self) -> None:
        missing = FakeRunner([(1, "", "MANIFEST_UNKNOWN: manifest unknown")])
        self.assertEqual(
            lookup_digest("registry/image:missing", missing).state,
            LookupState.MISSING,
        )

        for detail in ("unauthorized", "dial tcp: timeout", "too many requests"):
            with self.subTest(detail=detail), self.assertRaises(RegistryError):
                lookup_digest("registry/image:tag", FakeRunner([(1, "", detail)]))

    def test_alias_copy_uses_the_resolved_digest_and_is_verified(self) -> None:
        runner = FakeRunner(
            [
                (0, f"{DIGEST}\n", ""),
                (0, "", ""),
                (0, f"{DIGEST}\n", ""),
            ]
        )
        self.assertEqual(
            promote_alias("registry/source:tag", "registry/dest:tag", runner),
            DIGEST,
        )
        self.assertEqual(
            runner.calls[1],
            ["crane", "copy", f"registry/source@{DIGEST}", "registry/dest:tag"],
        )

    def test_alias_verification_rejects_registry_drift(self) -> None:
        different = f"sha256:{'b' * 64}"
        runner = FakeRunner(
            [(0, f"{DIGEST}\n", ""), (0, "", ""), (0, f"{different}\n", "")]
        )
        with self.assertRaisesRegex(RegistryError, "alias verification failed"):
            promote_alias("registry/source:tag", "registry/dest:tag", runner)

    def test_alias_is_always_verified_after_copy(self) -> None:
        runner = FakeRunner(
            [
                (0, f"{DIGEST}\n", ""),
                (0, "", ""),
                (0, f"{DIGEST}\n", ""),
            ]
        )
        self.assertEqual(
            promote_alias("registry/source:tag", "registry/dest:latest", runner),
            DIGEST,
        )

    def test_expected_digest_is_checked_without_copying(self) -> None:
        runner = FakeRunner([(0, f"{DIGEST}\n", "")])
        self.assertEqual(verify_digest("registry/image:tag", DIGEST, runner), DIGEST)
        self.assertFalse(any(call[1] == "copy" for call in runner.calls))


if __name__ == "__main__":
    unittest.main()
