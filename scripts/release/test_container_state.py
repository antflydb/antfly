#!/usr/bin/env python3
"""Tests for immutable per-release OCI identity records."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("release_container_state.py")
COMMIT = "1" * 40
LEDGER = "2" * 64
DIGEST = f"sha256:{'3' * 64}"


def load_module():
    spec = importlib.util.spec_from_file_location("release_container_state", SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load {SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class MemoryStore:
    def __init__(self, document: dict[str, object] | None = None) -> None:
        self.document = document
        self.creates = 0

    def load(self) -> dict[str, object] | None:
        return self.document

    def create(self, document: dict[str, object]) -> None:
        if self.document is not None:
            raise AssertionError("immutable record already exists")
        self.document = dict(document)
        self.creates += 1


class ConflictError(Exception):
    def __init__(self) -> None:
        self.response = {
            "Error": {"Code": "PreconditionFailed"},
            "ResponseMetadata": {"HTTPStatusCode": 412},
        }


class RacingStore(MemoryStore):
    def __init__(self, raced_document: dict[str, object]) -> None:
        super().__init__()
        self.raced_document = raced_document

    def create(self, _document: dict[str, object]) -> None:
        self.document = dict(self.raced_document)
        raise ConflictError


class ContainerIdentityTests(unittest.TestCase):
    def identity(self, module, digest: str = DIGEST) -> dict[str, object]:
        return module.container_identity("v1.2.3", "stable", COMMIT, LEDGER, digest)

    def test_new_identity_is_created_once_and_resumable(self) -> None:
        module = load_module()
        store = MemoryStore()
        identity = self.identity(module)
        module.bind_container(store, identity)
        module.bind_container(store, identity)
        self.assertEqual(store.document, identity)
        self.assertEqual(store.creates, 1)

    def test_existing_identity_cannot_change_digest(self) -> None:
        module = load_module()
        store = MemoryStore(self.identity(module))
        drifted = self.identity(module, f"sha256:{'4' * 64}")
        with self.assertRaisesRegex(SystemExit, "different contents"):
            module.bind_container(store, drifted)
        self.assertEqual(store.document, self.identity(module))

    def test_concurrent_identical_create_is_resumable(self) -> None:
        module = load_module()
        identity = self.identity(module)
        store = RacingStore(identity)
        module.bind_container(store, identity)
        self.assertEqual(store.document, identity)

    def test_concurrent_different_create_fails(self) -> None:
        module = load_module()
        identity = self.identity(module)
        raced = self.identity(module, f"sha256:{'4' * 64}")
        with self.assertRaisesRegex(SystemExit, "created concurrently"):
            module.bind_container(RacingStore(raced), identity)

    def test_resolve_requires_the_exact_release_identity(self) -> None:
        module = load_module()
        store = MemoryStore(self.identity(module))
        self.assertEqual(
            module.resolve_container(store, "v1.2.3", "stable", COMMIT, LEDGER),
            DIGEST,
        )
        with self.assertRaisesRegex(SystemExit, "different commit"):
            module.resolve_container(store, "v1.2.3", "stable", "5" * 40, LEDGER)

    def test_missing_identity_does_not_authorize_a_rebuild_digest(self) -> None:
        module = load_module()
        self.assertIsNone(
            module.resolve_container(MemoryStore(), "v1.2.3", "stable", COMMIT, LEDGER)
        )

    def test_missing_identity_still_requires_a_valid_requested_identity(self) -> None:
        module = load_module()
        with self.assertRaisesRegex(SystemExit, "invalid release commit"):
            module.resolve_container(
                MemoryStore(), "v1.2.3", "stable", "not-a-commit", LEDGER
            )

    def test_record_key_is_ledger_addressed(self) -> None:
        module = load_module()
        self.assertEqual(
            module.container_record_key(LEDGER),
            f"antfly/container-identities/{LEDGER}.json",
        )
        with self.assertRaisesRegex(SystemExit, "invalid release ledger"):
            module.container_record_key("bad")

    def test_malformed_stored_identity_fails_closed(self) -> None:
        module = load_module()
        malformed = self.identity(module)
        malformed["unexpected"] = True
        with self.assertRaisesRegex(SystemExit, "malformed"):
            module.resolve_container(
                MemoryStore(malformed), "v1.2.3", "stable", COMMIT, LEDGER
            )


if __name__ == "__main__":
    unittest.main()
