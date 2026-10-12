# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Bind independent captures to the complete reviewed upstream Python tree."""
import hashlib

SOURCE_SHA256 = "70f7b5084c98a420558be740b726de8ef86edb2882f93cba8af6946db914e6d4"


def verify_source(directory):
    digest = hashlib.sha256()
    for path in sorted((directory / "gliner2").rglob("*.py")):
        digest.update(str(path.relative_to(directory)).encode())
        digest.update(b"\0" + path.read_bytes() + b"\0")
    if digest.hexdigest() != SOURCE_SHA256:
        raise ValueError("upstream GLiNER2 source differs from the pinned revision")
    return SOURCE_SHA256
