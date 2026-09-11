#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

"""Track schema reads for the OpenAPI joiners' Make-compatible depfiles."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path

import yaml

_inputs: ContextVar[set[Path] | None] = ContextVar("openapi_inputs", default=None)


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    inputs = _inputs.get()
    if inputs is not None:
        inputs.add(path.absolute())
    if not isinstance(data, dict):
        raise RuntimeError(f"expected mapping at {path}")
    return data


def escape_path(path: Path) -> str:
    value = str(path)
    if "\n" in value or "\r" in value:
        raise ValueError("depfile paths cannot contain newlines")
    return (
        value.replace("\\", "\\\\")
        .replace("$", "$$")
        .replace(" ", "\\ ")
        .replace("\t", "\\\t")
        .replace("#", "\\#")
    )


@contextmanager
def record_dependencies(depfile: Path | None):
    inputs: set[Path] = set()
    token = _inputs.set(inputs if depfile is not None else None)
    try:
        yield
        if depfile is not None:
            # Zig consumes the prerequisites; the dummy target is intentionally
            # independent of the cache's temporary output-directory name.
            depfile.write_text(
                "openapi: " + " ".join(escape_path(p) for p in sorted(inputs)) + "\n",
                encoding="utf-8",
            )
    finally:
        _inputs.reset(token)
