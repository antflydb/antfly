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

"""Verify retained qualification helpers against their recorded byte identities."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REGISTRY = Path(__file__).with_name("frozen_qualification_helpers.json")


def frozen_pins(root: Path = ROOT):
    root = root.resolve()
    for manifest, definition in json.loads(REGISTRY.read_text()).items():
        manifest_path = (root / manifest).resolve()
        if not manifest_path.is_relative_to(root):
            raise ValueError(f"qualification manifest escapes repository: {manifest}")
        data = json.loads(manifest_path.read_text())
        for name, pin in data[definition["key"]].items():
            path = (root / definition["base"] / name).resolve()
            if not path.is_relative_to(root):
                raise ValueError(f"frozen helper escapes repository: {name}")
            if not path.is_relative_to(root / "zig/pkg/inference"):
                raise ValueError(
                    f"frozen helper is outside its Apache inference package: {name}"
                )
            yield path.relative_to(root).as_posix(), pin, manifest


def check_frozen_helpers(root: Path = ROOT) -> list[str]:
    errors = []
    try:
        pins = list(frozen_pins(root))
    except (OSError, ValueError, KeyError, TypeError) as error:
        return [f"invalid qualification provenance: {error}"]
    for name, expected, manifest in pins:
        path = root / name
        if not path.is_file():
            errors.append(f"missing frozen qualification source: {name}")
            continue
        data = path.read_bytes()
        actual = {"size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
        if actual != expected:
            errors.append(f"frozen qualification identity differs: {name} ({manifest})")
    package_license = root / "zig/pkg/inference/LICENSE"
    canonical_license = root / "LICENSES/Apache-2.0.txt"
    if (
        not package_license.is_file()
        or not canonical_license.is_file()
        or package_license.read_bytes() != canonical_license.read_bytes()
    ):
        errors.append("frozen inference tooling must retain its Apache package LICENSE")
    return sorted(set(errors))
