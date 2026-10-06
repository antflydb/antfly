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

"""Verify explicitly licensed embedded fonts and assets outside Apache source roots."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ASSETS = json.loads(
    Path(__file__).with_name("embedded_asset_licenses.json").read_text()
)
FONT_EXTENSIONS = {".ttf", ".ttc", ".otf", ".woff", ".woff2"}


def is_font(path: Path) -> bool:
    if path.suffix.lower() in FONT_EXTENSIONS:
        return True
    if path.is_file():
        with path.open("rb") as source:
            magic = source.read(12)
        return len(magic) == 12 and magic[:4] in {
            b"\x00\x01\x00\x00",
            b"OTTO",
            b"ttcf",
            b"wOFF",
            b"wOF2",
        }
    return False


def check_asset_records(root: Path = ROOT) -> list[str]:
    errors = []
    for name, record in ASSETS.items():
        path = (root / name).resolve()
        if not path.is_relative_to(root.resolve()) or not path.is_file():
            errors.append(f"missing or external licensed embedded asset: {name}")
            continue
        data = path.read_bytes()
        if (
            len(data) != record["size_bytes"]
            or hashlib.sha256(data).hexdigest() != record["sha256"]
        ):
            errors.append(f"embedded asset identity differs: {name}")
        if record["license"] != "Apache-2.0":
            errors.append(
                f"embedded asset must retain its reviewed Apache license: {name}"
            )
        if is_font(path):
            for key in ("notice", "license_text", "source"):
                if not record.get(key):
                    errors.append(f"missing font {key}: {name}")
            for key in ("notice", "license_text"):
                if record.get(key) and not (root / record[key]).is_file():
                    errors.append(f"missing font license file: {record[key]}")
    return errors


def check_embedded_asset(
    name: str, apache_source_scope: bool, root: Path = ROOT
) -> str | None:
    if name in ASSETS:
        return None
    if is_font(root / name):
        return f"unreviewed embedded font: {name}"
    if not apache_source_scope:
        return f"embedded asset outside Apache source scope: {name}"
    return None
