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

"""Canonical headers for Apache generators that emit Unicode-derived data."""

from pathlib import Path


def unicode_source_header(generator: str) -> str:
    root = next(
        parent
        for parent in Path(generator).resolve().parents
        if (parent / "LICENSES/third-party/Unicode-V3.txt").is_file()
    )
    bodies = (
        (root / "scripts/license-header-apache.txt").read_text(),
        (root / "LICENSES/third-party/Unicode-V3.txt").read_text(),
    )
    return "".join(
        "\n".join("// " + line if line else "//" for line in body.rstrip().splitlines())
        + "\n\n"
        for body in bodies
    )
