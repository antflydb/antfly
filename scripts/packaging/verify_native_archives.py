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

"""Verify release archive product contents and native target identities."""

from __future__ import annotations

import argparse
import json
import tarfile
from pathlib import Path


def verify(directory: Path, platform: dict, products: list[str]) -> None:
    archives = sorted(directory.glob("*.tar.gz"))
    if len(archives) != len(products):
        raise ValueError(
            f"expected {len(products)} product archives, found {len(archives)}"
        )
    darwin = platform["archive_os"] == "Darwin"
    expected_arch = platform["archive_arch"]
    suffix = (
        f"_{platform['archive_os']}_{expected_arch}{platform['archive_suffix']}.tar.gz"
    )
    for product in products:
        prefix = "antfly_" if product == "server" else f"antfly-{product}_"
        matching = [
            path
            for path in archives
            if path.name.startswith(prefix) and path.name.endswith(suffix)
        ]
        if len(matching) != 1:
            raise ValueError(f"missing or ambiguous {product} archive for {suffix}")
        path = matching[0]
        with tarfile.open(path) as archive:
            members = {
                member.name.removeprefix("./"): member
                for member in archive.getmembers()
            }
            library = "lib/libantfly.dylib" if darwin else "lib/libantfly.so"
            binaries = (
                ["antfly"]
                if product == "server"
                else ["antfly-lite", "antfly-inference", "antfly-inference-worker"]
            )
            for name in [
                *binaries,
                library,
                "include/antfly.h",
                "README.md",
                "LICENSE",
                "THIRD_PARTY_NOTICES.md",
            ]:
                member = members.get(name)
                if member is None or not member.isfile() or member.size == 0:
                    raise ValueError(f"{path.name}: missing or empty {name}")
                if name not in [*binaries, library]:
                    continue
                if name in binaries and not member.mode & 0o111:
                    raise ValueError(f"{path.name}: {name} is not executable")
                stream = archive.extractfile(member)
                assert stream is not None
                header = stream.read(20)
                if darwin:
                    valid = (
                        header[:4] == b"\xcf\xfa\xed\xfe"
                        and int.from_bytes(header[4:8], "little") == 0x100000C
                    )
                else:
                    machine = 183 if expected_arch == "arm64" else 62
                    valid = (
                        header[:6] == b"\x7fELF\x02\x01"
                        and int.from_bytes(header[18:20], "little") == machine
                    )
                if not valid:
                    raise ValueError(f"{path.name}: {name} has the wrong native target")
                if darwin and name == library and member.size >= 80 * 1024**2:
                    raise ValueError(
                        f"{path.name}: libantfly exceeds the 80MiB Darwin retained-code budget"
                    )
        print(f"verified {path.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-dir", type=Path, required=True)
    parser.add_argument("--platform", type=json.loads, required=True)
    parser.add_argument("--products", type=json.loads, required=True)
    args = parser.parse_args()
    verify(args.archive_dir, args.platform, args.products)


if __name__ == "__main__":
    main()
