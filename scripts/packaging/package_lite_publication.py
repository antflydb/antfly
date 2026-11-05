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

"""Build the POSIX publication worker alongside the matching native binary."""

import argparse
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


def package(output):
    scripts = Path(__file__).resolve().parents[1]
    entries = {"__main__.py": (scripts / "antfly-publication").read_bytes()}
    for file in (scripts / "lite_publication").iterdir():
        if file.suffix in (".py", ".json") and not file.name.startswith("test_"):
            entries["lite_publication/" + file.name] = file.read_bytes()
    with output.open("wb") as stream:
        stream.write(b"#!/usr/bin/env python3\n")
        with ZipFile(stream, "w") as archive:
            for name, contents in sorted(entries.items()):
                archive.writestr(ZipInfo(name), contents, compress_type=ZIP_DEFLATED)
    output.chmod(0o755)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    package(parser.parse_args().output)
