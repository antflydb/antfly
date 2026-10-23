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

"""Reject incomplete or mis-targeted release packages."""

import io
import tarfile
import tempfile
import unittest
from pathlib import Path

from verify_native_archives import verify


class NativeArchivesTests(unittest.TestCase):
    def test_target_and_library_only_layout_are_verified(self):
        platform = {
            "archive_os": "Linux",
            "archive_arch": "arm64",
            "archive_suffix": "_gnu",
        }
        for machine, extra_cli, error in (
            (183, False, None),
            (62, False, "wrong native target"),
            (183, True, "CLI executable in library archive"),
        ):
            with (
                self.subTest(machine=machine, extra_cli=extra_cli),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                header = bytearray(20)
                header[:6] = b"\x7fELF\x02\x01"
                header[18:20] = machine.to_bytes(2, "little")
                names = [
                    "lib/libantfly.so",
                    "include/antfly.h",
                    "lib/pkgconfig/libantfly.pc",
                    "README.md",
                    "LICENSE",
                    "THIRD_PARTY_NOTICES.md",
                ]
                if extra_cli:
                    names.append("antfly-inference-worker")
                with tarfile.open(
                    root / "antfly-embedded_test_Linux_arm64_gnu.tar.gz", "w:gz"
                ) as archive:
                    for name in names:
                        member = tarfile.TarInfo("./" + name)
                        member.size = len(header)
                        member.mode = 0o755
                        archive.addfile(member, io.BytesIO(header))
                if error:
                    with self.assertRaisesRegex(ValueError, error):
                        verify(root, platform, ["embedded"])
                else:
                    verify(root, platform, ["embedded"])
