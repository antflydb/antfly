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
from unittest import mock
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

    def test_darwin_size_gate_uses_exact_bytes_and_reports_measured_size(self):
        platform = {
            "archive_os": "Darwin",
            "archive_arch": "arm64",
            "archive_suffix": "",
        }
        limit = 88 * 1024**2
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "antfly-embedded_test_Darwin_arm64.tar.gz"
            header = bytearray(20)
            header[:4] = b"\xcf\xfa\xed\xfe"
            header[4:8] = (0x100000C).to_bytes(4, "little")
            names = (
                "lib/libantfly.dylib",
                "include/antfly.h",
                "lib/pkgconfig/libantfly.pc",
                "README.md",
                "LICENSE",
                "THIRD_PARTY_NOTICES.md",
            )
            with tarfile.open(path, "w:gz") as archive:
                for name in names:
                    member = tarfile.TarInfo(name)
                    member.size = len(header)
                    member.mode = 0o755
                    archive.addfile(member, io.BytesIO(header))
            for size in (limit - 1, limit, limit + 1):
                with self.subTest(size=size), tarfile.open(path) as archive:
                    # Exercise the inventory boundary without an 88 MiB payload.
                    members = archive.getmembers()
                    members[0].size = size
                    with (
                        mock.patch(
                            "verify_native_archives.tarfile.open", return_value=archive
                        ),
                        mock.patch.object(
                            archive,
                            "extractfile",
                            side_effect=lambda _: io.BytesIO(header),
                        ),
                    ):
                        if size < limit:
                            verify(root, platform, ["embedded"])
                        else:
                            with self.assertRaisesRegex(
                                ValueError, f"{size} bytes.*88MiB"
                            ):
                                verify(root, platform, ["embedded"])
