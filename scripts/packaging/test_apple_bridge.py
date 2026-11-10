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

import ctypes
import json
import os
import platform
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import apple_bridge as bridge


class AppleBridgeArtifactTests(unittest.TestCase):
    @unittest.skipUnless(
        platform.system() == "Darwin"
        and platform.machine() == "arm64"
        and int(platform.mac_ver()[0].split(".")[0]) >= 26,
        "native ARM64 Apple bridge requires macOS 26 or later",
    )
    def test_invalid_generation_budgets_fail_before_model_access(self):
        library = bridge.verify(bridge.SOURCE.parent.parent / "prebuilt/arm64", "arm64")
        native = ctypes.CDLL(str(library.resolve()))
        cancel_type = ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_void_p)
        output_type = ctypes.CFUNCTYPE(
            ctypes.c_int32, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t
        )
        cancel = cancel_type(lambda _: 0)
        responses = []

        def receive(_context, data, count):
            responses.append(ctypes.string_at(data, count))
            return 0

        output = output_type(receive)
        invoke = native.antfly_apple_invoke
        invoke.argtypes = [
            ctypes.c_int32,
            ctypes.c_char_p,
            ctypes.c_size_t,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_size_t,
            ctypes.c_void_p,
            cancel_type,
            output_type,
        ]
        invoke.restype = ctypes.c_int32
        for budget in (0, -1, 2**31, 2**60, 2**63 - 1, True, "1024", 1.5, None):
            with self.subTest(budget=budget):
                request = json.dumps(
                    {
                        "messages": [{"role": "user", "text": "Hello"}],
                        "max_tokens": budget,
                        "require_completion": True,
                    }
                ).encode()
                self.assertEqual(
                    invoke(
                        1,
                        request,
                        len(request),
                        None,
                        0,
                        65536,
                        ctypes.c_void_p(1),
                        cancel,
                        output,
                    ),
                    1,
                )
                self.assertEqual(responses, [])
        # Invalid requests release the serialized bridge gate and leave the
        # process usable, even when Apple Intelligence has no installed model.
        self.assertEqual(
            invoke(3, b"{}", 2, None, 0, 65536, ctypes.c_void_p(1), cancel, output), 0
        )
        self.assertIn("generation_status", json.loads(b"".join(responses)))

    def test_parallel_generate_checks_only_after_bridge_build(self):
        makefile = (bridge.ROOT / "Makefile").read_text()
        generate = next(
            line for line in makefile.splitlines() if line.startswith("generate:")
        )
        targets = makefile[
            makefile.index("apple-bridge-check:") : makefile.index(
                "graph-identifier-generate:"
            )
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runner = root / "runner.py"
            runner.write_text(
                "import pathlib,sys,time\n"
                "done=pathlib.Path(__file__).with_name('built')\n"
                "if 'build' in sys.argv:\n"
                " time.sleep(0.2);done.touch()\n"
                "else:\n"
                " assert done.exists(), 'verification raced regeneration'\n"
            )
            uname = root / "uname"
            uname.write_text(
                '#!/bin/sh\nif [ "$1" = -s ]; then echo Darwin; else echo arm64; fi\n'
            )
            uname.chmod(0o755)
            fixture = root / "Makefile"
            fixture.write_text(
                "SCRIPTS_PY := python3 "
                + str(runner)
                + "\n"
                + generate
                + "\n\t@:\n"
                + "graph-identifier-generate provider-matrix-generate:\n\t@:\n"
                + targets
            )
            subprocess.run(
                ["make", "-f", str(fixture), "-j4", "generate"],
                env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"]},
                check=True,
                capture_output=True,
                text=True,
            )

    def test_committed_bridge_matches_its_source(self):
        bridge.verify(bridge.SOURCE.parent.parent / "prebuilt/arm64", "arm64")

    def fixture(self, root):
        binary = root / bridge.LIBRARY
        binary.write_bytes(
            b"\xcf\xfa\xed\xfe" + (0x100000C).to_bytes(4, "little") + b"fixture"
        )
        document = {
            "schema_version": 1,
            "abi_version": 2,
            "source_commit": "a" * 40,
            "architecture": "arm64",
            "source_sha256": bridge.digest(bridge.SOURCE),
            "sha256": bridge.digest(binary),
            "sdk_version": "27.0",
        }
        (root / "apple-bridge.json").write_text(json.dumps(document))
        return document

    def test_source_bound_artifact_verifies_without_executing_mac_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            with patch.object(
                bridge.subprocess, "run", side_effect=AssertionError("execution")
            ):
                self.assertEqual(bridge.verify(root, "arm64"), root / bridge.LIBRARY)

    def test_mismatched_source_commit_hash_architecture_abi_or_sdk_is_rejected(self):
        for field, value in [
            ("source_commit", "invalid"),
            ("source_sha256", "0" * 64),
            ("sha256", "0" * 64),
            ("architecture", "x86_64"),
            ("abi_version", 1),
            ("sdk_version", "26.1"),
        ]:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                document = self.fixture(root)
                document[field] = value
                (root / "apple-bridge.json").write_text(json.dumps(document))
                with self.assertRaises(ValueError):
                    bridge.verify(root, "arm64")

    def test_tampered_binary_extra_member_and_symlink_are_rejected(self):
        for damage in ("bytes", "extra", "link"):
            with (
                self.subTest(damage=damage),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                self.fixture(root)
                if damage == "bytes":
                    (root / bridge.LIBRARY).write_bytes(b"tampered")
                elif damage == "extra":
                    (root / "unexpected").write_bytes(b"payload")
                else:
                    binary = root / bridge.LIBRARY
                    binary.unlink()
                    binary.symlink_to(bridge.SOURCE)
                with self.assertRaises(ValueError):
                    bridge.verify(root, "arm64")

    def test_unrelated_release_commit_does_not_require_a_rebuild(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = self.fixture(root)
            document["source_commit"] = "b" * 40
            (root / "apple-bridge.json").write_text(json.dumps(document))
            self.assertEqual(bridge.verify(root, "arm64"), root / bridge.LIBRARY)

    def test_macho_architecture_is_checked_independently_of_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            document = self.fixture(root)
            binary = root / bridge.LIBRARY
            binary.write_bytes(b"\xcf\xfa\xed\xfe" + (0x1000007).to_bytes(4, "little"))
            document["sha256"] = bridge.digest(binary)
            (root / "apple-bridge.json").write_text(json.dumps(document))
            with self.assertRaisesRegex(ValueError, "Mach-O"):
                bridge.verify(root, "arm64")
