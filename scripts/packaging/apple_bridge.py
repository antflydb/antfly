#!/usr/bin/env python3
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

"""Build the small Swift bridge on macOS and verify it before cross packaging."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import platform
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "zig/lib/apple_native/src/bridge.swift"
LIBRARY = "libantfly-apple.dylib"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify(directory, commit, architecture):
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Invalid Apple bridge source commit")
    metadata_path = directory / "apple-bridge.json"
    if metadata_path.is_symlink():
        raise ValueError("Apple bridge metadata must not be a symlink")
    metadata = json.loads(metadata_path.read_bytes())
    library = directory / LIBRARY
    if library.is_symlink() or set(p.name for p in directory.iterdir()) != {
        LIBRARY,
        "apple-bridge.json",
    }:
        raise ValueError("Apple bridge artifact has unexpected members or links")
    expected = {
        "schema_version": 1,
        "abi_version": 2,
        "source_commit": commit,
        "architecture": architecture,
        "source_sha256": digest(SOURCE),
        "sha256": digest(library),
    }
    if any(metadata.get(key) != value for key, value in expected.items()):
        raise ValueError("Apple bridge source, ABI, architecture, or digest differs")
    sdk = metadata.get("sdk_version", "")
    if not re.fullmatch(r"\d+(?:\.\d+){1,2}", sdk) or int(sdk.split(".")[0]) < 27:
        raise ValueError("Apple bridge requires the macOS 27 SDK or newer")
    header = library.read_bytes()[:8]
    cpu = {"arm64": 0x100000C, "x86_64": 0x1000007}[architecture]
    if (
        header[:4] != b"\xcf\xfa\xed\xfe"
        or int.from_bytes(header[4:8], "little") != cpu
    ):
        raise ValueError("Apple bridge has the wrong Mach-O architecture")
    return library


def build(directory, commit, architecture):
    if platform.system() != "Darwin":
        raise ValueError(
            "Build the Apple bridge on macOS; use verify on cross-build hosts"
        )
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Invalid Apple bridge source commit")
    actual = subprocess.check_output(
        ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
    ).strip()
    if actual != commit:
        raise ValueError("Apple bridge checkout differs from release source")
    sdk = subprocess.check_output(
        ["xcrun", "--sdk", "macosx", "--show-sdk-version"], text=True
    ).strip()
    if int(sdk.split(".")[0]) < 27:
        raise ValueError("Apple bridge compilation needs a macOS 27 SDK and Swift 6")
    directory.mkdir(parents=True, exist_ok=True)
    library = directory / LIBRARY
    subprocess.run(
        [
            "xcrun",
            "swiftc",
            "-parse-as-library",
            "-swift-version",
            "6",
            "-target",
            architecture + "-apple-macos26.0",
            "-module-cache-path",
            str(ROOT / "zig/.zig-cache/apple-swift-modules"),
            "-O",
            "-emit-library",
            "-Xlinker",
            "-install_name",
            "-Xlinker",
            "@rpath/" + LIBRARY,
            "-Xlinker",
            "-adhoc_codesign",
            str(SOURCE),
            "-o",
            str(library),
        ],
        check=True,
    )
    (directory / "apple-bridge.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "abi_version": 2,
                "source_commit": commit,
                "architecture": architecture,
                "source_sha256": digest(SOURCE),
                "sha256": digest(library),
                "sdk_version": sdk,
            },
            sort_keys=True,
            indent=2,
        )
        + "\n"
    )
    verify(directory, commit, architecture)
    subprocess.run(["codesign", "--verify", "--strict", str(library)], check=True)
    if (
        platform.machine() == architecture
        and int(platform.mac_ver()[0].split(".")[0]) >= 26
    ):
        bridge = ctypes.CDLL(str(library.resolve()))
        bridge.antfly_apple_bridge_abi_version.restype = ctypes.c_uint32
        if bridge.antfly_apple_bridge_abi_version() != 2:
            raise ValueError("Compiled Apple bridge exported an incompatible ABI")
        cancel_type = ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_void_p)
        output_type = ctypes.CFUNCTYPE(
            ctypes.c_int32, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t
        )
        responses = []
        cancel = cancel_type(lambda _: 0)

        def receive(_context, data, count):
            responses.append(ctypes.string_at(data, count))
            return 0

        output = output_type(receive)
        invoke = bridge.antfly_apple_invoke
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
        if invoke(3, b"{}", 2, None, 0, 65536, ctypes.c_void_p(1), cancel, output) != 0:
            raise ValueError("Compiled Apple bridge failed its availability operation")
        availability = json.loads(b"".join(responses))
        if not {
            "generation_status",
            "speech_available",
            "speech_installed_locales",
        }.issubset(availability):
            raise ValueError(
                "Compiled Apple bridge returned incomplete availability metadata"
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["build", "verify"])
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--architecture", choices=["arm64", "x86_64"], default="arm64")
    args = parser.parse_args()
    if args.command == "build":
        build(args.directory, args.source_commit, args.architecture)
    else:
        verify(args.directory, args.source_commit, args.architecture)


if __name__ == "__main__":
    main()
