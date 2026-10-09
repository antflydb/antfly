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

"""Generate and verify libantfly's export boundary from its public C header."""

from __future__ import annotations

import argparse
import json
import ctypes
from pathlib import Path
import re
import shutil
import subprocess


def public_symbols(header: Path) -> set[str]:
    source = re.sub(r"/\*.*?\*/|//[^\n]*", "", header.read_text(), flags=re.S)
    names = set(re.findall(r"\b(antfly_[A-Za-z0-9_]+)\s*\(", source))
    if not names:
        raise ValueError(f"no public C functions found in {header}")
    return names


def manifest(header: Path, output: Path, object_format: str) -> None:
    names = sorted(public_symbols(header))
    if object_format == "macho":
        text = "".join(f"_{name}\n" for name in names)
    else:
        text = (
            "{\n  global:\n"
            + "".join(f"    {name};\n" for name in names)
            + "  local: *;\n};\n"
        )
    output.write_text(text)


def exported_symbols(
    library: Path, object_format: str, nm: str | None = None
) -> set[str]:
    nm = nm or shutil.which("llvm-nm") or shutil.which("nm")
    if not nm:
        raise ValueError(
            "export verification requires nm (llvm-nm for Mach-O cross builds)"
        )
    if object_format == "macho":
        args = ["-P", "-g", "-U"]
    else:
        args = ["-P", "-D", "--defined-only", "--extern-only"]
    result = subprocess.run(
        [nm, *args, str(library)], check=True, text=True, capture_output=True
    )
    names = set()
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) >= 2:
            name = fields[0]
            if object_format == "macho" and name.startswith("_"):
                name = name[1:]
            names.add(name)
    return names


def check(
    header: Path, library: Path, object_format: str, nm: str | None = None
) -> None:
    expected = public_symbols(header)
    actual = exported_symbols(library, object_format, nm)
    extra, missing = actual - expected, expected - actual
    if extra or missing:
        raise ValueError(
            f"libantfly exports differ from antfly.h: unexpected={sorted(extra)}, missing={sorted(missing)}"
        )
    print(
        f"libantfly export boundary passed: {len(actual)} public functions, no internal exports"
    )


def bundled_macho_object(argument: str) -> bool:
    # Zig's static archive already contains direct object inputs, including
    # objects owned by its imported modules. Linking them again would define
    # their symbols twice. Archive/dylib inputs still belong on the final link.
    if argument.startswith("-"):
        return False
    try:
        with open(argument, "rb") as source:
            header = source.read(16)
    except OSError:
        return False
    if len(header) != 16:
        return False
    if header[:4] in (b"\xcf\xfa\xed\xfe", b"\xce\xfa\xed\xfe"):
        return int.from_bytes(header[12:16], "little") == 1
    if header[:4] in (b"\xfe\xed\xfa\xcf", b"\xfe\xed\xfa\xce"):
        return int.from_bytes(header[12:16], "big") == 1
    return False


def link_macho(args: argparse.Namespace) -> None:
    sdk_settings = json.loads((args.sdk / "SDKSettings.json").read_text())
    sdk_version = sdk_settings["Version"]
    link_args = [arg for arg in args.link_args if not bundled_macho_object(arg)]
    command = [
        args.linker,
        "-dylib",
        "-arch",
        args.arch,
        "-platform_version",
        "macos",
        args.deployment,
        sdk_version,
        "-syslibroot",
        str(args.sdk),
        "-install_name",
        "@rpath/libantfly.dylib",
        "-headerpad_max_install_names",
        "-dead_strip",
        "-adhoc_codesign",
        "-exported_symbols_list",
        str(args.exports),
        "-o",
        str(args.output),
        "-force_load",
        str(args.archive),
        *link_args,
        "-lSystem",
    ]
    # Each declared API is a link root, including APIs owned by a provider
    # archive that otherwise has no undefined reference from the C API object.
    for name in args.exports.read_text().splitlines():
        command.extend(["-u", name])
    subprocess.run(command, check=True)


def consumer_test(
    header: Path, library: Path, source: Path, work: Path, object_format: str
) -> None:
    cc = shutil.which("clang") or shutil.which("cc")
    if not cc:
        raise ValueError("consumer linkage regression requires clang or cc")
    work.mkdir(parents=True, exist_ok=True)
    object_file = work / "consumer.o"
    shared_object = work / "consumer-shared.o"
    common = [
        cc,
        "-std=c11",
        "-Wall",
        "-Wextra",
        "-Werror",
        "-fPIC",
        "-I",
        str(header.parent),
    ]
    subprocess.run([*common, "-c", str(source), "-o", str(object_file)], check=True)
    subprocess.run(
        [*common, "-DCONSUMER_SHARED", "-c", str(source), "-o", str(shared_object)],
        check=True,
    )
    executable = work / "consumer"
    shared = work / ("consumer.dylib" if object_format == "macho" else "consumer.so")
    # Put the dependency first: a leaked __dso_handle must not bind a later
    # consumer object's direct relocation to libantfly's copy.
    linking = [cc, "-Xlinker", "-rpath", "-Xlinker", str(library.parent)]
    inputs = lambda obj: (
        [str(library), str(obj)]
        if object_format == "macho"
        else [str(obj), str(library)]
    )
    subprocess.run([*linking, *inputs(object_file), "-o", str(executable)], check=True)
    subprocess.run(
        [
            *linking,
            "-dynamiclib" if object_format == "macho" else "-shared",
            *inputs(shared_object),
            "-o",
            str(shared),
        ],
        check=True,
    )
    subprocess.run([str(executable)], check=True)
    consumer = ctypes.CDLL(str(shared))
    consumer.consumer_check.restype = ctypes.c_int
    if consumer.consumer_check() != 0:
        raise ValueError("shared consumer check failed")
    print("libantfly executable and shared-library consumer linkage passed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser("manifest")
    generate.add_argument("--header", type=Path, required=True)
    generate.add_argument("--output", type=Path, required=True)
    generate.add_argument("--format", choices=["macho", "elf"], required=True)
    verify = commands.add_parser("check")
    verify.add_argument("--header", type=Path, required=True)
    verify.add_argument("--library", type=Path, required=True)
    verify.add_argument("--format", choices=["macho", "elf"], required=True)
    verify.add_argument("--nm")
    consumer = commands.add_parser("consumer-test")
    consumer.add_argument("--header", type=Path, required=True)
    consumer.add_argument("--library", type=Path, required=True)
    consumer.add_argument("--source", type=Path, required=True)
    consumer.add_argument("--work", type=Path, required=True)
    consumer.add_argument("--format", choices=["macho", "elf"], required=True)
    link = commands.add_parser("link-macho")
    link.add_argument("--linker", required=True)
    link.add_argument("--sdk", type=Path, required=True)
    link.add_argument("--deployment", required=True)
    link.add_argument("--arch", choices=["arm64", "x86_64"], required=True)
    link.add_argument("--exports", type=Path, required=True)
    link.add_argument("--archive", type=Path, required=True)
    link.add_argument("--output", type=Path, required=True)
    link.add_argument("link_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        if args.command == "manifest":
            manifest(args.header, args.output, args.format)
        elif args.command == "check":
            check(args.header, args.library, args.format, args.nm)
        elif args.command == "consumer-test":
            consumer_test(
                args.header, args.library, args.source, args.work, args.format
            )
        else:
            if args.link_args[:1] == ["--"]:
                args.link_args = args.link_args[1:]
            link_macho(args)
    except (ValueError, OSError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
