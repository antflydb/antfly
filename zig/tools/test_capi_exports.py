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

from __future__ import annotations

import argparse
import importlib.util
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import tempfile
import sys
import time
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location(
    "capi_exports", Path(__file__).with_name("capi_exports.py")
)
exports = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exports)


def run_fixture_build(command, cwd, timeout=300):
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env={**os.environ, "ZIG_LOCAL_CACHE_DIR": str(cwd / ".zig-cache")},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=os.name == "posix",
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except BaseException:
        # Stop the owned compiler group before fixture cleanup removes files
        # its children still use. Killing only the runner leaves those alive.
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        elif process.poll() is None:
            process.kill()
        process.communicate()
        raise
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


class ExportPolicyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.header = self.root / "antfly.h"
        self.header.write_text("""
/* Calling antfly_comment_only() is not an API declaration. */
// antfly_line_comment()
typedef void (*antfly_callback)(void *);
int antfly_first(void);
void antfly_second(
    int argument);
""")

    def test_manifest_is_exact_and_excludes_callbacks_and_comments(self):
        self.assertEqual(
            exports.public_symbols(self.header), {"antfly_first", "antfly_second"}
        )
        macho, elf = self.root / "exports", self.root / "map"
        exports.manifest(self.header, macho, "macho")
        exports.manifest(self.header, elf, "elf")
        self.assertEqual(macho.read_text(), "_antfly_first\n_antfly_second\n")
        self.assertIn("antfly_first;", elf.read_text())
        self.assertIn("local: *;", elf.read_text())
        self.assertNotIn("antfly_*", elf.read_text())

    def test_checker_rejects_internal_exports_even_with_antfly_prefix(self):
        for name in [
            "antfly_private",
            "termite_metal_buffer_alloc",
            "__dso_handle",
            "memcpy",
        ]:
            with (
                self.subTest(symbol=name),
                patch.object(
                    exports,
                    "exported_symbols",
                    return_value={"antfly_first", "antfly_second", name},
                ),
            ):
                with self.assertRaisesRegex(ValueError, "unexpected="):
                    exports.check(self.header, self.root / "lib", "macho")

    def test_checker_rejects_missing_public_functions(self):
        with patch.object(exports, "exported_symbols", return_value={"antfly_first"}):
            with self.assertRaisesRegex(ValueError, "antfly_second"):
                exports.check(self.header, self.root / "lib", "elf")

    def test_nm_normalization_preserves_internal_leaks(self):
        output = "_antfly_first T 100 0\n___dso_handle D 200 0\n"
        with patch.object(
            exports.subprocess,
            "run",
            return_value=subprocess.CompletedProcess([], 0, output, ""),
        ):
            self.assertEqual(
                exports.exported_symbols(self.root / "lib", "macho", "nm"),
                {"antfly_first", "__dso_handle"},
            )

    @unittest.skipUnless(os.name == "posix", "POSIX compiler process groups")
    def test_fixture_timeout_stops_compiler_children(self):
        # The child holds the output pipe open. A runner-only kill would wait
        # for its five-second alarm instead of enforcing the fixture deadline.
        child_pid = self.root / "compiler.pid"
        command = [
            sys.executable,
            "-c",
            """
import os, signal, time
from pathlib import Path
pid = os.fork()
if pid == 0:
    signal.alarm(5)
    print("compiler child ready", flush=True)
    time.sleep(10)
else:
    Path("compiler.pid").write_text(str(pid))
    time.sleep(10)
""",
        ]
        started = time.monotonic()
        with self.assertRaises(subprocess.TimeoutExpired) as caught:
            run_fixture_build(command, self.root, timeout=1)
        self.assertIn("compiler child ready", caught.exception.output.decode())
        self.assertLess(time.monotonic() - started, 4)
        state = subprocess.run(
            ["ps", "-o", "stat=", "-p", child_pid.read_text()],
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.assertTrue(not state or state.startswith("Z"), state)

    @unittest.skipUnless(shutil.which("zig"), "Zig compiler required")
    def test_production_elf_helper_links_debug_with_lld(self):
        zig_root = Path(__file__).resolve().parents[1]
        for relative in (
            "build_support/embedded/shared_library.zig",
            "tools/capi_exports.py",
        ):
            destination = self.root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(zig_root / relative, destination)
        header = self.root / "pkg/antfly-embedded/include/antfly.h"
        header.parent.mkdir(parents=True)
        shutil.copyfile(self.header, header)
        (self.root / "probe.zig").write_text("""
const std = @import("std");
extern fn antfly_private_submit(*const std.Io, *std.Io.Group, *std.atomic.Value(u32)) bool;
pub export fn antfly_first() c_int {
    var threaded: std.Io.Threaded = .init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    var group: std.Io.Group = .init;
    var count: std.atomic.Value(u32) = .init(0);
    if (!antfly_private_submit(&io, &group, &count)) return 0;
    group.await(io) catch return 0;
    return @intCast(count.load(.acquire));
}
export fn antfly_second(argument: c_int) void { _ = argument; }
export fn antfly_private_probe() u32 { return 2; }
""")
        (self.root / "cli.zig").write_text("""
const probe = @import("probe.zig");
pub fn main() !void {
    if (probe.antfly_first() != 42) return error.ExecutorCallbackFailed;
}
""")
        (self.root / "provider.zig").write_text("""
const std = @import("std");
fn setCount(count: *std.atomic.Value(u32)) void { count.store(42, .release); }
export fn antfly_private_submit(io: *const std.Io, group: *std.Io.Group, count: *std.atomic.Value(u32)) bool {
    group.concurrent(io.*, setCount, .{count}) catch return false;
    return true;
}
""")
        (self.root / "consumer.c").write_text(
            '#include "antfly.h"\n'
            "int main(void) { return antfly_first() == 42 ? 0 : 1; }\n"
        )
        (self.root / "build.zig").write_text("""
const std = @import("std");
const shared_library = @import("build_support/embedded/shared_library.zig");
pub fn build(b: *std.Build) void {
    const native_probe = b.option(bool, "native-executor-probe", "Execute the borrowed executor probe") orelse false;
    const target = if (native_probe) b.graph.host else
        b.resolveTargetQuery(.{ .cpu_arch = .x86_64, .os_tag = .linux, .abi = .musl });
    const module = b.createModule(.{
        .root_source_file = b.path("probe.zig"),
        .target = target,
        .optimize = .Debug,
        .link_libc = true,
        .pic = true,
    });
    const provider_module = b.createModule(.{
        .root_source_file = b.path("provider.zig"),
        .target = target,
        .optimize = .Debug,
        .link_libc = true,
        .pic = true,
    });
    const provider = shared_library.addProviderArchive(b, provider_module, "provider", 512 * 1024 * 1024);
    module.linkLibrary(provider);
    const library = shared_library.add(b, module);
    b.getInstallStep().dependOn(&b.addInstallFileWithDir(library.binary, .lib, "libantfly.so").step);
    const cli_module = b.createModule(.{
        .root_source_file = b.path("cli.zig"),
        .target = target,
        .optimize = .Debug,
        .link_libc = true,
    });
    cli_module.linkLibrary(provider);
    const cli = b.addExecutable(.{
        .name = "lite-executor-probe",
        .root_module = cli_module,
        .use_llvm = shared_library.llvmBackend(target.result),
    });
    b.installArtifact(cli);
    if (native_probe) {
        b.getInstallStep().dependOn(&b.addRunArtifact(cli).step);
        const consumer = b.createModule(.{ .target = target, .optimize = .Debug, .link_libc = true });
        consumer.addCSourceFile(.{ .file = b.path("consumer.c"), .flags = &.{"-std=c11"} });
        library.link(consumer);
        const executable = b.addExecutable(.{ .name = "executor-probe", .root_module = consumer });
        b.getInstallStep().dependOn(&b.addRunArtifact(executable).step);
    }
}
""")
        # Keep source and local outputs private, but reuse the caller's Zig
        # toolchain cache. Rebuilding the build runner and compiler runtimes
        # in a fresh global cache can exceed two minutes on cold CI runners.
        result = run_fixture_build([shutil.which("zig"), "build"], self.root)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        library = self.root / "zig-out/lib/libantfly.so"
        self.assertEqual(library.read_bytes()[:4], b"\x7fELF")
        nm = shutil.which("llvm-nm")
        if nm or platform.system() == "Linux":
            exports.check(header, library, "elf", nm)
        if platform.system() == "Linux":
            # A symbol-only link check misses mismatched backend conventions.
            # Submit provider callbacks to both library- and executable-owned executors.
            result = run_fixture_build(
                [shutil.which("zig"), "build", "-Dnative-executor-probe=true"],
                self.root,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    @unittest.skipUnless(
        platform.system() in {"Darwin", "Linux"}
        and (shutil.which("clang") or shutil.which("cc"))
        and shutil.which("xcrun" if platform.system() == "Darwin" else "ar"),
        "native compiler and archiver required",
    )
    def test_native_link_policy_and_executable_and_shared_consumers(self):
        self.header.write_text("""
#include <stdint.h>
typedef int antfly_error_code;
typedef struct { uint32_t size; } antfly_open_options;
#define ANTFLY_OK 0
uint32_t antfly_abi_version(void);
antfly_error_code antfly_open_options_init(antfly_open_options *);
""")
        source = self.root / "api.c"
        source.write_text("""
#include "antfly.h"
uint32_t antfly_abi_version(void) { return 1; }
int antfly_open_options_init(antfly_open_options *options) { options->size = sizeof(*options); return 0; }
int antfly_private(void) { return 7; }
int termite_metal_buffer_alloc(void) { return 8; }
""")
        object_file = self.root / "api.o"
        cc = shutil.which("clang") or shutil.which("cc")
        subprocess.run(
            [cc, "-fPIC", "-c", str(source), "-o", str(object_file)], check=True
        )
        macho = platform.system() == "Darwin"
        fmt = "macho" if macho else "elf"
        manifest = self.root / "exports"
        exports.manifest(self.header, manifest, fmt)
        library = self.root / ("libantfly.dylib" if macho else "libantfly.so")
        if macho:
            sdk = Path(
                subprocess.check_output(["xcrun", "--show-sdk-path"], text=True).strip()
            )
            archive = self.root / "api.a"
            # GNU ar can place Mach-O members at two-byte alignment. Apple's
            # linker requires eight-byte alignment, so use the SDK archiver.
            subprocess.run(
                ["xcrun", "ar", "rcs", str(archive), str(object_file)], check=True
            )
            exports.link_macho(
                argparse.Namespace(
                    sdk=sdk,
                    linker="/usr/bin/ld",
                    arch="arm64" if platform.machine() == "arm64" else "x86_64",
                    deployment="11.0",
                    exports=manifest,
                    output=library,
                    map=self.root / "library.map",
                    archive=archive,
                    link_args=[str(object_file)],
                )
            )
            self.assertIn(
                "_antfly_abi_version",
                (self.root / "library.map").read_text(errors="replace"),
            )
        else:
            subprocess.run(
                [
                    cc,
                    "-shared",
                    str(object_file),
                    "-Xlinker",
                    "--version-script",
                    "-Xlinker",
                    str(manifest),
                    "-o",
                    str(library),
                ],
                check=True,
            )
        exports.check(self.header, library, fmt)
        consumer = (
            Path(__file__).parents[1] / "pkg/antfly-embedded/tests/capi_link_consumer.c"
        )
        exports.consumer_test(
            self.header, library, consumer, self.root / "consumer", fmt
        )


if __name__ == "__main__":
    unittest.main()
