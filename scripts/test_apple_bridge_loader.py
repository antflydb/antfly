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

"""Exercise the production loader in isolated processes and relocated layouts."""

import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "zig/lib/apple_native/src"
OS_MOCK = r"""
#include <stdlib.h>
#include <string.h>
#include <sys/sysctl.h>
int sysctlbyname(const char *name, void *out, size_t *size, void *newval, size_t newlen) {
    (void)newval; (void)newlen;
    if (strcmp(name, "kern.osproductversion")) return -1;
    const char *version = getenv("ANTFLY_TEST_OS_VERSION");
    if (!version) version = "27.0.1";
    size_t length = strlen(version) + 1;
    if (*size < length) return -1;
    memcpy(out, version, length); *size = length; return 0;
}
"""
DRIVER = r"""
#include "loader.h"
#include <dlfcn.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
static antfly_apple_invoke_fn invoke;
static int expected;
static int cancel(void *ctx) { (void)ctx; return 0; }
static int output(void *ctx, const uint8_t *bytes, size_t count) {
    (void)ctx; (void)bytes; (void)count; return 0;
}
static void *run(void *ctx) {
    for (int i = 0; i < 8; ++i)
        if (invoke(3, (const uint8_t *)"{}", 2, NULL, 0, 4096, ctx, cancel, output) != expected)
            return (void *)1;
    return NULL;
}
int main(int argc, char **argv) {
    if (argc < 2) return 2;
    expected = atoi(argv[1]);
#ifdef HOSTED
    if (argc < 3) return 2;
    void *handle = dlopen(argv[2], RTLD_NOW | RTLD_LOCAL);
    if (!handle) return 3;
    invoke = (antfly_apple_invoke_fn)dlsym(handle, "antfly_apple_loader_invoke");
    if (!invoke) return 4;
#else
    if (argc > 2 && !strcmp(argv[2], "preflight"))
        return antfly_apple_loader_available() == expected ? 0 : 7;
    invoke = antfly_apple_loader_invoke;
#endif
    for (int round = 0; round < 2; ++round) {
        pthread_t threads[32];
        for (int i = 0; i < 32; ++i) if (pthread_create(&threads[i], NULL, run, NULL)) return 5;
        for (int i = 0; i < 32; ++i) {
            void *result;
            if (pthread_join(threads[i], &result) || result) return 6;
        }
        // Change discovery after first use: both success and failure are cached.
        const char *second = getenv("ANTFLY_TEST_SECOND_PATH");
        setenv("ANTFLY_APPLE_BRIDGE_PATH", second ? second : "/nonexistent/bridge.dylib", 1);
    }
    return 0;
}
"""
FIXTURE = r"""
#include "loader.h"
#include <stdatomic.h>
#include <stdio.h>
static atomic_int abi_calls;
__attribute__((constructor)) static void loaded(void) { fputs("bridge-loaded\n", stderr); }
uint32_t antfly_apple_bridge_abi_version(void) {
    atomic_fetch_add(&abi_calls, 1); return ABI_VERSION;
}
#ifndef MISSING_INVOKE
int antfly_apple_invoke(int operation, const uint8_t *json, size_t length,
    const uint8_t *audio, size_t audio_length, size_t limit, void *ctx,
    antfly_apple_cancel cancel, antfly_apple_output output) {
    (void)operation; (void)json; (void)length; (void)audio; (void)audio_length;
    (void)limit; (void)ctx; (void)cancel; (void)output;
    return atomic_load(&abi_calls) == 1 ? 0 : 13;
}
#endif
"""


@unittest.skipUnless(platform.system() == "Darwin", "macOS loader tests")
class LoaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="antfly-apple-loader-")
        cls.directory = Path(cls.temp.name)
        cls.compiler = [
            "xcrun",
            "clang",
            "-std=c11",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-target",
            f"{platform.machine()}-apple-macos15.0",
            "-I",
            str(SOURCE),
        ]
        for name, contents in [
            ("os.c", OS_MOCK),
            ("driver.c", DRIVER),
            ("fixture.c", FIXTURE),
        ]:
            (cls.directory / name).write_text(contents)
        cls.driver = cls.directory / "driver"
        cls.compile(
            [SOURCE / "loader.c", cls.directory / "os.c", cls.directory / "driver.c"],
            cls.driver,
        )
        cls.host = cls.directory / "host"
        cls.compile([cls.directory / "driver.c"], cls.host, ["-DHOSTED"])
        cls.library = cls.directory / "libloader.dylib"
        cls.compile(
            [SOURCE / "loader.c", cls.directory / "os.c"], cls.library, ["-dynamiclib"]
        )
        cls.fixtures = {}
        for name, flags in [
            ("valid", ["-DABI_VERSION=1"]),
            ("wrong-abi", ["-DABI_VERSION=2"]),
            ("missing-symbol", ["-DABI_VERSION=1", "-DMISSING_INVOKE"]),
        ]:
            path = cls.directory / f"{name}.dylib"
            cls.compile([cls.directory / "fixture.c"], path, ["-dynamiclib", *flags])
            cls.fixtures[name] = path

    @classmethod
    def compile(cls, sources, output, flags=()):
        subprocess.run(
            [*cls.compiler, *flags, *map(str, sources), "-o", str(output)],
            check=True,
            capture_output=True,
            text=True,
        )

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def execute(
        self,
        status,
        *,
        executable=None,
        owner=None,
        override=None,
        version="27.0.1",
        preflight=False,
    ):
        env = dict(os.environ)
        env.pop("ANTFLY_APPLE_BRIDGE_PATH", None)
        env.update(
            ANTFLY_TEST_OS_VERSION=version,
            ANTFLY_TEST_SECOND_PATH=str(self.fixtures["valid"]),
        )
        if override is not None:
            env["ANTFLY_APPLE_BRIDGE_PATH"] = str(override)
        command = [str(executable or self.driver), str(status)]
        if preflight:
            command.append("preflight")
        elif owner:
            command.append(str(owner))
        result = subprocess.run(
            command, env=env, cwd="/", capture_output=True, text=True, timeout=30
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def test_concurrent_load_and_cached_dispatch(self):
        result = self.execute(0, override=self.fixtures["valid"])
        self.assertEqual(result.stderr.count("bridge-loaded"), 1)

    def test_older_os_never_loads_library(self):
        for version in ["15.6.1", "25.0", "invalid", ""]:
            with self.subTest(version=version):
                result = self.execute(
                    2, override=self.fixtures["valid"], version=version
                )
                self.assertNotIn("bridge-loaded", result.stderr)

    def test_os_preflight_does_not_load_swift(self):
        for version, status in [("15.6.1", 2), ("26.0", 0), ("27.0.1", 0)]:
            with self.subTest(version=version):
                result = self.execute(
                    status,
                    version=version,
                    override=self.fixtures["valid"],
                    preflight=True,
                )
                self.assertNotIn("bridge-loaded", result.stderr)

    def test_missing_library_is_cached(self):
        self.execute(12, override=self.directory / "missing.dylib")

    def test_relative_override_is_rejected(self):
        self.execute(12, override="valid.dylib")

    def test_abi_and_symbol_mismatch(self):
        for name in ["wrong-abi", "missing-symbol"]:
            with self.subTest(name=name):
                self.execute(15, override=self.fixtures[name])

    def test_relocated_cli_layouts(self):
        for name, binary_dir, library_dir in [
            ("installed", "bin", "lib"),
            ("archive", "", "lib"),
            ("adjacent", "bin", "bin"),
        ]:
            with self.subTest(name=name):
                root = self.directory / name
                (root / binary_dir).mkdir(parents=True, exist_ok=True)
                (root / library_dir).mkdir(parents=True, exist_ok=True)
                executable = root / binary_dir / "antfly"
                shutil.copy2(self.driver, executable)
                shutil.copy2(
                    self.fixtures["valid"], root / library_dir / "libantfly-apple.dylib"
                )
                self.execute(0, executable=executable)

    def test_lite_uses_library_owner_not_host_location(self):
        root = self.directory / "lite" / "lib"
        root.mkdir(parents=True)
        owner = root / "libantfly.dylib"
        shutil.copy2(self.library, owner)
        shutil.copy2(self.fixtures["valid"], root / "libantfly-apple.dylib")
        self.execute(0, executable=self.host, owner=owner)


if __name__ == "__main__":
    unittest.main()
