// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");

/// Generated paths are resolved during the make phase in Zig 0.17. Pass the
/// explicit test library path through env rather than baking a cache path into
/// production binaries or resolving the installation prefix at configure time.
pub fn configureTest(b: *std.Build, run: *std.Build.Step.Run, bridge: ?std.Build.LazyPath) void {
    if (bridge) |path| {
        const arguments = run.argv.toOwnedSlice(b.allocator) catch @panic("OOM");
        run.addArg("env");
        run.addFileArg2(path, .{ .prefix = "ANTFLY_APPLE_BRIDGE_PATH=", .make_absolute = true });
        run.argv.appendSlice(b.allocator, arguments) catch @panic("OOM");
        run.has_side_effects = true;
    }
}

pub const Modules = struct {
    native: *std.Build.Module,
    readers: *std.Build.Module,
    generating: *std.Build.Module,
    transcribing: *std.Build.Module,
};

/// One capability switch for Vision, SpeechAnalyzer and Foundation Models.
/// Cross builds use the bridge built from the same source on a Mac runner.
pub fn configure(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, link_libc: bool, modules: Modules) ?*std.Build.Step.InstallFile {
    const enabled = b.option(bool, "apple-providers", "Enable Apple Vision OCR, transcription and generation (default on macOS; bundled ARM64 bridge)") orelse (target.result.os.tag == .macos and link_libc);
    const prebuilt = b.option([]const u8, "apple-bridge-path", "Path to a matching prebuilt libantfly-apple.dylib for macOS cross compilation");
    if (enabled and (target.result.os.tag != .macos or !link_libc))
        @panic("-Dapple-providers=true requires macOS and libc");
    if (!enabled and prebuilt != null)
        @panic("-Dapple-bridge-path requires Apple providers to be enabled");
    const options = b.addOptions();
    options.addOption(bool, "enabled", enabled);
    const module = options.createModule();
    modules.native.addImport("apple_native_options", module);
    modules.readers.addImport("apple_reader_options", module);
    modules.generating.addImport("antfly_apple_native", modules.native);
    modules.transcribing.addImport("antfly_apple_native", modules.native);
    if (!enabled) return null;
    modules.native.link_libc = true;
    @import("antfly_platform").addMacosSdkPaths(b, modules.native, target);
    modules.native.addCSourceFile(.{
        .file = b.path("lib/apple_native/src/loader.c"),
        .flags = &.{ "-std=c11", "-Wall", "-Wextra", "-Werror" },
    });
    @import("antfly_platform").addMacosSdkPaths(b, modules.readers, target);
    for ([_][]const u8{ "Foundation", "CoreGraphics", "ImageIO", "Vision" }) |framework|
        modules.readers.linkFramework(framework, .{});
    modules.readers.addCSourceFile(.{
        .file = b.path("lib/readers/src/apple_vision.m"),
        .flags = &.{ "-fobjc-arc", "-fblocks" },
    });
    var verification: ?*std.Build.Step.Run = null;
    const bundled = prebuilt == null and target.result.cpu.arch == .aarch64;
    if (bundled) {
        const check = b.addSystemCommand(&.{"python3"});
        check.addFileArg(b.path("../scripts/packaging/apple_bridge.py"));
        check.addArgs(&.{ "verify", "--directory" });
        check.addDirectoryArg(b.path("lib/apple_native/prebuilt/arm64"));
        check.addArgs(&.{ "--architecture", "arm64" });
        check.has_side_effects = true;
        verification = check;
    }
    const bridge: std.Build.LazyPath = if (bundled) b.path("lib/apple_native/prebuilt/arm64/libantfly-apple.dylib") else if (prebuilt) |path| (if (std.fs.path.isAbsolute(path)) std.Build.LazyPath{ .cwd_relative = path } else b.path(path)) else native: {
        if (b.graph.host.result.os.tag != .macos)
            @panic("macOS cross builds require -Dapple-bridge-path, or explicitly -Dapple-providers=false");
        const swift = b.addSystemCommand(&.{ "xcrun", "swiftc", "-parse-as-library", "-swift-version", "6", "-target", if (target.result.cpu.arch == .aarch64) "arm64-apple-macos26.0" else "x86_64-apple-macos26.0", "-module-cache-path" });
        swift.addDirectoryArg(std.Build.LazyPath.cache_root.path(b, "apple-swift-modules"));
        swift.addArg(switch (optimize) {
            .debug => "-Onone",
            .small => "-Osize",
            .safe, .fast => "-O",
        });
        swift.addArgs(&.{ "-emit-library", "-Xlinker", "-install_name", "-Xlinker", "@rpath/libantfly-apple.dylib", "-Xlinker", "-adhoc_codesign" });
        swift.addFileArg(b.path("lib/apple_native/src/bridge.swift"));
        swift.addArg("-o");
        break :native swift.addOutputFileArg("libantfly-apple.dylib");
    };
    const install = b.addInstallFileWithDir(bridge, .lib, "libantfly-apple.dylib");
    if (verification) |check| install.step.dependOn(&check.step);
    b.getInstallStep().dependOn(&install.step);
    b.step("apple-native-bridge", "Build or install the native Apple provider bridge").dependOn(&install.step);
    return install;
}
