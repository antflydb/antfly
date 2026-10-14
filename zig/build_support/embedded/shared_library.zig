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

//! Final shared-library exports belong to the public header, never to linked archives.
const std = @import("std");

/// Executor owners and provider archives must share the internal Zig callback ABI.
pub fn llvmBackend(target: std.Target) ?bool {
    return if (target.ofmt == .elf) true else null;
}

pub fn inferenceCompileMaxRss(os: std.Target.Os.Tag, shared: bool, measured_darwin: bool) u64 {
    const gib = 1024 * 1024 * 1024;
    if (os == .macos and shared) return (if (measured_darwin) @as(u64, 12) else 20) * gib;
    return (if (os == .macos and measured_darwin) @as(u64, 8) else 12) * gib;
}

test "qualified inference core keeps peak margin and fits beside API compilation" {
    const gib = 1024 * 1024 * 1024;
    const claim = inferenceCompileMaxRss(.macos, true, true);
    // Fresh merged-main MaxRSS 9G upper bound, plus at least 25%.
    try std.testing.expect(claim >= 9 * gib + 9 * gib / 4);
    try std.testing.expect(claim + 14 * gib <= 28 * gib);
    try std.testing.expect(inferenceCompileMaxRss(.macos, true, false) + 14 * gib > 28 * gib);
    try std.testing.expectEqual(12 * gib, inferenceCompileMaxRss(.linux, true, false));
}

/// Provider archives borrow std.Io executors from the C API and Lite CLI. Internal Zig
/// callbacks must use the same backend calling convention as their owner.
pub fn addProviderArchive(b: *std.Build, module: *std.Build.Module, name: []const u8, max_rss: u64) *std.Build.Step.Compile {
    return b.addLibrary(.{
        .name = name,
        .linkage = .static,
        .root_module = module,
        .use_llvm = llvmBackend(module.resolved_target.?.result),
        .max_rss = max_rss,
    });
}

pub const Library = struct {
    binary: std.Build.LazyPath,
    compile: ?*std.Build.Step.Compile,
    install: *std.Build.Step,
    check: *std.Build.Step.Run,

    pub fn link(self: Library, module: *std.Build.Module) void {
        if (self.compile) |compile| module.linkLibrary(compile) else {
            module.addObjectFile(self.binary);
            module.addRPath(self.binary.dirname());
        }
    }
};

pub fn add(b: *std.Build, module: *std.Build.Module, providers: []const *std.Build.Step.Compile) Library {
    return addWithStorageOwner(b, module, providers, null);
}

pub fn addWithStorageOwner(b: *std.Build, module: *std.Build.Module, providers: []const *std.Build.Step.Compile, storage_owner: ?*std.Build.Step.Compile) Library {
    const target = module.resolved_target.?.result;
    const macho = target.os.tag == .macos;
    const elf = target.ofmt == .elf;
    // Mach-O archives compile without provider inputs. Only the final link
    // needs their emitted code, so LLVM owners can run independently.
    const link_module = if (storage_owner != null and !macho) b.createModule(.{ .target = module.resolved_target, .optimize = module.optimize, .pic = true, .link_libc = module.link_libc }) else module;
    if (!macho) {
        if (storage_owner) |owner| link_module.linkLibrary(owner);
        for (providers) |provider| link_module.linkLibrary(provider);
    }
    const header = b.path("pkg/antfly-embedded/include/antfly.h");
    var exports: ?std.Build.LazyPath = null;
    if (macho or elf) {
        const generate = command(b, "manifest");
        generate.addArgs(&.{ "--format", if (macho) "macho" else "elf", "--header" });
        generate.addFileArg(header);
        generate.addArg("--output");
        exports = generate.addOutputFileArg(if (macho) "antfly.exports" else "antfly.map");
    }
    const compile = if (macho and storage_owner != null) storage_owner.? else b.addLibrary(.{
        .linkage = if (macho) .static else .dynamic,
        .name = if (macho) "antfly-capi-objects" else "antfly",
        .root_module = link_module,
        // ELF version scripts must also hide compiler-provided Debug UBSan
        // symbols. Use the linker that applies the final export boundary to
        // every linked object, including those runtime objects.
        .use_lld = if (elf) true else null,
        // Zig's Debug self-hosted backend cannot link through LLD. Keep the
        // code generator compatible with the required ELF export linker.
        .use_llvm = llvmBackend(target),
        // Mach-O ReleaseSafe codegen measured 12.92 GB; match the C API
        // test estimate so fresh builds can schedule the complete archive.
        .max_rss = @as(usize, if (macho) 14 else 12) * 1024 * 1024 * 1024,
    });
    const binary = if (macho) blk: {
        module.pic = true;
        compile.bundle_compiler_rt = true;
        // The external Mach-O linker cannot add Zig's C/ObjC sanitizer
        // runtime. Own it in the final archive alongside compiler-rt.
        compile.bundle_ubsan_rt = true;
        const linker = b.option([]const u8, "macos-linker", "Mach-O linker executable (ld on macOS, ld64.lld for cross builds)") orelse
            if (b.graph.host.result.os.tag == .macos) "/usr/bin/ld" else "ld64.lld";
        const run = command(b, "link-macho");
        const deployment = target.os.version_range.semver.min;
        run.addArgs(&.{ "--linker", linker, "--arch", if (target.cpu.arch == .aarch64) "arm64" else "x86_64", "--deployment", b.fmt("{d}.{d}.{d}", .{ deployment.major, deployment.minor, deployment.patch }), "--sdk" });
        run.addDirectoryArg(b.named_lazy_paths.get("antfly_macos_sdk_root") orelse @panic("macOS C API requires a selected SDK"));
        run.addArg("--exports");
        run.addFileArg(exports.?);
        run.addArg("--archive");
        run.addFileArg(compile.getEmittedBin());
        run.addArg("--output");
        const output = run.addOutputFileArg("libantfly.dylib");
        // Retain atom sizes and archive owners alongside the cached binary.
        // Release archives contain only the library, not this diagnostic map.
        run.addArg("--map");
        _ = run.addOutputFileArg("libantfly.map");
        run.addArg("--");
        var visited: std.AutoHashMap(*std.Build.Module, void) = .init(b.allocator);
        var libraries: std.AutoHashMap(*std.Build.Step.Compile, void) = .init(b.allocator);
        // Traverse imports without caching Module.getGraph: composition owners
        // still add build-info imports after this helper returns.
        addLinks(b, run, module, &visited, &libraries);
        for (providers) |provider| {
            if (!(libraries.getOrPut(provider) catch @panic("OOM")).found_existing) {
                run.addFileArg(provider.getEmittedBin());
                addLinks(b, run, provider.root_module, &visited, &libraries);
            }
        }
        break :blk output;
    } else blk: {
        compile.link_gc_sections = true;
        if (storage_owner != null) forcePublicSymbols(b, compile);
        if (exports) |map| compile.setVersionScript(map);
        break :blk compile.getEmittedBin();
    };
    const check = command(b, "check");
    if (macho or elf) {
        check.addArgs(&.{ "--format", if (macho) "macho" else "elf", "--header" });
        check.addFileArg(header);
        check.addArg("--library");
        check.addFileArg(binary);
    }
    if ((macho or elf) and target.os.tag == b.graph.host.result.os.tag and target.cpu.arch == b.graph.host.result.cpu.arch and target.abi == b.graph.host.result.abi) {
        const consumer = command(b, "consumer-test");
        consumer.addArgs(&.{ "--format", if (macho) "macho" else "elf", "--header" });
        consumer.addFileArg(header);
        consumer.addArg("--library");
        consumer.addFileArg(binary);
        consumer.addArg("--source");
        consumer.addFileArg(b.path("pkg/antfly-embedded/tests/capi_link_consumer.c"));
        consumer.addArg("--work");
        _ = consumer.addOutputDirectoryArg("consumer-linkage");
        // Also run under ordinary C API smoke validation, without making the
        // release library require a host executable or host compiler.
        b.step("capi-linkage-test", "Link executable and shared-library consumers with local constructor handles").dependOn(&consumer.step);
    }
    const install = if (macho) &b.addInstallFileWithDir(binary, .lib, "libantfly.dylib").step else &b.addInstallArtifact(compile, .{}).step;
    if (macho or elf) install.dependOn(&check.step);
    return .{ .binary = binary, .compile = if (macho) null else compile, .install = install, .check = check };
}

fn command(b: *std.Build, operation: []const u8) *std.Build.Step.Run {
    const run = b.addSystemCommand(&.{"python3"});
    run.addFileArg(b.path("tools/capi_exports.py"));
    run.addArg(operation);
    return run;
}

fn addLinks(b: *std.Build, run: *std.Build.Step.Run, module: *std.Build.Module, visited: *std.AutoHashMap(*std.Build.Module, void), libraries: *std.AutoHashMap(*std.Build.Step.Compile, void)) void {
    if ((visited.getOrPut(module) catch @panic("OOM")).found_existing) return;
    for (module.lib_paths.items) |path| {
        run.addArg("-L");
        run.addDirectoryArg(path);
    }
    for (module.include_dirs.items) |dir| switch (dir) {
        .framework_path, .framework_path_system => |path| {
            run.addArg("-F");
            run.addDirectoryArg(path);
        },
        else => {},
    };
    for (module.rpaths.items) |rpath| {
        run.addArg("-rpath");
        switch (rpath) {
            .lazy_path => |path| run.addDirectoryArg(path),
            .special => |path| run.addArg(path),
        }
    }
    var frameworks = module.frameworks.iterator();
    while (frameworks.next()) |entry| run.addArgs(&.{ if (entry.value_ptr.weak) "-weak_framework" else "-framework", entry.key_ptr.* });
    if (module.link_libcpp orelse false) run.addArg("-lc++");
    for (module.link_objects.items) |object| switch (object) {
        .other_step => |library| {
            if (!(libraries.getOrPut(library) catch @panic("OOM")).found_existing) {
                run.addFileArg(library.getEmittedBin());
                addLinks(b, run, library.root_module, visited, libraries);
            }
        },
        .static_path => |path| run.addFileArg(path),
        .system_lib => |lib| run.addArg(b.fmt("{s}{s}", .{ if (lib.weak) "-weak-l" else "-l", lib.name })),
        else => {}, // C/ObjC sources are compiled into their owning archive.
    };
    for (module.import_table.values()) |import| addLinks(b, run, import, visited, libraries);
}

// A source-less ELF facade must root the public header symbols in its provider
// archive. Version-script entries alone do not cause archive extraction.
fn forcePublicSymbols(b: *std.Build, artifact: *std.Build.Step.Compile) void {
    const path = b.root.join(b.allocator, "pkg/antfly-embedded/include/antfly.h") catch @panic("OOM");
    b.dependOnFileContents(b.path("pkg/antfly-embedded/include/antfly.h"));
    const source = path.root_dir.handle.readFileAlloc(b.graph.io, path.sub_path, b.allocator, .limited(256 * 1024)) catch @panic("unable to read antfly.h");
    var index: usize = 0;
    while (index < source.len) {
        if (std.mem.startsWith(u8, source[index..], "/*")) {
            index += 2;
            while (index + 1 < source.len and !std.mem.startsWith(u8, source[index..], "*/")) : (index += 1) {}
            index = @min(index + 2, source.len);
            continue;
        }
        if (std.mem.startsWith(u8, source[index..], "//")) {
            while (index < source.len and source[index] != '\n') : (index += 1) {}
            continue;
        }
        if (std.ascii.isAlphabetic(source[index]) or source[index] == '_') {
            const start = index;
            while (index < source.len and (std.ascii.isAlphanumeric(source[index]) or source[index] == '_')) : (index += 1) {}
            const name = source[start..index];
            var after = index;
            while (after < source.len and std.ascii.isWhitespace(source[after])) : (after += 1) {}
            if (std.mem.startsWith(u8, name, "antfly_") and after < source.len and source[after] == '(') artifact.forceUndefinedSymbol(name);
        } else index += 1;
    }
}
