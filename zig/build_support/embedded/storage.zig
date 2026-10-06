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
const Translator = @import("translate_c").Translator;

pub const LmdbBackend = enum {
    c,
    zig,
};

pub const lmdb_c_flags = [_][]const u8{
    "-pthread",
    "-fno-sanitize=alignment",
};

pub fn makeLmdbBuildOptions(
    b: *std.Build,
    backend: LmdbBackend,
    evented_async_io: bool,
    storage_sim_soak: bool,
) *std.Build.Step.Options {
    const options = b.addOptions();
    options.addOption([]const u8, "lmdb_backend", @tagName(backend));
    options.addOption(bool, "lmdb_evented_async_io", evented_async_io);
    options.addOption(bool, "storage_sim_soak", storage_sim_soak);
    return options;
}

/// LMDB consumers declare their engine and optional C implementation explicitly.
pub fn configureLmdb(b: *std.Build, module: *std.Build.Module, engine: *std.Build.Module, include_c: bool) void {
    module.addImport("lmdb_engine", engine);
    module.addIncludePath(b.path("deps/lmdb"));
    // Share one translation module across consumers of this engine; Zig rejects
    // the same generated source owned by multiple module instances.
    module.addImport("lmdb_c_bindings", engine.import_table.get("lmdb_c_bindings").?);
    if (include_c) {
        module.addCSourceFiles(.{
            .files = &.{ "deps/lmdb/mdb.c", "deps/lmdb/midl.c" },
            .flags = &lmdb_c_flags,
        });
    }
}

pub fn makeRootBuildOptions(
    b: *std.Build,
    storage_sim_soak: bool,
    with_tla: bool,
    link_libc: bool,
    standalone_runtime_focused_test: bool,
    linked_storage: bool,
) *std.Build.Step.Options {
    const options = b.addOptions();
    options.addOption(bool, "storage_sim_soak", storage_sim_soak);
    options.addOption(bool, "with_tla", with_tla);
    options.addOption(bool, "link_libc", link_libc);
    options.addOption(bool, "standalone_runtime_focused_test", standalone_runtime_focused_test);
    options.addOption(bool, "bench_minimal_deps", false);
    options.addOption(bool, "linked_storage", linked_storage);
    return options;
}

/// Capability advertising belongs to Lite consumers, not general DB options.
pub fn createLiteOptions(b: *std.Build, local_inference_runtime: bool) *std.Build.Module {
    const options = b.addOptions();
    options.addOption(bool, "local_inference_runtime", local_inference_runtime);
    return options.createModule();
}

fn addMacosSdkPaths(b: *std.Build, module: *std.Build.Module, target: std.Build.ResolvedTarget) void {
    @import("antfly_platform").addMacosSdkPaths(b, module, target);
}

pub fn makeLmdbEngineModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    link_libc: bool,
    build_options: *std.Build.Step.Options,
    platform_mod: *std.Build.Module,
) *std.Build.Module {
    const mod = b.createModule(.{
        .root_source_file = b.path("lib/lmdb/src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addOptions("build_options", build_options);
    mod.addImport("antfly_platform", platform_mod);
    const bindings = Translator.init(b.dependency("translate_c", .{}), .{
        .libc_file = @import("antfly_platform").macosSdkLibCFile(b, target),
        .c_source_file = b.path("deps/lmdb/lmdb.h"),
        .target = target,
        .optimize = optimize,
        .link_libc = target.result.os.tag != .freestanding,
    });
    mod.addImport("lmdb_c_bindings", bindings.mod);
    if (link_libc and target.result.os.tag != .freestanding) {
        mod.link_libc = true;
        addMacosSdkPaths(b, mod, target);
    }
    return mod;
}

pub fn makeLmdbModule(
    b: *std.Build,
    root_path: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    build_options: *std.Build.Step.Options,
    lmdb_engine_mod: *std.Build.Module,
    platform_mod: *std.Build.Module,
    hash_mod: *std.Build.Module,
) *std.Build.Module {
    const mod = b.createModule(.{
        .root_source_file = b.path(root_path),
        .target = target,
        .optimize = optimize,
    });
    @import("source_owner.zig").attach(mod);
    mod.addImport("antfly_source_root", mod);
    if (std.mem.startsWith(u8, root_path, "lib/lmdb/")) {
        // The wrapper and engine must share one options module: Zig rejects
        // importing the same generated options source as two distinct modules.
        mod.addImport("build_options", lmdb_engine_mod.import_table.get("build_options").?);
    } else {
        mod.addOptions("build_options", build_options);
    }
    configureLmdb(b, mod, lmdb_engine_mod, true);
    mod.addImport("storage_sim_fixture", b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/local/storage/sim_fixture.zig"),
        .target = target,
        .optimize = optimize,
    }));
    mod.addImport("antfly_platform", platform_mod);
    mod.addImport("antfly_hash", hash_mod);
    mod.link_libc = true;
    addMacosSdkPaths(b, mod, target);
    return mod;
}
