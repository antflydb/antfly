// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

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
    module.addIncludePath(b.path("lib/lmdb"));
    if (include_c) {
        module.addCSourceFiles(.{
            .files = &.{ "lib/lmdb/mdb.c", "lib/lmdb/midl.c" },
            .flags = &lmdb_c_flags,
        });
    }
}

pub fn makeRootBuildOptions(
    b: *std.Build,
    backend: LmdbBackend,
    evented_async_io: bool,
    storage_sim_soak: bool,
    with_tla: bool,
    link_libc: bool,
    standalone_runtime_focused_test: bool,
    lmdb_enabled: bool,
    linked_storage: bool,
) *std.Build.Step.Options {
    const options = b.addOptions();
    // Disabled storage engines must not change the production module identity.
    options.addOption([]const u8, "lmdb_backend", @tagName(if (lmdb_enabled) backend else .zig));
    options.addOption(bool, "lmdb_evented_async_io", lmdb_enabled and evented_async_io);
    options.addOption(bool, "storage_sim_soak", storage_sim_soak);
    options.addOption(bool, "with_tla", with_tla);
    options.addOption(bool, "link_libc", link_libc);
    options.addOption(bool, "standalone_runtime_focused_test", standalone_runtime_focused_test);
    options.addOption(bool, "lmdb_enabled", lmdb_enabled);
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
    if (target.result.os.tag != .macos) return;
    const sdk_root = b.sysroot orelse
        b.graph.environ_map.get("SDK_PATH") orelse
        std.zig.system.darwin.getSdk(b.allocator, b.graph.io, &target.result) orelse
        return;
    module.addSystemIncludePath(.{ .cwd_relative = b.fmt("{s}/usr/include", .{sdk_root}) });
    module.addLibraryPath(.{ .cwd_relative = b.fmt("{s}/usr/lib", .{sdk_root}) });
    module.addFrameworkPath(.{ .cwd_relative = b.fmt("{s}/System/Library/Frameworks", .{sdk_root}) });
}

pub fn makeLmdbEngineModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    link_libc: bool,
    build_options: *std.Build.Step.Options,
) *std.Build.Module {
    const mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/lmdb/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addOptions("build_options", build_options);
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
    optimize: std.builtin.OptimizeMode,
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
    mod.addImport("antfly_source_root", mod);
    mod.addOptions("build_options", build_options);
    mod.addImport("lmdb_engine", lmdb_engine_mod);
    mod.addImport("antfly_platform", platform_mod);
    mod.addImport("antfly_hash", hash_mod);
    mod.addCSourceFiles(.{
        .files = &.{ "lib/lmdb/mdb.c", "lib/lmdb/midl.c" },
        .flags = &lmdb_c_flags,
    });
    mod.addIncludePath(b.path("lib/lmdb"));
    mod.link_libc = true;
    addMacosSdkPaths(b, mod, target);
    return mod;
}
