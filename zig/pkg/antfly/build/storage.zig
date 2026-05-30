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

pub fn makeRootBuildOptions(
    b: *std.Build,
    backend: LmdbBackend,
    evented_async_io: bool,
    storage_sim_soak: bool,
    with_tla: bool,
    link_libc: bool,
    swarm_runtime_focused_test: bool,
    antfly_version: []const u8,
) *std.Build.Step.Options {
    const options = b.addOptions();
    options.addOption([]const u8, "lmdb_backend", @tagName(backend));
    options.addOption(bool, "lmdb_evented_async_io", evented_async_io);
    options.addOption(bool, "storage_sim_soak", storage_sim_soak);
    options.addOption(bool, "with_tla", with_tla);
    options.addOption(bool, "link_libc", link_libc);
    options.addOption(bool, "swarm_runtime_focused_test", swarm_runtime_focused_test);
    options.addOption(bool, "bench_minimal_deps", false);
    options.addOption([]const u8, "antfly_version", antfly_version);
    return options;
}

/// Provide the `lmdb_pthread` module imported by `pkg/antfly/src/lmdb/env.zig`.
/// Zig 0.17 removed `@cImport`, so the libc-backed pthread surface comes from an
/// `addTranslateC` of `pthread_c.h`; freestanding/no-libc builds get the Zig
/// `pthread_stub.zig` fallback instead.
pub fn addLmdbPthreadImport(
    b: *std.Build,
    mod: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    link_libc: bool,
) void {
    if (link_libc and target.result.os.tag != .freestanding) {
        const tc = b.addTranslateC(.{
            .root_source_file = b.path("pkg/antfly/src/lmdb/pthread_c.h"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        mod.addImport("lmdb_pthread", tc.createModule());
    } else {
        mod.addImport("lmdb_pthread", b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/lmdb/pthread_stub.zig"),
            .target = target,
            .optimize = optimize,
        }));
    }
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
    }
    addLmdbPthreadImport(b, mod, target, optimize, link_libc);
    return mod;
}

/// Provide the `lmdb_c_bindings` module imported by
/// `pkg/antfly/src/storage/lmdb_c_api.zig`. Zig 0.17 removed `@cImport`, so the
/// C-backend bindings come from an `addTranslateC` of `lmdb.h` (with lib/lmdb on
/// the include path); the Zig backend and no-libc builds get the hand-written
/// `lmdb_c_stub.zig` instead.
pub fn addLmdbCBindingsImport(
    b: *std.Build,
    mod: *std.Build.Module,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    backend: LmdbBackend,
    link_libc: bool,
) void {
    if (backend == .c and link_libc and target.result.os.tag != .freestanding) {
        const tc = b.addTranslateC(.{
            .root_source_file = b.path("pkg/antfly/src/storage/lmdb_c.h"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        });
        tc.addIncludePath(b.path("lib/lmdb"));
        mod.addImport("lmdb_c_bindings", tc.createModule());
    } else {
        mod.addImport("lmdb_c_bindings", b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/storage/lmdb_c_stub.zig"),
            .target = target,
            .optimize = optimize,
        }));
    }
}

pub fn makeLmdbModule(
    b: *std.Build,
    root_path: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    build_options: *std.Build.Step.Options,
    lmdb_engine_mod: *std.Build.Module,
    platform_mod: *std.Build.Module,
    backend: LmdbBackend,
) *std.Build.Module {
    const mod = b.createModule(.{
        .root_source_file = b.path(root_path),
        .target = target,
        .optimize = optimize,
    });
    mod.addOptions("build_options", build_options);
    mod.addImport("lmdb_engine", lmdb_engine_mod);
    mod.addImport("antfly_platform", platform_mod);
    mod.addCSourceFiles(.{
        .files = &.{ "lib/lmdb/mdb.c", "lib/lmdb/midl.c" },
        .flags = &lmdb_c_flags,
    });
    mod.addIncludePath(b.path("lib/lmdb"));
    mod.link_libc = true;
    // These modules always link libc and compile the C LMDB sources, so the
    // binding selection follows the requested backend (translate-c for `.c`).
    addLmdbCBindingsImport(b, mod, target, optimize, backend, true);
    return mod;
}
