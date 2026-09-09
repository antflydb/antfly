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

const max_openapi_spec_bytes = 2 * 1024 * 1024;

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
    standalone_runtime_focused_test: bool,
    lite_local_inference_runtime: bool,
    lmdb_enabled: bool,
    antfly_version: []const u8,
) *std.Build.Step.Options {
    const options = b.addOptions();
    options.addOption([]const u8, "lmdb_backend", @tagName(backend));
    options.addOption(bool, "lmdb_evented_async_io", evented_async_io);
    options.addOption(bool, "storage_sim_soak", storage_sim_soak);
    options.addOption(bool, "with_tla", with_tla);
    options.addOption(bool, "link_libc", link_libc);
    options.addOption(bool, "standalone_runtime_focused_test", standalone_runtime_focused_test);
    options.addOption(bool, "lite_local_inference_runtime", lite_local_inference_runtime);
    options.addOption(bool, "lmdb_enabled", lmdb_enabled);
    options.addOption(bool, "bench_minimal_deps", false);
    options.addOption([]const u8, "antfly_version", antfly_version);
    EmbeddedOpenApiOptions.add(b, options);
    return options;
}

/// Read embedded source contents only when their options module is needed.
/// Graph construction must also work when regeneration needs to recreate a
/// missing openapi.yaml. Keep the emitted option values identical to ordinary
/// string options so runtime artifacts retain their existing cache identity.
const EmbeddedOpenApiOptions = struct {
    step: std.Build.Step,
    options: *std.Build.Step.Options,
    base_length: ?usize = null,

    const sources = [_][2][]const u8{
        .{ "ard_openapi_ard_yaml", "../specs/openapi/ard/api.yaml" },
        .{ "ard_openapi_antfly_yaml", "../openapi.yaml" },
        .{ "ard_openapi_metadata_yaml", "../specs/openapi/antfly/metadata.yaml" },
        .{ "ard_openapi_extensions_yaml", "../specs/openapi/extensions/api.yaml" },
        .{ "ard_openapi_auth_yaml", "../specs/openapi/auth/api.yaml" },
        .{ "ard_openapi_inference_config_yaml", "../specs/openapi/inference/config.yaml" },
    };

    fn add(b: *std.Build, options: *std.Build.Step.Options) void {
        const embedded = b.allocator.create(EmbeddedOpenApiOptions) catch @panic("OOM");
        embedded.* = .{
            .step = std.Build.Step.init(.{
                .id = .custom,
                .name = "read embedded OpenAPI schemas",
                .owner = b,
                .makeFn = make,
            }),
            .options = options,
        };
        options.step.dependOn(&embedded.step);
    }

    fn make(step: *std.Build.Step, _: std.Build.Step.MakeOptions) !void {
        const embedded: *EmbeddedOpenApiOptions = @fieldParentPtr("step", step);
        const b = step.owner;
        // A watch rebuild replaces the previous file contents, rather than
        // appending duplicate declarations to the options module.
        if (embedded.base_length) |length| {
            embedded.options.contents.shrinkRetainingCapacity(length);
        } else {
            embedded.base_length = embedded.options.contents.items.len;
        }
        const add_inputs = !step.inputs.populated();
        for (sources) |source| {
            const path = b.path(source[1]);
            if (add_inputs) try step.addWatchInput(path);
            const contents = std.Io.Dir.cwd().readFileAlloc(b.graph.io, path.getPath(b), b.allocator, .limited(max_openapi_spec_bytes)) catch |err| {
                return step.fail("failed to read build input {s}: {t}", .{ source[1], err });
            };
            defer b.allocator.free(contents);
            embedded.options.addOption([]const u8, source[0], contents);
        }
    }
};

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
