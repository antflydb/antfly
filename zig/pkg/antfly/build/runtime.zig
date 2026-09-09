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
const AntflyRootImports = @import("imports.zig").AntflyRootImports;

pub const RuntimeArtifactRole = enum {
    cli,
    data,
    inference,
    metadata,
    standalone,
};

pub const RuntimeLibraryUnit = enum {
    api_kernel,
    distributed,
    // Serverless/lake execution is a large, independently deployable graph.
    // Keep it out of the PIC storage kernel so LLVM never has to optimize the
    // two closures as one ARM64 ReleaseFast compilation unit.
    serverless,
    inference,
    // Remote/client commands do not own storage or server runtimes.
    cli,
};

// Static archives must be presented from consumers to providers. The
// distributed/application unit calls into both the API kernel and inference
// unit, while the remote CLI is an executable-facing leaf. Keep this separate
// from RuntimeLibraryUnit declaration order: declaration order controls build
// graph construction, not the final link's dependency topology.

pub const runtime_library_link_order = [_]RuntimeLibraryUnit{
    .cli,
    .serverless,
    .distributed,
    .api_kernel,
    .inference,
};

comptime {
    const unit_count = std.meta.fields(RuntimeLibraryUnit).len;
    if (runtime_library_link_order.len != unit_count)
        @compileError("runtime_library_link_order must contain every runtime library unit exactly once");
    var seen = [_]bool{false} ** unit_count;
    for (runtime_library_link_order) |unit| {
        const index = @intFromEnum(unit);
        if (seen[index])
            @compileError("runtime_library_link_order contains a duplicate runtime library unit");
        seen[index] = true;
    }
}

pub fn setStripRecursively(module: *std.Build.Module, visited: *std.AutoHashMap(*std.Build.Module, void)) void {
    const result = visited.getOrPut(module) catch @panic("OOM");
    if (result.found_existing) return;

    module.strip = true;
    for (module.import_table.values()) |imported_module| {
        setStripRecursively(imported_module, visited);
    }
}
const addMacosSdkPaths = @import("../../../lib/platform/build_support.zig").addMacosSdkPaths;

pub const AddRuntimeOptions = struct {
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    strip: bool,
    link_libc: bool,
    sanitize_thread: bool,
    runtime_artifact_role: ?RuntimeArtifactRole,
    production_build_options: *std.Build.Step.Options,
    structlog_mod: *std.Build.Module,
    platform_mod: *std.Build.Module,
    hash_mod: *std.Build.Module,
    production_antfly_imports: AntflyRootImports,
    antfly_client_pkg_mod: *std.Build.Module,
    capi_mod: *std.Build.Module,
    libantfly_link_mod: *std.Build.Module,
};
pub const AddRuntimeResult = struct {
    antfly_main_mod: *std.Build.Module,
    antfly_main: *std.Build.Step.Compile,
    runtime_library_artifacts: [std.meta.fields(RuntimeLibraryUnit).len]?*std.Build.Step.Compile,
};

pub fn addRuntime(b: *std.Build, options: AddRuntimeOptions) AddRuntimeResult {
    const target = options.target;
    const optimize = options.optimize;
    const strip = options.strip;
    const link_libc = options.link_libc;
    const sanitize_thread = options.sanitize_thread;
    const runtime_artifact_role = options.runtime_artifact_role;
    const production_build_options = options.production_build_options;
    const structlog_mod = options.structlog_mod;
    const platform_mod = options.platform_mod;
    const hash_mod = options.hash_mod;
    const production_antfly_imports = options.production_antfly_imports;
    const antfly_client_pkg_mod = options.antfly_client_pkg_mod;
    const capi_mod = options.capi_mod;
    const libantfly_link_mod = options.libantfly_link_mod;
    const antfly_main_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/main.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_thread = sanitize_thread,
    });
    antfly_main_mod.addImport("antfly-client", antfly_client_pkg_mod);
    antfly_main_mod.addImport("structlog", structlog_mod);
    antfly_main_mod.addImport("antfly_platform", platform_mod);
    antfly_main_mod.addImport("antfly_hash", hash_mod);
    antfly_main_mod.addOptions("build_options", production_build_options);
    addMacosSdkPaths(b, antfly_main_mod, target);

    const antfly_main = b.addExecutable(.{
        .name = "antfly",
        .root_module = antfly_main_mod,
    });

    var runtime_library_artifacts: [std.meta.fields(RuntimeLibraryUnit).len]?*std.Build.Step.Compile = @splat(null);
    inline for (std.meta.tags(RuntimeLibraryUnit)) |unit| {
        // The executable, C API, and focused artifacts reuse their owning
        // runtime units instead of recompiling implementations in each root.
        const unit_options = b.addOptions();
        unit_options.addOption(RuntimeLibraryUnit, "unit", unit);

        const role_mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/runtime_artifact_lib.zig"),
            .target = target,
            .optimize = optimize,
            .sanitize_thread = sanitize_thread,
            .pic = if (unit == .distributed) true else null,
        });
        production_antfly_imports.configureRuntime(
            b,
            role_mod,
            false,
            link_libc,
            unit == .inference,
        );
        addMacosSdkPaths(b, role_mod, target);
        role_mod.addImport("antfly-client", antfly_client_pkg_mod);
        if (unit == .distributed) role_mod.addImport("antfly_storage_root", role_mod);
        role_mod.addOptions("runtime_library_options", unit_options);
        const role_usermgr_storage_mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
            .target = target,
            .optimize = optimize,
        });
        role_usermgr_storage_mod.addImport("antfly_root", role_mod);
        role_usermgr_storage_mod.addImport("antfly_platform", platform_mod);
        role_mod.addImport("usermgr_storage", role_usermgr_storage_mod);

        const role_artifact = b.addLibrary(.{
            .name = if (unit == .distributed)
                "antfly-storage-kernel"
            else
                b.fmt("antfly-runtime-{s}", .{@tagName(unit)}),
            .root_module = role_mod,
            .linkage = .static,
            .max_rss = switch (unit) {
                // Claims conservatively cover clean production ReleaseFast
                // peaks measured for both aarch64-linux-musl and explicit
                // aarch64-macos (including Metal and Accelerate). They are
                // scheduling reservations, not hard process limits. A larger
                // budget can overlap more units while a smaller cgroup
                // automatically schedules only the subset that fits.
                // aarch64-macOS ReleaseFast codegen reached 9.95 GB with
                // platform frameworks. Linux ARM64 reached 4.99 GB in the
                // v0.2.1-rc0 release build, while the integrated HA API kernel
                // reached 8.10 GB in a clean aarch64-linux-musl ReleaseFast
                // build. Reserve 10 GiB so the scheduler serializes competing
                // roots instead of discarding a successful production build.
                .api_kernel => @as(usize, if (target.result.os.tag == .macos) 11 else 10) * 1024 * 1024 * 1024,
                // Clean aarch64-macOS ReleaseFast storage codegen reached
                // 19.51 GB (18.17 GiB) with the platform frameworks enabled.
                // A clean native aarch64-linux-musl production container build
                // reached 19.89 GB (18.52 GiB) for the current production
                // graph. Reserve 20 GiB on both targets so Zig's scheduler does
                // not discard a successfully compiled production artifact.
                // Use the same Linux-target claim for native and cross builds;
                // the target artifact determines the dominant codegen shape.
                .distributed => 20 * 1024 * 1024 * 1024,
                // This is deliberately a separate non-PIC product unit. The
                // cold aarch64-macOS ReleaseFast build peaks near 2 GiB;
                // the 10 GiB reservation keeps it serialized with the macOS
                // storage kernel until both release runners confirm that.
                .serverless => 10 * 1024 * 1024 * 1024,
                // The broad aarch64-macOS ReleaseFast inference root now
                // reaches roughly 13.6 GB after storage/runtime integration.
                // Reserve enough headroom for mode-dependent IR; the build
                // scheduler can overlap whichever roots fit without forcing
                // callers to serialize the whole build.
                .inference => 16 * 1024 * 1024 * 1024,
                // Clean aarch64-macOS ReleaseFast codegen currently peaks
                // around 2.23 GB, just above the former 2 GiB reservation.
                .cli => 3 * 1024 * 1024 * 1024,
            },
        });
        const runtime_unit_step = b.step(
            b.fmt("runtime-unit-{s}", .{@tagName(unit)}),
            b.fmt("Build only the {s} runtime library unit", .{@tagName(unit)}),
        );
        runtime_unit_step.dependOn(&role_artifact.step);
        runtime_library_artifacts[@intFromEnum(unit)] = role_artifact;
        if (unit == .distributed) {
            // The executable and C ABI libraries share this one optimized
            // PIC object. Give the final links enough section granularity
            // to retain only the C ABI roots in the shared libraries while
            // the executable retains the runtime entry points as well.
            role_artifact.link_function_sections = true;
            role_artifact.link_data_sections = true;
        }
        // Zig's build runner uses these claims to run as many LLVM codegen
        // steps concurrently as fit in available RAM. The distributed
        // archive is PIC because the executable and C ABI libraries share
        // it; both consumers therefore reuse the same analyzed and
        // optimized storage graph.
        if (unit == .distributed) {
            libantfly_link_mod.linkLibrary(role_artifact);
        }
        if (strip) {
            var visited = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
            defer visited.deinit();
            setStripRecursively(role_mod, &visited);
        }
    }

    for (runtime_library_link_order) |unit| {
        antfly_main.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(unit)].?);
    }

    if (runtime_artifact_role) |role| {
        const role_options = b.addOptions();
        role_options.addOption(RuntimeArtifactRole, "role", role);

        const role_mod = b.createModule(.{
            .root_source_file = b.path("pkg/antfly/src/runtime_artifact_main.zig"),
            .target = target,
            .optimize = optimize,
            .sanitize_thread = sanitize_thread,
        });
        role_mod.addImport("structlog", structlog_mod);
        role_mod.addImport("antfly_platform", platform_mod);
        role_mod.link_libc = link_libc;
        addMacosSdkPaths(b, role_mod, target);
        role_mod.addOptions("runtime_artifact_options", role_options);

        const role_name = @tagName(role);
        const role_exe = b.addExecutable(.{
            .name = b.fmt("antfly-{s}", .{role_name}),
            .root_module = role_mod,
        });
        role_exe.link_gc_sections = true;
        switch (role) {
            .cli => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.cli)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.distributed)].?);
            },
            .data, .metadata => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.distributed)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.api_kernel)].?);
            },
            .inference => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.inference)].?);
            },
            .standalone => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.distributed)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.api_kernel)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.inference)].?);
            },
        }
        if (strip) {
            var visited = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
            defer visited.deinit();
            setStripRecursively(role_mod, &visited);
        }
        const install_role = b.addInstallArtifact(role_exe, .{});
        const role_step = b.step("runtime-artifact", "Build and install one focused server runtime artifact");
        role_step.dependOn(&install_role.step);
    }
    if (strip) {
        var visited = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
        defer visited.deinit();
        setStripRecursively(antfly_main_mod, &visited);
        setStripRecursively(capi_mod, &visited);
    }
    return .{
        .antfly_main_mod = antfly_main_mod,
        .antfly_main = antfly_main,
        .runtime_library_artifacts = runtime_library_artifacts,
    };
}
