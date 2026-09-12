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
    graph_metric_maintenance,
    inference,
    metadata,
    standalone,
};

pub const RuntimeLibraryUnit = enum {
    api_kernel,
    distributed,
    storage_kernel,
    enrichment_compute,
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
    .storage_kernel,
    .enrichment_compute,
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
    structlog_mod: *std.Build.Module,
    platform_mod: *std.Build.Module,
    hash_mod: *std.Build.Module,
    production_antfly_imports: AntflyRootImports,
    antfly_client_pkg_mod: *std.Build.Module,
    capi_mod: *std.Build.Module,
    libantfly_link_mod: *std.Build.Module,
};
pub const AddRuntimeResult = struct {
    antfly_main_tests: *std.Build.Step.Compile,
    antfly_main: *std.Build.Step.Compile,
    run_linked_inference_abi_integration: *std.Build.Step.Run,
    runtime_library_artifacts: [std.meta.fields(RuntimeLibraryUnit).len]?*std.Build.Step.Compile,
};

pub fn addRuntime(b: *std.Build, options: AddRuntimeOptions) AddRuntimeResult {
    const target = options.target;
    const optimize = options.optimize;
    const strip = options.strip;
    const link_libc = options.link_libc;
    const sanitize_thread = options.sanitize_thread;
    const runtime_artifact_role = options.runtime_artifact_role;
    const structlog_mod = options.structlog_mod;
    const platform_mod = options.platform_mod;
    const hash_mod = options.hash_mod;
    const production_antfly_imports = options.production_antfly_imports;
    const antfly_client_pkg_mod = options.antfly_client_pkg_mod;
    const capi_mod = options.capi_mod;
    const libantfly_link_mod = options.libantfly_link_mod;
    const main_module_options: std.Build.Module.CreateOptions = .{
        .root_source_file = b.path("pkg/antfly/src/main.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_thread = sanitize_thread,
        .imports = &.{
            .{ .name = "antfly-client", .module = antfly_client_pkg_mod },
            .{ .name = "structlog", .module = structlog_mod },
            .{ .name = "antfly_platform", .module = platform_mod },
            .{ .name = "antfly_hash", .module = hash_mod },
            .{ .name = "build_info", .module = production_antfly_imports.build_info.module },
        },
    };
    const antfly_main_mod = b.createModule(main_module_options);
    // Tests share imports, not the product's final-link inputs and archives.
    const antfly_main_tests = b.addTest(.{
        .root_module = b.createModule(main_module_options),
        .test_runner = .{
            .path = b.path("pkg/antfly/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    addMacosSdkPaths(b, antfly_main_tests.root_module, target);
    production_antfly_imports.build_info.link(antfly_main_mod);
    production_antfly_imports.build_info.link(libantfly_link_mod);
    addMacosSdkPaths(b, antfly_main_mod, target);

    const antfly_main = b.addExecutable(.{
        .name = "antfly",
        .root_module = antfly_main_mod,
    });

    var runtime_library_artifacts: [std.meta.fields(RuntimeLibraryUnit).len]?*std.Build.Step.Compile = @splat(null);
    inline for (std.meta.tags(RuntimeLibraryUnit)) |unit| {
        // The executable, C API, and focused artifacts reuse their owning
        // runtime units instead of recompiling implementations in each root.
        const role_mod = b.createModule(.{
            .root_source_file = b.path(b.fmt("pkg/antfly/src/runtime_{s}_root.zig", .{@tagName(unit)})),
            .target = target,
            .optimize = optimize,
            .sanitize_thread = sanitize_thread,
            .pic = if (unit == .storage_kernel or unit == .enrichment_compute) true else null,
        });
        var role_imports = production_antfly_imports;
        role_imports.boundary_profile = switch (unit) {
            .cli, .inference => .common,
            .distributed, .api_kernel, .serverless => .owner,
            .enrichment_compute => .enrichment,
            .storage_kernel => .all,
        };
        switch (unit) {
            .cli => role_imports.configureCli(role_mod, link_libc),
            .inference => role_imports.configureInference(b, role_mod, link_libc),
            .distributed, .storage_kernel => role_imports.configureStorage(b, role_mod, link_libc),
            .enrichment_compute => role_imports.configureEnrichment(b, role_mod, link_libc),
            .api_kernel => role_imports.configureApi(role_mod, link_libc),
            .serverless => role_imports.configureServerless(b, role_mod, link_libc),
        }
        role_imports.storage_boundary.configureProfile(role_mod, unit != .storage_kernel and unit != .enrichment_compute, true, role_imports.boundary_profile);
        if (unit == .storage_kernel) {
            const capi_options = b.addOptions();
            capi_options.addOption(bool, "linked_storage", true);
            role_mod.addOptions("capi_build_options", capi_options);
        }
        addMacosSdkPaths(b, role_mod, target);
        if (unit == .cli or unit == .distributed or unit == .storage_kernel) role_mod.addImport("antfly-client", antfly_client_pkg_mod);
        if (unit == .storage_kernel) role_mod.addImport("antfly_storage_root", role_mod);
        if (unit != .cli and unit != .inference and unit != .enrichment_compute) {
            const role_usermgr_storage_mod = b.createModule(.{
                .root_source_file = b.path("pkg/antfly/src/usermgr/storage_imports.zig"),
                .target = target,
                .optimize = optimize,
            });
            role_usermgr_storage_mod.addImport("antfly_root", role_mod);
            role_usermgr_storage_mod.addImport("antfly_platform", platform_mod);
            role_mod.addImport("usermgr_storage", role_usermgr_storage_mod);
        }

        const role_artifact = b.addLibrary(.{
            .name = if (unit == .storage_kernel)
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
                // Physical storage now compiles separately from distributed
                // coordination. Retain the split kernel's conservative 20 GiB
                // reservation; the former monolithic 24/22 GiB measurements
                // do not describe either of these independent artifacts.
                .storage_kernel => 20 * 1024 * 1024 * 1024,
                .distributed => 11 * 1024 * 1024 * 1024,
                .enrichment_compute => 4 * 1024 * 1024 * 1024,
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
        if (unit == .storage_kernel) {
            // The executable and C ABI libraries share this one optimized
            // PIC object. Give the final links enough section granularity
            // to retain only the C ABI roots in the shared libraries while
            // the executable retains the runtime entry points as well.
            role_artifact.link_function_sections = true;
            role_artifact.link_data_sections = true;
        }
        // Zig's build runner uses these claims to run as many LLVM codegen
        // steps concurrently as fit in available RAM. The storage archive
        // is PIC and shared by the executable and C API final links.
        if (unit == .storage_kernel) {
            libantfly_link_mod.linkLibrary(role_artifact);
        }
        if (strip) {
            var visited = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
            defer visited.deinit();
            setStripRecursively(role_mod, &visited);
        }
    }

    libantfly_link_mod.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.enrichment_compute)].?);

    // Exercise the real production archive boundary for encoded-image reads.
    // The probe resolves only the exported C function table, so it cannot
    // accidentally pass by importing inference_host.zig into the test root.
    const linked_inference_abi_integration_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/inference_abi_integration.zig"),
        .target = target,
        .optimize = optimize,
    });
    // This executable loads the production archive, including version consumers.
    production_antfly_imports.build_info.link(linked_inference_abi_integration_mod);
    linked_inference_abi_integration_mod.link_libc = link_libc;
    addMacosSdkPaths(b, linked_inference_abi_integration_mod, target);
    const linked_inference_abi_integration = b.addExecutable(.{
        .name = "linked-inference-abi-integration",
        .root_module = linked_inference_abi_integration_mod,
    });
    linked_inference_abi_integration.root_module.linkLibrary(
        runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.inference)].?,
    );
    const run_linked_inference_abi_integration = b.addRunArtifact(linked_inference_abi_integration);

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
        production_antfly_imports.build_info.link(role_mod);

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
            .graph_metric_maintenance => {},
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
        if (role != .inference) {
            role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.storage_kernel)].?);
            role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.enrichment_compute)].?);
            role_exe.root_module.linkLibrary(runtime_library_artifacts[@intFromEnum(RuntimeLibraryUnit.inference)].?);
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
        .antfly_main_tests = antfly_main_tests,
        .antfly_main = antfly_main,
        .run_linked_inference_abi_integration = run_linked_inference_abi_integration,
        .runtime_library_artifacts = runtime_library_artifacts,
    };
}
