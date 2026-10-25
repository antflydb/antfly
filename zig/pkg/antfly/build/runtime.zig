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
const AntflyRootImports = @import("../../../build_support/antfly/imports.zig").AntflyRootImports;

pub const RuntimeArtifactRole = @import("../../../build_support/antfly/runtime_roles.zig").RuntimeArtifactRole;

const runtime_memory = @import("runtime_memory.zig");
pub const RuntimeLibraryUnit = runtime_memory.RuntimeLibraryUnit;
const runtimeCompileMaxRss = runtime_memory.runtimeCompileMaxRss;

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
    const unit_count = @typeInfo(RuntimeLibraryUnit).@"enum".field_names.len;
    if (runtime_library_link_order.len != unit_count)
        @compileError("runtime_library_link_order must contain every runtime library unit exactly once");
    var seen = @as([unit_count]bool, @splat(false));
    for (runtime_library_link_order) |unit| {
        const index = @backingInt(unit);
        if (seen[index])
            @compileError("runtime_library_link_order contains a duplicate runtime library unit");
        seen[index] = true;
    }
}

pub fn setStripRecursively(module: *std.Build.Module, visited: *std.AutoHashMap(*std.Build.Module, void)) void {
    @import("../../../build_support/embedded/shared_library.zig").setStripRecursively(module, visited);
}

pub fn stripBuildGraph(b: *std.Build) void {
    @import("../../../build_support/embedded/shared_library.zig").stripBuildGraph(b);
}
const addMacosSdkPaths = @import("antfly_platform").addMacosSdkPaths;

pub const AddRuntimeOptions = struct {
    target: std.Build.ResolvedTarget,
    optimize: std.lang.Optimize,
    strip: bool,
    link_libc: bool,
    sanitize_thread: bool,
    cpu_inference: bool = false,
    runtime_artifact_role: ?RuntimeArtifactRole,
    structlog_mod: *std.Build.Module,
    platform_mod: *std.Build.Module,
    hash_mod: *std.Build.Module,
    production_antfly_imports: AntflyRootImports,
    antfly_client_pkg_mod: *std.Build.Module,
    capi_mod: *std.Build.Module,
    native_storage: ?*std.Build.Step.Compile,
    native_enrichment: *std.Build.Step.Compile,
    native_inference: *std.Build.Step.Compile,
};
pub const AddRuntimeResult = struct {
    antfly_main_tests: *std.Build.Step.Compile,
    antfly_main: *std.Build.Step.Compile,
    run_linked_inference_abi_integration: *std.Build.Step.Run,
    runtime_library_artifacts: [@typeInfo(RuntimeLibraryUnit).@"enum".field_names.len]?*std.Build.Step.Compile,
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
    addSharedSignalStackOwner(b, antfly_main_mod, target, link_libc);
    addSharedSignalStackOwner(b, capi_mod, target, link_libc);
    // Tests share imports, not the product's final-link inputs and archives.
    const antfly_main_tests = b.addTest(.{
        .root_module = b.createModule(main_module_options),
        .test_runner = .{
            .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
            .mode = .simple,
        },
    });
    addMacosSdkPaths(b, antfly_main_tests.root_module, target);
    production_antfly_imports.build_info.link(antfly_main_mod);
    addMacosSdkPaths(b, antfly_main_mod, target);

    const antfly_main = b.addExecutable(.{
        .name = "antfly",
        .root_module = antfly_main_mod,
        .use_llvm = @import("../../../build_support/embedded/shared_library.zig").llvmBackend(target.result),
    });

    var runtime_library_artifacts: [@typeInfo(RuntimeLibraryUnit).@"enum".field_names.len]?*std.Build.Step.Compile = @splat(null);
    inline for (std.meta.tags(RuntimeLibraryUnit)) |unit| {
        // The server executable and focused server artifacts reuse their owning
        // runtime units. Public embedded products have independent owners.
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
        // Compute owns document/media processing, not a physical storage handle.
        // Only the storage kernel may declare physical DB source inputs.
        role_imports.storage_boundary.configureProfile(role_mod, unit != .storage_kernel, true, role_imports.boundary_profile);
        if (unit == .inference) {
            const native_exports = b.createModule(.{
                .root_source_file = b.path("pkg/inference/src/host/native_exports.zig"),
                .target = target,
                .optimize = optimize,
            });
            production_antfly_imports.configureInference(b, native_exports, link_libc);
            role_mod.addImport("antfly_inference_native_exports", native_exports);
        }
        if (unit == .storage_kernel) {
            const capi_options = b.addOptions();
            capi_options.addOption(bool, "linked_storage", true);
            // This archive is shared by the default `libantfly` and by the
            // `antfly` executable. Both link the inference runtime archive
            // in-process (see `link_anchor.zig` and the embedded library
            // composition), so the
            // embedded-inference construction path is always available.
            capi_options.addOption(bool, "inference_enabled", true);
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

        // Both products consume the same three coarse compute exports. The
        // Apache owner is already PIC and storage-free, and receives the
        // same sanitizer setting as the server.
        const compile_max_rss = if (unit == .inference and production_antfly_imports.shared_inference_core) 4 * 1024 * 1024 * 1024 else runtimeCompileMaxRss(unit, .{
            .host = b.graph.host.result,
            .target = target.result,
            .optimize = optimize,
            .strip = strip,
            .cpu_inference = options.cpu_inference,
            .sanitize_thread = sanitize_thread,
            .macos_metal_blas_release = production_antfly_imports.darwin_release_memory,
            .macos_shared_storage_sdk27 = production_antfly_imports.darwin_shared_storage_sdk27,
        });
        const role_artifact = if (unit == .enrichment_compute) blk: {
            // Reuse the implementation without replacing the runtime's
            // measured profile reservation with the embedded default.
            options.native_enrichment.step.max_rss = compile_max_rss;
            break :blk options.native_enrichment;
        } else b.addLibrary(.{
            .name = if (unit == .storage_kernel)
                "antfly-storage-kernel"
            else
                b.fmt("antfly-runtime-{s}", .{@tagName(unit)}),
            .root_module = role_mod,
            .linkage = .static,
            .use_llvm = @import("../../../build_support/embedded/shared_library.zig").llvmBackend(target.result),
            .max_rss = compile_max_rss,
        });
        // Final images select their inference provider. Source-level owner
        // fixtures compile it locally; production images link the archive.
        // Keep that choice off this shared adapter's transitive link graph.
        const runtime_unit_step = b.step(
            b.fmt("runtime-unit-{s}", .{@tagName(unit)}),
            b.fmt("Build only the {s} runtime library unit", .{@tagName(unit)}),
        );
        runtime_unit_step.dependOn(&role_artifact.step);
        if (unit == .storage_kernel and options.native_storage != null)
            b.step("runtime-unit-storage-adapter", "Compile only the server storage adapter without waiting for shared engine emission").dependOn(&role_artifact.step);
        if (unit == .storage_kernel) {
            if (options.native_storage) |owner| runtime_unit_step.dependOn(&owner.step);
        }
        runtime_library_artifacts[@backingInt(unit)] = role_artifact;
        if (unit == .storage_kernel) {
            // The executable and C ABI libraries share this one optimized
            // PIC object. Give the final links enough section granularity
            // to retain only the C ABI roots in the shared libraries while
            // the executable retains the runtime entry points as well.
            role_artifact.link_function_sections = true;
            role_artifact.link_data_sections = true;
        }
        // Zig's build runner uses these claims to run as many LLVM codegen
        // steps concurrently as fit in available RAM. The server storage
        // archive owns private operations used by the executable.
        if (strip) {
            var visited = std.AutoHashMap(*std.Build.Module, void).init(b.allocator);
            defer visited.deinit();
            setStripRecursively(role_mod, &visited);
        }
    }

    if (options.native_storage) |owner| {
        // Exercise the server's production build/source options against the
        // independently compiled Apache owner, not just the embedded profile.
        const client = b.createModule(.{ .root_source_file = b.path("pkg/antfly-embedded/src/public_capi_root.zig"), .target = target, .optimize = optimize });
        production_antfly_imports.configureStorage(b, client, link_libc);
        production_antfly_imports.storage_boundary.configureProfile(client, false, true, .all);
        const capi_options = b.addOptions();
        capi_options.addOption(bool, "linked_storage", true);
        capi_options.addOption(bool, "inference_enabled", true);
        client.addOptions("capi_build_options", capi_options);
        client.addImport("antfly_storage_root", client);
        const probe_mod = b.createModule(.{ .root_source_file = b.path("pkg/antfly-embedded/src/storage_owner_linkage_probe.zig"), .target = target, .optimize = optimize });
        probe_mod.addImport("storage_client", @import("../../../build_support/embedded/shared_library.zig").storageConsumerModule(b, client));
        probe_mod.linkLibrary(owner);
        production_antfly_imports.build_info.link(probe_mod);
        probe_mod.linkLibrary(options.native_inference);
        probe_mod.linkLibrary(options.native_enrichment);
        const run = @import("../../../build_support/embedded/shared_library.zig").addStorageProbe(b, probe_mod, "storage-server-owner-linkage-probe");
        _ = run.addOutputDirectoryArg("storage-server-owner-linkage");
        run.has_side_effects = true;
        b.top_level_steps.get("storage-owner-linkage-test").?.step.dependOn(&run.step);
    }

    // Exercise the real production archive boundary for encoded-image reads.
    // The probe resolves only the exported C function table, so it cannot
    // accidentally pass by importing pkg/inference/src/host/host.zig into the test root.
    const linked_inference_abi_integration_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/inference_abi_integration.zig"),
        .target = target,
        .optimize = optimize,
    });
    linked_inference_abi_integration_mod.addImport("antfly_inference_bridge", production_antfly_imports.inference_bridge);
    linked_inference_abi_integration_mod.addImport("antfly_runtime_abi", production_antfly_imports.runtime_abi);
    // This executable loads the production archive, including version consumers.
    production_antfly_imports.build_info.link(linked_inference_abi_integration_mod);
    linked_inference_abi_integration_mod.link_libc = link_libc;
    addSharedSignalStackOwner(b, linked_inference_abi_integration_mod, target, link_libc);
    addMacosSdkPaths(b, linked_inference_abi_integration_mod, target);
    const linked_inference_abi_integration = b.addExecutable(.{
        .name = "linked-inference-abi-integration",
        .root_module = linked_inference_abi_integration_mod,
        .use_llvm = @import("../../../build_support/embedded/shared_library.zig").llvmBackend(target.result),
    });
    linked_inference_abi_integration.root_module.linkLibrary(
        runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.inference)].?,
    );
    if (production_antfly_imports.shared_inference_core) linked_inference_abi_integration_mod.linkLibrary(options.native_inference);
    const run_linked_inference_abi_integration = b.addRunArtifact(linked_inference_abi_integration);

    const worker_probe_mod = b.createModule(.{
        .root_source_file = b.path("pkg/antfly/src/inference_worker_abi_integration.zig"),
        .target = target,
        .optimize = optimize,
    });
    worker_probe_mod.addImport("antfly_inference_bridge", production_antfly_imports.inference_bridge);
    worker_probe_mod.addImport("antfly_inference_host", production_antfly_imports.inference_host);
    production_antfly_imports.build_info.link(worker_probe_mod);
    worker_probe_mod.link_libc = link_libc;
    addSharedSignalStackOwner(b, worker_probe_mod, target, link_libc);
    addMacosSdkPaths(b, worker_probe_mod, target);
    const worker_probe = b.addExecutable(.{ .name = "inference-worker-abi-integration", .root_module = worker_probe_mod });
    worker_probe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.inference)].?);
    const run_worker_probe = b.addRunArtifact(worker_probe);
    b.step("linked-inference-worker-integration-test", "Run actual worker CPU demand transport and parent ownership checks").dependOn(&run_worker_probe.step);

    for (runtime_library_link_order) |unit| {
        antfly_main.root_module.linkLibrary(runtime_library_artifacts[@backingInt(unit)].?);
    }
    // ELF's archive extraction must see the adapter's undefined calls before
    // the shared physical owner. Mach-O also permits this dependency order.
    if (options.native_storage) |owner| antfly_main.root_module.linkLibrary(owner);

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
        addSharedSignalStackOwner(b, role_mod, target, link_libc);
        addMacosSdkPaths(b, role_mod, target);
        role_mod.addOptions("runtime_artifact_options", role_options);
        production_antfly_imports.build_info.link(role_mod);

        const role_name = @tagName(role);
        const role_exe = b.addExecutable(.{
            .name = b.fmt("antfly-{s}", .{role_name}),
            .root_module = role_mod,
            .use_llvm = @import("../../../build_support/embedded/shared_library.zig").llvmBackend(target.result),
        });
        role_exe.link_gc_sections = true;
        switch (role) {
            .cli => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.cli)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.distributed)].?);
            },
            .data, .metadata => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.distributed)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.api_kernel)].?);
            },
            .inference => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.inference)].?);
                if (production_antfly_imports.shared_inference_core) role_exe.root_module.linkLibrary(options.native_inference);
            },
            .standalone => {
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.distributed)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.api_kernel)].?);
                role_exe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.inference)].?);
            },
        }
        if (role != .inference) {
            role_exe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.storage_kernel)].?);
            role_exe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.enrichment_compute)].?);
            role_exe.root_module.linkLibrary(runtime_library_artifacts[@backingInt(RuntimeLibraryUnit.inference)].?);
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

fn addSharedSignalStackOwner(b: *std.Build, module: *std.Build.Module, target: std.Build.ResolvedTarget, link_libc: bool) void {
    // RTLD_NEXT requires dynamic libc. Static musl targets retain Zig's
    // original per-runtime stacks and do not interpose pthread creation.
    if (!link_libc or !(target.result.os.tag == .macos or (target.result.os.tag == .linux and target.result.abi.isGnu()))) return;
    module.addCSourceFile(.{ .file = b.path("lib/platform/src/native_signal_stack.c"), .flags = &.{ "-std=c11", "-pthread" } });
    if (target.result.os.tag == .linux) module.linkSystemLibrary("dl", .{});
}
