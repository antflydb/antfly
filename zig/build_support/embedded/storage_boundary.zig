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

/// Only declare contracts consumed by a compilation owner. Zig includes every
/// declared module in the cache key, even when no function calls its APIs.
pub const Profile = enum { common, owner, enrichment, all };

/// Shared contracts for independently compiled storage consumers and providers.
/// Construction declares only contract inputs; it does not build an owner.
pub const Modules = struct {
    ordinals: *std.Build.Module,
    shared_core: bool,
    engine_options: *std.Build.Step.Options,
    engine_errors: *std.Build.Module,
    test_error_logs: *std.Build.Module,
    memory: *std.Build.Module,
    failure: *std.Build.Module,
    identity: *std.Build.Module,
    owner: *std.Build.Module,
    enrichment: *std.Build.Module,
    query_client: *std.Build.Module,
    physical_sources: *std.Build.Step.Options,
    control_sources: *std.Build.Step.Options,
    direct_runtime: *std.Build.Step.Options,
    linked_runtime: *std.Build.Step.Options,

    pub fn configure(self: Modules, module: *std.Build.Module, control: bool, linked: bool) void {
        self.configureProfile(module, control, linked, .all);
    }

    /// Direct implementation tests need source selection but no ABI imports.
    pub fn configureSources(self: Modules, module: *std.Build.Module, control: bool, linked: bool) void {
        @import("source_owner.zig").attach(module);
        module.addImport("antfly_source_root", module);
        module.addImport("antfly_test_error_logs", self.test_error_logs);
        module.addImport("storage_ordinals", self.ordinals);
        module.addOptions("storage_engine_options", self.engine_options);
        module.addImport("storage_engine_errors", self.engine_errors);
        module.addOptions("storage_source_options", if (control) self.control_sources else self.physical_sources);
        module.addOptions("standalone_runtime_options", if (linked) self.linked_runtime else self.direct_runtime);
    }

    pub fn configureProfile(self: Modules, module: *std.Build.Module, control: bool, linked: bool, profile: Profile) void {
        self.configureSources(module, control, linked);
        module.addImport("runtime_memory_abi", self.memory);
        if (profile == .common) return;
        module.addImport("runtime_failure_abi", self.failure);
        module.addImport("runtime_failure_identity", self.identity);
        module.addImport("kernel_error_identity", self.identity);
        if (profile == .owner or profile == .all) {
            module.addImport("kernel_owner_abi", self.owner);
            module.addImport("local_query_client", self.query_client);
        }
        if (profile == .enrichment or profile == .all)
            module.addImport("enrichment_compute_abi", self.enrichment);
    }
};

pub fn create(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize) Modules {
    return createWithOptions(b, target, optimize, .{});
}

pub const Options = struct { sanitize_thread: bool = false, error_names: ?*std.Build.Module = null };

pub fn createWithOptions(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.lang.Optimize, options: Options) Modules {
    const supported_os = target.result.os.tag == .macos or target.result.os.tag == .linux;
    const supported_arch = target.result.cpu.arch == .aarch64 or target.result.cpu.arch == .x86_64;
    const native = supported_os and supported_arch;
    const engine_options = b.addOptions();
    engine_options.addOption(bool, "shared_storage_core", native);
    engine_options.addOption(bool, "storage_engine_owner", false);
    const ordinal_options = b.addOptions();
    ordinal_options.addOption(bool, "use_native_ordinal_core", native);
    const ordinals = b.createModule(.{ .root_source_file = b.path("pkg/antfly-embedded/src/storage/ordinals/root.zig"), .target = target, .optimize = optimize });
    ordinals.addOptions("storage_ordinal_options", ordinal_options);
    if (native) {
        const owner = b.createModule(.{ .root_source_file = b.path("pkg/antfly-embedded/src/storage/ordinals/exports.zig"), .target = target, .optimize = optimize, .pic = true, .sanitize_thread = options.sanitize_thread });
        const archive = @import("shared_library.zig").addProviderArchive(b, owner, "antfly-storage-ordinals", 1024 * 1024 * 1024);
        archive.link_function_sections = true;
        archive.link_data_sections = true;
        ordinals.linkLibrary(archive);
    }
    const local_path = b.path("pkg/antfly-embedded/src");
    const test_error_logs = b.createModule(.{ .root_source_file = local_path.path(b, "test_error_logs.zig"), .target = target, .optimize = optimize });
    const memory = b.createModule(.{ .root_source_file = b.path("lib/runtime/src/runtime_memory_abi.zig"), .target = target, .optimize = optimize });
    const failure = b.createModule(.{ .root_source_file = local_path.path(b, "runtime_failure_abi.zig"), .target = target, .optimize = optimize });
    const identity = b.createModule(.{ .root_source_file = local_path.path(b, "runtime_failure_identity.zig"), .target = target, .optimize = optimize });
    identity.addImport("runtime_failure_abi", failure);
    const owner = b.createModule(.{ .root_source_file = local_path.path(b, "storage/kernel_owner_abi.zig"), .target = target, .optimize = optimize });
    owner.addImport("runtime_failure_abi", failure);
    owner.addImport("runtime_memory_abi", memory);
    const enrichment = b.createModule(.{ .root_source_file = local_path.path(b, "storage/enrichment_compute_abi.zig"), .target = target, .optimize = optimize });
    enrichment.addImport("runtime_failure_abi", failure);
    enrichment.addImport("runtime_memory_abi", memory);
    const query_client = b.createModule(.{ .root_source_file = local_path.path(b, "storage/query_client.zig"), .target = target, .optimize = optimize });
    query_client.addImport("kernel_owner_abi", owner);
    query_client.addImport("kernel_error_identity", identity);
    const physical_sources = b.addOptions();
    physical_sources.addOption(bool, "control_only", false);
    const control_sources = b.addOptions();
    control_sources.addOption(bool, "control_only", true);
    const direct_runtime = b.addOptions();
    direct_runtime.addOption(bool, "linked_inference", false);
    direct_runtime.addOption(bool, "linked_runtime_boundaries", false);
    const linked_runtime = b.addOptions();
    linked_runtime.addOption(bool, "linked_inference", true);
    linked_runtime.addOption(bool, "linked_runtime_boundaries", true);
    return .{
        .ordinals = ordinals,
        .shared_core = native,
        .engine_options = engine_options,
        // Named callbacks preserve error identities in direct-source builds too,
        // including Wasm. Native owners can share their existing decoder module.
        .engine_errors = options.error_names orelse b.createModule(.{
            .root_source_file = b.path("pkg/inference/src/host/engine_errors.zig"),
            .target = target,
            .optimize = optimize,
        }),
        .test_error_logs = test_error_logs,
        .memory = memory,
        .failure = failure,
        .identity = identity,
        .owner = owner,
        .enrichment = enrichment,
        .query_client = query_client,
        .physical_sources = physical_sources,
        .control_sources = control_sources,
        .direct_runtime = direct_runtime,
        .linked_runtime = linked_runtime,
    };
}
