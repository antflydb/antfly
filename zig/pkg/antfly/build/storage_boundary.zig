// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");

/// Only declare contracts consumed by a compilation owner. Zig includes every
/// declared module in the cache key, even when no function calls its APIs.
pub const Profile = enum { common, owner, enrichment, all };

/// Shared contracts for independently compiled storage consumers and providers.
/// Construction declares only contract inputs; it does not build an owner.
pub const Modules = struct {
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
        module.addImport("antfly_source_root", module);
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

pub fn create(b: *std.Build, owner_path: std.Build.LazyPath, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) Modules {
    const memory = b.createModule(.{ .root_source_file = owner_path.path(b, "runtime_memory_abi.zig"), .target = target, .optimize = optimize });
    const failure = b.createModule(.{ .root_source_file = owner_path.path(b, "runtime_failure_abi.zig"), .target = target, .optimize = optimize });
    const identity = b.createModule(.{ .root_source_file = owner_path.path(b, "runtime_failure_identity.zig"), .target = target, .optimize = optimize });
    identity.addImport("runtime_failure_abi", failure);
    const owner = b.createModule(.{ .root_source_file = owner_path.path(b, "storage/kernel_owner_abi.zig"), .target = target, .optimize = optimize });
    owner.addImport("runtime_failure_abi", failure);
    const enrichment = b.createModule(.{ .root_source_file = owner_path.path(b, "storage/enrichment_compute_abi.zig"), .target = target, .optimize = optimize });
    enrichment.addImport("runtime_failure_abi", failure);
    enrichment.addImport("runtime_memory_abi", memory);
    const query_client = b.createModule(.{ .root_source_file = owner_path.path(b, "storage/local_query_client.zig"), .target = target, .optimize = optimize });
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
