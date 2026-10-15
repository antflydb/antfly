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
/// Independently compiled ELF owners and their callers share Zig callback ABI.
pub fn llvmBackend(target: std.Target) ?bool {
    return if (target.ofmt == .elf) true else null;
}

/// Module.init(.existing) copies collection pointers. Each graph clone must own
/// its mutable buffers before SDK finalizers or link composition append to them.
pub fn cloneModule(b: *std.Build, original: *std.Build.Module) *std.Build.Module {
    const copy = b.allocator.create(std.Build.Module) catch @panic("OOM");
    copy.init(original.owner, .{ .existing = original });
    copy.import_table = original.import_table.clone(b.allocator) catch @panic("OOM");
    copy.c_macros = original.c_macros.clone(b.allocator) catch @panic("OOM");
    copy.include_dirs = original.include_dirs.clone(b.allocator) catch @panic("OOM");
    copy.lib_paths = original.lib_paths.clone(b.allocator) catch @panic("OOM");
    copy.rpaths = original.rpaths.clone(b.allocator) catch @panic("OOM");
    copy.frameworks = original.frameworks.clone(b.allocator) catch @panic("OOM");
    copy.link_objects = original.link_objects.clone(b.allocator) catch @panic("OOM");
    copy.cached_graph = .{ .modules = &.{}, .names = &.{} };
    return copy;
}

const Binding = struct { consumer: *std.Build.Module, local: *std.Build.Module };
var bindings: std.ArrayList(Binding) = .empty;
var storage_provider: ?*std.Build.Step.Compile = null;
var storage_dependencies: []const *std.Build.Step.Compile = &.{};
const StorageVisit = struct { module: *std.Build.Module, testing: bool };

pub fn registerStorageProvider(provider: *std.Build.Step.Compile, dependencies: []const *std.Build.Step.Compile) void {
    storage_provider = provider;
    storage_dependencies = dependencies;
}

/// One local source owner per consumer profile. Source files are never copied
/// into a server root; source selection remains explicit at the module boundary.
pub fn attach(consumer: *std.Build.Module) void {
    consumer.addImport("antfly_decisions", @import("../../lib/decisions/build.zig").create(consumer.owner, consumer.owner.path("lib/decisions/root.zig")));
    if (consumer.import_table.contains("antfly_local_sources")) return;
    const b = consumer.owner;
    const root = consumer.root_source_file orelse return;
    const path = @import("../antfly/source_paths.zig").authored(b, root) orelse return;
    if (std.mem.indexOf(u8, path, "pkg/antfly-embedded/src/") != null) return;
    const local = b.createModule(.{
        .root_source_file = b.path("pkg/antfly-embedded/src/source_catalog.zig"),
        .target = consumer.resolved_target,
        .optimize = consumer.optimize,
    });
    const empty = b.addOptions();
    empty.addOption([]const []const u8, "names", &.{});
    local.addOptions("antfly_local_test_sources", empty);
    consumer.addImport("antfly_local_sources", local);
    bindings.append(b.allocator, .{ .consumer = consumer, .local = local }) catch @panic("OOM");
}

/// Run after composition: late-bound options and test capabilities must be the
/// same declarations as the consumer's, rather than a second configuration.
pub fn finalize(b: *std.Build) void {
    for (bindings.items) |binding| {
        // Unused literal imports also enter Zig's cache key. Select a catalog
        // that cannot declare physical inputs for this compilation profile.
        binding.local.root_source_file = b.path(if (@import("../antfly/test_partitions.zig").controlOnly(binding.consumer))
            "pkg/antfly-embedded/src/source_catalog_control.zig"
        else
            "pkg/antfly-embedded/src/source_catalog.zig");
        var imports = binding.consumer.import_table.iterator();
        while (imports.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.*, "antfly_source_root") or
                std.mem.eql(u8, entry.key_ptr.*, "antfly_local_sources")) continue;
            binding.local.addImport(entry.key_ptr.*, entry.value_ptr.*);
        }
        binding.local.addImport("antfly_source_root", binding.local);
        binding.local.addImport("antfly_test_source_root", binding.consumer);
        binding.local.addImport("antfly_server_test_sources", binding.consumer);
        binding.local.link_libc = binding.consumer.link_libc;
    }
    @import("../antfly/test_partitions.zig").add(b);
    // Static consumers compile with unresolved engine calls. Provider code is
    // a final-link input, never a prerequisite for their LLVM object emission.
    if (storage_provider) |provider| {
        var steps: std.AutoHashMap(*std.Build.Step, void) = .init(b.allocator);
        for (b.top_level_steps.values()) |root| attachStorageFinalLinks(b, &root.step, provider, &steps);
    }
}

/// Cloned consumer graphs retain their own late-bound source configuration.
pub fn adopt(consumer: *std.Build.Module) void {
    const local = consumer.import_table.get("antfly_local_sources") orelse return;
    for (bindings.items) |binding| if (binding.consumer == consumer) return;
    bindings.append(consumer.owner.allocator, .{ .consumer = consumer, .local = local }) catch @panic("OOM");
}

pub fn localFor(consumer: *std.Build.Module) ?*std.Build.Module {
    for (bindings.items) |binding| if (binding.consumer == consumer) return binding.local;
    return null;
}

fn attachStorageFinalLinks(b: *std.Build, step: *std.Build.Step, provider: *std.Build.Step.Compile, seen: *std.AutoHashMap(*std.Build.Step, void)) void {
    if ((seen.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Compile)) |artifact| {
        const consumer_target = artifact.root_module.resolved_target.?.result;
        const provider_target = provider.root_module.resolved_target.?.result;
        // Browser and cross-target artifacts share this build graph, but cannot
        // consume the native engine archive or its libc/system-library inputs.
        const same_target = consumer_target.cpu.arch == provider_target.cpu.arch and
            consumer_target.os.tag == provider_target.os.tag and
            consumer_target.abi == provider_target.abi and consumer_target.ofmt == provider_target.ofmt;
        if (same_target and (artifact.kind == .exe or artifact.kind == .@"test" or (artifact.kind == .lib and artifact.linkage == .dynamic))) {
            var linked = false;
            for (artifact.root_module.link_objects.items) |input| if (input == .other_step and input.other_step == provider) {
                linked = true;
            };
            if (!linked) {
                var modules: std.AutoHashMap(StorageVisit, void) = .init(b.allocator);
                if (needsStorageProvider(b, artifact.root_module, provider, artifact.kind == .@"test", &modules)) {
                    artifact.root_module.linkLibrary(provider);
                    linked = true;
                }
            }
            // Private engine calls resolve in the final image. Keep provider
            // dependencies here, not on static owners' codegen prerequisites.
            if (linked) for (storage_dependencies) |dependency| {
                var found = false;
                for (artifact.root_module.link_objects.items) |input| {
                    if (input == .other_step and input.other_step == dependency) found = true;
                }
                if (!found) artifact.root_module.linkLibrary(dependency);
            };
            if (linked) artifact.use_llvm = llvmBackend(consumer_target);
        }
    }
    for (step.dependencies.items) |dependency| attachStorageFinalLinks(b, dependency, provider, seen);
}

fn needsStorageProvider(b: *std.Build, module: *std.Build.Module, provider: *std.Build.Step.Compile, testing: bool, seen: *std.AutoHashMap(StorageVisit, void)) bool {
    // One module can feed both a test object and a production archive. Its
    // builtin.is_test selection differs, so visit each compilation context.
    if (module == provider.root_module or (seen.getOrPut(.{ .module = module, .testing = testing }) catch @panic("OOM")).found_existing) return false;
    if (!testing) if (module.root_source_file) |root| {
        if (@import("../antfly/source_paths.zig").authored(b, root)) |path| {
            if (module.import_table.contains("storage_engine_options") and
                std.mem.indexOf(u8, path, "pkg/antfly-embedded/src/") != null and
                !std.mem.endsWith(u8, path, "source_catalog_control.zig")) return true;
        }
    };
    for (module.link_objects.items) |input| if (input == .other_step) {
        if (input.other_step == provider) continue;
        // A shared library owns its private providers. Its consumers must not
        // inherit another copy of those private engines or their hidden calls.
        if (input.other_step.kind == .lib and input.other_step.linkage == .dynamic) continue;
        if (std.mem.eql(u8, input.other_step.name, "antfly-storage-kernel")) return true;
        // Crossing an artifact boundary selects its own builtin.is_test;
        // production libraries linked by tests still require their providers.
        if (needsStorageProvider(b, input.other_step.root_module, provider, input.other_step.kind.isTest(), seen)) return true;
    };
    var imports = module.import_table.iterator();
    while (imports.next()) |entry| if (needsStorageProvider(b, entry.value_ptr.*, provider, testing, seen)) return true;
    return false;
}
