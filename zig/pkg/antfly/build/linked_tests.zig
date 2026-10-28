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

/// Compile consumer tests independently of provider archives. Provider edits
/// invalidate the final link, but do not become inputs to test code generation.
pub const Artifact = struct {
    object: *std.Build.Step.Compile,
    executable: *std.Build.Step.Compile,

    pub fn run(self: Artifact, b: *std.Build) *std.Build.Step.Run {
        const step = b.addRunArtifact(self.executable);
        @import("../../../build_support/antfly/test_support.zig").configureTestRun(step);
        @import("../../../build_support/antfly/test_support.zig").addRuntimeTestFilters(b, step, self.object.filters);
        return step;
    }
};

pub fn add(b: *std.Build, options: std.Build.TestOptions) Artifact {
    var object_options = options;
    object_options.use_llvm = @import("../../../build_support/embedded/source_owner.zig").llvmBackend(options.root_module.resolved_target.?.result);
    object_options.max_rss = @import("runtime_memory.zig").testCompileMaxRss(options.name, b.graph.host.result, options.root_module.resolved_target.?.result, options.root_module.optimize, options.max_rss);
    object_options.emit_object = true;
    object_options.test_runner = options.test_runner orelse .{
        .path = b.path("pkg/antfly-embedded/src/test_runner.zig"),
        .mode = .simple,
    };
    const module = b.createModule(.{
        .target = options.root_module.resolved_target.?,
        .optimize = options.root_module.optimize.?,
        .link_libc = options.root_module.link_libc,
        .strip = false,
    });
    var clones = std.AutoHashMap(*std.Build.Module, *std.Build.Module).init(b.allocator);
    object_options.root_module = splitNativeSources(b, options.root_module, module, options.name, &clones);
    const object = b.addTest(object_options);
    module.addObject(object);
    const executable = b.addExecutable(.{ .name = options.name, .root_module = module, .use_llvm = object_options.use_llvm });
    return .{ .object = object, .executable = executable };
}

pub const Pair = struct {
    consumer: Artifact,
    implementation: *std.Build.Step.Compile,

    pub fn run(self: Pair, b: *std.Build) *std.Build.Step.Run {
        return runPair(b, self.consumer, self.implementation);
    }
};

pub fn addPair(b: *std.Build, options: std.Build.TestOptions, implementation: *std.Build.Step.Compile) Pair {
    var consumer_options = options;
    consumer_options.filters = @import("../../../build_support/antfly/test_support.zig").selectTestFilters(b, options.filters);
    return .{ .consumer = add(b, consumer_options), .implementation = implementation };
}

/// A public selection may span consumers and implementation tests. Inventory
/// both through Zig's run steps (including its foreign-execution support),
/// then validate the union before allowing either partition to execute.
pub fn runPair(b: *std.Build, consumer: Artifact, implementation: *std.Build.Step.Compile) *std.Build.Step.Run {
    // One physical executable serves several public selections. Derive its
    // compile selection from those callers, including their storage regression
    // patterns; no second inventory of individual test names is maintained.
    var filters: std.StringArrayHashMapUnmanaged(void) = .empty;
    for (implementation.filters) |filter| filters.put(b.allocator, filter, {}) catch @panic("OOM");
    for (consumer.object.filters) |filter| filters.put(b.allocator, filter, {}) catch @panic("OOM");
    implementation.filters = b.allocator.dupe([]const u8, filters.keys()) catch @panic("OOM");
    const audit = b.addSystemCommand(&.{"python3"});
    audit.addFileArg2(b.path("tools/audit_test_selection.py"), .{ .make_absolute = true });
    for (consumer.object.filters) |filter| audit.addArgs(&.{ "--filter", filter });
    const args = buildArguments(b) orelse &.{};
    var index: usize = 0;
    while (index < args.len) : (index += 1) {
        if (std.mem.eql(u8, args[index], "--allow-empty-test-filter")) {
            audit.addArg("--allow-empty");
        } else if (std.mem.eql(u8, args[index], "--skip-test-filter")) {
            index += 1;
            if (index >= args.len) @panic("missing skip test filter");
            audit.addArgs(&.{ "--skip-filter", args[index] });
        } else if (std.mem.startsWith(u8, args[index], "--skip-test-filter=")) {
            audit.addArgs(&.{ "--skip-filter", args[index]["--skip-test-filter=".len..] });
        }
    }
    for ([_]*std.Build.Step.Compile{ consumer.executable, implementation }) |artifact| {
        const inventory = b.addRunArtifact(artifact);
        inventory.addArgs(&.{ "--list-tests", "--allow-empty-test-filter" });
        audit.addArg("--inventory");
        audit.addFileArg2(inventory.captureStdErr(.{}), .{ .make_absolute = true });
    }
    audit.addArg("--");
    audit.addPassthruArgs();
    const implementation_run = @import("../../../build_support/antfly/test_support.zig").addFilteredTestRunArtifactWithRuntimeFilters(b, implementation, consumer.object.filters);
    implementation_run.addArgs(&.{ "--allow-empty-test-filter", "--allow-empty-owner" });
    implementation_run.step.dependOn(&audit.step);
    const consumer_run = consumer.run(b);
    consumer_run.addArgs(&.{ "--allow-empty-test-filter", "--allow-empty-owner" });
    consumer_run.step.dependOn(&implementation_run.step);
    return consumer_run;
}

/// Keep C compilation at the link boundary too. Combining C and Zig into a
/// relocatable Mach-O test object loses its debug map. Cloning the import
/// graph preserves shared module identity and leaves implementation suites
/// untouched; each native module retains its own flags and include paths.
pub fn splitNativeSources(
    b: *std.Build,
    original: *std.Build.Module,
    final: *std.Build.Module,
    name: []const u8,
    clones: *std.AutoHashMap(*std.Build.Module, *std.Build.Module),
) *std.Build.Module {
    if (clones.get(original)) |existing| return existing;
    const copy = @import("../../../build_support/embedded/source_owner.zig").cloneModule(b, original);
    copy.import_table = .empty;
    copy.link_objects = .empty;
    copy.cached_graph = .{ .modules = &.{}, .names = &.{} };
    clones.put(original, copy) catch @panic("OOM");
    const native_module = @import("../../../build_support/embedded/source_owner.zig").cloneModule(b, original);
    native_module.root_source_file = null;
    native_module.import_table = .empty;
    native_module.cached_graph = .{ .modules = &.{}, .names = &.{} };
    native_module.link_objects = .empty;
    native_module.resolved_target = original.resolved_target orelse final.resolved_target;
    native_module.optimize = original.optimize orelse final.optimize;
    for (original.link_objects.items) |link| switch (link) {
        .c_source_file, .c_source_files, .assembly_file => native_module.link_objects.append(b.allocator, link) catch @panic("OOM"),
        else => copy.link_objects.append(b.allocator, link) catch @panic("OOM"),
    };
    if (native_module.link_objects.items.len != 0) {
        const native = b.addLibrary(.{
            .name = b.fmt("{s}-native-{d}", .{ name, clones.count() }),
            .root_module = native_module,
        });
        final.linkLibrary(native);
    }
    for (original.import_table.keys(), original.import_table.values()) |key, dependency|
        copy.addImport(key, splitNativeSources(b, dependency, final, name, clones));
    @import("../../../build_support/embedded/source_owner.zig").adopt(copy);
    return copy;
}

fn buildArguments(b: *std.Build) ?[]const []const u8 {
    if (!b.available_options_map.contains("test-filter"))
        return b.option([]const []const u8, "test-filter", "Compile-time test filters (runtime filters follow --)");
    const input = b.user_input_options.get("test-filter") orelse return null;
    return switch (input) {
        .scalar => |value| blk: {
            const values = b.allocator.alloc([]const u8, 1) catch @panic("OOM");
            values[0] = value;
            break :blk values;
        },
        .list => |values| values.items,
        else => null,
    };
}

/// Resolve final native links after owners have attached their metadata and
/// runtime archives, and after ownership/inventory audits have identified the
/// test objects. Preserve the authored run arguments and prerequisites.
pub fn finalizeNativeLinks(b: *std.Build) void {
    if (b.graph.host.result.os.tag != .macos) return;
    var visited = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    var images = std.AutoHashMap(*std.Build.Step.Compile, @import("../../../build_support/embedded/shared_library.zig").NativeExecutable).init(b.allocator);
    for (b.top_level_steps.values()) |root| finalizeStep(b, &root.step, &visited, &images);
}

fn finalizeStep(
    b: *std.Build,
    step: *std.Build.Step,
    visited: *std.AutoHashMap(*std.Build.Step, void),
    images: *std.AutoHashMap(*std.Build.Step.Compile, @import("../../../build_support/embedded/shared_library.zig").NativeExecutable),
) void {
    if ((visited.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Run)) |run| {
        for (run.argv.items) |*arg| {
            if (arg.* != .artifact) continue;
            const authored = arg.artifact;
            const facade = authored.artifact;
            if (facade.kind != .exe or facade.root_module.root_source_file != null) continue;
            const target = facade.root_module.resolved_target.?.result;
            if (target.os.tag != .macos or target.cpu.arch != b.graph.host.result.cpu.arch) continue;
            const object = @import("unit_test_inventory.zig").testObject(facade) orelse continue;
            if (object.kind != .test_obj) continue;
            const entry = images.getOrPut(facade) catch @panic("OOM");
            if (!entry.found_existing) {
                const native = @import("../../../build_support/embedded/shared_library.zig");
                entry.value_ptr.* = native.nativeExecutableLink(b, object, facade.name);
                native.addNativeExecutableLinks(b, entry.value_ptr.*, object, facade.root_module);
            }
            const image = entry.value_ptr.*;
            arg.* = .{ .lazy_path = .{ .prefix = authored.prefix, .suffix = authored.suffix, .make_absolute = authored.make_absolute, .lazy_path = image.binary } };
            // addRunArtifact authored this dependency. Replace just the final
            // linker, retaining every other ordering and audit dependency.
            for (step.dependencies.items) |*dependency| {
                if (dependency.* == &facade.step) dependency.* = &image.link.step;
            }
            if (run.producer == facade) run.producer = null;
            image.binary.addStepDependencies(step);
        }
    }
    for (step.dependencies.items) |dependency| finalizeStep(b, dependency, visited, images);
}
