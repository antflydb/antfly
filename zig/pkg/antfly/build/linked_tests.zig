// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");

/// Compile consumer tests independently of provider archives. Provider edits
/// invalidate the final link, but do not become inputs to test code generation.
pub const Artifact = struct {
    object: *std.Build.Step.Compile,
    executable: *std.Build.Step.Compile,

    pub fn run(self: Artifact, b: *std.Build) *std.Build.Step.Run {
        const step = b.addRunArtifact(self.executable);
        @import("test_support.zig").configureTestRun(step);
        @import("test_support.zig").addRuntimeTestFilters(b, step, self.object.filters);
        return step;
    }
};

pub fn add(b: *std.Build, options: std.Build.TestOptions) Artifact {
    var object_options = options;
    object_options.emit_object = true;
    object_options.test_runner = options.test_runner orelse .{
        .path = b.path("pkg/antfly/src/test_runner.zig"),
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
    const executable = b.addExecutable(.{ .name = options.name, .root_module = module });
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
    consumer_options.filters = @import("test_support.zig").selectTestFilters(b, options.filters);
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
    audit.addFileArg(b.path("tools/audit_test_selection.py"));
    for (consumer.object.filters) |filter| audit.addArgs(&.{ "--filter", filter });
    const args = b.args orelse &.{};
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
        audit.addFileArg(inventory.captureStdErr(.{}));
    }
    const implementation_run = @import("test_support.zig").addFilteredTestRunArtifactWithRuntimeFilters(b, implementation, consumer.object.filters);
    implementation_run.addArg("--allow-empty-test-filter");
    implementation_run.step.dependOn(&audit.step);
    const consumer_run = consumer.run(b);
    consumer_run.addArg("--allow-empty-test-filter");
    consumer_run.step.dependOn(&implementation_run.step);
    return consumer_run;
}

/// Keep C compilation at the link boundary too. Combining C and Zig into a
/// relocatable Mach-O test object loses its debug map. Cloning the import
/// graph preserves shared module identity and leaves implementation suites
/// untouched; each native module retains its own flags and include paths.
fn splitNativeSources(
    b: *std.Build,
    original: *std.Build.Module,
    final: *std.Build.Module,
    name: []const u8,
    clones: *std.AutoHashMap(*std.Build.Module, *std.Build.Module),
) *std.Build.Module {
    if (clones.get(original)) |existing| return existing;
    const copy = b.allocator.create(std.Build.Module) catch @panic("OOM");
    copy.init(original.owner, .{ .existing = original });
    copy.import_table = .empty;
    copy.link_objects = .empty;
    copy.cached_graph = .{ .modules = &.{}, .names = &.{} };
    clones.put(original, copy) catch @panic("OOM");
    const native_module = b.allocator.create(std.Build.Module) catch @panic("OOM");
    native_module.init(original.owner, .{ .existing = original });
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
    return copy;
}
