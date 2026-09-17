// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const inventory = @import("unit_test_inventory.zig");
const Step = std.Build.Step;

pub const Rule = struct {
    source: []const u8,
    artifact: []const u8,
    /// The first runtime test filter (or first compile filter for unfiltered runs).
    selection: []const u8,
    skip: []const []const u8,
};

const rules = @import("unit_test_ownership_rules.zig").rules;

/// Give the aggregate its own selections. Focused targets keep their original
/// run nodes, arguments, and transitive prerequisites. Compiler artifacts are
/// shared; cloning a run never creates another compiler invocation.
pub fn apply(b: *std.Build, aggregate: *Step) *Step {
    const before = b.allocator.create(Step) catch @panic("OOM");
    before.* = Step.init(.{ .id = .top_level, .name = "unit ownership baseline", .owner = b });
    before.dependencies.appendSlice(aggregate.dependencies.items) catch @panic("OOM");
    var copies = std.AutoHashMap(*Step, *Step).init(b.allocator);
    for (aggregate.dependencies.items) |*dependency| dependency.* = copySelected(b, dependency.*, &copies);
    return before;
}

pub fn selection(run: *Step.Run, object: *Step.Compile) []const u8 {
    for (run.argv.items, 0..) |arg, index| {
        if (arg != .bytes) continue;
        if (std.mem.startsWith(u8, arg.bytes, "--test-filter=")) return arg.bytes["--test-filter=".len..];
        if (std.mem.eql(u8, arg.bytes, "--test-filter") and index + 1 < run.argv.items.len) {
            const next = run.argv.items[index + 1];
            if (next == .bytes) return next.bytes;
        }
    }
    return if (object.filters.len > 0) object.filters[0] else "all";
}

fn copySelected(b: *std.Build, original: *Step, copies: *std.AutoHashMap(*Step, *Step)) *Step {
    if (copies.get(original)) |copy| return copy;
    // Compilation/code generation is shared with focused targets.
    if (original.id != .run and original.id != .top_level) return original;
    const original_run = original.cast(Step.Run);
    var skips: []const []const u8 = &.{};
    if (original_run) |run| blk: {
        for (run.argv.items) |arg| {
            if (arg == .bytes and std.mem.eql(u8, arg.bytes, "--list-tests")) break :blk;
        }
        for (run.argv.items) |arg| {
            if (arg != .artifact) continue;
            const object = inventory.testObject(arg.artifact.artifact) orelse continue;
            const source = object.root_module.root_source_file orelse continue;
            const path = switch (source) {
                .src_path => |p| p.sub_path,
                else => continue,
            };
            for (rules) |rule| {
                if (std.mem.eql(u8, path, rule.source) and std.mem.eql(u8, object.name, rule.artifact) and std.mem.eql(u8, selection(run, object), rule.selection)) {
                    skips = rule.skip;
                    break;
                }
            }
            break;
        }
    }
    var dependencies = std.array_list.Managed(*Step).init(b.allocator);
    var changed = skips.len != 0;
    for (original.dependencies.items) |dependency| {
        const copy = copySelected(b, dependency, copies);
        changed = changed or copy != dependency;
        dependencies.append(copy) catch @panic("OOM");
    }
    if (!changed) {
        copies.put(original, original) catch @panic("OOM");
        return original;
    }
    const copy: *Step = if (original_run) |run| blk: {
        const cloned = Step.Run.create(b, original.name);
        const fresh_step = cloned.step;
        cloned.* = run.*;
        cloned.step = fresh_step;
        cloned.argv = .fromOwnedSlice(b.allocator.dupe(Step.Run.Arg, run.argv.items) catch @panic("OOM"));
        if (run.stdio == .check) cloned.stdio = .{ .check = run.stdio.check.clone(b.allocator) catch @panic("OOM") };
        cloned.captured_stdout = null;
        cloned.captured_stderr = null;
        if (run.captured_stdout != null) _ = cloned.captureStdOut(.{});
        if (run.captured_stderr != null) _ = cloned.captureStdErr(.{});
        for (skips) |filter| {
            if (run.producer == null) {
                // Partition-wrapper options must precede forwarded user args.
                const index = for (cloned.argv.items, 0..) |arg, i| {
                    if (arg == .bytes and std.mem.eql(u8, arg.bytes, "--")) break i;
                } else cloned.argv.items.len;
                cloned.argv.insertSlice(b.allocator, index, &.{
                    .{ .bytes = b.dupe("--common-skip-filter") },
                    .{ .bytes = b.dupe(filter) },
                }) catch @panic("OOM");
            } else cloned.addArgs(&.{ "--skip-test-filter", filter });
        }
        break :blk &cloned.step;
    } else blk: {
        const cloned = b.allocator.create(Step) catch @panic("OOM");
        cloned.* = Step.init(.{ .id = .top_level, .name = original.name, .owner = b });
        break :blk cloned;
    };
    copy.max_rss = original.max_rss;
    copy.dependencies = dependencies;
    copies.put(original, copy) catch @panic("OOM");
    return copy;
}
