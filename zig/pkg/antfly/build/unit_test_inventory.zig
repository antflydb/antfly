// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");

/// Inventory the selected run nodes, including linked consumers and DB-core's
/// two runtime partitions. Merely listing a compiled artifact is insufficient:
/// compile filters, suite filters and runtime exclusions all affect ownership.
pub fn add(b: *std.Build, aggregate: *std.Build.Step, baseline: ?*std.Build.Step, other_roots: []const *std.Build.Step) void {
    const audit = b.addSystemCommand(&.{"python3"});
    audit.addFileArg(b.path("tools/audit_unit_test_ownership.py"));
    if (b.option(bool, "unit-test-inventory-allow-overlap", "Report overlapping unit ownership without failing (migration diagnostics)") orelse false)
        audit.addArg("--allow-overlap");
    var visited = std.AutoHashMap(*std.Build.Step, void).init(b.allocator);
    var count: usize = 0;
    collect(b, aggregate, audit, &visited, &count, "--inventory");
    for (other_roots) |root| collect(b, root, audit, &visited, &count, "--inventory");
    if (baseline) |before| {
        visited.clearRetainingCapacity();
        count = 0;
        collect(b, before, audit, &visited, &count, "--baseline-inventory");
        for (other_roots) |root| collect(b, root, audit, &visited, &count, "--baseline-inventory");
    }
    audit.addArg("--report");
    const report = audit.addOutputFileArg("unit-test-inventory.json");
    const step = b.step("unit-test-inventory", "Audit unique ownership across all four CI unit gates");
    step.dependOn(&b.addInstallFile(report, "unit-test-inventory.json").step);
}

pub fn testObject(artifact: *std.Build.Step.Compile) ?*std.Build.Step.Compile {
    if (artifact.kind == .@"test" or artifact.kind == .test_obj) return artifact;
    if (artifact.kind != .exe or artifact.root_module.root_source_file != null) return null;
    for (artifact.root_module.link_objects.items) |link| {
        if (link == .other_step and (link.other_step.kind == .@"test" or link.other_step.kind == .test_obj)) return link.other_step;
    }
    return null;
}

fn collect(b: *std.Build, step: *std.Build.Step, audit: *std.Build.Step.Run, visited: *std.AutoHashMap(*std.Build.Step, void), count: *usize, inventory_arg: []const u8) void {
    if ((visited.getOrPut(step) catch @panic("OOM")).found_existing) return;
    if (step.cast(std.Build.Step.Run)) |run| blk: {
        for (run.argv.items) |arg| {
            if (arg == .bytes and std.mem.eql(u8, arg.bytes, "--list-tests")) break :blk;
        }
        for (run.argv.items) |arg| {
            if (arg != .artifact) continue;
            const object = testObject(arg.artifact.artifact) orelse continue;
            const source = object.root_module.root_source_file orelse continue;
            const path = switch (source) {
                .src_path => |p| p.sub_path,
                else => continue,
            };
            const list = std.Build.Step.Run.create(b, b.fmt("inventory {s}", .{path}));
            const runner_path = if (object.test_runner) |runner| switch (runner.path) {
                .src_path => |p| p.sub_path,
                else => @panic("unknown unit test runner path"),
            } else "";
            const inference_runner = std.mem.endsWith(u8, runner_path, "test_runner_filter.zig");
            const protocol_runner = object.test_runner == null or object.test_runner.?.mode != .simple;
            if (protocol_runner or inference_runner) {
                list.addArg("python3");
                list.addFileArg(b.path("tools/audit_unit_test_ownership.py"));
                list.addArg(if (protocol_runner) "--protocol-executable" else "--inference-executable");
                list.addArtifactArg(arg.artifact.artifact);
                if (inference_runner) {
                    list.addArg("--");
                    for (run.argv.items[1..]) |value| {
                        if (value != .bytes) @panic("unexpected inference test argument");
                        list.addArg(value.bytes);
                    }
                }
            } else {
                if (!std.mem.endsWith(u8, runner_path, "antfly/src/test_runner.zig")) @panic("unit inventory needs an adapter for this test runner");
                // Preserve actual filters on simple runners and linked executables.
                for (run.argv.items) |value| switch (value) {
                    .bytes => |bytes| list.addArg(bytes),
                    .artifact => |a| list.addPrefixedArtifactArg(a.prefix, a.artifact),
                    .lazy_path => |p| list.addPrefixedFileArg(p.prefix, p.lazy_path),
                    else => @panic("unexpected unit test inventory argument"),
                };
                if (run.producer == null) {
                    const has_separator = for (run.argv.items) |value| {
                        if (value == .bytes and std.mem.eql(u8, value.bytes, "--")) break true;
                    } else false;
                    if (!has_separator) list.addArg("--");
                }
                list.addArgs(&.{ "--list-tests", "--allow-empty-test-filter" });
            }
            list.environ_map = run.environ_map;
            list.cwd = run.cwd;
            const selection = @import("unit_test_ownership.zig").selection(run, object);
            audit.addArgs(&.{ inventory_arg, b.fmt("{s} [{s}] #{d}", .{ path, selection, count.* }) });
            audit.addFileArg(list.captureStdErr(.{}));
            count.* += 1;
            break;
        }
    }
    for (step.dependencies.items) |dependency| collect(b, dependency, audit, visited, count, inventory_arg);
}
