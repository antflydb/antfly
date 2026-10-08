// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Union of changed-file dependencies across independently bounded builders.
//! Previous coverage is supplied only after the builder's ordinary seed proof.
const std = @import("std");
const A = std.mem.Allocator;
pub const File = struct { id: []const u8, digest: [32]u8 };
pub const Coverage = struct { files: []const []const u8, fingerprints: []const [32]u8 };
pub const Requirement = struct { reused: bool = false, previous: ?Coverage = null };
pub fn requiredFiles(a: A, files: []const File, requirements: []const Requirement) ![]bool {
    const needed = try a.alloc(bool, files.len);
    errdefer a.free(needed);
    @memset(needed, false);
    var old: std.StringHashMapUnmanaged([32]u8) = .empty;
    defer old.deinit(a);
    for (requirements) |requirement| {
        if (requirement.reused) continue;
        const coverage = requirement.previous orelse {
            @memset(needed, true);
            return needed;
        };
        if (coverage.files.len != coverage.fingerprints.len) return error.InvalidNativeLakeFileState;
        old.clearRetainingCapacity();
        for (coverage.files, coverage.fingerprints) |id, digest| {
            const entry = try old.getOrPut(a, id);
            if (entry.found_existing) return error.InvalidNativeLakeFileState;
            entry.value_ptr.* = digest;
        }
        for (files, needed) |file, *required| {
            required.* = required.* or if (old.get(file.id)) |digest| !std.mem.eql(u8, &digest, &file.digest) else true;
        }
    }
    return needed;
}

test "cohort replay unions changes and admits new recipes without omitting files under OOM" {
    const Probe = struct {
        fn run(a: A) !void {
            const files = [_]File{ .{ .id = "a", .digest = @splat(1) }, .{ .id = "b", .digest = @splat(2) }, .{ .id = "new", .digest = @splat(3) } };
            const previous: Coverage = .{ .files = &.{ "a", "b", "deleted" }, .fingerprints = &.{ @splat(1), @splat(9), @splat(4) } };
            var plans: [17]Requirement = @splat(.{ .previous = previous });
            plans[0] = .{ .reused = true };
            const changed = try requiredFiles(a, &files, &plans);
            defer a.free(changed);
            try std.testing.expectEqualSlices(bool, &.{ false, true, true }, changed);
            plans[16] = .{}; // A new or incompatible index must see every file.
            const complete = try requiredFiles(a, &files, &plans);
            defer a.free(complete);
            try std.testing.expectEqualSlices(bool, &.{ true, true, true }, complete);
            @memset(&plans, .{ .reused = true });
            const reused = try requiredFiles(a, &files, &plans);
            defer a.free(reused);
            try std.testing.expectEqualSlices(bool, &.{ false, false, false }, reused);
        }
    };
    try Probe.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
    try std.testing.expectError(error.InvalidNativeLakeFileState, requiredFiles(std.testing.allocator, &.{}, &.{.{ .previous = .{ .files = &.{"a"}, .fingerprints = &.{} } }}));
}
