// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Maximum-sized role-scope validation and permutation-insensitive no-op
//! publication comparison. Setup allocation is outside the measured loop.
const std = @import("std");
const settings = @import("sql_settings");
test "role default validation benchmark" {
    const a = std.testing.allocator;
    const count = 1024;
    const iterations = 64;
    const roles = try a.alloc(settings.RoleDefault, count);
    defer a.free(roles);
    var initialized: usize = 0;
    defer for (roles[0..initialized]) |role| a.free(role.database);
    for (roles, 0..) |*role, index| {
        role.* = .{ .principal = "alice", .database = try std.fmt.allocPrint(a, "database_{d}", .{index}), .value = .{ .string = "value" } };
        initialized += 1;
    }
    const comparison = try a.dupe(settings.RoleDefault, roles);
    defer a.free(comparison);
    std.mem.reverse(settings.RoleDefault, comparison);
    const record: settings.Record = .{ .identity = .{ .id = 1, .generation = 1 }, .name = "app.mode", .kind = .string, .default = .{ .string = "base" }, .role_defaults = roles };
    const input: settings.Input = .{ .name = record.name, .kind = record.kind, .default = record.default, .role_defaults = comparison };
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..iterations) |_| {
        try record.validate();
        try std.testing.expect(input.matches(record));
    }
    const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
    std.debug.print("ROLE_DEFAULT_VALIDATION scopes={} iterations={} elapsed_ns={} per_iteration_ns={}\n", .{ count, iterations, elapsed, @divTrunc(elapsed, iterations) });
}
