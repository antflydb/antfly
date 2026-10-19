// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Borrow the original execution and its retained read-policy guard during output.
const std = @import("std");
const sources = @import("antfly_local_sources");
const scheduler = sources.common_request_admission.runnable_scheduler;
const admission = sources.common_bounded_admission;
const Control = @import("sql_json_source.zig").Control;

pub const Owner = struct {
    execution: *scheduler.Execution,
    options: admission.Options,
    validator: struct { ptr: *anyopaque, check: *const fn (*anyopaque, bool) anyerror!void },
    failure: ?anyerror = null,
    pub fn control(self: *Owner) Control {
        return .{ .ptr = self, .check = check };
    }
    fn check(raw: *anyopaque) !void {
        try @as(*Owner, @ptrCast(@alignCast(raw))).checkWork(false);
    }
    pub fn finish(self: *Owner) !void {
        try self.checkWork(true);
    }
    fn checkWork(self: *Owner, force: bool) !void {
        if (self.failure) |err| return err;
        self.checkOnce(force) catch |err| {
            self.failure = err;
            return err;
        };
    }
    fn checkOnce(self: *Owner, force: bool) !void {
        try self.options.check();
        try self.execution.checkpoint();
        try self.validator.check(self.validator.ptr, force);
        try self.options.check();
    }
};

test "SQL output owner preserves root and revalidates retained policy after serialization yields" {
    const Guard = sources.sql_read_fence.Guard;
    var execution: scheduler.Execution = .{};
    var node = try scheduler.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 1 });
    try execution.start(&node, .{ .io = std.testing.io, .retained_bytes = 1024 }, .query);
    defer execution.finish();
    const entry = &execution.entry.?;
    var guard = Guard.init(std.testing.allocator);
    defer guard.deinit();
    try guard.capture(.{ .database = "default", .namespace = "public", .name = "docs", .physical = "table-7", .id = 7, .schema_version = 1, .catalog_revision = 19, .filter = "tenant=original" });
    const Current = struct {
        execution: *scheduler.Execution,
        guard: *Guard,
        checks: usize = 0,
        changed: bool = false,
        fn validate(raw: *anyopaque, _: std.mem.Allocator, tables: []const sources.sql_read_fence.Table) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            try std.testing.expectEqualStrings("tenant=original", tables[0].filter.?);
            if (self.changed) return error.CatalogGenerationChanged;
        }
        fn check(raw: *anyopaque, force: bool) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.guard.check(std.testing.allocator, self.execution.resume_generation, force, .{ .ptr = self, .check = validate });
        }
    };
    var current: Current = .{ .execution = &execution, .guard = &guard };
    var owner: Owner = .{ .execution = &execution, .options = entry.options, .validator = .{ .ptr = &current, .check = Current.check } };
    const text: [16384]u8 = @splat('x');
    const encoded = try @import("sql_json_output.zig").valueAlloc(std.testing.allocator, .{ .value = &text }, .{}, owner.control());
    defer std.testing.allocator.free(encoded);
    try owner.finish();
    try std.testing.expect(current.checks > 4);
    try std.testing.expect(entry == &execution.entry.?);
    try std.testing.expectEqual(@as(usize, 1), node.stats().operations);
    try std.testing.expectEqual(@as(u64, 1024), node.stats().retained_bytes);
    current.changed = true;
    try std.testing.expectError(error.CatalogGenerationChanged, owner.finish());
    const checks = current.checks;
    current.changed = false;
    try std.testing.expectError(error.CatalogGenerationChanged, owner.finish());
    try std.testing.expectEqual(checks, current.checks);
}
