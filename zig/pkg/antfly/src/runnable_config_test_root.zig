// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
pub const antfly_sources = @import("source_owner_embedded_db.zig");
const config = @import("antfly_local_sources").common_config;
const runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const openapi = @import("antfly_common_openapi");

test "config runnable scheduler public DTO roundtrip preserves managed startup capacity" {
    const allocator = std.testing.allocator;
    const dto: openapi.RunnableSchedulingConfigValue = .{ .slots = 3, .control_slots = 1, .cleanup_slots = 1, .max_operations = 4 };
    const body = try std.json.Stringify.valueAlloc(allocator, .{ .admission = .{ .runnable = dto } }, .{});
    defer allocator.free(body);
    var parsed = try config.Config.parseFromSlice(allocator, body);
    defer parsed.deinit();
    var scheduler = try runnable.Scheduler.init(parsed.admission.runnable.?);
    defer scheduler.close();
    try scheduler.ensureCleanupCpuBeforePublication();
    const policy = scheduler.policyAndStats().config;
    try std.testing.expectEqual(@as(usize, 3), policy.slots);
    try std.testing.expectEqual(@as(usize, 1), policy.cleanup_slots);
    try std.testing.expectEqual(@as(usize, 1), policy.ordinarySlots());
    var implicit = try config.Config.parseFromSlice(allocator,
        \\{"admission":{"runnable":{"slots":3,"max_operations":4}}}
    );
    defer implicit.deinit();
    var implicit_scheduler = try runnable.Scheduler.init(implicit.admission.runnable.?);
    defer implicit_scheduler.close();
    try implicit_scheduler.ensureCleanupCpuBeforePublication();
    try std.testing.expectEqualDeep(policy, implicit_scheduler.policyAndStats().config);
    for ([_][]const u8{
        \\{"admission":{"runnable":{"slots":2}}}
        ,
        \\{"admission":{"runnable":{"slots":3,"control_slots":2}}}
        ,
        \\{"admission":{"runnable":{"cleanup_slots":2}}}
        ,
        \\{"admission":{"runnable":{"cleanup_slots":-1}}}
        ,
        \\{"admission":{"runnable":{"max_operations":2}}}
        ,
        \\{"admission":{"runnable":{"max_operations":3}}}
        ,
    }) |invalid| try std.testing.expectError(error.InvalidConfig, config.Config.parseFromSlice(allocator, invalid));
    var disabled = try config.Config.parseFromSlice(allocator, "{}");
    defer disabled.deinit();
    try std.testing.expect(disabled.admission.runnable == null);
}
