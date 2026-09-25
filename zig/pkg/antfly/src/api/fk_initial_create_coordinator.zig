// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! The worker selects one unfinished owner from durable metadata receipts.
//! A restart repeats at most that owner step; Raft receipt probes make it
//! idempotent without trusting a process-local cursor.
const std = @import("std");
const publication = @import("../metadata/fk_generation_publication.zig");
const owner = @import("relational_fk_generation_publication.zig");

pub const Target = union(enum) {
    seal_support,
    child: struct { table_name: []const u8, group_id: u64, action: owner.InitialChildAction },
    parent: struct { table_name: []const u8, table_id: u64, group_id: u64, action: owner.Action },
    publish_child,
};

pub fn next(value: publication.InitialPublication) !?Target {
    const work = (try value.nextWork()) orelse return null;
    return switch (work.target) {
        .seal_support => .seal_support,
        .child => |child| .{ .child = .{ .table_name = work.child_table_name, .group_id = child.group_id, .action = child.action } },
        .parent => |parent| .{ .parent = .{ .table_name = parent.table_name, .table_id = parent.table_id, .group_id = parent.group_id, .action = parent.action } },
        .publish_child => .publish_child,
    };
}

test "FK initial create coordinator resumes hidden child and parent receipts" {
    const Range = @import("../metadata/table_manager.zig").RangeRecord;
    const child_range: Range = .{ .table_id = 7, .group_id = 11, .range_id = 11, .start_key = "", .end_key = null };
    const parent_range: Range = .{ .table_id = 9, .group_id = 13, .range_id = 13, .start_key = "", .end_key = null };
    var value: publication.InitialPublication = .{
        .plan = .{
            .id = @splat(1),
            .catalog_id = 3,
            .expected_catalog_revision = 1,
            .child = .{ .table_id = 7, .name = "table:3" },
            .child_ranges = &.{child_range},
            .parents = &.{.{ .table = .{ .table_id = 9, .name = "parents" }, .ranges = &.{parent_range}, .fences = &.{undefined}, .transitions = &.{} }},
            .logical_name = "children",
            .namespace_id = 2,
        },
        .plan_digest = @splat(2),
        .candidate = undefined,
        .revision = 1,
        .phase = .preparing_support,
    };
    try std.testing.expect((try next(value)).? == .seal_support);
    value.phase = .provisioning_child;
    try std.testing.expectEqual(owner.InitialChildAction.provision, (try next(value)).?.child.action);
    value.phase = .staging_parents;
    try std.testing.expectEqual(owner.Action.stage, (try next(value)).?.parent.action);
    value.phase = .published_hidden;
    try std.testing.expectEqual(owner.InitialChildAction.release, (try next(value)).?.child.action);
    value.phase = .publishing_child;
    try std.testing.expect((try next(value)).? == .publish_child);
    value.phase = .canceling;
    try std.testing.expectEqual(owner.Action.cancel, (try next(value)).?.parent.action);
    value.parent_canceled = &.{.{ .group_id = 13, .digest = @splat(3) }};
    try std.testing.expectEqual(owner.InitialChildAction.cancel, (try next(value)).?.child.action);
    value.phase = .published;
    try std.testing.expect((try next(value)) == null);
}
