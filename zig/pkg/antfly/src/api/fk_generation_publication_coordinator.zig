// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! One bounded owner step per supervisor turn. The durable metadata
//! publication, not this cursor, is the source of truth after a crash.
const std = @import("std");
const publication = @import("../metadata/fk_generation_publication.zig");
const owner = @import("relational_fk_generation_publication.zig");

pub const Target = union(enum) {
    child: struct { table_name: []const u8, group_id: u64, action: owner.SourceAction },
    parent: struct { table_name: []const u8, table_id: u64, group_id: u64, action: owner.Action },
    publish_child,
};

fn contains(receipts: []const publication.Receipt, group_id: u64) bool {
    var left: usize = 0;
    var right = receipts.len;
    while (left < right) {
        const mid = left + (right - left) / 2;
        if (receipts[mid].group_id < group_id) left = mid + 1 else right = mid;
    }
    return left < receipts.len and receipts[left].group_id == group_id;
}

fn childWithoutReceipt(value: publication.Publication, receipts: []const publication.Receipt, action: owner.SourceAction) ?Target {
    for (value.plan.child_ranges) |range| {
        if (!contains(receipts, range.group_id)) return .{ .child = .{
            .table_name = value.plan.child_before.name,
            .group_id = range.group_id,
            .action = action,
        } };
    }
    return null;
}

fn parentWithoutReceipt(value: publication.Publication, receipts: []const publication.Receipt, action: owner.Action) ?Target {
    for (value.plan.parents) |parent| for (parent.ranges) |range| {
        if (!contains(receipts, range.group_id)) return .{ .parent = .{
            .table_name = parent.table.name,
            .table_id = parent.table.table_id,
            .group_id = range.group_id,
            .action = action,
        } };
    };
    return null;
}

pub fn next(value: publication.Publication) !?Target {
    return switch (value.phase) {
        .fencing_child => childWithoutReceipt(value, value.child_fenced, .fence) orelse error.GenerationPublicationChanged,
        .staging_parents => parentWithoutReceipt(value, value.parent_staged, .stage) orelse error.GenerationPublicationChanged,
        .activating_parents => parentWithoutReceipt(value, value.parent_activated, .activate) orelse error.GenerationPublicationChanged,
        .acknowledging_parents => parentWithoutReceipt(value, value.parent_acknowledged, .acknowledge) orelse error.GenerationPublicationChanged,
        .publishing_child => .publish_child,
        .installing_child => childWithoutReceipt(value, value.child_installed, .install) orelse error.GenerationPublicationChanged,
        .canceling => parentWithoutReceipt(value, value.parent_canceled, .cancel) orelse
            childWithoutReceipt(value, value.child_canceled, .cancel) orelse
            error.GenerationPublicationChanged,
        .published, .canceled => null,
    };
}

test "FK generation publication coordinator resumes from durable owner receipts" {
    const Range = @import("../metadata/table_manager.zig").RangeRecord;
    const child_range: Range = .{ .table_id = 7, .group_id = 11, .range_id = 11, .start_key = "", .end_key = null };
    const parent_range: Range = .{ .table_id = 9, .group_id = 13, .range_id = 13, .start_key = "", .end_key = null };
    var value: publication.Publication = .{
        .plan = .{
            .id = @splat(1),
            .child_before = .{ .table_id = 7, .name = "children" },
            .child_after = .{ .table_id = 7, .name = "children" },
            .child_catalog_before_b64 = "",
            .child_ranges = &.{child_range},
            .child_fences = &.{undefined},
            .parents = &.{.{ .table = .{ .table_id = 9, .name = "parents" }, .ranges = &.{parent_range}, .fences = &.{undefined}, .transitions = &.{} }},
        },
        .plan_digest = @splat(2),
        .child_identity = undefined,
        .revision = 1,
        .phase = .fencing_child,
    };
    try std.testing.expectEqual(owner.SourceAction.fence, (try next(value)).?.child.action);
    value.phase = .staging_parents;
    try std.testing.expectEqual(owner.Action.stage, (try next(value)).?.parent.action);
    value.phase = .activating_parents;
    try std.testing.expectEqual(owner.Action.activate, (try next(value)).?.parent.action);
    value.phase = .acknowledging_parents;
    try std.testing.expectEqual(owner.Action.acknowledge, (try next(value)).?.parent.action);
    value.phase = .publishing_child;
    try std.testing.expect((try next(value)).? == .publish_child);
    value.phase = .installing_child;
    try std.testing.expectEqual(owner.SourceAction.install, (try next(value)).?.child.action);
    value.phase = .canceling;
    try std.testing.expectEqual(owner.Action.cancel, (try next(value)).?.parent.action);
    value.parent_canceled = &.{.{ .group_id = 13, .digest = @splat(3) }};
    try std.testing.expectEqual(owner.SourceAction.cancel, (try next(value)).?.child.action);
    value.phase = .published;
    try std.testing.expect((try next(value)) == null);
}
