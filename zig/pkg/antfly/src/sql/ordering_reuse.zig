// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Prefix-compatible window orders can share a permutation only when the
//! consumer is peer-invariant. ROWS/navigation retain their stable tie order.
const std = @import("std");
const binding = @import("window_binding.zig");
fn prefix(weak: binding.Sort, strong: binding.Sort) bool {
    if (!std.mem.eql(usize, weak.partition, strong.partition) or weak.order.len > strong.order.len or !std.mem.eql(usize, weak.order, strong.order[0..weak.order.len])) return false;
    for (weak.directions, strong.directions[0..weak.directions.len]) |a, b| {
        if (a.descending != b.descending or (a.nulls_first orelse a.descending) != (b.nulls_first orelse b.descending)) return false;
    }
    return true;
}
fn peerInvariant(bound: anytype, index: usize) bool {
    for (bound.specs) |spec| if (spec.sort == index) {
        switch (spec.kind) {
            .rank, .dense_rank, .percent_rank, .cume_dist => {},
            .count, .sum, .min, .max, .bool_and, .bool_or => {
                if (spec.frame) |frame| {
                    if (frame.mode == .rows or frame.exclusion == .current or frame.exclusion == .ties) return false;
                }
                if ((spec.kind == .sum or spec.kind == .min or spec.kind == .max) and (spec.type == .number or spec.type == .json)) return false;
            },
            else => return false,
        }
    };
    return true;
}
pub fn plan(a: std.mem.Allocator, bound: anytype) ![]usize {
    const roots = try a.alloc(usize, bound.sorts.len);
    for (bound.sorts, roots, 0..) |weak, *root, index| {
        root.* = index;
        for (bound.sorts, 0..) |strong, candidate| {
            if (!prefix(weak, strong)) continue;
            if (strong.order.len != weak.order.len and !peerInvariant(bound, index)) continue;
            if (strong.order.len > bound.sorts[root.*].order.len or (strong.order.len == bound.sorts[root.*].order.len and candidate < root.*)) root.* = candidate;
        }
    }
    return roots;
}
test "SQL window ordering reuse retains navigation and ROWS tie semantics" {
    const a = std.testing.allocator;
    const sorts = [_]binding.Sort{
        .{ .partition = &.{0}, .order = &.{1}, .directions = &.{.{}} },
        .{ .partition = &.{0}, .order = &.{ 1, 2 }, .directions = &.{ .{}, .{} } },
        .{ .partition = &.{0}, .order = &.{1}, .directions = &.{.{ .descending = true }} },
    };
    const specs = [_]binding.Spec{.{ .kind = .rank, .arguments = &.{}, .filter = null, .sort = 0, .frame = null, .type = .integer, .star = false }};
    const bound: binding.Bound = .{ .input = undefined, .statement = undefined, .sorts = &sorts, .specs = &specs, .outputs = &.{}, .orders = &.{}, .names = &.{} };
    const roots = try plan(a, bound);
    defer a.free(roots);
    try std.testing.expectEqualSlices(usize, &.{ 1, 1, 2 }, roots);
    var changed = bound;
    var navigation = specs;
    navigation[0].kind = .lag;
    changed.specs = &navigation;
    const independent = try plan(a, changed);
    defer a.free(independent);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 2 }, independent);
}
