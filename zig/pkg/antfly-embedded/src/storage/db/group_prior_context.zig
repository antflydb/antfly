// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded, deterministic prior-artifact input. Callers read an ordered
//! group-local index, never scan the source table or include later segments.
const std = @import("std");
const groups = @import("group_sources.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Position = struct {
    ordinal: i64,
    segment: []const u8,
    pub fn before(left: Position, right: Position) bool {
        return left.ordinal < right.ordinal or (left.ordinal == right.ordinal and std.mem.order(u8, left.segment, right.segment) == .lt);
    }
};
pub const Item = struct {
    group: []const u8,
    position: Position,
    unit: groups.Digest,
    unit_ordinal: i64 = 0,
    ordinal: u32,
    fingerprint: groups.Digest,
    value: V,
};
pub const Renderer = struct {
    ptr: *anyopaque,
    render: *const fn (*anyopaque, A, []const u8, V) anyerror![]const u8,
};
pub const Context = struct {
    text: []const u8,
    items: []const Item,
    fingerprint: groups.Digest,
};
fn newestFirst(_: void, left: Item, right: Item) bool {
    if (Position.before(left.position, right.position)) return false;
    if (Position.before(right.position, left.position)) return true;
    if (left.unit_ordinal != right.unit_ordinal) return left.unit_ordinal > right.unit_ordinal;
    const order = std.mem.order(u8, &left.unit, &right.unit);
    return order == .gt or (order == .eq and left.ordinal > right.ordinal);
}

/// The range reader limits candidate segments to horizon_segments and caps
/// decoded source bytes. This additional bound prevents an accidentally broad
/// adapter result from turning context selection into an unbounded sort.
pub fn select(a: A, config: groups.PriorContext, group: []const u8, current: Position, candidates: []const Item, renderer: Renderer) !Context {
    try config.validate();
    if (candidates.len > @as(usize, config.max_items) * config.horizon_segments) return error.GroupedPriorContextLimitExceeded;
    const sorted = try a.dupe(Item, candidates);
    defer a.free(sorted);
    for (sorted) |item| if (!std.mem.eql(u8, item.group, group) or !Position.before(item.position, current) or std.mem.eql(u8, item.position.segment, current.segment)) return error.InvalidGroupedPriorContext;
    std.mem.sort(Item, sorted, {}, newestFirst);
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(a);
    var items: std.ArrayList(Item) = .empty;
    defer items.deinit(a);
    var segments: usize = 0;
    var previous: ?Position = null;
    for (sorted) |item| {
        if (items.items.len == config.max_items) break;
        if (previous == null or Position.before(item.position, previous.?)) {
            if (segments == config.horizon_segments) break;
            segments += 1;
            previous = item.position;
        }
        const rendered = try renderer.render(renderer.ptr, a, config.render, item.value);
        defer a.free(rendered);
        const separator: usize = @intFromBool(items.items.len != 0);
        if (separator + rendered.len > config.max_bytes - text.items.len) break;
        if (separator != 0) try text.append(a, '\n');
        try text.appendSlice(a, rendered);
        try items.append(a, item);
    }
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    const policy = try std.json.Stringify.valueAlloc(a, config, .{});
    defer a.free(policy);
    hash.update(&groups.digest(policy));
    hash.update(&groups.digest(group));
    for (items.items) |item| {
        hash.update(&item.fingerprint);
        hash.update(&item.unit);
        var ordinal: [4]u8 = undefined;
        std.mem.writeInt(u32, &ordinal, item.ordinal, .big);
        hash.update(&ordinal);
    }
    hash.update(text.items);
    var fingerprint: groups.Digest = undefined;
    hash.final(&fingerprint);
    const selected = try items.toOwnedSlice(a);
    errdefer a.free(selected);
    return .{ .text = try text.toOwnedSlice(a), .items = selected, .fingerprint = fingerprint };
}

/// A source change seeds one bounded invalidation interval. Recomputed
/// artifacts do not extend it, preventing a transitive whole-thread cascade.
pub fn invalidated(a: A, changed: Position, ordered_later: []const Position, horizon: u16) ![]const Position {
    if (horizon == 0 or horizon > 128 or ordered_later.len > horizon) return error.InvalidGroupedPriorContext;
    var previous = changed;
    for (ordered_later) |position| {
        if (!Position.before(previous, position)) return error.InvalidGroupedPriorContext;
        previous = position;
    }
    return a.dupe(Position, ordered_later);
}

test "group prior context excludes future and foreign groups and hashes rendered history" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Render = struct {
        fn render(_: *anyopaque, alloc: A, _: []const u8, value: V) ![]const u8 {
            return alloc.dupe(u8, value.string);
        }
    };
    var ptr: u8 = 0;
    const renderer = Renderer{ .ptr = &ptr, .render = Render.render };
    const config = groups.PriorContext{ .artifact = "claims", .render = "{{statement}}", .max_items = 2, .max_bytes = 16, .horizon_segments = 2 };
    const older = Item{ .group = "thread", .position = .{ .ordinal = 1, .segment = "a" }, .unit = groups.digest("a"), .ordinal = 0, .fingerprint = groups.digest("one"), .value = .{ .string = "one" } };
    var newer = older;
    newer.position = .{ .ordinal = 2, .segment = "b" };
    newer.unit = groups.digest("b");
    newer.fingerprint = groups.digest("two");
    newer.value = .{ .string = "two" };
    const current = Position{ .ordinal = 3, .segment = "c" };
    const context = try select(a, config, "thread", current, &.{ older, newer }, renderer);
    try std.testing.expectEqualStrings("two\none", context.text);
    const reordered = try select(a, config, "thread", current, &.{ newer, older }, renderer);
    try std.testing.expectEqualSlices(u8, &context.fingerprint, &reordered.fingerprint);
    newer.value = .{ .string = "changed" };
    newer.fingerprint = groups.digest("changed");
    const changed = try select(a, config, "thread", current, &.{ older, newer }, renderer);
    try std.testing.expect(!std.mem.eql(u8, &context.fingerprint, &changed.fingerprint));
    try std.testing.expectError(error.InvalidGroupedPriorContext, select(a, config, "thread", older.position, &.{newer}, renderer));
    newer.group = "another-thread";
    try std.testing.expectError(error.InvalidGroupedPriorContext, select(a, config, "thread", current, &.{newer}, renderer));
}

test "group prior context item byte and propagation horizon limits are explicit" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const later = [_]Position{ .{ .ordinal = 2, .segment = "b" }, .{ .ordinal = 3, .segment = "c" } };
    const changed = Position{ .ordinal = 1, .segment = "a" };
    const affected = try invalidated(a, changed, &later, 2);
    try std.testing.expectEqual(@as(usize, 2), affected.len);
    try std.testing.expectError(error.InvalidGroupedPriorContext, invalidated(a, changed, &later, 1));
    try std.testing.expectError(error.InvalidGroupedPriorContext, invalidated(a, later[1], &later, 2));
}
