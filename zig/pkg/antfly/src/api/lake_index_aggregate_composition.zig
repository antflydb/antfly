// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Compose independently authenticated reducer slots without assuming that
//! spilled materializations enumerate groups in the same order.
const std = @import("std");
const local = @import("antfly_local_sources");
const Cursor = local.sql_catalog.AggregatePartialCursor;
const Recipe = local.sql_aggregate_materialization.Recipe;
const Datum = local.sql_scalar.Datum;
const GroupResult = local.sql_operators.GroupResult;
const A = std.mem.Allocator;

pub const Factory = struct {
    ptr: *anyopaque,
    /// Every child is already selected from the same complete publication.
    /// Opening is lazy so at most one root and block are retained at a time.
    open: *const fn (*anyopaque, usize) anyerror!Cursor,
};

pub const Composition = struct {
    a: A,
    factory: Factory,
    width: usize,
    child: ?Cursor = null,
    slot: usize = 0,
    consumed: [256]bool = @splat(false),

    pub fn create(a: A, recipe: Recipe, factory: Factory) !*Composition {
        if (recipe.inputs.len == 0 or recipe.inputs.len > 256) return error.InvalidSqlBackendResponse;
        for (recipe.inputs) |input| {
            if (input.spec.distinct or input.spec.kind == .pattern_set) return error.UnsupportedNativeAggregateComposition;
        }
        const self = try a.create(Composition);
        self.* = .{ .a = a, .factory = factory, .width = recipe.inputs.len };
        return self;
    }
    pub fn cursor(self: *Composition) Cursor {
        return .{ .ptr = self, .next = next, .close = close };
    }
    fn next(raw: *anyopaque, a: A, maximum: u32) !?[]const GroupResult {
        const self: *Composition = @ptrCast(@alignCast(raw));
        if (maximum == 0) return error.InvalidSqlLimit;
        while (self.slot < self.width) {
            if (self.child == null) {
                if (self.consumed[self.slot]) {
                    self.slot += 1;
                    continue;
                }
                self.child = try self.factory.open(self.factory.ptr, self.slot);
            }
            const child = self.child.?;
            const page = (try child.next(child.ptr, a, maximum)) orelse {
                child.close(child.ptr);
                self.child = null;
                self.slot += 1;
                continue;
            };
            if (page.len == 0 or page.len > maximum) return error.InvalidSqlBackendResponse;
            const rows = try a.alloc(GroupResult, page.len);
            const slots = try a.dupe(u16, &.{@intCast(self.slot)});
            for (rows, page) |*row, input| {
                const mapping = input.aggregate_slots orelse slots;
                if (input.aggregates.len != mapping.len or mapping.len == 0) return error.InvalidSqlBackendResponse;
                var has_current = false;
                for (mapping, 0..) |slot, i| {
                    if (slot >= self.width) return error.InvalidSqlBackendResponse;
                    for (mapping[0..i]) |previous| if (previous == slot) return error.InvalidSqlBackendResponse;
                    has_current = has_current or slot == self.slot;
                    self.consumed[slot] = true;
                }
                if (!has_current) return error.InvalidSqlBackendResponse;
                row.* = .{ .keys = input.keys, .aggregates = input.aggregates, .ordinal = input.ordinal, .aggregate_slots = mapping };
            }
            return rows;
        }
        return null;
    }
    fn close(raw: *anyopaque) void {
        const self: *Composition = @ptrCast(@alignCast(raw));
        if (self.child) |child| child.close(child.ptr);
        const a = self.a;
        a.destroy(self);
    }
};

test "external lake aggregate composition preserves exact slots across different group orders" {
    const a = std.testing.allocator;
    const specs = [_]local.sql_operators.AggregateSpec{ .{ .kind = .sum, .input_type = .integer }, .{ .kind = .count } };
    const recipe: Recipe = .{ .keys = &.{.{ .path = "key", .type = .integer, .nullable = false }}, .inputs = &.{ .{ .spec = specs[0], .column = .{ .path = "value", .type = .integer, .nullable = true } }, .{ .spec = specs[1], .column = null } } };
    var source = struct {
        groups: [2]*local.sql_operators.Grouped,
        position: usize = 0,
        opened: usize = 0,
        closed: usize = 0,
        active: bool = false,
        fn open(raw: *anyopaque, slot: usize) !Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.active);
            self.active = true;
            self.opened += 1;
            self.position = slot;
            return .{ .ptr = self, .next = pull, .close = closeChild };
        }
        fn pull(raw: *anyopaque, alloc: A, _: u32) !?[]const GroupResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const row = (try self.groups[self.position].nextPartialResult(alloc)) orelse return null;
            return try alloc.dupe(GroupResult, &.{row});
        }
        fn closeChild(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.active = false;
            self.closed += 1;
        }
    }{ .groups = .{ try local.sql_operators.Grouped.create(a, specs[0..1], .{}), try local.sql_operators.Grouped.create(a, specs[1..2], .{}) } };
    defer for (source.groups) |group| group.deinit();
    for ([_]i64{ 1, 2 }) |key| try source.groups[0].add(&.{Datum.fromJson(.{ .integer = key })}, &.{Datum.fromJson(.{ .integer = 9007199254740993 })});
    for ([_]i64{ 2, 1 }) |key| try source.groups[1].add(&.{Datum.fromJson(.{ .integer = key })}, &.{Datum.fromJson(.{ .integer = 1 })});
    const composition = try Composition.create(a, recipe, .{ .ptr = &source, .open = @TypeOf(source).open });
    const cursor = composition.cursor();
    defer cursor.close(cursor.ptr);
    const result = try local.sql_operators.Grouped.create(a, &specs, .{});
    defer result.deinit();
    while (true) {
        var page = std.heap.ArenaAllocator.init(a);
        defer page.deinit();
        const rows = (try cursor.next(cursor.ptr, page.allocator(), 1)) orelse break;
        for (rows) |row| try result.importPartialMapped(row.keys, row.aggregates, row.aggregate_slots, row.ordinal);
    }
    try std.testing.expectEqual(@as(usize, 2), source.opened);
    try std.testing.expectEqual(@as(usize, 2), source.closed);
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    var count: usize = 0;
    while (try result.nextResult(output.allocator())) |row| {
        count += 1;
        try std.testing.expectEqual(@as(i64, 9007199254740993), row.aggregates[0].value.integer);
        try std.testing.expectEqual(@as(i64, 1), row.aggregates[1].value.integer);
    }
    try std.testing.expectEqual(@as(usize, 2), count);
}
