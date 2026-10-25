// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Portable typed range evidence; never interprets private IDs as public ties.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const Native = local.segment.SegmentIndexSortBoundValue;
pub const Bound = struct {
    pub const Value = struct {
        kind: enum { null_value, datetime_ns, u64_val, i64_val, f64_val, bool_val, bytes_val, id },
        text: []const u8,
        pub fn from(a: A, v: Native) !Value {
            return switch (v) {
                .null_value => .{ .kind = .null_value, .text = "" },
                inline .datetime_ns, .u64_val, .i64_val, .f64_val => |number, tag| .{ .kind = @field(@FieldType(Value, "kind"), @tagName(tag)), .text = try std.fmt.allocPrint(a, "{d}", .{number}) },
                .bool_val => |boolean| .{ .kind = .bool_val, .text = if (boolean) "true" else "false" },
                .bytes_val => |bytes| .{ .kind = .bytes_val, .text = try a.dupe(u8, bytes) },
                .id => |id| .{ .kind = .id, .text = try a.dupe(u8, id) },
            };
        }
        pub fn native(self: Value) !Native {
            return switch (self.kind) {
                .null_value => if (self.text.len == 0) .null_value else error.InvalidNativeLakeTextCorpus,
                .datetime_ns => .{ .datetime_ns = try std.fmt.parseInt(i128, self.text, 10) },
                .u64_val => .{ .u64_val = try std.fmt.parseInt(u64, self.text, 10) },
                .i64_val => .{ .i64_val = try std.fmt.parseInt(i64, self.text, 10) },
                .f64_val => .{ .f64_val = try std.fmt.parseFloat(f64, self.text) },
                .bool_val => if (std.mem.eql(u8, self.text, "true")) .{ .bool_val = true } else if (std.mem.eql(u8, self.text, "false")) .{ .bool_val = false } else error.InvalidNativeLakeTextCorpus,
                .bytes_val => .{ .bytes_val = self.text },
                .id => .{ .id = self.text },
            };
        }
    };
    fields: []const local.segment.SegmentIndexSortField,
    first: []const Value,
    last: []const Value,
    pub fn fromReader(a: A, reader: *const local.segment.SegmentReader) !?Bound {
        const fields = (try reader.indexSortFieldsAlloc(a)) orelse return null;
        const bounds = (try reader.indexSortBoundsAlloc(a)) orelse return null;
        if (fields.len == 0 or fields.len > 32 or fields.len != bounds.first.len or fields.len != bounds.last.len) return null;
        var bytes: usize = 0;
        const first = try a.alloc(Value, fields.len);
        const last = try a.alloc(Value, fields.len);
        for (fields, first, last, bounds.first, bounds.last) |field, *low, *high, left, right| {
            low.* = try Value.from(a, left);
            high.* = try Value.from(a, right);
            bytes +|= field.field.len + low.text.len + high.text.len + 128;
        }
        return if (bytes <= 4096) .{ .fields = fields, .first = first, .last = last } else null;
    }
    pub fn reversal(self: Bound, a: A, order: local.storage_db_query_search_exec.SearchHitOrder) !?bool {
        if (self.fields.len > 32 or self.fields.len != self.first.len or self.fields.len != self.last.len) return null;
        const first = try a.alloc(Native, self.fields.len);
        const last = try a.alloc(Native, self.fields.len);
        for (first, last, self.first, self.last) |*left, *right, low, high| {
            left.* = try low.native();
            right.* = try high.native();
        }
        return order.nativeBoundsReversal(self.fields, .{ .first = first, .last = last });
    }
    fn nativeBounds(self: Bound, a: A) !local.segment.SegmentIndexSortBounds {
        if (self.fields.len > 32 or self.fields.len != self.first.len or self.fields.len != self.last.len) return error.InvalidNativeLakeTextCorpus;
        const first = try a.alloc(Native, self.fields.len);
        const last = try a.alloc(Native, self.fields.len);
        for (first, last, self.first, self.last) |*l, *r, low, high| {
            l.* = try low.native();
            r.* = try high.native();
        }
        return .{ .first = first, .last = last };
    }
    pub fn compareBest(self: Bound, other: Bound, a: A, order: local.storage_db_query_search_exec.SearchHitOrder) !std.math.Order {
        return order.compareNativeBestBounds(a, self.fields, try self.nativeBounds(a), other.fields, try other.nativeBounds(a));
    }
    pub fn mayCompete(self: Bound, a: A, order: local.storage_db_query_search_exec.SearchHitOrder, cutoff: []const std.json.Value) !bool {
        if (self.fields.len > 32 or self.fields.len != self.first.len or self.fields.len != self.last.len) return error.InvalidNativeLakeTextCorpus;
        const first = try a.alloc(Native, self.fields.len);
        const last = try a.alloc(Native, self.fields.len);
        for (first, last, self.first, self.last) |*left, *right, low, high| {
            left.* = try low.native();
            right.* = try high.native();
        }
        return order.nativeBoundsMayCompete(self.fields, .{ .first = first, .last = last }, cutoff);
    }
};

test "external lake global frontier portable sort values retain signed and exact domains" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const values = [_]Native{ .{ .datetime_ns = -922337203685477580900 }, .{ .u64_val = std.math.maxInt(u64) }, .{ .i64_val = std.math.minInt(i64) }, .{ .f64_val = -1.2345678901234567e-200 }, .{ .bool_val = true }, .{ .bytes_val = "a\x00b" }, .{ .id = "private-id" } };
    for (values) |value| {
        const encoded = try std.json.Stringify.valueAlloc(a, try Bound.Value.from(a, value), .{});
        const restored = try std.json.parseFromSliceLeaky(Bound.Value, a, encoded, .{});
        try std.testing.expectEqualDeep(value, try restored.native());
    }
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, (Bound.Value{ .kind = .bool_val, .text = "TRUE" }).native());
}
