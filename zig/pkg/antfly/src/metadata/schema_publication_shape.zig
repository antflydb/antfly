// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Independent shape checks for immutable, schema-only publication plans.
const std = @import("std");
const records = @import("antfly_local_sources").common_topology_records;
const tables = @import("table_manager.zig");
const Value = std.json.Value;

/// Object order is irrelevant; array order and numerical representation are
/// not silently rewritten. Depth is bounded independently of parser settings.
pub fn equal(a: Value, b: Value, depth: usize) bool {
    if (depth > 128 or std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null => true,
        .bool => |v| v == b.bool,
        .integer => |v| v == b.integer,
        .float => |v| v == b.float,
        .number_string => |v| std.mem.eql(u8, v, b.number_string),
        .string => |v| std.mem.eql(u8, v, b.string),
        .array => |v| blk: {
            if (v.items.len != b.array.items.len) break :blk false;
            for (v.items, b.array.items) |left, right| if (!equal(left, right, depth + 1)) break :blk false;
            break :blk true;
        },
        .object => |v| blk: {
            if (v.count() != b.object.count()) break :blk false;
            var it = v.iterator();
            while (it.next()) |entry| {
                const right = b.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!equal(entry.value_ptr.*, right, depth + 1)) break :blk false;
            }
            break :blk true;
        },
    };
}

fn equalExcept(a: Value, b: Value, allowed: []const []const u8) bool {
    if (a != .object or b != .object) return false;
    for ([_]bool{ false, true }) |reverse| {
        const left = if (reverse) b else a;
        const right = if (reverse) a else b;
        var it = left.object.iterator();
        while (it.next()) |entry| {
            const skip = for (allowed) |key| {
                if (std.mem.eql(u8, key, entry.key_ptr.*)) break true;
            } else false;
            if (skip) continue;
            const other = right.object.get(entry.key_ptr.*) orelse return false;
            if (!equal(entry.value_ptr.*, other, 0)) return false;
        }
    }
    return true;
}

pub fn requireSchemaDelta(alloc: std.mem.Allocator, before: []const u8, after: []const u8, allowed: []const []const u8) !void {
    var left = try std.json.parseFromSlice(Value, alloc, before, .{});
    defer left.deinit();
    var right = try std.json.parseFromSlice(Value, alloc, after, .{});
    defer right.deinit();
    if (!equalExcept(left.value, right.value, allowed)) return error.InvalidGenerationPublication;
}

fn version(schema: Value) !i64 {
    if (schema != .object) return error.InvalidGenerationPublication;
    const v = schema.object.get("version") orelse return 0;
    if (v != .integer or v.integer < 0 or v.integer > std.math.maxInt(u32)) return error.InvalidGenerationPublication;
    return v.integer;
}

/// Only the three schema/public-index fields can change. The predecessor read
/// layout remains pinned; publication cannot retire it or substitute the new
/// write layout before migration readiness. All other table settings are fixed.
pub fn requireTableImages(alloc: std.mem.Allocator, before: records.TableRecord, after: records.TableRecord) !void {
    var expected = before;
    expected.schema_json = after.schema_json;
    expected.read_schema_json = after.read_schema_json;
    expected.indexes_json = after.indexes_json;
    if (!tables.tableDefinitionsEqual(expected, after)) return error.InvalidGenerationPublication;
    const read_before = if (before.read_schema_json.len == 0) before.schema_json else before.read_schema_json;
    if (after.read_schema_json.len == 0) return error.InvalidGenerationPublication;
    var left = try std.json.parseFromSlice(Value, alloc, read_before, .{});
    defer left.deinit();
    var right = try std.json.parseFromSlice(Value, alloc, after.read_schema_json, .{});
    defer right.deinit();
    if (try version(left.value) != try version(right.value) or !equalExcept(left.value, right.value, &.{"version"}))
        return error.InvalidGenerationPublication;
}
