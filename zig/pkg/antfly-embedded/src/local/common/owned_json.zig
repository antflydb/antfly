// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Heap-owned JSON trees. Reserve containers before cloning children so an
//! allocation failure never strands an uninstalled key or nested value.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn deinit(alloc: Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .string, .number_string => |bytes| alloc.free(bytes),
        .array => |*array| {
            for (array.items) |*item| deinit(alloc, item);
            array.deinit();
        },
        .object => |*object| {
            var iter = object.iterator();
            while (iter.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                deinit(alloc, entry.value_ptr);
            }
            object.deinit(alloc);
        },
        else => {},
    }
    value.* = undefined;
}

pub fn clone(alloc: Allocator, value: std.json.Value) Allocator.Error!std.json.Value {
    return switch (value) {
        .null, .bool, .integer, .float => value,
        .string => |bytes| .{ .string = try alloc.dupe(u8, bytes) },
        .number_string => |bytes| .{ .number_string = try alloc.dupe(u8, bytes) },
        .array => |array| blk: {
            var result = std.json.Value{ .array = std.json.Array.init(alloc) };
            errdefer deinit(alloc, &result);
            try result.array.ensureTotalCapacity(array.items.len);
            for (array.items) |item| result.array.appendAssumeCapacity(try clone(alloc, item));
            break :blk result;
        },
        .object => |object| blk: {
            var result = std.json.Value{ .object = .empty };
            errdefer deinit(alloc, &result);
            try result.object.ensureTotalCapacity(alloc, object.count());
            var iter = object.iterator();
            while (iter.next()) |entry| {
                const key = try alloc.dupe(u8, entry.key_ptr.*);
                errdefer alloc.free(key);
                result.object.putAssumeCapacity(key, try clone(alloc, entry.value_ptr.*));
            }
            break :blk result;
        },
    };
}

/// Transfer only on success; caller retains `value` on failure.
pub fn put(alloc: Allocator, object: *std.json.ObjectMap, key: []const u8, value: std.json.Value) Allocator.Error!void {
    if (object.getPtr(key)) |existing| {
        deinit(alloc, existing);
        existing.* = value;
        return;
    }
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    try object.put(alloc, owned_key, value);
}

pub fn putClone(alloc: Allocator, object: *std.json.ObjectMap, key: []const u8, value: std.json.Value) Allocator.Error!void {
    var owned = try clone(alloc, value);
    errdefer deinit(alloc, &owned);
    try put(alloc, object, key, owned);
}
