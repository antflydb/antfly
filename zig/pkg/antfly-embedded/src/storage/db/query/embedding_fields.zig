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

const std = @import("std");

/// Owned dense vectors, with one numeric slab per field rather than a JSON
/// value per component. The projection borrows this owner until serialization.
pub const EmbeddingFields = struct {
    values: std.StringArrayHashMapUnmanaged([]const f32) = .empty,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.values.keys(), self.values.values()) |name, vector| {
            alloc.free(name);
            alloc.free(vector);
        }
        self.values.deinit(alloc);
        self.* = .{};
    }
    pub fn putOwned(self: *@This(), alloc: std.mem.Allocator, name: []const u8, vector: []const f32) !void {
        if (self.values.getPtr(name)) |previous| {
            alloc.free(previous.*);
            previous.* = vector;
            return;
        }
        const owned = try alloc.dupe(u8, name);
        errdefer alloc.free(owned);
        try self.values.put(alloc, owned, vector);
    }
    pub fn materialize(self: *const @This(), alloc: std.mem.Allocator) !std.json.Value {
        var object: std.json.Value = .{ .object = .empty };
        errdefer @import("../../../common/owned_json.zig").deinit(alloc, &object);
        for (self.values.keys(), self.values.values()) |name, vector| {
            var array = std.json.Array.init(alloc);
            errdefer array.deinit();
            try array.ensureTotalCapacity(vector.len);
            for (vector) |component| array.appendAssumeCapacity(.{ .float = @as(f64, component) });
            const owned = try alloc.dupe(u8, name);
            errdefer alloc.free(owned);
            try object.object.put(alloc, owned, .{ .array = array });
        }
        return object;
    }
};

/// Synchronous, restartable typed fields. Each vector is borrowed only until
/// next/reset/close; serialization completes before the DB lease is released.
pub const EmbeddingStream = struct {
    ctx: *anyopaque,
    next_fn: *const fn (*anyopaque) anyerror!?Field,
    reset_fn: *const fn (*anyopaque) anyerror!void,
    close_fn: *const fn (*anyopaque) void,
    /// Optional random field lookup. Borrow is invalidated by next/reset/lookup.
    field_fn: ?*const fn (*anyopaque, []const u8) anyerror!?[]const f32 = null,
    cached: ?EmbeddingFields = null,
    cached_complete: bool = false,
    cache_allocator: ?std.mem.Allocator = null,
    pub const Field = struct { name: []const u8, vector: []const f32 };

    pub fn deinit(self: *@This()) void {
        if (self.cached) |*cached| cached.deinit(self.cache_allocator.?);
        self.close_fn(self.ctx);
    }
    /// Cache only selected vectors so projection nodes can retain stable
    /// borrows without pinning or copying every unrelated field.
    pub fn getField(self: *@This(), alloc: std.mem.Allocator, name: []const u8) !?[]const f32 {
        if (self.cached) |*cached| if (cached.values.get(name)) |vector| return vector;
        if (self.cached_complete) return null;
        const lookup = self.field_fn orelse return (try self.fields(alloc)).values.get(name);
        const borrowed = (try lookup(self.ctx, name)) orelse return null;
        const owner = self.cache_allocator orelse @import("antfly_platform").allocator.reclaimingScratch(alloc);
        const vector = try owner.dupe(f32, borrowed);
        errdefer owner.free(vector);
        if (self.cached == null) {
            self.cached = .{};
            self.cache_allocator = owner;
        }
        try self.cached.?.putOwned(owner, name, vector);
        return vector;
    }
    /// Compatibility materialization for callers that need all stable vectors.
    pub fn fields(self: *@This(), alloc: std.mem.Allocator) !*const EmbeddingFields {
        if (!self.cached_complete) {
            const owner = self.cache_allocator orelse @import("antfly_platform").allocator.reclaimingScratch(alloc);
            const partial = if (self.cached) |*cached| cached else null;
            try self.reset_fn(self.ctx);
            // Completing a partial cache must preserve slabs already borrowed
            // by projection nodes. A first full load keeps the original single
            // map path, including last-value replacement for duplicate names.
            var ordered: EmbeddingFields = .{};
            errdefer if (partial != null) ordered.values.deinit(owner) else ordered.deinit(owner);
            while (try self.next_fn(self.ctx)) |item| {
                if (partial) |cached| {
                    if (!cached.values.contains(item.name)) {
                        const vector = try owner.dupe(f32, item.vector);
                        errdefer owner.free(vector);
                        try cached.putOwned(owner, item.name, vector);
                    }
                    const i = cached.values.getIndex(item.name).?;
                    try ordered.values.put(owner, cached.values.keys()[i], cached.values.values()[i]);
                } else {
                    const vector = try owner.dupe(f32, item.vector);
                    errdefer owner.free(vector);
                    try ordered.putOwned(owner, item.name, vector);
                }
            }
            if (partial) |cached| cached.values.deinit(owner);
            self.cached = ordered;
            self.cache_allocator = owner;
            self.cached_complete = true;
        }
        return &self.cached.?;
    }
    pub fn write(self: *@This(), writer: *std.Io.Writer) !void {
        try writer.writeByte('{');
        var count: usize = 0;
        if (self.cached_complete) {
            const cached = &self.cached.?;
            for (cached.values.keys(), cached.values.values()) |name, vector| {
                try writeField(writer, &count, .{ .name = name, .vector = vector });
            }
        } else {
            try self.reset_fn(self.ctx);
            while (try self.next_fn(self.ctx)) |field| try writeField(writer, &count, field);
        }
        try writer.writeByte('}');
    }
    fn writeField(writer: *std.Io.Writer, count: *usize, field: Field) !void {
        if (count.* != 0) try writer.writeByte(',');
        try std.json.Stringify.value(field.name, .{}, writer);
        try writer.writeAll(":[");
        for (field.vector, 0..) |component, i| {
            if (i != 0) try writer.writeByte(',');
            try std.json.Stringify.value(@as(f64, component), .{}, writer);
        }
        try writer.writeByte(']');
        count.* += 1;
    }
};
