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
