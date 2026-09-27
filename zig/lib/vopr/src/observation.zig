// Copyright 2026 Antfly, Inc.
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
const ids = @import("id.zig");

pub const Feature = struct {
    id: ids.StableId,
    name: []const u8,
    value: i64,

    pub fn named(name: []const u8, value: i64) Feature {
        return .{ .id = ids.stable("observation", name), .name = name, .value = value };
    }
};

pub const Builder = struct {
    features: std.ArrayListUnmanaged(Feature) = .empty,

    pub fn deinit(self: *Builder, allocator: std.mem.Allocator) void {
        self.features.deinit(allocator);
        self.* = .{};
    }

    pub fn add(self: *Builder, allocator: std.mem.Allocator, feature: Feature) !void {
        if (feature.id == 0) return error.InvalidObservationId;
        if (feature.name.len == 0) return error.EmptyObservationName;
        try self.features.append(allocator, feature);
    }

    pub fn addNamed(self: *Builder, allocator: std.mem.Allocator, name: []const u8, value: i64) !void {
        try self.add(allocator, Feature.named(name, value));
    }

    pub fn canonicalize(self: *Builder) !void {
        std.mem.sort(Feature, self.features.items, {}, lessThan);
        for (self.features.items, 0..) |feature, index| {
            if (index > 0 and self.features.items[index - 1].id == feature.id) return error.DuplicateObservationId;
        }
    }

    pub fn digest(self: *const Builder) u64 {
        return digestFeatures(self.features.items);
    }

    fn lessThan(_: void, lhs: Feature, rhs: Feature) bool {
        if (lhs.id != rhs.id) return lhs.id < rhs.id;
        return std.mem.lessThan(u8, lhs.name, rhs.name);
    }
};

pub fn digestFeatures(features: []const Feature) u64 {
    var hash: u64 = 0xcbf29ce484222325;
    for (features) |feature| {
        hashU64(&hash, feature.id);
        hashU64(&hash, @bitCast(feature.value));
    }
    return hash;
}

fn hashU64(hash: *u64, value: u64) void {
    for (0..8) |byte_index| {
        const shift: u6 = @intCast(byte_index * 8);
        hash.* ^= @as(u8, @truncate(value >> shift));
        hash.* *%= 0x100000001b3;
    }
}

test "observations canonicalize before hashing" {
    var a = Builder{};
    defer a.deinit(std.testing.allocator);
    try a.add(std.testing.allocator, .{ .id = 9, .name = "b", .value = -4 });
    try a.add(std.testing.allocator, .{ .id = 2, .name = "a", .value = 7 });
    try a.canonicalize();

    var b = Builder{};
    defer b.deinit(std.testing.allocator);
    try b.add(std.testing.allocator, .{ .id = 2, .name = "a", .value = 7 });
    try b.add(std.testing.allocator, .{ .id = 9, .name = "b", .value = -4 });
    try b.canonicalize();
    try std.testing.expectEqual(a.digest(), b.digest());
}
