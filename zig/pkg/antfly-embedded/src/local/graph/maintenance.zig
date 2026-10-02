// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Durable, byte-bounded graph maintenance pages. Intents contain identities,
//! never edge payloads. A single oversized identity is admitted indivisibly.
const std = @import("std");
pub const max_records = 1024;
pub const max_bytes = 4 * 1024 * 1024;
pub const prune_key = "meta:graph_prune:v1";
pub const counters_key = "meta:graph_counters_rebuild:v1";
pub const range_key = "meta:graph_prune_range:v1";
pub const ownership_key = "meta:graph_ownership_fence:v1";

/// Whole-task ownership survives between bounded prune pages and restarts.
/// Views borrow the encoded value; callers retain it through serialization.
pub const RangeProgress = struct {
    lower: []const u8,
    upper: []const u8,
    after: []const u8 = "",

    pub fn encode(self: @This(), alloc: std.mem.Allocator) ![]u8 {
        var size: usize = 12;
        for ([_][]const u8{ self.lower, self.upper, self.after }) |part| {
            if (part.len > std.math.maxInt(u32)) return error.InvalidGraphMaintenancePage;
            size = try std.math.add(usize, size, part.len);
        }
        const bytes = try alloc.alloc(u8, size);
        var pos: usize = 0;
        for ([_][]const u8{ self.lower, self.upper, self.after }) |part| {
            std.mem.writeInt(u32, bytes[pos..][0..4], @intCast(part.len), .little);
            pos += 4;
            @memcpy(bytes[pos..][0..part.len], part);
            pos += part.len;
        }
        return bytes;
    }

    pub fn decode(bytes: []const u8) !@This() {
        var parts: [3][]const u8 = undefined;
        var pos: usize = 0;
        for (&parts) |*part| {
            if (bytes.len - pos < 4) return error.InvalidGraphMaintenancePage;
            const len = std.mem.readInt(u32, bytes[pos..][0..4], .little);
            pos += 4;
            if (len > bytes.len - pos) return error.InvalidGraphMaintenancePage;
            part.* = bytes[pos..][0..len];
            pos += len;
        }
        if (pos != bytes.len or (parts[1].len > 0 and std.mem.order(u8, parts[0], parts[1]) == .gt) or
            (parts[2].len > 0 and (std.mem.order(u8, parts[2], parts[0]) == .lt or
                (parts[1].len > 0 and std.mem.order(u8, parts[2], parts[1]) != .lt)))) return error.InvalidGraphMaintenancePage;
        return .{ .lower = parts[0], .upper = parts[1], .after = parts[2] };
    }
};

pub fn encodeKeys(alloc: std.mem.Allocator, keys: []const []const u8) ![]u8 {
    if (keys.len == 0 or keys.len > max_records) return error.InvalidGraphMaintenancePage;
    var size: usize = 4;
    for (keys) |key| {
        if (key.len > std.math.maxInt(u32)) return error.InvalidGraphMaintenancePage;
        size = try std.math.add(usize, size, try std.math.add(usize, 4, key.len));
    }
    const bytes = try alloc.alloc(u8, size);
    std.mem.writeInt(u32, bytes[0..4], @intCast(keys.len), .little);
    var at: usize = 4;
    for (keys) |key| {
        std.mem.writeInt(u32, bytes[at..][0..4], @intCast(key.len), .little);
        at += 4;
        @memcpy(bytes[at..][0..key.len], key);
        at += key.len;
    }
    return bytes;
}

/// Returned slices borrow the encoded intent; only the slice array is owned.
pub fn decodeKeys(alloc: std.mem.Allocator, bytes: []const u8) ![][]const u8 {
    if (bytes.len < 4) return error.InvalidGraphMaintenancePage;
    const count = std.mem.readInt(u32, bytes[0..4], .little);
    if (count == 0 or count > max_records or count > (bytes.len - 4) / 4) return error.InvalidGraphMaintenancePage;
    const keys = try alloc.alloc([]const u8, count);
    errdefer alloc.free(keys);
    var at: usize = 4;
    for (keys) |*key| {
        if (bytes.len - at < 4) return error.InvalidGraphMaintenancePage;
        const len = std.mem.readInt(u32, bytes[at..][0..4], .little);
        at += 4;
        if (len == 0 or len > bytes.len - at) return error.InvalidGraphMaintenancePage;
        key.* = bytes[at..][0..len];
        at += len;
    }
    if (at != bytes.len) return error.InvalidGraphMaintenancePage;
    return keys;
}

test "graph maintenance intent validates lengths and owns only bounded identity slices" {
    const a = std.testing.allocator;
    const bytes = try encodeKeys(a, &.{ "a", "b" });
    defer a.free(bytes);
    const keys = try decodeKeys(a, bytes);
    defer a.free(keys);
    try std.testing.expectEqualStrings("b", keys[1]);
    try std.testing.expectError(error.InvalidGraphMaintenancePage, decodeKeys(a, bytes[0 .. bytes.len - 1]));
}

test "graph maintenance range progress rejects truncation trailing bytes and escaped cursor bounds" {
    const a = std.testing.allocator;
    const original = RangeProgress{ .lower = "\x01m\x00\xff", .upper = "\x01z", .after = "\x01n\x00\x00edge" };
    const bytes = try original.encode(a);
    defer a.free(bytes);
    const decoded = try RangeProgress.decode(bytes);
    try std.testing.expectEqualStrings(original.lower, decoded.lower);
    try std.testing.expectEqualStrings(original.upper, decoded.upper);
    try std.testing.expectEqualStrings(original.after, decoded.after);
    for (0..bytes.len) |len| try std.testing.expectError(error.InvalidGraphMaintenancePage, RangeProgress.decode(bytes[0..len]));
    const trailing = try std.mem.concat(a, u8, &.{ bytes, "x" });
    defer a.free(trailing);
    try std.testing.expectError(error.InvalidGraphMaintenancePage, RangeProgress.decode(trailing));
    for ([_]RangeProgress{
        .{ .lower = "z", .upper = "a" },
        .{ .lower = "m", .upper = "z", .after = "a" },
        .{ .lower = "m", .upper = "z", .after = "z" },
    }) |invalid| {
        const raw = try invalid.encode(a);
        defer a.free(raw);
        try std.testing.expectError(error.InvalidGraphMaintenancePage, RangeProgress.decode(raw));
    }
    const Run = struct {
        fn run(alloc: std.mem.Allocator, progress: RangeProgress) !void {
            const raw = try progress.encode(alloc);
            defer alloc.free(raw);
            _ = try RangeProgress.decode(raw);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Run.run, .{original});
}
