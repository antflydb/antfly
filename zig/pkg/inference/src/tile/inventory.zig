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

//! Read-only inventory derived from the actual native regions and typed catalogs.
const std = @import("std");
const Region = @import("region.zig").Region;
const schedules = @import("q4_q8_schedule.zig");
pub const Entry = struct { family_region: []const u8, output: []const u8, bytes: usize, sha256: []const u8, owner: enum { tile_region, quant_region, generated_file } };
const Row = struct { schedule: schedules.Schedule, threads: usize, static_shared_bytes: usize, abi: []const u8 };

pub fn render(allocator: std.mem.Allocator, regions: []const Region, extra: []const Entry) ![]u8 {
    const entries = try allocator.alloc(Entry, regions.len + extra.len);
    defer allocator.free(entries);
    var initialized: usize = 0;
    defer for (entries[0..initialized]) |entry| allocator.free(entry.sha256);
    for (regions, entries[0..regions.len]) |region, *entry| {
        const prefix = "// tile-codegen:begin ";
        const begin = (std.mem.indexOf(u8, region.begin, prefix) orelse return error.InvalidRegion) + prefix.len;
        const end = std.mem.indexOfPos(u8, region.begin, begin, " family") orelse return error.InvalidRegion;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(region.data, &digest, .{});
        const hex = std.fmt.bytesToHex(digest, .lower);
        entry.* = .{ .family_region = region.begin[begin..end], .output = region.path, .bytes = region.data.len, .sha256 = try allocator.dupe(u8, &hex), .owner = .tile_region };
        initialized += 1;
    }
    @memcpy(entries[regions.len..], extra);
    var rows: [4]Row = undefined;
    inline for (.{ schedules.linear2, schedules.linear4, schedules.linear8, schedules.qkv4 }, 0..) |profile, i| {
        rows[i] = .{ .schedule = profile, .threads = profile.threads(), .static_shared_bytes = profile.staticSharedBytes(), .abi = profile.abi() };
    }
    return std.json.Stringify.valueAlloc(allocator, .{
        .schema = @as(u32, 1),
        .native_regions = entries,
        .standalone_cuda = @import("cuda_standalone.zig").profiles,
        .q4_q8_rows = rows,
        .tile_source_inputs = @import("source_inputs.zig").paths,
    }, .{ .whitespace = .indent_2 });
}

fn checkInventory(allocator: std.mem.Allocator) !void {
    const data = try render(allocator, &.{.{ .path = "fixture.cu", .begin = "// tile-codegen:begin fixture.0 family", .end = "unused", .data = "abc" }}, &.{});
    defer allocator.free(data);
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, data, .{});
    defer parsed.deinit();
    const entries = parsed.value.object.get("native_regions").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualStrings("fixture.0", entries[0].object.get("family_region").?.string);
    try std.testing.expectEqualStrings("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", entries[0].object.get("sha256").?.string);
    const rows = parsed.value.object.get("q4_q8_rows").?.array.items;
    try std.testing.expectEqual(@as(i64, 512), rows[3].object.get("static_shared_bytes").?.integer);
}

test "ownership inventory records emitted bytes and releases partial allocations" {
    try checkInventory(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkInventory, .{});
}
