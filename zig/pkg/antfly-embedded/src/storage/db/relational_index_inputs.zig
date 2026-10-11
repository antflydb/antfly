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

//! Immutable native worker inputs. Length-framed raw bytes avoid JSON escaping
//! or base64 expansion. The durable job separately binds the manifest digest;
//! a checksum is corruption detection, not metadata execution authority.
const std = @import("std");
const catalog = @import("relational_integrity_catalog.zig");
pub const key = "\x00\x00__metadata__:relational_index_owner_inputs:v1";
pub const max_schema_bytes = 2 * 1024 * 1024;
const header_len = 20;
pub const max_bytes = header_len + 2 * max_schema_bytes + catalog.max_catalog_bytes + 1024 + 32;

pub const View = struct {
    before_json: []const u8,
    after_json: []const u8,
    predecessor: []const u8,
    name: []const u8,

    fn validate(self: View) !void {
        if (self.before_json.len == 0 or self.before_json.len > max_schema_bytes or
            self.after_json.len == 0 or self.after_json.len > max_schema_bytes or
            self.predecessor.len == 0 or self.predecessor.len > catalog.max_catalog_bytes or
            self.name.len == 0 or self.name.len > 1024) return error.InvalidPrivateIndexInputs;
    }
};

pub fn digest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

pub fn decode(bytes: []const u8) !View {
    if (bytes.len < header_len + 32 or bytes.len > max_bytes or !std.mem.eql(u8, bytes[0..4], "AIM1")) return error.InvalidPrivateIndexInputs;
    var lengths: [4]usize = undefined;
    var total: usize = header_len + 32;
    for (&lengths, 0..) |*length, i| {
        length.* = std.mem.readInt(u32, bytes[4 + 4 * i ..][0..4], .little);
        total = std.math.add(usize, total, length.*) catch return error.InvalidPrivateIndexInputs;
    }
    if (total != bytes.len or !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidPrivateIndexInputs;
    var parts: [4][]const u8 = undefined;
    var offset: usize = header_len;
    for (lengths, &parts) |length, *part| {
        part.* = bytes[offset .. offset + length];
        offset += length;
    }
    const view: View = .{ .before_json = parts[0], .after_json = parts[1], .predecessor = parts[2], .name = parts[3] };
    try view.validate();
    return view;
}

fn boundBytes(txn: anytype, expected: [32]u8) ![]const u8 {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return error.InvalidPrivateIndexInputs,
        else => return err,
    };
    _ = try decode(raw);
    if (!std.mem.eql(u8, &digest(raw), &expected)) return error.InvalidPrivateIndexInputs;
    return raw;
}

/// Receipt production rechecks durable inputs without allocating or compiling
/// schemas. This is not a page-application guard and grants no execution lease.
pub fn require(txn: anytype, expected: [32]u8) !void {
    _ = try boundBytes(txn, expected);
}

pub const Prepared = struct {
    alloc: std.mem.Allocator,
    bytes: []const u8,

    pub fn deinit(self: *Prepared) void {
        self.alloc.free(self.bytes);
        self.* = undefined;
    }

    pub fn init(alloc: std.mem.Allocator, view: View) !Prepared {
        try view.validate();
        const parts = [_][]const u8{ view.before_json, view.after_json, view.predecessor, view.name };
        var length: usize = header_len + 32;
        for (parts) |part| length += part.len; // Each component was bounded above.
        const bytes = try alloc.alloc(u8, length);
        @memcpy(bytes[0..4], "AIM1");
        var offset: usize = header_len;
        for (parts, 0..) |part, i| {
            std.mem.writeInt(u32, bytes[4 + 4 * i ..][0..4], @intCast(part.len), .little);
            @memcpy(bytes[offset .. offset + part.len], part);
            offset += part.len;
        }
        @memcpy(bytes[offset..], &digest(bytes[0..offset]));
        return .{ .alloc = alloc, .bytes = bytes };
    }

    /// Own the full buffer before another transaction read can invalidate it.
    pub fn load(alloc: std.mem.Allocator, txn: anytype, expected: [32]u8) !Prepared {
        const raw = try boundBytes(txn, expected);
        return .{ .alloc = alloc, .bytes = try alloc.dupe(u8, raw) };
    }
};
