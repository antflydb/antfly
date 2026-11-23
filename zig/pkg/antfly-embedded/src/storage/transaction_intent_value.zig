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

//! Shared durable intent value codec. History reservation and transaction
//! recovery must interpret exactly the same prepared row bytes.
const std = @import("std");
pub const Value = struct {
    value: ?[]const u8,
    prepared_row: ?[]const u8 = null,

    pub fn decode(bytes: []const u8) !Value {
        if (bytes.len == 0) return error.InvalidTxnRecord;
        return switch (bytes[0]) {
            0 => .{ .value = bytes[1..] },
            1 => if (bytes.len == 1) .{ .value = null } else error.InvalidTxnRecord,
            2 => blk: {
                if (bytes.len < 5) return error.InvalidTxnRecord;
                const length = std.mem.readInt(u32, bytes[1..5], .little);
                if (length > bytes.len - 5 or length == bytes.len - 5) return error.InvalidTxnRecord;
                break :blk .{ .value = bytes[5..][0..length], .prepared_row = bytes[5 + length ..] };
            },
            else => error.InvalidTxnRecord,
        };
    }
};

/// Removing DB-owned special fields never grows the document. Only inputs
/// which need serialization pay a parse/count pass; ordinary bytes are exact.
pub fn documentImageBytes(alloc: std.mem.Allocator, value: []const u8) !u64 {
    if (std.mem.indexOf(u8, value, "_edges") == null and std.mem.indexOf(u8, value, "_embeddings") == null and std.mem.indexOfScalar(u8, value, '\\') == null) return value.len;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{ .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidTxnRecord,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return value.len;
    var buffer: [1024]u8 = undefined;
    var counter = std.Io.Writer.Discarding.init(&buffer);
    try std.json.Stringify.value(parsed.value, .{}, &counter.writer);
    return @max(value.len, counter.fullCount());
}
