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

//! Canonical CJ2 byte sizing from key-list totals. Counts may conservatively
//! overestimate unique keys, but individual keys must remain CJ2-encodable.
//! This is a wire budget, not a durable grant or decoder/allocator RAM budget.
const std = @import("std");
pub const magic = "CJ2\x00";
pub const header_bytes = magic.len + @sizeOf(u16) + @sizeOf(u64) + 2 * @sizeOf(u8);

pub const KeyList = struct {
    count: u64 = 0,
    key_bytes: u64 = 0,

    pub fn addKey(self: *KeyList, len: usize) !void {
        if (len > std.math.maxInt(u32)) return error.RecordTooLarge;
        self.* = try self.plus(.{ .count = 1, .key_bytes = len });
    }

    pub fn plus(self: KeyList, other: KeyList) !KeyList {
        _ = try self.encodedBytes();
        _ = try other.encodedBytes();
        const next: KeyList = .{
            .count = std.math.add(u64, self.count, other.count) catch return error.RecordTooLarge,
            .key_bytes = std.math.add(u64, self.key_bytes, other.key_bytes) catch return error.RecordTooLarge,
        };
        _ = try next.encodedBytes();
        return next;
    }

    pub fn encodedBytes(self: KeyList) !u64 {
        if (self.count > std.math.maxInt(u32) or (self.count == 0 and self.key_bytes != 0)) return error.RecordTooLarge;
        if (self.count == 0) return 0;
        if (self.key_bytes > self.count * @as(u64, std.math.maxInt(u32))) return error.RecordTooLarge;
        const lengths = self.count * @sizeOf(u32);
        return std.math.add(u64, @sizeOf(u32) + lengths, self.key_bytes) catch error.RecordTooLarge;
    }
};

pub const Record = struct {
    changed_doc_keys: KeyList = .{},
    deleted_doc_keys: KeyList = .{},
    overwritten_doc_keys: KeyList = .{},
    changed_artifact_keys: KeyList = .{},

    pub fn plus(self: Record, other: Record) !Record {
        var next: Record = .{};
        inline for (comptime std.meta.fieldNames(Record)) |name| @field(next, name) = try @field(self, name).plus(@field(other, name));
        _ = try next.encodedBytes();
        return next;
    }

    pub fn encodedBytes(self: Record) !u64 {
        var bytes: u64 = header_bytes;
        inline for (comptime std.meta.fieldNames(Record)) |name| bytes = std.math.add(u64, bytes, try @field(self, name).encodedBytes()) catch return error.RecordTooLarge;
        return bytes;
    }
};

test "change journal shape merges lists without duplicating headers and checks wire limits" {
    const left: Record = .{ .deleted_doc_keys = .{ .count = 1, .key_bytes = 0 } };
    const right: Record = .{ .deleted_doc_keys = .{ .count = 2, .key_bytes = 7 }, .changed_artifact_keys = .{ .count = 1, .key_bytes = 13 } };
    const merged = try left.plus(right);
    try std.testing.expectEqual(@as(u64, 60), try merged.encodedBytes());
    try std.testing.expectEqual(@as(u64, 16), try (Record{}).encodedBytes());
    var list: KeyList = .{ .count = std.math.maxInt(u32), .key_bytes = 0 };
    const previous = list;
    try std.testing.expectError(error.RecordTooLarge, list.addKey(1));
    try std.testing.expectEqualDeep(previous, list);
    try std.testing.expectError(error.RecordTooLarge, (KeyList{ .key_bytes = 1 }).encodedBytes());
    try std.testing.expectError(error.RecordTooLarge, (KeyList{ .count = 1, .key_bytes = @as(u64, std.math.maxInt(u32)) + 1 }).encodedBytes());
    const maximum: KeyList = .{ .count = std.math.maxInt(u32), .key_bytes = @as(u64, std.math.maxInt(u32)) * std.math.maxInt(u32) };
    try std.testing.expectError(error.RecordTooLarge, maximum.encodedBytes());
}
