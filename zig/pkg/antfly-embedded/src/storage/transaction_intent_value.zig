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
const Allocator = std.mem.Allocator;
const document_revision_reservation = @import("document_revision_reservation.zig");

pub const Value = struct {
    document_revision: ?u64 = null,
    prepared_graph_effects: ?[]const u8 = null,
    row_policy_requirement: @import("../usermgr/row_policy_authority.zig").MutationRequirement = .native,
    value: ?[]const u8,
    prepared_row: ?[]const u8 = null,

    pub fn decode(bytes: []const u8) !Value {
        return decodeAt(bytes, 0);
    }

    fn decodeAt(bytes: []const u8, depth: u8) !Value {
        if (depth > 3 or bytes.len == 0) return error.InvalidTxnRecord;
        return switch (bytes[0]) {
            0 => .{ .value = bytes[1..] },
            1 => if (bytes.len == 1) .{ .value = null } else error.InvalidTxnRecord,
            2 => blk: {
                if (bytes.len < 5) return error.InvalidTxnRecord;
                const length = std.mem.readInt(u32, bytes[1..5], .little);
                if (length > bytes.len - 5 or length == bytes.len - 5) return error.InvalidTxnRecord;
                break :blk .{ .value = bytes[5..][0..length], .prepared_row = bytes[5 + length ..] };
            },
            3 => blk: {
                if (bytes.len < 3 or bytes[2] > 2) return error.InvalidTxnRecord;
                const requirement = std.enums.fromInt(@import("../usermgr/row_policy_authority.zig").MutationRequirement, bytes[1]) orelse return error.InvalidTxnRecord;
                if (requirement == .native) return error.InvalidTxnRecord;
                var value = try decodeAt(bytes[2..], depth + 1);
                if (value.row_policy_requirement != .native) return error.InvalidTxnRecord;
                value.row_policy_requirement = requirement;
                break :blk value;
            },
            4 => blk: {
                if (bytes.len < 7) return error.InvalidTxnRecord;
                const length = std.mem.readInt(u32, bytes[1..5], .little);
                if (length == 0 or length > @import("db/prepared_graph_effects.zig").max_encoded_bytes or length >= bytes.len - 5) return error.InvalidTxnRecord;
                var base = try decodeAt(bytes[5 + length ..], depth + 1);
                if (base.value == null or base.prepared_graph_effects != null) return error.InvalidTxnRecord;
                base.prepared_graph_effects = bytes[5..][0..length];
                break :blk base;
            },
            6 => blk: {
                if (depth != 0 or bytes.len < 10) return error.InvalidTxnRecord;
                var base = try decodeAt(bytes[9..], depth + 1);
                base.document_revision = try document_revision_reservation.decodeVersion(bytes[1..9]);
                break :blk base;
            },
            else => error.InvalidTxnRecord,
        };
    }

    /// Only use the revision assigned by the accepting owner's native ledger.
    pub fn withDocumentRevision(alloc: Allocator, base: []const u8, revision: u64) ![]u8 {
        if (revision == 0 or revision > document_revision_reservation.max_version or
            (try decode(base)).document_revision != null) return error.InvalidTxnRecord;
        const bytes = try alloc.alloc(u8, 9 + base.len);
        bytes[0] = 6;
        std.mem.writeInt(u64, bytes[1..9], revision, .little);
        @memcpy(bytes[9..], base);
        return bytes;
    }

    pub fn encode(alloc: Allocator, intent: anytype) ![]u8 {
        const policy: usize = if (intent.row_policy_requirement == .native) 0 else 2;
        const graph: usize = if (intent.prepared_graph_effects) |effects| 5 + effects.len else 0;
        if (intent.prepared_graph_effects) |effects| {
            if (intent.value == null or effects.len == 0 or effects.len > @import("db/prepared_graph_effects.zig").max_encoded_bytes) return error.InvalidPreparedGraphEffects;
        }
        const base_len = if (intent.value) |v| if (intent.prepared_row) |row| 5 + v.len + row.len else 1 + v.len else 1;
        const bytes = try alloc.alloc(u8, graph + policy + base_len);
        errdefer alloc.free(bytes);
        const base = bytes[graph..];
        if (policy != 0) {
            base[0] = 3;
            base[1] = @backingInt(intent.row_policy_requirement);
        }
        const payload = base[policy..];
        if (intent.value) |v| {
            if (intent.prepared_row) |row| {
                const json_len = std.math.cast(u32, v.len) orelse return error.TransactionTooLarge;
                payload[0] = 2;
                std.mem.writeInt(u32, payload[1..5], json_len, .little);
                @memcpy(payload[5..][0..v.len], v);
                @memcpy(payload[5 + v.len ..], row);
            } else {
                payload[0] = 0;
                @memcpy(payload[1..], v);
            }
        } else payload[0] = 1;
        if (intent.prepared_graph_effects) |effects| {
            bytes[0] = 4;
            std.mem.writeInt(u32, bytes[1..5], @intCast(effects.len), .little);
            @memcpy(bytes[5..][0..effects.len], effects);
        }
        return bytes;
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
