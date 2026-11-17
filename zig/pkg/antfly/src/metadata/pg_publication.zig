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

//! Durable PostgreSQL publication membership, keyed by database incarnation.
const std = @import("std");
const feed = @import("changefeed_control.zig");
const Identity = @import("../pgwire/pgoutput.zig").Identity;
const A = std.mem.Allocator;
pub const protocol_version: u16 = 110;
pub const max_wire_bytes = 256 * 1024;

pub const Member = struct {
    table: feed.Table,
    permission_name: []const u8,
    namespace: []const u8,
    name: []const u8,
    oid: u32,
    identity: Identity,
    key_columns: []const []const u8,
};

fn validIdentifier(name: []const u8) bool {
    return name.len != 0 and name.len <= 63 and std.unicode.utf8ValidateSlice(name) and std.mem.indexOfScalar(u8, name, 0) == null;
}

pub fn validateMembers(members: []const Member) !void {
    if (members.len == 0 or members.len > feed.max_tables) return error.InvalidChangefeed;
    for (members, 0..) |member, index| {
        if (member.table.contract != .native_relational or member.table.fields.len != 0 or member.table.predicates.len != 0 or
            member.table.table_id == 0 or member.table.policy_revision == 0 or std.mem.allEqual(u8, &member.table.binding, 0) or
            member.permission_name.len == 0 or member.permission_name.len > 1024 or member.oid == 0 or
            !validIdentifier(member.namespace) or !validIdentifier(member.name) or
            (index != 0 and members[index - 1].table.table_id >= member.table.table_id))
            return error.ChangefeedCapabilityUnsupported;
        if ((member.identity != .default and member.identity != .full) or
            (member.identity == .default and member.key_columns.len == 0))
            return error.ChangefeedCapabilityUnsupported;
        if (member.key_columns.len > 256) return error.InvalidChangefeed;
        for (member.key_columns, 0..) |name, column| {
            if (!validIdentifier(name) or (column != 0 and std.mem.order(u8, member.key_columns[column - 1], name) != .lt))
                return error.InvalidChangefeed;
        }
        for (members[0..index]) |prior| if (prior.oid == member.oid) return error.InvalidChangefeed;
    }
}

pub const Record = struct {
    database: feed.Digest,
    name: []const u8,
    owner: feed.Digest,
    revision: u64,
    /// Null marks a durable drop. Recreation needs the tombstone revision.
    members: ?[]const Member,

    pub fn validate(self: Record) !void {
        if (std.mem.allEqual(u8, &self.database, 0) or std.mem.allEqual(u8, &self.owner, 0) or
            !validIdentifier(self.name) or self.revision == 0) return error.InvalidChangefeed;
        if (self.members) |members| try validateMembers(members);
    }
};

pub const Write = struct {
    expected_revision: u64,
    next: Record,

    pub fn validate(self: Write) !void {
        try self.next.validate();
        if (self.expected_revision == std.math.maxInt(u64) or self.next.revision != self.expected_revision + 1 or
            (self.expected_revision == 0 and self.next.members == null)) return error.InvalidChangefeed;
    }

    pub fn applies(self: Write, old: ?Record) bool {
        if (old) |state| return state.revision == self.expected_revision and
            std.meta.eql(state.owner, self.next.owner) and
            (state.members != null or self.next.members != null);
        return self.expected_revision == 0;
    }
};

pub fn key(buffer: []u8, group: u64, database: feed.Digest, name: []const u8) ![]const u8 {
    if (group == 0 or std.mem.allEqual(u8, &database, 0) or !validIdentifier(name)) return error.InvalidChangefeed;
    const digest = feed.hash(.{ .domain = "antfly:pg-publication-key:v1", .database = database, .name = name });
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:changefeeds:{d}:publication:{s}", .{ group, std.fmt.bytesToHex(digest, .lower) });
}

pub fn parseWrite(a: A, bytes: []const u8) !std.json.Parsed(Write) {
    if (bytes.len == 0 or bytes.len > max_wire_bytes) return error.InvalidChangefeed;
    var parsed = try std.json.parseFromSlice(Write, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}

pub fn parseRecord(a: A, bytes: []const u8) !std.json.Parsed(Record) {
    if (bytes.len == 0 or bytes.len > max_wire_bytes) return error.ChangefeedCorrupt;
    var parsed = try std.json.parseFromSlice(Record, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate();
    return parsed;
}

pub fn validateEncodedWrite(bytes: []const u8) !void {
    var parsed = try parseWrite(std.heap.page_allocator, bytes);
    defer parsed.deinit();
}
