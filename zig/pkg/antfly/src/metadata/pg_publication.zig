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
    members: ?[]const Member = null,

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

const Context = @import("antfly_local_sources").api_operation.RequestContext;

pub fn requireAuthority(request: Context) !void {
    try request.ensureActive();
    const principal = request.principal orelse return error.Forbidden;
    if (principal.kind != .internal or !std.mem.eql(u8, principal.subject, "pg-publication-maintenance"))
        return error.Forbidden;
}

/// The caller owns the returned record and decides whether a tombstone is
/// visible to its client. The revision is needed even after a drop.
pub fn readOnService(svc: anytype, a: A, database: feed.Digest, name: []const u8, request: Context) !?std.json.Parsed(Record) {
    try requireAuthority(request);
    var buffer: [256]u8 = undefined;
    _ = try key(&buffer, svc.metadata_group_id, database, name);
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    var observed = try store.getPgPublication(a, svc.metadata_group_id, database, name);
    errdefer if (observed) |*record| record.deinit();
    try request.ensureActive();
    return observed;
}

/// A successful retry requires the exact durable record requested below,
/// including its owner, revision, and full historical relation bindings.
pub fn mutateOnService(svc: anytype, a: A, write: Write, request: Context) !void {
    try requireAuthority(request);
    try write.validate();
    const expected = feed.hash(write.next);
    var before = try readOnService(svc, a, write.next.database, write.next.name, request);
    defer if (before) |*record| record.deinit();
    if (before) |record| if (std.meta.eql(feed.hash(record.value), expected)) return;
    if (!write.applies(if (before) |record| record.value else null)) return error.PgPublicationConflict;
    const encoded = try std.json.Stringify.valueAlloc(a, write, .{});
    defer a.free(encoded);
    if (encoded.len > max_wire_bytes) return error.InvalidChangefeed;
    _ = try svc.ensureTableTopologyProtocolReadyWithContext(request, protocol_version);
    try request.ensureActive();
    var submitted = false;
    const receipt = svc.proposeTransitionCommandWithReceiptObserved(.{ .apply_pg_publication = encoded }, &submitted) catch |err|
        return if (submitted) error.MetadataMutationOutcomeUnknown else err;
    svc.waitForTransitionAppliedWithContext(receipt, request) catch return error.MetadataMutationOutcomeUnknown;
    var after = readOnService(svc, a, write.next.database, write.next.name, request) catch return error.MetadataMutationOutcomeUnknown;
    defer if (after) |*record| record.deinit();
    if (after == null or !std.meta.eql(feed.hash(after.?.value), expected)) return error.MetadataMutationOutcomeUnknown;
}

pub const path = "/internal/v1/catalog/pg-publication-v1";
pub const max_transport_bytes = max_wire_bytes + 4096;
pub const Request = struct {
    version: u8 = 1,
    group: u64,
    incarnation: feed.Digest,
    database: feed.Digest,
    name: []const u8,
    write: ?Write = null,

    pub fn validate(self: Request) !void {
        if (self.version != 1 or std.mem.allEqual(u8, &self.incarnation, 0)) return error.InvalidChangefeed;
        var buffer: [256]u8 = undefined;
        _ = try key(&buffer, self.group, self.database, self.name);
        if (self.write) |write| {
            try write.validate();
            if (!std.meta.eql(self.database, write.next.database) or !std.mem.eql(u8, self.name, write.next.name))
                return error.InvalidChangefeed;
        }
    }
};

pub const Reply = struct {
    version: u8 = 1,
    group: u64,
    incarnation: feed.Digest,
    database: feed.Digest,
    name: []const u8,
    record: ?Record,

    pub fn validate(self: Reply, request: Request) !void {
        try request.validate();
        if (self.version != request.version or self.group != request.group or !std.meta.eql(self.incarnation, request.incarnation) or
            !std.meta.eql(self.database, request.database) or !std.mem.eql(u8, self.name, request.name))
            return error.ChangefeedCorrupt;
        if (self.record) |record| {
            try record.validate();
            if (!std.meta.eql(record.database, self.database) or !std.mem.eql(u8, record.name, self.name))
                return error.ChangefeedCorrupt;
        }
        if (request.write) |write| {
            const record = self.record orelse return error.MetadataMutationOutcomeUnknown;
            if (!std.meta.eql(feed.hash(record), feed.hash(write.next))) return error.MetadataMutationOutcomeUnknown;
        }
    }
};

pub const Port = struct {
    ptr: *anyopaque,
    read: *const fn (*anyopaque, A, feed.Digest, []const u8, Context) anyerror!?std.json.Parsed(Record),
    mutate: *const fn (*anyopaque, A, Write, Context) anyerror!void,

    pub fn fromService(svc: anytype) Port {
        const Service = @typeInfo(@TypeOf(svc)).pointer.child;
        const Adapter = struct {
            fn read(raw: *anyopaque, a: A, database: feed.Digest, name: []const u8, context: Context) !?std.json.Parsed(Record) {
                const self: *Service = @ptrCast(@alignCast(raw));
                return readOnService(self, a, database, name, context);
            }
            fn mutate(raw: *anyopaque, a: A, write: Write, context: Context) !void {
                const self: *Service = @ptrCast(@alignCast(raw));
                return mutateOnService(self, a, write, context);
            }
        };
        return .{ .ptr = svc, .read = Adapter.read, .mutate = Adapter.mutate };
    }
};
