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

//! Private, replicated fan-in controls. Public batch JSON cannot construct
//! these commands. Owner ingress authenticates the table route and namespace;
//! deterministic apply checks the catalog, lease and exact observed state.
const std = @import("std");
const index = @import("group_source_index.zig");
const state = @import("group_source_state.zig");
const groups = @import("group_sources.zig");
const identity = @import("group_source_identity.zig");
pub const max_page_items = 128;
pub const max_partition_bytes = 4 * 1024 * 1024;
pub const Action = enum { invalidate, claim, heartbeat, publish, finish, acknowledge, backfill, retire, activate };
pub const Invalidate = struct { owner: state.Owner, event: index.Invalidation };
pub const Claim = struct { group: state.Group, pending: state.Pending, owner: groups.Digest, now_ms: u64, ttl_ms: u32 };
pub const Held = struct { group: state.Group, partition: index.Partition, lease: state.Lease, now_ms: u64, ttl_ms: u32 = 30000 };
pub const SourceCut = struct { owner: state.Owner, revision: u64 };
pub const Publish = struct {
    held: Held,
    /// The metadata route observation that selected every current source owner.
    topology_epoch: u64,
    sources: []const SourceCut,
    units: []const groups.Unit,
    /// Stable partition input digest, including an empty member set.
    input_digest: groups.Digest,
};
pub const Finish = struct { held: Held, input_digest: groups.Digest, through_sequence: u64, publication: groups.Digest };
pub const Acknowledge = struct { document: []const u8, key_base64: []const u8, observed_base64: []const u8, delivery: ?bool = null };
pub const Backfill = struct {
    /// Logical route of the source owner; never a group coordinator route.
    route: @import("group_source_bytes.zig").Bytes,
    /// Expected durable cursor and leader-selected bounded replay page.
    cursor: []const u8 = "",
    page: ?@import("group_source_page.zig").Page = null,
};
pub const Command = struct {
    action: Action,
    artifact: []const u8,
    definition: index.Definition,
    namespace: [24]u8,
    invalidate: ?Invalidate = null,
    claim: ?Claim = null,
    held: ?Held = null,
    publish: ?Publish = null,
    finish: ?Finish = null,
    acknowledge: ?Acknowledge = null,
    backfill: ?Backfill = null,
    pub fn validate(self: Command) !void {
        try self.definition.validate();
        if (self.artifact.len == 0 or self.artifact.len > 256) return error.InvalidGroupedSourceCommand;
        var bodies: usize = 0;
        inline for (.{ "invalidate", "claim", "held", "publish", "finish", "acknowledge", "backfill" }) |field| if (@field(self, field) != null) {
            bodies += 1;
        };
        if (bodies != 1) return error.InvalidGroupedSourceCommand;
        switch (self.action) {
            .invalidate => {
                const value = self.invalidate orelse return error.InvalidGroupedSourceCommand;
                if (!std.meta.eql(value.event.definition, self.definition) or value.event.source_revision == 0 or value.owner.incarnation == 0 or value.event.document.len == 0 or value.event.document.len > 4096) return error.InvalidGroupedSourceCommand;
                try validatePartition(value.event.partition);
            },
            .claim => {
                const value = self.claim orelse return error.InvalidGroupedSourceCommand;
                try self.validateGroup(value.group);
                try validatePartition(value.pending.partition);
                if (!std.mem.eql(u8, value.group.key, value.pending.partition.group) or value.pending.revision == 0 or value.now_ms == 0 or value.ttl_ms == 0 or value.ttl_ms > 300000) return error.InvalidGroupedSourceCommand;
            },
            .heartbeat => try self.validateHeld(self.held orelse return error.InvalidGroupedSourceCommand),
            .publish => {
                const value = self.publish orelse return error.InvalidGroupedSourceCommand;
                try self.validateHeld(value.held);
                if (value.sources.len == 0 or value.sources.len > 256 or value.units.len > max_page_items or std.mem.allEqual(u8, &value.input_digest, 0)) return error.InvalidGroupedSourceCommand;
                var bytes: usize = 0;
                for (value.sources) |source| if (source.owner.incarnation == 0) return error.InvalidGroupedSourceCommand;
                for (value.units) |unit| {
                    if (!std.mem.eql(u8, unit.group, value.held.group.key) or !std.mem.eql(u8, unit.segment, value.held.partition.segment) or unit.member_keys.len == 0 or unit.member_keys.len > 4096 or unit.ordinal_start > unit.ordinal_end or !std.unicode.utf8ValidateSlice(unit.text)) return error.InvalidGroupedSourceCommand;
                    bytes = std.math.add(usize, bytes, unit.text.len) catch return error.InvalidGroupedSourceCommand;
                    for (unit.member_keys) |member| {
                        if (member.len == 0 or member.len > 4096 or identity.isAnchor(member)) return error.InvalidGroupedSourceCommand;
                        bytes = std.math.add(usize, bytes, member.len) catch return error.InvalidGroupedSourceCommand;
                    }
                    if (bytes > max_partition_bytes) return error.GroupedSourcePartitionTooLarge;
                }
            },
            .finish => {
                const value = self.finish orelse return error.InvalidGroupedSourceCommand;
                try self.validateHeld(value.held);
                if (std.mem.allEqual(u8, &value.publication, 0) or std.mem.allEqual(u8, &value.input_digest, 0)) return error.InvalidGroupedSourceCommand;
            },
            .acknowledge => {
                const value = self.acknowledge orelse return error.InvalidGroupedSourceCommand;
                if (value.document.len == 0 or value.document.len > 4096 or value.key_base64.len == 0 or value.key_base64.len > 32768 or value.observed_base64.len == 0 or value.observed_base64.len > index.max_record_bytes + 36) return error.InvalidGroupedSourceCommand;
            },
            .backfill, .retire, .activate => {
                const value = self.backfill orelse return error.InvalidGroupedSourceCommand;
                if (value.cursor.len > 32768 or value.route.value.len > 4096) return error.InvalidGroupedSourceCommand;
                if (self.action != .activate) {
                    const page = value.page orelse return error.InvalidGroupedSourceCommand;
                    try page.validate();
                }
            },
        }
    }
    fn validateGroup(self: Command, group: state.Group) !void {
        if (!std.meta.eql(group.definition, self.definition) or group.key.len == 0 or group.key.len > 4096) return error.InvalidGroupedSourceCommand;
    }
    fn validateHeld(self: Command, held: Held) !void {
        try self.validateGroup(held.group);
        try validatePartition(held.partition);
        if (!std.mem.eql(u8, held.group.key, held.partition.group) or held.lease.token == 0 or held.lease.revision == 0 or held.now_ms == 0 or held.ttl_ms == 0 or held.ttl_ms > 300000) return error.InvalidGroupedSourceCommand;
    }
    pub fn routeAlloc(self: Command, a: std.mem.Allocator) ![]u8 {
        try self.validate();
        return switch (self.action) {
            .invalidate => (state.Group{ .definition = self.definition, .key = self.invalidate.?.event.partition.group }).anchor(a),
            .claim => self.claim.?.group.anchor(a),
            .heartbeat => self.held.?.group.anchor(a),
            .publish => self.publish.?.held.group.anchor(a),
            .finish => self.finish.?.held.group.anchor(a),
            .acknowledge => a.dupe(u8, self.acknowledge.?.document),
            .backfill, .retire, .activate => a.dupe(u8, self.backfill.?.route.value),
        };
    }
};
fn validatePartition(partition: index.Partition) !void {
    if (partition.group.len == 0 or partition.group.len > 4096 or partition.segment.len == 0 or partition.segment.len > 4096) return error.InvalidGroupedSourceCommand;
}
pub const QueryAction = enum { definitions, settled, readiness, partition, pending, prior, status, outbox, backfill, retirement, activation, backfill_page, retirement_page };
pub const Query = struct {
    action: QueryAction,
    artifact: []const u8 = "",
    definition: index.Definition = .{ .table_incarnation = 0, .configuration = @splat(0) },
    group: []const u8 = "",
    segment: []const u8 = "",
    cell: i64 = 0,
    ordinal: i64 = std.math.minInt(i64),
    document: []const u8 = "",
    cursor: []const u8 = "",
    limit: u16 = max_page_items,
    max_bytes: u32 = 65536,
    source_owner: ?state.Owner = null,
    event: ?index.Invalidation = null,
    documents: []const []const u8 = &.{},
    through_sequence: u64 = 0,
    pub fn validate(self: Query) !void {
        if (self.action == .definitions) {
            if (self.artifact.len != 0 or self.group.len != 0 or self.segment.len != 0 or self.cursor.len != 0 or self.source_owner != null or self.event != null) return error.InvalidGroupedSourceQuery;
            return;
        }
        try self.definition.validate();
        if ((self.event == null) != (self.source_owner == null)) return error.InvalidGroupedSourceQuery;
        if (self.event) |event| if (self.action != .status or !std.meta.eql(event.definition, self.definition) or !std.mem.eql(u8, event.partition.group, self.group)) return error.InvalidGroupedSourceQuery;
        if (self.documents.len > 384 or (self.documents.len > 0 and self.action != .settled and self.action != .readiness)) return error.InvalidGroupedSourceQuery;
        if (self.action == .readiness and self.documents.len > 16) return error.InvalidGroupedSourceQuery;
        var root: ?[]const u8 = null;
        for (self.documents) |document| {
            const selected = identity.root(document) orelse return error.InvalidGroupedSourceQuery;
            const configuration = identity.configurationOf(document) orelse return error.InvalidGroupedSourceQuery;
            if (!std.mem.eql(u8, &configuration, &self.definition.configuration) or identity.tableOf(document) != self.definition.table_incarnation) return error.InvalidGroupedSourceQuery;
            if (root) |previous| {
                if (!std.mem.eql(u8, previous, selected)) return error.InvalidGroupedSourceQuery;
            } else root = selected;
        }
        if (self.artifact.len == 0 or self.artifact.len > 256 or self.group.len > 4096 or self.segment.len > 4096 or self.document.len > 4096 or self.cursor.len > 32768 or self.limit == 0 or self.limit > max_page_items or self.max_bytes == 0 or self.max_bytes > max_partition_bytes) return error.InvalidGroupedSourceQuery;
        if ((self.action == .partition or self.action == .prior) and self.group.len == 0) return error.InvalidGroupedSourceQuery;
        if (self.action == .partition and self.segment.len == 0) return error.InvalidGroupedSourceQuery;
    }
};

/// A private control is exclusive. Reflecting the request keeps newly added
/// mutation fields from accidentally bypassing this admission boundary.
pub fn validateRequest(req: anytype) !void {
    const command = req.group_source orelse return;
    try command.validate();
    const empty: @TypeOf(req) = .{};
    inline for (@typeInfo(@TypeOf(req)).@"struct".field_names, @typeInfo(@TypeOf(req)).@"struct".field_types) |name, T| {
        if (comptime !std.mem.eql(u8, name, "group_source") and !std.mem.eql(u8, name, "sync_level")) {
            if (comptime @typeInfo(T) == .pointer and @typeInfo(T).pointer.size == .slice) {
                if (@field(req, name).len != 0) return error.InvalidGroupedSourceCommand;
            } else if (!std.meta.eql(@field(req, name), @field(empty, name))) return error.InvalidGroupedSourceCommand;
        }
    }
}
