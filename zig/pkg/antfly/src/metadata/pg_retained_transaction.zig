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

//! Serialize a complete authorized native row transaction at its frozen slot
//! position. The retained schema determines tuple types and physical ordinals.
const std = @import("std");
const local = @import("antfly_local_sources");
const history = local.api_local_table_writes.table_write_source.history_page;
const Rows = local.storage_db_algebraic_relational_row_codec;
const schema_api = local.schema_mod;
const output = @import("../pgwire/pgoutput.zig");
const relation_values = @import("../pgwire/values.zig");
const spool = @import("pg_spool.zig");
const upload = @import("pg_spool_upload.zig");
const feeds = @import("changefeed_control.zig");
const A = std.mem.Allocator;

pub const Encoded = struct {
    arena: std.heap.ArenaAllocator,
    messages: []const []const u8,
    candidate: upload.Candidate,

    pub fn deinit(self: *Encoded) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// The caller must qualify complete publication membership and replica
/// identity. Only one immutable historical layout is interpreted per frame.
pub fn encode(a: A, state: feeds.State, page: history.Page, relation: relation_values.HistoricalRelation, issued_at: u64, byte_limit: usize) !Encoded {
    try state.validate();
    const slot = state.slot orelse return error.ChangefeedCapabilityUnsupported;
    const binding = slot.binding orelse return error.ChangefeedCapabilityUnsupported;
    const header = page.header orelse return error.InvalidPgSpool;
    if (state.phase != .streaming or issued_at == 0 or issued_at >= state.expires_at or
        state.roots.len != 1 or state.definition.?.tables.len != 1 or
        state.definition.?.tables[0].contract != .native_relational or
        header.transaction_scope != .root_local or header.committed_at_ns <= 0 or
        !std.meta.eql(header.namespace, state.roots[0].namespace))
        return error.ChangefeedCapabilityUnsupported;
    if (page.events.len == 0 or page.first_ordinal != 0 or page.frame_events != page.events.len or page.events.len > 65533 or
        page.next.event != 0 or page.next.sequence != header.sequence)
        return error.InvalidPgSpool;
    if (byte_limit < 47 or byte_limit > spool.max_bytes) return error.InvalidPgSpool;

    const roots = try state.rootsAt(a, slot.mapped_cursor);
    defer a.free(roots);
    if (roots[0].delivered.event != 0 or header.sequence != std.math.add(u64, roots[0].delivered.sequence, 1) catch return error.InvalidPgSpool)
        return error.ChangefeedResynchronizationRequired;

    const schema_bytes = header.schema_after orelse header.schema_before orelse return error.ChangefeedCapabilityUnsupported;
    if (header.schema_before) |before| if (!std.mem.eql(u8, before, schema_bytes)) return error.ChangefeedCapabilityUnsupported;
    for (page.events) |event| {
        if ((event.before != null and header.schema_before == null) or (event.after != null and header.schema_after == null))
            return error.ChangefeedCapabilityUnsupported;
    }
    var scratch_arena = std.heap.ArenaAllocator.init(a);
    defer scratch_arena.deinit();
    const scratch = scratch_arena.allocator();
    const parsed = try schema_api.parseValidatedTableSchema(scratch, schema_bytes);
    const schema = try schema_api.deriveRuntimeTableSchema(scratch, parsed);
    if (schema.storage_mode != .relational or schema.external_base_source != null or schema.relational_columns.len == 0)
        return error.ChangefeedCapabilityUnsupported;
    var layout = try Rows.PhysicalLayout.init(scratch, schema);
    defer layout.deinit();

    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const messages = try owned.alloc([]const u8, page.events.len + 3);
    const reserved_bytes = 47; // Fixed Begin and Commit message lengths.
    var bytes: usize = reserved_bytes;
    messages[1] = try relation.encodeRelation(owned, schema, @min(output.max_message_bytes, byte_limit - bytes));
    bytes += messages[1].len;
    for (page.events, messages[2 .. messages.len - 1]) |event, *message| {
        message.* = try relation.encodeChange(owned, schema, &layout, event.before, event.after, 0, @min(output.max_message_bytes, byte_limit - bytes));
        bytes += message.len;
    }
    const end_lsn = std.math.add(u64, slot.next_lsn, bytes) catch return error.InvalidPgSpool;
    const commit_lsn: output.Lsn = @fromBackingInt(end_lsn - 26);
    const time = try output.postgresTime(header.committed_at_ns);
    const staged = for (state.cursors) |receipt| {
        if (std.meta.eql(receipt.before, slot.mapped_cursor)) break receipt;
    } else null;
    if (staged == null and !std.meta.eql(state.latest_cursor, slot.mapped_cursor)) return error.ChangefeedConflict;
    const sequence = if (staged) |receipt| receipt.sequence else std.math.add(u64, state.latest_delivery, 1) catch return error.InvalidPgSpool;
    const original_issued_at = if (staged) |receipt| receipt.issued_at else issued_at;
    const xid: u32 = @intCast((sequence - 1) % (std.math.maxInt(u32) - 2) + 3);
    messages[0] = try output.encode(owned, .{ .begin = .{ .final_lsn = commit_lsn, .commit_time = time, .xid = xid } }, 21);
    messages[messages.len - 1] = try output.encode(owned, .{ .commit = .{ .commit_lsn = commit_lsn, .end_lsn = @fromBackingInt(end_lsn), .commit_time = time } }, 26);
    var hash = upload.WireHasher.init();
    for (messages) |message| try hash.add(message);
    const advances = try owned.dupe(feeds.Advance, &.{.{ .namespace = header.namespace, .position = .{ .sequence = header.sequence } }});
    var candidate: upload.Candidate = .{
        .delivery = .{ .sequence = sequence, .before = slot.mapped_cursor, .cursor = @splat(0), .authority = @splat(0), .issued_at = original_issued_at, .advances = advances },
        .projection = binding.projection,
        .completion = feeds.hash(.{ .domain = "antfly:pgoutput-root-local-completion:v1", .header = header, .events = page.events }),
        .wire = hash.final(),
        .messages = @intCast(messages.len),
        .visible_events = @intCast(page.events.len),
        .bytes = bytes,
        .xid = xid,
        .commit_time = time,
    };
    candidate.delivery.authority = candidate.proof(state);
    candidate.delivery.cursor = candidate.delivery.identity(state.id, state.epoch);
    if (staged) |receipt| {
        if (!std.meta.eql(feeds.hash(receipt), feeds.hash(candidate.delivery))) return error.ChangefeedConflict;
    }
    return .{ .arena = arena, .messages = messages, .candidate = candidate };
}

/// Source policy owns route qualification and publication membership. Its read
/// returns one complete frame after the supplied private retained position.
pub fn Producer(comptime Policy: type) type {
    return struct {
        policy: *Policy,
        allocator: A,
        byte_limit: usize,
        const Self = @This();
        const Spec = feeds.pg_slot.spool_control.Spec;

        pub fn authorize(self: *Self, state: feeds.State, request: anytype) !void {
            try request.ensureActive();
            try self.policy.authorize(state, request);
        }

        pub fn nowSeconds(self: *Self, request: anytype) !u64 {
            return self.policy.nowSeconds(request);
        }

        fn transaction(self: *Self, a: A, state: feeds.State, request: anytype) !?Encoded {
            try self.authorize(state, request);
            try state.validate();
            const slot = state.slot orelse return error.ChangefeedCapabilityUnsupported;
            if (state.roots.len != 1) return error.ChangefeedCapabilityUnsupported;
            const positions = try state.rootsAt(a, slot.mapped_cursor);
            defer a.free(positions);
            const position = positions[0].delivered;
            var page = (try self.policy.read(a, state, position, request)) orelse return null;
            defer page.deinit();
            const header = page.header orelse return error.InvalidPgSpool;
            const relation = try self.policy.relation(state, header, request);
            const now = try self.nowSeconds(request);
            if (now >= state.expires_at or now < state.created_at or header.committed_at_ns <= 0)
                return error.ChangefeedResynchronizationRequired;
            // Preflight runs again before an issue receipt exists. Tie its
            // timestamp to immutable source evidence so crossing a clock
            // tick cannot move the cursor between prepare and verify.
            const commit_seconds = @as(u64, @intCast(header.committed_at_ns)) / std.time.ns_per_s;
            const issued_at = @max(state.created_at, commit_seconds);
            var encoded = try encode(a, state, page, relation, issued_at, self.byte_limit);
            errdefer encoded.deinit();
            try self.authorize(state, request);
            return encoded;
        }

        pub fn prepare(self: *Self, a: A, state: feeds.State, request: anytype) !?upload.Candidate {
            var encoded = (try self.transaction(a, state, request)) orelse return null;
            defer encoded.deinit();
            var candidate = encoded.candidate;
            candidate.delivery.advances = try a.dupe(feeds.Advance, candidate.delivery.advances);
            return candidate;
        }

        pub fn verify(self: *Self, state: feeds.State, spec: Spec, request: anytype) !void {
            var iterator = try self.open(self.allocator, state, spec, request);
            iterator.deinit();
        }

        pub fn open(self: *Self, a: A, state: feeds.State, spec: Spec, request: anytype) !Iterator {
            var encoded = (try self.transaction(a, state, request)) orelse return error.ChangefeedResynchronizationRequired;
            errdefer encoded.deinit();
            const candidate = try encoded.candidate.serializationSpec(state, spec.storage);
            if (!std.meta.eql(candidate, spec)) return error.PgSpoolWireMismatch;
            return .{ .encoded = encoded };
        }

        pub const Iterator = struct {
            encoded: Encoded,
            position: usize = 0,

            pub fn deinit(self: *Iterator) void {
                self.encoded.deinit();
                self.* = undefined;
            }

            pub fn next(self: *Iterator, a: A, limit: usize, request: anytype) !?[]u8 {
                try request.ensureActive();
                if (self.position == self.encoded.messages.len) return null;
                const bytes = self.encoded.messages[self.position];
                if (bytes.len > limit) return error.InvalidPgSpool;
                const copy = try a.dupe(u8, bytes);
                self.position += 1;
                return copy;
            }
        };
    };
}

pub fn testRetainedTransaction() !void {
    const a = std.testing.allocator;
    var fixture = std.heap.ArenaAllocator.init(a);
    defer fixture.deinit();
    const scratch = fixture.allocator();
    const definition: feeds.Definition = .{
        .owner = @splat(1),
        .authorization = @splat(2),
        .projection = .rows,
        .tables = &.{.{ .table_id = 7, .binding = @splat(3), .policy_revision = 1, .contract = .native_relational }},
    };
    var namespace: feeds.Namespace = @splat(0);
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 1, .big);
    std.mem.writeInt(u64, namespace[16..24], 1, .big);
    const id = feeds.pg_slot.feedId(@splat(5), "retained");
    var state: feeds.State = .{};
    const mutations = [_]feeds.Mutation{
        .{ .create = .{ .id = id, .definition = definition, .now = 100, .expires_at = 200 } },
        .{ .plan_roots = &.{.{ .namespace = namespace, .ownership_epoch = 1 }} },
        .{ .enroll = .{ .namespace = namespace, .ownership_epoch = 1, .grant_sequence = 2, .consumer_epoch = 1, .start = 10 } },
        .{ .seal = .{ .cut = @splat(6), .slot = .{ .database = @splat(5), .name = "retained", .consistent_lsn = 100 } } },
    };
    for (mutations, 1..) |mutation, sequence| state = try mutation.apply(scratch, state, .{ .incarnation = @splat(4), .sequence = sequence });
    state.slot.?.binding = .{ .publications = &.{"retained_pub"}, .projection = @splat(8) };
    const schema_json =
        \\{"version":7,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const published = try schema_api.parseValidatedTableSchema(scratch, schema_json);
    const schema = try schema_api.deriveRuntimeTableSchema(scratch, published);
    const old = try Rows.serializeOrdinal(scratch, schema.version, schema.relational_columns, &.{
        .{ .ordinal = 0, .path = "id", .value_type = .i64_val, .value = .{ .i64_val = 17 } },
    }, @splat(0));
    const new = try Rows.serializeOrdinal(scratch, schema.version, schema.relational_columns, &.{
        .{ .ordinal = 0, .path = "id", .value_type = .i64_val, .value = .{ .i64_val = 18 } },
    }, @splat(0));
    var page: history.Page = .{
        .arena = undefined,
        .header = .{ .namespace = namespace, .sequence = 11, .transaction_id = @splat(9), .transaction_scope = .root_local, .committed_at_ns = 946684801000000000, .schema_before = schema_json, .schema_after = schema_json },
        .events = &.{.{ .key = "key", .before = old, .after = new }},
        .frame_events = 1,
        .next = .{ .sequence = 11 },
        .observed_latest = 11,
        .caught_up = true,
    };
    const relation: relation_values.HistoricalRelation = .{ .oid = 42, .namespace = "public", .name = "rows", .identity = .default, .key_ordinals = &.{0} };
    var first = try encode(a, state, page, relation, 111, 1024);
    defer first.deinit();
    state = try (feeds.Mutation{ .issue = first.candidate.delivery }).apply(scratch, state, .{ .incarnation = @splat(4), .sequence = 5 });
    var replay = try encode(a, state, page, relation, 112, 1024);
    defer replay.deinit();
    try std.testing.expectEqualDeep(first.messages, replay.messages);
    try std.testing.expectEqualDeep(first.candidate, replay.candidate);
    try std.testing.expectEqual(@as(u32, 4), first.candidate.messages);
    try std.testing.expectEqual(@as(i64, 1_000_000), first.candidate.commit_time);
    try std.testing.expectEqualStrings("BRUC", &.{ first.messages[0][0], first.messages[1][0], first.messages[2][0], first.messages[3][0] });
    try std.testing.expectEqual(@as(u8, 'K'), first.messages[2][5]);
    try std.testing.expectEqualStrings("17", first.messages[2][13..15]);
    try std.testing.expectEqual(@as(u64, 100) + first.candidate.bytes - 26, std.mem.readInt(u64, first.messages[0][1..9], .big));
    try std.testing.expectEqual(@as(u64, 100) + first.candidate.bytes, std.mem.readInt(u64, first.messages[3][10..18], .big));
    var changed_relation = relation;
    changed_relation.oid = 43;
    try std.testing.expectError(error.ChangefeedConflict, encode(a, state, page, changed_relation, 112, 1024));
    page.frame_events = 2;
    try std.testing.expectError(error.InvalidPgSpool, encode(a, state, page, relation, 111, 1024));
}
