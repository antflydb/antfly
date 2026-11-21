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
fn commitTime(page: history.Page) !i64 {
    const header = page.header orelse return error.InvalidPgSpool;
    switch (header.transaction_scope) {
        .root_local => {
            if (page.completion != null or header.committed_at_ns <= 0) return error.ChangefeedCapabilityUnsupported;
            return header.committed_at_ns;
        },
        .coordinated => {
            const proof = page.completion orelse return error.ReadUnavailable;
            if (proof.commit_version == 0 or std.mem.allEqual(u8, &proof.certificate_digest, 0)) return error.HistoryCorrupt;
            return std.math.cast(i64, proof.commit_version) orelse error.HistoryCorrupt;
        },
        .unknown => return error.ChangefeedCapabilityUnsupported,
    }
}

pub const Frame = struct {
    stream: ?feeds.Namespace = null,
    page: history.Page,
    relation: relation_values.HistoricalRelation,
};

pub fn encode(a: A, state: feeds.State, frames: []const Frame, issued_at: u64, byte_limit: usize) !Encoded {
    try state.validate();
    const slot = state.slot orelse return error.ChangefeedCapabilityUnsupported;
    const binding = slot.binding orelse return error.ChangefeedCapabilityUnsupported;
    if (state.phase != .streaming or issued_at == 0 or issued_at >= state.expires_at)
        return error.ChangefeedCapabilityUnsupported;
    if (frames.len == 0 or frames.len > state.roots.len or frames.len > feeds.max_advances or byte_limit < 47 or byte_limit > spool.max_bytes)
        return error.InvalidPgSpool;
    const first = frames[0].page;
    const first_header = first.header orelse return error.InvalidPgSpool;
    const commit_time_ns = try commitTime(first);
    if (first.completion == null and frames.len != 1) return error.InvalidPgSpool;

    const roots = try state.rootsAt(a, slot.mapped_cursor);
    defer a.free(roots);
    if (first.completion != null and roots.len > 1) {
        if (first.completion_effects.len != roots.len) return error.HistoryCorrupt;
        var member: usize = 0;
        for (roots, first.completion_effects) |root, effect| {
            if (effect == null) continue;
            if (member >= frames.len) return error.ReadUnavailable;
            const matched = frames[member].page.header orelse return error.InvalidPgSpool;
            if (!std.meta.eql(root.namespace, matched.namespace) or effect.?.sequence != matched.sequence)
                return error.HistoryCorrupt;
            member += 1;
        }
        if (member != frames.len) return error.HistoryCorrupt;
    }
    const effects_digest = feeds.hash(first.completion_effects);
    var message_count: usize = 2;
    for (frames) |frame| {
        const page = frame.page;
        if (page.events.len == 0 or page.first_ordinal != 0 or page.frame_events != page.events.len)
            return error.InvalidPgSpool;
        message_count += page.events.len + 1;
        if (message_count > std.math.maxInt(u16)) return error.HistoryTooLarge;
    }

    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const messages = try owned.alloc([]const u8, message_count);
    const advances = try owned.alloc(feeds.Advance, frames.len);
    var bytes: usize = 47; // Fixed Begin and Commit message lengths.
    var next: usize = 1;
    for (frames, advances, 0..) |frame, *advance, index| {
        const page = frame.page;
        const header = page.header orelse return error.InvalidPgSpool;
        if (!std.meta.eql(header.transaction_id, first_header.transaction_id) or !std.meta.eql(page.completion, first.completion) or !std.meta.eql(effects_digest, feeds.hash(page.completion_effects)) or try commitTime(page) != commit_time_ns)
            return error.HistoryCorrupt;
        const stream = frame.stream orelse header.namespace;
        if (index != 0 and std.mem.order(u8, &advances[index - 1].namespace, &stream) != .lt)
            return error.InvalidPgSpool;
        if (page.next.event != 0 or page.next.sequence != header.sequence) return error.InvalidPgSpool;
        const root = roots[try state.rootIndex(stream)];
        if (!std.meta.eql(root.storageNamespace(), header.namespace)) return error.HistoryCorrupt;
        if (root.delivered.event != 0 or header.sequence != std.math.add(u64, root.delivered.sequence, 1) catch return error.InvalidPgSpool)
            return error.ChangefeedResynchronizationRequired;
        const table_id = std.mem.readInt(u64, header.namespace[0..8], .big);
        for (state.definition.?.tables) |table| {
            if (table.table_id != table_id) continue;
            if (table.contract != .native_relational) return error.ChangefeedCapabilityUnsupported;
            break;
        } else return error.ChangefeedResynchronizationRequired;
        advance.* = .{ .namespace = stream, .position = .{ .sequence = header.sequence } };

        var scratch_arena = std.heap.ArenaAllocator.init(a);
        defer scratch_arena.deinit();
        const scratch = scratch_arena.allocator();
        const schema_bytes = header.schema_after orelse header.schema_before orelse return error.ChangefeedCapabilityUnsupported;
        if (header.schema_before) |before| if (!std.mem.eql(u8, before, schema_bytes)) return error.ChangefeedCapabilityUnsupported;
        const parsed = try schema_api.parseValidatedTableSchema(scratch, schema_bytes);
        const schema = try schema_api.deriveRuntimeTableSchema(scratch, parsed);
        if (schema.storage_mode != .relational or schema.external_base_source != null or schema.relational_columns.len == 0)
            return error.ChangefeedCapabilityUnsupported;
        var layout = try Rows.PhysicalLayout.init(scratch, schema);
        defer layout.deinit();
        messages[next] = try frame.relation.encodeRelation(owned, schema, @min(output.max_message_bytes, byte_limit - bytes));
        bytes += messages[next].len;
        next += 1;
        for (page.events) |event| {
            if ((event.before != null and header.schema_before == null) or (event.after != null and header.schema_after == null))
                return error.ChangefeedCapabilityUnsupported;
            messages[next] = try frame.relation.encodeChange(owned, schema, &layout, event.before, event.after, 0, @min(output.max_message_bytes, byte_limit - bytes));
            bytes += messages[next].len;
            next += 1;
        }
    }
    const end_lsn = std.math.add(u64, slot.next_lsn, bytes) catch return error.InvalidPgSpool;
    const commit_lsn: output.Lsn = @fromBackingInt(end_lsn - 26);
    const time = try output.postgresTime(commit_time_ns);
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
    var candidate: upload.Candidate = .{
        .delivery = .{ .sequence = sequence, .before = slot.mapped_cursor, .cursor = @splat(0), .authority = @splat(0), .issued_at = original_issued_at, .advances = advances },
        .projection = binding.projection,
        .completion = if (first.completion) |proof| proof.certificate_digest else feeds.hash(.{ .domain = "antfly:pgoutput-root-local-completion:v1", .header = first_header, .events = first.events }),
        .wire = hash.final(),
        .messages = @intCast(messages.len),
        .visible_events = @intCast(messages.len - frames.len - 2),
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

/// Source policy authorizes the complete publication and qualifies the relation
/// for the captured schema. The retained transport owns physical routing.
pub fn Producer(comptime Policy: type) type {
    return struct {
        policy: *Policy,
        allocator: A,
        source: @import("../api/history_roots.zig").Source,
        page_events: u32 = 1024,
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

        const root_source = @import("../api/history_roots.zig");

        fn transaction(self: *Self, a: A, state: feeds.State, request: anytype) !?Encoded {
            try self.authorize(state, request);
            try state.validate();
            const slot = state.slot orelse return error.ChangefeedCapabilityUnsupported;
            const positions = try state.rootsAt(a, slot.mapped_cursor);
            defer a.free(positions);
            const now = try self.nowSeconds(request);
            if (now >= state.expires_at or now < state.created_at) return error.ChangefeedResynchronizationRequired;
            const context = try self.policy.context(request);
            var captured = try self.source.capturePhysical(a, state.definition.?, context);
            defer captured.deinit();
            // An issued cursor pins the first participant even when another
            // root advances between admission and spool verification.
            for (state.cursors) |receipt| if (std.meta.eql(receipt.before, slot.mapped_cursor)) {
                if (receipt.advances.len == 0) return error.ChangefeedResynchronizationRequired;
                return self.readTransaction(a, state, &captured, positions, try state.rootIndex(receipt.advances[0].namespace), now, request);
            };
            if (positions.len == 0) return error.ChangefeedResynchronizationRequired;
            // Rotate probes at each mapped delivery so a busy root leaves
            // another root a chance to deliver its next eligible transaction.
            const start: usize = @intCast(state.latest_delivery % positions.len);
            for (0..positions.len) |offset| {
                const index = (start + offset) % positions.len;
                if (positions[index].released) continue;
                const candidate = self.readTransaction(a, state, &captured, positions, index, now, request) catch |err| switch (err) {
                    error.ReadUnavailable => continue,
                    else => return err,
                };
                if (candidate) |ready| return ready;
            }
            return null;
        }

        fn readTransaction(self: *Self, a: A, state: feeds.State, captured: *const root_source.Captured, positions: []const feeds.Root, seed: usize, now: u64, request: anytype) !?Encoded {
            const context = try self.policy.context(request);
            var read: root_source.Source.RelationalFrameRead = .{ .root_index = seed, .position = positions[seed].delivered, .now = now, .max_events_per_page = self.page_events };
            var first = (try self.source.readRelationalFrame(a, state, captured, read, context)) orelse return null;
            defer first.deinit();
            const first_header = first.header orelse return error.InvalidPgSpool;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            var frames: std.ArrayList(Frame) = .empty;
            defer for (frames.items) |*frame| {
                if (!std.meta.eql(frame.page.header.?.namespace, first_header.namespace)) frame.page.deinit();
            };

            if (first.completion == null or positions.len == 1) {
                try frames.append(scratch, .{ .stream = positions[seed].namespace, .page = first, .relation = try self.policy.relation(scratch, state, first_header, request) });
            } else {
                if (first.completion_effects.len != positions.len or first.completion_effects[seed] == null)
                    return error.HistoryCorrupt;
                for (positions, first.completion_effects) |position, effect| {
                    const expected = effect orelse continue;
                    if (position.delivered.event != 0 or expected.sequence <= position.delivered.sequence)
                        return error.ChangefeedResynchronizationRequired;
                    if (expected.sequence - position.delivered.sequence != 1) return null;
                }
                const frame_codec = local.storage_committed_history.frame;
                var remaining = std.math.sub(usize, read.max_frame_bytes, try frame_codec.encodedLength(first_header, first.events)) catch return error.HistoryTooLarge;
                for (positions, first.completion_effects, 0..) |position, effect, index| {
                    if (effect == null) continue;
                    if (index == seed) {
                        try frames.append(scratch, .{ .stream = positions[seed].namespace, .page = first, .relation = try self.policy.relation(scratch, state, first_header, request) });
                        continue;
                    }
                    read.root_index = index;
                    read.position = position.delivered;
                    read.max_frame_bytes = @intCast(remaining);
                    var page = (try self.source.readRelationalFrame(a, state, captured, read, context)) orelse return error.ReadUnavailable;
                    errdefer page.deinit();
                    const header = page.header orelse return error.HistoryCorrupt;
                    remaining = std.math.sub(usize, remaining, try frame_codec.encodedLength(header, page.events)) catch return error.HistoryTooLarge;
                    try frames.append(scratch, .{ .stream = position.namespace, .page = page, .relation = try self.policy.relation(scratch, state, header, request) });
                }
            }
            // A replay must use commit evidence rather than the polling clock.
            const commit_seconds = @as(u64, @intCast(try commitTime(first))) / std.time.ns_per_s;
            const issued_at = @max(state.created_at, commit_seconds);
            var encoded = try encode(a, state, frames.items, issued_at, self.byte_limit);
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
    {
        var coordinated = page;
        coordinated.header.?.transaction_scope = .coordinated;
        coordinated.header.?.committed_at_ns = 0;
        try std.testing.expectError(error.ReadUnavailable, encode(a, state, &.{.{ .page = coordinated, .relation = relation }}, 111, 1024));
        coordinated.completion = .{ .certificate_digest = @splat(8), .commit_version = 946684801000000000 };
        var qualified = try encode(a, state, &.{.{ .page = coordinated, .relation = relation }}, 111, 1024);
        defer qualified.deinit();
        try std.testing.expectEqual(@as(i64, 1_000_000), qualified.candidate.commit_time);
        try std.testing.expectEqual(coordinated.completion.?.certificate_digest, qualified.candidate.completion);
    }
    var first = try encode(a, state, &.{.{ .page = page, .relation = relation }}, 111, 1024);
    defer first.deinit();
    state = try (feeds.Mutation{ .issue = first.candidate.delivery }).apply(scratch, state, .{ .incarnation = @splat(4), .sequence = 5 });
    var replay = try encode(a, state, &.{.{ .page = page, .relation = relation }}, 112, 1024);
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
    try std.testing.expectError(error.ChangefeedConflict, encode(a, state, &.{.{ .page = page, .relation = changed_relation }}, 112, 1024));
    {
        var peer_namespace = namespace;
        std.mem.writeInt(u64, peer_namespace[0..8], 8, .big);
        var multi_definition = definition;
        multi_definition.tables = &.{
            definition.tables[0],
            .{ .table_id = 8, .binding = @splat(14), .policy_revision = 1, .contract = .native_relational },
        };
        var multi: feeds.State = .{};
        const multi_mutations = [_]feeds.Mutation{
            .{ .create = .{ .id = id, .definition = multi_definition, .now = 100, .expires_at = 200 } },
            .{ .plan_roots = &.{ .{ .namespace = namespace, .ownership_epoch = 1 }, .{ .namespace = peer_namespace, .ownership_epoch = 1 } } },
            .{ .enroll = .{ .namespace = namespace, .ownership_epoch = 1, .grant_sequence = 2, .consumer_epoch = 1, .start = 10 } },
            .{ .enroll = .{ .namespace = peer_namespace, .ownership_epoch = 1, .grant_sequence = 2, .consumer_epoch = 1, .start = 12 } },
            .{ .seal = .{ .cut = @splat(6), .slot = .{ .database = @splat(5), .name = "retained", .consistent_lsn = 100 } } },
        };
        for (multi_mutations, 1..) |mutation, sequence| multi = try mutation.apply(scratch, multi, .{ .incarnation = @splat(4), .sequence = sequence });
        multi.slot.?.binding = .{ .publications = &.{"retained_pub"}, .projection = @splat(8) };
        var origin = page;
        origin.header.?.transaction_scope = .coordinated;
        origin.header.?.committed_at_ns = 0;
        origin.completion = .{ .certificate_digest = @splat(8), .commit_version = 946684801000000000 };
        origin.completion_effects = &.{ .{ .sequence = 11, .frame_digest = @splat(29) }, .{ .sequence = 13, .frame_digest = @splat(30) } };
        var peer = origin;
        peer.header.?.namespace = peer_namespace;
        peer.header.?.sequence = 13;
        peer.next.sequence = 13;
        peer.observed_latest = 13;
        const both = [_]Frame{ .{ .page = origin, .relation = relation }, .{ .page = peer, .relation = changed_relation } };
        try std.testing.expectError(error.ReadUnavailable, encode(a, multi, both[0..1], 111, 1024));
        var pair = try encode(a, multi, &both, 111, 1024);
        defer pair.deinit();
        try std.testing.expectEqual(@as(u32, 2), pair.candidate.visible_events);
        try std.testing.expectEqual(@as(u32, 6), pair.candidate.messages);
        try std.testing.expectEqual(@as(u64, 11), pair.candidate.delivery.advances[0].position.sequence);
        try std.testing.expectEqual(@as(u64, 13), pair.candidate.delivery.advances[1].position.sequence);
        try std.testing.expectEqualStrings("BRURUC", &.{ pair.messages[0][0], pair.messages[1][0], pair.messages[2][0], pair.messages[3][0], pair.messages[4][0], pair.messages[5][0] });
    }
    page.frame_events = 2;
    try std.testing.expectError(error.InvalidPgSpool, encode(a, state, &.{.{ .page = page, .relation = relation }}, 111, 1024));
}
