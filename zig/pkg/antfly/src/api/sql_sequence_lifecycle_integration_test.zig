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

//! Unchanged PostgreSQL DDL witnesses through authenticated native owners.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const http = @import("http_server.zig");
const sequence = @import("../system_catalog/sequence_catalog.zig");
const wire = @import("antfly_metadata_openapi").types;
const SourceCampaign = @import("sql_document_mutation_lifecycle_integration_test.zig").SourceCampaign;
const Json = std.json.Value;

const Image = struct {
    kind: enum { sequence, table },
    integer: ?sequence.sequence.Integer = null,
    increment: ?i64 = null,
    minimum: ?i64 = null,
    maximum: ?i64 = null,
    start: ?i64 = null,
    cache: ?u64 = null,
    cycle: ?bool = null,
    last_value: ?i64 = null,
    is_called: ?bool = null,
    owned_by: ?struct { namespace: []const u8, table: []const u8, column: []const u8 } = null,
    rows: ?[]const []const Json = null,
};
const Entry = struct {
    id: []const u8,
    profile: []const u8,
    sql: []const u8,
    setup: []const []const u8,
    sqlstate: ?[]const u8,
    command_tag: []const u8,
    before: ?Image,
    after: ?Image,
    object_identity_unchanged: bool,
};

const Campaign = struct {
    pub const native_sessions = true;
    pub const native_auth = true;
    pub const cold_catalog = true;
    entry: Entry,
    inner: SourceCampaign,
    prior_id: ?u64 = null,
    final_id: ?u64 = null,
    source_owner: ?sequence.sequence.OwnedBy = null,

    fn cut(ctx: *fixture.Context, a: std.mem.Allocator) !sequence.Snapshot {
        const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const lookup: local.system_catalog_domain.ResolveMany = .{ .relations = &.{.{ .name = "usage_records" }}, .include_query_definitions = true };
        const bytes = try source.systemCatalog(a, .{}, .{ .resolve_many = lookup });
        const resolved = try std.json.parseFromSliceLeaky(local.system_catalog_domain.ResolvedMany, a, bytes, .{});
        try resolved.validateRelations(lookup);
        const owner = resolved.relations[0] orelse return error.MissingSourceRelation;
        const request: sequence.SnapshotRequest = .{
            .targets = &.{.{ .table = "usage_records_id_seq" }},
            .include_counters = true,
            .owner = .{ .guard = .{ .target = .{ .name = "usage_records" }, .logical_table = "usage_records", .owner = owner.owner, .incarnation = resolved.relation_epoch.?.incarnation }, .column = "id" },
        };
        const snapshot = try source.systemCatalog(a, .{ .setting_admin = true }, .{ .sql_sequence_snapshot = request });
        const value = try std.json.parseFromSliceLeaky(sequence.Snapshot, a, snapshot, .{});
        try value.validate(request);
        return value;
    }

    fn checkImage(self: *@This(), ctx: *fixture.Context, expected: ?Image) !?u64 {
        var scratch = std.heap.ArenaAllocator.init(ctx.alloc);
        defer scratch.deinit();
        const snapshot = try cut(ctx, scratch.allocator());
        const source_owner = snapshot.owner orelse return error.MissingSequenceOwner;
        if (self.source_owner) |prior| try std.testing.expectEqualDeep(prior, source_owner.column) else self.source_owner = source_owner.column;
        const entry = snapshot.entries[0];
        const image = expected orelse {
            try std.testing.expect(entry.value == .absent);
            return null;
        };
        if (image.kind == .table) {
            try std.testing.expect(entry.value == .other);
            try self.inner.verifyRows(ctx);
            return null;
        }
        try std.testing.expect(entry.value == .sequence);
        const definition = try entry.value.sequence.definition();
        const state = entry.counter orelse return error.MissingSequenceCounter;
        try std.testing.expectEqual(image.integer.?, definition.integer);
        try std.testing.expectEqual(image.increment.?, definition.increment);
        try std.testing.expectEqual(image.minimum.?, definition.minimum);
        try std.testing.expectEqual(image.maximum.?, definition.maximum);
        try std.testing.expectEqual(image.start.?, definition.start);
        try std.testing.expectEqual(image.cache.?, definition.cache);
        try std.testing.expectEqual(image.cycle.?, definition.cycle);
        try std.testing.expectEqual(image.last_value.?, state.last_value);
        try std.testing.expectEqual(image.is_called.?, state.is_called);
        if (image.owned_by) |owner| {
            try std.testing.expectEqualStrings("public", owner.namespace);
            try std.testing.expectEqualStrings("usage_records", owner.table);
            try std.testing.expectEqualStrings("id", owner.column);
            try std.testing.expectEqualDeep(snapshot.owner.?.column, definition.owned_by.?);
        } else try std.testing.expect(definition.owned_by == null);
        return entry.value.sequence.id;
    }

    fn success(ctx: *fixture.Context, sql: []const u8, tag: []const u8) !void {
        var response = try client.execute(ctx, sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("sequence setup sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings(tag, parsed.value.command_tag);
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.inner.setup(ctx);
        ctx.headers = ctx.admin_headers;
        if (std.mem.eql(u8, self.entry.profile, "existing_sequence")) {
            try success(ctx, self.entry.setup[2], "CREATE SEQUENCE");
            // Seed the real PostgreSQL cached high-water state through the
            // native allocator, not fabricated bytes or an uncalled sequence.
            var scratch = std.heap.ArenaAllocator.init(ctx.alloc);
            defer scratch.deinit();
            const a = scratch.allocator();
            const snapshot = try cut(ctx, a);
            const entry = snapshot.entries[0];
            var nonce: [16]u8 = undefined;
            try ctx.io.randomSecure(&nonce);
            const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
            const bytes = try source.systemCatalog(a, .{ .setting_admin = true }, .{ .sql_sequence_counter_mutate = .{
                .target = entry.target,
                .namespace_id = entry.namespace_id.?,
                .sequence_id = entry.value.sequence.id,
                .incarnation = snapshot.incarnation,
                .request = .{ .expected = entry.counter.?, .request_id = nonce, .operation = .reserve },
            } });
            const actual = try std.json.parseFromSliceLeaky(sequence.CounterOutcome, a, bytes, .{});
            const receipt = try actual.result();
            try std.testing.expectEqual(@as(i64, 100), receipt.lease.?.first);
            try std.testing.expectEqual(@as(u64, 4), receipt.lease.?.count);
        }
        self.prior_id = try self.checkImage(ctx, self.entry.before);
        if (std.mem.eql(u8, self.entry.id, "sql-0312")) {
            var reader = ctx.*;
            reader.headers = ctx.reader_headers;
            var denied = try client.execute(&reader, self.entry.sql, .never);
            defer denied.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 403), denied.status);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, denied.body, .{});
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings("42501", diagnostic.value.code);
            try std.testing.expectEqual(self.prior_id, try self.checkImage(ctx, self.entry.before));
            try self.inner.fingerprints(ctx, .prepared);
        }
        var response = try client.execute(ctx, self.entry.sql, .write_admission);
        defer response.deinit(ctx.alloc);
        std.debug.print("sequence original={s} profile={s} status={d}\n", .{ self.entry.id, self.entry.profile, response.status });
        if (self.entry.sqlstate) |code| {
            var parsed = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings(code, parsed.value.code);
        } else {
            if (response.status != 200) std.debug.print("sequence body={s}\n", .{response.body});
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings(self.entry.command_tag, parsed.value.command_tag);
            try std.testing.expectEqual(@as(i64, 0), parsed.value.rows_affected);
            try std.testing.expectEqual(@as(usize, 0), parsed.value.rows.len);
        }
        self.final_id = try self.checkImage(ctx, self.entry.after);
        if (self.entry.object_identity_unchanged) try std.testing.expectEqual(self.prior_id, self.final_id);
        try self.inner.fingerprints(ctx, .prepared);
        try self.inner.verify(ctx, false);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try std.testing.expectEqual(self.final_id, try self.checkImage(ctx, self.entry.after));
        try self.inner.verify(ctx, true);
    }
};

test "mounted SQL original sequence declarations preserve allocated counters ownership and all cold rows" {
    const a = std.testing.allocator;
    const originals = [_][]const u8{ "sql-0311", "sql-0312", "sql-0313", "sql-0314", "sql-0315", "sql-0316", "sql-0317" };
    const expected_profiles = [_]usize{ 3, 1, 3, 1, 1, 3, 3 };
    var observed_profiles: [originals.len]usize = @splat(0);
    const Reference = struct { format: u8, reference: []const u8, entries: []const Entry };
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.sequence_declaration_reference, .{});
    defer reference.deinit();
    try std.testing.expectEqual(@as(u8, 1), reference.value.format);
    try std.testing.expectEqual(@as(usize, 15), reference.value.entries.len);
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    for (reference.value.entries) |entry| {
        const index = for (originals, 0..) |id, i| {
            if (std.mem.eql(u8, id, entry.id)) break i;
        } else return error.UnexpectedSequenceOriginal;
        observed_profiles[index] += 1;
        try std.testing.expectEqualStrings((try corpus.get(entry.id)).sql, entry.sql);
        var retained = std.heap.ArenaAllocator.init(a);
        defer retained.deinit();
        const source_rows = [_][]const Json{&.{.{ .integer = 1 }}};
        const collision_rows = [_][]const Json{&.{.{ .string = "keep" }}};
        const tables = [_]@import("sql_document_mutation_lifecycle_integration_test.zig").SourceTable{
            .{ .name = "usage_records", .create = entry.setup[0], .seed = entry.setup[1], .read = "SELECT id FROM usage_records ORDER BY id", .final = .{ .columns = &.{"id"}, .column_oids = &.{20}, .rows = &source_rows } },
            .{ .name = "usage_records_id_seq", .create = if (std.mem.eql(u8, entry.profile, "existing_table")) entry.setup[2] else "", .seed = if (std.mem.eql(u8, entry.profile, "existing_table")) entry.setup[3] else "", .read = "SELECT id FROM usage_records_id_seq ORDER BY id", .final = .{ .columns = &.{"id"}, .column_oids = &.{25}, .rows = &collision_rows } },
        };
        try fixture.runWithState(Campaign, .{ .entry = entry, .inner = .{ .retained = &retained, .entry = .{ .id = entry.id, .profile = entry.profile, .sqlstate = entry.sqlstate, .command_tag = entry.command_tag, .affected = 0, .output = .{ .columns = &.{}, .column_oids = &.{}, .rows = &.{} }, .tables = tables[0..if (std.mem.eql(u8, entry.profile, "existing_table")) @as(usize, 2) else 1] } } });
    }
    try std.testing.expectEqualDeep(expected_profiles, observed_profiles);
}
