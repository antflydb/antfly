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

const ExecutionCampaign = struct {
    pub const native_sessions = true;
    pub const native_auth = true;
    pub const cold_catalog = true;
    first: [32]u8 = undefined,
    second: [32]u8 = undefined,

    fn open(ctx: *fixture.Context) ![32]u8 {
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(wire.SQLConnectionResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(usize, 32), parsed.value.connection_id.len);
        return parsed.value.connection_id[0..32].*;
    }
    fn call(ctx: *fixture.Context, connection: ?[]const u8, sql: []const u8, code: ?[]const u8, expected: []const []const Json) !void {
        const ddl = std.mem.startsWith(u8, sql, "CREATE ") or std.mem.startsWith(u8, sql, "ALTER ") or std.mem.startsWith(u8, sql, "DROP ");
        return callWithRetry(ctx, connection, sql, code, expected, if (ddl) .write_admission else .never);
    }
    fn callWithRetry(ctx: *fixture.Context, connection: ?[]const u8, sql: []const u8, code: ?[]const u8, expected: []const []const Json, retry: client.Retry) !void {
        var response = try client.executeConnected(ctx, null, connection, sql, retry);
        defer response.deinit(ctx.alloc);
        if (code) |state| {
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
            defer diagnostic.deinit();
            if (!std.mem.eql(u8, state, diagnostic.value.code)) std.debug.print("sequence diagnostic sql={s} expected={s} body={s}\n", .{ sql, state, response.body });
            try std.testing.expectEqualStrings(state, diagnostic.value.code);
            return;
        }
        if (response.status != 200) std.debug.print("sequence execution sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(expected.len, parsed.value.rows.len);
        for (parsed.value.rows, expected, 0..) |actual, wanted, index| {
            const matches = try @import("sql_parity_reference.zig").rowMatchesWithNulls(ctx.alloc, parsed.value.columns, actual, if (parsed.value.sql_nulls) |nulls| nulls[index] else null, wanted, null);
            if (!matches) std.debug.print("sequence row mismatch sql={s} body={s}\n", .{ sql, response.body });
            try std.testing.expect(matches);
        }
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        // Exercise sequence effects in a published native database, as the
        // original sequence-DDL witnesses do with their pre-existing owner.
        try call(ctx, null, "CREATE TABLE sequence_anchor(id TEXT PRIMARY KEY,n BIGINT)", null, &.{});
        const anchor = try fixture.waitServingSchema(ctx, "sequence_anchor", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, anchor.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "sequence_anchor");
        try call(ctx, null, "INSERT INTO sequence_anchor(id,n) VALUES ('a',0),('b',0)", null, &.{});
        try call(ctx, null, "CREATE SEQUENCE first_seq START 100 INCREMENT 10 CACHE 4", null, &.{});
        try call(ctx, null, "CREATE SEQUENCE second_seq", null, &.{});
        self.first = try open(ctx);
        self.second = try open(ctx);
        const Reference = struct { sessions: []const struct { sql: []const u8, connection: []const u8, sqlstate: ?[]const u8, output: ?struct { rows: []const []const Json } } };
        var reference = try std.json.parseFromSlice(Reference, ctx.alloc, local.sql_parity_fixtures.identity_reference, .{ .ignore_unknown_fields = true });
        defer reference.deinit();
        for (reference.value.sessions) |step| {
            const id = if (std.mem.eql(u8, step.connection, "first")) &self.first else &self.second;
            try call(ctx, id, step.sql, step.sqlstate, if (step.output) |output| output.rows else &.{});
        }
        try self.pgwire(ctx);
        try self.documentTruncate(ctx);
        try call(ctx, &self.first, "BEGIN", null, &.{});
        try call(ctx, &self.first, "SELECT nextval('second_seq')", null, &.{&.{.{ .integer = 501 }}});
        try call(ctx, &self.first, "SAVEPOINT retained", null, &.{});
        try call(ctx, &self.first, "SELECT nextval('second_seq')", null, &.{&.{.{ .integer = 502 }}});
        try call(ctx, &self.first, "ROLLBACK TO retained", null, &.{});
        try call(ctx, &self.first, "SELECT currval('second_seq'),lastval()", null, &.{&.{ .{ .integer = 502 }, .{ .integer = 502 } }});
        try call(ctx, &self.first, "ROLLBACK", null, &.{});
        try call(ctx, &self.first, "SELECT currval('second_seq'),lastval()", null, &.{&.{ .{ .integer = 502 }, .{ .integer = 502 } }});
        try call(ctx, &self.first, "BEGIN READ ONLY", null, &.{});
        try call(ctx, &self.first, "SELECT nextval('second_seq')", "25006", &.{});
        try call(ctx, &self.first, "ROLLBACK", null, &.{});
        try call(ctx, &self.first, "SELECT nextval('first_seq')", null, &.{&.{.{ .integer = 993 }}});
    }
    fn documentTruncate(_: *@This(), ctx: *fixture.Context) !void {
        const schema = "{\"schema\":{\"storage_mode\":\"document\",\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}},\"additionalProperties\":true}}}}}";
        var created = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.metadata_uri, "/db/v1/tables/docs", .POST, schema);
        defer created.deinit(ctx.alloc);
        if (created.status != 200) std.debug.print("document truncate create status={d} body={s}\n", .{ created.status, created.body });
        try std.testing.expectEqual(@as(u16, 200), created.status);
        const prior = try fixture.waitServingSchema(ctx, "docs", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, prior.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "docs");
        try call(ctx, null, "INSERT INTO docs(_id,title) VALUES('doc:a','before')", null, &.{});
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        const original = try corpus.get("sql-1103");
        const Reference = struct { document_truncate: struct { id: []const u8, sql: []const u8, empty: struct { rows: []const []const Json } } };
        var reference = try std.json.parseFromSlice(Reference, ctx.alloc, local.sql_parity_fixtures.identity_reference, .{ .ignore_unknown_fields = true });
        defer reference.deinit();
        try std.testing.expectEqualStrings("sql-1103", reference.value.document_truncate.id);
        try std.testing.expectEqualStrings(original.sql, reference.value.document_truncate.sql);
        try std.testing.expectEqual(@as(usize, 0), reference.value.document_truncate.empty.rows.len);
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        try call(&reader, null, original.sql, "42501", &.{});
        try call(ctx, null, "SELECT _id,title FROM docs ORDER BY _id", null, &.{&.{ .{ .string = "doc:a" }, .{ .string = "before" } }});
        var coordinator = ctx.*;
        coordinator.base = ctx.metadata_uri;
        var response = try client.execute(&coordinator, original.sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status != 202 and response.status != 409) std.debug.print("document TRUNCATE status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expect(response.status == 202 or response.status == 409);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings("DDL PENDING", parsed.value.command_tag);
        const receipt = parsed.value.ddl_receipt orelse return error.MissingTruncateReceipt;
        const expected_state: @TypeOf(receipt.state) = if (response.status == 202) .pending else .admission_unknown;
        try std.testing.expectEqual(expected_state, receipt.state);
        const job_id = receipt.restore_job_id orelse return error.MissingTruncateJob;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s;
        while (true) {
            try ctx.checkDrivers();
            var observed = try fixture.job(ctx.alloc, ctx.transport, ctx.headers, ctx.metadata_uri, job_id);
            defer observed.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), observed.status);
            var job = try std.json.parseFromSlice(wire.RestoreJob, ctx.alloc, observed.body, .{ .ignore_unknown_fields = true });
            defer job.deinit();
            if (std.mem.eql(u8, job.value.phase, "succeeded")) break;
            if (std.mem.eql(u8, job.value.phase, "failed")) {
                std.debug.print("document truncate failed {s}\n", .{observed.body});
                return error.DocumentTruncateFailed;
            }
            if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.DocumentTruncateTimeout;
            try ctx.io.sleep(.fromMilliseconds(10), .awake);
        }
        const current = try fixture.waitServingSchema(ctx, "docs", 0);
        try std.testing.expect(current.table_id != prior.table_id);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, current.table_id);
        try call(ctx, null, "SELECT _id,title FROM docs ORDER BY _id", null, &.{});
        try call(ctx, null, "INSERT INTO docs(_id,title) VALUES('doc:a','after')", null, &.{});
        try call(ctx, null, "SELECT _id,title FROM docs ORDER BY _id", null, &.{&.{ .{ .string = "doc:a" }, .{ .string = "after" } }});
    }
    fn frame(writer: *std.Io.Writer, tag: u8, bytes: []const u8) !void {
        try writer.writeByte(tag);
        try writer.writeInt(u32, @intCast(bytes.len + 4), .big);
        try writer.writeAll(bytes);
    }
    fn pgwire(_: *@This(), ctx: *fixture.Context) !void {
        try call(ctx, null, "CREATE SEQUENCE pg_seq CACHE 4", null, &.{});
        var arena = std.heap.ArenaAllocator.init(ctx.alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const protocol = @import("../pgwire/protocol.zig");
        var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = if (ctx.data.http_server) |*api| api else return error.MissingNativeSqlApi };
        var input = std.Io.Writer.Allocating.init(a);
        defer input.deinit();
        const startup = "user\x00pk-rewriter\x00database\x00default\x00\x00";
        try input.writer.writeInt(u32, @intCast(startup.len + 8), .big);
        try input.writer.writeInt(u32, 196608, .big);
        try input.writer.writeAll(startup);
        try frame(&input.writer, 'p', "pk-rewrite-durable-password\x00");
        for ([_][]const u8{
            "SELECT currval('pg_seq')",                    "SELECT nextval('pg_seq')", "BEGIN",             "SELECT nextval('pg_seq')", "ROLLBACK",        "SELECT currval('pg_seq')",
            "PREPARE seqplan AS SELECT nextval('pg_seq')", "EXECUTE seqplan",          "DISCARD SEQUENCES", "SELECT currval('pg_seq')", "EXECUTE seqplan", "DISCARD ALL",
            "EXECUTE seqplan",
        }) |sql| try frame(&input.writer, 'Q', try std.mem.concat(a, u8, &.{ sql, "\x00" }));
        try frame(&input.writer, 'X', "");
        var reader = std.Io.Reader.fixed(input.written());
        var output = std.Io.Writer.Allocating.init(a);
        defer output.deinit();
        var connection: protocol.Session = .{ .alloc = a, .io = ctx.io, .source = adapter.backend(), .reader = &reader, .writer = &output.writer };
        defer connection.deinit();
        try connection.run();
        var cursor: protocol.Cursor = .{ .bytes = output.written() };
        var rows: std.ArrayList(i64) = .empty;
        var states: std.ArrayList([]const u8) = .empty;
        while (cursor.offset < cursor.bytes.len) {
            const tag = try cursor.int(u8);
            const size = try cursor.int(u32);
            if (size < 4) return error.ProtocolViolation;
            var body: protocol.Cursor = .{ .bytes = try cursor.take(size - 4) };
            if (tag == 'D') {
                try std.testing.expectEqual(@as(u16, 1), try body.int(u16));
                const size_ = try body.int(u32);
                try rows.append(a, try std.fmt.parseInt(i64, try body.take(size_), 10));
                try body.finish();
            } else if (tag == 'E') {
                while (true) {
                    const field = try body.int(u8);
                    if (field == 0) break;
                    const text = try body.string();
                    if (field == 'C') try states.append(a, text);
                }
            } else if (tag == 'T') {
                try std.testing.expectEqual(@as(u16, 1), try body.int(u16));
                _ = try body.string();
                _ = try body.int(u32);
                _ = try body.int(u16);
                try std.testing.expectEqual(@as(u32, 20), try body.int(u32));
            }
        }
        try std.testing.expectEqualSlices(i64, &.{ 1, 2, 2, 3, 5 }, rows.items);
        try std.testing.expectEqual(@as(usize, 3), states.items.len);
        for (states.items, [_][]const u8{ "55000", "55000", "26000" }) |actual, expected| try std.testing.expectEqualStrings(expected, actual);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        try call(ctx, &self.first, "SELECT currval('first_seq'),currval('second_seq'),lastval()", null, &.{&.{ .{ .integer = 993 }, .{ .integer = 502 }, .{ .integer = 993 } }});
        _ = try fixture.waitServingSchema(ctx, "docs", 0);
        try call(ctx, null, "SELECT _id,title FROM docs ORDER BY _id", null, &.{&.{ .{ .string = "doc:a" }, .{ .string = "after" } }});
        // Unused pre-crash values (1000,1007,1014) are never reissued.
        try call(ctx, &self.first, "SELECT nextval('first_seq')", null, &.{&.{.{ .integer = 1021 }}});
        try call(ctx, &self.second, "SELECT currval('first_seq')", null, &.{&.{.{ .integer = 937 }}});
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        try call(&reader, null, "SELECT nextval('first_seq')", "42501", &.{});
    }
};
test "mounted SQL sequence effects preserve PostgreSQL sessions cache allocation rollback and cold owners" {
    // sql-1103: exact document TRUNCATE original, asynchronous publication,
    // authorization, future writes and independently reopened native owners.
    try fixture.run(ExecutionCampaign);
}

const IdentityStep = struct {
    sql: []const u8,
    sqlstate: ?[]const u8,
    tag: ?[]const u8,
    output: ?struct { columns: []const Json, rows: []const []const Json },
};
const IdentityEntry = struct { id: []const u8, sql: []const u8, steps: []const IdentityStep };
const IdentityCampaign = struct {
    pub const native_sessions = true;
    pub const native_auth = true;
    pub const cold_catalog = true;
    entry: IdentityEntry,
    credentials: client.ApiKeyHeaders = .{},
    connection: [32]u8 = undefined,
    copy_typed_id: i64 = 2,

    fn publicMutation(ctx: *fixture.Context, path: []const u8, body: []const u8) !@import("../raft/transport/http_common.zig").HttpResponse {
        const deadline = @import("antfly_platform").time.monotonicNs() +| 15 * std.time.ns_per_s;
        while (true) {
            try ctx.checkDrivers();
            const remaining = deadline -| @import("antfly_platform").time.monotonicNs();
            if (remaining == 0) return error.IdentityPublicWriteUnavailable;
            var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, path, .POST, body, @intCast(@max(1, remaining / std.time.ns_per_ms)));
            if (response.status == 503) {
                const Diagnostic = struct { code: []const u8, retryable: bool = false };
                var diagnostic = std.json.parseFromSlice(Diagnostic, ctx.alloc, response.body, .{ .ignore_unknown_fields = true }) catch return response;
                defer diagnostic.deinit();
                if (diagnostic.value.retryable and std.mem.eql(u8, diagnostic.value.code, "transaction_precommit_read_unavailable")) {
                    // No row decision exists. Nontransactional counters may
                    // have advanced; assert uniqueness, not gaplessness.
                    response.deinit(ctx.alloc);
                    try ctx.io.sleep(.fromMilliseconds(20), .awake);
                    continue;
                }
            }
            return response;
        }
    }
    fn publicWrites(self: *@This(), ctx: *fixture.Context) !void {
        try ExecutionCampaign.call(ctx, null, "CREATE TABLE copy_rows(id bigint GENERATED ALWAYS AS IDENTITY,status text)", null, &.{});
        const serving = try fixture.waitServingSchema(ctx, "copy_rows", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "copy_rows");
        const manager = (ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi).cfg.user_manager orelse return error.MissingUserManager;
        var permission = try @import("../usermgr/mod.zig").Permission.initOwned(ctx.alloc, .table, "copy_rows", .write);
        defer permission.deinit(ctx.alloc);
        var namespace_permission = try @import("../usermgr/mod.zig").Permission.initOwned(ctx.alloc, .namespace, "default.public", .read);
        defer namespace_permission.deinit(ctx.alloc);
        try manager.addPermissionToUser("pk-rewriter", permission);
        try manager.addPermissionToUser("pk-rewriter", namespace_permission);
        var key = try manager.createApiKey("pk-rewriter", "identity-table-writer", &.{ permission, namespace_permission }, &.{}, null);
        defer key.deinit(manager.alloc);
        const authorization = try std.fmt.allocPrint(ctx.alloc, "ApiKey {s}", .{key.encoded});
        defer ctx.alloc.free(authorization);
        const headers = [_]@import("../raft/transport/http_common.zig").RequestHeader{.{ .name = "authorization", .value = authorization }};
        var writer = ctx.*;
        writer.headers = &headers;
        var batch = try publicMutation(&writer, "/db/v1/tables/copy_rows/batch", "{\"inserts\":{\"missing\":{\"status\":\"missing\"},\"explicit\":{\"id\":99,\"status\":\"explicit\"}}}");
        defer batch.deinit(ctx.alloc);
        if (batch.status != 201) std.debug.print("identity public batch status={d} body={s}\n", .{ batch.status, batch.body });
        try std.testing.expectEqual(@as(u16, 201), batch.status);
        const request = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .schema_version = serving.version, .mutations = .{.{ .key = "typed", .expected_version = "0", .row = .{ .status = "typed" } }} }, .{});
        defer ctx.alloc.free(request);
        var typed = try publicMutation(&writer, "/db/v1/tables/copy_rows/rows/mutate", request);
        defer typed.deinit(ctx.alloc);
        if (typed.status != 201) std.debug.print("identity typed mutation status={d} body={s}\n", .{ typed.status, typed.body });
        try std.testing.expectEqual(@as(u16, 201), typed.status);
        var invalid = try publicMutation(ctx, "/db/v1/tables/copy_rows/batch", "{\"inserts\":{\"null\":{\"id\":null,\"status\":\"null\"}}}");
        defer invalid.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 400), invalid.status);
        var observed = try client.execute(ctx, "SELECT id FROM copy_rows WHERE status='typed'", .read);
        defer observed.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), observed.status);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, observed.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(usize, 1), parsed.value.rows.len);
        self.copy_typed_id = switch (parsed.value.rows[0][0]) {
            .integer => |number| number,
            .string, .number_string => |text| try std.fmt.parseInt(i64, text, 10),
            else => return error.InvalidIdentityRow,
        };
        try std.testing.expect(self.copy_typed_id >= 2 and self.copy_typed_id < 98);
        try ExecutionCampaign.call(&writer, null, "SELECT nextval('copy_rows_id_seq')", "42501", &.{});
        try ExecutionCampaign.call(&writer, null, "INSERT INTO copy_rows(id,status) OVERRIDING SYSTEM VALUE VALUES(nextval('copy_rows_id_seq'),'forbidden')", "42501", &.{});
        try ExecutionCampaign.call(&writer, null, "INSERT INTO copy_rows(status) VALUES('scoped')", null, &.{});
        try self.verifyCopyRows(ctx);
    }
    fn verifyCopyRows(self: *@This(), ctx: *fixture.Context) !void {
        try ExecutionCampaign.callWithRetry(ctx, null, "SELECT id,status FROM copy_rows ORDER BY id", null, &.{
            &.{ .{ .integer = 1 }, .{ .string = "missing" } },
            &.{ .{ .integer = self.copy_typed_id }, .{ .string = "typed" } },
            &.{ .{ .integer = self.copy_typed_id + 1 }, .{ .string = "scoped" } },
            &.{ .{ .integer = 99 }, .{ .string = "explicit" } },
        }, .read);
    }

    /// Exercise the real native snapshot installer, not just reopening live
    /// files. The restored allocator must retain its exact definition, owner,
    /// high-water mark, generation and nonce-bound acknowledgement behavior.
    fn restoreMetadata(_: *@This(), ctx: *fixture.Context) !void {
        const store = ctx.metadata.server.svc.projectedStore() orelse return error.MissingMetadataStore;
        const group = ctx.metadata.server.svc.metadata_group_id;
        var snapshot: ?[]u8 = null;
        defer if (snapshot) |bytes| ctx.alloc.free(bytes);
        var index: u64 = 0;
        for (0..32) |_| {
            const before = (try store.latestCheckpoint(group)) orelse return error.MetadataCheckpointMissing;
            const bytes = try store.snapshotBuilder().buildSnapshot(ctx.alloc, group);
            const after = (try store.latestCheckpoint(group)) orelse return error.MetadataCheckpointMissing;
            if (before.commit_index == after.commit_index) {
                snapshot = bytes;
                index = before.commit_index;
                break;
            }
            ctx.alloc.free(bytes);
            try ctx.io.sleep(.fromMilliseconds(1), .awake);
        } else return error.MetadataCheckpointBusy;
        var temporary = std.testing.tmpDir(.{});
        defer temporary.cleanup();
        const directory = try temporary.dir.realPathFileAlloc(ctx.io, ".", ctx.alloc);
        defer ctx.alloc.free(directory);
        const root = try std.fmt.allocPrint(ctx.alloc, "{s}/restored", .{directory});
        defer ctx.alloc.free(root);
        const native = @import("../storage/metadata_raft_apply_client.zig");
        var restored = try native.RaftApplyStore.init(ctx.alloc, .{ .root_dir = root });
        defer restored.deinit();
        try std.testing.expect(try restored.snapshotBuilder().installSnapshot(ctx.alloc, group, index, snapshot.?));
        const domain = local.system_catalog_domain;
        const target: domain.Target = .{ .database = "default", .namespace = "public", .table = "usage_records" };
        const table = (try restored.resolveSystemCatalogTable(ctx.alloc, group, target)) orelse return error.RestoredIdentityTableMissing;
        defer @import("../metadata/table_manager.zig").freeTable(ctx.alloc, table);
        const query: sequence.SnapshotRequest = .{
            .targets = &.{.{ .database = "default", .namespace = "public", .table = "usage_records_id_seq" }},
            .include_counters = true,
            .column_default = null,
        };
        const original_bytes = try store.sqlSequenceSnapshot(ctx.alloc, group, query, .{});
        defer ctx.alloc.free(original_bytes);
        var original = try std.json.parseFromSlice(sequence.Snapshot, ctx.alloc, original_bytes, .{});
        defer original.deinit();
        const restored_bytes = try restored.sqlSequenceSnapshot(ctx.alloc, group, query, .{});
        defer ctx.alloc.free(restored_bytes);
        var observed = try std.json.parseFromSlice(sequence.Snapshot, ctx.alloc, restored_bytes, .{});
        defer observed.deinit();
        const actual = observed.value.entries[0];
        try std.testing.expectEqualDeep(original.value.incarnation, observed.value.incarnation);
        try std.testing.expectEqualDeep(original.value.entries[0].value, actual.value);
        try std.testing.expectEqualDeep(original.value.entries[0].counter, actual.counter);
        const definition = try actual.value.sequence.definition();
        try std.testing.expectEqual(table.table_id, definition.owned_by.?.table_id);
        const expected = try actual.counter.?.reserve(definition);
        var nonce: [16]u8 = undefined;
        try ctx.io.randomSecure(&nonce);
        const allocation: sequence.CounterRequest = .{
            .target = actual.target,
            .namespace_id = actual.namespace_id.?,
            .sequence_id = actual.value.sequence.id,
            .incarnation = observed.value.incarnation,
            .request = .{ .request_id = nonce, .expected = actual.counter.?, .operation = .reserve },
        };
        const encoded = try std.json.Stringify.valueAlloc(ctx.alloc, allocation, .{});
        defer ctx.alloc.free(encoded);
        try restored.applyStandaloneCommand(group, .{ .apply_sql_sequence_counter = encoded });
        const receipt = try ((try restored.sqlSequenceCounterOutcome(ctx.alloc, group, allocation)) orelse return error.RestoredIdentityAllocationMissing).result();
        try std.testing.expectEqualDeep(expected.lease, receipt.lease.?);
        try restored.applyStandaloneCommand(group, .{ .apply_sql_sequence_counter = encoded });
        try std.testing.expectEqualDeep(receipt, try ((try restored.sqlSequenceCounterOutcome(ctx.alloc, group, allocation)) orelse return error.RestoredIdentityAllocationMissing).result());
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        ctx.headers = ctx.admin_headers;
        self.connection = try ExecutionCampaign.open(ctx);
        for (self.entry.steps, 0..) |step, index| {
            if (index == 0) {
                var created = try client.executeConnected(ctx, null, &self.connection, step.sql, .write_admission);
                defer created.deinit(ctx.alloc);
                if (created.status != 200 and created.status != 202) std.debug.print("identity CREATE id={s} body={s}\n", .{ self.entry.id, created.body });
                try std.testing.expect(created.status == 200 or created.status == 202);
                const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
                try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
                try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
            } else if (std.mem.startsWith(u8, step.sql, "ALTER ") or std.mem.startsWith(u8, step.sql, "TRUNCATE ")) {
                var coordinator = ctx.*;
                coordinator.base = ctx.metadata_uri;
                var response = try client.execute(&coordinator, step.sql, .write_admission);
                defer response.deinit(ctx.alloc);
                if (response.status != 200 and response.status != 202 and response.status != 409) std.debug.print("identity DDL sql={s} body={s}\n", .{ step.sql, response.body });
                try std.testing.expect(response.status == 200 or response.status == 202 or response.status == 409);
                var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
                defer parsed.deinit();
                if (parsed.value.ddl_receipt) |receipt| {
                    if (receipt.restore_job_id) |job_id| {
                        const deadline = @import("antfly_platform").time.monotonicNs() +| 60 * std.time.ns_per_s;
                        while (true) {
                            try ctx.checkDrivers();
                            var observed = try fixture.job(ctx.alloc, ctx.transport, ctx.headers, ctx.metadata_uri, job_id);
                            defer observed.deinit(ctx.alloc);
                            var job = try std.json.parseFromSlice(wire.RestoreJob, ctx.alloc, observed.body, .{ .ignore_unknown_fields = true });
                            defer job.deinit();
                            if (std.mem.eql(u8, job.value.phase, "succeeded")) break;
                            if (std.mem.eql(u8, job.value.phase, "failed") or std.mem.eql(u8, job.value.phase, "canceled")) {
                                std.debug.print("identity DDL failed sql={s} body={s}\n", .{ step.sql, observed.body });
                                return error.IdentityDdlFailed;
                            }
                            if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.IdentityDdlTimeout;
                            try ctx.io.sleep(.fromMilliseconds(20), .awake);
                        }
                    }
                    _ = try fixture.waitServingSchema(ctx, "usage_records", @intCast(receipt.schema_version));
                }
            } else {
                try ExecutionCampaign.call(ctx, &self.connection, step.sql, step.sqlstate, if (step.output) |out| out.rows else &.{});
            }
        }
        if (std.mem.eql(u8, self.entry.id, "sql-0320")) {
            try ExecutionCampaign.call(ctx, &self.connection, "INSERT INTO usage_records(id,status) OVERRIDING SYSTEM VALUE VALUES(100,'conflict') ON CONFLICT(id) DO UPDATE SET id=101", "428C9", &.{});
            try ExecutionCampaign.call(ctx, &self.connection, "MERGE INTO usage_records t USING (VALUES (100)) s(id) ON t.id=s.id WHEN MATCHED THEN UPDATE SET id=101", "428C9", &.{});
            try ExecutionCampaign.call(ctx, &self.connection, "MERGE INTO usage_records t USING (VALUES (999)) s(id) ON t.id=s.id WHEN NOT MATCHED THEN INSERT(id,status) VALUES(s.id,'explicit')", "428C9", &.{});
        }
        try self.publicWrites(ctx);
        try self.restoreMetadata(ctx);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        const connection = try ExecutionCampaign.open(ctx);
        const initial: i64 = if (std.mem.eql(u8, self.entry.id, "sql-0320")) 100 else 1;
        try ExecutionCampaign.callWithRetry(ctx, &connection, "SELECT id,status FROM usage_records ORDER BY id", null, &.{&.{ .{ .integer = initial }, .{ .string = "restarted" } }}, .read);
        try ExecutionCampaign.call(ctx, &connection, "INSERT INTO usage_records(status) VALUES('cold') RETURNING id,status", null, &.{&.{ .{ .integer = if (initial == 100) 140 else 2 }, .{ .string = "cold" } }});
        try self.verifyCopyRows(ctx);
        // DROP shares the public all-owner drain for active constraints. A
        // ready schema alone does not prove retirement reached its barrier.
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .schema_version = serving.version, .drop = true }, .{});
        defer ctx.alloc.free(body);
        var retirement = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.metadata_uri, "/db/v1/tables/usage_records/constraints/retire", .POST, body);
        defer retirement.deinit(ctx.alloc);
        if (retirement.status != 202) std.debug.print("identity retirement body={s}\n", .{retirement.body});
        try std.testing.expectEqual(@as(u16, 202), retirement.status);
        const deadline = @import("antfly_platform").time.monotonicNs() +| 30 * std.time.ns_per_s;
        while (true) {
            try ctx.checkDrivers();
            var status = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.metadata_uri, "/db/v1/tables/usage_records/constraints/status", .GET, null);
            defer status.deinit(ctx.alloc);
            if (status.status == 200) {
                const Status = struct { retirement: ?struct { phase: []const u8, failure: ?[]const u8 = null } = null };
                var observed = try std.json.parseFromSlice(Status, ctx.alloc, status.body, .{ .ignore_unknown_fields = true });
                defer observed.deinit();
                if (observed.value.retirement) |job| {
                    if (job.failure != null) return error.IdentityRetirementFailed;
                    if (std.mem.eql(u8, job.phase, "ready_to_drop")) break;
                }
            } else if (status.status != 409 and status.status != 503 and status.status != 504) {
                std.debug.print("identity retirement status={d} body={s}\n", .{ status.status, status.body });
                return error.IdentityRetirementUnavailable;
            }
            if (@import("antfly_platform").time.monotonicNs() >= deadline) {
                std.debug.print("identity retirement stalled status={d} body={s}\n", .{ status.status, status.body });
                return error.IdentityRetirementTimeout;
            }
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        var coordinator = ctx.*;
        coordinator.base = ctx.metadata_uri;
        try ExecutionCampaign.call(&coordinator, null, "DROP TABLE usage_records", null, &.{});
        try ExecutionCampaign.call(ctx, &connection, "SELECT nextval('usage_records_id_seq')", "42P01", &.{});
    }
};

test "mounted SQL original serial and identity declarations preserve PostgreSQL writes and cold owners" {
    // sql-0318, sql-0319, sql-0320: exact original declarations, PostgreSQL
    // write postimages, real checkpoint restore, and independently cold owners.
    const a = std.testing.allocator;
    const Reference = struct { format: u8, reference: []const u8, entries: []const IdentityEntry };
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.identity_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    try std.testing.expectEqual(@as(usize, 3), reference.value.entries.len);
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    for (reference.value.entries) |entry| {
        try std.testing.expectEqualStrings((try corpus.get(entry.id)).sql, entry.sql);
        try fixture.runWithState(IdentityCampaign, .{ .entry = entry });
    }
}
