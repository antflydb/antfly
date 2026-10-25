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
        var response = try client.executeConnected(ctx, null, connection, sql, if (ddl) .write_admission else .never);
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
