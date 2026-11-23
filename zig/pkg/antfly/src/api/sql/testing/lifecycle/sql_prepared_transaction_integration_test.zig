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

//! Prepared GIDs through the mounted SQL API, data Raft, native intents and a
//! complete metadata/session/data-owner reopen. No SQL mutation is replayed.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("../owner_test_fixture.zig");
const client = @import("../owner_test_client.zig");
const transport = @import("../../../../raft/transport/http_common.zig");
const http = @import("../../../http_server.zig");
const registry = @import("../../../../system_catalog/prepared_transactions.zig");
const wire = @import("antfly_metadata_openapi").types;

fn execute(ctx: *fixture.Context, connection: ?[]const u8, statement: []const u8) !transport.HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    return client.executeConnected(&authenticated, null, connection, statement, .never);
}
fn command(ctx: *fixture.Context, connection: ?[]const u8, statement: []const u8, expected: []const u8) !void {
    var response = try execute(ctx, connection, statement);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("prepared SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 202);
    var parsed = try std.json.parseFromSlice(struct { command_tag: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings(expected, parsed.value.command_tag);
}
fn rows(ctx: *fixture.Context, statement: []const u8, expected: std.json.Value) !void {
    return connectedRows(ctx, null, statement, expected);
}
fn connectedRows(ctx: *fixture.Context, connection: ?[]const u8, statement: []const u8, expected: std.json.Value) !void {
    return connectedRowsWithColumns(ctx, connection, statement, expected, &.{.{ .name = "amount", .oid = 20 }});
}

const ExpectedColumn = struct { name: []const u8, oid: u32 };

fn connectedRowsWithColumns(ctx: *fixture.Context, connection: ?[]const u8, statement: []const u8, expected: std.json.Value, columns: []const ExpectedColumn) !void {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    // A pure observer may repeat only a proven pre-admission read failure.
    // Never retry PREPARE or either decision command after an uncertain reply.
    var response = try client.executeConnected(&authenticated, null, connection, statement, if (connection == null) .read else .never);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("prepared observation SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    const compare = @import("../parity_reference.zig");
    try std.testing.expectEqual(columns.len, parsed.value.columns.len);
    for (columns, parsed.value.columns) |want, actual| {
        try std.testing.expectEqualStrings(want.name, actual.name);
        try std.testing.expect(compare.postgresColumnMatches(actual, want.oid));
    }
    const expected_nulls = try ctx.alloc.alloc(bool, columns.len);
    defer ctx.alloc.free(expected_nulls);
    @memset(expected_nulls, false);
    try std.testing.expect(expected == .array);
    try std.testing.expectEqual(expected.array.items.len, parsed.value.rows.len);
    const nulls = parsed.value.sql_nulls orelse return error.MissingSqlNullBitmap;
    for (parsed.value.rows, nulls, expected.array.items) |actual, flags, want| {
        try std.testing.expect(want == .array);
        try std.testing.expect(try compare.rowMatchesWithNulls(ctx.alloc, parsed.value.columns, actual, flags, want.array.items, expected_nulls));
    }
}
fn diagnostic(ctx: *fixture.Context, connection: ?[]const u8, statement: []const u8, code: []const u8, reader: bool) !void {
    var authenticated = ctx.*;
    authenticated.headers = if (reader) ctx.reader_headers else ctx.admin_headers;
    var response = try client.executeConnected(&authenticated, null, connection, statement, .never);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("prepared diagnostic SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expect(response.status >= 400);
    var parsed = try std.json.parseFromSlice(struct { code: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqualStrings(code, parsed.value.code);
}
fn openConnection(ctx: *fixture.Context) ![32]u8 {
    var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.connection_id.len != 32) return error.InvalidSqlConnection;
    return parsed.value.connection_id[0..32].*;
}
fn prepared(ctx: *fixture.Context, gid: []const u8, expected: bool) !void {
    var region = std.heap.ArenaAllocator.init(ctx.alloc);
    defer region.deinit();
    const a = region.allocator();
    const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const bytes = try source.systemCatalog(a, .{ .setting_admin = true }, .{ .sql_prepared_read = .{ .gid = gid } });
    const cut = try std.json.parseFromSliceLeaky(registry.Snapshot, a, bytes, .{});
    if (!expected) {
        try std.testing.expectEqual(@as(usize, 0), cut.records.len);
        return;
    }
    try std.testing.expectEqual(@as(usize, 1), cut.records.len);
    const record = cut.records[0];
    try std.testing.expect(record.phase == .prepared and record.was_prepared);
    try std.testing.expect(record.participants.len >= 2);
    try record.validate(a);
    const work = try source.systemCatalog(a, .{ .setting_admin = true }, .{ .sql_prepared_read = .{ .work = null } });
    const page = try std.json.parseFromSliceLeaky(registry.Snapshot, a, work, .{});
    for (page.records) |job| try std.testing.expect(!std.mem.eql(u8, &job.identity.operation_id, &record.identity.operation_id));
}

const Reference = struct {
    format: u32,
    postgres_major: u32,
    cases: []const struct { id: []const u8, sql: []const u8, command_tag: []const u8 },
    preimage: std.json.Value,
    committed: std.json.Value,
    rolled_back: std.json.Value,
};

const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    reference: Reference,

    pub fn configureAuth(manager: *@import("../../../../usermgr/mod.zig").UserManager) !void {
        const users = @import("../../../../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }

    fn original(self: *@This(), ctx: *fixture.Context, connection_id: ?[]const u8, id: []const u8) !void {
        for (self.reference.cases) |entry| if (std.mem.eql(u8, entry.id, id)) return command(ctx, connection_id, entry.sql, entry.command_tag);
        return error.TestUnexpectedResult;
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        var authenticated = ctx.*;
        authenticated.headers = ctx.admin_headers;
        inline for (.{ "prepared_a", "prepared_b" }) |name| {
            const create = "CREATE TABLE " ++ name ++ " (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL)";
            var created = try client.execute(&authenticated, create, .write_admission);
            defer created.deinit(ctx.alloc);
            errdefer std.debug.print("prepared setup SQL={s} status={d} body={s}\n", .{ create, created.status, created.body });
            try std.testing.expect(created.status == 200 or created.status == 202);
            const serving = try fixture.waitServingSchema(ctx, name, 0);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, name);
            try command(ctx, null, "INSERT INTO " ++ name ++ " VALUES(1,10)", "INSERT");
        }
        const first = try openConnection(ctx);
        try command(ctx, &first, "BEGIN", "BEGIN");
        try command(ctx, &first, "UPDATE prepared_a SET amount=11 WHERE id=1", "UPDATE");
        try command(ctx, &first, "UPDATE prepared_b SET amount=11 WHERE id=1", "UPDATE");
        // Execute the unchanged original inventory statement.
        try self.original(ctx, &first, "sql-0771");
        try prepared(ctx, "usage_batch", true);
        // GID ownership is independent of the originating connection. Neither
        // another principal nor an explicit transaction may decide it.
        try diagnostic(ctx, null, "ROLLBACK PREPARED 'usage_batch'", "42501", true);
        const contender = try openConnection(ctx);
        try command(ctx, &contender, "BEGIN", "BEGIN");
        try diagnostic(ctx, &contender, "COMMIT PREPARED 'usage_batch'", "25001", false);
        try command(ctx, &contender, "ROLLBACK", "ROLLBACK");
        try command(ctx, &contender, "BEGIN", "BEGIN");
        try diagnostic(ctx, &contender, "PREPARE TRANSACTION 'usage_batch'", "42710", false);
        try prepared(ctx, "usage_batch", true);
        try rows(ctx, "SELECT amount FROM prepared_a", self.reference.preimage);
        try rows(ctx, "SELECT amount FROM prepared_b", self.reference.preimage);
        // The detached original connection can start another transaction;
        // the durable GID independently owns the first transaction's intents.
        try command(ctx, &first, "BEGIN", "BEGIN");
        try command(ctx, &first, "ROLLBACK", "ROLLBACK");
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        try prepared(ctx, "usage_batch", true);
        try rows(ctx, "SELECT amount FROM prepared_a", self.reference.preimage);
        try rows(ctx, "SELECT amount FROM prepared_b", self.reference.preimage);
        try self.original(ctx, null, "sql-0772");
        try prepared(ctx, "usage_batch", false);
        try rows(ctx, "SELECT amount FROM prepared_a", self.reference.committed);
        try rows(ctx, "SELECT amount FROM prepared_b", self.reference.committed);
        const second = try openConnection(ctx);
        try command(ctx, &second, "BEGIN", "BEGIN");
        try command(ctx, &second, "UPDATE prepared_a SET amount=12 WHERE id=1", "UPDATE");
        try command(ctx, &second, "UPDATE prepared_b SET amount=12 WHERE id=1", "UPDATE");
        try self.original(ctx, &second, "sql-0771");
        try self.original(ctx, null, "sql-0773");
        try prepared(ctx, "usage_batch", false);
        try rows(ctx, "SELECT amount FROM prepared_a", self.reference.rolled_back);
        try rows(ctx, "SELECT amount FROM prepared_b", self.reference.rolled_back);
        try diagnostic(ctx, null, "COMMIT PREPARED 'usage_batch'", "42704", false);
        // Empty string is a valid PostgreSQL GID. An empty transaction still
        // has a real durable metadata decision and releases its reservation.
        const empty = try openConnection(ctx);
        try command(ctx, &empty, "BEGIN", "BEGIN");
        try command(ctx, &empty, "PREPARE TRANSACTION ''", "PREPARE TRANSACTION");
        try command(ctx, null, "COMMIT PREPARED ''", "COMMIT PREPARED");
        try prepared(ctx, "", false);
        try command(ctx, null, "PREPARE TRANSACTION 'outside'", "ROLLBACK");
    }
};

test "mounted SQL original prepared transactions retain multi-owner intents across complete cold restart" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.prepared_transaction_reference, .{});
    defer reference.deinit();
    try std.testing.expectEqual(@as(u32, 1), reference.value.format);
    try std.testing.expect(reference.value.postgres_major >= 18);
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    try std.testing.expectEqual(@as(usize, 3), reference.value.cases.len);
    for ([_][]const u8{ "sql-0771", "sql-0772", "sql-0773" }, reference.value.cases) |id, entry| {
        try std.testing.expectEqualStrings(id, entry.id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, entry.sql);
    }
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}

fn wireFreeze(ctx: *fixture.Context) !void {
    const frames = @import("../../../../pgwire/cursor_reference_test_support.zig");
    const protocol = @import("../../../../pgwire/protocol.zig");
    const a = ctx.alloc;
    var input: std.Io.Writer.Allocating = .init(a);
    defer input.deinit();
    try frames.startup(a, &input.writer, "pk-rewriter", "pk-rewrite-durable-password", "default");
    // An existing table and a retained cursor both fail before CopyInResponse.
    for ([_][]const u8{
        "COPY private_ddl_seed FROM STDIN WITH (FREEZE)\x00",
        "BEGIN\x00",
        "CREATE TABLE private_stdin (amount BIGINT)\x00",
        "SAVEPOINT held\x00",
        "DECLARE freeze_cursor CURSOR FOR SELECT 1\x00",
        "COPY private_stdin FROM STDIN WITH (FREEZE)\x00",
        "ROLLBACK TO SAVEPOINT held\x00",
        "RELEASE SAVEPOINT held\x00",
        "COPY private_stdin FROM STDIN WITH (FREEZE)\x00",
    }) |sql| try frames.frame(&input.writer, 'Q', sql);
    try frames.frame(&input.writer, 'd', "41\n");
    try frames.frame(&input.writer, 'c', "");
    try frames.frame(&input.writer, 'Q', "COMMIT\x00");
    for ([_][]const u8{
        "BEGIN\x00",
        "TRUNCATE private_stdin\x00",
        "SAVEPOINT child_truncate\x00",
        "TRUNCATE private_stdin\x00",
        "ROLLBACK TO SAVEPOINT child_truncate\x00",
        "RELEASE SAVEPOINT child_truncate\x00",
        "COPY private_stdin FROM STDIN WITH (FREEZE)\x00",
        "ROLLBACK\x00",
        "BEGIN\x00",
        "TRUNCATE private_stdin\x00",
        "COPY private_stdin FROM STDIN WITH (FREEZE)\x00",
    }) |sql| try frames.frame(&input.writer, 'Q', sql);
    try frames.frame(&input.writer, 'd', "52\n55\n");
    try frames.frame(&input.writer, 'c', "");
    try frames.frame(&input.writer, 'Q', "UPDATE private_stdin SET amount = 53 WHERE amount = 52\x00");
    try frames.frame(&input.writer, 'Q', "DELETE FROM private_stdin WHERE amount = 55\x00");
    try frames.frame(&input.writer, 'Q', "COMMIT\x00");
    try frames.frame(&input.writer, 'Q', "BEGIN\x00");
    try frames.frame(&input.writer, 'Q', "CREATE TABLE usage_records (id BIGINT, status TEXT DEFAULT 'ready')\x00");
    try frames.frame(&input.writer, 'Q', "COPY usage_records (id, status) FROM STDIN WITH (FORMAT csv, HEADER true, FREEZE true, ON_ERROR ignore, REJECT_LIMIT 10, LOG_VERBOSITY verbose, FORCE_NOT_NULL (id, status), FORCE_NULL (status), DELIMITER ',', QUOTE '\"', ESCAPE '!', NULL '', DEFAULT 'n/a', ENCODING 'UTF8')\x00");
    try frames.frame(&input.writer, 'd', "id,status\n1,n/a\nbad,ignored\n2,\n");
    try frames.frame(&input.writer, 'c', "");
    try frames.frame(&input.writer, 'Q', "COMMIT\x00");
    try frames.frame(&input.writer, 'X', "");
    var source = std.Io.Reader.fixed(input.written());
    var output: std.Io.Writer.Allocating = .init(a);
    defer output.deinit();
    var adapter: @import("../../transport/pgwire.zig").Adapter = .{ .server = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi };
    var session: protocol.Session = .{ .alloc = a, .io = ctx.io, .source = adapter.backend(), .reader = &source, .writer = &output.writer };
    defer session.deinit();
    try session.run();
    var cursor: protocol.Cursor = .{ .bytes = output.written() };
    const expected = [_][]const u8{ "55000", "25000", "55000" };
    var errors: usize = 0;
    var accepted: usize = 0;
    var completed: usize = 0;
    var committed: usize = 0;
    while (cursor.offset != cursor.bytes.len) {
        const tag = try cursor.int(u8);
        const length = try cursor.int(u32);
        var body: protocol.Cursor = .{ .bytes = try cursor.take(length - 4) };
        switch (tag) {
            'G' => {
                try std.testing.expectEqual(@min(accepted + 2, expected.len), errors);
                accepted += 1;
            },
            'C' => {
                const command_tag = try body.string();
                if (std.mem.eql(u8, command_tag, "COPY 1") or std.mem.eql(u8, command_tag, "COPY 2")) completed += 1;
                if (std.mem.eql(u8, command_tag, "COMMIT")) committed += 1;
            },
            'E' => {
                var code: []const u8 = "";
                var message: []const u8 = "";
                while (try body.int(u8) != 0) {
                    const field = body.bytes[body.offset - 1];
                    const value = try body.string();
                    if (field == 'C') code = value;
                    if (field == 'M') message = value;
                }
                errdefer std.debug.print("pgwire COPY FREEZE diagnostic {s} after {} accepted, {} committed: {s}\n", .{ code, accepted, committed, message });
                try std.testing.expect(errors < expected.len);
                try std.testing.expectEqualStrings(expected[errors], code);
                errors += 1;
            },
            else => {},
        }
    }
    try std.testing.expectEqual(expected.len, errors);
    try std.testing.expectEqual(@as(usize, 3), accepted);
    try std.testing.expectEqual(@as(usize, 3), completed);
    try std.testing.expectEqual(@as(usize, 3), committed);
}

fn resolvePublishedTable(ctx: *fixture.Context, name: []const u8, deadline: u64) !std.json.Parsed(local.system_catalog_domain.ResolvedMany) {
    const domain = local.system_catalog_domain;
    const a = ctx.alloc;
    const lookup: domain.ResolveMany = .{ .targets = &.{.{ .database = "default", .namespace = "public", .table = name }} };
    const source = @import("../../../http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const raw = try source.systemCatalog(a, .{ .deadline_ns = deadline }, .{ .resolve_many = lookup });
    defer a.free(raw);
    var resolved = try std.json.parseFromSlice(domain.ResolvedMany, a, raw, .{ .allocate = .alloc_always });
    errdefer resolved.deinit();
    try resolved.value.validateAuthority(a, lookup);
    return resolved;
}

fn checkPublishedFrozenBirths(ctx: *fixture.Context, name: []const u8, predecessor: ?u64) !void {
    const catalog = @import("../../../table_catalog.zig");
    const stages = @import("../../../../metadata/restore_staging.zig");
    const server = if (ctx.data.http_server) |*value| value else return error.MissingOwnerReadSource;
    const owner = ctx.data.kernel_owner_source orelse return error.MissingOwnerReadSource;
    const svc = ctx.metadata.server.svc;
    const a = ctx.alloc;
    const deadline = @import("antfly_platform").time.monotonicNs() +| 15 * std.time.ns_per_s;
    var resolved = try resolvePublishedTable(ctx, name, deadline);
    defer resolved.deinit();
    const published = resolved.value.tables[0] orelse return error.TableNotFound;
    var routing = (try catalog.authoritativeTableRoutingSnapshot(a, server.catalogSource(), published.name, deadline)) orelse return error.TableNotFound;
    defer routing.deinit(a);
    try std.testing.expectEqual(published.table_id, routing.table_id);
    try std.testing.expectEqual(@as(usize, 1), routing.ranges.len);
    const route = routing.resolveRouteForKey("") orelse return error.TopologyChanged;
    const source = @import("../../../http_server.zig").StatusSource.fromMetadataHttpService(svc);
    const lookup: @import("../../../../system_catalog/server_call.zig").Call = .{ .sql_population_lineage = .{ .table_id = published.table_id, .owner_group_id = route.group_id } };
    try std.testing.expectError(error.Forbidden, source.systemCatalog(a, .{ .deadline_ns = deadline }, lookup));
    const raw = try source.systemCatalog(a, .{ .deadline_ns = deadline, .setting_admin = true }, lookup);
    defer a.free(raw);
    var parsed = try std.json.parseFromSlice(?stages.SqlPopulationLineage, a, raw, .{});
    defer parsed.deinit();
    const lineage = parsed.value orelse return error.InvalidRestoreStaging;
    try std.testing.expectEqual(published.table_id, lineage.table_id);
    const prior = lineage.predecessor_table_id orelse return error.InvalidRestoreStaging;
    try std.testing.expect(prior != published.table_id);
    if (predecessor) |expected| {
        try std.testing.expectEqual(expected, prior);
        const mismatched = try source.systemCatalog(a, .{ .deadline_ns = deadline, .setting_admin = true }, .{ .sql_population_lineage = .{ .table_id = expected, .owner_group_id = route.group_id } });
        defer a.free(mismatched);
        try std.testing.expectEqualStrings("null", mismatched);
    }
    const binding = lineage.frozen orelse return error.InvalidRestoreStaging;
    const opts: local.storage_db_types.ScanOptions = .{
        .relational_query = .{ .fields = &.{"amount"}, .schema_version = 0 },
        .execution_deadline_ns = deadline,
    };
    var fence = routing.fenceForRoute(route);
    fence.admission_deadline_ns = deadline;
    var changed = binding;
    changed.content[0] ^= 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, owner.openFrozenRelationalRead(a, fence, published.name, changed, "", "", opts));
    const Http = @import("../../../../raft/transport/http_common.zig");
    const Signed = struct {
        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, request: Http.HttpRequest) !Http.HttpResponse {
            const mounted: *fixture.Context = @ptrCast(@alignCast(ptr));
            var internal = mounted.internalDataClient();
            internal.alloc = alloc;
            return internal.executeRequest(request);
        }
    };
    const executor: Http.RequestExecutor = .{ .ptr = ctx, .clock_io = ctx.transport.clock_io, .vtable = &.{ .execute = Signed.execute } };
    const rpc = @import("../../../retained_read_client.zig").Client;
    const frozen = if (predecessor != null)
        try rpc.openFrozen(a, executor, ctx.base, published.name, fence, 0, binding, "", "", opts)
    else
        try (server.table_reads orelse return error.MissingOwnerReadSource).openFrozenRelationalSpan(a, .{ .table = published.name, .table_id = published.table_id, .opts = opts }, &.{binding});
    defer frozen.deinit();
    const expected: []const i64 = if (predecessor == null) &.{ 52, 55 } else &.{ 52, 55, 58, 59 };
    var value_storage: [4]i64 = undefined;
    const values = value_storage[0..expected.len];
    var point: ?[]u8 = null;
    var surviving: ?[]u8 = null;
    var superseded: ?[]u8 = null;
    defer if (surviving) |key| a.free(key);
    defer if (superseded) |key| a.free(key);
    defer if (point) |owned| a.free(owned);
    for (values) |*value| {
        var page = try frozen.next(a, 1);
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        const cell = try page.rows[0].cell("amount");
        if (cell.value != .integer) return error.TestUnexpectedResult;
        value.* = cell.value.integer;
        const key = local.storage_db_relational_population_contract.frozen_key;
        const birth = try key.decode(page.rows[0].id);
        try std.testing.expectEqual(if (value.* == 59) key.Lineage.surviving else key.Lineage.superseded, birth.lineage);
        try std.testing.expectEqual(lineage.write_version, page.rows[0].version);
        if (value.* == 52) point = try birth.logicalAlloc(a);
        if (value.* == 59) surviving = try birth.logicalAlloc(a);
        if (value.* == 52) superseded = try birth.logicalAlloc(a);
    }
    var end = try frozen.next(a, 1);
    defer end.deinit();
    try std.testing.expectEqual(@as(usize, 0), end.rows.len);
    var point_opts = opts;
    point_opts.inclusive_from = true;
    point_opts.exclusive_to = false;
    const exact = try rpc.openFrozen(a, executor, ctx.base, published.name, fence, 0, binding, point.?, point.?, point_opts);
    defer exact.deinit();
    var matched = try exact.next(a, 2);
    defer matched.deinit();
    try std.testing.expectEqual(@as(usize, 1), matched.rows.len);
    try std.testing.expectEqual(@as(i64, 52), (try matched.rows[0].cell("amount")).value.integer);
    point_opts.include_content_hashes = true;
    if (surviving) |key| {
        const witness = try owner.openFrozenRelationalRead(a, fence, published.name, binding, key, key, point_opts);
        defer witness.deinit();
        var verified = try witness.next(a, 1);
        defer verified.deinit();
        try std.testing.expectEqual(@as(usize, 1), verified.rows.len);
        const birth = try local.storage_db_relational_population_contract.frozen_key.decode(verified.rows[0].id);
        try std.testing.expectEqualStrings(key, try birth.logicalAlloc(verified.arena.allocator()));
        try std.testing.expect(verified.rows[0].expected_content_digest != null);
    }
    if (superseded) |key| {
        const stale = try owner.openFrozenRelationalRead(a, fence, published.name, binding, key, key, point_opts);
        defer stale.deinit();
        var observed = try stale.next(a, 1);
        defer observed.deinit();
        try std.testing.expectEqual(@as(usize, 1), observed.rows.len);
        try std.testing.expect(observed.rows[0].stale_frozen_birth and observed.rows[0].expected_content_digest == null);
    }
    std.mem.sort(i64, values, {}, std.sort.asc(i64));
    try std.testing.expectEqualSlices(i64, expected, values);
}

fn checkOriginalFreezeRows(ctx: *fixture.Context) !void {
    _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
    var expected = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[1,\"ready\"],[2,\"\"]]", .{});
    defer expected.deinit();
    try connectedRowsWithColumns(ctx, null, "SELECT id, status FROM usage_records ORDER BY id", expected.value, &.{
        .{ .name = "id", .oid = 20 },
        .{ .name = "status", .oid = 25 },
    });
}

test "mounted SQL original prepared transactions isolate an unpublished table declaration" {
    const PrivateCreate = struct {
        pub const native_auth = true;
        pub const native_sessions = true;
        pub const cold_catalog = true;
        credentials: client.ApiKeyHeaders = .{},

        pub fn configureAuth(manager: *@import("../../../../usermgr/mod.zig").UserManager) !void {
            try Campaign.configureAuth(manager);
            var global = try @import("../../../../usermgr/mod.zig").Permission.initOwned(manager.alloc, .@"*", "*", .admin);
            defer global.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", global);
        }

        pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
            try self.credentials.activateWithCatalog(ctx, true);
            var ingress = ctx.*;
            ingress.base = ctx.metadata_uri;
            const manager = ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?;
            var global = try @import("../../../../usermgr/mod.zig").Permission.initOwned(ctx.alloc, .@"*", "*", .admin);
            defer global.deinit(ctx.alloc);
            var key = try manager.createApiKey("pk-rewriter", "private-copy-freeze", &.{global}, &.{}, null);
            defer key.deinit(manager.alloc);
            const authorization = try std.fmt.allocPrint(ctx.alloc, "ApiKey {s}", .{key.encoded});
            defer ctx.alloc.free(authorization);
            const headers = [_]@import("../../../../raft/transport/http_common.zig").RequestHeader{.{ .name = "authorization", .value = authorization }};
            ingress.headers = &headers;
            ingress.admin_headers = &headers;
            var seeded = try client.execute(&ingress, "CREATE TABLE private_ddl_seed (id BIGINT PRIMARY KEY, amount BIGINT)", .write_admission);
            defer seeded.deinit(ctx.alloc);
            try std.testing.expect(seeded.status == 200 or seeded.status == 202);
            _ = try fixture.waitServingSchema(ctx, "private_ddl_seed", 0);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "private_ddl_seed");
            const connection = try openConnection(&ingress);
            const deadline = @import("antfly_platform").time.monotonicNs() +| 15 * std.time.ns_per_s;
            var predecessor = try resolvePublishedTable(ctx, "private_ddl_seed", deadline);
            defer predecessor.deinit();
            const before = predecessor.value.tables[0] orelse return error.TableNotFound;
            const older = try openConnection(&ingress);
            try command(&ingress, &older, "BEGIN ISOLATION LEVEL SERIALIZABLE", "BEGIN");
            var scalar_only = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[1]]", .{});
            defer scalar_only.deinit();
            try connectedRows(&ingress, &older, "SELECT 1::BIGINT AS amount", scalar_only.value);
            const read_only = try openConnection(&ingress);
            try command(&ingress, &read_only, "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY", "BEGIN");
            try connectedRows(&ingress, &read_only, "SELECT 1::BIGINT AS amount", scalar_only.value);
            try command(&ingress, &connection, "BEGIN", "BEGIN");
            try command(&ingress, &connection, "TRUNCATE private_ddl_seed", "TRUNCATE TABLE");
            try command(&ingress, &connection, "COPY private_ddl_seed FROM PROGRAM 'printf \"1,52\\n2,55\\n3,58\\n\"' WITH (FORMAT CSV, FREEZE)", "COPY");
            try command(&ingress, &connection, "UPDATE private_ddl_seed SET amount = 53 WHERE amount = 52", "UPDATE");
            try command(&ingress, &connection, "DELETE FROM private_ddl_seed WHERE amount = 55", "DELETE");
            try command(&ingress, &connection, "DELETE FROM private_ddl_seed WHERE id = 3", "DELETE");
            try command(&ingress, &connection, "COPY private_ddl_seed FROM PROGRAM 'printf \"3,59\\n\"' WITH (FORMAT CSV, FREEZE)", "COPY");
            try command(&ingress, &connection, "INSERT INTO private_ddl_seed VALUES (4, 57)", "INSERT");
            try command(&ingress, &connection, "COMMIT", "COMMIT");
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "private_ddl_seed");
            var live_rows = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[53],[57],[59]]", .{});
            defer live_rows.deinit();
            try rows(&ingress, "SELECT amount FROM private_ddl_seed ORDER BY amount", live_rows.value);
            var frozen_rows = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[52],[55],[58],[59]]", .{});
            defer frozen_rows.deinit();
            try connectedRows(&ingress, &older, "SELECT amount FROM private_ddl_seed ORDER BY amount", frozen_rows.value);
            var duplicate_births = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[58],[59]]", .{});
            defer duplicate_births.deinit();
            try connectedRows(&ingress, &older, "SELECT amount FROM private_ddl_seed WHERE id = 3 ORDER BY amount", duplicate_births.value);
            try connectedRows(&ingress, &read_only, "SELECT amount FROM private_ddl_seed ORDER BY amount", frozen_rows.value);
            try command(&ingress, &read_only, "COMMIT", "COMMIT");
            try command(&ingress, &older, "SAVEPOINT surviving_birth", "SAVEPOINT");
            var updated = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[99]]", .{});
            defer updated.deinit();
            try connectedRows(&ingress, &older, "UPDATE private_ddl_seed SET amount = 99 WHERE amount + 1 = 60 RETURNING amount", updated.value);
            var overlay_rows = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[52],[55],[58],[99]]", .{});
            defer overlay_rows.deinit();
            try connectedRows(&ingress, &older, "SELECT amount FROM private_ddl_seed ORDER BY amount", overlay_rows.value);
            var duplicate_overlay = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[58],[99]]", .{});
            defer duplicate_overlay.deinit();
            try connectedRows(&ingress, &older, "SELECT amount FROM private_ddl_seed WHERE id = 3 ORDER BY amount", duplicate_overlay.value);
            try command(&ingress, &older, "ROLLBACK TO SAVEPOINT surviving_birth", "ROLLBACK");
            try connectedRows(&ingress, &older, "SELECT amount FROM private_ddl_seed ORDER BY amount", frozen_rows.value);
            try diagnostic(&ingress, &older, "UPDATE private_ddl_seed SET amount = 99 WHERE amount = 52", "40001", false);
            try command(&ingress, &older, "ROLLBACK TO SAVEPOINT surviving_birth", "ROLLBACK");
            try command(&ingress, &older, "RELEASE SAVEPOINT surviving_birth", "RELEASE");
            try checkPublishedFrozenBirths(ctx, "private_ddl_seed", before.table_id);
            try connectedRows(&ingress, &older, "UPDATE private_ddl_seed SET amount = 99 WHERE amount = 59 RETURNING amount", updated.value);
            try command(&ingress, &older, "COMMIT", "COMMIT");
            var committed_rows = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[53],[57],[99]]", .{});
            defer committed_rows.deinit();
            try rows(&ingress, "SELECT amount FROM private_ddl_seed ORDER BY amount", committed_rows.value);
            try command(&ingress, &older, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", "BEGIN");
            try connectedRows(&ingress, &older, "SELECT 1::BIGINT AS amount", scalar_only.value);
            try command(&ingress, &connection, "BEGIN", "BEGIN");
            try command(&ingress, &connection, "TRUNCATE private_ddl_seed", "TRUNCATE TABLE");
            try command(&ingress, &connection, "COPY private_ddl_seed FROM PROGRAM 'printf \"4,71\\n\"' WITH (FORMAT CSV)", "COPY");
            try command(&ingress, &connection, "COMMIT", "COMMIT");
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "private_ddl_seed");
            var copied = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[71]]", .{});
            defer copied.deinit();
            try rows(&ingress, "SELECT amount FROM private_ddl_seed", copied.value);
            try connectedRows(&ingress, &older, "SELECT amount FROM private_ddl_seed", .{ .array = std.json.Array.init(ctx.alloc) });
            try command(&ingress, &older, "ROLLBACK", "ROLLBACK");

            try wireFreeze(ctx);
            _ = try fixture.waitServingSchema(ctx, "private_stdin", 0);
            var stdin_expected = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[53]]", .{});
            defer stdin_expected.deinit();
            try rows(&ingress, "SELECT amount FROM private_stdin", stdin_expected.value);
            try checkPublishedFrozenBirths(ctx, "private_stdin", null);
            try checkOriginalFreezeRows(&ingress);
            try command(&ingress, &connection, "BEGIN", "BEGIN");
            try command(&ingress, &connection, "CREATE TABLE private_unpublished (amount BIGINT)", "CREATE TABLE");
            try connectedRows(&ingress, &connection, "SELECT amount FROM private_unpublished", .{ .array = std.json.Array.init(ctx.alloc) });
            try connectedRows(&ingress, &connection, "SELECT p.amount FROM private_unpublished p JOIN private_ddl_seed s ON p.amount = s.amount", .{ .array = std.json.Array.init(ctx.alloc) });
            try diagnostic(&ingress, null, "SELECT amount FROM private_unpublished", "42P01", false);
            try command(&ingress, &connection, "ROLLBACK", "ROLLBACK");
            try diagnostic(&ingress, null, "SELECT amount FROM private_unpublished", "42P01", false);
            try command(&ingress, &connection, "BEGIN", "BEGIN");
            try command(&ingress, &connection, "CREATE TABLE private_published (amount BIGINT DEFAULT 17, CONSTRAINT private_amount_key UNIQUE (amount))", "CREATE TABLE");
            try command(&ingress, &connection, "INSERT INTO private_published (amount) VALUES (DEFAULT)", "INSERT");
            try command(&ingress, &connection, "SAVEPOINT private_freeze", "SAVEPOINT");
            try diagnostic(&ingress, &connection, "COPY private_published FROM PROGRAM 'exit 99' WITH (FREEZE)", "55000", false);
            try command(&ingress, &connection, "ROLLBACK TO SAVEPOINT private_freeze", "ROLLBACK");
            try command(&ingress, &connection, "RELEASE SAVEPOINT private_freeze", "RELEASE");
            try command(&ingress, &connection, "COPY private_published FROM PROGRAM 'printf \"29\\n\"' WITH (FREEZE)", "COPY");
            try command(&ingress, &connection, "SAVEPOINT private_duplicate", "SAVEPOINT");
            try diagnostic(&ingress, &connection, "INSERT INTO private_published (amount) VALUES (17)", "23505", false);
            try command(&ingress, &connection, "ROLLBACK TO SAVEPOINT private_duplicate", "ROLLBACK");
            var expected = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[17],[29]]", .{});
            defer expected.deinit();
            try connectedRows(&ingress, &connection, "SELECT amount FROM private_published ORDER BY amount", expected.value);
            try diagnostic(&ingress, null, "SELECT amount FROM private_published ORDER BY amount", "42P01", false);
            try command(&ingress, &connection, "COMMIT", "COMMIT");
            _ = try fixture.waitServingSchema(ctx, "private_published", 0);
            try rows(&ingress, "SELECT amount FROM private_published ORDER BY amount", expected.value);
        }

        pub fn afterRestart(_: *@This(), ctx: *fixture.Context) !void {
            var ingress = ctx.*;
            ingress.base = ctx.metadata_uri;
            _ = try fixture.waitServingSchema(ctx, "private_stdin", 0);
            var stdin_expected = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[53]]", .{});
            defer stdin_expected.deinit();
            try rows(&ingress, "SELECT amount FROM private_stdin", stdin_expected.value);
            try checkPublishedFrozenBirths(ctx, "private_stdin", null);
            try checkOriginalFreezeRows(&ingress);
            _ = try fixture.waitServingSchema(ctx, "private_published", 0);
            var expected = try std.json.parseFromSlice(std.json.Value, ctx.alloc, "[[17],[29]]", .{});
            defer expected.deinit();
            try rows(&ingress, "SELECT amount FROM private_published ORDER BY amount", expected.value);
            try diagnostic(&ingress, null, "SELECT amount FROM private_unpublished", "42P01", false);
        }
    };
    try fixture.run(PrivateCreate);
}
