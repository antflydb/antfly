// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Qualification against the mounted catalog and remote owner, not mock RLS
//! evaluators. Uses existing policy publication and pgwire engine contracts.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const http = @import("http_server.zig");
const platform = @import("antfly_platform");
const wire = @import("../pgwire/backend.zig");
const A = std.mem.Allocator;

fn command(ctx: *fixture.Context, sql: []const u8) !void {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    authenticated.base = ctx.metadata_uri;
    var response = try client.execute(&authenticated, sql, .read);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) {
        std.debug.print("view security command status={d} sql={s} body={s}\n", .{ response.status, sql, response.body });
        return error.SqlViewSecurityCommandFailed;
    }
}

fn read(ctx: *fixture.Context, sql: []const u8, expected: []const i64) !void {
    var authenticated = ctx.*;
    authenticated.headers = ctx.reader_headers;
    authenticated.base = ctx.metadata_uri;
    var response = try client.execute(&authenticated, sql, .read);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) {
        std.debug.print("view security read status={d} sql={s} body={s}\n", .{ response.status, sql, response.body });
        return error.SqlViewSecurityReadFailed;
    }
    var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(expected.len, parsed.value.rows.len);
    for (parsed.value.rows, expected) |row, n| try std.testing.expectEqual(n, try fixture.sqlInteger(row[0]));
}

fn active(ctx: *fixture.Context, table_id: u64) !void {
    const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    const deadline = platform.time.monotonicNs() +| 20 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        const bytes = try source.systemCatalog(ctx.alloc, .{ .deadline_ns = deadline, .row_policy_install_authority = true }, .{ .policy_publication_status = table_id });
        defer ctx.alloc.free(bytes);
        var parsed = try std.json.parseFromSlice(?local.system_catalog_policies.PublicationStamp, ctx.alloc, bytes, .{});
        defer parsed.deinit();
        if (parsed.value) |publication| if (publication.phase == .active) return;
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.SqlViewPolicyPublicationTimeout;
}

pub fn beforeRestart(ctx: *fixture.Context) !void {
    var created = try client.execute(ctx, "CREATE TABLE secured_rows(id BIGINT,amount NUMERIC,tags TEXT[])", .write_admission);
    defer created.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), created.status);
    const schema = try fixture.waitServingSchema(ctx, "secured_rows", 0);
    try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, schema.table_id);
    try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "secured_rows");
    try command(ctx, "INSERT INTO secured_rows(_id,id,amount,tags) VALUES('one',1,20,ARRAY['visible']),('two',2,2,ARRAY['visible'])");
    try command(ctx, "CREATE POLICY owner_rows ON secured_rows FOR SELECT TO \"pk-rewriter\" USING(id=1 AND amount>10 AND tags IS NOT NULL)");
    try command(ctx, "CREATE POLICY caller_rows ON secured_rows FOR SELECT TO \"pk-reader\" USING(id=2 AND amount>0 AND tags IS NOT NULL)");
    try command(ctx, "ALTER TABLE secured_rows ENABLE ROW LEVEL SECURITY");
    try active(ctx, schema.table_id);
    try command(ctx, "CREATE VIEW owner_rows_v AS SELECT id,CURRENT_USER AS actor FROM secured_rows");
    try command(ctx, "CREATE VIEW invoker_rows_v WITH(security_invoker=true) AS SELECT id,CURRENT_USER AS actor FROM secured_rows");
    try command(ctx, "CREATE VIEW nested_invoker_rows_v AS SELECT id,actor FROM invoker_rows_v");
    try afterRestart(ctx);

    const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
    const manager = api.cfg.user_manager orelse return error.MissingUserManager;
    try manager.setRowFilter("pk-reader", "secured_rows", "{\"term\":{\"id\":2}}");
    defer manager.removeRowFilter("pk-reader", "secured_rows") catch {};
    // Caller filters remain restrictive even when owner RLS selects row one.
    try read(ctx, "SELECT id FROM owner_rows_v", &.{});
    try read(ctx, "SELECT id FROM invoker_rows_v", &.{2});
    try manager.removeRowFilter("pk-reader", "secured_rows");
    try manager.setRowFilter("pk-rewriter", "secured_rows", "{\"term\":{\"id\":2}}");
    defer manager.removeRowFilter("pk-rewriter", "secured_rows") catch {};
    try read(ctx, "SELECT id FROM owner_rows_v", &.{});
    try read(ctx, "SELECT id FROM invoker_rows_v", &.{2});
    try manager.removeRowFilter("pk-rewriter", "secured_rows");
    try pgwire(ctx);
}

pub fn afterRestart(ctx: *fixture.Context) !void {
    try read(ctx, "SELECT id FROM owner_rows_v WHERE actor=CURRENT_USER", &.{1});
    try read(ctx, "SELECT id FROM invoker_rows_v WHERE actor=CURRENT_USER", &.{2});
    try read(ctx, "SELECT id FROM nested_invoker_rows_v WHERE actor=CURRENT_USER", &.{2});
}

fn request(ctx: *fixture.Context, cancel: *const std.atomic.Value(bool), sql: []const u8) wire.Request {
    return .{ .statement = sql, .limit = 100, .io = ctx.io, .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.Io.Clock.awake.now(ctx.io).nanoseconds + 15 * std.time.ns_per_s } }, .cancel_requested = cancel };
}

fn pgwire(ctx: *fixture.Context) !void {
    const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
    var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = api };
    const backend = adapter.backend();
    const identity = try backend.vtable.authenticate(backend.context, ctx.alloc, "pk-reader", "pk-reader-durable-password");
    defer identity.release(identity.context, ctx.alloc);
    defer backend.vtable.disconnect(backend.context, identity, null);
    var arena = std.heap.ArenaAllocator.init(ctx.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var cancel: std.atomic.Value(bool) = .init(false);
    try command(ctx, "CREATE VIEW pgwire_view AS SELECT id FROM secured_rows");
    var query = request(ctx, &cancel, "SELECT id FROM pgwire_view");
    const description = try backend.vtable.describe(backend.context, a, identity, query);
    query.binding_guard = description.binding_guard orelse return error.SqlViewPreparedGuardMissing;
    try command(ctx, "ALTER VIEW pgwire_view RENAME TO pgwire_renamed_view");
    try command(ctx, "CREATE VIEW pgwire_view AS SELECT 99::BIGINT AS id");
    var result = try backend.vtable.execute(backend.context, a, identity, query);
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 1), result.rowCount());
    try std.testing.expectEqual(@as(i64, 1), try fixture.sqlInteger((try result.cell(a, 0, 0)).value));
    try command(ctx, "CREATE OR REPLACE VIEW pgwire_renamed_view AS SELECT id FROM secured_rows WHERE id=1");
    try std.testing.expectError(error.CatalogGenerationChanged, backend.vtable.execute(backend.context, a, identity, query));
    try cursor(ctx, backend);
    try protocol(ctx, backend);
}

fn cursor(ctx: *fixture.Context, backend: wire.Backend) !void {
    try command(ctx, "CREATE VIEW cursor_view AS SELECT id FROM usage_records");
    const manager = (ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi).cfg.user_manager.?;
    const users = @import("../usermgr/mod.zig");
    var permission = try users.Permission.initOwned(ctx.alloc, .table, "cursor_view", .read);
    defer permission.deinit(ctx.alloc);
    var user = try manager.createUser("view-cursor-reader", "cursor-password", &.{permission});
    defer user.deinit(manager.alloc);
    const identity = try backend.vtable.authenticate(backend.context, ctx.alloc, "view-cursor-reader", "cursor-password");
    defer identity.release(identity.context, ctx.alloc);
    defer backend.vtable.disconnect(backend.context, identity, null);
    var arena = std.heap.ArenaAllocator.init(ctx.alloc);
    defer arena.deinit();
    var cancel: std.atomic.Value(bool) = .init(false);
    const query = request(ctx, &cancel, "SELECT id FROM cursor_view");
    const stream = (try backend.vtable.open_stream.?(backend.context, ctx.alloc, identity, query)) orelse return error.SqlViewStreamDeclined;
    defer stream.close(stream.context);
    var page = try stream.next(stream.context, arena.allocator(), query, 1);
    try std.testing.expectEqual(@as(usize, 1), page.result.rowCount());
    page.result.deinit();
    try stream.validate.?(stream.context, arena.allocator(), query);
    try manager.setRowFilter("view-cursor-reader", "cursor_view", "{\"term\":{\"id\":2}}");
    defer manager.removeRowFilter("view-cursor-reader", "cursor_view") catch {};
    try std.testing.expectError(error.Forbidden, stream.validate.?(stream.context, arena.allocator(), query));
    try manager.removeRowFilter("view-cursor-reader", "cursor_view");
    // Detached spool authorization must not borrow the freed binder arena.
    stream.detach.?(stream.context);
    try stream.validate.?(stream.context, arena.allocator(), query);
    try manager.removePermissionFromUser("view-cursor-reader", "cursor_view", .table);
    try std.testing.expectError(error.Forbidden, stream.validate.?(stream.context, arena.allocator(), query));
    try manager.addPermissionToUser("view-cursor-reader", permission);
    try command(ctx, "DROP VIEW cursor_view");
    try command(ctx, "CREATE VIEW cursor_view AS SELECT 99::BIGINT AS id");
    try std.testing.expectError(error.CatalogGenerationChanged, stream.validate.?(stream.context, arena.allocator(), query));
}

fn frame(out: *std.Io.Writer, tag: u8, payload: []const u8) !void {
    try out.writeByte(tag);
    try out.writeInt(u32, @intCast(payload.len + 4), .big);
    try out.writeAll(payload);
}

fn protocol(ctx: *fixture.Context, backend: wire.Backend) !void {
    var input: std.Io.Writer.Allocating = .init(ctx.alloc);
    defer input.deinit();
    const startup = "user\x00pk-reader\x00database\x00default\x00\x00";
    try input.writer.writeInt(u32, startup.len + 8, .big);
    try input.writer.writeInt(u32, 196608, .big);
    try input.writer.writeAll(startup);
    try frame(&input.writer, 'p', "pk-reader-durable-password\x00");
    try frame(&input.writer, 'Q', "SELECT id FROM owner_rows_v\x00");
    try frame(&input.writer, 'X', "");
    var reader = std.Io.Reader.fixed(input.written());
    var output: std.Io.Writer.Allocating = .init(ctx.alloc);
    defer output.deinit();
    const pg = @import("../pgwire/protocol.zig");
    var session: pg.Session = .{ .alloc = ctx.alloc, .io = ctx.io, .source = backend, .reader = &reader, .writer = &output.writer };
    defer session.deinit();
    try session.run();
    var frames: pg.Cursor = .{ .bytes = output.written() };
    var rows: usize = 0;
    var completions: usize = 0;
    while (frames.offset < frames.bytes.len) {
        const tag = try frames.int(u8);
        const size = try frames.int(u32);
        const payload = try frames.take(size - 4);
        if (tag == 'E') {
            std.debug.print("view pgwire protocol error {s}\n", .{payload});
            return error.SqlViewProtocolFailed;
        }
        if (tag == 'C') completions += 1;
        if (tag == 'D') {
            var row: pg.Cursor = .{ .bytes = payload };
            try std.testing.expectEqual(@as(u16, 1), try row.int(u16));
            const length = try row.int(i32);
            try std.testing.expectEqual(@as(i32, 1), length);
            try std.testing.expectEqualStrings("1", try row.take(@intCast(length)));
            rows += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1), rows);
    try std.testing.expectEqual(@as(usize, 1), completions);
}
