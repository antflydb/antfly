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

//! Native temporal expressions over the shared immutable typed pipeline.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;
const compare = @import("sql_parity_reference.zig");
const Json = std.json.Value;
const Output = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const Json, sql_nulls: []const []const bool };
const Entry = struct {
    id: []const u8,
    source_id: ?[]const u8 = null,
    sql: []const u8,
    parameters: []const Json,
    create: []const u8,
    seed: []const u8,
    read: []const u8,
    before: Output,
    output: ?Output,
    final: Output,
    sqlstate: ?[]const u8,
    affected: i64,
    parameter_descriptors: []const local.sql_scalar.Type = &.{},
    cold: []const struct { sql: []const u8, output: Output, final: Output },
};

fn success(ctx: *fixture.Context, statement: []const u8, connection: ?[]const u8, retry: client.Retry) !void {
    var response = try client.executeConnected(ctx, null, connection, statement, retry);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) std.debug.print("temporal bin command sql={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 202);
}

fn expectOutput(ctx: *fixture.Context, actual: wire.SQLResponse, expected: Output, date: []const u8) !void {
    try std.testing.expectEqual(expected.columns.len, actual.columns.len);
    for (expected.columns, expected.column_oids, actual.columns) |name, oid, column| {
        try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expect(compare.postgresColumnMatches(column, oid));
    }
    try std.testing.expectEqual(expected.rows.len, actual.rows.len);
    const nulls = actual.sql_nulls orelse return error.MissingSqlNullBitmap;
    var scratch = std.heap.ArenaAllocator.init(ctx.alloc);
    defer scratch.deinit();
    const a = scratch.allocator();
    for (expected.rows, expected.sql_nulls, actual.rows, nulls) |want, flags, row, actual_flags| {
        const normalized = try a.dupe(Json, want);
        for (normalized) |*value| if (value.* == .string) {
            if (std.mem.eql(u8, value.string, "$transaction_date")) value.* = .{ .string = date };
            if (std.mem.eql(u8, value.string, "$transaction_midnight")) value.* = .{ .string = try std.fmt.allocPrint(a, "{s} 00:00:00+00", .{date}) };
        };
        if (!try compare.rowMatchesWithNulls(a, actual.columns, row, actual_flags, normalized, flags)) {
            std.debug.print("temporal bin row mismatch expected={f} actual={f}\n", .{ std.json.fmt(normalized, .{}), std.json.fmt(row, .{}) });
            return error.TemporalBinPostimageMismatch;
        }
    }
}

fn observe(ctx: *fixture.Context, statement: []const u8, expected: Output, date: []const u8) !void {
    var response = try client.execute(ctx, statement, .read);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("temporal bin observation sql={s} body={s}\n", .{ statement, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer result.deinit();
    try expectOutput(ctx, result.value, expected, date);
}

// HTTP JSON parameters are context-inferred; pgwire Parse can declare exact
// builtin inputs. Preserve the source's integer parameter identity for the
// original temporal comparison through the real authenticated native adapter.
fn typedRejection(ctx: *fixture.Context, entry: Entry) !void {
    const pg = @import("../pgwire/backend.zig");
    var adapter: @import("sql_pgwire.zig").Adapter = .{ .server = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi };
    const backend = adapter.backend();
    const identity = try backend.vtable.authenticate(backend.context, ctx.alloc, "pk-rewriter", "pk-rewrite-durable-password");
    defer identity.release(identity.context, ctx.alloc);
    var canceled: std.atomic.Value(bool) = .init(false);
    var diagnostic: pg.Diagnostic = .{};
    const types = try ctx.alloc.alloc(pg.Type, entry.parameter_descriptors.len);
    defer ctx.alloc.free(types);
    for (entry.parameter_descriptors, types) |descriptor, *kind| kind.* = if (descriptor.kind) |family| switch (family) {
        inline else => |tag| @field(pg.Type, @tagName(tag)),
    } else .unknown;
    const request: pg.Request = .{
        .statement = entry.sql,
        .parameters = entry.parameters,
        .parameter_descriptors = entry.parameter_descriptors,
        .parameter_types = types,
        .limit = 128,
        .io = ctx.io,
        .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = std.math.maxInt(i96) } },
        .cancel_requested = &canceled,
        .diagnostics = &diagnostic,
    };
    var arena: std.heap.ArenaAllocator = .init(ctx.alloc);
    defer arena.deinit();
    if (backend.vtable.describe(backend.context, arena.allocator(), identity, request)) |_| return error.MissingTemporalDomainRejection else |_| {
        try std.testing.expectEqualStrings(entry.sqlstate.?, &(diagnostic.code orelse return error.MissingSqlDiagnostic));
    }
    diagnostic = .{};
    if (backend.vtable.execute(backend.context, arena.allocator(), identity, request)) |result| {
        var owned = result;
        owned.deinit();
        return error.MissingTemporalDomainRejection;
    } else |_| {
        try std.testing.expectEqualStrings(entry.sqlstate.?, &(diagnostic.code orelse return error.MissingSqlDiagnostic));
    }
}

const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    entry: Entry,
    retained: *std.heap.ArenaAllocator,
    date: []const u8 = "",
    credentials: client.ApiKeyHeaders = .{},

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        try success(ctx, self.entry.create, null, .write_admission);
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        try success(ctx, self.entry.seed, null, .write_admission);
        try observe(ctx, self.entry.read, self.entry.before, self.date);
        var opened = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/sql/connections", .POST, "{}", 15_000);
        defer opened.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), opened.status);
        var connection = try std.json.parseFromSlice(wire.SQLConnectionResponse, ctx.alloc, opened.body, .{});
        defer connection.deinit();
        const id = connection.value.connection_id;
        const close_path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{id});
        defer ctx.alloc.free(close_path);
        defer {
            var closed = fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.headers, ctx.base, close_path, .DELETE, null, 15_000) catch null;
            if (closed) |*response| response.deinit(ctx.alloc);
        }
        try success(ctx, "BEGIN", id, .never);
        var clock_response = try client.executeConnected(ctx, null, id, "SELECT now()::text AS instant,CURRENT_DATE AS day", .session_observation);
        defer clock_response.deinit(ctx.alloc);
        if (clock_response.status != 200) std.debug.print("temporal bin clock body={s}\n", .{clock_response.body});
        try std.testing.expectEqual(@as(u16, 200), clock_response.status);
        var clock = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, clock_response.body, .{});
        defer clock.deinit();
        const instant = clock.value.rows[0][0].string;
        const day = clock.value.rows[0][1].string;
        try std.testing.expectEqualStrings(instant[0..10], day);
        try std.testing.expect(compare.postgresColumnMatches(clock.value.columns[1], 1082));
        self.date = try self.retained.allocator().dupe(u8, day);
        var response = try client.executeConnectedParameters(ctx, id, self.entry.sql, self.entry.parameters, .never);
        defer response.deinit(ctx.alloc);
        if (self.entry.sqlstate) |code| {
            if (response.status != 400) std.debug.print("temporal bin original id={s} body={s}\n", .{ self.entry.id, response.body });
            try std.testing.expectEqual(@as(u16, 400), response.status);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings(code, diagnostic.value.code);
            try success(ctx, "ROLLBACK", id, .never);
        } else {
            if (response.status != 200) std.debug.print("temporal bin original id={s} body={s}\n", .{ self.entry.id, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            try std.testing.expectEqual(self.entry.affected, result.value.rows_affected);
            try expectOutput(ctx, result.value, self.entry.output.?, self.date);
            try success(ctx, "COMMIT", id, .never);
        }
        try observe(ctx, self.entry.read, self.entry.final, self.date);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try observe(ctx, self.entry.read, self.entry.final, self.date);
        try self.runColdProbes(ctx);
    }

    fn runColdProbes(self: *@This(), ctx: *fixture.Context) !void {
        for (self.entry.cold) |probe| {
            var response = try client.execute(ctx, probe.sql, .never);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("temporal bin cold sql={s} body={s}\n", .{ probe.sql, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            try expectOutput(ctx, result.value, probe.output, self.date);
            try observe(ctx, self.entry.read, probe.final, self.date);
        }
    }
};

test "mounted SQL original temporal bins and transaction dates preserve typed writes and cold postimages" {
    // sql-0179, sql-0264, sql-0265, sql-0266, sql-1467, sql-1477.
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(struct { entries: []const Entry }, a, local.sql_parity_fixtures.temporal_bin_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    try std.testing.expectEqual(@as(usize, 6), reference.value.entries.len);
    for (reference.value.entries) |entry| {
        const original = try corpus.get(entry.source_id orelse entry.id);
        try std.testing.expectEqualStrings(original.sql, entry.sql);
        var retained = std.heap.ArenaAllocator.init(a);
        defer retained.deinit();
        fixture.runWithState(Campaign, .{ .entry = entry, .retained = &retained }) catch |err| {
            std.debug.print("temporal bin original id={s}\n", .{entry.id});
            return err;
        };
    }
}

const DomainCampaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    entries: []const Entry,
    campaign: Campaign,

    fn originals(self: *@This(), ctx: *fixture.Context) !void {
        for (self.entries) |entry| {
            errdefer std.debug.print("temporal domain witness={s}\n", .{entry.id});
            if (entry.parameter_descriptors.len != 0) {
                try typedRejection(ctx, entry);
            } else {
                var response = try client.executeParameters(ctx, entry.sql, entry.parameters, .never);
                defer response.deinit(ctx.alloc);
                try std.testing.expectEqual(@as(u16, 400), response.status);
                var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
                defer diagnostic.deinit();
                try std.testing.expectEqualStrings(entry.sqlstate.?, diagnostic.value.code);
            }
            try observe(ctx, entry.read, entry.final, "");
        }
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.campaign.credentials.activate(ctx);
        const first = self.entries[0];
        try success(ctx, first.create, null, .write_admission);
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        try success(ctx, first.seed, null, .write_admission);
        try observe(ctx, first.read, first.before, "");
        try self.originals(ctx);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try self.originals(ctx);
        try self.campaign.runColdProbes(ctx);
    }
};

test "mounted SQL temporal originals and mutation assignments preserve diagnostics precise kinds and cold postimages" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(struct { entries: []const Entry }, a, local.sql_parity_fixtures.temporal_domain_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    try std.testing.expectEqual(@as(usize, 5), reference.value.entries.len);
    const first = reference.value.entries[0];
    const original_ids = [_][]const u8{ "sql-0201", "sql-0592" };
    // Every command rejects before mutation, so one shared profile and one
    // owner restart qualify the complete cohort without five cluster boots.
    for (reference.value.entries, 0..) |entry, index| {
        if (index < original_ids.len) try std.testing.expectEqualStrings(original_ids[index], entry.id) else try std.testing.expect(std.mem.startsWith(u8, entry.id, "bigint-domain-"));
        const original = try corpus.get(entry.source_id orelse entry.id);
        try std.testing.expectEqualStrings(original.sql, entry.sql);
        try std.testing.expectEqual(original.params.len, entry.parameters.len);
        for (original.params, entry.parameters) |expected, actual| {
            try std.testing.expectEqual(try fixture.sqlInteger(try fixture.field(expected, "integer")), try fixture.sqlInteger(actual));
        }
        try std.testing.expectEqualStrings(first.create, entry.create);
        try std.testing.expectEqualStrings(first.seed, entry.seed);
        try std.testing.expectEqualStrings(first.read, entry.read);
    }
    var retained = std.heap.ArenaAllocator.init(a);
    defer retained.deinit();
    try fixture.runWithState(DomainCampaign, .{ .entries = reference.value.entries, .campaign = .{ .entry = first, .retained = &retained } });
}
