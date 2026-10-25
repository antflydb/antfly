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

//! Complete original document TRUNCATE lifecycles through the shared owner lane.
const std = @import("std");
const platform = @import("antfly_platform");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const compare = @import("sql_parity_reference.zig");
const wire = @import("antfly_metadata_openapi").types;
const http = @import("http_server.zig");
const Json = std.json.Value;
const names = [_][]const u8{ "docs", "docs_archive", "untouched" };
const schema = "{\"schema\":{\"storage_mode\":\"document\",\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}},\"additionalProperties\":true}}}}}";
const Entry = struct {
    id: []const u8,
    sql: []const u8,
    selected: []const []const u8,
    before: Json,
    failure: struct { sql: []const u8, sqlstate: []const u8, after: Json },
    tag: []const u8,
    empty: Json,
    final: Json,
};
const Fingerprint = struct { version: u64, digest: [32]u8 };
const Cut = struct { ids: [names.len]u64, physical: [names.len][]const u8 };

/// Every member is resolved in ONE catalog transaction. Reading each name
/// separately could turn two valid cuts into a false mixed-generation report.
fn cut(ctx: *fixture.Context, a: std.mem.Allocator) !Cut {
    const domain = local.system_catalog_domain;
    const source = http.StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
    var targets: [names.len]domain.Target = undefined;
    for (&targets, names) |*target, name| target.* = .{ .table = name };
    const lookup: domain.ResolveMany = .{ .targets = &targets };
    const bytes = try source.systemCatalog(a, .{ .deadline_ns = platform.time.monotonicNs() +| 5 * std.time.ns_per_s }, .{ .resolve_many = lookup });
    const resolved = try std.json.parseFromSliceLeaky(domain.ResolvedMany, a, bytes, .{});
    if (resolved.tables.len != names.len) return error.InvalidTruncateCatalogCut;
    var result: Cut = undefined;
    for (resolved.tables, &result.ids, &result.physical) |table, *id, *physical| {
        const value = table orelse return error.MissingTruncateTable;
        id.* = value.table_id;
        physical.* = value.name;
    }
    return result;
}

fn fingerprint(ctx: *fixture.Context, physical: []const u8) !Fingerprint {
    const source = ctx.data.http_server.?.table_reads orelse return error.MissingTruncateReader;
    var primary = (try source.lookup(ctx.alloc, physical, "doc:a", .{ .include_primary_digest = true }, .read_index)) orelse return error.MissingTruncatePrimary;
    defer primary.deinit(ctx.alloc);
    return .{ .version = primary.version, .digest = primary.expected_content_digest orelse return error.MissingTruncateDigest };
}

fn readImage(ctx: *fixture.Context, expected: Json) !void {
    for (names) |name| {
        const sql = try std.fmt.allocPrint(ctx.alloc, "SELECT _id,title FROM {s} ORDER BY _id", .{name});
        defer ctx.alloc.free(sql);
        var response = try client.execute(ctx, sql, .read);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("document truncate read sql={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        const reference = expected.object.get(name) orelse return error.MissingTruncatePostimage;
        const rows = reference.object.get("rows").?.array.items;
        const columns = reference.object.get("columns").?.array.items;
        const oids = reference.object.get("column_oids").?.array.items;
        try std.testing.expectEqual(columns.len, parsed.value.columns.len);
        for (parsed.value.columns, columns, oids) |column, label, oid| {
            try std.testing.expectEqualStrings(label.string, column.name);
            try std.testing.expect(compare.postgresColumnMatches(column, @intCast(oid.integer)));
        }
        try std.testing.expectEqual(rows.len, parsed.value.rows.len);
        for (parsed.value.rows, rows, 0..) |actual, row, index| {
            try std.testing.expect(try compare.rowMatchesWithNulls(ctx.alloc, parsed.value.columns, actual, if (parsed.value.sql_nulls) |nulls| nulls[index] else null, row.array.items, null));
        }
    }
}

fn reject(ctx: *fixture.Context, sql: []const u8, code: []const u8) !void {
    var response = try client.execute(ctx, sql, .write_admission);
    defer response.deinit(ctx.alloc);
    try std.testing.expect(response.status >= 400);
    var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
    defer diagnostic.deinit();
    try std.testing.expectEqualStrings(code, diagnostic.value.code);
    // A known pre-admission failure carries no durable job to reconcile.
    var envelope = try std.json.parseFromSlice(Json, ctx.alloc, response.body, .{});
    defer envelope.deinit();
    if (envelope.value.object.get("ddl_receipt")) |receipt| try std.testing.expect(receipt == .null);
}

const Campaign = struct {
    pub const native_sessions = true;
    pub const native_auth = true;
    pub const cold_catalog = true;
    entry: Entry,
    credentials: client.ApiKeyHeaders = .{},
    final_ids: [names.len]u64 = undefined,
    final_fingerprints: [names.len]Fingerprint = undefined,

    fn selected(self: *const @This(), name: []const u8) bool {
        for (self.entry.selected) |member| if (std.mem.eql(u8, member, name)) return true;
        return false;
    }

    fn unchanged(self: *@This(), ctx: *fixture.Context, original: Cut, prints: [names.len]Fingerprint) !void {
        var scratch = std.heap.ArenaAllocator.init(ctx.alloc);
        defer scratch.deinit();
        try std.testing.expectEqualDeep(original.ids, (try cut(ctx, scratch.allocator())).ids);
        for (original.physical, prints) |physical, expected| try std.testing.expectEqualDeep(expected, try fingerprint(ctx, physical));
        try readImage(ctx, self.entry.failure.after);
    }

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        ctx.headers = ctx.admin_headers;
        for (names) |name| {
            const suffix = try std.fmt.allocPrint(ctx.alloc, "/db/v1/tables/{s}", .{name});
            defer ctx.alloc.free(suffix);
            var created = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.metadata_uri, suffix, .POST, schema);
            defer created.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), created.status);
            const serving = try fixture.waitServingSchema(ctx, name, 0);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, name);
            const sql = try std.fmt.allocPrint(ctx.alloc, "INSERT INTO {s}(_id,title) VALUES ('doc:a','{s}:before')", .{ name, name });
            defer ctx.alloc.free(sql);
            var seeded = try client.execute(ctx, sql, .write_admission);
            defer seeded.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), seeded.status);
        }
        try readImage(ctx, self.entry.before);
        var scratch = std.heap.ArenaAllocator.init(ctx.alloc);
        defer scratch.deinit();
        const original = try cut(ctx, scratch.allocator());
        var prints: [names.len]Fingerprint = undefined;
        for (original.physical, &prints) |physical, *print| print.* = try fingerprint(ctx, physical);
        var coordinator = ctx.*;
        coordinator.base = ctx.metadata_uri;
        var reader = coordinator;
        reader.headers = ctx.reader_headers;
        try reject(&reader, self.entry.sql, "42501");
        try self.unchanged(ctx, original, prints);
        try reject(&coordinator, self.entry.failure.sql, self.entry.failure.sqlstate);
        try self.unchanged(ctx, original, prints);

        // Exact original text, exactly one admitted request. An unknown reply
        // is reconciled through this job ID and never by replaying TRUNCATE.
        var response = try client.execute(&coordinator, self.entry.sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status != 202 and response.status != 409) std.debug.print("document truncate id={s} status={d} body={s}\n", .{ self.entry.id, response.status, response.body });
        try std.testing.expect(response.status == 202 or response.status == 409);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings("DDL PENDING", parsed.value.command_tag);
        const receipt = parsed.value.ddl_receipt orelse return error.MissingTruncateReceipt;
        try std.testing.expectEqual(if (response.status == 202) @TypeOf(receipt.state).pending else .admission_unknown, receipt.state);
        const job_id = receipt.restore_job_id orelse return error.MissingTruncateJob;
        const deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
        var observation_arena = std.heap.ArenaAllocator.init(ctx.alloc);
        defer observation_arena.deinit();
        while (true) {
            _ = observation_arena.reset(.retain_capacity);
            try ctx.checkDrivers();
            // A public name cut must contain either ALL original generations
            // or ALL replacements, never a partially published cohort.
            const observed_cut = cut(ctx, observation_arena.allocator()) catch |err| switch (err) {
                error.CatalogPublicationProofPending, error.CatalogOccupancyNotReady => null,
                else => return err,
            };
            if (observed_cut) |current| {
                var replaced: usize = 0;
                for (names, original.ids, current.ids) |name, before, after| {
                    if (self.selected(name)) {
                        if (before != after) replaced += 1;
                    } else try std.testing.expectEqual(before, after);
                }
                try std.testing.expect(replaced == 0 or replaced == self.entry.selected.len);
            }
            var observed = try fixture.job(ctx.alloc, ctx.transport, ctx.headers, ctx.metadata_uri, job_id);
            defer observed.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), observed.status);
            var job = try std.json.parseFromSlice(wire.RestoreJob, ctx.alloc, observed.body, .{ .ignore_unknown_fields = true });
            defer job.deinit();
            if (std.mem.eql(u8, job.value.phase, "succeeded")) break;
            if (std.mem.eql(u8, job.value.phase, "failed") or std.mem.eql(u8, job.value.phase, "canceled")) {
                std.debug.print("document truncate job id={s} body={s}\n", .{ self.entry.id, observed.body });
                return error.DocumentTruncateFailed;
            }
            if (platform.time.monotonicNs() >= deadline) return error.DocumentTruncateTimeout;
            try ctx.io.sleep(.fromMilliseconds(20), .awake);
        }
        const published = try cut(ctx, scratch.allocator());
        self.final_ids = published.ids;
        for (names, original.ids, published.ids, prints, published.physical) |name, before, after, print, physical| {
            if (self.selected(name)) try std.testing.expect(before != after) else {
                try std.testing.expectEqual(before, after);
                try std.testing.expectEqualDeep(print, try fingerprint(ctx, physical));
            }
            const serving = try fixture.waitServingSchema(ctx, name, 0);
            try std.testing.expectEqual(after, serving.table_id);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, name);
        }
        try readImage(ctx, self.entry.empty);
        for (self.entry.selected) |name| {
            const sql = try std.fmt.allocPrint(ctx.alloc, "INSERT INTO {s}(_id,title) VALUES ('doc:a','{s}:after')", .{ name, name });
            defer ctx.alloc.free(sql);
            var written = try client.execute(ctx, sql, .write_admission);
            defer written.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), written.status);
        }
        try readImage(ctx, self.entry.final);
        for (published.physical, &self.final_fingerprints) |physical, *print| print.* = try fingerprint(ctx, physical);
    }

    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        ctx.headers = ctx.admin_headers;
        for (names) |name| _ = try fixture.waitServingSchema(ctx, name, 0);
        var scratch = std.heap.ArenaAllocator.init(ctx.alloc);
        defer scratch.deinit();
        const reopened = try cut(ctx, scratch.allocator());
        try std.testing.expectEqualDeep(self.final_ids, reopened.ids);
        try readImage(ctx, self.entry.final);
        for (reopened.physical, self.final_fingerprints) |physical, expected| try std.testing.expectEqualDeep(expected, try fingerprint(ctx, physical));
    }
};

test "mounted SQL original document multi table and CASCADE truncate publish atomic cohorts and recover cold rows" {
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    var reference = try std.json.parseFromSlice(struct { entries: []const Entry }, std.testing.allocator, local.sql_parity_fixtures.document_truncate_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    try std.testing.expectEqual(@as(usize, 2), reference.value.entries.len);
    for (reference.value.entries, [_][]const u8{ "sql-1102", "sql-1104" }) |entry, id| {
        try std.testing.expectEqualStrings(id, entry.id);
        const original = try corpus.get(id);
        try std.testing.expectEqualStrings(original.sql, entry.sql);
        try std.testing.expectEqualStrings("TRUNCATE TABLE", entry.tag);
        try fixture.runWithState(Campaign, .{ .entry = entry });
    }
}
