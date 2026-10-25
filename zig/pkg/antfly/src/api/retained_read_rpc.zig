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

//! Private service-authenticated retained read protocol. Never expose this
//! protocol through public SQL routing: the coordinator owns user authorization
//! and supplies the already-authorized row policy on open. Owner credentials
//! are reauthenticated for every operation, including close and paging.
const std = @import("std");
const owner_mod = @import("retained_read_owner.zig");
const registry = @import("../storage/retained_read_registry.zig");
const types = @import("antfly_local_sources").storage_db_types;
const reads = @import("antfly_local_sources").api_table_read_source;
const time = @import("antfly_platform").time;

pub const max_request_bytes = 4 << 20;

/// SQL validation keeps its public diagnostic; operational failures retain
/// the shared runtime class instead of becoming an unknown SQL/internal error.
pub fn normalizationFailureHttpStatus(err: anyerror) u16 {
    const diagnostic = @import("antfly_local_sources").sql_errors.describe(err);
    if (!std.mem.eql(u8, diagnostic.code, "XX000")) return diagnostic.httpStatus();
    const errors = @import("antfly_runtime_abi").error_abi;
    const code = std.enums.fromInt(errors.Code, errors.statusFromError(err).code) orelse return 500;
    return switch (code) {
        .invalid_argument => 400,
        .not_found => 404,
        .already_exists, .conflict => 409,
        .unauthorized => 401,
        .forbidden => 403,
        .unavailable, .retryable => 503,
        .timeout => 504,
        .cancelled => 408,
        .unsupported => 501,
        .ok, .out_of_memory, .corrupt, .internal => 500,
    };
}

test "retained read admission is independent of RLS primary lookup and fails closed without a barrier" {
    const Fake = struct {
        admissions: usize = 0,
        raw_reads: usize = 0,
        fn admit(raw: *anyopaque, _: std.mem.Allocator, route: @import("../metadata/api.zig").CatalogRouteFence, group: u64, table: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(route.route.group_id, group);
            try std.testing.expectEqualStrings("rows", table);
            self.admissions += 1;
        }
        fn lookup(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: types.LookupOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.raw_reads += 1;
            return error.RowPolicyAuthenticationRequired;
        }
    };
    const a = std.testing.allocator;
    var fixture: Fake = .{};
    var resources = try registry.Registry.init(a, std.testing.io, 1, 8, 8, std.time.ns_per_s);
    defer resources.deinit();
    var owner: owner_mod.Owner = .{ .registry = &resources, .source = .{
        .ptr = &fixture,
        .route_fence = .{
            .metadata_group_id = 1,
            .catalog_revision = 1,
            .table_id = 2,
            .topology_epoch = 4,
            .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } },
        },
        .vtable = &.{ .lookup = Fake.lookup, .scan = undefined, .query = undefined, .admit_statement_read_group_local_routed = Fake.admit },
    } };
    const scope: registry.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 0 };
    const response = try execute(a, owner, scope, "rows", .{ .operation = .admit, .schema_version = 0 });
    defer a.free(response);
    try std.testing.expectEqual(@as(usize, 1), fixture.admissions);
    try std.testing.expectEqual(@as(usize, 0), fixture.raw_reads);
    owner.source.vtable = &.{ .lookup = Fake.lookup, .scan = undefined, .query = undefined };
    try std.testing.expectError(error.SqlStatementSnapshotRequired, execute(a, owner, scope, "rows", .{ .operation = .admit, .schema_version = 0 }));
    try std.testing.expectEqual(@as(usize, 0), fixture.raw_reads);
}

pub const max_response_bytes = 32 << 20;
pub const max_lease_ms: u32 = 30_000;
pub const Request = struct {
    operation: enum { admit, capture, validate, snapshot, open, open_snapshot, next, normalize, range_proofs, close, cancel, renew_snapshot },
    schema_version: u32,
    token: ?registry.Token = null,
    connection: u128 = 0,
    lease_ms: u32 = 5000,
    sequence: u64 = 0,
    limit: u32 = 128,
    from: []const u8 = "",
    to: []const u8 = "",
    query: ?types.RelationalRowQuery = null,
    filter_query_json: []const u8 = "",
    inclusive_from: bool = false,
    exclusive_to: bool = false,
    sql_document_preimage: bool = false,
    include_content_hashes: bool = false,
    include_range_proofs: bool = false,
    row_policy_principal_proof: []const u8 = "",
    row_policy_database: []const u8 = "",
    writes: []const types.BatchWrite = &.{},
};

// The legacy wire shape deliberately excludes native pointers/directories.
pub const WireRow = struct {
    id: []const u8,
    version: u64,
    schema_version: u32,
    value: std.json.Value,
    expected_content_digest: ?[32]u8 = null,
    document: ?std.json.Value = null,
    sql_nulls: ?[]const bool = null,
};
const native_page = @import("retained_read_page.zig");

pub const Response = struct {
    text_filter_digest: ?[32]u8 = null,
    range_proofs: []const @import("antfly_local_sources").storage_range_protection.Proof = &.{},
    token: ?registry.Token = null,
    busy: bool = false,
    sequence: u64 = 0,
    rows: []const WireRow = &.{},
    after: ?[]const u8 = null,
    writes: []const types.BatchWrite = &.{},
};

/// Scope is constructed by authenticated ingress, never deserialized from a
/// request. Body schema identity is only a requested epoch; native open checks
/// it against the pinned schema before a cursor can be created.
pub fn execute(alloc: std.mem.Allocator, owner: owner_mod.Owner, scope: registry.Scope, table: []const u8, input: Request) ![]u8 {
    return executeWithPageFormat(alloc, owner, scope, table, input, false);
}

pub fn executeWithPageFormat(alloc: std.mem.Allocator, owner: owner_mod.Owner, scope: registry.Scope, table: []const u8, input: Request, native: bool) ![]u8 {
    return executeWithCodec(alloc, owner, scope, table, input, if (native) .v1 else .json);
}
pub fn executeWithCodec(alloc: std.mem.Allocator, owner: owner_mod.Owner, scope: registry.Scope, table: []const u8, input: Request, format: native_page.Format) ![]u8 {
    if (input.schema_version != scope.schema_version) return error.RetainedReadScopeChanged;
    if (input.lease_ms == 0 or input.lease_ms > max_lease_ms) return error.InvalidRetainedReadLease;
    const deadline = time.monotonicNs() +| @as(u64, input.lease_ms) * std.time.ns_per_ms;
    switch (input.operation) {
        .cancel => {
            if (input.connection == 0) return error.InvalidRetainedReadToken;
            owner.registry.disconnectScoped(scope, input.connection);
            return encode(alloc, .{});
        },
        .admit => {
            // This establishes only quorum/topology readiness. Row-policy
            // proofs belong to open; admission must never perform a raw read.
            try owner.source.admitStatementReadGroupLocal(alloc, scope.group_id, table);
            return encode(alloc, .{});
        },
        .capture => {
            if (!owner.source.remote_statement_fences_safe) return error.SqlStatementSnapshotRequired;
            // Mutation freeze has a shorter safety ceiling than read snapshots.
            if (input.lease_ms > 5000) return error.InvalidRetainedReadLease;
            const route = owner.source.route_fence orelse return error.CatalogRouteFenceRequired;
            const token = try owner.capture(scope, input.connection, route, table, deadline);
            errdefer if (token) |value| owner.registry.close(value, scope) catch {};
            return encode(alloc, .{ .token = token, .busy = token == null });
        },
        .validate => {
            try owner.validateCapture(scope, input.token orelse return error.InvalidRetainedReadToken);
            return encode(alloc, .{});
        },
        .snapshot => {
            const token = try owner.captureSnapshot(scope, input.token orelse return error.InvalidRetainedReadToken, input.connection, deadline);
            errdefer owner.registry.close(token, scope) catch {};
            return encode(alloc, .{ .token = token });
        },
        .renew_snapshot => {
            // Build the acknowledgement before mutation: allocation failure
            // must not extend a lease the caller could not observe. No native
            // work or cursor advancement is replayed by this operation.
            const response = try encode(alloc, .{});
            errdefer alloc.free(response);
            try owner.registry.renewSnapshot(input.token orelse return error.InvalidRetainedReadToken, scope, input.connection, time.monotonicNs(), deadline);
            return response;
        },
        .open => {
            const opened = try owner.openQualified(scope, input.token orelse return error.InvalidRetainedReadToken, input.connection, input.from, input.to, .{
                .relational_query = input.query orelse return error.InvalidRetainedReadQuery,
                .filter_query_json = input.filter_query_json,
                .inclusive_from = input.inclusive_from,
                .exclusive_to = input.exclusive_to,
                .sql_document_preimage = input.sql_document_preimage,
                .include_content_hashes = input.include_content_hashes,
                .include_range_proofs = input.include_range_proofs,
                .row_policy_principal_proof = input.row_policy_principal_proof,
                .row_policy_database = input.row_policy_database,
            }, deadline);
            errdefer owner.registry.close(opened.token, scope) catch {};
            if (opened.text_filter_digest) |digest| return encode(alloc, .{ .token = opened.token, .text_filter_digest = digest });
            return encode(alloc, .{ .token = opened.token });
        },
        .open_snapshot => {
            const token = try owner.openSnapshot(scope, input.token orelse return error.InvalidRetainedReadToken, input.connection, input.from, input.to, .{
                .relational_query = input.query orelse return error.InvalidRetainedReadQuery,
                .filter_query_json = input.filter_query_json,
                .inclusive_from = input.inclusive_from,
                .exclusive_to = input.exclusive_to,
                .sql_document_preimage = input.sql_document_preimage,
                .include_content_hashes = input.include_content_hashes,
                .include_range_proofs = input.include_range_proofs,
                .row_policy_principal_proof = input.row_policy_principal_proof,
                .row_policy_database = input.row_policy_database,
            }, deadline);
            errdefer owner.registry.close(token, scope) catch {};
            return encode(alloc, .{ .token = token });
        },
        .next => {
            const token = input.token orelse return error.InvalidRetainedReadToken;
            var page = try owner.next(alloc, scope, token, input.sequence, input.limit);
            defer page.deinit();
            // Cursor has advanced. Encoding failure poisons it instead of
            // allowing a retry to omit a page without an explicit failure.
            errdefer owner.registry.close(token, scope) catch {};
            if (format != .json) return native_page.encodeVersion(alloc, input.sequence, page.rows, page.after, if (format == .v2) .v2 else .v1);
            return encodeLegacyPage(alloc, input.sequence, page.rows, page.after);
        },
        .range_proofs => {
            const proofs = try owner.rangeProofs(alloc, scope, input.token orelse return error.InvalidRetainedReadToken);
            defer alloc.free(proofs);
            return encode(alloc, .{ .range_proofs = proofs });
        },
        .normalize => {
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const writes = try owner.normalize(arena.allocator(), scope, input.token orelse return error.InvalidRetainedReadToken, input.writes);
            return encode(alloc, .{ .writes = writes });
        },
        .close => {
            try owner.registry.close(input.token orelse return error.InvalidRetainedReadToken, scope);
            return encode(alloc, .{});
        },
    }
}

pub fn encodeLegacyPage(alloc: std.mem.Allocator, sequence: u64, input: []const reads.RelationalReadView.Row, after: ?[]const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var budget: @import("antfly_local_sources").sql_memory_budget = .{ .backing = arena.allocator(), .limit = max_response_bytes * 4 };
    const a = budget.allocator();
    const rows = try a.alloc(WireRow, input.len);
    for (input, rows) |row, *out| {
        out.* = .{ .id = row.id, .version = row.version, .schema_version = row.schema_version, .value = row.value, .expected_content_digest = row.expected_content_digest, .document = row.document, .sql_nulls = row.sql_nulls };
        if (row.typed_cells) |cells| {
            var object: std.json.ObjectMap = .empty;
            const flags = try a.alloc(bool, cells.count());
            var count: usize = 0;
            const source: @import("antfly_local_sources").sql_catalog.Row = .{ .id = row.id, .version = row.version, .value = .null, .typed_cells = cells };
            for (cells.layout.names) |name| {
                if (!try source.hasField(name)) continue;
                const datum = try source.cell(name);
                const value = if (datum.array) |array| try @import("antfly_local_sources").sql_array_wire.toJsonLeaky(a, array.*, .{}) else if (datum.numeric) |number| value: {
                    var context: @import("antfly_local_sources").sql_numeric_value.Context = .{ .alloc = a };
                    break :value std.json.Value{ .string = try @import("antfly_local_sources").sql_numeric_value.format(&context, number.*) };
                } else if (datum.range()) |range| value: {
                    try datum.validateTemporal();
                    const ranges = @import("antfly_local_sources").sql_range_value;
                    var context: ranges.Context = .{ .alloc = a };
                    break :value std.json.Value{ .string = try ranges.formatLeaky(&context, range.*) };
                } else if (datum.temporal()) |temporal| value: {
                    try datum.validateTemporal();
                    var text: [@import("antfly_local_sources").sql_temporal_text.output_capacity]u8 = undefined;
                    break :value std.json.Value{ .string = try a.dupe(u8, try @import("antfly_local_sources").sql_temporal_text.format(temporal.*, &text)) };
                } else if (datum.value == .float and !std.math.isFinite(datum.value.float))
                    std.json.Value{ .string = if (std.math.isNan(datum.value.float)) "NaN" else if (datum.value.float < 0) "-Infinity" else "Infinity" }
                else
                    datum.value;
                try object.put(a, name, value);
                flags[count] = datum.sql_null;
                count += 1;
            }
            out.value = .{ .object = object };
            out.sql_nulls = flags[0..count];
        }
    }
    return encode(alloc, .{ .sequence = sequence, .rows = rows, .after = after });
}

pub fn encode(alloc: std.mem.Allocator, response: Response) ![]u8 {
    // Bound actual writer growth, including JSON string escaping, before it
    // allocates. Returned bytes have no allocator callbacks and may be freed
    // directly through the backing allocator after this budget goes away.
    var budget: @import("antfly_local_sources").sql_memory_budget = .{ .backing = alloc, .limit = max_response_bytes };
    const bytes = std.json.Stringify.valueAlloc(budget.allocator(), response, .{}) catch |err| {
        if (budget.exhausted) return error.RetainedReadPageTooLarge;
        return err;
    };
    errdefer alloc.free(bytes);
    if (bytes.len > max_response_bytes) return error.RetainedReadPageTooLarge;
    return bytes;
}

/// A response owns all strings and exact number lexemes. SQL-null metadata is
/// mandatory and aligned; otherwise a JSON null could silently become SQL NULL.
pub fn decodePage(alloc: std.mem.Allocator, bytes: []const u8, sequence: u64, schema_version: u32, limit: u32) !reads.RelationalReadView.Page {
    if (native_page.isNative(bytes)) return native_page.decode(alloc, bytes, sequence, schema_version, limit);
    if (limit == 0 or limit > 4096 or bytes.len > max_response_bytes) return error.InvalidRetainedReadPage;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const response = try std.json.parseFromSliceLeaky(Response, arena.allocator(), bytes, .{ .allocate = .alloc_always, .parse_numbers = false });
    if (response.sequence != sequence or response.token != null or response.busy or response.rows.len > limit or response.writes.len != 0) return error.InvalidRetainedReadPage;
    for (response.rows) |row| {
        if (row.schema_version != schema_version or row.value != .object) return error.InvalidRetainedReadPage;
        const nulls = row.sql_nulls orelse return error.InvalidRetainedReadPage;
        if (nulls.len != row.value.object.count()) return error.InvalidRetainedReadPage;
        for (nulls, row.value.object.values()) |sql_null, value| if (sql_null and value != .null) return error.InvalidRetainedReadPage;
    }
    const rows = try arena.allocator().alloc(reads.RelationalReadView.Row, response.rows.len);
    for (response.rows, rows) |row, *out| out.* = .{ .id = row.id, .version = row.version, .schema_version = row.schema_version, .value = row.value, .expected_content_digest = row.expected_content_digest, .document = row.document, .sql_nulls = row.sql_nulls };
    return .{ .arena = arena, .rows = rows, .after = response.after };
}

pub const consumer_tests = consumerTests();
comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const root = @import("antfly_source_root");
    if (@hasDecl(root, "implementation_tests_only") and root.implementation_tests_only) return struct {};
    return struct {
        test "retained read RPC native pages preserve typed payloads and legacy negotiation" {
            const Probe = struct {
                fn run(a: std.mem.Allocator, version: @import("antfly_local_sources").sql_spill.ColumnarVersion) !void {
                    const src = @import("antfly_local_sources");
                    var arena = std.heap.ArenaAllocator.init(a);
                    defer arena.deinit();
                    const owned = arena.allocator();
                    var array = try src.sql_array_text.decode(a, .int64, "[0:2]={9007199254740993,NULL,-7}", .{});
                    defer array.deinit();
                    var context: src.sql_numeric_value.Context = .{ .alloc = a };
                    var number = try src.sql_numeric_value.parse(&context, "9007199254740993.1200");
                    defer number.deinit();
                    const layout = try src.sql_catalog.Row.TypedLayout.init(owned, &.{ "a", "n", "j", "z", "missing", "f", "_id" });
                    const values = [_]src.sql_catalog.Row.Cell{ .typedArray(&array.value), .typedNumeric(&number.value), .json(.null), .{}, .{}, .json(.{ .float = std.math.nan(f64) }), .json(.{ .string = "payload-id" }) };
                    const rows = [_]reads.RelationalReadView.Row{.{ .id = "row", .version = std.math.maxInt(u64), .schema_version = 3, .value = .null, .typed_cells = .{ .layout = layout, .values = &values, .presence = &.{ true, true, true, true, false, true, true } }, .expected_content_digest = @splat(7) }};
                    const bytes = try native_page.encodeVersion(a, 4, &rows, "next", version);
                    defer a.free(bytes);
                    var page = try decodePage(a, bytes, 4, 3, 1);
                    defer page.deinit();
                    const row: src.sql_catalog.Row = .{ .id = page.rows[0].id, .version = page.rows[0].version, .value = page.rows[0].value, .typed_cells = page.rows[0].typed_cells };
                    try std.testing.expectEqual(@as(u64, std.math.maxInt(u64)), row.version);
                    try std.testing.expectEqual(@as([32]u8, @splat(7)), page.rows[0].expected_content_digest.?);
                    try std.testing.expectEqual(@as(i64, 9007199254740993), (try row.cell("a")).array.?.flatCell(0).value.integer);
                    try std.testing.expectEqual(@as(i32, 0), (try row.cell("a")).array.?.dimensions[0].lower);
                    try std.testing.expect((try row.cell("a")).array.?.flatCell(1).sql_null);
                    try std.testing.expectEqual(version == .v2, (try row.cell("a")).array.?.stored != null);
                    const text = try src.sql_numeric_value.format(&context, (try row.cell("n")).numeric.?.*);
                    defer a.free(text);
                    try std.testing.expectEqualStrings("9007199254740993.1200", text);
                    try std.testing.expect(!(try row.cell("j")).sql_null);
                    try std.testing.expect((try row.cell("z")).sql_null);
                    try std.testing.expect(!try row.hasField("missing"));
                    try std.testing.expect(std.math.isNan((try row.cell("f")).value.float));
                    try std.testing.expectEqualStrings("row", (try page.rows[0].typed_cells.?.at(6)).value.string);
                    try std.testing.expectEqualStrings("next", page.after.?);
                    const legacy = try encodeLegacyPage(a, 4, &rows, "next");
                    defer a.free(legacy);
                    var legacy_page = try decodePage(a, legacy, 4, 3, 1);
                    defer legacy_page.deinit();
                    try std.testing.expect(legacy_page.rows[0].value.object.get("a").? == .object);
                    try std.testing.expect(!legacy_page.rows[0].sql_nulls.?[2]);
                    const legacy_float = try src.sql_document_row.declaredCell(a, .{ .name = "f", .path = "f", .type = .number, .element_type = .float64 }, .json(legacy_page.rows[0].value.object.get("f").?));
                    try std.testing.expect(std.math.isNan(legacy_float.value.float));
                    @memset(bytes, 0);
                    try std.testing.expectEqualStrings("row", row.id);
                    try std.testing.expectEqual(@as(i64, -7), (try row.cell("a")).array.?.flatCell(2).value.integer);
                }
            };
            for ([_]@import("antfly_local_sources").sql_spill.ColumnarVersion{ .v1, .v2 }) |version| try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{version});
        }

        test "retained read RPC native pages reject truncation identity and invalid framing" {
            const a = std.testing.allocator;
            const row: reads.RelationalReadView.Row = .{ .id = "a", .version = 2, .schema_version = 3, .value = .{ .object = .empty }, .sql_nulls = &.{} };
            for ([_]@import("antfly_local_sources").sql_spill.ColumnarVersion{ .v1, .v2 }) |version| {
                const bytes = try native_page.encodeVersion(a, 4, &.{row}, null, version);
                defer a.free(bytes);
                for (0..bytes.len) |length| {
                    if (native_page.decode(a, bytes[0..length], 4, 3, 1)) |value| {
                        var page = value;
                        page.deinit();
                        return error.AcceptedTruncatedNativePage;
                    } else |_| {}
                }
                try std.testing.expectError(error.InvalidRetainedReadPage, native_page.decode(a, bytes, 5, 3, 1));
                try std.testing.expectError(error.InvalidRetainedReadPage, native_page.decode(a, bytes, 4, 2, 1));
                try std.testing.expectError(error.InvalidRetainedReadPage, native_page.decode(a, bytes, 4, 3, 0));
            }
        }

        test "retained read RPC native pages tile both axes without losing presence" {
            const src = @import("antfly_local_sources");
            const a = std.testing.allocator;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const owned = arena.allocator();
            const names = try owned.alloc([]const u8, 1025);
            for (names, 0..) |*name, index| name.* = try std.fmt.allocPrint(owned, "c{d}", .{index});
            const layout = try src.sql_catalog.Row.TypedLayout.init(owned, names);
            const cells = try owned.alloc(src.sql_catalog.Row.Cell, names.len);
            for (cells, 0..) |*cell, index| cell.* = .json(.{ .integer = @intCast(index) });
            const rows = try owned.alloc(reads.RelationalReadView.Row, 257);
            for (rows, 0..) |*row, index| row.* = .{ .id = try std.fmt.allocPrint(owned, "r{d}", .{index}), .version = index, .schema_version = 3, .value = .null, .typed_cells = .{ .layout = layout, .values = cells } };
            for ([_]@import("antfly_local_sources").sql_spill.ColumnarVersion{ .v1, .v2 }) |version| {
                const bytes = try native_page.encodeVersion(a, 4, rows, "next", version);
                defer a.free(bytes);
                var page = try decodePage(a, bytes, 4, 3, 257);
                defer page.deinit();
                try std.testing.expectEqual(@as(usize, 257), page.rows.len);
                try std.testing.expectEqualStrings("r256", page.rows[256].id);
                try std.testing.expect(page.rows[256].typed_cells.?.columnar != null);
                try std.testing.expectEqual(@as(usize, 0), page.rows[256].typed_cells.?.values.len);
                const value = try page.rows[256].typed_cells.?.at(1024);
                try std.testing.expectEqual(@as(i64, 1024), value.value.integer);
                try std.testing.expect(!value.sql_null);
                try std.testing.expectEqualStrings("next", page.after.?);
            }
        }

        test "retained read RPC validates exact typed page identity and null metadata" {
            const bytes = "{\"sequence\":4,\"rows\":[{\"id\":\"a\",\"version\":7,\"schema_version\":3,\"value\":{\"n\":9007199254740993,\"j\":null,\"missing\":null},\"sql_nulls\":[false,false,true]}],\"after\":\"a\"}";
            var page = try decodePage(std.testing.allocator, bytes, 4, 3, 1);
            defer page.deinit();
            try std.testing.expectEqualStrings("9007199254740993", page.rows[0].value.object.get("n").?.number_string);
            try std.testing.expectEqualSlices(bool, &.{ false, false, true }, page.rows[0].sql_nulls.?);
            try std.testing.expectError(error.InvalidRetainedReadPage, decodePage(std.testing.allocator, bytes, 5, 3, 1));
            try std.testing.expectError(error.InvalidRetainedReadPage, decodePage(std.testing.allocator, bytes, 4, 2, 1));
            try std.testing.expectError(error.InvalidRetainedReadPage, decodePage(std.testing.allocator, "{\"rows\":[{\"id\":\"a\",\"version\":1,\"schema_version\":3,\"value\":{\"j\":null}}]}", 0, 3, 1));
            try std.testing.expectError(error.InvalidRetainedReadPage, decodePage(std.testing.allocator, "{\"rows\":[{\"id\":\"a\",\"version\":1,\"schema_version\":3,\"value\":{\"j\":2},\"sql_nulls\":[true]}]}", 0, 3, 1));
        }
    };
}
