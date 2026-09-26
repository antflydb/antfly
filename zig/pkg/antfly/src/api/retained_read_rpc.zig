// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Private service-authenticated retained read protocol. Never expose this
//! protocol through public SQL routing: the coordinator owns user authorization
//! and supplies the already-authorized row policy on open. Owner credentials
//! are reauthenticated for every operation, including close and paging.
const std = @import("std");
const owner_mod = @import("retained_read_owner.zig");
const registry = @import("../storage/retained_read_registry.zig");
const types = @import("../storage/db/types.zig");
const reads = @import("table_read_source.zig");
const time = @import("antfly_platform").time;

pub const max_request_bytes = 4 << 20;
pub const max_response_bytes = 32 << 20;
pub const max_lease_ms: u32 = 30_000;
pub const Request = struct {
    operation: enum { admit, capture, validate, snapshot, open, open_snapshot, next, normalize, range_proofs, close, cancel },
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

pub const Response = struct {
    range_proofs: []const @import("../storage/range_protection.zig").Proof = &.{},
    token: ?registry.Token = null,
    busy: bool = false,
    sequence: u64 = 0,
    rows: []const reads.RelationalReadView.Row = &.{},
    after: ?[]const u8 = null,
    writes: []const types.BatchWrite = &.{},
};

/// Scope is constructed by authenticated ingress, never deserialized from a
/// request. Body schema identity is only a requested epoch; native open checks
/// it against the pinned schema before a cursor can be created.
pub fn execute(alloc: std.mem.Allocator, owner: owner_mod.Owner, scope: registry.Scope, table: []const u8, input: Request) ![]u8 {
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
            // A bounded point probe establishes the owner read-index barrier
            // without retaining any mutation fence or row data.
            if (try owner.source.lookupGroupLocal(alloc, scope.group_id, table, "", .{}, .read_index)) |value| {
                var found = value;
                found.deinit(alloc);
            }
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
        .open => {
            const token = try owner.open(scope, input.token orelse return error.InvalidRetainedReadToken, input.connection, input.from, input.to, .{
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
            return encode(alloc, .{ .sequence = input.sequence, .rows = page.rows, .after = page.after });
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

fn encode(alloc: std.mem.Allocator, response: Response) ![]u8 {
    // Bound actual writer growth, including JSON string escaping, before it
    // allocates. Returned bytes have no allocator callbacks and may be freed
    // directly through the backing allocator after this budget goes away.
    var budget: @import("../sql/memory_budget.zig") = .{ .backing = alloc, .limit = max_response_bytes };
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
    return .{ .arena = arena, .rows = response.rows, .after = response.after };
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
