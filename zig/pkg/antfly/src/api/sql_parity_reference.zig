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

//! Test-only exact-source reference runner. This is not a production SQL oracle.
const std = @import("std");
const fixtures = @import("antfly_local_sources").sql_parity_fixtures;
const compiler = @import("antfly_local_sources").sql_compiler;
const wire = @import("antfly_metadata_openapi").types;
const httpx = @import("httpx");
const Json = std.json.Value;
const OrderedGroup = struct { rows: []const []const Json, sql_nulls: []const []const bool };

test "SQL PostgreSQL reference distinguishes JSON null and validates ordered peer prefixes" {
    const alloc = std.testing.allocator;
    try std.testing.expect(try rowMatchesWithNulls(alloc, &.{.{ .name = "j", .type = .json }}, &.{.null}, &.{false}, &.{.null}, &.{false}));
    try std.testing.expect(!try rowMatchesWithNulls(alloc, &.{.{ .name = "j", .type = .json }}, &.{.null}, &.{true}, &.{.null}, &.{false}));
    const groups = [_]OrderedGroup{
        .{ .rows = &.{&.{.{ .string = "first" }}}, .sql_nulls = &.{&.{false}} },
        .{ .rows = &.{ &.{.{ .string = "peer-a" }}, &.{.{ .string = "peer-b" }} }, .sql_nulls = &.{ &.{false}, &.{false} } },
        .{ .rows = &.{&.{.{ .string = "worse" }}}, .sql_nulls = &.{&.{false}} },
    };
    var response: wire.SQLResponse = .{ .columns = &.{.{ .name = "id", .type = .string }}, .rows = &.{ &.{.{ .string = "first" }}, &.{.{ .string = "peer-b" }} }, .rows_affected = 0, .sql_nulls = &.{ &.{false}, &.{false} }, .command_tag = "SELECT" };
    try std.testing.expect(try orderedPrefixMatches(alloc, response, &groups));
    for ([_][]const []const Json{
        &.{ &.{.{ .string = "first" }}, &.{.{ .string = "worse" }} },
        &.{ &.{.{ .string = "first" }}, &.{.{ .string = "first" }} },
        &.{ &.{.{ .string = "peer-a" }}, &.{.{ .string = "peer-b" }} },
    }) |rows| {
        response.rows = rows;
        try std.testing.expect(!try orderedPrefixMatches(alloc, response, &groups));
    }
}

const Reference = struct {
    reference: []const u8 = "",
    entries: []const struct { id: []const u8, columns: []const []const u8, rows: []const []const Json, sql_nulls: ?[]const []const bool = null, column_oids: ?[]const u32 = null, ordered_groups: ?[]const OrderedGroup = null },
};

fn orderedPrefixMatches(alloc: std.mem.Allocator, result: wire.SQLResponse, groups: []const OrderedGroup) !bool {
    var offset: usize = 0;
    for (groups) |group| {
        try std.testing.expectEqual(group.rows.len, group.sql_nulls.len);
        const count = @min(group.rows.len, result.rows.len - offset);
        const used = try alloc.alloc(bool, group.rows.len);
        defer alloc.free(used);
        @memset(used, false);
        for (result.rows[offset..][0..count], offset..) |row, index| {
            var found = false;
            for (group.rows, group.sql_nulls, 0..) |want, nulls, target| {
                if (used[target] or !try rowMatchesWithNulls(alloc, result.columns, row, if (result.sql_nulls) |flags| flags[index] else null, want, nulls)) continue;
                used[target] = true;
                found = true;
                break;
            }
            if (!found) return false;
        }
        offset += count;
        if (offset == result.rows.len) return true;
    }
    return false;
}

fn postgresTypeMatches(kind: wire.SQLColumnType, oid: u32) bool {
    return switch (oid) {
        16 => kind == .boolean,
        20, 21, 23 => kind == .integer,
        700, 701, 1700 => kind == .number,
        25, 1042, 1043 => kind == .string,
        114, 3802 => kind == .json,
        2950 => kind == .uuid,
        1114, 1184 => kind == .datetime,
        else => false, // Arrays and other types need their own typed contract.
    };
}

fn equivalent(a: Json, b: Json) bool {
    if (a == .float and b == .float) return std.math.approxEqRel(f64, a.float, b.float, 1e-12) or a.float == b.float;
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .null => true,
        .bool => a.bool == b.bool,
        .integer => a.integer == b.integer,
        .float => a.float == b.float,
        .string => std.mem.eql(u8, a.string, b.string),
        .number_string => std.mem.eql(u8, a.number_string, b.number_string),
        .array => blk: {
            if (a.array.items.len != b.array.items.len) break :blk false;
            for (a.array.items, b.array.items) |left, right| if (!equivalent(left, right)) break :blk false;
            break :blk true;
        },
        .object => blk: {
            if (a.object.count() != b.object.count()) break :blk false;
            var it = a.object.iterator();
            while (it.next()) |entry| if (!equivalent(entry.value_ptr.*, b.object.get(entry.key_ptr.*) orelse break :blk false)) break :blk false;
            break :blk true;
        },
    };
}

fn rowMatches(alloc: std.mem.Allocator, columns: []const wire.SQLColumn, actual: []const Json, nulls: ?[]const bool, expected: []const Json) !bool {
    return rowMatchesWithNulls(alloc, columns, actual, nulls, expected, null);
}

fn rowMatchesWithNulls(alloc: std.mem.Allocator, columns: []const wire.SQLColumn, actual: []const Json, nulls: ?[]const bool, expected: []const Json, expected_nulls: ?[]const bool) !bool {
    if (actual.len != expected.len or actual.len != columns.len) return false;
    if (nulls) |flags| if (flags.len != actual.len) return false;
    if (expected_nulls) |flags| if (flags.len != actual.len) return false;
    for (columns, actual, expected, 0..) |column, cell, want, index| {
        const sql_null = if (expected_nulls) |flags| flags[index] else want == .null;
        if (nulls) |flags| if (flags[index] != sql_null) return false;
        if (want == .null) {
            if (cell != .null) return false;
            continue;
        }
        var value = cell;
        switch (column.type) {
            .integer => if (cell == .string) {
                value = .{ .integer = try std.fmt.parseInt(i64, cell.string, 10) };
            },
            .number => {
                if (cell == .string) value = .{ .float = try std.fmt.parseFloat(f64, cell.string) };
                // JSON has one numeric domain: a number column's integral
                // float can serialize without a decimal point. Normalize
                // only an exactly representable integer, never JSON cells or
                // integer wire columns, and never round away bigint bits.
                if (cell == .integer) {
                    const number: f64 = @floatFromInt(cell.integer);
                    if (@as(i128, @intFromFloat(number)) != cell.integer) return false;
                    value = .{ .float = number };
                }
            },
            .boolean => if (want == .integer) {
                if (cell != .bool or cell.bool != (want.integer != 0)) return false;
                continue;
            },
            .json => if (want == .string and expected_nulls == null) {
                const decoded = try std.json.parseFromSlice(Json, alloc, want.string, .{});
                defer decoded.deinit();
                if (!equivalent(cell, decoded.value)) return false;
                continue;
            },
            else => {},
        }
        if (value == .float and want == .integer) {
            // A floating-point oracle comparison must never hide lost bigint
            // precision. Integer wire columns are parsed exactly above.
            if (want.integer < -9007199254740992 or want.integer > 9007199254740992) return false;
            if (value.float != @as(f64, @floatFromInt(want.integer))) return false;
        } else if (!equivalent(value, want)) return false;
    }
    return true;
}

pub fn runNumberWireContracts(alloc: std.mem.Allocator) !void {
    const number = [_]wire.SQLColumn{.{ .name = "n", .type = .number }};
    const integer = [_]wire.SQLColumn{.{ .name = "n", .type = .integer }};
    const json = [_]wire.SQLColumn{.{ .name = "n", .type = .json }};
    try std.testing.expect(try rowMatches(alloc, &number, &.{.{ .integer = 33 }}, &.{false}, &.{.{ .float = 33 }}));
    try std.testing.expect(!try rowMatches(alloc, &number, &.{.{ .integer = 9007199254740993 }}, &.{false}, &.{.{ .float = 9007199254740992 }}));
    try std.testing.expect(!try rowMatches(alloc, &integer, &.{.{ .string = "9007199254740993" }}, &.{false}, &.{.{ .integer = 9007199254740992 }}));
    try std.testing.expect(!try rowMatches(alloc, &json, &.{.{ .integer = 33 }}, &.{false}, &.{.{ .float = 33 }}));
}

fn execute(alloc: std.mem.Allocator, handler: anytype, case: *const fixtures.Corpus.Case) !std.json.Parsed(wire.SQLResponse) {
    var parameter_arena = std.heap.ArenaAllocator.init(alloc);
    defer parameter_arena.deinit();
    const parameters = try fixtures.Corpus.logicalParameters(parameter_arena.allocator(), case);
    const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = case.sql, .parameters = parameters }, .{});
    defer alloc.free(body);
    var request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/db/v1/sql");
    defer request.deinit();
    request.body = body;
    var context = httpx.Context.init(alloc, std.testing.io, &request);
    defer context.deinit();
    var response = try handler.executeSQL(&context);
    defer response.deinit();
    if (response.status.code != 200) {
        std.debug.print("REFERENCE {s} rejected {s}\n", .{ case.id, response.body orelse "" });
        return error.NativeParityAdmissionFailed;
    }
    // The returned parse owns strings; no response/request buffer escapes.
    return std.json.parseFromSlice(wire.SQLResponse, alloc, response.body.?, .{ .allocate = .alloc_always });
}

/// Explicit native contracts where SQLite is not a compatible oracle. Verify
/// complete multisets plus the declared ordering, allowing unspecified peer ties.
pub fn runNativeContracts(alloc: std.mem.Allocator, handler: anytype, ids: []const []const u8) !void {
    const contracts = [_]struct { id: []const u8, names: []const []const u8, rows: []const []const Json, order_column: usize }{
        .{ .id = "sql-0208", .names = &.{ "id", "status" }, .order_column = 1, .rows = &.{
            &.{ .{ .integer = 9 }, .null },
            &.{ .{ .integer = 5 }, .{ .string = "pending" } },
            &.{ .{ .integer = 2 }, .{ .string = "open" } },
            &.{ .{ .integer = 4 }, .{ .string = "open" } },
            &.{ .{ .integer = 7 }, .{ .string = "open" } },
        } },
        .{ .id = "sql-1369", .names = &.{ "tenant_id", "id", "row_number" }, .order_column = 2, .rows = &.{
            &.{ .{ .integer = 1 }, .{ .integer = 2 }, .{ .integer = 1 } },
            &.{ .{ .integer = 2 }, .{ .integer = 4 }, .{ .integer = 1 } },
            &.{ .{ .integer = 2 }, .{ .integer = 7 }, .{ .integer = 2 } },
        } },
    };
    var corpus = try fixtures.Corpus.init(alloc);
    defer corpus.deinit();
    try std.testing.expectEqual(contracts.len, ids.len);
    for (contracts, ids) |contract, id| {
        try std.testing.expectEqualStrings(contract.id, id);
        const parsed = try execute(alloc, handler, try corpus.get(id));
        defer parsed.deinit();
        const result = parsed.value;
        try std.testing.expectEqualStrings("SELECT", result.command_tag);
        try std.testing.expectEqual(@as(i64, 0), result.rows_affected);
        try std.testing.expectEqual(contract.rows.len, result.rows.len);
        try std.testing.expectEqual(contract.names.len, result.columns.len);
        try std.testing.expectEqual(result.rows.len, result.sql_nulls.?.len);
        for (result.columns, contract.names) |column, name| try std.testing.expectEqualStrings(name, column.name);
        var used: [5]bool = @splat(false);
        for (result.rows, 0..) |row, index| {
            var found = false;
            for (contract.rows, 0..) |expected, target| {
                if (used[target] or !try rowMatches(alloc, result.columns, row, result.sql_nulls.?[index], expected)) continue;
                used[target] = true;
                found = true;
                break;
            }
            try std.testing.expect(found);
            if (index == 0) continue;
            const previous = result.rows[index - 1][contract.order_column];
            const current = row[contract.order_column];
            if (contract.order_column == 1) {
                // PostgreSQL's descending default puts SQL NULL first.
                if (previous == .null) continue;
                try std.testing.expect(current != .null);
                try std.testing.expect(std.mem.order(u8, previous.string, current.string) != .lt);
            } else try std.testing.expect(try std.fmt.parseInt(i64, previous.string, 10) <= try std.fmt.parseInt(i64, current.string, 10));
        }
    }
}

/// Each exact point mutation starts from the same two-row native fixture.
/// Check API postimages/preimages and direct storage read-back independently.
pub fn runPointMutations(alloc: std.mem.Allocator, handler: anytype, db: anytype, case_ids: []const []const u8) !void {
    const contracts = [_]struct { id: []const u8, names: []const []const u8, row: []const Json, status: ?[]const u8 }{
        .{ .id = "sql-1497", .names = &.{ "id", "status", "status_key" }, .row = &.{ .{ .string = "u1" }, .{ .string = "processing" }, .{ .string = "processing" } }, .status = "processing" },
        .{ .id = "sql-1498", .names = &.{"status"}, .row = &.{.{ .string = "open" }}, .status = "open" },
        .{ .id = "sql-1504", .names = &.{ "id", "status", "quantity", "status_key" }, .row = &.{ .{ .string = "u1" }, .{ .string = "processing" }, .{ .integer = 2 }, .{ .string = "processing" } }, .status = "processing" },
        .{ .id = "sql-1505", .names = &.{ "id", "status", "quantity" }, .row = &.{ .{ .string = "u1" }, .{ .string = "processing" }, .{ .integer = 2 } }, .status = "processing" },
        .{ .id = "sql-1506", .names = &.{ "id", "returned_status" }, .row = &.{ .{ .string = "u1" }, .{ .string = "processing" } }, .status = "processing" },
        .{ .id = "sql-1507", .names = &.{ "id", "returned_status" }, .row = &.{ .{ .string = "u1" }, .{ .string = "processing" } }, .status = "processing" },
        .{ .id = "sql-1513", .names = &.{ "id", "status_key" }, .row = &.{ .{ .string = "u1" }, .{ .string = "open" } }, .status = null },
        .{ .id = "sql-1518", .names = &.{ "id", "status_key" }, .row = &.{ .{ .string = "u1" }, .{ .string = "open" } }, .status = null },
        .{ .id = "sql-1519", .names = &.{ "id", "status_key" }, .row = &.{ .{ .string = "u1" }, .{ .string = "open" } }, .status = null },
        .{ .id = "sql-1520", .names = &.{ "id", "status", "quantity" }, .row = &.{ .{ .string = "u1" }, .{ .string = "OPEN" }, .{ .integer = 2 } }, .status = null },
        .{ .id = "sql-1521", .names = &.{ "id", "status", "quantity" }, .row = &.{ .{ .string = "u1" }, .{ .string = "OPEN" }, .{ .integer = 2 } }, .status = null },
        .{ .id = "sql-1522", .names = &.{ "id", "deleted_status" }, .row = &.{ .{ .string = "u1" }, .{ .string = "OPEN" } }, .status = null },
    };
    var corpus = try fixtures.Corpus.init(alloc);
    defer corpus.deinit();
    try std.testing.expectEqual(contracts.len, case_ids.len);
    for (contracts, case_ids, 0..) |contract, id, index| {
        try std.testing.expectEqualStrings(contract.id, id);
        try db.batch(.{ .writes = &.{
            .{ .key = "a", .value = "{\"id\":\"u1\",\"status\":\"OPEN\",\"quantity\":2}" },
            .{ .key = "b", .value = "{\"id\":\"u2\",\"status\":\"closed\",\"quantity\":7}" },
        }, .timestamp_ns = @as(u64, @intCast(index + 100)) });
        const response = try execute(alloc, handler, try corpus.get(id));
        defer response.deinit();
        const result = response.value;
        try std.testing.expectEqualStrings(if (contract.status != null) "UPDATE" else "DELETE", result.command_tag);
        try std.testing.expectEqual(@as(i64, 1), result.rows_affected);
        try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        try std.testing.expectEqual(contract.names.len, result.columns.len);
        try std.testing.expect(result.sql_nulls != null);
        try std.testing.expectEqual(@as(usize, 1), result.sql_nulls.?.len);
        for (result.columns, contract.names) |column, name| try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expect(try rowMatches(alloc, result.columns, result.rows[0], result.sql_nulls.?[0], contract.row));
        var changed = try db.lookup(alloc, "a", .{});
        defer if (changed) |*stored| stored.deinit(alloc);
        if (contract.status) |status| {
            try std.testing.expect(changed != null);
            const stored = changed.?;
            const value = try std.json.parseFromSlice(Json, alloc, stored.json, .{});
            defer value.deinit();
            try std.testing.expectEqual(@as(usize, 3), value.value.object.count());
            try std.testing.expectEqualStrings("u1", value.value.object.get("id").?.string);
            try std.testing.expectEqualStrings(status, value.value.object.get("status").?.string);
            try std.testing.expectEqual(@as(i64, 2), value.value.object.get("quantity").?.integer);
        } else try std.testing.expect(changed == null);
        var untouched = (try db.lookup(alloc, "b", .{})).?;
        defer untouched.deinit(alloc);
        const other = try std.json.parseFromSlice(Json, alloc, untouched.json, .{});
        defer other.deinit();
        try std.testing.expectEqual(@as(usize, 3), other.value.object.count());
        try std.testing.expectEqualStrings("u2", other.value.object.get("id").?.string);
        try std.testing.expectEqualStrings("closed", other.value.object.get("status").?.string);
        try std.testing.expectEqual(@as(i64, 7), other.value.object.get("quantity").?.integer);
    }
    // RETURNING evaluation is part of admission, not post-commit work. A
    // projection error must leave both primary bytes and version untouched.
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"id\":\"u1\",\"status\":\"OPEN\",\"quantity\":2}" }}, .timestamp_ns = 1000 });
    var before = (try db.lookup(alloc, "a", .{ .include_primary_digest = true })).?;
    defer before.deinit(alloc);
    var request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/db/v1/sql");
    defer request.deinit();
    request.body = "{\"statement\":\"UPDATE usage_records SET status='processing' WHERE id='u1' RETURNING *,1/0 AS invalid\"}";
    var context = httpx.Context.init(alloc, std.testing.io, &request);
    defer context.deinit();
    var rejected = try handler.executeSQL(&context);
    defer rejected.deinit();
    try std.testing.expectEqual(@as(u16, 400), rejected.status.code);
    const diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, alloc, rejected.body.?, .{});
    defer diagnostic.deinit();
    try std.testing.expectEqualStrings("22012", diagnostic.value.code);
    var after = (try db.lookup(alloc, "a", .{ .include_primary_digest = true })).?;
    defer after.deinit(alloc);
    try std.testing.expect(before.version != null and before.expected_content_digest != null);
    try std.testing.expectEqual(before.version, after.version);
    try std.testing.expectEqual(before.expected_content_digest, after.expected_content_digest);
    try std.testing.expectEqualStrings(before.json, after.json);
}

pub const MutationReference = struct {
    schema: Json,
    seeds: []const struct { key: []const u8, value: Json },
    storage_columns: []const []const u8,
    entries: []const struct { id: []const u8, columns: []const []const u8, rows: []const []const Json, affected: i64, final: []const []const Json },
};

pub const PostgresMutationReference = struct {
    pub const Seed = struct { key: []const u8, value: Json };
    pub const Table = struct { name: []const u8, schema: Json, rows: []const Seed };
    pub const Rows = struct {
        columns: []const []const u8,
        column_oids: []const u32,
        rows: []const []const Json,
        sql_nulls: []const []const bool,
    };
    profile: struct { schema: Json, rows: []const Seed, additional_tables: []const Table },
    entries: []const struct {
        id: []const u8,
        command_tag: []const u8,
        affected: i64,
        columns: []const []const u8,
        column_oids: []const u32,
        rows: []const []const Json,
        sql_nulls: []const []const bool,
        final_tables: std.json.ArrayHashMap(Rows),
    },
};

/// Exact-source native execution plus complete, independent storage read-back
/// of every fixture table. Selection is explicit and fail-closed, never a
/// discovery skip. Constraint-owner activation is a separate fixture contract;
/// callers must not credit key-changing cases using unconstrained native DBs.
pub fn runPostgresMutations(alloc: std.mem.Allocator, handler: anytype, tables: anytype, reference: PostgresMutationReference, ids: []const []const u8) !void {
    var corpus = try fixtures.Corpus.init(alloc);
    defer corpus.deinit();
    const BatchWrite = @import("antfly_local_sources").storage_db_types.BatchWrite;
    try std.testing.expectEqual(reference.profile.additional_tables.len + 1, tables.len);
    for (ids, 0..) |id, ordinal| {
        const expected = for (reference.entries) |entry| {
            if (std.mem.eql(u8, id, entry.id)) break entry;
        } else return error.MissingPostgresMutationReference;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        try std.testing.expectEqual(tables.len, expected.final_tables.map.count());
        for (tables, 0..) |table, table_index| {
            for (tables[0..table_index]) |prior| try std.testing.expect(!std.mem.eql(u8, table.name, prior.name));
            const seeds = if (std.mem.eql(u8, table.name, "usage_records")) reference.profile.rows else for (reference.profile.additional_tables) |profile| {
                if (std.mem.eql(u8, profile.name, table.name)) break profile.rows;
            } else return error.MissingPostgresMutationTable;
            var previous = try table.db.scan(a, "", "", .{ .include_documents = true, .limit = 4097 });
            defer previous.deinit(a);
            try std.testing.expect(previous.documents.len <= 4096);
            const deletes = try a.alloc([]const u8, previous.documents.len);
            for (previous.documents, deletes) |document, *key| key.* = document.id;
            if (deletes.len != 0) try table.db.batch(.{ .deletes = deletes, .timestamp_ns = @as(u64, @intCast(ordinal * 2 + 1000)) });
            const writes = try a.alloc(BatchWrite, seeds.len);
            for (seeds, writes) |seed, *write| write.* = .{ .key = seed.key, .value = try std.json.Stringify.valueAlloc(a, seed.value, .{}) };
            try table.db.batch(.{ .writes = writes, .timestamp_ns = @as(u64, @intCast(ordinal * 2 + 1001)) });
        }
        const response = try execute(a, handler, try corpus.get(id));
        defer response.deinit();
        const result = response.value;
        try std.testing.expectEqualStrings(expected.command_tag, result.command_tag);
        try std.testing.expectEqual(expected.affected, result.rows_affected);
        try std.testing.expectEqual(expected.columns.len, result.columns.len);
        try std.testing.expectEqual(expected.columns.len, expected.column_oids.len);
        for (result.columns, expected.columns, expected.column_oids) |column, name, oid| {
            try std.testing.expectEqualStrings(name, column.name);
            try std.testing.expect(postgresTypeMatches(column.type, oid));
        }
        try std.testing.expectEqual(expected.rows.len, result.rows.len);
        try std.testing.expectEqual(expected.rows.len, expected.sql_nulls.len);
        const used = try a.alloc(bool, expected.rows.len);
        @memset(used, false);
        for (result.rows, 0..) |row, index| {
            const flags = result.sql_nulls orelse return error.MissingSqlNullProvenance;
            try std.testing.expectEqual(result.rows.len, flags.len);
            var matched = false;
            for (expected.rows, expected.sql_nulls, 0..) |want, nulls, target| {
                if (used[target] or !try rowMatchesWithNulls(a, result.columns, row, flags[index], want, nulls)) continue;
                used[target] = true;
                matched = true;
                break;
            }
            try std.testing.expect(matched);
        }
        for (tables) |table| {
            const final = expected.final_tables.map.get(table.name) orelse return error.MissingPostgresMutationTable;
            try std.testing.expectEqual(final.columns.len, final.column_oids.len);
            try std.testing.expectEqual(final.rows.len, final.sql_nulls.len);
            var stored = try table.db.scan(a, "", "", .{ .include_documents = true, .limit = 4097 });
            defer stored.deinit(a);
            try std.testing.expectEqual(final.rows.len, stored.documents.len);
            try std.testing.expectEqual(stored.documents.len, stored.hashes.len);
            const storage_used = try a.alloc(bool, final.rows.len);
            @memset(storage_used, false);
            for (stored.documents, stored.hashes) |document, hash| {
                try std.testing.expectEqualStrings(document.id, hash.id);
                const parsed = try std.json.parseFromSlice(Json, a, document.json, .{});
                defer parsed.deinit();
                try std.testing.expect(parsed.value == .object);
                for (parsed.value.object.keys()) |key| {
                    var known = false;
                    for (final.columns) |column| if (std.mem.eql(u8, key, column)) {
                        known = true;
                        break;
                    };
                    try std.testing.expect(known);
                }
                var matched = false;
                for (final.rows, final.sql_nulls, 0..) |want, nulls, target| {
                    if (storage_used[target]) continue;
                    try std.testing.expectEqual(final.columns.len, want.len);
                    try std.testing.expectEqual(want.len, nulls.len);
                    var equal = true;
                    for (final.columns, want, nulls) |column, cell, sql_null| {
                        const value = parsed.value.object.get(column) orelse .null;
                        var json_null = false;
                        for (hash.json_null_fields) |name| if (std.mem.eql(u8, name, column)) {
                            json_null = true;
                            break;
                        };
                        equal = equal and equivalent(value, cell) and sql_null == (value == .null and !json_null);
                    }
                    if (!equal) continue;
                    storage_used[target] = true;
                    matched = true;
                    break;
                }
                if (!matched) std.debug.print("POSTGRES MUTATION STORAGE {s}/{s}: {s}\n", .{ id, table.name, document.json });
                try std.testing.expect(matched);
            }
        }
    }
}

/// Complete physical read-back is independent of SQL projection/binding. Each
/// case has a bounded arena and resets every physical key, including generated
/// INSERT identities, so outcomes cannot depend on the previous case.
pub fn runMutationReference(alloc: std.mem.Allocator, handler: anytype, db: anytype, reference: MutationReference) !void {
    var corpus = try fixtures.Corpus.init(alloc);
    defer corpus.deinit();
    const batch_write = @import("antfly_local_sources").storage_db_types.BatchWrite;
    for (reference.entries, 0..) |expected, ordinal| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var previous = try db.scan(a, "", "", .{ .include_documents = true, .limit = 4097 });
        defer previous.deinit(a);
        try std.testing.expect(previous.documents.len <= 4096);
        const deletes = try a.alloc([]const u8, previous.documents.len);
        for (previous.documents, deletes) |document, *key| key.* = document.id;
        if (deletes.len != 0) try db.batch(.{ .deletes = deletes, .timestamp_ns = @as(u64, @intCast(ordinal * 2 + 100)) });
        const writes = try a.alloc(batch_write, reference.seeds.len);
        for (reference.seeds, writes) |seed, *write| write.* = .{ .key = seed.key, .value = try std.json.Stringify.valueAlloc(a, seed.value, .{}) };
        try db.batch(.{ .writes = writes, .timestamp_ns = @as(u64, @intCast(ordinal * 2 + 101)) });
        const case = try corpus.get(expected.id);
        const response = try execute(a, handler, case);
        defer response.deinit();
        const result = response.value;
        try std.testing.expectEqualStrings(if (std.mem.startsWith(u8, case.family, "insert")) "INSERT" else if (std.mem.startsWith(u8, case.family, "update")) "UPDATE" else "DELETE", result.command_tag);
        try std.testing.expectEqual(expected.affected, result.rows_affected);
        try std.testing.expectEqual(expected.columns.len, result.columns.len);
        for (result.columns, expected.columns) |column, name| try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expectEqual(expected.rows.len, result.rows.len);
        const used = try a.alloc(bool, expected.rows.len);
        @memset(used, false);
        for (result.rows, 0..) |row, index| {
            try std.testing.expect(result.sql_nulls != null and result.sql_nulls.?.len == result.rows.len);
            var matched = false;
            for (expected.rows, 0..) |want, target| {
                if (used[target] or !try rowMatches(a, result.columns, row, result.sql_nulls.?[index], want)) continue;
                used[target] = true;
                matched = true;
                break;
            }
            try std.testing.expect(matched);
        }
        var stored = try db.scan(a, "", "", .{ .include_documents = true, .limit = 4097 });
        defer stored.deinit(a);
        try std.testing.expectEqual(expected.final.len, stored.documents.len);
        const storage_used = try a.alloc(bool, expected.final.len);
        @memset(storage_used, false);
        for (stored.documents) |document| {
            const value = try std.json.parseFromSlice(Json, a, document.json, .{});
            defer value.deinit();
            try std.testing.expect(value.value == .object);
            for (value.value.object.keys()) |key| {
                var known = false;
                for (reference.storage_columns) |column| if (std.mem.eql(u8, key, column)) {
                    known = true;
                    break;
                };
                try std.testing.expect(known);
            }
            var matched = false;
            for (expected.final, 0..) |want, target| {
                if (storage_used[target]) continue;
                var equal = true;
                for (reference.storage_columns, want) |column, cell| {
                    const actual = value.value.object.get(column) orelse .null;
                    if (std.mem.eql(u8, column, "metadata") or std.mem.eql(u8, column, "tags")) {
                        if (cell == .string) {
                            const decoded = try std.json.parseFromSlice(Json, a, cell.string, .{});
                            defer decoded.deinit();
                            equal = equal and equivalent(actual, decoded.value);
                        } else equal = equal and equivalent(actual, cell);
                    } else if (std.mem.eql(u8, column, "enabled") and cell == .integer) {
                        equal = equal and actual == .bool and actual.bool == (cell.integer != 0);
                    } else equal = equal and equivalent(actual, cell);
                }
                if (!equal) continue;
                storage_used[target] = true;
                matched = true;
                break;
            }
            if (!matched) std.debug.print("MUTATION STORAGE {s}: {s}\n", .{ expected.id, document.json });
            try std.testing.expect(matched);
        }
    }
}

pub const DocumentReference = struct {
    reference: []const u8,
    seeds: []const struct { key: []const u8, value: Json },
    entries: []const struct {
        id: []const u8,
        schema: Json,
        native_schema: ?Json = null,
        columns: []const []const u8,
        column_oids: []const u32,
        rows: []const []const Json,
        sql_nulls: ?[]const []const bool = null,
        affected: i64,
        final: []const struct { key: []const u8, value: Json },
    },
};

pub fn runDocuments(alloc: std.mem.Allocator, handler: anytype, db: anytype, source: anytype, reference: DocumentReference) !void {
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", reference.reference);
    var corpus = try fixtures.Corpus.init(alloc);
    defer corpus.deinit();
    const discovery = try std.testing.environ.containsUnempty(alloc, "ANTFLY_SQL_DOCUMENT_DISCOVERY");
    var passed: usize = 0;
    const original_schema = source.schema;
    const original_record_schema = source.records[0].schema_json;
    defer {
        source.schema = original_schema;
        source.records[0].schema_json = original_record_schema;
    }
    for (reference.entries, 0..) |expected, ordinal| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        var previous = try db.scan(a, "", "", .{ .include_documents = true, .limit = 4097 });
        defer previous.deinit(a);
        try std.testing.expect(previous.documents.len <= 4096);
        const deletes = try a.alloc([]const u8, previous.documents.len);
        for (previous.documents, deletes) |document, *key| key.* = document.id;
        if (deletes.len != 0) try db.batch(.{ .deletes = deletes, .timestamp_ns = @as(u64, @intCast(ordinal * 2 + 1000)) });
        // Catalog epochs cannot reuse a version for a different layout. Keep
        // the source schema as provenance, but give each isolated fixture
        // publication a fresh real version after deleting its predecessor.
        var schema = try std.json.parseFromSliceLeaky(Json, a, try std.json.Stringify.valueAlloc(a, expected.native_schema orelse expected.schema, .{}), .{});
        try schema.object.put(a, "version", .{ .integer = @intCast(ordinal + 1) });
        source.schema = try std.json.Stringify.valueAlloc(a, schema, .{});
        source.records[0].schema_json = source.schema;
        db.setSchemaJson(a, source.schema) catch |err| {
            if (discovery and err == error.InvalidSchemaUpdateRequest) {
                std.debug.print("DOCUMENT PROFILE {s}: source schema requires a current native owner profile\n", .{expected.id});
                continue;
            }
            return err;
        };
        const writes = try a.alloc(@import("antfly_local_sources").storage_db_types.BatchWrite, reference.seeds.len);
        for (reference.seeds, writes) |seed, *write| write.* = .{ .key = seed.key, .value = try std.json.Stringify.valueAlloc(a, seed.value, .{}) };
        try db.batch(.{ .writes = writes, .timestamp_ns = @as(u64, @intCast(ordinal * 2 + 1001)) });
        const case = try corpus.get(expected.id);
        const body = try std.json.Stringify.valueAlloc(a, .{ .statement = case.sql, .parameters = try fixtures.Corpus.logicalParameters(a, case) }, .{});
        var request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
        defer request.deinit();
        request.body = body;
        var context = httpx.Context.init(a, std.testing.io, &request);
        defer context.deinit();
        var response = try handler.executeSQL(&context);
        defer response.deinit();
        if (response.status.code != 200 and discovery) {
            std.debug.print("DOCUMENT DISCOVERY {s}: {s}\n", .{ case.id, response.body orelse "" });
            continue;
        }
        if (response.status.code != 200) std.debug.print("DOCUMENT {s}: {s}\n", .{ case.id, response.body orelse "" });
        try std.testing.expectEqual(@as(u16, 200), response.status.code);
        const parsed = try std.json.parseFromSlice(wire.SQLResponse, a, response.body.?, .{});
        defer parsed.deinit();
        const result = parsed.value;
        try std.testing.expectEqualStrings(if (std.mem.startsWith(u8, case.sql, "INSERT")) "INSERT" else if (std.mem.startsWith(u8, case.sql, "UPDATE")) "UPDATE" else "DELETE", result.command_tag);
        try std.testing.expectEqual(expected.affected, result.rows_affected);
        try std.testing.expectEqual(expected.columns.len, result.columns.len);
        try std.testing.expectEqual(expected.column_oids.len, result.columns.len);
        for (result.columns, expected.column_oids) |column, oid| try std.testing.expect(postgresTypeMatches(column.type, oid));
        for (result.columns, expected.columns) |column, name| try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expectEqual(expected.rows.len, result.rows.len);
        if (result.rows.len != 0) {
            try std.testing.expect(result.sql_nulls != null);
            try std.testing.expectEqual(result.rows.len, result.sql_nulls.?.len);
        }
        if (expected.sql_nulls) |nulls| try std.testing.expectEqual(expected.rows.len, nulls.len);
        const used = try a.alloc(bool, expected.rows.len);
        @memset(used, false);
        for (result.rows, 0..) |row, index| {
            var matched = false;
            for (expected.rows, 0..) |want, target| {
                if (used[target] or !try rowMatchesWithNulls(a, result.columns, row, result.sql_nulls.?[index], want, if (expected.sql_nulls) |nulls| nulls[target] else null)) continue;
                used[target] = true;
                matched = true;
                break;
            }
            try std.testing.expect(matched);
        }
        var stored = try db.scan(a, "", "", .{ .include_documents = true, .limit = 4097 });
        defer stored.deinit(a);
        try std.testing.expectEqual(expected.final.len, stored.documents.len);
        for (expected.final) |want| {
            const document = for (stored.documents) |document| {
                if (std.mem.eql(u8, document.id, want.key)) break document;
            } else return error.TestExpectedDocumentMissing;
            const value = try std.json.parseFromSlice(Json, a, document.json, .{});
            defer value.deinit();
            if (!equivalent(value.value, want.value)) std.debug.print("DOCUMENT STATE {s}: {s}\n", .{ case.id, document.json });
            try std.testing.expect(equivalent(value.value, want.value));
        }
        passed += 1;
        if (discovery) std.debug.print("DOCUMENT PASS {s}\n", .{case.id});
    }
    if (!discovery) try std.testing.expectEqual(reference.entries.len, passed);
    std.debug.print("SQL document campaign native outcomes {d}/{d}; discovery alone grants no disposition credit\n", .{ passed, reference.entries.len });
}

/// Exact-source negative contracts are strict regardless of discovery settings.
pub fn expectRejection(alloc: std.mem.Allocator, handler: anytype, case_id: []const u8, sqlstate: []const u8) !void {
    var corpus = try fixtures.Corpus.init(alloc);
    defer corpus.deinit();
    const case = try corpus.get(case_id);
    var parameters = std.heap.ArenaAllocator.init(alloc);
    defer parameters.deinit();
    const body = try std.json.Stringify.valueAlloc(alloc, .{ .statement = case.sql, .parameters = try fixtures.Corpus.logicalParameters(parameters.allocator(), case) }, .{});
    defer alloc.free(body);
    var request = try httpx.Request.init(alloc, .POST, "http://127.0.0.1/db/v1/sql");
    defer request.deinit();
    request.body = body;
    var context = httpx.Context.init(alloc, std.testing.io, &request);
    defer context.deinit();
    var response = try handler.executeSQL(&context);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 400), response.status.code);
    const diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, alloc, response.body.?, .{});
    defer diagnostic.deinit();
    try std.testing.expectEqualStrings(sqlstate, diagnostic.value.code);
}

pub fn run(alloc: std.mem.Allocator, handler: anytype, case_ids: []const []const u8) !void {
    return runReference(alloc, handler, case_ids, fixtures.read_reference);
}

/// Supplemental PostgreSQL contracts, not original-corpus disposition credit.
pub fn runArrayExpressions(alloc: std.mem.Allocator, handler: anytype) !void {
    const reference = try std.json.parseFromSlice(struct {
        reference: []const u8,
        entries: []const struct { sql: []const u8, value: Json },
    }, alloc, fixtures.array_expression_reference, .{});
    defer reference.deinit();
    try std.testing.expectEqual(@as(usize, 164), reference.value.entries.len);
    for (reference.value.entries) |entry| {
        const sql = try std.fmt.allocPrint(alloc, "SELECT {s} AS value", .{entry.sql});
        defer alloc.free(sql);
        const case: fixtures.Corpus.Case = .{ .id = "array-expression-contract", .name = entry.sql, .family = "array", .sql = sql, .params = &.{}, .source_expectation = "success" };
        const response = try execute(alloc, handler, &case);
        defer response.deinit();
        const result = response.value;
        try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        try std.testing.expectEqual(@as(usize, 1), result.columns.len);
        try std.testing.expectEqualStrings("value", result.columns[0].name);
        try std.testing.expectEqualStrings("SELECT", result.command_tag);
        try std.testing.expectEqual(@as(i64, 0), result.rows_affected);
        try std.testing.expect(try rowMatchesWithNulls(alloc, result.columns, result.rows[0], result.sql_nulls.?[0], &.{entry.value}, &.{entry.value == .null}));
    }
    std.debug.print("SQL public array expression contracts: 164 passed; no original disposition credit\n", .{});
}

pub fn runInternalArrayQueries(alloc: std.mem.Allocator, handler: anytype) !void {
    const Type = @FieldType(wire.SQLColumn, "type");
    const Case = struct { sql: []const u8, rows: []const u8, types: []const Type };
    const cases = [_]Case{
        .{ .sql = "WITH q AS (SELECT ARRAY[1,NULL,3]::bigint[] a) SELECT cardinality(a), array_length(a,1) FROM q", .rows = "[[\"3\",\"3\"]]", .types = &.{ .integer, .integer } },
        .{ .sql = "WITH q AS MATERIALIZED (SELECT ARRAY[1,NULL,3]::bigint[] a) SELECT cardinality(a), array_lower(a,1) FROM q", .rows = "[[\"3\",\"1\"]]", .types = &.{ .integer, .integer } },
        .{ .sql = "SELECT cardinality(a) FROM (SELECT ARRAY[1,NULL,3]::bigint[] a ORDER BY 1) q", .rows = "[[\"3\"]]", .types = &.{.integer} },
        .{ .sql = "SELECT cardinality(a) FROM (SELECT ARRAY[1,NULL,3]::bigint[] a UNION ALL SELECT ARRAY[4]::bigint[]) q ORDER BY 1", .rows = "[[\"1\"],[\"3\"]]", .types = &.{.integer} },
        .{ .sql = "SELECT cardinality(p), array_length(p,1), 1.5 = ANY(p), 2.5 = ANY(p), 2.0 = ANY(p) FROM (SELECT percentile_cont(ARRAY[0.25,NULL,0.75]) WITHIN GROUP (ORDER BY x) p FROM (SELECT 1.0 x UNION ALL SELECT 3.0 x) t) q", .rows = "[[\"3\",\"3\",true,true,null]]", .types = &.{ .integer, .integer, .boolean, .boolean, .boolean } },
        .{ .sql = "SELECT cardinality(a), row_number() OVER (ORDER BY cardinality(a)) FROM (SELECT ARRAY[1,2]::bigint[] a UNION ALL SELECT ARRAY[3]::bigint[]) q ORDER BY 1", .rows = "[[\"1\",\"1\"],[\"2\",\"2\"]]", .types = &.{ .integer, .integer } },
        .{ .sql = "SELECT cardinality((SELECT ARRAY[1,NULL,3]::bigint[]))", .rows = "[[\"3\"]]", .types = &.{.integer} },
        .{ .sql = "SELECT count(*) FROM (SELECT ARRAY[1,NULL]::int2[] a UNION SELECT ARRAY[1,NULL]::int8[]) q", .rows = "[[\"1\"]]", .types = &.{.integer} },
        .{ .sql = "SELECT count(*) FROM (SELECT ARRAY[1]::int4[] a INTERSECT SELECT ARRAY[1]::float8[]) q", .rows = "[[\"1\"]]", .types = &.{.integer} },
        .{ .sql = "SELECT count(*) FROM (SELECT ARRAY[1]::int4[] a EXCEPT SELECT ARRAY[1]::float8[]) q", .rows = "[[\"0\"]]", .types = &.{.integer} },
        .{ .sql = "SELECT cardinality(a),2.5=ANY(a) FROM (VALUES(ARRAY[1]::int2[]),(ARRAY[2.5]::float4[])) q(a) ORDER BY 1,2", .rows = "[[\"1\",false],[\"1\",true]]", .types = &.{ .integer, .boolean } },
        .{ .sql = "SELECT count(*) FROM ((SELECT ARRAY[16777216]::int8[] a UNION SELECT ARRAY[16777217]::int8[]) UNION ALL SELECT ARRAY[1]::float4[]) q", .rows = "[[\"3\"]]", .types = &.{.integer} },
        .{ .sql = "SELECT count(*) FROM (SELECT ARRAY[16777216]::int8[] a UNION SELECT ARRAY[16777217]::float4[]) q", .rows = "[[\"1\"]]", .types = &.{.integer} },
        .{ .sql = "SELECT cardinality(a),1=ANY(a) FROM (SELECT '{1,NULL}' a UNION SELECT ARRAY[1,NULL]::int4[]) q", .rows = "[[\"2\",true]]", .types = &.{ .integer, .boolean } },
        .{ .sql = "SELECT count(*) FROM (SELECT NULL::int2[] a UNION SELECT NULL::float8[]) q", .rows = "[[\"1\"]]", .types = &.{.integer} },
        .{ .sql = "SELECT x FROM (VALUES(NULL),(NULL),(1)) q(x) ORDER BY x", .rows = "[[\"1\"],[null],[null]]", .types = &.{.integer} },
    };
    for (cases) |entry| {
        const case: fixtures.Corpus.Case = .{ .id = "internal-array-query-contract", .name = entry.sql, .family = "array", .sql = entry.sql, .params = &.{}, .source_expectation = "success" };
        const response = try execute(alloc, handler, &case);
        defer response.deinit();
        const result = response.value;
        const rows = try std.json.Stringify.valueAlloc(alloc, result.rows, .{});
        defer alloc.free(rows);
        try std.testing.expectEqualStrings(entry.rows, rows);
        try std.testing.expectEqual(entry.types.len, result.columns.len);
        for (entry.types, result.columns) |kind, column| try std.testing.expectEqual(kind, column.type);
        try std.testing.expectEqualStrings("SELECT", result.command_tag);
        try std.testing.expectEqual(@as(i64, 0), result.rows_affected);
        try std.testing.expect(result.sql_nulls != null);
        try std.testing.expectEqual(result.rows.len, result.sql_nulls.?.len);
        for (result.rows, result.sql_nulls.?) |row, nulls| {
            try std.testing.expectEqual(row.len, nulls.len);
            for (row, nulls) |cell, is_null| try std.testing.expectEqual(cell == .null, is_null);
        }
    }
    std.debug.print("SQL public internal-array query contracts: {d} passed; no original disposition credit\n", .{cases.len});
    try runArrayResults(alloc, handler);
}

fn runArrayResults(alloc: std.mem.Allocator, handler: anytype) !void {
    const sources = @import("antfly_local_sources");
    const Case = struct { sql: []const u8, kind: sources.sql_array_value.ElementType, expected: ?[]const u8, params: []const u8 = "[]" };
    for ([_]Case{
        .{ .sql = "SELECT ARRAY[-9223372036854775808,NULL,9223372036854775807]::bigint[]", .kind = .int64, .expected = "{-9223372036854775808,NULL,9223372036854775807}" },
        .{ .sql = "SELECT '[0:1][3:4]={{1,NULL},{3,4}}'::int4[]", .kind = .int32, .expected = "[0:1][3:4]={{1,NULL},{3,4}}" },
        .{ .sql = "SELECT ARRAY[]::text[]", .kind = .text, .expected = "{}" },
        .{ .sql = "SELECT NULL::int4[]", .kind = .int32, .expected = null },
        .{ .sql = "SELECT ARRAY['null'::jsonb,NULL,'{\"a\":[1,2]}'::jsonb]::jsonb[]", .kind = .jsonb, .expected = "{\"null\",NULL,\"{\\\"a\\\":[1,2]}\"}" },
        .{ .sql = "SELECT a FROM (SELECT ARRAY[1,NULL]::int2[] a UNION SELECT ARRAY[1,NULL]::int8[]) q ORDER BY 1", .kind = .int64, .expected = "{1,NULL}" },
        .{ .sql = "SELECT a,row_number() OVER (ORDER BY cardinality(a)) FROM (SELECT ARRAY[1,NULL]::int4[] a) q", .kind = .int32, .expected = "{1,NULL}" },
        .{ .sql = "SELECT $1::bigint[] a,cardinality($1) n", .kind = .int64, .expected = "[-1:1]={9007199254740993,NULL,2}", .params = "[{\"string\":\"[-1:1]={9007199254740993,NULL,2}\"}]" },
        .{ .sql = "SELECT q.a FROM (SELECT $1::bigint[] a) q", .kind = .int64, .expected = "[-1:1]={9007199254740993,NULL,2}", .params = "[{\"string\":\"[-1:1]={9007199254740993,NULL,2}\"}]" },
        .{ .sql = "SELECT $1::bigint[]", .kind = .int64, .expected = "[0:1]={9007199254740993,NULL}", .params = "[{\"json\":\"{\\\"dimensions\\\":[{\\\"length\\\":2,\\\"lower_bound\\\":0}],\\\"values\\\":[\\\"9007199254740993\\\",null],\\\"sql_nulls\\\":[false,true]}\"}]" },
        .{ .sql = "SELECT lag($1::int2[],1,$2::float8[]) OVER () a", .kind = .float64, .expected = "[0:1]={3.5,NULL}", .params = "[{\"string\":\"{1,2}\"},{\"string\":\"[0:1]={3.5,NULL}\"}]" },
    }) |entry| {
        const parameters = try std.json.parseFromSlice([]const Json, alloc, entry.params, .{ .parse_numbers = false });
        defer parameters.deinit();
        const case: fixtures.Corpus.Case = .{ .id = "array-result-contract", .name = entry.sql, .family = "array", .sql = entry.sql, .params = parameters.value, .source_expectation = "success" };
        const response = try execute(alloc, handler, &case);
        defer response.deinit();
        const result = response.value;
        try std.testing.expectEqual(@as(usize, 1), result.rows.len);
        try std.testing.expectEqual(wire.SQLColumnType.array, result.columns[0].type);
        try std.testing.expectEqualStrings(@tagName(entry.kind), @tagName(result.columns[0].element_type.?));
        const cell = result.rows[0][0];
        const flag = result.sql_nulls.?[0][0];
        if (entry.expected) |text| {
            try std.testing.expect(!flag);
            var actual = try sources.sql_array_wire.decode(alloc, entry.kind, cell, .{});
            defer actual.deinit();
            var expected = try sources.sql_array_text.decode(alloc, entry.kind, text, .{});
            defer expected.deinit();
            var work: sources.sql_array_value.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try expected.value.compare(actual.value, &work));
        } else try std.testing.expect(flag and cell == .null);
    }
}

pub fn runReference(alloc: std.mem.Allocator, handler: anytype, case_ids: []const []const u8, reference_bytes: []const u8) !void {
    const discovery = try std.testing.environ.containsUnempty(alloc, "ANTFLY_SQL_READ_DISCOVERY");
    return runReferenceWithDiscovery(alloc, handler, case_ids, reference_bytes, discovery);
}

/// Evidence gates must fail closed even in a discovery-configured shell.
pub fn runReferenceStrict(alloc: std.mem.Allocator, handler: anytype, case_ids: []const []const u8, reference_bytes: []const u8) !void {
    return runReferenceWithDiscovery(alloc, handler, case_ids, reference_bytes, false);
}

fn runReferenceWithDiscovery(alloc: std.mem.Allocator, handler: anytype, case_ids: []const []const u8, reference_bytes: []const u8, discovery: bool) !void {
    var corpus = try fixtures.Corpus.init(alloc);
    defer corpus.deinit();
    const reference = try std.json.parseFromSlice(Reference, alloc, reference_bytes, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var failures: usize = 0;
    const postgres_reference = std.mem.eql(u8, reference.value.reference, "PostgreSQL exact SQL");
    try std.testing.expectEqual(case_ids.len, reference.value.entries.len);
    for (reference.value.entries, case_ids) |expected, case_id| {
        try std.testing.expectEqualStrings(case_id, expected.id);
        const case = try corpus.get(expected.id);
        try std.testing.expect(!std.mem.eql(u8, case.source_expectation, "rejection"));
        const parsed = execute(alloc, handler, case) catch |err| {
            if (discovery and err == error.NativeParityAdmissionFailed) continue;
            return err;
        };
        defer parsed.deinit();
        const result = parsed.value;
        var compiled = try compiler.compile(alloc, case.sql, .{});
        defer compiled.deinit();
        const ordered = compiled.statement == .select and compiled.statement.select.order_by.len != 0;
        try std.testing.expect(compiled.statement == .select);
        if (result.rows.len != 0) {
            try std.testing.expect(result.sql_nulls != null);
            try std.testing.expectEqual(result.rows.len, result.sql_nulls.?.len);
        } else if (result.sql_nulls) |nulls| try std.testing.expectEqual(result.rows.len, nulls.len);
        const used = try alloc.alloc(bool, expected.rows.len);
        defer alloc.free(used);
        @memset(used, false);
        var matches = result.rows.len == expected.rows.len and result.columns.len == expected.columns.len and result.rows_affected == 0;
        matches = matches and std.mem.eql(u8, result.command_tag, "SELECT");
        if (matches) for (result.columns, expected.columns) |column, name| {
            // SQLite and PostgreSQL choose different implicit expression
            // labels. Explicit identifiers/aliases must still agree exactly.
            const identifier = name.len != 0 and for (name) |byte| {
                if (!std.ascii.isAlphanumeric(byte) and byte != '_') break false;
            } else true;
            if ((identifier or postgres_reference) and !std.mem.eql(u8, column.name, name)) {
                matches = false;
                break;
            }
        };
        if (matches) if (expected.column_oids) |oids| {
            try std.testing.expectEqual(result.columns.len, oids.len);
            for (result.columns, oids) |column, oid| if (!postgresTypeMatches(column.type, oid)) {
                matches = false;
                break;
            };
        };
        if (matches and expected.ordered_groups != null) matches = try orderedPrefixMatches(alloc, result, expected.ordered_groups.?);
        if (matches and expected.ordered_groups == null) for (result.rows, 0..) |row, row_index| {
            var found = false;
            for (expected.rows, 0..) |want, want_index| {
                if (used[want_index] or (ordered and row_index != want_index)) continue;
                if (expected.sql_nulls) |nulls| try std.testing.expectEqual(expected.rows.len, nulls.len);
                if (try rowMatchesWithNulls(alloc, result.columns, row, if (result.sql_nulls) |nulls| nulls[row_index] else null, want, if (expected.sql_nulls) |nulls| nulls[want_index] else null)) {
                    used[want_index] = true;
                    found = true;
                    break;
                }
            }
            if (!found) {
                matches = false;
                break;
            }
        };
        if (!matches) {
            const diagnostic = try std.json.Stringify.valueAlloc(alloc, result, .{});
            defer alloc.free(diagnostic);
            std.debug.print("REFERENCE {s} result mismatch: {s}\n", .{ case.id, diagnostic });
            failures += 1;
        } else if (discovery) std.debug.print("READ PASS {s}\n", .{case.id});
    }
    try std.testing.expectEqual(@as(usize, 0), failures);
}
