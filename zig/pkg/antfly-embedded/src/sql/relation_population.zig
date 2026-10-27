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

//! Pull-based query output for the existing hidden-owner publication lane.
//! Retain one native result lease and one bounded transport page. WITH NO DATA
//! describes the query without opening a storage cursor or evaluating rows.
const std = @import("std");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const compiler = @import("compiler.zig");
const describe = @import("describe.zig");
const runtime = @import("runtime.zig");
const reads = @import("read_stream.zig");
const delivery = @import("../storage/db/relational_population_contract.zig");
const A = std.mem.Allocator;
const page_goal_bytes = 256 * 1024;

pub const Source = struct {
    allocator: A,
    schema_json: []const u8,
    definition: [32]u8,
    source_definition: [32]u8,
    columns: []const catalog.Column,
    output: []const describe.Column,
    backend: catalog.Backend,
    compiled: compiler.Compiled,
    parameters: []const std.json.Value,
    limits: runtime.Limits,
    with_data: bool,
    scratch: std.heap.ArenaAllocator,
    stream: ?*reads.Stream = null,
    pending: ?reads.BatchPage = null,
    pending_index: usize = 0,
    emitted: u64 = 0,
    underlying_end: bool = false,
    exhausted: bool = false,
    failed: bool = false,

    /// The parent execution owns compiled syntax, parameters and schema bytes.
    /// Source itself owns its result leases and scratch until deinit().
    pub fn init(a: A, backend: catalog.Backend, compiled: *const compiler.Compiled, intent: ast.RelationPopulation, parameters: []const std.json.Value, limits: runtime.Limits) !*Source {
        const query = intent.query orelse return error.InvalidSqlBackendResponse;
        var read = compiled.*;
        // A transparent relation boundary lets the shared native iterator own
        // constant, aggregate and scan producers alike. In particular, a
        // SELECT without FROM must not depend on spill availability just to
        // deliver its singleton. Keep the original query intact below the
        // boundary so LIMIT, ordering and expression evaluation happen once.
        read.statement = .{ .select = query.* };
        if (query.table == null and query.source == null and query.ctes.len == 0 and query.values_arms.len == 0 and query.set_operation == null) {
            const source = try a.create(ast.Relation);
            source.* = .{ .derived = .{ .query = query, .alias = "relation_population" } };
            read.statement = .{ .select = .{ .source = source } };
        }
        if (parameters.len != compiled.parameter_count) return error.InvalidSqlParameters;
        var statement_backend = backend;
        statement_backend.parameter_fallback_types = try runtime.parameterFallbackTypesNative(a, parameters, limits.parameter_cells);
        const bound = try describe.bind(a, statement_backend, &read, &.{});
        if (intent.columns.len > bound.columns.len or bound.columns.len == 0 or bound.columns.len > 1024) return error.InvalidSqlSyntax;
        const definitions = try a.alloc(ast.Column, bound.columns.len);
        const columns = try a.alloc(catalog.Column, bound.columns.len);
        var names: std.StringHashMapUnmanaged(void) = .empty;
        defer names.deinit(a);
        for (bound.columns, definitions, columns, 0..) |column, *definition, *target, ordinal| {
            const name = if (ordinal < intent.columns.len) intent.columns[ordinal] else column.name;
            if (name.len == 0 or std.mem.eql(u8, name, "_id") or std.mem.eql(u8, name, "_version") or std.mem.eql(u8, name, "_doc")) return error.UnsupportedSqlShape;
            if ((try names.getOrPut(a, name)).found_existing) return error.DuplicateColumn;
            if (column.temporal_kind) |kind| if (kind != .timestamp_tz) return error.UnsupportedRelationalValueType;
            definition.* = .{ .name = name, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = true };
            target.* = .{ .name = name, .path = name, .type = column.type, .element_type = column.element_type, .numeric_modifier = column.numeric_modifier, .nullable = true };
        }
        const schema = try @import("ddl_runtime.zig").createSchemaAlloc(a, .{ .table = intent.name, .columns = definitions });
        var hash_buffer: [4096]u8 = undefined;
        var hash: std.Io.Writer.Hashing(std.crypto.hash.Blake3) = .init(&hash_buffer);
        hash.hasher.update("antfly:relation-population:query:v1\x00");
        // This identifies the program. The metadata-issued capture ID identifies
        // one invocation and cannot be reused to resume/reexecute a query.
        try std.json.Stringify.value(.{ .query = query.*, .schema = schema }, .{}, &hash.writer);
        try hash.writer.flush();
        var digest: [32]u8 = undefined;
        hash.hasher.final(&digest);
        const self = try a.create(Source);
        self.* = .{ .allocator = a, .schema_json = schema, .definition = digest, .source_definition = try sourceIdentity(bound), .columns = columns, .output = bound.columns, .backend = statement_backend, .compiled = read, .parameters = parameters, .limits = limits, .with_data = intent.with_data, .exhausted = !intent.with_data, .scratch = .init(a) };
        return self;
    }
    pub fn deinit(self: *Source) void {
        if (self.pending) |*page| page.deinit();
        if (self.stream) |stream| stream.close();
        self.scratch.deinit();
        self.allocator.destroy(self);
    }
    pub fn iface(self: *Source) catalog.PopulationSource {
        return .{ .schema_json = self.schema_json, .definition = self.definition, .source_definition = self.source_definition, .ptr = self, .next = next, .finish = finish };
    }
    fn validateBinding(raw: *anyopaque, a: A, bound: describe.BoundStatement) !void {
        const self: *Source = @ptrCast(@alignCast(raw));
        if (self.backend.execution_binding_validator) |prior| try prior.validate(prior.ptr, a, bound);
        // Validate before opening any snapshot/spill producer. Matching result
        // types alone cannot detect a dropped/recreated source or a changed
        // schema epoch with the same projected columns.
        if (!std.mem.eql(u8, &self.source_definition, &try sourceIdentity(bound))) return error.CatalogGenerationChanged;
    }
    fn finish(raw: *anyopaque) !void {
        const self: *Source = @ptrCast(@alignCast(raw));
        if (self.failed or !self.exhausted) return error.SourceSnapshotIncomplete;
        try self.backend.vtable.checkpoint(self.backend.ptr);
    }
    fn next(raw: *anyopaque, max_rows: u32) anyerror!?[]const delivery.Row {
        const self: *Source = @ptrCast(@alignCast(raw));
        if (self.failed) return error.SqlStreamFailed;
        return self.pull(max_rows) catch |err| {
            self.failed = true;
            return err;
        };
    }
    fn pull(self: *Source, requested: u32) !?[]const delivery.Row {
        try self.backend.vtable.checkpoint(self.backend.ptr);
        if (requested == 0 or requested > delivery.max_page_rows) return error.InvalidSqlLimit;
        if (self.exhausted) return null;
        _ = self.scratch.reset(.retain_capacity);
        const a = self.scratch.allocator();
        if (self.stream == null) {
            var limits = self.limits;
            limits.result_rows = std.math.maxInt(usize);
            limits.native_result = true;
            var pinned = self.backend;
            pinned.execution_binding_validator = .{ .ptr = self, .validate = validateBinding };
            self.stream = (try reads.Stream.open(self.allocator, pinned, &self.compiled, self.parameters, limits)) orelse return error.UnsupportedSqlExecution;
            const actual = self.stream.?.context.binding.columns;
            if (actual.len != self.output.len) return error.CatalogGenerationChanged;
            for (actual, self.output) |current, expected| {
                if (!std.mem.eql(u8, current.name, expected.name) or current.type != expected.type or
                    current.element_type != expected.element_type or current.temporal_kind != expected.temporal_kind or
                    !@import("scalar.zig").NumericModifier.eql(current.numeric_modifier, expected.numeric_modifier))
                    return error.CatalogGenerationChanged;
            }
        }
        var rows: std.ArrayList(delivery.Row) = .empty;
        var bytes: usize = 0;
        while (rows.items.len < requested and bytes < page_goal_bytes) {
            if (self.pending == null) {
                if (self.underlying_end) {
                    self.exhausted = true;
                    break;
                }
                self.pending = try self.stream.?.nextBatch(requested);
                self.pending_index = 0;
                self.underlying_end = self.pending.?.exhausted;
                if (self.pending.?.values.len() == 0) {
                    self.pending.?.deinit();
                    self.pending = null;
                    if (!self.underlying_end) return error.InvalidSqlBackendResponse;
                    self.exhausted = true;
                    break;
                }
            }
            const page = &self.pending.?;
            const cells = try a.alloc(StorageCell, self.columns.len);
            var nulls: std.ArrayList([]const u8) = .empty;
            for (self.columns, cells, 0..) |column, *cell, ordinal| {
                // Identity JSON projections retain verified encoded payloads.
                // Keep them encoded until the destination's ordinary typed-row
                // preparation, rather than decoding/cloning a sender DOM.
                if (column.type == .json) if (try page.values.encodedJson(self.pending_index, ordinal)) |encoded| {
                    cell.* = .{ .encoded = encoded };
                    if (std.mem.eql(u8, std.mem.trim(u8, encoded, " \t\r\n"), "null")) try nulls.append(a, column.name);
                    continue;
                };
                const datum = try page.values.cell(a, self.pending_index, ordinal);
                const value = try runtime.encodeStorageDatum(a, datum, column, self.limits.retained_bytes);
                if (column.type == .json and value == .null and !datum.sql_null) try nulls.append(a, column.name);
                cell.* = .{ .value = value };
            }
            const document = try std.json.Stringify.valueAlloc(a, StorageRow{ .columns = self.columns, .cells = cells }, .{});
            bytes = std.math.add(usize, bytes, document.len) catch return error.RelationPopulationPageTooLarge;
            if (bytes > delivery.max_page_bytes) return error.RelationPopulationPageTooLarge;
            std.mem.sort([]const u8, nulls.items, {}, struct {
                fn less(_: void, left: []const u8, right: []const u8) bool {
                    return std.mem.order(u8, left, right) == .lt;
                }
            }.less);
            if (self.emitted == std.math.maxInt(u64)) return error.SqlProgramLimitExceeded;
            try rows.append(a, .{ .ordinal = self.emitted, .document = document, .json_null_fields = nulls.items });
            self.emitted += 1;
            self.pending_index += 1;
            if (self.pending_index == page.values.len()) {
                page.deinit();
                self.pending = null;
            }
        }
        if (rows.items.len == 0) return null;
        return rows.items;
    }
};

const StorageCell = union(enum) { value: std.json.Value, encoded: []const u8 };
const StorageRow = struct {
    columns: []const catalog.Column,
    cells: []const StorageCell,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.beginObject();
        for (self.columns, self.cells) |column, cell| {
            try writer.objectField(column.path);
            switch (cell) {
                .value => |value| try writer.write(value),
                .encoded => |encoded| {
                    try writer.beginWriteRaw();
                    try writer.writer.writeAll(encoded);
                    writer.endWriteRaw();
                },
            }
        }
        try writer.endObject();
    }
};

/// Fixed-memory catalog identity projection. This is deliberately a source
/// definition fingerprint, not a data-snapshot attestation. Native cursors
/// still obtain their real statement cut through the existing read backend.
fn sourceIdentity(bound: describe.BoundStatement) ![32]u8 {
    const Identity = struct {
        id: u64,
        physical_name: []const u8,
        schema_version: u32,
        scope: ?catalog.Table.Scope,
        storage_mode: @FieldType(catalog.Table, "storage_mode"),
        row_policy_purpose: @FieldType(catalog.Table, "row_policy_purpose"),
        sql_select_required: bool,
    };
    const Manifest = struct {
        entries: [64]Identity = undefined,
        count: usize = 0,
        fn table(self: *@This(), value: catalog.Table) !void {
            if (value.id == 0) return; // compiler-owned virtual relation
            const identity: Identity = .{ .id = value.id, .physical_name = value.physical_name, .schema_version = value.schema_version, .scope = value.scope, .storage_mode = value.storage_mode, .row_policy_purpose = value.row_policy_purpose, .sql_select_required = value.sql_select_required };
            for (self.entries[0..self.count]) |prior| if (prior.id == value.id) {
                if (prior.schema_version != identity.schema_version or prior.storage_mode != identity.storage_mode or prior.row_policy_purpose != identity.row_policy_purpose or prior.sql_select_required != identity.sql_select_required or !std.mem.eql(u8, prior.physical_name, identity.physical_name)) return error.CatalogGenerationChanged;
                if (prior.scope) |old| {
                    const current = identity.scope orelse return error.CatalogGenerationChanged;
                    if (old.revision != current.revision or !std.mem.eql(u8, old.database, current.database) or !std.mem.eql(u8, old.namespace, current.namespace) or !std.mem.eql(u8, old.name, current.name)) return error.CatalogGenerationChanged;
                } else if (identity.scope != null) return error.CatalogGenerationChanged;
                return;
            };
            if (self.count == self.entries.len) return error.SqlProgramLimitExceeded;
            self.entries[self.count] = identity;
            self.count += 1;
        }
        fn collect(self: *@This(), value: describe.BoundStatement, depth: usize) anyerror!void {
            if (depth == 64) return error.SqlProgramLimitExceeded;
            if (value.table) |physical| try self.table(physical);
            if (value.relation) |relation| for (relation.scans) |scan| try self.table(scan.table);
            if (value.window) |window| try self.collect(window.input.*, depth + 1);
        }
        fn less(_: void, left: Identity, right: Identity) bool {
            return left.id < right.id;
        }
    };
    var manifest: Manifest = .{};
    try manifest.collect(bound, 0);
    std.mem.sort(Identity, manifest.entries[0..manifest.count], {}, Manifest.less);
    var buffer: [4096]u8 = undefined;
    var hash: std.Io.Writer.Hashing(std.crypto.hash.Blake3) = .init(&buffer);
    hash.hasher.update("antfly:relation-population:source-definitions:v1\x00");
    try std.json.Stringify.value(.{ .sources = manifest.entries[0..manifest.count], .parameters = bound.parameter_descriptors }, .{}, &hash.writer);
    try hash.writer.flush();
    var digest: [32]u8 = undefined;
    hash.hasher.final(&digest);
    return digest;
}

const Fixture = struct {
    scan_calls: usize = 0,
    mutations: usize = 0,
    population_calls: usize = 0,
    populated_rows: u64 = 0,
    population_pages: usize = 0,
    cancelled: bool = false,
    pending_publication: bool = false,
    incomplete_producer: bool = false,
    skip_population: bool = false,
    fn backend(self: *Fixture) catalog.Backend {
        return .{ .ptr = self, .execution_io = std.testing.io, .vtable = &.{ .resolve = resolve, .scan = scan, .mutate = mutate, .checkpoint = checkpoint, .ddl = ddl } };
    }
    fn resolve(_: *anyopaque, _: A, _: ast.Name, _: catalog.Action) !catalog.Table {
        return error.TableNotFound;
    }
    fn scan(raw: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.scan_calls += 1;
        return error.UnsupportedSqlExecution;
    }
    fn mutate(raw: *anyopaque, _: A, _: A, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.mutations += 1;
        return error.UnsupportedSqlExecution;
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        if (self.cancelled) return error.QueryCanceled;
    }
    fn ddl(raw: *anyopaque, _: catalog.DdlAllocators, request: catalog.Ddl) !catalog.DdlOutcome {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.population_calls += 1;
        if (request != .populate) return error.UnexpectedMutation;
        if (self.skip_population) return .{ .population_skipped = true };
        const producer = request.populate.source orelse return error.SourceSnapshotIncomplete;
        if (self.incomplete_producer) return error.SourceSnapshotIncomplete;
        while (try producer.next(producer.ptr, delivery.max_page_rows)) |rows| {
            self.population_pages += 1;
            self.populated_rows += rows.len;
        }
        try producer.finish(producer.ptr);
        return .{ .rows_affected = self.populated_rows, .mutation_outcome = if (self.pending_publication) .committed_pending else .committed };
    }
};

test "SQL relation population source describes NO DATA without evaluating or opening rows" {
    const a = std.testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    var compiled = try compiler.compile(a, "CREATE TABLE dst AS SELECT 1 / 0 AS n WITH NO DATA", .{});
    defer compiled.deinit();
    var backend: Fixture = .{};
    const source = try Source.init(arena.allocator(), backend.backend(), &compiled, compiled.statement.catalog_ddl.population.?, &.{}, .{});
    defer source.deinit();
    const producer = source.iface();
    try std.testing.expect(try producer.next(producer.ptr, 128) == null);
    try producer.finish(producer.ptr);
    try std.testing.expect(source.stream == null);
    try std.testing.expectEqual(@as(usize, 0), backend.scan_calls);
    try std.testing.expectEqual(@as(usize, 0), backend.mutations);
}

test "SQL relation population source retains typed descriptors numeric scale and JSON null provenance" {
    const a = std.testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    var compiled = try compiler.compile(a, "CREATE TABLE dst AS SELECT 1::BIGINT AS id, 'null'::JSONB AS j, 3.50::NUMERIC(12,2) AS n", .{});
    defer compiled.deinit();
    var backend: Fixture = .{};
    const source = try Source.init(arena.allocator(), backend.backend(), &compiled, compiled.statement.catalog_ddl.population.?, &.{}, .{});
    defer source.deinit();
    const producer = source.iface();
    try std.testing.expectError(error.SourceSnapshotIncomplete, producer.finish(producer.ptr));
    const rows = (try producer.next(producer.ptr, 128)).?;
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqual(@as(usize, 1), rows[0].json_null_fields.len);
    try std.testing.expectEqualStrings("j", rows[0].json_null_fields[0]);
    var document = try std.json.parseFromSlice(std.json.Value, a, rows[0].document, .{ .parse_numbers = false });
    defer document.deinit();
    try std.testing.expect(document.value.object.get("j").? == .null);
    try std.testing.expectEqual(@as(?@import("array_value.zig").ElementType, .int64), source.columns[0].element_type);
    try std.testing.expectEqual(@as(u16, 12), source.columns[2].numeric_modifier.?.precision);
    try std.testing.expectEqual(@as(i16, 2), source.columns[2].numeric_modifier.?.scale);
    try std.testing.expect(try producer.next(producer.ptr, 128) == null);
    try producer.finish(producer.ptr);
}

test "SQL relation population source streams complete bounded pages beyond public result limits" {
    const a = std.testing.allocator;
    var text: std.Io.Writer.Allocating = .init(a);
    defer text.deinit();
    try text.writer.writeAll("CREATE TABLE dst(n) AS VALUES ");
    for (0..257) |i| {
        if (i != 0) try text.writer.writeByte(',');
        try text.writer.print("({d}::BIGINT)", .{i});
    }
    var compiled = try compiler.compile(a, text.written(), .{});
    defer compiled.deinit();
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    var backend: Fixture = .{};
    const source = try Source.init(arena.allocator(), backend.backend(), &compiled, compiled.statement.catalog_ddl.population.?, &.{}, .{ .result_rows = 2 });
    defer source.deinit();
    const producer = source.iface();
    var total: usize = 0;
    var pages: usize = 0;
    while (try producer.next(producer.ptr, 128)) |rows| {
        try std.testing.expect(rows.len <= 128);
        total += rows.len;
        pages += 1;
    }
    try producer.finish(producer.ptr);
    try std.testing.expectEqual(@as(usize, 257), total);
    try std.testing.expect(pages >= 3);
    try std.testing.expectEqual(@as(usize, 0), backend.mutations);
}

test "SQL relation population runtime routes complete producer cohorts through the DDL authority" {
    const a = std.testing.allocator;
    for ([_]struct { sql: []const u8, rows: u64 }{
        .{ .sql = "CREATE TABLE dst AS SELECT 1::BIGINT AS n", .rows = 1 },
        .{ .sql = "CREATE TABLE dst(x) AS VALUES (1::BIGINT),(2::BIGINT),(3::BIGINT)", .rows = 3 },
        .{ .sql = "SELECT 1::BIGINT AS n INTO dst", .rows = 1 },
        .{ .sql = "WITH src AS (SELECT 7::BIGINT AS n) SELECT n INTO dst FROM src", .rows = 1 },
        .{ .sql = "CREATE MATERIALIZED VIEW dst AS SELECT 1::BIGINT AS n", .rows = 1 },
        .{ .sql = "CREATE MATERIALIZED VIEW dst AS SELECT 1/0 AS n WITH NO DATA", .rows = 0 },
        .{ .sql = "CREATE TABLE dst AS SELECT 1/0 AS n WITH NO DATA", .rows = 0 },
        .{ .sql = "CREATE TABLE dst AS SELECT count(*) AS n FROM (VALUES (1),(2),(3)) src(n)", .rows = 1 },
        .{ .sql = "CREATE TABLE dst AS SELECT a.n,b.n AS m FROM (VALUES(1),(2)) a(n) CROSS JOIN (VALUES(3),(4)) b(n)", .rows = 4 },
        .{ .sql = "CREATE TABLE dst AS SELECT n FROM (VALUES(1),(2),(3)) src(n) WHERE n>1 LIMIT 1", .rows = 1 },
    }) |case| {
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var fixture: Fixture = .{};
        var result = try runtime.execute(a, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(if (std.mem.indexOf(u8, case.sql, "WITH NO DATA") != null)
            (if (std.mem.startsWith(u8, case.sql, "CREATE MATERIALIZED")) "CREATE MATERIALIZED VIEW" else "CREATE TABLE AS")
        else
            "SELECT", result.output.command_tag);
        try std.testing.expectEqual(@as(?u64, case.rows), result.output.completion_rows);
        try std.testing.expectEqual(case.rows, result.output.rows_affected);
        try std.testing.expectEqual(case.rows, fixture.populated_rows);
        try std.testing.expectEqual(@as(usize, 1), fixture.population_calls);
        try std.testing.expectEqual(@as(usize, 0), result.output.columns.len);
        try std.testing.expectEqual(@as(usize, 0), fixture.mutations);
    }
}

test "SQL relation population prepared parameters retain widths and do not dispatch during describe" {
    const a = std.testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    var compiled = try compiler.compile(a, "CREATE TABLE dst AS SELECT $1::BIGINT AS n, $2::TEXT AS label", .{});
    defer compiled.deinit();
    var fixture: Fixture = .{};
    const bound = try describe.bind(arena.allocator(), fixture.backend(), &compiled, &.{});
    try std.testing.expectEqual(@as(usize, 0), bound.columns.len);
    try std.testing.expectEqual(@as(usize, 2), bound.parameter_types.len);
    try std.testing.expectEqual(@as(usize, 0), fixture.population_calls);
    var result = try runtime.execute(a, fixture.backend(), &compiled, &.{ .{ .integer = 9007199254740993 }, .{ .string = "exact" } }, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 1), result.output.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), fixture.population_calls);
}

test "SQL relation population completion failure cancellation and pending publication keep distinct outcomes" {
    const a = std.testing.allocator;
    var compiled = try compiler.compile(a, "CREATE TABLE dst AS SELECT 1::BIGINT AS n", .{});
    defer compiled.deinit();
    var fixture: Fixture = .{ .cancelled = true };
    try std.testing.expectError(error.QueryCanceled, runtime.execute(a, fixture.backend(), &compiled, &.{}, .{}));
    try std.testing.expectEqual(@as(usize, 0), fixture.population_calls);
    fixture = .{ .incomplete_producer = true };
    try std.testing.expectError(error.SourceSnapshotIncomplete, runtime.execute(a, fixture.backend(), &compiled, &.{}, .{}));
    fixture = .{ .pending_publication = true };
    var pending = try runtime.execute(a, fixture.backend(), &compiled, &.{}, .{});
    defer pending.deinit();
    try std.testing.expectEqualStrings("DDL PENDING", pending.output.command_tag);
    try std.testing.expectEqual(@as(u64, 1), pending.output.rows_affected);
    for ([_][]const u8{ "CREATE TEMP TABLE dst AS SELECT 1 AS n", "CREATE UNLOGGED TABLE dst AS SELECT 1 AS n" }) |sql| {
        var temporary = try compiler.compile(a, sql, .{});
        defer temporary.deinit();
        var guarded: Fixture = .{};
        try std.testing.expectError(error.UnsupportedSqlExecution, runtime.execute(a, guarded.backend(), &temporary, &.{}, .{}));
        try std.testing.expectEqual(@as(usize, 0), guarded.population_calls);
    }
}

test "SQL relation population source rejects changed source identity before opening rows and retains authorization validation" {
    const Physical = struct {
        id: u64 = 41,
        version: u32 = 3,
        name: []const u8 = "physical_source",
        opens: usize = 0,
        checks: usize = 0,
        forbidden: bool = false,
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .execution_binding_validator = .{ .ptr = self, .validate = validate }, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = open, .mutate = mutate, .checkpoint = check, .ddl = ddl } };
        }
        fn resolve(raw: *anyopaque, _: A, _: ast.Name, action: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (action != .read) return error.UnexpectedMutation;
            return .{ .id = self.id, .schema_version = self.version, .physical_name = self.name, .columns = &.{.{ .name = "n", .path = "n", .type = .integer }} };
        }
        fn scan(_: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedRowScan;
        }
        fn open(raw: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.opens += 1;
            return error.UnexpectedRowScan;
        }
        fn validate(raw: *anyopaque, _: A, _: describe.BoundStatement) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.forbidden) return error.Forbidden;
        }
        fn check(_: *anyopaque) !void {}
        fn mutate(_: *anyopaque, _: A, _: A, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.UnexpectedMutation;
        }
        fn ddl(_: *anyopaque, _: catalog.DdlAllocators, _: catalog.Ddl) !catalog.DdlOutcome {
            return error.UnexpectedMutation;
        }
    };
    const a = std.testing.allocator;
    var compiled = try compiler.compile(a, "CREATE TABLE dst AS SELECT n FROM src", .{});
    defer compiled.deinit();
    for (0..4) |variant| {
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        var physical: Physical = .{};
        const source = try Source.init(arena.allocator(), physical.backend(), &compiled, compiled.statement.catalog_ddl.population.?, &.{}, .{});
        defer source.deinit();
        switch (variant) {
            0 => physical.id += 1,
            1 => physical.version += 1,
            2 => physical.name = "replacement_source",
            3 => physical.forbidden = true,
            else => unreachable,
        }
        const producer = source.iface();
        try std.testing.expectError(if (variant == 3) error.Forbidden else error.CatalogGenerationChanged, producer.next(producer.ptr, 128));
        try std.testing.expectEqual(@as(usize, 0), physical.opens);
        try std.testing.expectEqual(@as(usize, 1), physical.checks);
        try std.testing.expectError(error.SqlStreamFailed, producer.next(producer.ptr, 128));
    }
}

test "SQL relation population conditional completion skips evaluation and reports PostgreSQL creation tags" {
    const a = std.testing.allocator;
    for ([_]struct { sql: []const u8, tag: []const u8 }{
        .{ .sql = "CREATE TABLE IF NOT EXISTS dst AS SELECT 1/0 AS n", .tag = "CREATE TABLE AS" },
        .{ .sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS dst AS SELECT 1/0 AS n", .tag = "CREATE MATERIALIZED VIEW" },
    }) |case| {
        var compiled = try compiler.compile(a, case.sql, .{});
        defer compiled.deinit();
        var fixture: Fixture = .{ .skip_population = true };
        var result = try runtime.execute(a, fixture.backend(), &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqualStrings(case.tag, result.output.command_tag);
        try std.testing.expectEqual(@as(usize, 0), fixture.population_pages);
        try std.testing.expectEqual(@as(u64, 0), result.output.rows_affected);
    }
}

test "SQL relation population encoded JSON remains lazy and preserves exact tokens and SQL null provenance" {
    const Encoded = struct {
        offset: usize = 0,
        materialized: usize = 0,
        closed: bool = false,
        fn resolve(_: *anyopaque, _: A, name: ast.Name, _: catalog.Action) !catalog.Table {
            return .{ .id = 1, .physical_name = name.table, .schema_version = 1, .columns = &.{.{ .name = "j", .path = "j", .type = .json }} };
        }
        fn scan(_: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedRowScan;
        }
        fn mutate(_: *anyopaque, _: A, _: A, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.UnexpectedMutation;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn open(raw: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !?catalog.Cursor {
            return .{ .ptr = raw, .next = nextRows, .next_columns = nextColumns, .close = close };
        }
        fn nextRows(_: *anyopaque, _: A, _: u32) !catalog.Page {
            return error.UnexpectedRowScan;
        }
        fn cell(raw: *anyopaque, _: A, index: usize, _: usize) !@import("scalar.zig").Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (index % 3 == 2) return .{};
            self.materialized += 1;
            return error.UnexpectedScalarMaterialization;
        }
        fn json(_: *anyopaque, index: usize, _: usize) !?[]const u8 {
            return switch (index % 3) {
                0 => "{\"exact\":9007199254740993.1250}",
                1 => " null ",
                else => null,
            };
        }
        fn nextColumns(raw: *anyopaque, a: A, limit: u32) !catalog.ColumnPage {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const count = @min(limit, 19 - self.offset);
            const indices = try a.alloc(usize, count);
            for (indices, 0..) |*index, i| index.* = self.offset + i;
            self.offset += count;
            const values = try a.create(@import("execution_batch.zig").Batch);
            values.* = .{ .reader = .{ .ptr = self, .read = cell, .read_json = json, .count = 19, .width = 1 } };
            return .{ .native = .{ .values = values, .names = &.{"j"} }, .selection = indices, .after = if (self.offset == 19) null else "more" };
        }
        fn close(raw: *anyopaque) void {
            @as(*@This(), @ptrCast(@alignCast(raw))).closed = true;
        }
        fn ddl(_: *anyopaque, _: catalog.DdlAllocators, _: catalog.Ddl) !catalog.DdlOutcome {
            return error.UnexpectedMutation;
        }
    };
    const a = std.testing.allocator;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    var compiled = try compiler.compile(a, "CREATE TABLE dst AS SELECT j FROM src", .{});
    defer compiled.deinit();
    var fixture: Encoded = .{};
    const backend: catalog.Backend = .{ .ptr = &fixture, .vtable = &.{ .resolve = Encoded.resolve, .scan = Encoded.scan, .open_scan = Encoded.open, .mutate = Encoded.mutate, .checkpoint = Encoded.checkpoint, .ddl = Encoded.ddl } };
    const source = try Source.init(arena.allocator(), backend, &compiled, compiled.statement.catalog_ddl.population.?, &.{}, .{ .execution_batch_rows = 7 });
    const producer = source.iface();
    var count: usize = 0;
    while (try producer.next(producer.ptr, 5)) |rows| for (rows) |row| {
        try std.testing.expectEqual(@as(u64, @intCast(count)), row.ordinal);
        var doc = try std.json.parseFromSlice(std.json.Value, a, row.document, .{ .parse_numbers = false });
        defer doc.deinit();
        if (count % 3 == 0) {
            try std.testing.expectEqualStrings("9007199254740993.1250", doc.value.object.get("j").?.object.get("exact").?.number_string);
        } else {
            try std.testing.expect(doc.value.object.get("j").? == .null);
        }
        try std.testing.expectEqual(@as(usize, @intFromBool(count % 3 == 1)), row.json_null_fields.len);
        count += 1;
    };
    try producer.finish(producer.ptr);
    source.deinit();
    try std.testing.expectEqual(@as(usize, 19), count);
    try std.testing.expectEqual(@as(usize, 0), fixture.materialized);
    try std.testing.expect(fixture.closed);
}
