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

//! Captured ranked frontiers with demand-driven, snapshot-local SQL hydration.
//! Providers own the ranking cut and primary snapshot; this cursor never opens
//! a fresh DB read, retains an apply lock, or routes through a JSON query API.
const std = @import("std");
const catalog = @import("catalog.zig");
const documents = @import("document_row.zig");
const operators = @import("operators.zig");
const A = std.mem.Allocator;
const Json = std.json.Value;

pub fn metadata(name: []const u8) bool {
    return std.mem.eql(u8, name, "_id") or std.mem.eql(u8, name, "score") or std.mem.eql(u8, name, "_highlights");
}

/// Names borrow the immutable bound plan. Only the returned directory is owned.
pub fn storageFields(a: A, fields: []const []const u8) ![]const []const u8 {
    if (fields.len > 256) return error.SqlProgramLimitExceeded;
    var selected: std.StringArrayHashMapUnmanaged(void) = .empty;
    defer selected.deinit(a);
    for (fields) |field| if (!metadata(field)) try selected.put(a, field, {});
    return a.dupe([]const u8, selected.keys());
}

/// Provider.Result owns its hit frontier; Reader owns an immutable primary
/// snapshot. Lease retains authorization, not a writer-excluding apply fence.
/// Provider.document/hasSource/highlights adapt native hit payloads only.
pub fn Cursor(comptime Provider: type) type {
    return struct {
        const Self = @This();
        a: A,
        projection: documents.Projection,
        result: Provider.Result,
        reader: ?Provider.Reader,
        lease: ?Provider.Lease,
        needs_document: bool,
        position: usize = 0,

        /// Consume result, reader and lease on success only. Projection names,
        /// layout and paths do not borrow the compiler or preparation arena.
        pub fn init(a: A, table: catalog.Table, fields: []const []const u8, result: Provider.Result, reader: ?Provider.Reader, lease: ?Provider.Lease) !*Self {
            if (result.hits.len > 10000 or fields.len > 256 or table.columns.len > 1024) return error.SqlProgramLimitExceeded;
            const self = try a.create(Self);
            errdefer a.destroy(self);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const temporary = scratch.allocator();
            var paths: std.StringHashMapUnmanaged(void) = .empty;
            for (fields) |field| if (!std.mem.eql(u8, field, "_id")) try paths.put(temporary, field, {});
            var names: std.ArrayList([]const u8) = .empty;
            var needs_document = false;
            // Multiple logical aliases may own one physical path. Keep their
            // complete logical directory while reading the physical cell once.
            for (table.columns) |column| if (paths.contains(column.path)) {
                try names.append(temporary, column.name);
                needs_document = needs_document or !metadata(column.path);
            };
            if (table.isDocumentRoot("_doc") and paths.contains("_doc")) {
                try names.append(temporary, "_doc");
                needs_document = true;
            }
            var path_iterator = paths.keyIterator();
            while (path_iterator.next()) |path_entry| {
                const path = path_entry.*;
                const exists = for (table.columns) |column| {
                    if (std.mem.eql(u8, column.path, path)) break true;
                } else false;
                if (!exists and !table.isDocumentRoot(path)) return error.UndefinedColumn;
            }
            self.* = .{ .a = a, .projection = try documents.Projection.init(a, table, names.items), .result = result, .reader = reader, .lease = lease, .needs_document = needs_document };
            return self;
        }

        pub fn iface(self: *Self) catalog.Cursor {
            return .{ .ptr = self, .next = next, .close = close };
        }

        pub fn next(raw: *anyopaque, a: A, limit: u32) !catalog.Page {
            const self: *Self = @ptrCast(@alignCast(raw));
            if (limit == 0) return error.InvalidSqlParameters;
            var page = std.heap.ArenaAllocator.init(a);
            errdefer page.deinit();
            const pa = page.allocator();
            const end = self.position + @min(@as(usize, limit), self.result.hits.len - self.position);
            const rows = try pa.alloc(catalog.Row, end - self.position);
            const names = try pa.alloc([]const u8, self.projection.columns.len);
            for (self.projection.columns, names) |column, *name| name.* = try pa.dupe(u8, column.name);
            const layout = try catalog.Row.TypedLayout.init(pa, names);
            for (self.result.hits[self.position..end], rows) |hit, *out| {
                // Even an ID-only relational projection verifies membership in
                // the captured primary cut, without decoding unselected fields.
                const typed = if (self.reader) |*reader| (if (comptime @hasDecl(Provider.Reader, "lookupSqlRow")) try reader.lookupSqlRow(pa, hit.id) else try reader.lookupTypedRow(pa, hit.id)) orelse return error.SqlStatementReadUnavailable else null;
                if (typed == null and !Provider.hasSource(hit)) return error.SqlStatementReadUnavailable;
                const document = if (typed) |row| row.typed orelse return error.InvalidSqlBackendResponse else if (self.needs_document) try Provider.document(&page, hit) else Json{ .object = .empty };
                if (document != .object) return error.InvalidSqlBackendResponse;
                const source: catalog.Row = .{ .id = hit.id, .version = if (typed) |row| row.version else 0, .value = document, .sql_nulls = if (typed) |row| row.sql_nulls else null, .document = if (self.projection.document_root) document else null };
                const values = try pa.alloc(catalog.Row.Cell, self.projection.columns.len);
                const presence = try pa.alloc(bool, values.len);
                for (self.projection.columns, values, presence) |column, *value, *present| {
                    var cell: catalog.Row.Cell = if (std.mem.eql(u8, column.path, "score"))
                        if (hit.score) |score| catalog.Row.Cell.json(.{ .float = score }) else .{}
                    else if (std.mem.eql(u8, column.path, "_highlights"))
                        if (try Provider.highlights(pa, hit)) |highlights| catalog.Row.Cell.json(highlights) else .{}
                    else
                        try source.cell(column.path);
                    // Document JSON null is a JSONB value; only a missing
                    // property is SQL NULL. Relational bitmaps remain decisive.
                    if (typed == null and !metadata(column.path) and column.type == .json and document.object.contains(column.path)) cell.sql_null = false;
                    present.* = if (metadata(column.path) or (self.projection.document_root and std.mem.eql(u8, column.name, "_doc"))) !cell.sql_null else document.object.contains(column.path);
                    if (cell.sql_null and !column.nullable) return error.InvalidSqlBackendResponse;
                    // Native readers can attest that their typed projection
                    // already belongs to this page. Borrowed hit/vector source
                    // payloads still require an owned selected-cell copy.
                    const typed_page_owned = comptime if (@hasDecl(Provider, "typed_page_owned")) Provider.typed_page_owned else false;
                    const owned = if (typed != null and typed_page_owned) cell else try operators.cloneDatum(pa, cell);
                    value.* = try documents.declaredCell(pa, column, owned);
                }
                out.* = .{ .id = try pa.dupe(u8, hit.id), .version = source.version, .value = .null, .typed_cells = .{ .layout = layout, .values = values, .presence = presence } };
            }
            const after = if (end < self.result.hits.len) try std.fmt.allocPrint(pa, "{d}", .{end}) else null;
            // Failed pages never advance the frontier. Arena cleanup is atomic
            // even when a later row or output allocation fails.
            self.position = end;
            return .{ .rows = rows, .after = after, .owned_arena = page };
        }

        pub fn close(raw: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(raw));
            if (self.reader) |*reader| reader.deinit();
            if (self.lease) |*lease| lease.release();
            self.result.deinit();
            self.projection.deinit(self.a);
            self.a.destroy(self);
        }
    };
}

const TestProvider = struct {
    const State = struct { reads: usize = 0, documents: usize = 0, highlights: usize = 0, results_closed: usize = 0, readers_closed: usize = 0, leases_closed: usize = 0 };
    const Hit = struct { id: []const u8, score: ?f32 = null, state: *State };
    pub const Result = struct {
        hits: []const Hit,
        state: *State,
        pub fn deinit(self: *@This()) void {
            self.state.results_closed += 1;
        }
    };
    pub const Reader = struct {
        state: *State,
        captured_text: []const u8 = "before",
        const Row = struct { version: u64, typed: ?Json, sql_nulls: ?[]const bool };
        pub fn lookupTypedRow(self: *@This(), a: A, id: []const u8) !?Row {
            self.state.reads += 1;
            if (std.mem.eql(u8, id, "bad")) return error.SqlStatementReadUnavailable;
            var object: std.json.ObjectMap = .empty;
            try object.put(a, "text", .{ .string = self.captured_text });
            try object.put(a, "payload", .null);
            try object.put(a, "nullable_json", .null);
            try object.put(a, "n", .{ .string = "9007199254740993.5000" });
            return .{ .version = 7, .typed = .{ .object = object }, .sql_nulls = try a.dupe(bool, &.{ false, false, true, false }) };
        }
        pub fn deinit(self: *@This()) void {
            self.state.readers_closed += 1;
        }
    };
    pub const Lease = struct {
        state: *State,
        pub fn release(self: *@This()) void {
            self.state.leases_closed += 1;
        }
    };
    pub fn hasSource(_: Hit) bool {
        return true;
    }
    pub fn document(arena: *std.heap.ArenaAllocator, hit: Hit) !Json {
        hit.state.documents += 1;
        var object: std.json.ObjectMap = .empty;
        try object.put(arena.allocator(), "payload", .null);
        return .{ .object = object };
    }
    pub fn highlights(a: A, hit: Hit) !?Json {
        hit.state.highlights += 1;
        return .{ .string = try a.dupe(u8, "fragment") };
    }
};
const test_table: catalog.Table = .{ .id = 1, .physical_name = "rows", .schema_version = 7, .columns = &.{
    .{ .name = "summary", .path = "text", .type = .string },
    .{ .name = "alias", .path = "text", .type = .string },
    .{ .name = "payload", .path = "payload", .type = .json },
    .{ .name = "nullable_json", .path = "nullable_json", .type = .json },
    .{ .name = "n", .path = "n", .type = .number, .element_type = .numeric },
    .{ .name = "score", .path = "score", .type = .number },
    .{ .name = "_highlights", .path = "_highlights", .type = .json },
} };

test "SQL captured search cursor hydrates demanded pages and owns cells after close" {
    const a = std.testing.allocator;
    var state: TestProvider.State = .{};
    const hits = [_]TestProvider.Hit{ .{ .id = "a", .score = 1, .state = &state }, .{ .id = "bad", .state = &state } };
    const C = Cursor(TestProvider);
    const cursor = try C.init(a, test_table, &.{ "text", "payload", "nullable_json", "n", "score" }, .{ .hits = &hits, .state = &state }, .{ .state = &state }, .{ .state = &state });
    var open = true;
    defer if (open) C.close(cursor);
    try std.testing.expectEqual(@as(usize, 0), state.reads);
    var page = try C.next(cursor, a, 1);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 1), state.reads);
    try std.testing.expectEqual(@as(usize, 0), state.documents);
    try std.testing.expectEqual(@as(usize, 0), state.highlights);
    try std.testing.expect(page.after != null);
    try std.testing.expectEqualStrings("before", (try page.rows[0].cell("summary")).value.string);
    try std.testing.expectEqualStrings("before", (try page.rows[0].cell("alias")).value.string);
    try std.testing.expect(!(try page.rows[0].cell("payload")).sql_null);
    try std.testing.expect((try page.rows[0].cell("nullable_json")).sql_null);
    const n = (try page.rows[0].cell("n")).numeric.?;
    try std.testing.expectEqual(@as(u16, 4), n.scale);
    try std.testing.expectError(error.SqlStatementReadUnavailable, C.next(cursor, a, 1));
    try std.testing.expectEqual(@as(usize, 1), cursor.position);
    C.close(cursor);
    open = false;
    try std.testing.expectEqual(@as(usize, 1), state.results_closed);
    try std.testing.expectEqual(@as(usize, 1), state.readers_closed);
    try std.testing.expectEqual(@as(usize, 1), state.leases_closed);
    try std.testing.expectEqualStrings("a", page.rows[0].id);
    try std.testing.expectEqualStrings("before", (try page.rows[0].cell("summary")).value.string);
    try std.testing.expectEqual(@as(u16, 4), (try page.rows[0].cell("n")).numeric.?.scale);
}

test "SQL captured search cursor metadata avoids document and highlight materialization" {
    const a = std.testing.allocator;
    var state: TestProvider.State = .{};
    const hits = [_]TestProvider.Hit{.{ .id = "a", .state = &state }};
    const C = Cursor(TestProvider);
    const cursor = try C.init(a, test_table, &.{"score"}, .{ .hits = &hits, .state = &state }, null, .{ .state = &state });
    defer C.close(cursor);
    try std.testing.expectError(error.InvalidSqlParameters, C.next(cursor, a, 0));
    var page = try C.next(cursor, a, 1);
    defer page.deinit();
    try std.testing.expect((try page.rows[0].cell("score")).sql_null);
    try std.testing.expectEqual(@as(usize, 0), state.documents);
    try std.testing.expectEqual(@as(usize, 0), state.highlights);
    try std.testing.expect(page.after == null);
    const fields = try storageFields(a, &.{ "_id", "score", "text", "text", "_highlights" });
    defer a.free(fields);
    try std.testing.expectEqual(@as(usize, 1), fields.len);
    try std.testing.expectEqualStrings("text", fields[0]);
}

test "SQL captured search cursor document root is explicit and page owned" {
    const a = std.testing.allocator;
    var state: TestProvider.State = .{};
    const hits = [_]TestProvider.Hit{.{ .id = "a", .state = &state }};
    var table = test_table;
    table.storage_mode = .document;
    const C = Cursor(TestProvider);
    const cursor = try C.init(a, table, &.{"_doc"}, .{ .hits = &hits, .state = &state }, null, .{ .state = &state });
    var live = true;
    defer if (live) C.close(cursor);
    var page = try C.next(cursor, a, 1);
    defer page.deinit();
    C.close(cursor);
    live = false;
    const root = try page.rows[0].cell("_doc");
    try std.testing.expect(!root.sql_null);
    try std.testing.expect(root.value == .object);
    try std.testing.expectEqual(@as(usize, 1), root.value.object.count());
    try std.testing.expect(root.value.object.get("payload").? == .null);
    try std.testing.expectEqual(@as(usize, 1), state.documents);
    try std.testing.expectEqual(@as(usize, 1), state.results_closed);
}

test "SQL captured search cursor preserves document JSON null and demanded highlights" {
    const a = std.testing.allocator;
    var state: TestProvider.State = .{};
    const hits = [_]TestProvider.Hit{.{ .id = "a", .state = &state }};
    const C = Cursor(TestProvider);
    const cursor = try C.init(a, test_table, &.{ "payload", "_highlights" }, .{ .hits = &hits, .state = &state }, null, .{ .state = &state });
    defer C.close(cursor);
    var page = try C.next(cursor, a, 1);
    defer page.deinit();
    try std.testing.expect(!(try page.rows[0].cell("payload")).sql_null);
    try std.testing.expectEqualStrings("fragment", (try page.rows[0].cell("_highlights")).value.string);
    try std.testing.expectEqual(@as(usize, 1), state.documents);
    try std.testing.expectEqual(@as(usize, 1), state.highlights);
}

test "SQL captured search cursor unwinds every preparation and hydration allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            var state: TestProvider.State = .{};
            const hits = [_]TestProvider.Hit{.{ .id = "a", .state = &state }};
            var result: TestProvider.Result = .{ .hits = &hits, .state = &state };
            var reader: TestProvider.Reader = .{ .state = &state };
            var lease: TestProvider.Lease = .{ .state = &state };
            var transferred = false;
            defer {
                if (!transferred) {
                    result.deinit();
                    reader.deinit();
                    lease.release();
                }
                std.debug.assert(state.results_closed == 1 and state.readers_closed == 1 and state.leases_closed == 1);
            }
            const C = Cursor(TestProvider);
            const cursor = try C.init(a, test_table, &.{ "text", "payload", "nullable_json", "n", "score", "_highlights" }, result, reader, lease);
            transferred = true;
            defer C.close(cursor);
            var page = C.next(cursor, a, 1) catch |err| {
                std.debug.assert(cursor.position == 0);
                return err;
            };
            defer page.deinit();
            try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL captured search cursor hydration frontier benchmark" {
    const a = std.testing.allocator;
    if (!try std.testing.environ.containsUnempty(a, "ANTFLY_SQL_SEARCH_BENCHMARK")) return error.SkipZigTest;
    const hits = try a.alloc(TestProvider.Hit, 10000);
    defer a.free(hits);
    const text: [512]u8 = @splat('x');
    for ([_]u32{ 1, 128, 10000 }) |demand| {
        var state: TestProvider.State = .{};
        for (hits) |*hit| hit.* = .{ .id = "a", .score = 1, .state = &state };
        var memory = @import("memory_budget.zig"){ .backing = a, .limit = 128 * 1024 * 1024 };
        const C = Cursor(TestProvider);
        const cursor = try C.init(memory.allocator(), test_table, &.{"text"}, .{ .hits = hits, .state = &state }, .{ .state = &state, .captured_text = &text }, .{ .state = &state });
        var open = true;
        defer if (open) C.close(cursor);
        const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        var page = try C.next(cursor, memory.allocator(), demand);
        const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - started;
        try std.testing.expectEqual(@as(usize, demand), state.reads);
        try std.testing.expectEqual(@as(usize, demand), page.rows.len);
        const peak = memory.peak;
        page.deinit();
        C.close(cursor);
        open = false;
        try std.testing.expectEqual(@as(usize, 0), memory.live);
        std.debug.print("SQL search hydration only: candidates=10000 demand={d} lookups={d} peak_bytes={d} elapsed_ns={d} ranking_io_and_frontier_excluded=true\n", .{ demand, state.reads, peak, elapsed });
    }
}
