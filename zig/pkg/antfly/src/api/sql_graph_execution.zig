// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native graph relations retain engine bindings until SQL has completed its
//! own projection, ordering and pagination. Graph and ordinary row cuts must
//! never be combined by opening independent queries.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const ast = local.sql_compiler.ast;
const db_types = local.storage_db_types;
const Adapter = @import("sql_execution.zig").Adapter;
const A = std.mem.Allocator;
const maximum_bindings = 10000;

pub fn open(adapter: *Adapter, a: A, requests: []const catalog.StatementScan) !?catalog.StatementRead {
    const has_graph = for (requests) |scan| {
        if (scan.request.graph != null) break true;
    } else false;
    if (!has_graph) return null;
    // Native graph execution has its own retained identity/index cut. A future
    // combined provider must explicitly attest the same cut for row scans.
    try requireStandalone(requests, adapter.active_transaction != null or adapter.dynamic_snapshot != null or adapter.pending_transaction_cut != null or adapter.range_reads != null or adapter.staged != null);
    if (adapter.read_requirements.after_write != null or adapter.read_requirements.visibility != null) return error.UnsupportedReadVisibility;
    const scan = requests[0];
    const graph = scan.request.graph.?;
    if (graph.start != .string or graph.start.string.len == 0 or graph.aliases.len < 2 or graph.aliases.len != graph.edge_types.len + 1) return error.InvalidSqlParameters;
    try adapter.verify(a, scan.table);
    const scope = scan.table.scope orelse return error.InvalidSqlBackendResponse;
    const logical = try (local.system_catalog_domain.Target{ .database = scope.database, .namespace = scope.namespace, .table = scope.name }).resourceNameAlloc(a);
    defer a.free(logical);
    const owner = try a.create(Owner);
    owner.* = .{ .a = a, .arena = .init(a), .adapter = adapter, .table = scan.table };
    errdefer Owner.close(owner);
    const retained = owner.arena.allocator();
    owner.fields = try retained.dupe([]const u8, scan.request.fields);
    for (owner.fields) |*field| field.* = try retained.dupe(u8, field.*);
    owner.aliases = try retained.dupe([]const u8, graph.aliases);
    for (owner.aliases) |*alias| alias.* = try retained.dupe(u8, alias.*);
    // Deep-own only the cursor metadata; immutable compiled descriptors expire
    // after execution setup and may not be retained by detached SQL cursors.
    owner.table = .{
        .id = scan.table.id,
        .schema_version = scan.table.schema_version,
        .physical_name = try retained.dupe(u8, scan.table.physical_name),
        .columns = &.{},
        .storage_mode = scan.table.storage_mode,
        .scope = .{ .database = try retained.dupe(u8, scope.database), .namespace = try retained.dupe(u8, scope.namespace), .name = try retained.dupe(u8, scope.name), .revision = scope.revision },
        .sql_access = scan.table.sql_access,
        .sql_native_rights = scan.table.sql_native_rights,
        .sql_required_privileges = scan.table.sql_required_privileges,
        .row_policy_purpose = scan.table.row_policy_purpose,
        .read_authority = if (@import("sql_read_authority.zig").context(scan.table)) |authority| try @import("sql_read_authority.zig").clone(retained, authority) else null,
    };
    const body = try requestJson(a, graph);
    defer a.free(body);
    const authority = @import("sql_read_authority.zig").context(scan.table);
    const identity = if (authority) |value| value.effective else adapter.identity.*;
    adapter.statistics_reads_started = true;
    var response = adapter.server.executeCatalogRetrievalQueryWithDelivery(a, try adapter.context.platformDeadline(), logical, body, identity, .{
        .ptr = owner,
        .start_fn = Owner.start,
        .write_fn = Owner.write,
        .native_graph_result = .{ .ptr = owner, .consume = Owner.consume },
    }) catch |err| {
        if (err == error.TableNotFound) {
            // A still-bound logical table with absent physical serving state
            // is availability. A dropped/replaced scope fails verification.
            try adapter.verify(a, scan.table);
            return failure(adapter.context, error.ReadUnavailable);
        }
        return failure(adapter.context, err);
    };
    defer response.deinit(a);
    try adapter.context.ensureActive();
    if (!owner.received) return error.UnsupportedSqlExecution;
    try adapter.verify(a, scan.table);
    owner.cursors[0] = .{ .ptr = owner, .next = Owner.next, .close = Owner.borrowedClose };
    return .{ .ptr = owner, .cursors = &owner.cursors, .close = Owner.close };
}

fn failure(context: local.api_operation.RequestContext, err: anyerror) anyerror {
    const normalized = @import("table_reads.zig").normalizeDistributedQueryOperationalError(err);
    return switch (normalized) {
        error.ReadUnavailable, error.DistributedQueryUnavailable => blk: {
            context.ensureActive() catch |expired| break :blk expired;
            break :blk error.SqlStatementReadUnavailable;
        },
        error.GraphWorkBudgetExceeded, error.GraphDistinctBudgetExceeded, error.GraphExploredEdgesBudgetExceeded, error.GraphExploredEdgeBytesBudgetExceeded, error.GraphMatchOperationLimitExceeded => error.SqlProgramLimitExceeded,
        error.GraphQueryModeUnsupported, error.GraphExternalAliasDocumentFilterUnsupported, error.GraphExternalAliasSourceUnsupported, error.GraphReverseVariablePathUnsupported, error.GraphStartIdentityRequired, error.GraphAnchorFilterRequiresIndex, error.InvalidRetrievalAgentRequest => error.UnsupportedSqlExecution,
        // Preserve authorization, missing indexes, corruption, OOM and unknown
        // decisions. A cancellation must never relabel an unknown outcome.
        else => err,
    };
}

fn requireStandalone(requests: []const catalog.StatementScan, explicit_cut: bool) !void {
    if (requests.len != 1 or requests[0].request.graph == null or explicit_cut) return error.SqlStatementSnapshotRequired;
    // A delegated view has both owner authority and caller restrictions. The
    // ordinary graph boundary currently binds one identity, so never silently
    // discard the caller's filter by substituting the effective owner here.
    if (requests[0].table.read_authority != null or requests[0].table.external_base_source != null) return error.UnsupportedSqlExecution;
}

fn validateResult(graph: db_types.GraphSearchResult, aliases: []const []const u8) !void {
    if (!std.mem.eql(u8, graph.name, "sql_graph")) return error.InvalidSqlBackendResponse;
    if (graph.truncated or graph.matches.len > maximum_bindings) return error.SqlProgramLimitExceeded;
    for (graph.matches) |match| {
        if (match.bindings.len != aliases.len or match.null_aliases.len != 0) return error.InvalidSqlBackendResponse;
        for (aliases) |alias| {
            var found: usize = 0;
            for (match.bindings) |binding| if (std.mem.eql(u8, binding.alias, alias)) {
                if (binding.node.key.len == 0 or !std.math.isFinite(binding.node.distance)) return error.InvalidSqlBackendResponse;
                found += 1;
            };
            if (found != 1) return error.InvalidSqlBackendResponse;
        }
    }
}

pub fn requestJson(a: A, graph: *const ast.Graph) ![]u8 {
    if (graph.start != .string or graph.aliases.len < 2 or graph.aliases.len != graph.edge_types.len + 1) return error.InvalidSqlParameters;
    var arena: std.heap.ArenaAllocator = .init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var nodes: std.json.ObjectMap = .empty;
    for (graph.aliases, 0..) |alias, i| {
        var node: std.json.ObjectMap = .empty;
        if (i == 0) {
            var filter: std.json.ObjectMap = .empty;
            var ids: std.array_list.Managed(std.json.Value) = .init(scratch);
            try ids.append(.{ .string = graph.start.string });
            try filter.put(scratch, "ids", .{ .array = ids });
            try node.put(scratch, "filter", .{ .object = filter });
        }
        try nodes.put(scratch, alias, .{ .object = node });
    }
    const Edge = struct { from: []const u8, to: []const u8, types: []const []const u8 };
    const edges = try scratch.alloc(Edge, graph.edge_types.len);
    for (edges, graph.edge_types, 0..) |*edge, edge_type, i| edge.* = .{ .from = graph.aliases[i], .to = graph.aliases[i + 1], .types = try scratch.dupe([]const u8, &.{edge_type}) };
    return std.json.Stringify.valueAlloc(a, .{
        .graph_queries = .{ .sql_graph = .{
            .index = graph.index,
            .match = .{ .anchor = graph.aliases[0], .nodes = std.json.Value{ .object = nodes }, .edges = edges },
            .@"return" = .{ .bindings = graph.aliases, .limit = maximum_bindings },
        } },
        .limit = 0,
    }, .{});
}

const Owner = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    adapter: *Adapter,
    table: catalog.Table,
    fields: [][]const u8 = &.{},
    aliases: [][]const u8 = &.{},
    rows: []const catalog.Row = &.{},
    position: usize = 0,
    received: bool = false,
    cursors: [1]catalog.Cursor = undefined,

    fn start(_: *anyopaque, _: usize) !void {}
    fn write(_: *anyopaque, _: []const u8) !void {}
    fn borrowedClose(_: *anyopaque) void {}
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.arena.deinit();
        self.a.destroy(self);
    }
    fn consume(raw: *anyopaque, results: []const db_types.GraphSearchResult) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.adapter.context.ensureActive();
        if (self.received or results.len != 1 or !std.mem.eql(u8, results[0].name, "sql_graph")) return error.InvalidSqlBackendResponse;
        const graph = results[0];
        // Exhaustive SQL semantics: a graph work/result ceiling is an error,
        // never a prefix that could change a later ORDER BY or WHERE result.
        try validateResult(graph, self.aliases);
        try self.adapter.context.ensureActive();
        const a = self.arena.allocator();
        const rows = try a.alloc(catalog.Row, graph.matches.len);
        for (graph.matches, rows, 0..) |match, *row, ordinal| {
            try self.adapter.context.ensureActive();
            var values: std.json.ObjectMap = .empty;
            for (self.fields) |field| {
                var parts = std.mem.splitScalar(u8, field, 0);
                const alias = parts.next() orelse return error.InvalidSqlBackendResponse;
                const name = parts.next() orelse return error.InvalidSqlBackendResponse;
                if (parts.next() != null) return error.InvalidSqlBackendResponse;
                const binding = for (match.bindings) |binding| {
                    if (std.mem.eql(u8, alias, binding.alias)) break binding.node;
                } else return error.InvalidSqlBackendResponse;
                const value: std.json.Value = if (std.mem.eql(u8, name, "key")) .{ .string = try a.dupe(u8, binding.key) } else if (std.mem.eql(u8, name, "depth")) .{ .integer = binding.depth } else if (std.mem.eql(u8, name, "distance")) .{ .float = binding.distance } else return error.InvalidSqlBackendResponse;
                try values.put(a, field, value);
            }
            row.* = .{ .id = try std.fmt.allocPrint(a, "{d:0>20}", .{ordinal}), .version = 0, .value = .{ .object = values } };
        }
        self.rows = rows;
        self.received = true;
    }
    fn next(raw: *anyopaque, _: A, limit: u32) !catalog.Page {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.adapter.context.ensureActive();
        try self.adapter.verify(self.a, self.table);
        if (limit == 0) return error.InvalidSqlParameters;
        const end = @min(self.rows.len, self.position +| limit);
        const rows = self.rows[self.position..end];
        self.position = end;
        return .{ .rows = rows, .after = if (end < self.rows.len) rows[rows.len - 1].id else null };
    }
};

test "SQL native graph admission rejects mixed snapshots and delegated filter substitution" {
    const graph: ast.Graph = .{ .index = "links", .start = .{ .string = "root" }, .aliases = &.{ "a", "b" }, .edge_types = &.{"cites"} };
    const table: catalog.Table = .{ .id = 1, .physical_name = "docs", .schema_version = 1, .columns = &.{} };
    var scans = [_]catalog.StatementScan{.{ .table = table, .request = .{ .graph = &graph, .fields = &.{}, .limit = 256 } }};
    try requireStandalone(&scans, false);
    try std.testing.expectError(error.SqlStatementSnapshotRequired, requireStandalone(&scans, true));
    const mixed = [_]catalog.StatementScan{ scans[0], .{ .table = table, .request = .{ .fields = &.{}, .limit = 256 } } };
    try std.testing.expectError(error.SqlStatementSnapshotRequired, requireStandalone(&mixed, false));
    var authority: u8 = 0;
    scans[0].table.read_authority = &authority;
    try std.testing.expectError(error.UnsupportedSqlExecution, requireStandalone(&scans, false));
    scans[0].table.read_authority = null;
    scans[0].table.external_base_source = .{
        .binding = .{ .table_id = "lake", .format = .parquet, .source_uri = "s3://example/rows", .schema_fingerprint = "schema" },
        .table_id = @constCast("lake"),
        .source_uri = @constCast("s3://example/rows"),
        .schema_fingerprint = @constCast("schema"),
    };
    try std.testing.expectError(error.UnsupportedSqlExecution, requireStandalone(&scans, false));
}

test "SQL native graph result rejects truncated malformed aliases and nonfinite distance" {
    var bindings = [_]db_types.GraphPatternBinding{
        .{ .alias = @constCast("a"), .node = .{ .key = @constCast("root"), .depth = 0, .distance = 0 } },
        .{ .alias = @constCast("b"), .node = .{ .key = @constCast("child"), .depth = 1, .distance = 1 } },
    };
    var matches = [_]db_types.GraphPatternMatch{.{ .bindings = &bindings, .path = &.{} }};
    var result: db_types.GraphSearchResult = .{ .name = @constCast("sql_graph"), .hits = &.{}, .total_hits = 1, .matches = &matches };
    const aliases = [_][]const u8{ "a", "b" };
    try validateResult(result, &aliases);
    result.truncated = true;
    try std.testing.expectError(error.SqlProgramLimitExceeded, validateResult(result, &aliases));
    result.truncated = false;
    bindings[1].alias = @constCast("a");
    try std.testing.expectError(error.InvalidSqlBackendResponse, validateResult(result, &aliases));
    bindings[1].alias = @constCast("c");
    try std.testing.expectError(error.InvalidSqlBackendResponse, validateResult(result, &aliases));
    bindings[1].alias = @constCast("b");
    bindings[1].node.distance = std.math.inf(f64);
    try std.testing.expectError(error.InvalidSqlBackendResponse, validateResult(result, &aliases));
    bindings[1].node.distance = std.math.nan(f64);
    try std.testing.expectError(error.InvalidSqlBackendResponse, validateResult(result, &aliases));
    bindings[1].node.distance = 1;
    matches[0].bindings = bindings[0..1];
    try std.testing.expectError(error.InvalidSqlBackendResponse, validateResult(result, &aliases));
    matches[0].bindings = &bindings;
    try validateResult(result, &aliases);
}

test "SQL native graph failures preserve typed authority corruption budgets cancellation and unknown outcomes" {
    const live: local.api_operation.RequestContext = .{};
    const expired: local.api_operation.RequestContext = .{ .deadline_ns = 0 };
    var stopped = std.atomic.Value(bool).init(true);
    const canceled: local.api_operation.RequestContext = .{ .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&stopped) };
    for ([_]anyerror{ error.ReadUnavailable, error.DistributedQueryUnavailable, error.TopologyChanged, error.ConnectionRefused }) |err| {
        try std.testing.expectEqual(error.SqlStatementReadUnavailable, failure(live, err));
        try std.testing.expectEqual(error.DeadlineExceeded, failure(expired, err));
        try std.testing.expectEqual(error.Canceled, failure(canceled, err));
    }
    for ([_]anyerror{ error.GraphWorkBudgetExceeded, error.GraphDistinctBudgetExceeded, error.GraphExploredEdgesBudgetExceeded, error.GraphExploredEdgeBytesBudgetExceeded }) |err| {
        try std.testing.expectEqual(error.SqlProgramLimitExceeded, failure(live, err));
    }
    for ([_]anyerror{ error.GraphQueryModeUnsupported, error.GraphAnchorFilterRequiresIndex, error.InvalidRetrievalAgentRequest }) |err| {
        try std.testing.expectEqual(error.UnsupportedSqlExecution, failure(live, err));
    }
    for ([_]local.api_operation.RequestContext{ live, expired, canceled }) |context| {
        for ([_]anyerror{ error.Forbidden, error.IndexNotFound, error.InvalidSqlBackendResponse, error.OutOfMemory, error.MetadataMutationOutcomeUnknown, error.SqlMutationOutcomeUnknown }) |err| {
            try std.testing.expectEqual(err, failure(context, err));
        }
    }
}

test "SQL native graph internal scoped handoff does not widen public literal target admission" {
    const a = std.testing.allocator;
    const Fake = struct {
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
    };
    var state: u8 = 0;
    var server = @import("http_server.zig").ApiHttpServer.init(a, .{}, .{ .ptr = &state, .vtable = &.{ .status = Fake.status } }, null, null);
    defer server.deinit();
    const target: local.system_catalog_domain.Target = .{ .database = "tenant", .namespace = "app", .table = "docs" };
    const key = try target.resourceNameAlloc(a);
    defer a.free(key);
    try std.testing.expectEqualStrings("tenant", (try local.system_catalog_domain.Target.parse(key)).database);
    try std.testing.expectError(error.InvalidCatalogName, server.executeCatalogRetrievalQuery(a, .{}, key, "{}", null));
}

test "SQL native graph original binding retains aliases hidden depth order and exact projected source fields" {
    const Fixture = struct {
        resolves: usize = 0,
        fn resolve(raw: *anyopaque, _: A, name: ast.Name, action: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.resolves += 1;
            try std.testing.expectEqual(catalog.Action.read, action);
            try std.testing.expectEqualStrings("usage_records", name.table);
            // The authorized physical schema deliberately has no key/depth
            // fields. They are owned by the native graph binding relation.
            return .{ .id = 41, .physical_name = "table:graph-source", .schema_version = 7, .columns = &.{.{ .name = "id", .path = "id", .type = .string }} };
        }
        fn scan(_: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedGraphRead;
        }
        fn mutate(_: *anyopaque, _: A, _: A, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.UnexpectedGraphMutation;
        }
        fn checkpoint(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const original = (try corpus.get("sql-1209")).sql;
    const parameterized = "MATCH (doc)-[:cites]->(target) WITH GRAPH docs_edge_graph ON usage_records START $1 RETURN doc.key AS source_id,target.key AS target_id ORDER BY target.depth ASC LIMIT 5";
    for ([_][]const u8{ original, parameterized }) |sql| {
        var fixture: Fixture = .{};
        const backend: catalog.Backend = .{ .ptr = &fixture, .supports_graph_relations = true, .vtable = &.{ .resolve = Fixture.resolve, .scan = Fixture.scan, .mutate = Fixture.mutate, .checkpoint = Fixture.checkpoint } };
        var compiled = try local.sql_compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        const bound = try local.sql_describe.bind(arena.allocator(), backend, &compiled, &.{});
        try std.testing.expectEqual(@as(usize, 2), bound.columns.len);
        try std.testing.expectEqualStrings("source_id", bound.columns[0].name);
        try std.testing.expectEqualStrings("target_id", bound.columns[1].name);
        for (bound.columns) |column| try std.testing.expectEqual(ast.ColumnType.string, column.type);
        try std.testing.expectEqual(@as(usize, 1), bound.order_keys.len);
        const scans = bound.relation.?.scans;
        try std.testing.expectEqual(@as(usize, 1), scans.len);
        try std.testing.expectEqual(@as(u64, 41), scans[0].table.id);
        try std.testing.expectEqual(@as(usize, 1), scans[0].table.columns.len);
        try std.testing.expect(scans[0].request.graph != null);
        try std.testing.expectEqual(@as(usize, 3), scans[0].request.fields.len);
        for ([_][]const u8{ "doc\x00key", "target\x00key", "target\x00depth" }) |expected| {
            var found: usize = 0;
            for (scans[0].request.fields) |field| if (std.mem.eql(u8, expected, field)) {
                found += 1;
            };
            try std.testing.expectEqual(@as(usize, 1), found);
        }
        if (compiled.parameter_count != 0) try std.testing.expectEqual(ast.ColumnType.string, bound.parameter_types[0].?);
        try std.testing.expect(fixture.resolves > 0);
    }
}

test "SQL native graph producer selection retains explicit native bindings while preserving public carrier gates" {
    const Response = local.api_query_response;
    const Sink = struct {
        fn start(_: *anyopaque, _: usize) !void {}
        fn write(_: *anyopaque, _: []const u8) !void {}
        fn public(_: *anyopaque, _: Response.ResultView) !void {}
        fn graph(_: *anyopaque, _: []const db_types.GraphSearchResult) !void {}
    };
    var state: u8 = 0;
    const public: Response.Delivery = .{ .ptr = &state, .start_fn = Sink.start, .write_fn = Sink.write, .typed_result = .{ .ptr = &state, .consume = Sink.public } };
    try std.testing.expect(Response.Delivery.forQuery(public, false, false) != null);
    try std.testing.expect(Response.Delivery.forQuery(public, true, false) == null);
    try std.testing.expect(Response.Delivery.forQuery(public, false, true) == null);
    try std.testing.expect(Response.Delivery.forQuery(public, true, true) == null);
    var native = public;
    native.typed_result = null;
    native.native_graph_result = .{ .ptr = &state, .consume = Sink.graph };
    for ([_]bool{ false, true }) |retained| {
        const selected = Response.Delivery.forQuery(native, retained, true) orelse return error.NativeGraphDeliveryLost;
        try std.testing.expect(selected.native_graph_result != null);
        try std.testing.expect(selected.ptr == native.ptr);
    }
    try std.testing.expect(Response.Delivery.forQuery(native, true, false) == null);
    try std.testing.expect(Response.Delivery.forQuery(null, false, true) == null);
}

test "SQL native graph primary identity domains intersect credential filters and preserve Boolean exclusions and thresholds" {
    const Case = struct { json: []const u8, ids: ?[]const []const u8 };
    const cases = [_]Case{
        .{ .json = "{\"bool\":{\"must\":[{\"doc_id\":[\"root\"]},{\"doc_id\":{\"ids\":[\"root\",\"a\",\"b\"]}}]}}", .ids = &.{"root"} },
        .{ .json = "{\"bool\":{\"must\":[{\"doc_id\":[\"root\"]},{\"doc_id\":{\"ids\":[\"a\",\"b\"]}}]}}", .ids = &.{} },
        .{ .json = "{\"conjuncts\":[{\"doc_id\":[\"a\",\"b\",\"c\"]},{\"doc_id\":[\"b\",\"c\",\"d\"]}]}", .ids = &.{ "b", "c" } },
        .{ .json = "{\"disjuncts\":[{\"conjuncts\":[{\"doc_id\":[\"a\",\"b\"]},{\"doc_id\":[\"b\",\"c\"]}]},{\"doc_id\":[\"z\"]}]}", .ids = &.{ "b", "z" } },
        .{ .json = "{\"bool\":{\"must\":[{\"doc_id\":[\"a\",\"b\",\"c\"]}],\"must_not\":[{\"doc_id\":[\"b\"]}]}}", .ids = &.{ "a", "c" } },
        .{ .json = "{\"bool\":{\"should\":[{\"doc_id\":[\"a\",\"b\"]},{\"doc_id\":[\"b\",\"c\"]},{\"doc_id\":[\"b\",\"d\"]}],\"minimum_should_match\":2}}", .ids = &.{"b"} },
        .{ .json = "{\"bool\":{\"should\":[{\"bool\":{\"must_not\":[{\"doc_id\":[\"a\"]}]}},{\"doc_id\":[\"a\",\"b\"]}],\"minimum_should_match\":2}}", .ids = &.{"b"} },
        .{ .json = "{\"conjuncts\":[{\"doc_id\":[\"a\",\"b\"]},{\"bool\":{\"must_not\":[{\"doc_id\":[\"a\"]}]}}]}", .ids = &.{"b"} },
        .{ .json = "{\"bool\":{\"must_not\":[{\"doc_id\":[\"a\"]}]}}", .ids = null },
        .{ .json = "{\"conjuncts\":[{\"doc_id\":[\"a\"]},{\"term\":{\"path\":\"/tenant\",\"term\":\"acme\"}}]}", .ids = null },
        .{ .json = "{\"bool\":{\"must\":[{\"doc_id\":[\"a\"]}],\"should\":[{\"term\":{\"path\":\"/tenant\",\"term\":\"acme\"}}],\"minimum_should_match\":0}}", .ids = null },
    };
    const a = std.testing.allocator;
    for (cases) |case| {
        const actual = try local.storage_db_query_search_exec.exactStructuredFilterDocIdsAlloc(a, case.json);
        defer if (actual) |ids| {
            for (ids) |id| a.free(id);
            a.free(ids);
        };
        if (case.ids) |expected| {
            const ids = actual orelse return error.FiniteIdentityDomainLost;
            try std.testing.expectEqual(expected.len, ids.len);
            for (expected) |id| {
                var count: usize = 0;
                for (ids) |found| if (std.mem.eql(u8, id, found)) {
                    count += 1;
                };
                try std.testing.expectEqual(@as(usize, 1), count);
            }
        } else try std.testing.expect(actual == null);
    }
}

test "SQL native graph primary identity Boolean resolution releases every failed allocation" {
    const Fixture = struct {
        fn run(a: A) !void {
            const ids = (try local.storage_db_query_search_exec.exactStructuredFilterDocIdsAlloc(a, "{\"bool\":{\"must\":[{\"doc_id\":[\"a\",\"b\",\"c\"]}],\"should\":[{\"doc_id\":[\"a\",\"b\"]},{\"doc_id\":[\"b\",\"c\"]}],\"minimum_should_match\":1,\"must_not\":[{\"doc_id\":[\"b\"]}]}}")) orelse return error.FiniteIdentityDomainLost;
            defer {
                for (ids) |id| a.free(id);
                a.free(ids);
            }
            try std.testing.expectEqual(@as(usize, 2), ids.len);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
