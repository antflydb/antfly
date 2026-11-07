// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Graph TVFs and ordinary aliases fork one captured owner cohort. Native
//! adjacency pages never reopen /query, and all endpoints pass the existing
//! SQL primary-read authorization/RLS path before becoming traversal state.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const ast = local.sql_compiler.ast;
const reads = local.api_table_read_source;
const Adapter = @import("sql_execution.zig").Adapter;
const A = std.mem.Allocator;
const pattern = local.graph_pattern;
const graph_mod = local.graph_graph;
const max_nodes = 10000;
const max_edges = 100000;
const max_state_bytes = 32 << 20;

const HitCursor = struct {
    owner: *Owner,
    table: catalog.Table,
    rows: []const catalog.Row,
    position: usize = 0,
    fn next(raw: *anyopaque, _: A, limit: u32) !catalog.Page {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.owner.adapter.context.ensureActive();
        try self.owner.adapter.verify(self.owner.alloc, self.table);
        if (limit == 0) return error.InvalidSqlParameters;
        const end = @min(self.rows.len, self.position +| limit);
        const result = self.rows[self.position..end];
        self.position = end;
        return .{ .rows = result, .after = if (end < self.rows.len) result[result.len - 1].id else null };
    }
};

fn cloneEdge(a: A, edge: graph_mod.Edge) !graph_mod.Edge {
    var copy = edge;
    const fields = .{ "source", "target", "edge_type", "edge_id", "owner_document", "metadata", "winner_key_hex" };
    var strings: usize = 0;
    errdefer inline for (fields, 0..) |field, position| {
        if (position < strings) a.free(@field(copy, field));
    };
    inline for (fields) |field| {
        @field(copy, field) = try a.dupe(u8, @field(edge, field));
        strings += 1;
    }
    return copy;
}

const Owner = struct {
    alloc: A,
    arena: std.heap.ArenaAllocator,
    adapter: *Adapter,
    snapshot: ?reads.RelationalStatementSnapshot = null,
    borrowed_snapshot: ?*reads.RelationalStatementSnapshot = null,
    ordinary: ?catalog.StatementRead = null,
    cursors: []catalog.Cursor = &.{},
    state_bytes: usize = 0,
    scanned_edges: usize = 0,

    fn cut(self: *@This()) *reads.RelationalStatementSnapshot {
        return self.borrowed_snapshot orelse &self.snapshot.?;
    }
    fn consume(self: *@This(), bytes: usize) !void {
        if (bytes > max_state_bytes -| self.state_bytes) return error.SqlProgramLimitExceeded;
        self.state_bytes += bytes;
    }
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.ordinary) |statement| statement.close(statement.ptr);
        if (self.snapshot) |snapshot| snapshot.deinit();
        self.arena.deinit();
        const alloc = self.alloc;
        alloc.destroy(self);
    }
    fn borrowedClose(_: *anyopaque) void {}

    fn verificationTable(self: *@This(), table: catalog.Table) !catalog.Table {
        return @import("sql_read_authority.zig").retainVerificationTable(self.arena.allocator(), table);
    }

    /// Each batch opens bounded exact physical points, never a table scan.
    /// The ordinary adapter injects effective credentials AND SQL policy proof;
    /// a missing/hidden intermediate cannot seed another expansion.
    fn authorize(self: *@This(), table: catalog.Table, keys: []const []const u8, known: *std.StringHashMapUnmanaged(bool)) !void {
        var scratch: std.heap.ArenaAllocator = .init(self.alloc);
        defer scratch.deinit();
        const a = scratch.allocator();
        var pending: std.ArrayList([]const u8) = .empty;
        var unique: std.StringHashMapUnmanaged(void) = .empty;
        for (keys) |key| {
            if (known.contains(key) or (try unique.getOrPut(a, key)).found_existing) continue;
            try pending.append(a, key);
        }
        if (known.count() + pending.items.len > max_nodes) return error.SqlProgramLimitExceeded;
        var offset: usize = 0;
        while (offset < pending.items.len) {
            try self.adapter.context.ensureActive();
            const batch = pending.items[offset..@min(offset + 64, pending.items.len)];
            var scans: [64]catalog.StatementScan = undefined;
            for (batch, scans[0..batch.len]) |key, *scan| scan.* = .{ .table = table, .request = .{ .fields = &.{}, .primary_key = key, .limit = 1, .primary_order = true } };
            const prior = self.adapter.dynamic_snapshot;
            const prior_table = self.adapter.dynamic_table;
            self.adapter.dynamic_snapshot = self.cut();
            self.adapter.dynamic_table = null;
            defer {
                self.adapter.dynamic_snapshot = prior;
                self.adapter.dynamic_table = prior_table;
            }
            const statement = try self.adapter.openDynamicStatement(self.alloc, scans[0..batch.len]);
            defer statement.close(statement.ptr);
            if (statement.cursors.len != batch.len) return error.InvalidSqlBackendResponse;
            for (batch, statement.cursors) |key, cursor| {
                // Release decoded row/policy payloads after each point. The
                // scratch arena retains only bounded key/index bookkeeping.
                var page = try cursor.next(cursor.ptr, self.alloc, 1);
                defer page.deinit();
                if (page.rows.len > 1 or (page.rows.len == 1 and !std.mem.eql(u8, page.rows[0].id, key))) return error.InvalidSqlBackendResponse;
                try self.consume(key.len + @sizeOf(bool) + 32);
                const owned_key = try self.arena.allocator().dupe(u8, key);
                try known.put(self.arena.allocator(), owned_key, page.rows.len == 1);
            }
            offset += batch.len;
        }
    }

    /// The matcher owns topology semantics. This adapter supplies only the
    /// captured adjacency and SQL-authorized node/producer visibility.
    const PatternReader = struct {
        owner: *Owner,
        table: catalog.Table,
        index: []const u8,
        known: *std.StringHashMapUnmanaged(bool),
        work: *pattern.WorkBudget,

        pub fn canonicalizeTable(self: @This(), table: ?[]const u8) ?[]const u8 {
            const name = table orelse return null;
            return if (std.mem.eql(u8, name, self.table.physical_name)) null else name;
        }
        pub fn routingIndexTable(self: @This(), table: ?[]const u8) ?[]const u8 {
            return table orelse self.table.physical_name;
        }
        pub fn freeEdges(_: @This(), a: A, edges: []graph_mod.Edge) void {
            graph_mod.GraphIndex.freeEdges(a, edges);
        }
        fn admit(raw: ?*anyopaque, a: A, nodes: []const local.graph_node_admission.NodeRef) ![]bool {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            var keys: std.ArrayList([]const u8) = .empty;
            defer keys.deinit(a);
            for (nodes) |node| {
                if (node.external or self.canonicalizeTable(node.table) != null) return error.UnsupportedSqlExecution;
                try keys.append(a, node.key);
            }
            try self.owner.authorize(self.table, keys.items, self.known);
            const visible = try a.alloc(bool, nodes.len);
            for (nodes, visible) |node, *value| value.* = self.known.get(node.key).?;
            return visible;
        }
        pub fn getEdgesBounded(self: @This(), a: A, table: ?[]const u8, key: []const u8, types: []const []const u8, direction: graph_mod.EdgeDirection, edge_limit: usize, byte_limit: usize) ![]graph_mod.Edge {
            if (self.canonicalizeTable(table) != null or direction != .out) return error.UnsupportedSqlExecution;
            // Both matcher node admission and adjacency producer checks use
            // this query-owned cache; never copy an unmanaged map head.
            var edges: std.ArrayList(graph_mod.Edge) = .empty;
            errdefer {
                for (edges.items) |edge| graph_mod.GraphIndex.freeEdge(a, edge);
                edges.deinit(a);
            }
            var owned_bytes: usize = 0;
            var continuation: ?reads.RelationalStatementSnapshot.GraphRead.graph.EdgeScanCursor = null;
            defer if (continuation) |*cursor| cursor.deinit(a);
            while (true) {
                try self.owner.adapter.context.ensureActive();
                var page = try self.owner.cut().graphPage(a, self.table.physical_name, 0, .{
                    .index = self.index,
                    .key = key,
                    .edge_types = types,
                    .direction = direction,
                    .cursor = continuation,
                    .limits = .{ .max_edges = 128, .max_owned_bytes = 1 << 20, .max_scanned_rows = 4096 },
                    .execution_deadline_ns = (try self.owner.adapter.context.platformDeadline()).deadline_ns,
                    .cancellation = self.owner.adapter.context.cancellation,
                });
                defer page.deinit(a);
                self.owner.scanned_edges +|= page.page.scanned_rows;
                if (self.owner.scanned_edges > max_edges or page.page.byte_limit_hit) return error.SqlProgramLimitExceeded;
                const groups = page.owner_documents orelse return error.SqlStatementSnapshotRequired;
                if (groups.len != page.page.edges.len) return error.InvalidSqlBackendResponse;
                try self.work.consumePhysicalEdges(page.page.scanned_rows);
                // The matcher charges returned public edge payloads. Producer
                // facts and filtered edges never reach it, so charge them here.
                var private_bytes: usize = 0;
                for (page.page.edges, groups) |edge, owners| {
                    private_bytes +|= edge.edge_id.len +| edge.owner_document.len +| edge.winner_key_hex.len;
                    private_bytes +|= owners.len *| @sizeOf([]u8);
                    for (owners) |producer| private_bytes +|= producer.len;
                }
                try self.work.consumeEdgeBytes(private_bytes);
                var producers: std.ArrayList([]const u8) = .empty;
                defer producers.deinit(a);
                for (page.page.edges, groups) |edge, owners| {
                    if (!std.mem.eql(u8, edge.source, key) or owners.len == 0) return error.InvalidSqlBackendResponse;
                    const expected_type = for (types) |kind| {
                        if (std.mem.eql(u8, kind, edge.edge_type)) break true;
                    } else types.len == 0;
                    if (!expected_type) return error.InvalidSqlBackendResponse;
                    try producers.appendSlice(a, owners);
                }
                try self.owner.authorize(self.table, producers.items, self.known);
                for (page.page.edges, groups) |edge, owners| {
                    const visible = for (owners) |producer| {
                        if (self.known.get(producer).?) break true;
                    } else false;
                    if (!visible) {
                        try self.work.consumeMaterializedEdges(&.{edge});
                        continue;
                    }
                    if (edges.items.len >= @min(edge_limit, self.work.edgeLimit())) return error.GraphExploredEdgesBudgetExceeded;
                    var bytes: usize = @sizeOf(graph_mod.Edge);
                    inline for (.{ "source", "target", "edge_type", "edge_id", "owner_document", "metadata", "winner_key_hex" }) |field| bytes +|= @field(edge, field).len;
                    if (bytes > @min(byte_limit, self.work.edgeByteLimit()) -| owned_bytes) return error.GraphExploredEdgeBytesBudgetExceeded;
                    const copy = try cloneEdge(a, edge);
                    errdefer graph_mod.GraphIndex.freeEdge(a, copy);
                    try edges.append(a, copy);
                    owned_bytes += bytes;
                }
                if (continuation) |*cursor| cursor.deinit(a);
                continuation = page.page.next_cursor;
                page.page.next_cursor = null;
                if (continuation == null) break;
            }
            return edges.toOwnedSlice(a);
        }
    };

    fn fullBindings(self: *@This(), scan: catalog.StatementScan) ![]const catalog.Row {
        const specification = scan.request.graph.?;
        if (specification.start != .string or specification.start.string.len == 0 or specification.aliases.len < 2 or specification.aliases.len != specification.edge_types.len + 1) return error.InvalidSqlParameters;
        var scratch: std.heap.ArenaAllocator = .init(self.alloc);
        defer scratch.deinit();
        const a = scratch.allocator();
        const nodes = try a.alloc(pattern.MatchNode, specification.aliases.len);
        for (nodes, specification.aliases) |*node, alias| node.* = .{ .alias = alias };
        const edges = try a.alloc(pattern.MatchEdge, specification.edge_types.len);
        for (edges, specification.edge_types, 0..) |*edge, kind, i| edge.* = .{ .from = specification.aliases[i], .to = specification.aliases[i + 1], .step = .{ .types = try a.dupe([]const u8, &.{kind}) } };
        var known: std.StringHashMapUnmanaged(bool) = .empty;
        var budget = pattern.WorkBudget.initWithLimits(.{ .max_explored_nodes = max_nodes, .max_explored_edges = max_edges, .max_explored_edge_bytes = max_state_bytes, .max_intermediate_states = max_nodes, .max_retained_state_bytes = max_state_bytes });
        var reader: PatternReader = .{ .owner = self, .table = scan.table, .index = specification.index, .known = &known, .work = &budget };
        // Pattern inputs borrow the temporary arena; matcher state and
        // adjacency pages must honor frees throughout bounded traversal.
        const matches = try pattern.matchConjunctivePatternWithEdgeReader(self.alloc, reader, &.{specification.start.string}, .{
            .anchor_alias = specification.aliases[0],
            .nodes = nodes,
            .edges = edges,
        }, .{
            .owning_table = scan.table.physical_name,
            .require_complete = true,
            .max_results = 0,
            .return_aliases = specification.aliases,
            .include_paths = false,
            .node_admission = .{ .ctx = &reader, .filter_many = PatternReader.admit },
            .work_budget = &budget,
            .max_explored_nodes = max_nodes,
            .max_explored_edges = max_edges,
            .max_intermediate_states = max_nodes,
        });
        defer pattern.freeMatches(self.alloc, matches);
        return self.projectBindings(scan.request.fields, matches);
    }

    fn projectBindings(self: *@This(), fields: []const []const u8, matches: []const pattern.PatternMatch) ![]const catalog.Row {
        if (matches.len > max_nodes) return error.SqlProgramLimitExceeded;
        const retained = self.arena.allocator();
        const rows = try retained.alloc(catalog.Row, matches.len);
        for (matches, rows, 0..) |match, *row, ordinal| {
            var values: std.json.ObjectMap = .empty;
            for (fields) |field| {
                var parts = std.mem.splitScalar(u8, field, 0);
                const alias = parts.next() orelse return error.InvalidSqlBackendResponse;
                const name = parts.next() orelse return error.InvalidSqlBackendResponse;
                if (parts.next() != null) return error.InvalidSqlBackendResponse;
                const binding = for (match.bindings) |binding| {
                    if (std.mem.eql(u8, alias, binding.alias)) break binding;
                } else return error.InvalidSqlBackendResponse;
                const value: std.json.Value = if (std.mem.eql(u8, name, "key")) .{ .string = try retained.dupe(u8, binding.key) } else if (std.mem.eql(u8, name, "depth")) .{ .integer = binding.depth } else if (std.mem.eql(u8, name, "distance")) .{ .float = @floatFromInt(binding.depth) } else return error.InvalidSqlBackendResponse;
                try self.consume(field.len + binding.key.len + 64);
                try values.put(retained, try retained.dupe(u8, field), value);
            }
            row.* = .{ .id = try std.fmt.allocPrint(retained, "{d:0>20}", .{ordinal}), .version = 0, .value = .{ .object = values } };
        }
        return rows;
    }

    const State = struct { key: []const u8, selected: ?[]const u8 = null };
    fn graph(self: *@This(), scan: catalog.StatementScan) ![]const catalog.Row {
        const specification = scan.request.graph orelse return error.InvalidSqlBackendResponse;
        const selected_alias = specification.hit_alias orelse return self.fullBindings(scan);
        if (specification.start != .string or specification.start.string.len == 0 or specification.aliases.len != specification.edge_types.len + 1) return error.InvalidSqlParameters;
        const selection = for (specification.aliases, 0..) |alias, index| {
            if (std.mem.eql(u8, alias, selected_alias)) break index;
        } else return error.InvalidSqlBackendResponse;
        const a = self.arena.allocator();
        var known: std.StringHashMapUnmanaged(bool) = .empty;
        try self.authorize(scan.table, &.{specification.start.string}, &known);
        if (!known.get(specification.start.string).?) return &.{};
        var frontier: std.ArrayList(State) = .empty;
        const anchor = try a.dupe(u8, specification.start.string);
        try frontier.append(a, .{ .key = anchor, .selected = if (selection == 0) anchor else null });
        for (specification.edge_types, 0..) |edge_type, depth| {
            var next: std.ArrayList(State) = .empty;
            for (frontier.items) |state| {
                var continuation: ?reads.RelationalStatementSnapshot.GraphRead.graph.EdgeScanCursor = null;
                defer if (continuation) |*cursor| cursor.deinit(self.alloc);
                while (true) {
                    try self.adapter.context.ensureActive();
                    var page = try self.cut().graphPage(self.alloc, scan.table.physical_name, 0, .{
                        .index = specification.index,
                        .key = state.key,
                        .edge_types = &.{edge_type},
                        .direction = .out,
                        .cursor = continuation,
                        .limits = .{ .max_edges = 128, .max_owned_bytes = 1 << 20, .max_scanned_rows = 4096 },
                        .execution_deadline_ns = (try self.adapter.context.platformDeadline()).deadline_ns,
                        .cancellation = self.adapter.context.cancellation,
                    });
                    defer page.deinit(self.alloc);
                    self.scanned_edges +|= page.page.scanned_rows;
                    if (self.scanned_edges > max_edges or page.page.byte_limit_hit) return error.SqlProgramLimitExceeded;
                    const groups = page.owner_documents orelse return error.SqlStatementSnapshotRequired;
                    if (groups.len != page.page.edges.len) return error.InvalidSqlBackendResponse;
                    var targets: std.ArrayList([]const u8) = .empty;
                    defer targets.deinit(self.alloc);
                    for (page.page.edges, groups) |edge, owners| {
                        if (!std.mem.eql(u8, edge.source, state.key) or !std.mem.eql(u8, edge.edge_type, edge_type) or owners.len == 0) return error.InvalidSqlBackendResponse;
                        try targets.append(self.alloc, edge.target);
                        // These are document producer keys, not graph-node or
                        // entity aliases. Read actual rows at this same cut.
                        try targets.appendSlice(self.alloc, owners);
                    }
                    try self.authorize(scan.table, targets.items, &known);
                    for (page.page.edges, groups) |edge, owners| {
                        if (!known.get(edge.target).?) continue;
                        const producer_visible = for (owners) |owner| {
                            if (known.get(owner).?) break true;
                        } else false;
                        if (!producer_visible) continue;
                        if (next.items.len >= max_nodes) return error.SqlProgramLimitExceeded;
                        try self.consume(edge.target.len + @sizeOf(State));
                        const key = try a.dupe(u8, edge.target);
                        try next.append(a, .{ .key = key, .selected = if (selection == depth + 1) key else state.selected });
                    }
                    if (continuation) |*cursor| cursor.deinit(self.alloc);
                    continuation = page.page.next_cursor;
                    page.page.next_cursor = null;
                    if (continuation == null) break;
                }
            }
            frontier = next;
        }
        var unique: std.StringHashMapUnmanaged(void) = .empty;
        var rows: std.ArrayList(catalog.Row) = .empty;
        for (frontier.items) |state| {
            const key = state.selected orelse return error.InvalidSqlBackendResponse;
            if ((try unique.getOrPut(a, key)).found_existing) continue;
            var values: std.json.ObjectMap = .empty;
            try values.put(a, "id", .{ .string = key });
            try values.put(a, "score", .null);
            try values.put(a, "graph_name", .{ .string = "graph_match" });
            try rows.append(a, .{ .id = key, .version = 0, .value = .{ .object = values } });
        }
        return rows.toOwnedSlice(a);
    }
};

pub fn open(adapter: *Adapter, alloc: A, requests: []const catalog.StatementScan) !catalog.StatementRead {
    if (requests.len == 0 or requests.len > 64 or (adapter.staged != null and adapter.staged.?.tables.len != 0)) return error.SqlStatementSnapshotRequired;
    // A transaction without graph-generation guards must never silently gain
    // graph reads merely because an immutable primary root is available.
    if (adapter.active_transaction != null or adapter.range_reads != null) return error.SqlRangeTrackingRequired;
    if (adapter.read_requirements.after_write != null or adapter.read_requirements.visibility != null) return error.UnsupportedReadVisibility;
    const owner = try alloc.create(Owner);
    owner.* = .{ .alloc = alloc, .arena = .init(alloc), .adapter = adapter };
    errdefer Owner.close(owner);
    const a = owner.arena.allocator();
    var targets: std.ArrayList(reads.RelationalSnapshotTarget) = .empty;
    for (requests) |request| {
        try adapter.verify(a, request.table);
        if (request.table.external_base_source != null or request.request.search != null) return error.UnsupportedSqlExecution;
        const position = for (targets.items, 0..) |target, index| {
            if (std.mem.eql(u8, target.table, request.table.physical_name)) break index;
        } else blk: {
            try targets.append(a, .{ .relation_owner_minter = try @import("sql_relation_lifetimes.zig").ownerMinterForRelation(adapter.server, adapter.context, request.table.sql_persistence), .table = request.table.physical_name, .schema_version = request.table.schema_version, .table_id = request.table.id });
            break :blk targets.items.len - 1;
        };
        if (request.request.graph) |graph| {
            const prior = targets.items[position].graph_indexes;
            const exists = for (prior) |name| {
                if (std.mem.eql(u8, name, graph.index)) break true;
            } else false;
            if (!exists) {
                const names = try a.alloc([]const u8, prior.len + 1);
                @memcpy(names[0..prior.len], prior);
                names[prior.len] = graph.index;
                targets.items[position].graph_indexes = names;
            }
        }
    }
    if (adapter.dynamic_snapshot) |snapshot| owner.borrowed_snapshot = snapshot else {
        const source = adapter.server.table_reads orelse return error.SqlStatementSnapshotRequired;
        adapter.statistics_reads_started = true;
        owner.snapshot = try source.openRelationalSnapshotCohort(alloc, targets.items, .read_index, adapter.context.cancellation, (try adapter.context.platformDeadline()).deadline_ns);
    }
    owner.cursors = try a.alloc(catalog.Cursor, requests.len);
    var ordinary: std.ArrayList(catalog.StatementScan) = .empty;
    var positions: std.ArrayList(usize) = .empty;
    for (requests, owner.cursors, 0..) |request, *cursor, index| {
        if (request.request.graph != null) {
            const retained = try a.create(HitCursor);
            retained.* = .{ .owner = owner, .table = try owner.verificationTable(request.table), .rows = try owner.graph(request) };
            cursor.* = .{ .ptr = retained, .next = HitCursor.next, .close = Owner.borrowedClose };
        } else {
            try ordinary.append(a, request);
            try positions.append(a, index);
        }
    }
    if (ordinary.items.len != 0) {
        const prior = adapter.dynamic_snapshot;
        const prior_table = adapter.dynamic_table;
        adapter.dynamic_snapshot = owner.cut();
        adapter.dynamic_table = null;
        defer {
            adapter.dynamic_snapshot = prior;
            adapter.dynamic_table = prior_table;
        }
        owner.ordinary = try adapter.openDynamicStatement(alloc, ordinary.items);
        if (owner.ordinary.?.cursors.len != positions.items.len) return error.InvalidSqlBackendResponse;
        for (positions.items, owner.ordinary.?.cursors) |position, cursor| owner.cursors[position] = cursor;
    }
    return .{ .ptr = owner, .cursors = owner.cursors, .close = Owner.close };
}

test "SQL native graph retained canonical bindings preserve all aliases depth and owned edge provenance" {
    const a = std.testing.allocator;
    var source: std.heap.ArenaAllocator = .init(a);
    const temporary = source.allocator();
    var edge: graph_mod.Edge = .{ .source = try temporary.dupe(u8, "a"), .target = try temporary.dupe(u8, "b"), .edge_type = try temporary.dupe(u8, "R"), .weight = 1, .created_at = 2, .updated_at = 3, .metadata = "", .origin_group_id = 72, .winner_rank = 19 };
    edge.owner_document = try temporary.dupe(u8, "fact-owner");
    const retained_edge = try cloneEdge(a, edge);
    defer graph_mod.GraphIndex.freeEdge(a, retained_edge);
    source.deinit();
    try std.testing.expectEqualStrings("fact-owner", retained_edge.owner_document);
    try std.testing.expectEqual(@as(u64, 72), retained_edge.origin_group_id);
    try std.testing.expectEqual(edge.winner_rank, retained_edge.winner_rank);
    var owner: Owner = .{ .alloc = a, .arena = .init(a), .adapter = undefined };
    defer owner.arena.deinit();
    var bindings = [_]pattern.PatternBinding{
        .{ .alias = @constCast("a"), .key = @constCast("first"), .depth = 0 },
        .{ .alias = @constCast("b"), .key = @constCast("second"), .depth = 1 },
    };
    const matches = [_]pattern.PatternMatch{.{ .bindings = &bindings, .path = &.{} }};
    const rows = try owner.projectBindings(&.{ "a\x00key", "b\x00key", "b\x00depth", "b\x00distance" }, &matches);
    try std.testing.expectEqual(@as(usize, 1), rows.len);
    try std.testing.expectEqualStrings("first", rows[0].value.object.get("a\x00key").?.string);
    try std.testing.expectEqualStrings("second", rows[0].value.object.get("b\x00key").?.string);
    try std.testing.expectEqual(@as(i64, 1), rows[0].value.object.get("b\x00depth").?.integer);
    try std.testing.expectEqual(@as(f64, 1), rows[0].value.object.get("b\x00distance").?.float);
}

test "SQL native graph producer authorization releases each decoded point page before the next key" {
    const Tracker = struct {
        live: usize = 0,
        fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const memory = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
            self.live += len;
            return memory;
        }
        fn release(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.live -= memory.len;
            std.testing.allocator.rawFree(memory, alignment, ra);
        }
        fn allocator(self: *@This()) A {
            return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = A.noResize, .remap = A.noRemap, .free = release } };
        }
    };
    const View = reads.RelationalReadView;
    const Fake = struct {
        tracker: *Tracker,
        pages: usize = 0,
        opens: usize = 0,
        closes: usize = 0,
        baseline: usize = 0,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn catalogRead(_: *anyopaque, a: A, _: local.api_operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            if (call == .policy_publication_status) return a.dupe(u8, "null");
            if (call != .resolve_many) return error.UnexpectedGraphMetadataRead;
            return std.json.Stringify.valueAlloc(a, .{ .revision = 2, .tables = .{.{ .table_id = 7, .name = "physical" }}, .logical_names = .{"docs"} }, .{});
        }
        fn open(raw: *anyopaque, _: A, scan: reads.RelationalStatementScan) !View {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqualStrings("physical", scan.table);
            try std.testing.expect(scan.from.len != 0);
            self.opens += 1;
            return .{ .ptr = self, .vtable = &.{ .next = next, .close = closeView } };
        }
        fn next(raw: *anyopaque, a: A, limit: u32) !View.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u32, 1), limit);
            if (self.pages == 0) self.baseline = self.tracker.live;
            // The prior decoded payload must already be gone. Allow bounded
            // key/cache bookkeeping, not accumulation of the 256KiB pages.
            try std.testing.expect(self.tracker.live <= self.baseline + 64 * 1024);
            self.pages += 1;
            var arena: std.heap.ArenaAllocator = .init(a);
            errdefer arena.deinit();
            const decoded = try arena.allocator().alloc(u8, 256 * 1024);
            @memset(decoded, 0);
            return .{ .arena = arena, .rows = &.{}, .after = null };
        }
        fn closeView(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closes += 1;
        }
        fn closeSnapshot(_: *anyopaque) void {}
    };
    var tracker: Tracker = .{};
    var fake: Fake = .{ .tracker = &tracker };
    const http = @import("http_server.zig");
    var server = http.ApiHttpServer.init(std.testing.allocator, .{}, .{ .ptr = &fake, .vtable = &.{ .status = Fake.status, .system_catalog = Fake.catalogRead } }, null, null);
    defer server.deinit();
    var identity: ?http.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
    var snapshot: reads.RelationalStatementSnapshot = .{ .ptr = &fake, .vtable = &.{ .open = Fake.open, .close = Fake.closeSnapshot } };
    const a = tracker.allocator();
    {
        var owner: Owner = .{ .alloc = a, .arena = .init(a), .adapter = &adapter, .borrowed_snapshot = &snapshot };
        defer owner.arena.deinit();
        var keys: [64][]const u8 = undefined;
        for (&keys, 0..) |*key, i| key.* = try std.fmt.allocPrint(owner.arena.allocator(), "producer-{d}", .{i});
        var known: std.StringHashMapUnmanaged(bool) = .empty;
        const table: catalog.Table = .{ .id = 7, .physical_name = "physical", .schema_version = 1, .columns = &.{}, .scope = .{ .database = "main", .namespace = "public", .name = "docs", .revision = 2 } };
        try owner.authorize(table, &keys, &known);
        try std.testing.expectEqual(@as(usize, 64), fake.pages);
        try std.testing.expectEqual(fake.opens, fake.closes);
        try std.testing.expectEqual(@as(u32, 64), known.count());
        for (keys) |key| try std.testing.expect(!known.get(key).?);
    }
    try std.testing.expectEqual(@as(usize, 0), tracker.live);
}

test "SQL native graph all producer-denied pages consume physical edge and decoded byte budgets" {
    const Page = reads.RelationalStatementSnapshot.GraphRead.Page;
    const Request = reads.RelationalStatementSnapshot.GraphRead.Request;
    const Fake = struct {
        pages: usize = 0,
        fn open(_: *anyopaque, _: A, _: reads.RelationalStatementScan) !reads.RelationalReadView {
            return error.UnexpectedPrimaryReadForCachedDenial;
        }
        fn close(_: *anyopaque) void {}
        fn graphPage(raw: *anyopaque, a: A, table: []const u8, group: u64, request: Request) !Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqualStrings("physical", table);
            try std.testing.expectEqual(@as(u64, 0), group);
            try std.testing.expectEqualStrings("a", request.key);
            self.pages += 1;
            var metadata: [600]u8 = undefined;
            @memset(&metadata, 'x');
            var source_edges = [_]graph_mod.Edge{.{ .source = "a", .target = "b", .edge_type = "R", .metadata = &metadata, .weight = 1, .created_at = 1, .updated_at = 1 }};
            var producers = [_][]u8{@constCast("hidden-producer")};
            var groups = [_][][]u8{&producers};
            const borrowed: Page = .{ .proof = .{ .source_sequence = 1, .source_graph_sequence = 1, .applied_sequence = 1, .config_hash = 1, .graph_generation = 1, .reverse_generation = 1, .owner_generation = 1, .ownership_fingerprint = 1, .ttl_read_at_ns = 1 }, .page = .{ .edges = &source_edges, .scanned_rows = 1 }, .owner_documents = &groups };
            var result = try borrowed.clone(a);
            errdefer result.deinit(a);
            if (self.pages < 3) {
                const kind = try a.dupe(u8, "R");
                errdefer a.free(kind);
                result.page.next_cursor = .{ .direction = .out, .type_index = 0, .edge_type = kind, .adjacent_key = try std.fmt.allocPrint(a, "page-{d}", .{self.pages}) };
            }
            return result;
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |byte_limited| {
        var fake: Fake = .{};
        var adapter: Adapter = .{ .server = undefined, .identity = undefined, .context = .{} };
        var snapshot: reads.RelationalStatementSnapshot = .{ .ptr = &fake, .vtable = &.{ .open = Fake.open, .graph_page = Fake.graphPage, .close = Fake.close } };
        var owner: Owner = .{ .alloc = a, .arena = .init(a), .adapter = &adapter, .borrowed_snapshot = &snapshot };
        defer owner.arena.deinit();
        var known: std.StringHashMapUnmanaged(bool) = .empty;
        defer known.deinit(a);
        // This is a retained negative primary authorization, never an empty
        // policy or a fabricated grant. No denied fact may produce an edge.
        try known.put(a, "hidden-producer", false);
        var budget = pattern.WorkBudget.initWithLimits(.{ .max_explored_nodes = 10, .max_explored_edges = if (byte_limited) 10 else 1, .max_explored_edge_bytes = if (byte_limited) 1024 else 10000 });
        const table: catalog.Table = .{ .id = 7, .physical_name = "physical", .schema_version = 1, .columns = &.{} };
        const reader: Owner.PatternReader = .{ .owner = &owner, .table = table, .index = "g", .known = &known, .work = &budget };
        try std.testing.expectError(error.GraphWorkBudgetExceeded, reader.getEdgesBounded(a, null, "a", &.{"R"}, .out, budget.edgeLimit(), budget.edgeByteLimit()));
        try std.testing.expectEqual(@as(usize, 2), fake.pages);
        const expected_dimension: @TypeOf(budget.exhaustion().?.dimension) = if (byte_limited) .explored_edge_bytes else .explored_edges;
        try std.testing.expectEqual(expected_dimension, budget.exhaustion().?.dimension);
        try std.testing.expect(!known.get("hidden-producer").?);
    }
}
