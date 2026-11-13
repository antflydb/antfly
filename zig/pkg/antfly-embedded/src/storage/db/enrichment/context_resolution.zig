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

//! Resolve bound context sources into a rendered, fingerprinted producer
//! input plus the lineage needed to publish it.
//!
//! Reads go through a host `Reader` so the same resolution runs in embedded,
//! standalone, and distributed runtimes: the resident DB serves its own table,
//! and hosts that own a table registry (Lite's table handles, the server's
//! routing-aware `TableReadSource`) serve foreign table identities under the
//! enrichment's execution identity. A host that cannot serve a table returns
//! `error.ContextSourceTableUnavailable`, which surfaces as explicit retry or
//! terminal debt; context is never silently dropped.
//!
//! Query sources are executed as a recall-safe *prefilter* (string conjuncts
//! as analyzed AND matches; numeric/boolean conjuncts omitted) and every
//! returned row is re-verified against the exact bound predicate. Prefilter
//! pages continue until the host reports exhaustion, so a verified member
//! set is complete or the resolution fails with `ContextBudgetExceeded`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ctx = @import("context_sources.zig");
const store = @import("context_dependency_store.zig");

pub const Row = struct {
    key: []u8,
    /// JSON document.
    value: []u8,
};

pub const Page = struct {
    rows: []Row,
    /// True when the host has no rows after this page.
    exhausted: bool,

    pub fn deinit(self: *Page, alloc: Allocator) void {
        for (self.rows) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const Reader = struct {
    ptr: *anyopaque,
    /// Immutable identity of the enrichment's own table ("" when the host has
    /// no table identities, e.g. a standalone single-table DB).
    own_table_id: []const u8 = "",
    lookup_fn: *const fn (ptr: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8) anyerror!?[]u8,
    query_fn: *const fn (ptr: *anyopaque, alloc: Allocator, table_id: []const u8, filter_query_json: []const u8, offset: u32, limit: u32) anyerror!Page,
    /// Opaque position the subsequent reads are at least as fresh as.
    frontier_fn: ?*const fn (ptr: *anyopaque, alloc: Allocator, table_id: []const u8) anyerror![]u8 = null,
    /// Direct (non-generated) adjacency of one node, at most `limit` edges.
    edges_fn: ?*const fn (ptr: *anyopaque, alloc: Allocator, table_id: []const u8, graph_index: []const u8, node: []const u8, edge_types: []const []const u8, direction: ctx.GraphSpec.Direction, options: EdgeOptions) anyerror![]Edge = null,
    /// Current values of a row's generated artifacts as a JSON object
    /// `{"<artifact>": value|null}` (asset: parsed JSON or text; chunk:
    /// array of payloads; embedding: vector).
    artifacts_fn: ?*const fn (ptr: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8, names: []const []const u8) anyerror![]u8 = null,
    /// Ranked retrieval (full-text relevance or vector k-NN), rank order.
    search_fn: ?*const fn (ptr: *anyopaque, alloc: Allocator, table_id: []const u8, query: SearchQuery) anyerror!Page = null,

    pub fn isOwn(self: Reader, table_id: []const u8) bool {
        return table_id.len == 0 or std.mem.eql(u8, table_id, self.own_table_id);
    }

    /// Identity recorded in lineage: the canonical id even for own-table reads.
    pub fn identity(self: Reader, table_id: []const u8) []const u8 {
        return if (table_id.len == 0) self.own_table_id else table_id;
    }
};

pub const EdgeOptions = struct {
    limit: u32,
    /// Include field-derived and enrichment-generated edges.
    include_generated: bool = false,
};

pub const Edge = struct {
    edge_type: []u8,
    neighbor: []u8,
    outgoing: bool,
};

pub fn freeEdges(alloc: Allocator, edges: []Edge) void {
    for (edges) |e| {
        alloc.free(e.edge_type);
        alloc.free(e.neighbor);
    }
    alloc.free(edges);
}

pub const SearchQuery = struct {
    text: []const u8 = "",
    text_field: []const u8 = "",
    vector: []const f32 = &.{},
    vector_index: []const u8 = "",
    k: u32,
};

pub const Limits = struct {
    page_rows: u32 = 256,
    /// Prefilter rows examined per source, as a multiple of max_documents
    /// plus a floor. Exceeding it fails explicitly instead of truncating.
    scan_factor: u32 = 8,
    scan_floor: u32 = 1024,
};

pub const Resolution = struct {
    arena: std.heap.ArenaAllocator,
    rendered: ctx.Rendered,
    inputs: []const store.ManifestInput,
    queries: []const store.ManifestQuery,
    absent: []const store.ManifestInput,
    /// `{"<table_id>":"<position>",...}` sorted by table id.
    frontier: []const u8,
    /// Exact subscription set after reading: bound tuples plus nodes and
    /// members discovered while traversing, with residual values updated
    /// (top-N boundaries). Register before publishing.
    tuples: []const ctx.SubscriptionTuple,
    /// Some consumed source is a snapshot (publish as `unverified`).
    snapshot: bool,
    refresh_interval_ms: u64,

    pub fn deinit(self: *Resolution, alloc: Allocator) void {
        self.rendered.deinit(alloc);
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Recall-safe prefilter for a bound query. Returns `{"match_all":{}}` when
/// no conjunct can be pushed down (verification still enforces equality).
pub fn prefilterJsonAlloc(alloc: Allocator, bound: *const ctx.Bound) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var pushed: usize = 0;
    try out.appendSlice(alloc, "{\"bool\":{\"filter\":[");
    for (bound.source.conjuncts, bound.values) |c, set| {
        // Artifact values are not in the table's text index.
        if (!c.isEquality() or ctx.artifactOfField(c.field) != null) continue;
        var strings: usize = 0;
        for (set) |v| {
            if (v.len > 0 and v[0] == '"') strings += 1;
        }
        // Mixed or non-string sets cannot be pushed down safely.
        if (strings == 0 or strings != set.len) continue;
        if (pushed != 0) try out.append(alloc, ',');
        pushed += 1;
        if (set.len > 1) try out.appendSlice(alloc, "{\"bool\":{\"should\":[");
        for (set, 0..) |v, i| {
            if (i != 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, "{\"match\":{\"field\":");
            try ctx.appendJsonString(alloc, &out, c.field);
            try out.appendSlice(alloc, ",\"text\":");
            try out.appendSlice(alloc, v);
            try out.appendSlice(alloc, "}}");
        }
        if (set.len > 1) try out.appendSlice(alloc, "]}}");
    }
    if (pushed == 0) {
        out.clearRetainingCapacity();
        try out.appendSlice(alloc, "{\"match_all\":{}}");
        return try out.toOwnedSlice(alloc);
    }
    try out.appendSlice(alloc, "]}}");
    return try out.toOwnedSlice(alloc);
}

/// Storage-evaluated filter for scans (no text index required; relational
/// tables prune columnar blocks): string equality and numeric ranges, which
/// the stored-row predicate evaluates with the same semantics as context
/// resolution. Everything else stays a verified residual.
pub fn scanFilterJsonAlloc(alloc: Allocator, bound: *const ctx.Bound) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var pushed: usize = 0;
    try out.appendSlice(alloc, "{\"bool\":{\"filter\":[");
    var residual: ?std.json.Value = null;
    if (bound.residual.len != 0) residual = std.json.parseFromSliceLeaky(std.json.Value, a, bound.residual, .{}) catch null;
    for (bound.source.conjuncts, bound.values, 0..) |c, set, ci| {
        if (ctx.artifactOfField(c.field) != null) continue;
        if (c.isEquality()) {
            var strings: usize = 0;
            for (set) |v| {
                if (v.len > 0 and v[0] == '"') strings += 1;
            }
            if (strings == 0 or strings != set.len) continue;
            if (pushed != 0) try out.append(alloc, ',');
            pushed += 1;
            try out.appendSlice(alloc, "{\"terms\":{\"field\":");
            try ctx.appendJsonString(alloc, &out, c.field);
            try out.appendSlice(alloc, ",\"values\":[");
            for (set, 0..) |v, i| {
                if (i != 0) try out.append(alloc, ',');
                try out.appendSlice(alloc, v);
            }
            try out.appendSlice(alloc, "]}}");
        } else if (c.op == .range) {
            const params = residual orelse continue;
            const entries = params.object.get("c") orelse continue;
            if (entries != .array or ci >= entries.array.items.len or entries.array.items[ci] != .object) continue;
            const bounds = entries.array.items[ci].object;
            var lower: ?f64 = null;
            var upper: ?f64 = null;
            var inclusive_min = true;
            var inclusive_max = true;
            var numeric = true;
            var it = bounds.iterator();
            while (it.next()) |entry| {
                const v: f64 = switch (entry.value_ptr.*) {
                    .integer => |n| @floatFromInt(n),
                    .float => |f| f,
                    else => blk: {
                        numeric = false;
                        break :blk 0;
                    },
                };
                const name = entry.key_ptr.*;
                if (std.mem.eql(u8, name, "gt") or std.mem.eql(u8, name, "gte")) {
                    lower = v;
                    inclusive_min = name.len == 3;
                } else {
                    upper = v;
                    inclusive_max = name.len == 3;
                }
            }
            if (!numeric or (lower == null and upper == null)) continue;
            if (pushed != 0) try out.append(alloc, ',');
            pushed += 1;
            try out.appendSlice(alloc, "{\"numeric_range\":{\"field\":");
            try ctx.appendJsonString(alloc, &out, c.field);
            if (lower) |v| try out.print(alloc, ",\"min\":{d},\"inclusive_min\":{}", .{ v, inclusive_min });
            if (upper) |v| try out.print(alloc, ",\"max\":{d},\"inclusive_max\":{}", .{ v, inclusive_max });
            try out.appendSlice(alloc, "}}");
        }
    }
    if (pushed == 0) {
        out.clearRetainingCapacity();
        return try out.toOwnedSlice(alloc);
    }
    try out.appendSlice(alloc, "]}}");
    return try out.toOwnedSlice(alloc);
}

/// Envelope handed to `Reader.query_fn`: `{"search":<text-index filter>,
/// "scan":<storage filter or "">}`. Hosts choose the path they can serve.
pub fn queryEnvelopeAlloc(alloc: Allocator, bound: *const ctx.Bound) ![]u8 {
    const search = try prefilterJsonAlloc(alloc, bound);
    defer alloc.free(search);
    const scan = try scanFilterJsonAlloc(alloc, bound);
    defer alloc.free(scan);
    return std.fmt.allocPrint(alloc, "{{\"search\":{s},\"scan\":{s}}}", .{ search, if (scan.len == 0) "null" else scan });
}

/// Resolve every bound source and render the producer input. Errors:
/// `RequiredContextMissing`, `ContextBudgetExceeded` (explicit refusal, never
/// truncated output) and any host read error.
pub fn resolveAlloc(alloc: Allocator, reader: Reader, bindings: *const ctx.Bindings, limits: Limits) !Resolution {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    var resolved = try a.alloc(ctx.ResolvedSource, bindings.items.len);
    var inputs = std.ArrayListUnmanaged(store.ManifestInput).empty;
    var queries = std.ArrayListUnmanaged(store.ManifestQuery).empty;
    var absent = std.ArrayListUnmanaged(store.ManifestInput).empty;
    var frontiers = std.StringArrayHashMapUnmanaged([]const u8).empty;
    var tuples = std.ArrayListUnmanaged(ctx.SubscriptionTuple).empty;
    var snapshot = false;
    var refresh_interval_ms: u64 = 0;
    for (bindings.items) |*bound| {
        if (!bound.source.isLive()) {
            snapshot = true;
            const interval = bound.source.refresh_interval_ms;
            if (interval != 0 and (refresh_interval_ms == 0 or interval < refresh_interval_ms)) refresh_interval_ms = interval;
        }
        // Graph and ordered sources replace their own tuples after reading.
        if (bound.source.kind == .graph or bound.source.order_by != null) continue;
        const residual = try a.dupe(u8, bound.residual);
        for (bound.tuples) |t| try tuples.append(a, .{ .spec = bound.spec, .tuple = t, .residual = residual });
    }

    for (bindings.items, 0..) |*bound, i| {
        const source = bound.source;
        // Results outlive the bindings and declaration: own every string.
        const source_name = try a.dupe(u8, source.name);
        const table_identity = try a.dupe(u8, reader.identity(source.table_id));
        if (!frontiers.contains(table_identity)) {
            const position: []const u8 = if (reader.frontier_fn) |f| try f(reader.ptr, a, source.table_id) else "";
            try frontiers.put(a, table_identity, position);
        }
        switch (source.kind) {
            .document => {
                resolved[i] = .{ .bound = bound, .members = &.{} };
                const key = try a.dupe(u8, bound.key orelse continue);
                const raw = (try reader.lookup_fn(reader.ptr, a, source.table_id, key)) orelse {
                    try absent.append(a, .{ .source = source_name, .table_id = table_identity, .key = key, .revision = @splat(0) });
                    continue;
                };
                const parsed = try logicalRow(a, reader, source, key, raw);
                const member = ctx.ConsumedInput{ .key = key, .projection = try ctx.projectAlloc(a, parsed, source.fields) };
                const members = try a.alloc(ctx.ConsumedInput, 1);
                members[0] = member;
                resolved[i] = .{ .bound = bound, .members = members, .present = true };
                try inputs.append(a, .{ .source = source_name, .table_id = table_identity, .key = key, .revision = member.revision() });
            },
            .query => {
                var ordered = false;
                const members = try resolveQuery(a, alloc, reader, bound, limits, &tuples, &ordered);
                for (members) |m| try inputs.append(a, .{ .source = source_name, .table_id = table_identity, .key = m.key, .revision = m.revision() });
                try queries.append(a, .{
                    .source = source_name,
                    .table_id = table_identity,
                    .query_identity = bound.query_identity,
                    .member_count = @intCast(members.len),
                    .membership = store.membershipDigest(members),
                    .complete = true,
                });
                resolved[i] = .{ .bound = bound, .members = members, .ordered = ordered };
            },
            .graph => {
                const members = try resolveGraph(a, alloc, reader, bound, &tuples);
                for (members) |m| try inputs.append(a, .{ .source = source_name, .table_id = table_identity, .key = m.key, .revision = m.revision() });
                try queries.append(a, .{
                    .source = source_name,
                    .table_id = table_identity,
                    .query_identity = bound.query_identity,
                    .member_count = @intCast(members.len),
                    .membership = store.membershipDigest(members),
                    .complete = true,
                });
                resolved[i] = .{ .bound = bound, .members = members, .ordered = true };
            },
            .search => {
                const members = try resolveSearch(a, alloc, reader, bound);
                for (members) |m| try inputs.append(a, .{ .source = source_name, .table_id = table_identity, .key = m.key, .revision = m.revision() });
                try queries.append(a, .{
                    .source = source_name,
                    .table_id = table_identity,
                    .query_identity = bound.query_identity,
                    .member_count = @intCast(members.len),
                    .membership = store.membershipDigest(members),
                    // Ranked snapshot: complete for the declared k, never verified.
                    .complete = false,
                });
                resolved[i] = .{ .bound = bound, .members = members, .ordered = true };
            },
        }
    }
    const rendered = try ctx.renderAlloc(alloc, resolved);
    errdefer {
        var r = rendered;
        r.deinit(alloc);
    }
    // Frontier vector: one opaque position per table identity, never a scalar
    // pretending to order independent tables.
    const SortCtx = struct {
        keys: [][]const u8,
        pub fn lessThan(self: @This(), l: usize, r: usize) bool {
            return std.mem.lessThan(u8, self.keys[l], self.keys[r]);
        }
    };
    frontiers.sort(SortCtx{ .keys = frontiers.keys() });
    var frontier = std.ArrayListUnmanaged(u8).empty;
    try frontier.append(a, '{');
    for (frontiers.keys(), frontiers.values(), 0..) |k, v, i| {
        if (i != 0) try frontier.append(a, ',');
        try ctx.appendJsonString(a, &frontier, k);
        try frontier.append(a, ':');
        try ctx.appendJsonString(a, &frontier, v);
    }
    try frontier.append(a, '}');
    return .{
        .arena = arena,
        .rendered = rendered,
        .inputs = inputs.items,
        .queries = queries.items,
        .absent = absent.items,
        .frontier = frontier.items,
        .tuples = blk: {
            ctx.sortUniqueTuples(&tuples);
            break :blk tuples.items;
        },
        .snapshot = snapshot,
        .refresh_interval_ms = refresh_interval_ms,
    };
}

/// Parse a row and, when the source reads generated artifacts, attach them
/// under `_artifacts` so projections and predicates see one logical row.
fn logicalRow(a: Allocator, reader: Reader, source: *const ctx.Source, key: []const u8, raw: []const u8) !std.json.Value {
    var row = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
    if (source.artifact_names.len == 0 or row != .object) return row;
    const artifacts_fn = reader.artifacts_fn orelse return error.ContextArtifactsUnavailable;
    const values = try artifacts_fn(reader.ptr, a, source.table_id, key, source.artifact_names);
    try row.object.put(a, "_artifacts", try std.json.parseFromSliceLeaky(std.json.Value, a, values, .{}));
    return row;
}

const Candidate = struct {
    key: []const u8,
    row: std.json.Value,
};

/// Bounded candidate scan with exact verification. Unordered results larger
/// than `max_documents` fail; ordered results keep the declared top-N and
/// record its boundary in the dependent's subscription value.
fn resolveQuery(a: Allocator, alloc: Allocator, reader: Reader, bound: *const ctx.Bound, limits: Limits, tuples: *std.ArrayListUnmanaged(ctx.SubscriptionTuple), ordered: *bool) ![]ctx.ConsumedInput {
    const source = bound.source;
    var matches = std.ArrayListUnmanaged(Candidate).empty;
    if (!bound.unsatisfiable) {
        const filter = try queryEnvelopeAlloc(a, bound);
        const budget: u32 = if (source.hasResidual()) source.max_candidates else @max(limits.scan_floor, source.max_documents *| limits.scan_factor);
        var offset: u32 = 0;
        var seen = std.StringHashMapUnmanaged(void).empty;
        while (true) {
            if (offset >= budget) return error.ContextBudgetExceeded;
            var page = try reader.query_fn(reader.ptr, alloc, source.table_id, filter, offset, @min(limits.page_rows, budget - offset + 1));
            defer page.deinit(alloc);
            for (page.rows) |row| {
                if (seen.contains(row.key)) continue;
                // Equality prefilter first: artifact reads only for candidates.
                const plain = try std.json.parseFromSliceLeaky(std.json.Value, a, row.value, .{});
                if (source.artifact_names.len == 0 and !try ctx.rowMatches(a, bound, plain)) continue;
                const parsed = if (source.artifact_names.len == 0) plain else try logicalRow(a, reader, source, row.key, row.value);
                if (source.artifact_names.len != 0 and !try ctx.rowMatches(a, bound, parsed)) continue;
                const key = try a.dupe(u8, row.key);
                try seen.put(a, key, {});
                try matches.append(a, .{ .key = key, .row = parsed });
                if (source.order_by == null and matches.items.len > source.max_documents) return error.ContextBudgetExceeded;
            }
            offset += @intCast(page.rows.len);
            if (page.exhausted or page.rows.len == 0) break;
        }
    }
    var members = std.ArrayListUnmanaged(ctx.ConsumedInput).empty;
    if (source.order_by) |order| {
        ordered.* = true;
        const Sort = struct {
            a: Allocator,
            field: []const u8,
            desc: bool,
            fn lt(self: @This(), l: Candidate, r: Candidate) bool {
                const rv = ctx.resolveFieldPath(r.row, self.field) orelse std.json.Value.null;
                return ctx.orderCompare(self.a, ctx.resolveFieldPath(l.row, self.field), l.key, rv, r.key, self.desc) == .lt;
            }
        };
        std.mem.sort(Candidate, matches.items, Sort{ .a = a, .field = order.field, .desc = order.descending }, Sort.lt);
        const full = matches.items.len >= source.max_documents;
        const kept = matches.items[0..@min(matches.items.len, source.max_documents)];
        const residual = if (kept.len == 0)
            try a.dupe(u8, bound.residual)
        else
            try ctx.withBoundaryAlloc(a, bound.residual, ctx.resolveFieldPath(kept[kept.len - 1].row, order.field), kept[kept.len - 1].key, full);
        // A full top-N subscribes only to values ranking at or above its
        // boundary (range-aware blocks); otherwise to the whole domain.
        const boundary = if (kept.len == 0) null else ctx.resolveFieldPath(kept[kept.len - 1].row, order.field);
        const ordered_tuples = try ctx.orderedTuplesAlloc(a, bound, boundary, full and kept.len != 0);
        for (ordered_tuples) |t| try tuples.append(a, .{ .spec = bound.spec, .tuple = t, .residual = residual });
        for (kept) |m| try members.append(a, .{ .key = m.key, .projection = try ctx.projectAlloc(a, m.row, source.fields) });
    } else {
        std.mem.sort(Candidate, matches.items, {}, struct {
            fn lt(_: void, l: Candidate, r: Candidate) bool {
                return std.mem.lessThan(u8, l.key, r.key);
            }
        }.lt);
        for (matches.items) |m| try members.append(a, .{ .key = m.key, .projection = try ctx.projectAlloc(a, m.row, source.fields) });
    }
    return members.items;
}

/// Bounded breadth-first traversal over direct edges. Every node whose
/// adjacency was read subscribes to its graph tuple; every reached member
/// subscribes to its document tuple (so its projection and creation count).
fn resolveGraph(a: Allocator, alloc: Allocator, reader: Reader, bound: *const ctx.Bound, tuples: *std.ArrayListUnmanaged(ctx.SubscriptionTuple)) ![]ctx.ConsumedInput {
    const source = bound.source;
    const g = source.graph;
    const start = bound.start orelse return &.{};
    const edges_fn = reader.edges_fn orelse return error.ContextGraphUnavailable;
    const Visit = struct { key: []const u8, depth: u8, edge_type: []const u8, outgoing: bool };
    var visited = std.StringArrayHashMapUnmanaged(Visit).empty;
    try visited.put(a, start, .{ .key = start, .depth = 0, .edge_type = "", .outgoing = true });
    var frontier = std.ArrayListUnmanaged([]const u8).empty;
    try frontier.append(a, start);
    var depth: u8 = 0;
    while (depth < g.depth and frontier.items.len != 0) : (depth += 1) {
        var next = std.ArrayListUnmanaged([]const u8).empty;
        std.mem.sort([]const u8, frontier.items, {}, struct {
            fn lt(_: void, l: []const u8, r: []const u8) bool {
                return std.mem.lessThan(u8, l, r);
            }
        }.lt);
        for (frontier.items) |node| {
            if (source.isLive()) try tuples.append(a, .{ .spec = bound.spec, .tuple = ctx.graphTuple(bound.spec, node) });
            const edges = try edges_fn(reader.ptr, alloc, source.table_id, g.graph_index, node, g.edge_types, g.direction, .{ .limit = g.max_fanout + 1, .include_generated = g.include_generated });
            defer freeEdges(alloc, edges);
            if (edges.len > g.max_fanout) return error.ContextBudgetExceeded;
            for (edges) |edge| {
                if (visited.contains(edge.neighbor)) continue;
                const key = try a.dupe(u8, edge.neighbor);
                try visited.put(a, key, .{ .key = key, .depth = depth + 1, .edge_type = try a.dupe(u8, edge.edge_type), .outgoing = edge.outgoing });
                try next.append(a, key);
                if (visited.count() - 1 > source.max_documents) return error.ContextBudgetExceeded;
            }
        }
        frontier = next;
    }
    var members = std.ArrayListUnmanaged(ctx.ConsumedInput).empty;
    var order = std.ArrayListUnmanaged(Visit).empty;
    for (visited.values()[1..]) |v| try order.append(a, v);
    std.mem.sort(Visit, order.items, {}, struct {
        fn lt(_: void, l: Visit, r: Visit) bool {
            if (l.depth != r.depth) return l.depth < r.depth;
            return std.mem.lessThan(u8, l.key, r.key);
        }
    }.lt);
    for (order.items) |v| {
        if (source.isLive()) try tuples.append(a, .{ .spec = bound.spec, .tuple = ctx.documentTuple(bound.spec, v.key) });
        const raw = try reader.lookup_fn(reader.ptr, a, source.table_id, v.key);
        const projection = if (raw) |doc| try ctx.projectAlloc(a, try logicalRow(a, reader, source, v.key, doc), source.fields) else "null";
        var extra = std.ArrayListUnmanaged(u8).empty;
        try extra.print(a, "\"depth\":{d},\"edge_type\":", .{v.depth});
        try ctx.appendJsonString(a, &extra, v.edge_type);
        try extra.appendSlice(a, if (v.outgoing) ",\"direction\":\"out\"" else ",\"direction\":\"in\"");
        try members.append(a, .{ .key = v.key, .projection = projection, .extra = extra.items });
    }
    return members.items;
}

/// Ranked snapshot retrieval in rank order.
fn resolveSearch(a: Allocator, alloc: Allocator, reader: Reader, bound: *const ctx.Bound) ![]ctx.ConsumedInput {
    const source = bound.source;
    if (bound.unsatisfiable) return &.{};
    const search_fn = reader.search_fn orelse return error.ContextSearchUnavailable;
    const s = source.search;
    var page = try search_fn(reader.ptr, alloc, source.table_id, .{ .text = bound.search_text, .text_field = s.text_field, .vector = bound.query_vector, .vector_index = s.vector_index, .k = s.k });
    defer page.deinit(alloc);
    var members = std.ArrayListUnmanaged(ctx.ConsumedInput).empty;
    for (page.rows[0..@min(page.rows.len, s.k)], 0..) |row, rank| {
        const parsed = try logicalRow(a, reader, source, row.key, row.value);
        try members.append(a, .{ .key = try a.dupe(u8, row.key), .projection = try ctx.projectAlloc(a, parsed, source.fields), .extra = try std.fmt.allocPrint(a, "\"rank\":{d}", .{rank + 1}) });
    }
    return members.items;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// In-memory multi-table reader used by tests (and by embedded hosts' tests).
pub const MemoryTables = struct {
    alloc: Allocator,
    /// table_id -> sorted rows
    tables: std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(Row)) = .empty,
    queries: usize = 0,

    pub fn deinit(self: *MemoryTables) void {
        for (self.tables.values()) |*rows| {
            for (rows.items) |row| {
                self.alloc.free(row.key);
                self.alloc.free(row.value);
            }
            rows.deinit(self.alloc);
        }
        for (self.tables.keys()) |k| self.alloc.free(k);
        self.tables.deinit(self.alloc);
    }

    pub fn put(self: *MemoryTables, table: []const u8, key: []const u8, value: ?[]const u8) !void {
        const slot = try self.tables.getOrPut(self.alloc, table);
        if (!slot.found_existing) {
            slot.key_ptr.* = try self.alloc.dupe(u8, table);
            slot.value_ptr.* = .empty;
        }
        const rows = slot.value_ptr;
        for (rows.items, 0..) |row, i| if (std.mem.eql(u8, row.key, key)) {
            self.alloc.free(row.value);
            if (value) |v| {
                rows.items[i].value = try self.alloc.dupe(u8, v);
            } else {
                self.alloc.free(row.key);
                _ = rows.orderedRemove(i);
            }
            return;
        };
        const v = value orelse return;
        var at: usize = 0;
        while (at < rows.items.len and std.mem.lessThan(u8, rows.items[at].key, key)) at += 1;
        try rows.insert(self.alloc, at, .{ .key = try self.alloc.dupe(u8, key), .value = try self.alloc.dupe(u8, v) });
    }

    pub fn reader(self: *MemoryTables, own: []const u8) Reader {
        return .{ .ptr = self, .own_table_id = own, .lookup_fn = lookup, .query_fn = query, .frontier_fn = frontier, .edges_fn = edges, .search_fn = search, .artifacts_fn = artifacts };
    }

    /// Edges are rows of table "edges:<graph_index>" keyed "<source>|<type>|<target>".
    fn edges(p: *anyopaque, alloc: Allocator, _: []const u8, graph_index: []const u8, node: []const u8, types: []const []const u8, direction: ctx.GraphSpec.Direction, options: EdgeOptions) anyerror![]Edge {
        const limit = options.limit;
        const self: *MemoryTables = @ptrCast(@alignCast(p));
        var name_buf: [128]u8 = undefined;
        const table = try std.fmt.bufPrint(&name_buf, "edges:{s}", .{graph_index});
        var out = std.ArrayListUnmanaged(Edge).empty;
        errdefer {
            for (out.items) |e| {
                alloc.free(e.edge_type);
                alloc.free(e.neighbor);
            }
            out.deinit(alloc);
        }
        const rows = self.rowsFor(table) orelse return out.toOwnedSlice(alloc);
        for (rows.items) |row| {
            var parts = std.mem.splitScalar(u8, row.key, '|');
            const src = parts.next().?;
            const typ = parts.next().?;
            const dst = parts.next().?;
            if (types.len != 0) {
                var ok = false;
                for (types) |t| if (std.mem.eql(u8, t, typ)) {
                    ok = true;
                };
                if (!ok) continue;
            }
            const out_match = direction != .in and std.mem.eql(u8, src, node);
            const in_match = direction != .out and std.mem.eql(u8, dst, node);
            if (!out_match and !in_match) continue;
            if (out.items.len >= limit) break;
            try out.append(alloc, .{ .edge_type = try alloc.dupe(u8, typ), .neighbor = try alloc.dupe(u8, if (out_match) dst else src), .outgoing = out_match });
        }
        return out.toOwnedSlice(alloc);
    }

    /// Artifacts are rows of table "artifacts" keyed "<doc>|<name>".
    fn artifacts(p: *anyopaque, alloc: Allocator, _: []const u8, key: []const u8, names: []const []const u8) anyerror![]u8 {
        const self: *MemoryTables = @ptrCast(@alignCast(p));
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(alloc);
        try out.append(alloc, '{');
        for (names, 0..) |name, i| {
            if (i != 0) try out.append(alloc, ',');
            try ctx.appendJsonString(alloc, &out, name);
            try out.append(alloc, ':');
            var buf: [256]u8 = undefined;
            const id = try std.fmt.bufPrint(&buf, "{s}|{s}", .{ key, name });
            var value: []const u8 = "null";
            if (self.rowsFor("artifacts")) |rows| for (rows.items) |row| if (std.mem.eql(u8, row.key, id)) {
                value = row.value;
            };
            try out.appendSlice(alloc, value);
        }
        try out.append(alloc, '}');
        return out.toOwnedSlice(alloc);
    }

    /// Toy relevance: rows whose text field contains the query, by key.
    fn search(p: *anyopaque, alloc: Allocator, table_id: []const u8, q: SearchQuery) anyerror!Page {
        const self: *MemoryTables = @ptrCast(@alignCast(p));
        var out = std.ArrayListUnmanaged(Row).empty;
        errdefer out.deinit(alloc);
        const rows = self.rowsFor(if (table_id.len == 0) "own" else table_id) orelse return .{ .rows = &.{}, .exhausted = true };
        for (rows.items) |row| {
            if (q.text.len != 0 and std.mem.indexOf(u8, row.value, q.text) == null) continue;
            try out.append(alloc, .{ .key = try alloc.dupe(u8, row.key), .value = try alloc.dupe(u8, row.value) });
        }
        return .{ .rows = try out.toOwnedSlice(alloc), .exhausted = true };
    }

    fn rowsFor(self: *MemoryTables, table_id: []const u8) ?*std.ArrayListUnmanaged(Row) {
        return self.tables.getPtr(table_id);
    }

    fn lookup(p: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8) anyerror!?[]u8 {
        const self: *MemoryTables = @ptrCast(@alignCast(p));
        const rows = self.rowsFor(if (table_id.len == 0) "own" else table_id) orelse return null;
        for (rows.items) |row| if (std.mem.eql(u8, row.key, key)) return try alloc.dupe(u8, row.value);
        return null;
    }

    /// Unindexed prefilter: returns every row (worst-case recall-safe host).
    fn query(p: *anyopaque, alloc: Allocator, table_id: []const u8, _: []const u8, offset: u32, limit: u32) anyerror!Page {
        const self: *MemoryTables = @ptrCast(@alignCast(p));
        self.queries += 1;
        const rows = self.rowsFor(if (table_id.len == 0) "own" else table_id) orelse return .{ .rows = &.{}, .exhausted = true };
        const start = @min(offset, rows.items.len);
        const end = @min(rows.items.len, start + limit);
        const out = try alloc.alloc(Row, end - start);
        for (rows.items[start..end], out) |row, *o| o.* = .{ .key = try alloc.dupe(u8, row.key), .value = try alloc.dupe(u8, row.value) };
        return .{ .rows = out, .exhausted = end == rows.items.len };
    }

    fn frontier(_: *anyopaque, alloc: Allocator, table_id: []const u8) anyerror![]u8 {
        return std.fmt.allocPrint(alloc, "pos:{s}", .{table_id});
    }
};

const issue_sources =
    \\{"organization":{"document":{"table":"organizations","key_from":"/organization_id"},"fields":["voice","guidance"],"required":true},
    \\ "policies":{"query":{"table":"policies","filter_query":{"conjuncts":[
    \\   {"field":"tenant_id","term":{"$source":"/tenant_id"}},{"field":"organization_id","term":{"$source":"/organization_id"}},
    \\   {"field":"locale","term":{"$source":"/locale"}},{"field":"status","term":"approved"}]}},
    \\   "fields":["policy_id","revision","payload"],"max_documents":3}}
;

fn testConfig(alloc: Allocator) !ctx.Config {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), issue_sources, .{});
    const Resolve = struct {
        fn resolve(_: *anyopaque, a: Allocator, table: []const u8) anyerror!?[]u8 {
            return try std.fmt.allocPrint(a, "id-{s}", .{table});
        }
    };
    var dummy: u8 = 0;
    const stored = try ctx.canonicalFromPublicAlloc(alloc, value, null, .{ .ptr = &dummy, .resolve_fn = Resolve.resolve });
    defer alloc.free(stored);
    return ctx.parseStoredJson(alloc, stored);
}

fn bindJson(alloc: Allocator, config: *const ctx.Config, raw: []const u8) !ctx.Bindings {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const doc = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), raw, .{});
    return ctx.bindAlloc(alloc, config, doc);
}

test "resolution renders the issue example and records lineage and frontier vector" {
    const alloc = testing.allocator;
    var tables = MemoryTables{ .alloc = alloc };
    defer tables.deinit();
    try tables.put("id-organizations", "org-1", "{\"voice\":\"warm\",\"guidance\":\"be brief\",\"logo\":\"x\"}");
    try tables.put("id-policies", "p-2", "{\"policy_id\":\"p-2\",\"revision\":2,\"payload\":\"b\",\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\"}");
    try tables.put("id-policies", "p-1", "{\"policy_id\":\"p-1\",\"revision\":1,\"payload\":\"a\",\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\"}");
    try tables.put("id-policies", "p-3", "{\"policy_id\":\"p-3\",\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"draft\"}");
    try tables.put("id-policies", "p-4", "{\"policy_id\":\"p-4\",\"tenant_id\":\"t2\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\"}");
    var config = try testConfig(alloc);
    defer config.deinit();
    var bindings = try bindJson(alloc, &config, "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\"}");
    defer bindings.deinit();
    var resolution = try resolveAlloc(alloc, tables.reader("id-docs"), &bindings, .{ .page_rows = 2 });
    defer resolution.deinit(alloc);
    try testing.expectEqualStrings(
        "{\"organization\":{\"key\":\"org-1\",\"value\":{\"guidance\":\"be brief\",\"voice\":\"warm\"}},\"policies\":{\"count\":2,\"members\":[{\"key\":\"p-1\",\"value\":{\"payload\":\"a\",\"policy_id\":\"p-1\",\"revision\":1}},{\"key\":\"p-2\",\"value\":{\"payload\":\"b\",\"policy_id\":\"p-2\",\"revision\":2}}]}}",
        resolution.rendered.json,
    );
    try testing.expectEqual(@as(usize, 3), resolution.inputs.len);
    try testing.expectEqual(@as(usize, 1), resolution.queries.len);
    try testing.expectEqual(@as(u32, 2), resolution.queries[0].member_count);
    try testing.expectEqualStrings("{\"id-organizations\":\"pos:id-organizations\",\"id-policies\":\"pos:id-policies\"}", resolution.frontier);

    // Equivalent inputs in a different storage order fingerprint identically.
    var shuffled = MemoryTables{ .alloc = alloc };
    defer shuffled.deinit();
    for ([_][]const u8{ "p-4", "p-3", "p-2", "p-1" }) |key| {
        for (tables.tables.getPtr("id-policies").?.items) |row| if (std.mem.eql(u8, row.key, key)) try shuffled.put("id-policies", key, row.value);
    }
    try shuffled.put("id-organizations", "org-1", "{\"guidance\":\"be brief\",\"voice\":\"warm\",\"logo\":\"y\"}");
    var again = try resolveAlloc(alloc, shuffled.reader("id-docs"), &bindings, .{});
    defer again.deinit(alloc);
    try testing.expectEqualSlices(u8, &resolution.rendered.fingerprint, &again.rendered.fingerprint);
}

test "resolution fails explicitly on overflow and missing required inputs, retains empty queries" {
    const alloc = testing.allocator;
    var tables = MemoryTables{ .alloc = alloc };
    defer tables.deinit();
    var config = try testConfig(alloc);
    defer config.deinit();
    var bindings = try bindJson(alloc, &config, "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\"}");
    defer bindings.deinit();
    try testing.expectError(error.RequiredContextMissing, resolveAlloc(alloc, tables.reader("id-docs"), &bindings, .{}));

    try tables.put("id-organizations", "org-1", "{\"voice\":\"v\"}");
    var empty = try resolveAlloc(alloc, tables.reader("id-docs"), &bindings, .{});
    defer empty.deinit(alloc);
    try testing.expect(std.mem.indexOf(u8, empty.rendered.json, "\"policies\":{\"count\":0,\"members\":[]}") != null);
    try testing.expectEqual(@as(usize, 1), empty.queries.len);

    var buf: [16]u8 = undefined;
    for (0..4) |i| {
        try tables.put("id-policies", try std.fmt.bufPrint(&buf, "p-{d}", .{i}), "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\"}");
    }
    try testing.expectError(error.ContextBudgetExceeded, resolveAlloc(alloc, tables.reader("id-docs"), &bindings, .{}));
    // A prefilter scan budget smaller than the candidate set also refuses.
    try testing.expectError(error.ContextBudgetExceeded, resolveAlloc(alloc, tables.reader("id-docs"), &bindings, .{ .page_rows = 1, .scan_floor = 2, .scan_factor = 0 }));
}

test "prefilter pushes down only string conjuncts" {
    const alloc = testing.allocator;
    var config = try ctx.parsePublicJson(alloc,
        \\{"q":{"query":{"filter_query":{"conjuncts":[{"field":"org","term":{"$source":"/org"}},{"field":"n","term":3},{"field":"locale","terms":["en","fr"]}]}},"fields":["x"]}}
    , "");
    defer config.deinit();
    var bindings = try bindJson(alloc, &config, "{\"org\":\"o\"}");
    defer bindings.deinit();
    const filter = try prefilterJsonAlloc(alloc, &bindings.items[0]);
    defer alloc.free(filter);
    try testing.expectEqualStrings(
        "{\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"match\":{\"field\":\"locale\",\"text\":\"en\"}},{\"match\":{\"field\":\"locale\",\"text\":\"fr\"}}]}},{\"match\":{\"field\":\"org\",\"text\":\"o\"}}]}}",
        filter,
    );
}

test "ordered top-N, graph traversal, and snapshot search resolve with exact subscriptions" {
    const alloc = testing.allocator;
    var tables = MemoryTables{ .alloc = alloc };
    defer tables.deinit();
    for ([_][2][]const u8{ .{ "n1", "{\"org\":\"o\",\"at\":1,\"title\":\"a\"}" }, .{ "n2", "{\"org\":\"o\",\"at\":3,\"title\":\"b solar\"}" }, .{ "n3", "{\"org\":\"o\",\"at\":2,\"title\":\"c solar\"}" }, .{ "n4", "{\"org\":\"x\",\"at\":9,\"title\":\"d\"}" } }) |r| try tables.put("own", r[0], r[1]);
    try tables.put("edges:refs", "dep|cites|n1", "{}");
    try tables.put("edges:refs", "n1|cites|n2", "{}");
    try tables.put("edges:refs", "n1|mentions|n3", "{}");
    var config = try ctx.parsePublicJson(alloc,
        \\{"latest":{"query":{"filter_query":{"field":"org","term":{"$source":"/org"}},"order_by":{"field":"at","order":"desc"}},"fields":["title"],"max_documents":2},
        \\ "cites":{"graph":{"graph_index":"refs","edge_types":["cites"],"depth":2},"fields":["title"],"max_documents":10},
        \\ "solar":{"search":{"text":"solar","text_field":"title","k":5},"fields":["title"],"tracking":"snapshot"}}
    , "");
    defer config.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var bindings = try ctx.bindForKeyAlloc(alloc, &config, "dep", try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"org\":\"o\"}", .{}));
    defer bindings.deinit();
    var r = try resolveAlloc(alloc, tables.reader(""), &bindings, .{});
    defer r.deinit(alloc);
    try testing.expectEqualStrings(
        "{\"cites\":{\"count\":2,\"members\":[{\"key\":\"n1\",\"depth\":1,\"edge_type\":\"cites\",\"direction\":\"out\",\"value\":{\"title\":\"a\"}},{\"key\":\"n2\",\"depth\":2,\"edge_type\":\"cites\",\"direction\":\"out\",\"value\":{\"title\":\"b solar\"}}]}," ++
            "\"latest\":{\"count\":2,\"members\":[{\"key\":\"n2\",\"value\":{\"title\":\"b solar\"}},{\"key\":\"n3\",\"value\":{\"title\":\"c solar\"}}]}," ++
            "\"solar\":{\"count\":2,\"members\":[{\"key\":\"n2\",\"rank\":1,\"value\":{\"title\":\"b solar\"}},{\"key\":\"n3\",\"rank\":2,\"value\":{\"title\":\"c solar\"}}],\"tracking\":\"snapshot\"}}",
        r.rendered.json,
    );
    try testing.expect(r.snapshot);
    // Graph: adjacency of dep and n1, document tuples of n1 and n2; ordered:
    // one equality tuple carrying the recorded boundary.
    const cites = &bindings.items[0];
    var graph_tuples: usize = 0;
    var boundary_recorded = false;
    for (r.tuples) |t| {
        if (std.mem.eql(u8, &t.spec, &cites.spec)) graph_tuples += 1;
        if (std.mem.indexOf(u8, t.residual, "\"b\":{\"full\":true,\"k\":\"n3\"") != null) boundary_recorded = true;
    }
    try testing.expectEqual(@as(usize, 4), graph_tuples);
    try testing.expect(boundary_recorded);
    // Fan-out beyond the declared bound fails explicitly.
    var tight = try ctx.parsePublicJson(alloc,
        \\{"g":{"graph":{"graph_index":"refs","depth":2,"max_fanout":1},"fields":["title"]}}
    , "");
    defer tight.deinit();
    var tight_bindings = try ctx.bindForKeyAlloc(alloc, &tight, "dep", try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{}", .{}));
    defer tight_bindings.deinit();
    try testing.expectError(error.ContextBudgetExceeded, resolveAlloc(alloc, tables.reader(""), &tight_bindings, .{}));
}

test "context reads generated artifacts as fields and predicates" {
    const alloc = testing.allocator;
    var tables = MemoryTables{ .alloc = alloc };
    defer tables.deinit();
    try tables.put("own", "p1", "{\"org\":\"o\",\"title\":\"one\"}");
    try tables.put("own", "p2", "{\"org\":\"o\",\"title\":\"two\"}");
    try tables.put("artifacts", "p1|classify", "{\"label\":\"policy\",\"score\":0.9}");
    try tables.put("artifacts", "p2|classify", "{\"label\":\"faq\",\"score\":0.7}");
    try tables.put("artifacts", "p1|summary", "\"short summary\"");
    var config = try ctx.parsePublicJson(alloc,
        \\{"policies":{"query":{"filter_query":{"conjuncts":[{"field":"org","term":{"$source":"/org"}},{"field":"_artifacts.classify.label","term":"policy"}]}},"fields":["title","_artifacts.summary"]}}
    , "");
    defer config.deinit();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var bindings = try ctx.bindForKeyAlloc(alloc, &config, "dep", try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"org\":\"o\"}", .{}));
    defer bindings.deinit();
    const filter = try prefilterJsonAlloc(alloc, &bindings.items[0]);
    defer alloc.free(filter);
    try testing.expect(std.mem.indexOf(u8, filter, "_artifacts") == null);
    var r = try resolveAlloc(alloc, tables.reader(""), &bindings, .{});
    defer r.deinit(alloc);
    try testing.expectEqualStrings(
        "{\"policies\":{\"count\":1,\"members\":[{\"key\":\"p1\",\"value\":{\"_artifacts.summary\":\"short summary\",\"title\":\"one\"}}]}}",
        r.rendered.json,
    );
}
