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

//! Named context sources for asset-producer enrichments.
//!
//! A context source declares input that lives outside the enrichment's source
//! row: one referenced document (`document`) or the bounded matching set of a
//! parameterized equality query (`query`). The runtime renders the projected
//! values into a canonical `{"context":{...}}` block that participates in the
//! producer skip state, and registers *subscriptions* so a later change to an
//! input invalidates the dependent artifact.
//!
//! This module is pure: it owns the declaration grammar, typed `$source`
//! bindings, predicate evaluation, canonical projection/rendering, input
//! fingerprints, and subscription tuples. Storage of subscriptions, freshness,
//! and manifests lives in `context_dependency_store.zig`.
//!
//! Subscription tuples are the indexed invalidation strategy. A dependent row
//! subscribes to the digests of its *bound* predicate tuples; a changed input
//! row computes the tuples it matches before and after the change. Equal
//! digests identify exactly the dependents whose matching set may differ, so
//! invalidation never scans dependent artifacts. Document sources subscribe to
//! the referenced key itself, which also detects a missing optional document
//! becoming present.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Sha256 = std.crypto.hash.sha2.Sha256;

pub const Digest = [Sha256.digest_length]u8;

pub const max_sources: usize = 16;
pub const max_conjuncts: usize = 16;
pub const max_fields: usize = 64;
pub const max_set_values: usize = 256;
pub const max_name_bytes: usize = 128;
pub const max_pointer_bytes: usize = 512;
pub const default_max_documents: u32 = 100;
pub const max_max_documents: u32 = 10_000;
pub const default_max_bytes: u64 = 1024 * 1024;
pub const max_max_bytes: u64 = 16 * 1024 * 1024;
pub const max_debounce_ms: u64 = 24 * 60 * 60 * 1000;
/// A changed input row whose array-valued predicate fields expand to more
/// tuples than this invalidates every subscriber of the source instead. That
/// stays correct (over-invalidation is safe, skip state suppresses identical
/// inputs) while bounding per-change work.
pub const max_row_tuples: usize = 256;

pub const Error = error{
    InvalidContextSourcesConfig,
    UnsupportedContextPredicate,
    ContextBudgetExceeded,
    RequiredContextMissing,
    InvalidContextBinding,
    UnknownContextSourceTable,
    OutOfMemory,
};

pub const SourceKind = enum { document, query, graph, search };
pub const RefreshMode = enum { background, manual };
pub const Overflow = enum { @"error" };

/// `live` sources register subscriptions and keep dependents current through
/// exact invalidation. `snapshot` sources (required for ranked retrieval,
/// whose membership is relative to the whole corpus) register nothing: their
/// dependents are reported `unverified` and rebuild on a schedule or manually.
pub const Tracking = enum { live, snapshot };

pub const max_graph_depth: u8 = 3;
pub const max_graph_fanout: u32 = 1000;
pub const max_vector_dims: u32 = 4096;
pub const default_max_candidates: u32 = 1000;
pub const max_max_candidates: u32 = 100_000;
pub const max_search_k: u32 = 1000;

/// A literal canonical scalar or a typed `$source` binding.
pub const Operand = struct {
    literal: []const u8 = "",
    pointer: []const u8 = "",

    pub fn present(self: Operand) bool {
        return self.literal.len != 0 or self.pointer.len != 0;
    }
};

pub const Range = struct {
    gt: Operand = .{},
    gte: Operand = .{},
    lt: Operand = .{},
    lte: Operand = .{},
};

pub const Metric = enum { cosine, dot };

pub const Similarity = struct {
    threshold: f64 = 0,
    metric: Metric = .cosine,
    dims: u32 = 0,
    /// Caller-declared identity of the embedding space. Part of the source
    /// identity: changing the model is a declaration change and rebuilds.
    model: []const u8 = "",
};

pub const MatchOperator = enum { @"and", @"or" };

/// Pinned analyzer for per-row `match` evaluation. Part of the source
/// identity so an analyzer change is a declaration change.
pub const match_analyzer = "antfly.context.simple.v1";

/// One predicate conjunct. Equality conjuncts (`term`, `terms`) form the
/// indexed subscription tuple. The others are *residual* per-row tests
/// evaluated on both the previous and next value of a changed row, which is
/// what keeps their invalidation exact without corpus-relative state.
pub const Conjunct = struct {
    field: []const u8,
    op: Op,
    /// `term`/`terms` literal set; one literal for `prefix`/`match`.
    literals: []const []const u8 = &.{},
    /// `$source` binding for `term`/`terms`/`prefix`/`match`/`similar`.
    source_pointer: []const u8 = "",
    range: Range = .{},
    match_operator: MatchOperator = .@"and",
    similarity: Similarity = .{},

    pub const Op = enum { term, terms, range, prefix, match, similar };

    pub fn isBound(self: Conjunct) bool {
        return self.source_pointer.len != 0;
    }

    pub fn isEquality(self: Conjunct) bool {
        return self.op == .term or self.op == .terms;
    }
};

pub const OrderBy = struct {
    field: []const u8,
    descending: bool = false,
};

pub const GraphSpec = struct {
    graph_index: []const u8 = "",
    /// JSON pointer to the start node key; empty means the dependent row itself.
    start_from: []const u8 = "",
    edge_types: []const []const u8 = &.{},
    direction: Direction = .out,
    depth: u8 = 1,
    max_fanout: u32 = 100,
    /// Also traverse field-derived and enrichment-generated edges. Their
    /// producers must not depend on the consuming enrichment.
    include_generated: bool = false,

    pub const Direction = enum { out, in, both };
};

/// Ranked retrieval (snapshot only): full-text relevance or vector k-NN.
pub const SearchSpec = struct {
    text: Operand = .{},
    text_field: []const u8 = "",
    vector_index: []const u8 = "",
    /// `$source` pointer to the query vector in the dependent row.
    vector_from: []const u8 = "",
    k: u32 = 10,
};

pub const Source = struct {
    name: []const u8,
    kind: SourceKind,
    /// Public table name. Empty means the enrichment's own table.
    table: []const u8 = "",
    /// Immutable table identity resolved at admission. Empty means the
    /// enrichment's own table. Renaming or recreating a table changes the
    /// identity, so a dependency can never silently retarget.
    table_id: []const u8 = "",
    /// `document` only: JSON pointer yielding the referenced key.
    key_from: []const u8 = "",
    /// `query` only.
    conjuncts: []const Conjunct = &.{},
    /// `query` only: top-N by a field (ties by key). `max_documents` is N.
    order_by: ?OrderBy = null,
    graph: GraphSpec = .{},
    search: SearchSpec = .{},
    /// Projected fields consumed by the producer, sorted and unique.
    fields: []const []const u8 = &.{},
    /// Fields whose change can alter this source's result: projected fields,
    /// residual predicate fields, and the order field (sorted, unique).
    relevant_fields: []const []const u8 = &.{},
    /// Generated artifacts this source reads through `_artifacts.` paths
    /// (sorted, unique).
    artifact_names: []const []const u8 = &.{},
    /// `document` only: a missing referenced document fails explicitly.
    required: bool = false,
    max_documents: u32 = default_max_documents,
    max_bytes: u64 = default_max_bytes,
    /// Bound on candidate rows examined by residual or ordered queries.
    max_candidates: u32 = default_max_candidates,
    overflow: Overflow = .@"error",
    tracking: Tracking = .live,
    /// Snapshot sources: rebuild interval (0 = manual only).
    refresh_interval_ms: u64 = 0,

    /// Stable identity of the declaration. Subscriptions are keyed by it, so
    /// changing a source's meaning orphans the old subscriptions instead of
    /// matching them against a different predicate.
    pub fn specDigest(self: Source) Digest {
        var h = Sha256.init(.{});
        hashField(&h, "antfly.context_source.v2");
        hashField(&h, self.name);
        hashField(&h, @tagName(self.kind));
        hashField(&h, self.table_id);
        hashField(&h, self.key_from);
        hashField(&h, @tagName(self.tracking));
        for (self.conjuncts) |c| {
            hashField(&h, c.field);
            hashField(&h, @tagName(c.op));
            hashField(&h, c.source_pointer);
            for (c.literals) |lit| hashField(&h, lit);
            inline for (.{ "gt", "gte", "lt", "lte" }) |name| {
                hashField(&h, @field(c.range, name).literal);
                hashField(&h, @field(c.range, name).pointer);
            }
            if (c.op == .match) {
                hashField(&h, match_analyzer);
                hashField(&h, @tagName(c.match_operator));
            }
            if (c.op == .similar) {
                var buf: [64]u8 = undefined;
                hashField(&h, std.fmt.bufPrint(&buf, "{d}:{d}", .{ c.similarity.threshold, c.similarity.dims }) catch unreachable);
                hashField(&h, @tagName(c.similarity.metric));
                hashField(&h, c.similarity.model);
            }
            hashField(&h, "|");
        }
        if (self.order_by) |o| {
            hashField(&h, o.field);
            hashField(&h, if (o.descending) "desc" else "asc");
        }
        if (self.kind == .graph) {
            hashField(&h, self.graph.graph_index);
            hashField(&h, self.graph.start_from);
            for (self.graph.edge_types) |t| hashField(&h, t);
            hashField(&h, @tagName(self.graph.direction));
            hashField(&h, &.{ self.graph.depth, @intFromBool(self.graph.include_generated) });
        }
        if (self.kind == .search) {
            hashField(&h, self.search.text.literal);
            hashField(&h, self.search.text.pointer);
            hashField(&h, self.search.text_field);
            hashField(&h, self.search.vector_index);
            hashField(&h, self.search.vector_from);
        }
        for (self.fields) |field| hashField(&h, field);
        var out: Digest = undefined;
        h.final(&out);
        return out;
    }

    pub fn targetsOwnTable(self: Source) bool {
        return self.table_id.len == 0;
    }

    pub fn isLive(self: Source) bool {
        return self.tracking == .live;
    }

    /// Whether invalidation needs per-subscriber evaluation of the changed
    /// row (residual predicates or a top-N boundary).
    pub fn hasResidual(self: Source) bool {
        if (self.order_by != null) return true;
        for (self.conjuncts) |c| if (!c.isEquality()) return true;
        return false;
    }
};

pub const Refresh = struct {
    mode: RefreshMode = .background,
    debounce_ms: u64 = 0,
};

/// Parsed, canonical declaration. Every string is owned by `arena`.
pub const Config = struct {
    arena: std.heap.ArenaAllocator,
    sources: []const Source,
    refresh: Refresh,

    pub fn deinit(self: *Config) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn find(self: *const Config, name: []const u8) ?*const Source {
        for (self.sources) |*source| if (std.mem.eql(u8, source.name, name)) return source;
        return null;
    }

    pub fn findSpec(self: *const Config, spec: Digest) ?*const Source {
        for (self.sources) |*source| if (std.mem.eql(u8, &source.specDigest(), &spec)) return source;
        return null;
    }

    /// Whether any live source reads generated data (artifacts or derived
    /// graph edges), which requires artifact-change notices.
    pub fn readsGenerated(self: *const Config) bool {
        for (self.sources) |s| if (s.isLive() and (s.artifact_names.len != 0 or s.graph.include_generated)) return true;
        return false;
    }

    pub fn hasSnapshot(self: *const Config) bool {
        for (self.sources) |s| if (!s.isLive()) return true;
        return false;
    }

    /// Smallest snapshot refresh interval (0 when none is scheduled).
    pub fn snapshotRefreshMs(self: *const Config) u64 {
        var best: u64 = 0;
        for (self.sources) |s| {
            if (s.isLive() or s.refresh_interval_ms == 0) continue;
            if (best == 0 or s.refresh_interval_ms < best) best = s.refresh_interval_ms;
        }
        return best;
    }

    /// Canonical stored form: `{"sources":{...sorted...},"refresh":{...}}`.
    pub fn canonicalJsonAlloc(self: *const Config, alloc: Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(alloc);
        try out.appendSlice(alloc, "{\"sources\":{");
        for (self.sources, 0..) |source, i| {
            if (i != 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, source.name);
            try out.append(alloc, ':');
            try appendSourceJson(alloc, &out, source, true);
        }
        try out.appendSlice(alloc, "},\"refresh\":{\"mode\":");
        try appendJsonString(alloc, &out, @tagName(self.refresh.mode));
        try out.appendSlice(alloc, ",\"debounce_ms\":");
        try out.print(alloc, "{d}", .{self.refresh.debounce_ms});
        try out.appendSlice(alloc, "}}");
        return try out.toOwnedSlice(alloc);
    }

    /// Public form of `context_sources` (without identities).
    pub fn publicSourcesJsonAlloc(self: *const Config, alloc: Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(alloc);
        try out.append(alloc, '{');
        for (self.sources, 0..) |source, i| {
            if (i != 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, source.name);
            try out.append(alloc, ':');
            try appendSourceJson(alloc, &out, source, false);
        }
        try out.append(alloc, '}');
        return try out.toOwnedSlice(alloc);
    }

    pub fn publicRefreshJsonAlloc(self: *const Config, alloc: Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(alloc);
        try out.appendSlice(alloc, "{\"mode\":");
        try appendJsonString(alloc, &out, @tagName(self.refresh.mode));
        if (self.refresh.debounce_ms != 0) {
            try out.appendSlice(alloc, ",\"debounce\":");
            var buf: [32]u8 = undefined;
            try appendJsonString(alloc, &out, formatDurationMs(&buf, self.refresh.debounce_ms));
        }
        try out.append(alloc, '}');
        return try out.toOwnedSlice(alloc);
    }

    pub fn digest(self: *const Config) Digest {
        var h = Sha256.init(.{});
        for (self.sources) |source| h.update(&source.specDigest());
        hashField(&h, @tagName(self.refresh.mode));
        var out: Digest = undefined;
        h.final(&out);
        return out;
    }
};

fn appendOperand(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), operand: Operand) !void {
    if (operand.pointer.len != 0) {
        try out.appendSlice(alloc, "{\"$source\":");
        try appendJsonString(alloc, out, operand.pointer);
        try out.append(alloc, '}');
    } else try out.appendSlice(alloc, operand.literal);
}

fn appendSourceJson(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), source: Source, with_identity: bool) !void {
    try out.append(alloc, '{');
    switch (source.kind) {
        .document => {
            try out.appendSlice(alloc, "\"document\":{");
            try appendTableJson(alloc, out, source, with_identity);
            try out.appendSlice(alloc, "\"key_from\":");
            try appendJsonString(alloc, out, source.key_from);
            try out.append(alloc, '}');
        },
        .query => {
            try out.appendSlice(alloc, "\"query\":{");
            try appendTableJson(alloc, out, source, with_identity);
            try out.appendSlice(alloc, "\"filter_query\":{\"conjuncts\":[");
            for (source.conjuncts, 0..) |c, i| {
                if (i != 0) try out.append(alloc, ',');
                try out.appendSlice(alloc, "{\"field\":");
                try appendJsonString(alloc, out, c.field);
                try out.append(alloc, ',');
                switch (c.op) {
                    .term, .terms, .prefix, .match => {
                        try appendJsonString(alloc, out, @tagName(c.op));
                        try out.append(alloc, ':');
                        if (c.isBound()) {
                            try appendOperand(alloc, out, .{ .pointer = c.source_pointer });
                        } else if (c.op == .terms) {
                            try out.append(alloc, '[');
                            for (c.literals, 0..) |lit, j| {
                                if (j != 0) try out.append(alloc, ',');
                                try out.appendSlice(alloc, lit);
                            }
                            try out.append(alloc, ']');
                        } else try out.appendSlice(alloc, c.literals[0]);
                        if (c.op == .match) {
                            try out.appendSlice(alloc, ",\"operator\":");
                            try appendJsonString(alloc, out, @tagName(c.match_operator));
                        }
                    },
                    .range => {
                        try out.appendSlice(alloc, "\"range\":{");
                        var first = true;
                        inline for (.{ "gt", "gte", "lt", "lte" }) |name| {
                            const operand = @field(c.range, name);
                            if (operand.present()) {
                                if (!first) try out.append(alloc, ',');
                                first = false;
                                try out.appendSlice(alloc, "\"" ++ name ++ "\":");
                                try appendOperand(alloc, out, operand);
                            }
                        }
                        try out.append(alloc, '}');
                    },
                    .similar => {
                        try out.appendSlice(alloc, "\"similar\":{\"vector\":");
                        try appendOperand(alloc, out, .{ .pointer = c.source_pointer });
                        try out.print(alloc, ",\"threshold\":{d},\"metric\":\"{s}\",\"dims\":{d},\"model\":", .{ c.similarity.threshold, @tagName(c.similarity.metric), c.similarity.dims });
                        try appendJsonString(alloc, out, c.similarity.model);
                        try out.append(alloc, '}');
                    },
                }
                try out.append(alloc, '}');
            }
            try out.appendSlice(alloc, "]}");
            if (source.order_by) |o| {
                try out.appendSlice(alloc, ",\"order_by\":{\"field\":");
                try appendJsonString(alloc, out, o.field);
                try out.appendSlice(alloc, if (o.descending) ",\"order\":\"desc\"}" else ",\"order\":\"asc\"}");
            }
            try out.append(alloc, '}');
        },
        .graph => {
            const g = source.graph;
            try out.appendSlice(alloc, "\"graph\":{");
            try appendTableJson(alloc, out, source, with_identity);
            try out.appendSlice(alloc, "\"graph_index\":");
            try appendJsonString(alloc, out, g.graph_index);
            if (g.start_from.len != 0) {
                try out.appendSlice(alloc, ",\"start_from\":");
                try appendJsonString(alloc, out, g.start_from);
            }
            try out.appendSlice(alloc, ",\"edge_types\":[");
            for (g.edge_types, 0..) |t, i| {
                if (i != 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, t);
            }
            try out.print(alloc, "],\"direction\":\"{s}\",\"depth\":{d},\"max_fanout\":{d}", .{ @tagName(g.direction), g.depth, g.max_fanout });
            if (g.include_generated) try out.appendSlice(alloc, ",\"include_generated\":true");
            try out.append(alloc, '}');
        },
        .search => {
            const q = source.search;
            try out.appendSlice(alloc, "\"search\":{");
            try appendTableJson(alloc, out, source, with_identity);
            var first = true;
            if (q.text.present()) {
                first = false;
                try out.appendSlice(alloc, "\"text\":");
                try appendOperand(alloc, out, q.text);
                try out.appendSlice(alloc, ",\"text_field\":");
                try appendJsonString(alloc, out, q.text_field);
            }
            if (q.vector_from.len != 0) {
                if (!first) try out.append(alloc, ',');
                first = false;
                try out.appendSlice(alloc, "\"vector\":");
                try appendOperand(alloc, out, .{ .pointer = q.vector_from });
                try out.appendSlice(alloc, ",\"vector_index\":");
                try appendJsonString(alloc, out, q.vector_index);
            }
            try out.print(alloc, ",\"k\":{d}}}", .{q.k});
        },
    }
    try out.appendSlice(alloc, ",\"fields\":[");
    for (source.fields, 0..) |field, i| {
        if (i != 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, field);
    }
    try out.append(alloc, ']');
    if (source.kind == .document) {
        try out.appendSlice(alloc, if (source.required) ",\"required\":true" else ",\"required\":false");
    } else if (source.kind != .search) {
        try out.print(alloc, ",\"max_documents\":{d}", .{source.max_documents});
    }
    if (source.kind == .query and (source.hasResidual() or source.tracking == .snapshot))
        try out.print(alloc, ",\"max_candidates\":{d}", .{source.max_candidates});
    try out.print(alloc, ",\"max_bytes\":{d},\"overflow\":\"error\"", .{source.max_bytes});
    if (source.tracking == .snapshot) {
        try out.appendSlice(alloc, ",\"tracking\":\"snapshot\"");
        if (source.refresh_interval_ms != 0) {
            var buf: [32]u8 = undefined;
            try out.appendSlice(alloc, ",\"refresh_interval\":");
            try appendJsonString(alloc, out, formatDurationMs(&buf, source.refresh_interval_ms));
        }
    }
    try out.append(alloc, '}');
}

fn appendTableJson(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), source: Source, with_identity: bool) !void {
    if (source.table.len != 0) {
        try out.appendSlice(alloc, "\"table\":");
        try appendJsonString(alloc, out, source.table);
        try out.append(alloc, ',');
    }
    if (with_identity and source.table_id.len != 0) {
        try out.appendSlice(alloc, "\"table_id\":");
        try appendJsonString(alloc, out, source.table_id);
        try out.append(alloc, ',');
    }
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parse the canonical stored form produced by `Config.canonicalJsonAlloc`.
pub fn parseStoredJson(alloc: Allocator, raw: []const u8) Error!Config {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidContextSourcesConfig,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidContextSourcesConfig;
    const sources = parsed.value.object.get("sources") orelse return error.InvalidContextSourcesConfig;
    const refresh = parsed.value.object.get("refresh");
    return parsePublicValue(alloc, sources, refresh, true);
}

/// Parse public `context_sources` and optional `refresh` objects. Unknown
/// fields and unsupported predicates are rejected at admission so a typo can
/// never silently widen or drop a dependency.
pub fn parsePublicValue(alloc: Allocator, sources_value: std.json.Value, refresh_value: ?std.json.Value, allow_identity: bool) Error!Config {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    if (sources_value != .object) return error.InvalidContextSourcesConfig;
    const count = sources_value.object.count();
    if (count == 0 or count > max_sources) return error.InvalidContextSourcesConfig;
    var sources = try a.alloc(Source, count);
    var it = sources_value.object.iterator();
    var i: usize = 0;
    while (it.next()) |entry| : (i += 1) {
        sources[i] = try parseSource(a, entry.key_ptr.*, entry.value_ptr.*, allow_identity);
    }
    std.mem.sort(Source, sources, {}, struct {
        fn lt(_: void, l: Source, r: Source) bool {
            return std.mem.lessThan(u8, l.name, r.name);
        }
    }.lt);
    var refresh = Refresh{};
    if (refresh_value) |value| switch (value) {
        .null => {},
        .object => |object| {
            var rit = object.iterator();
            while (rit.next()) |entry| {
                const key = entry.key_ptr.*;
                const field = entry.value_ptr.*;
                if (std.mem.eql(u8, key, "mode")) {
                    if (field != .string) return error.InvalidContextSourcesConfig;
                    refresh.mode = std.meta.stringToEnum(RefreshMode, field.string) orelse return error.InvalidContextSourcesConfig;
                } else if (std.mem.eql(u8, key, "debounce")) {
                    if (field != .string) return error.InvalidContextSourcesConfig;
                    refresh.debounce_ms = parseDurationMs(field.string) orelse return error.InvalidContextSourcesConfig;
                } else if (std.mem.eql(u8, key, "debounce_ms")) {
                    if (field != .integer or field.integer < 0) return error.InvalidContextSourcesConfig;
                    refresh.debounce_ms = @intCast(field.integer);
                } else return error.InvalidContextSourcesConfig;
            }
            if (refresh.debounce_ms > max_debounce_ms) return error.InvalidContextSourcesConfig;
        },
        else => return error.InvalidContextSourcesConfig,
    };
    return .{ .arena = arena, .sources = sources, .refresh = refresh };
}

pub fn parsePublicJson(alloc: Allocator, sources_raw: []const u8, refresh_raw: []const u8) Error!Config {
    var sources = std.json.parseFromSlice(std.json.Value, alloc, sources_raw, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidContextSourcesConfig,
    };
    defer sources.deinit();
    if (refresh_raw.len == 0) return parsePublicValue(alloc, sources.value, null, false);
    var refresh = std.json.parseFromSlice(std.json.Value, alloc, refresh_raw, .{}) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidContextSourcesConfig,
    };
    defer refresh.deinit();
    return parsePublicValue(alloc, sources.value, refresh.value, false);
}

fn uintField(field: std.json.Value, min: u64, max: u64) Error!u64 {
    if (field != .integer or field.integer < 0) return error.InvalidContextSourcesConfig;
    const v: u64 = @intCast(field.integer);
    if (v < min or v > max) return error.InvalidContextSourcesConfig;
    return v;
}

fn parseSource(a: Allocator, name: []const u8, value: std.json.Value, allow_identity: bool) Error!Source {
    if (!validName(name)) return error.InvalidContextSourcesConfig;
    if (value != .object) return error.InvalidContextSourcesConfig;
    var source = Source{ .name = try a.dupe(u8, name), .kind = .document };
    var seen_target = false;
    var seen_required = false;
    var seen_max_documents = false;
    var seen_max_candidates = false;
    var fields = std.ArrayListUnmanaged([]const u8).empty;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const field = entry.value_ptr.*;
        const kind: ?SourceKind = std.meta.stringToEnum(SourceKind, key);
        if (kind) |k| {
            if (seen_target) return error.InvalidContextSourcesConfig;
            seen_target = true;
            if (field != .object) return error.InvalidContextSourcesConfig;
            source.kind = k;
            try parseTarget(a, &source, field.object, allow_identity);
        } else if (std.mem.eql(u8, key, "fields")) {
            if (field != .array or field.array.items.len == 0 or field.array.items.len > max_fields)
                return error.InvalidContextSourcesConfig;
            for (field.array.items) |item| {
                if (item != .string or !validFieldPath(item.string)) return error.InvalidContextSourcesConfig;
                try fields.append(a, try a.dupe(u8, item.string));
            }
        } else if (std.mem.eql(u8, key, "required")) {
            if (field != .bool) return error.InvalidContextSourcesConfig;
            source.required = field.bool;
            seen_required = true;
        } else if (std.mem.eql(u8, key, "max_documents")) {
            source.max_documents = @intCast(try uintField(field, 1, max_max_documents));
            seen_max_documents = true;
        } else if (std.mem.eql(u8, key, "max_candidates")) {
            source.max_candidates = @intCast(try uintField(field, 1, max_max_candidates));
            seen_max_candidates = true;
        } else if (std.mem.eql(u8, key, "max_bytes")) {
            source.max_bytes = try uintField(field, 1, max_max_bytes);
        } else if (std.mem.eql(u8, key, "overflow")) {
            // Truncated context must never publish as complete; `error` is the
            // only admitted overflow policy (ordered top-N declares its limit).
            if (field != .string or !std.mem.eql(u8, field.string, "error")) return error.InvalidContextSourcesConfig;
        } else if (std.mem.eql(u8, key, "tracking")) {
            if (field != .string) return error.InvalidContextSourcesConfig;
            source.tracking = std.meta.stringToEnum(Tracking, field.string) orelse return error.InvalidContextSourcesConfig;
        } else if (std.mem.eql(u8, key, "refresh_interval")) {
            if (field != .string) return error.InvalidContextSourcesConfig;
            source.refresh_interval_ms = parseDurationMs(field.string) orelse return error.InvalidContextSourcesConfig;
            if (source.refresh_interval_ms < 1000) return error.InvalidContextSourcesConfig;
        } else return error.InvalidContextSourcesConfig;
    }
    if (!seen_target or fields.items.len == 0) return error.InvalidContextSourcesConfig;
    if (source.kind != .document and seen_required) return error.InvalidContextSourcesConfig;
    if ((source.kind == .document or source.kind == .search) and seen_max_documents) return error.InvalidContextSourcesConfig;
    if (source.kind != .query and seen_max_candidates) return error.InvalidContextSourcesConfig;
    // Ranked retrieval has no exact per-row membership test: it may only be
    // consumed as an explicitly unverified snapshot.
    if (source.kind == .search and source.tracking != .snapshot) return error.UnsupportedContextPredicate;
    if (source.refresh_interval_ms != 0 and source.tracking != .snapshot) return error.InvalidContextSourcesConfig;
    if (source.kind == .search) source.max_documents = source.search.k;
    std.mem.sort([]const u8, fields.items, {}, struct {
        fn lt(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.lt);
    for (fields.items[1..], 1..) |field, idx| {
        if (std.mem.eql(u8, field, fields.items[idx - 1])) return error.InvalidContextSourcesConfig;
    }
    source.fields = fields.items;
    var relevant = std.ArrayListUnmanaged([]const u8).empty;
    try relevant.appendSlice(a, fields.items);
    for (source.conjuncts) |c| if (!c.isEquality()) try relevant.append(a, c.field);
    if (source.order_by) |o| try relevant.append(a, o.field);
    for (source.conjuncts) |c| if (c.isEquality()) try relevant.append(a, c.field);
    sortUniqueStrings(&relevant);
    var artifacts = std.ArrayListUnmanaged([]const u8).empty;
    for (relevant.items) |field| if (artifactOfField(field)) |artifact| try artifacts.append(a, artifact);
    sortUniqueStrings(&artifacts);
    source.artifact_names = artifacts.items;
    // Keep `relevant_fields` to non-equality fields plus projections, as
    // before; equality fields were only needed to collect artifact names.
    var trimmed = std.ArrayListUnmanaged([]const u8).empty;
    try trimmed.appendSlice(a, fields.items);
    for (source.conjuncts) |c| if (!c.isEquality()) try trimmed.append(a, c.field);
    if (source.order_by) |o| try trimmed.append(a, o.field);
    sortUniqueStrings(&trimmed);
    source.relevant_fields = trimmed.items;
    return source;
}

fn parseTarget(a: Allocator, source: *Source, object: std.json.ObjectMap, allow_identity: bool) Error!void {
    var it = object.iterator();
    var seen_filter = false;
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const field = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "table")) {
            if (field != .string or !validName(field.string)) return error.InvalidContextSourcesConfig;
            source.table = try a.dupe(u8, field.string);
        } else if (allow_identity and std.mem.eql(u8, key, "table_id")) {
            if (field != .string or field.string.len == 0 or field.string.len > max_name_bytes) return error.InvalidContextSourcesConfig;
            source.table_id = try a.dupe(u8, field.string);
        } else switch (source.kind) {
            .document => if (std.mem.eql(u8, key, "key_from")) {
                if (field != .string or !validPointer(field.string)) return error.InvalidContextSourcesConfig;
                source.key_from = try a.dupe(u8, field.string);
            } else return error.InvalidContextSourcesConfig,
            .query => if (std.mem.eql(u8, key, "filter_query")) {
                seen_filter = true;
                source.conjuncts = try parseFilter(a, field);
            } else if (std.mem.eql(u8, key, "order_by")) {
                source.order_by = try parseOrderBy(a, field);
            } else return error.InvalidContextSourcesConfig,
            .graph => try parseGraphField(a, &source.graph, key, field),
            .search => try parseSearchField(a, &source.search, key, field),
        }
    }
    switch (source.kind) {
        .document => if (source.key_from.len == 0) return error.InvalidContextSourcesConfig,
        .query => {
            if (!seen_filter) return error.InvalidContextSourcesConfig;
            var similar: usize = 0;
            for (source.conjuncts) |c| if (c.op == .similar) {
                similar += 1;
            };
            if (similar > 1) return error.UnsupportedContextPredicate;
        },
        .graph => if (source.graph.graph_index.len == 0) return error.InvalidContextSourcesConfig,
        .search => {
            const s = source.search;
            const has_text = s.text.present();
            const has_vector = s.vector_from.len != 0;
            if (has_text == has_vector) return error.InvalidContextSourcesConfig;
            if (has_text and s.text_field.len == 0) return error.InvalidContextSourcesConfig;
            if (has_vector and s.vector_index.len == 0) return error.InvalidContextSourcesConfig;
        },
    }
}

fn parseOrderBy(a: Allocator, value: std.json.Value) Error!OrderBy {
    if (value != .object) return error.InvalidContextSourcesConfig;
    var out = OrderBy{ .field = "" };
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const field = entry.value_ptr.*;
        if (std.mem.eql(u8, entry.key_ptr.*, "field")) {
            if (field != .string or !validFieldPath(field.string)) return error.InvalidContextSourcesConfig;
            out.field = try a.dupe(u8, field.string);
        } else if (std.mem.eql(u8, entry.key_ptr.*, "order")) {
            if (field != .string) return error.InvalidContextSourcesConfig;
            if (std.mem.eql(u8, field.string, "desc")) {
                out.descending = true;
            } else if (!std.mem.eql(u8, field.string, "asc")) return error.InvalidContextSourcesConfig;
        } else return error.InvalidContextSourcesConfig;
    }
    if (out.field.len == 0) return error.InvalidContextSourcesConfig;
    return out;
}

fn parseGraphField(a: Allocator, g: *GraphSpec, key: []const u8, field: std.json.Value) Error!void {
    if (std.mem.eql(u8, key, "graph_index")) {
        if (field != .string or !validName(field.string)) return error.InvalidContextSourcesConfig;
        g.graph_index = try a.dupe(u8, field.string);
    } else if (std.mem.eql(u8, key, "start_from")) {
        if (field != .string or !validPointer(field.string)) return error.InvalidContextSourcesConfig;
        g.start_from = try a.dupe(u8, field.string);
    } else if (std.mem.eql(u8, key, "edge_types")) {
        if (field != .array or field.array.items.len > 32) return error.InvalidContextSourcesConfig;
        var types = std.ArrayListUnmanaged([]const u8).empty;
        for (field.array.items) |item| {
            if (item != .string or item.string.len == 0 or item.string.len > max_name_bytes) return error.InvalidContextSourcesConfig;
            try types.append(a, try a.dupe(u8, item.string));
        }
        sortUniqueStrings(&types);
        g.edge_types = types.items;
    } else if (std.mem.eql(u8, key, "direction")) {
        if (field != .string) return error.InvalidContextSourcesConfig;
        g.direction = std.meta.stringToEnum(GraphSpec.Direction, field.string) orelse return error.InvalidContextSourcesConfig;
    } else if (std.mem.eql(u8, key, "depth")) {
        g.depth = @intCast(try uintField(field, 1, max_graph_depth));
    } else if (std.mem.eql(u8, key, "max_fanout")) {
        g.max_fanout = @intCast(try uintField(field, 1, max_graph_fanout));
    } else if (std.mem.eql(u8, key, "include_generated")) {
        if (field != .bool) return error.InvalidContextSourcesConfig;
        g.include_generated = field.bool;
    } else return error.InvalidContextSourcesConfig;
}

fn parseSearchField(a: Allocator, s: *SearchSpec, key: []const u8, field: std.json.Value) Error!void {
    if (std.mem.eql(u8, key, "text")) {
        s.text = try parseOperand(a, field, true);
        if (s.text.literal.len != 0 and s.text.literal[0] != '"') return error.InvalidContextSourcesConfig;
    } else if (std.mem.eql(u8, key, "text_field")) {
        if (field != .string or !validFieldPath(field.string)) return error.InvalidContextSourcesConfig;
        s.text_field = try a.dupe(u8, field.string);
    } else if (std.mem.eql(u8, key, "vector")) {
        const operand = try parseOperand(a, field, false);
        if (operand.pointer.len == 0) return error.InvalidContextSourcesConfig;
        s.vector_from = operand.pointer;
    } else if (std.mem.eql(u8, key, "vector_index")) {
        if (field != .string or !validName(field.string)) return error.InvalidContextSourcesConfig;
        s.vector_index = try a.dupe(u8, field.string);
    } else if (std.mem.eql(u8, key, "k")) {
        s.k = @intCast(try uintField(field, 1, max_search_k));
    } else return error.InvalidContextSourcesConfig;
}

fn parseOperand(a: Allocator, value: std.json.Value, allow_literal: bool) Error!Operand {
    if (try sourceBinding(a, value)) |pointer| return .{ .pointer = pointer };
    if (!allow_literal) return error.InvalidContextSourcesConfig;
    const lit = (try canonicalScalarAlloc(a, value)) orelse return error.UnsupportedContextPredicate;
    return .{ .literal = lit };
}

/// Admitted grammar: `{"conjuncts":[...]}` or a single bare conjunct, where a
/// conjunct is `term`, `terms`, `range`, `prefix`, `match` (filter-mode, no
/// scoring), or `similar` (similarity threshold). Disjunctions, scored
/// full-text, and nearest-k retrieval are rejected for live tracking.
fn parseFilter(a: Allocator, value: std.json.Value) Error![]const Conjunct {
    if (value != .object) return error.UnsupportedContextPredicate;
    var list = std.ArrayListUnmanaged(Conjunct).empty;
    if (value.object.get("conjuncts")) |conjuncts| {
        if (value.object.count() != 1) return error.UnsupportedContextPredicate;
        if (conjuncts != .array or conjuncts.array.items.len == 0 or conjuncts.array.items.len > max_conjuncts)
            return error.UnsupportedContextPredicate;
        for (conjuncts.array.items) |item| try list.append(a, try parseConjunct(a, item));
    } else {
        try list.append(a, try parseConjunct(a, value));
    }
    // Canonical member order: conjunction is commutative, so sort by field
    // then operator so equivalent declarations share one identity.
    std.mem.sort(Conjunct, list.items, {}, struct {
        fn lt(_: void, l: Conjunct, r: Conjunct) bool {
            return switch (std.mem.order(u8, l.field, r.field)) {
                .lt => true,
                .gt => false,
                .eq => @backingInt(l.op) < @backingInt(r.op),
            };
        }
    }.lt);
    var bound_sets: usize = 0;
    for (list.items) |c| if (c.op == .terms and c.isBound()) {
        bound_sets += 1;
    };
    // A dependent subscribes to the cartesian product of its bound sets; one
    // bound set keeps that linear in the bound values.
    if (bound_sets > 1) return error.UnsupportedContextPredicate;
    return list.items;
}

fn parseConjunct(a: Allocator, value: std.json.Value) Error!Conjunct {
    if (value != .object) return error.UnsupportedContextPredicate;
    var field_name: ?[]const u8 = null;
    var conjunct = Conjunct{ .field = "", .op = .term };
    var seen_value = false;
    var seen_operator = false;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        const key = entry.key_ptr.*;
        const field = entry.value_ptr.*;
        if (std.mem.eql(u8, key, "field")) {
            if (field != .string or !validFieldPath(field.string)) return error.InvalidContextSourcesConfig;
            field_name = try a.dupe(u8, field.string);
            continue;
        }
        if (std.mem.eql(u8, key, "operator")) {
            if (field != .string) return error.InvalidContextSourcesConfig;
            conjunct.match_operator = std.meta.stringToEnum(MatchOperator, field.string) orelse return error.InvalidContextSourcesConfig;
            seen_operator = true;
            continue;
        }
        const op = std.meta.stringToEnum(Conjunct.Op, key) orelse return error.UnsupportedContextPredicate;
        if (seen_value) return error.UnsupportedContextPredicate;
        seen_value = true;
        conjunct.op = op;
        switch (op) {
            .term, .prefix, .match => {
                if (try sourceBinding(a, field)) |pointer| {
                    conjunct.source_pointer = pointer;
                } else {
                    const lit = (try canonicalScalarAlloc(a, field)) orelse return error.UnsupportedContextPredicate;
                    if (op != .term and lit[0] != '"') return error.UnsupportedContextPredicate;
                    const lits = try a.alloc([]const u8, 1);
                    lits[0] = lit;
                    conjunct.literals = lits;
                }
            },
            .terms => {
                if (try sourceBinding(a, field)) |pointer| {
                    conjunct.source_pointer = pointer;
                } else {
                    if (field != .array or field.array.items.len == 0 or field.array.items.len > max_set_values)
                        return error.UnsupportedContextPredicate;
                    var lits = std.ArrayListUnmanaged([]const u8).empty;
                    for (field.array.items) |item| {
                        const lit = (try canonicalScalarAlloc(a, item)) orelse return error.UnsupportedContextPredicate;
                        try lits.append(a, lit);
                    }
                    sortUniqueStrings(&lits);
                    conjunct.literals = lits.items;
                }
            },
            .range => {
                if (field != .object or field.object.count() == 0) return error.UnsupportedContextPredicate;
                var rit = field.object.iterator();
                while (rit.next()) |bound| {
                    const operand = try parseOperand(a, bound.value_ptr.*, true);
                    if (std.mem.eql(u8, bound.key_ptr.*, "gt")) {
                        conjunct.range.gt = operand;
                    } else if (std.mem.eql(u8, bound.key_ptr.*, "gte")) {
                        conjunct.range.gte = operand;
                    } else if (std.mem.eql(u8, bound.key_ptr.*, "lt")) {
                        conjunct.range.lt = operand;
                    } else if (std.mem.eql(u8, bound.key_ptr.*, "lte")) {
                        conjunct.range.lte = operand;
                    } else return error.UnsupportedContextPredicate;
                }
                if (conjunct.range.gt.present() and conjunct.range.gte.present()) return error.UnsupportedContextPredicate;
                if (conjunct.range.lt.present() and conjunct.range.lte.present()) return error.UnsupportedContextPredicate;
            },
            .similar => {
                if (field != .object) return error.UnsupportedContextPredicate;
                var seen_threshold = false;
                var sit = field.object.iterator();
                while (sit.next()) |opt| {
                    const v = opt.value_ptr.*;
                    const name = opt.key_ptr.*;
                    if (std.mem.eql(u8, name, "vector")) {
                        conjunct.source_pointer = (try sourceBinding(a, v)) orelse return error.UnsupportedContextPredicate;
                    } else if (std.mem.eql(u8, name, "threshold")) {
                        conjunct.similarity.threshold = switch (v) {
                            .float => |f| f,
                            .integer => |n| @floatFromInt(n),
                            else => return error.InvalidContextSourcesConfig,
                        };
                        seen_threshold = true;
                    } else if (std.mem.eql(u8, name, "metric")) {
                        if (v != .string) return error.InvalidContextSourcesConfig;
                        conjunct.similarity.metric = std.meta.stringToEnum(Metric, v.string) orelse return error.InvalidContextSourcesConfig;
                    } else if (std.mem.eql(u8, name, "dims")) {
                        conjunct.similarity.dims = @intCast(try uintField(v, 1, max_vector_dims));
                    } else if (std.mem.eql(u8, name, "model")) {
                        if (v != .string or v.string.len == 0 or v.string.len > max_pointer_bytes) return error.InvalidContextSourcesConfig;
                        conjunct.similarity.model = try a.dupe(u8, v.string);
                    } else return error.UnsupportedContextPredicate;
                }
                const s = conjunct.similarity;
                if (!seen_threshold or s.dims == 0 or s.model.len == 0 or conjunct.source_pointer.len == 0)
                    return error.InvalidContextSourcesConfig;
                if (s.metric == .cosine and (s.threshold < -1 or s.threshold > 1)) return error.InvalidContextSourcesConfig;
            },
        }
    }
    conjunct.field = field_name orelse return error.UnsupportedContextPredicate;
    if (!seen_value) return error.UnsupportedContextPredicate;
    if (seen_operator and conjunct.op != .match) return error.UnsupportedContextPredicate;
    return conjunct;
}

fn sourceBinding(a: Allocator, value: std.json.Value) Error!?[]const u8 {
    if (value != .object) return null;
    const pointer = value.object.get("$source") orelse return null;
    if (value.object.count() != 1) return error.UnsupportedContextPredicate;
    if (pointer != .string or !validPointer(pointer.string)) return error.InvalidContextSourcesConfig;
    return try a.dupe(u8, pointer.string);
}

fn validName(name: []const u8) bool {
    if (name.len == 0 or name.len > max_name_bytes) return false;
    for (name) |c| switch (c) {
        'a'...'z', 'A'...'Z', '0'...'9', '_', '-', '.' => {},
        else => return false,
    };
    return true;
}

/// Virtual namespace exposing a row's generated artifacts to context sources:
/// `_artifacts.<artifact>[.<path>]` reads the named asset (parsed JSON or
/// text), chunk stream (array of chunk payloads), or document embedding
/// (vector). Admission rejects artifacts that depend on the consuming
/// enrichment, so a context source is always strictly downstream.
pub const artifacts_prefix = "_artifacts.";

/// Artifact named by a virtual `_artifacts.` field path, if any.
pub fn artifactOfField(path: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, path, artifacts_prefix)) return null;
    const rest = path[artifacts_prefix.len..];
    const end = std.mem.indexOfScalar(u8, rest, '.') orelse rest.len;
    return rest[0..end];
}

/// Field paths are dotted names. Other fields starting with `_` are reserved
/// for engine-owned metadata and rejected.
fn validFieldPath(path: []const u8) bool {
    if (path.len == 0 or path.len > max_pointer_bytes) return false;
    if (artifactOfField(path)) |artifact| return validName(artifact) and !std.mem.endsWith(u8, path, ".");
    if (path[0] == '_') return false;
    var segments = std.mem.splitScalar(u8, path, '.');
    while (segments.next()) |segment| if (segment.len == 0) return false;
    return true;
}

fn validPointer(pointer: []const u8) bool {
    return pointer.len > 1 and pointer.len <= max_pointer_bytes and pointer[0] == '/';
}

fn sortUniqueStrings(list: *std.ArrayListUnmanaged([]const u8)) void {
    std.mem.sort([]const u8, list.items, {}, struct {
        fn lt(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.lt);
    var w: usize = 0;
    for (list.items) |item| {
        if (w != 0 and std.mem.eql(u8, list.items[w - 1], item)) continue;
        list.items[w] = item;
        w += 1;
    }
    list.items.len = w;
}

pub fn parseDurationMs(text: []const u8) ?u64 {
    if (text.len < 2) return null;
    const Unit = struct { suffix: []const u8, ms: u64 };
    const units = [_]Unit{ .{ .suffix = "ms", .ms = 1 }, .{ .suffix = "s", .ms = 1000 }, .{ .suffix = "m", .ms = 60_000 }, .{ .suffix = "h", .ms = 3_600_000 } };
    for (units) |unit| {
        if (!std.mem.endsWith(u8, text, unit.suffix)) continue;
        const digits = text[0 .. text.len - unit.suffix.len];
        if (digits.len == 0) return null;
        for (digits) |c| if (c < '0' or c > '9') {
            // "ms" also ends with "s": let the loop try the next unit.
            if (unit.ms != 1000 or !std.mem.endsWith(u8, text, "ms")) return null;
            break;
        };
        const n = std.fmt.parseInt(u64, digits, 10) catch continue;
        return std.math.mul(u64, n, unit.ms) catch null;
    }
    return null;
}

fn formatDurationMs(buf: []u8, ms: u64) []const u8 {
    if (ms % 3_600_000 == 0) return std.fmt.bufPrint(buf, "{d}h", .{ms / 3_600_000}) catch unreachable;
    if (ms % 60_000 == 0) return std.fmt.bufPrint(buf, "{d}m", .{ms / 60_000}) catch unreachable;
    if (ms % 1000 == 0) return std.fmt.bufPrint(buf, "{d}s", .{ms / 1000}) catch unreachable;
    return std.fmt.bufPrint(buf, "{d}ms", .{ms}) catch unreachable;
}

/// Host hook that maps a public table name to its immutable identity. Hosts
/// return null for unknown or unauthorized tables (admission then fails), and
/// should map the enrichment's own table to its own identity.
pub const TableResolver = struct {
    ptr: *anyopaque,
    resolve_fn: *const fn (ptr: *anyopaque, alloc: Allocator, table: []const u8) anyerror!?[]u8,

    pub fn resolve(self: TableResolver, alloc: Allocator, table: []const u8) !?[]u8 {
        return self.resolve_fn(self.ptr, alloc, table);
    }
};

/// Canonicalize public `context_sources` (and optional `refresh`) into the
/// stored form, resolving every named table through `resolver`. Without a
/// resolver, named tables stay unresolved and catalog admission rejects them;
/// unnamed sources target the enrichment's own table.
pub fn canonicalFromPublicAlloc(alloc: Allocator, sources_value: std.json.Value, refresh_value: ?std.json.Value, resolver: ?TableResolver) ![]u8 {
    var config = try parsePublicValue(alloc, sources_value, refresh_value, false);
    defer config.deinit();
    try resolveTables(alloc, &config, resolver);
    return config.canonicalJsonAlloc(alloc);
}

/// Re-resolve table identities of an already stored declaration from table
/// names (restore, import, or a host re-validating a client-supplied stored
/// form). Client-provided identities are never trusted.
pub fn reresolveStoredAlloc(alloc: Allocator, stored: []const u8, resolver: ?TableResolver) ![]u8 {
    var config = try parseStoredJson(alloc, stored);
    defer config.deinit();
    for (config.sources) |*source| @constCast(source).table_id = "";
    try resolveTables(alloc, &config, resolver);
    return config.canonicalJsonAlloc(alloc);
}

fn resolveTables(alloc: Allocator, config: *Config, resolver: ?TableResolver) !void {
    const r = resolver orelse return;
    const a = config.arena.allocator();
    for (config.sources) |*source| {
        if (source.table.len == 0) continue;
        const id = (try r.resolve(alloc, source.table)) orelse return error.UnknownContextSourceTable;
        defer alloc.free(id);
        if (id.len == 0 or id.len > max_name_bytes) return error.UnknownContextSourceTable;
        @constCast(source).table_id = try a.dupe(u8, id);
    }
}

/// Distinct foreign table identities referenced by a declaration.
pub fn foreignTableIdsAlloc(alloc: Allocator, config: *const Config, own_table_id: []const u8) ![][]const u8 {
    var out = std.ArrayListUnmanaged([]const u8).empty;
    errdefer out.deinit(alloc);
    for (config.sources) |source| {
        if (source.table_id.len == 0 or std.mem.eql(u8, source.table_id, own_table_id)) continue;
        var seen = false;
        for (out.items) |id| if (std.mem.eql(u8, id, source.table_id)) {
            seen = true;
        };
        if (!seen) try out.append(alloc, source.table_id);
    }
    return try out.toOwnedSlice(alloc);
}

// ---------------------------------------------------------------------------
// Canonical scalars, pointers, projection
// ---------------------------------------------------------------------------

/// Canonical bytes for a JSON scalar. Integral floats collapse to integers so
/// `1` and `1.0` bind identically. Objects/arrays/null are not scalars.
pub fn canonicalScalarAlloc(alloc: Allocator, value: std.json.Value) Allocator.Error!?[]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    switch (value) {
        .string => |s| try appendJsonString(alloc, &out, s),
        .integer => |n| try out.print(alloc, "{d}", .{n}),
        .number_string => |s| {
            const f = std.fmt.parseFloat(f64, s) catch return null;
            try appendCanonicalFloat(alloc, &out, f);
        },
        .float => |f| try appendCanonicalFloat(alloc, &out, f),
        .bool => |b| try out.appendSlice(alloc, if (b) "true" else "false"),
        else => return null,
    }
    return try out.toOwnedSlice(alloc);
}

fn appendCanonicalFloat(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), f: f64) !void {
    if (std.math.isFinite(f) and @floor(f) == f and @abs(f) < 9.007199254740992e15) {
        try out.print(alloc, "{d}", .{@as(i64, @intFromFloat(f))});
    } else {
        try out.print(alloc, "{e}", .{f});
    }
}

/// RFC 6901 JSON pointer lookup.
pub fn resolvePointer(root: std.json.Value, pointer: []const u8) ?std.json.Value {
    if (pointer.len == 0) return root;
    if (pointer[0] != '/') return null;
    var current = root;
    var tokens = std.mem.splitScalar(u8, pointer[1..], '/');
    var buf: [max_pointer_bytes]u8 = undefined;
    while (tokens.next()) |raw_token| {
        const token = unescapePointerToken(&buf, raw_token) orelse return null;
        switch (current) {
            .object => |object| current = object.get(token) orelse return null,
            .array => |array| {
                const index = std.fmt.parseInt(usize, token, 10) catch return null;
                if (index >= array.items.len) return null;
                current = array.items[index];
            },
            else => return null,
        }
    }
    return current;
}

fn unescapePointerToken(buf: []u8, token: []const u8) ?[]const u8 {
    if (std.mem.indexOfScalar(u8, token, '~') == null) return token;
    var w: usize = 0;
    var i: usize = 0;
    while (i < token.len) : (i += 1) {
        if (w >= buf.len) return null;
        if (token[i] == '~') {
            if (i + 1 >= token.len) return null;
            buf[w] = switch (token[i + 1]) {
                '0' => '~',
                '1' => '/',
                else => return null,
            };
            i += 1;
        } else buf[w] = token[i];
        w += 1;
    }
    return buf[0..w];
}

pub fn resolveFieldPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var current = root;
    var segments = std.mem.splitScalar(u8, path, '.');
    while (segments.next()) |segment| {
        if (current != .object) return null;
        current = current.object.get(segment) orelse return null;
    }
    return current;
}

/// Canonical projection of the consumed fields: an object with sorted keys
/// (fields are pre-sorted at admission), missing fields omitted, and values
/// serialized with sorted object keys at every depth. Two rows with equal
/// projections are equivalent inputs regardless of unconsumed fields.
pub fn projectAlloc(alloc: Allocator, row: std.json.Value, fields: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    for (fields) |field| {
        const value = resolveFieldPath(row, field) orelse continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, field);
        try out.append(alloc, ':');
        try appendCanonicalValue(alloc, &out, value);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

/// Projection that keeps the nested structure of dotted paths, so the result
/// is itself a row on which the same paths resolve (shadows and notice rows
/// are re-evaluated as rows). Keys are sorted; missing paths are omitted.
pub fn projectTreeAlloc(alloc: Allocator, row: std.json.Value, fields: []const []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var root = std.json.ObjectMap.empty;
    for (fields) |field| {
        const value = resolveFieldPath(row, field) orelse continue;
        var node = &root;
        var segments = std.mem.splitScalar(u8, field, '.');
        var segment = segments.next().?;
        while (segments.next()) |next| {
            const slot = try node.getOrPut(a, segment);
            if (!slot.found_existing or slot.value_ptr.* != .object) slot.value_ptr.* = .{ .object = .empty };
            node = &slot.value_ptr.object;
            segment = next;
        }
        try node.put(a, segment, value);
    }
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendCanonicalValue(alloc, &out, .{ .object = root });
    return try out.toOwnedSlice(alloc);
}

pub fn appendCanonicalValue(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: std.json.Value) !void {
    switch (value) {
        .null => try out.appendSlice(alloc, "null"),
        .object => |object| {
            const keys = try alloc.alloc([]const u8, object.count());
            defer alloc.free(keys);
            var it = object.iterator();
            var i: usize = 0;
            while (it.next()) |entry| : (i += 1) keys[i] = entry.key_ptr.*;
            std.mem.sort([]const u8, keys, {}, struct {
                fn lt(_: void, l: []const u8, r: []const u8) bool {
                    return std.mem.lessThan(u8, l, r);
                }
            }.lt);
            try out.append(alloc, '{');
            for (keys, 0..) |key, j| {
                if (j != 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, key);
                try out.append(alloc, ':');
                try appendCanonicalValue(alloc, out, object.get(key).?);
            }
            try out.append(alloc, '}');
        },
        .array => |array| {
            try out.append(alloc, '[');
            for (array.items, 0..) |item, j| {
                if (j != 0) try out.append(alloc, ',');
                try appendCanonicalValue(alloc, out, item);
            }
            try out.append(alloc, ']');
        },
        else => {
            const scalar = (try canonicalScalarAlloc(alloc, value)).?;
            defer alloc.free(scalar);
            try out.appendSlice(alloc, scalar);
        },
    }
}

pub fn appendJsonString(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), s: []const u8) !void {
    const encoded = try std.json.Stringify.valueAlloc(alloc, s, .{});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

fn hashField(h: *Sha256, bytes: []const u8) void {
    var len: [8]u8 = undefined;
    std.mem.writeInt(u64, &len, bytes.len, .little);
    h.update(&len);
    h.update(bytes);
}

// ---------------------------------------------------------------------------
// Binding (dependent side)
// ---------------------------------------------------------------------------

/// One context source bound against a dependent source document.
pub const Bound = struct {
    source: *const Source,
    spec: Digest,
    /// `document`: the referenced key, null when `key_from` is missing.
    key: ?[]const u8 = null,
    /// `graph`: start node.
    start: ?[]const u8 = null,
    /// `query`: one resolved value list per conjunct (canonical scalars);
    /// meaningful for equality conjuncts only.
    values: []const []const []const u8 = &.{},
    /// A missing/non-scalar binding makes the match set statically empty.
    unsatisfiable: bool = false,
    /// Subscription tuples this dependent must register before reading.
    tuples: []const Digest = &.{},
    /// Canonical identity of the bound query (spec plus resolved values).
    query_identity: Digest = @splat(0),
    /// Resolved residual parameters (`{"c":[...],"b":...}`), stored as the
    /// subscription value so invalidation can evaluate changed rows per
    /// dependent. Empty when the source has no residual predicate.
    residual: []const u8 = "",
    /// `similar`/`search`: query vector from the dependent row.
    query_vector: []const f32 = &.{},
    /// `search`: query text.
    search_text: []const u8 = "",
};

pub const Bindings = struct {
    arena: std.heap.ArenaAllocator,
    items: []const Bound,

    pub fn deinit(self: *Bindings) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Union of every tuple this document subscribes to before reading,
    /// sorted and unique. Residual values are borrowed from the bindings.
    pub fn allTuplesAlloc(self: *const Bindings, alloc: Allocator) ![]SubscriptionTuple {
        var list = std.ArrayListUnmanaged(SubscriptionTuple).empty;
        errdefer list.deinit(alloc);
        for (self.items) |item| for (item.tuples) |tuple| try list.append(alloc, .{ .spec = item.spec, .tuple = tuple, .residual = item.residual });
        sortUniqueTuples(&list);
        return try list.toOwnedSlice(alloc);
    }
};

pub const SubscriptionTuple = struct {
    spec: Digest,
    tuple: Digest,
    /// Subscription value: the dependent's residual parameters (may be "").
    residual: []const u8 = "",

    pub fn lessThan(_: void, l: SubscriptionTuple, r: SubscriptionTuple) bool {
        return switch (std.mem.order(u8, &l.spec, &r.spec)) {
            .lt => true,
            .gt => false,
            .eq => std.mem.lessThan(u8, &l.tuple, &r.tuple),
        };
    }

    pub fn eql(l: SubscriptionTuple, r: SubscriptionTuple) bool {
        return std.mem.eql(u8, &l.spec, &r.spec) and std.mem.eql(u8, &l.tuple, &r.tuple);
    }
};

pub fn sortUniqueTuples(list: *std.ArrayListUnmanaged(SubscriptionTuple)) void {
    std.mem.sort(SubscriptionTuple, list.items, {}, SubscriptionTuple.lessThan);
    var w: usize = 0;
    for (list.items) |item| {
        if (w != 0 and list.items[w - 1].eql(item)) continue;
        list.items[w] = item;
        w += 1;
    }
    list.items.len = w;
}

pub fn documentTuple(spec: Digest, key: []const u8) Digest {
    var h = Sha256.init(.{});
    hashField(&h, "doc");
    h.update(&spec);
    hashField(&h, key);
    var out: Digest = undefined;
    h.final(&out);
    return out;
}

/// Adjacency subscription of one graph node: any direct edge written or
/// deleted at this endpoint can change the traversal through it.
pub fn graphTuple(spec: Digest, node: []const u8) Digest {
    var h = Sha256.init(.{});
    hashField(&h, "graph");
    h.update(&spec);
    hashField(&h, node);
    var out: Digest = undefined;
    h.final(&out);
    return out;
}

// ---------------------------------------------------------------------------
// Range-aware (interval) subscriptions
// ---------------------------------------------------------------------------
//
// Values map to order-preserving 64-bit keys (numbers by IEEE order, strings
// by their first 8 bytes, booleans 0/1), tagged by type. A dependent's
// interval subscribes to a small canonical cover of dyadic blocks
// `(level, key >> level)`; a changed value touches exactly its 64 ancestor
// blocks plus the root. Matching blocks prove only that the value *may* lie in
// the interval (string keys truncate, open bounds are widened), so the exact
// per-subscriber residual test still decides. No row change fans out to
// dependents whose interval cannot contain it.

pub const max_interval_blocks: usize = 16;
const root_level: u8 = 64;

pub const OrderKey = struct { tag: u8, key: u64 };

pub fn orderKey(value: std.json.Value) ?OrderKey {
    return switch (value) {
        .integer => |n| floatKey(@floatFromInt(n)),
        .float => |f| floatKey(f),
        .number_string => |text| floatKey(std.fmt.parseFloat(f64, text) catch return null),
        .string => |text| blk: {
            var buf: [8]u8 = @splat(0);
            const n = @min(text.len, 8);
            @memcpy(buf[0..n], text[0..n]);
            break :blk .{ .tag = 's', .key = std.mem.readInt(u64, &buf, .big) };
        },
        .bool => |b| .{ .tag = 'b', .key = @intFromBool(b) },
        else => null,
    };
}

fn floatKey(value: f64) ?OrderKey {
    if (!std.math.isFinite(value)) return null;
    const f = if (value == 0) 0 else value;
    const bits: u64 = @bitCast(f);
    return .{ .tag = 'n', .key = if (bits >> 63 == 1) ~bits else bits | (@as(u64, 1) << 63) };
}

pub const Block = struct { level: u8, prefix: u64 };

/// Canonical dyadic cover of `[lo, hi]` with at most `max_interval_blocks`
/// blocks (coarsened by raising the minimum level when needed).
pub fn coverBlocks(out: *std.ArrayListUnmanaged(Block), a: Allocator, lo: u64, hi: u64) !void {
    var floor: u7 = 0;
    while (true) : (floor += 1) {
        out.clearRetainingCapacity();
        const l0: u128 = lo >> @intCast(floor);
        const h0: u128 = hi >> @intCast(floor);
        const bits: u8 = 64 - @as(u8, floor);
        var l = l0;
        while (l <= h0) {
            var k: u8 = 0;
            while (k < bits) : (k += 1) {
                const size = @as(u128, 1) << @intCast(k + 1);
                if (l & (size - 1) != 0 or l + size - 1 > h0) break;
            }
            try out.append(a, .{ .level = k + floor, .prefix = @intCast(l >> @intCast(k)) });
            l += @as(u128, 1) << @intCast(k);
            if (out.items.len > max_interval_blocks) break;
        }
        if (out.items.len <= max_interval_blocks or floor == 63) return;
    }
}

fn blockTuple(base: Digest, tag: u8, block: Block) Digest {
    var h = Sha256.init(.{});
    hashField(&h, "interval");
    h.update(&base);
    if (block.level >= root_level) {
        hashField(&h, "root");
    } else {
        h.update(&.{ tag, block.level });
        var be: [8]u8 = undefined;
        std.mem.writeInt(u64, &be, block.prefix, .big);
        h.update(&be);
    }
    var out: Digest = undefined;
    h.final(&out);
    return out;
}

/// Field indexed by intervals: the first `range`/`prefix` conjunct, else the
/// top-N order field.
pub fn intervalField(source: *const Source) ?[]const u8 {
    for (source.conjuncts) |c| if (c.op == .range or c.op == .prefix) return c.field;
    if (source.order_by) |o| return o.field;
    return null;
}

fn intervalConjunct(source: *const Source) ?usize {
    for (source.conjuncts, 0..) |c, i| if (c.op == .range or c.op == .prefix) return i;
    return null;
}

/// A dependent's interval `[lo, hi]` (null = whole domain).
const Interval = ?struct { tag: u8, lo: u64, hi: u64 };

fn canonicalKey(a: Allocator, canonical: []const u8) ?OrderKey {
    const value = std.json.parseFromSliceLeaky(std.json.Value, a, canonical, .{}) catch return null;
    return orderKey(value);
}

fn rangeInterval(a: Allocator, lower: ?[]const u8, upper: ?[]const u8) Interval {
    const lo = if (lower) |v| canonicalKey(a, v) else null;
    const hi = if (upper) |v| canonicalKey(a, v) else null;
    if (lower != null and lo == null) return null;
    if (upper != null and hi == null) return null;
    if (lo == null and hi == null) return null;
    if (lo != null and hi != null and lo.?.tag != hi.?.tag) return null;
    const tag = if (lo) |k| k.tag else hi.?.tag;
    const l = if (lo) |k| k.key else 0;
    const h = if (hi) |k| k.key else std.math.maxInt(u64);
    if (l > h) return .{ .tag = tag, .lo = 1, .hi = 0 };
    return .{ .tag = tag, .lo = l, .hi = h };
}

fn prefixInterval(prefix: []const u8) Interval {
    var lo: [8]u8 = @splat(0);
    var hi: [8]u8 = @splat(0xff);
    const n = @min(prefix.len, 8);
    @memcpy(lo[0..n], prefix[0..n]);
    @memcpy(hi[0..n], prefix[0..n]);
    return .{ .tag = 's', .lo = std.mem.readInt(u64, &lo, .big), .hi = std.mem.readInt(u64, &hi, .big) };
}

fn intervalTuplesAlloc(a: Allocator, bases: []const Digest, interval: Interval) ![]const Digest {
    var blocks = std.ArrayListUnmanaged(Block).empty;
    var tag: u8 = 0;
    if (interval) |iv| {
        if (iv.lo > iv.hi) return &.{};
        tag = iv.tag;
        try coverBlocks(&blocks, a, iv.lo, iv.hi);
        if (blocks.items.len == 1 and blocks.items[0].level >= root_level) blocks.items[0] = .{ .level = root_level, .prefix = 0 };
    } else try blocks.append(a, .{ .level = root_level, .prefix = 0 });
    var out = std.ArrayListUnmanaged(Digest).empty;
    for (bases) |base| for (blocks.items) |block| try out.append(a, blockTuple(base, tag, block));
    std.mem.sort(Digest, out.items, {}, struct {
        fn lt(_: void, l: Digest, r: Digest) bool {
            return std.mem.lessThan(u8, &l, &r);
        }
    }.lt);
    return out.items;
}

/// Tuples for a top-N source once its boundary is known: a full result
/// subscribes only to values ranking at or above the boundary.
pub fn orderedTuplesAlloc(a: Allocator, bound: *const Bound, boundary: ?std.json.Value, full: bool) ![]const Digest {
    const source = bound.source;
    const order = source.order_by orelse return bound.tuples;
    if (bound.unsatisfiable) return &.{};
    if (intervalConjunct(source) != null) return bound.tuples;
    const bases = try equalityBasesAlloc(a, bound);
    var interval: Interval = null;
    if (full) if (boundary) |b| if (orderKey(b)) |k| {
        interval = if (order.descending) .{ .tag = k.tag, .lo = k.key, .hi = std.math.maxInt(u64) } else .{ .tag = k.tag, .lo = 0, .hi = k.key };
    };
    return intervalTuplesAlloc(a, bases, interval);
}

fn equalityBasesAlloc(a: Allocator, bound: *const Bound) ![]const Digest {
    var sets = std.ArrayListUnmanaged([]const []const u8).empty;
    for (bound.source.conjuncts, bound.values) |c, set| if (c.isEquality()) try sets.append(a, set);
    return expandTuplesAlloc(a, bound.spec, sets.items, max_set_values);
}

/// Row-side interval tuples: every ancestor block of each value, plus root.
fn rowIntervalTuplesAlloc(a: Allocator, source: *const Source, bases: []const Digest, row: std.json.Value) !?[]Digest {
    const field = intervalField(source).?;
    var out = std.ArrayListUnmanaged(Digest).empty;
    for (bases) |base| try out.append(a, blockTuple(base, 0, .{ .level = root_level, .prefix = 0 }));
    const value = resolveFieldPath(row, field) orelse {
        // A missing value can never satisfy a range/prefix; for top-N it
        // sorts last and only matters while the result is not full (root).
        if (intervalConjunct(source) != null) return null;
        return out.items;
    };
    const items: []const std.json.Value = switch (value) {
        .array => |array| array.items,
        else => (&value)[0..1],
    };
    for (items) |item| {
        const k = orderKey(item) orelse continue;
        for (bases) |base| {
            var level: u8 = 0;
            while (level < root_level) : (level += 1) {
                try out.append(a, blockTuple(base, k.tag, .{ .level = level, .prefix = k.key >> @intCast(level) }));
                if (out.items.len > max_row_interval_tuples) return error.ContextBudgetExceeded;
            }
        }
    }
    return out.items;
}

pub const max_row_interval_tuples: usize = 4096;

fn queryTuple(spec: Digest, values: []const []const u8) Digest {
    var h = Sha256.init(.{});
    hashField(&h, "query");
    h.update(&spec);
    for (values) |value| hashField(&h, value);
    var out: Digest = undefined;
    h.final(&out);
    return out;
}

pub fn bindAlloc(alloc: Allocator, config: *const Config, source_doc: std.json.Value) Error!Bindings {
    return bindForKeyAlloc(alloc, config, "", source_doc);
}

/// Bind every source against the dependent document (`doc_key` is its key,
/// the default graph start). Missing optional bindings are not errors: they
/// render as empty. Snapshot sources bind their parameters but subscribe to
/// nothing.
pub fn bindForKeyAlloc(alloc: Allocator, config: *const Config, doc_key: []const u8, source_doc: std.json.Value) Error!Bindings {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    const items = try a.alloc(Bound, config.sources.len);
    for (config.sources, 0..) |*source, i| {
        var bound = Bound{ .source = source, .spec = source.specDigest() };
        switch (source.kind) {
            .document => {
                bound.key = try bindKey(a, source_doc, source.key_from);
                if (bound.key) |key| bound.tuples = try oneTuple(a, documentTuple(bound.spec, key));
            },
            .graph => {
                bound.start = if (source.graph.start_from.len == 0)
                    (if (doc_key.len == 0) null else try a.dupe(u8, doc_key))
                else
                    try bindKey(a, source_doc, source.graph.start_from);
                if (bound.start) |start| bound.tuples = try oneTuple(a, graphTuple(bound.spec, start));
            },
            .query => try bindQuery(a, &bound, source_doc),
            .search => {
                const s = source.search;
                if (s.text.present()) {
                    const text = if (s.text.pointer.len != 0) resolvePointer(source_doc, s.text.pointer) else null;
                    bound.search_text = if (text) |t| (if (t == .string) try a.dupe(u8, t.string) else "") else if (s.text.literal.len != 0)
                        try literalString(a, s.text.literal)
                    else
                        "";
                    if (bound.search_text.len == 0) bound.unsatisfiable = true;
                } else {
                    bound.query_vector = (try vectorAt(a, source_doc, s.vector_from, 0)) orelse blk: {
                        bound.unsatisfiable = true;
                        break :blk &.{};
                    };
                }
                var h = Sha256.init(.{});
                h.update(&bound.spec);
                hashField(&h, bound.search_text);
                h.update(std.mem.sliceAsBytes(bound.query_vector));
                h.final(&bound.query_identity);
            },
        }
        if (!source.isLive()) bound.tuples = &.{};
        items[i] = bound;
    }
    return .{ .arena = arena, .items = items };
}

fn oneTuple(a: Allocator, digest: Digest) ![]const Digest {
    const out = try a.alloc(Digest, 1);
    out[0] = digest;
    return out;
}

fn bindKey(a: Allocator, doc: std.json.Value, pointer: []const u8) !?[]const u8 {
    const value = resolvePointer(doc, pointer) orelse return null;
    return switch (value) {
        .string => |s| if (s.len == 0) null else try a.dupe(u8, s),
        .integer, .float, .number_string => (try canonicalScalarAlloc(a, value)).?,
        else => null,
    };
}

fn literalString(a: Allocator, canonical: []const u8) ![]const u8 {
    const parsed = std.json.parseFromSliceLeaky(std.json.Value, a, canonical, .{}) catch return "";
    return if (parsed == .string) parsed.string else "";
}

fn vectorAt(a: Allocator, root: std.json.Value, pointer_or_field: []const u8, dims: u32) !?[]const f32 {
    const value = (if (pointer_or_field.len != 0 and pointer_or_field[0] == '/') resolvePointer(root, pointer_or_field) else resolveFieldPath(root, pointer_or_field)) orelse return null;
    if (value != .array or value.array.items.len == 0 or value.array.items.len > max_vector_dims) return null;
    if (dims != 0 and value.array.items.len != dims) return null;
    const out = try a.alloc(f32, value.array.items.len);
    for (value.array.items, out) |item, *o| o.* = switch (item) {
        .float => |f| @floatCast(f),
        .integer => |n| @floatFromInt(n),
        .number_string => |s| std.fmt.parseFloat(f32, s) catch return null,
        else => return null,
    };
    return out;
}

fn resolveOperand(a: Allocator, doc: std.json.Value, operand: Operand) !?[]const u8 {
    if (operand.literal.len != 0) return operand.literal;
    const value = resolvePointer(doc, operand.pointer) orelse return null;
    return try canonicalScalarAlloc(a, value);
}

fn bindQuery(a: Allocator, bound: *Bound, source_doc: std.json.Value) !void {
    const source = bound.source;
    const values = try a.alloc([]const []const u8, source.conjuncts.len);
    var residual = std.ArrayListUnmanaged(u8).empty;
    try residual.appendSlice(a, "{\"c\":[");
    var equality_sets = std.ArrayListUnmanaged([]const []const u8).empty;
    for (source.conjuncts, 0..) |c, j| {
        if (j != 0) try residual.append(a, ',');
        values[j] = &.{};
        switch (c.op) {
            .term, .terms => {
                try residual.appendSlice(a, "null");
                if (!c.isBound()) {
                    values[j] = c.literals;
                } else if (resolvePointer(source_doc, c.source_pointer)) |resolved| {
                    var list = std.ArrayListUnmanaged([]const u8).empty;
                    switch (resolved) {
                        .array => |array| if (c.op == .terms) {
                            if (array.items.len > max_set_values) return error.ContextBudgetExceeded;
                            for (array.items) |item| try list.append(a, (try canonicalScalarAlloc(a, item)) orelse return error.InvalidContextBinding);
                        },
                        .null => {},
                        else => try list.append(a, (try canonicalScalarAlloc(a, resolved)) orelse return error.InvalidContextBinding),
                    }
                    sortUniqueStrings(&list);
                    values[j] = list.items;
                }
                if (values[j].len == 0) bound.unsatisfiable = true;
                try equality_sets.append(a, values[j]);
            },
            .range => {
                try residual.append(a, '{');
                var first = true;
                inline for (.{ "gt", "gte", "lt", "lte" }) |name| {
                    const operand = @field(c.range, name);
                    if (operand.present()) {
                        const v = (try resolveOperand(a, source_doc, operand)) orelse blk: {
                            bound.unsatisfiable = true;
                            break :blk "null";
                        };
                        if (!first) try residual.append(a, ',');
                        first = false;
                        try residual.appendSlice(a, "\"" ++ name ++ "\":");
                        try residual.appendSlice(a, v);
                    }
                }
                try residual.append(a, '}');
            },
            .prefix => {
                const v = (try resolveOperand(a, source_doc, .{ .literal = if (c.literals.len != 0) c.literals[0] else "", .pointer = c.source_pointer })) orelse "null";
                if (v[0] != '"') bound.unsatisfiable = true;
                try residual.appendSlice(a, "{\"p\":");
                try residual.appendSlice(a, if (v[0] == '"') v else "\"\"");
                try residual.append(a, '}');
            },
            .match => {
                const v = (try resolveOperand(a, source_doc, .{ .literal = if (c.literals.len != 0) c.literals[0] else "", .pointer = c.source_pointer })) orelse "null";
                const text = if (v[0] == '"') try literalString(a, v) else "";
                const tokens = try tokenizeAlloc(a, text);
                if (tokens.len == 0) bound.unsatisfiable = true;
                try residual.appendSlice(a, "{\"t\":[");
                for (tokens, 0..) |t, k| {
                    if (k != 0) try residual.append(a, ',');
                    try appendJsonString(a, &residual, t);
                }
                try residual.appendSlice(a, "]}");
            },
            .similar => {
                const vector = (try vectorAt(a, source_doc, c.source_pointer, c.similarity.dims)) orelse blk: {
                    bound.unsatisfiable = true;
                    break :blk &.{};
                };
                bound.query_vector = vector;
                try residual.appendSlice(a, "{\"v\":[");
                for (vector, 0..) |f, k| {
                    if (k != 0) try residual.append(a, ',');
                    try residual.print(a, "{d}", .{f});
                }
                try residual.appendSlice(a, "]}");
            },
        }
    }
    try residual.appendSlice(a, "]}");
    bound.values = values;
    if (source.hasResidual()) bound.residual = residual.items;
    var h = Sha256.init(.{});
    hashField(&h, "query-identity");
    h.update(&bound.spec);
    for (values) |set| {
        for (set) |value| hashField(&h, value);
        hashField(&h, "|");
    }
    hashField(&h, bound.residual);
    h.final(&bound.query_identity);
    if (!bound.unsatisfiable) {
        const bases = try expandTuplesAlloc(a, bound.spec, equality_sets.items, max_set_values);
        if (intervalConjunct(source)) |j| {
            const c = source.conjuncts[j];
            const interval: Interval = switch (c.op) {
                .range => blk: {
                    var lower: ?[]const u8 = null;
                    var upper: ?[]const u8 = null;
                    if (c.range.gte.present()) lower = try resolveOperand(a, source_doc, c.range.gte);
                    if (c.range.gt.present()) lower = try resolveOperand(a, source_doc, c.range.gt);
                    if (c.range.lte.present()) upper = try resolveOperand(a, source_doc, c.range.lte);
                    if (c.range.lt.present()) upper = try resolveOperand(a, source_doc, c.range.lt);
                    break :blk rangeInterval(a, lower, upper);
                },
                .prefix => blk: {
                    const v = (try resolveOperand(a, source_doc, .{ .literal = if (c.literals.len != 0) c.literals[0] else "", .pointer = c.source_pointer })) orelse break :blk null;
                    break :blk prefixInterval(try literalString(a, v));
                },
                else => unreachable,
            };
            bound.tuples = try intervalTuplesAlloc(a, bases, interval);
        } else if (source.order_by != null) {
            // Before the boundary is known: the whole domain.
            bound.tuples = try intervalTuplesAlloc(a, bases, null);
        } else bound.tuples = bases;
    }
}

/// Cartesian product of the equality value sets (no sets: one spec-wide
/// tuple shared by every dependent of the source).
fn expandTuplesAlloc(a: Allocator, spec: Digest, values: []const []const []const u8, cap: usize) Error![]const Digest {
    var total: usize = 1;
    for (values) |set| {
        if (set.len == 0) return &.{};
        total = std.math.mul(usize, total, set.len) catch return error.ContextBudgetExceeded;
        if (total > cap) return error.ContextBudgetExceeded;
    }
    const out = try a.alloc(Digest, total);
    const cursor = try a.alloc(usize, values.len);
    defer a.free(cursor);
    @memset(cursor, 0);
    const scratch = try a.alloc([]const u8, values.len);
    defer a.free(scratch);
    for (out) |*digest| {
        for (values, 0..) |set, j| scratch[j] = set[cursor[j]];
        digest.* = queryTuple(spec, scratch);
        var j = values.len;
        while (j > 0) {
            j -= 1;
            cursor[j] += 1;
            if (cursor[j] < values[j].len) break;
            cursor[j] = 0;
        }
    }
    std.mem.sort(Digest, out, {}, struct {
        fn lt(_: void, l: Digest, r: Digest) bool {
            return std.mem.lessThan(u8, &l, &r);
        }
    }.lt);
    return out;
}

// ---------------------------------------------------------------------------
// Per-row predicate evaluation
// ---------------------------------------------------------------------------

/// Pinned simple analyzer: lowercase ASCII, split on ASCII non-alphanumerics
/// (non-ASCII bytes stay inside tokens), sorted unique.
pub fn tokenizeAlloc(a: Allocator, text: []const u8) ![]const []const u8 {
    var list = std.ArrayListUnmanaged([]const u8).empty;
    var start: ?usize = null;
    var i: usize = 0;
    while (i <= text.len) : (i += 1) {
        const word = i < text.len and (std.ascii.isAlphanumeric(text[i]) or text[i] >= 0x80);
        if (word) {
            if (start == null) start = i;
        } else if (start) |s| {
            const token = try a.alloc(u8, i - s);
            for (text[s..i], token) |c, *o| o.* = std.ascii.toLower(c);
            try list.append(a, token);
            start = null;
        }
    }
    sortUniqueStrings(&list);
    return list.items;
}

/// Equality conjuncts of a bound query against a row.
fn equalityMatches(bound: *const Bound, row: std.json.Value) bool {
    if (bound.unsatisfiable) return false;
    for (bound.source.conjuncts, bound.values) |c, set| {
        if (!c.isEquality()) continue;
        const value = resolveFieldPath(row, c.field) orelse return false;
        if (!valueInSet(value, set)) return false;
    }
    return true;
}

/// Full predicate of a bound query (equality and residual, no top-N bound).
pub fn rowMatches(alloc: Allocator, bound: *const Bound, row: std.json.Value) !bool {
    if (!equalityMatches(bound, row)) return false;
    if (bound.residual.len == 0) return true;
    return residualMatches(alloc, bound.source, bound.residual, "", row, false);
}

/// Score of `similar` for a row (null when the row has no comparable vector).
pub fn similarityScore(alloc: Allocator, conjunct: Conjunct, query: []const f32, row: std.json.Value) !?f64 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const vector = (try vectorAt(arena.allocator(), row, conjunct.field, conjunct.similarity.dims)) orelse return null;
    if (vector.len != query.len) return null;
    var dot: f64 = 0;
    var na: f64 = 0;
    var nb: f64 = 0;
    for (vector, query) |x, y| {
        dot += @as(f64, x) * y;
        na += @as(f64, x) * x;
        nb += @as(f64, y) * y;
    }
    return switch (conjunct.similarity.metric) {
        .dot => dot,
        .cosine => if (na == 0 or nb == 0) null else dot / (@sqrt(na) * @sqrt(nb)),
    };
}

/// Evaluate a dependent's residual parameters (its subscription value) on
/// one row. With `check_boundary`, a full top-N result additionally requires
/// the row to rank at or above the recorded boundary (ties by key).
pub fn residualMatches(alloc: Allocator, source: *const Source, residual_json: []const u8, key: []const u8, row: std.json.Value, check_boundary: bool) !bool {
    if (residual_json.len == 0) return true;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const parsed = std.json.parseFromSliceLeaky(std.json.Value, a, residual_json, .{}) catch return true;
    if (parsed != .object) return true;
    if (parsed.object.get("c")) |entries| if (entries == .array) {
        if (entries.array.items.len == source.conjuncts.len) for (source.conjuncts, entries.array.items) |c, entry| {
            if (c.isEquality() or entry != .object) continue;
            const value = resolveFieldPath(row, c.field) orelse return false;
            const ok = switch (c.op) {
                .range => try rangeMatches(a, entry.object, value),
                .prefix => blk: {
                    const p = entry.object.get("p") orelse break :blk false;
                    break :blk p == .string and anyString(value, p.string, startsWith);
                },
                .match => blk: {
                    const tokens = entry.object.get("t") orelse break :blk false;
                    if (tokens != .array) break :blk false;
                    break :blk try matchTokens(a, value, tokens.array.items, c.match_operator);
                },
                .similar => blk: {
                    const v = entry.object.get("v") orelse break :blk false;
                    if (v != .array) break :blk false;
                    const query = try a.alloc(f32, v.array.items.len);
                    for (v.array.items, query) |item, *o| o.* = switch (item) {
                        .float => |f| @floatCast(f),
                        .integer => |n| @floatFromInt(n),
                        else => 0,
                    };
                    const score = (try similarityScore(a, c, query, row)) orelse break :blk false;
                    break :blk score >= c.similarity.threshold;
                },
                .term, .terms => unreachable,
            };
            if (!ok) return false;
        };
    };
    if (!check_boundary) return true;
    const order = source.order_by orelse return true;
    const boundary = parsed.object.get("b") orelse return true;
    if (boundary != .object) return true;
    const full = boundary.object.get("full") orelse return true;
    if (full != .bool or !full.bool) return true;
    const bv = boundary.object.get("v") orelse return true;
    const bk = boundary.object.get("k") orelse return true;
    if (bk != .string) return true;
    const rv = resolveFieldPath(row, order.field);
    return orderCompare(a, rv, key, bv, bk.string, order.descending) != .gt;
}

fn startsWith(haystack: []const u8, needle: []const u8) bool {
    return std.mem.startsWith(u8, haystack, needle);
}

fn anyString(value: std.json.Value, arg: []const u8, comptime pred: fn ([]const u8, []const u8) bool) bool {
    switch (value) {
        .string => |s| return pred(s, arg),
        .array => |array| {
            for (array.items) |item| if (item == .string and pred(item.string, arg)) return true;
            return false;
        },
        else => return false,
    }
}

fn matchTokens(a: Allocator, value: std.json.Value, query: []const std.json.Value, op: MatchOperator) !bool {
    var row_tokens = std.ArrayListUnmanaged([]const u8).empty;
    switch (value) {
        .string => |s| try row_tokens.appendSlice(a, try tokenizeAlloc(a, s)),
        .array => |array| for (array.items) |item| if (item == .string) try row_tokens.appendSlice(a, try tokenizeAlloc(a, item.string)),
        else => return false,
    }
    var hits: usize = 0;
    for (query) |q| {
        if (q != .string) continue;
        for (row_tokens.items) |t| if (std.mem.eql(u8, t, q.string)) {
            hits += 1;
            break;
        };
    }
    return switch (op) {
        .@"and" => hits == query.len and query.len != 0,
        .@"or" => hits != 0,
    };
}

fn scalarOrder(a: Allocator, lhs: std.json.Value, rhs: std.json.Value) !?std.math.Order {
    const l = (try canonicalScalarAlloc(a, lhs)) orelse return null;
    const r = (try canonicalScalarAlloc(a, rhs)) orelse return null;
    const l_string = l[0] == '"';
    const r_string = r[0] == '"';
    if (l_string != r_string) return null;
    if (l_string) return std.mem.order(u8, l, r);
    const lf = std.fmt.parseFloat(f64, l) catch return null;
    const rf = std.fmt.parseFloat(f64, r) catch return null;
    return std.math.order(lf, rf);
}

fn rangeMatches(a: Allocator, bounds: std.json.ObjectMap, value: std.json.Value) !bool {
    const items: []const std.json.Value = switch (value) {
        .array => |array| array.items,
        else => (&value)[0..1],
    };
    for (items) |item| {
        var ok = true;
        var it = bounds.iterator();
        while (it.next()) |entry| {
            const order = (try scalarOrder(a, item, entry.value_ptr.*)) orelse {
                ok = false;
                break;
            };
            const name = entry.key_ptr.*;
            const pass = if (std.mem.eql(u8, name, "gt")) order == .gt else if (std.mem.eql(u8, name, "gte")) order != .lt else if (std.mem.eql(u8, name, "lt")) order == .lt else order != .gt;
            if (!pass) {
                ok = false;
                break;
            }
        }
        if (ok) return true;
    }
    return false;
}

/// Sort order of (value, key): by value (missing values last), then key.
/// Returns `.lt` when the first sorts before the second.
pub fn orderCompare(a: Allocator, value: ?std.json.Value, key: []const u8, other: std.json.Value, other_key: []const u8, descending: bool) std.math.Order {
    const missing_l = value == null or value.? == .null;
    const missing_r = other == .null;
    if (missing_l != missing_r) return if (missing_l) .gt else .lt;
    if (!missing_l) {
        const by_value = (scalarOrder(a, value.?, other) catch null) orelse .eq;
        const directed = if (descending) by_value.invert() else by_value;
        if (directed != .eq) return directed;
    }
    return std.mem.order(u8, key, other_key);
}

/// Record a top-N boundary (the last member) in a dependent's residual.
pub fn withBoundaryAlloc(alloc: Allocator, residual: []const u8, boundary_value: ?std.json.Value, boundary_key: []const u8, full: bool) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    if (residual.len > 2 and residual[residual.len - 1] == '}') {
        try out.appendSlice(alloc, residual[0 .. residual.len - 1]);
        try out.append(alloc, ',');
    } else try out.appendSlice(alloc, "{\"c\":[],");
    try out.print(alloc, "\"b\":{{\"full\":{},\"k\":", .{full});
    try appendJsonString(alloc, &out, boundary_key);
    try out.appendSlice(alloc, ",\"v\":");
    if (boundary_value) |v| try appendCanonicalValue(alloc, &out, v) else try out.appendSlice(alloc, "null");
    try out.appendSlice(alloc, "}}");
    return try out.toOwnedSlice(alloc);
}

fn valueInSet(value: std.json.Value, set: []const []const u8) bool {
    var buf: [256]u8 = undefined;
    switch (value) {
        .array => |array| {
            for (array.items) |item| if (scalarInSet(&buf, item, set)) return true;
            return false;
        },
        else => return scalarInSet(&buf, value, set),
    }
}

fn scalarInSet(buf: []u8, value: std.json.Value, set: []const []const u8) bool {
    var fba = std.heap.FixedBufferAllocator.init(buf);
    const scalar = (canonicalScalarAlloc(fba.allocator(), value) catch return slowScalarInSet(value, set)) orelse return false;
    for (set) |candidate| if (std.mem.eql(u8, candidate, scalar)) return true;
    return false;
}

fn slowScalarInSet(value: std.json.Value, set: []const []const u8) bool {
    const scalar = (canonicalScalarAlloc(std.heap.page_allocator, value) catch return false) orelse return false;
    defer std.heap.page_allocator.free(scalar);
    for (set) |candidate| if (std.mem.eql(u8, candidate, scalar)) return true;
    return false;
}

// ---------------------------------------------------------------------------
// Change side: which subscriptions does an input row change touch?
// ---------------------------------------------------------------------------

/// Tuples of a candidate row over the equality conjuncts of a query source:
/// for every equality conjunct, the row values that could satisfy *some*
/// dependent binding (any value for bound conjuncts, only matching literals
/// for literal ones). Residual conjuncts are evaluated per subscriber.
pub const RowTuples = union(enum) {
    /// Row matches no dependent (a literal conjunct failed or a field is missing).
    none,
    tuples: []Digest,
    /// Expansion exceeded `max_row_tuples`: notify every subscriber.
    all,

    pub fn deinit(self: *RowTuples, alloc: Allocator) void {
        switch (self.*) {
            .tuples => |t| alloc.free(t),
            else => {},
        }
        self.* = .none;
    }
};

pub fn rowTuplesAlloc(alloc: Allocator, source: *const Source, spec: Digest, row: std.json.Value) !RowTuples {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var sets = std.ArrayListUnmanaged([]const []const u8).empty;
    for (source.conjuncts) |c| {
        if (!c.isEquality()) continue;
        const value = resolveFieldPath(row, c.field) orelse return .none;
        var list = std.ArrayListUnmanaged([]const u8).empty;
        const items: []const std.json.Value = switch (value) {
            .array => |array| array.items,
            else => (&value)[0..1],
        };
        for (items) |item| {
            const scalar = (try canonicalScalarAlloc(a, item)) orelse continue;
            if (!c.isBound()) {
                var hit = false;
                for (c.literals) |lit| if (std.mem.eql(u8, lit, scalar)) {
                    hit = true;
                };
                if (!hit) continue;
            }
            try list.append(a, scalar);
        }
        sortUniqueStrings(&list);
        if (list.items.len == 0) return .none;
        try sets.append(a, list.items);
    }
    const expanded = expandTuplesAlloc(a, spec, sets.items, max_row_tuples) catch |err| switch (err) {
        error.ContextBudgetExceeded => return .all,
        else => |e| return e,
    };
    if (intervalField(source) != null) {
        const interval = (rowIntervalTuplesAlloc(a, source, expanded, row) catch |err| switch (err) {
            error.ContextBudgetExceeded => return .all,
            else => |e| return e,
        }) orelse return .none;
        std.mem.sort(Digest, interval, {}, struct {
            fn lt(_: void, l: Digest, r: Digest) bool {
                return std.mem.lessThan(u8, &l, &r);
            }
        }.lt);
        var w: usize = 0;
        for (interval) |d| {
            if (w != 0 and std.mem.eql(u8, &interval[w - 1], &d)) continue;
            interval[w] = d;
            w += 1;
        }
        return .{ .tuples = try alloc.dupe(Digest, interval[0..w]) };
    }
    return .{ .tuples = try alloc.dupe(Digest, expanded) };
}

pub const ChangeKind = enum {
    member_added,
    member_removed,
    member_changed,
    document_created,
    document_deleted,
    document_changed,
    edge_changed,

    pub fn jsonName(self: ChangeKind) []const u8 {
        return @tagName(self);
    }
};

pub const TouchedTuple = struct {
    tuple: Digest,
    change: ChangeKind,
};

pub const Touched = struct {
    /// Every subscriber of the source must be notified (row expansion overflow).
    all: bool = false,
    all_change: ChangeKind = .member_changed,
    tuples: []TouchedTuple = &.{},

    pub fn deinit(self: *Touched, alloc: Allocator) void {
        alloc.free(self.tuples);
        self.* = .{};
    }
};

/// Compute the subscription tuples a single input row change touches, given
/// its previous and next values (null when absent). Changes that keep the row
/// in the same tuple with an identical projection of relevant fields touch
/// nothing: irrelevant field updates never invalidate. Residual predicates
/// and top-N boundaries are not decided here (they depend on each
/// subscriber's parameters); a touched tuple is filtered per subscriber.
pub fn touchedTuplesAlloc(
    alloc: Allocator,
    source: *const Source,
    key: []const u8,
    previous: ?std.json.Value,
    next: ?std.json.Value,
) !Touched {
    if (!source.isLive() or source.kind == .search) return .{};
    const spec = source.specDigest();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const old_projection: ?[]u8 = if (previous) |row| try projectTreeAlloc(a, row, source.relevant_fields) else null;
    const new_projection: ?[]u8 = if (next) |row| try projectTreeAlloc(a, row, source.relevant_fields) else null;
    const projection_equal = old_projection != null and new_projection != null and
        std.mem.eql(u8, old_projection.?, new_projection.?);

    var out = std.ArrayListUnmanaged(TouchedTuple).empty;
    errdefer out.deinit(alloc);
    switch (source.kind) {
        .search => unreachable,
        .document, .graph => {
            if (previous == null and next == null) return .{};
            if (projection_equal) return .{};
            const change: ChangeKind = if (previous == null) .document_created else if (next == null) .document_deleted else .document_changed;
            try out.append(alloc, .{ .tuple = documentTuple(spec, key), .change = change });
        },
        .query => {
            var old_tuples: RowTuples = if (previous) |row| try rowTuplesAlloc(a, source, spec, row) else .none;
            var new_tuples: RowTuples = if (next) |row| try rowTuplesAlloc(a, source, spec, row) else .none;
            if (old_tuples == .all or new_tuples == .all) {
                const change: ChangeKind = if (old_tuples == .none) .member_added else if (new_tuples == .none) .member_removed else .member_changed;
                return .{ .all = true, .all_change = change };
            }
            const old_list: []const Digest = if (old_tuples == .tuples) old_tuples.tuples else &.{};
            const new_list: []const Digest = if (new_tuples == .tuples) new_tuples.tuples else &.{};
            var i: usize = 0;
            var j: usize = 0;
            while (i < old_list.len or j < new_list.len) {
                const take_old = j >= new_list.len or (i < old_list.len and std.mem.lessThan(u8, &old_list[i], &new_list[j]));
                const take_new = i >= old_list.len or (j < new_list.len and std.mem.lessThan(u8, &new_list[j], &old_list[i]));
                if (take_old) {
                    try out.append(alloc, .{ .tuple = old_list[i], .change = .member_removed });
                    i += 1;
                } else if (take_new) {
                    try out.append(alloc, .{ .tuple = new_list[j], .change = .member_added });
                    j += 1;
                } else {
                    if (!projection_equal) try out.append(alloc, .{ .tuple = old_list[i], .change = .member_changed });
                    i += 1;
                    j += 1;
                }
            }
            old_tuples.deinit(a);
            new_tuples.deinit(a);
        },
    }
    return .{ .tuples = try out.toOwnedSlice(alloc) };
}

/// Whether a notice about one row is relevant to one subscriber, given the
/// subscriber's residual parameters and the row's previous/next relevant
/// projections (`null`/empty when absent).
pub fn subscriberRelevant(alloc: Allocator, source: *const Source, residual: []const u8, key: []const u8, previous: ?std.json.Value, next: ?std.json.Value) !bool {
    if (residual.len == 0 or !source.hasResidual()) return true;
    if (previous) |row| if (try residualMatches(alloc, source, residual, key, row, true)) return true;
    if (next) |row| if (try residualMatches(alloc, source, residual, key, row, true)) return true;
    return false;
}

// ---------------------------------------------------------------------------
// Resolved context, rendering, fingerprints
// ---------------------------------------------------------------------------

/// One input actually consumed. `revision` is the content digest of the
/// projected values, an engine-neutral revision that is stable across
/// replicas and identical for equivalent inputs.
pub const ConsumedInput = struct {
    key: []const u8,
    projection: []const u8,
    /// Extra member fields rendered before `value` (graph path, rank, ...),
    /// e.g. `"depth":1,"edge_type":"cites"`.
    extra: []const u8 = "",

    pub fn revision(self: ConsumedInput) Digest {
        var h = Sha256.init(.{});
        hashField(&h, self.key);
        hashField(&h, self.projection);
        var out: Digest = undefined;
        h.final(&out);
        return out;
    }
};

pub const ResolvedSource = struct {
    bound: *const Bound,
    /// Members sorted by key unless `ordered`; for `document` at most one.
    members: []const ConsumedInput,
    /// Members are in their declared order (top-N, graph depth, search rank).
    ordered: bool = false,
    /// `document`: whether the referenced document existed.
    present: bool = false,
    /// Opaque reader frontier position for the source table/shard(s).
    frontier: []const u8 = "",
};

pub const Rendered = struct {
    /// `{"organization":{...}|null,"policies":{"count":n,"members":[...]}}`.
    json: []u8,
    fingerprint: Digest,
    bytes: u64,

    pub fn deinit(self: *Rendered, alloc: Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

/// Canonically render resolved context and enforce declared budgets. Callers
/// pass at most `max_documents + 1` members so overflow is detected rather
/// than silently truncated (ordered top-N is truncated by declaration).
pub fn renderAlloc(alloc: Allocator, resolved: []const ResolvedSource) Error!Rendered {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '{');
    for (resolved, 0..) |item, i| {
        const source = item.bound.source;
        if (i != 0) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, source.name);
        try out.append(alloc, ':');
        const start = out.items.len;
        switch (source.kind) {
            .document => {
                if (!item.present or item.members.len == 0) {
                    if (source.required) return error.RequiredContextMissing;
                    try out.appendSlice(alloc, "null");
                } else {
                    const member = item.members[0];
                    try out.appendSlice(alloc, "{\"key\":");
                    try appendJsonString(alloc, &out, member.key);
                    try out.appendSlice(alloc, ",\"value\":");
                    try out.appendSlice(alloc, member.projection);
                    try out.append(alloc, '}');
                }
            },
            .query, .graph, .search => {
                if (item.members.len > source.max_documents) return error.ContextBudgetExceeded;
                try out.print(alloc, "{{\"count\":{d},\"members\":[", .{item.members.len});
                var previous: ?[]const u8 = null;
                for (item.members, 0..) |member, j| {
                    if (!item.ordered) {
                        if (previous) |p| if (!std.mem.lessThan(u8, p, member.key)) return error.InvalidContextBinding;
                        previous = member.key;
                    }
                    if (j != 0) try out.append(alloc, ',');
                    try out.appendSlice(alloc, "{\"key\":");
                    try appendJsonString(alloc, &out, member.key);
                    if (member.extra.len != 0) {
                        try out.append(alloc, ',');
                        try out.appendSlice(alloc, member.extra);
                    }
                    try out.appendSlice(alloc, ",\"value\":");
                    try out.appendSlice(alloc, member.projection);
                    try out.append(alloc, '}');
                }
                try out.appendSlice(alloc, "]");
                if (!source.isLive()) try out.appendSlice(alloc, ",\"tracking\":\"snapshot\"");
                try out.append(alloc, '}');
            },
        }
        if (out.items.len - start > source.max_bytes) return error.ContextBudgetExceeded;
    }
    try out.append(alloc, '}');
    var fingerprint: Digest = undefined;
    Sha256.hash(out.items, &fingerprint, .{});
    const bytes = out.items.len;
    return .{ .json = try out.toOwnedSlice(alloc), .fingerprint = fingerprint, .bytes = bytes };
}

/// The documented producer input contract appended to rendered source text:
/// a marker line carrying the fingerprint, then `{"context":{...}}`.
pub fn producerBlockAlloc(alloc: Allocator, rendered: *const Rendered) ![]u8 {
    return std.fmt.allocPrint(alloc, "[context sha256:{x}]\n{{\"context\":{s}}}", .{ &rendered.fingerprint, rendered.json });
}

pub fn digestHex(digest: Digest) [Sha256.digest_length * 2]u8 {
    return std.fmt.bytesToHex(digest, .lower);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

const issue_example =
    \\{
    \\  "organization": {
    \\    "document": {"table": "organizations", "key_from": "/organization_id"},
    \\    "fields": ["voice", "guidance"],
    \\    "required": true
    \\  },
    \\  "policies": {
    \\    "query": {
    \\      "table": "policies",
    \\      "filter_query": {"conjuncts": [
    \\        {"field": "tenant_id", "term": {"$source": "/tenant_id"}},
    \\        {"field": "organization_id", "term": {"$source": "/organization_id"}},
    \\        {"field": "locale", "term": {"$source": "/locale"}},
    \\        {"field": "status", "term": "approved"}
    \\      ]}
    \\    },
    \\    "fields": ["policy_id", "revision", "payload"],
    \\    "max_documents": 500,
    \\    "max_bytes": 1048576,
    \\    "overflow": "error"
    \\  }
    \\}
;

fn parseTestConfig(alloc: Allocator) !Config {
    return parsePublicJson(alloc, issue_example, "{\"mode\":\"background\",\"debounce\":\"2s\"}");
}

fn parseValue(arena: Allocator, raw: []const u8) !std.json.Value {
    return (try std.json.parseFromSliceLeaky(std.json.Value, arena, raw, .{}));
}

test "context sources parse, canonicalize, and round trip through stored form" {
    const alloc = testing.allocator;
    var config = try parseTestConfig(alloc);
    defer config.deinit();
    try testing.expectEqual(@as(usize, 2), config.sources.len);
    try testing.expectEqualStrings("organization", config.sources[0].name);
    try testing.expectEqual(SourceKind.document, config.sources[0].kind);
    try testing.expect(config.sources[0].required);
    try testing.expectEqualStrings("guidance", config.sources[0].fields[0]);
    const policies = config.find("policies").?;
    try testing.expectEqual(@as(usize, 4), policies.conjuncts.len);
    // Conjuncts are canonicalized by field name.
    try testing.expectEqualStrings("locale", policies.conjuncts[0].field);
    try testing.expectEqualStrings("status", policies.conjuncts[2].field);
    try testing.expectEqualStrings("tenant_id", policies.conjuncts[3].field);
    try testing.expectEqualStrings("\"approved\"", policies.conjuncts[2].literals[0]);
    try testing.expectEqual(@as(u64, 2000), config.refresh.debounce_ms);

    const stored = try config.canonicalJsonAlloc(alloc);
    defer alloc.free(stored);
    var reparsed = try parseStoredJson(alloc, stored);
    defer reparsed.deinit();
    const again = try reparsed.canonicalJsonAlloc(alloc);
    defer alloc.free(again);
    try testing.expectEqualStrings(stored, again);
    try testing.expectEqualSlices(u8, &config.digest(), &reparsed.digest());

    // Reordered declaration has an identical identity.
    var reordered = try parsePublicJson(alloc,
        \\{"policies":{"fields":["revision","payload","policy_id"],"max_documents":500,"query":{"table":"policies","filter_query":{"conjuncts":[
        \\ {"field":"status","term":"approved"},{"field":"locale","term":{"$source":"/locale"}},
        \\ {"field":"organization_id","term":{"$source":"/organization_id"}},{"field":"tenant_id","term":{"$source":"/tenant_id"}}]}}},
        \\ "organization":{"required":true,"fields":["guidance","voice"],"document":{"key_from":"/organization_id","table":"organizations"}}}
    , "{\"debounce\":\"2000ms\"}");
    defer reordered.deinit();
    try testing.expectEqualSlices(u8, &config.digest(), &reordered.digest());

    const public = try config.publicRefreshJsonAlloc(alloc);
    defer alloc.free(public);
    try testing.expectEqualStrings("{\"mode\":\"background\",\"debounce\":\"2s\"}", public);
}

test "context sources reject unsupported predicates and malformed declarations" {
    const alloc = testing.allocator;
    const bad = [_][]const u8{
        "{}",
        "{\"a\":{\"document\":{\"key_from\":\"/k\"}}}", // no fields
        "{\"a\":{\"document\":{\"key_from\":\"k\"},\"fields\":[\"x\"]}}", // bad pointer
        "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"_artifacts\"]}}", // reserved field
        "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"x\",\"x\"]}}",
        "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"x\"],\"max_documents\":3}}",
        "{\"a\":{\"query\":{\"filter_query\":{\"field\":\"f\",\"term\":\"v\"}},\"fields\":[\"x\"],\"required\":true}}",
        "{\"a\":{\"query\":{\"filter_query\":{\"field\":\"f\",\"fuzzy\":\"x\"}},\"fields\":[\"x\"]}}",
        "{\"a\":{\"query\":{\"filter_query\":{\"disjuncts\":[]}},\"fields\":[\"x\"]}}",
        "{\"a\":{\"query\":{\"filter_query\":{\"conjuncts\":[{\"field\":\"f\",\"terms\":{\"$source\":\"/a\"}},{\"field\":\"g\",\"terms\":{\"$source\":\"/b\"}}]}},\"fields\":[\"x\"]}}",
        "{\"a\":{\"query\":{\"filter_query\":{\"field\":\"f\",\"term\":{\"$source\":\"/a\",\"x\":1}}},\"fields\":[\"x\"]}}",
        "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"x\"],\"overflow\":\"truncate\"}}",
        "{\"a\":{\"document\":{\"key_from\":\"/k\",\"table_id\":\"7\"},\"fields\":[\"x\"]}}", // identity is engine-owned
        "{\"bad name\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"x\"]}}",
    };
    for (bad) |raw| {
        if (parsePublicJson(alloc, raw, "")) |parsed| {
            var config = parsed;
            config.deinit();
            std.debug.print("unexpectedly admitted: {s}\n", .{raw});
            return error.TestUnexpectedResult;
        } else |_| {}
    }
    try testing.expectError(error.InvalidContextSourcesConfig, parsePublicJson(alloc, "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"x\"]}}", "{\"mode\":\"eventually\"}"));
}

test "bindings resolve typed source values and subscription tuples" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var config = try parseTestConfig(alloc);
    defer config.deinit();
    const doc = try parseValue(arena.allocator(), "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\"}");
    var bindings = try bindAlloc(alloc, &config, doc);
    defer bindings.deinit();
    const org = bindings.items[0];
    try testing.expectEqualStrings("org-1", org.key.?);
    try testing.expectEqual(@as(usize, 1), org.tuples.len);
    const policies = bindings.items[1];
    try testing.expect(!policies.unsatisfiable);
    try testing.expectEqual(@as(usize, 1), policies.tuples.len);

    // The policy row's own tuple equals the dependent's subscription tuple.
    const row = try parseValue(arena.allocator(), "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\",\"payload\":\"p\"}");
    try testing.expect((try rowMatches(alloc, &policies, row)));
    var row_tuples = try rowTuplesAlloc(alloc, policies.source, policies.spec, row);
    defer row_tuples.deinit(alloc);
    try testing.expectEqualSlices(u8, &policies.tuples[0], &row_tuples.tuples[0]);

    // A draft row matches no dependent; another organization's row has a different tuple.
    const draft = try parseValue(arena.allocator(), "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"draft\"}");
    try testing.expect(!(try rowMatches(alloc, &policies, draft)));
    var draft_tuples = try rowTuplesAlloc(alloc, policies.source, policies.spec, draft);
    defer draft_tuples.deinit(alloc);
    try testing.expect(draft_tuples == .none);
    const other = try parseValue(arena.allocator(), "{\"tenant_id\":\"t2\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\"}");
    var other_tuples = try rowTuplesAlloc(alloc, policies.source, policies.spec, other);
    defer other_tuples.deinit(alloc);
    try testing.expect(!std.mem.eql(u8, &policies.tuples[0], &other_tuples.tuples[0]));

    // Missing bindings: empty query, absent document key, no subscriptions.
    const sparse = try parseValue(arena.allocator(), "{\"tenant_id\":\"t1\"}");
    var unbound = try bindAlloc(alloc, &config, sparse);
    defer unbound.deinit();
    try testing.expect(unbound.items[0].key == null);
    try testing.expect(unbound.items[1].unsatisfiable);
    const all = try unbound.allTuplesAlloc(alloc);
    defer alloc.free(all);
    try testing.expectEqual(@as(usize, 0), all.len);
}

test "set membership bindings expand one tuple per bound value" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var config = try parsePublicJson(alloc,
        \\{"terms":{"query":{"filter_query":{"conjuncts":[{"field":"locale","terms":{"$source":"/locales"}},{"field":"kind","terms":["glossary","style"]}]}},"fields":["text"]}}
    , "");
    defer config.deinit();
    const doc = try parseValue(arena.allocator(), "{\"locales\":[\"en-GB\",\"fr-FR\",\"en-GB\"]}");
    var bindings = try bindAlloc(alloc, &config, doc);
    defer bindings.deinit();
    // 2 distinct locales x 2 literal kinds.
    try testing.expectEqual(@as(usize, 4), bindings.items[0].tuples.len);
    const row = try parseValue(arena.allocator(), "{\"locale\":\"fr-FR\",\"kind\":[\"style\",\"other\"],\"text\":\"t\"}");
    try testing.expect((try rowMatches(alloc, &bindings.items[0], row)));
    var tuples = try rowTuplesAlloc(alloc, bindings.items[0].source, bindings.items[0].spec, row);
    defer tuples.deinit(alloc);
    try testing.expectEqual(@as(usize, 1), tuples.tuples.len);
    var found = false;
    for (bindings.items[0].tuples) |t| {
        if (std.mem.eql(u8, &t, &tuples.tuples[0])) found = true;
    }
    try testing.expect(found);
}

test "touched tuples ignore irrelevant updates and report membership transitions" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var config = try parseTestConfig(alloc);
    defer config.deinit();
    const policies = config.find("policies").?;
    const approved = try parseValue(a, "{\"tenant_id\":\"t1\",\"organization_id\":\"o\",\"locale\":\"en-GB\",\"status\":\"approved\",\"payload\":\"v1\",\"note\":\"x\"}");
    const approved_note = try parseValue(a, "{\"tenant_id\":\"t1\",\"organization_id\":\"o\",\"locale\":\"en-GB\",\"status\":\"approved\",\"payload\":\"v1\",\"note\":\"y\"}");
    const approved_v2 = try parseValue(a, "{\"tenant_id\":\"t1\",\"organization_id\":\"o\",\"locale\":\"en-GB\",\"status\":\"approved\",\"payload\":\"v2\"}");
    const withdrawn = try parseValue(a, "{\"tenant_id\":\"t1\",\"organization_id\":\"o\",\"locale\":\"en-GB\",\"status\":\"withdrawn\",\"payload\":\"v2\"}");

    var irrelevant = try touchedTuplesAlloc(alloc, policies, "p1", approved, approved_note);
    defer irrelevant.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), irrelevant.tuples.len);

    var changed = try touchedTuplesAlloc(alloc, policies, "p1", approved, approved_v2);
    defer changed.deinit(alloc);
    try testing.expectEqual(@as(usize, 1), changed.tuples.len);
    try testing.expectEqual(ChangeKind.member_changed, changed.tuples[0].change);

    var left = try touchedTuplesAlloc(alloc, policies, "p1", approved_v2, withdrawn);
    defer left.deinit(alloc);
    try testing.expectEqual(@as(usize, 1), left.tuples.len);
    try testing.expectEqual(ChangeKind.member_removed, left.tuples[0].change);

    var inserted = try touchedTuplesAlloc(alloc, policies, "p2", null, approved);
    defer inserted.deinit(alloc);
    try testing.expectEqual(ChangeKind.member_added, inserted.tuples[0].change);

    var deleted = try touchedTuplesAlloc(alloc, policies, "p2", approved, null);
    defer deleted.deinit(alloc);
    try testing.expectEqual(ChangeKind.member_removed, deleted.tuples[0].change);

    var never = try touchedTuplesAlloc(alloc, policies, "p3", null, withdrawn);
    defer never.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), never.tuples.len);

    // Document sources: creation of a missing optional reference is a touch.
    const org = config.find("organization").?;
    const org_row = try parseValue(a, "{\"voice\":\"warm\",\"guidance\":\"g\",\"logo\":\"a.png\"}");
    const org_logo = try parseValue(a, "{\"voice\":\"warm\",\"guidance\":\"g\",\"logo\":\"b.png\"}");
    var created = try touchedTuplesAlloc(alloc, org, "org-1", null, org_row);
    defer created.deinit(alloc);
    try testing.expectEqual(ChangeKind.document_created, created.tuples[0].change);
    try testing.expectEqualSlices(u8, &documentTuple(org.specDigest(), "org-1"), &created.tuples[0].tuple);
    var logo = try touchedTuplesAlloc(alloc, org, "org-1", org_row, org_logo);
    defer logo.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), logo.tuples.len);
}

test "rendering is canonical, fingerprinted, and enforces budgets explicitly" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var config = try parsePublicJson(alloc,
        \\{"org":{"document":{"key_from":"/org"},"fields":["voice"],"required":true},
        \\ "rules":{"query":{"filter_query":{"field":"org","term":{"$source":"/org"}}},"fields":["text"],"max_documents":2,"max_bytes":200}}
    , "");
    defer config.deinit();
    var bindings = try bindAlloc(alloc, &config, try parseValue(a, "{\"org\":\"o1\"}"));
    defer bindings.deinit();
    const org_proj = try projectAlloc(a, try parseValue(a, "{\"voice\":{\"b\":1,\"a\":[1.0,\"x\"]},\"ignored\":true}"), config.sources[0].fields);
    try testing.expectEqualStrings("{\"voice\":{\"a\":[1,\"x\"],\"b\":1}}", org_proj);
    const members = [_]ConsumedInput{ .{ .key = "r1", .projection = "{\"text\":\"a\"}" }, .{ .key = "r2", .projection = "{\"text\":\"b\"}" } };
    var rendered = try renderAlloc(alloc, &.{
        .{ .bound = &bindings.items[0], .members = &.{.{ .key = "o1", .projection = org_proj }}, .present = true },
        .{ .bound = &bindings.items[1], .members = &members },
    });
    defer rendered.deinit(alloc);
    try testing.expectEqualStrings(
        "{\"org\":{\"key\":\"o1\",\"value\":{\"voice\":{\"a\":[1,\"x\"],\"b\":1}}},\"rules\":{\"count\":2,\"members\":[{\"key\":\"r1\",\"value\":{\"text\":\"a\"}},{\"key\":\"r2\",\"value\":{\"text\":\"b\"}}]}}",
        rendered.json,
    );
    var again = try renderAlloc(alloc, &.{
        .{ .bound = &bindings.items[0], .members = &.{.{ .key = "o1", .projection = org_proj }}, .present = true },
        .{ .bound = &bindings.items[1], .members = &members },
    });
    defer again.deinit(alloc);
    try testing.expectEqualSlices(u8, &rendered.fingerprint, &again.fingerprint);
    const block = try producerBlockAlloc(alloc, &rendered);
    defer alloc.free(block);
    try testing.expect(std.mem.startsWith(u8, block, "[context sha256:"));

    // Empty result is complete and distinct from a missing optional document.
    var empty = try renderAlloc(alloc, &.{
        .{ .bound = &bindings.items[0], .members = &.{.{ .key = "o1", .projection = org_proj }}, .present = true },
        .{ .bound = &bindings.items[1], .members = &.{} },
    });
    defer empty.deinit(alloc);
    try testing.expect(std.mem.endsWith(u8, empty.json, "\"rules\":{\"count\":0,\"members\":[]}}"));

    // Required missing document and member/byte overflow fail explicitly.
    try testing.expectError(error.RequiredContextMissing, renderAlloc(alloc, &.{.{ .bound = &bindings.items[0], .members = &.{}, .present = false }}));
    const three = [_]ConsumedInput{ .{ .key = "r1", .projection = "{}" }, .{ .key = "r2", .projection = "{}" }, .{ .key = "r3", .projection = "{}" } };
    try testing.expectError(error.ContextBudgetExceeded, renderAlloc(alloc, &.{.{ .bound = &bindings.items[1], .members = &three }}));
    const filler: [300]u8 = @splat('x');
    const big = [_]ConsumedInput{.{ .key = "r1", .projection = "{\"text\":\"" ++ filler ++ "\"}" }};
    try testing.expectError(error.ContextBudgetExceeded, renderAlloc(alloc, &.{.{ .bound = &bindings.items[1], .members = &big }}));
    // Unordered members are a caller bug, not silently reordered output.
    const unordered = [_]ConsumedInput{ .{ .key = "r2", .projection = "{}" }, .{ .key = "r1", .projection = "{}" } };
    try testing.expectError(error.InvalidContextBinding, renderAlloc(alloc, &.{.{ .bound = &bindings.items[1], .members = &unordered }}));
}

test "table names resolve to immutable identities and client identities are not trusted" {
    const alloc = testing.allocator;
    const Map = struct {
        fn resolve(_: *anyopaque, a: Allocator, table: []const u8) anyerror!?[]u8 {
            if (std.mem.eql(u8, table, "policies")) return try a.dupe(u8, "tbl-42");
            return null;
        }
    };
    var dummy: u8 = 0;
    const resolver = TableResolver{ .ptr = &dummy, .resolve_fn = Map.resolve };
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const public = try parseValue(arena.allocator(), "{\"p\":{\"query\":{\"table\":\"policies\",\"filter_query\":{\"field\":\"org\",\"term\":{\"$source\":\"/org\"}}},\"fields\":[\"text\"]}}");
    const stored = try canonicalFromPublicAlloc(alloc, public, null, resolver);
    defer alloc.free(stored);
    try testing.expect(std.mem.indexOf(u8, stored, "\"table_id\":\"tbl-42\"") != null);
    // A forged identity in a stored form is replaced by re-resolution.
    const forged = try std.mem.replaceOwned(u8, alloc, stored, "tbl-42", "tbl-evil");
    defer alloc.free(forged);
    const fixed = try reresolveStoredAlloc(alloc, forged, resolver);
    defer alloc.free(fixed);
    try testing.expectEqualStrings(stored, fixed);
    const unknown = try parseValue(arena.allocator(), "{\"p\":{\"document\":{\"table\":\"nope\",\"key_from\":\"/k\"},\"fields\":[\"x\"]}}");
    try testing.expectError(error.UnknownContextSourceTable, canonicalFromPublicAlloc(alloc, unknown, null, resolver));
    var config = try parseStoredJson(alloc, stored);
    defer config.deinit();
    const foreign = try foreignTableIdsAlloc(alloc, &config, "tbl-1");
    defer alloc.free(foreign);
    try testing.expectEqual(@as(usize, 1), foreign.len);
}

test "duration parsing" {
    try testing.expectEqual(@as(?u64, 2000), parseDurationMs("2s"));
    try testing.expectEqual(@as(?u64, 250), parseDurationMs("250ms"));
    try testing.expectEqual(@as(?u64, 120_000), parseDurationMs("2m"));
    try testing.expectEqual(@as(?u64, 3_600_000), parseDurationMs("1h"));
    try testing.expectEqual(@as(?u64, null), parseDurationMs("s"));
    try testing.expectEqual(@as(?u64, null), parseDurationMs("2x"));
    try testing.expectEqual(@as(?u64, null), parseDurationMs("-2s"));
}

fn bindTest(alloc: Allocator, config: *const Config, raw: []const u8) !Bindings {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    return bindForKeyAlloc(alloc, config, "dep", try parseValue(arena.allocator(), raw));
}

test "range, prefix, and filter-mode match are residual per-row predicates" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var config = try parsePublicJson(alloc,
        \\{"recent":{"query":{"filter_query":{"conjuncts":[
        \\  {"field":"org","term":{"$source":"/org"}},
        \\  {"field":"published","range":{"gte":{"$source":"/since"},"lt":"2027-01-01"}},
        \\  {"field":"sku","prefix":"EU-"},
        \\  {"field":"body","match":{"$source":"/topic"},"operator":"and"}]}},
        \\ "fields":["title"]}}
    , "");
    defer config.deinit();
    const source = &config.sources[0];
    try testing.expect(source.hasResidual());
    try testing.expectEqualStrings("body", source.relevant_fields[0]);
    var bindings = try bindTest(alloc, &config, "{\"org\":\"o1\",\"since\":\"2026-06-01\",\"topic\":\"Solar Panels\"}");
    defer bindings.deinit();
    const bound = &bindings.items[0];
    try testing.expect(bound.tuples.len >= 1 and bound.tuples.len <= max_interval_blocks);
    const hit = try parseValue(a, "{\"org\":\"o1\",\"published\":\"2026-07-04\",\"sku\":\"EU-12\",\"body\":\"New solar panels, cheaper.\",\"title\":\"t\"}");
    try testing.expect(try rowMatches(alloc, bound, hit));
    for ([_][]const u8{
        "{\"org\":\"o1\",\"published\":\"2026-05-04\",\"sku\":\"EU-12\",\"body\":\"solar panels\"}",
        "{\"org\":\"o1\",\"published\":\"2027-02-01\",\"sku\":\"EU-12\",\"body\":\"solar panels\"}",
        "{\"org\":\"o1\",\"published\":\"2026-07-04\",\"sku\":\"US-12\",\"body\":\"solar panels\"}",
        "{\"org\":\"o1\",\"published\":\"2026-07-04\",\"sku\":\"EU-12\",\"body\":\"solar only\"}",
    }) |raw| try testing.expect(!try rowMatches(alloc, bound, try parseValue(a, raw)));
    // Subscriber relevance: a change entirely outside the window is ignored,
    // moving into or out of the window is relevant.
    const outside_a = try parseValue(a, "{\"published\":\"2025-01-01\",\"sku\":\"EU-1\",\"body\":\"solar panels\"}");
    const outside_b = try parseValue(a, "{\"published\":\"2025-02-01\",\"sku\":\"EU-1\",\"body\":\"solar panels\"}");
    try testing.expect(!try subscriberRelevant(alloc, source, bound.residual, "k", outside_a, outside_b));
    try testing.expect(try subscriberRelevant(alloc, source, bound.residual, "k", outside_a, hit));
    try testing.expect(try subscriberRelevant(alloc, source, bound.residual, "k", hit, null));
    // Residual-field changes are relevant to touched tuples; unprojected ones are not.
    var touched = try touchedTuplesAlloc(alloc, source, "k", hit, try parseValue(a, "{\"org\":\"o1\",\"published\":\"2026-07-05\",\"sku\":\"EU-12\",\"body\":\"New solar panels, cheaper.\",\"title\":\"t\"}"));
    defer touched.deinit(alloc);
    try testing.expect(touched.tuples.len >= 1);
    var ignored = try touchedTuplesAlloc(alloc, source, "k", hit, try parseValue(a, "{\"org\":\"o1\",\"published\":\"2026-07-04\",\"sku\":\"EU-12\",\"body\":\"New solar panels, cheaper.\",\"title\":\"t\",\"x\":1}"));
    defer ignored.deinit(alloc);
    try testing.expectEqual(@as(usize, 0), ignored.tuples.len);
}

test "similarity threshold is an exact per-row predicate over pinned vectors" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var config = try parsePublicJson(alloc,
        \\{"near":{"query":{"filter_query":{"conjuncts":[{"field":"kind","term":"faq"},
        \\  {"field":"vec","similar":{"vector":{"$source":"/vec"},"threshold":0.9,"dims":3,"model":"m@1"}}]}},"fields":["q"]}}
    , "");
    defer config.deinit();
    var bindings = try bindTest(alloc, &config, "{\"vec\":[1,0,0]}");
    defer bindings.deinit();
    const bound = &bindings.items[0];
    try testing.expect(try rowMatches(alloc, bound, try parseValue(a, "{\"kind\":\"faq\",\"vec\":[0.99,0.05,0]}")));
    try testing.expect(!try rowMatches(alloc, bound, try parseValue(a, "{\"kind\":\"faq\",\"vec\":[0,1,0]}")));
    try testing.expect(!try rowMatches(alloc, bound, try parseValue(a, "{\"kind\":\"faq\",\"vec\":[1,0]}")));
    // A model change is a declaration change.
    var other = try parsePublicJson(alloc,
        \\{"near":{"query":{"filter_query":{"conjuncts":[{"field":"kind","term":"faq"},
        \\  {"field":"vec","similar":{"vector":{"$source":"/vec"},"threshold":0.9,"dims":3,"model":"m@2"}}]}},"fields":["q"]}}
    , "");
    defer other.deinit();
    try testing.expect(!std.mem.eql(u8, &config.digest(), &other.digest()));
    try testing.expectError(error.InvalidContextSourcesConfig, parsePublicJson(alloc,
        \\{"n":{"query":{"filter_query":{"field":"v","similar":{"vector":{"$source":"/v"},"threshold":2,"dims":3,"model":"m"}}},"fields":["q"]}}
    , ""));
}

test "ordered top-N relevance follows the recorded boundary" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var config = try parsePublicJson(alloc,
        \\{"latest":{"query":{"filter_query":{"field":"org","term":{"$source":"/org"}},"order_by":{"field":"at","order":"desc"}},"fields":["title"],"max_documents":2}}
    , "");
    defer config.deinit();
    const source = &config.sources[0];
    var bindings = try bindTest(alloc, &config, "{\"org\":\"o\"}");
    defer bindings.deinit();
    const residual = try withBoundaryAlloc(alloc, bindings.items[0].residual, std.json.Value{ .integer = 50 }, "k5", true);
    defer alloc.free(residual);
    const older = try parseValue(a, "{\"org\":\"o\",\"at\":10}");
    const older2 = try parseValue(a, "{\"org\":\"o\",\"at\":11}");
    const newer = try parseValue(a, "{\"org\":\"o\",\"at\":90}");
    try testing.expect(!try subscriberRelevant(alloc, source, residual, "k9", older, older2));
    try testing.expect(try subscriberRelevant(alloc, source, residual, "k9", null, newer));
    try testing.expect(try subscriberRelevant(alloc, source, residual, "k1", newer, older));
    // Not yet full: every match is relevant.
    const partial = try withBoundaryAlloc(alloc, bindings.items[0].residual, std.json.Value{ .integer = 50 }, "k5", false);
    defer alloc.free(partial);
    try testing.expect(try subscriberRelevant(alloc, source, partial, "k9", older, older2));
}

test "graph and snapshot search sources bind and validate" {
    const alloc = testing.allocator;
    var config = try parsePublicJson(alloc,
        \\{"cites":{"graph":{"graph_index":"refs","edge_types":["cites"],"depth":2,"max_fanout":5},"fields":["title"],"max_documents":20},
        \\ "similar_docs":{"search":{"vector":{"$source":"/embedding"},"vector_index":"dense","k":5},"fields":["title"],"tracking":"snapshot","refresh_interval":"1h"}}
    , "");
    defer config.deinit();
    var bindings = try bindTest(alloc, &config, "{\"embedding\":[0.1,0.2]}");
    defer bindings.deinit();
    const cites = bindings.items[0];
    try testing.expectEqualStrings("dep", cites.start.?);
    try testing.expectEqualSlices(u8, &graphTuple(cites.spec, "dep"), &cites.tuples[0]);
    const search = bindings.items[1];
    try testing.expectEqual(@as(usize, 0), search.tuples.len);
    try testing.expectEqual(@as(usize, 2), search.query_vector.len);
    try testing.expect(config.hasSnapshot());
    try testing.expectEqual(@as(u64, 3_600_000), config.snapshotRefreshMs());
    const stored = try config.canonicalJsonAlloc(alloc);
    defer alloc.free(stored);
    var again = try parseStoredJson(alloc, stored);
    defer again.deinit();
    try testing.expectEqualSlices(u8, &config.digest(), &again.digest());
    // Ranked retrieval cannot be tracked live; depth and fanout are bounded.
    try testing.expectError(error.UnsupportedContextPredicate, parsePublicJson(alloc,
        \\{"s":{"search":{"text":"solar","text_field":"body","k":5},"fields":["t"]}}
    , ""));
    try testing.expectError(error.InvalidContextSourcesConfig, parsePublicJson(alloc,
        \\{"g":{"graph":{"graph_index":"refs","depth":4},"fields":["t"]}}
    , ""));
    try testing.expectError(error.UnsupportedContextPredicate, parsePublicJson(alloc,
        \\{"q":{"query":{"filter_query":{"disjuncts":[]}},"fields":["t"]}}
    , ""));
}

test "pinned analyzer tokenizes deterministically" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const tokens = try tokenizeAlloc(arena.allocator(), "Solar-Panels, SOLAR café!");
    try testing.expectEqual(@as(usize, 3), tokens.len);
    try testing.expectEqualStrings("café", tokens[0]);
    try testing.expectEqualStrings("panels", tokens[1]);
    try testing.expectEqualStrings("solar", tokens[2]);
}

test "artifact virtual fields are admitted and recorded per source" {
    const alloc = testing.allocator;
    var config = try parsePublicJson(alloc,
        \\{"labels":{"query":{"filter_query":{"conjuncts":[{"field":"org","term":{"$source":"/org"}},{"field":"_artifacts.classify.label","term":"policy"},
        \\  {"field":"_artifacts.emb","similar":{"vector":{"$source":"/v"},"threshold":0.8,"dims":2,"model":"m"}}]}},"fields":["title","_artifacts.summary"]},
        \\ "g":{"graph":{"graph_index":"kg","include_generated":true},"fields":["title"]}}
    , "");
    defer config.deinit();
    const labels = config.find("labels").?;
    try testing.expectEqual(@as(usize, 3), labels.artifact_names.len);
    try testing.expectEqualStrings("classify", labels.artifact_names[0]);
    try testing.expectEqualStrings("emb", labels.artifact_names[1]);
    try testing.expectEqualStrings("summary", labels.artifact_names[2]);
    try testing.expect(config.readsGenerated());
    try testing.expectEqualStrings("summary", artifactOfField("_artifacts.summary").?);
    try testing.expect(artifactOfField("title") == null);
    try testing.expectError(error.InvalidContextSourcesConfig, parsePublicJson(alloc, "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"_embeddings\"]}}", ""));
    try testing.expectError(error.InvalidContextSourcesConfig, parsePublicJson(alloc, "{\"a\":{\"document\":{\"key_from\":\"/k\"},\"fields\":[\"_artifacts.\"]}}", ""));
}

test "tree projection keeps nested paths resolvable" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const row = try parseValue(arena.allocator(), "{\"_artifacts\":{\"c\":{\"label\":\"x\",\"n\":1},\"s\":\"t\"},\"a\":{\"b\":2},\"z\":3}");
    const tree = try projectTreeAlloc(alloc, row, &.{ "_artifacts.c.label", "a.b", "missing" });
    defer alloc.free(tree);
    try testing.expectEqualStrings("{\"_artifacts\":{\"c\":{\"label\":\"x\"}},\"a\":{\"b\":2}}", tree);
    const again = try parseValue(arena.allocator(), tree);
    try testing.expectEqualStrings("x", resolveFieldPath(again, "_artifacts.c.label").?.string);
}

test "interval subscriptions touch only dependents whose range can contain a value" {
    const alloc = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var config = try parsePublicJson(alloc,
        \\{"w":{"query":{"filter_query":{"field":"at","range":{"gte":{"$source":"/lo"},"lt":{"$source":"/hi"}}}},"fields":["t"],"max_documents":50}}
    , "");
    defer config.deinit();
    var narrow = try bindTest(alloc, &config, "{\"lo\":100,\"hi\":200}");
    defer narrow.deinit();
    var wide = try bindTest(alloc, &config, "{\"lo\":-1e9,\"hi\":1e9}");
    defer wide.deinit();
    try testing.expect(narrow.items[0].tuples.len >= 1 and narrow.items[0].tuples.len <= max_interval_blocks);
    const source = &config.sources[0];
    const spec = source.specDigest();
    const Hit = struct {
        fn any(sub: []const Digest, row: []const Digest) bool {
            for (sub) |s1| for (row) |r| if (std.mem.eql(u8, &s1, &r)) return true;
            return false;
        }
    };
    for ([_]struct { v: []const u8, narrow: bool, wide: bool }{
        .{ .v = "{\"at\":150}", .narrow = true, .wide = true },
        .{ .v = "{\"at\":100}", .narrow = true, .wide = true },
        .{ .v = "{\"at\":5000}", .narrow = false, .wide = true },
        .{ .v = "{\"at\":-7}", .narrow = false, .wide = true },
        .{ .v = "{\"at\":\"150\"}", .narrow = false, .wide = false },
    }) |case| {
        var row = try rowTuplesAlloc(alloc, source, spec, try parseValue(a, case.v));
        defer row.deinit(alloc);
        const tuples: []const Digest = if (row == .tuples) row.tuples else &.{};
        try testing.expectEqual(case.narrow, Hit.any(narrow.items[0].tuples, tuples));
        try testing.expectEqual(case.wide, Hit.any(wide.items[0].tuples, tuples));
    }
    // Missing field never matches a range.
    var missing = try rowTuplesAlloc(alloc, source, spec, try parseValue(a, "{}"));
    defer missing.deinit(alloc);
    try testing.expect(missing == .none);

    // Prefix and top-N boundaries use the same index.
    var prefixed = try parsePublicJson(alloc,
        \\{"p":{"query":{"filter_query":{"field":"sku","prefix":{"$source":"/p"}}},"fields":["t"]}}
    , "");
    defer prefixed.deinit();
    var pb = try bindTest(alloc, &prefixed, "{\"p\":\"EU-\"}");
    defer pb.deinit();
    for ([_]struct { v: []const u8, hit: bool }{ .{ .v = "{\"sku\":\"EU-12\"}", .hit = true }, .{ .v = "{\"sku\":\"US-12\"}", .hit = false } }) |case| {
        var row = try rowTuplesAlloc(alloc, &prefixed.sources[0], prefixed.sources[0].specDigest(), try parseValue(a, case.v));
        defer row.deinit(alloc);
        try testing.expectEqual(case.hit, Hit.any(pb.items[0].tuples, row.tuples));
    }
    var ordered = try parsePublicJson(alloc,
        \\{"top":{"query":{"filter_query":{"field":"org","term":{"$source":"/org"}},"order_by":{"field":"at","order":"desc"}},"fields":["t"],"max_documents":2}}
    , "");
    defer ordered.deinit();
    var ob = try bindTest(alloc, &ordered, "{\"org\":\"o\"}");
    defer ob.deinit();
    const boundary_tuples = try orderedTuplesAlloc(a, &ob.items[0], std.json.Value{ .integer = 50 }, true);
    for ([_]struct { v: []const u8, hit: bool }{ .{ .v = "{\"org\":\"o\",\"at\":90}", .hit = true }, .{ .v = "{\"org\":\"o\",\"at\":10}", .hit = false }, .{ .v = "{\"org\":\"x\",\"at\":90}", .hit = false } }) |case| {
        var row = try rowTuplesAlloc(alloc, &ordered.sources[0], ordered.sources[0].specDigest(), try parseValue(a, case.v));
        defer row.deinit(alloc);
        const tuples: []const Digest = if (row == .tuples) row.tuples else &.{};
        try testing.expectEqual(case.hit, Hit.any(boundary_tuples, tuples));
    }
    // Not full yet: every member of the tuple is relevant (root).
    const open_tuples = try orderedTuplesAlloc(a, &ob.items[0], std.json.Value{ .integer = 50 }, false);
    var low = try rowTuplesAlloc(alloc, &ordered.sources[0], ordered.sources[0].specDigest(), try parseValue(a, "{\"org\":\"o\",\"at\":10}"));
    defer low.deinit(alloc);
    try testing.expect(Hit.any(open_tuples, low.tuples));
}

test "dyadic cover is exact and bounded" {
    const alloc = testing.allocator;
    var blocks = std.ArrayListUnmanaged(Block).empty;
    defer blocks.deinit(alloc);
    try coverBlocks(&blocks, alloc, 4, 11);
    try testing.expectEqual(@as(usize, 2), blocks.items.len); // [4,7] [8,11]
    try coverBlocks(&blocks, alloc, 0, std.math.maxInt(u64));
    try testing.expectEqual(@as(usize, 1), blocks.items.len);
    try testing.expectEqual(@as(u8, 64), blocks.items[0].level);
    try coverBlocks(&blocks, alloc, 3, 1 << 40);
    try testing.expect(blocks.items.len <= max_interval_blocks);
    for (blocks.items) |b| try testing.expect(b.level <= 64);
}
