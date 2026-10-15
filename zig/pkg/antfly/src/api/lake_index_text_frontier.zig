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

//! Publication-wide, metadata-only best-first admission. Disjoint hash ranges
//! use the term tree's authenticated reductions; inventories stay unopened
//! until a singleton wins. Under frontier pressure, a bounded DFS completes
//! the overflow subtree without dropping or repeating candidates.
const std = @import("std");
const local = @import("antfly_local_sources");
const corpus = @import("lake_index_native_text.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const page_store = @import("../serverless/graph_segment/page_store.zig");
const Token = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Id = [32]u8;
const Plan = local.search_search.TextNavigationPlan;
const ties = @import("lake_index_text_ties.zig");
const Position = ties.Position;
const Metrics = @import("lake_query_metrics.zig").Metrics;
const Node = struct {
    first: Id,
    last: Id,
    upper: f32,
    minimum: ?Position = null,
    fn priority(_: void, l: Node, r: Node) std.math.Order {
        const score = std.math.order(r.upper, l.upper);
        if (score == .eq and l.minimum != null and r.minimum != null) {
            const order = ties.compare(l.minimum.?, r.minimum.?);
            if (order != .eq) return order;
        }
        return if (score == .eq) std.mem.order(u8, &l.first, &r.first) else score;
    }
};
const Self = @This();
const Pending = struct { identity: Id, bytes: u64 };
a: A,
budget: local.sql_memory_budget,
scratch: std.heap.ArenaAllocator = undefined,
syntax: std.heap.ArenaAllocator = undefined,
root: corpus.Root,
recent: ?*const local.index.IndexSnapshot = null,
store: stores.ArtifactStore,
cached: artifacts.CachedRead,
token: Token,
reads: u64 = 256 * 1024 * 1024,
charged_bytes: u64 = 0,
metrics: ?*Metrics = null,
admission_windows: u64 = 0,
writes: u64 = 0,
pages: page_store.PageStore = undefined,
cache: tree.Cache = undefined,
plan: ?*Plan = null,
heap: std.PriorityQueue(Node, void, Node.priority) = .initContext({}),
// Binary hash ranges have at most 256 branching ancestors.
overflow: [257]Node = undefined,
overflow_len: usize = 0,
frontier_limit: usize = 512,
pruned: bool = false,
pruned_ranges: u64 = 0,
overflow_subtrees: u64 = 0,
reductions: u64 = 0,
admitted: u64 = 0,
summary_cache_hits: u64 = 0,
prefetch_reads: u64 = 8 * 1024 * 1024,
prefetch_tasks: usize = 0,
pending: ?Pending = null,
selection: ?[]const Id = null,
proof: ?*@import("lake_index_pack_proof.zig") = null,
predicate_probe: ?struct { ptr: *anyopaque, allows: *const fn (*anyopaque, Id) anyerror!bool } = null,
pattern_matchers: std.ArrayList(struct { query: local.search_search.SearchQuery, matcher: *local.search_search.TermPattern }) = .empty,
pattern_compilations: u64 = 0,
pattern_reductions: u64 = 0,
score_ceilings: std.AutoHashMapUnmanaged([32]u8, f32) = .empty,
score_ceiling_hits: u64 = 0,
fn scoreCeiling(self: *Self, envelope: corpus.statistics.Summary, avg: f32, config: local.section_inverted.BM25Config, idf: f32, legacy: bool) !?f32 {
    var hash = std.crypto.hash.Blake3.init(.{});
    var encoded: [corpus.statistics.Summary.max_bytes]u8 = undefined;
    hash.update(envelope.encode(&encoded));
    var constants: [17]u8 = undefined;
    for ([_]f32{ avg, config.k1, config.b, idf }, 0..) |value, i| std.mem.writeInt(u32, constants[i * 4 ..][0..4], @bitCast(value), .little);
    constants[16] = @intFromBool(legacy);
    hash.update(&constants);
    var key: [32]u8 = undefined;
    hash.final(&key);
    if (self.score_ceilings.get(key)) |value| {
        self.score_ceiling_hits += 1;
        return value;
    }
    const value = envelope.exactScoreCeiling(avg, config, idf, legacy) orelse return null;
    if (self.score_ceilings.count() < 256) try self.score_ceilings.put(self.budget.allocator(), key, value);
    return value;
}
fn patternMatcher(self: *Self, query: local.search_search.SearchQuery) !*local.search_search.TermPattern {
    for (self.pattern_matchers.items) |entry| if (std.meta.activeTag(entry.query) == std.meta.activeTag(query)) {
        const equal = switch (query) {
            inline .prefix, .wildcard, .regexp, .fuzzy => |value, tag| std.meta.eql(value, @field(entry.query, @tagName(tag))),
            else => false,
        };
        if (equal) return entry.matcher;
    };
    const a = self.budget.allocator();
    const matcher = try local.search_search.TermPattern.create(a, query);
    errdefer matcher.deinit();
    try self.pattern_matchers.append(a, .{ .query = query, .matcher = matcher });
    self.pattern_compilations += 1;
    return matcher;
}
pub fn restrictProof(self: *Self, proof: *@import("lake_index_pack_proof.zig")) !void {
    std.debug.assert(self.admitted == 0 and self.pending == null);
    self.proof = proof;
    self.heap.clearRetainingCapacity();
    if (try self.bound(@splat(0), @splat(255))) |node| try self.heap.push(self.budget.allocator(), node);
}

/// Intersect authenticated score ranges with a complete predicate admission
/// proof. Own the small sorted set; residual row filtering stays in the scorer.
pub fn restrict(self: *Self, identities: []const Id) !void {
    std.debug.assert(self.admitted == 0 and self.pending == null);
    const owned = try self.syntax.allocator().dupe(Id, identities);
    std.mem.sort(Id, owned, {}, struct {
        fn less(_: void, l: Id, r: Id) bool {
            return std.mem.lessThan(u8, &l, &r);
        }
    }.less);
    self.selection = owned;
    self.heap.clearRetainingCapacity();
    if (try self.bound(@splat(0), @splat(255))) |node| try self.heap.push(self.budget.allocator(), node);
}

pub fn create(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Token, text: local.storage_db_types.TextQuery, analysis: local.introducer.TextAnalysisConfig, schema: local.storage_schema.TableSchema) !?*Self {
    return createObserved(a, root, store, cached, token, text, analysis, schema, null);
}
pub fn createObserved(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Token, text: local.storage_db_types.TextQuery, analysis: local.introducer.TextAnalysisConfig, schema: local.storage_schema.TableSchema, metrics: ?*Metrics) !?*Self {
    return initialize(a, root, store, cached, token, text, analysis, schema, metrics, true, null);
}
pub fn prepareObserved(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Token, text: local.storage_db_types.TextQuery, analysis: local.introducer.TextAnalysisConfig, schema: local.storage_schema.TableSchema, metrics: ?*Metrics) !?*Self {
    return initialize(a, root, store, cached, token, text, analysis, schema, metrics, false, null);
}
pub fn createRecentObserved(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Token, text: local.storage_db_types.TextQuery, analysis: local.introducer.TextAnalysisConfig, schema: local.storage_schema.TableSchema, metrics: ?*Metrics, recent: *const local.index.IndexSnapshot) !?*Self {
    return initialize(a, root, store, cached, token, text, analysis, schema, metrics, true, recent);
}
pub fn prepareRecentObserved(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Token, text: local.storage_db_types.TextQuery, analysis: local.introducer.TextAnalysisConfig, schema: local.storage_schema.TableSchema, metrics: ?*Metrics, recent: *const local.index.IndexSnapshot) !?*Self {
    return initialize(a, root, store, cached, token, text, analysis, schema, metrics, false, recent);
}
fn initialize(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Token, text: local.storage_db_types.TextQuery, analysis: local.introducer.TextAnalysisConfig, schema: local.storage_schema.TableSchema, metrics: ?*Metrics, routed: bool, recent: ?*const local.index.IndexSnapshot) !?*Self {
    if (root.inventory_version != 3 or root.collection == null or root.term_statistics == null or (routed and (root.term_directory.version != 2 or root.term_directory.leaves == null))) return null;
    const self = try a.create(Self);
    self.* = .{ .a = a, .budget = .{ .backing = a, .limit = 8 * 1024 * 1024 }, .root = root, .store = store, .cached = cached, .token = token, .metrics = metrics, .recent = recent };
    const ba = self.budget.allocator();
    self.scratch = .init(ba);
    self.syntax = .init(ba);
    // Query-wide accounting must not use PageStore.streaming, which resets
    // its allowance on every page read.
    self.pages = .{ .domain = root.domain, .artifacts = &self.store, .cancellation = token, .remaining_read_bytes = &self.reads, .remaining_write_bytes = &self.writes, .read_cache = .{ .ptr = self, .read = readPage } };
    self.cache = .{ .alloc = ba, .underlying = self.pages.store() };
    errdefer self.deinit();
    if (recent) |snapshot| {
        const collection = root.collection.?;
        var fields: std.ArrayList(corpus.Collection.Field) = .empty;
        for (collection.fields) |field| try fields.append(self.syntax.allocator(), .{ .name = field.name, .total_length = try std.math.add(u64, field.total_length, snapshot.liveTotalFieldLen(field.name)) });
        var extra = snapshot.global_total_field_len.iterator();
        while (extra.next()) |field| {
            var found = false;
            for (collection.fields) |base| if (std.mem.eql(u8, base.name, field.key_ptr.*)) {
                found = true;
                break;
            };
            if (!found) try fields.append(self.syntax.allocator(), .{ .name = field.key_ptr.*, .total_length = field.value_ptr.* });
        }
        self.root.collection = .{ .documents = try std.math.add(u32, collection.documents, snapshot.scoringDocCount()), .fields = fields.items };
    }
    const query = try local.storage_db_query_search_exec.textQueryToSearchQuery(self.syntax.allocator(), text, analysis, schema);
    self.plan = if (routed) try Plan.create(ba, query, .{ .ptr = self, .count = self.root.collection.?.documents, .average = average, .frequencies = frequencies, .patterns = patternTerms, .range_patterns = true }, .{}) else try Plan.prepare(ba, query, .{ .ptr = self, .count = self.root.collection.?.documents, .average = average, .frequencies = frequencies, .patterns = patternTerms, .range_patterns = true }, .{});
    if (self.plan == null) {
        self.deinit();
        return null;
    }
    if (routed) if (try self.bound(@splat(0), @splat(255))) |node| try self.heap.push(ba, node);
    return self;
}
pub fn deinit(self: *Self) void {
    // Owned here so failed preparation, fallback and cancellation also account
    // for their charged work, before any borrowed request state is released.
    if (self.metrics) |metrics| metrics.recordNavigation(.{ .queries = 1, .admitted_segments = self.admitted, .admission_windows = self.admission_windows, .subtree_reductions = self.reductions, .pruned_ranges = self.pruned_ranges, .overflow_subtrees = self.overflow_subtrees, .metadata_read_bytes = self.charged_bytes, .summary_cache_hits = self.summary_cache_hits });
    const a = self.a;
    for (self.pattern_matchers.items) |entry| entry.matcher.deinit();
    self.pattern_matchers.deinit(self.budget.allocator());
    self.score_ceilings.deinit(self.budget.allocator());
    if (self.plan) |plan| plan.deinit();
    self.heap.deinit(self.budget.allocator());
    self.cache.deinit();
    self.scratch.deinit();
    self.syntax.deinit();
    a.destroy(self);
}
fn average(raw: *anyopaque, field: []const u8) f32 {
    const self: *Self = @ptrCast(@alignCast(raw));
    const collection = self.root.collection.?;
    if (collection.documents == 0) return 1;
    for (collection.fields) |f| if (std.mem.eql(u8, f.name, field)) return @as(f32, @floatFromInt(f.total_length)) / @as(f32, @floatFromInt(collection.documents));
    return 1;
}
fn frequencies(raw: *anyopaque, _: A, field: []const u8, terms: []const []const u8, out: []u32) !void {
    const self: *Self = @ptrCast(@alignCast(raw));
    if (terms.len != out.len) return error.InvalidNativeLakeTextStatistics;
    for (terms, out) |term, *frequency| {
        try self.check();
        _ = self.scratch.reset(.retain_capacity);
        const a = self.scratch.allocator();
        const key = try corpus.statistics.keyAlloc(a, field, term);
        var cursor = try tree.Cursor.init(a, self.cache.store(), self.root.term_statistics, key, null);
        defer cursor.deinit();
        const entry = (try cursor.next()) orelse {
            frequency.* = 0;
            continue;
        };
        if (!std.mem.eql(u8, entry.key, key)) {
            frequency.* = 0;
            continue;
        }
        if (entry.value.len != 8) return error.InvalidNativeLakeTextStatistics;
        frequency.* = @intCast(@min(std.math.maxInt(u32), std.mem.readInt(u64, entry.value[0..8], .big)));
    }
    if (self.recent) |snapshot| {
        var temporary = std.heap.ArenaAllocator.init(self.a);
        defer temporary.deinit();
        const extra = try temporary.allocator().alloc(u32, terms.len);
        try snapshot.termDocFreqs(temporary.allocator(), field, terms, extra);
        for (out, extra) |*frequency, value| frequency.* = try std.math.add(u32, frequency.*, value);
    }
}

/// Discover selective pattern vocabulary once from the authenticated global
/// dictionary, without touching a segment or posting payload. Broad/expensive
/// expansions return unknown and preserve the complete bounded-window path.
fn patternTerms(raw: *anyopaque, out: A, query: local.search_search.SearchQuery) !?[]const local.search_search.TermQuery {
    const self: *Self = @ptrCast(@alignCast(raw));
    const matcher = try self.patternMatcher(query);
    var terms: std.ArrayList(local.search_search.TermQuery) = .empty;
    var memory: local.sql_memory_budget = .{ .backing = self.a, .limit = 2 * 1024 * 1024 };
    var scratch = std.heap.ArenaAllocator.init(memory.allocator());
    defer scratch.deinit();
    const a = scratch.allocator();
    if (matcher.field().len +| matcher.prefix.len > tree.max_key_bytes - 4) return null;
    const field_key = try corpus.statistics.keyAlloc(a, matcher.field(), "");
    const lower = try corpus.statistics.keyAlloc(a, matcher.field(), matcher.prefix);
    var cursor = try tree.Cursor.init(a, self.cache.store(), self.root.term_statistics, lower, null);
    defer cursor.deinit();
    var examined: usize = 0;
    while (try cursor.next()) |entry| {
        try self.check();
        if (!std.mem.startsWith(u8, entry.key, field_key)) break;
        const term = entry.key[field_key.len..];
        if (!std.mem.startsWith(u8, term, matcher.prefix)) break;
        if (examined == 4096) return null;
        examined += 1;
        const matched = try matcher.matches(term);
        if (matched) {
            if (terms.items.len == 128) return null;
            try terms.append(out, .{ .field = try out.dupe(u8, matcher.field()), .term = try out.dupe(u8, term) });
        }
        // A literal wildcard is one exact dictionary lookup, even when many
        // longer terms share its bytes. Do not turn it into a prefix census.
        if (matcher.wildcard) |literal| if (literal.exact) break;
        if (matcher.regex != null or matcher.fuzzy != null) {
            const seek = (try matcher.seekAfter(a, term)) orelse break;
            defer a.free(seek);
            const key = try corpus.statistics.keyAlloc(a, matcher.field(), seek);
            defer a.free(key);
            try cursor.seekForward(key);
        }
    }
    // Materialized prefix fields override the ordinary field only in segments
    // where they exist. Union both possibilities, keeping admission a superset.
    if (matcher.filter == .prefix) if (matcher.filter.prefix.indexed_field) |field| {
        const key = try corpus.statistics.keyAlloc(a, field, matcher.filter.prefix.prefix);
        var indexed = try tree.Cursor.init(a, self.cache.store(), self.root.term_statistics, key, null);
        defer indexed.deinit();
        if (try indexed.next()) |entry| if (std.mem.eql(u8, entry.key, key)) {
            if (terms.items.len == 128) return null;
            try terms.append(out, .{ .field = try out.dupe(u8, field), .term = try out.dupe(u8, matcher.filter.prefix.prefix) });
        };
    };
    return try terms.toOwnedSlice(out);
}

fn check(self: *Self) !void {
    try self.token.check();
    try self.cached.context.ensureActive();
}
fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, _: Id, token: Token, budget: *u64) ![]u8 {
    const self: *Self = @ptrCast(@alignCast(raw));
    try self.check();
    if (offset != 0 or length != ref.byte_len) return error.InvalidNativeLakeTextCorpus;
    try stores.chargeReadBudget(budget, length);
    self.charged_bytes += length;
    return artifacts.readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached);
}
const Range = struct {
    owner: *Self,
    first: Id,
    last: Id,
    workspace: ?A = null,
    found_first: ?Id = null,
    found_last: Id = undefined,
    summaries: std.StringHashMapUnmanaged(corpus.statistics.Summary) = .empty,
    fn summary(raw: *anyopaque, field: []const u8, term: []const u8, avg: f32, config: local.section_inverted.BM25Config) !local.index.IndexSnapshot.TextTermSummary {
        const self: *Range = @ptrCast(@alignCast(raw));
        const a = self.owner.scratch.allocator();
        const key = try corpus.statistics.keyAlloc(a, field, term);
        // Repeated clauses and both canonical lowerings share each envelope.
        if (self.summaries.get(key)) |value| {
            self.owner.summary_cache_hits += 1;
            return value.bound(avg, config);
        }
        const value = (try self.readSummary(key)) orelse return .{ .frequency = 0, .tf_upper = 0 };
        try self.summaries.put(a, key, value);
        return value.bound(avg, config);
    }
    // Fixed frequency makes both supported scorer formulas monotone in the
    // encoded document length, including f32 rounding. Other envelopes retain
    // their conservative margin; no real-number monotonicity assumption is made.
    fn score(raw: *anyopaque, field: []const u8, term: []const u8, avg: f32, config: local.section_inverted.BM25Config, idf: f32, legacy: bool) !?f32 {
        const self: *Range = @ptrCast(@alignCast(raw));
        const key = try corpus.statistics.keyAlloc(self.owner.scratch.allocator(), field, term);
        const envelope = self.summaries.get(key) orelse return null;
        return self.owner.scoreCeiling(envelope, avg, config, idf, legacy);
    }
    fn whole(self: *Range) !bool {
        const root = self.owner.root.tie_bounds orelse self.owner.root.directories.segments;
        if (root) |inventory| {
            const a = self.owner.scratch.allocator();
            var upper: [33]u8 = undefined;
            upper[0..32].* = self.last;
            upper[32] = 255;
            var forward = try tree.Cursor.init(a, self.owner.cache.store(), inventory, &self.first, &upper);
            defer forward.deinit();
            const first = (try forward.next()) orelse return false;
            if (first.key.len != 36) return error.InvalidNativeLakeTextCorpus;
            const first_id = first.key[0..32].*;
            var reverse = try tree.Cursor.initReverse(a, self.owner.cache.store(), inventory, &self.first, &upper);
            defer reverse.deinit();
            const last = (try reverse.next()) orelse return error.InvalidNativeLakeTextCorpus;
            if (last.key.len != 36) return error.InvalidNativeLakeTextCorpus;
            self.found_first = first_id;
            self.found_last = last.key[0..32].*;
        } else {
            self.found_first = self.first;
            self.found_last = self.last;
        }
        return true;
    }
    /// Broad patterns prove presence for one hash range at a time. Stop at the
    /// first witness and retain the whole input range: later matching terms may
    /// occur outside that witness's envelope. No flat vocabulary union is built.
    fn pattern(raw: *anyopaque, query: local.search_search.SearchQuery) !Plan.RangeBound {
        const self: *Range = @ptrCast(@alignCast(raw));
        const matcher = try self.owner.patternMatcher(query);
        const boost = switch (query) {
            inline .prefix, .wildcard, .regexp, .fuzzy => |q| q.boost,
            else => unreachable,
        };
        var memory: local.sql_memory_budget = .{ .backing = self.owner.a, .limit = 2 * 1024 * 1024 };
        var arena = std.heap.ArenaAllocator.init(memory.allocator());
        defer arena.deinit();
        const a = arena.allocator();
        if (matcher.field().len +| matcher.prefix.len > tree.max_key_bytes - 4) return .{ .upper = 0, .possible = false, .exact = true };
        if (matcher.filter == .prefix) if (matcher.filter.prefix.indexed_field) |indexed| {
            const key = try corpus.statistics.keyAlloc(a, indexed, matcher.filter.prefix.prefix);
            if (try self.readSummary(key)) |_| {
                const present = try self.whole();
                return .{ .upper = if (boost >= 0 and std.math.isFinite(boost)) boost else std.math.inf(f32), .possible = present, .exact = boost >= 0 and std.math.isFinite(boost) };
            }
        };
        // Escaped term keys preserve lexical order. Omit the exact-term
        // terminator to visit the field/prefix subtree across all vocabulary.
        const key = try corpus.statistics.keyAlloc(a, matcher.field(), matcher.prefix);
        const encoded = try corpus.term_directory.prefixAlloc(a, key);
        const lower = encoded[0 .. encoded.len - 2];
        const upper = try a.dupe(u8, lower);
        var length = upper.len;
        while (length != 0 and upper[length - 1] == 255) length -= 1;
        if (length == 0) return error.InvalidNativeLakeTermDirectory;
        upper[length - 1] += 1;
        const Visitor = struct {
            range: *Range,
            matcher: *local.search_search.TermPattern,
            a: A,
            work: usize = 0,
            fn check(v: *@This()) !void {
                try v.range.owner.check();
                // Unknown is conservative, independently of dictionary size.
                if (v.work == 1024 and (v.range.owner.root.tie_bounds != null or v.range.owner.root.directories.segments != null)) return error.PatternProbeBudgetExceeded;
                v.work += 1;
            }
            pub fn descend(v: *@This(), bytes: []const u8, records: u64) !bool {
                try v.check();
                if (bytes.len == 0) return true;
                const branch = try corpus.term_directory.Branch.decode(bytes);
                if (branch.count != records) return error.InvalidNativeLakeTermDirectory;
                v.range.owner.pattern_reductions += 1;
                return std.mem.order(u8, &branch.last, &v.range.first) != .lt and std.mem.order(u8, &branch.first, &v.range.last) != .gt;
            }
            pub fn record(v: *@This(), bytes: []const u8, value: []const u8) !void {
                try v.check();
                if (bytes.len < 34) return error.InvalidNativeLakeTermDirectory;
                const id = bytes[bytes.len - 32 ..][0..32];
                if (std.mem.order(u8, id, &v.range.first) == .lt or std.mem.order(u8, id, &v.range.last) == .gt) return;
                const prefix = bytes[0 .. bytes.len - 32];
                var term: std.ArrayList(u8) = .empty;
                defer term.deinit(v.a);
                var i: usize = 0;
                while (i < prefix.len) : (i += 1) {
                    if (prefix[i] == 0) {
                        i += 1;
                        if (i >= prefix.len) return error.InvalidNativeLakeTermDirectory;
                        if (prefix[i] == 0) {
                            if (i + 1 != prefix.len) return error.InvalidNativeLakeTermDirectory;
                            break;
                        }
                        if (prefix[i] != 255) return error.InvalidNativeLakeTermDirectory;
                        try term.append(v.a, 0);
                    } else try term.append(v.a, prefix[i]);
                }
                const field = try corpus.statistics.keyAlloc(v.a, v.matcher.field(), "");
                defer v.a.free(field);
                if (!std.mem.startsWith(u8, term.items, field)) return error.InvalidNativeLakeTermDirectory;
                _ = try corpus.statistics.Summary.decode(value);
                if (try v.matcher.matches(term.items[field.len..])) return error.PatternWitnessFound;
            }
        };
        var visitor: Visitor = .{ .range = self, .matcher = matcher, .a = memory.allocator() };
        tree.visitRange(memory.allocator(), self.owner.cache.store(), self.owner.root.term_directory.leaves, lower, upper[0..length], &visitor) catch |err| switch (err) {
            error.PatternWitnessFound, error.PatternProbeBudgetExceeded => {
                const present = try self.whole();
                return .{ .upper = if (boost >= 0 and std.math.isFinite(boost)) boost else std.math.inf(f32), .possible = present, .exact = boost >= 0 and std.math.isFinite(boost) };
            },
            else => return err,
        };
        return .{ .upper = 0, .possible = false, .exact = true };
    }
    fn readSummary(self: *Range, key: []const u8) !?corpus.statistics.Summary {
        const a = self.workspace orelse self.owner.scratch.allocator();
        const prefix = try corpus.term_directory.prefixAlloc(a, key);
        const lower = try std.mem.concat(a, u8, &.{ prefix, &self.first });
        const upper = try std.mem.concat(a, u8, &.{ prefix, &self.last, &.{0} });
        const Fold = struct {
            value: ?corpus.term_directory.Branch = null,
            prefix: []const u8,
            range: *Range,
            fn add(self_: *@This(), branch: corpus.term_directory.Branch) !void {
                if (std.mem.order(u8, &branch.first, &self_.range.first) == .lt or std.mem.order(u8, &branch.last, &self_.range.last) == .gt) return error.InvalidNativeLakeTermDirectory;
                if (self_.value) |*value| try value.merge(branch) else self_.value = branch;
            }
            pub fn aggregate(self_: *@This(), bytes: []const u8, records: u64) !void {
                const branch = try corpus.term_directory.Branch.decode(bytes);
                if (branch.count != records) return error.InvalidNativeLakeTermDirectory;
                self_.range.owner.reductions += 1;
                try self_.add(branch);
            }
            pub fn record(self_: *@This(), k: []const u8, v: []const u8) !void {
                if (k.len != self_.prefix.len + 32 or !std.mem.startsWith(u8, k, self_.prefix)) return error.InvalidNativeLakeTermDirectory;
                const id: Id = k[k.len - 32 ..][0..32].*;
                try self_.add(.{ .first = id, .last = id, .envelope = try corpus.statistics.Summary.decode(v) });
            }
        };
        var fold: Fold = .{ .prefix = prefix, .range = self };
        try tree.reduceRange(a, self.owner.cache.store(), self.owner.root.term_directory.leaves, lower, upper, &fold);
        if (fold.value) |value| {
            if (self.found_first) |first| {
                if (std.mem.order(u8, &value.first, &first) == .lt) self.found_first = value.first;
                if (std.mem.order(u8, &value.last, &self.found_last) == .gt) self.found_last = value.last;
            } else {
                self.found_first = value.first;
                self.found_last = value.last;
            }
            return value.envelope;
        }
        return null;
    }
};
fn bound(self: *Self, input_first: Id, input_last: Id) !?Node {
    try self.check();
    var first = input_first;
    var last = input_last;
    if (self.selection) |ids| {
        var lo: usize = 0;
        var hi = ids.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, &ids[mid], &first) == .lt) lo = mid + 1 else hi = mid;
        }
        const start = lo;
        hi = ids.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, &ids[mid], &last) != .gt) lo = mid + 1 else hi = mid;
        }
        if (start == lo) return null;
        first = ids[start];
        last = ids[lo - 1];
    }
    if (self.proof) |proof| {
        const clipped = (try @import("lake_index_pack_proof.zig").clip(proof, first, last)) orelse return null;
        first = clipped.first;
        last = clipped.last;
    }
    _ = self.scratch.reset(.retain_capacity);
    var range: Range = .{ .owner = self, .first = first, .last = last };
    const result = try self.plan.?.rangeBound(.{ .ptr = &range, .read = Range.summary, .score = Range.score, .pattern = Range.pattern });
    if (!result.possible) return null;
    if (range.found_first) |actual| {
        // BM25 rounding margins cannot establish an equal-score proof. Avoid
        // tie-tree reads entirely unless this bound has exact f32 arithmetic.
        const minimum = if (result.exact and self.root.tie_bounds != null) try ties.range(self.scratch.allocator(), self.cache.store(), self.root.tie_bounds.?, actual, range.found_last) else null;
        return .{ .first = actual, .last = range.found_last, .upper = result.upper, .minimum = minimum };
    }
    return null;
}
/// Warm immutable sibling/term pages with independent readers and scratch.
/// Only the coordinator mutates scores, summaries, quotas and traversal order.
fn prefetchSiblings(self: *Self, first: Id, left_end: Id, boundary: Id, last: Id) !void {
    const io = self.cached.context.io orelse return;
    if (self.prefetch_reads < 2 * 1024 * 1024) return;
    const Term = struct { field: []const u8, term: []const u8 };
    var terms: [8]Term = undefined;
    var used: usize = 0;
    var fields = self.plan.?.stats.fields.iterator();
    gather: while (fields.next()) |field| {
        var keys = field.value_ptr.frequencies.keyIterator();
        while (keys.next()) |term| {
            terms[used] = .{ .field = field.key_ptr.*, .term = term.* };
            used += 1;
            if (used == terms.len) break :gather;
        }
    }
    if (used == 0) return;
    const Warm = struct {
        owner: *Self,
        terms: []const Term,
        first: Id,
        last: Id,
        reads: u64 = 1024 * 1024,
        done: std.Io.Event = .unset,
        fn read(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, _: u64, length: usize, _: Id, token: Token, budget: *u64) ![]u8 {
            const warm: *@This() = @ptrCast(@alignCast(raw));
            try warm.owner.cached.context.ensureActive();
            try stores.chargeReadBudget(budget, length);
            return artifacts.readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, warm.owner.cached);
        }
        fn run(warm: *@This()) anyerror!void {
            defer warm.done.set(warm.owner.cached.context.io.?);
            var memory: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(std.heap.page_allocator), .limit = 2 * 1024 * 1024 };
            var arena = std.heap.ArenaAllocator.init(memory.allocator());
            defer arena.deinit();
            var store = warm.owner.store;
            var writes: u64 = 0;
            var pages: page_store.PageStore = .{ .domain = warm.owner.root.domain, .artifacts = &store, .cancellation = warm.owner.token, .remaining_read_bytes = &warm.reads, .remaining_write_bytes = &writes, .read_cache = .{ .ptr = warm, .read = read } };
            var cache: tree.Cache = .{ .alloc = memory.allocator(), .underlying = pages.store() };
            defer cache.deinit();
            const Fold = struct {
                pub fn aggregate(_: *@This(), _: []const u8, _: u64) !void {}
                pub fn record(_: *@This(), _: []const u8, _: []const u8) !void {}
            };
            for (warm.terms) |term| {
                try warm.owner.check();
                _ = arena.reset(.retain_capacity);
                const a = arena.allocator();
                const key = try corpus.statistics.keyAlloc(a, term.field, term.term);
                const prefix = try corpus.term_directory.prefixAlloc(a, key);
                const lower = try std.mem.concat(a, u8, &.{ prefix, &warm.first });
                const upper = try std.mem.concat(a, u8, &.{ prefix, &warm.last, &.{0} });
                var fold: Fold = .{};
                try tree.reduceRange(a, cache.store(), warm.owner.root.term_directory.leaves, lower, upper, &fold);
            }
        }
    };
    var workers = [_]Warm{
        .{ .owner = self, .terms = terms[0..used], .first = first, .last = left_end },
        .{ .owner = self, .terms = terms[0..used], .first = boundary, .last = last },
    };
    const scheduler = local.sql_parallel_scheduler;
    var tasks: [2]?scheduler.Task(anyerror!void) = @splat(null);
    defer for (&tasks, &workers) |*handle, *worker| if (handle.*) |task_| {
        var task = task_;
        _ = task.cancel(io) catch {};
        self.prefetch_reads += worker.reads;
        self.charged_bytes += 1024 * 1024 - worker.reads;
    };
    for (&workers, &tasks) |*worker, *task| {
        task.* = scheduler.global().submitIoTransient(io, 2 * 1024 * 1024, Warm.run, .{worker});
        if (task.* != null) {
            self.prefetch_reads -= 1024 * 1024;
            self.prefetch_tasks += 1;
        }
    }
    for (&tasks, &workers) |*handle, *worker| if (handle.*) |task_| {
        try worker.done.wait(io);
        var task = task_;
        handle.* = null;
        task.await(io) catch {}; // optional warming never proves absence
        self.prefetch_reads += worker.reads;
        self.charged_bytes += 1024 * 1024 - worker.reads;
    };
    try self.check();
}
pub fn next(self: *Self, cutoff: ?f32) !?Id {
    return self.nextRanked(cutoff, null);
}
pub fn nextRanked(self: *Self, cutoff: ?f32, position: ?Position) !?Id {
    try self.check();
    while (self.overflow_len != 0 or self.heap.items.len != 0) {
        try self.check();
        const depth_first = self.overflow_len != 0;
        const node = if (depth_first) blk: {
            self.overflow_len -= 1;
            break :blk self.overflow[self.overflow_len];
        } else self.heap.pop().?;
        if (cutoff) |floor| if (node.upper < floor or (node.upper == floor and position != null and node.minimum != null and ties.compare(node.minimum.?, position.?) != .lt)) {
            self.pruned = true;
            self.pruned_ranges += 1;
            continue;
        };
        if (std.mem.eql(u8, &node.first, &node.last)) {
            if (self.predicate_probe) |probe| if (!try probe.allows(probe.ptr, node.first)) continue;
            self.admitted += 1;
            return node.first;
        }
        var boundary = node.first;
        for (node.first, node.last, 0..) |l, r, i| if (l != r) {
            const bit: u3 = @intCast(7 - @clz(l ^ r));
            boundary[i] = (l & (@as(u8, 255) << bit)) | (@as(u8, 1) << bit);
            @memset(boundary[i + 1 ..], 0);
            break;
        };
        var left_end = boundary;
        var i = left_end.len;
        while (i > 0) {
            i -= 1;
            left_end[i] -%= 1;
            if (left_end[i] != 255) break;
        }
        try self.prefetchSiblings(node.first, left_end, boundary, node.last);
        const left = try self.bound(node.first, left_end);
        const right = try self.bound(boundary, node.last);
        const fallback = depth_first or self.heap.items.len + @as(usize, @intFromBool(left != null)) + @as(usize, @intFromBool(right != null)) > self.frontier_limit;
        if (fallback) {
            if (!depth_first) self.overflow_subtrees += 1;
            for ([_]?Node{ right, left }) |child| if (child) |value| {
                std.debug.assert(self.overflow_len < self.overflow.len);
                self.overflow[self.overflow_len] = value;
                self.overflow_len += 1;
            };
        } else {
            for ([_]?Node{ left, right }) |child| if (child) |value| try self.heap.push(self.budget.allocator(), value);
        }
    }
    return null;
}

/// Bound admission by count and authenticated payload size. A carried leaf owns
/// no descriptor, physical manifest or posting payload.
pub fn nextWindow(self: *Self, output: []Id, cutoff: ?f32, max_bytes: u64) !?[]const Id {
    return self.nextWindowRanked(output, cutoff, null, max_bytes);
}
pub fn nextWindowRanked(self: *Self, output: []Id, cutoff: ?f32, position: ?Position, max_bytes: u64) !?[]const Id {
    if (output.len == 0 or max_bytes == 0) return error.InvalidNativeLakeTextCorpus;
    var count: usize = 0;
    var bytes: u64 = 0;
    while (count < output.len and bytes < max_bytes) {
        try self.check();
        const leaf: Pending = if (self.pending) |held| held else blk: {
            const id = (try self.nextRanked(cutoff, position)) orelse break;
            _ = self.scratch.reset(.retain_capacity);
            const size = try @import("lake_index_text_directory.zig").segmentBytes(self.scratch.allocator(), self.cache.store(), self.root, id);
            break :blk .{ .identity = id, .bytes = size };
        };
        if (count != 0 and leaf.bytes > max_bytes -| bytes) {
            self.pending = leaf;
            break;
        }
        self.pending = null;
        output[count] = leaf.identity;
        count += 1;
        bytes += leaf.bytes;
    }
    if (count != 0) self.admission_windows += 1;
    return if (count == 0) null else output[0..count];
}

test "external lake global frontier reaches late competitive segments and completes under pressure" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("text-global-frontier");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(6), .attempt = @splat(4) };
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(3), .context = .{} };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var reads: u64 = 128 * 1024 * 1024;
    var writes: u64 = 128 * 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = @splat(6), .attempt = @splat(4), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const key = try corpus.statistics.keyAlloc(sa, "body", "common");
    const prefix = try corpus.term_directory.prefixAlloc(sa, key);
    const rare_key = try corpus.statistics.keyAlloc(sa, "body", "rare");
    const rare_prefix = try corpus.term_directory.prefixAlloc(sa, rare_key);
    const Source = struct {
        prefix: []const u8,
        rare_prefix: []const u8,
        position: u32 = 0,
        key: [128]u8 = undefined,
        value: [corpus.statistics.Summary.max_bytes]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.position == 8195) return null;
            const common = self.position < 8192;
            const prefix_ = if (common) self.prefix else self.rare_prefix;
            const ordinal = if (common) self.position else ([_]u32{ 3, 31, 8191 })[self.position - 8192];
            @memcpy(self.key[0..prefix_.len], prefix_);
            @memset(self.key[prefix_.len..][0..32], 0);
            std.mem.writeInt(u32, self.key[prefix_.len + 28 ..][0..4], ordinal, .big);
            const strong = common and self.position == 8191;
            const summary: corpus.statistics.Summary = .{ .frequency = 1, .max_frequency = if (strong) 100 else 1, .min_norm = 100 };
            self.position += 1;
            return .{ .key = self.key[0 .. prefix_.len + 32], .value = summary.encode(&self.value) };
        }
    };
    var source: Source = .{ .prefix = prefix, .rare_prefix = rare_prefix };
    var reduced = pages.store();
    reduced.reduce = corpus.term_directory.reduce;
    const leaves = try tree.buildSorted(a, reduced, &source);
    const Global = struct {
        key: []const u8,
        rare_key: []const u8,
        position: u2 = 0,
        value: [8]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.position == 2) return null;
            const common = self.position == 0;
            self.position += 1;
            std.mem.writeInt(u64, &self.value, if (common) 8192 else 3, .big);
            return .{ .key = if (common) self.key else self.rare_key, .value = &self.value };
        }
    };
    var global: Global = .{ .key = key, .rare_key = rare_key };
    const root: corpus.Root = .{ .domain = @splat(6), .binding = undefined, .config_json = "{}", .inventory_version = 3, .term_statistics = try tree.buildSorted(a, pages.store(), &global), .collection = .{ .documents = 8192, .fields = &.{.{ .name = "body", .total_length = 819200 }} }, .term_directory = .{ .version = 2, .leaves = leaves } };
    var last: Id = @splat(0);
    std.mem.writeInt(u32, last[28..32], 8191, .big);
    const frontier = (try create(a, root, store, cached, .none, .{ .term = .{ .field = "body", .term = "common" } }, .{}, .{})).?;
    defer frontier.deinit();
    const before = frontier.reads;
    try std.testing.expectEqual(last, (try frontier.next(null)).?);
    try std.testing.expect(before - frontier.reads < 2 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 1), frontier.admitted);
    try std.testing.expect(frontier.reductions > 0);
    try std.testing.expect(frontier.summary_cache_hits > 0);
    try std.testing.expectEqual(@as(u64, 256 * 1024 * 1024) - frontier.reads + (8 * 1024 * 1024 - frontier.prefetch_reads), frontier.charged_bytes);
    var warm_cached = cached;
    warm_cached.context.io = std.testing.io;
    const warmed = (try create(a, root, store, warm_cached, .none, .{ .term = .{ .field = "body", .term = "common" } }, .{}, .{})).?;
    defer warmed.deinit();
    try std.testing.expectEqual(last, (try warmed.next(null)).?);
    try std.testing.expect(warmed.prefetch_tasks > 0);
    try std.testing.expect(warmed.prefetch_reads <= 8 * 1024 * 1024);
    try std.testing.expectEqual(@as(u64, 256 * 1024 * 1024) - warmed.reads + (8 * 1024 * 1024 - warmed.prefetch_reads), warmed.charged_bytes);
    // Ordinary task admission remains pinned until the handle is joined, so
    // this deterministically saturates optional I/O warming without a sleep.
    const Hold = struct {
        fn run() void {}
    };
    var held = local.sql_parallel_scheduler.global().submitIo(std.testing.io, 32 * 1024 * 1024, Hold.run, .{}).?;
    defer held.await(std.testing.io);
    const saturated = (try create(a, root, store, warm_cached, .none, .{ .term = .{ .field = "body", .term = "common" } }, .{}, .{})).?;
    defer saturated.deinit();
    try std.testing.expectEqual(last, (try saturated.next(null)).?);
    try std.testing.expectEqual(@as(usize, 0), saturated.prefetch_tasks);
    try std.testing.expectEqual(@as(u64, 8 * 1024 * 1024), saturated.prefetch_reads);

    const strong = local.section_inverted.bm25Score(100, 100, 8192, 8192, 100, .{});
    try std.testing.expect((try frontier.next(strong)) == null);
    try std.testing.expect(frontier.pruned);
    // Equal ceilings are still eligible, including under a one-entry frontier.
    const complete = (try create(a, root, store, cached, .none, .{ .term = .{ .field = "body", .term = "common" } }, .{}, .{})).?;
    defer complete.deinit();
    complete.frontier_limit = 1;
    const equal_cutoff = local.section_inverted.BM25TermScorer.init(100, local.section_inverted.bm25Idf(8192, 8192), .{}).score(1, 100);
    var seen: [8192]bool = @splat(false);
    var count: usize = 0;
    while (try complete.next(equal_cutoff)) |id| {
        const ordinal = std.mem.readInt(u32, id[28..32], .big);
        try std.testing.expect(ordinal < seen.len);
        try std.testing.expect(!seen[ordinal]);
        seen[ordinal] = true;
        count += 1;
    }
    try std.testing.expectEqual(seen.len, count);
    try std.testing.expect(!complete.pruned);
    const conjunction = (try create(a, root, store, cached, .none, .{ .bool_query = .{ .must = &.{
        .{ .term = .{ .field = "body", .term = "common" } },
        .{ .term = .{ .field = "body", .term = "rare" } },
    } } }, .{}, .{})).?;
    defer conjunction.deinit();
    var selected: usize = 0;
    while (try conjunction.next(null)) |id| {
        const ordinal = std.mem.readInt(u32, id[28..32], .big);
        try std.testing.expect(ordinal == 3 or ordinal == 31 or ordinal == 8191);
        selected += 1;
    }
    // Membership proof must work even when fewer than K rows match, so no
    // competitive cutoff exists. No unrelated inventory is admitted.
    try std.testing.expectEqual(@as(usize, 3), selected);
    try std.testing.expectEqual(@as(u64, 3), conjunction.admitted);
    // Metadata-only pattern proofs open the same three rare segments. Their
    // constant score is independent of expansion count and global BM25 DF.
    for ([_]local.storage_db_types.TextQuery{
        .{ .prefix = .{ .field = "body", .prefix = "ra" } },
        .{ .wildcard = .{ .field = "body", .pattern = "ra?e" } },
        .{ .regexp = .{ .field = "body", .pattern = "ra[a-z]e" } },
        .{ .fuzzy = .{ .field = "body", .term = "raze", .max_edits = 1 } },
    }) |pattern| {
        const patterned = (try create(a, root, store, cached, .none, pattern, .{}, .{})).?;
        defer patterned.deinit();
        var matches: usize = 0;
        while (try patterned.next(null)) |id| {
            const ordinal = std.mem.readInt(u32, id[28..32], .big);
            try std.testing.expect(ordinal == 3 or ordinal == 31 or ordinal == 8191);
            matches += 1;
        }
        try std.testing.expectEqual(@as(usize, 3), matches);
    }
    const filtered = (try create(a, root, store, cached, .none, .{ .term = .{ .field = "body", .term = "common" } }, .{}, .{})).?;
    defer filtered.deinit();
    var low: Id = @splat(0);
    low[31] = 3;
    try filtered.restrict(&.{ last, low });
    try std.testing.expectEqual(last, (try filtered.next(null)).?);
    try std.testing.expect((try filtered.next(strong)) == null);
    try std.testing.expectEqual(@as(u64, 1), filtered.admitted);
    const empty = (try create(a, root, store, cached, .none, .{ .term = .{ .field = "body", .term = "common" } }, .{}, .{})).?;
    defer empty.deinit();
    try empty.restrict(&.{});
    try std.testing.expect((try empty.next(null)) == null);
    // Pack hashes ascend while logical file order descends.
    const TieSource = struct {
        position: u32 = 0,
        key_: [36]u8 = @splat(0),
        value: [36]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.position == 8192) return null;
            std.mem.writeInt(u32, self.key_[28..32], self.position, .big);
            var file: Id = @splat(0);
            std.mem.writeInt(u32, file[28..32], 8191 - self.position, .big);
            self.value = ties.encode(.{ .file = file, .rank = 0 });
            self.position += 1;
            return .{ .key = &self.key_, .value = &self.value };
        }
    };
    var tie_source: TieSource = .{};
    var tie_store = pages.store();
    tie_store.reduce = ties.reduce;
    var tied_root = root;
    tied_root.tie_bounds = try tree.buildSorted(a, tie_store, &tie_source);
    const tied = (try create(a, tied_root, store, cached, .none, .{ .prefix = .{ .field = "body", .prefix = "ra" } }, .{}, .{})).?;
    defer tied.deinit();
    try std.testing.expectEqual(last, (try tied.nextRanked(null, null)).?);
    try std.testing.expect((try tied.nextRanked(1, .{ .file = @splat(0), .rank = 0 })) == null);
    try std.testing.expect(tied.pruned);
    try std.testing.expectEqual(@as(u64, 1), tied.admitted);
    const recent_tie = (try create(a, tied_root, store, cached, .none, .{ .prefix = .{ .field = "body", .prefix = "ra" } }, .{}, .{})).?;
    defer recent_tie.deinit();
    try std.testing.expect((try recent_tie.nextRanked(1, .{ .file = @splat(0), .rank = 0, .recent = true })) != null);
    const Vocabulary = struct {
        position: usize = 0,
        key_: [64]u8 = undefined,
        value: [8]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.position == 5001) return null;
            self.key_[0..8].* = "\x00\x00\x00\x04body".*;
            const term = if (self.position == 5000) "rare" else try std.fmt.bufPrint(self.key_[8..], "a{d:0>5}", .{self.position});
            if (self.position == 5000) @memcpy(self.key_[8..12], term);
            self.position += 1;
            std.mem.writeInt(u64, &self.value, 1, .big);
            return .{ .key = self.key_[0 .. 8 + term.len], .value = &self.value };
        }
    };
    var vocabulary: Vocabulary = .{};
    var large_root = root;
    large_root.term_statistics = try tree.buildSorted(a, pages.store(), &vocabulary);
    for ([_]local.storage_db_types.TextQuery{ .{ .regexp = .{ .field = "body", .pattern = "ra[a-z]e" } }, .{ .fuzzy = .{ .field = "body", .term = "raze", .max_edits = 1 } } }) |pattern| {
        const routed = (try create(a, large_root, store, cached, .none, pattern, .{}, .{})).?;
        defer routed.deinit();
        var matches: usize = 0;
        while (try routed.next(null)) |_| {
            matches += 1;
        }
        try std.testing.expectEqual(@as(usize, 3), matches);
        try std.testing.expect(routed.charged_bytes < 2 * 1024 * 1024);
    }
    const ManyTerms = struct {
        position: u32 = 0,
        key_bytes: [12]u8 = undefined,
        count: [8]u8 = undefined,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.position == 129) return null;
            self.key_bytes[0..8].* = "\x00\x00\x00\x04body".*;
            std.mem.writeInt(u32, self.key_bytes[8..12], self.position, .big);
            std.mem.writeInt(u64, &self.count, 1, .big);
            self.position += 1;
            return .{ .key = &self.key_bytes, .value = &self.count };
        }
    };
    var many: ManyTerms = .{};
    var broad_root = root;
    broad_root.term_statistics = try tree.buildSorted(a, pages.store(), &many);
    // A broad vocabulary uses range-local witnesses instead of a truncated union.
    // Its binary prefix has no matching term envelopes in the directory.
    const broad = (try create(a, broad_root, store, cached, .none, .{ .prefix = .{ .field = "body", .prefix = "\x00" } }, .{}, .{})).?;
    defer broad.deinit();
    try std.testing.expect((try broad.next(null)) == null);
    try std.testing.expectEqual(@as(u64, 1), broad.pattern_compilations);
    const unexpanded = (try create(a, broad_root, store, cached, .none, .{ .regexp = .{ .field = "body", .pattern = ".*" } }, .{}, .{})).?;
    defer unexpanded.deinit();
    try std.testing.expect((try unexpanded.next(null)) != null);
    try std.testing.expect((try unexpanded.next(null)) != null);
    try std.testing.expectEqual(@as(u64, 1), unexpanded.pattern_compilations);
    try std.testing.expect(unexpanded.pattern_reductions > 0);
    const Entries = struct {
        records: []const tree.Cursor.Record,
        position: usize = 0,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.position == self.records.len) return null;
            defer self.position += 1;
            return self.records[self.position];
        }
    };
    var broad_buffer: [corpus.statistics.Summary.max_bytes]u8 = undefined;
    const records = try sa.alloc(tree.Cursor.Record, 129);
    for (records, 0..) |*record, i| {
        var term: [4]u8 = undefined;
        std.mem.writeInt(u32, &term, @intCast(i), .big);
        const term_key = try corpus.statistics.keyAlloc(sa, "body", &term);
        record.* = .{ .key = try std.mem.concat(sa, u8, &.{ try corpus.term_directory.prefixAlloc(sa, term_key), &last }), .value = try sa.dupe(u8, (corpus.statistics.Summary{ .frequency = 1, .max_frequency = 1, .min_norm = 100 }).encode(&broad_buffer)) };
    }
    var many_routes: Entries = .{ .records = records };
    broad_root.term_directory.leaves = try tree.buildSorted(a, reduced, &many_routes);
    const broad_present = (try create(a, broad_root, store, cached, .none, .{ .prefix = .{ .field = "body", .prefix = "" } }, .{}, .{})).?;
    defer broad_present.deinit();
    try std.testing.expectEqual(last, (try broad_present.next(null)).?);
    try std.testing.expect((try broad_present.next(null)) == null);
    const bm25_ties = (try create(a, tied_root, store, cached, .none, .{ .term = .{ .field = "body", .term = "rare" } }, .{}, .{})).?;
    defer bm25_ties.deinit();
    try std.testing.expectEqual(last, (try bm25_ties.next(null)).?);
    const exact_score = local.section_inverted.BM25TermScorer.init(100, local.section_inverted.bm25Idf(8192, 3), .{}).score(1, 100);
    try std.testing.expect((try bm25_ties.nextRanked(exact_score, .{ .file = @splat(0), .rank = 0 })) == null);
    try std.testing.expectEqual(@as(u64, 1), bm25_ties.admitted);
    const absent = (try create(a, root, store, cached, .none, .{ .prefix = .{ .field = "body", .prefix = "missing" } }, .{}, .{})).?;
    defer absent.deinit();
    try std.testing.expect((try absent.next(null)) == null);
    const optional = try create(a, root, store, cached, .none, .{ .bool_query = .{ .should = &.{.{ .term = .{ .field = "body", .term = "common" } }}, .pure_should_optional = true } }, .{}, .{});
    try std.testing.expect(optional == null);
    // Accepted rows add global statistics exactly once, including terms absent
    // from the archive and fields absent from its retained collection counters.
    var overlay: @import("lake_search_overlay.zig").Overlay = .{ .pending = undefined, .key_filter = "" };
    try overlay.rows.put(sa, "wal1:recent", try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"body\":\"common rare novel\"}", .{}));
    var recent = try overlay.recent(a, "text", .{}, null, "body", .{});
    defer recent.deinit();
    const prepared_recent = (try prepareRecentObserved(a, root, store, cached, .none, .{ .match = .{ .field = "body", .text = "common rare novel" } }, .{}, .{}, null, recent.snapshot)).?;
    defer prepared_recent.deinit();
    try std.testing.expectEqual(@as(u32, 8193), prepared_recent.root.collection.?.documents);
    var dfs: [3]u32 = undefined;
    try prepared_recent.plan.?.preparedFrequencies("body", &.{ "common", "rare", "novel" }, &dfs);
    try std.testing.expectEqualSlices(u32, &.{ 8193, 4, 1 }, &dfs);
    try std.testing.expectError(error.UnpreparedTextTerm, prepared_recent.plan.?.preparedFrequencies("body", &.{"unknown"}, dfs[0..1]));
    const recent_frontier = (try createRecentObserved(a, root, store, cached, .none, .{ .term = .{ .field = "body", .term = "common" } }, .{}, .{}, null, recent.snapshot)).?;
    defer recent_frontier.deinit();
    try std.testing.expectEqual(last, (try recent_frontier.next(null)).?);
    try std.testing.expectEqual(prepared_recent.root.collection.?.documents, recent_frontier.root.collection.?.documents);
    // Cancellation never turns a partial frontier into a successful result.
    const Cancel = struct {
        fn yes(_: *const anyopaque) bool {
            return true;
        }
    };
    complete.token = .{ .ptr = complete, .is_cancelled_fn = Cancel.yes };
    try std.testing.expectError(error.Canceled, complete.next(null));
    // Exhaustion survives a tree-cache miss even with a warm shared cache.
    const exhausted = (try create(a, root, store, cached, .none, .{ .term = .{ .field = "body", .term = "common" } }, .{}, .{})).?;
    defer exhausted.deinit();
    exhausted.cache.deinit();
    exhausted.reads = 0;
    const charged = exhausted.charged_bytes;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, exhausted.next(null));
    try std.testing.expectEqual(charged, exhausted.charged_bytes);
    var broken = root;
    broken.term_directory.leaves.?.digest = @splat(159);
    var metrics: Metrics = .{};
    try std.testing.expectError(error.FileNotFound, createObserved(a, broken, store, cached, .none, .{ .term = .{ .field = "body", .term = "common" } }, .{}, .{}, &metrics));
    try std.testing.expectEqual(@as(u64, 1), metrics.snapshot().navigation.queries);
    try std.testing.expect(metrics.snapshot().navigation.metadata_read_bytes > 0);
}
