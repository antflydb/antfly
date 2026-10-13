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
const Metrics = @import("lake_query_metrics.zig").Metrics;
const Node = struct {
    first: Id,
    last: Id,
    upper: f32,
    fn priority(_: void, l: Node, r: Node) std.math.Order {
        const score = std.math.order(r.upper, l.upper);
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
pending: ?Pending = null,

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
    self.plan = if (routed) try Plan.create(ba, query, .{ .ptr = self, .count = self.root.collection.?.documents, .average = average, .frequencies = frequencies }, .{}) else try Plan.prepare(ba, query, .{ .ptr = self, .count = self.root.collection.?.documents, .average = average, .frequencies = frequencies }, .{});
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
    found_first: ?Id = null,
    found_last: Id = undefined,
    summaries: std.StringHashMapUnmanaged(local.index.IndexSnapshot.TextTermSummary) = .empty,
    fn summary(raw: *anyopaque, field: []const u8, term: []const u8, avg: f32, config: local.section_inverted.BM25Config) !local.index.IndexSnapshot.TextTermSummary {
        const self: *Range = @ptrCast(@alignCast(raw));
        const a = self.owner.scratch.allocator();
        const key = try corpus.statistics.keyAlloc(a, field, term);
        // Repeated clauses and both canonical lowerings share each envelope.
        if (self.summaries.get(key)) |value| {
            self.owner.summary_cache_hits += 1;
            return value;
        }
        const value = try self.readSummary(key, avg, config);
        try self.summaries.put(a, key, value);
        return value;
    }
    fn readSummary(self: *Range, key: []const u8, avg: f32, config: local.section_inverted.BM25Config) !local.index.IndexSnapshot.TextTermSummary {
        const a = self.owner.scratch.allocator();
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
            return value.envelope.bound(avg, config);
        }
        return .{ .frequency = 0, .tf_upper = 0 };
    }
};
fn bound(self: *Self, first: Id, last: Id) !?Node {
    try self.check();
    _ = self.scratch.reset(.retain_capacity);
    var range: Range = .{ .owner = self, .first = first, .last = last };
    const result = try self.plan.?.rangeBound(.{ .ptr = &range, .read = Range.summary });
    if (!result.possible) return null;
    return if (range.found_first) |actual| .{ .first = actual, .last = range.found_last, .upper = result.upper } else null;
}
pub fn next(self: *Self, cutoff: ?f32) !?Id {
    try self.check();
    while (self.overflow_len != 0 or self.heap.items.len != 0) {
        try self.check();
        const depth_first = self.overflow_len != 0;
        const node = if (depth_first) blk: {
            self.overflow_len -= 1;
            break :blk self.overflow[self.overflow_len];
        } else self.heap.pop().?;
        if (cutoff) |floor| if (node.upper < floor) {
            self.pruned = true;
            self.pruned_ranges += 1;
            continue;
        };
        if (std.mem.eql(u8, &node.first, &node.last)) {
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
    if (output.len == 0 or max_bytes == 0) return error.InvalidNativeLakeTextCorpus;
    var count: usize = 0;
    var bytes: u64 = 0;
    while (count < output.len and bytes < max_bytes) {
        try self.check();
        const leaf: Pending = if (self.pending) |held| held else blk: {
            const id = (try self.next(cutoff)) orelse break;
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
    try std.testing.expectEqual(@as(u64, 256 * 1024 * 1024) - frontier.reads, frontier.charged_bytes);
    const strong = local.section_inverted.bm25Score(100, 100, 8192, 8192, 100, .{});
    try std.testing.expect((try frontier.next(strong)) == null);
    try std.testing.expect(frontier.pruned);
    // Equal ceilings are still eligible, including under a one-entry frontier.
    const complete = (try create(a, root, store, cached, .none, .{ .term = .{ .field = "body", .term = "common" } }, .{}, .{})).?;
    defer complete.deinit();
    complete.frontier_limit = 1;
    const Weak = struct {
        fn summary(_: *anyopaque, _: []const u8, _: []const u8, avg: f32, config: local.section_inverted.BM25Config) !local.index.IndexSnapshot.TextTermSummary {
            return (corpus.statistics.Summary{ .frequency = 1, .max_frequency = 1, .min_norm = 100 }).bound(avg, config);
        }
    };
    const equal_cutoff = try complete.plan.?.bound(.{ .ptr = complete, .read = Weak.summary });
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
