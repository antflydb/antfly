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

//! Bounded necessary-term proofs select file partitions before corpus admission.
//! Unknown or broad sets retain the full serving path; negative clauses never
//! establish a positive restriction. Scoring still uses authenticated global totals.
const std = @import("std");
const local = @import("antfly_local_sources");
const corpus = @import("lake_index_native_text.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const pages_mod = @import("../serverless/graph_segment/page_store.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Id = [32]u8;
const max_candidates = 4096;

pub fn select(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, cancellation: Cancellation, query: local.storage_db_types.TextQuery, analysis: local.introducer.TextAnalysisConfig, schema: local.storage_schema.TableSchema) !?[]const Id {
    if (root.collection == null or root.segment_files == null or root.serving_routes == null or root.inventory_version != 2 or root.term_directory.version != 2 or root.term_directory.leaves == null) return null;
    var budget: local.sql_memory_budget = .{ .backing = a, .limit = 8 * 1024 * 1024 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    const scratch = arena.allocator();
    var selector: Selector = .{ .a = scratch, .root = root, .store = store, .cached = cached, .cancellation = cancellation };
    const selected = selector.run(query, analysis, schema) catch |err| switch (err) {
        error.OutOfMemory, error.ArtifactReadBudgetExceeded, error.NativeLakeTermDirectoryKeyTooLarge => return null,
        else => return err,
    };
    if (selected) |files| return try a.dupe(Id, files);
    return null;
}
fn less(_: void, l: Id, r: Id) bool {
    return std.mem.order(u8, &l, &r) == .lt;
}
const Selector = struct {
    a: A,
    root: corpus.Root,
    store: stores.ArtifactStore,
    cached: artifacts.CachedRead,
    cancellation: Cancellation,
    reads: u64 = 8 * 1024 * 1024,
    writes: u64 = 0,
    pages: pages_mod.PageStore = undefined,
    work: usize = 512,
    fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, _: Id, token: Cancellation, budget: *u64) ![]u8 {
        const self: *Selector = @ptrCast(@alignCast(raw));
        try self.cached.context.ensureActive();
        if (offset != 0 or length != ref.byte_len) return error.InvalidNativeLakeTextCorpus;
        try stores.chargeReadBudget(budget, length);
        return artifacts.readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached);
    }
    fn run(self: *Selector, query: local.storage_db_types.TextQuery, analysis: local.introducer.TextAnalysisConfig, schema: local.storage_schema.TableSchema) !?[]const Id {
        self.pages = .{ .domain = self.root.domain, .artifacts = &self.store, .cancellation = self.cancellation, .remaining_read_bytes = &self.reads, .remaining_write_bytes = &self.writes, .read_cache = .{ .ptr = self, .read = readPage } };
        const lowered = try local.storage_db_query_search_exec.textQueryToSearchQuery(self.a, query, analysis, schema);
        const selected = (try self.necessary(lowered, 0)) orelse return null;
        var files: std.AutoHashMapUnmanaged(Id, void) = .empty;
        for (selected) |id| {
            var cursor = try tree.Cursor.init(self.a, self.pages.store(), self.root.segment_files, &id, null);
            defer cursor.deinit();
            const record = (try cursor.next()) orelse return error.InvalidNativeLakeTextCorpus;
            if (!std.mem.eql(u8, record.key, &id) or record.value.len != 32) return error.InvalidNativeLakeTextCorpus;
            try files.put(self.a, record.value[0..32].*, {});
        }
        // A selection spanning the whole corpus has no cold-admission benefit.
        if (files.count() >= self.root.manifests.len and files.count() != 0) return null;
        const result = try self.a.alloc(Id, files.count());
        var iterator = files.keyIterator();
        for (result) |*id| id.* = iterator.next().?.*;
        std.mem.sort(Id, result, {}, less);
        return result;
    }
    fn term(self: *Selector, field: []const u8, text: []const u8) !?[]const Id {
        if (self.work == 0) return null;
        self.work -= 1;
        const key = corpus.statistics.keyAlloc(self.a, field, text) catch |err| switch (err) {
            error.InvalidNativeLakeTextStatistics => return null,
            else => return err,
        };
        const prefix = try corpus.term_directory.prefixAlloc(self.a, key);
        const end = try self.a.dupe(u8, prefix);
        end[end.len - 1] = 1;
        var cursor = try tree.Cursor.init(self.a, self.pages.store(), self.root.term_directory.leaves, prefix, end);
        defer cursor.deinit();
        var result: std.ArrayList(Id) = .empty;
        while (try cursor.next()) |record| {
            if (result.items.len == max_candidates) return null;
            if (record.key.len != prefix.len + 32 or !std.mem.startsWith(u8, record.key, prefix)) return error.InvalidNativeLakeTermDirectory;
            _ = try corpus.statistics.Summary.decode(record.value);
            try result.append(self.a, record.key[prefix.len..][0..32].*);
        }
        return result.items;
    }
    fn unite(self: *Selector, left: ?[]const Id, right: ?[]const Id) !?[]const Id {
        const l = left orelse return null;
        const r = right orelse return null;
        var set: std.AutoHashMapUnmanaged(Id, void) = .empty;
        for (l) |id| try set.put(self.a, id, {});
        for (r) |id| {
            try set.put(self.a, id, {});
            if (set.count() > max_candidates) return null;
        }
        const result = try self.a.alloc(Id, set.count());
        var iterator = set.keyIterator();
        for (result) |*id| id.* = iterator.next().?.*;
        return result;
    }
    fn necessary(self: *Selector, query: local.search_search.SearchQuery, depth: usize) anyerror!?[]const Id {
        try self.cached.context.ensureActive();
        try self.cancellation.check();
        if (depth > 64 or self.work == 0) return null;
        self.work -= 1;
        switch (query) {
            .match_none => return &.{},
            .term => |value| return self.term(value.field, value.term),
            .match => |value| {
                const analyzer = value.analyzer orelse &local.search_analysis.default_analyzer;
                const tokens = try analyzer.analyze(self.a, value.text);
                var result: ?[]const Id = &.{};
                for (tokens) |token| result = try self.unite(result, try self.term(value.field, token.term));
                return result;
            },
            .term_phrase => |value| {
                if (value.max_edits != 0 or value.auto_fuzzy) return null;
                var result: ?[]const Id = null;
                if (value.terms.len == 0) return &.{};
                for (value.terms) |text| if (try self.term(value.field, text)) |set| {
                    if (result == null or set.len < result.?.len) result = set;
                };
                return result;
            },
            .bool_query => |value| {
                const minimum = if (value.should.len == 0) 0 else if (value.min_should == 0 and value.must.len == 0 and !value.pure_should_optional) 1 else value.min_should;
                if (minimum > value.should.len) return &.{};
                var result: ?[]const Id = null;
                for (value.must) |child| if (try self.necessary(child, depth + 1)) |set| {
                    if (result == null or set.len < result.?.len) result = set;
                };
                if (minimum != 0) {
                    var optional: ?[]const Id = &.{};
                    for (value.should) |child| optional = try self.unite(optional, try self.necessary(child, depth + 1));
                    if (optional) |set| if (result == null or set.len < result.?.len) {
                        result = set;
                    };
                }
                return result;
            },
            else => return null,
        }
    }
};

test "external lake necessary term routing keeps Boolean supersets and global collection totals" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("serving-selection");
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
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 64 * 1024 * 1024;
    var pages: pages_mod.PageStore = .{ .domain = @splat(6), .attempt = @splat(4), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const Source = struct {
        records: []const tree.Cursor.Record,
        position: usize = 0,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.position == self.records.len) return null;
            defer self.position += 1;
            return self.records[self.position];
        }
    };
    const common = try corpus.term_directory.prefixAlloc(sa, try corpus.statistics.keyAlloc(sa, "body", "common"));
    const rare = try corpus.term_directory.prefixAlloc(sa, try corpus.statistics.keyAlloc(sa, "body", "rare"));
    var buffer: [corpus.statistics.Summary.max_bytes]u8 = undefined;
    const summary = (corpus.statistics.Summary{ .frequency = 1, .max_frequency = 1, .min_norm = 1 }).encode(&buffer);
    const first: Id = @splat(1);
    const second: Id = @splat(2);
    var leaves: Source = .{ .records = &.{
        .{ .key = try std.mem.concat(sa, u8, &.{ common, &first }), .value = summary },
        .{ .key = try std.mem.concat(sa, u8, &.{ common, &second }), .value = summary },
        .{ .key = try std.mem.concat(sa, u8, &.{ rare, &first }), .value = summary },
    } };
    const leaf_root = try tree.buildSorted(a, pages.store(), &leaves);
    const file_a: Id = @splat(10);
    const file_b: Id = @splat(20);
    var files: Source = .{ .records = &.{ .{ .key = &first, .value = &file_a }, .{ .key = &second, .value = &file_b } } };
    const file_root = try tree.buildSorted(a, pages.store(), &files);
    const root: corpus.Root = .{ .domain = @splat(6), .binding = undefined, .config_json = "{}", .inventory_version = 2, .serving_routes = leaf_root, .segment_files = file_root, .collection = .{ .documents = 1000, .fields = &.{.{ .name = "body", .total_length = 10000 }} }, .term_directory = .{ .version = 2, .leaves = leaf_root }, .manifests = &.{ .{ .artifact_id = "", .checksum = "", .byte_len = 1 }, .{ .artifact_id = "", .checksum = "", .byte_len = 1 } } };
    const selected = (try select(a, root, store, cached, .none, .{ .term = .{ .field = "body", .term = "rare" } }, .{}, .{})).?;
    defer a.free(selected);
    try std.testing.expectEqualSlices(Id, &.{file_a}, selected);
    try std.testing.expectEqual(@as(u32, 1000), root.collection.?.documents);
    const absent = (try select(a, root, store, cached, .none, .{ .term = .{ .field = "body", .term = "absent" } }, .{}, .{})).?;
    defer a.free(absent);
    try std.testing.expectEqual(@as(usize, 0), absent.len);
    try std.testing.expect((try select(a, root, store, cached, .none, .{ .match = .{ .field = "body", .text = "COMMON rare" } }, .{}, .{})) == null);
    var selector: Selector = .{ .a = sa, .root = root, .store = store, .cached = cached, .cancellation = .none };
    selector.pages = .{ .domain = root.domain, .artifacts = &selector.store, .remaining_read_bytes = &selector.reads, .remaining_write_bytes = &selector.writes, .read_cache = .{ .ptr = &selector, .read = Selector.readPage } };
    const term: local.search_search.SearchQuery = .{ .term = .{ .field = "body", .term = "rare" } };
    const unknown: local.search_search.SearchQuery = .{ .prefix = .{ .field = "body", .prefix = "r" } };
    try std.testing.expect((try selector.necessary(.{ .bool_query = .{ .should = &.{ term, unknown } } }, 0)) == null);
    try std.testing.expect((try selector.necessary(.{ .bool_query = .{ .must_not = &.{term} } }, 0)) == null);
    const must = (try selector.necessary(.{ .bool_query = .{ .must = &.{term}, .should = &.{unknown} } }, 0)).?;
    try std.testing.expectEqualSlices(Id, &.{first}, must);
    const optional = (try selector.necessary(.{ .bool_query = .{ .should = &.{term}, .pure_should_optional = true } }, 0));
    try std.testing.expect(optional == null);
    const impossible = (try selector.necessary(.{ .bool_query = .{ .should = &.{term}, .min_should = 2 } }, 0)).?;
    try std.testing.expectEqual(@as(usize, 0), impossible.len);
}
