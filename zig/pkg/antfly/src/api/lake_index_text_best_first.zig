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

//! Publication-wide score traversal over authenticated term-page envelopes.
//! Admissions remain independently verified; unknown proofs use the exact route.
const std = @import("std");
const local = @import("antfly_local_sources");
const corpus = @import("lake_index_native_text.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
const pages_mod = @import("../serverless/graph_segment/page_store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;

pub const Reader = struct {
    const Term = struct { field: []const u8, text: []const u8, boost: f32 };
    a: A,
    arena: std.heap.ArenaAllocator,
    root: corpus.Root,
    store: stores.ArtifactStore,
    cached: artifacts.CachedRead,
    token: Cancellation,
    reads: u64 = 32 * 1024 * 1024,
    writes: u64 = 0,
    pages: pages_mod.PageStore = undefined,
    cursor: tree.BestFirst = undefined,
    opened: bool = false,
    prefix: []const u8 = "",
    average: f32 = 1,
    idf_boost: f32 = 0,

    pub fn create(a: A, root: corpus.Root, store: stores.ArtifactStore, cached: artifacts.CachedRead, token: Cancellation, query: local.storage_db_types.TextQuery, analysis: local.introducer.TextAnalysisConfig, schema: local.storage_schema.TableSchema) !?*Reader {
        if (root.collection == null or root.collection.?.documents == 0 or root.term_statistics == null or root.term_directory.version != 2 or root.term_directory.leaves == null) return null;
        const self = try a.create(Reader);
        self.* = .{ .a = a, .arena = .init(a), .root = root, .store = store, .cached = cached, .token = token };
        errdefer self.deinit();
        const ca = self.arena.allocator();
        const lowered = try local.storage_db_query_search_exec.textQueryToSearchQuery(ca, query, analysis, schema);
        const term: Term = switch (lowered) {
            .term => |v| .{ .field = v.field, .text = v.term, .boost = v.boost },
            .match => |v| blk: {
                const analyzer = v.analyzer orelse &local.search_analysis.default_analyzer;
                const tokens = try analyzer.analyze(ca, v.text);
                if (tokens.len != 1) {
                    self.deinit();
                    return null;
                }
                break :blk .{ .field = v.field, .text = tokens[0].term, .boost = v.boost };
            },
            else => {
                self.deinit();
                return null;
            },
        };
        if (!std.math.isFinite(term.boost) or term.boost < 0) {
            self.deinit();
            return null;
        }
        const total = for (root.collection.?.fields) |field| {
            if (std.mem.eql(u8, field.name, term.field)) break field.total_length;
        } else {
            self.deinit();
            return null;
        };
        self.average = if (total == 0) 1 else @as(f32, @floatFromInt(total)) / @as(f32, @floatFromInt(root.collection.?.documents));
        self.pages = .{ .domain = root.domain, .artifacts = &self.store, .cancellation = token, .remaining_read_bytes = &self.reads, .remaining_write_bytes = &self.writes, .read_cache = .{ .ptr = self, .read = readPage } };
        const key = try corpus.statistics.keyAlloc(ca, term.field, term.text);
        var global = try tree.Cursor.init(a, self.pages.store(), root.term_statistics, key, null);
        defer global.deinit();
        const record = try global.next();
        const frequency: u64 = if (record) |v| if (std.mem.eql(u8, v.key, key)) blk: {
            if (v.value.len != 8) return error.InvalidNativeLakeTextStatistics;
            break :blk std.mem.readInt(u64, v.value[0..8], .big);
        } else 0 else 0;
        if (frequency > root.collection.?.documents) return error.InvalidNativeLakeTextStatistics;
        self.idf_boost = local.section_inverted.bm25Idf(root.collection.?.documents, @intCast(frequency)) * term.boost;
        self.prefix = try corpus.term_directory.prefixAlloc(ca, key);
        const end = try ca.dupe(u8, self.prefix);
        end[end.len - 1] = 1;
        self.cursor = try tree.BestFirst.init(a, self.pages.store(), root.term_directory.leaves, self.prefix, end, .{ .ptr = self, .value = bound });
        self.opened = true;
        return self;
    }
    pub fn deinit(self: *Reader) void {
        if (self.opened) self.cursor.deinit();
        self.arena.deinit();
        self.a.destroy(self);
    }
    fn readPage(raw: *anyopaque, a: A, store: *stores.ArtifactStore, ref: local.serverless_manifest_artifact_ref.ArtifactRef, offset: u64, length: usize, _: [32]u8, token: Cancellation, budget: *u64) ![]u8 {
        const self: *Reader = @ptrCast(@alignCast(raw));
        try self.cached.context.ensureActive();
        if (offset != 0 or length != ref.byte_len) return error.InvalidNativeLakeTextCorpus;
        try stores.chargeReadBudget(budget, length);
        return artifacts.readArtifact(a, store.*, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, token, self.cached);
    }
    fn bound(raw: *anyopaque, bytes: []const u8, records: ?u64) !f32 {
        const self: *Reader = @ptrCast(@alignCast(raw));
        const summary = if (records) |count| blk: {
            const branch = try corpus.term_directory.Branch.decode(bytes);
            if (branch.count != count) return error.InvalidNativeLakeTermDirectory;
            break :blk branch.envelope;
        } else try corpus.statistics.Summary.decode(bytes);
        const score = summary.bound(self.average, .{}).tf_upper * self.idf_boost;
        // Float products can round down. Preserve a conservative envelope.
        return if (std.math.isFinite(score) and score > 0) @bitCast(@as(u32, @bitCast(score)) + 2) else score;
    }
    pub fn next(self: *Reader, cutoff: ?f32) !?[32]u8 {
        try self.cached.context.ensureActive();
        try self.token.check();
        const record = (try self.cursor.next(cutoff)) orelse return null;
        if (record.key.len != self.prefix.len + 32 or !std.mem.startsWith(u8, record.key, self.prefix)) return error.InvalidNativeLakeTermDirectory;
        return record.key[self.prefix.len..][0..32].*;
    }
};

test "external lake publication score traversal uses global statistics boosts and equal bounds" {
    const a = std.testing.allocator;
    var directory_ = try local.common_test_directory.TestDirectory.init("text-best-first");
    defer directory_.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory_.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(6), 1, std.testing.io);
    var cache = local.serverless_query_lake_serving_cache.Cache.init(a);
    defer cache.deinit();
    const cached: artifacts.CachedRead = .{ .cache = &cache, .scope = @splat(3), .context = .{ .io = std.testing.io } };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var reads: u64 = 0;
    var writes: u64 = 0;
    var pages: pages_mod.PageStore = .{ .domain = store.upload_scope.?.domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes, .streaming = true };
    const Source = struct {
        records: []const tree.Cursor.Record,
        at: usize = 0,
        pub fn next(self: *@This()) !?tree.Cursor.Record {
            if (self.at == self.records.len) return null;
            const result = self.records[self.at];
            self.at += 1;
            return result;
        }
    };
    const key = try corpus.statistics.keyAlloc(ca, "body", "quasar");
    var frequency: [8]u8 = undefined;
    std.mem.writeInt(u64, &frequency, 3, .big);
    var global: Source = .{ .records = &.{.{ .key = key, .value = &frequency }} };
    const df = try tree.buildSorted(a, pages.store(), &global);
    const prefix = try corpus.term_directory.prefixAlloc(ca, key);
    var low_bytes: [corpus.statistics.Summary.max_bytes]u8 = undefined;
    var high_bytes: [corpus.statistics.Summary.max_bytes]u8 = undefined;
    const low = (corpus.statistics.Summary{ .frequency = 1, .max_frequency = 1, .min_norm = 100 }).encode(&low_bytes);
    const high = (corpus.statistics.Summary{ .frequency = 1, .max_frequency = 20, .min_norm = 1 }).encode(&high_bytes);
    const first: [32]u8 = @splat(1);
    const second: [32]u8 = @splat(2);
    const third: [32]u8 = @splat(3);
    var source: Source = .{ .records = &.{
        .{ .key = try std.mem.concat(ca, u8, &.{ prefix, &first }), .value = low },
        .{ .key = try std.mem.concat(ca, u8, &.{ prefix, &second }), .value = high },
        .{ .key = try std.mem.concat(ca, u8, &.{ prefix, &third }), .value = high },
    } };
    var reduced = pages.store();
    reduced.reduce = corpus.term_directory.reduce;
    const leaves = try tree.buildSorted(a, reduced, &source);
    const root: corpus.Root = .{ .domain = store.upload_scope.?.domain, .binding = undefined, .config_json = "{}", .inventory_version = 3, .collection = .{ .documents = 1000, .fields = &.{.{ .name = "body", .total_length = 10000 }} }, .term_statistics = df, .term_directory = .{ .version = 2, .leaves = leaves } };
    const query: local.storage_db_types.TextQuery = .{ .term = .{ .field = "body", .term = "quasar", .boost = 2 } };
    const reader = (try Reader.create(a, root, store, cached, .none, query, .{}, .{})).?;
    defer reader.deinit();
    const id = (try reader.next(null)).?;
    try std.testing.expect(std.mem.eql(u8, &id, &second) or std.mem.eql(u8, &id, &third));
    const cutoff = highBound: {
        const summary = try corpus.statistics.Summary.decode(high);
        break :highBound summary.bound(10, .{}).tf_upper * local.section_inverted.bm25Idf(1000, 3) * 2;
    };
    const other = (try reader.next(cutoff)).?;
    try std.testing.expect(!std.mem.eql(u8, &id, &other));
    try std.testing.expect((try reader.next(cutoff)) == null);
    try std.testing.expect(reader.cursor.pruned);
    try std.testing.expect((try Reader.create(a, root, store, cached, .none, .{ .match = .{ .field = "body", .text = "quasar galaxy" } }, .{}, .{})) == null);
}
