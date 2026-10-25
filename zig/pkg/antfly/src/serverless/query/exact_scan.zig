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

//! Bounded exact-vector scans. The embedding host authenticates continuation
//! state before calling this private kernel; every step reopens a pinned version.
const std = @import("std");
const A = std.mem.Allocator;
const indexed = @import("indexed_reader.zig");
const runtime = @import("runtime.zig");
const tree = @import("../graph_segment/page_tree.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const common = @import("../paged_projection.zig");
const paged = @import("../vector_segment/paged.zig");
const vectors = @import("antfly_vector").vector;
const platform_time = @import("antfly_platform").time;
pub const max_state_bytes = 128 * 1024;
pub const State = struct {
    format: u8 = 1,
    generation: []const u8,
    cutoff: []const u8,
    query: [32]u8,
    after: []const u8 = "",
    scanned: []const u8 = "0",
    top: []const indexed.ScoredDoc = &.{},
};
pub const Step = struct {
    state: State,
    complete: bool,
    step_records: usize,
    page_reads: usize,
    read_bytes: u64,
    pub fn deinit(self: *Step, a: A) void {
        a.free(self.state.generation);
        a.free(self.state.cutoff);
        a.free(self.state.after);
        a.free(self.state.scanned);
        for (self.state.top) |row| a.free(row.doc_id);
        a.free(self.state.top);
    }
};
// Unlike borrowed TopDocs, this owns at most k IDs and deduplicates multiple
// projections of one document, retaining its best exact distance across steps.
const Selection = struct {
    rows: std.ArrayList(indexed.ScoredDoc) = .empty,
    positions: std.StringHashMapUnmanaged(usize) = .empty,
    capacity: usize,
    bytes: usize = 0,
    fn deinit(self: *@This(), a: A) void {
        for (self.rows.items) |row| a.free(row.doc_id);
        self.rows.deinit(a);
        self.positions.deinit(a);
    }
    fn swap(self: *@This(), l: usize, r: usize) void {
        std.mem.swap(indexed.ScoredDoc, &self.rows.items[l], &self.rows.items[r]);
        self.positions.getPtr(self.rows.items[l].doc_id).?.* = l;
        self.positions.getPtr(self.rows.items[r].doc_id).?.* = r;
    }
    fn down(self: *@This(), start: usize) void {
        var i = start;
        while (i * 2 + 1 < self.rows.items.len) {
            var child = i * 2 + 1;
            if (child + 1 < self.rows.items.len and indexed.lessScoredDoc({}, self.rows.items[child], self.rows.items[child + 1])) child += 1;
            if (!indexed.lessScoredDoc({}, self.rows.items[i], self.rows.items[child])) break;
            self.swap(i, child);
            i = child;
        }
    }
    fn add(self: *@This(), a: A, value: indexed.ScoredDoc) !void {
        if (self.positions.get(value.doc_id)) |i| {
            if (indexed.lessScoredDoc({}, value, self.rows.items[i])) {
                self.rows.items[i].score = value.score;
                self.rows.items[i].distance = value.distance;
                self.down(i);
            }
            return;
        }
        const full = self.rows.items.len == self.capacity;
        if (full and !indexed.lessScoredDoc({}, value, self.rows.items[0])) return;
        const removed = if (full) self.rows.items[0].doc_id.len else 0;
        if (self.bytes - removed + value.doc_id.len > max_state_bytes / 2) return error.ExactScanStateTooLarge;
        try self.positions.ensureUnusedCapacity(a, 1);
        if (!full) try self.rows.ensureUnusedCapacity(a, 1);
        const id = try a.dupe(u8, value.doc_id);
        const row = indexed.ScoredDoc{ .doc_id = id, .score = value.score, .distance = value.distance };
        if (full) {
            const old = self.rows.items[0];
            _ = self.positions.remove(old.doc_id);
            self.bytes -= old.doc_id.len;
            a.free(old.doc_id);
            self.rows.items[0] = row;
            self.positions.putAssumeCapacity(id, 0);
            self.down(0);
        } else {
            var i = self.rows.items.len;
            self.rows.appendAssumeCapacity(row);
            self.positions.putAssumeCapacity(id, i);
            while (i > 0) {
                const parent = (i - 1) / 2;
                if (!indexed.lessScoredDoc({}, self.rows.items[parent], self.rows.items[i])) break;
                self.swap(parent, i);
                i = parent;
            }
        }
        self.bytes += id.len;
    }
};
const Meter = struct {
    backing: tree.Store,
    reads: usize = 0,
    fn get(ptr: *anyopaque, a: A, ref: tree.Ref) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.reads >= 128) return error.ExactScanYield;
        self.reads += 1;
        return self.backing.get(self.backing.ptr, a, ref);
    }
    fn many(ptr: *anyopaque, a: A, refs_: []const tree.Ref, output: []tree.Page) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (refs_.len > 128 - self.reads) return error.ExactScanYield;
        self.reads += refs_.len;
        return self.backing.read_many.?.read(self.backing.read_many.?.ptr, a, refs_, output);
    }
    fn put(_: *anyopaque, _: tree.Ref, _: []const u8) anyerror!void {
        return error.ExactScanWriteForbidden;
    }
    fn check(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.backing.check(self.backing.ptr);
    }
    fn store(self: *@This()) tree.Store {
        return .{ .domain = self.backing.domain, .ptr = self, .get = get, .put = put, .check = check, .read_many = if (self.backing.read_many) |_| .{ .ptr = self, .read = many } else null };
    }
};
fn decimal(text: []const u8) !u64 {
    if (text.len == 0 or text.len > 20 or (text.len > 1 and text[0] == '0')) return error.InvalidExactScanState;
    for (text) |c| if (!std.ascii.isDigit(c)) return error.InvalidExactScanState;
    return std.fmt.parseInt(u64, text, 10) catch error.InvalidExactScanState;
}
pub fn generation(state: State) !u64 {
    return decimal(state.generation);
}
fn queryBinding(a: A, session: *runtime.QuerySession, checksum: []const u8, query: []const f32, limit: usize) ![32]u8 {
    const identity = try std.json.Stringify.valueAlloc(a, .{ .format = @as(u8, 1), .namespace = session.manifest.namespace, .source = checksum, .query = query, .limit = limit }, .{});
    defer a.free(identity);
    var result: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(identity, &result, .{});
    return result;
}
pub fn scan(a: A, session: *runtime.QuerySession, query: []const f32, limit: usize, previous: ?State, maximum: usize) !Step {
    if (limit == 0 or limit > 100 or maximum == 0 or maximum > 512) return error.QueryCandidateBudgetExceeded;
    try session.checkCancellation();
    const index = try indexed.resolveVectorArtifactIndex(session, .{ .text = &.{} });
    const source = session.manifest.artifacts[index];
    if (source.metadata_version != common.metadata_version) return error.ExactScanCapabilityUnsupported;
    var remaining: u64 = 4 * 1024 * 1024;
    var writes: u64 = 0;
    var pages = PageStore{ .domain = PageStore.namespaceDomain(session.manifest.namespace), .artifacts = session.artifacts, .cancellation = session.readCancellation(), .remaining_read_bytes = &remaining, .remaining_write_bytes = &writes, .coalesce_reads = true, .read_cache = session.graphAdjacencyCache() };
    if (source.byte_len > remaining) return error.ArtifactReadBudgetExceeded;
    remaining -= source.byte_len;
    const parsed = try common.load(a, &pages, source);
    defer parsed.deinit();
    const root = parsed.value;
    if (query.len != root.dims and root.docs != null) return error.VectorDimsMismatch;
    const binding = try queryBinding(a, session, source.checksum, query, limit);
    const total = if (root.docs) |ref| ref.records else 0;
    var scanned: u64 = 0;
    var after: [36]u8 = undefined;
    var has_after = false;
    var selection = Selection{ .capacity = limit };
    defer selection.deinit(a);
    if (previous) |state| {
        if (state.format != 1 or !std.mem.eql(u8, &state.query, &binding) or try generation(state) != session.version() or try decimal(state.cutoff) != session.manifest.wal_end_lsn or state.top.len > limit) return error.ExactScanBindingMismatch;
        scanned = try decimal(state.scanned);
        if (scanned > total or (scanned == 0) != (state.after.len == 0)) return error.InvalidExactScanState;
        if (state.after.len != 0) {
            if (state.after.len != 72) return error.InvalidExactScanState;
            _ = std.fmt.hexToBytes(&after, state.after) catch return error.InvalidExactScanState;
            has_after = true;
        }
        for (state.top) |row| {
            if (row.doc_id.len == 0 or row.distance == null or !std.math.isFinite(row.distance.?)) return error.InvalidExactScanState;
            try selection.add(a, row);
        }
        if (selection.rows.items.len != state.top.len) return error.InvalidExactScanState;
    }
    const normalized = if (root.docs != null) try indexed.normalizedCosineQueryAlloc(a, root.metric, query) else query;
    defer if (root.docs != null and root.metric == .cosine) a.free(@constCast(normalized));
    const measure = indexed.vectorQueryMeasure(normalized, root.metric);
    var meter = Meter{ .backing = pages.store() };
    var cache = tree.Cache{ .alloc = a, .underlying = meter.store(), .max_bytes = 512 * 1024, .decoded = true };
    defer cache.deinit();
    var cursor = try tree.Cursor.initAtRank(a, cache.store(), root.docs, scanned);
    defer cursor.deinit();
    const started = platform_time.monotonicNs();
    const count_limit = @min(maximum, @max(@as(usize, 1), (1024 * 1024) / (@as(usize, root.dims) * 4 + 128)));
    var count: usize = 0;
    while (scanned < total and count < count_limit) {
        if (count != 0 and platform_time.monotonicNs() -| started >= std.time.ns_per_s) break;
        cursor.readAhead(count_limit - count, @intCast(@min(remaining, 128 * 1024))) catch |err| switch (err) {
            // Optional look-ahead admission must not prevent a smaller useful
            // single-page read at the end of a bounded step.
            error.ArtifactReadBudgetExceeded, error.ExactScanYield => {},
            else => return err,
        };
        const record = (cursor.next() catch |err| switch (err) {
            error.ArtifactReadBudgetExceeded, error.ExactScanYield => if (count != 0) break else return err,
            else => return err,
        }) orelse return error.InvalidExactScanState;
        if (has_after and std.mem.order(u8, record.key, &after) != .gt) return error.InvalidExactScanState;
        if (record.key.len != 36) return error.InvalidVectorValue;
        try session.checkCancellation();
        var entry = try paged.decodeVector(a, record.value, root.dims);
        defer entry.deinit(a);
        const similarity = indexed.similarityForQuery(normalized, measure, entry.vector, root.metric);
        if (!std.math.isFinite(similarity)) return error.InvalidVectorQuery;
        if (similarity > 0) try selection.add(a, .{ .doc_id = entry.doc_id, .score = indexed.scoreFromSimilarity(similarity), .distance = vectors.distanceToQuery(normalized, measure, entry.vector, root.metric) });
        @memcpy(&after, record.key);
        has_after = true;
        count += 1;
        scanned += 1;
    }
    const gen = try std.fmt.allocPrint(a, "{d}", .{session.version()});
    errdefer a.free(gen);
    const cutoff = try std.fmt.allocPrint(a, "{d}", .{session.manifest.wal_end_lsn});
    errdefer a.free(cutoff);
    const position = try a.dupe(u8, if (has_after) &std.fmt.bytesToHex(after, .lower) else "");
    errdefer a.free(position);
    const visited = try std.fmt.allocPrint(a, "{d}", .{scanned});
    errdefer a.free(visited);
    const top = try selection.rows.toOwnedSlice(a);
    std.mem.sort(indexed.ScoredDoc, top, {}, indexed.lessScoredDoc);
    return .{ .state = .{ .generation = gen, .cutoff = cutoff, .query = binding, .after = position, .scanned = visited, .top = top }, .complete = scanned == total, .step_records = count, .page_reads = meter.reads, .read_bytes = 4 * 1024 * 1024 - remaining };
}

pub fn materialize(a: A, session: *runtime.QuerySession, rows: []const indexed.ScoredDoc) ![]@import("request.zig").SearchHit {
    var reads: u64 = 4 * 1024 * 1024;
    const reader = try @import("document_facts_reader.zig").Reader.create(a, session, &reads) orelse return error.ExactScanCapabilityUnsupported;
    defer reader.destroy();
    const Hit = @import("request.zig").SearchHit;
    const hits = try a.alloc(Hit, rows.len);
    var initialized: usize = 0;
    errdefer {
        for (hits[0..initialized]) |hit| {
            a.free(hit.doc_id);
            a.free(hit.body);
        }
        a.free(hits);
    }
    var bytes: u64 = 0;
    for (rows, hits) |row, *hit| {
        const fact = try reader.lookup(row.doc_id) orelse return error.DocumentBodyNotFound;
        bytes += fact.body.bytes;
        if (bytes > 1024 * 1024) return error.QueryCandidateBudgetExceeded;
        const id = try a.dupe(u8, row.doc_id);
        errdefer a.free(id);
        hit.* = .{ .doc_id = id, .body = try reader.readBodyAlloc(fact), .score = row.score, .distance = row.distance };
        initialized += 1;
    }
    return hits;
}

test "serverless exact scan resumes bounded immutable ranges and merges multi-projection ties" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/exact", .{tmp.sub_path});
    defer a.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var store = fs.artifactStore();
    defer store.deinit();
    var reads: u64 = 32 * 1024 * 1024;
    var writes: u64 = 32 * 1024 * 1024;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var ids: [770][16]u8 = undefined;
    var values: [770][8]f32 = undefined;
    var entries: [770]@import("../vector_segment/mod.zig").Entry = undefined;
    var seed: u32 = 71;
    for (&ids, &values, &entries, 0..) |*id, *vector, *entry, i| {
        for (vector) |*v| {
            seed = seed *% 1664525 +% 1013904223;
            v.* = @as(f32, @floatFromInt(seed >> 8)) / 8388608.0 - 1;
        }
        entry.* = .{ .doc_id = try std.fmt.bufPrint(id, "doc/{d}", .{if (i < 768) i else 0}), .vector = vector };
    }
    const ref = try paged.publishAlloc(a, &pages, null, &entries, &.{}, "dense", @splat(1), .cosine, null);
    defer @import("../build/builder.zig").freeArtifactRef(a, ref);
    var artifacts = [_]@import("../manifest/mod.zig").ArtifactRef{ref};
    var session = runtime.QuerySession{ .alloc = a, .artifacts = &store, .owns_manifest = false, .manifest = .{ .namespace = "docs", .version = 1, .built_at_ns = 1, .wal_start_lsn = 1, .wal_end_lsn = 770, .stats = .{ .document_count = 768 }, .artifacts = &artifacts } };
    const query = [_]f32{ 1, 0, 0, 0, 0, 0, 0, 0 };
    var stats: indexed.SearchExecutionStats = .{};
    const oracle = try paged.searchAlloc(a, &session, ref, .{ .text = &.{}, .vector = @constCast(&query), .limit = 10, .search_effort = 1 }, &stats);
    defer indexed.freeScoredDocs(a, oracle);
    const cache_path = try std.fmt.allocPrint(a, "{s}/cache", .{path});
    defer a.free(cache_path);
    var shared = try @import("cache.zig").QueryCache.init(a, cache_path);
    defer shared.deinit();
    session.cache = &shared;
    // Cold duplicate claims, failed-fill cleanup, then a zero-I/O warm batch.
    const projection = try common.load(a, &pages, ref);
    defer projection.deinit();
    var branch = try tree.readPage(a, pages.store(), projection.value.docs.?);
    defer branch.deinit(a);
    try std.testing.expectEqual(@as(u8, 1), branch.ref.height);
    try std.testing.expect(branch.entries[0].child.?.pack != null and branch.entries[1].child.?.pack != null);
    const refs_ = [_]tree.Ref{ branch.entries[0].child.?.*, branch.entries[0].child.?.*, branch.entries[1].child.?.* };
    const hook = session.graphAdjacencyCache().?;
    var encoded: [3][]u8 = undefined;
    var allowance: u64 = 0;
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, hook.read_many.?(hook.ptr, a, &store, pages.domain, &refs_, &encoded, .none, &allowance));
    try std.testing.expectEqual(@as(usize, 0), shared.graph_metric_blocks.snapshot().entries);
    allowance = 4 * 1024 * 1024;
    try hook.read_many.?(hook.ptr, a, &store, pages.domain, &refs_, &encoded, .none, &allowance);
    for (encoded) |bytes| a.free(bytes);
    try std.testing.expect(allowance < 4 * 1024 * 1024);
    allowance = 0;
    try hook.read_many.?(hook.ptr, a, &store, pages.domain, &refs_, &encoded, .none, &allowance);
    for (encoded) |bytes| a.free(bytes);
    var current: ?Step = null;
    defer if (current) |*step| step.deinit(a);
    var steps: usize = 0;
    while (true) {
        const next = try scan(a, &session, &query, 10, if (current) |step| step.state else null, 13);
        if (current) |*step| step.deinit(a);
        current = next;
        steps += 1;
        try std.testing.expect(next.step_records <= 13 and next.page_reads <= 128 and next.read_bytes <= 4 * 1024 * 1024);
        if (steps == 1) {
            try std.testing.expectError(error.ExactScanBindingMismatch, scan(a, &session, &query, 9, next.state, 13));
            session.manifest.version = 2;
            try std.testing.expectError(error.ExactScanBindingMismatch, scan(a, &session, &query, 10, next.state, 13));
            session.manifest.version = 1;
            var stale = next.state;
            stale.scanned = "99999";
            try std.testing.expectError(error.InvalidExactScanState, scan(a, &session, &query, 10, stale, 13));
        }
        if (next.complete) break;
        try std.testing.expect(steps < 100);
    }
    try std.testing.expect(steps > 1);
    try std.testing.expectEqualStrings("770", current.?.state.scanned);
    try std.testing.expectEqual(oracle.len, current.?.state.top.len);
    for (oracle, current.?.state.top) |l, r| {
        try std.testing.expectEqualStrings(l.doc_id, r.doc_id);
        // Cluster payloads store FP16 values; the exact tree retains FP32.
        // Rankings agree on this corpus, while distances may differ slightly.
        try std.testing.expectApproxEqAbs(l.distance.?, r.distance.?, 0.001);
    }
}

test "serverless exact top-k deduplicates improvements and releases failed allocations" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, selectionFailure, .{});
}
fn selectionFailure(a: A) !void {
    var selection = Selection{ .capacity = 3 };
    defer selection.deinit(a);
    try selection.add(a, .{ .doc_id = "a", .score = 1, .distance = 0.9 });
    try selection.add(a, .{ .doc_id = "b", .score = 2, .distance = 0.8 });
    try selection.add(a, .{ .doc_id = "c", .score = 3, .distance = 0.7 });
    try selection.add(a, .{ .doc_id = "a", .score = 5, .distance = 0.5 });
    try selection.add(a, .{ .doc_id = "d", .score = 4, .distance = 0.6 });
    std.mem.sort(indexed.ScoredDoc, selection.rows.items, {}, indexed.lessScoredDoc);
    try std.testing.expectEqualStrings("a", selection.rows.items[0].doc_id);
    try std.testing.expectEqualStrings("d", selection.rows.items[1].doc_id);
    try std.testing.expectEqualStrings("c", selection.rows.items[2].doc_id);
}
