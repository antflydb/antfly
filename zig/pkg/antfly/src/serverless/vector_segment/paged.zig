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

//! Adaptive projection shards and native quantized vector-cluster payloads.
const std = @import("std");
const A = std.mem.Allocator;
const common = @import("../paged_projection.zig");
const tree = @import("../graph_segment/page_tree.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const manifest = @import("../manifest/mod.zig");
const runtime = @import("../query/runtime.zig");
const request = @import("../query/request.zig");
const indexed = @import("../query/indexed_reader.zig");
const segment = @import("mod.zig");
const build = @import("../build/vector_index.zig");
const cancellation = @import("../maintenance_cancellation.zig");
const vectors = @import("antfly_vector").vector;
const max_shard_bytes = 1024 * 1024;
const max_shard_entries = 1024;

const Shard = struct {
    start: [32]u8 = @splat(0),
    bits: u16 = 0,
    clusters: u32 = 0,
    entries: u32 = 0,
    fn encode(self: Shard) [10]u8 {
        var value: [10]u8 = undefined;
        std.mem.writeInt(u16, value[0..2], self.bits, .little);
        std.mem.writeInt(u32, value[2..6], self.clusters, .little);
        std.mem.writeInt(u32, value[6..10], self.entries, .little);
        return value;
    }
    fn decode(key: []const u8, value: []const u8) !Shard {
        if (key.len != 32 or value.len != 10) return error.InvalidVectorShard;
        var self: Shard = .{};
        @memcpy(&self.start, key);
        self.bits = std.mem.readInt(u16, value[0..2], .little);
        self.clusters = std.mem.readInt(u32, value[2..6], .little);
        self.entries = std.mem.readInt(u32, value[6..10], .little);
        if (self.bits > 256 or self.bits % 4 != 0 or self.clusters > 64) return error.InvalidVectorShard;
        return self;
    }
};
fn hashId(id: []const u8) [32]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(id, &digest, .{});
    return digest;
}
fn rangeEnd(a: A, shard: Shard) !?[]u8 {
    if (shard.bits == 0) return null;
    var end = shard.start;
    var i: usize = (shard.bits - 1) / 8;
    var carry: u16 = @as(u16, 1) << @intCast(8 - (if (shard.bits % 8 == 0) @as(u16, 8) else shard.bits % 8));
    while (true) {
        const sum = @as(u16, end[i]) + carry;
        end[i] = @truncate(sum);
        carry = sum >> 8;
        if (carry == 0) return try a.dupe(u8, &end);
        if (i == 0) return null;
        i -= 1;
    }
}
fn findShard(a: A, store: tree.Store, root: ?tree.Ref, hash: [32]u8) !Shard {
    if (root == null) return .{};
    const upper = try common.upperBound(a, &hash);
    defer if (upper) |value| a.free(value);
    var cursor = try tree.Cursor.initReverse(a, store, root, "", upper);
    defer cursor.deinit();
    const record = (try cursor.next()) orelse return error.InvalidVectorShard;
    const shard = try Shard.decode(record.key, record.value);
    var prefix = hash;
    for (&prefix, 0..) |*byte, i| {
        if (i * 8 >= shard.bits) byte.* = 0 else if (i * 8 + 8 > shard.bits) byte.* &= @as(u8, 255) << @intCast(8 - shard.bits % 8);
    }
    if (!std.mem.eql(u8, &prefix, &shard.start)) return error.InvalidVectorShard;
    return shard;
}
fn vectorValue(a: A, entry: segment.Entry) ![]u8 {
    const size = try std.math.add(usize, 8 + entry.doc_id.len, try std.math.mul(usize, entry.vector.len, 4));
    const value = try a.alloc(u8, size);
    errdefer a.free(value);
    std.mem.writeInt(u32, value[0..4], @intCast(entry.doc_id.len), .little);
    std.mem.writeInt(u32, value[4..8], @intCast(entry.vector.len), .little);
    @memcpy(value[8..][0..entry.doc_id.len], entry.doc_id);
    for (entry.vector, 0..) |number, i| {
        if (!std.math.isFinite(number)) return error.InvalidVectorValue;
        std.mem.writeInt(u32, value[8 + entry.doc_id.len + i * 4 ..][0..4], @bitCast(number), .little);
    }
    return value;
}
pub fn decodeVector(a: A, value: []const u8, dims: u32) !segment.Entry {
    if (value.len < 8) return error.InvalidVectorValue;
    const id_len = std.mem.readInt(u32, value[0..4], .little);
    const count = std.mem.readInt(u32, value[4..8], .little);
    if (count != dims or @as(u64, id_len) + 8 + @as(u64, dims) * 4 != value.len) return error.InvalidVectorValue;
    const id = try a.dupe(u8, value[8..][0..id_len]);
    errdefer a.free(id);
    const vector = try a.alloc(f32, dims);
    errdefer a.free(vector);
    for (vector, 0..) |*number, i| {
        number.* = @bitCast(std.mem.readInt(u32, value[8 + id_len + i * 4 ..][0..4], .little));
        if (!std.math.isFinite(number.*)) return error.InvalidVectorValue;
    }
    return .{ .doc_id = id, .vector = vector };
}
fn clusterKey(a: A, start: [32]u8, ordinal: u32) ![]u8 {
    const key = try a.alloc(u8, 36);
    @memcpy(key[0..32], &start);
    std.mem.writeInt(u32, key[32..36], ordinal, .big);
    return key;
}

// Globally trained, immutable coarse centers partition subsequent updates by
// vector locality. Identity -> routing memberships keeps deletes exact even
// when one document has projections in several coarse cells.
fn loadCenters(a: A, store: tree.Store, root: common.Root) ![][]f32 {
    var out = std.ArrayList([]f32).empty;
    errdefer {
        for (out.items) |v| a.free(v);
        out.deinit(a);
    }
    var cursor = try tree.Cursor.init(a, store, root.global_centers, "", null);
    defer cursor.deinit();
    while (try cursor.next()) |row| {
        if (out.items.len >= 64 or row.key.len != 2 or std.mem.readInt(u16, row.key[0..2], .big) != out.items.len or row.value.len != @as(usize, root.dims) * 4) return error.InvalidGlobalVectorCenters;
        const vector = try a.alloc(f32, root.dims);
        errdefer a.free(vector);
        for (vector, 0..) |*value, i| {
            value.* = @bitCast(std.mem.readInt(u32, row.value[i * 4 ..][0..4], .little));
            if (!std.math.isFinite(value.*)) return error.InvalidGlobalVectorCenters;
        }
        try out.append(a, vector);
    }
    if (out.items.len == 0) return error.InvalidGlobalVectorCenters;
    return out.toOwnedSlice(a);
}
fn freeCenters(a: A, centers: [][]f32) void {
    for (centers) |v| a.free(v);
    a.free(centers);
}
fn nearestCenter(vector: []const f32, centers: []const []f32, metric: vectors.DistanceMetric) u16 {
    var best: u16 = 0;
    var best_score: f32 = -std.math.inf(f32);
    const measure = indexed.vectorQueryMeasure(vector, metric);
    for (centers, 0..) |center, i| {
        const score = indexed.routingScoreForQuery(vector, measure, center, metric);
        if (score > best_score) {
            best = @intCast(i);
            best_score = score;
        }
    }
    return best;
}
fn routedHash(entry: segment.Entry, centers: []const []f32, metric: vectors.DistanceMetric) [32]u8 {
    var hash = hashId(entry.doc_id);
    std.mem.writeInt(u16, hash[0..2], nearestCenter(entry.vector, centers, metric), .big);
    return hash;
}
// Estimate the best member score using the cluster's measured mean alignment
// and residual spread. This is a ranking heuristic, never an exact prune bound.
fn cosinePriority(score: f32, average_distance: f32, count: u32, dims: u32) f32 {
    const alignment = std.math.clamp(1 - average_distance, 0, 1);
    const variance = @max(@as(f32, 0), (1 - score * score) * (1 - alignment * alignment)) / @as(f32, @floatFromInt(dims));
    return score * alignment + @sqrt(variance * 2 * @log(@as(f32, @floatFromInt(@max(count, 1)))));
}

fn addMembership(a: A, map: *std.StringHashMapUnmanaged(?[]const u8), id: []const u8, hash: [32]u8) !void {
    const identity = hashId(id);
    const previous = map.get(&identity) orelse null;
    if (previous) |bytes| {
        var i: usize = 0;
        while (i < bytes.len) : (i += 32) if (std.mem.eql(u8, bytes[i..][0..32], &hash)) return;
    }
    const length = if (previous) |bytes| bytes.len else 0;
    if (length >= 32 * 1024) return error.VectorDocumentBudgetExceeded;
    const value = try a.alloc(u8, length + 32);
    if (previous) |bytes| @memcpy(value[0..length], bytes);
    @memcpy(value[length..], &hash);
    try map.put(a, try a.dupe(u8, &identity), value);
}
fn trainGlobal(a: A, scratch: A, pages: *PageStore, store: tree.Store, root: *common.Root, maintenance: ?cancellation.Token) !void {
    // An existing large legacy index needs a bounded reindex; never migrate an
    // unbounded archive in an ordinary incremental publication.
    const count = root.docs.?.records;
    if (root.format != 1) return;
    const maximum = @min(@as(u64, 2048), (4 * 1024 * 1024) / (@as(u64, root.dims) * 4));
    const training_count = std.math.floorPowerOfTwo(u64, maximum);
    if (training_count < 256 or count < training_count or count > training_count + 256 or @as(u64, root.dims) * count * 4 > 4 * 1024 * 1024) return;
    var samples = std.ArrayList(segment.Entry).empty;
    defer {
        for (samples.items) |*entry| entry.deinit(a);
        samples.deinit(a);
    }
    var cursor = try tree.Cursor.init(a, store, root.docs, "", null);
    defer cursor.deinit();
    while (samples.items.len < training_count) {
        const row = (try cursor.next()) orelse return error.InvalidVectorValue;
        var entry = try decodeVector(a, row.value, root.dims);
        errdefer entry.deinit(a);
        try samples.append(a, entry);
    }
    // Transfer the decoded sample into native training. It already owns its
    // vectors; a second full projection copy consumes unnecessary WASM space.
    const owned = try samples.toOwnedSlice(a);
    var trained = try build.buildClusteredSegmentWithPolicyAllocUntil(a, root.metric, root.dims, owned, .{ .target_cluster_count = 64 }, maintenance);
    defer trained.deinit(a);
    const center_changes = try scratch.alloc(tree.Mutation, trained.clusters.len);
    const center_values = try scratch.alloc([]f32, trained.clusters.len);
    for (trained.clusters, center_changes, center_values, 0..) |cluster, *change, *center, i| {
        const key = try scratch.alloc(u8, 2);
        std.mem.writeInt(u16, key[0..2], @intCast(i), .big);
        const value = try scratch.alloc(u8, @as(usize, root.dims) * 4);
        for (cluster.centroid, 0..) |number, d| std.mem.writeInt(u32, value[d * 4 ..][0..4], @bitCast(number), .little);
        change.* = .{ .key = key, .value = value };
        center.* = cluster.centroid;
    }
    root.global_centers = try tree.apply(a, store, null, center_changes);
    var changed = std.StringHashMapUnmanaged(?[]const u8).empty;
    var identities = std.StringHashMapUnmanaged(?[]const u8).empty;
    var all = try tree.Cursor.init(a, store, root.docs, "", null);
    defer all.deinit();
    while (try all.next()) |row| {
        try pages.cancellation.check();
        var entry = try decodeVector(a, row.value, root.dims);
        defer entry.deinit(a);
        const hash = routedHash(entry, center_values, root.metric);
        const ordinal = std.mem.readInt(u32, row.key[32..36], .big);
        try changed.put(scratch, try clusterKey(scratch, hash, ordinal), try scratch.dupe(u8, row.value));
        try addMembership(scratch, &identities, entry.doc_id, hash);
    }
    root.docs = try common.applyMap(a, scratch, store, null, changed);
    root.postings = try common.applyMap(a, scratch, store, null, identities);
    root.directory = null;
    root.centroids = null;
    root.format = 2;
}

pub fn publishAlloc(a: A, pages: *PageStore, prior: ?manifest.ArtifactRef, delta: []const segment.Entry, removed: []const []const u8, name: []const u8, fingerprint: [32]u8, metric: vectors.DistanceMetric, maintenance: ?cancellation.Token) !manifest.ArtifactRef {
    const parsed = if (prior) |ref| try common.load(a, pages, ref) else null;
    defer if (parsed) |value| value.deinit();
    var root = if (parsed) |value| value.value else common.Root{ .kind = .vector_segment, .domain = pages.domain, .index_name = name, .source_fingerprint = fingerprint, .metric = metric };
    if (root.kind != .vector_segment or root.metric != metric or !std.mem.eql(u8, name, root.index_name) or !std.mem.eql(u8, &root.source_fingerprint, &fingerprint)) return error.VectorIndexDefinitionChanged;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var projections = std.StringHashMapUnmanaged(?[]const u8).empty;
    var affected = std.StringHashMapUnmanaged(Shard).empty;
    var text_pages = pages.*;
    text_pages.prefix_keys = true;
    // Reuse the balanced incremental/pack writer for bounded updates. Untouched
    // subtrees keep their immutable refs; no complete-tree repack is required.
    text_pages.pack_pages = true;
    var cache = tree.Cache{ .alloc = a, .underlying = text_pages.store() };
    defer cache.deinit();
    const centers = if (root.global_centers != null) try loadCenters(a, cache.store(), root) else null;
    defer if (centers) |values| freeCenters(a, values);
    var identity_changes = std.StringHashMapUnmanaged(?[]const u8).empty;
    var removals = std.ArrayList([]const u8).empty;
    try removals.appendSlice(scratch, removed);
    for (delta) |entry| try removals.append(scratch, entry.doc_id);
    var removed_seen = std.StringHashMapUnmanaged(void).empty;
    for (removals.items) |id| {
        if ((try removed_seen.getOrPut(scratch, id)).found_existing) continue;
        try pages.cancellation.check();
        const identity = hashId(id);
        const found = if (root.format == 2) try tree.lookupMany(a, cache.store(), root.postings, &.{&identity}) else null;
        defer if (found) |values| {
            if (values[0]) |value| value.deinit(a);
            a.free(values);
        };
        const hashes: []const u8 = if (found) |values| (if (values[0]) |value| value.value else "") else &identity;
        if (hashes.len % 32 != 0) return error.InvalidVectorValue;
        var offset: usize = 0;
        while (offset < hashes.len) : (offset += 32) {
            const hash: [32]u8 = hashes[offset..][0..32].*;
            const shard = try findShard(a, cache.store(), root.directory, hash);
            try affected.put(scratch, try scratch.dupe(u8, &shard.start), shard);
            const upper = try common.upperBound(a, &hash);
            defer if (upper) |value| a.free(value);
            var cursor = try tree.Cursor.init(a, cache.store(), root.docs, &hash, upper);
            defer cursor.deinit();
            while (try cursor.next()) |record| {
                if (record.key.len != 36 or record.value.len < 8) return error.InvalidVectorValue;
                const length = std.mem.readInt(u32, record.value[0..4], .little);
                if (length > record.value.len - 8 or !std.mem.eql(u8, record.value[8..][0..length], id)) return error.VectorDocumentHashCollision;
                try projections.put(scratch, try scratch.dupe(u8, record.key), null);
            }
        }
        if (root.format == 2) try identity_changes.put(scratch, try scratch.dupe(u8, &identity), null);
    }
    var ordinals = std.StringHashMapUnmanaged(u32).empty;
    for (delta) |entry| {
        try pages.cancellation.check();
        if (entry.vector.len == 0 or entry.vector.len > 16384) return error.InconsistentVectorDims;
        if (root.dims == 0) root.dims = @intCast(entry.vector.len);
        if (root.dims != entry.vector.len) return error.InconsistentVectorDims;
        const hash = if (centers) |values| routedHash(entry, values, root.metric) else hashId(entry.doc_id);
        if (centers != null) try addMembership(scratch, &identity_changes, entry.doc_id, hash);
        const shard = try findShard(a, cache.store(), root.directory, hash);
        try affected.put(scratch, try scratch.dupe(u8, &shard.start), shard);
        const slot = try ordinals.getOrPut(scratch, entry.doc_id);
        if (!slot.found_existing) slot.value_ptr.* = 0;
        const key = try clusterKey(scratch, hash, slot.value_ptr.*);
        slot.value_ptr.* += 1;
        try projections.put(scratch, key, try vectorValue(scratch, entry));
    }
    root.docs = try common.applyMap(a, scratch, cache.store(), root.docs, projections);
    if (root.format == 2) root.postings = try common.applyMap(a, scratch, cache.store(), root.postings, identity_changes);
    if (root.docs == null) {
        root.directory = null;
        root.centroids = null;
        root.dims = 0;
        root.postings = null;
        root.global_centers = null;
        root.format = 1;
        return common.publish(a, pages, root);
    }
    const previous_format = root.format;
    try trainGlobal(a, scratch, &text_pages, cache.store(), &root, maintenance);
    if (root.format != previous_format) {
        affected.clearRetainingCapacity();
        for (0..@intCast(root.global_centers.?.records)) |i| {
            var start: [32]u8 = @splat(0);
            std.mem.writeInt(u16, start[0..2], @intCast(i), .big);
            try affected.put(scratch, try scratch.dupe(u8, &start), .{ .start = start, .bits = 16 });
        }
    }
    var directories = std.StringHashMapUnmanaged(?[]const u8).empty;
    var centroids = std.StringHashMapUnmanaged(?[]const u8).empty;
    var work = Rebuild{ .a = a, .scratch = scratch, .pages = pages, .store = cache.store(), .root = &root, .directories = &directories, .centroids = &centroids, .maintenance = maintenance };
    var it = affected.valueIterator();
    while (it.next()) |shard| {
        for (0..shard.clusters) |ordinal| try centroids.put(scratch, try clusterKey(scratch, shard.start, @intCast(ordinal)), null);
        try directories.put(scratch, try scratch.dupe(u8, &shard.start), null);
        try work.run(shard.*);
    }
    root.directory = try common.applyMap(a, scratch, cache.store(), root.directory, directories);
    root.centroids = try common.applyMap(a, scratch, cache.store(), root.centroids, centroids);
    return common.publish(a, pages, root);
}

const Rebuild = struct {
    a: A,
    scratch: A,
    pages: *PageStore,
    store: tree.Store,
    root: *common.Root,
    directories: *std.StringHashMapUnmanaged(?[]const u8),
    centroids: *std.StringHashMapUnmanaged(?[]const u8),
    maintenance: ?cancellation.Token,
    fn run(self: *@This(), original: Shard) anyerror!void {
        var entries = std.ArrayList(segment.Entry).empty;
        defer {
            for (entries.items) |*entry| entry.deinit(self.a);
            entries.deinit(self.a);
        }
        const upper = try rangeEnd(self.a, original);
        defer if (upper) |value| self.a.free(value);
        var cursor = try tree.Cursor.init(self.a, self.store, self.root.docs, &original.start, upper);
        defer cursor.deinit();
        var bytes: usize = 0;
        var first_hash: ?[32]u8 = null;
        var distinct = false;
        var split = false;
        while (try cursor.next()) |record| {
            try self.pages.cancellation.check();
            if (record.key.len != 36) return error.InvalidVectorValue;
            var hash: [32]u8 = undefined;
            @memcpy(&hash, record.key[0..32]);
            if (first_hash) |first| {
                if (!std.mem.eql(u8, &first, &hash)) distinct = true;
            } else first_hash = hash;
            bytes = try std.math.add(usize, bytes, record.value.len);
            if (bytes > max_shard_bytes or (entries.items.len >= max_shard_entries and distinct)) {
                split = true;
                break;
            }
            if (entries.items.len >= 8192) return error.VectorDocumentBudgetExceeded;
            var entry = try decodeVector(self.a, record.value, self.root.dims);
            errdefer entry.deinit(self.a);
            try entries.append(self.a, entry);
        }
        if (split) {
            if (original.bits == 256 or !distinct) return error.VectorDocumentBudgetExceeded;
            // Release projection values before descending into bounded children.
            for (entries.items) |*entry| entry.deinit(self.a);
            entries.clearRetainingCapacity();
            for (0..16) |nibble| {
                var child: Shard = .{ .start = original.start, .bits = original.bits + 4 };
                const index: usize = original.bits / 8;
                child.start[index] |= @as(u8, @intCast(nibble)) << @intCast(if (original.bits % 8 == 0) @as(u16, 4) else 0);
                try self.run(child);
            }
            return;
        }
        var shard = original;
        shard.clusters = 0;
        shard.entries = @intCast(entries.items.len);
        if (entries.items.len != 0) {
            const owned = try entries.toOwnedSlice(self.a);
            var built = try build.buildClusteredSegmentWithPolicyAllocUntil(self.a, self.root.metric, self.root.dims, owned, .{ .target_cluster_count = @min(@as(usize, 64), (owned.len + 127) / 128), .base_probe_count = self.root.base_probe_count, .shortlist_multiplier = self.root.shortlist_multiplier }, self.maintenance);
            defer built.deinit(self.a);
            shard.clusters = @intCast(built.clusters.len);
            var offset: usize = 0;
            while (offset < built.clusters.len) {
                const end = @min(built.clusters.len, offset + 8);
                var contents: [8][]const u8 = undefined;
                var encoded: usize = 0;
                defer for (contents[0..encoded]) |payload| self.a.free(payload);
                for (built.clusters[offset..end], contents[0 .. end - offset]) |cluster, *payload| {
                    try self.pages.cancellation.check();
                    var local = [_]segment.Cluster{cluster};
                    local[0].start_index = 0;
                    const start: usize = cluster.start_index;
                    payload.* = try segment.encodeAlloc(self.a, .{ .dims = self.root.dims, .metric = self.root.metric, .base_probe_count = self.root.base_probe_count, .shortlist_multiplier = self.root.shortlist_multiplier, .clusters = &local, .entries = built.entries[start..][0..cluster.entry_count] });
                    encoded += 1;
                    if (payload.len > self.pages.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
                    self.pages.remaining_write_bytes.* -= payload.len;
                }
                var backend = self.pages.artifacts.*;
                const scope = @import("../artifacts/store.zig").UploadScope{ .domain = self.pages.domain, .attempt = self.pages.attempt };
                if (backend.upload_scope) |authority| {
                    if (!std.mem.eql(u8, &authority.domain, &scope.domain) or authority.fencingToken() != scope.fencingToken()) return error.InvalidArtifactUploadScope;
                }
                backend.upload_scope = scope;
                var results: [8]@import("../artifacts/store.zig").ArtifactMetadata = undefined;
                try backend.putBatchWithCancellation(null, contents[0..encoded], results[0..encoded], self.pages.cancellation);
                defer for (results[0..encoded]) |*metadata| metadata.deinit(backend.allocator);
                for (built.clusters[offset..end], results[0..encoded], offset..) |cluster, uploaded, ordinal| {
                    const blob = try common.Blob.fromMetadata(uploaded, self.pages.attempt);
                    const value = try self.scratch.alloc(u8, 72 + @as(usize, self.root.dims) * 4);
                    @memcpy(value[0..56], &blob.encode());
                    std.mem.writeInt(u32, value[56..60], cluster.entry_count, .little);
                    std.mem.writeInt(u32, value[60..64], @bitCast(cluster.routing_distance_min), .little);
                    std.mem.writeInt(u32, value[64..68], @bitCast(cluster.routing_distance_max), .little);
                    std.mem.writeInt(u32, value[68..72], @bitCast(cluster.routing_distance_avg), .little);
                    for (cluster.centroid, 0..) |number, i| std.mem.writeInt(u32, value[72 + i * 4 ..][0..4], @bitCast(number), .little);
                    try self.centroids.put(self.scratch, try clusterKey(self.scratch, shard.start, @intCast(ordinal)), value);
                }
                offset = end;
            }
        }
        try self.directories.put(self.scratch, try self.scratch.dupe(u8, &shard.start), try self.scratch.dupe(u8, &shard.encode()));
    }
};

pub fn searchAlloc(a: A, session: *runtime.QuerySession, ref: manifest.ArtifactRef, req: request.QueryRequest, stats: *indexed.SearchExecutionStats) ![]indexed.ScoredDoc {
    const query = req.vector orelse return error.VectorQueryRequired;
    var reads: u64 = 32 * 1024 * 1024;
    var writes: u64 = 0;
    var pages = PageStore{ .domain = PageStore.namespaceDomain(session.manifest.namespace), .artifacts = session.artifacts, .cancellation = session.cancellation, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const parsed = try common.load(a, &pages, ref);
    defer parsed.deinit();
    const root = parsed.value;
    if (root.docs == null or req.limit == 0) return a.alloc(indexed.ScoredDoc, 0);
    if (query.len != root.dims) return error.VectorDimsMismatch;
    const normalized = try indexed.normalizedCosineQueryAlloc(a, root.metric, query);
    defer if (root.metric == .cosine) a.free(@constCast(normalized));
    const measure = indexed.vectorQueryMeasure(normalized, root.metric);
    var cache = tree.Cache{ .alloc = a, .underlying = pages.store() };
    defer cache.deinit();
    const Route = struct { blob: common.Blob, entries: u32, coarse: u16 };
    var routes = std.ArrayList(Route).empty;
    defer routes.deinit(a);
    var ranked = std.ArrayList(indexed.ScoredCluster).empty;
    defer ranked.deinit(a);
    const center = try a.alloc(f32, root.dims);
    defer a.free(center);
    if (req.coarse_probes > 64) return error.InvalidVectorQuery;
    const coarse_centers = if (root.format == 2) try loadCenters(a, cache.store(), root) else null;
    defer if (coarse_centers) |values| freeCenters(a, values);
    var selected_cells: [64]u16 = undefined;
    var cell_count: usize = 0;
    if (coarse_centers) |centers| {
        // Full effort always retains the exact control. Zero preserves legacy
        // all-route ordering; explicit budgets qualify coarse-first I/O/recall.
        if (req.coarse_probes != 0 and req.coarse_probes < centers.len and (req.search_effort == null or req.search_effort.? < 1)) {
            var cells: [64]indexed.ScoredCluster = undefined;
            for (centers, 0..) |value, index| cells[index] = .{ .cluster_index = index, .score = indexed.routingScoreForQuery(normalized, measure, value, root.metric) };
            std.mem.sort(indexed.ScoredCluster, cells[0..centers.len], {}, indexed.lessScoredCluster);
            cell_count = req.coarse_probes;
            for (cells[0..cell_count], selected_cells[0..cell_count]) |hit, *cell| cell.* = @intCast(hit.cluster_index);
        }
    }
    for (0..@max(cell_count, 1)) |cell_index| {
        var lower: [2]u8 = undefined;
        if (cell_count != 0) std.mem.writeInt(u16, &lower, selected_cells[cell_index], .big);
        const upper = if (cell_count != 0) try common.upperBound(a, &lower) else null;
        defer if (upper) |value| a.free(value);
        var cursor = try tree.Cursor.init(a, cache.store(), root.centroids, if (cell_count != 0) &lower else "", upper);
        defer cursor.deinit();
        while (try cursor.next()) |row| {
            try session.checkCancellation();
            if (row.value.len != 72 + @as(usize, root.dims) * 4) return error.InvalidPagedProjectionRoute;
            for (center, 0..) |*number, i| {
                number.* = @bitCast(std.mem.readInt(u32, row.value[72 + i * 4 ..][0..4], .little));
                if (!std.math.isFinite(number.*)) return error.InvalidPagedProjectionRoute;
            }
            var score = indexed.routingScoreForQuery(normalized, measure, center, root.metric);
            if (root.format == 2 and root.metric == .cosine) {
                const average: f32 = @bitCast(std.mem.readInt(u32, row.value[68..72], .little));
                if (!std.math.isFinite(average)) return error.InvalidPagedProjectionRoute;
                score = cosinePriority(score, average, std.mem.readInt(u32, row.value[56..60], .little), root.dims);
            }
            if (!std.math.isFinite(score)) return error.InvalidVectorQuery;
            try ranked.append(a, .{ .cluster_index = routes.items.len, .score = score });
            if (row.key.len != 36) return error.InvalidPagedProjectionRoute;
            try routes.append(a, .{ .blob = try common.Blob.decode(row.value), .entries = std.mem.readInt(u32, row.value[56..60], .little), .coarse = std.mem.readInt(u16, row.key[0..2], .big) });
        }
    }
    if ((cell_count == 0 and routes.items.len != (if (root.centroids) |r| r.records else 0)) or routes.items.len > (if (root.centroids) |r| r.records else 0)) return error.InvalidPagedProjectionRoot;
    std.mem.sort(indexed.ScoredCluster, ranked.items, {}, indexed.lessScoredCluster);
    const hinted_probes = indexed.effectiveProbeCount(req, root.base_probe_count, ranked.items);
    // Frozen coarse cells introduce boundary uncertainty in incremental roots.
    // Budget a 25% routing margin, capped by the available routes. num_probes
    // remains a hint, as in the native adaptive probing policy. Full effort is
    // unchanged and I/O/deadline budgets still bound admission.
    const probes = if (root.format == 2) @min(ranked.items.len, hinted_probes + (hinted_probes + 3) / 4) else hinted_probes;
    var selected = std.ArrayList(indexed.ScoredCluster).empty;
    defer selected.deinit(a);
    if (root.format == 2) {
        const coarse = coarse_centers.?;
        const primary = nearestCenter(normalized, coarse, root.metric);
        const visited = try a.alloc(bool, routes.items.len);
        defer a.free(visited);
        @memset(visited, false);
        // The nearest coarse cell owns an identical query vector's membership.
        // Visit its best leaves first, then spend the remaining budget globally.
        for (ranked.items) |hit| {
            if (selected.items.len == probes) break;
            if (routes.items[hit.cluster_index].coarse != primary) continue;
            visited[hit.cluster_index] = true;
            try selected.append(a, hit);
        }
        for (ranked.items) |hit| {
            if (selected.items.len == probes) break;
            if (!visited[hit.cluster_index]) try selected.append(a, hit);
        }
    } else try selected.appendSlice(a, ranked.items[0..probes]);
    const Value = struct { score: u32, distance: ?f32 };
    var scores = std.StringHashMapUnmanaged(Value).empty;
    defer {
        var it = scores.keyIterator();
        while (it.next()) |key| a.free(key.*);
        scores.deinit(a);
    }
    for (selected.items) |hit| {
        try session.checkCancellation();
        const route = routes.items[hit.cluster_index];
        if (route.blob.bytes > reads) return error.ArtifactReadBudgetExceeded;
        reads -= route.blob.bytes;
        const id = try route.blob.identity(pages.domain);
        const payload = try session.artifacts.getVerifiedAllocWithCancellationUsingAllocator(a, &id, route.blob.bytes, &std.fmt.bytesToHex(route.blob.digest, .lower), session.cancellation);
        defer a.free(payload);
        var decoded = try segment.decodeAlloc(a, payload);
        defer decoded.deinit(a);
        if (decoded.dims != root.dims or decoded.metric != root.metric or decoded.clusters.len != 1 or decoded.entries.len != route.entries) return error.InvalidPagedProjectionRoute;
        var local: indexed.SearchExecutionStats = .{};
        const hits = try indexed.searchVectorSegmentDocIdsWithCancellationAlloc(a, decoded, query, 0, req.offset +| req.limit, req.min_score, 1, 1, &local, session.cancellation);
        defer indexed.freeScoredDocs(a, hits);
        inline for (comptime std.meta.fieldNames(indexed.SearchExecutionStats)) |field| @field(stats, field) += @field(local, field);
        for (hits) |value| {
            const slot = try scores.getOrPut(a, value.doc_id);
            if (!slot.found_existing) {
                errdefer _ = scores.remove(value.doc_id);
                slot.key_ptr.* = try a.dupe(u8, value.doc_id);
                slot.value_ptr.* = .{ .score = value.score, .distance = value.distance };
            } else if (value.score > slot.value_ptr.score or (value.score == slot.value_ptr.score and value.distance != null and slot.value_ptr.distance != null and value.distance.? < slot.value_ptr.distance.?)) slot.value_ptr.* = .{ .score = value.score, .distance = value.distance };
        }
    }
    var top = @import("../query/top_docs.zig").TopDocs{ .capacity = req.offset +| req.limit };
    defer top.deinit(a);
    var it = scores.iterator();
    while (it.next()) |entry| try top.add(a, .{ .doc_id = entry.key_ptr.*, .score = entry.value_ptr.score, .distance = entry.value_ptr.distance });
    const result = top.sorted();
    const start = @min(req.offset, result.len);
    const end = @min(result.len, start +| req.limit);
    const output = try a.alloc(indexed.ScoredDoc, end - start);
    var initialized: usize = 0;
    errdefer {
        for (output[0..initialized]) |row| a.free(row.doc_id);
        a.free(output);
    }
    for (result[start..end], output) |value, *row| {
        row.* = .{ .doc_id = try a.dupe(u8, value.doc_id), .score = value.score, .distance = value.distance };
        initialized += 1;
    }
    return output;
}

test "serverless paged vectors preserve exhaustive native scores and immutable generations" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/projections", .{tmp.sub_path});
    defer a.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var store = fs.artifactStore();
    defer store.deinit();
    var reads: u64 = 32 * 1024 * 1024;
    var writes: u64 = 32 * 1024 * 1024;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    const builder = @import("../build/builder.zig");
    var session = runtime.QuerySession{ .alloc = a, .artifacts = &store, .owns_manifest = false, .manifest = .{ .namespace = "docs", .version = 1, .built_at_ns = 1, .wal_start_lsn = 1, .wal_end_lsn = 3, .stats = .{ .document_count = 3 }, .artifacts = &.{} } };

    const entries = [_]segment.Entry{
        .{ .doc_id = @constCast("a"), .vector = @constCast(&[_]f32{ 1, 0, 0 }) },
        .{ .doc_id = @constCast("b"), .vector = @constCast(&[_]f32{ 0, 1, 0 }) },
        .{ .doc_id = @constCast("a"), .vector = @constCast(&[_]f32{ 0, 0, 1 }) },
    };
    const ref = try publishAlloc(a, &pages, null, &entries, &.{}, "dense", @splat(1), .cosine, null);
    defer builder.freeArtifactRef(a, ref);
    var stats: indexed.SearchExecutionStats = .{};
    const query = [_]f32{ 1, 0, 0 };
    const owned = try a.alloc(segment.Entry, entries.len);
    for (entries, owned) |entry, *copy| copy.* = .{ .doc_id = try a.dupe(u8, entry.doc_id), .vector = try a.dupe(f32, entry.vector) };
    var flat = try build.buildClusteredSegmentWithPolicyAllocUntil(a, .cosine, 3, owned, .{ .target_cluster_count = 1 }, null);
    defer flat.deinit(a);
    const expected = try indexed.searchVectorSegmentDocIdsWithCancellationAlloc(a, flat, &query, 0, 10, 0, 256, 1, &stats, .none);
    defer indexed.freeScoredDocs(a, expected);
    const actual = try searchAlloc(a, &session, ref, .{ .text = &.{}, .vector = @constCast(&query), .limit = 10, .search_effort = 1 }, &stats);
    defer indexed.freeScoredDocs(a, actual);
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |l, r| {
        try std.testing.expectEqualStrings(l.doc_id, r.doc_id);
        try std.testing.expectEqual(l.score, r.score);
    }
    pages.attempt = @splat(2);
    const empty = try publishAlloc(a, &pages, ref, &.{}, &.{ "a", "b" }, "dense", @splat(1), .cosine, null);
    defer builder.freeArtifactRef(a, empty);
    const fresh = try searchAlloc(a, &session, empty, .{ .text = &.{}, .vector = @constCast(&query), .limit = 10 }, &stats);
    defer indexed.freeScoredDocs(a, fresh);
    try std.testing.expectEqual(@as(usize, 0), fresh.len);
    const old = try searchAlloc(a, &session, ref, .{ .text = &.{}, .vector = @constCast(&query), .limit = 10 }, &stats);
    defer indexed.freeScoredDocs(a, old);
    try std.testing.expectEqual(expected.len, old.len);
    try std.testing.expectError(error.VectorIndexDefinitionChanged, publishAlloc(a, &pages, ref, &.{}, &.{}, "dense", @splat(9), .cosine, null));
}

test "serverless global vector routing retains immutable memberships across moves and multi-projection deletes" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/global", .{tmp.sub_path});
    defer a.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var store = fs.artifactStore();
    defer store.deinit();
    var reads: u64 = 32 * 1024 * 1024;
    var writes: u64 = 32 * 1024 * 1024;
    var pages = PageStore{ .domain = PageStore.namespaceDomain("docs"), .attempt = @splat(1), .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var entries: [2048]segment.Entry = undefined;
    var values: [2048][16]f32 = undefined;
    var ids: [2048][16]u8 = undefined;
    var seed: u32 = 17;
    for (&entries, &values, &ids, 0..) |*entry, *vector, *id, i| {
        for (vector) |*value| {
            seed = seed *% 1664525 +% 1013904223;
            value.* = @as(f32, @floatFromInt(seed >> 8)) / 8388608.0 - 1;
        }
        entry.* = .{ .doc_id = try std.fmt.bufPrint(id, "seed/{d}", .{i}), .vector = vector };
    }
    const builder = @import("../build/builder.zig");
    const first = try publishAlloc(a, &pages, null, &entries, &.{}, "dense", @splat(1), .cosine, null);
    defer builder.freeArtifactRef(a, first);
    const root = try common.load(a, &pages, first);
    defer root.deinit();
    try std.testing.expectEqual(@as(u8, 2), root.value.format);
    try std.testing.expect(root.value.global_centers != null);
    try std.testing.expectEqual(@as(u64, 2048), root.value.postings.?.records);
    // Collection must retain the new routing tree as well as document identity
    // memberships; legacy collectors cannot discover this format's new root.
    const collection = @import("../hosted_collection.zig");
    const context = collection.Context{ .alloc = a, .namespace = "docs", .prefix = "antfly", .artifacts = &store, .manifests = undefined, .progress = undefined, .wal = undefined, .io = undefined };
    const traced = try context.trace(.{ .kind = .projection_root, .artifact = .{ .kind = first.kind, .metadata_version = first.metadata_version, .artifact_id = first.artifact_id, .byte_len = first.byte_len, .checksum = first.checksum } });
    defer a.free(traced);
    const children = try std.json.parseFromSlice(struct { children: []collection.Task }, a, traced, .{ .ignore_unknown_fields = true });
    defer children.deinit();
    var retained_centers = false;
    for (children.value.children) |child| if (child.page) |page| {
        if (std.meta.eql(page, root.value.global_centers.?)) retained_centers = true;
    };
    try std.testing.expect(retained_centers);

    var positive: [16]f32 = @splat(0);
    positive[0] = 1;
    var negative = positive;
    negative[0] = -1;
    const pair = [_]segment.Entry{ .{ .doc_id = @constCast("moving"), .vector = &positive }, .{ .doc_id = @constCast("moving"), .vector = &negative } };
    pages.attempt = @splat(2);
    const second = try publishAlloc(a, &pages, first, &pair, &.{}, "dense", @splat(1), .cosine, null);
    defer builder.freeArtifactRef(a, second);
    const second_root = try common.load(a, &pages, second);
    defer second_root.deinit();
    try std.testing.expectEqual(@as(u64, 2050), second_root.value.docs.?.records);
    pages.attempt = @splat(3);
    const moved = try publishAlloc(a, &pages, second, pair[0..1], &.{}, "dense", @splat(1), .cosine, null);
    defer builder.freeArtifactRef(a, moved);
    const moved_root = try common.load(a, &pages, moved);
    defer moved_root.deinit();
    try std.testing.expectEqual(@as(u64, 2049), moved_root.value.docs.?.records);
    pages.attempt = @splat(4);
    const deleted = try publishAlloc(a, &pages, moved, &.{}, &.{"moving"}, "dense", @splat(1), .cosine, null);
    defer builder.freeArtifactRef(a, deleted);
    const deleted_root = try common.load(a, &pages, deleted);
    defer deleted_root.deinit();
    try std.testing.expectEqual(@as(u64, 2048), deleted_root.value.docs.?.records);
    try std.testing.expectEqual(@as(u64, 2048), deleted_root.value.postings.?.records);
    var session = runtime.QuerySession{ .alloc = a, .artifacts = &store, .owns_manifest = false, .manifest = .{ .namespace = "docs", .version = 1, .built_at_ns = 1, .wal_start_lsn = 1, .wal_end_lsn = 2048, .stats = .{ .document_count = 2048 }, .artifacts = &.{} } };
    var stats: indexed.SearchExecutionStats = .{};
    const old_hits = try searchAlloc(a, &session, second, .{ .text = &.{}, .vector = &negative, .limit = 1, .search_effort = 1 }, &stats);
    defer indexed.freeScoredDocs(a, old_hits);
    try std.testing.expectEqualStrings("moving", old_hits[0].doc_id);
    const coarse_hits = try searchAlloc(a, &session, second, .{ .text = &.{}, .vector = &negative, .limit = 1, .num_probes = 2, .coarse_probes = 1 }, &stats);
    defer indexed.freeScoredDocs(a, coarse_hits);
    try std.testing.expectEqualStrings("moving", coarse_hits[0].doc_id);
    const fresh_hits = try searchAlloc(a, &session, deleted, .{ .text = &.{}, .vector = &negative, .limit = 10, .search_effort = 1 }, &stats);
    defer indexed.freeScoredDocs(a, fresh_hits);
    for (fresh_hits) |hit| try std.testing.expect(!std.mem.eql(u8, "moving", hit.doc_id));
}
