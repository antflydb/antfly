// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const hbc = @import("hbc.zig");
const hbc_runtime = @import("hbc_runtime.zig");
const types = @import("types.zig");
const vec = @import("antfly_vector").vector;

pub const VectorId = u64;
pub const PostingId = u64;

pub const PostingView = struct {
    id: PostingId,
    parent: PostingId,
    level: u16,
    centroid: []const f32,
    members: []const VectorId,
    state: types.PostingState,

    pub fn usesNonQuantizedPayload(self: PostingView) bool {
        return self.parent == 0;
    }

    pub fn hasFreshStoredPayload(self: PostingView) bool {
        return !self.state.payload_dirty;
    }
};

pub const PostingState = types.PostingState;

pub const PostingMaintenanceOptions = struct {
    max_postings: usize = std.math.maxInt(usize),
    refresh_payloads: bool = true,
    refresh_ancestors: bool = true,
    rebalance_layout: bool = false,
    max_layout_changes: usize = std.math.maxInt(usize),
    max_boundary_reassignments: usize = 0,
    boundary_reassignment_min_improvement: f32 = 0.0,
};

pub const PostingMaintenanceResult = struct {
    scanned_nodes: u64 = 0,
    scanned_postings: u64 = 0,
    dirty_postings: u64 = 0,
    repaired_postings: u64 = 0,
    centroid_refreshed: u64 = 0,
    payload_refreshed: u64 = 0,
    ancestor_refresh_roots: u64 = 0,
    split_postings: u64 = 0,
    merged_postings: u64 = 0,
    boundary_reassigned_vectors: u64 = 0,
    skipped_missing: u64 = 0,
    limit_reached: bool = false,
};

pub const PostingBacklogStats = struct {
    scanned_nodes: u64 = 0,
    scanned_postings: u64 = 0,
    dirty_postings: u64 = 0,
    centroid_dirty_postings: u64 = 0,
    payload_dirty_postings: u64 = 0,
    max_centroid_version_lag: u64 = 0,
    max_payload_version_lag: u64 = 0,
    max_mutation_version: u64 = 0,
    skipped_missing: u64 = 0,

    pub fn needsRepair(self: PostingBacklogStats) bool {
        return self.dirty_postings != 0;
    }

    pub fn write(self: PostingBacklogStats, writer: *std.Io.Writer) !void {
        try writer.print(
            "posting_backlog scanned_nodes={d} scanned_postings={d} dirty_postings={d} centroid_dirty_postings={d} payload_dirty_postings={d} max_centroid_version_lag={d} max_payload_version_lag={d} max_mutation_version={d} skipped_missing={d}\n",
            .{
                self.scanned_nodes,
                self.scanned_postings,
                self.dirty_postings,
                self.centroid_dirty_postings,
                self.payload_dirty_postings,
                self.max_centroid_version_lag,
                self.max_payload_version_lag,
                self.max_mutation_version,
                self.skipped_missing,
            },
        );
    }
};

pub const PostingStore = struct {
    pub fn view(node: *const types.Node) !PostingView {
        if (!node.is_leaf) return error.ExpectedLeaf;
        return .{
            .id = node.id,
            .parent = node.parent,
            .level = node.level,
            .centroid = node.centroid,
            .members = node.members,
            .state = node.posting_state,
        };
    }

    pub fn copyMemberIds(
        alloc: std.mem.Allocator,
        scratch: anytype,
        posting: PostingView,
    ) ![]VectorId {
        try scratch.ensureMemberIdCapacity(alloc, posting.members.len);
        const member_ids = scratch.member_ids[0..posting.members.len];
        @memcpy(member_ids, posting.members);
        return member_ids;
    }

    pub fn appendMember(
        alloc: std.mem.Allocator,
        node: *types.Node,
        vector_id: VectorId,
    ) !usize {
        return appendMembers(alloc, node, &.{vector_id});
    }

    pub fn appendMembers(
        alloc: std.mem.Allocator,
        node: *types.Node,
        vector_ids: []const VectorId,
    ) !usize {
        if (!node.is_leaf) return error.ExpectedLeaf;
        const old_len = node.members.len;
        if (vector_ids.len == 0) return old_len;
        node.members = if (old_len == 0)
            try alloc.alloc(u64, vector_ids.len)
        else
            try alloc.realloc(node.members, old_len + vector_ids.len);
        @memcpy(node.members[old_len..][0..vector_ids.len], vector_ids);
        noteMembersChanged(node);
        return old_len;
    }

    pub fn removeMember(
        alloc: std.mem.Allocator,
        node: *types.Node,
        vector_id: VectorId,
    ) !void {
        if (!node.is_leaf) return error.ExpectedLeaf;
        const found_index = indexOfMember(node.members, vector_id) orelse return error.NotFound;
        const new_len = node.members.len - 1;
        if (new_len == 0) {
            if (node.members.len > 0) alloc.free(node.members);
            node.members = &.{};
            noteMembersChanged(node);
            return;
        }

        var new_members = try alloc.alloc(u64, new_len);
        errdefer alloc.free(new_members);
        @memcpy(new_members[0..found_index], node.members[0..found_index]);
        @memcpy(new_members[found_index..], node.members[found_index + 1 ..]);
        alloc.free(node.members);
        node.members = new_members;
        noteMembersChanged(node);
    }

    pub fn removeMembers(
        alloc: std.mem.Allocator,
        node: *types.Node,
        vector_ids: []const VectorId,
    ) !usize {
        if (!node.is_leaf) return error.ExpectedLeaf;
        if (vector_ids.len == 0 or node.members.len == 0) return 0;

        var kept = try alloc.alloc(u64, node.members.len);
        errdefer alloc.free(kept);
        var kept_count: usize = 0;
        var removed_count: usize = 0;
        for (node.members) |member_id| {
            if (containsMember(vector_ids, member_id)) {
                removed_count += 1;
            } else {
                kept[kept_count] = member_id;
                kept_count += 1;
            }
        }

        if (removed_count == 0) {
            alloc.free(kept);
            return 0;
        }

        if (kept_count == 0) {
            alloc.free(kept);
            alloc.free(node.members);
            node.members = &.{};
            noteMembersChanged(node);
            return removed_count;
        }

        const new_members = try alloc.realloc(kept, kept_count);
        alloc.free(node.members);
        node.members = new_members;
        noteMembersChanged(node);
        return removed_count;
    }

    pub fn noteMembersChanged(node: *types.Node) void {
        noteVectorsChanged(node);
    }

    pub fn noteVectorsChanged(node: *types.Node) void {
        if (!node.is_leaf) return;
        node.posting_state.noteMembersChanged(node.members.len);
    }

    pub fn noteCentroidRefreshed(node: *types.Node) void {
        if (!node.is_leaf) return;
        node.posting_state.noteCentroidRefreshed();
    }

    pub fn notePayloadRefreshed(node: *types.Node) void {
        if (!node.is_leaf) return;
        node.posting_state.notePayloadRefreshed();
    }

    pub fn loadState(index: anytype, txn: anytype, posting_id: PostingId, is_not_found: fn (anyerror) bool) !PostingState {
        var key_buf: [12]u8 = undefined;
        const data = index.getNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, posting_id, .posting)) catch |err| {
            if (is_not_found(err)) return .{};
            return err;
        };
        return try decodeState(data);
    }

    pub fn saveState(index: anytype, txn: anytype, posting_id: PostingId, state: PostingState) !void {
        var key_buf: [12]u8 = undefined;
        var buf: [state_encoded_size]u8 = undefined;
        try index.putNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, posting_id, .posting), encodeState(state, &buf));
    }

    pub fn deleteState(index: anytype, txn: anytype, posting_id: PostingId) !void {
        var key_buf: [12]u8 = undefined;
        try index.deleteNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, posting_id, .posting));
    }

    pub fn recomputeCentroid(index: anytype, txn: anytype, node: *types.Node) !void {
        if (!node.is_leaf) return error.ExpectedLeaf;
        if (node.members.len == 0) {
            @memset(node.centroid, 0);
            noteCentroidRefreshed(node);
            return;
        }

        index.write_profile.centroid_recompute_calls += 1;
        index.write_profile.centroid_recompute_members_total += @intCast(node.members.len);
        if (node.members.len > index.write_profile.centroid_recompute_members_max) {
            index.write_profile.centroid_recompute_members_max = @intCast(node.members.len);
            if (indexHasExternalVectorLoader(index) and node.members.len > index.config.max_cached_vectors) {
                std.log.warn(
                    "hbc centroid recompute external posting_members={} max_cached_vectors={} active_count={} node_count={}",
                    .{ node.members.len, index.config.max_cached_vectors, index.metadata.active_count, index.metadata.node_count },
                );
            }
        }

        if (node.centroid.len != index.config.dims) {
            if (node.centroid.len > 0) index.alloc.free(node.centroid);
            node.centroid = try index.alloc.alloc(f32, index.config.dims);
        }
        @memset(node.centroid, 0);

        const vector_scratch = try index.alloc.alloc(f32, index.config.dims);
        defer index.alloc.free(vector_scratch);
        const transformed = try index.alloc.alloc(f32, index.config.dims);
        defer index.alloc.free(transformed);

        for (node.members) |member_id| {
            const v = try index.getVectorScratch(txn, member_id, vector_scratch);
            _ = index.transformVector(v, transformed);
            vec.add(node.centroid, transformed);
        }
        vec.scale(1.0 / @as(f32, @floatFromInt(node.members.len)), node.centroid);
        normalizeCentroidForMetric(index, node.centroid);
        noteCentroidRefreshed(node);
    }

    pub fn loadTransformedVectorsForQuantizedRefresh(
        index: anytype,
        txn: anytype,
        node: *const types.Node,
        vectors: []f32,
        options: anytype,
    ) !void {
        if (!node.is_leaf) return error.ExpectedLeaf;
        const dims: usize = @intCast(index.metadata.dims);
        if (vectors.len < node.members.len * dims) return error.BufferTooSmall;

        const raw_scratch = try index.alloc.alloc(f32, dims);
        defer index.alloc.free(raw_scratch);
        const transformed_scratch = try index.alloc.alloc(f32, dims);
        defer index.alloc.free(transformed_scratch);

        for (node.members, 0..) |member_id, i| {
            const raw_v = try getBatchVectorScratch(index, txn, member_id, raw_scratch, options);
            const transformed = index.transformVector(raw_v, transformed_scratch);
            @memcpy(std.mem.sliceAsBytes(vectors[i * dims ..][0..dims]), std.mem.sliceAsBytes(transformed));
        }
    }

    pub fn refreshQuantizedPayload(
        index: anytype,
        txn: anytype,
        node: *const types.Node,
        vectors: []const f32,
        now_fn: fn () u64,
        elapsed_fn: fn (u64) u64,
    ) !void {
        const posting = try view(node);
        const count = posting.members.len;
        const dims: usize = @intCast(index.metadata.dims);
        if (vectors.len < count * dims) return error.BufferTooSmall;

        if (index.getCachedQuantizedPtr(posting.id)) |cached| {
            switch (cached.*) {
                .nonquant => |*set| {
                    if (!posting.usesNonQuantizedPayload()) {
                        const compute_start = now_fn();
                        var fresh: hbc_runtime.QuantizedSet = .{ .rabit = try index.quantizer.quantize(posting.centroid, vectors, count) };
                        index.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
                        defer fresh.deinit(index.alloc);
                        const store_start = now_fn();
                        try index.saveQuantized(txn, posting.id, &fresh);
                        index.write_profile.quantized_store_ns += elapsed_fn(store_start);
                        return;
                    }
                    set.vectors.dims = @intCast(dims);
                    set.vectors.count = @intCast(count);
                    if (set.vectors.data.len == 0) {
                        set.vectors.data = try index.alloc.alloc(f32, count * dims);
                    } else {
                        set.vectors.data = try index.alloc.realloc(set.vectors.data, count * dims);
                    }
                    @memcpy(set.vectors.data, vectors[0 .. count * dims]);
                    noteMutatedCachedQuantized(index, posting.id);
                    const store_start = now_fn();
                    try index.putQuantizedCached(txn, posting.id, cached);
                    index.write_profile.quantized_store_ns += elapsed_fn(store_start);
                    return;
                },
                .rabit => |*set| {
                    if (posting.usesNonQuantizedPayload()) {
                        const compute_start = now_fn();
                        var fresh: hbc_runtime.QuantizedSet = .{ .nonquant = .{
                            .vectors = .{
                                .dims = @intCast(dims),
                                .count = @intCast(count),
                                .data = try index.alloc.dupe(f32, vectors[0 .. count * dims]),
                            },
                        } };
                        index.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
                        defer fresh.deinit(index.alloc);
                        const store_start = now_fn();
                        try index.saveQuantized(txn, posting.id, &fresh);
                        index.write_profile.quantized_store_ns += elapsed_fn(store_start);
                        return;
                    }
                    const compute_start = now_fn();
                    try index.quantizer.quantizeInto(set, posting.centroid, vectors, count);
                    index.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
                    noteMutatedCachedQuantized(index, posting.id);
                    const store_start = now_fn();
                    try index.putQuantizedCached(txn, posting.id, cached);
                    index.write_profile.quantized_store_ns += elapsed_fn(store_start);
                    return;
                },
            }
        }

        const compute_start = now_fn();
        var qs: hbc_runtime.QuantizedSet = if (posting.usesNonQuantizedPayload())
            .{ .nonquant = .{
                .vectors = .{
                    .dims = @intCast(dims),
                    .count = @intCast(count),
                    .data = try index.alloc.dupe(f32, vectors[0 .. count * dims]),
                },
            } }
        else
            .{ .rabit = try index.quantizer.quantize(posting.centroid, vectors, count) };
        index.write_profile.quantized_compute_ns += elapsed_fn(compute_start);
        defer qs.deinit(index.alloc);
        const store_start = now_fn();
        try index.saveQuantized(txn, posting.id, &qs);
        index.write_profile.quantized_store_ns += elapsed_fn(store_start);
    }
};

const state_format_version: u8 = 1;
const state_flag_dirty: u8 = 1 << 0;
const state_flag_centroid_dirty: u8 = 1 << 1;
const state_flag_payload_dirty: u8 = 1 << 2;
const state_encoded_size: usize = 1 + 1 + 8 + 8 + 8;

fn encodeState(state: PostingState, buf: *[state_encoded_size]u8) []const u8 {
    buf[0] = state_format_version;
    buf[1] = (if (state.dirty) state_flag_dirty else 0) |
        (if (state.centroid_dirty) state_flag_centroid_dirty else 0) |
        (if (state.payload_dirty) state_flag_payload_dirty else 0);
    std.mem.writeInt(u64, buf[2..10], state.mutation_version, .little);
    std.mem.writeInt(u64, buf[10..18], state.centroid_version, .little);
    std.mem.writeInt(u64, buf[18..26], state.payload_version, .little);
    return buf;
}

pub fn decodeState(data: []const u8) !PostingState {
    if (data.len < state_encoded_size) return error.Corrupted;
    if (data[0] != state_format_version) return error.UnsupportedPostingStateVersion;
    const flags = data[1];
    return .{
        .mutation_version = std.mem.readInt(u64, data[2..10], .little),
        .centroid_version = std.mem.readInt(u64, data[10..18], .little),
        .payload_version = std.mem.readInt(u64, data[18..26], .little),
        .dirty = (flags & state_flag_dirty) != 0,
        .centroid_dirty = (flags & state_flag_centroid_dirty) != 0,
        .payload_dirty = (flags & state_flag_payload_dirty) != 0,
    };
}

fn indexOfMember(members: []const VectorId, vector_id: VectorId) ?usize {
    for (members, 0..) |member_id, i| {
        if (member_id == vector_id) return i;
    }
    return null;
}

fn containsMember(members: []const VectorId, vector_id: VectorId) bool {
    return indexOfMember(members, vector_id) != null;
}

fn indexHasExternalVectorLoader(index: anytype) bool {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime @hasDecl(Index, "hasExternalVectorLoader")) return index.hasExternalVectorLoader();
    return false;
}

fn normalizeCentroidForMetric(index: anytype, centroid: []f32) void {
    if (index.config.metric == .cosine and centroid.len > 0) {
        _ = vec.normalize(centroid);
    }
}

fn batchVectorLookup(options: anytype) ?hbc_runtime.BatchVectorLookup {
    const Options = @TypeOf(options);
    if (comptime @hasField(Options, "batch_vectors")) return options.batch_vectors;
    return null;
}

fn getBatchVectorScratch(index: anytype, txn: anytype, vector_id: VectorId, scratch: []f32, options: anytype) ![]const f32 {
    if (batchVectorLookup(options)) |lookup| {
        if (lookup.get(vector_id)) |vector| {
            if (vector.len > scratch.len) return error.BufferTooSmall;
            return vector;
        }
    }
    return try index.getVectorScratch(txn, vector_id, scratch);
}

fn noteMutatedCachedQuantized(index: anytype, posting_id: PostingId) void {
    const Index = switch (@typeInfo(@TypeOf(index))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(index),
    };
    if (comptime @hasDecl(Index, "noteMutatedCachedQuantized")) {
        index.noteMutatedCachedQuantized(posting_id);
    }
}

pub const AssignmentMap = struct {
    pub fn put(index: anytype, txn: anytype, vector_id: VectorId, posting_id: PostingId) !void {
        var key_buf: [10]u8 = undefined;
        var val_buf: [8]u8 = undefined;
        val_buf = @bitCast(std.mem.nativeToLittle(u64, posting_id));
        try index.putNamespaced(txn, .vecs, hbc.encodeVecLeafKey(&key_buf, vector_id), &val_buf);
    }

    pub fn get(index: anytype, txn: anytype, vector_id: VectorId) !PostingId {
        var key_buf: [10]u8 = undefined;
        const data = try index.getNamespaced(txn, .vecs, hbc.encodeVecLeafKey(&key_buf, vector_id));
        if (data.len < @sizeOf(u64)) return error.Corrupted;
        return std.mem.readInt(u64, data[0..8], .little);
    }

    pub fn delete(index: anytype, txn: anytype, vector_id: VectorId) !void {
        var key_buf: [10]u8 = undefined;
        try index.deleteNamespaced(txn, .vecs, hbc.encodeVecLeafKey(&key_buf, vector_id));
    }
};

pub const CentroidDirectory = struct {
    pub const Probe = struct {
        posting_id: PostingId,
        distance: f32,
        error_bound: f32 = 0,
    };

    pub fn findPosting(
        index: anytype,
        txn: anytype,
        root_id: PostingId,
        query: []const f32,
        allow_quantized: bool,
    ) !PostingId {
        return try index.findLeafWithOptions(txn, root_id, query, allow_quantized);
    }

    // Current HBC remains the first centroid directory implementation. This
    // type is intentionally thin for now; later implementations can expose the
    // same "query to posting IDs" contract without changing PostingStore.
};

test "posting view rejects internal nodes" {
    var children = [_]u64{2};
    const node = types.Node{
        .id = 1,
        .is_leaf = false,
        .level = 0,
        .parent = 0,
        .centroid = &.{},
        .children = children[0..],
        .members = &.{},
    };
    try std.testing.expectError(error.ExpectedLeaf, PostingStore.view(&node));
}

test "posting view exposes leaf as posting" {
    var centroid = [_]f32{ 1.0, 2.0 };
    var members = [_]u64{ 10, 20 };
    const node = types.Node{
        .id = 7,
        .is_leaf = true,
        .level = 1,
        .parent = 3,
        .centroid = centroid[0..],
        .children = &.{},
        .members = members[0..],
    };
    const posting = try PostingStore.view(&node);
    try std.testing.expectEqual(@as(PostingId, 7), posting.id);
    try std.testing.expectEqual(@as(PostingId, 3), posting.parent);
    try std.testing.expectEqualSlices(u64, members[0..], posting.members);
    try std.testing.expect(!posting.usesNonQuantizedPayload());
}

test "posting store appends and removes members" {
    const alloc = std.testing.allocator;
    const members = try alloc.dupe(u64, &[_]u64{ 1, 2 });
    var node = types.Node{
        .id = 7,
        .is_leaf = true,
        .level = 1,
        .parent = 3,
        .centroid = &.{},
        .children = &.{},
        .members = members,
    };
    defer node.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), try PostingStore.appendMember(alloc, &node, 3));
    try std.testing.expectEqualSlices(u64, &[_]u64{ 1, 2, 3 }, node.members);

    try PostingStore.removeMember(alloc, &node, 2);
    try std.testing.expectEqualSlices(u64, &[_]u64{ 1, 3 }, node.members);

    const removed = try PostingStore.removeMembers(alloc, &node, &[_]u64{ 1, 9 });
    try std.testing.expectEqual(@as(usize, 1), removed);
    try std.testing.expectEqualSlices(u64, &[_]u64{3}, node.members);
}

test "posting state tracks dirty versions" {
    var state = PostingState{};
    state.noteMembersChanged(3);
    try std.testing.expectEqual(@as(u64, 1), state.mutation_version);
    try std.testing.expect(state.dirty);
    try std.testing.expect(state.centroid_dirty);
    try std.testing.expect(state.payload_dirty);

    state.noteCentroidRefreshed();
    try std.testing.expectEqual(@as(u64, 1), state.centroid_version);
    try std.testing.expect(!state.centroid_dirty);
    try std.testing.expect(state.dirty);

    state.notePayloadRefreshed();
    try std.testing.expectEqual(@as(u64, 1), state.payload_version);
    try std.testing.expect(!state.payload_dirty);
    try std.testing.expect(!state.dirty);
}

test "posting state encoding round trips" {
    const state = PostingState{
        .mutation_version = 7,
        .centroid_version = 5,
        .payload_version = 6,
        .dirty = true,
        .centroid_dirty = true,
        .payload_dirty = false,
    };
    var buf: [state_encoded_size]u8 = undefined;
    const decoded = try decodeState(encodeState(state, &buf));
    try std.testing.expectEqual(state.mutation_version, decoded.mutation_version);
    try std.testing.expectEqual(state.centroid_version, decoded.centroid_version);
    try std.testing.expectEqual(state.payload_version, decoded.payload_version);
    try std.testing.expectEqual(state.dirty, decoded.dirty);
    try std.testing.expectEqual(state.centroid_dirty, decoded.centroid_dirty);
    try std.testing.expectEqual(state.payload_dirty, decoded.payload_dirty);
}
