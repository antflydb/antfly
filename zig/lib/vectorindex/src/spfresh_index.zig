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
const centroid_directory = @import("centroid_directory.zig");
const builtin = @import("builtin");
const types = @import("types.zig");
const hbc = @import("hbc.zig");
const hbc_runtime = @import("hbc_runtime.zig");
const posting = @import("posting.zig");
const search_types = @import("search_types.zig");
const proto = @import("antfly_vector").proto;
const vec = @import("antfly_vector").vector;

pub const FlatCentroidBlock = struct {
    const Exact = struct {
        vectors: []const f32,
        measures: []const f32,
    };

    const Encoding = union(enum) {
        rabitq: proto.RaBitQuantizedVectorSet,
        exact: Exact,
    };

    posting_ids: []const u64,
    covering_radii: []const f32,
    encoding: Encoding,
    /// Base-generation rows whose posting id is superseded by a native delta
    /// are left mmap-backed in place and masked during scans. Delta blocks do
    /// not carry this mask.
    shadowed_posting_bits: []const u64 = &.{},
    owned: bool = true,

    fn deinit(self: *FlatCentroidBlock, alloc: std.mem.Allocator) void {
        if (!self.owned) {
            self.* = undefined;
            return;
        }
        alloc.free(@constCast(self.posting_ids));
        alloc.free(@constCast(self.covering_radii));
        switch (self.encoding) {
            .rabitq => |quantized_value| {
                var quantized = quantized_value;
                quantized.deinit(alloc);
            },
            .exact => |exact| {
                alloc.free(@constCast(exact.vectors));
                alloc.free(@constCast(exact.measures));
            },
        }
        self.* = undefined;
    }
};

pub const FlatCentroidBackingLease = struct {
    ptr: *anyopaque,
    release_fn: *const fn (*anyopaque) void,

    pub fn release(self: FlatCentroidBackingLease) void {
        self.release_fn(self.ptr);
    }
};

pub const FlatCentroidDirectory = struct {
    blocks: []FlatCentroidBlock = &.{},
    ref_count: std.atomic.Value(u32) = .init(1),
    root_node_snapshot: u64 = 0,
    node_count_snapshot: u64 = 0,
    publish_generation_snapshot: u64 = 0,
    posting_count: usize = 0,
    owned_shadowed_posting_bits: []u64 = &.{},
    backing: ?FlatCentroidBackingLease = null,
    missing_node_count: usize = 0,
    invalid_posting_count: usize = 0,
    accounting_context: ?*anyopaque = null,
    release_accounting: ?*const fn (*anyopaque, u64) void = null,
    accounted_bytes: u64 = 0,

    pub fn retain(self: *FlatCentroidDirectory) void {
        _ = self.ref_count.fetchAdd(1, .acq_rel);
    }

    pub fn release(self: *FlatCentroidDirectory, alloc: std.mem.Allocator) void {
        if (self.ref_count.fetchSub(1, .acq_rel) != 1) return;
        self.deinit(alloc);
        alloc.destroy(self);
    }

    fn deinit(self: *FlatCentroidDirectory, alloc: std.mem.Allocator) void {
        const accounting_context = self.accounting_context;
        const release_accounting = self.release_accounting;
        const accounted_bytes = self.accounted_bytes;
        for (self.blocks) |*block| block.deinit(alloc);
        alloc.free(self.blocks);
        alloc.free(self.owned_shadowed_posting_bits);
        if (self.backing) |backing| backing.release();
        self.* = .{};
        if (release_accounting) |release_fn| release_fn(accounting_context.?, accounted_bytes);
    }

    fn complete(self: *const FlatCentroidDirectory) bool {
        return self.missing_node_count == 0 and self.invalid_posting_count == 0;
    }

    pub fn bytes(self: *const FlatCentroidDirectory) u64 {
        var total: u64 = @sizeOf(FlatCentroidDirectory) +|
            @as(u64, @intCast(self.blocks.len * @sizeOf(FlatCentroidBlock)));
        for (self.blocks) |*block| {
            if (!block.owned) continue;
            total +|= @as(u64, @intCast(block.posting_ids.len * @sizeOf(u64)));
            total +|= @as(u64, @intCast(block.covering_radii.len * @sizeOf(f32)));
            switch (block.encoding) {
                .rabitq => |quantized| {
                    total +|= @as(u64, @intCast(quantized.centroid.len * @sizeOf(f32)));
                    total +|= @as(u64, @intCast(quantized.codes.data.len * @sizeOf(u64)));
                    total +|= @as(u64, @intCast(quantized.code_counts.len * @sizeOf(u32)));
                    total +|= @as(u64, @intCast(quantized.centroid_distances.len * @sizeOf(f32)));
                    total +|= @as(u64, @intCast(quantized.quantized_dot_products.len * @sizeOf(f32)));
                    total +|= @as(u64, @intCast(quantized.centroid_dot_products.len * @sizeOf(f32)));
                },
                .exact => |exact| {
                    total +|= @as(u64, @intCast(exact.vectors.len * @sizeOf(f32)));
                    total +|= @as(u64, @intCast(exact.measures.len * @sizeOf(f32)));
                },
            }
        }
        total +|= @as(u64, @intCast(self.owned_shadowed_posting_bits.len * @sizeOf(u64)));
        return total;
    }
};

pub const OwnedExactCentroidOverlay = struct {
    posting_ids: []u64,
    covering_radii: []f32,
    measures: []f32,
    vectors: []f32,

    pub fn deinit(self: *OwnedExactCentroidOverlay, alloc: std.mem.Allocator) void {
        alloc.free(self.posting_ids);
        alloc.free(self.covering_radii);
        alloc.free(self.measures);
        alloc.free(self.vectors);
        self.* = undefined;
    }
};

fn postingBitIsSet(bits: []const u64, posting_id: u64) bool {
    const word_u64 = posting_id / 64;
    const word = std.math.cast(usize, word_u64) orelse return false;
    if (word >= bits.len) return false;
    return (bits[word] & (@as(u64, 1) << @intCast(posting_id % 64))) != 0;
}

/// Creates a zero-copy exact directory over an immutable, externally leased
/// generation. Ownership of `backing` transfers to the returned directory,
/// including on error.
pub fn exactFlatCentroidDirectoryFromReader(
    alloc: std.mem.Allocator,
    reader: centroid_directory.Reader,
    root_node: u64,
    node_count: u64,
    publish_generation: u64,
    backing: FlatCentroidBackingLease,
) !FlatCentroidDirectory {
    errdefer backing.release();
    var blocks = try alloc.alloc(FlatCentroidBlock, reader.block_count);
    errdefer alloc.free(blocks);
    var iter = reader.blocks();
    var index: usize = 0;
    while (try iter.next()) |block| : (index += 1) {
        blocks[index] = .{
            .posting_ids = block.posting_ids,
            .covering_radii = block.covering_radii,
            .encoding = .{ .exact = .{
                .vectors = block.vectors,
                .measures = block.measures,
            } },
            .owned = false,
        };
    }
    std.debug.assert(index == blocks.len);
    return .{
        .blocks = blocks,
        .root_node_snapshot = root_node,
        .node_count_snapshot = node_count,
        .publish_generation_snapshot = publish_generation,
        .posting_count = reader.posting_count,
        .backing = backing,
    };
}

/// Builds a directory that keeps the complete immutable base zero-copy while
/// owning only the latest centroids changed above it. `shadowed_posting_ids`
/// includes both replacements and tombstones. Ownership of the overlay,
/// shadowed ids, and backing lease transfers to this function on every path.
pub fn layeredExactFlatCentroidDirectoryFromReader(
    alloc: std.mem.Allocator,
    reader: centroid_directory.Reader,
    overlay_value: OwnedExactCentroidOverlay,
    shadowed_posting_ids: []u64,
    root_node: u64,
    node_count: u64,
    publish_generation: u64,
    backing: FlatCentroidBackingLease,
) !FlatCentroidDirectory {
    var overlay = overlay_value;
    var overlay_owned = true;
    errdefer if (overlay_owned) overlay.deinit(alloc);
    defer alloc.free(shadowed_posting_ids);
    errdefer backing.release();
    const expected_vector_values = std.math.mul(usize, overlay.posting_ids.len, reader.dims) catch
        return error.InvalidCentroidDirectory;
    if (overlay.posting_ids.len != overlay.covering_radii.len or
        overlay.posting_ids.len != overlay.measures.len or
        overlay.vectors.len != expected_vector_values)
    {
        return error.InvalidCentroidDirectory;
    }

    const bit_words_u64 = std.math.add(u64, node_count, 64) catch return error.OutOfMemory;
    const bit_words = std.math.cast(usize, bit_words_u64 / 64) orelse return error.OutOfMemory;
    const shadowed_bits = try alloc.alloc(u64, bit_words);
    errdefer alloc.free(shadowed_bits);
    @memset(shadowed_bits, 0);
    for (shadowed_posting_ids) |posting_id| {
        const word = std.math.cast(usize, posting_id / 64) orelse continue;
        if (word < shadowed_bits.len) shadowed_bits[word] |= @as(u64, 1) << @intCast(posting_id % 64);
    }

    const overlay_blocks: usize = @intFromBool(overlay.posting_ids.len != 0);
    var blocks = try alloc.alloc(FlatCentroidBlock, reader.block_count + overlay_blocks);
    var initialized_blocks: usize = 0;
    errdefer {
        for (blocks[0..initialized_blocks]) |*block| block.deinit(alloc);
        alloc.free(blocks);
    }
    var block_index: usize = 0;
    if (overlay.posting_ids.len != 0) {
        blocks[0] = .{
            .posting_ids = overlay.posting_ids,
            .covering_radii = overlay.covering_radii,
            .encoding = .{ .exact = .{
                .vectors = overlay.vectors,
                .measures = overlay.measures,
            } },
        };
        overlay_owned = false;
        block_index = 1;
        initialized_blocks = 1;
    } else {
        overlay.deinit(alloc);
        overlay_owned = false;
    }

    var visible_base_postings: usize = 0;
    var iter = reader.blocks();
    while (try iter.next()) |block| : (block_index += 1) {
        blocks[block_index] = .{
            .posting_ids = block.posting_ids,
            .covering_radii = block.covering_radii,
            .encoding = .{ .exact = .{
                .vectors = block.vectors,
                .measures = block.measures,
            } },
            .shadowed_posting_bits = shadowed_bits,
            .owned = false,
        };
        initialized_blocks += 1;
        for (block.posting_ids) |posting_id| {
            visible_base_postings += @intFromBool(!postingBitIsSet(shadowed_bits, posting_id));
        }
    }
    std.debug.assert(block_index == blocks.len);
    return .{
        .blocks = blocks,
        .root_node_snapshot = root_node,
        .node_count_snapshot = node_count,
        .publish_generation_snapshot = publish_generation,
        .posting_count = visible_base_postings + overlay.posting_ids.len,
        .owned_shadowed_posting_bits = shadowed_bits,
        .backing = backing,
    };
}

pub const FlatCentroidSelection = struct {
    /// Borrowed from the request scratch and valid until that handle is
    /// released. Keeping ownership with the governed scratch avoids an
    /// unaccounted allocation and retains capacity across requests.
    probes: []FlatCentroidProbe,
    total_postings: usize,
};

fn flatSelectionLimit(
    coverage_policy: search_types.CoveragePolicy,
    max_postings: usize,
    posting_count: usize,
) usize {
    return if (coverage_policy == .complete_snapshot)
        posting_count
    else
        @min(max_postings, posting_count);
}

test "approximate flat routing bounds request scratch while complete coverage remains exhaustive" {
    try std.testing.expectEqual(@as(usize, 2_064), flatSelectionLimit(.best_effort, 2_064, 8_878));
    try std.testing.expectEqual(@as(usize, 8_878), flatSelectionLimit(.complete_snapshot, 2_064, 8_878));
    try std.testing.expectEqual(@as(usize, 64), flatSelectionLimit(.best_effort, 2_064, 64));
}

fn checkedAddMul(total: *u64, count: u64, element_size: u64) !void {
    const bytes = std.math.mul(u64, count, element_size) catch return error.OutOfMemory;
    total.* = std.math.add(u64, total.*, bytes) catch return error.OutOfMemory;
}

pub const FlatCentroidBuildReservation = struct {
    transient_bytes: u64 = 0,
    retained_bytes: u64 = 0,

    fn empty(self: @This()) bool {
        return self.transient_bytes == 0 and self.retained_bytes == 0;
    }
};

pub const FlatCentroidBuildClaim = union(enum) {
    owner,
    retry,
    /// Owns one retained reference; the caller must eventually release it.
    ready: *FlatCentroidDirectory,
};

pub const FlatCentroidBuildOutcome = union(enum) {
    retry,
    failed: anyerror,
    /// The flight retains its own reference until every waiter has consumed
    /// the outcome.
    ready: *FlatCentroidDirectory,
};

/// Conservative peak allocation for a cold flat-directory build. It covers
/// the larger of the retained RaBitQ output and an exact persisted-generation
/// overlay for the worst case (every published node is a leaf), traversal
/// arrays, raw centroid blocks, and quantization/overlay construction
/// workspace. A native WAL tail may legitimately shadow every base posting;
/// loading that generation owns a float32 overlay even when new directories
/// normally use RaBitQ. ArrayList growth is budgeted at twice its logical
/// capacity.
pub fn projectedFlatCentroidDirectoryBuildBytes(
    node_count: u64,
    dims: usize,
    block_size_raw: usize,
    max_node_entries: usize,
) !FlatCentroidBuildReservation {
    const block_size = @max(block_size_raw, @as(usize, 1));
    const block_count = std.math.divCeil(u64, node_count, @as(u64, @intCast(block_size))) catch return error.OutOfMemory;
    const code_width = std.math.divCeil(u64, @as(u64, @intCast(dims)), 64) catch return error.OutOfMemory;
    const visited_words = std.math.divCeil(u64, node_count, @bitSizeOf(usize)) catch return error.OutOfMemory;

    var retained: u64 = @sizeOf(FlatCentroidDirectory);
    // Retained directory and its worst-case block-array capacity.
    try checkedAddMul(&retained, @max(block_count *| 2, 16), @sizeOf(FlatCentroidBlock));
    try checkedAddMul(&retained, node_count, @sizeOf(u64) + @sizeOf(f32));
    var rabit_payload: u64 = 0;
    try checkedAddMul(&rabit_payload, node_count, code_width *| @sizeOf(u64));
    try checkedAddMul(&rabit_payload, node_count, 4 * @sizeOf(f32) + @sizeOf(u32));
    try checkedAddMul(&rabit_payload, block_count, @as(u64, @intCast(dims)) *| @sizeOf(f32));
    var exact_overlay_payload: u64 = 0;
    try checkedAddMul(
        &exact_overlay_payload,
        node_count,
        @as(u64, @intCast(dims)) *| @sizeOf(f32) + @sizeOf(f32),
    );
    retained = std.math.add(u64, retained, @max(rabit_payload, exact_overlay_payload)) catch
        return error.OutOfMemory;
    // Traversal and block construction workspace. Quantization temporarily
    // holds a normalized copy alongside the raw centroid block.
    var transient: u64 = 0;
    try checkedAddMul(&transient, @max(node_count *| 2, 16), @sizeOf(u64));
    try checkedAddMul(&transient, visited_words, @sizeOf(usize));
    try checkedAddMul(&transient, @intCast(block_size), @sizeOf(u64) + @sizeOf(f32));
    try checkedAddMul(&transient, @intCast(block_size), @as(u64, @intCast(dims)) *| @sizeOf(f32));
    try checkedAddMul(&transient, @min(node_count, @as(u64, @intCast(block_size))), @as(u64, @intCast(dims)) *| @sizeOf(f32));
    try checkedAddMul(&transient, 1, @as(u64, @intCast(dims)) *| @sizeOf(f32));
    // One decoded node is live at a time while traversing the directory.
    try checkedAddMul(&transient, 1, @sizeOf(types.Node));
    try checkedAddMul(&transient, 1, @as(u64, @intCast(dims)) *| @sizeOf(f32));
    try checkedAddMul(&transient, @intCast(max_node_entries), @sizeOf(u64));
    // Persisted exact overlays are accumulated in growable arrays before
    // ownership moves into the directory. Retained admission covers the final
    // payload; one additional logical payload covers geometric growth and a
    // potentially allocating shrink-to-owned handoff.
    transient = std.math.add(u64, transient, exact_overlay_payload) catch
        return error.OutOfMemory;
    return .{ .transient_bytes = transient, .retained_bytes = retained };
}

test "flat centroid projection covers a million-scale exact WAL overlay" {
    const node_count: u64 = 8_950;
    const dims: usize = 768;
    const projection = try projectedFlatCentroidDirectoryBuildBytes(node_count, dims, 1_280, 128);
    const exact_payload = node_count *
        (@as(u64, dims) * @sizeOf(f32) + @sizeOf(f32));

    try std.testing.expect(projection.retained_bytes >= exact_payload);
    try std.testing.expect(projection.transient_bytes >= exact_payload);
}

pub const FlatCentroidProbe = search_types.FlatCentroidProbe;
const cancellable_flat_sort_chunk_size: usize = 4_096;

fn lockAtomicMutex(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn isNotFoundGeneric(err: anyerror) bool {
    return err == error.NotFound;
}

fn nowNsI128Fixed() i128 {
    return 0;
}

fn elapsedSinceNsFixed(start: i128) u64 {
    _ = start;
    return 0;
}

fn nowNsU64Fixed() u64 {
    return 0;
}

fn elapsedSinceU64Fixed(start: u64) u64 {
    _ = start;
    return 0;
}

fn savePackedNodeValue(self: anytype, txn: anytype, node: *const types.Node) !void {
    const header = hbc.NodeHeader{
        .is_leaf = node.is_leaf,
        .level = node.level,
        .parent = node.parent,
    };
    const centroid_bytes = std.mem.sliceAsBytes(node.centroid);
    const ids_bytes = if (node.is_leaf) std.mem.sliceAsBytes(node.members) else std.mem.sliceAsBytes(node.children);
    const packed_len = hbc.packedNodeValueSize(centroid_bytes.len, ids_bytes.len);
    const packed_value = try self.alloc.alloc(u8, packed_len);
    defer self.alloc.free(packed_value);
    const encoded = try hbc.encodePackedNodeValue(packed_value, header, node.covering_radius, centroid_bytes, ids_bytes);
    var key_buf: [12]u8 = undefined;
    try self.putNamespaced(txn, .nodes, hbc.encodeNodeKey(&key_buf, node.id, .packed_node), encoded);
}

fn flatProbeScore(probe: FlatCentroidProbe) f32 {
    // ANN routing follows centroid proximity. Proof bounds are intentionally
    // separate: RaBitQ uncertainty must not promote a farther posting ahead
    // of a nearer estimated centroid. The error interval is retained on each
    // probe for sound suffix stopping, but is not a routing score.
    return probe.distance;
}

fn flatProbeWorseThan(_: void, lhs: FlatCentroidProbe, rhs: FlatCentroidProbe) std.math.Order {
    if (flatProbeLess({}, rhs, lhs)) return .lt;
    if (flatProbeLess({}, lhs, rhs)) return .gt;
    return .eq;
}

fn siftFlatProbeUp(probes: []FlatCentroidProbe, start_index: usize) void {
    const child = probes[start_index];
    var child_index = start_index;
    while (child_index > 0) {
        const parent_index = (child_index - 1) >> 1;
        const parent = probes[parent_index];
        if (flatProbeWorseThan({}, child, parent) != .lt) break;
        probes[child_index] = parent;
        child_index = parent_index;
    }
    probes[child_index] = child;
}

fn siftFlatProbeDown(probes: []FlatCentroidProbe, start_index: usize) void {
    const parent = probes[start_index];
    var parent_index = start_index;
    while (true) {
        const left_index = parent_index * 2 + 1;
        if (left_index >= probes.len) break;
        const right_index = left_index + 1;
        const worse_child_index = if (right_index < probes.len and
            flatProbeWorseThan({}, probes[right_index], probes[left_index]) == .lt)
            right_index
        else
            left_index;
        const worse_child = probes[worse_child_index];
        if (flatProbeWorseThan({}, worse_child, parent) != .lt) break;
        probes[parent_index] = worse_child;
        parent_index = worse_child_index;
    }
    probes[parent_index] = parent;
}

/// Maintain a fixed-capacity max heap whose root is the least desirable
/// retained probe. Insertion remains O(log k) without allocating a separate
/// PriorityQueue backing buffer outside the resource governor.
fn insertFlatProbe(probes: []FlatCentroidProbe, count: *usize, candidate: FlatCentroidProbe) void {
    if (probes.len == 0) return;
    if (count.* < probes.len) {
        probes[count.*] = candidate;
        siftFlatProbeUp(probes[0 .. count.* + 1], count.*);
        count.* += 1;
        return;
    }
    if (!flatProbeLess({}, candidate, probes[0])) return;
    probes[0] = candidate;
    siftFlatProbeDown(probes, 0);
}

fn flatAnnScore(probe: FlatCentroidProbe) f32 {
    return flatProbeScore(probe);
}

fn flatProbeLess(_: void, lhs: FlatCentroidProbe, rhs: FlatCentroidProbe) bool {
    const lhs_score = flatAnnScore(lhs);
    const rhs_score = flatAnnScore(rhs);
    if (std.math.isNan(lhs_score)) {
        if (std.math.isNan(rhs_score)) return lhs.posting_id < rhs.posting_id;
        return false;
    }
    if (std.math.isNan(rhs_score)) return true;
    if (lhs_score != rhs_score) return lhs_score < rhs_score;
    return lhs.posting_id < rhs.posting_id;
}

fn populateFlatProbeSuffixBounds(probes: []FlatCentroidProbe) void {
    var suffix_lower_bound = std.math.inf(f32);
    var suffix_resolved = true;
    var reverse_index = probes.len;
    while (reverse_index > 0) {
        reverse_index -= 1;
        const probe = &probes[reverse_index];
        suffix_resolved = suffix_resolved and probe.bound_resolved and std.math.isFinite(probe.member_lower_bound);
        if (suffix_resolved) suffix_lower_bound = @min(suffix_lower_bound, probe.member_lower_bound);
        probe.suffix_bounds_resolved = suffix_resolved;
        probe.suffix_member_lower_bound = if (suffix_resolved) suffix_lower_bound else -std.math.inf(f32);
    }
}

test "flat probes route by centroid and retain sound suffix proof bounds" {
    var probes = [_]FlatCentroidProbe{
        .{ .posting_id = 1, .distance = 0.4, .error_bound = 0, .member_lower_bound = 0.0, .bound_resolved = true },
        .{ .posting_id = 2, .distance = 0.1, .error_bound = 0, .member_lower_bound = 0.08, .bound_resolved = true },
        .{ .posting_id = 3, .distance = 0.3, .error_bound = 0, .member_lower_bound = 0.2, .bound_resolved = true },
    };
    std.mem.sort(FlatCentroidProbe, &probes, {}, flatProbeLess);
    try std.testing.expectEqual(@as(u64, 2), probes[0].posting_id);
    try std.testing.expectEqual(@as(u64, 3), probes[1].posting_id);
    populateFlatProbeSuffixBounds(&probes);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), probes[0].suffix_member_lower_bound, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), probes[1].suffix_member_lower_bound, 0.000001);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), probes[2].suffix_member_lower_bound, 0.000001);
}

test "flat probes do not promote uncertain farther centroids" {
    var probes = [_]FlatCentroidProbe{
        .{ .posting_id = 1, .distance = 0.2, .error_bound = 0.15 },
        .{ .posting_id = 2, .distance = 0.1, .error_bound = 0.0 },
    };
    std.mem.sort(FlatCentroidProbe, &probes, {}, flatProbeLess);
    try std.testing.expectEqual(@as(u64, 2), probes[0].posting_id);
    try std.testing.expectEqual(@as(u64, 1), probes[1].posting_id);
}

test "bounded flat probe heap retains the best stable frontier" {
    var retained: [3]FlatCentroidProbe = undefined;
    var retained_count: usize = 0;
    const candidates = [_]FlatCentroidProbe{
        .{ .posting_id = 5, .distance = 0.5, .error_bound = 0 },
        .{ .posting_id = 2, .distance = 0.2, .error_bound = 0 },
        .{ .posting_id = 4, .distance = 0.4, .error_bound = 0 },
        .{ .posting_id = 1, .distance = 0.1, .error_bound = 0 },
        .{ .posting_id = 3, .distance = 0.3, .error_bound = 0 },
        // A non-finite routing score is always worse than a finite candidate.
        .{ .posting_id = 0, .distance = std.math.nan(f32), .error_bound = 0 },
    };
    for (candidates) |candidate| insertFlatProbe(&retained, &retained_count, candidate);
    try std.testing.expectEqual(retained.len, retained_count);
    std.mem.sort(FlatCentroidProbe, &retained, {}, flatProbeLess);
    try std.testing.expectEqual(@as(u64, 1), retained[0].posting_id);
    try std.testing.expectEqual(@as(u64, 2), retained[1].posting_id);
    try std.testing.expectEqual(@as(u64, 3), retained[2].posting_id);
}

fn flatMemberLowerBound(
    metric: vec.DistanceMetric,
    distance: f32,
    error_bound: f32,
    covering_radius: f32,
) ?f32 {
    if (!std.math.isFinite(covering_radius) or covering_radius < 0) return null;
    const centroid_metric_lower = @max(@as(f32, 0), distance - error_bound);
    if (!std.math.isFinite(centroid_metric_lower)) return null;
    const centroid_chord_lower = switch (metric) {
        .l2_squared => @sqrt(centroid_metric_lower),
        .cosine => @sqrt(2.0 * @min(centroid_metric_lower, 2.0)),
        .inner_product => return null,
    };
    const member_chord_lower = @max(@as(f32, 0), centroid_chord_lower - covering_radius);
    return switch (metric) {
        .l2_squared => member_chord_lower * member_chord_lower,
        .cosine => 0.5 * member_chord_lower * member_chord_lower,
        .inner_product => unreachable,
    };
}

test "flat cosine member bound uses the persisted chord radius" {
    const centroid_distance: f32 = 0.5;
    const covering_radius: f32 = 0.25;
    const lower = flatMemberLowerBound(.cosine, centroid_distance, 0, covering_radius) orelse
        return error.TestUnexpectedResult;
    const expected_chord = @sqrt(@as(f32, 2.0) * centroid_distance) - covering_radius;
    try std.testing.expectApproxEqAbs(0.5 * expected_chord * expected_chord, lower, 0.000001);
    try std.testing.expect(flatMemberLowerBound(.inner_product, centroid_distance, 0, covering_radius) == null);
}

fn sortFlatProbesCancellable(
    probes: []FlatCentroidProbe,
    merge_buffer: []FlatCentroidProbe,
    cancellation: ?search_types.CancellationToken,
) !void {
    try checkCancellation(cancellation);
    if (probes.len < 2) return;

    // Preserve the standard adaptive block sort for the overwhelmingly common
    // small frontier and for direct library callers without cancellation. Large
    // cancellable frontiers are sorted in bounded chunks and merged linearly;
    // this avoids heapsort's poor locality and O(n log n) work on already sorted
    // inputs while keeping cancellation latency independent of total index size.
    if (cancellation == null or probes.len <= cancellable_flat_sort_chunk_size) {
        std.mem.sort(FlatCentroidProbe, probes, {}, flatProbeLess);
        try checkCancellation(cancellation);
        return;
    }

    var chunk_start: usize = 0;
    while (chunk_start < probes.len) {
        try checkCancellation(cancellation);
        const chunk_end = chunk_start + @min(cancellable_flat_sort_chunk_size, probes.len - chunk_start);
        std.mem.sort(FlatCentroidProbe, probes[chunk_start..chunk_end], {}, flatProbeLess);
        chunk_start = chunk_end;
    }

    if (merge_buffer.len < probes.len) return error.BufferTooSmall;
    var source = probes;
    var destination = merge_buffer;
    var run_width: usize = cancellable_flat_sort_chunk_size;
    while (run_width < probes.len) {
        try checkCancellation(cancellation);
        var start: usize = 0;
        while (start < probes.len) {
            const middle = start + @min(run_width, probes.len - start);
            const end = middle + @min(run_width, probes.len - middle);
            try mergeFlatProbeRunsCancellable(source, destination, start, middle, end, cancellation);
            start = end;
        }
        const previous_source = source;
        source = destination;
        destination = previous_source;
        if (run_width > probes.len / 2) break;
        run_width *= 2;
    }

    if (source.ptr != probes.ptr) {
        var offset: usize = 0;
        while (offset < probes.len) {
            try checkCancellation(cancellation);
            const end = offset + @min(cancellable_flat_sort_chunk_size, probes.len - offset);
            @memcpy(probes[offset..end], source[offset..end]);
            offset = end;
        }
    }
    try checkCancellation(cancellation);
}

fn mergeFlatProbeRunsCancellable(
    source: []const FlatCentroidProbe,
    destination: []FlatCentroidProbe,
    start: usize,
    middle: usize,
    end: usize,
    cancellation: ?search_types.CancellationToken,
) !void {
    var left = start;
    var right = middle;
    var out = start;
    while (left < middle and right < end) : (out += 1) {
        if ((out & 0xff) == 0) try checkCancellation(cancellation);
        if (flatProbeLess({}, source[right], source[left])) {
            destination[out] = source[right];
            right += 1;
        } else {
            destination[out] = source[left];
            left += 1;
        }
    }
    while (left < middle) : ({
        left += 1;
        out += 1;
    }) {
        if ((out & 0xff) == 0) try checkCancellation(cancellation);
        destination[out] = source[left];
    }
    while (right < end) : ({
        right += 1;
        out += 1;
    }) {
        if ((out & 0xff) == 0) try checkCancellation(cancellation);
        destination[out] = source[right];
    }
}

fn checkCancellation(cancellation: ?search_types.CancellationToken) !void {
    if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
}

pub fn flatProbeLessForTesting(lhs: FlatCentroidProbe, rhs: FlatCentroidProbe) bool {
    if (!builtin.is_test) @compileError("test-only flat probe ordering seam");
    return flatProbeLess({}, lhs, rhs);
}

test "flat frontier cancellable sort preserves deterministic ordering" {
    var probes = [_]FlatCentroidProbe{
        .{ .posting_id = 9, .distance = 4, .error_bound = 1 },
        .{ .posting_id = 7, .distance = 2, .error_bound = 0 },
        .{ .posting_id = 3, .distance = 3, .error_bound = 1 },
        .{ .posting_id = 1, .distance = 2, .error_bound = 0 },
        .{ .posting_id = 5, .distance = 1, .error_bound = 0 },
    };
    try sortFlatProbesCancellable(&probes, &.{}, null);
    for (probes[0 .. probes.len - 1], probes[1..]) |lhs, rhs| {
        try std.testing.expect(!flatProbeLess({}, rhs, lhs));
    }
    try std.testing.expectEqualSlices(u64, &.{ 5, 1, 7, 3, 9 }, &.{
        probes[0].posting_id,
        probes[1].posting_id,
        probes[2].posting_id,
        probes[3].posting_id,
        probes[4].posting_id,
    });
}

test "flat frontier scoring sort honors cancellation" {
    var cancelled = std.atomic.Value(bool).init(true);
    var probes = [_]FlatCentroidProbe{
        .{ .posting_id = 2, .distance = 2, .error_bound = 0 },
        .{ .posting_id = 1, .distance = 1, .error_bound = 0 },
    };
    try std.testing.expectError(
        error.Cancelled,
        sortFlatProbesCancellable(&probes, &.{}, search_types.CancellationToken.fromAtomic(&cancelled)),
    );
}

test "large cancellable flat frontier uses deterministic chunked merge" {
    const probe_count = 4_097;
    const probes = try std.testing.allocator.alloc(FlatCentroidProbe, probe_count);
    defer std.testing.allocator.free(probes);
    const merge_buffer = try std.testing.allocator.alloc(FlatCentroidProbe, probe_count);
    defer std.testing.allocator.free(merge_buffer);
    for (probes, 0..) |*probe, i| {
        const reverse_id: u64 = @intCast(probe_count - i);
        probe.* = .{
            .posting_id = reverse_id,
            .distance = @floatFromInt(reverse_id % 31),
            .error_bound = 0,
        };
    }
    var cancelled = std.atomic.Value(bool).init(false);
    try sortFlatProbesCancellable(
        probes,
        merge_buffer,
        search_types.CancellationToken.fromAtomic(&cancelled),
    );
    for (probes[0 .. probes.len - 1], probes[1..]) |lhs, rhs| {
        try std.testing.expect(!flatProbeLess({}, rhs, lhs));
    }
}

fn flatL2MemberLowerBound(distance: f32, error_bound: f32, covering_radius: f32) ?f32 {
    if (!std.math.isFinite(covering_radius) or covering_radius < 0) return null;
    const centroid_squared_lower = @max(@as(f32, 0), distance - error_bound);
    if (!std.math.isFinite(centroid_squared_lower)) return null;
    const member_distance_lower = @max(@as(f32, 0), @sqrt(centroid_squared_lower) - covering_radius);
    return member_distance_lower * member_distance_lower;
}

fn publishedRootNodeSnapshot(self: anytype) u64 {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "publishedRootNode")) return self.publishedRootNode();
    return self.metadata.root_node;
}

fn publishedNodeCountSnapshot(self: anytype) u64 {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "publishedNodeCount")) return self.publishedNodeCount();
    return self.metadata.node_count;
}

fn publishedGenerationSnapshot(self: anytype) u64 {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "publishedGeneration")) return self.publishedGeneration();
    return 0;
}

pub const PublishedSnapshot = struct {
    root_node: u64,
    node_count: u64,
    publish_generation: u64,
};

fn waitForStablePublicationIfSupported(
    self: anytype,
    generation: u64,
    cancellation: ?search_types.CancellationToken,
) !void {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "waitForPublishedSearchState")) {
        return try self.waitForPublishedSearchState(generation, cancellation);
    }
    if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
    if (builtin.os.tag == .freestanding) {
        std.atomic.spinLoopHint();
    } else {
        std.Thread.yield() catch {};
    }
}

fn loadStablePublishedSnapshot(
    self: anytype,
    cancellation: ?search_types.CancellationToken,
) !PublishedSnapshot {
    const Index = comptime @TypeOf(self.*);
    if (comptime !@hasDecl(Index, "publishedGeneration")) {
        return .{
            .root_node = publishedRootNodeSnapshot(self),
            .node_count = publishedNodeCountSnapshot(self),
            .publish_generation = 0,
        };
    }

    while (true) {
        const generation = publishedGenerationSnapshot(self);
        if ((generation & 1) != 0) {
            try waitForStablePublicationIfSupported(self, generation, cancellation);
            continue;
        }
        const root_node = publishedRootNodeSnapshot(self);
        const node_count = publishedNodeCountSnapshot(self);
        const generation_after = publishedGenerationSnapshot(self);
        if (generation == generation_after and (generation_after & 1) == 0) {
            return .{
                .root_node = root_node,
                .node_count = node_count,
                .publish_generation = generation,
            };
        }
        try waitForStablePublicationIfSupported(self, generation_after, cancellation);
    }
}

fn loadPublishedNode(self: anytype, txn: anytype, node_id: u64) !types.Node {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "loadFlatCentroidDirectoryNodeFromStorage")) {
        return try self.loadFlatCentroidDirectoryNodeFromStorage(txn, node_id);
    }
    if (comptime @hasDecl(Index, "loadSearchNodeFromStorage")) {
        return try self.loadSearchNodeFromStorage(txn, node_id);
    }
    return try self.loadNode(txn, node_id);
}

fn directoryMatches(directory: *const FlatCentroidDirectory, root_node: u64, node_count: u64, publish_generation: u64) bool {
    return directory.root_node_snapshot == root_node and
        directory.node_count_snapshot == node_count and
        directory.publish_generation_snapshot == publish_generation;
}

fn snapshotStillPublished(self: anytype, snapshot: PublishedSnapshot) bool {
    const generation = publishedGenerationSnapshot(self);
    return (generation & 1) == 0 and
        generation == snapshot.publish_generation and
        publishedRootNodeSnapshot(self) == snapshot.root_node and
        publishedNodeCountSnapshot(self) == snapshot.node_count;
}

fn finishFlatCentroidBuildIfSupported(
    self: anytype,
    generation: u64,
    outcome: FlatCentroidBuildOutcome,
) void {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "finishFlatCentroidDirectoryBuild")) {
        self.finishFlatCentroidDirectoryBuild(generation, outcome);
    }
}

fn flatCentroidBuildFailureOutcome(err: anyerror) FlatCentroidBuildOutcome {
    return switch (err) {
        // A request-local cancellation or publication race says nothing about
        // whether another waiter can build the same generation successfully.
        error.Cancelled, error.StalePublishedSnapshot => .retry,
        else => .{ .failed = err },
    };
}

fn appendFlatCentroidBlock(
    self: anytype,
    blocks: *std.ArrayListUnmanaged(FlatCentroidBlock),
    posting_ids: []const u64,
    covering_radii: []const f32,
    centroids: []const f32,
    dims: usize,
) !void {
    if (posting_ids.len == 0) return;
    const zero = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(zero);
    @memset(zero, 0);

    const ids = try self.alloc.dupe(u64, posting_ids);
    errdefer self.alloc.free(ids);
    const radii = try self.alloc.dupe(f32, covering_radii);
    errdefer self.alloc.free(radii);
    const encoding: FlatCentroidBlock.Encoding = switch (self.config.centroid_directory_mode) {
        .auto, .flat_exact => blk: {
            const vectors = try self.alloc.dupe(f32, centroids);
            errdefer self.alloc.free(vectors);
            const measures = try self.alloc.alloc(f32, posting_ids.len);
            errdefer self.alloc.free(measures);
            for (measures, 0..) |*measure, i| {
                const centroid = vectors[i * dims ..][0..dims];
                measure.* = switch (self.config.metric) {
                    .l2_squared => vec.dot(centroid, centroid),
                    .cosine => vec.norm(centroid),
                    .inner_product => 0,
                };
            }
            break :blk .{ .exact = .{ .vectors = vectors, .measures = measures } };
        },
        .hbc, .flat_rabitq => blk: {
            var quantized = try self.quantizer.quantize(zero, centroids, posting_ids.len);
            errdefer quantized.deinit(self.alloc);
            break :blk .{ .rabitq = quantized };
        },
    };
    try blocks.append(self.alloc, .{
        .posting_ids = ids,
        .covering_radii = radii,
        .encoding = encoding,
    });
}

/// Chooses the routing representation from index scale, not request effort.
/// The exact directory wins once topology traversal becomes pointer-heavy;
/// small indexes keep the tree and avoid paying for a complete centroid scan.
pub fn usesFlatCentroidDirectory(self: anytype) bool {
    if (!self.config.use_quantization) return false;
    return switch (self.config.centroid_directory_mode) {
        .hbc => false,
        .flat_rabitq, .flat_exact => true,
        .auto => blk: {
            const Index = comptime @TypeOf(self.*);
            const active_count = if (comptime @hasDecl(Index, "publishedActiveCount"))
                self.publishedActiveCount()
            else
                self.metadata.active_count;
            const leaf_size = @max(@as(u64, self.config.leaf_size), 1);
            // Use the completed-leaf scale here. A partially filled final
            // leaf should not switch the whole index to a flat directory one
            // batch before the configured posting boundary.
            const estimated_postings = active_count / leaf_size;
            break :blk estimated_postings >= self.config.flat_exact_min_postings;
        },
    };
}

test "automatic centroid routing switches only at the posting scale boundary" {
    const TestIndex = struct {
        config: types.HBCConfig,
        active_count: u64,

        pub fn publishedActiveCount(self: *const @This()) u64 {
            return self.active_count;
        }
    };

    var index: TestIndex = .{
        .config = .{
            .dims = 8,
            .leaf_size = 100,
            .centroid_directory_mode = .auto,
            .flat_exact_min_postings = 1024,
        },
        .active_count = 102_399,
    };
    try std.testing.expect(!usesFlatCentroidDirectory(&index));
    index.active_count = 102_400;
    try std.testing.expect(usesFlatCentroidDirectory(&index));
    index.config.centroid_directory_mode = .hbc;
    try std.testing.expect(!usesFlatCentroidDirectory(&index));
    index.config.centroid_directory_mode = .flat_exact;
    index.active_count = 1;
    try std.testing.expect(usesFlatCentroidDirectory(&index));
}

fn buildFlatCentroidDirectory(
    self: anytype,
    txn: anytype,
    root_node: u64,
    node_count: u64,
    publish_generation: u64,
    cancellation: ?search_types.CancellationToken,
) !FlatCentroidDirectory {
    const dims: usize = @intCast(self.config.dims);
    const block_size = @max(self.config.flat_centroid_block_size, @as(usize, 1));
    var blocks = std.ArrayListUnmanaged(FlatCentroidBlock).empty;
    errdefer {
        for (blocks.items) |*block| block.deinit(self.alloc);
        blocks.deinit(self.alloc);
    }

    var posting_ids = try self.alloc.alloc(u64, block_size);
    defer self.alloc.free(posting_ids);
    var centroids = try self.alloc.alloc(f32, block_size * dims);
    defer self.alloc.free(centroids);
    var covering_radii = try self.alloc.alloc(f32, block_size);
    defer self.alloc.free(covering_radii);
    var pending = std.ArrayListUnmanaged(u64).empty;
    defer pending.deinit(self.alloc);
    // Published node ids are bounded by node_count. A dense bitset is exact,
    // has predictable resource use, and avoids an O(nodes) hash-table peak on
    // cold large-index directory construction.
    const visited_word_bits = @bitSizeOf(usize);
    const visited_words_u64 = std.math.divCeil(u64, node_count, visited_word_bits) catch return error.OutOfMemory;
    const visited_word_count = std.math.cast(usize, visited_words_u64) orelse return error.OutOfMemory;
    const visited_words = try self.alloc.alloc(usize, visited_word_count);
    defer self.alloc.free(visited_words);
    @memset(visited_words, 0);

    var block_count: usize = 0;
    var posting_count: usize = 0;
    var missing_node_count: usize = 0;
    var invalid_posting_count: usize = 0;

    if (root_node == 0 or root_node > node_count) {
        invalid_posting_count += 1;
    } else {
        const zero_based = root_node - 1;
        const visited_word_index: usize = @intCast(zero_based / visited_word_bits);
        const visited_bit_index: std.math.Log2Int(usize) = @intCast(zero_based % visited_word_bits);
        visited_words[visited_word_index] |= @as(usize, 1) << visited_bit_index;
        try pending.append(self.alloc, root_node);
    }

    var cursor: usize = 0;
    while (cursor < pending.items.len) : (cursor += 1) {
        const node_id = pending.items[cursor];
        if (cursor % 64 == 0) {
            if (cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        }
        var node = loadPublishedNode(self, txn, node_id) catch |err| {
            if (isNotFoundGeneric(err)) {
                missing_node_count += 1;
                continue;
            }
            return err;
        };
        defer node.deinit(self.alloc);
        if (!node.is_leaf) {
            for (node.children) |child_id| {
                if (child_id == 0 or child_id > node_count) {
                    invalid_posting_count += 1;
                    continue;
                }
                const zero_based = child_id - 1;
                const visited_word_index: usize = @intCast(zero_based / visited_word_bits);
                const visited_bit_index: std.math.Log2Int(usize) = @intCast(zero_based % visited_word_bits);
                const visited_mask = @as(usize, 1) << visited_bit_index;
                if (visited_words[visited_word_index] & visited_mask != 0) {
                    // A valid HBC topology is a strict tree. Mark children when
                    // they are scheduled so corrupt duplicate edges and cycles
                    // cannot grow the traversal queue beyond node_count.
                    invalid_posting_count += 1;
                    continue;
                }
                visited_words[visited_word_index] |= visited_mask;
                try pending.append(self.alloc, child_id);
            }
            continue;
        }
        if (node.members.len == 0) continue;
        if (node.centroid.len != dims) {
            invalid_posting_count += 1;
            continue;
        }

        posting_ids[block_count] = node.id;
        covering_radii[block_count] = node.covering_radius;
        @memcpy(centroids[block_count * dims ..][0..dims], node.centroid);
        block_count += 1;
        posting_count += 1;

        if (block_count == block_size) {
            try appendFlatCentroidBlock(self, &blocks, posting_ids[0..block_count], covering_radii[0..block_count], centroids[0 .. block_count * dims], dims);
            block_count = 0;
        }
    }
    if (block_count > 0) {
        try appendFlatCentroidBlock(self, &blocks, posting_ids[0..block_count], covering_radii[0..block_count], centroids[0 .. block_count * dims], dims);
    }

    return .{
        .blocks = try blocks.toOwnedSlice(self.alloc),
        .root_node_snapshot = root_node,
        .node_count_snapshot = node_count,
        .publish_generation_snapshot = publish_generation,
        .posting_count = posting_count,
        .missing_node_count = missing_node_count,
        .invalid_posting_count = invalid_posting_count,
    };
}

fn loadOrBuildFlatCentroidDirectory(self: anytype, txn: anytype, root_node: u64, node_count: u64, publish_generation: u64) !FlatCentroidDirectory {
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "loadPersistedFlatCentroidDirectory")) {
        if (try self.loadPersistedFlatCentroidDirectory(txn, root_node, node_count, publish_generation)) |persisted| {
            return persisted;
        }
    }
    return try buildFlatCentroidDirectory(self, txn, root_node, node_count, publish_generation, null);
}

pub fn clearFlatCentroidDirectory(self: anytype) void {
    var stale: ?*FlatCentroidDirectory = null;
    lockAtomicMutex(&self.flat_centroid_mu);
    stale = self.flat_centroid_directory;
    self.flat_centroid_directory = null;
    self.flat_centroid_mu.unlock();
    if (stale) |directory| directory.release(self.alloc);
}

fn acquireFlatCentroidDirectory(
    self: anytype,
    txn: anytype,
    expected_snapshot: ?PublishedSnapshot,
    cancellation: ?search_types.CancellationToken,
) !*FlatCentroidDirectory {
    const Index = comptime @TypeOf(self.*);
    while (true) {
        // Complete callers retain the generation-bound transaction supplied by
        // their snapshot. Best-effort callers open a fresh runtime transaction
        // for the elected build owner below.
        const snapshot = expected_snapshot orelse try loadStablePublishedSnapshot(self, cancellation);

        var stale: ?*FlatCentroidDirectory = null;
        lockAtomicMutex(&self.flat_centroid_mu);
        if (self.flat_centroid_directory) |directory| {
            if (directoryMatches(directory, snapshot.root_node, snapshot.node_count, snapshot.publish_generation)) {
                directory.retain();
                self.flat_centroid_mu.unlock();
                return directory;
            }
        }
        self.flat_centroid_mu.unlock();

        if (comptime @hasDecl(Index, "beginFlatCentroidDirectoryBuild")) {
            switch (try self.beginFlatCentroidDirectoryBuild(snapshot.publish_generation, cancellation)) {
                .owner => {},
                .retry => continue,
                .ready => |directory| {
                    // The adapter keys flights by generation, but retain the
                    // full snapshot check at this generic boundary. This
                    // keeps a buggy or wrapped generation from handing a
                    // caller a directory for different topology.
                    if (directoryMatches(directory, snapshot.root_node, snapshot.node_count, snapshot.publish_generation)) {
                        return directory;
                    }
                    directory.release(self.alloc);
                    continue;
                },
            }
        }
        var build_flight_open = comptime @hasDecl(Index, "finishFlatCentroidDirectoryBuild");
        var build_outcome: FlatCentroidBuildOutcome = .retry;
        defer if (build_flight_open) finishFlatCentroidBuildIfSupported(self, snapshot.publish_generation, build_outcome);
        errdefer |err| build_outcome = flatCentroidBuildFailureOutcome(err);

        var build_reservation: FlatCentroidBuildReservation = .{};
        if (comptime @hasDecl(Index, "reserveFlatCentroidDirectoryBuildBytes")) {
            build_reservation = try self.reserveFlatCentroidDirectoryBuildBytes(
                try projectedFlatCentroidDirectoryBuildBytes(
                    snapshot.node_count,
                    @intCast(self.config.dims),
                    self.config.flat_centroid_block_size,
                    @max(self.config.leaf_size, self.config.branching_factor),
                ),
            );
        }
        var build_reservation_open = !build_reservation.empty();
        errdefer if (comptime @hasDecl(Index, "releaseFlatCentroidDirectoryBuildBytes")) {
            if (build_reservation_open) self.releaseFlatCentroidDirectoryBuildBytes(build_reservation);
        };

        const built = try self.alloc.create(FlatCentroidDirectory);
        errdefer self.alloc.destroy(built);
        if (expected_snapshot != null) {
            built.* = try buildFlatCentroidDirectory(self, txn, snapshot.root_node, snapshot.node_count, snapshot.publish_generation, cancellation);
        } else if (comptime @hasDecl(Index, "beginRuntimeSearchTxn")) {
            var build_txn = try self.beginRuntimeSearchTxn();
            defer build_txn.abort();
            built.* = try loadOrBuildFlatCentroidDirectory(self, &build_txn, snapshot.root_node, snapshot.node_count, snapshot.publish_generation);
        } else {
            built.* = try loadOrBuildFlatCentroidDirectory(self, txn, snapshot.root_node, snapshot.node_count, snapshot.publish_generation);
        }
        errdefer built.deinit(self.alloc);
        if (comptime @hasDecl(Index, "accountFlatCentroidDirectory")) {
            try self.accountFlatCentroidDirectory(built, build_reservation);
            build_reservation_open = false;
        } else if (comptime @hasDecl(Index, "releaseFlatCentroidDirectoryBuildBytes")) {
            if (build_reservation_open) {
                self.releaseFlatCentroidDirectoryBuildBytes(build_reservation);
                build_reservation_open = false;
            }
        }

        if (expected_snapshot == null) {
            const current = try loadStablePublishedSnapshot(self, cancellation);
            if (current.root_node != snapshot.root_node or
                current.node_count != snapshot.node_count or
                current.publish_generation != snapshot.publish_generation)
            {
                built.deinit(self.alloc);
                self.alloc.destroy(built);
                if (build_flight_open) finishFlatCentroidBuildIfSupported(self, snapshot.publish_generation, .retry);
                build_flight_open = false;
                continue;
            }
        }

        lockAtomicMutex(&self.flat_centroid_mu);
        if (expected_snapshot != null and !snapshotStillPublished(self, snapshot)) {
            self.flat_centroid_mu.unlock();
            if (build_flight_open) finishFlatCentroidBuildIfSupported(self, snapshot.publish_generation, .{ .ready = built });
            build_flight_open = false;
            return built;
        }
        if (self.flat_centroid_directory) |directory| {
            if (directoryMatches(directory, snapshot.root_node, snapshot.node_count, snapshot.publish_generation)) {
                directory.retain();
                self.flat_centroid_mu.unlock();
                built.deinit(self.alloc);
                self.alloc.destroy(built);
                if (build_flight_open) finishFlatCentroidBuildIfSupported(self, snapshot.publish_generation, .{ .ready = directory });
                build_flight_open = false;
                return directory;
            }
            stale = directory;
            self.flat_centroid_directory = null;
        } else {
            stale = null;
        }
        built.retain();
        self.flat_centroid_directory = built;
        self.flat_centroid_mu.unlock();
        if (stale) |directory| directory.release(self.alloc);
        if (build_flight_open) finishFlatCentroidBuildIfSupported(self, snapshot.publish_generation, .{ .ready = built });
        build_flight_open = false;
        return built;
    }
}

/// Scores one immutable flat directory and retains only its best bounded
/// frontier. This avoids sorting and allocating every posting at large scale;
/// the caller's ANN effort remains the hard limit on leaves actually scored.
pub fn selectFlatPostingsAlloc(
    self: anytype,
    txn: anytype,
    query: []const f32,
    max_postings: usize,
    scratch_handle: anytype,
    profile: *search_types.SearchProfile,
    coverage_policy: search_types.CoveragePolicy,
    expected_snapshot: ?PublishedSnapshot,
    cancellation: ?search_types.CancellationToken,
    now_fn_u64: fn () u64,
    elapsed_fn_u64: fn (u64) u64,
) !FlatCentroidSelection {
    const scratch = &scratch_handle.scratch;
    const start = now_fn_u64();
    const directory = try acquireFlatCentroidDirectory(self, txn, expected_snapshot, cancellation);
    defer directory.release(self.alloc);
    defer profile.child_expand_ns += elapsed_fn_u64(start);

    var posting_count: usize = 0;
    var max_block_count: usize = 0;
    for (directory.blocks) |*block| {
        max_block_count = @max(max_block_count, block.posting_ids.len);
        for (block.posting_ids) |posting_id| {
            if (postingBitIsSet(block.shadowed_posting_bits, posting_id)) continue;
            posting_count = std.math.add(usize, posting_count, 1) catch return error.OutOfMemory;
        }
    }
    std.debug.assert(posting_count == directory.posting_count);
    // Complete-snapshot searches promise exhaustive coverage of the
    // published directory. Approximate searches retain the bounded ANN
    // frontier so their latency remains governed by the caller's effort.
    // Approximate requests retain only their governed ANN frontier. The
    // complete-directory radius experiment produced no certified stops on
    // either qualification corpus, while multiplying per-query scratch by
    // the posting count and exhausting the public query budget under normal
    // concurrency. Complete-snapshot validation remains exhaustive.
    const selection_limit = flatSelectionLimit(coverage_policy, max_postings, posting_count);
    const needs_merge = cancellation != null and selection_limit > cancellable_flat_sort_chunk_size;
    const previous_accounted_bytes = scratch_handle.accounted_bytes;
    const Index = comptime @TypeOf(self.*);
    if (comptime @hasDecl(Index, "reserveSearchScratchBytes")) {
        const target_bytes = try scratch.projectedBytesWithFlatProbeCapacity(
            selection_limit,
            needs_merge,
            max_block_count,
        );
        try self.reserveSearchScratchBytes(scratch_handle, target_bytes);
    }
    errdefer if (comptime @hasDecl(Index, "rollbackSearchScratchBytes")) {
        self.rollbackSearchScratchBytes(scratch_handle, previous_accounted_bytes);
    };
    try scratch.ensureFlatProbeCapacity(self.alloc, selection_limit, needs_merge);
    const selected = scratch.flat_probes[0..selection_limit];
    var selected_count: usize = 0;
    const query_measure: f32 = switch (self.config.metric) {
        .l2_squared => vec.dot(query, query),
        .cosine => vec.norm(query),
        .inner_product => 0,
    };
    if (coverage_policy == .complete_snapshot and !directory.complete()) {
        return error.IncompletePublishedSnapshot;
    }

    for (directory.blocks) |*block| {
        try checkCancellation(cancellation);
        const count = block.posting_ids.len;
        try scratch.ensureFlatCentroidScoreCapacity(self.alloc, count);
        const distances = scratch.distances[0..count];
        const error_bounds = scratch.error_bounds[0..count];
        switch (block.encoding) {
            .rabitq => |quantized| try self.quantizer.estimateDistancesWithScratch(&quantized, query, distances, error_bounds, &scratch.estimate),
            .exact => |exact| {
                const dims: usize = @intCast(self.config.dims);
                for (distances, error_bounds, exact.measures, 0..) |*distance, *error_bound, measure, i| {
                    const centroid = exact.vectors[i * dims ..][0..dims];
                    const dot = vec.dot(query, centroid);
                    distance.* = switch (self.config.metric) {
                        .l2_squared => @max(@as(f32, 0), query_measure + measure - 2.0 * dot),
                        .cosine => if (query_measure == 0 or measure == 0) 1 else 1.0 - dot / (query_measure * measure),
                        .inner_product => -dot,
                    };
                    error_bound.* = 0;
                }
            },
        }
        for (block.posting_ids, 0..) |posting_id, i| {
            if ((i & 0xff) == 0) try checkCancellation(cancellation);
            if (postingBitIsSet(block.shadowed_posting_bits, posting_id)) continue;
            const member_lower_bound = flatMemberLowerBound(
                self.config.metric,
                distances[i],
                error_bounds[i],
                block.covering_radii[i],
            );
            const candidate: FlatCentroidProbe = .{
                .posting_id = posting_id,
                .distance = distances[i],
                .error_bound = error_bounds[i],
                .member_lower_bound = member_lower_bound orelse -std.math.inf(f32),
                .bound_resolved = member_lower_bound != null,
            };
            insertFlatProbe(selected, &selected_count, candidate);
        }
    }

    const probes = selected[0..selected_count];
    try sortFlatProbesCancellable(probes, scratch.flat_probe_merge, cancellation);
    // A suffix bound proves stopping only when the selection contains the
    // complete directory. A bounded top frontier deliberately leaves omitted
    // postings unresolved, preserving approximate-search correctness.
    if (selection_limit == posting_count) populateFlatProbeSuffixBounds(probes);
    profile.approx_nodes_expanded += @intCast(directory.blocks.len);
    return .{ .probes = probes, .total_postings = posting_count };
}

fn recomputeAncestorCentroids(
    self: anytype,
    txn: anytype,
    start_parent_id: u64,
    options: hbc_runtime.BatchInsertOptions,
) !void {
    var parent_id = start_parent_id;
    while (parent_id != 0) {
        var parent = try self.loadNode(txn, parent_id);
        defer parent.deinit(self.alloc);
        try self.recomputeInternalCentroid(txn, &parent);
        try self.saveNodeWithOptionsMode(txn, &parent, options, false);
        parent_id = parent.parent;
    }
}

pub fn repairDirtyPostingsTxn(self: anytype, txn: anytype) !posting.PostingMaintenanceResult {
    return try repairDirtyPostingsTxnWithOptions(self, txn, .{});
}

pub fn postingBacklogStatsTxn(self: anytype, txn: anytype) !posting.PostingBacklogStats {
    var result: posting.PostingBacklogStats = .{};

    var node_id: u64 = 1;
    while (node_id <= self.metadata.node_count) : (node_id += 1) {
        var node = self.loadNode(txn, node_id) catch |err| {
            if (isNotFoundGeneric(err)) {
                result.skipped_missing += 1;
                continue;
            }
            return err;
        };
        defer node.deinit(self.alloc);

        result.scanned_nodes += 1;
        if (!node.is_leaf) continue;
        result.scanned_postings += 1;

        const state = node.posting_state;
        result.max_mutation_version = @max(result.max_mutation_version, state.mutation_version);
        if (!state.dirty) continue;

        result.dirty_postings += 1;
        if (state.centroid_dirty) {
            result.centroid_dirty_postings += 1;
            result.max_centroid_version_lag = @max(
                result.max_centroid_version_lag,
                state.mutation_version -| state.centroid_version,
            );
        }
        if (state.payload_dirty) {
            result.payload_dirty_postings += 1;
            result.max_payload_version_lag = @max(
                result.max_payload_version_lag,
                state.mutation_version -| state.payload_version,
            );
        }
    }

    return result;
}

pub fn runAutoPostingMaintenanceTxn(self: anytype, txn: anytype) !void {
    const max_postings = self.config.auto_posting_maintenance_max_postings;
    if (max_postings == 0) return;
    _ = try repairDirtyPostingsTxnWithOptions(self, txn, .{ .max_postings = max_postings });
}

fn postingQuantizedPayloadValid(self: anytype, txn: anytype, node: *const types.Node) !bool {
    if (!self.config.use_quantization or !node.is_leaf) return true;
    const Index = switch (@typeInfo(@TypeOf(self))) {
        .pointer => |pointer| pointer.child,
        else => @TypeOf(self),
    };
    // A native immutable generation deliberately omits per-posting protobuf
    // values from the generic namespace. Validate its borrowed directory view
    // before falling back to the mutable/WAL overlay representation; treating
    // that intentional absence as corruption rewrites every leaf at each
    // stable source tip.
    if (comptime @hasDecl(Index, "loadNativeQuantizedView")) {
        if (try self.loadNativeQuantizedView(txn, node.id, node.parent == 0, node.members.len)) |native| {
            return switch (native) {
                .nonquant => |set| set.vectors.dims == self.config.dims and
                    set.getCount() == node.members.len and
                    set.vectors.data.len == node.members.len * self.config.dims,
                .rabit => |set| blk: {
                    const code_width = std.math.cast(usize, set.codes.width) orelse break :blk false;
                    const expected_codes = std.math.mul(usize, node.members.len, code_width) catch break :blk false;
                    break :blk set.metric == self.config.metric and
                        set.centroid.len == self.config.dims and
                        set.getCount() == node.members.len and
                        set.codes.count == node.members.len and
                        set.codes.data.len == expected_codes and
                        set.code_counts.len == node.members.len and
                        set.centroid_distances.len == node.members.len and
                        set.quantized_dot_products.len == node.members.len and
                        (self.config.metric == .l2_squared or set.centroid_dot_products.len == node.members.len);
                },
            };
        }
    }
    var key_buf: [10]u8 = undefined;
    const data = self.getNamespaced(txn, .quant, hbc.encodeQuantKey(&key_buf, node.id)) catch |err| {
        // Empty postings have no quantized payload by construction:
        // refreshQuantizedWithOptions deletes the key when count == 0. Treat
        // that canonical absence as valid or stable-tip validation repeatedly
        // "repairs" the empty root back to the same absent state forever.
        if (isNotFoundGeneric(err)) return node.members.len == 0;
        return err;
    };
    if (node.parent == 0) {
        var set = proto.NonQuantizedVectorSet.decode(self.alloc, data) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return false,
        };
        defer set.deinit(self.alloc);
        const dims = std.math.cast(usize, set.vectors.dims) orelse return false;
        const expected_values = std.math.mul(usize, node.members.len, self.config.dims) catch return false;
        return dims == self.config.dims and
            set.getCount() == node.members.len and
            set.vectors.data.len == expected_values;
    }

    var set = proto.RaBitQuantizedVectorSet.decode(self.alloc, data) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return false,
    };
    defer set.deinit(self.alloc);
    const code_width = std.math.cast(usize, set.codes.width) orelse return false;
    const expected_code_bytes = std.math.mul(usize, node.members.len, code_width) catch return false;
    return set.metric == self.config.metric and
        set.centroid.len == self.config.dims and
        set.getCount() == node.members.len and
        set.codes.count == node.members.len and
        set.codes.data.len == expected_code_bytes and
        set.code_counts.len == node.members.len and
        set.centroid_distances.len == node.members.len and
        set.quantized_dot_products.len == node.members.len and
        (self.config.metric == .l2_squared or set.centroid_dot_products.len == node.members.len);
}

fn replaceOwnedU64Slice(alloc: std.mem.Allocator, target: *[]u64, replacement: *?[]u64) void {
    const owned = replacement.* orelse unreachable;
    if (target.*.len > 0) alloc.free(target.*);
    target.* = owned;
    replacement.* = null;
}

fn removeUniqueChildLinkAlloc(
    alloc: std.mem.Allocator,
    children: []const u64,
    child_id: u64,
) !?[]u64 {
    var match_count: usize = 0;
    for (children) |cid| {
        if (cid == child_id) match_count += 1;
    }
    // A merge is safe only when the recorded parent owns exactly one link to
    // the leaf. Zero links are a stale parent pointer; duplicate links are
    // tree corruption. In either case, leave both postings untouched for the
    // tree-link repair sweep instead of sizing a len-1 array incorrectly.
    if (match_count != 1) return null;

    const replacement = try alloc.alloc(u64, children.len - 1);
    errdefer alloc.free(replacement);
    var wi: usize = 0;
    for (children) |cid| {
        if (cid == child_id) continue;
        replacement[wi] = cid;
        wi += 1;
    }
    std.debug.assert(wi == replacement.len);
    return replacement;
}

fn noteTreeLinkInconsistencyIfSupported(self: anytype) void {
    const Index = comptime switch (@typeInfo(@TypeOf(self))) {
        .pointer => |pointer| pointer.child,
        else => @TypeOf(self),
    };
    if (comptime @hasDecl(Index, "noteTreeLinkInconsistency")) {
        self.noteTreeLinkInconsistency();
    }
}

fn mergeUnderfullPostingWithNearestSibling(
    self: anytype,
    txn: anytype,
    leaf: *const types.Node,
) !bool {
    if (!leaf.is_leaf or leaf.parent == 0 or leaf.members.len == 0) return false;
    if (leaf.members.len >= self.minLeafOccupancy()) return false;

    var parent = try self.loadNode(txn, leaf.parent);
    defer parent.deinit(self.alloc);
    try parent.ensureUnbacked(self.alloc);

    var new_children: ?[]u64 = try removeUniqueChildLinkAlloc(self.alloc, parent.children, leaf.id);
    defer if (new_children) |owned| self.alloc.free(owned);
    if (new_children == null) {
        std.log.warn(
            "hbc: underfull leaf {d} is not uniquely linked by recorded parent {d}; skipping merge",
            .{ leaf.id, leaf.parent },
        );
        noteTreeLinkInconsistencyIfSupported(self);
        return false;
    }

    var best_sibling_id: u64 = 0;
    var best_dist: f32 = std.math.inf(f32);
    for (parent.children) |cid| {
        if (cid == leaf.id) continue;
        var sibling = try self.loadNode(txn, cid);
        defer sibling.deinit(self.alloc);
        if (!sibling.is_leaf) continue;
        if (sibling.members.len + leaf.members.len > self.config.leaf_size) continue;
        const dist = vec.distance(leaf.centroid, sibling.centroid, self.config.metric);
        if (dist < best_dist) {
            best_dist = dist;
            best_sibling_id = cid;
        }
    }
    if (best_sibling_id == 0) return false;

    var sibling = try self.loadNode(txn, best_sibling_id);
    defer sibling.deinit(self.alloc);
    try sibling.ensureUnbacked(self.alloc);

    const merged_len = sibling.members.len + leaf.members.len;
    var merged: ?[]u64 = try self.alloc.alloc(u64, merged_len);
    errdefer if (merged) |owned| self.alloc.free(owned);
    @memcpy(merged.?[0..sibling.members.len], sibling.members);
    @memcpy(merged.?[sibling.members.len..], leaf.members);
    replaceOwnedU64Slice(self.alloc, &sibling.members, &merged);
    posting.PostingStore.noteMembersChanged(&sibling);
    try posting.PostingStore.recomputeCentroid(self, txn, &sibling);
    if (self.config.use_quantization) {
        const refresh_options: hbc_runtime.BatchInsertOptions = .{};
        try self.refreshQuantizedWithOptions(txn, &sibling, refresh_options);
    }
    posting.PostingStore.notePayloadRefreshed(&sibling);
    const save_options: hbc_runtime.BatchInsertOptions = .{};
    try self.saveNodeWithOptionsMode(txn, &sibling, save_options, false);

    for (leaf.members) |mid| try self.putVecLeaf(txn, mid, best_sibling_id);

    replaceOwnedU64Slice(self.alloc, &parent.children, &new_children);
    try self.recomputeInternalCentroid(txn, &parent);
    try self.saveNodeWithOptionsMode(txn, &parent, save_options, false);
    try self.deleteNode(txn, leaf.id);
    try self.collapseSingleChildParents(txn, leaf.parent);
    return true;
}

const BoundaryMove = struct {
    vector_id: u64,
    from_index: usize,
    to_index: usize,
};

fn targetedBoundaryReassignParent(
    self: anytype,
    txn: anytype,
    parent_id: u64,
    max_moves: usize,
    min_improvement: f32,
) !usize {
    if (parent_id == 0 or max_moves == 0) return 0;

    var parent = self.loadNode(txn, parent_id) catch |err| {
        if (isNotFoundGeneric(err)) return 0;
        return err;
    };
    defer parent.deinit(self.alloc);
    if (parent.is_leaf or parent.children.len < 2) return 0;

    var leaves = try self.alloc.alloc(types.Node, parent.children.len);
    var initialized: usize = 0;
    defer {
        for (leaves[0..initialized]) |*leaf| leaf.deinit(self.alloc);
        self.alloc.free(leaves);
    }

    for (parent.children) |child_id| {
        var child = self.loadNode(txn, child_id) catch |err| {
            if (isNotFoundGeneric(err)) continue;
            return err;
        };
        if (!child.is_leaf) {
            child.deinit(self.alloc);
            return 0;
        }
        try child.ensureUnbacked(self.alloc);
        leaves[initialized] = child;
        initialized += 1;
    }
    if (initialized < 2) return 0;

    const moves_cap = @min(max_moves, @as(usize, @intCast(std.math.maxInt(u32))));
    var moves = try self.alloc.alloc(BoundaryMove, moves_cap);
    defer self.alloc.free(moves);
    const planned_out = try self.alloc.alloc(usize, initialized);
    defer self.alloc.free(planned_out);
    @memset(planned_out, 0);

    const dims: usize = @intCast(self.config.dims);
    const raw_scratch = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(raw_scratch);
    const transformed = try self.alloc.alloc(f32, dims);
    defer self.alloc.free(transformed);

    var move_count: usize = 0;
    const min_source_members = self.minLeafOccupancy();
    for (leaves[0..initialized], 0..) |*source, source_index| {
        if (move_count >= moves.len) break;
        if (source.members.len <= min_source_members) continue;
        for (source.members) |member_id| {
            if (move_count >= moves.len) break;
            if (source.members.len - planned_out[source_index] <= min_source_members) break;

            const raw = try self.getVectorScratch(txn, member_id, raw_scratch);
            _ = self.transformVector(raw, transformed);
            const current_dist = vec.distance(source.centroid, transformed, self.config.metric);

            var best_index = source_index;
            var best_dist = current_dist;
            for (leaves[0..initialized], 0..) |*candidate, candidate_index| {
                if (candidate_index == source_index) continue;
                if (candidate.members.len >= self.config.leaf_size) continue;
                const dist = vec.distance(candidate.centroid, transformed, self.config.metric);
                if (dist + min_improvement < best_dist) {
                    best_dist = dist;
                    best_index = candidate_index;
                }
            }
            if (best_index == source_index) continue;

            moves[move_count] = .{
                .vector_id = member_id,
                .from_index = source_index,
                .to_index = best_index,
            };
            move_count += 1;
            planned_out[source_index] += 1;
        }
    }
    if (move_count == 0) return 0;

    const changed = try self.alloc.alloc(bool, initialized);
    defer self.alloc.free(changed);
    @memset(changed, false);

    var applied: usize = 0;
    for (moves[0..move_count]) |move| {
        if (move.from_index == move.to_index) continue;
        posting.PostingStore.removeMember(self.alloc, &leaves[move.from_index], move.vector_id) catch continue;
        _ = try posting.PostingStore.appendMember(self.alloc, &leaves[move.to_index], move.vector_id);
        try self.putVecLeaf(txn, move.vector_id, leaves[move.to_index].id);
        changed[move.from_index] = true;
        changed[move.to_index] = true;
        applied += 1;
    }
    if (applied == 0) return 0;

    for (leaves[0..initialized], 0..) |*leaf, i| {
        if (!changed[i]) continue;
        if (leaf.members.len == 0) {
            @memset(leaf.centroid, 0);
        } else {
            try posting.PostingStore.recomputeCentroid(self, txn, leaf);
        }
        if (self.config.use_quantization) {
            const refresh_options: hbc_runtime.BatchInsertOptions = .{};
            try self.refreshQuantizedWithOptions(txn, leaf, refresh_options);
        }
        posting.PostingStore.notePayloadRefreshed(leaf);
        const save_options: hbc_runtime.BatchInsertOptions = .{};
        try self.saveNodeWithOptionsMode(txn, leaf, save_options, false);
    }

    try parent.ensureUnbacked(self.alloc);
    try self.recomputeInternalCentroid(txn, &parent);
    const save_options: hbc_runtime.BatchInsertOptions = .{};
    try self.saveNodeWithOptionsMode(txn, &parent, save_options, false);
    return applied;
}

pub fn repairDirtyPostingsTxnWithOptions(
    self: anytype,
    txn: anytype,
    options: posting.PostingMaintenanceOptions,
) !posting.PostingMaintenanceResult {
    var result: posting.PostingMaintenanceResult = .{};
    if (options.max_postings == 0) {
        result.limit_reached = true;
        return result;
    }

    var layout_changes: usize = 0;
    var boundary_moves: usize = 0;

    var node_id: u64 = 1;
    while (node_id <= self.metadata.node_count) : (node_id += 1) {
        if (options.should_continue) |should_continue| {
            const context = options.continue_context orelse return error.MissingMaintenanceContinueContext;
            if (!should_continue(context)) {
                result.limit_reached = true;
                break;
            }
        }
        var node = self.loadNode(txn, node_id) catch |err| {
            if (isNotFoundGeneric(err)) {
                result.skipped_missing += 1;
                continue;
            }
            return err;
        };
        defer node.deinit(self.alloc);

        result.scanned_nodes += 1;
        if (!node.is_leaf) continue;
        result.scanned_postings += 1;

        if (options.validate_payloads and !try postingQuantizedPayloadValid(self, txn, &node)) {
            try node.ensureUnbacked(self.alloc);
            node.posting_state.mutation_version +|= 1;
            node.posting_state.dirty = true;
            node.posting_state.payload_dirty = true;
        }

        if (options.rebalance_layout and layout_changes < options.max_layout_changes) {
            if (node.members.len > self.config.leaf_size) {
                const old_parent_id = node.parent;
                const split_options: hbc_runtime.BatchInsertOptions = .{};
                try self.splitLeafWithOptions(txn, &node, split_options);
                result.split_postings += 1;
                layout_changes += 1;
                if (boundary_moves < options.max_boundary_reassignments) {
                    const parent_id = if (old_parent_id == 0) self.metadata.root_node else old_parent_id;
                    const moved = try targetedBoundaryReassignParent(
                        self,
                        txn,
                        parent_id,
                        options.max_boundary_reassignments - boundary_moves,
                        options.boundary_reassignment_min_improvement,
                    );
                    boundary_moves += moved;
                    result.boundary_reassigned_vectors += @intCast(moved);
                }
                continue;
            }

            if (try mergeUnderfullPostingWithNearestSibling(self, txn, &node)) {
                result.merged_postings += 1;
                layout_changes += 1;
                if (boundary_moves < options.max_boundary_reassignments) {
                    const moved = try targetedBoundaryReassignParent(
                        self,
                        txn,
                        node.parent,
                        options.max_boundary_reassignments - boundary_moves,
                        options.boundary_reassignment_min_improvement,
                    );
                    boundary_moves += moved;
                    result.boundary_reassigned_vectors += @intCast(moved);
                }
                continue;
            }
        } else if (options.rebalance_layout and layout_changes >= options.max_layout_changes) {
            result.limit_reached = true;
        }

        if (!node.posting_state.dirty) continue;

        result.dirty_postings += 1;
        if (result.repaired_postings >= options.max_postings) {
            result.limit_reached = true;
            break;
        }

        try node.ensureUnbacked(self.alloc);

        var refreshed_centroid = false;
        var refreshed_payload = false;
        if (node.posting_state.centroid_dirty) {
            try posting.PostingStore.recomputeCentroid(self, txn, &node);
            refreshed_centroid = true;
            result.centroid_refreshed += 1;
        }

        if (node.posting_state.payload_dirty and options.refresh_payloads) {
            if (self.config.use_quantization) {
                const quant_start = nowNsU64Fixed();
                try self.refreshQuantizedWithOptions(txn, &node, .{});
                self.write_profile.refresh_quantized_ns += elapsedSinceU64Fixed(quant_start);
            }
            posting.PostingStore.notePayloadRefreshed(&node);
            refreshed_payload = true;
            result.payload_refreshed += 1;
        }

        if (!node.posting_state.centroid_dirty and !node.posting_state.payload_dirty) {
            node.posting_state.dirty = false;
        }

        if (refreshed_centroid or refreshed_payload) {
            try savePackedNodeValue(self, txn, &node);
        }
        try posting.PostingStore.saveState(self, txn, node.id, node.posting_state);
        try self.cacheNode(&node);

        if (refreshed_centroid and options.refresh_ancestors and node.parent != 0) {
            try recomputeAncestorCentroids(self, txn, node.parent, .{});
            result.ancestor_refresh_roots += 1;
        }

        if (refreshed_centroid or refreshed_payload) {
            result.repaired_postings += 1;
        }
    }

    self.write_profile.posting_maintenance_scanned_nodes += result.scanned_nodes;
    self.write_profile.posting_maintenance_scanned_postings += result.scanned_postings;
    self.write_profile.posting_maintenance_dirty_postings += result.dirty_postings;
    self.write_profile.posting_maintenance_repaired_postings += result.repaired_postings;
    self.write_profile.posting_maintenance_centroid_refreshed += result.centroid_refreshed;
    self.write_profile.posting_maintenance_payload_refreshed += result.payload_refreshed;
    self.write_profile.posting_maintenance_ancestor_refresh_roots += result.ancestor_refresh_roots;
    self.write_profile.posting_maintenance_split_postings += result.split_postings;
    self.write_profile.posting_maintenance_merged_postings += result.merged_postings;
    self.write_profile.posting_maintenance_boundary_reassigned_vectors += result.boundary_reassigned_vectors;

    return result;
}

test "owned slice replacement survives later error cleanup" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var node = types.Node{
                .id = 1,
                .is_leaf = true,
                .level = 0,
                .parent = 0,
                .centroid = &.{},
                .children = &.{},
                .members = try alloc.dupe(u64, &.{ 1, 2 }),
            };
            defer node.deinit(alloc);

            var replacement: ?[]u64 = try alloc.dupe(u64, &.{ 3, 4, 5 });
            errdefer if (replacement) |owned| alloc.free(owned);
            replaceOwnedU64Slice(alloc, &node.members, &replacement);
            return error.InjectedFailure;
        }
    };

    try std.testing.expectError(error.InjectedFailure, Runner.run(std.testing.allocator));
}

test "underfull merge child unlink requires exactly one parent link" {
    const alloc = std.testing.allocator;

    try std.testing.expect((try removeUniqueChildLinkAlloc(alloc, &.{ 2, 3, 4 }, 9)) == null);
    try std.testing.expect((try removeUniqueChildLinkAlloc(alloc, &.{ 2, 3, 2 }, 2)) == null);

    const replacement = (try removeUniqueChildLinkAlloc(alloc, &.{ 2, 3, 4 }, 3)) orelse
        return error.TestUnexpectedResult;
    defer alloc.free(replacement);
    try std.testing.expectEqualSlices(u64, &.{ 2, 4 }, replacement);
}

test "posting backlog stats starts clean" {
    const TestIndex = struct {
        alloc: std.mem.Allocator,
        metadata: hbc.IndexMetadata = .{
            .dims = 0,
            .branching_factor = 0,
            .leaf_size = 0,
            .node_count = 0,
        },
        config: types.HBCConfig = .{ .dims = 0 },

        fn loadNode(_: *@This(), _: anytype, _: u64) !types.Node {
            return error.NotFound;
        }
    };
    var index = TestIndex{ .alloc = std.testing.allocator };
    const txn = {};
    const stats = try postingBacklogStatsTxn(&index, txn);
    try std.testing.expectEqual(@as(u64, 0), stats.scanned_nodes);
    try std.testing.expect(!stats.needsRepair());
}

test "flat centroid directory match includes publish generation" {
    const directory = FlatCentroidDirectory{
        .root_node_snapshot = 11,
        .node_count_snapshot = 42,
        .publish_generation_snapshot = 7,
    };

    try std.testing.expect(directoryMatches(&directory, 11, 42, 7));
    try std.testing.expect(!directoryMatches(&directory, 11, 42, 8));
}

test "flat centroid build failures retry only request-local outcomes" {
    for ([_]anyerror{ error.Cancelled, error.StalePublishedSnapshot }) |err| {
        switch (flatCentroidBuildFailureOutcome(err)) {
            .retry => {},
            else => return error.TestUnexpectedResult,
        }
    }
    for ([_]anyerror{ error.Canceled, error.ResourceBudgetExceeded }) |expected| {
        switch (flatCentroidBuildFailureOutcome(expected)) {
            .failed => |err| try std.testing.expectEqual(expected, err),
            else => return error.TestUnexpectedResult,
        }
    }
}
