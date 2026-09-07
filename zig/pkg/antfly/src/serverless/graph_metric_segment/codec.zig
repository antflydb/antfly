// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const bounded_decode = @import("../bounded_decode.zig");
const graph_mod = @import("../../graph/graph.zig");
const artifact_ref = @import("../manifest/artifact_ref.zig");
const types = @import("types.zig");

pub const wire_magic = "AFGM";
pub const wire_version: u16 = artifact_ref.graph_metric_segment_wire_version;
const fixed_header_len = wire_magic.len + @sizeOf(u16) + 4 * @sizeOf(u8) + @sizeOf(u32) +
    3 * @sizeOf(u64) + 3 * @sizeOf(u32);
const routing_magic = "AFGR";
// Footer: AFGR/count, flat entries grouped into authenticated 64-entry pages,
// AFGD directory, AFTK root, root-length/footer-length trailer. The root binds
// both the directory digest (sparse reads) and full point digest (verification).
const directory_magic = "AFGD";
pub const routing_page_entries: usize = 64;
const top_tier_magic = "AFTK";
pub const routing_trailer_len: usize = 8;
pub const score_block_entries: usize = 1024;
pub const ranked_score_block_entries: usize = 256;
pub const max_persisted_top_entries: usize = 10_000;
pub const max_routing_bytes: usize = 16 * 1024 * 1024;
pub const max_score_node_id_bytes: usize = 4096;
pub const max_ranked_score_block_bytes: usize = @sizeOf(u16) + max_score_node_id_bytes + ranked_score_block_entries * (ranked_score_fixed_len + max_score_node_id_bytes);
const routing_header_len = routing_magic.len + @sizeOf(u32);
const routing_entry_fixed_len = @sizeOf(u32) + @sizeOf(u64) + @sizeOf(u32) + std.crypto.hash.sha2.Sha256.digest_length;
const top_tier_header_len = top_tier_magic.len + @sizeOf(u32) + @sizeOf(u32);
const routing_root_metadata_len = 3 * @sizeOf(u64) + 64;
const routing_root_trailer_len = @sizeOf(u32) + routing_trailer_len;
const ranked_score_fixed_len = @sizeOf(u16) + @sizeOf(u64);
const ranked_routing_entry_len = @sizeOf(u64) + @sizeOf(u32) + std.crypto.hash.sha2.Sha256.digest_length;
const max_ranked_score_blocks = (max_persisted_top_entries + ranked_score_block_entries - 1) / ranked_score_block_entries;

pub const Header = struct {
    version: u16,
    kind: @import("../../graph/graph.zig").GraphMetricKind,
    materialization_state: types.MaterializationState,
    rejection_reason: types.RejectionReason,
    config_fingerprint: u64,
    materializer_fingerprint: u64,
    source_graph_artifact_id: []const u8,
    source_graph_checksum: []const u8,
    converged: bool,
    iterations_completed: u32,
    delta: f64,
    edge_type_count: u32,
};

pub const Control = struct {
    header: Header,
    score_count: u32,
    score_data_offset: u64,
};

pub const RoutingEntry = struct {
    /// Global score-block ordinal, including when only selected pages are loaded.
    block_index: usize = 0,
    first_node_id: []const u8,
    offset: u64,
    len: usize,
    checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8 = @splat(0),
};

pub const RankedRoutingEntry = struct {
    offset: u64,
    len: usize,
    checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8,
};

const PersistedTopScore = struct {
    node_id: []const u8,
    value: f64,
};

pub const ArtifactIntegrity = struct {
    control_len: u32,
    routing_footer_len: u32,
    control_checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    routing_checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8,
    point_index_checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8,
};

pub const RoutingIndex = struct {
    entries: []RoutingEntry,
    top_score_count: usize,
    ranked_entries: []RankedRoutingEntry,
    footer_offset: u64,
    primary_data_offset: u64 = 0,
    primary_data_end: u64 = 0,
    point_index_checksum: [32]u8 = @splat(0),
    directory_len: usize = 0,
    directory_checksum: [32]u8 = @splat(0),

    pub fn deinit(self: *RoutingIndex, alloc: Allocator) void {
        alloc.free(self.entries);
        alloc.free(self.ranked_entries);
        self.* = undefined;
    }

    pub fn find(self: RoutingIndex, node_id: []const u8) ?RoutingEntry {
        const index = self.findIndex(node_id) orelse return null;
        return self.entries[index];
    }

    pub fn findIndex(self: RoutingIndex, node_id: []const u8) ?usize {
        var low: usize = 0;
        var high = self.entries.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (std.mem.order(u8, self.entries[mid].first_node_id, node_id) != .gt)
                low = mid + 1
            else
                high = mid;
        }
        return if (low == 0) null else low - 1;
    }
};

pub const BorrowedBlockScore = struct {
    node_suffix: []const u8,
    value: f64,

    pub fn nodeIdLen(self: @This(), node_prefix: []const u8) usize {
        return node_prefix.len + self.node_suffix.len;
    }

    pub fn orderNode(self: @This(), node_prefix: []const u8, node_id: []const u8) std.math.Order {
        const shared_prefix = @min(node_prefix.len, node_id.len);
        const prefix_order = std.mem.order(u8, node_prefix[0..shared_prefix], node_id[0..shared_prefix]);
        if (prefix_order != .eq) return prefix_order;
        if (node_id.len < node_prefix.len) return .gt;
        return std.mem.order(u8, self.node_suffix, node_id[node_prefix.len..]);
    }

    pub fn eqlNode(self: @This(), node_prefix: []const u8, node_id: []const u8) bool {
        return self.nodeIdLen(node_prefix) == node_id.len and self.orderNode(node_prefix, node_id) == .eq;
    }

    pub fn copyNode(self: @This(), node_prefix: []const u8, destination: []u8) ![]u8 {
        if (destination.len < self.nodeIdLen(node_prefix)) return error.NoSpaceLeft;
        @memcpy(destination[0..node_prefix.len], node_prefix);
        @memcpy(destination[node_prefix.len..][0..self.node_suffix.len], self.node_suffix);
        return destination[0..self.nodeIdLen(node_prefix)];
    }

    pub fn dupeNodeAlloc(self: @This(), alloc: Allocator, node_prefix: []const u8) ![]u8 {
        const result = try alloc.alloc(u8, self.nodeIdLen(node_prefix));
        errdefer alloc.free(result);
        _ = try self.copyNode(node_prefix, result);
        return result;
    }
};

pub const DecodedScoreBlock = struct {
    /// Stored once per block rather than once per score, keeping the bounded
    /// decode frame compact even when node IDs have long shared prefixes.
    node_prefix: []const u8 = &.{},
    scores: [score_block_entries]BorrowedBlockScore = undefined,
    len: usize = 0,

    pub fn score(self: *const DecodedScoreBlock, node_id: []const u8) ?f64 {
        var low: usize = 0;
        var high = self.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (self.scores[mid].orderNode(self.node_prefix, node_id)) {
                .lt => low = mid + 1,
                .gt => high = mid,
                .eq => return self.scores[mid].value,
            }
        }
        return null;
    }
};

/// Exact bounded prefix needed to decode provenance from a current artifact.
/// Logical names are manifest metadata and deliberately do not affect the
/// content address.
pub fn headerProbeLen(
    artifact_byte_len: u64,
    source_graph_artifact_id: []const u8,
    source_graph_checksum: []const u8,
) !usize {
    var required: usize = fixed_header_len;
    required = std.math.add(usize, required, source_graph_artifact_id.len) catch return error.GraphMetricSegmentTooLarge;
    required = std.math.add(usize, required, source_graph_checksum.len) catch return error.GraphMetricSegmentTooLarge;
    const required_u64 = std.math.cast(u64, required) orelse return error.GraphMetricSegmentTooLarge;
    return @intCast(@min(artifact_byte_len, required_u64));
}

pub fn controlProbeLen(
    artifact_byte_len: u64,
    source_graph_artifact_id: []const u8,
    source_graph_checksum: []const u8,
    edge_filter: graph_mod.GraphMetricEdgeFilter,
) !usize {
    var required = try headerProbeLen(
        std.math.maxInt(u64),
        source_graph_artifact_id,
        source_graph_checksum,
    );
    for (edge_filter.types) |edge_type| {
        required = std.math.add(usize, required, @sizeOf(u32) + edge_type.len) catch return error.GraphMetricSegmentTooLarge;
    }
    required = std.math.add(usize, required, @sizeOf(u32)) catch return error.GraphMetricSegmentTooLarge;
    const required_u64 = std.math.cast(u64, required) orelse return error.GraphMetricSegmentTooLarge;
    return @intCast(@min(artifact_byte_len, required_u64));
}

/// Decodes the bounded control prefix used by point-score readers and checks
/// that the persisted edge filter is exactly the configured set.
pub fn decodeControl(data: []const u8, expected_edge_filter: graph_mod.GraphMetricEdgeFilter) !Control {
    const header = try decodeHeader(data);
    var pos = fixed_header_len;
    pos = std.math.add(usize, pos, header.source_graph_artifact_id.len) catch return error.InvalidGraphMetricSegment;
    pos = std.math.add(usize, pos, header.source_graph_checksum.len) catch return error.InvalidGraphMetricSegment;
    if (header.edge_type_count != expected_edge_filter.types.len) return error.InvalidGraphMetricSegment;
    if ((header.edge_type_count == 0) != (expected_edge_filter.mode == .all)) return error.InvalidGraphMetricSegment;
    var previous: ?[]const u8 = null;
    for (0..header.edge_type_count) |_| {
        const edge_type_len = try readInt(u32, data, &pos);
        const edge_type = try take(data, &pos, edge_type_len);
        if (edge_type.len == 0 or (previous != null and std.mem.order(u8, previous.?, edge_type) != .lt)) {
            return error.InvalidGraphMetricSegment;
        }
        var found = false;
        for (expected_edge_filter.types) |expected| {
            if (std.mem.eql(u8, expected, edge_type)) {
                found = true;
                break;
            }
        }
        if (!found) return error.InvalidGraphMetricSegment;
        previous = edge_type;
    }
    const score_count = try readInt(u32, data, &pos);
    return .{ .header = header, .score_count = score_count, .score_data_offset = @intCast(pos) };
}

pub fn routingFooterLenFromTrailer(artifact_byte_len: u64, trailer: []const u8) !usize {
    if (trailer.len != routing_trailer_len) return error.InvalidGraphMetricSegment;
    const footer_len_u64 = std.mem.readInt(u64, trailer[0..routing_trailer_len], .little);
    if (footer_len_u64 < routing_header_len + routing_trailer_len or
        footer_len_u64 > max_routing_bytes or footer_len_u64 > artifact_byte_len)
    {
        return error.InvalidGraphMetricSegment;
    }
    return std.math.cast(usize, footer_len_u64) orelse error.InvalidGraphMetricSegment;
}

/// Decodes a routing footer whose node-id slices borrow from `footer`.
pub fn decodeRoutingIndexAlloc(alloc: Allocator, footer: []const u8, artifact_byte_len: u64) !RoutingIndex {
    return decodeRoutingIndexForVersionWithCancellationAlloc(alloc, footer, artifact_byte_len, wire_version, .none);
}

pub fn decodeRoutingIndexWithCancellationAlloc(
    alloc: Allocator,
    footer: []const u8,
    artifact_byte_len: u64,
    cancellation: CancellationToken,
) !RoutingIndex {
    return decodeRoutingIndexForVersionWithCancellationAlloc(alloc, footer, artifact_byte_len, wire_version, cancellation);
}

pub fn decodeRoutingIndexForVersionWithCancellationAlloc(
    alloc: Allocator,
    footer: []const u8,
    artifact_byte_len: u64,
    segment_version: u16,
    cancellation: CancellationToken,
) !RoutingIndex {
    try cancellation.check();
    if (segment_version != wire_version) return error.UnsupportedGraphMetricSegmentVersion;
    if (footer.len < routing_header_len + routing_trailer_len) return error.InvalidGraphMetricSegment;
    const footer_len = try routingFooterLenFromTrailer(artifact_byte_len, footer[footer.len - routing_trailer_len ..]);
    if (footer_len != footer.len) return error.InvalidGraphMetricSegment;
    const footer_offset = artifact_byte_len - footer.len;
    const root_len = try routingRootLenFromTrailer(footer);
    const point_index = footer[0 .. footer.len - root_len];
    var root = try decodeRoutingRootAlloc(alloc, footer[footer.len - root_len ..], artifact_byte_len, segment_version, cancellation);
    errdefer root.deinit(alloc);
    var point_checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(point_index, &point_checksum, .{});
    if (!std.mem.eql(u8, &point_checksum, &root.point_index_checksum)) return error.InvalidGraphMetricSegment;
    var pos: usize = 0;
    if (!std.mem.eql(u8, try take(footer, &pos, routing_magic.len), routing_magic)) return error.InvalidGraphMetricSegment;
    const entry_count = try readInt(u32, footer, &pos);
    if (@as(usize, entry_count) > (footer.len - routing_header_len - routing_trailer_len) / routing_entry_fixed_len) {
        return error.InvalidGraphMetricSegment;
    }
    const entries = try alloc.alloc(RoutingEntry, entry_count);
    errdefer alloc.free(entries);
    var previous_end: ?u64 = null;
    for (entries, 0..) |*entry, entry_index| {
        if (entry_index % 256 == 0) try cancellation.check();
        const first_node_id_len = try readInt(u32, footer, &pos);
        if (first_node_id_len == 0 or first_node_id_len > max_score_node_id_bytes) return error.InvalidGraphMetricSegment;
        const offset = try readInt(u64, footer, &pos);
        const len_u32 = try readInt(u32, footer, &pos);
        var checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        @memcpy(&checksum, try take(footer, &pos, checksum.len));
        if (len_u32 == 0) return error.InvalidGraphMetricSegment;
        const first_node_id = try take(footer, &pos, first_node_id_len);
        const end = std.math.add(u64, offset, len_u32) catch return error.InvalidGraphMetricSegment;
        if (end > footer_offset or (previous_end != null and offset != previous_end.?)) return error.InvalidGraphMetricSegment;
        if (entry_index > 0 and std.mem.order(u8, entries[entry_index - 1].first_node_id, first_node_id) != .lt) {
            return error.InvalidGraphMetricSegment;
        }
        entry.* = .{ .block_index = entry_index, .first_node_id = first_node_id, .offset = offset, .len = len_u32, .checksum = checksum };
        previous_end = end;
    }
    if (pos + root.directory_len != point_index.len or (previous_end orelse root.primary_data_offset) != root.primary_data_end or
        (entries.len != 0 and entries[0].offset != root.primary_data_offset)) return error.InvalidGraphMetricSegment;
    try validateDirectory(point_index, pos, footer_offset, entry_count, root.directory_checksum, cancellation);
    try cancellation.check();
    alloc.free(root.entries);
    root.entries = entries;
    return root;
}

/// The root is bounded independently of primary vector cardinality.
/// Its trailer length is included in the manifest-authenticated digest.
pub fn routingRootLen(score_count: usize) usize {
    return top_tier_header_len + routing_root_metadata_len + routing_root_trailer_len +
        scoreBlockCountForSize(@min(score_count, max_persisted_top_entries), ranked_score_block_entries) * ranked_routing_entry_len;
}

fn routingRootLenFromTrailer(footer: []const u8) !usize {
    if (footer.len < routing_root_trailer_len) return error.InvalidGraphMetricSegment;
    const len = std.mem.readInt(u32, footer[footer.len - routing_root_trailer_len ..][0..4], .little);
    if (len < routingRootLen(0) or len > routingRootLen(max_persisted_top_entries) or len > footer.len) return error.InvalidGraphMetricSegment;
    return len;
}

pub fn decodeRoutingRootAlloc(alloc: Allocator, root: []const u8, artifact_byte_len: u64, version: u16, cancellation: CancellationToken) !RoutingIndex {
    try cancellation.check();
    if (version != wire_version) return error.UnsupportedGraphMetricSegmentVersion;
    if (try routingRootLenFromTrailer(root) != root.len) return error.InvalidGraphMetricSegment;
    const footer_len = try routingFooterLenFromTrailer(artifact_byte_len, root[root.len - routing_trailer_len ..]);
    if (footer_len < root.len + routing_header_len) return error.InvalidGraphMetricSegment;
    const footer_offset = artifact_byte_len - footer_len;
    var pos: usize = 0;
    if (!std.mem.eql(u8, try take(root, &pos, top_tier_magic.len), top_tier_magic)) return error.InvalidGraphMetricSegment;
    const count = try readInt(u32, root, &pos);
    const blocks = try readInt(u32, root, &pos);
    if (count > max_persisted_top_entries or root.len != routingRootLen(count) or blocks != scoreBlockCountForSize(count, ranked_score_block_entries)) return error.InvalidGraphMetricSegment;
    const primary_start = try readInt(u64, root, &pos);
    const primary_end = try readInt(u64, root, &pos);
    if (primary_start > primary_end or primary_end > footer_offset) return error.InvalidGraphMetricSegment;
    var point_checksum: [32]u8 = undefined;
    @memcpy(&point_checksum, try take(root, &pos, 32));
    const directory_len = std.math.cast(usize, try readInt(u64, root, &pos)) orelse return error.InvalidGraphMetricSegment;
    if (directory_len < routing_header_len or directory_len > footer_len - root.len - routing_header_len) return error.InvalidGraphMetricSegment;
    var directory_checksum: [32]u8 = undefined;
    @memcpy(&directory_checksum, try take(root, &pos, 32));
    const entries = try alloc.alloc(RankedRoutingEntry, blocks);
    errdefer alloc.free(entries);
    var next_offset = primary_end;
    for (entries) |*entry| {
        const offset = try readInt(u64, root, &pos);
        const len = try readInt(u32, root, &pos);
        if (offset != next_offset or len == 0 or len > max_ranked_score_block_bytes or len > footer_offset - next_offset) return error.InvalidGraphMetricSegment;
        entry.* = .{ .offset = offset, .len = len, .checksum = undefined };
        @memcpy(&entry.checksum, try take(root, &pos, 32));
        next_offset += len;
    }
    if (next_offset != footer_offset or pos + routing_root_trailer_len != root.len) return error.InvalidGraphMetricSegment;
    return .{ .entries = try alloc.alloc(RoutingEntry, 0), .ranked_entries = entries, .top_score_count = count, .footer_offset = footer_offset, .primary_data_offset = primary_start, .primary_data_end = primary_end, .point_index_checksum = point_checksum, .directory_len = directory_len, .directory_checksum = directory_checksum };
}

fn readRoutingEntry(bytes: []const u8, pos: *usize) !RoutingEntry {
    const id_len = try readInt(u32, bytes, pos);
    if (id_len == 0 or id_len > max_score_node_id_bytes) return error.InvalidGraphMetricSegment;
    const offset = try readInt(u64, bytes, pos);
    const len = try readInt(u32, bytes, pos);
    if (len == 0) return error.InvalidGraphMetricSegment;
    var checksum: [32]u8 = undefined;
    @memcpy(&checksum, try take(bytes, pos, 32));
    return .{ .first_node_id = try take(bytes, pos, id_len), .offset = offset, .len = len, .checksum = checksum };
}

/// Directory entries describe authenticated routing pages, not score blocks.
/// Their identifiers borrow bytes; the caller retains the directory payload.
pub fn decodePointDirectoryAlloc(alloc: Allocator, bytes: []const u8, directory_offset: u64, footer_offset: u64, block_count: usize, cancellation: CancellationToken) ![]RoutingEntry {
    try cancellation.check();
    var pos: usize = 0;
    if (!std.mem.eql(u8, try take(bytes, &pos, directory_magic.len), directory_magic)) return error.InvalidGraphMetricSegment;
    const count = try readInt(u32, bytes, &pos);
    if (count != scoreBlockCountForSize(block_count, routing_page_entries) or count > bytes.len / routing_entry_fixed_len) return error.InvalidGraphMetricSegment;
    const entries = try alloc.alloc(RoutingEntry, count);
    errdefer alloc.free(entries);
    var next = std.math.add(u64, footer_offset, routing_header_len) catch return error.InvalidGraphMetricSegment;
    for (entries, 0..) |*entry, i| {
        try cancellation.check();
        entry.* = try readRoutingEntry(bytes, &pos);
        entry.block_index = i * routing_page_entries;
        if (entry.offset != next or next > directory_offset or entry.len > directory_offset - next or
            entry.len > routing_page_entries * (routing_entry_fixed_len + max_score_node_id_bytes) or
            (i > 0 and std.mem.order(u8, entries[i - 1].first_node_id, entry.first_node_id) != .lt)) return error.InvalidGraphMetricSegment;
        next += entry.len;
    }
    if (pos != bytes.len or next != directory_offset) return error.InvalidGraphMetricSegment;
    return entries;
}

pub fn decodePointPageAlloc(alloc: Allocator, bytes: []const u8, page: RoutingEntry, block_count: usize, primary_start: u64, primary_end: u64, cancellation: CancellationToken) ![]RoutingEntry {
    try cancellation.check();
    if (page.block_index >= block_count or bytes.len != page.len) return error.InvalidGraphMetricSegment;
    const count = @min(routing_page_entries, block_count - page.block_index);
    const entries = try alloc.alloc(RoutingEntry, count);
    errdefer alloc.free(entries);
    var pos: usize = 0;
    var next: ?u64 = null;
    for (entries, 0..) |*entry, i| {
        try cancellation.check();
        entry.* = try readRoutingEntry(bytes, &pos);
        entry.block_index = page.block_index + i;
        if (entry.offset < primary_start or entry.offset > primary_end or entry.len > primary_end - entry.offset or
            (next != null and entry.offset != next.?) or
            (i > 0 and std.mem.order(u8, entries[i - 1].first_node_id, entry.first_node_id) != .lt)) return error.InvalidGraphMetricSegment;
        next = entry.offset + entry.len;
    }
    if (pos != bytes.len or !std.mem.eql(u8, entries[0].first_node_id, page.first_node_id) or
        (page.block_index == 0 and entries[0].offset != primary_start) or
        (page.block_index + count == block_count and next.? != primary_end)) return error.InvalidGraphMetricSegment;
    return entries;
}

/// Validate the complete directory against the canonical flat page records.
/// Full artifact validation and warm-start reads use this allocation-free path.
fn validateDirectory(point: []const u8, directory_start: usize, footer_offset: u64, block_count: usize, checksum: [32]u8, cancellation: CancellationToken) !void {
    var actual: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(point[directory_start..], &actual, .{});
    if (!std.mem.eql(u8, &actual, &checksum)) return error.InvalidGraphMetricSegment;
    var pos = directory_start;
    if (!std.mem.eql(u8, try take(point, &pos, directory_magic.len), directory_magic)) return error.InvalidGraphMetricSegment;
    const page_count = scoreBlockCountForSize(block_count, routing_page_entries);
    if (try readInt(u32, point, &pos) != page_count) return error.InvalidGraphMetricSegment;
    var record_pos: usize = routing_header_len;
    for (0..page_count) |page_index| {
        try cancellation.check();
        const page = try readRoutingEntry(point, &pos);
        const start = record_pos;
        const first = try readRoutingEntry(point[0..directory_start], &record_pos);
        const count = @min(routing_page_entries, block_count - page_index * routing_page_entries);
        for (1..count) |_| _ = try readRoutingEntry(point[0..directory_start], &record_pos);
        std.crypto.hash.sha2.Sha256.hash(point[start..record_pos], &actual, .{});
        if (page.offset != footer_offset + start or page.len != record_pos - start or
            !std.mem.eql(u8, page.first_node_id, first.first_node_id) or !std.mem.eql(u8, &page.checksum, &actual)) return error.InvalidGraphMetricSegment;
    }
    if (pos != point.len or record_pos != directory_start) return error.InvalidGraphMetricSegment;
}

pub fn artifactIntegrity(segment: types.Segment, payload: []const u8) !ArtifactIntegrity {
    if (payload.len > std.math.maxInt(u32)) return error.GraphMetricSegmentTooLarge;
    const control_len = try controlProbeLen(
        payload.len,
        segment.source_graph_artifact_id,
        segment.source_graph_checksum,
        segment.edge_filter,
    );
    const control = try decodeControl(payload[0..control_len], segment.edge_filter);
    if (control.score_data_offset != control_len) return error.InvalidGraphMetricSegment;
    if (payload.len < routing_trailer_len) return error.InvalidGraphMetricSegment;
    const routing_len = try routingFooterLenFromTrailer(payload.len, payload[payload.len - routing_trailer_len ..]);
    var control_checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload[0..control_len], &control_checksum, .{});
    var routing_checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    const root_len = try routingRootLenFromTrailer(payload);
    if (root_len != routingRootLen(control.score_count) or root_len > routing_len) return error.InvalidGraphMetricSegment;
    std.crypto.hash.sha2.Sha256.hash(payload[payload.len - root_len ..], &routing_checksum, .{});
    var point_index_checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload[payload.len - routing_len .. payload.len - root_len], &point_index_checksum, .{});
    return .{
        .control_len = @intCast(control_len),
        .routing_footer_len = @intCast(routing_len),
        .control_checksum = control_checksum,
        .routing_checksum = routing_checksum,
        .point_index_checksum = point_index_checksum,
    };
}

pub fn decodeScoreBlockWithCancellation(block: []const u8, cancellation: CancellationToken) !DecodedScoreBlock {
    var decoded = DecodedScoreBlock{};
    var pos: usize = 0;
    const prefix_len = try readInt(u16, block, &pos);
    if (prefix_len > max_score_node_id_bytes) return error.InvalidGraphMetricSegment;
    const prefix = try take(block, &pos, prefix_len);
    decoded.node_prefix = prefix;
    var previous_suffix: ?[]const u8 = null;
    var score_index: usize = 0;
    while (pos < block.len) : (score_index += 1) {
        if (score_index >= score_block_entries) return error.InvalidGraphMetricSegment;
        if (score_index % 256 == 0) try cancellation.check();
        const suffix_len = try readInt(u16, block, &pos);
        const node_len = prefix.len + suffix_len;
        if (node_len == 0 or node_len > max_score_node_id_bytes) return error.InvalidGraphMetricSegment;
        const value: f64 = @bitCast(try readInt(u64, block, &pos));
        if (!validMetricScore(value)) return error.InvalidGraphMetricSegment;
        const suffix = try take(block, &pos, suffix_len);
        if (previous_suffix != null and std.mem.order(u8, previous_suffix.?, suffix) != .lt) return error.InvalidGraphMetricSegment;
        decoded.scores[score_index] = .{ .node_suffix = suffix, .value = value };
        previous_suffix = suffix;
    }
    if (score_index == 0) return error.InvalidGraphMetricSegment;
    decoded.len = score_index;
    try cancellation.check();
    return decoded;
}

pub fn decodeRankedScoreBlockWithCancellation(block: []const u8, cancellation: CancellationToken) !DecodedScoreBlock {
    var decoded = DecodedScoreBlock{};
    var pos: usize = 0;
    const prefix_len = try readInt(u16, block, &pos);
    if (prefix_len > max_score_node_id_bytes) return error.InvalidGraphMetricSegment;
    const prefix = try take(block, &pos, prefix_len);
    decoded.node_prefix = prefix;
    var score_index: usize = 0;
    while (pos < block.len) : (score_index += 1) {
        if (score_index >= ranked_score_block_entries) return error.InvalidGraphMetricSegment;
        if (score_index % 64 == 0) try cancellation.check();
        const suffix_len = try readInt(u16, block, &pos);
        const node_len = prefix.len + suffix_len;
        if (node_len == 0 or node_len > max_score_node_id_bytes) return error.InvalidGraphMetricSegment;
        const value: f64 = @bitCast(try readInt(u64, block, &pos));
        if (!validMetricScore(value)) return error.InvalidGraphMetricSegment;
        const suffix = try take(block, &pos, suffix_len);
        if (score_index > 0) {
            const previous = decoded.scores[score_index - 1];
            if (previous.value < value or
                (previous.value == value and std.mem.order(u8, previous.node_suffix, suffix) != .lt))
            {
                return error.InvalidGraphMetricSegment;
            }
        }
        decoded.scores[score_index] = .{ .node_suffix = suffix, .value = value };
    }
    if (score_index == 0) return error.InvalidGraphMetricSegment;
    decoded.len = score_index;
    try cancellation.check();
    return decoded;
}

pub fn scoreFromBlockWithCancellation(block: []const u8, node_id: []const u8, cancellation: CancellationToken) !?f64 {
    const decoded = try decodeScoreBlockWithCancellation(block, cancellation);
    return decoded.score(node_id);
}

/// Decodes only provenance and lifecycle metadata from a bounded prefix. Score
/// vectors and edge filters are deliberately not materialized.
pub fn decodeHeader(data: []const u8) !Header {
    if (data.len < fixed_header_len) return error.InvalidGraphMetricSegment;
    var pos: usize = 0;
    if (!std.mem.eql(u8, try take(data, &pos, 4), wire_magic)) return error.InvalidGraphMetricSegment;
    const version = try readInt(u16, data, &pos);
    if (version != wire_version) return error.UnsupportedGraphMetricSegmentVersion;
    if (pos >= data.len) return error.InvalidGraphMetricSegment;
    const kind = std.enums.fromInt(@import("../../graph/graph.zig").GraphMetricKind, data[pos]) orelse return error.InvalidGraphMetricSegment;
    pos += 1;
    const materialization_state = std.enums.fromInt(types.MaterializationState, data[pos]) orelse return error.InvalidGraphMetricSegment;
    pos += 1;
    const rejection_reason = std.enums.fromInt(types.RejectionReason, data[pos]) orelse return error.InvalidGraphMetricSegment;
    pos += 1;
    const converged_byte = (try take(data, &pos, 1))[0];
    if (converged_byte > 1) return error.InvalidGraphMetricSegment;
    const iterations = try readInt(u32, data, &pos);
    const fingerprint = try readInt(u64, data, &pos);
    const materializer_fingerprint = try readInt(u64, data, &pos);
    const delta: f64 = @bitCast(try readInt(u64, data, &pos));
    if (!validMetricScore(delta)) return error.InvalidGraphMetricSegment;
    switch (materialization_state) {
        .ready => if (rejection_reason != .none) return error.InvalidGraphMetricSegment,
        .rejected => if (rejection_reason == .none or converged_byte != 0 or iterations != 0 or delta != 0) return error.InvalidGraphMetricSegment,
    }
    const artifact_len = try readInt(u32, data, &pos);
    const checksum_len = try readInt(u32, data, &pos);
    const edge_type_count = try readInt(u32, data, &pos);
    const source_graph_artifact_id = try take(data, &pos, artifact_len);
    const source_graph_checksum = try take(data, &pos, checksum_len);
    return .{
        .kind = kind,
        .version = version,
        .materialization_state = materialization_state,
        .rejection_reason = rejection_reason,
        .config_fingerprint = fingerprint,
        .materializer_fingerprint = materializer_fingerprint,
        .source_graph_artifact_id = source_graph_artifact_id,
        .source_graph_checksum = source_graph_checksum,
        .converged = converged_byte == 1,
        .iterations_completed = iterations,
        .delta = delta,
        .edge_type_count = edge_type_count,
    };
}

pub fn encodedSize(segment: types.Segment) !usize {
    return encodedSizeWithCancellation(segment, .none);
}

pub fn encodedSizeWithCancellation(segment: types.Segment, cancellation: CancellationToken) !usize {
    const prepared = try prepareTopTier(segment.scores, cancellation);
    return try encodedSizeWithPreparedTopTier(segment, &prepared, cancellation);
}

fn encodedSizeWithPreparedTopTier(segment: types.Segment, prepared: *const PreparedTopTier, cancellation: CancellationToken) !usize {
    try validateSegmentWithCancellation(segment, cancellation);
    var size: usize = fixed_header_len;
    // Logical index/metric names live in the manifest reference. Keeping them
    // out of the content-addressed payload lets aliases and equivalently
    // configured metrics share one immutable object.
    size = std.math.add(usize, size, segment.source_graph_artifact_id.len) catch return error.GraphMetricSegmentTooLarge;
    size = std.math.add(usize, size, segment.source_graph_checksum.len) catch return error.GraphMetricSegmentTooLarge;
    for (segment.edge_filter.types) |edge_type| {
        size = std.math.add(usize, size, 4 + edge_type.len) catch return error.GraphMetricSegmentTooLarge;
    }
    size = std.math.add(usize, size, @sizeOf(u32)) catch return error.GraphMetricSegmentTooLarge;
    var score_start: usize = 0;
    while (score_start < segment.scores.len) : (score_start += score_block_entries) {
        try cancellation.check();
        const score_end = @min(segment.scores.len, score_start + score_block_entries);
        size = std.math.add(usize, size, try scoreBlockEncodedSize(segment.scores[score_start..score_end])) catch
            return error.GraphMetricSegmentTooLarge;
    }
    size = std.math.add(usize, size, prepared.payload_size) catch return error.GraphMetricSegmentTooLarge;
    size = std.math.add(usize, size, try routingEncodedSize(segment.scores, prepared)) catch return error.GraphMetricSegmentTooLarge;
    return size;
}

pub fn encodeAlloc(alloc: Allocator, segment: types.Segment) ![]u8 {
    return encodeAllocWithCancellation(alloc, segment, .none);
}

pub fn encodeAllocWithCancellation(alloc: Allocator, segment: types.Segment, cancellation: CancellationToken) ![]u8 {
    return try encodeAllocWithCancellationAndLimit(alloc, segment, cancellation, std.math.maxInt(usize));
}

pub fn encodeAllocWithCancellationAndLimit(
    alloc: Allocator,
    segment: types.Segment,
    cancellation: CancellationToken,
    max_encoded_bytes: usize,
) ![]u8 {
    const prepared = try prepareTopTier(segment.scores, cancellation);
    const size = try encodedSizeWithPreparedTopTier(segment, &prepared, cancellation);
    if (size > max_encoded_bytes) return error.GraphMetricSegmentTooLarge;
    const data = try alloc.alloc(u8, size);
    errdefer alloc.free(data);
    var pos: usize = 0;
    putBytes(data, &pos, wire_magic);
    putInt(u16, data, &pos, wire_version);
    data[pos] = @intFromEnum(segment.kind);
    pos += 1;
    data[pos] = @intFromEnum(segment.materialization_state);
    pos += 1;
    data[pos] = @intFromEnum(segment.rejection_reason);
    pos += 1;
    data[pos] = @intFromBool(segment.converged);
    pos += 1;
    putInt(u32, data, &pos, segment.iterations_completed);
    putInt(u64, data, &pos, segment.config_fingerprint);
    putInt(u64, data, &pos, segment.materializer_fingerprint);
    putInt(u64, data, &pos, @bitCast(segment.delta));
    putInt(u32, data, &pos, @intCast(segment.source_graph_artifact_id.len));
    putInt(u32, data, &pos, @intCast(segment.source_graph_checksum.len));
    putInt(u32, data, &pos, @intCast(segment.edge_filter.types.len));
    putBytes(data, &pos, segment.source_graph_artifact_id);
    putBytes(data, &pos, segment.source_graph_checksum);
    for (segment.edge_filter.types) |edge_type| {
        putInt(u32, data, &pos, @intCast(edge_type.len));
        putBytes(data, &pos, edge_type);
    }
    putInt(u32, data, &pos, @intCast(segment.scores.len));
    const score_data_offset = pos;
    var primary_start: usize = 0;
    while (primary_start < segment.scores.len) : (primary_start += score_block_entries) {
        try cancellation.check();
        const primary_end = @min(segment.scores.len, primary_start + score_block_entries);
        const block_scores = segment.scores[primary_start..primary_end];
        const prefix_len = scoreBlockPrefixLen(block_scores);
        putInt(u16, data, &pos, @intCast(prefix_len));
        putBytes(data, &pos, block_scores[0].node_id[0..prefix_len]);
        for (block_scores) |score| {
            const suffix = score.node_id[prefix_len..];
            putInt(u16, data, &pos, @intCast(suffix.len));
            putInt(u64, data, &pos, @bitCast(score.value));
            putBytes(data, &pos, suffix);
        }
    }
    const score_data_end = pos;
    var ranked_entries: [max_ranked_score_blocks]RankedRoutingEntry = undefined;
    var ranked_block_index: usize = 0;
    while (ranked_block_index < prepared.blockCount()) : (ranked_block_index += 1) {
        try cancellation.check();
        const ranked_start = ranked_block_index * ranked_score_block_entries;
        const ranked_end = @min(prepared.count, ranked_start + ranked_score_block_entries);
        const block_offset = pos;
        const block_indexes = prepared.indexes[ranked_start..ranked_end];
        const prefix_len = rankedBlockPrefixLen(segment.scores, block_indexes);
        putInt(u16, data, &pos, @intCast(prefix_len));
        putBytes(data, &pos, segment.scores[block_indexes[0]].node_id[0..prefix_len]);
        for (block_indexes) |top_index| {
            const score = segment.scores[top_index];
            const suffix = score.node_id[prefix_len..];
            putInt(u16, data, &pos, @intCast(suffix.len));
            putInt(u64, data, &pos, @bitCast(score.value));
            putBytes(data, &pos, suffix);
        }
        const block_len = pos - block_offset;
        var block_checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(data[block_offset..pos], &block_checksum, .{});
        ranked_entries[ranked_block_index] = .{
            .offset = block_offset,
            .len = block_len,
            .checksum = block_checksum,
        };
    }

    const footer_offset = pos;
    putBytes(data, &pos, routing_magic);
    const block_count = scoreBlockCount(segment.scores.len);
    putInt(u32, data, &pos, @intCast(block_count));
    var block_offset = score_data_offset;
    var score_index: usize = 0;
    while (score_index < segment.scores.len) : (score_index += score_block_entries) {
        try cancellation.check();
        const block_end = @min(segment.scores.len, score_index + score_block_entries);
        const block_len = try scoreBlockEncodedSize(segment.scores[score_index..block_end]);
        const first_node_id = segment.scores[score_index].node_id;
        putInt(u32, data, &pos, @intCast(first_node_id.len));
        putInt(u64, data, &pos, @intCast(block_offset));
        putInt(u32, data, &pos, @intCast(block_len));
        var block_checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(data[block_offset..][0..block_len], &block_checksum, .{});
        putBytes(data, &pos, &block_checksum);
        putBytes(data, &pos, first_node_id);
        block_offset += block_len;
    }
    std.debug.assert(block_offset == score_data_end);
    const directory_offset = pos;
    putBytes(data, &pos, directory_magic);
    putInt(u32, data, &pos, @intCast(scoreBlockCountForSize(block_count, routing_page_entries)));
    var page_start = footer_offset + routing_header_len;
    var first_block: usize = 0;
    while (first_block < block_count) : (first_block += routing_page_entries) {
        try cancellation.check();
        var page_end = page_start;
        for (first_block..@min(block_count, first_block + routing_page_entries)) |_| {
            _ = try readRoutingEntry(data[0..directory_offset], &page_end);
        }
        const first_id = segment.scores[first_block * score_block_entries].node_id;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(data[page_start..page_end], &checksum, .{});
        putInt(u32, data, &pos, @intCast(first_id.len));
        putInt(u64, data, &pos, @intCast(page_start));
        putInt(u32, data, &pos, @intCast(page_end - page_start));
        putBytes(data, &pos, &checksum);
        putBytes(data, &pos, first_id);
        page_start = page_end;
    }
    std.debug.assert(page_start == directory_offset);
    const directory_len = pos - directory_offset;
    var directory_checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(data[directory_offset..pos], &directory_checksum, .{});
    var point_checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(data[footer_offset..pos], &point_checksum, .{});
    const root_offset = pos;
    putBytes(data, &pos, top_tier_magic);
    putInt(u32, data, &pos, @intCast(prepared.count));
    putInt(u32, data, &pos, @intCast(prepared.blockCount()));
    putInt(u64, data, &pos, @intCast(score_data_offset));
    putInt(u64, data, &pos, @intCast(score_data_end));
    putBytes(data, &pos, &point_checksum);
    putInt(u64, data, &pos, @intCast(directory_len));
    putBytes(data, &pos, &directory_checksum);
    for (ranked_entries[0..prepared.blockCount()]) |entry| {
        putInt(u64, data, &pos, entry.offset);
        putInt(u32, data, &pos, @intCast(entry.len));
        putBytes(data, &pos, &entry.checksum);
    }
    putInt(u32, data, &pos, @intCast(pos + routing_root_trailer_len - root_offset));
    putInt(u64, data, &pos, @intCast(pos + routing_trailer_len - footer_offset));
    std.debug.assert(pos == data.len);
    try cancellation.check();
    return data;
}

pub fn decodeAlloc(alloc: Allocator, data: []const u8) !types.Segment {
    return decodeAllocWithLimitsAndCancellation(alloc, data, .{}, .none);
}

pub fn decodeAllocWithLimits(alloc: Allocator, data: []const u8, limits: bounded_decode.Limits) !types.Segment {
    return decodeAllocWithLimitsAndCancellation(alloc, data, limits, .none);
}

pub fn decodeAllocWithCancellation(alloc: Allocator, data: []const u8, cancellation: CancellationToken) !types.Segment {
    return decodeAllocWithLimitsAndCancellation(alloc, data, .{}, cancellation);
}

pub fn decodeAllocWithLimitsAndCancellation(
    alloc: Allocator,
    data: []const u8,
    limits: bounded_decode.Limits,
    cancellation: CancellationToken,
) !types.Segment {
    try cancellation.check();
    var budget = try bounded_decode.Budget.init(data.len, limits);
    var limiter = try bounded_decode.AllocationLimiter.init(alloc, limits.max_allocation_bytes);
    var decoded = decodeBoundedAlloc(limiter.allocator(), data, &budget, cancellation) catch |err| {
        if (err == error.OutOfMemory and limiter.limit_exceeded) return error.DecodedArtifactTooLarge;
        return err;
    };
    errdefer decoded.deinit(alloc);
    try cancellation.check();
    return decoded;
}

fn decodeBoundedAlloc(alloc: Allocator, data: []const u8, budget: *bounded_decode.Budget, cancellation: CancellationToken) !types.Segment {
    if (data.len < fixed_header_len + @sizeOf(u32)) return error.InvalidGraphMetricSegment;
    var pos: usize = 0;
    if (!std.mem.eql(u8, take(data, &pos, 4) catch return error.InvalidGraphMetricSegment, wire_magic)) return error.InvalidGraphMetricSegment;
    const version = readInt(u16, data, &pos) catch return error.InvalidGraphMetricSegment;
    if (version != wire_version) return error.UnsupportedGraphMetricSegmentVersion;
    const kind = std.enums.fromInt(@import("../../graph/graph.zig").GraphMetricKind, data[pos]) orelse return error.InvalidGraphMetricSegment;
    pos += 1;
    const materialization_state = std.enums.fromInt(types.MaterializationState, data[pos]) orelse return error.InvalidGraphMetricSegment;
    pos += 1;
    const rejection_reason = std.enums.fromInt(types.RejectionReason, data[pos]) orelse return error.InvalidGraphMetricSegment;
    pos += 1;
    const converged_byte = data[pos];
    pos += 1;
    if (converged_byte > 1) return error.InvalidGraphMetricSegment;
    const iterations = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const fingerprint = readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment;
    const materializer_fingerprint = readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment;
    const delta: f64 = @bitCast(readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment);
    if (!validMetricScore(delta)) return error.InvalidGraphMetricSegment;
    const artifact_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const checksum_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const edge_type_count = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const identity_len = std.math.add(usize, artifact_len, checksum_len) catch return error.InvalidGraphMetricSegment;
    try budget.admitBytes(identity_len);
    const artifact_id = try dupeTake(alloc, data, &pos, artifact_len);
    errdefer alloc.free(artifact_id);
    const checksum = try dupeTake(alloc, data, &pos, checksum_len);
    errdefer alloc.free(checksum);
    _ = try budget.admitCount([]const u8, edge_type_count, data.len - pos, 4);
    const edge_types = try alloc.alloc([]const u8, edge_type_count);
    errdefer alloc.free(edge_types);
    var initialized_edge_types: usize = 0;
    errdefer for (edge_types[0..initialized_edge_types]) |edge_type| alloc.free(edge_type);
    for (edge_types, 0..) |*edge_type, edge_type_index| {
        if (edge_type_index % 16 == 0) try cancellation.check();
        const edge_type_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
        try budget.admitBytes(edge_type_len);
        edge_type.* = try dupeTake(alloc, data, &pos, edge_type_len);
        initialized_edge_types += 1;
    }
    const score_count = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    _ = try budget.admitCount(types.Score, score_count, data.len - pos, ranked_score_fixed_len);
    const scores = try alloc.alloc(types.Score, score_count);
    errdefer alloc.free(scores);
    var initialized: usize = 0;
    errdefer for (scores[0..initialized]) |*score| score.deinit(alloc);
    var block_prefix: []const u8 = &.{};
    for (scores, 0..) |*score, score_index| {
        if (score_index % 4096 == 0) try cancellation.check();
        if (score_index % score_block_entries == 0) {
            const prefix_len = readInt(u16, data, &pos) catch return error.InvalidGraphMetricSegment;
            if (prefix_len > max_score_node_id_bytes) return error.InvalidGraphMetricSegment;
            block_prefix = take(data, &pos, prefix_len) catch return error.InvalidGraphMetricSegment;
        }
        const suffix_len = readInt(u16, data, &pos) catch return error.InvalidGraphMetricSegment;
        const node_len = std.math.add(usize, block_prefix.len, suffix_len) catch return error.InvalidGraphMetricSegment;
        if (node_len == 0 or node_len > max_score_node_id_bytes) return error.InvalidGraphMetricSegment;
        const value: f64 = @bitCast(readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment);
        if (!validMetricScore(value)) return error.InvalidGraphMetricSegment;
        try budget.admitBytes(node_len);
        const suffix = take(data, &pos, suffix_len) catch return error.InvalidGraphMetricSegment;
        const node_id = try alloc.alloc(u8, node_len);
        errdefer alloc.free(node_id);
        @memcpy(node_id[0..block_prefix.len], block_prefix);
        @memcpy(node_id[block_prefix.len..], suffix);
        score.* = .{ .node_id = node_id, .value = value };
        initialized += 1;
    }
    try validateRoutingFooter(data, pos, scores, cancellation);
    var segment = types.Segment{ .metadata_version = version, .kind = kind, .source_graph_artifact_id = artifact_id, .source_graph_checksum = checksum, .config_fingerprint = fingerprint, .materializer_fingerprint = materializer_fingerprint, .materialization_state = materialization_state, .rejection_reason = rejection_reason, .edge_filter = .{ .mode = if (edge_type_count == 0) .all else .types, .types = edge_types }, .converged = converged_byte == 1, .iterations_completed = iterations, .delta = delta, .scores = scores };
    errdefer segment.deinit(alloc);
    try validateSegmentWithCancellation(segment, cancellation);
    return segment;
}

fn validateSegment(segment: types.Segment) !void {
    return validateSegmentWithCancellation(segment, .none);
}

fn validateSegmentWithCancellation(segment: types.Segment, cancellation: CancellationToken) !void {
    try cancellation.check();
    if (segment.source_graph_artifact_id.len == 0 or segment.source_graph_checksum.len == 0) return error.InvalidGraphMetricSegment;
    _ = std.math.cast(u32, segment.source_graph_artifact_id.len) orelse return error.GraphMetricSegmentTooLarge;
    _ = std.math.cast(u32, segment.source_graph_checksum.len) orelse return error.GraphMetricSegmentTooLarge;
    _ = std.math.cast(u32, segment.scores.len) orelse return error.GraphMetricSegmentTooLarge;
    if (!validMetricScore(segment.delta)) return error.InvalidGraphMetricSegment;
    switch (segment.materialization_state) {
        .ready => if (segment.rejection_reason != .none) return error.InvalidGraphMetricSegment,
        .rejected => {
            if (segment.rejection_reason == .none or segment.scores.len != 0 or segment.converged or segment.iterations_completed != 0 or segment.delta != 0) return error.InvalidGraphMetricSegment;
        },
    }
    if ((segment.edge_filter.mode == .all) != (segment.edge_filter.types.len == 0)) return error.InvalidGraphMetricSegment;
    for (segment.edge_filter.types, 0..) |edge_type, i| {
        if (edge_type.len == 0) return error.InvalidGraphMetricSegment;
        _ = std.math.cast(u32, edge_type.len) orelse return error.GraphMetricSegmentTooLarge;
        if (i > 0 and std.mem.order(u8, segment.edge_filter.types[i - 1], edge_type) != .lt) return error.InvalidGraphMetricSegment;
    }
    for (segment.scores, 0..) |score, i| {
        if (i % 4096 == 0) try cancellation.check();
        if (score.node_id.len == 0 or !validMetricScore(score.value)) return error.InvalidGraphMetricSegment;
        if (score.node_id.len > max_score_node_id_bytes) return error.GraphMetricSegmentTooLarge;
        _ = std.math.cast(u32, score.node_id.len) orelse return error.GraphMetricSegmentTooLarge;
        if (i > 0 and std.mem.order(u8, segment.scores[i - 1].node_id, score.node_id) != .lt) return error.InvalidGraphMetricSegment;
    }
    try cancellation.check();
}

fn scoreBlockCount(score_count: usize) usize {
    return scoreBlockCountForSize(score_count, score_block_entries);
}

fn scoreBlockCountForSize(score_count: usize, block_entries: usize) usize {
    return score_count / block_entries + @intFromBool(score_count % block_entries != 0);
}

fn commonPrefixLen(left: []const u8, right: []const u8) usize {
    const end = @min(left.len, right.len);
    var index: usize = 0;
    while (index < end and left[index] == right[index]) : (index += 1) {}
    return index;
}

fn validMetricScore(value: f64) bool {
    // Reject negative zero as well as negative/NaN/infinite values. This keeps
    // canonical encodings unique and makes unsigned IEEE-754 radix order match
    // numeric order exactly.
    const bits: u64 = @bitCast(value);
    return std.math.isFinite(value) and bits & (@as(u64, 1) << 63) == 0;
}

fn scoreBlockPrefixLen(scores: []const types.Score) usize {
    if (scores.len == 0) return 0;
    // Primary blocks are node-sorted, so the first/last common prefix is the
    // prefix shared by the complete block.
    return commonPrefixLen(scores[0].node_id, scores[scores.len - 1].node_id);
}

fn rankedBlockPrefixLen(scores: []const types.Score, indexes: []const usize) usize {
    if (indexes.len == 0) return 0;
    var prefix_len = scores[indexes[0]].node_id.len;
    for (indexes[1..]) |index| {
        prefix_len = @min(prefix_len, commonPrefixLen(scores[indexes[0]].node_id[0..prefix_len], scores[index].node_id));
        if (prefix_len == 0) break;
    }
    return prefix_len;
}

fn scoreBlockEncodedSize(scores: []const types.Score) !usize {
    const prefix_len = scoreBlockPrefixLen(scores);
    var size = std.math.add(usize, @sizeOf(u16), prefix_len) catch return error.GraphMetricSegmentTooLarge;
    for (scores) |score| {
        size = std.math.add(usize, size, ranked_score_fixed_len + score.node_id.len - prefix_len) catch
            return error.GraphMetricSegmentTooLarge;
    }
    return size;
}

fn rankedBlockEncodedSize(scores: []const types.Score, indexes: []const usize) !usize {
    const prefix_len = rankedBlockPrefixLen(scores, indexes);
    var size = std.math.add(usize, @sizeOf(u16), prefix_len) catch return error.GraphMetricSegmentTooLarge;
    for (indexes) |index| {
        size = std.math.add(usize, size, ranked_score_fixed_len + scores[index].node_id.len - prefix_len) catch
            return error.GraphMetricSegmentTooLarge;
    }
    return size;
}

fn topScoreRanksBefore(a: PersistedTopScore, b: PersistedTopScore) bool {
    if (a.value != b.value) return a.value > b.value;
    return std.mem.order(u8, a.node_id, b.node_id) == .lt;
}

fn scoreIndexRanksBefore(scores: []const types.Score, a: usize, b: usize) bool {
    return topScoreRanksBefore(
        .{ .node_id = scores[a].node_id, .value = scores[a].value },
        .{ .node_id = scores[b].node_id, .value = scores[b].value },
    );
}

fn siftWorstScoreUp(scores: []const types.Score, heap: []usize, start: usize) void {
    var child = start;
    while (child > 0) {
        const parent = (child - 1) / 2;
        if (!scoreIndexRanksBefore(scores, heap[parent], heap[child])) break;
        std.mem.swap(usize, &heap[parent], &heap[child]);
        child = parent;
    }
}

fn siftWorstScoreDown(scores: []const types.Score, heap: []usize, start: usize) void {
    var parent = start;
    while (true) {
        const left = parent * 2 + 1;
        if (left >= heap.len) return;
        const right = left + 1;
        var worse_child = left;
        if (right < heap.len and scoreIndexRanksBefore(scores, heap[left], heap[right])) worse_child = right;
        if (!scoreIndexRanksBefore(scores, heap[parent], heap[worse_child])) return;
        std.mem.swap(usize, &heap[parent], &heap[worse_child]);
        parent = worse_child;
    }
}

fn selectTopScoreIndexes(
    scores: []const types.Score,
    storage: *[max_persisted_top_entries]usize,
    cancellation: CancellationToken,
) ![]usize {
    const target = @min(scores.len, max_persisted_top_entries);
    // Express the ratio without multiplication so this admission check stays
    // overflow-safe if the persisted tier limit becomes configurable later.
    if (target >= 512 and scores.len > target and scores.len - target > target) {
        return try selectTopScoreIndexesRadix(scores, storage, target, cancellation);
    }
    var heap_len: usize = 0;
    for (scores, 0..) |_, score_index| {
        if (score_index % 4096 == 0) try cancellation.check();
        if (heap_len < target) {
            storage[heap_len] = score_index;
            siftWorstScoreUp(scores, storage[0 .. heap_len + 1], heap_len);
            heap_len += 1;
        } else if (target > 0 and scoreIndexRanksBefore(scores, score_index, storage[0])) {
            storage[0] = score_index;
            siftWorstScoreDown(scores, storage[0..heap_len], 0);
        }
    }
    var remaining = heap_len;
    while (remaining > 1) {
        if (remaining % 256 == 0) try cancellation.check();
        std.mem.swap(usize, &storage[0], &storage[remaining - 1]);
        remaining -= 1;
        siftWorstScoreDown(scores, storage[0..remaining], 0);
    }
    return storage[0..heap_len];
}

fn scoreIndexLessThan(scores: []const types.Score, left: usize, right: usize) bool {
    return scoreIndexRanksBefore(scores, left, right);
}

/// Select a large persisted prefix by IEEE-754 radix instead of paying
/// O(N log K) heap comparisons. Graph metric scores are finite and
/// non-negative, so their bit representation preserves numeric ordering.
/// Equal-score membership remains deterministic because the input vector is
/// node-sorted and the final K indexes use the canonical score/node ordering.
fn selectTopScoreIndexesRadix(
    scores: []const types.Score,
    storage: *[max_persisted_top_entries]usize,
    target: usize,
    cancellation: CancellationToken,
) ![]usize {
    std.debug.assert(target > 0 and target <= storage.len and target < scores.len);
    var prefix: u64 = 0;
    var prefix_mask: u64 = 0;
    var rank = target - 1;
    for (0..8) |pass| {
        var counts: [256]usize = @splat(0);
        const shift: u6 = @intCast(56 - pass * 8);
        for (scores, 0..) |score, score_index| {
            if (score_index % 4096 == 0) try cancellation.check();
            const bits: u64 = @bitCast(score.value);
            if (bits & prefix_mask != prefix) continue;
            counts[@as(u8, @truncate(bits >> shift))] += 1;
        }
        var bucket: usize = counts.len;
        while (bucket > 0) {
            bucket -= 1;
            if (rank < counts[bucket]) break;
            rank -= counts[bucket];
        }
        prefix |= @as(u64, @intCast(bucket)) << shift;
        prefix_mask |= @as(u64, 0xff) << shift;
    }
    const threshold: f64 = @bitCast(prefix);
    var selected: usize = 0;
    for (scores, 0..) |score, score_index| {
        if (score_index % 4096 == 0) try cancellation.check();
        if (score.value > threshold) {
            if (selected >= target) return error.InvalidGraphMetricSegment;
            storage[selected] = score_index;
            selected += 1;
        }
    }
    for (scores, 0..) |score, score_index| {
        if (selected == target) break;
        if (score_index % 4096 == 0) try cancellation.check();
        if (score.value == threshold) {
            storage[selected] = score_index;
            selected += 1;
        }
    }
    if (selected != target) return error.InvalidGraphMetricSegment;
    std.mem.sort(usize, storage[0..selected], scores, scoreIndexLessThan);
    return storage[0..selected];
}

const PreparedTopTier = struct {
    indexes: [max_persisted_top_entries]usize = undefined,
    count: usize = 0,
    payload_size: usize = 0,

    fn blockCount(self: *const PreparedTopTier) usize {
        return scoreBlockCountForSize(self.count, ranked_score_block_entries);
    }
};

fn prepareTopTier(scores: []const types.Score, cancellation: CancellationToken) !PreparedTopTier {
    var prepared = PreparedTopTier{};
    const selected = try selectTopScoreIndexes(scores, &prepared.indexes, cancellation);
    prepared.count = selected.len;
    var ranked_start: usize = 0;
    while (ranked_start < selected.len) : (ranked_start += ranked_score_block_entries) {
        try cancellation.check();
        const ranked_end = @min(selected.len, ranked_start + ranked_score_block_entries);
        prepared.payload_size = std.math.add(
            usize,
            prepared.payload_size,
            try rankedBlockEncodedSize(scores, selected[ranked_start..ranked_end]),
        ) catch return error.GraphMetricSegmentTooLarge;
    }
    return prepared;
}

fn routingEncodedSize(scores: []const types.Score, prepared: *const PreparedTopTier) !usize {
    var size: usize = 2 * routing_header_len + top_tier_header_len + routing_root_metadata_len + routing_root_trailer_len;
    var score_index: usize = 0;
    while (score_index < scores.len) : (score_index += score_block_entries) {
        size = std.math.add(usize, size, routing_entry_fixed_len) catch return error.GraphMetricSegmentTooLarge;
        size = std.math.add(usize, size, scores[score_index].node_id.len) catch return error.GraphMetricSegmentTooLarge;
        if (score_index / score_block_entries % routing_page_entries == 0) {
            size = std.math.add(usize, size, routing_entry_fixed_len + scores[score_index].node_id.len) catch return error.GraphMetricSegmentTooLarge;
        }
    }
    size = std.math.add(usize, size, std.math.mul(usize, prepared.blockCount(), ranked_routing_entry_len) catch return error.GraphMetricSegmentTooLarge) catch return error.GraphMetricSegmentTooLarge;
    if (size > max_routing_bytes) return error.GraphMetricSegmentTooLarge;
    return size;
}

fn validateRoutingFooter(data: []const u8, score_data_end: usize, scores: []const types.Score, cancellation: CancellationToken) !void {
    try cancellation.check();
    if (score_data_end > data.len or data.len < routing_trailer_len) return error.InvalidGraphMetricSegment;
    const footer_len = try routingFooterLenFromTrailer(data.len, data[data.len - routing_trailer_len ..]);
    const footer_offset = data.len - footer_len;
    if (score_data_end > footer_offset) return error.InvalidGraphMetricSegment;
    const footer = data[footer_offset..];
    var pos: usize = 0;
    if (!std.mem.eql(u8, try take(footer, &pos, routing_magic.len), routing_magic)) return error.InvalidGraphMetricSegment;
    if (try readInt(u32, footer, &pos) != scoreBlockCount(scores.len)) return error.InvalidGraphMetricSegment;
    var expected_offset = score_data_end;
    for (0..scoreBlockCount(scores.len)) |block_index| {
        try cancellation.check();
        const score_start = block_index * score_block_entries;
        const score_end = @min(scores.len, score_start + score_block_entries);
        const block_len = try scoreBlockEncodedSize(scores[score_start..score_end]);
        expected_offset -= block_len;
    }
    const primary_start = expected_offset;
    for (0..scoreBlockCount(scores.len)) |block_index| {
        try cancellation.check();
        const score_start = block_index * score_block_entries;
        const score_end = @min(scores.len, score_start + score_block_entries);
        const block_len = try scoreBlockEncodedSize(scores[score_start..score_end]);
        const first_len = try readInt(u32, footer, &pos);
        const offset = try readInt(u64, footer, &pos);
        const encoded_block_len = try readInt(u32, footer, &pos);
        var encoded_checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        @memcpy(&encoded_checksum, try take(footer, &pos, encoded_checksum.len));
        const first = try take(footer, &pos, first_len);
        if (!std.mem.eql(u8, first, scores[score_start].node_id) or
            offset != expected_offset or encoded_block_len != block_len)
        {
            return error.InvalidGraphMetricSegment;
        }
        var actual_checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        const block_offset = std.math.cast(usize, offset) orelse return error.InvalidGraphMetricSegment;
        if (block_offset > data.len or block_len > data.len - block_offset) return error.InvalidGraphMetricSegment;
        std.crypto.hash.sha2.Sha256.hash(data[block_offset..][0..block_len], &actual_checksum, .{});
        if (!std.mem.eql(u8, &actual_checksum, &encoded_checksum)) return error.InvalidGraphMetricSegment;
        expected_offset += block_len;
    }
    if (expected_offset != score_data_end) return error.InvalidGraphMetricSegment;
    const directory_start = pos;
    if (!std.mem.eql(u8, try take(footer, &pos, directory_magic.len), directory_magic)) return error.InvalidGraphMetricSegment;
    const page_count = scoreBlockCountForSize(scoreBlockCount(scores.len), routing_page_entries);
    if (try readInt(u32, footer, &pos) != page_count) return error.InvalidGraphMetricSegment;
    for (0..page_count) |_| _ = try readRoutingEntry(footer, &pos);
    const directory_len = pos - directory_start;
    var directory_checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(footer[directory_start..pos], &directory_checksum, .{});
    try validateDirectory(footer[0..pos], directory_start, footer_offset, scoreBlockCount(scores.len), directory_checksum, cancellation);
    {
        var point_checksum: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(footer[0..pos], &point_checksum, .{});
        if (!std.mem.eql(u8, try take(footer, &pos, top_tier_magic.len), top_tier_magic)) return error.InvalidGraphMetricSegment;
        var top_storage: [max_persisted_top_entries]usize = undefined;
        const expected_top = try selectTopScoreIndexes(scores, &top_storage, cancellation);
        if (try readInt(u32, footer, &pos) != expected_top.len) return error.InvalidGraphMetricSegment;
        const block_count = try readInt(u32, footer, &pos);
        if (block_count != scoreBlockCountForSize(expected_top.len, ranked_score_block_entries)) return error.InvalidGraphMetricSegment;
        if (try readInt(u64, footer, &pos) != primary_start or try readInt(u64, footer, &pos) != score_data_end or
            !std.mem.eql(u8, try take(footer, &pos, 32), &point_checksum)) return error.InvalidGraphMetricSegment;
        if (try readInt(u64, footer, &pos) != directory_len or
            !std.mem.eql(u8, try take(footer, &pos, 32), &directory_checksum)) return error.InvalidGraphMetricSegment;
        var ranked_offset = score_data_end;
        for (0..block_count) |block_index| {
            try cancellation.check();
            const offset = try readInt(u64, footer, &pos);
            const block_len = try readInt(u32, footer, &pos);
            var encoded_checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            @memcpy(&encoded_checksum, try take(footer, &pos, encoded_checksum.len));
            if (offset != ranked_offset or block_len == 0 or block_len > max_ranked_score_block_bytes) return error.InvalidGraphMetricSegment;
            const block_offset = std.math.cast(usize, offset) orelse return error.InvalidGraphMetricSegment;
            if (block_offset > footer_offset or block_len > footer_offset - block_offset) return error.InvalidGraphMetricSegment;
            const block = data[block_offset..][0..block_len];
            var actual_checksum: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(block, &actual_checksum, .{});
            if (!std.mem.eql(u8, &actual_checksum, &encoded_checksum)) return error.InvalidGraphMetricSegment;
            const decoded = try decodeRankedScoreBlockWithCancellation(block, cancellation);
            const ranked_start = block_index * ranked_score_block_entries;
            const ranked_end = @min(expected_top.len, ranked_start + ranked_score_block_entries);
            if (decoded.len != ranked_end - ranked_start) return error.InvalidGraphMetricSegment;
            for (decoded.scores[0..decoded.len], expected_top[ranked_start..ranked_end]) |actual, score_index| {
                const expected = scores[score_index];
                if (!actual.eqlNode(decoded.node_prefix, expected.node_id) or @as(u64, @bitCast(actual.value)) != @as(u64, @bitCast(expected.value))) {
                    return error.InvalidGraphMetricSegment;
                }
            }
            ranked_offset = std.math.add(usize, block_offset, block_len) catch return error.InvalidGraphMetricSegment;
        }
        if (ranked_offset != footer_offset) return error.InvalidGraphMetricSegment;
    }
    if (try readInt(u32, footer, &pos) != routingRootLen(scores.len) or pos + routing_trailer_len != footer.len) return error.InvalidGraphMetricSegment;
    try cancellation.check();
}

test "serverless graph metric large top tier uses exact radix selection" {
    const alloc = std.testing.allocator;
    const score_count = max_persisted_top_entries * 2 + 1;
    const scores = try alloc.alloc(types.Score, score_count);
    defer alloc.free(scores);
    for (scores, 0..) |*score, index| score.* = .{
        .node_id = @constCast("node"),
        .value = @floatFromInt(index),
    };
    var storage: [max_persisted_top_entries]usize = undefined;
    const selected = try selectTopScoreIndexes(scores, &storage, .none);
    try std.testing.expectEqual(max_persisted_top_entries, selected.len);
    try std.testing.expectEqual(score_count - 1, selected[0]);
    try std.testing.expectEqual(score_count - max_persisted_top_entries, selected[selected.len - 1]);
}

test "serverless graph metric score blocks compress shared node prefixes" {
    const scores = [_]types.Score{
        .{ .node_id = @constCast("tenant:west:node:0001"), .value = 1 },
        .{ .node_id = @constCast("tenant:west:node:0002"), .value = 2 },
        .{ .node_id = @constCast("tenant:west:node:0003"), .value = 3 },
    };
    var uncompressed_size: usize = 0;
    for (scores) |score| uncompressed_size += @sizeOf(u32) + @sizeOf(u64) + score.node_id.len;
    try std.testing.expect(try scoreBlockEncodedSize(&scores) < uncompressed_size);
}

test "serverless graph metric canonical scores reject negative zero" {
    const scores = [_]types.Score{.{ .node_id = @constCast("node"), .value = -0.0 }};
    const segment = types.Segment{
        .kind = .pagerank,
        .source_graph_artifact_id = @constCast("sha256:graph"),
        .source_graph_checksum = @constCast("sha256:sum"),
        .config_fingerprint = 1,
        .edge_filter = .{},
        .converged = true,
        .iterations_completed = 1,
        .delta = 0,
        .scores = @constCast(&scores),
    };
    try std.testing.expectError(error.InvalidGraphMetricSegment, encodedSize(segment));

    const valid_scores = [_]types.Score{.{ .node_id = @constCast("node"), .value = 0.0 }};
    var noncanonical_delta = segment;
    noncanonical_delta.scores = @constCast(&valid_scores);
    noncanonical_delta.delta = -0.0;
    try std.testing.expectError(error.InvalidGraphMetricSegment, encodedSize(noncanonical_delta));
}

fn putBytes(data: []u8, pos: *usize, value: []const u8) void {
    @memcpy(data[pos.*..][0..value.len], value);
    pos.* += value.len;
}
fn putInt(comptime T: type, data: []u8, pos: *usize, value: T) void {
    std.mem.writeInt(T, data[pos.*..][0..@sizeOf(T)], value, .little);
    pos.* += @sizeOf(T);
}
fn readInt(comptime T: type, data: []const u8, pos: *usize) !T {
    if (pos.* + @sizeOf(T) > data.len) return error.InvalidGraphMetricSegment;
    const value = std.mem.readInt(T, data[pos.*..][0..@sizeOf(T)], .little);
    pos.* += @sizeOf(T);
    return value;
}
fn take(data: []const u8, pos: *usize, len: usize) ![]const u8 {
    if (pos.* > data.len or len > data.len - pos.*) return error.InvalidGraphMetricSegment;
    const value = data[pos.*..][0..len];
    pos.* += len;
    return value;
}
fn dupeTake(alloc: Allocator, data: []const u8, pos: *usize, len: usize) ![]u8 {
    return try alloc.dupe(u8, try take(data, pos, len));
}

test "serverless graph metric segment round trips with binary-search lookup" {
    const alloc = std.testing.allocator;
    var scores = try alloc.alloc(types.Score, 2);
    scores[0] = .{ .node_id = try alloc.dupe(u8, "a"), .value = 0.25 };
    scores[1] = .{ .node_id = try alloc.dupe(u8, "b"), .value = 0.75 };
    var segment = types.Segment{ .kind = .pagerank, .source_graph_artifact_id = try alloc.dupe(u8, "sha256:graph"), .source_graph_checksum = try alloc.dupe(u8, "sha256:sum"), .config_fingerprint = 42, .edge_filter = .{}, .converged = true, .iterations_completed = 12, .delta = 0.00001, .scores = scores };
    defer segment.deinit(alloc);
    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    const header_len = try headerProbeLen(
        encoded.len,
        segment.source_graph_artifact_id,
        segment.source_graph_checksum,
    );
    try std.testing.expectEqual(
        fixed_header_len + segment.source_graph_artifact_id.len + segment.source_graph_checksum.len,
        header_len,
    );
    const header = try decodeHeader(encoded[0..header_len]);
    try std.testing.expectEqual(graph_mod.GraphMetricKind.pagerank, header.kind);
    try std.testing.expectEqualStrings(segment.source_graph_artifact_id, header.source_graph_artifact_id);
    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(?f64, 0.75), decoded.score("b"));
    try std.testing.expect(decoded.score("missing") == null);
    const root_len = routingRootLen(scores.len);
    var root = try decodeRoutingRootAlloc(alloc, encoded[encoded.len - root_len ..], encoded.len, wire_version, .none);
    defer root.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), root.entries.len);
    try std.testing.expectEqual(@as(usize, 1), root.ranked_entries.len);
    try std.testing.expectEqual(@as(usize, 1872), routingRootLen(1_000_000));
    try std.testing.expectEqual(routingRootLen(max_persisted_top_entries), routingRootLen(std.math.maxInt(usize)));
    try std.testing.expectError(error.InvalidGraphMetricSegment, decodeRoutingRootAlloc(alloc, encoded[encoded.len - root_len + 1 ..], encoded.len, wire_version, .none));
    const integrity = try artifactIntegrity(segment, encoded);
    try std.testing.expectEqualSlices(u8, &integrity.point_index_checksum, &root.point_index_checksum);
    const footer = encoded[encoded.len - integrity.routing_footer_len ..];
    footer[0] ^= 1;
    try std.testing.expectError(error.InvalidGraphMetricSegment, decodeRoutingIndexAlloc(alloc, footer, encoded.len));
    footer[0] ^= 1;
}

test "serverless graph metric routing resolves exact scores without decoding the vector" {
    const alloc = std.testing.allocator;
    var scores = try alloc.alloc(types.Score, score_block_entries + 1);
    var initialized: usize = 0;
    var scores_transferred = false;
    errdefer if (!scores_transferred) {
        for (scores[0..initialized]) |*score| score.deinit(alloc);
        alloc.free(scores);
    };
    for (scores, 0..) |*score, i| {
        score.* = .{
            .node_id = try std.fmt.allocPrint(alloc, "node:{d:0>4}", .{i}),
            .value = @floatFromInt(i),
        };
        initialized += 1;
    }
    var segment = types.Segment{
        .kind = .pagerank,
        .source_graph_artifact_id = try alloc.dupe(u8, "sha256:graph"),
        .source_graph_checksum = try alloc.dupe(u8, "sha256:sum"),
        .config_fingerprint = 42,
        .edge_filter = .{},
        .converged = true,
        .iterations_completed = 12,
        .delta = 0.00001,
        .scores = scores,
    };
    scores_transferred = true;
    defer segment.deinit(alloc);
    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    const control_len = try controlProbeLen(encoded.len, "sha256:graph", "sha256:sum", .{});
    const control = try decodeControl(encoded[0..control_len], .{});
    try std.testing.expectEqual(@as(u32, score_block_entries + 1), control.score_count);

    const footer_len = try routingFooterLenFromTrailer(encoded.len, encoded[encoded.len - routing_trailer_len ..]);
    const footer_offset = encoded.len - footer_len;
    var routing = try decodeRoutingIndexAlloc(alloc, encoded[footer_offset..], encoded.len);
    defer routing.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), routing.entries.len);
    try std.testing.expectEqual(score_block_entries + 1, routing.top_score_count);
    try std.testing.expectEqual(@as(usize, 5), routing.ranked_entries.len);
    const first_ranked_entry = routing.ranked_entries[0];
    const first_ranked = try decodeRankedScoreBlockWithCancellation(
        encoded[@intCast(first_ranked_entry.offset)..][0..first_ranked_entry.len],
        .none,
    );
    try std.testing.expectEqual(@as(usize, ranked_score_block_entries), first_ranked.len);
    try std.testing.expect(first_ranked.scores[0].eqlNode(first_ranked.node_prefix, "node:1024"));
    try std.testing.expectEqual(@as(f64, 1024), first_ranked.scores[0].value);
    try std.testing.expect(first_ranked.scores[first_ranked.len - 1].eqlNode(first_ranked.node_prefix, "node:0769"));
    try std.testing.expectEqual(control.score_data_offset, routing.entries[0].offset);
    const entry = routing.find("node:1024").?;
    try std.testing.expectEqual(@as(?f64, 1024), try scoreFromBlockWithCancellation(
        encoded[@intCast(entry.offset)..][0..entry.len],
        "node:1024",
        .none,
    ));
    try std.testing.expectEqual(@as(?f64, null), try scoreFromBlockWithCancellation(
        encoded[@intCast(entry.offset)..][0..entry.len],
        "node:missing",
        .none,
    ));

    const corrupted = try alloc.dupe(u8, encoded);
    defer alloc.free(corrupted);
    corrupted[@as(usize, @intCast(entry.offset)) + @sizeOf(u32)] ^= 0x01;
    try std.testing.expectError(error.InvalidGraphMetricSegment, decodeAlloc(alloc, corrupted));

    const corrupted_top = try alloc.dupe(u8, encoded);
    defer alloc.free(corrupted_top);
    corrupted_top[@as(usize, @intCast(first_ranked_entry.offset)) + @sizeOf(u32)] ^= 0x01;
    try std.testing.expectError(error.InvalidGraphMetricSegment, decodeAlloc(alloc, corrupted_top));
}

test "serverless graph metric segment round trips terminal materialization rejection" {
    const alloc = std.testing.allocator;
    var segment = types.Segment{
        .kind = .pagerank,
        .source_graph_artifact_id = try alloc.dupe(u8, "sha256:graph"),
        .source_graph_checksum = try alloc.dupe(u8, "sha256:sum"),
        .config_fingerprint = 42,
        .materialization_state = .rejected,
        .rejection_reason = .build_budget_exceeded,
        .edge_filter = .{},
        .converged = false,
        .iterations_completed = 0,
        .delta = 0,
        .scores = try alloc.alloc(types.Score, 0),
    };
    defer segment.deinit(alloc);
    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(types.MaterializationState.rejected, decoded.materialization_state);
    try std.testing.expectEqual(types.RejectionReason.build_budget_exceeded, decoded.rejection_reason);
    try std.testing.expectEqual(@as(usize, 0), decoded.scores.len);
}

test "serverless graph metric segment rejects unpublished legacy wire versions" {
    const alloc = std.testing.allocator;
    var segment = types.Segment{
        .kind = .degree,
        .source_graph_artifact_id = try alloc.dupe(u8, "sha256:graph"),
        .source_graph_checksum = try alloc.dupe(u8, "sha256:sum"),
        .config_fingerprint = 9,
        .edge_filter = .{},
        .converged = true,
        .iterations_completed = 1,
        .delta = 0,
        .scores = try alloc.alloc(types.Score, 0),
    };
    defer segment.deinit(alloc);
    const current = try encodeAlloc(alloc, segment);
    defer alloc.free(current);
    const version_one = try alloc.dupe(u8, current);
    defer alloc.free(version_one);
    std.mem.writeInt(u16, version_one[4..6], 1, .little);
    try std.testing.expectError(error.UnsupportedGraphMetricSegmentVersion, decodeHeader(version_one));
    try std.testing.expectError(error.UnsupportedGraphMetricSegmentVersion, decodeAlloc(alloc, version_one));

    const version_six = try alloc.dupe(u8, current);
    defer alloc.free(version_six);
    std.mem.writeInt(u16, version_six[4..6], 6, .little);
    try std.testing.expectError(error.UnsupportedGraphMetricSegmentVersion, decodeHeader(version_six));
    try std.testing.expectError(error.UnsupportedGraphMetricSegmentVersion, decodeAlloc(alloc, version_six));

    const future = try alloc.dupe(u8, current);
    defer alloc.free(future);
    std.mem.writeInt(u16, future[4..6], wire_version + 1, .little);
    try std.testing.expectError(error.UnsupportedGraphMetricSegmentVersion, decodeHeader(future));
    try std.testing.expectError(error.UnsupportedGraphMetricSegmentVersion, decodeAlloc(alloc, future));
}
