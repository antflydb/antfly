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
const types = @import("types.zig");

pub const wire_magic = "AFGM";
pub const wire_version: u16 = 4;
const fixed_header_len_v1 = 4 + 2 + 1 + 1 + 4 + 8 + 8 + 4 + 4 + 4 + 4 + 4;
const fixed_header_len_v2 = fixed_header_len_v1 + 2;
const fixed_header_len = fixed_header_len_v2 + 8;
const routing_magic = "AFGR";
pub const routing_trailer_len: usize = 8;
pub const score_block_entries: usize = 1024;
pub const max_routing_bytes: usize = 8 * 1024 * 1024;
pub const max_score_node_id_bytes: usize = 4096;
const routing_header_len = routing_magic.len + @sizeOf(u32);
const routing_entry_fixed_len = @sizeOf(u32) + @sizeOf(u64) + @sizeOf(u32);

pub const Header = struct {
    version: u16,
    kind: @import("../../graph/graph.zig").GraphMetricKind,
    materialization_state: types.MaterializationState,
    rejection_reason: types.RejectionReason,
    config_fingerprint: u64,
    materializer_fingerprint: u64,
    graph_index_name: []const u8,
    metric_name: []const u8,
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
    first_node_id: []const u8,
    offset: u64,
    len: usize,
};

pub const RoutingIndex = struct {
    entries: []RoutingEntry,
    footer_offset: u64,

    pub fn deinit(self: *RoutingIndex, alloc: Allocator) void {
        alloc.free(self.entries);
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
    node_id: []const u8,
    value: f64,
};

pub const DecodedScoreBlock = struct {
    scores: [score_block_entries]BorrowedBlockScore = undefined,
    len: usize = 0,

    pub fn score(self: *const DecodedScoreBlock, node_id: []const u8) ?f64 {
        var low: usize = 0;
        var high = self.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (std.mem.order(u8, self.scores[mid].node_id, node_id)) {
                .lt => low = mid + 1,
                .gt => high = mid,
                .eq => return self.scores[mid].value,
            }
        }
        return null;
    }
};

/// Exact bounded prefix needed to decode provenance from an artifact produced
/// by this codec. Older wire versions have shorter fixed headers, so the small
/// current-version surplus remains valid for backward-compatible status reads.
pub fn headerProbeLen(
    artifact_byte_len: u64,
    graph_index_name: []const u8,
    metric_name: []const u8,
    source_graph_artifact_id: []const u8,
    source_graph_checksum: []const u8,
) !usize {
    var required: usize = fixed_header_len;
    required = std.math.add(usize, required, graph_index_name.len) catch return error.GraphMetricSegmentTooLarge;
    required = std.math.add(usize, required, metric_name.len) catch return error.GraphMetricSegmentTooLarge;
    required = std.math.add(usize, required, source_graph_artifact_id.len) catch return error.GraphMetricSegmentTooLarge;
    required = std.math.add(usize, required, source_graph_checksum.len) catch return error.GraphMetricSegmentTooLarge;
    const required_u64 = std.math.cast(u64, required) orelse return error.GraphMetricSegmentTooLarge;
    return @intCast(@min(artifact_byte_len, required_u64));
}

pub fn controlProbeLen(
    artifact_byte_len: u64,
    graph_index_name: []const u8,
    metric_name: []const u8,
    source_graph_artifact_id: []const u8,
    source_graph_checksum: []const u8,
    edge_filter: graph_mod.GraphMetricEdgeFilter,
) !usize {
    var required = try headerProbeLen(
        std.math.maxInt(u64),
        graph_index_name,
        metric_name,
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
    var pos = fixedHeaderLenForVersion(header.version);
    pos = std.math.add(usize, pos, header.graph_index_name.len) catch return error.InvalidGraphMetricSegment;
    pos = std.math.add(usize, pos, header.metric_name.len) catch return error.InvalidGraphMetricSegment;
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
    return decodeRoutingIndexWithCancellationAlloc(alloc, footer, artifact_byte_len, .none);
}

pub fn decodeRoutingIndexWithCancellationAlloc(
    alloc: Allocator,
    footer: []const u8,
    artifact_byte_len: u64,
    cancellation: CancellationToken,
) !RoutingIndex {
    try cancellation.check();
    if (footer.len < routing_trailer_len) return error.InvalidGraphMetricSegment;
    const footer_len = try routingFooterLenFromTrailer(artifact_byte_len, footer[footer.len - routing_trailer_len ..]);
    if (footer_len != footer.len) return error.InvalidGraphMetricSegment;
    const footer_offset = artifact_byte_len - footer.len;
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
        if (len_u32 == 0) return error.InvalidGraphMetricSegment;
        const first_node_id = try take(footer, &pos, first_node_id_len);
        const end = std.math.add(u64, offset, len_u32) catch return error.InvalidGraphMetricSegment;
        if (end > footer_offset or (previous_end != null and offset != previous_end.?)) return error.InvalidGraphMetricSegment;
        if (entry_index > 0 and std.mem.order(u8, entries[entry_index - 1].first_node_id, first_node_id) != .lt) {
            return error.InvalidGraphMetricSegment;
        }
        entry.* = .{ .first_node_id = first_node_id, .offset = offset, .len = len_u32 };
        previous_end = end;
    }
    if (pos + routing_trailer_len != footer.len) return error.InvalidGraphMetricSegment;
    if (entries.len > 0 and previous_end.? != footer_offset) return error.InvalidGraphMetricSegment;
    try cancellation.check();
    return .{ .entries = entries, .footer_offset = footer_offset };
}

pub fn decodeScoreBlockWithCancellation(block: []const u8, cancellation: CancellationToken) !DecodedScoreBlock {
    var decoded = DecodedScoreBlock{};
    var pos: usize = 0;
    var previous: ?[]const u8 = null;
    var score_index: usize = 0;
    while (pos < block.len) : (score_index += 1) {
        if (score_index >= score_block_entries) return error.InvalidGraphMetricSegment;
        if (score_index % 256 == 0) try cancellation.check();
        const node_len = try readInt(u32, block, &pos);
        if (node_len == 0 or node_len > max_score_node_id_bytes) return error.InvalidGraphMetricSegment;
        const value: f64 = @bitCast(try readInt(u64, block, &pos));
        if (!std.math.isFinite(value)) return error.InvalidGraphMetricSegment;
        const current = try take(block, &pos, node_len);
        if (previous != null and std.mem.order(u8, previous.?, current) != .lt) return error.InvalidGraphMetricSegment;
        decoded.scores[score_index] = .{ .node_id = current, .value = value };
        previous = current;
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
    if (data.len < fixed_header_len_v1) return error.InvalidGraphMetricSegment;
    var pos: usize = 0;
    if (!std.mem.eql(u8, try take(data, &pos, 4), wire_magic)) return error.InvalidGraphMetricSegment;
    const version = try readInt(u16, data, &pos);
    if (version == 0 or version > wire_version) return error.UnsupportedGraphMetricSegmentVersion;
    if (pos >= data.len) return error.InvalidGraphMetricSegment;
    const kind = std.enums.fromInt(@import("../../graph/graph.zig").GraphMetricKind, data[pos]) orelse return error.InvalidGraphMetricSegment;
    pos += 1;
    const materialization_state: types.MaterializationState = if (version >= 2) blk: {
        if (pos >= data.len) return error.InvalidGraphMetricSegment;
        const value = std.enums.fromInt(types.MaterializationState, data[pos]) orelse return error.InvalidGraphMetricSegment;
        pos += 1;
        break :blk value;
    } else .ready;
    const rejection_reason: types.RejectionReason = if (version >= 2) blk: {
        if (pos >= data.len) return error.InvalidGraphMetricSegment;
        const value = std.enums.fromInt(types.RejectionReason, data[pos]) orelse return error.InvalidGraphMetricSegment;
        pos += 1;
        break :blk value;
    } else .none;
    const converged_byte = (try take(data, &pos, 1))[0];
    if (converged_byte > 1) return error.InvalidGraphMetricSegment;
    const iterations = try readInt(u32, data, &pos);
    const fingerprint = try readInt(u64, data, &pos);
    const materializer_fingerprint = if (version >= 3) try readInt(u64, data, &pos) else 0;
    const delta: f64 = @bitCast(try readInt(u64, data, &pos));
    if (!std.math.isFinite(delta)) return error.InvalidGraphMetricSegment;
    switch (materialization_state) {
        .ready => if (rejection_reason != .none) return error.InvalidGraphMetricSegment,
        .rejected => if (rejection_reason == .none or converged_byte != 0 or iterations != 0 or delta != 0) return error.InvalidGraphMetricSegment,
    }
    const graph_len = try readInt(u32, data, &pos);
    const metric_len = try readInt(u32, data, &pos);
    const artifact_len = try readInt(u32, data, &pos);
    const checksum_len = try readInt(u32, data, &pos);
    const edge_type_count = try readInt(u32, data, &pos);
    const graph_index_name = try take(data, &pos, graph_len);
    const metric_name = try take(data, &pos, metric_len);
    const source_graph_artifact_id = try take(data, &pos, artifact_len);
    const source_graph_checksum = try take(data, &pos, checksum_len);
    return .{
        .kind = kind,
        .version = version,
        .materialization_state = materialization_state,
        .rejection_reason = rejection_reason,
        .config_fingerprint = fingerprint,
        .materializer_fingerprint = materializer_fingerprint,
        .graph_index_name = graph_index_name,
        .metric_name = metric_name,
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
    try validateSegmentWithCancellation(segment, cancellation);
    var size: usize = fixed_header_len;
    size = std.math.add(usize, size, segment.graph_index_name.len) catch return error.GraphMetricSegmentTooLarge;
    size = std.math.add(usize, size, segment.metric_name.len) catch return error.GraphMetricSegmentTooLarge;
    size = std.math.add(usize, size, segment.source_graph_artifact_id.len) catch return error.GraphMetricSegmentTooLarge;
    size = std.math.add(usize, size, segment.source_graph_checksum.len) catch return error.GraphMetricSegmentTooLarge;
    for (segment.edge_filter.types) |edge_type| {
        size = std.math.add(usize, size, 4 + edge_type.len) catch return error.GraphMetricSegmentTooLarge;
    }
    size = std.math.add(usize, size, @sizeOf(u32)) catch return error.GraphMetricSegmentTooLarge;
    for (segment.scores, 0..) |score, score_index| {
        if (score_index % 4096 == 0) try cancellation.check();
        size = std.math.add(usize, size, 4 + 8) catch return error.GraphMetricSegmentTooLarge;
        size = std.math.add(usize, size, score.node_id.len) catch return error.GraphMetricSegmentTooLarge;
    }
    size = std.math.add(usize, size, try routingEncodedSize(segment.scores)) catch return error.GraphMetricSegmentTooLarge;
    return size;
}

pub fn encodeAlloc(alloc: Allocator, segment: types.Segment) ![]u8 {
    return encodeAllocWithCancellation(alloc, segment, .none);
}

pub fn encodeAllocWithCancellation(alloc: Allocator, segment: types.Segment, cancellation: CancellationToken) ![]u8 {
    const size = try encodedSizeWithCancellation(segment, cancellation);
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
    putInt(u32, data, &pos, @intCast(segment.graph_index_name.len));
    putInt(u32, data, &pos, @intCast(segment.metric_name.len));
    putInt(u32, data, &pos, @intCast(segment.source_graph_artifact_id.len));
    putInt(u32, data, &pos, @intCast(segment.source_graph_checksum.len));
    putInt(u32, data, &pos, @intCast(segment.edge_filter.types.len));
    putBytes(data, &pos, segment.graph_index_name);
    putBytes(data, &pos, segment.metric_name);
    putBytes(data, &pos, segment.source_graph_artifact_id);
    putBytes(data, &pos, segment.source_graph_checksum);
    for (segment.edge_filter.types) |edge_type| {
        putInt(u32, data, &pos, @intCast(edge_type.len));
        putBytes(data, &pos, edge_type);
    }
    putInt(u32, data, &pos, @intCast(segment.scores.len));
    const score_data_offset = pos;
    for (segment.scores, 0..) |score, score_index| {
        if (score_index % 4096 == 0) try cancellation.check();
        putInt(u32, data, &pos, @intCast(score.node_id.len));
        putInt(u64, data, &pos, @bitCast(score.value));
        putBytes(data, &pos, score.node_id);
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
        var block_len: usize = 0;
        for (segment.scores[score_index..block_end]) |score| {
            block_len = std.math.add(usize, block_len, 12 + score.node_id.len) catch return error.GraphMetricSegmentTooLarge;
        }
        const first_node_id = segment.scores[score_index].node_id;
        putInt(u32, data, &pos, @intCast(first_node_id.len));
        putInt(u64, data, &pos, @intCast(block_offset));
        putInt(u32, data, &pos, @intCast(block_len));
        putBytes(data, &pos, first_node_id);
        block_offset += block_len;
    }
    std.debug.assert(block_offset == footer_offset);
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
    if (data.len < fixed_header_len_v1 + 4) return error.InvalidGraphMetricSegment;
    var pos: usize = 0;
    if (!std.mem.eql(u8, take(data, &pos, 4) catch return error.InvalidGraphMetricSegment, wire_magic)) return error.InvalidGraphMetricSegment;
    const version = readInt(u16, data, &pos) catch return error.InvalidGraphMetricSegment;
    if (version == 0 or version > wire_version) return error.UnsupportedGraphMetricSegmentVersion;
    const kind = std.enums.fromInt(@import("../../graph/graph.zig").GraphMetricKind, data[pos]) orelse return error.InvalidGraphMetricSegment;
    pos += 1;
    const materialization_state: types.MaterializationState = if (version >= 2) blk: {
        if (pos >= data.len) return error.InvalidGraphMetricSegment;
        const value = std.enums.fromInt(types.MaterializationState, data[pos]) orelse return error.InvalidGraphMetricSegment;
        pos += 1;
        break :blk value;
    } else .ready;
    const rejection_reason: types.RejectionReason = if (version >= 2) blk: {
        if (pos >= data.len) return error.InvalidGraphMetricSegment;
        const value = std.enums.fromInt(types.RejectionReason, data[pos]) orelse return error.InvalidGraphMetricSegment;
        pos += 1;
        break :blk value;
    } else .none;
    const converged_byte = data[pos];
    pos += 1;
    if (converged_byte > 1) return error.InvalidGraphMetricSegment;
    const iterations = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const fingerprint = readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment;
    const materializer_fingerprint = if (version >= 3)
        readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment
    else
        0;
    const delta: f64 = @bitCast(readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment);
    if (!std.math.isFinite(delta)) return error.InvalidGraphMetricSegment;
    const graph_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const metric_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const artifact_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const checksum_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const edge_type_count = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
    const names_len = std.math.add(usize, graph_len, metric_len) catch return error.InvalidGraphMetricSegment;
    const identity_len = std.math.add(usize, artifact_len, checksum_len) catch return error.InvalidGraphMetricSegment;
    try budget.admitBytes(std.math.add(usize, names_len, identity_len) catch return error.InvalidGraphMetricSegment);
    const graph_name = try dupeTake(alloc, data, &pos, graph_len);
    errdefer alloc.free(graph_name);
    const metric_name = try dupeTake(alloc, data, &pos, metric_len);
    errdefer alloc.free(metric_name);
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
    _ = try budget.admitCount(types.Score, score_count, data.len - pos, 12);
    const scores = try alloc.alloc(types.Score, score_count);
    errdefer alloc.free(scores);
    var initialized: usize = 0;
    errdefer for (scores[0..initialized]) |*score| score.deinit(alloc);
    for (scores, 0..) |*score, score_index| {
        if (score_index % 4096 == 0) try cancellation.check();
        const node_len = readInt(u32, data, &pos) catch return error.InvalidGraphMetricSegment;
        const value: f64 = @bitCast(readInt(u64, data, &pos) catch return error.InvalidGraphMetricSegment);
        if (!std.math.isFinite(value)) return error.InvalidGraphMetricSegment;
        try budget.admitBytes(node_len);
        score.* = .{ .node_id = try dupeTake(alloc, data, &pos, node_len), .value = value };
        initialized += 1;
    }
    if (version >= 4) {
        try validateRoutingFooter(data, pos, scores, cancellation);
    } else if (pos != data.len) return error.InvalidGraphMetricSegment;
    var segment = types.Segment{ .metadata_version = version, .graph_index_name = graph_name, .metric_name = metric_name, .kind = kind, .source_graph_artifact_id = artifact_id, .source_graph_checksum = checksum, .config_fingerprint = fingerprint, .materializer_fingerprint = materializer_fingerprint, .materialization_state = materialization_state, .rejection_reason = rejection_reason, .edge_filter = .{ .mode = if (edge_type_count == 0) .all else .types, .types = edge_types }, .converged = converged_byte == 1, .iterations_completed = iterations, .delta = delta, .scores = scores };
    errdefer segment.deinit(alloc);
    try validateSegmentWithCancellation(segment, cancellation);
    return segment;
}

fn validateSegment(segment: types.Segment) !void {
    return validateSegmentWithCancellation(segment, .none);
}

fn validateSegmentWithCancellation(segment: types.Segment, cancellation: CancellationToken) !void {
    try cancellation.check();
    if (segment.graph_index_name.len == 0 or segment.metric_name.len == 0 or segment.source_graph_artifact_id.len == 0 or segment.source_graph_checksum.len == 0) return error.InvalidGraphMetricSegment;
    _ = std.math.cast(u32, segment.graph_index_name.len) orelse return error.GraphMetricSegmentTooLarge;
    _ = std.math.cast(u32, segment.metric_name.len) orelse return error.GraphMetricSegmentTooLarge;
    _ = std.math.cast(u32, segment.source_graph_artifact_id.len) orelse return error.GraphMetricSegmentTooLarge;
    _ = std.math.cast(u32, segment.source_graph_checksum.len) orelse return error.GraphMetricSegmentTooLarge;
    _ = std.math.cast(u32, segment.scores.len) orelse return error.GraphMetricSegmentTooLarge;
    if (!std.math.isFinite(segment.delta)) return error.InvalidGraphMetricSegment;
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
        if (score.node_id.len == 0 or !std.math.isFinite(score.value)) return error.InvalidGraphMetricSegment;
        if (score.node_id.len > max_score_node_id_bytes) return error.GraphMetricSegmentTooLarge;
        _ = std.math.cast(u32, score.node_id.len) orelse return error.GraphMetricSegmentTooLarge;
        if (i > 0 and std.mem.order(u8, segment.scores[i - 1].node_id, score.node_id) != .lt) return error.InvalidGraphMetricSegment;
    }
    _ = try routingEncodedSize(segment.scores);
    try cancellation.check();
}

fn fixedHeaderLenForVersion(version: u16) usize {
    return if (version == 1) fixed_header_len_v1 else if (version == 2) fixed_header_len_v2 else fixed_header_len;
}

fn scoreBlockCount(score_count: usize) usize {
    return score_count / score_block_entries + @intFromBool(score_count % score_block_entries != 0);
}

fn routingEncodedSize(scores: []const types.Score) !usize {
    var size: usize = routing_header_len + routing_trailer_len;
    var score_index: usize = 0;
    while (score_index < scores.len) : (score_index += score_block_entries) {
        size = std.math.add(usize, size, routing_entry_fixed_len) catch return error.GraphMetricSegmentTooLarge;
        size = std.math.add(usize, size, scores[score_index].node_id.len) catch return error.GraphMetricSegmentTooLarge;
    }
    if (size > max_routing_bytes) return error.GraphMetricSegmentTooLarge;
    return size;
}

fn validateRoutingFooter(data: []const u8, footer_offset: usize, scores: []const types.Score, cancellation: CancellationToken) !void {
    try cancellation.check();
    if (footer_offset > data.len or data.len - footer_offset < routing_header_len + routing_trailer_len) {
        return error.InvalidGraphMetricSegment;
    }
    const footer = data[footer_offset..];
    if (try routingFooterLenFromTrailer(data.len, footer[footer.len - routing_trailer_len ..]) != footer.len) {
        return error.InvalidGraphMetricSegment;
    }
    var pos: usize = 0;
    if (!std.mem.eql(u8, try take(footer, &pos, routing_magic.len), routing_magic)) return error.InvalidGraphMetricSegment;
    if (try readInt(u32, footer, &pos) != scoreBlockCount(scores.len)) return error.InvalidGraphMetricSegment;
    var expected_offset = footer_offset;
    for (0..scoreBlockCount(scores.len)) |block_index| {
        try cancellation.check();
        const score_start = block_index * score_block_entries;
        const score_end = @min(scores.len, score_start + score_block_entries);
        var block_len: usize = 0;
        for (scores[score_start..score_end], 0..) |score, block_score_index| {
            if (block_score_index % 256 == 0) try cancellation.check();
            block_len = std.math.add(usize, block_len, 12 + score.node_id.len) catch return error.InvalidGraphMetricSegment;
        }
        expected_offset -= block_len;
    }
    for (0..scoreBlockCount(scores.len)) |block_index| {
        try cancellation.check();
        const score_start = block_index * score_block_entries;
        const score_end = @min(scores.len, score_start + score_block_entries);
        var block_len: usize = 0;
        for (scores[score_start..score_end], 0..) |score, block_score_index| {
            if (block_score_index % 256 == 0) try cancellation.check();
            block_len += 12 + score.node_id.len;
        }
        const first_len = try readInt(u32, footer, &pos);
        const offset = try readInt(u64, footer, &pos);
        const encoded_block_len = try readInt(u32, footer, &pos);
        const first = try take(footer, &pos, first_len);
        if (!std.mem.eql(u8, first, scores[score_start].node_id) or
            offset != expected_offset or encoded_block_len != block_len)
        {
            return error.InvalidGraphMetricSegment;
        }
        expected_offset += block_len;
    }
    if (expected_offset != footer_offset or pos + routing_trailer_len != footer.len) return error.InvalidGraphMetricSegment;
    try cancellation.check();
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
    var segment = types.Segment{ .graph_index_name = try alloc.dupe(u8, "graph"), .metric_name = try alloc.dupe(u8, "pagerank"), .kind = .pagerank, .source_graph_artifact_id = try alloc.dupe(u8, "sha256:graph"), .source_graph_checksum = try alloc.dupe(u8, "sha256:sum"), .config_fingerprint = 42, .edge_filter = .{}, .converged = true, .iterations_completed = 12, .delta = 0.00001, .scores = scores };
    defer segment.deinit(alloc);
    const encoded = try encodeAlloc(alloc, segment);
    defer alloc.free(encoded);
    const header_len = try headerProbeLen(
        encoded.len,
        segment.graph_index_name,
        segment.metric_name,
        segment.source_graph_artifact_id,
        segment.source_graph_checksum,
    );
    try std.testing.expectEqual(
        fixed_header_len + segment.graph_index_name.len + segment.metric_name.len + segment.source_graph_artifact_id.len + segment.source_graph_checksum.len,
        header_len,
    );
    const header = try decodeHeader(encoded[0..header_len]);
    try std.testing.expectEqualStrings(segment.metric_name, header.metric_name);
    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(?f64, 0.75), decoded.score("b"));
    try std.testing.expect(decoded.score("missing") == null);
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
        .graph_index_name = try alloc.dupe(u8, "graph"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
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
    const control_len = try controlProbeLen(encoded.len, "graph", "pagerank", "sha256:graph", "sha256:sum", .{});
    const control = try decodeControl(encoded[0..control_len], .{});
    try std.testing.expectEqual(@as(u32, score_block_entries + 1), control.score_count);

    const footer_len = try routingFooterLenFromTrailer(encoded.len, encoded[encoded.len - routing_trailer_len ..]);
    const footer_offset = encoded.len - footer_len;
    var routing = try decodeRoutingIndexAlloc(alloc, encoded[footer_offset..], encoded.len);
    defer routing.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), routing.entries.len);
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
}

test "serverless graph metric segment round trips terminal materialization rejection" {
    const alloc = std.testing.allocator;
    var segment = types.Segment{
        .graph_index_name = try alloc.dupe(u8, "graph"),
        .metric_name = try alloc.dupe(u8, "pagerank"),
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

test "serverless graph metric segment decodes version one artifacts as ready" {
    const alloc = std.testing.allocator;
    var segment = types.Segment{
        .graph_index_name = try alloc.dupe(u8, "graph"),
        .metric_name = try alloc.dupe(u8, "degree"),
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
    const version_four = try encodeAlloc(alloc, segment);
    defer alloc.free(version_four);
    const footer_len = try routingFooterLenFromTrailer(version_four.len, version_four[version_four.len - routing_trailer_len ..]);
    const version_three = try alloc.dupe(u8, version_four[0 .. version_four.len - footer_len]);
    defer alloc.free(version_three);
    std.mem.writeInt(u16, version_three[4..6], 3, .little);
    const version_one = try alloc.alloc(u8, version_three.len - 10);
    defer alloc.free(version_one);
    @memcpy(version_one[0..7], version_three[0..7]);
    @memcpy(version_one[7..20], version_three[9..22]);
    @memcpy(version_one[20..], version_three[30..]);
    std.mem.writeInt(u16, version_one[4..6], 1, .little);

    const header = try decodeHeader(version_one);
    try std.testing.expectEqual(types.MaterializationState.ready, header.materialization_state);
    try std.testing.expectEqual(types.RejectionReason.none, header.rejection_reason);
    var decoded = try decodeAlloc(alloc, version_one);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(types.MaterializationState.ready, decoded.materialization_state);
    try std.testing.expectEqual(types.RejectionReason.none, decoded.rejection_reason);
    try std.testing.expectEqual(@as(u64, 0), decoded.materializer_fingerprint);

    const version_two = try alloc.alloc(u8, version_three.len - 8);
    defer alloc.free(version_two);
    @memcpy(version_two[0..22], version_three[0..22]);
    @memcpy(version_two[22..], version_three[30..]);
    std.mem.writeInt(u16, version_two[4..6], 2, .little);
    var decoded_v2 = try decodeAlloc(alloc, version_two);
    defer decoded_v2.deinit(alloc);
    try std.testing.expectEqual(types.MaterializationState.ready, decoded_v2.materialization_state);
    try std.testing.expectEqual(@as(u64, 0), decoded_v2.materializer_fingerprint);
}
