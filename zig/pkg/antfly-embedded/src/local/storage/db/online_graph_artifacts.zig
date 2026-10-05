// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
//! Physical graph transfer, including the deletion/precedence state needed by
//! future mutations. A graph generation is never copied between owners merely
//! because their index names match. Callers authenticate both catalog bindings
//! and fence publication before applying the returned afterimage.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const codec = @import("enrichment/artifact_codec.zig");
const states = @import("graph_asset_state.zig");
const contenders = @import("graph_edge_contender.zig");
const catalogs = @import("artifact_inventory.zig");
const view = @import("artifact_catalog_view.zig");
const Allocator = std.mem.Allocator;

pub const Effect = struct {
    key: []u8,
    value: ?[]u8,
    pub fn deinit(self: Effect, alloc: Allocator) void {
        alloc.free(self.key);
        if (self.value) |value| alloc.free(value);
    }
};

pub fn isKey(key: []const u8) bool {
    return keys.isGraphEdgeArtifactKey(key) or keys.isGraphAssetStateKey(key) or
        keys.isGraphEdgeContenderKey(key) or keys.isGraphGlobalEdgeContenderKey(key);
}

fn matches(key: []const u8, name: []const u8) bool {
    return keys.matchesGraphEdgeIndexName(key, name) or keys.matchesGraphAssetStateIndexName(key, name) or
        keys.matchesGraphEdgeContenderIndexName(key, name) or keys.matchesGraphGlobalEdgeContenderIndexName(key, name);
}

const Binding = struct { name: []const u8, source: u64, receiver: u64 };

/// Prepare once for a certified source/receiver catalog pair, then use O(1)
/// lookups for all records in a page. Arena ownership pins names and encoded
/// lookup keys; no caller catalog buffer survives by accident.
pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    bindings: std.StringHashMapUnmanaged(Binding),

    pub fn init(alloc: Allocator, source: catalogs.Catalogs, source_binding: catalogs.Binding, receiver: catalogs.Catalogs, receiver_binding: catalogs.Binding) !Plan {
        if (!source_binding.compatible(receiver_binding) or
            !std.mem.eql(u8, &source.digest(), &source_binding.digest) or
            !std.mem.eql(u8, &receiver.digest(), &receiver_binding.digest) or
            !std.mem.eql(u8, &try source.semanticDigest(alloc), &source_binding.semantic_digest) or
            !std.mem.eql(u8, &try receiver.semanticDigest(alloc), &receiver_binding.semantic_digest)) return error.ArtifactCatalogDrift;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var target: std.StringHashMapUnmanaged(view.Entry) = .empty;
        var receiver_view = try view.Iterator.init(receiver.indexes);
        while (try receiver_view.next()) |entry| {
            const inserted = try target.getOrPut(owned, entry.name);
            if (inserted.found_existing) return error.InvalidArtifactCatalogCommand;
            inserted.value_ptr.* = entry;
        }
        var bindings: std.StringHashMapUnmanaged(Binding) = .empty;
        var source_view = try view.Iterator.init(source.indexes);
        while (try source_view.next()) |entry| {
            if (entry.kind != 3) continue;
            const destination = target.get(entry.name) orelse return error.ArtifactCatalogDrift;
            if (destination.kind != 3 or entry.generation == 0 or destination.generation == 0) return error.ArtifactCatalogDrift;
            var encoded: std.ArrayListUnmanaged(u8) = .empty;
            try keys.appendEncodedComponent(&encoded, owned, entry.name);
            const inserted = try bindings.getOrPut(owned, encoded.items);
            if (inserted.found_existing) return error.InvalidArtifactCatalogCommand;
            inserted.value_ptr.* = .{ .name = try owned.dupe(u8, entry.name), .source = entry.generation, .receiver = destination.generation };
        }
        return .{ .arena = arena, .bindings = bindings };
    }

    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn rebind(self: *const Plan, alloc: Allocator, key: []const u8, value: ?[]const u8) !Effect {
        try validate(key, value);
        const doc_end = (keys.findComponentTerminator(key, 1) orelse unreachable) + 2;
        var index_start = doc_end + 1;
        if (keys.isGraphEdgeArtifactKey(key)) index_start = (keys.findComponentTerminator(key, index_start) orelse unreachable) + 2;
        const index_end = (keys.findComponentTerminator(key, index_start) orelse unreachable) + 2;
        const binding = self.bindings.get(key[index_start..index_end]) orelse return error.ArtifactCatalogDrift;
        return rebindWithBinding(alloc, key, value, binding);
    }
};

fn edgeGeneration(raw: []const u8) !u64 {
    const header = try codec.decodeHeader(raw);
    if (header.kind != .graph_edge or header.version != codec.graph_edge_codec_version or
        !header.flags.has_graph_generation or header.flags.portable_unbound_graph_generation or
        header.flags._reserved != 0 or header.payload_len < 36) return error.InvalidGraphTransfer;
    const payload = raw[codec.header_len..];
    const generation = std.mem.readInt(u64, payload[0..8], .little);
    if (generation == 0 or std.mem.readInt(u32, payload[32..36], .little) != payload.len - 36)
        return error.InvalidGraphTransfer;
    try @import("../../graph/edge_weight.zig").validateStored(@bitCast(std.mem.readInt(u64, payload[8..16], .little)));
    return generation;
}

fn stateGeneration(raw: []const u8, segment: bool) !u64 {
    if (!segment) return states.coverageGeneration(raw);
    if (raw.len < 16 or raw.len > states.hard_max_manifest_bytes or !std.mem.eql(u8, raw[0..4], "AGB1"))
        return error.InvalidGraphAssetState;
    const count = std.mem.readInt(u32, raw[12..16], .big);
    if (count == 0 or count > states.hard_max_edges_per_document or count > (raw.len - 16) / 4)
        return error.InvalidGraphAssetState;
    var offset: usize = 16;
    for (0..count) |_| {
        if (raw.len - offset < 4) return error.InvalidGraphAssetState;
        const len = std.mem.readInt(u32, raw[offset..][0..4], .big);
        offset += 4;
        if (len > raw.len - offset) return error.InvalidGraphAssetState;
        offset += len;
    }
    if (offset != raw.len) return error.InvalidGraphAssetState;
    return std.mem.readInt(u64, raw[4..12], .big);
}

/// Allocation-free structural check used by retained frame readers. Exact
/// catalog generation and embedded ownership checks follow at receiver apply.
pub fn validate(key: []const u8, value: ?[]const u8) !void {
    if (!isKey(key)) return error.InvalidGraphTransfer;
    const raw = value orelse return;
    if (raw.len > states.hard_max_manifest_bytes) return error.ResourceLimitExceeded;
    if (keys.isGraphEdgeArtifactKey(key)) {
        _ = try edgeGeneration(raw);
    } else if (keys.isGraphAssetStateKey(key)) {
        if (try stateGeneration(raw, !keys.isGraphAssetStateRootKey(key)) == 0) return error.InvalidGraphTransfer;
    } else {
        if (raw.len < 12) return error.InvalidGraphEdgeContender;
        const generation = std.mem.readInt(u64, raw[4..12], .big);
        if (generation == 0) return error.InvalidGraphTransfer;
        // Visible counts have no embedded key or payload and a fixed length.
        if (keys.isGraphEdgeContenderKey(key) and raw.len == 20) {
            _ = try contenders.decodeVisibleCount(raw, generation);
        } else {
            const entry = (try contenders.decode(raw, generation)) orelse return error.InvalidGraphTransfer;
            if (!keys.isGraphEdgeArtifactKey(entry.edge_key) or !keys.isGraphAssetStateRootKey(entry.state_key))
                return error.InvalidGraphTransfer;
            if (entry.payload.len != 0 and try edgeGeneration(entry.payload) != generation) return error.GraphGenerationMismatch;
            if (keys.isGraphGlobalEdgeContenderKey(key) != (entry.payload.len != 0)) return error.InvalidGraphTransfer;
        }
    }
}

fn sameOwner(a: []const u8, b: []const u8) bool {
    if (!keys.isInternalUserKey(a) or !keys.isInternalUserKey(b)) return false;
    const left_end = keys.findComponentTerminator(a, 1) orelse return false;
    const right_end = keys.findComponentTerminator(b, 1) orelse return false;
    // Encoded components are canonical even for embedded NULs. Comparing the
    // borrowed prefixes avoids two allocations per edge in a large manifest.
    return std.mem.eql(u8, a[1..left_end], b[1..right_end]);
}

fn rebindValue(alloc: Allocator, key: []const u8, raw: []const u8, name: []const u8, source_generation: u64, receiver_generation: u64) ![]u8 {
    try validate(key, raw);
    if (keys.isGraphEdgeArtifactKey(key)) {
        if (try edgeGeneration(raw) != source_generation) return error.GraphGenerationMismatch;
        const result = try alloc.dupe(u8, raw);
        std.mem.writeInt(u64, result[codec.header_len..][0..8], receiver_generation, .little);
        return result;
    }
    if (keys.isGraphAssetStateKey(key)) {
        const segment = !keys.isGraphAssetStateRootKey(key);
        if (try stateGeneration(raw, segment) != source_generation) return error.GraphGenerationMismatch;
        // Check embedded identities without allocating a per-edge key array.
        if (segment or try states.format(raw) == .v4) {
            var offset: usize = 16;
            const count = std.mem.readInt(u32, raw[12..16], .big);
            for (0..count) |_| {
                const len = std.mem.readInt(u32, raw[offset..][0..4], .big);
                offset += 4;
                const edge = raw[offset..][0..len];
                if (!keys.matchesGraphEdgeIndexName(edge, name) or !sameOwner(key, edge)) return error.InvalidGraphTransfer;
                offset += len;
            }
        }
        const result = try alloc.dupe(u8, raw);
        std.mem.writeInt(u64, result[4..12], receiver_generation, .big);
        return result;
    }
    if (std.mem.readInt(u64, raw[4..12], .big) != source_generation) return error.GraphGenerationMismatch;
    if (raw.len == 20 and keys.isGraphEdgeContenderKey(key)) {
        const document = (try keys.decodeDocumentComponentAlloc(alloc, key)) orelse return error.InvalidGraphTransfer;
        defer alloc.free(document);
        const expected = try keys.graphEdgeContenderCountKeyAlloc(alloc, document, name);
        defer alloc.free(expected);
        if (!std.mem.eql(u8, expected, key)) return error.InvalidGraphTransfer;
        const result = try alloc.dupe(u8, raw);
        std.mem.writeInt(u64, result[4..12], receiver_generation, .big);
        return result;
    }
    const entry = (try contenders.decode(raw, source_generation)) orelse return error.GraphGenerationMismatch;
    if (!keys.matchesGraphEdgeIndexName(entry.edge_key, name) or !keys.matchesGraphAssetStateIndexName(entry.state_key, name) or
        !sameOwner(key, entry.edge_key) or !sameOwner(key, entry.state_key)) return error.InvalidGraphTransfer;
    const document = (try keys.decodeDocumentComponentAlloc(alloc, key)) orelse return error.InvalidGraphTransfer;
    defer alloc.free(document);
    const expected = if (keys.isGraphGlobalEdgeContenderKey(key))
        try keys.graphGlobalEdgeContenderKeyAlloc(alloc, name, source_generation, entry.edge_key, @intCast(entry.source_priority), entry.state_key)
    else
        try keys.graphEdgeContenderKeyAlloc(alloc, document, name, entry.edge_key, entry.state_key);
    defer alloc.free(expected);
    if (!std.mem.eql(u8, expected, key)) return error.InvalidGraphTransfer;
    const result = try alloc.dupe(u8, raw);
    std.mem.writeInt(u64, result[4..12], receiver_generation, .big);
    if (entry.payload.len != 0) std.mem.writeInt(u64, result[result.len - entry.payload.len + codec.header_len ..][0..8], receiver_generation, .little);
    return result;
}

/// Both catalogs must be exact, previously certified bindings. Compatibility
/// is checked once per batch by the caller; names alone are not authority.
pub fn rebindAlloc(alloc: Allocator, key: []const u8, value: ?[]const u8, source_catalogs: catalogs.Catalogs, receiver_catalogs: catalogs.Catalogs) !Effect {
    try validate(key, value);
    var source = try view.Iterator.init(source_catalogs.indexes);
    var found: ?view.Entry = null;
    while (try source.next()) |entry| if (matches(key, entry.name)) {
        if (found != null or entry.kind != 3 or entry.generation == 0) return error.InvalidGraphTransfer;
        found = entry;
    };
    const original = found orelse return error.ArtifactCatalogDrift;
    var receiver = try view.Iterator.init(receiver_catalogs.indexes);
    var generation: ?u64 = null;
    while (try receiver.next()) |entry| if (std.mem.eql(u8, entry.name, original.name)) {
        if (generation != null or entry.kind != 3 or entry.generation == 0) return error.InvalidGraphTransfer;
        generation = entry.generation;
    };
    const target = generation orelse return error.ArtifactCatalogDrift;
    return rebindWithBinding(alloc, key, value, .{ .name = original.name, .source = original.generation, .receiver = target });
}

fn rebindWithBinding(alloc: Allocator, key: []const u8, value: ?[]const u8, binding: Binding) !Effect {
    const rebound_key = if (keys.isGraphGlobalEdgeContenderKey(key))
        try keys.rebindGraphGlobalEdgeContenderKeyAlloc(alloc, key, binding.source, binding.receiver)
    else
        try alloc.dupe(u8, key);
    errdefer alloc.free(rebound_key);
    return .{ .key = rebound_key, .value = if (value) |raw| try rebindValue(alloc, key, raw, binding.name, binding.source, binding.receiver) else null };
}

const test_source: catalogs.Catalogs = .{ .indexes = "AIDX\x02\x00\x00\x00\x01\x00\x00\x00" ++
    "\x01\x00\x00\x00g\x03\x02\x00\x00\x00{}\x07\x00\x00\x00\x00\x00\x00\x00" };
const test_receiver: catalogs.Catalogs = .{ .indexes = "AIDX\x02\x00\x00\x00\x01\x00\x00\x00" ++
    "\x01\x00\x00\x00g\x03\x02\x00\x00\x00{}\x09\x00\x00\x00\x00\x00\x00\x00" };

test "online graph transfer rebinds edge bytes and rejects stale incarnations" {
    const alloc = std.testing.allocator;
    const key = try keys.graphEdgeArtifactKeyAlloc(alloc, "doc", "g", "links", "target");
    defer alloc.free(key);
    const raw = try codec.encodeGraphEdgeAlloc(alloc, 123, 7, 0.5, 10, 11, "{}");
    defer alloc.free(raw);
    const rebound = try rebindAlloc(alloc, key, raw, test_source, test_receiver);
    defer rebound.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 9), try edgeGeneration(rebound.value.?));
    try std.testing.expectEqualSlices(u8, raw[codec.header_len + 8 ..], rebound.value.?[codec.header_len + 8 ..]);
    try std.testing.expectEqualSlices(u8, raw[0..codec.header_len], rebound.value.?[0..codec.header_len]);
    try std.testing.expectError(error.GraphGenerationMismatch, rebindAlloc(alloc, key, raw, test_receiver, test_source));
    const portable = try codec.encodePortableUnboundGraphEdgeAlloc(alloc, 0.5, 10, 11, "{}");
    defer alloc.free(portable);
    try std.testing.expectError(error.InvalidGraphTransfer, validate(key, portable));
}

test "online graph transfer preserves manifests contenders counts and tombstones" {
    const alloc = std.testing.allocator;
    const edge = try keys.graphEdgeArtifactKeyAlloc(alloc, "doc", "g", "links", "target");
    defer alloc.free(edge);
    const state_prefix = try keys.graphAssetStateIndexPrefixAlloc(alloc, "doc", "g");
    defer alloc.free(state_prefix);
    const state = try std.mem.concat(alloc, u8, &.{ state_prefix, "field\x00\x00" });
    defer alloc.free(state);
    const Pair = struct { key: []const u8 };
    const writes = [_]Pair{.{ .key = edge }};
    const state_raw = try states.encodeAlloc(alloc, 7, &writes);
    defer alloc.free(state_raw);
    const root = try rebindAlloc(alloc, state, state_raw, test_source, test_receiver);
    defer root.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 9), try states.coverageGeneration(root.value.?));
    const segment_key = try keys.graphAssetStateSegmentKeyAlloc(alloc, state, 0);
    defer alloc.free(segment_key);
    const segment_raw = try states.encodeSegmentAlloc(alloc, 7, &writes);
    defer alloc.free(segment_raw);
    const segment = try rebindAlloc(alloc, segment_key, segment_raw, test_source, test_receiver);
    defer segment.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 9), try stateGeneration(segment.value.?, true));
    const edge_raw = try codec.encodeGraphEdgeAlloc(alloc, null, 7, 1, 0, 0, "");
    defer alloc.free(edge_raw);
    const contender_key = try keys.graphGlobalEdgeContenderKeyAlloc(alloc, "g", 7, edge, 2, state);
    defer alloc.free(contender_key);
    const contender_raw = try contenders.encodeAlloc(alloc, 7, 2, edge, state, edge_raw);
    defer alloc.free(contender_raw);
    const contender = try rebindAlloc(alloc, contender_key, contender_raw, test_source, test_receiver);
    defer contender.deinit(alloc);
    const expected_key = try keys.graphGlobalEdgeContenderKeyAlloc(alloc, "g", 9, edge, 2, state);
    defer alloc.free(expected_key);
    try std.testing.expectEqualSlices(u8, expected_key, contender.key);
    const decoded = (try contenders.decode(contender.value.?, 9)).?;
    try std.testing.expectEqual(@as(u64, 9), try edgeGeneration(decoded.payload));
    const tombstone = try rebindAlloc(alloc, contender_key, null, test_source, test_receiver);
    defer tombstone.deinit(alloc);
    try std.testing.expectEqualSlices(u8, expected_key, tombstone.key);
    try std.testing.expect(tombstone.value == null);
    const count_key = try keys.graphEdgeContenderCountKeyAlloc(alloc, "doc", "g");
    defer alloc.free(count_key);
    const count_raw = try contenders.encodeVisibleCount(7, 13);
    const count = try rebindAlloc(alloc, count_key, &count_raw, test_source, test_receiver);
    defer count.deinit(alloc);
    try std.testing.expectEqual(@as(?usize, 13), try contenders.decodeVisibleCount(count.value.?, 9));
}

test "online graph transfer rejects foreign ownership hidden in a manifest" {
    const alloc = std.testing.allocator;
    const edge = try keys.graphEdgeArtifactKeyAlloc(alloc, "other", "g", "links", "target");
    defer alloc.free(edge);
    const prefix = try keys.graphAssetStateIndexPrefixAlloc(alloc, "doc", "g");
    defer alloc.free(prefix);
    const key = try std.mem.concat(alloc, u8, &.{ prefix, "field\x00\x00" });
    defer alloc.free(key);
    const writes = [_]struct { key: []const u8 }{.{ .key = edge }};
    const raw = try states.encodeAlloc(alloc, 7, &writes);
    defer alloc.free(raw);
    try std.testing.expectError(error.InvalidGraphTransfer, rebindAlloc(alloc, key, raw, test_source, test_receiver));
}

test "online graph transfer pinned plan preserves distinct logical endpoints" {
    const alloc = std.testing.allocator;
    const source_binding: catalogs.Binding = .{ .epoch = 1, .digest = test_source.digest(), .semantic_digest = try test_source.semanticDigest(alloc) };
    const receiver_binding: catalogs.Binding = .{ .epoch = 2, .digest = test_receiver.digest(), .semantic_digest = try test_receiver.semanticDigest(alloc) };
    var plan = try Plan.init(alloc, test_source, source_binding, test_receiver, receiver_binding);
    defer plan.deinit();
    const edge = try keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "owner\x00doc", "g", "links", "person/target", "person/source");
    defer alloc.free(edge);
    const raw = try codec.encodeGraphEdgeAlloc(alloc, null, 7, 1, 0, 0, "");
    defer alloc.free(raw);
    const result = try plan.rebind(alloc, edge, raw);
    defer result.deinit(alloc);
    try std.testing.expectEqualSlices(u8, edge, result.key);
    try std.testing.expectEqual(@as(u64, 9), try edgeGeneration(result.value.?));
    var wrong = source_binding;
    wrong.digest[0] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogDrift, Plan.init(alloc, test_source, wrong, test_receiver, receiver_binding));
}

test "online graph transfer requires explicit source effect protocol for values and tombstones" {
    const alloc = std.testing.allocator;
    const pages = @import("merge_page_contract.zig");
    const edge = try keys.graphEdgeArtifactKeyAlloc(alloc, "doc", "g", "links", "target");
    defer alloc.free(edge);
    const raw = try codec.encodeGraphEdgeAlloc(alloc, null, 7, 1, 0, 0, "");
    defer alloc.free(raw);
    var source: pages.Source = .{
        .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 },
        .pin_digest = @splat(1),
        .applied_index = 1,
        .retention = .{ .epoch = 1, .after_sequence = 0 },
        .artifact_catalog = .{ .epoch = 1, .digest = test_source.digest(), .semantic_digest = try test_source.semanticDigest(alloc) },
    };
    try std.testing.expectError(error.InvalidMergePage, pages.validateArtifactEffect(source, edge, raw));
    try std.testing.expectError(error.InvalidMergePage, pages.validateArtifactEffect(source, edge, null));
    source.artifact_catalog.?.effect_protocol = 15;
    try pages.validateArtifactEffect(source, edge, raw);
    try pages.validateArtifactEffect(source, edge, null);
    try std.testing.expectError(error.InvalidMergePage, pages.validateArtifactEffect(source, catalogs.ordered_key, "foreign authority"));
    source.artifact_catalog.?.effect_protocol = 16;
    try std.testing.expectError(error.InvalidMergePage, pages.validateArtifactEffect(source, edge, raw));
}
