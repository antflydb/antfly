// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const resolver_lib = @import("antfly_resolver");

const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const artifact_ids = @import("artifact_ids.zig");
const db_internal = @import("internal.zig");
const mapper = @import("document_mapper.zig");
const artifact_repair = @import("artifact_repair.zig");
const asset_producer_mod = @import("enrichment/asset_producer.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const appendUniqueOwnedKey = db_internal.appendUniqueOwnedKey;
const collectGraphArtifactsForDocIndex = db_internal.collectGraphArtifactsForDocIndex;
const containsStoreWriteKey = db_internal.containsStoreWriteKey;
const encodeGraphAssetStateKeysAlloc = db_internal.encodeGraphAssetStateKeysAlloc;
const filterChangedGraphMaterializationBatch = db_internal.filterChangedGraphMaterializationBatch;
const freeOwnedConstKeySlice = db_internal.freeOwnedConstKeySlice;
const graphArtifactContentType = db_internal.graphArtifactContentType;
const graphAssetStateKeyAlloc = db_internal.graphAssetStateKeyAlloc;
const loadGraphAssetStateKeysAlloc = db_internal.loadGraphAssetStateKeysAlloc;
const storeDocumentValueForGraphSource = db_internal.storeDocumentValueForGraphSource;

fn containsDeleteKey(list: []const []const u8, key: []const u8) bool {
    return db_internal.containsKey(list, key);
}

pub fn freeOwnedKeySlice(alloc: Allocator, keys: [][]u8) void {
    for (keys) |key| alloc.free(key);
    alloc.free(keys);
}

fn concatKVPairSlices(alloc: Allocator, lhs: []const docstore_mod.KVPair, rhs: []const docstore_mod.KVPair) ![]docstore_mod.KVPair {
    const out = try alloc.alloc(docstore_mod.KVPair, lhs.len + rhs.len);
    @memcpy(out[0..lhs.len], lhs);
    for (rhs, 0..) |write, i| out[lhs.len + i] = write;
    return out;
}

pub const OwnedDenseEmbeddingWrites = struct {
    alloc: Allocator,
    owns_doc_keys: bool = false,
    writes: []mapper.DenseEmbeddingWrite = &.{},
    allocation_len: usize = 0,

    pub fn deinit(self: *@This()) void {
        if (self.owns_doc_keys) {
            for (self.writes) |write| {
                self.alloc.free(@constCast(write.doc_key));
                if (write.parent_doc_key) |parent_doc_key| self.alloc.free(@constCast(parent_doc_key));
            }
        }
        if (self.allocation_len > 0) self.alloc.free(self.writes.ptr[0..self.allocation_len]);
        self.* = undefined;
    }
};

pub const OwnedSparseEmbeddingWrites = struct {
    alloc: Allocator,
    owned_doc_keys: []const []const u8 = &.{},
    writes: []mapper.SparseEmbeddingWrite = &.{},
    allocation_len: usize = 0,

    pub fn deinit(self: *@This()) void {
        for (self.owned_doc_keys) |doc_key| self.alloc.free(@constCast(doc_key));
        if (self.owned_doc_keys.len > 0) self.alloc.free(self.owned_doc_keys);
        if (self.allocation_len > 0) self.alloc.free(self.writes.ptr[0..self.allocation_len]);
        self.* = undefined;
    }
};

pub const OwnedGraphMutations = struct {
    alloc: Allocator,
    writes: []types.GraphEdgeWrite = &.{},
    deletes: []types.GraphEdgeDelete = &.{},

    pub fn deinit(self: *OwnedGraphMutations) void {
        for (self.writes) |write| {
            self.alloc.free(@constCast(write.index_name));
            self.alloc.free(@constCast(write.source));
            self.alloc.free(@constCast(write.target));
            self.alloc.free(@constCast(write.edge_type));
            if (write.metadata_json.len > 0) self.alloc.free(@constCast(write.metadata_json));
        }
        if (self.writes.len > 0) self.alloc.free(self.writes);

        for (self.deletes) |delete| {
            self.alloc.free(@constCast(delete.index_name));
            self.alloc.free(@constCast(delete.source));
            self.alloc.free(@constCast(delete.target));
            self.alloc.free(@constCast(delete.edge_type));
        }
        if (self.deletes.len > 0) self.alloc.free(self.deletes);
        self.* = undefined;
    }
};

pub const OwnedEmbeddingArtifactWriteIdentity = struct {
    doc_key: []u8,
    parent_doc_key: ?[]u8 = null,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.doc_key);
        if (self.parent_doc_key) |parent_doc_key| alloc.free(parent_doc_key);
        self.* = undefined;
    }
};

pub fn decodeEmbeddingArtifactWriteIdentityAlloc(
    alloc: Allocator,
    artifact_key: []const u8,
    expected_embedding_name: []const u8,
) !?OwnedEmbeddingArtifactWriteIdentity {
    if (artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key)) |maybe_identity| {
        var identity = maybe_identity orelse return null;
        defer identity.deinit(alloc);
        if (!std.mem.eql(u8, identity.embedding_name, expected_embedding_name)) return null;

        const doc_key = try alloc.dupe(u8, identity.doc_key);
        errdefer alloc.free(doc_key);
        const parent_doc_key = if (identity.parent_doc_key) |parent_key| try alloc.dupe(u8, parent_key) else null;
        errdefer if (parent_doc_key) |owned_parent| alloc.free(owned_parent);
        return .{
            .doc_key = doc_key,
            .parent_doc_key = parent_doc_key,
        };
    } else |err| switch (err) {
        error.InvalidInternalUserKey => {},
        else => return err,
    }

    if (try internal_keys.parseEmbeddingArtifactKeyView(artifact_key)) |identity| {
        if (!std.mem.eql(u8, identity.artifact_name, expected_embedding_name)) return null;
        return .{
            .doc_key = try alloc.dupe(u8, identity.doc_key),
        };
    }

    return null;
}

test "artifact replay dense embedding write identity accepts legacy embedding artifact keys" {
    const alloc = std.testing.allocator;

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_key);

    var identity = (try decodeEmbeddingArtifactWriteIdentityAlloc(alloc, artifact_key, "dv_v1")).?;
    defer identity.deinit(alloc);
    try std.testing.expectEqualStrings("doc:a", identity.doc_key);
    try std.testing.expect(identity.parent_doc_key == null);

    try std.testing.expectEqual(null, try decodeEmbeddingArtifactWriteIdentityAlloc(alloc, artifact_key, "other_idx"));
}

pub fn collectDenseEmbeddingWritesForArtifacts(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !OwnedDenseEmbeddingWrites {
    var filtered = std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite).empty;
    errdefer {
        for (filtered.items) |write| {
            alloc.free(@constCast(write.doc_key));
            if (write.parent_doc_key) |parent_doc_key| alloc.free(@constCast(parent_doc_key));
        }
        filtered.deinit(alloc);
    }

    try appendDenseEmbeddingWritesForArtifacts(alloc, index_manager, &filtered, artifact_keys, index_name);

    const writes = try filtered.toOwnedSlice(alloc);
    return .{
        .alloc = alloc,
        .owns_doc_keys = true,
        .writes = writes,
        .allocation_len = writes.len,
    };
}

pub fn appendDenseEmbeddingWritesForArtifacts(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    out: *std.ArrayListUnmanaged(mapper.DenseEmbeddingWrite),
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !void {
    const expected_embedding_name = index_manager.denseEmbeddingName(index_name) orelse index_name;
    for (artifact_keys) |artifact_key| {
        var identity = (try decodeEmbeddingArtifactWriteIdentityAlloc(alloc, artifact_key, expected_embedding_name)) orelse continue;
        var identity_transferred = false;
        defer if (!identity_transferred) identity.deinit(alloc);
        try out.append(alloc, .{
            .index_name = @constCast(index_name),
            .doc_key = identity.doc_key,
            .parent_doc_key = identity.parent_doc_key,
            .artifact_key = @constCast(artifact_key),
            .vector = &.{},
        });
        identity_transferred = true;
    }
}

pub fn collectSparseEmbeddingWritesForArtifacts(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !OwnedSparseEmbeddingWrites {
    var filtered = std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite).empty;
    var owned_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (owned_doc_keys.items) |doc_key| alloc.free(@constCast(doc_key));
        owned_doc_keys.deinit(alloc);
        filtered.deinit(alloc);
    }

    try appendSparseEmbeddingWritesForArtifacts(alloc, index_manager, &filtered, &owned_doc_keys, artifact_keys, index_name);

    const writes = try filtered.toOwnedSlice(alloc);
    errdefer if (writes.len > 0) alloc.free(writes);
    const owned_keys = try owned_doc_keys.toOwnedSlice(alloc);
    return .{
        .alloc = alloc,
        .owned_doc_keys = owned_keys,
        .writes = writes,
        .allocation_len = writes.len,
    };
}

pub fn appendSparseEmbeddingWritesForArtifacts(
    alloc: Allocator,
    index_manager: *index_manager_mod.IndexManager,
    out: *std.ArrayListUnmanaged(mapper.SparseEmbeddingWrite),
    owned_doc_keys: *std.ArrayListUnmanaged([]const u8),
    artifact_keys: []const []const u8,
    index_name: []const u8,
) !void {
    const expected_embedding_name = index_manager.sparseEmbeddingName(index_name) orelse index_name;
    for (artifact_keys) |artifact_key| {
        if (try internal_keys.parseEmbeddingArtifactKeyView(artifact_key)) |identity| {
            if (!std.mem.eql(u8, identity.artifact_name, expected_embedding_name)) continue;
            try out.append(alloc, .{
                .index_name = @constCast(index_name),
                .doc_key = @constCast(identity.doc_key),
                .artifact_key = @constCast(artifact_key),
                .indices = &.{},
                .values = &.{},
            });
            continue;
        }

        var identity = artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key) catch |err| switch (err) {
            error.InvalidInternalUserKey => continue,
            else => return err,
        } orelse continue;
        defer identity.deinit(alloc);
        if (!std.mem.eql(u8, identity.embedding_name, expected_embedding_name)) continue;
        var doc_key = try alloc.dupe(u8, identity.doc_key);
        errdefer if (doc_key.len > 0) alloc.free(doc_key);
        try out.append(alloc, .{
            .index_name = @constCast(index_name),
            .doc_key = doc_key,
            .artifact_key = @constCast(artifact_key),
            .indices = &.{},
            .values = &.{},
        });
        try owned_doc_keys.append(alloc, doc_key);
        doc_key = doc_key[0..0];
    }
}

pub const GraphMutationCollectionOptions = struct {
    repair: GraphReplayRepairOptions = .{},
};

pub fn collectGraphMutationsForArtifacts(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    artifact_keys: []const []const u8,
    index_name: []const u8,
    options: GraphMutationCollectionOptions,
) !OwnedGraphMutations {
    var writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
    errdefer {
        for (writes.items) |write| {
            alloc.free(@constCast(write.index_name));
            alloc.free(@constCast(write.source));
            alloc.free(@constCast(write.target));
            alloc.free(@constCast(write.edge_type));
            if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
        }
        writes.deinit(alloc);
    }
    var deletes = std.ArrayListUnmanaged(types.GraphEdgeDelete).empty;
    errdefer {
        for (deletes.items) |delete| {
            alloc.free(@constCast(delete.index_name));
            alloc.free(@constCast(delete.source));
            alloc.free(@constCast(delete.target));
            alloc.free(@constCast(delete.edge_type));
        }
        deletes.deinit(alloc);
    }

    var txn = try store.beginReadTxn();
    defer txn.abort();

    for (artifact_keys) |artifact_key| {
        const parsed = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(alloc, artifact_key)) orelse continue;
        defer {
            alloc.free(parsed.doc_key);
            alloc.free(parsed.index_name);
            alloc.free(parsed.edge_type);
            alloc.free(parsed.target_doc_key);
        }
        if (!std.mem.eql(u8, parsed.index_name, index_name)) continue;

        const raw = txn.get(artifact_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (raw) |value| {
            var decoded = enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, value) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => {
                    if (options.repair.enabled) {
                        const artifact_name = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ parsed.edge_type, parsed.target_doc_key });
                        defer alloc.free(artifact_name);
                        try artifact_repair.recordArtifactRepairIssueForReplay(
                            alloc,
                            store,
                            .graph,
                            parsed.index_name,
                            parsed.doc_key,
                            "",
                            "",
                            "",
                            artifact_name,
                            artifact_key,
                            null,
                            options.repair.sequence,
                            .corrupt_artifact,
                            options.repair.issue_mutex,
                        );
                        return error.ArtifactRepairRequired;
                    }
                    return err;
                },
            };
            errdefer decoded.deinit(alloc);
            try writes.append(alloc, .{
                .index_name = try alloc.dupe(u8, parsed.index_name),
                .source = try alloc.dupe(u8, parsed.doc_key),
                .target = try alloc.dupe(u8, parsed.target_doc_key),
                .edge_type = try alloc.dupe(u8, parsed.edge_type),
                .weight = decoded.weight,
                .created_at = decoded.created_at,
                .updated_at = decoded.updated_at,
                .metadata_json = decoded.metadata_json,
            });
            decoded.metadata_json = &.{};
            decoded.deinit(alloc);
        } else {
            try deletes.append(alloc, .{
                .index_name = try alloc.dupe(u8, parsed.index_name),
                .source = try alloc.dupe(u8, parsed.doc_key),
                .target = try alloc.dupe(u8, parsed.target_doc_key),
                .edge_type = try alloc.dupe(u8, parsed.edge_type),
            });
        }
    }

    return .{
        .alloc = alloc,
        .writes = try writes.toOwnedSlice(alloc),
        .deletes = try deletes.toOwnedSlice(alloc),
    };
}

pub const GraphReplayRepairOptions = struct {
    enabled: bool = false,
    sequence: u64 = 0,
    issue_mutex: ?*std.atomic.Mutex = null,
};

pub fn graphArtifactSourceConsumesRef(
    index_manager: *index_manager_mod.IndexManager,
    source: index_manager_mod.GraphArtifactSource,
    artifact_ref: types.ArtifactRef,
) bool {
    if (!std.mem.eql(u8, source.artifact_name, artifact_ref.name)) return false;
    return switch (artifact_ref.kind) {
        .asset => graphAssetSourceConsumesAssetRef(index_manager, artifact_ref),
        .chunk => index_manager.getEnrichment(.chunk, artifact_ref.name) != null,
        .embedding => false,
    };
}

const GraphArtifactRefView = struct {
    name: []const u8,
    kind: types.ArtifactKind,
    unit_id_present: bool = false,
};

pub fn graphArtifactSourceConsumesArtifactKey(
    index_manager: *index_manager_mod.IndexManager,
    source: index_manager_mod.GraphArtifactSource,
    artifact_key: []const u8,
) bool {
    if (decodeArtifactRefViewForGraphApplicability(artifact_key) catch null) |artifact_ref| {
        return graphArtifactSourceConsumesRefView(index_manager, source, artifact_ref);
    }

    var artifact_ref = (artifact_ids.decodeArtifactRefAlloc(index_manager.alloc, artifact_key) catch return false) orelse return false;
    defer artifact_ref.deinit(index_manager.alloc);
    return graphArtifactSourceConsumesRef(index_manager, source, artifact_ref);
}

fn graphArtifactSourceConsumesRefView(
    index_manager: *index_manager_mod.IndexManager,
    source: index_manager_mod.GraphArtifactSource,
    artifact_ref: GraphArtifactRefView,
) bool {
    if (!std.mem.eql(u8, source.artifact_name, artifact_ref.name)) return false;
    return switch (artifact_ref.kind) {
        .asset => graphAssetSourceConsumesAssetRefView(index_manager, artifact_ref),
        .chunk => index_manager.getEnrichment(.chunk, artifact_ref.name) != null,
        .embedding => false,
    };
}

fn graphAssetSourceConsumesAssetRefView(index_manager: *index_manager_mod.IndexManager, artifact_ref: GraphArtifactRefView) bool {
    if (index_manager.getEnrichment(.asset, artifact_ref.name) == null) return false;
    if (artifact_ref.unit_id_present) return true;
    const cfg = index_manager.getEnrichment(.asset, artifact_ref.name) orelse return false;
    var producer_cfg = asset_producer_mod.parseProducerConfig(index_manager.alloc, cfg.producer_json) catch return true;
    defer producer_cfg.deinit(index_manager.alloc);
    return producer_cfg.type != .document_extraction;
}

fn decodeArtifactRefViewForGraphApplicability(key: []const u8) !?GraphArtifactRefView {
    if (!internal_keys.isInternalUserKey(key)) return null;

    const doc_term = internal_keys.findComponentTerminator(key, 1) orelse return null;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != internal_keys.artifact_kind) return null;
    pos += 1;

    const type_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
    const raw_kind = (try internal_keys.decodeBodyView(key[pos..type_term])) orelse return null;
    const kind = try artifactKindFromInternalLabel(raw_kind);
    pos = type_term + 2;

    const name_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
    const name = (try internal_keys.decodeBodyView(key[pos..name_term])) orelse return null;
    pos = name_term + 2;

    var unit_id_present = false;
    if (kind == .asset and pos < key.len and key[pos] == internal_keys.document_unit_record_kind) {
        pos += 1;
        const unit_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
        if ((try internal_keys.decodeBodyView(key[pos..unit_term])) == null) return null;
        pos = unit_term + 2;
        if (pos == key.len) return .{ .name = name, .kind = .asset, .unit_id_present = true };
    } else if (kind == .chunk) {
        if (pos < key.len and key[pos] == internal_keys.document_unit_record_kind) {
            pos += 1;
            const unit_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
            if ((try internal_keys.decodeBodyView(key[pos..unit_term])) == null) return null;
            pos = unit_term + 2;
            unit_id_present = true;
        }
        if (pos + 1 + @sizeOf(u32) > key.len or key[pos] != internal_keys.chunk_record_kind) return error.InvalidInternalUserKey;
        pos += 1 + @sizeOf(u32);
        if (pos == key.len) return .{ .name = name, .kind = .chunk, .unit_id_present = unit_id_present };
    }

    if (pos == key.len) return .{ .name = name, .kind = kind, .unit_id_present = unit_id_present };
    if (key[pos] != internal_keys.derived_embedding_kind) return error.InvalidInternalUserKey;
    pos += 1;
    const derived_name_term = internal_keys.findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
    const derived_name = (try internal_keys.decodeBodyView(key[pos..derived_name_term])) orelse return null;
    if (derived_name_term + 2 != key.len) return error.InvalidInternalUserKey;
    return .{ .name = derived_name, .kind = .embedding };
}

fn artifactKindFromInternalLabel(raw_kind: []const u8) !types.ArtifactKind {
    if (std.mem.eql(u8, raw_kind, "chunk")) return .chunk;
    if (std.mem.eql(u8, raw_kind, "asset")) return .asset;
    if (std.mem.eql(u8, raw_kind, "embedding")) return .embedding;
    return error.InvalidInternalUserKey;
}

fn graphAssetSourceConsumesAssetRef(index_manager: *index_manager_mod.IndexManager, artifact_ref: types.ArtifactRef) bool {
    if (index_manager.getEnrichment(.asset, artifact_ref.name) == null) return false;
    if (artifact_ref.unit_id != null) return true;
    const cfg = index_manager.getEnrichment(.asset, artifact_ref.name) orelse return false;
    var producer_cfg = asset_producer_mod.parseProducerConfig(index_manager.alloc, cfg.producer_json) catch return true;
    defer producer_cfg.deinit(index_manager.alloc);
    return producer_cfg.type != .document_extraction;
}

pub fn graphArtifactRefUsesDocumentWideFallback(artifact_ref: types.ArtifactRef) bool {
    return artifact_ref.kind == .asset and artifact_ref.unit_id == null and artifact_ref.chunk_id == null and artifact_ref.source == null;
}

pub fn graphArtifactStateNameAlloc(alloc: Allocator, artifact_ref: types.ArtifactRef) ![]u8 {
    if (graphArtifactRefUsesDocumentWideFallback(artifact_ref)) return try alloc.dupe(u8, artifact_ref.name);

    var state_name = std.ArrayListUnmanaged(u8).empty;
    errdefer state_name.deinit(alloc);
    try state_name.appendSlice(alloc, artifact_ref.name);
    try state_name.append(alloc, '\x1f');
    try state_name.appendSlice(alloc, @tagName(artifact_ref.kind));
    if (artifact_ref.unit_id) |unit_id| {
        try state_name.append(alloc, '\x1f');
        try state_name.appendSlice(alloc, "unit:");
        try state_name.appendSlice(alloc, unit_id);
    }
    if (artifact_ref.chunk_id) |chunk_id| {
        const chunk_part = try std.fmt.allocPrint(alloc, "chunk:{d}", .{chunk_id});
        defer alloc.free(chunk_part);
        try state_name.append(alloc, '\x1f');
        try state_name.appendSlice(alloc, chunk_part);
    }
    return try state_name.toOwnedSlice(alloc);
}

test "graph artifact state identity separates document unit and chunk provenance" {
    const alloc = std.testing.allocator;

    const document_wide: types.ArtifactRef = .{
        .document_id = @constCast("doc:a"),
        .name = @constCast("artifact"),
        .kind = .asset,
    };
    const unit: types.ArtifactRef = .{
        .document_id = @constCast("doc:a"),
        .name = @constCast("artifact"),
        .kind = .asset,
        .unit_id = @constCast("1"),
    };
    const chunk: types.ArtifactRef = .{
        .document_id = @constCast("doc:a"),
        .name = @constCast("artifact"),
        .kind = .chunk,
        .chunk_id = 7,
        .unit_id = @constCast("1"),
    };

    const document_wide_name = try graphArtifactStateNameAlloc(alloc, document_wide);
    defer alloc.free(document_wide_name);
    const unit_name = try graphArtifactStateNameAlloc(alloc, unit);
    defer alloc.free(unit_name);
    const chunk_name = try graphArtifactStateNameAlloc(alloc, chunk);
    defer alloc.free(chunk_name);

    try std.testing.expect(graphArtifactRefUsesDocumentWideFallback(document_wide));
    try std.testing.expect(!graphArtifactRefUsesDocumentWideFallback(unit));
    try std.testing.expect(!graphArtifactRefUsesDocumentWideFallback(chunk));
    try std.testing.expectEqualStrings("artifact", document_wide_name);
    try std.testing.expect(!std.mem.eql(u8, document_wide_name, unit_name));
    try std.testing.expect(!std.mem.eql(u8, unit_name, chunk_name));
    try std.testing.expect(std.mem.indexOf(u8, unit_name, "unit:1") != null);
    try std.testing.expect(std.mem.indexOf(u8, chunk_name, "chunk:7") != null);
}

pub const GraphMaterializationOptions = struct {
    relational_base_rows: bool = false,
    require_resolution_contract: bool = false,
    repair: GraphReplayRepairOptions = .{},
};

pub fn materializeGraphSourceArtifactsForIndex(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_manager: *index_manager_mod.IndexManager,
    changed_artifact_keys: []const []const u8,
    index_name: []const u8,
    options: GraphMaterializationOptions,
) ![][]u8 {
    const source = index_manager.graphArtifactSource(index_name) orelse return try alloc.alloc([]u8, 0);

    var changed = std.ArrayListUnmanaged([]u8).empty;
    errdefer freeOwnedKeySlice(alloc, changed.items);

    for (changed_artifact_keys) |artifact_key| {
        if (internal_keys.isResolutionArtifactKey(artifact_key)) {
            try materializeMentionEdgesForResolutionKey(alloc, store, index_manager, &changed, index_name, source, artifact_key, options);
            continue;
        }
        var artifact_ref = (try db_internal.decodeArtifactRefIfKnownAlloc(alloc, artifact_key)) orelse continue;
        defer artifact_ref.deinit(alloc);
        if (!graphArtifactSourceConsumesRef(index_manager, source, artifact_ref)) continue;

        var deletes = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (deletes.items) |key| alloc.free(@constCast(key));
            deletes.deinit(alloc);
        }

        const raw = store.get(alloc, artifact_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        defer if (raw) |value| alloc.free(value);

        var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
        defer {
            for (writes.items) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            writes.deinit(alloc);
        }

        if (raw) |value| {
            const raw_doc = try storeDocumentValueForGraphSource(alloc, store, artifact_ref.document_id, options.relational_base_rows);
            defer if (raw_doc) |doc_value| alloc.free(doc_value);
            const graph_writes = graphWritesFromArtifactValueAlloc(alloc, index_name, artifact_ref.document_id, value, source, graphArtifactContentType(index_manager, source.artifact_name), raw_doc) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => {
                    if (options.repair.enabled) {
                        try artifact_repair.recordArtifactRepairIssueForRefReplay(
                            alloc,
                            store,
                            index_name,
                            artifact_ref,
                            artifact_key,
                            options.repair.sequence,
                            .corrupt_artifact,
                            options.repair.issue_mutex,
                        );
                        return error.ArtifactRepairRequired;
                    }
                    return err;
                },
            };
            defer freeGraphWrites(alloc, graph_writes);
            for (graph_writes) |write| {
                const key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, write.source, write.index_name, write.edge_type, write.target);
                var key_owned = true;
                errdefer if (key_owned) alloc.free(key);
                const payload = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, write.weight, write.created_at, write.updated_at, write.metadata_json);
                var payload_owned = true;
                errdefer if (payload_owned) alloc.free(payload);
                try writes.append(alloc, .{ .key = key, .value = payload });
                key_owned = false;
                payload_owned = false;
                try appendUniqueOwnedKey(alloc, &changed, key);
            }
        }

        const state_name = try graphArtifactStateNameAlloc(alloc, artifact_ref);
        defer alloc.free(state_name);
        const state_key = try graphAssetStateKeyAlloc(alloc, artifact_ref.document_id, index_name, state_name);
        defer alloc.free(state_key);
        if (try loadGraphAssetStateKeysAlloc(alloc, store, state_key)) |previous_keys| {
            defer freeOwnedConstKeySlice(alloc, previous_keys);
            for (previous_keys) |previous_key| {
                if (containsStoreWriteKey(writes.items, previous_key)) continue;
                try deletes.append(alloc, try alloc.dupe(u8, previous_key));
                try appendUniqueOwnedKey(alloc, &changed, previous_key);
            }
        } else if (graphArtifactRefUsesDocumentWideFallback(artifact_ref)) {
            const protected_keys = try resolutionMentionStateKeysForGraphSourceAlloc(alloc, store, index_manager, artifact_ref.document_id, index_name, source);
            defer freeOwnedConstKeySlice(alloc, protected_keys);
            const existing = try collectGraphArtifactsForDocIndex(alloc, store, artifact_ref.document_id, index_name);
            defer docstore_mod.DocStore.freeResults(alloc, existing);
            for (existing) |entry| {
                if (containsStoreWriteKey(writes.items, entry.key)) continue;
                if (containsDeleteKey(protected_keys, entry.key)) continue;
                try deletes.append(alloc, try alloc.dupe(u8, entry.key));
                try appendUniqueOwnedKey(alloc, &changed, entry.key);
            }
        }

        const state_value = try encodeGraphAssetStateKeysAlloc(alloc, writes.items);
        var state_value_owned = true;
        defer if (state_value_owned) alloc.free(state_value);
        try writes.append(alloc, .{
            .key = try alloc.dupe(u8, state_key),
            .value = state_value,
        });
        state_value_owned = false;

        if (writes.items.len > 0 or deletes.items.len > 0) {
            var changed_batch = try filterChangedGraphMaterializationBatch(alloc, store, writes.items, deletes.items);
            defer changed_batch.deinit(alloc);
            if (changed_batch.writes.len > 0 or changed_batch.deletes.len > 0) {
                try store.putBatch(changed_batch.writes, changed_batch.deletes);
            }
        }
    }

    return try changed.toOwnedSlice(alloc);
}

fn materializeMentionEdgesForResolutionKey(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_manager: *index_manager_mod.IndexManager,
    changed: *std.ArrayListUnmanaged([]u8),
    index_name: []const u8,
    source: index_manager_mod.GraphArtifactSource,
    resolution_key: []const u8,
    options: GraphMaterializationOptions,
) !void {
    if (source.mention_edge_type.len == 0) return;
    const parsed_key = (try internal_keys.parseResolutionArtifactKeyAlloc(alloc, resolution_key)) orelse return;
    defer alloc.free(parsed_key.doc_key);
    defer alloc.free(parsed_key.artifact_name);

    const raw_resolution = store.get(alloc, resolution_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    defer if (raw_resolution) |raw| alloc.free(raw);

    const cfg = graphResolverConfigForResolution(index_manager, source.artifact_name, parsed_key.artifact_name) orelse {
        if (options.require_resolution_contract) return error.MissingResolverArtifactContract;
        return;
    };
    const state_name = try mentionGraphStateNameAlloc(alloc, source.artifact_name, cfg.resolution_artifact);
    defer alloc.free(state_name);
    const state_key = try graphAssetStateKeyAlloc(alloc, parsed_key.doc_key, index_name, state_name);
    defer alloc.free(state_key);

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        writes.deinit(alloc);
    }
    var mention_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (mention_writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        mention_writes.deinit(alloc);
    }
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (deletes.items) |key| alloc.free(@constCast(key));
        deletes.deinit(alloc);
    }

    if (raw_resolution) |raw| {
        const raw_extraction = loadSourceExtractionForResolution(alloc, store, parsed_key.doc_key, cfg.source_artifact) catch null;
        defer if (raw_extraction) |raw_src| alloc.free(raw_src);
        const mention_edge_writes = try mentionEdgeWritesFromResolutionAlloc(
            alloc,
            index_name,
            parsed_key.doc_key,
            raw,
            raw_extraction,
            source.mention_edge_type,
            cfg,
        );
        defer freeGraphWrites(alloc, mention_edge_writes);
        for (mention_edge_writes) |write| {
            const key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, write.source, write.index_name, write.edge_type, write.target);
            var key_owned = true;
            errdefer if (key_owned) alloc.free(key);
            const payload = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, write.weight, write.created_at, write.updated_at, write.metadata_json);
            var payload_owned = true;
            errdefer if (payload_owned) alloc.free(payload);
            try writes.append(alloc, .{ .key = key, .value = payload });
            key_owned = false;
            payload_owned = false;
            try appendUniqueOwnedKey(alloc, changed, key);
        }
        try appendMentionEvidenceArtifactsFromResolution(
            alloc,
            &mention_writes,
            changed,
            parsed_key.doc_key,
            resolution_key,
            raw,
            raw_extraction,
            cfg,
        );
    }

    if (try loadGraphAssetStateKeysAlloc(alloc, store, state_key)) |previous_keys| {
        defer freeOwnedConstKeySlice(alloc, previous_keys);
        for (previous_keys) |previous_key| {
            if (containsStoreWriteKey(writes.items, previous_key)) continue;
            try deletes.append(alloc, try alloc.dupe(u8, previous_key));
            try appendUniqueOwnedKey(alloc, changed, previous_key);
        }
    }

    const state_value = try encodeGraphAssetStateKeysAlloc(alloc, writes.items);
    var state_value_owned = true;
    defer if (state_value_owned) alloc.free(state_value);
    try writes.append(alloc, .{
        .key = try alloc.dupe(u8, state_key),
        .value = state_value,
    });
    state_value_owned = false;

    const mention_state_name = try mentionArtifactStateNameAlloc(alloc, source.artifact_name, cfg.resolution_artifact);
    defer alloc.free(mention_state_name);
    const mention_state_key = try graphAssetStateKeyAlloc(alloc, parsed_key.doc_key, index_name, mention_state_name);
    defer alloc.free(mention_state_key);
    if (try loadGraphAssetStateKeysAlloc(alloc, store, mention_state_key)) |previous_keys| {
        defer freeOwnedConstKeySlice(alloc, previous_keys);
        for (previous_keys) |previous_key| {
            if (containsStoreWriteKey(mention_writes.items, previous_key)) continue;
            try deletes.append(alloc, try alloc.dupe(u8, previous_key));
            try appendUniqueOwnedKey(alloc, changed, previous_key);
        }
    }

    const mention_state_value = try encodeGraphAssetStateKeysAlloc(alloc, mention_writes.items);
    var mention_state_value_owned = true;
    defer if (mention_state_value_owned) alloc.free(mention_state_value);
    try mention_writes.append(alloc, .{
        .key = try alloc.dupe(u8, mention_state_key),
        .value = mention_state_value,
    });
    mention_state_value_owned = false;

    if (writes.items.len > 0 or mention_writes.items.len > 0 or deletes.items.len > 0) {
        const combined = try concatKVPairSlices(alloc, writes.items, mention_writes.items);
        defer alloc.free(combined);
        var changed_batch = try filterChangedGraphMaterializationBatch(alloc, store, combined, deletes.items);
        defer changed_batch.deinit(alloc);
        if (changed_batch.writes.len > 0 or changed_batch.deletes.len > 0) {
            try store.putBatch(changed_batch.writes, changed_batch.deletes);
        }
    }
}

pub fn freeGraphWrites(alloc: Allocator, writes: []types.GraphEdgeWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.index_name));
        alloc.free(@constCast(write.source));
        alloc.free(@constCast(write.target));
        alloc.free(@constCast(write.edge_type));
        if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
    }
    if (writes.len > 0) alloc.free(writes);
}

pub fn graphResolverConfigForResolution(
    index_manager: *index_manager_mod.IndexManager,
    source_artifact: []const u8,
    resolution_artifact: []const u8,
) ?*const index_manager_mod.ResolverConfig {
    for (index_manager.resolvers.items) |*cfg| {
        if (std.mem.eql(u8, cfg.source_artifact, source_artifact) and
            std.mem.eql(u8, cfg.resolution_artifact, resolution_artifact))
        {
            return cfg;
        }
    }
    return null;
}

pub fn graphResolverConfigForResolutionArtifact(
    index_manager: *index_manager_mod.IndexManager,
    resolution_artifact: []const u8,
) ?*const index_manager_mod.ResolverConfig {
    for (index_manager.resolvers.items) |*cfg| {
        if (std.mem.eql(u8, cfg.resolution_artifact, resolution_artifact)) return cfg;
    }
    return null;
}

pub fn mentionGraphStateNameAlloc(alloc: Allocator, source_artifact: []const u8, resolution_artifact: []const u8) ![]u8 {
    return try db_internal.mentionGraphStateNameAlloc(alloc, source_artifact, resolution_artifact);
}

pub fn mentionArtifactStateNameAlloc(alloc: Allocator, source_artifact: []const u8, resolution_artifact: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}\x1fresolution_mention_artifacts\x1f{s}", .{ source_artifact, resolution_artifact });
}

fn resolutionMentionArtifactNameAlloc(
    alloc: Allocator,
    source_artifact: []const u8,
    resolution_artifact: []const u8,
    local_id: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(alloc, "_resolution_mention\x1f{s}\x1f{s}\x1f{s}", .{ source_artifact, resolution_artifact, local_id });
}

fn resolutionMentionArtifactKeyAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    source_artifact: []const u8,
    resolution_artifact: []const u8,
    local_id: []const u8,
) ![]u8 {
    const name = try resolutionMentionArtifactNameAlloc(alloc, source_artifact, resolution_artifact, local_id);
    defer alloc.free(name);
    return try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", name);
}

pub fn resolutionMentionStateKeysForGraphSourceAlloc(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_manager: *index_manager_mod.IndexManager,
    doc_key: []const u8,
    index_name: []const u8,
    source: index_manager_mod.GraphArtifactSource,
) ![][]const u8 {
    if (source.mention_edge_type.len == 0) return try alloc.alloc([]const u8, 0);

    var protected = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (protected.items) |key| alloc.free(@constCast(key));
        protected.deinit(alloc);
    }

    for (index_manager.resolvers.items) |cfg| {
        if (!std.mem.eql(u8, cfg.source_artifact, source.artifact_name)) continue;

        const state_name = try mentionGraphStateNameAlloc(alloc, source.artifact_name, cfg.resolution_artifact);
        defer alloc.free(state_name);
        const state_key = try graphAssetStateKeyAlloc(alloc, doc_key, index_name, state_name);
        defer alloc.free(state_key);

        const state_keys = try loadGraphAssetStateKeysAlloc(alloc, store, state_key) orelse continue;
        defer freeOwnedConstKeySlice(alloc, state_keys);
        for (state_keys) |key| {
            try protected.append(alloc, try alloc.dupe(u8, key));
        }
    }

    return try protected.toOwnedSlice(alloc);
}

pub fn loadSourceExtractionForResolution(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    doc_key: []const u8,
    source_artifact: []const u8,
) !?[]u8 {
    if (try sourceArtifactKeyFromResolutionScopeAlloc(alloc, doc_key, source_artifact)) |source_key| {
        defer alloc.free(source_key);
        return store.get(alloc, source_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
    }
    const extraction_key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", source_artifact);
    defer alloc.free(extraction_key);
    return store.get(alloc, extraction_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

fn sourceArtifactKeyFromResolutionScopeAlloc(alloc: Allocator, doc_key: []const u8, source_artifact: []const u8) !?[]u8 {
    var artifact_ref = (try db_internal.decodeArtifactRefIfKnownAlloc(alloc, doc_key)) orelse return null;
    defer artifact_ref.deinit(alloc);
    if (artifact_ref.kind != .asset and artifact_ref.kind != .chunk) return null;
    if (!std.mem.eql(u8, artifact_ref.name, source_artifact)) return null;
    return try alloc.dupe(u8, doc_key);
}

fn sourceArtifactKeyForResolutionAlloc(alloc: Allocator, doc_key: []const u8, source_artifact: []const u8) ![]u8 {
    if (try sourceArtifactKeyFromResolutionScopeAlloc(alloc, doc_key, source_artifact)) |source_key| return source_key;
    return try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", source_artifact);
}

fn extractionConfidenceForLocalId(entities: []const resolver_lib.ExtractedEntity, local_id: []const u8) ?f64 {
    for (entities) |entity| {
        if (std.mem.eql(u8, entity.local_id, local_id)) return entity.confidence;
    }
    return null;
}

fn extractionEntityForLocalId(entities: []const resolver_lib.ExtractedEntity, local_id: []const u8) ?resolver_lib.ExtractedEntity {
    for (entities) |entity| {
        if (std.mem.eql(u8, entity.local_id, local_id)) return entity;
    }
    return null;
}

fn resolutionDecisionCreatesCanonicalEdge(decision: resolver_lib.Decision) bool {
    return switch (decision) {
        .new, .match => true,
        .review => false,
    };
}

fn decisionName(decision: resolver_lib.Decision) []const u8 {
    return switch (decision) {
        .new => "new",
        .match => "match",
        .review => "review",
    };
}

/// Build `doc -> entity` mention edges (provenance) from the durable resolution
/// artifact for canonical decisions. The target is the resolved DocRef, so
/// prefix/ANN matches, merge redirects, and curator overrides are reflected in
/// graph state. Review-band decisions remain visible through the review queue,
/// not as ordinary resolved provenance. The optional extraction artifact is
/// consulted only for the original mention confidence used by provenance weight
/// calibration.
pub fn mentionEdgeWritesFromResolutionAlloc(
    alloc: Allocator,
    index_name: []const u8,
    doc_key: []const u8,
    resolution_raw: []const u8,
    extraction_raw: ?[]const u8,
    mention_edge_type: []const u8,
    cfg: *const index_manager_mod.ResolverConfig,
) ![]types.GraphEdgeWrite {
    var parsed_resolution = resolver_lib.parseResolution(alloc, resolution_raw) catch return try alloc.alloc(types.GraphEdgeWrite, 0);
    defer parsed_resolution.deinit();
    var parsed_extraction: ?resolver_lib.ParsedEntities = if (extraction_raw) |raw|
        resolver_lib.parseExtractionEntities(alloc, raw) catch null
    else
        null;
    defer if (parsed_extraction) |*parsed| parsed.deinit();

    var aggregates = std.ArrayListUnmanaged(MentionEdgeAggregate).empty;
    defer {
        for (aggregates.items) |*aggregate| aggregate.deinit(alloc);
        aggregates.deinit(alloc);
    }
    for (parsed_resolution.entities) |entity| {
        if (!resolutionDecisionCreatesCanonicalEdge(entity.decision)) continue;
        if (entity.doc_ref.key.len == 0) continue;
        const mention_confidence = if (parsed_extraction) |parsed|
            extractionConfidenceForLocalId(parsed.entities, entity.local_id) orelse entity.confidence
        else
            entity.confidence;
        const mention_artifact_key = try resolutionMentionArtifactKeyAlloc(alloc, doc_key, cfg.source_artifact, cfg.resolution_artifact, entity.local_id);
        var mention_key_owned = true;
        errdefer if (mention_key_owned) alloc.free(mention_artifact_key);
        const aggregate = try findOrAppendMentionEdgeAggregate(alloc, &aggregates, entity.doc_ref.key, entity.doc_ref.table);
        try aggregate.appendMentionArtifactKey(alloc, mention_artifact_key);
        mention_key_owned = false;
        aggregate.mention_confidence = @max(aggregate.mention_confidence, mention_confidence);
    }

    var writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
    errdefer freeGraphWrites(alloc, writes.items);
    for (aggregates.items) |aggregate| {
        const metadata = try mentionEdgeMetadataJsonAlloc(alloc, aggregate);
        errdefer alloc.free(metadata);
        try writes.append(alloc, .{
            .index_name = try alloc.dupe(u8, index_name),
            .source = try alloc.dupe(u8, doc_key),
            .target = try alloc.dupe(u8, aggregate.target),
            .edge_type = try alloc.dupe(u8, mention_edge_type),
            .weight = cfg.fusedMentionWeight(aggregate.mention_confidence),
            .metadata_json = metadata,
        });
    }
    return try writes.toOwnedSlice(alloc);
}

const MentionEdgeAggregate = struct {
    target: []u8,
    target_table: []u8,
    mention_confidence: f64,
    mention_artifact_keys: std.ArrayListUnmanaged([]u8) = .empty,

    fn deinit(self: *MentionEdgeAggregate, alloc: Allocator) void {
        alloc.free(self.target);
        alloc.free(self.target_table);
        for (self.mention_artifact_keys.items) |key| alloc.free(key);
        self.mention_artifact_keys.deinit(alloc);
        self.* = undefined;
    }

    fn appendMentionArtifactKey(self: *MentionEdgeAggregate, alloc: Allocator, key: []u8) !void {
        try self.mention_artifact_keys.append(alloc, key);
    }
};

fn findOrAppendMentionEdgeAggregate(
    alloc: Allocator,
    aggregates: *std.ArrayListUnmanaged(MentionEdgeAggregate),
    target: []const u8,
    target_table: []const u8,
) !*MentionEdgeAggregate {
    for (aggregates.items) |*existing| {
        if (std.mem.eql(u8, existing.target, target)) return existing;
    }
    const target_owned = try alloc.dupe(u8, target);
    var target_owned_live = true;
    errdefer if (target_owned_live) alloc.free(target_owned);
    const table_owned = try alloc.dupe(u8, target_table);
    var table_owned_live = true;
    errdefer if (table_owned_live) alloc.free(table_owned);
    try aggregates.append(alloc, .{
        .target = target_owned,
        .target_table = table_owned,
        .mention_confidence = 0,
    });
    target_owned_live = false;
    table_owned_live = false;
    return &aggregates.items[aggregates.items.len - 1];
}

fn mentionEdgeMetadataJsonAlloc(alloc: Allocator, aggregate: MentionEdgeAggregate) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .target_table = aggregate.target_table,
        .mention_count = aggregate.mention_artifact_keys.items.len,
        .mention_artifact_keys = aggregate.mention_artifact_keys.items,
    }, .{});
}

pub fn appendMentionEvidenceArtifactsFromResolution(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    changed: *std.ArrayListUnmanaged([]u8),
    doc_key: []const u8,
    resolution_key: []const u8,
    resolution_raw: []const u8,
    extraction_raw: ?[]const u8,
    cfg: *const index_manager_mod.ResolverConfig,
) !void {
    var parsed_resolution = resolver_lib.parseResolution(alloc, resolution_raw) catch return;
    defer parsed_resolution.deinit();
    var parsed_extraction: ?resolver_lib.ParsedEntities = if (extraction_raw) |raw|
        resolver_lib.parseExtractionEntities(alloc, raw) catch null
    else
        null;
    defer if (parsed_extraction) |*parsed| parsed.deinit();
    const source_artifact_key = try sourceArtifactKeyForResolutionAlloc(alloc, doc_key, cfg.source_artifact);
    defer alloc.free(source_artifact_key);

    for (parsed_resolution.entities) |entity| {
        if (!resolutionDecisionCreatesCanonicalEdge(entity.decision)) continue;
        if (entity.doc_ref.key.len == 0) continue;
        const extraction_entity = if (parsed_extraction) |parsed|
            extractionEntityForLocalId(parsed.entities, entity.local_id)
        else
            null;
        const key = try resolutionMentionArtifactKeyAlloc(alloc, doc_key, cfg.source_artifact, cfg.resolution_artifact, entity.local_id);
        var key_owned = true;
        errdefer if (key_owned) alloc.free(key);
        const payload = try mentionEvidencePayloadAlloc(alloc, doc_key, key, source_artifact_key, resolution_key, cfg, entity, extraction_entity);
        var payload_owned = true;
        errdefer if (payload_owned) alloc.free(payload);
        try writes.append(alloc, .{ .key = key, .value = payload });
        key_owned = false;
        payload_owned = false;
        try appendUniqueOwnedKey(alloc, changed, key);
    }
}

fn mentionEvidencePayloadAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    mention_artifact_key: []const u8,
    source_artifact_key: []const u8,
    resolution_key: []const u8,
    cfg: *const index_manager_mod.ResolverConfig,
    entity: resolver_lib.ResolvedEntity,
    extraction_entity: ?resolver_lib.ExtractedEntity,
) ![]u8 {
    const mention_label = if (extraction_entity) |source| source.label else entity.label;
    const mention_text = if (extraction_entity) |source| source.text else entity.surface_form;
    const mention_confidence = if (extraction_entity) |source| source.confidence else entity.confidence;
    return try std.json.Stringify.valueAlloc(alloc, .{
        ._schema = "antfly.resolution_mention.v1",
        ._parent_doc_key = doc_key,
        ._artifact_kind = "resolution_mention",
        ._artifact_key = mention_artifact_key,
        .source_artifact = cfg.source_artifact,
        .source_artifact_key = source_artifact_key,
        .resolution_artifact = cfg.resolution_artifact,
        .resolution_artifact_key = resolution_key,
        .resolver = cfg.name,
        .resolver_table = cfg.table,
        .config_generation = cfg.config_generation,
        .local_id = entity.local_id,
        .decision = decisionName(entity.decision),
        .confidence = entity.confidence,
        .canonical = .{
            .table = entity.doc_ref.table,
            .key = entity.doc_ref.key,
            .name = entity.canonical_name,
            .label = entity.label,
        },
        .mention = .{
            .text = mention_text,
            .label = mention_label,
            .confidence = mention_confidence,
        },
    }, .{});
}

pub fn graphWritesFromArtifactValueAlloc(
    alloc: Allocator,
    index_name: []const u8,
    doc_key: []const u8,
    raw: []const u8,
    source: index_manager_mod.GraphArtifactSource,
    artifact_content_type: []const u8,
    raw_doc: ?[]const u8,
) ![]types.GraphEdgeWrite {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    var parsed_doc = if (raw_doc) |doc| try std.json.parseFromSlice(std.json.Value, alloc, doc, .{}) else null;
    defer if (parsed_doc) |*doc| doc.deinit();
    const doc_value: ?std.json.Value = if (parsed_doc) |doc| doc.value else null;

    var writes = std.ArrayListUnmanaged(types.GraphEdgeWrite).empty;
    errdefer freeGraphWrites(alloc, writes.items);

    switch (source.format) {
        .extraction_relation => try appendRelationItemsFromPath(alloc, &writes, index_name, doc_key, doc_value, parsed.value, source.path, source.mapping, source.artifact_name, artifact_content_type, parsed.value),
        .extraction_graph => {
            if (source.path.len > 0) {
                try appendRelationItemsFromPath(alloc, &writes, index_name, doc_key, doc_value, parsed.value, source.path, source.mapping, source.artifact_name, artifact_content_type, parsed.value);
            } else if (parsed.value == .object) {
                if (parsed.value.object.get("relations")) |relations| try appendRelationValueItems(alloc, &writes, index_name, doc_key, doc_value, relations, source.mapping, source.artifact_name, artifact_content_type, parsed.value);
                if (parsed.value.object.get("edges")) |edges| try appendRelationValueItems(alloc, &writes, index_name, doc_key, doc_value, edges, source.mapping, source.artifact_name, artifact_content_type, parsed.value);
            }
        },
    }

    return try writes.toOwnedSlice(alloc);
}

fn appendRelationItemsFromPath(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
    index_name: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    root: std.json.Value,
    path: []const u8,
    mapping: index_manager_mod.GraphArtifactMapping,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) !void {
    if (path.len == 0 or std.mem.eql(u8, path, "$")) return appendRelationValueItems(alloc, writes, index_name, doc_key, doc_value, root, mapping, artifact_name, artifact_content_type, artifact_value);
    const selected = selectGraphArtifactPath(root, path) orelse return;
    try appendRelationValueItems(alloc, writes, index_name, doc_key, doc_value, selected, mapping, artifact_name, artifact_content_type, artifact_value);
}

fn selectGraphArtifactPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var trimmed = path;
    if (std.mem.startsWith(u8, trimmed, "$.")) trimmed = trimmed[2..];
    if (std.mem.endsWith(u8, trimmed, "[*]")) trimmed = trimmed[0 .. trimmed.len - 3];
    if (trimmed.len == 0) return root;

    var current = root;
    var parts = std.mem.splitScalar(u8, trimmed, '.');
    while (parts.next()) |part| {
        if (part.len == 0) return null;
        if (current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

fn appendRelationValueItems(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
    index_name: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    value: std.json.Value,
    mapping: index_manager_mod.GraphArtifactMapping,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) !void {
    if (value == .array) {
        for (value.array.items, 0..) |item, i| try appendRelationItem(alloc, writes, index_name, doc_key, doc_value, item, i, mapping, artifact_name, artifact_content_type, artifact_value);
    } else {
        try appendRelationItem(alloc, writes, index_name, doc_key, doc_value, value, 0, mapping, artifact_name, artifact_content_type, artifact_value);
    }
}

fn appendRelationItem(
    alloc: Allocator,
    writes: *std.ArrayListUnmanaged(types.GraphEdgeWrite),
    index_name: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    mapping: index_manager_mod.GraphArtifactMapping,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) !void {
    if (item != .object) return;
    const mapped_edge_type = if (mapping.edge_type_template.len > 0)
        try renderGraphArtifactTemplateAlloc(alloc, mapping.edge_type_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        null;
    defer if (mapped_edge_type) |value| alloc.free(value);
    const edge_type = if (mapped_edge_type) |value|
        std.mem.trim(u8, value, &std.ascii.whitespace)
    else
        jsonStringField(item, "type") orelse jsonStringField(item, "edge_type") orelse jsonStringField(item, "relation") orelse return;
    if (edge_type.len == 0) return;

    const mapped_source = if (mapping.source_template.len > 0)
        try renderGraphArtifactTemplateAlloc(alloc, mapping.source_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        null;
    defer if (mapped_source) |value| alloc.free(value);
    const source_doc = if (mapped_source) |value| blk: {
        const trimmed = std.mem.trim(u8, value, &std.ascii.whitespace);
        break :blk if (trimmed.len > 0) trimmed else doc_key;
    } else if (item.object.get("source")) |source_value|
        jsonEndpointDocumentIdResolved(source_value, artifact_value) orelse doc_key
    else
        doc_key;

    const mapped_target = if (mapping.target_template.len > 0)
        try renderGraphArtifactTemplateAlloc(alloc, mapping.target_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        null;
    defer if (mapped_target) |value| alloc.free(value);
    const target_doc = if (mapped_target) |value| blk: {
        const trimmed = std.mem.trim(u8, value, &std.ascii.whitespace);
        if (trimmed.len == 0) return;
        break :blk trimmed;
    } else blk: {
        const target_value = item.object.get("target") orelse return;
        break :blk jsonEndpointDocumentIdResolved(target_value, artifact_value) orelse return;
    };

    const weight = if (mapping.weight_template.len > 0) blk: {
        const rendered = try renderGraphArtifactTemplateAlloc(alloc, mapping.weight_template, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
        defer alloc.free(rendered);
        const trimmed = std.mem.trim(u8, rendered, &std.ascii.whitespace);
        break :blk if (trimmed.len > 0) try std.fmt.parseFloat(f64, trimmed) else 1.0;
    } else jsonFloatField(item, "weight") orelse jsonFloatField(item, "confidence") orelse 1.0;
    const metadata_json = if (mapping.metadata_template_json.len > 0)
        try renderGraphArtifactMetadataTemplateAlloc(alloc, mapping.metadata_template_json, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)
    else
        try std.json.Stringify.valueAlloc(alloc, item, .{});
    errdefer alloc.free(metadata_json);

    try writes.append(alloc, .{
        .index_name = try alloc.dupe(u8, index_name),
        .source = try alloc.dupe(u8, source_doc),
        .target = try alloc.dupe(u8, target_doc),
        .edge_type = try alloc.dupe(u8, edge_type),
        .weight = weight,
        .created_at = 0,
        .updated_at = 0,
        .metadata_json = metadata_json,
    });
}

fn renderGraphArtifactTemplateAlloc(
    alloc: Allocator,
    template_source: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var pos: usize = 0;
    while (pos < template_source.len) {
        const start = std.mem.indexOfPos(u8, template_source, pos, "{{") orelse {
            try out.appendSlice(alloc, template_source[pos..]);
            break;
        };
        try out.appendSlice(alloc, template_source[pos..start]);
        const body_start = start + 2;
        const end = std.mem.indexOfPos(u8, template_source, body_start, "}}") orelse {
            try out.appendSlice(alloc, template_source[start..]);
            break;
        };
        const expr = std.mem.trim(u8, template_source[body_start..end], &std.ascii.whitespace);
        const rendered = try renderGraphArtifactExpressionAlloc(alloc, expr, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
        defer alloc.free(rendered);
        try out.appendSlice(alloc, rendered);
        pos = end + 2;
    }
    return try out.toOwnedSlice(alloc);
}

fn renderGraphArtifactExpressionAlloc(
    alloc: Allocator,
    expr: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ![]u8 {
    if (std.mem.startsWith(u8, expr, "default ")) {
        var parts = std.mem.tokenizeAny(u8, expr["default ".len..], &std.ascii.whitespace);
        const path = parts.next() orelse return try alloc.dupe(u8, "");
        const fallback = parts.next() orelse "";
        const value = graphTemplateValue(path, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
        const text = if (value) |found| try graphJsonValueTextAlloc(alloc, found) else try alloc.dupe(u8, fallback);
        if (std.mem.trim(u8, text, &std.ascii.whitespace).len == 0 and fallback.len > 0) {
            alloc.free(text);
            return try alloc.dupe(u8, fallback);
        }
        return text;
    }
    if (graphTemplateValue(expr, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value)) |value| {
        return try graphJsonValueTextAlloc(alloc, value);
    }
    return try alloc.dupe(u8, "");
}

fn graphTemplateValue(
    path: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ?std.json.Value {
    if (std.mem.eql(u8, path, "_doc.key")) return .{ .string = doc_key };
    if (std.mem.startsWith(u8, path, "_doc.value.")) {
        const doc = doc_value orelse return null;
        return selectJsonDotPath(doc, path["_doc.value.".len..]);
    }
    if (std.mem.eql(u8, path, "_artifact.name")) return .{ .string = artifact_name };
    if (std.mem.eql(u8, path, "_artifact.content_type")) return .{ .string = artifact_content_type };
    if (std.mem.eql(u8, path, "_artifact.value")) return artifact_value;
    if (std.mem.startsWith(u8, path, "_artifact.value.")) return selectJsonDotPath(artifact_value, path["_artifact.value.".len..]);
    if (std.mem.eql(u8, path, "_item_index")) return .{ .integer = @intCast(item_index) };
    if (std.mem.eql(u8, path, "_item")) return item;
    if (std.mem.startsWith(u8, path, "_item.")) return selectGraphItemDotPath(item, path["_item.".len..], artifact_value);
    return null;
}

fn selectGraphItemDotPath(item: std.json.Value, path: []const u8, artifact_value: std.json.Value) ?std.json.Value {
    if (std.mem.eql(u8, path, "source") or std.mem.startsWith(u8, path, "source.")) {
        if (item != .object) return null;
        const endpoint = item.object.get("source") orelse return null;
        const selected = resolveGraphEndpointEntity(endpoint, artifact_value) orelse endpoint;
        if (std.mem.eql(u8, path, "source")) return selected;
        return selectJsonDotPath(selected, path["source.".len..]);
    }
    if (std.mem.eql(u8, path, "target") or std.mem.startsWith(u8, path, "target.")) {
        if (item != .object) return null;
        const endpoint = item.object.get("target") orelse return null;
        const selected = resolveGraphEndpointEntity(endpoint, artifact_value) orelse endpoint;
        if (std.mem.eql(u8, path, "target")) return selected;
        return selectJsonDotPath(selected, path["target.".len..]);
    }
    return selectJsonDotPath(item, path);
}

fn selectJsonDotPath(root: std.json.Value, path: []const u8) ?std.json.Value {
    var current = root;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (part.len == 0) return null;
        if (current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

fn graphJsonValueTextAlloc(alloc: Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .null => try alloc.dupe(u8, ""),
        .bool => |b| try alloc.dupe(u8, if (b) "true" else "false"),
        .integer => |n| try std.fmt.allocPrint(alloc, "{d}", .{n}),
        .float => |n| try std.fmt.allocPrint(alloc, "{d}", .{n}),
        .number_string => |s| try alloc.dupe(u8, s),
        .string => |s| try alloc.dupe(u8, s),
        .array, .object => try std.json.Stringify.valueAlloc(alloc, value, .{}),
    };
}

fn renderGraphArtifactMetadataTemplateAlloc(
    alloc: Allocator,
    metadata_template_json: []const u8,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, metadata_template_json, .{});
    defer parsed.deinit();
    var rendered = try renderGraphArtifactMetadataValueAlloc(alloc, parsed.value, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value);
    defer freeGraphRenderedJsonValue(alloc, &rendered);
    return try std.json.Stringify.valueAlloc(alloc, rendered, .{});
}

fn renderGraphArtifactMetadataValueAlloc(
    alloc: Allocator,
    value: std.json.Value,
    doc_key: []const u8,
    doc_value: ?std.json.Value,
    item: std.json.Value,
    item_index: usize,
    artifact_name: []const u8,
    artifact_content_type: []const u8,
    artifact_value: std.json.Value,
) !std.json.Value {
    return switch (value) {
        .string => |text| .{ .string = try renderGraphArtifactTemplateAlloc(alloc, text, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value) },
        .array => |array| blk: {
            var out = std.json.Array.init(alloc);
            errdefer out.deinit();
            for (array.items) |child| try out.append(try renderGraphArtifactMetadataValueAlloc(alloc, child, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value));
            break :blk .{ .array = out };
        },
        .object => |object| blk: {
            var out = std.json.ObjectMap.empty;
            errdefer out.deinit(alloc);
            var it = object.iterator();
            while (it.next()) |entry| {
                try out.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try renderGraphArtifactMetadataValueAlloc(alloc, entry.value_ptr.*, doc_key, doc_value, item, item_index, artifact_name, artifact_content_type, artifact_value));
            }
            break :blk .{ .object = out };
        },
        else => value,
    };
}

fn freeGraphRenderedJsonValue(alloc: Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .string => |text| alloc.free(@constCast(text)),
        .array => |*array| {
            for (array.items) |*item| freeGraphRenderedJsonValue(alloc, item);
            array.deinit();
        },
        .object => |*object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                freeGraphRenderedJsonValue(alloc, entry.value_ptr);
            }
            object.deinit(alloc);
        },
        else => {},
    }
    value.* = .null;
}

fn jsonEndpointDocumentId(value: std.json.Value) ?[]const u8 {
    return switch (value) {
        .string => value.string,
        .object => jsonStringField(value, "document_id") orelse jsonStringField(value, "doc_key") orelse jsonStringField(value, "key") orelse jsonStringField(value, "id") orelse jsonStringField(value, "local_id") orelse if (value.object.get("doc_ref")) |doc_ref| jsonEndpointDocumentId(doc_ref) else null,
        else => null,
    };
}

fn jsonEndpointDocumentIdResolved(value: std.json.Value, artifact_value: std.json.Value) ?[]const u8 {
    return jsonEndpointDocumentId(value) orelse if (resolveGraphEndpointEntity(value, artifact_value)) |entity| jsonEndpointDocumentId(entity) else null;
}

fn resolveGraphEndpointEntity(value: std.json.Value, artifact_value: std.json.Value) ?std.json.Value {
    if (value != .object) return null;
    if (jsonIntegerField(value, "entity_index")) |entity_index| return graphArtifactEntityAtIndex(artifact_value, entity_index);
    const entity_id = jsonStringField(value, "entity_id") orelse jsonStringField(value, "id") orelse jsonStringField(value, "local_id") orelse return null;
    return findGraphArtifactEntity(artifact_value, entity_id);
}

fn findGraphArtifactEntity(artifact_value: std.json.Value, entity_id: []const u8) ?std.json.Value {
    if (artifact_value != .object) return null;
    const entities = artifact_value.object.get("_entities") orelse artifact_value.object.get("entities") orelse return null;
    return switch (entities) {
        .array => |array| blk: {
            for (array.items) |entity| {
                const id = jsonStringField(entity, "id") orelse jsonStringField(entity, "local_id") orelse continue;
                if (std.mem.eql(u8, id, entity_id)) break :blk entity;
            }
            break :blk null;
        },
        .object => entities.object.get(entity_id),
        else => null,
    };
}

fn graphArtifactEntityAtIndex(artifact_value: std.json.Value, entity_index: i64) ?std.json.Value {
    if (entity_index < 0 or artifact_value != .object) return null;
    const entities = artifact_value.object.get("_entities") orelse artifact_value.object.get("entities") orelse return null;
    if (entities != .array) return null;
    const index: usize = @intCast(entity_index);
    if (index >= entities.array.items.len) return null;
    return entities.array.items[index];
}

fn jsonStringField(value: std.json.Value, field: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return if (found == .string) found.string else null;
}

fn jsonIntegerField(value: std.json.Value, field: []const u8) ?i64 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return switch (found) {
        .integer => found.integer,
        else => null,
    };
}

fn jsonFloatField(value: std.json.Value, field: []const u8) ?f64 {
    if (value != .object) return null;
    const found = value.object.get(field) orelse return null;
    return switch (found) {
        .float => found.float,
        .integer => @floatFromInt(found.integer),
        else => null,
    };
}
