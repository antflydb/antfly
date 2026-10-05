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

//! Artifact/read projection from borrowed local core and pinned transactions.
//! Contexts are synchronous borrows; statement admission belongs to DB.
const std = @import("std");
const Allocator = std.mem.Allocator;
const hierarchy_navigation = @import("../hierarchy_navigation.zig");
const types = @import("types.zig");
const db_core = @import("core.zig");
const docstore_mod = @import("../docstore.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const artifact_ids = @import("artifact_ids.zig");
const internal_keys = @import("../internal_keys.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const db_query_projection = @import("query/projection.zig");
const assetContentTypeIsJson = @import("document_collectors.zig").assetContentTypeIsJson;
const freeJsonValue = db_query_projection.freeJsonValue;
const cloneJsonValue = db_query_projection.cloneJsonValue;
const putOwnedValue = db_query_projection.putOwnedValue;

pub const Source = struct { core: *db_core.DBCore };

pub fn loadChunkFieldValueTxn(self: Source, alloc: Allocator, doc_key: []const u8, read_txn: ?*docstore_mod.DocStore.Txn) !?std.json.Value {
    if (read_txn == null) {
        var read = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        return loadChunkFieldValueTxn(self, alloc, doc_key, &read);
    }
    // One logical scan pins heads and members together, skips obsolete tails,
    // and owns no second copy of the raw output set beside projected JSON.
    var cursor = try @import("artifact_chunk_cursor.zig").Cursor(docstore_mod.DocStore.Txn).open(alloc, read_txn.?, doc_key);
    defer cursor.close();

    var chunks_obj = std.json.ObjectMap.empty;
    errdefer {
        var it = chunks_obj.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr);
        }
        chunks_obj.deinit(alloc);
    }

    var chunk_count: usize = 0;
    while (try cursor.next()) |entry| {
        // Classification already validated the chunk identity. Borrow the
        // common unescaped stream name instead of allocating document/name/
        // unit identity strings for every member; retain the binary fallback.
        var artifact_ref: ?types.ArtifactRef = null;
        defer if (artifact_ref) |*identity| identity.deinit(alloc);
        const chunk_name = (try internal_keys.artifactNameView(entry.key)) orelse blk: {
            artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, entry.key)) orelse continue;
            break :blk artifact_ref.?.name;
        };

        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, entry.value, .{});
        defer parsed.deinit();
        var cloned = try cloneJsonValue(alloc, parsed.value);
        errdefer freeJsonValue(alloc, &cloned);
        try db_query_projection.normalizeChunkArtifactForQuery(alloc, &cloned);

        if (chunks_obj.getPtr(chunk_name)) |existing| {
            if (existing.* != .array) {
                freeJsonValue(alloc, existing);
                existing.* = .{ .array = std.json.Array.init(alloc) };
            }
            try existing.array.append(cloned);
        } else {
            var arr = std.json.Array.init(alloc);
            // `cloned` remains owned by the outer errdefer until the entire
            // group is installed. The array owns only its backing allocation
            // on this error path; freeing its item again would double-free it.
            errdefer arr.deinit();
            try arr.append(cloned);
            const name = try alloc.dupe(u8, chunk_name);
            errdefer alloc.free(name);
            try chunks_obj.put(alloc, name, .{ .array = arr });
        }

        chunk_count += 1;
    }

    if (chunk_count == 0) {
        var empty = std.json.Value{ .object = chunks_obj };
        freeJsonValue(alloc, &empty);
        return null;
    }
    return .{ .object = chunks_obj };
}

pub fn loadEmbeddingFieldValueTxn(self: Source, alloc: Allocator, doc_key: []const u8, read_txn: ?*docstore_mod.DocStore.Txn) !?std.json.Value {
    const prefix = try internal_keys.artifactTypePrefixAlloc(alloc, doc_key, "embedding");
    defer alloc.free(prefix);

    const artifacts = if (read_txn) |txn|
        try docstore_mod.DocStore.scanPrefixTxn(alloc, txn, prefix)
    else
        try self.core.scanStorePrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);

    var embeddings_obj = std.json.ObjectMap.empty;
    errdefer {
        var it = embeddings_obj.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr);
        }
        embeddings_obj.deinit(alloc);
    }

    var embedding_count: usize = 0;
    for (artifacts) |entry| {
        if (!internal_keys.isInternalUserKey(entry.key)) continue;

        var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, entry.key)) orelse continue;
        defer artifact_ref.deinit(alloc);
        if (artifact_ref.kind != .embedding or artifact_ref.source != null) continue;

        var vector = enrichment_artifact_codec.decodeDenseEmbeddingJsonVectorAlloc(alloc, entry.value) catch continue;
        errdefer freeJsonValue(alloc, &vector);
        try putOwnedValue(alloc, &embeddings_obj, artifact_ref.name, vector);
        embedding_count += 1;
    }

    if (embedding_count == 0) {
        var empty = std.json.Value{ .object = embeddings_obj };
        freeJsonValue(alloc, &empty);
        return null;
    }
    return .{ .object = embeddings_obj };
}

pub fn loadArtifactFieldValueTxn(
    self: Source,
    alloc: Allocator,
    doc_key: []const u8,
    read_txn: ?*docstore_mod.DocStore.Txn,
    artifact_catalog: ?*const index_manager_mod.IndexManager.AssetContentTypeSnapshot,
) !?std.json.Value {
    const prefix = try internal_keys.artifactRootPrefixAlloc(alloc, doc_key);
    defer alloc.free(prefix);

    const artifacts = if (read_txn) |txn|
        try docstore_mod.DocStore.scanPrefixTxn(alloc, txn, prefix)
    else
        try self.core.scanStorePrefix(alloc, prefix);
    defer docstore_mod.DocStore.freeResults(alloc, artifacts);

    var artifacts_obj = std.json.ObjectMap.empty;
    errdefer {
        var it = artifacts_obj.iterator();
        while (it.next()) |entry| {
            alloc.free(entry.key_ptr.*);
            freeJsonValue(alloc, entry.value_ptr);
        }
        artifacts_obj.deinit(alloc);
    }

    var artifact_count: usize = 0;
    for (artifacts) |entry| {
        var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, entry.key)) orelse continue;
        defer artifact_ref.deinit(alloc);

        var artifact_value = try artifactProjectionValue(self, alloc, artifact_ref, entry.value, artifact_catalog);
        errdefer freeJsonValue(alloc, &artifact_value);

        try appendArtifactProjectionValue(alloc, &artifacts_obj, artifact_ref.name, artifact_ref.kind, artifact_value);
        artifact_count += 1;
    }

    if (artifact_count == 0) {
        var empty = std.json.Value{ .object = artifacts_obj };
        freeJsonValue(alloc, &empty);
        return null;
    }
    return .{ .object = artifacts_obj };
}

pub fn artifactProjectionValue(
    self: Source,
    alloc: Allocator,
    artifact_ref: types.ArtifactRef,
    raw: []const u8,
    artifact_catalog: ?*const index_manager_mod.IndexManager.AssetContentTypeSnapshot,
) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    errdefer {
        var value = std.json.Value{ .object = obj };
        freeJsonValue(alloc, &value);
    }

    {
        const artifact_id = try artifact_ids.artifactPublicIdAlloc(alloc, artifact_ref);
        errdefer alloc.free(artifact_id);
        try putOwnedValue(alloc, &obj, "artifact_id", .{ .string = artifact_id });
    }

    {
        var ref_value = try artifactRefJsonValue(alloc, artifact_ref);
        errdefer freeJsonValue(alloc, &ref_value);
        try putOwnedValue(alloc, &obj, "artifact_ref", ref_value);
    }

    try putOwnedValue(alloc, &obj, "kind", .{ .string = try alloc.dupe(u8, artifactKindText(artifact_ref.kind)) });
    const content_type = if (artifact_ref.kind == .asset and artifact_catalog != null)
        try artifact_catalog.?.contentTypeAlloc(alloc, artifact_ref.name)
    else
        try artifactContentTypeAlloc(self, alloc, artifact_ref.kind, artifact_ref.name);
    try putOwnedValue(alloc, &obj, "content_type", .{ .string = content_type });
    try putOwnedValue(alloc, &obj, "status", .{ .string = try alloc.dupe(u8, "ready") });

    if (enrichment_artifact_codec.sourceHash(raw) catch null) |source_hash| {
        try putOwnedValue(alloc, &obj, "source_hash", .{ .string = try std.fmt.allocPrint(alloc, "xxh64:{x}", .{source_hash}) });
    }

    switch (artifact_ref.kind) {
        .chunk => {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch null;
            if (parsed) |*owned| {
                defer owned.deinit();
                var cloned = try cloneJsonValue(alloc, owned.value);
                errdefer freeJsonValue(alloc, &cloned);
                try db_query_projection.normalizeChunkArtifactForQuery(alloc, &cloned);
                try putOwnedValue(alloc, &obj, "value", cloned);
            } else {
                try putOwnedValue(alloc, &obj, "value", .{ .string = try alloc.dupe(u8, raw) });
            }
        },
        .asset => {
            var value = try assetPayloadJsonValue(alloc, content_type, raw);
            errdefer freeJsonValue(alloc, &value);
            hierarchy_navigation.stripPublicInternalFieldsValue(alloc, &value);
            try putOwnedValue(alloc, &obj, "value", value);
        },
        .embedding => {
            if (enrichment_artifact_codec.decodeDenseEmbeddingDims(raw) catch null) |dims| {
                try putOwnedValue(alloc, &obj, "dims", .{ .integer = @intCast(dims) });
            }
            try putOwnedValue(alloc, &obj, "value", .null);
        },
    }

    return .{ .object = obj };
}

pub fn appendArtifactProjectionValue(
    alloc: Allocator,
    artifacts_obj: *std.json.ObjectMap,
    artifact_name: []const u8,
    artifact_kind: types.ArtifactKind,
    artifact_value: std.json.Value,
) !void {
    if (artifacts_obj.getPtr(artifact_name)) |existing| {
        if (existing.* == .object) {
            if (existing.object.getPtr("items")) |items| {
                if (items.* == .array) {
                    try items.array.append(artifact_value);
                    return;
                }
            }
        }

        var first = try cloneJsonValue(alloc, existing.*);
        var first_moved = false;
        errdefer if (!first_moved) freeJsonValue(alloc, &first);
        var items = std.json.Array.init(alloc);
        errdefer {
            for (items.items) |*item| freeJsonValue(alloc, item);
            items.deinit();
        }
        try items.append(first);
        first_moved = true;
        try items.append(artifact_value);

        var grouped = std.json.ObjectMap.empty;
        errdefer {
            var value = std.json.Value{ .object = grouped };
            freeJsonValue(alloc, &value);
        }
        try putOwnedValue(alloc, &grouped, "kind", .{ .string = try alloc.dupe(u8, artifactSetKindText(artifact_kind)) });
        try putOwnedValue(alloc, &grouped, "status", .{ .string = try alloc.dupe(u8, "ready") });
        try putOwnedValue(alloc, &grouped, "items", .{ .array = items });

        freeJsonValue(alloc, existing);
        existing.* = .{ .object = grouped };
        return;
    }

    try artifacts_obj.put(alloc, try alloc.dupe(u8, artifact_name), artifact_value);
}

pub fn artifactRefJsonValue(alloc: Allocator, artifact_ref: types.ArtifactRef) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    errdefer {
        var value = std.json.Value{ .object = obj };
        freeJsonValue(alloc, &value);
    }
    try putOwnedValue(alloc, &obj, "document_id", .{ .string = try alloc.dupe(u8, artifact_ref.document_id) });
    try putOwnedValue(alloc, &obj, "name", .{ .string = try alloc.dupe(u8, artifact_ref.name) });
    try putOwnedValue(alloc, &obj, "kind", .{ .string = try alloc.dupe(u8, artifactKindText(artifact_ref.kind)) });
    if (artifact_ref.chunk_id) |chunk_id| {
        try putOwnedValue(alloc, &obj, "chunk_id", .{ .integer = @intCast(chunk_id) });
    }
    if (artifact_ref.source) |source| {
        var source_obj = std.json.ObjectMap.empty;
        errdefer {
            var value = std.json.Value{ .object = source_obj };
            freeJsonValue(alloc, &value);
        }
        try putOwnedValue(alloc, &source_obj, "kind", .{ .string = try alloc.dupe(u8, artifactKindText(source.kind)) });
        try putOwnedValue(alloc, &source_obj, "name", .{ .string = try alloc.dupe(u8, source.name) });
        if (source.chunk_id) |chunk_id| {
            try putOwnedValue(alloc, &source_obj, "chunk_id", .{ .integer = @intCast(chunk_id) });
        }
        try putOwnedValue(alloc, &obj, "source", .{ .object = source_obj });
    }
    return .{ .object = obj };
}

pub fn artifactKindText(kind: types.ArtifactKind) []const u8 {
    return switch (kind) {
        .chunk => "chunk",
        .asset => "asset",
        .embedding => "embedding",
    };
}

pub fn artifactSetKindText(kind: types.ArtifactKind) []const u8 {
    return switch (kind) {
        .chunk => "chunk_set",
        .asset => "asset_set",
        .embedding => "embedding_set",
    };
}

pub fn artifactContentType(kind: types.ArtifactKind) []const u8 {
    return switch (kind) {
        .chunk => "application/json",
        .asset => "application/octet-stream",
        .embedding => "application/vnd.antfly.embedding+binary",
    };
}

pub fn artifactContentTypeAlloc(self: Source, alloc: Allocator, kind: types.ArtifactKind, artifact_name: []const u8) ![]u8 {
    if (kind == .asset) {
        if (self.core.index_manager.getEnrichment(.asset, artifact_name)) |cfg| {
            if (cfg.content_type.len > 0) return try alloc.dupe(u8, cfg.content_type);
            return try alloc.dupe(u8, "text/plain");
        }
    }
    return try alloc.dupe(u8, artifactContentType(kind));
}

pub fn assetPayloadJsonValue(alloc: Allocator, content_type: []const u8, raw: []const u8) !std.json.Value {
    if (assetContentTypeIsJson(content_type)) {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        return try cloneJsonValue(alloc, parsed.value);
    }
    return .{ .string = try alloc.dupe(u8, raw) };
}

pub const TransactionProjectionContext = struct {
    source: Source,
    read_txn: *docstore_mod.DocStore.Txn,
    artifact_catalog: ?*const index_manager_mod.IndexManager.AssetContentTypeSnapshot = null,
};

pub fn loadChunkFieldValueTxnCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    doc_key: []const u8,
) anyerror!?std.json.Value {
    const projection: *TransactionProjectionContext = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try loadChunkFieldValueTxn(projection.source, alloc, doc_key, projection.read_txn);
}

pub fn loadEmbeddingFieldValueTxnCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    doc_key: []const u8,
) anyerror!?std.json.Value {
    const projection: *TransactionProjectionContext = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try loadEmbeddingFieldValueTxn(projection.source, alloc, doc_key, projection.read_txn);
}

pub fn loadArtifactFieldValueTxnCallback(
    ctx: ?*anyopaque,
    alloc: Allocator,
    doc_key: []const u8,
) anyerror!?std.json.Value {
    const projection: *TransactionProjectionContext = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try loadArtifactFieldValueTxn(
        projection.source,
        alloc,
        doc_key,
        projection.read_txn,
        projection.artifact_catalog,
    );
}

pub const ColumnScanMaterializer = struct {
    context: *TransactionProjectionContext,
    opts: types.ScanOptions,

    pub fn project(raw: *anyopaque, alloc: Allocator, key: []const u8, row: @import("algebraic/relational_row_codec.zig").OrdinalRowView) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const logical = try row.reconstructValueAlloc(alloc);
        if (self.opts.fields.len == 0 and self.opts.include_all_fields) return logical;
        defer alloc.free(logical);
        return projectLookupStoredBytesTxn(self.context, alloc, key, logical, .{
            .fields = self.opts.fields,
            .include_all_fields = self.opts.include_all_fields,
        });
    }
};

pub fn projectLookupStoredBytesTxn(
    context: *TransactionProjectionContext,
    alloc: Allocator,
    doc_key: []const u8,
    raw: []const u8,
    opts: types.LookupOptions,
) ![]u8 {
    return try db_query_projection.projectLookupStoredBytes(alloc, doc_key, raw, opts, .{
        .ctx = context,
        .load_chunks = loadChunkFieldValueTxnCallback,
        .load_embeddings = loadEmbeddingFieldValueTxnCallback,
        .load_artifacts = loadArtifactFieldValueTxnCallback,
    });
}

fn decodeArtifactRefIfKnownAlloc(alloc: Allocator, key: []const u8) !?types.ArtifactRef {
    return artifact_ids.decodeArtifactRefAlloc(alloc, key) catch |err| switch (err) {
        error.InvalidInternalUserKey => null,
        else => return err,
    };
}
