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
const putClonedValue = @import("../../common/owned_json.zig").putClone;

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

/// Retain only field identities while the immutable snapshot is pinned. Values
/// are resolved one at a time; payload cursors and decode scratch retire each
/// field before the next. No database owner escapes projection serialization.
pub fn openEmbeddingStreamTxn(self: Source, alloc: Allocator, doc_key: []const u8, read_txn: ?*docstore_mod.DocStore.Txn) !?db_query_projection.EmbeddingStream {
    const backing = @import("antfly_platform").allocator.reclaimingScratch(alloc);
    const state = try backing.create(EmbeddingStreamState);
    errdefer backing.destroy(state);
    state.* = .{ .backing = backing, .scratch = .init(backing) };
    errdefer state.freeKeys();
    errdefer state.scratch.deinit();
    if (read_txn == null) state.owned_txn = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
    errdefer if (state.owned_txn) |*txn| txn.abort();
    const txn = read_txn orelse &state.owned_txn.?;
    const a = backing;
    const prefix = try internal_keys.artifactTypePrefixAlloc(a, doc_key, "embedding");
    defer a.free(prefix);
    {
        // Retire the inventory cursor before admitting the value cursor; their
        // decoded pages and payload arenas must not overlap at row boundaries.
        var keys_cursor = try txn.openPhysicalKeyCursorAdapter();
        defer keys_cursor.close();
        var next = try keys_cursor.seekAtOrAfter(prefix);
        var adjacent = false;
        while (next) |entry| : (next = try keys_cursor.next()) {
            const contiguous = adjacent;
            adjacent = false;
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            if (!internal_keys.isInternalUserKey(entry.key)) continue;
            try state.resetScratch();
            const temporary = state.scratch.allocator();
            var identity = (try decodeArtifactRefIfKnownAlloc(temporary, entry.key)) orelse continue;
            defer identity.deinit(temporary);
            if (identity.kind != .embedding or identity.source != null) continue;
            adjacent = true;
            // Keys alone identify modern fields. Duplicate historical identities
            // use the validated compatibility inventory below.
            const owned_key = try a.dupe(u8, entry.key);
            errdefer a.free(owned_key);
            if (state.keys.getPtr(identity.name)) |previous| {
                state.legacy_duplicates = true;
                a.free(previous.key);
                previous.* = .{ .key = owned_key, .contiguous = contiguous };
            } else {
                const name = try a.dupe(u8, identity.name);
                errdefer a.free(name);
                try state.keys.put(a, name, .{ .key = owned_key, .contiguous = contiguous });
            }
        }
    }
    state.cursor = try txn.openCursor();
    errdefer state.cursor.?.close();
    if (state.legacy_duplicates) try state.validateLegacyOrder(prefix);
    // Prime one valid value without replaying it during the first write. A
    // completely invalid/empty artifact set must still fall back to primary.
    state.primed = try state.readNext();
    if (state.primed == null) {
        state.close();
        return null;
    }
    return .{ .ctx = state, .next_fn = EmbeddingStreamState.next, .reset_fn = EmbeddingStreamState.reset, .field_fn = EmbeddingStreamState.field, .close_fn = EmbeddingStreamState.closeErased };
}

const EmbeddingStreamState = struct {
    const Candidate = struct { key: []const u8, contiguous: bool = false };
    backing: Allocator,
    scratch: std.heap.ArenaAllocator,
    keys: std.StringArrayHashMapUnmanaged(Candidate) = .empty,
    primed: ?db_query_projection.EmbeddingStream.Field = null,
    legacy_duplicates: bool = false,
    resolved_index: ?usize = null,
    owned_txn: ?docstore_mod.DocStore.Txn = null,
    cursor: ?docstore_mod.DocStore.Txn.CursorAdapter = null,
    index: usize = 0,
    fn next(ctx: *anyopaque) anyerror!?db_query_projection.EmbeddingStream.Field {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (self.primed) |primed| {
            self.primed = null;
            return primed;
        }
        return self.readNext();
    }
    fn readNext(self: *@This()) !?db_query_projection.EmbeddingStream.Field {
        while (self.index < self.keys.count()) {
            const i = self.index;
            self.index += 1;
            if (try self.resolve(i)) |vector| return .{ .name = self.keys.keys()[i], .vector = vector };
        }
        return null;
    }
    fn validateLegacyOrder(self: *@This(), prefix: []const u8) !void {
        // Historical identities can name the same field more than once. Their
        // order is first *valid* occurrence, their value last valid occurrence.
        // Only this compatibility path needs a value inventory pass.
        self.freeKeys();
        self.keys = .empty;
        var row = try self.cursor.?.seekAtOrAfter(prefix);
        while (row) |entry| : (row = try self.cursor.?.next()) {
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            if (!internal_keys.isInternalUserKey(entry.key)) continue;
            try self.resetScratch();
            var identity = (try decodeArtifactRefIfKnownAlloc(self.scratch.allocator(), entry.key)) orelse continue;
            defer identity.deinit(self.scratch.allocator());
            if (identity.kind != .embedding or identity.source != null) continue;
            _ = enrichment_artifact_codec.decodeDenseEmbeddingDims(entry.value) catch continue;
            const key = try self.backing.dupe(u8, entry.key);
            errdefer self.backing.free(key);
            if (self.keys.getPtr(identity.name)) |old| {
                self.backing.free(old.key);
                old.* = .{ .key = key };
            } else {
                const name = try self.backing.dupe(u8, identity.name);
                errdefer self.backing.free(name);
                try self.keys.put(self.backing, name, .{ .key = key });
            }
        }
    }
    fn resolve(self: *@This(), i: usize) !?[]const f32 {
        const current = &self.keys.values()[i];
        try self.resetScratch();
        var row = if (!self.legacy_duplicates and current.contiguous and self.resolved_index != null and self.resolved_index.? + 1 == i)
            try self.cursor.?.next()
        else
            try self.cursor.?.seekAtOrAfter(current.key);
        self.resolved_index = i;
        while (row) |entry| {
            if (std.mem.order(u8, entry.key, current.key) != .lt) break;
            row = try self.cursor.?.next();
        }
        const entry = row orelse return error.InvalidEmbeddingSnapshot;
        if (!std.mem.eql(u8, entry.key, current.key)) return error.InvalidEmbeddingSnapshot;
        _ = enrichment_artifact_codec.decodeDenseEmbeddingDims(entry.value) catch return null;
        return (try enrichment_artifact_codec.denseEmbeddingVectorView(entry.value)) orelse try enrichment_artifact_codec.decodeDenseEmbeddingAlloc(self.scratch.allocator(), entry.value);
    }

    fn field(ctx: *anyopaque, name: []const u8) anyerror!?[]const f32 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        const i = self.keys.getIndex(name) orelse return null;
        if (self.primed) |primed| {
            if (std.mem.eql(u8, name, primed.name)) return primed.vector;
            // Resolving another field invalidates the primed borrow.
            self.primed = null;
        }
        return self.resolve(i);
    }
    fn resetScratch(self: *@This()) !void {
        if (!self.scratch.reset(if (self.scratch.queryCapacity() > 16 * 1024) .free_all else .retain_capacity)) return error.OutOfMemory;
    }
    fn reset(ctx: *anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (self.primed == null) {
            self.index = 0;
            self.resolved_index = null;
        }
    }
    fn closeErased(ctx: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.close();
    }
    fn freeKeys(self: *@This()) void {
        for (self.keys.keys(), self.keys.values()) |name, first| {
            self.backing.free(name);
            self.backing.free(first.key);
        }
        self.keys.deinit(self.backing);
    }
    fn close(self: *@This()) void {
        self.cursor.?.close();
        if (self.owned_txn) |*txn| txn.abort();
        self.scratch.deinit();
        self.freeKeys();
        const backing = self.backing;
        backing.destroy(self);
    }
};

/// Compact ownership for response projection. Scan results are retired before
/// returning; only dense f32 slabs and field names survive the read scope.
pub fn loadEmbeddingFieldsTxn(self: Source, alloc: Allocator, doc_key: []const u8, read_txn: ?*docstore_mod.DocStore.Txn) !?db_query_projection.EmbeddingFields {
    if (read_txn == null) {
        var read = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        return loadEmbeddingFieldsFromTxn(alloc, doc_key, &read);
    }
    return loadEmbeddingFieldsFromTxn(alloc, doc_key, read_txn.?);
}

fn loadEmbeddingFieldsFromTxn(alloc: Allocator, doc_key: []const u8, read_txn: *docstore_mod.DocStore.Txn) !?db_query_projection.EmbeddingFields {
    const prefix = try internal_keys.artifactTypePrefixAlloc(alloc, doc_key, "embedding");
    defer alloc.free(prefix);
    var cursor = try read_txn.openCursor();
    defer cursor.close();
    var next = try cursor.seekAtOrAfter(prefix);
    var fields: db_query_projection.EmbeddingFields = .{};
    errdefer fields.deinit(alloc);
    while (next) |entry| : (next = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, prefix)) break;
        if (!internal_keys.isInternalUserKey(entry.key)) continue;
        var identity = (try decodeArtifactRefIfKnownAlloc(alloc, entry.key)) orelse continue;
        defer identity.deinit(alloc);
        if (identity.kind != .embedding or identity.source != null) continue;
        const vector = enrichment_artifact_codec.decodeDenseEmbeddingAlloc(alloc, entry.value) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => continue,
        };
        errdefer alloc.free(vector);
        try fields.putOwned(alloc, identity.name, vector);
    }
    if (fields.values.count() == 0) {
        fields.deinit(alloc);
        return null;
    }
    return fields;
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
    if (read_txn == null) {
        var read = try self.core.store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        return loadArtifactFieldValueTxn(self, alloc, doc_key, &read, artifact_catalog);
    }
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
    // Preserve the physical family order for legacy names shared across kinds.
    // Only the logical cursor may enumerate chunks: legacy rows can be hidden
    // by a selected generation (including an empty replacement).
    for ([_]types.ArtifactKind{ .asset, .chunk, .embedding }) |kind| {
        if (kind == .chunk) {
            var chunks = try @import("artifact_chunk_cursor.zig").Cursor(docstore_mod.DocStore.Txn).open(alloc, read_txn.?, doc_key);
            defer chunks.close();
            while (try chunks.next()) |entry| {
                var ref = (try decodeArtifactRefIfKnownAlloc(alloc, entry.key)) orelse return error.ArtifactCatalogCorrupt;
                defer ref.deinit(alloc);
                var value = try artifactProjectionValue(self, alloc, ref, entry.value, artifact_catalog);
                errdefer freeJsonValue(alloc, &value);
                try appendArtifactProjectionValue(alloc, &artifacts_obj, ref.name, ref.kind, value);
                artifact_count += 1;
            }
            // Derived vectors live beneath logical chunk keys, outside the
            // root embedding family. Enumerate them once in the same snapshot
            // and exclude windows/ordinals hidden by the selected generation.
            const prefix = try internal_keys.artifactTypePrefixAlloc(alloc, doc_key, "chunk");
            defer alloc.free(prefix);
            var vectors = try read_txn.?.openCursor();
            defer vectors.close();
            var next = try vectors.seekAtOrAfter(prefix);
            while (next) |entry| : (next = try vectors.next()) {
                if (!std.mem.startsWith(u8, entry.key, prefix)) break;
                const base = (try internal_keys.derivedEmbeddingBaseKeyAlloc(alloc, entry.key)) orelse continue;
                defer alloc.free(base);
                if ((try @import("artifact_chunk_generation.zig").readMember(alloc, read_txn.?, base)) == null) continue;
                var ref = (try decodeArtifactRefIfKnownAlloc(alloc, entry.key)) orelse continue;
                defer ref.deinit(alloc);
                var value = try artifactProjectionValue(self, alloc, ref, entry.value, artifact_catalog);
                errdefer freeJsonValue(alloc, &value);
                try appendArtifactProjectionValue(alloc, &artifacts_obj, ref.name, ref.kind, value);
                artifact_count += 1;
            }
        } else {
            const prefix = try internal_keys.artifactTypePrefixAlloc(alloc, doc_key, @tagName(kind));
            defer alloc.free(prefix);
            const artifacts = try docstore_mod.DocStore.scanPrefixTxn(alloc, read_txn.?, prefix);
            defer docstore_mod.DocStore.freeResults(alloc, artifacts);
            for (artifacts) |entry| {
                var ref = (try decodeArtifactRefIfKnownAlloc(alloc, entry.key)) orelse continue;
                defer ref.deinit(alloc);
                var value = try artifactProjectionValue(self, alloc, ref, entry.value, artifact_catalog);
                errdefer freeJsonValue(alloc, &value);
                try appendArtifactProjectionValue(alloc, &artifacts_obj, ref.name, ref.kind, value);
                artifact_count += 1;
            }
        }
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

    try putClonedValue(alloc, &obj, "kind", .{ .string = artifactKindText(artifact_ref.kind) });
    const content_type = if (artifact_ref.kind == .asset and artifact_catalog != null)
        try artifact_catalog.?.contentTypeAlloc(alloc, artifact_ref.name)
    else
        try artifactContentTypeAlloc(self, alloc, artifact_ref.kind, artifact_ref.name);
    {
        errdefer alloc.free(content_type);
        try putOwnedValue(alloc, &obj, "content_type", .{ .string = content_type });
    }
    try putClonedValue(alloc, &obj, "status", .{ .string = "ready" });

    if (enrichment_artifact_codec.sourceHash(raw) catch null) |source_hash| {
        const text = try std.fmt.allocPrint(alloc, "xxh64:{x}", .{source_hash});
        errdefer alloc.free(text);
        try putOwnedValue(alloc, &obj, "source_hash", .{ .string = text });
    }

    switch (artifact_ref.kind) {
        .chunk => {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw, .{}) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => null,
            };
            if (parsed) |*owned| {
                defer owned.deinit();
                var cloned = try cloneJsonValue(alloc, owned.value);
                errdefer freeJsonValue(alloc, &cloned);
                try db_query_projection.normalizeChunkArtifactForQuery(alloc, &cloned);
                try putOwnedValue(alloc, &obj, "value", cloned);
            } else {
                try putClonedValue(alloc, &obj, "value", .{ .string = raw });
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

        var grouped = std.json.ObjectMap.empty;
        errdefer {
            var value = std.json.Value{ .object = grouped };
            freeJsonValue(alloc, &value);
        }
        try putClonedValue(alloc, &grouped, "kind", .{ .string = artifactSetKindText(artifact_kind) });
        try putClonedValue(alloc, &grouped, "status", .{ .string = "ready" });
        {
            var items = std.json.Array.init(alloc);
            errdefer items.deinit();
            try items.ensureTotalCapacity(2);
            try putOwnedValue(alloc, &grouped, "items", .{ .array = items });
        }
        // All fallible work is complete. Transfer both original values once,
        // without cloning the existing tree or consuming the caller on error.
        const items = &grouped.getPtr("items").?.array;
        items.appendAssumeCapacity(existing.*);
        items.appendAssumeCapacity(artifact_value);

        existing.* = .{ .object = grouped };
        return;
    }

    try putOwnedValue(alloc, artifacts_obj, artifact_name, artifact_value);
}

pub fn artifactRefJsonValue(alloc: Allocator, artifact_ref: types.ArtifactRef) !std.json.Value {
    var obj = std.json.ObjectMap.empty;
    errdefer {
        var value = std.json.Value{ .object = obj };
        freeJsonValue(alloc, &value);
    }
    try putClonedValue(alloc, &obj, "document_id", .{ .string = artifact_ref.document_id });
    try putClonedValue(alloc, &obj, "name", .{ .string = artifact_ref.name });
    try putClonedValue(alloc, &obj, "kind", .{ .string = artifactKindText(artifact_ref.kind) });
    if (artifact_ref.chunk_id) |chunk_id| {
        try putOwnedValue(alloc, &obj, "chunk_id", .{ .integer = @intCast(chunk_id) });
    }
    if (artifact_ref.window_index) |index| try putOwnedValue(alloc, &obj, "window_index", .{ .integer = index });
    if (artifact_ref.unit_id) |unit| try putClonedValue(alloc, &obj, "unit_id", .{ .string = unit });
    if (artifact_ref.source) |source| {
        var source_obj = std.json.ObjectMap.empty;
        errdefer {
            var value = std.json.Value{ .object = source_obj };
            freeJsonValue(alloc, &value);
        }
        try putClonedValue(alloc, &source_obj, "kind", .{ .string = artifactKindText(source.kind) });
        try putClonedValue(alloc, &source_obj, "name", .{ .string = source.name });
        if (source.chunk_id) |chunk_id| {
            try putOwnedValue(alloc, &source_obj, "chunk_id", .{ .integer = @intCast(chunk_id) });
        }
        if (source.window_index) |index| try putOwnedValue(alloc, &source_obj, "window_index", .{ .integer = index });
        if (source.unit_id) |unit| try putClonedValue(alloc, &source_obj, "unit_id", .{ .string = unit });
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

pub fn openEmbeddingStreamTxnCallback(ctx: ?*anyopaque, alloc: Allocator, doc_key: []const u8) anyerror!?db_query_projection.EmbeddingStream {
    const projection: *TransactionProjectionContext = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return openEmbeddingStreamTxn(projection.source, alloc, doc_key, projection.read_txn);
}

pub fn loadEmbeddingFieldsTxnCallback(ctx: ?*anyopaque, alloc: Allocator, doc_key: []const u8) anyerror!?db_query_projection.EmbeddingFields {
    const projection: *TransactionProjectionContext = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return loadEmbeddingFieldsTxn(projection.source, alloc, doc_key, projection.read_txn);
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
        .open_embeddings = openEmbeddingStreamTxnCallback,
        .load_embeddings_compact = loadEmbeddingFieldsTxnCallback,
        .load_artifacts = loadArtifactFieldValueTxnCallback,
    });
}

fn decodeArtifactRefIfKnownAlloc(alloc: Allocator, key: []const u8) !?types.ArtifactRef {
    return artifact_ids.decodeArtifactRefAlloc(alloc, key) catch |err| switch (err) {
        error.InvalidInternalUserKey => null,
        else => return err,
    };
}

test "artifact projection reference releases all failed allocations" {
    const Check = struct {
        fn run(alloc: Allocator) !void {
            var value = try artifactRefJsonValue(alloc, .{
                .document_id = @constCast("document"),
                .name = @constCast("embedding"),
                .kind = .embedding,
                .chunk_id = 7,
                .source = .{ .name = @constCast("chunks"), .kind = .chunk, .chunk_id = 3, .window_index = 0, .unit_id = @constCast("unit") },
            });
            defer freeJsonValue(alloc, &value);
            try std.testing.expectEqualStrings("document", value.object.get("document_id").?.string);
            try std.testing.expectEqualStrings("chunks", value.object.get("source").?.object.get("name").?.string);
            try std.testing.expectEqual(@as(i64, 0), value.object.get("source").?.object.get("window_index").?.integer);
            try std.testing.expectEqualStrings("unit", value.object.get("source").?.object.get("unit_id").?.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "artifact projection grouping transfers nested values only on success" {
    const Check = struct {
        fn run(alloc: Allocator) !void {
            var object = std.json.ObjectMap.empty;
            defer {
                var value = std.json.Value{ .object = object };
                freeJsonValue(alloc, &value);
            }
            for (0..4) |i| {
                var incoming = try artifactRefJsonValue(alloc, .{ .document_id = @constCast("doc"), .name = @constCast("asset"), .kind = .asset });
                appendArtifactProjectionValue(alloc, &object, "asset", .asset, incoming) catch |err| {
                    // A failed append must preserve both the existing group
                    // and this nested value, which is still the caller's.
                    try std.testing.expectEqualStrings("doc", incoming.object.get("document_id").?.string);
                    freeJsonValue(alloc, &incoming);
                    if (i > 0) {
                        const prior = object.get("asset").?;
                        if (i == 1) {
                            try std.testing.expectEqualStrings("doc", prior.object.get("document_id").?.string);
                        } else {
                            try std.testing.expectEqual(i, prior.object.get("items").?.array.items.len);
                        }
                    } else try std.testing.expectEqual(@as(usize, 0), object.count());
                    return err;
                };
            }
            const grouped = object.get("asset").?.object;
            try std.testing.expectEqualStrings("asset_set", grouped.get("kind").?.string);
            try std.testing.expectEqual(@as(usize, 4), grouped.get("items").?.array.items.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "public graph metric borrowed wire compact embedding cursor owns values beyond transaction lifetime" {
    const a = std.testing.allocator;
    const Lite = @import("../lite/docstore.zig");
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/compact-embeddings.aflite", .{directory.sub_path});
    defer a.free(path);
    var lite = try Lite.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    defer lite.close();
    var store = try docstore_mod.DocStore.openRuntime(a, try lite.runtimeStore(a));
    defer store.close();
    const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(a, "doc", "dense");
    defer a.free(key);
    const bad = try internal_keys.embeddingArtifactKeyForDocumentAlloc(a, "doc", "invalid");
    defer a.free(bad);
    const other = try internal_keys.embeddingArtifactKeyForDocumentAlloc(a, "other", "dense");
    defer a.free(other);
    const vector = [_]f32{ 0.1, -0.0, 1.25 };
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(a, null, &vector);
    defer a.free(payload);
    try store.put(key, payload);
    try store.put(bad, "invalid artifact");
    try store.put(other, payload);
    var read = try store.beginReadTxn();
    var fields = loadEmbeddingFieldsFromTxn(a, "doc", &read) catch |err| {
        read.abort();
        return err;
    };
    read.abort();
    defer if (fields) |*value| value.deinit(a);
    try std.testing.expect(fields != null);
    try std.testing.expectEqual(@as(usize, 1), fields.?.values.count());
    try std.testing.expectEqualSlices(f32, &vector, fields.?.values.get("dense").?);
    const Fixture = struct {
        fn run(failing: Allocator, txn: *docstore_mod.DocStore.Txn) !void {
            var result = try loadEmbeddingFieldsFromTxn(failing, "doc", txn);
            defer if (result) |*value| value.deinit(failing);
        }
    };
    var pinned = try store.beginReadTxn();
    defer pinned.abort();
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{&pinned});
    const StreamFixture = struct {
        fn run(failing: Allocator, txn: *docstore_mod.DocStore.Txn) !void {
            var stream = (try openEmbeddingStreamTxn(.{ .core = undefined }, failing, "doc", txn)).?;
            defer stream.deinit();
            const selected = (try stream.getField(failing, "dense")).?;
            try std.testing.expectEqualSlices(f32, &.{ 0.1, -0.0, 1.25 }, selected);
            try std.testing.expect(try stream.getField(failing, "invalid") == null);
            try std.testing.expect(try stream.getField(failing, "missing") == null);
            var writer: std.Io.Writer.Allocating = .init(failing);
            defer writer.deinit();
            stream.write(&writer.writer) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
            const first = try failing.dupe(u8, writer.written());
            defer failing.free(first);
            const loaded = try stream.fields(failing);
            try std.testing.expectEqual(selected.ptr, loaded.values.get("dense").?.ptr);
            try std.testing.expectEqualSlices(f32, &.{ 0.1, -0.0, 1.25 }, selected);
            writer.clearRetainingCapacity();
            stream.write(&writer.writer) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
            try std.testing.expectEqualStrings(first, writer.written());
            var missing = try openEmbeddingStreamTxn(.{ .core = undefined }, failing, "absent", txn);
            defer if (missing) |*value| value.deinit();
            try std.testing.expect(missing == null);
        }
    };
    try StreamFixture.run(a, &pinned);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, StreamFixture.run, .{&pinned});
}

test "public graph metric borrowed wire streamed embedding physical loader bounds vector memory" {
    const a = std.testing.allocator;
    const Lite = @import("../lite/docstore.zig");
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/streamed-embeddings.aflite", .{directory.sub_path});
    defer a.free(path);
    var lite = try Lite.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    defer lite.close();
    var store = try docstore_mod.DocStore.openRuntime(a, try lite.runtimeStore(a));
    defer store.close();
    var vector: [1536]f32 = undefined;
    for (&vector, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i)) / 1536;
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(a, null, &vector);
    defer a.free(payload);
    for (0..64) |i| {
        const name = try std.fmt.allocPrint(a, "v{d}", .{i});
        defer a.free(name);
        const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(a, "doc", name);
        defer a.free(key);
        try store.put(key, payload);
    }
    var read = try store.beginReadTxn();
    defer read.abort();
    const Fixture = struct {
        fn none(_: ?*anyopaque, _: Allocator, _: []const u8) !?std.json.Value {
            return null;
        }
        fn open(ctx: ?*anyopaque, alloc: Allocator, key: []const u8) !?db_query_projection.EmbeddingStream {
            const txn: *docstore_mod.DocStore.Txn = @ptrCast(@alignCast(ctx.?));
            return openEmbeddingStreamTxn(.{ .core = undefined }, alloc, key, txn);
        }
        fn compact(ctx: ?*anyopaque, alloc: Allocator, key: []const u8) !?db_query_projection.EmbeddingFields {
            const txn: *docstore_mod.DocStore.Txn = @ptrCast(@alignCast(ctx.?));
            return loadEmbeddingFieldsFromTxn(alloc, key, txn);
        }
    };
    // Count operations at the production payload cursor, after its first
    // value has been primed. A whole response must not replay that value or
    // turn every subsequent field into a point seek.
    {
        const Cursor = docstore_mod.DocStore.Txn.CursorAdapter;
        const Entry = @import("../backend_erased.zig").Entry;
        const Counts = struct { seeks: usize = 0, advances: usize = 0 };
        const Trace = struct {
            inner: Cursor,
            counts: *Counts,
            pub fn close(self: *@This()) void {
                self.inner.close();
            }
            pub fn first(self: *@This()) !?Entry {
                return self.inner.first();
            }
            pub fn last(self: *@This()) !?Entry {
                return self.inner.last();
            }
            pub fn prev(self: *@This()) !?Entry {
                return self.inner.prev();
            }
            pub fn next(self: *@This()) !?Entry {
                self.counts.advances += 1;
                return self.inner.next();
            }
            pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?Entry {
                self.counts.seeks += 1;
                return self.inner.seekAtOrAfter(key);
            }
            pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?Entry {
                return self.inner.seekAtOrBefore(key);
            }
        };
        var stream = (try openEmbeddingStreamTxn(.{ .core = undefined }, a, "doc", &read)).?;
        defer stream.deinit();
        const state: *EmbeddingStreamState = @ptrCast(@alignCast(stream.ctx));
        var counts: Counts = .{};
        state.cursor = try @import("../backend_erased.zig").cursorFrom(a, Trace{ .inner = state.cursor.?, .counts = &counts });
        var writer = std.Io.Writer.Allocating.init(a);
        defer writer.deinit();
        try stream.write(&writer.writer);
        try std.testing.expectEqual(@as(usize, 0), counts.seeks);
        try std.testing.expectEqual(@as(usize, 63), counts.advances);
        const first = try a.dupe(u8, writer.written());
        defer a.free(first);
        writer.clearRetainingCapacity();
        try stream.write(&writer.writer);
        try std.testing.expectEqualStrings(first, writer.written());
        try std.testing.expectEqual(@as(usize, 1), counts.seeks);
        try std.testing.expectEqual(@as(usize, 126), counts.advances);
    }
    const Counter = @import("../lite/test_allocator.zig").BudgetAllocator;
    var expected: ?[]u8 = null;
    defer if (expected) |body| a.free(body);
    for (0..2) |mode| {
        var budget: Counter = .{ .backing = a, .limit = 1024 * 1024 };
        const start = @import("antfly_platform").time.monotonicNs();
        const body = try db_query_projection.projectLookupStoredBytesWithOutput(budget.allocator(), a, "doc", "{}", .{ .fields = &.{"_embeddings"}, .include_all_fields = false }, .{ .ctx = &read, .load_embeddings = Fixture.none, .load_chunks = Fixture.none, .load_artifacts = Fixture.none, .load_embeddings_compact = Fixture.compact, .open_embeddings = if (mode == 1) Fixture.open else null });
        const elapsed = @import("antfly_platform").time.monotonicNs() - start;
        if (mode == 0) expected = body else {
            defer a.free(body);
            try std.testing.expectEqualStrings(expected.?, body);
            try std.testing.expect(budget.peak < 64 * 1024);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        std.debug.print("PHYSICAL_EMBEDDINGS mode={d} scratch_peak={d} elapsed_ns={d} bytes={d}\n", .{ mode, budget.peak, elapsed, body.len });
    }
}

test "public graph metric borrowed wire selected embeddings retain only selected vector slabs" {
    const a = std.testing.allocator;
    const Lite = @import("../lite/docstore.zig");
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/selected-embeddings.aflite", .{directory.sub_path});
    defer a.free(path);
    var lite = try Lite.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true });
    defer lite.close();
    var store = try docstore_mod.DocStore.openRuntime(a, try lite.runtimeStore(a));
    defer store.close();
    var vector: [1536]f32 = undefined;
    for (&vector, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i)) / 1536;
    const payload = try enrichment_artifact_codec.encodeDenseEmbeddingAlloc(a, null, &vector);
    defer a.free(payload);
    for (0..64) |i| {
        const name = try std.fmt.allocPrint(a, "v{d}", .{i});
        defer a.free(name);
        const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(a, "doc", name);
        defer a.free(key);
        try store.put(key, payload);
    }
    const bad = try internal_keys.embeddingArtifactKeyForDocumentAlloc(a, "doc", "invalid");
    defer a.free(bad);
    try store.put(bad, "not a dense vector");
    var read = try store.beginReadTxn();
    defer read.abort();
    const cases = [_][]const []const u8{
        &.{"_embeddings.v63.0"},                                            &.{"_embeddings.v63"},
        &.{ "_embeddings.v63.1", "_embeddings.v0.2", "_embeddings.v63.3" }, &.{"_embeddings.invalid"},
        &.{"_embeddings.missing"},                                          &.{ "_embeddings", "_embeddings.v63.0" },
    };
    for (cases, 0..) |fields, c| {
        var peaks: [2]usize = undefined;
        var expected: ?[]u8 = null;
        defer if (expected) |body| a.free(body);
        for (0..2) |mode| {
            var budget: @import("../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 1024 * 1024 };
            var compact = if (mode == 0) try loadEmbeddingFieldsFromTxn(budget.allocator(), "doc", &read) else null;
            defer if (compact) |*owner| owner.deinit(budget.allocator());
            var stream = if (mode == 1) try openEmbeddingStreamTxn(.{ .core = undefined }, budget.allocator(), "doc", &read) else null;
            defer if (stream) |*owner| owner.deinit();
            const query = @import("document_query.zig");
            var plan = (try query.SpanProjectionPlan.init(a, .{ .fields = fields, .include_all_fields = false })).?;
            defer plan.deinit();
            const sources = [_]query.ProjectionSource{.{ .name = "_embeddings", .embeddings = if (compact) |*owner| owner else null, .embedding_stream = if (stream) |*owner| owner else null }};
            const body = (try query.lookupJsonWithSources(budget.allocator(), a, "{}", &plan, &sources)).json;
            if (mode == 0) expected = body else {
                defer a.free(body);
                try std.testing.expectEqualStrings(expected.?, body);
            }
            peaks[mode] = budget.peak;
            if (compact) |*owner| {
                owner.deinit(budget.allocator());
                compact = null;
            }
            if (stream) |*owner| {
                owner.deinit();
                stream = null;
            }
            try std.testing.expectEqual(@as(usize, 0), budget.live);
        }
        std.debug.print("SELECTED_EMBEDDINGS case={d} compact_peak={d} lazy_peak={d}\n", .{ c, peaks[0], peaks[1] });
        try std.testing.expect(peaks[1] < peaks[0] / 4);
    }
}

test "model aware media artifact projection retains root window zero" {
    const a = std.testing.allocator;
    var value = try artifactRefJsonValue(a, .{ .document_id = @constCast("doc"), .name = @constCast("clips"), .kind = .chunk, .chunk_id = 0, .window_index = 0 });
    defer freeJsonValue(a, &value);
    try std.testing.expectEqual(@as(i64, 0), value.object.get("window_index").?.integer);
}
