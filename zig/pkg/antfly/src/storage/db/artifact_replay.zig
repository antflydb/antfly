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

const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const artifact_ids = @import("artifact_ids.zig");
const mapper = @import("document_mapper.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;

pub const OwnedDenseEmbeddingWrites = struct {
    alloc: Allocator,
    owns_doc_keys: bool = false,
    writes: []mapper.DenseEmbeddingWrite = &.{},

    pub fn deinit(self: *@This()) void {
        if (self.owns_doc_keys) {
            for (self.writes) |write| {
                self.alloc.free(@constCast(write.doc_key));
                if (write.parent_doc_key) |parent_doc_key| self.alloc.free(@constCast(parent_doc_key));
            }
        }
        if (self.writes.len > 0) self.alloc.free(self.writes);
        self.* = undefined;
    }
};

pub const OwnedSparseEmbeddingWrites = struct {
    alloc: Allocator,
    owned_doc_keys: []const []const u8 = &.{},
    writes: []mapper.SparseEmbeddingWrite = &.{},

    pub fn deinit(self: *@This()) void {
        for (self.owned_doc_keys) |doc_key| self.alloc.free(@constCast(doc_key));
        if (self.owned_doc_keys.len > 0) self.alloc.free(self.owned_doc_keys);
        if (self.writes.len > 0) self.alloc.free(self.writes);
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

    return .{
        .alloc = alloc,
        .owns_doc_keys = true,
        .writes = try filtered.toOwnedSlice(alloc),
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
        var identity = artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key) catch |err| switch (err) {
            error.InvalidInternalUserKey => continue,
            else => return err,
        } orelse continue;
        defer identity.deinit(alloc);
        if (!std.mem.eql(u8, identity.embedding_name, expected_embedding_name)) continue;
        const doc_key = try alloc.dupe(u8, identity.doc_key);
        errdefer alloc.free(doc_key);
        var parent_doc_key = if (identity.parent_doc_key) |parent_key| try alloc.dupe(u8, parent_key) else null;
        errdefer if (parent_doc_key) |owned_parent| alloc.free(owned_parent);
        try out.append(alloc, .{
            .index_name = @constCast(index_name),
            .doc_key = doc_key,
            .parent_doc_key = parent_doc_key,
            .artifact_key = @constCast(artifact_key),
            .vector = &.{},
        });
        parent_doc_key = null;
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

    return .{
        .alloc = alloc,
        .owned_doc_keys = try owned_doc_keys.toOwnedSlice(alloc),
        .writes = try filtered.toOwnedSlice(alloc),
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

pub fn collectGraphMutationsForArtifacts(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    artifact_keys: []const []const u8,
    index_name: []const u8,
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
            var decoded = try enrichment_artifact_codec.decodeGraphEdgeAlloc(alloc, value);
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
