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

//! Transactional ownership transfer for public local results.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const artifact_ids = @import("artifact_ids.zig");
pub const Scan = struct {
    alloc: Allocator,
    include_documents: bool,
    hashes: std.ArrayListUnmanaged(types.ScanHash) = .empty,
    documents: std.ArrayListUnmanaged(types.ScanDocument) = .empty,

    pub fn deinit(collector: *@This()) void {
        for (collector.hashes.items) |*entry| entry.deinit(collector.alloc);
        collector.hashes.deinit(collector.alloc);
        for (collector.documents.items) |*document| document.deinit(collector.alloc);
        collector.documents.deinit(collector.alloc);
    }

    pub fn visit(raw_context: ?*anyopaque, entry: types.ScanVisitEntry) anyerror!void {
        const collector: *@This() = @ptrCast(@alignCast(raw_context orelse return error.InvalidArgument));
        try collector.hashes.ensureUnusedCapacity(collector.alloc, 1);
        if (collector.include_documents) try collector.documents.ensureUnusedCapacity(collector.alloc, 1);
        const hash_id = try collector.alloc.dupe(u8, entry.id);
        errdefer collector.alloc.free(hash_id);
        const row_cursor = if (entry.relational_cursor) |value| try collector.alloc.dupe(u8, value) else null;
        errdefer if (row_cursor) |value| collector.alloc.free(value);
        const json_null_fields = try types.cloneJsonNullFields(collector.alloc, entry.json_null_fields);
        errdefer {
            for (json_null_fields) |field| collector.alloc.free(field);
            collector.alloc.free(json_null_fields);
        }
        if (collector.include_documents) {
            const document_id = try collector.alloc.dupe(u8, entry.id);
            errdefer collector.alloc.free(document_id);
            const document_json = try collector.alloc.dupe(u8, entry.document_json orelse return error.InvalidState);
            errdefer collector.alloc.free(document_json);
            collector.documents.appendAssumeCapacity(.{ .id = document_id, .json = document_json });
        }
        collector.hashes.appendAssumeCapacity(.{
            .id = hash_id,
            .hash = entry.hash,
            .content_hash = entry.content_hash,
            .relational_schema_version = entry.relational_schema_version,
            .relational_cursor = row_cursor,
            .json_null_fields = json_null_fields,
        });
    }

    pub fn finish(self: *Scan) !types.ScanResult {
        var result: types.ScanResult = .{ .hashes = &.{}, .documents = &.{} };
        errdefer result.deinit(self.alloc);
        result.hashes = try self.hashes.toOwnedSlice(self.alloc);
        result.documents = try self.documents.toOwnedSlice(self.alloc);
        return result;
    }
};

pub fn externalizeArtifactWritesAlloc(alloc: Allocator, writes: []types.BatchWrite) ![]types.ArtifactWrite {
    var transferred: usize = 0;
    defer alloc.free(writes);
    errdefer for (writes[transferred..]) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    };
    const out = try alloc.alloc(types.ArtifactWrite, writes.len);
    errdefer {
        for (out[0..transferred]) |*write| write.deinit(alloc);
        alloc.free(out);
    }
    for (writes, 0..) |write, i| {
        var identity = try artifact_ids.resolvePublicArtifactIdentityAlloc(alloc, write.key);
        defer identity.deinit(alloc);
        const id = try alloc.dupe(u8, identity.id);
        errdefer alloc.free(id);
        const ref = try identity.artifact_ref.?.clone(alloc);
        out[i] = .{ .id = id, .value = @constCast(write.value), .artifact_ref = ref };
        transferred += 1;
        alloc.free(@constCast(write.key));
    }
    return out;
}

/// Reserve first, then transfer only a fully initialized owned value.
pub fn appendArtifact(alloc: Allocator, out: *std.ArrayListUnmanaged(types.BatchWrite), key: []const u8, value: []const u8) !void {
    try out.ensureUnusedCapacity(alloc, 1);
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_value = try alloc.dupe(u8, value);
    out.appendAssumeCapacity(.{ .key = owned_key, .value = owned_value });
}

pub fn appendDocument(alloc: Allocator, out: *std.ArrayListUnmanaged(types.EnrichmentDocumentWrite), key: []const u8, value: []const u8, names: []const []const u8) !void {
    try out.ensureUnusedCapacity(alloc, 1);
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    const owned_value = try alloc.dupe(u8, value);
    errdefer alloc.free(owned_value);
    const owned_names = try alloc.alloc([]u8, names.len);
    var initialized: usize = 0;
    errdefer {
        for (owned_names[0..initialized]) |name| alloc.free(name);
        alloc.free(owned_names);
    }
    for (names, 0..) |name, i| {
        owned_names[i] = try alloc.dupe(u8, name);
        initialized += 1;
    }
    out.appendAssumeCapacity(.{ .key = owned_key, .value = owned_value, .target_index_names = owned_names });
}

pub fn appendDenseEmbeddingForConsumers(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite),
    doc_key: []const u8,
    parent_doc_key: ?[]const u8,
    artifact_key: []const u8,
    vector: []const f32,
    consumer_indexes: []const []const u8,
) !void {
    _ = parent_doc_key;
    var identity = try artifact_ids.resolvePublicArtifactIdentityAlloc(alloc, artifact_key);
    defer identity.deinit(alloc);
    for (consumer_indexes) |name| {
        try out.ensureUnusedCapacity(alloc, 1);
        const owned_name = try alloc.dupe(u8, name);
        errdefer alloc.free(owned_name);
        const owned_doc = try alloc.dupe(u8, doc_key);
        errdefer alloc.free(owned_doc);
        const owned_id = try alloc.dupe(u8, identity.id);
        errdefer alloc.free(owned_id);
        var owned_ref = try identity.artifact_ref.?.clone(alloc);
        errdefer owned_ref.deinit(alloc);
        const owned_vector = try alloc.dupe(f32, vector);
        out.appendAssumeCapacity(.{ .index_name = owned_name, .doc_key = owned_doc, .artifact_id = owned_id, .artifact_ref = owned_ref, .vector = owned_vector });
    }
}

/// Unfinished lists stay with the caller. Adopted slices belong to this result
/// until every conversion succeeds and the complete result is returned.
pub fn finishEnrichments(alloc: Allocator, artifacts: *std.ArrayListUnmanaged(types.BatchWrite), documents: *std.ArrayListUnmanaged(types.EnrichmentDocumentWrite), dense: *std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite), failed: *std.ArrayListUnmanaged([]u8)) !types.ComputeEnrichmentsResult {
    var result: types.ComputeEnrichmentsResult = .{};
    errdefer result.deinit(alloc);
    result.artifact_writes = try externalizeArtifactWritesAlloc(alloc, try artifacts.toOwnedSlice(alloc));
    result.documents = try documents.toOwnedSlice(alloc);
    result.dense_embeddings = try dense.toOwnedSlice(alloc);
    result.failed_keys = try failed.toOwnedSlice(alloc);
    return result;
}

pub const Enrichments = struct {
    alloc: Allocator,
    artifacts: std.ArrayListUnmanaged(types.BatchWrite) = .empty,
    documents: std.ArrayListUnmanaged(types.EnrichmentDocumentWrite) = .empty,
    dense: std.ArrayListUnmanaged(types.EnrichmentDenseEmbeddingWrite) = .empty,
    failed: std.ArrayListUnmanaged([]u8) = .empty,
    pub fn deinit(self: *Enrichments) void {
        for (self.artifacts.items) |write| {
            self.alloc.free(@constCast(write.key));
            self.alloc.free(@constCast(write.value));
        }
        self.artifacts.deinit(self.alloc);
        for (self.documents.items) |*doc| doc.deinit(self.alloc);
        self.documents.deinit(self.alloc);
        for (self.dense.items) |*embedding| embedding.deinit(self.alloc);
        self.dense.deinit(self.alloc);
        for (self.failed.items) |key| self.alloc.free(key);
        self.failed.deinit(self.alloc);
    }
    pub fn finish(self: *Enrichments) !types.ComputeEnrichmentsResult {
        return finishEnrichments(self.alloc, &self.artifacts, &self.documents, &self.dense, &self.failed);
    }
};

test "result collectors release scan rows across every allocation failure" {
    const F = struct {
        fn run(alloc: Allocator) !void {
            var scan: Scan = .{ .alloc = alloc, .include_documents = true };
            defer scan.deinit();
            for (0..3) |_| try Scan.visit(&scan, .{ .id = "doc", .hash = 7, .relational_cursor = "cursor", .json_null_fields = &.{"field"}, .document_json = "{\"field\":null}" });
            var result = try scan.finish();
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 3), result.hashes.len);
            try std.testing.expectEqualStrings("cursor", result.hashes[0].relational_cursor.?);
            try std.testing.expectEqual(@as(usize, 3), result.documents.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}

test "result collectors adopt enrichment arrays atomically across every allocation failure" {
    const F = struct {
        fn run(alloc: Allocator) !void {
            var collector: Enrichments = .{ .alloc = alloc };
            defer collector.deinit();
            for (0..3) |_| {
                const key = try @import("../internal_keys.zig").artifactNamedPrefixAlloc(alloc, "doc", "asset", "caption");
                defer alloc.free(key);
                try appendArtifact(alloc, &collector.artifacts, key, "caption");
            }
            try appendDocument(alloc, &collector.documents, "doc", "{}", &.{ "text", "other" });
            const artifact_key = try @import("../internal_keys.zig").artifactNamedPrefixAlloc(alloc, "doc", "asset", "caption");
            defer alloc.free(artifact_key);
            try appendDenseEmbeddingForConsumers(alloc, &collector.dense, "doc", null, artifact_key, &.{ 1, 2, 3 }, &.{"dense"});
            try collector.failed.ensureUnusedCapacity(alloc, 1);
            collector.failed.appendAssumeCapacity(try alloc.dupe(u8, "failed"));
            var result = try collector.finish();
            defer result.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 3), result.artifact_writes.len);
            try std.testing.expectEqualStrings("caption", result.artifact_writes[0].value);
            try std.testing.expectEqual(@as(usize, 1), result.documents.len);
            try std.testing.expectEqual(@as(usize, 1), result.dense_embeddings.len);
            try std.testing.expectEqual(@as(usize, 1), result.failed_keys.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}
