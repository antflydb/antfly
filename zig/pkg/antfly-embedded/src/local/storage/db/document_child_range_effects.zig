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
const Allocator = std.mem.Allocator;
const types = @import("types.zig");

const artifact_ids = @import("artifact_ids.zig");
const derived_types = @import("derived/derived_types.zig");
const enrichment_types = @import("enrichment/enrichment_types.zig");
const DocumentArtifactChildRangeDispatch = @import("document_child_range_outbox.zig").DocumentArtifactChildRangeDispatch;
const manifest = @import("document_child_range_manifest.zig");
const documentArtifactChildRangesFromManifestJsonAlloc = manifest.documentArtifactChildRangesFromManifestJsonAlloc;
const freeDocumentArtifactChildRanges = manifest.freeDocumentArtifactChildRanges;

/// The caller owns the read fence. Planning cannot perform server coordination.
pub const Reader = struct {
    ptr: *anyopaque,
    get: *const fn (*anyopaque, Allocator, []const u8) anyerror!?[]u8,
};
pub const Selector = struct {
    ptr: *anyopaque,
    select: *const fn (*anyopaque, types.DocumentArtifactChildRange) ?u64,
};

fn decodeArtifactRefIfKnownAlloc(alloc: Allocator, key: []const u8) !?types.ArtifactRef {
    return artifact_ids.decodeArtifactRefAlloc(alloc, key) catch |err| switch (err) {
        error.InvalidInternalUserKey => null,
        else => return err,
    };
}

pub const DocumentChildRangeRoutingSnapshot = struct {
    doc_key: []u8,
    manifest_artifact_name: []u8,
    child_ranges: []types.DocumentArtifactChildRange,

    pub fn deinit(self: *DocumentChildRangeRoutingSnapshot, alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.manifest_artifact_name);
        freeDocumentArtifactChildRanges(alloc, self.child_ranges);
        self.* = undefined;
    }
};

pub const DocumentChildRangeDispatchGroup = struct {
    owner_group_id: u64,
    doc_key: []u8,
    artifact_name: []u8,
    artifact_writes: std.ArrayListUnmanaged(types.BatchWrite) = .empty,
    artifact_delete_keys: std.ArrayListUnmanaged([]const u8) = .empty,
    documents: std.ArrayListUnmanaged(derived_types.DerivedDocument) = .empty,
    dense_embeddings: std.ArrayListUnmanaged(derived_types.DerivedDenseEmbeddingWrite) = .empty,
    sparse_embeddings: std.ArrayListUnmanaged(derived_types.DerivedSparseEmbeddingWrite) = .empty,
    generated_enrichment_refs: std.ArrayListUnmanaged(enrichment_types.GeneratedEnrichmentRef) = .empty,

    pub fn deinit(self: *DocumentChildRangeDispatchGroup, alloc: Allocator) void {
        alloc.free(self.doc_key);
        alloc.free(self.artifact_name);
        for (self.artifact_writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        self.artifact_writes.deinit(alloc);
        for (self.artifact_delete_keys.items) |key| alloc.free(@constCast(key));
        self.artifact_delete_keys.deinit(alloc);
        for (self.documents.items) |doc| derived_types.deinitDerivedDocument(alloc, doc);
        self.documents.deinit(alloc);
        for (self.dense_embeddings.items) |embedding|
            derived_types.deinitDerivedDenseEmbedding(alloc, embedding);
        self.dense_embeddings.deinit(alloc);
        for (self.sparse_embeddings.items) |embedding|
            derived_types.deinitDerivedSparseEmbedding(alloc, embedding);
        self.sparse_embeddings.deinit(alloc);
        for (self.generated_enrichment_refs.items) |request|
            enrichment_types.freeGeneratedRef(alloc, request);
        self.generated_enrichment_refs.deinit(alloc);
        self.* = undefined;
    }

    pub fn dispatch(self: DocumentChildRangeDispatchGroup, sync_level: types.SyncLevel) DocumentArtifactChildRangeDispatch {
        return .{
            .owner_group_id = self.owner_group_id,
            .doc_key = self.doc_key,
            .artifact_name = self.artifact_name,
            .child_batch = .{
                .artifact_writes = self.artifact_writes.items,
                .artifact_delete_keys = self.artifact_delete_keys.items,
                .documents = self.documents.items,
                .dense_embeddings = self.dense_embeddings.items,
                .sparse_embeddings = self.sparse_embeddings.items,
                .generated_enrichment_refs = self.generated_enrichment_refs.items,
                .sync_level = sync_level,
            },
        };
    }
};

pub const DocumentChildRangeRoute = struct {
    owner_group_id: u64,
    doc_key: []const u8,
    artifact_name: []const u8,
};

/// Partition the generated batch facet without borrowing a DB wrapper. All
/// owned slices and dispatch groups use the preparation allocator. The caller
/// retains the manifest/read fence through planning and atomic intent staging.
pub fn partitionRemoteDocumentChildRangeGeneratedBatch(
    reader: Reader,
    alloc: Allocator,
    generated: anytype,
    out: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
    selector: Selector,
) !void {
    var snapshots = std.ArrayListUnmanaged(DocumentChildRangeRoutingSnapshot).empty;
    defer {
        for (snapshots.items) |*snapshot| snapshot.deinit(alloc);
        snapshots.deinit(alloc);
    }
    try collectDocumentChildRangeRoutingSnapshots(reader, alloc, generated.*, &snapshots);
    if (snapshots.items.len == 0) return;

    try partitionRemoteArtifactWrites(alloc, snapshots.items, selector, &generated.artifact_writes, out);
    try partitionRemoteArtifactDeletes(alloc, snapshots.items, selector, &generated.artifact_delete_keys, out);
    try partitionRemoteDerivedDocuments(alloc, snapshots.items, selector, &generated.documents, out);
    try partitionRemoteDenseEmbeddings(alloc, snapshots.items, selector, &generated.dense_embeddings, out);
    try partitionRemoteSparseEmbeddings(alloc, snapshots.items, selector, &generated.sparse_embeddings, out);
}

pub fn collectDocumentChildRangeRoutingSnapshots(
    reader: Reader,
    alloc: Allocator,
    generated: anytype,
    out: *std.ArrayListUnmanaged(DocumentChildRangeRoutingSnapshot),
) !void {
    for (generated.artifact_writes) |write| {
        try appendDocumentChildRangeRoutingSnapshotFromValue(alloc, write.key, write.value, out);
    }
    for (generated.artifact_delete_keys) |key| {
        var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, key)) orelse continue;
        defer artifact_ref.deinit(alloc);
        if (artifact_ref.kind != .asset or artifact_ref.unit_id != null) continue;
        const existing = reader.get(reader.ptr, alloc, key) catch |err| switch (err) {
            error.NotFound => continue,
            else => return err,
        };
        defer if (existing) |value| alloc.free(value);
        if (existing) |value| try appendDocumentChildRangeRoutingSnapshotFromValue(alloc, key, value, out);
    }
}

pub fn appendDocumentChildRangeRoutingSnapshotFromValue(
    alloc: Allocator,
    key: []const u8,
    value: []const u8,
    out: *std.ArrayListUnmanaged(DocumentChildRangeRoutingSnapshot),
) !void {
    if (std.mem.indexOf(u8, value, "\"child_ranges\"") == null) return;
    var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, key)) orelse return;
    defer artifact_ref.deinit(alloc);
    if (artifact_ref.kind != .asset or artifact_ref.unit_id != null) return;

    const ranges = documentArtifactChildRangesFromManifestJsonAlloc(alloc, value) catch |err| switch (err) {
        error.InvalidDocumentExtractionManifest => return,
        else => return err,
    };
    errdefer freeDocumentArtifactChildRanges(alloc, ranges);
    if (ranges.len == 0) {
        freeDocumentArtifactChildRanges(alloc, ranges);
        return;
    }
    const doc_key = try alloc.dupe(u8, artifact_ref.document_id);
    errdefer alloc.free(doc_key);
    const manifest_artifact_name = try alloc.dupe(u8, artifact_ref.name);
    errdefer alloc.free(manifest_artifact_name);
    try out.append(alloc, .{
        .doc_key = doc_key,
        .manifest_artifact_name = manifest_artifact_name,
        .child_ranges = ranges,
    });
}

pub fn documentChildRangeRouteForKey(
    alloc: Allocator,
    snapshots: []const DocumentChildRangeRoutingSnapshot,
    selector: Selector,
    key: []const u8,
) !?DocumentChildRangeRoute {
    var artifact_ref = (try decodeArtifactRefIfKnownAlloc(alloc, key)) orelse return null;
    defer artifact_ref.deinit(alloc);

    const route_kind, const route_artifact_name = switch (artifact_ref.kind) {
        .asset => blk: {
            if (artifact_ref.unit_id == null) return null;
            break :blk .{ "unit", artifact_ref.name };
        },
        .chunk => .{ "chunk", artifact_ref.name },
        .embedding => blk: {
            const source = artifact_ref.source orelse return null;
            break :blk .{
                if (source.kind == .chunk) "chunk" else "unit",
                source.name,
            };
        },
    };

    for (snapshots) |snapshot| {
        if (!std.mem.eql(u8, snapshot.doc_key, artifact_ref.document_id)) continue;
        for (snapshot.child_ranges) |range| {
            if (!std.mem.eql(u8, range.range_kind, route_kind)) continue;
            if (!std.mem.eql(u8, range.artifact_name, route_artifact_name)) continue;
            if (!keyWithinDocumentChildRange(key, range)) continue;
            const owner_group_id = selector.select(selector.ptr, range) orelse continue;
            return .{
                .owner_group_id = owner_group_id,
                .doc_key = snapshot.doc_key,
                .artifact_name = range.artifact_name,
            };
        }
    }
    return null;
}

pub fn keyWithinDocumentChildRange(key: []const u8, range: types.DocumentArtifactChildRange) bool {
    if (std.mem.order(u8, key, range.start_key) == .lt) return false;
    if (range.end_key_exclusive.len == 0) return true;
    return std.mem.order(u8, key, range.end_key_exclusive) == .lt;
}

const local_document_child_range_destination = std.math.maxInt(usize);

pub fn ensureDocumentChildRangeDispatchGroupIndex(
    alloc: Allocator,
    groups: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
    route: DocumentChildRangeRoute,
) !usize {
    for (groups.items, 0..) |group, i| {
        if (group.owner_group_id == route.owner_group_id and
            std.mem.eql(u8, group.doc_key, route.doc_key) and
            std.mem.eql(u8, group.artifact_name, route.artifact_name))
        {
            return i;
        }
    }
    const doc_key = try alloc.dupe(u8, route.doc_key);
    errdefer alloc.free(doc_key);
    const artifact_name = try alloc.dupe(u8, route.artifact_name);
    errdefer alloc.free(artifact_name);
    try groups.append(alloc, .{
        .owner_group_id = route.owner_group_id,
        .doc_key = doc_key,
        .artifact_name = artifact_name,
    });
    return groups.items.len - 1;
}

pub fn countDocumentChildRangeDestinations(
    alloc: Allocator,
    destinations: []const usize,
    group_count: usize,
) !struct { local: usize, groups: []usize } {
    const group_counts = try alloc.alloc(usize, group_count);
    @memset(group_counts, 0);
    var local_count: usize = 0;
    for (destinations) |destination| {
        if (destination == local_document_child_range_destination) {
            local_count += 1;
        } else {
            group_counts[destination] += 1;
        }
    }
    return .{ .local = local_count, .groups = group_counts };
}

pub fn partitionRemoteArtifactWrites(
    alloc: Allocator,
    snapshots: []const DocumentChildRangeRoutingSnapshot,
    selector: Selector,
    writes: *[]types.BatchWrite,
    groups: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
) !void {
    if (writes.*.len == 0) return;
    const destinations = try alloc.alloc(usize, writes.*.len);
    defer if (destinations.len > 0) alloc.free(destinations);
    for (writes.*, 0..) |write, i| {
        destinations[i] = if (try documentChildRangeRouteForKey(alloc, snapshots, selector, write.key)) |route|
            try ensureDocumentChildRangeDispatchGroupIndex(alloc, groups, route)
        else
            local_document_child_range_destination;
    }

    const counts = try countDocumentChildRangeDestinations(alloc, destinations, groups.items.len);
    defer if (counts.groups.len > 0) alloc.free(counts.groups);
    const local = try alloc.alloc(types.BatchWrite, counts.local);
    errdefer if (local.len > 0) alloc.free(local);
    for (groups.items, counts.groups) |*group, count| {
        try group.artifact_writes.ensureUnusedCapacity(alloc, count);
    }

    var local_i: usize = 0;
    for (writes.*, destinations) |write, destination| {
        if (destination == local_document_child_range_destination) {
            local[local_i] = write;
            local_i += 1;
        } else {
            groups.items[destination].artifact_writes.appendAssumeCapacity(write);
        }
    }
    if (writes.*.len > 0) alloc.free(writes.*);
    writes.* = local;
}

pub fn partitionRemoteArtifactDeletes(
    alloc: Allocator,
    snapshots: []const DocumentChildRangeRoutingSnapshot,
    selector: Selector,
    keys: *[]const []const u8,
    groups: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
) !void {
    if (keys.*.len == 0) return;
    const destinations = try alloc.alloc(usize, keys.*.len);
    defer if (destinations.len > 0) alloc.free(destinations);
    for (keys.*, 0..) |key, i| {
        destinations[i] = if (try documentChildRangeRouteForKey(alloc, snapshots, selector, key)) |route|
            try ensureDocumentChildRangeDispatchGroupIndex(alloc, groups, route)
        else
            local_document_child_range_destination;
    }

    const counts = try countDocumentChildRangeDestinations(alloc, destinations, groups.items.len);
    defer if (counts.groups.len > 0) alloc.free(counts.groups);
    const local = try alloc.alloc([]const u8, counts.local);
    errdefer if (local.len > 0) alloc.free(local);
    for (groups.items, counts.groups) |*group, count| {
        try group.artifact_delete_keys.ensureUnusedCapacity(alloc, count);
    }

    var local_i: usize = 0;
    for (keys.*, destinations) |key, destination| {
        if (destination == local_document_child_range_destination) {
            local[local_i] = key;
            local_i += 1;
        } else {
            groups.items[destination].artifact_delete_keys.appendAssumeCapacity(key);
        }
    }
    if (keys.*.len > 0) alloc.free(keys.*);
    keys.* = local;
}

pub fn partitionRemoteDerivedDocuments(
    alloc: Allocator,
    snapshots: []const DocumentChildRangeRoutingSnapshot,
    selector: Selector,
    documents: *[]const derived_types.DerivedDocument,
    groups: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
) !void {
    if (documents.*.len == 0) return;
    const destinations = try alloc.alloc(usize, documents.*.len);
    defer if (destinations.len > 0) alloc.free(destinations);
    for (documents.*, 0..) |doc, i| {
        destinations[i] = if (try documentChildRangeRouteForKey(alloc, snapshots, selector, doc.key)) |route|
            try ensureDocumentChildRangeDispatchGroupIndex(alloc, groups, route)
        else
            local_document_child_range_destination;
    }

    const counts = try countDocumentChildRangeDestinations(alloc, destinations, groups.items.len);
    defer if (counts.groups.len > 0) alloc.free(counts.groups);
    const local = try alloc.alloc(derived_types.DerivedDocument, counts.local);
    errdefer if (local.len > 0) alloc.free(local);
    for (groups.items, counts.groups) |*group, count| {
        try group.documents.ensureUnusedCapacity(alloc, count);
    }

    var local_i: usize = 0;
    for (documents.*, destinations) |doc, destination| {
        if (destination == local_document_child_range_destination) {
            local[local_i] = doc;
            local_i += 1;
        } else {
            groups.items[destination].documents.appendAssumeCapacity(doc);
        }
    }
    if (documents.*.len > 0) alloc.free(documents.*);
    documents.* = local;
}

pub fn partitionRemoteDenseEmbeddings(
    alloc: Allocator,
    snapshots: []const DocumentChildRangeRoutingSnapshot,
    selector: Selector,
    embeddings: *[]const derived_types.DerivedDenseEmbeddingWrite,
    groups: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
) !void {
    if (embeddings.*.len == 0) return;
    const destinations = try alloc.alloc(usize, embeddings.*.len);
    defer if (destinations.len > 0) alloc.free(destinations);
    for (embeddings.*, 0..) |embedding, i| {
        const route_key = embedding.artifact_key orelse embedding.doc_key;
        destinations[i] = if (try documentChildRangeRouteForKey(alloc, snapshots, selector, route_key)) |route|
            try ensureDocumentChildRangeDispatchGroupIndex(alloc, groups, route)
        else
            local_document_child_range_destination;
    }

    const counts = try countDocumentChildRangeDestinations(alloc, destinations, groups.items.len);
    defer if (counts.groups.len > 0) alloc.free(counts.groups);
    const local = try alloc.alloc(derived_types.DerivedDenseEmbeddingWrite, counts.local);
    errdefer if (local.len > 0) alloc.free(local);
    for (groups.items, counts.groups) |*group, count| {
        try group.dense_embeddings.ensureUnusedCapacity(alloc, count);
    }

    var local_i: usize = 0;
    for (embeddings.*, destinations) |embedding, destination| {
        if (destination == local_document_child_range_destination) {
            local[local_i] = embedding;
            local_i += 1;
        } else {
            groups.items[destination].dense_embeddings.appendAssumeCapacity(embedding);
        }
    }
    if (embeddings.*.len > 0) alloc.free(embeddings.*);
    embeddings.* = local;
}

pub fn partitionRemoteSparseEmbeddings(
    alloc: Allocator,
    snapshots: []const DocumentChildRangeRoutingSnapshot,
    selector: Selector,
    embeddings: *[]const derived_types.DerivedSparseEmbeddingWrite,
    groups: *std.ArrayListUnmanaged(DocumentChildRangeDispatchGroup),
) !void {
    if (embeddings.*.len == 0) return;
    const destinations = try alloc.alloc(usize, embeddings.*.len);
    defer if (destinations.len > 0) alloc.free(destinations);
    for (embeddings.*, 0..) |embedding, i| {
        const route_key = embedding.artifact_key orelse embedding.doc_key;
        destinations[i] = if (try documentChildRangeRouteForKey(alloc, snapshots, selector, route_key)) |route|
            try ensureDocumentChildRangeDispatchGroupIndex(alloc, groups, route)
        else
            local_document_child_range_destination;
    }

    const counts = try countDocumentChildRangeDestinations(alloc, destinations, groups.items.len);
    defer if (counts.groups.len > 0) alloc.free(counts.groups);
    const local = try alloc.alloc(derived_types.DerivedSparseEmbeddingWrite, counts.local);
    errdefer if (local.len > 0) alloc.free(local);
    for (groups.items, counts.groups) |*group, count| {
        try group.sparse_embeddings.ensureUnusedCapacity(alloc, count);
    }

    var local_i: usize = 0;
    for (embeddings.*, destinations) |embedding, destination| {
        if (destination == local_document_child_range_destination) {
            local[local_i] = embedding;
            local_i += 1;
        } else {
            groups.items[destination].sparse_embeddings.appendAssumeCapacity(embedding);
        }
    }
    if (embeddings.*.len > 0) alloc.free(embeddings.*);
    embeddings.* = local;
}

test "child range planning uses caller destination policy and retains local key bounds" {
    const alloc = std.testing.allocator;
    const key = try @import("../internal_keys.zig").documentUnitArtifactKeyAlloc(alloc, "doc", "units", "page:1");
    defer alloc.free(key);
    var range: types.DocumentArtifactChildRange = .{
        .range_id = @constCast("r"),
        .range_kind = @constCast("unit"),
        .artifact_name = @constCast("units"),
        .split_boundary = @constCast("unit"),
        .placement = @constCast("local"),
        .start_key = @constCast(key),
        .end_key_exclusive = @constCast(""),
        .last_key = @constCast(key),
    };
    var snapshot: DocumentChildRangeRoutingSnapshot = .{ .doc_key = @constCast("doc"), .manifest_artifact_name = @constCast("units"), .child_ranges = @as(*[1]types.DocumentArtifactChildRange, @ptrCast(&range)) };
    const Policy = struct {
        fn select(ptr: *anyopaque, _: types.DocumentArtifactChildRange) ?u64 {
            return (@as(*u64, @ptrCast(@alignCast(ptr)))).*;
        }
    };
    var destination: u64 = 99;
    const selector: Selector = .{ .ptr = &destination, .select = Policy.select };
    const route = (try documentChildRangeRouteForKey(alloc, &.{snapshot}, selector, key)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(destination, route.owner_group_id);
    // A caller can choose a destination even without persisted server tags;
    // the planner still refuses effects outside the local manifest range.
    range.start_key = @constCast("~");
    snapshot.child_ranges = @as(*[1]types.DocumentArtifactChildRange, @ptrCast(&range));
    try std.testing.expect((try documentChildRangeRouteForKey(alloc, &.{snapshot}, selector, key)) == null);
}
