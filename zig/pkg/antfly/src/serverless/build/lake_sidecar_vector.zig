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

//! Lake-native dense vector sidecar builders over RowSource batches.

const std = @import("std");
const Allocator = std.mem.Allocator;
const shared_vector = @import("antfly_vector").vector;
const artifact_ref = @import("../manifest/artifact_ref.zig");
const artifact_store = @import("../artifacts/store.zig");
const document_projection = @import("../document_projection.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const vector_index = @import("vector_index.zig");
const vector_segment = @import("../vector_segment/mod.zig");
const external_rowsource = @import("../../storage/rowsource/external.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const lake_build_limits = @import("lake_build_limits.zig");

pub const VectorSidecarBuildOptions = struct {
    name: []const u8,
    vector_column: []const u8,
    metric: shared_vector.DistanceMetric = .cosine,
    policy: vector_index.BuildPolicy = .{},
    embedding_name: ?[]const u8 = null,
    artifact_id: []const u8 = &.{},
    limits: lake_build_limits.Limits = .{},
};

pub const VectorSidecarBuildResult = struct {
    payload: []u8,
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *VectorSidecarBuildResult, alloc: Allocator) void {
        alloc.free(self.payload);
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub const VectorSidecarPublishResult = struct {
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *VectorSidecarPublishResult, alloc: Allocator) void {
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub fn buildVectorSidecarFromRowSourceAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: VectorSidecarBuildOptions,
) !VectorSidecarBuildResult {
    var working_set = try lake_build_limits.WorkingSetAllocator.init(alloc, options.limits);
    return buildVectorSidecarBoundedAlloc(working_set.allocator(), source, binding, options) catch |err| {
        if (err == error.OutOfMemory and working_set.limit_exceeded) return error.LakeSidecarBuildBudgetExceeded;
        return err;
    };
}

fn buildVectorSidecarBoundedAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: VectorSidecarBuildOptions,
) !VectorSidecarBuildResult {
    try validateOptions(binding, source.kind, options);
    var budget = try lake_build_limits.Budget.init(options.limits);

    var entries = std.ArrayListUnmanaged(vector_segment.Entry).empty;
    var entries_owned_by_list = true;
    errdefer if (entries_owned_by_list) {
        freeVectorEntries(alloc, entries.items);
        entries.deinit(alloc);
    };
    var dims: ?u32 = null;

    while (try source.next(alloc)) |batch| {
        try budget.admitBatch(batch);
        try sidecar_manifest.validateBatchAgainstDeclaredArtifact(.{
            .name = options.name,
            .binding = binding,
            .artifact = .{
                .kind = .vector_segment,
                .name = options.name,
                .artifact_id = "pending",
                .byte_len = 1,
                .checksum = "pending",
            },
        }, batch);

        const column = batch.findColumn(options.vector_column).?;
        try appendBatchVectors(alloc, &entries, &dims, batch, column, options.embedding_name);
        try budget.checkRetainedItems(entries.items.len);
    }

    if (entries.items.len == 0 or dims == null) return error.EmptyLakeSidecarVectorSegment;

    const entry_slice = try entries.toOwnedSlice(alloc);
    entries_owned_by_list = false;

    var segment = try vector_index.buildClusteredSegmentWithPolicyAlloc(
        alloc,
        options.metric,
        dims.?,
        entry_slice,
        options.policy,
    );
    defer vector_segment.freeSegment(alloc, &segment);

    const payload = try vector_segment.encodeAlloc(alloc, segment);
    errdefer alloc.free(payload);
    try budget.checkOutputBytes(payload.len);

    var declaration = try declaredArtifactAlloc(alloc, binding, options, payload.len);
    errdefer freeOwnedDeclaration(alloc, declaration);
    try declaration.validate();

    return .{
        .payload = payload,
        .declaration = declaration,
    };
}

pub fn publishVectorSidecarFromRowSourceAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: VectorSidecarBuildOptions,
) !VectorSidecarPublishResult {
    var built = try buildVectorSidecarFromRowSourceAlloc(alloc, source, binding, options);
    defer alloc.free(built.payload);
    errdefer freeOwnedDeclaration(alloc, built.declaration);

    var metadata = try artifacts.put(built.payload);
    var metadata_owned = true;
    errdefer if (metadata_owned) metadata.deinit(alloc);

    alloc.free(built.declaration.artifact.artifact_id);
    alloc.free(built.declaration.artifact.checksum);
    built.declaration.artifact.artifact_id = metadata.artifact_id;
    built.declaration.artifact.byte_len = metadata.byte_len;
    built.declaration.artifact.checksum = metadata.checksum;
    metadata_owned = false;

    try built.declaration.validate();
    return .{ .declaration = built.declaration };
}

fn validateOptions(
    binding: source_binding.Binding,
    source_kind: rowsource.SourceKind,
    options: VectorSidecarBuildOptions,
) !void {
    try binding.validate();
    if (binding.sidecar_kind != .vector) return error.InvalidLakeSidecarVectorBuildOptions;
    if (source_kind != binding.source_kind) return error.SidecarSourceBindingMismatch;
    if (options.name.len == 0 or options.vector_column.len == 0) {
        return error.InvalidLakeSidecarVectorBuildOptions;
    }
    if (binding.column_bindings.len != 1) return error.InvalidLakeSidecarVectorBuildOptions;
    if (!std.mem.eql(u8, binding.column_bindings[0], options.vector_column)) {
        return error.SidecarSourceBindingMismatch;
    }
}

fn appendBatchVectors(
    alloc: Allocator,
    entries: *std.ArrayListUnmanaged(vector_segment.Entry),
    dims: *?u32,
    batch: rowsource.ColumnBatch,
    column: rowsource.ColumnVector,
    embedding_name: ?[]const u8,
) !void {
    switch (column.values) {
        .vector_f32 => |values| {
            for (values, 0..) |value, row| {
                if (column.nulls.isNull(row)) continue;
                try appendVectorEntry(alloc, entries, dims, batch.row_refs[row], value);
            }
        },
        .bytes => |values| {
            for (values, 0..) |value, row| {
                if (column.nulls.isNull(row)) continue;
                try appendJsonVectorEntry(alloc, entries, dims, batch.row_refs[row], value, embedding_name);
            }
        },
        .json => |values| {
            for (values, 0..) |value, row| {
                if (column.nulls.isNull(row)) continue;
                try appendJsonVectorEntry(alloc, entries, dims, batch.row_refs[row], value, embedding_name);
            }
        },
        else => return error.UnsupportedLakeSidecarVectorColumn,
    }
}

fn appendJsonVectorEntry(
    alloc: Allocator,
    entries: *std.ArrayListUnmanaged(vector_segment.Entry),
    dims: *?u32,
    row_ref: rowsource.RowRef,
    source_value: []const u8,
    embedding_name: ?[]const u8,
) !void {
    if (try appendProjectionVectorEntry(alloc, entries, dims, row_ref, source_value, embedding_name)) return;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source_value, .{});
    defer parsed.deinit();
    const vector = try jsonArrayVectorAlloc(alloc, parsed.value);
    defer alloc.free(vector);
    try appendVectorEntry(alloc, entries, dims, row_ref, vector);
}

fn appendProjectionVectorEntry(
    alloc: Allocator,
    entries: *std.ArrayListUnmanaged(vector_segment.Entry),
    dims: *?u32,
    row_ref: rowsource.RowRef,
    source_value: []const u8,
    embedding_name: ?[]const u8,
) !bool {
    var projection = document_projection.parseAlloc(alloc, source_value) catch return false;
    defer projection.deinit(alloc);
    const vector = if (embedding_name) |name|
        projection.findNamedEmbedding(name) orelse return false
    else
        projection.embedding orelse return false;
    try appendVectorEntry(alloc, entries, dims, row_ref, vector);
    return true;
}

fn appendVectorEntry(
    alloc: Allocator,
    entries: *std.ArrayListUnmanaged(vector_segment.Entry),
    dims: *?u32,
    row_ref: rowsource.RowRef,
    vector: []const f32,
) !void {
    if (vector.len == 0) return error.InvalidLakeSidecarVectorFeatures;
    if (vector.len > std.math.maxInt(u32)) return error.LakeSidecarVectorDimsTooLarge;
    const vector_dims: u32 = @intCast(vector.len);
    if (dims.*) |existing| {
        if (existing != vector_dims) return error.InconsistentVectorDims;
    } else {
        dims.* = vector_dims;
    }

    const doc_id = try source_binding.rowRefKeyAlloc(alloc, row_ref);
    errdefer alloc.free(doc_id);
    const owned_vector = try alloc.dupe(f32, vector);
    errdefer alloc.free(owned_vector);
    try entries.append(alloc, .{
        .doc_id = doc_id,
        .vector = owned_vector,
    });
}

fn jsonArrayVectorAlloc(alloc: Allocator, value: std.json.Value) ![]f32 {
    if (value != .array) return error.InvalidLakeSidecarVectorFeatures;
    if (value.array.items.len == 0) return error.InvalidLakeSidecarVectorFeatures;
    const vector = try alloc.alloc(f32, value.array.items.len);
    errdefer alloc.free(vector);
    for (value.array.items, 0..) |item, idx| {
        vector[idx] = try jsonValueAsF32(item);
    }
    return vector;
}

fn jsonValueAsF32(value: std.json.Value) !f32 {
    return switch (value) {
        .float => @floatCast(value.float),
        .integer => @floatFromInt(value.integer),
        .number_string => try std.fmt.parseFloat(f32, value.number_string),
        else => error.InvalidLakeSidecarVectorFeatures,
    };
}

fn declaredArtifactAlloc(
    alloc: Allocator,
    binding: source_binding.Binding,
    options: VectorSidecarBuildOptions,
    payload_len: usize,
) !sidecar_manifest.DeclaredArtifact {
    const name = try alloc.dupe(u8, options.name);
    errdefer alloc.free(name);
    const artifact_name = try alloc.dupe(u8, options.name);
    errdefer alloc.free(artifact_name);
    const artifact_id = if (options.artifact_id.len == 0)
        try std.fmt.allocPrint(
            alloc,
            "lake-vector:{d}:{s}:{d}:{s}:{d}",
            .{ options.name.len, options.name, binding.snapshot_id.len, binding.snapshot_id, payload_len },
        )
    else
        try alloc.dupe(u8, options.artifact_id);
    errdefer alloc.free(artifact_id);
    const checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{payload_len});
    errdefer alloc.free(checksum);
    const owned_binding = try cloneBindingAlloc(alloc, binding);
    errdefer freeOwnedBinding(alloc, owned_binding);

    return .{
        .name = name,
        .binding = owned_binding,
        .artifact = artifact_ref.ArtifactRef{
            .kind = .vector_segment,
            .name = artifact_name,
            .artifact_id = artifact_id,
            .byte_len = @intCast(payload_len),
            .checksum = checksum,
        },
    };
}

fn cloneBindingAlloc(alloc: Allocator, binding: source_binding.Binding) !source_binding.Binding {
    return try source_binding.cloneAlloc(alloc, binding);
}

fn freeOwnedDeclaration(alloc: Allocator, declaration: sidecar_manifest.DeclaredArtifact) void {
    alloc.free(declaration.name);
    freeOwnedBinding(alloc, declaration.binding);
    if (declaration.artifact.name.len > 0) alloc.free(declaration.artifact.name);
    alloc.free(declaration.artifact.artifact_id);
    alloc.free(declaration.artifact.checksum);
}

fn freeOwnedBinding(alloc: Allocator, binding: source_binding.Binding) void {
    source_binding.freeOwned(alloc, binding);
}

fn freeVectorEntries(alloc: Allocator, entries: []vector_segment.Entry) void {
    for (entries) |*entry| entry.deinit(alloc);
}

const MemoryArtifactStore = struct {
    alloc: Allocator,
    bytes: ?[]u8 = null,

    fn init(alloc: Allocator) MemoryArtifactStore {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *MemoryArtifactStore) void {
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }

    fn artifactStore(self: *MemoryArtifactStore) artifact_store.ArtifactStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn put(self: *MemoryArtifactStore, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.bytes = try self.alloc.dupe(u8, contents);
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:vector-sidecar"),
            .byte_len = @intCast(contents.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{contents.len}),
        };
    }

    fn getAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        if (!std.mem.eql(u8, artifact_id, "mem:vector-sidecar")) return error.ArtifactNotFound;
        const bytes = self.bytes orelse return error.ArtifactNotFound;
        return try alloc.dupe(u8, bytes);
    }

    fn getRangeAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const bytes = try self.getAlloc(alloc, artifact_id);
        defer alloc.free(bytes);
        if (offset > bytes.len) return error.InvalidRange;
        const start: usize = @intCast(offset);
        const end = @min(bytes.len, start + len);
        return try alloc.dupe(u8, bytes[start..end]);
    }

    fn stat(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        if (!std.mem.eql(u8, artifact_id, "mem:vector-sidecar")) return error.ArtifactNotFound;
        const bytes = self.bytes orelse return error.ArtifactNotFound;
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:vector-sidecar"),
            .byte_len = @intCast(bytes.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{bytes.len}),
        };
    }

    fn delete(self: *MemoryArtifactStore, artifact_id: []const u8) !void {
        if (!std.mem.eql(u8, artifact_id, "mem:vector-sidecar")) return error.ArtifactNotFound;
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.bytes = null;
    }

    const vtable: artifact_store.ArtifactStore.VTable = .{
        .deinit = erasedDeinit,
        .put = erasedPut,
        .get_alloc = erasedGetAlloc,
        .get_range_alloc = erasedGetRangeAlloc,
        .stat = erasedStat,
        .delete = erasedDelete,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedPut(ptr: *anyopaque, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.put(alloc, contents);
    }

    fn erasedGetAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getAlloc(alloc, artifact_id);
    }

    fn erasedGetRangeAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAlloc(alloc, artifact_id, offset, len);
    }

    fn erasedStat(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.stat(alloc, artifact_id);
    }

    fn erasedDelete(ptr: *anyopaque, artifact_id: []const u8) !void {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        try self.delete(artifact_id);
    }
};

test "lake vector sidecar builder consumes external row source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-226",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 1),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 2),
    };
    const vectors = [_][]const f32{
        &.{ 1.0, 0.0 },
        &.{ 0.0, 1.0 },
        &.{ 0.5, 0.5 },
    };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "embedding", .values = .{ .vector_f32 = &vectors } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = external_binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const binding = source_binding.bindingFromSnapshot(
        .vector,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"embedding"},
        "sha256:vector:v1",
    );

    var result = try buildVectorSidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
        .name = "events.embedding.vector",
        .vector_column = "embedding",
        .metric = .inner_product,
        .policy = .{ .target_cluster_count = 1, .base_probe_count = 1, .shortlist_multiplier = 2 },
    });
    defer result.deinit(alloc);

    try result.declaration.validate();
    try sidecar_manifest.validateBatchAgainstDeclaredArtifact(result.declaration, batches[0]);
    try std.testing.expectEqualStrings("events.embedding.vector", result.declaration.name);
    try std.testing.expectEqual(@as(u64, result.payload.len), result.declaration.artifact.byte_len);

    var segment = try vector_segment.decodeAlloc(alloc, result.payload);
    defer vector_segment.freeSegment(alloc, &segment);
    try std.testing.expectEqual(@as(u32, 2), segment.dims);
    try std.testing.expectEqual(shared_vector.DistanceMetric.inner_product, segment.metric);
    try std.testing.expectEqual(@as(usize, 3), segment.entries.len);
    try std.testing.expectEqual(@as(usize, 1), segment.clusters.len);

    const first_key = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(first_key);
    try std.testing.expect(std.mem.eql(u8, first_key, segment.entries[0].doc_id) or
        std.mem.eql(u8, first_key, segment.entries[1].doc_id) or
        std.mem.eql(u8, first_key, segment.entries[2].doc_id));
}

test "lake vector sidecar builder consumes json projection vectors" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-227",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 1),
    };
    const values = [_][]const u8{
        "{\"_embeddings\":{\"semantic\":[1.0,2.0]}}",
        "[3.0,4.0]",
    };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "embedding", .values = .{ .json = &values } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = external_binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const binding = source_binding.bindingFromSnapshot(
        .vector,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"embedding"},
        "sha256:vector:v1",
    );

    var result = try buildVectorSidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
        .name = "events.embedding.vector",
        .vector_column = "embedding",
        .metric = .l2_squared,
        .embedding_name = "semantic",
        .policy = .{ .target_cluster_count = 1 },
    });
    defer result.deinit(alloc);

    var segment = try vector_segment.decodeAlloc(alloc, result.payload);
    defer vector_segment.freeSegment(alloc, &segment);
    try std.testing.expectEqual(@as(u32, 2), segment.dims);
    try std.testing.expectEqual(shared_vector.DistanceMetric.l2_squared, segment.metric);
    try std.testing.expectEqual(@as(usize, 2), segment.entries.len);
}

test "lake vector sidecar publisher writes artifact store metadata into declaration" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-228",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
    };
    const vectors = [_][]const f32{
        &.{ 1.0, 0.0 },
    };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "embedding", .values = .{ .vector_f32 = &vectors } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = external_binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const binding = source_binding.bindingFromSnapshot(
        .vector,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"embedding"},
        "sha256:vector:v1",
    );

    var result = try publishVectorSidecarFromRowSourceAlloc(
        alloc,
        &artifacts,
        batch_source.rowSource(),
        binding,
        .{
            .name = "events.embedding.vector",
            .vector_column = "embedding",
            .policy = .{ .target_cluster_count = 1 },
        },
    );
    defer result.deinit(alloc);

    try result.declaration.validate();
    try std.testing.expectEqualStrings("mem:vector-sidecar", result.declaration.artifact.artifact_id);

    const stored = try artifacts.getAlloc(result.declaration.artifact.artifact_id);
    defer alloc.free(stored);
    try std.testing.expectEqual(@as(usize, @intCast(result.declaration.artifact.byte_len)), stored.len);

    var segment = try vector_segment.decodeAlloc(alloc, stored);
    defer vector_segment.freeSegment(alloc, &segment);
    try std.testing.expectEqual(@as(usize, 1), segment.entries.len);
}

test "lake vector sidecar builder rejects stale source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-229",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
    };
    const vectors = [_][]const f32{
        &.{ 1.0, 0.0 },
    };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "embedding", .values = .{ .vector_f32 = &vectors } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = external_binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const stale_snapshot = rowsource.SnapshotRef{ .table_id = "events", .snapshot_id = "iceberg-228" };
    const binding = source_binding.bindingFromSnapshot(
        .vector,
        .external_iceberg,
        stale_snapshot,
        external_binding.schema_fingerprint,
        &[_][]const u8{"embedding"},
        "sha256:vector:v1",
    );

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        buildVectorSidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
            .name = "events.embedding.vector",
            .vector_column = "embedding",
        }),
    );
}
