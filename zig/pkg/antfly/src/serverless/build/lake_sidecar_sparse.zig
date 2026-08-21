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

//! Lake-native sparse sidecar builders over RowSource batches.

const std = @import("std");
const Allocator = std.mem.Allocator;
const artifact_ref = @import("../manifest/artifact_ref.zig");
const artifact_store = @import("../artifacts/store.zig");
const document_projection = @import("../document_projection.zig");
const indexed_reader = @import("../query/indexed_reader.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const sparse_segment = @import("../sparse_segment/mod.zig");
const external_rowsource = @import("../../storage/rowsource/external.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const lake_build_limits = @import("lake_build_limits.zig");

pub const SparseSidecarBuildOptions = struct {
    name: []const u8,
    sparse_column: []const u8,
    artifact_id: []const u8 = &.{},
    limits: lake_build_limits.Limits = .{},
};

pub const SparseSidecarBuildResult = struct {
    payload: []u8,
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *SparseSidecarBuildResult, alloc: Allocator) void {
        alloc.free(self.payload);
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub const SparseSidecarPublishResult = struct {
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *SparseSidecarPublishResult, alloc: Allocator) void {
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub fn buildSparseSidecarFromRowSourceAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: SparseSidecarBuildOptions,
) !SparseSidecarBuildResult {
    var working_set = try lake_build_limits.WorkingSetAllocator.init(alloc, options.limits);
    return buildSparseSidecarBoundedAlloc(working_set.allocator(), source, binding, options) catch |err| {
        if (err == error.OutOfMemory and working_set.limit_exceeded) return error.LakeSidecarBuildBudgetExceeded;
        return err;
    };
}

fn buildSparseSidecarBoundedAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: SparseSidecarBuildOptions,
) !SparseSidecarBuildResult {
    try validateOptions(binding, source.kind, options);
    var budget = try lake_build_limits.Budget.init(options.limits);

    var docs = std.ArrayListUnmanaged(sparse_segment.DocumentEntry).empty;
    errdefer docs.deinit(alloc);
    errdefer freeDocumentEntries(alloc, docs.items);

    var term_map = std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(sparse_segment.Posting)).empty;
    defer {
        for (term_map.keys()) |term| alloc.free(term);
        for (term_map.values()) |*postings| postings.deinit(alloc);
        term_map.deinit(alloc);
    }
    var posting_count: usize = 0;

    while (try source.next(alloc)) |batch| {
        try budget.admitBatch(batch);
        try sidecar_manifest.validateBatchAgainstDeclaredArtifact(.{
            .name = options.name,
            .binding = binding,
            .artifact = .{
                .kind = .sparse_segment,
                .name = options.name,
                .artifact_id = "pending",
                .byte_len = 1,
                .checksum = "pending",
            },
        }, batch);

        const column = batch.findColumn(options.sparse_column).?;
        posting_count = std.math.add(usize, posting_count, try appendBatchSparse(alloc, &docs, &term_map, batch, column)) catch
            return error.LakeSidecarBuildBudgetExceeded;
        const retained_items = std.math.add(usize, docs.items.len, term_map.count()) catch
            return error.LakeSidecarBuildBudgetExceeded;
        const total_retained_items = std.math.add(usize, retained_items, posting_count) catch
            return error.LakeSidecarBuildBudgetExceeded;
        try budget.checkRetainedItems(total_retained_items);
    }

    var entries_owned_by_segment = false;
    const doc_entries = try docs.toOwnedSlice(alloc);
    errdefer if (!entries_owned_by_segment) {
        freeDocumentEntries(alloc, doc_entries);
        alloc.free(doc_entries);
    };

    const term_entries = try alloc.alloc(sparse_segment.TermEntry, term_map.count());
    errdefer if (!entries_owned_by_segment) alloc.free(term_entries);
    var terms_initialized: usize = 0;
    errdefer if (!entries_owned_by_segment) {
        for (term_entries[0..terms_initialized]) |*term| term.deinit(alloc);
    };

    for (term_map.keys(), 0..) |term, idx| {
        const owned_term = try alloc.dupe(u8, term);
        errdefer alloc.free(owned_term);
        const owned_postings = try term_map.values()[idx].toOwnedSlice(alloc);
        errdefer alloc.free(owned_postings);
        term_entries[idx] = .{
            .term = owned_term,
            .postings = owned_postings,
        };
        terms_initialized += 1;
    }
    std.mem.sort(sparse_segment.TermEntry, term_entries, {}, lessSparseTermEntry);

    var segment = sparse_segment.Segment{
        .docs = doc_entries,
        .terms = term_entries,
    };
    entries_owned_by_segment = true;
    defer segment.deinit(alloc);

    const encoded_size = try sparse_segment.encodedSize(segment);
    try budget.checkOutputBytes(encoded_size);
    const payload = try sparse_segment.encodeAlloc(alloc, segment);
    errdefer alloc.free(payload);
    std.debug.assert(payload.len == encoded_size);

    var declaration = try declaredArtifactAlloc(alloc, binding, options, payload.len);
    errdefer freeOwnedDeclaration(alloc, declaration);
    try declaration.validate();

    return .{
        .payload = payload,
        .declaration = declaration,
    };
}

pub fn publishSparseSidecarFromRowSourceAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: SparseSidecarBuildOptions,
) !SparseSidecarPublishResult {
    var built = try buildSparseSidecarFromRowSourceAlloc(alloc, source, binding, options);
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
    options: SparseSidecarBuildOptions,
) !void {
    try binding.validate();
    if (binding.sidecar_kind != .sparse) return error.InvalidLakeSidecarSparseBuildOptions;
    if (source_kind != binding.source_kind) return error.SidecarSourceBindingMismatch;
    if (options.name.len == 0 or options.sparse_column.len == 0) {
        return error.InvalidLakeSidecarSparseBuildOptions;
    }
    if (binding.column_bindings.len != 1) return error.InvalidLakeSidecarSparseBuildOptions;
    if (!std.mem.eql(u8, binding.column_bindings[0], options.sparse_column)) {
        return error.SidecarSourceBindingMismatch;
    }
}

fn appendBatchSparse(
    alloc: Allocator,
    docs: *std.ArrayListUnmanaged(sparse_segment.DocumentEntry),
    term_map: *std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(sparse_segment.Posting)),
    batch: rowsource.ColumnBatch,
    column: rowsource.ColumnVector,
) !usize {
    var posting_count: usize = 0;
    switch (column.values) {
        .bytes => |values| {
            for (values, 0..) |value, row| {
                if (column.nulls.isNull(row)) continue;
                posting_count = std.math.add(usize, posting_count, try appendSparseDocument(alloc, docs, term_map, batch.row_refs[row], value)) catch
                    return error.LakeSidecarBuildBudgetExceeded;
            }
        },
        .json => |values| {
            for (values, 0..) |value, row| {
                if (column.nulls.isNull(row)) continue;
                posting_count = std.math.add(usize, posting_count, try appendSparseDocument(alloc, docs, term_map, batch.row_refs[row], value)) catch
                    return error.LakeSidecarBuildBudgetExceeded;
            }
        },
        else => return error.UnsupportedLakeSidecarSparseColumn,
    }
    return posting_count;
}

fn appendSparseDocument(
    alloc: Allocator,
    docs: *std.ArrayListUnmanaged(sparse_segment.DocumentEntry),
    term_map: *std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(sparse_segment.Posting)),
    row_ref: rowsource.RowRef,
    source_value: []const u8,
) !usize {
    if (docs.items.len > std.math.maxInt(u32)) return error.LakeSidecarSparseTooManyDocs;
    const doc_index: u32 = @intCast(docs.items.len);

    var feature_count: u32 = 0;
    if (try appendProjectionSparseFeatures(alloc, term_map, doc_index, source_value, &feature_count)) {
        try appendSparseDocEntry(alloc, docs, row_ref, feature_count);
        return feature_count;
    }

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source_value, .{});
    defer parsed.deinit();
    try appendSparseFeaturesFromJsonValue(alloc, term_map, doc_index, parsed.value, &feature_count);
    try appendSparseDocEntry(alloc, docs, row_ref, feature_count);
    return feature_count;
}

fn appendProjectionSparseFeatures(
    alloc: Allocator,
    term_map: *std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(sparse_segment.Posting)),
    doc_index: u32,
    source_value: []const u8,
    feature_count: *u32,
) !bool {
    var projection = document_projection.parseAlloc(alloc, source_value) catch return false;
    defer projection.deinit(alloc);
    const features = projection.sparse_embedding orelse return false;
    for (features) |feature| {
        try appendFeature(alloc, term_map, doc_index, feature.term, feature.weight, feature_count);
    }
    return true;
}

fn appendSparseFeaturesFromJsonValue(
    alloc: Allocator,
    term_map: *std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(sparse_segment.Posting)),
    doc_index: u32,
    value: std.json.Value,
    feature_count: *u32,
) !void {
    switch (value) {
        .object => |obj| {
            var iter = obj.iterator();
            while (iter.next()) |entry| {
                try appendFeature(
                    alloc,
                    term_map,
                    doc_index,
                    entry.key_ptr.*,
                    try jsonValueAsF32(entry.value_ptr.*),
                    feature_count,
                );
            }
        },
        .array => |items| {
            for (items.items) |item| {
                if (item != .object) return error.InvalidLakeSidecarSparseFeatures;
                const term = jsonObjectString(item.object, "term") orelse
                    jsonObjectString(item.object, "token") orelse
                    return error.InvalidLakeSidecarSparseFeatures;
                const weight_value = item.object.get("weight") orelse
                    item.object.get("score") orelse
                    return error.InvalidLakeSidecarSparseFeatures;
                try appendFeature(alloc, term_map, doc_index, term, try jsonValueAsF32(weight_value), feature_count);
            }
        },
        else => return error.InvalidLakeSidecarSparseFeatures,
    }
}

fn appendFeature(
    alloc: Allocator,
    term_map: *std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(sparse_segment.Posting)),
    doc_index: u32,
    term: []const u8,
    weight: f32,
    feature_count: *u32,
) !void {
    const normalized_term = try indexed_reader.normalizeAlloc(alloc, term);
    var normalized_term_owned = true;
    errdefer if (normalized_term_owned) alloc.free(normalized_term);
    if (normalized_term.len == 0) {
        alloc.free(normalized_term);
        return;
    }
    const gop = try term_map.getOrPut(alloc, normalized_term);
    if (!gop.found_existing) {
        gop.value_ptr.* = .empty;
        normalized_term_owned = false;
    } else {
        alloc.free(normalized_term);
        normalized_term_owned = false;
    }
    try gop.value_ptr.append(alloc, .{
        .doc_index = doc_index,
        .weight = weight,
    });
    if (feature_count.* == std.math.maxInt(u32)) return error.LakeSidecarSparseTooManyFeatures;
    feature_count.* += 1;
}

fn appendSparseDocEntry(
    alloc: Allocator,
    docs: *std.ArrayListUnmanaged(sparse_segment.DocumentEntry),
    row_ref: rowsource.RowRef,
    feature_count: u32,
) !void {
    if (feature_count == 0) return;
    const doc_id = try source_binding.rowRefKeyAlloc(alloc, row_ref);
    errdefer alloc.free(doc_id);
    try docs.append(alloc, .{
        .doc_id = doc_id,
        .feature_count = feature_count,
    });
}

fn declaredArtifactAlloc(
    alloc: Allocator,
    binding: source_binding.Binding,
    options: SparseSidecarBuildOptions,
    payload_len: usize,
) !sidecar_manifest.DeclaredArtifact {
    const name = try alloc.dupe(u8, options.name);
    errdefer alloc.free(name);
    const artifact_name = try alloc.dupe(u8, options.name);
    errdefer alloc.free(artifact_name);
    const artifact_id = if (options.artifact_id.len == 0)
        try std.fmt.allocPrint(
            alloc,
            "lake-sparse:{d}:{s}:{d}:{s}:{d}",
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
            .kind = .sparse_segment,
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

fn freeDocumentEntries(alloc: Allocator, docs: []sparse_segment.DocumentEntry) void {
    for (docs) |*doc| doc.deinit(alloc);
}

fn jsonObjectString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .string => value.string,
        else => null,
    };
}

fn jsonValueAsF32(value: std.json.Value) !f32 {
    return switch (value) {
        .float => @floatCast(value.float),
        .integer => @floatFromInt(value.integer),
        .number_string => try std.fmt.parseFloat(f32, value.number_string),
        else => error.InvalidLakeSidecarSparseFeatures,
    };
}

fn lessSparseTermEntry(_: void, lhs: sparse_segment.TermEntry, rhs: sparse_segment.TermEntry) bool {
    return std.mem.lessThan(u8, lhs.term, rhs.term);
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
            .artifact_id = try alloc.dupe(u8, "mem:sparse-sidecar"),
            .byte_len = @intCast(contents.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{contents.len}),
        };
    }

    fn getAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        if (!std.mem.eql(u8, artifact_id, "mem:sparse-sidecar")) return error.ArtifactNotFound;
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
        if (!std.mem.eql(u8, artifact_id, "mem:sparse-sidecar")) return error.ArtifactNotFound;
        const bytes = self.bytes orelse return error.ArtifactNotFound;
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:sparse-sidecar"),
            .byte_len = @intCast(bytes.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{bytes.len}),
        };
    }

    fn delete(self: *MemoryArtifactStore, artifact_id: []const u8) !void {
        if (!std.mem.eql(u8, artifact_id, "mem:sparse-sidecar")) return error.ArtifactNotFound;
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

test "lake sparse sidecar builder consumes external row source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-223",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 1),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 2),
    };
    const sparse_values = [_][]const u8{
        "{\"alpha\":1.0,\"beta\":0.5}",
        "{\"sparse_embedding\":{\"beta\":2.0,\"gamma\":1.25}}",
        "{\"ignored\":3.0}",
    };
    const nulls = [_]u8{ 0, 0, 1 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "sparse_embedding", .values = .{ .json = &sparse_values }, .nulls = .{ .bytes = &nulls } },
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
        .sparse,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"sparse_embedding"},
        "sha256:sparse:v1",
    );

    var result = try buildSparseSidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
        .name = "events.sparse",
        .sparse_column = "sparse_embedding",
    });
    defer result.deinit(alloc);

    try result.declaration.validate();
    try sidecar_manifest.validateBatchAgainstDeclaredArtifact(result.declaration, batches[0]);
    try std.testing.expectEqualStrings("events.sparse", result.declaration.name);
    try std.testing.expectEqual(@as(u64, result.payload.len), result.declaration.artifact.byte_len);

    var segment = try sparse_segment.decodeAlloc(alloc, result.payload);
    defer sparse_segment.freeSegment(alloc, &segment);
    try std.testing.expectEqual(@as(usize, 2), segment.docs.len);
    try std.testing.expectEqual(@as(usize, 3), segment.terms.len);

    const first_key = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(first_key);
    try std.testing.expectEqualStrings(first_key, segment.docs[0].doc_id);
    try std.testing.expectEqual(@as(u32, 2), segment.docs[0].feature_count);

    const beta = findTerm(segment.terms, "beta").?;
    try std.testing.expectEqual(@as(usize, 2), beta.postings.len);
    try std.testing.expectEqual(@as(f32, 0.5), beta.postings[0].weight);
    try std.testing.expectEqual(@as(f32, 2.0), beta.postings[1].weight);
}

test "lake sparse sidecar builder releases every partial OOM allocation" {
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-sparse-oom",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
    };
    const sparse_values = [_][]const u8{"{\"alpha\":1.0,\"beta\":0.5}"};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "sparse_embedding", .values = .{ .json = &sparse_values } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = external_binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    const binding = source_binding.bindingFromSnapshot(
        .sparse,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"sparse_embedding"},
        "sha256:sparse:v1",
    );

    var reached_success = false;
    for (0..256) |fail_index| {
        var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = fail_index });
        const failing_alloc = failing.allocator();
        const result = buildSparseSidecarFromRowSourceAlloc(failing_alloc, batch_source.rowSource(), binding, .{
            .name = "events.sparse",
            .sparse_column = "sparse_embedding",
        });
        if (result) |value| {
            var built = value;
            built.deinit(failing_alloc);
            try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
            reached_success = true;
            break;
        } else |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        }
    }
    try std.testing.expect(reached_success);
}

test "lake sparse sidecar publisher writes artifact store metadata into declaration" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-224",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
    };
    const sparse_values = [_][]const u8{"[{\"term\":\"alpha\",\"weight\":1.0},{\"term\":\"beta\",\"weight\":0.5}]"};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "sparse_embedding", .values = .{ .bytes = &sparse_values } },
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
        .sparse,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"sparse_embedding"},
        "sha256:sparse:v1",
    );

    var result = try publishSparseSidecarFromRowSourceAlloc(
        alloc,
        &artifacts,
        batch_source.rowSource(),
        binding,
        .{
            .name = "events.sparse",
            .sparse_column = "sparse_embedding",
        },
    );
    defer result.deinit(alloc);

    try result.declaration.validate();
    try std.testing.expectEqualStrings("mem:sparse-sidecar", result.declaration.artifact.artifact_id);

    const stored = try artifacts.getAlloc(result.declaration.artifact.artifact_id);
    defer alloc.free(stored);
    try std.testing.expectEqual(@as(usize, @intCast(result.declaration.artifact.byte_len)), stored.len);

    var segment = try sparse_segment.decodeAlloc(alloc, stored);
    defer sparse_segment.freeSegment(alloc, &segment);
    try std.testing.expectEqual(@as(usize, 1), segment.docs.len);
    try std.testing.expectEqual(@as(usize, 2), segment.terms.len);
}

test "lake sparse sidecar builder rejects stale source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-225",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
    };
    const sparse_values = [_][]const u8{"{\"alpha\":1.0}"};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "sparse_embedding", .values = .{ .json = &sparse_values } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = external_binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const stale_snapshot = rowsource.SnapshotRef{ .table_id = "events", .snapshot_id = "iceberg-224" };
    const binding = source_binding.bindingFromSnapshot(
        .sparse,
        .external_iceberg,
        stale_snapshot,
        external_binding.schema_fingerprint,
        &[_][]const u8{"sparse_embedding"},
        "sha256:sparse:v1",
    );

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        buildSparseSidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
            .name = "events.sparse",
            .sparse_column = "sparse_embedding",
        }),
    );
}

fn findTerm(terms: []const sparse_segment.TermEntry, term: []const u8) ?sparse_segment.TermEntry {
    for (terms) |entry| {
        if (std.mem.eql(u8, entry.term, term)) return entry;
    }
    return null;
}
