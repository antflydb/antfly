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

//! Lake-native sidecar builders over RowSource batches.
//!
//! This module is deliberately narrow: it builds Antfly text sidecars from
//! already-decoded RowSource batches, preserving row identity through RowRef
//! document ids. Parquet/Iceberg/Lance readers stay outside this layer.

const std = @import("std");
const Allocator = std.mem.Allocator;
const artifact_ref = @import("../manifest/artifact_ref.zig");
const artifact_store = @import("../artifacts/store.zig");
const indexed_reader = @import("../query/indexed_reader.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const text_segment = @import("../text_segment/mod.zig");
const external_rowsource = @import("../../storage/rowsource/external.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const lake_build_limits = @import("lake_build_limits.zig");

pub const TextSidecarBuildOptions = struct {
    name: []const u8,
    text_column: []const u8,
    config_json: []const u8 = "{}",
    artifact_id: []const u8 = &.{},
    limits: lake_build_limits.Limits = .{},
};

pub const TextSidecarBuildResult = struct {
    payload: []u8,
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *TextSidecarBuildResult, alloc: Allocator) void {
        alloc.free(self.payload);
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub const TextSidecarPublishResult = struct {
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *TextSidecarPublishResult, alloc: Allocator) void {
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub fn buildTextSidecarFromRowSourceAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: TextSidecarBuildOptions,
) !TextSidecarBuildResult {
    var working_set = try lake_build_limits.WorkingSetAllocator.init(alloc, options.limits);
    return buildTextSidecarBoundedAlloc(working_set.allocator(), source, binding, options) catch |err| {
        if (err == error.OutOfMemory and working_set.limit_exceeded) return error.LakeSidecarBuildBudgetExceeded;
        return err;
    };
}

fn buildTextSidecarBoundedAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: TextSidecarBuildOptions,
) !TextSidecarBuildResult {
    try validateOptions(binding, source.kind, options);
    var budget = try lake_build_limits.Budget.init(options.limits);

    var docs = std.ArrayListUnmanaged(text_segment.DocumentEntry).empty;
    errdefer docs.deinit(alloc);
    errdefer freeDocumentEntries(alloc, docs.items);

    var term_map = std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(text_segment.Posting)).empty;
    defer {
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
                .kind = .text_segment,
                .name = options.name,
                .artifact_id = "pending",
                .byte_len = 1,
                .checksum = "pending",
            },
        }, batch);

        const column = batch.findColumn(options.text_column).?;
        posting_count = std.math.add(usize, posting_count, try appendBatchText(alloc, &docs, &term_map, batch, column)) catch
            return error.LakeSidecarBuildBudgetExceeded;
        const retained_items = std.math.add(usize, docs.items.len, term_map.count()) catch
            return error.LakeSidecarBuildBudgetExceeded;
        const total_retained_items = std.math.add(usize, retained_items, posting_count) catch
            return error.LakeSidecarBuildBudgetExceeded;
        try budget.checkRetainedItems(total_retained_items);
    }

    const doc_entries = try docs.toOwnedSlice(alloc);
    errdefer {
        freeDocumentEntries(alloc, doc_entries);
        alloc.free(doc_entries);
    }

    const term_entries = try alloc.alloc(text_segment.TermEntry, term_map.count());
    errdefer alloc.free(term_entries);
    var terms_initialized: usize = 0;
    errdefer {
        for (term_entries[0..terms_initialized]) |*term| term.deinit(alloc);
    }

    for (term_map.keys(), 0..) |term, idx| {
        term_entries[idx] = .{
            .term = try alloc.dupe(u8, term),
            .postings = try term_map.values()[idx].toOwnedSlice(alloc),
        };
        terms_initialized += 1;
    }
    std.mem.sort(text_segment.TermEntry, term_entries, {}, lessTermEntry);

    var segment = text_segment.Segment{
        .index_name = try alloc.dupe(u8, options.name),
        .source_name = try alloc.dupe(u8, options.text_column),
        .config_json = try alloc.dupe(u8, options.config_json),
        .docs = doc_entries,
        .terms = term_entries,
    };
    defer segment.deinit(alloc);

    const payload = try text_segment.encodeAlloc(alloc, segment);
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

pub fn publishTextSidecarFromRowSourceAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: TextSidecarBuildOptions,
) !TextSidecarPublishResult {
    var built = try buildTextSidecarFromRowSourceAlloc(alloc, source, binding, options);
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
    options: TextSidecarBuildOptions,
) !void {
    try binding.validate();
    if (binding.sidecar_kind != .text) return error.InvalidLakeSidecarTextBuildOptions;
    if (source_kind != binding.source_kind) return error.SidecarSourceBindingMismatch;
    if (options.name.len == 0 or options.text_column.len == 0) {
        return error.InvalidLakeSidecarTextBuildOptions;
    }
    if (binding.column_bindings.len != 1) return error.InvalidLakeSidecarTextBuildOptions;
    if (!std.mem.eql(u8, binding.column_bindings[0], options.text_column)) {
        return error.SidecarSourceBindingMismatch;
    }
}

fn appendBatchText(
    alloc: Allocator,
    docs: *std.ArrayListUnmanaged(text_segment.DocumentEntry),
    term_map: *std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(text_segment.Posting)),
    batch: rowsource.ColumnBatch,
    column: rowsource.ColumnVector,
) !usize {
    var posting_count: usize = 0;
    switch (column.values) {
        .bytes => |values| {
            for (values, 0..) |value, row| {
                if (column.nulls.isNull(row)) continue;
                posting_count = std.math.add(usize, posting_count, try appendDocument(alloc, docs, term_map, batch.row_refs[row], value)) catch
                    return error.LakeSidecarBuildBudgetExceeded;
            }
        },
        .json => |values| {
            for (values, 0..) |value, row| {
                if (column.nulls.isNull(row)) continue;
                posting_count = std.math.add(usize, posting_count, try appendDocument(alloc, docs, term_map, batch.row_refs[row], value)) catch
                    return error.LakeSidecarBuildBudgetExceeded;
            }
        },
        else => return error.UnsupportedLakeSidecarTextColumn,
    }
    return posting_count;
}

fn appendDocument(
    alloc: Allocator,
    docs: *std.ArrayListUnmanaged(text_segment.DocumentEntry),
    term_map: *std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(text_segment.Posting)),
    row_ref: rowsource.RowRef,
    source_text: []const u8,
) !usize {
    if (docs.items.len > std.math.maxInt(u32)) return error.LakeSidecarTextTooManyDocs;
    const doc_index: u32 = @intCast(docs.items.len);

    const doc_id = try source_binding.rowRefKeyAlloc(alloc, row_ref);
    var doc_id_owned = true;
    errdefer if (doc_id_owned) alloc.free(doc_id);

    const normalized_text = try indexed_reader.normalizeAlloc(alloc, source_text);
    var normalized_owned = true;
    errdefer if (normalized_owned) alloc.free(normalized_text);

    var token_count: u32 = 0;
    var per_doc = std.StringArrayHashMapUnmanaged(u32).empty;
    defer per_doc.deinit(alloc);

    var token_iter = std.mem.tokenizeAny(u8, normalized_text, " ");
    while (token_iter.next()) |token| {
        if (token_count == std.math.maxInt(u32)) return error.LakeSidecarTextTooManyTokens;
        token_count += 1;
        const gop = try per_doc.getOrPut(alloc, token);
        if (!gop.found_existing) gop.value_ptr.* = 0;
        gop.value_ptr.* += 1;
    }

    for (per_doc.keys(), per_doc.values()) |term, freq| {
        const gop = try term_map.getOrPut(alloc, term);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        try gop.value_ptr.append(alloc, .{
            .doc_index = doc_index,
            .term_freq = freq,
        });
    }

    try docs.append(alloc, .{
        .doc_id = doc_id,
        .normalized_text = normalized_text,
        .token_count = token_count,
    });
    doc_id_owned = false;
    normalized_owned = false;
    return per_doc.count();
}

fn declaredArtifactAlloc(
    alloc: Allocator,
    binding: source_binding.Binding,
    options: TextSidecarBuildOptions,
    payload_len: usize,
) !sidecar_manifest.DeclaredArtifact {
    const name = try alloc.dupe(u8, options.name);
    errdefer alloc.free(name);
    const artifact_name = try alloc.dupe(u8, options.name);
    errdefer alloc.free(artifact_name);
    const artifact_id = if (options.artifact_id.len == 0)
        try std.fmt.allocPrint(
            alloc,
            "lake-text:{d}:{s}:{d}:{s}:{d}",
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
            .kind = .text_segment,
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

fn freeDocumentEntries(alloc: Allocator, docs: []text_segment.DocumentEntry) void {
    for (docs) |*doc| doc.deinit(alloc);
}

fn lessTermEntry(_: void, lhs: text_segment.TermEntry, rhs: text_segment.TermEntry) bool {
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
            .artifact_id = try alloc.dupe(u8, "mem:text-sidecar"),
            .byte_len = @intCast(contents.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{contents.len}),
        };
    }

    fn getAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        if (!std.mem.eql(u8, artifact_id, "mem:text-sidecar")) return error.ArtifactNotFound;
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
        if (!std.mem.eql(u8, artifact_id, "mem:text-sidecar")) return error.ArtifactNotFound;
        const bytes = self.bytes orelse return error.ArtifactNotFound;
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:text-sidecar"),
            .byte_len = @intCast(bytes.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{bytes.len}),
        };
    }

    fn delete(self: *MemoryArtifactStore, artifact_id: []const u8) !void {
        if (!std.mem.eql(u8, artifact_id, "mem:text-sidecar")) return error.ArtifactNotFound;
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

test "lake text sidecar builder consumes external row source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-123",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 1),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 2),
    };
    const bodies = [_][]const u8{
        "Alpha beta",
        "beta BETA gamma",
        "ignored null",
    };
    const nulls = [_]u8{ 0, 0, 1 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "body", .values = .{ .bytes = &bodies }, .nulls = .{ .bytes = &nulls } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = external_binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const row_source = batch_source.rowSource();
    const binding = source_binding.bindingFromSnapshot(
        .text,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"body"},
        "sha256:text:v1",
    );

    var result = try buildTextSidecarFromRowSourceAlloc(alloc, row_source, binding, .{
        .name = "events.body.text",
        .text_column = "body",
        .config_json = "{\"analyzer\":\"simple\"}",
    });
    defer result.deinit(alloc);

    try result.declaration.validate();
    try sidecar_manifest.validateBatchAgainstDeclaredArtifact(result.declaration, batches[0]);
    try std.testing.expectEqualStrings("events.body.text", result.declaration.name);
    try std.testing.expectEqualStrings("len:", result.declaration.artifact.checksum[0..4]);
    try std.testing.expectEqual(@as(u64, result.payload.len), result.declaration.artifact.byte_len);

    var segment = try text_segment.decodeAlloc(alloc, result.payload);
    defer text_segment.freeSegment(alloc, &segment);
    try std.testing.expectEqualStrings("events.body.text", segment.index_name);
    try std.testing.expectEqualStrings("body", segment.source_name);
    try std.testing.expectEqual(@as(usize, 2), segment.docs.len);
    try std.testing.expectEqual(@as(usize, 3), segment.terms.len);

    const first_key = try source_binding.rowRefKeyAlloc(alloc, row_refs[0]);
    defer alloc.free(first_key);
    try std.testing.expectEqualStrings(first_key, segment.docs[0].doc_id);
    try std.testing.expectEqualStrings("alpha beta", segment.docs[0].normalized_text);

    const second_key = try source_binding.rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(second_key);
    try std.testing.expectEqualStrings(second_key, segment.docs[1].doc_id);

    const beta = findTerm(segment.terms, "beta").?;
    try std.testing.expectEqual(@as(usize, 2), beta.postings.len);
    try std.testing.expectEqual(@as(u32, 1), beta.postings[0].term_freq);
    try std.testing.expectEqual(@as(u32, 2), beta.postings[1].term_freq);

    var bounded_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    try std.testing.expectError(error.LakeSidecarBuildBudgetExceeded, buildTextSidecarFromRowSourceAlloc(
        alloc,
        bounded_source.rowSource(),
        binding,
        .{
            .name = "events.body.text",
            .text_column = "body",
            .limits = .{ .max_rows = 2 },
        },
    ));

    var working_set_bounded_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    try std.testing.expectError(error.LakeSidecarBuildBudgetExceeded, buildTextSidecarFromRowSourceAlloc(
        alloc,
        working_set_bounded_source.rowSource(),
        binding,
        .{
            .name = "events.body.text",
            .text_column = "body",
            .limits = .{ .max_working_set_bytes = 1 },
        },
    ));
}

test "lake text sidecar builder rejects stale source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-124",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
    };
    const bodies = [_][]const u8{"Alpha beta"};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "body", .values = .{ .bytes = &bodies } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = external_binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const stale_snapshot = rowsource.SnapshotRef{ .table_id = "events", .snapshot_id = "iceberg-123" };
    const binding = source_binding.bindingFromSnapshot(
        .text,
        .external_iceberg,
        stale_snapshot,
        external_binding.schema_fingerprint,
        &[_][]const u8{"body"},
        "sha256:text:v1",
    );

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        buildTextSidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
            .name = "events.body.text",
            .text_column = "body",
        }),
    );
}

test "lake text sidecar publisher writes artifact store metadata into declaration" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-125",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 1),
    };
    const bodies = [_][]const u8{ "Alpha beta", "gamma beta" };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "body", .values = .{ .bytes = &bodies } },
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
        .text,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"body"},
        "sha256:text:v1",
    );

    var result = try publishTextSidecarFromRowSourceAlloc(
        alloc,
        &artifacts,
        batch_source.rowSource(),
        binding,
        .{
            .name = "events.body.text",
            .text_column = "body",
        },
    );
    defer result.deinit(alloc);

    try result.declaration.validate();
    try std.testing.expectEqualStrings("mem:text-sidecar", result.declaration.artifact.artifact_id);
    try std.testing.expect(result.declaration.artifact.byte_len > 0);

    const stored = try artifacts.getAlloc(result.declaration.artifact.artifact_id);
    defer alloc.free(stored);
    try std.testing.expectEqual(@as(usize, @intCast(result.declaration.artifact.byte_len)), stored.len);

    var segment = try text_segment.decodeAlloc(alloc, stored);
    defer text_segment.freeSegment(alloc, &segment);
    try std.testing.expectEqual(@as(usize, 2), segment.docs.len);
    try std.testing.expectEqualStrings("events.body.text", segment.index_name);
}

fn findTerm(terms: []const text_segment.TermEntry, term: []const u8) ?text_segment.TermEntry {
    for (terms) |entry| {
        if (std.mem.eql(u8, entry.term, term)) return entry;
    }
    return null;
}
