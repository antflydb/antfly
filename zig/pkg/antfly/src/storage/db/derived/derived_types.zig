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
const types = @import("../types.zig");
const enrichment_types = @import("../enrichment/enrichment_types.zig");

pub const DerivedAction = enum {
    upsert,
    delete,
    preserve_base_document,
};

pub const DerivedTarget = enum {
    full_text,
    dense_vector,
    sparse_vector,
    graph,
    algebraic,
};

pub const DerivedTargetRef = struct {
    kind: DerivedTarget,
    index_name: []const u8,
};

pub const DerivedDocument = struct {
    key: []const u8,
    action: DerivedAction = .upsert,
    cleaned_value: ?[]const u8 = null,
    targets: []const DerivedTargetRef = &.{},
};

pub const DerivedGraphDocClear = struct {
    key: []const u8,
    index_names: []const []const u8 = &.{},
};

pub const DerivedDenseEmbeddingWrite = struct {
    index_name: []const u8,
    parent_doc_key: ?[]const u8 = null,
    doc_key: []const u8,
    artifact_key: ?[]const u8 = null,
    vector: []const f32,
};

pub const DerivedSparseEmbeddingWrite = struct {
    index_name: []const u8,
    doc_key: []const u8,
    artifact_key: ?[]const u8 = null,
    indices: []const u32,
    values: []const f32,
};

pub const DerivedBatch = struct {
    sequence: u64 = 0,
    documents: []const DerivedDocument = &.{},
    deleted_keys: []const []const u8 = &.{},
    overwritten_doc_keys: []const []const u8 = &.{},
    changed_artifact_keys: []const []const u8 = &.{},
    graph_doc_clears: []const DerivedGraphDocClear = &.{},
    dense_embeddings: []const DerivedDenseEmbeddingWrite = &.{},
    sparse_embeddings: []const DerivedSparseEmbeddingWrite = &.{},
    generated_enrichment_refs: []const enrichment_types.GeneratedEnrichmentRef = &.{},
    graph_writes: []const types.GraphEdgeWrite = &.{},
    graph_deletes: []const types.GraphEdgeDelete = &.{},
};

pub const DerivedLogRecord = struct {
    version: u16 = 1,
    batch: DerivedBatch,
};

pub const DecodedLogRecord = struct {
    alloc: Allocator,
    version: u16,
    batch: DerivedBatch,
    parsed: ?std.json.Parsed(DerivedLogRecord) = null,

    pub fn deinit(self: *DecodedLogRecord) void {
        if (self.parsed) |*parsed| {
            parsed.deinit();
        } else {
            deinitDerivedBatch(self.alloc, &self.batch);
        }
        self.* = undefined;
    }
};

pub fn deinitDerivedBatch(alloc: Allocator, batch: *DerivedBatch) void {
    for (batch.documents) |doc| {
        alloc.free(doc.key);
        if (doc.cleaned_value) |value| alloc.free(value);
        for (doc.targets) |target| alloc.free(target.index_name);
        if (doc.targets.len > 0) alloc.free(doc.targets);
    }
    if (batch.documents.len > 0) alloc.free(batch.documents);

    for (batch.deleted_keys) |key| alloc.free(key);
    if (batch.deleted_keys.len > 0) alloc.free(batch.deleted_keys);

    for (batch.overwritten_doc_keys) |key| alloc.free(key);
    if (batch.overwritten_doc_keys.len > 0) alloc.free(batch.overwritten_doc_keys);

    for (batch.changed_artifact_keys) |key| alloc.free(key);
    if (batch.changed_artifact_keys.len > 0) alloc.free(batch.changed_artifact_keys);

    for (batch.graph_doc_clears) |clear| {
        alloc.free(clear.key);
        for (clear.index_names) |index_name| alloc.free(index_name);
        if (clear.index_names.len > 0) alloc.free(clear.index_names);
    }
    if (batch.graph_doc_clears.len > 0) alloc.free(batch.graph_doc_clears);

    for (batch.dense_embeddings) |embedding| {
        alloc.free(embedding.index_name);
        if (embedding.parent_doc_key) |parent_doc_key| alloc.free(parent_doc_key);
        alloc.free(embedding.doc_key);
        if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
        if (embedding.vector.len > 0) alloc.free(embedding.vector);
    }
    if (batch.dense_embeddings.len > 0) alloc.free(batch.dense_embeddings);

    for (batch.sparse_embeddings) |embedding| {
        alloc.free(embedding.index_name);
        alloc.free(embedding.doc_key);
        if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
        if (embedding.indices.len > 0) alloc.free(embedding.indices);
        if (embedding.values.len > 0) alloc.free(embedding.values);
    }
    if (batch.sparse_embeddings.len > 0) alloc.free(batch.sparse_embeddings);

    enrichment_types.deinitGeneratedRefs(alloc, batch.generated_enrichment_refs);

    for (batch.graph_writes) |write| {
        alloc.free(@constCast(write.index_name));
        alloc.free(@constCast(write.source));
        alloc.free(@constCast(write.target));
        alloc.free(@constCast(write.edge_type));
        if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
    }
    if (batch.graph_writes.len > 0) alloc.free(batch.graph_writes);

    for (batch.graph_deletes) |delete| {
        alloc.free(@constCast(delete.index_name));
        alloc.free(@constCast(delete.source));
        alloc.free(@constCast(delete.target));
        alloc.free(@constCast(delete.edge_type));
    }
    if (batch.graph_deletes.len > 0) alloc.free(batch.graph_deletes);

    batch.* = undefined;
}

pub fn cloneBatch(alloc: Allocator, batch: DerivedBatch) !DerivedBatch {
    var docs = try alloc.alloc(DerivedDocument, batch.documents.len);
    var initialized_docs: usize = 0;
    errdefer {
        var tmp = DerivedBatch{ .documents = docs[0..initialized_docs] };
        deinitDerivedBatch(alloc, &tmp);
    }

    for (batch.documents, 0..) |doc, i| {
        var targets = try alloc.alloc(DerivedTargetRef, doc.targets.len);
        for (doc.targets, 0..) |target, j| {
            targets[j] = .{
                .kind = target.kind,
                .index_name = try alloc.dupe(u8, target.index_name),
            };
        }
        docs[i] = .{
            .key = try alloc.dupe(u8, doc.key),
            .action = doc.action,
            .cleaned_value = if (doc.cleaned_value) |value| try alloc.dupe(u8, value) else null,
            .targets = targets,
        };
        initialized_docs += 1;
    }

    var deleted = try alloc.alloc([]const u8, batch.deleted_keys.len);
    var initialized_deleted: usize = 0;
    errdefer {
        for (deleted[0..initialized_deleted]) |key| alloc.free(key);
        alloc.free(deleted);
    }
    for (batch.deleted_keys, 0..) |key, i| {
        deleted[i] = try alloc.dupe(u8, key);
        initialized_deleted += 1;
    }

    var overwritten = try alloc.alloc([]const u8, batch.overwritten_doc_keys.len);
    var initialized_overwritten: usize = 0;
    errdefer {
        for (overwritten[0..initialized_overwritten]) |key| alloc.free(key);
        alloc.free(overwritten);
    }
    for (batch.overwritten_doc_keys, 0..) |key, i| {
        overwritten[i] = try alloc.dupe(u8, key);
        initialized_overwritten += 1;
    }

    var changed_artifact_keys = try alloc.alloc([]const u8, batch.changed_artifact_keys.len);
    var initialized_changed_artifact_keys: usize = 0;
    errdefer {
        for (changed_artifact_keys[0..initialized_changed_artifact_keys]) |key| alloc.free(key);
        alloc.free(changed_artifact_keys);
    }
    for (batch.changed_artifact_keys, 0..) |key, i| {
        changed_artifact_keys[i] = try alloc.dupe(u8, key);
        initialized_changed_artifact_keys += 1;
    }

    var graph_doc_clears = try alloc.alloc(DerivedGraphDocClear, batch.graph_doc_clears.len);
    var initialized_graph_doc_clears: usize = 0;
    errdefer {
        var tmp = DerivedBatch{
            .graph_doc_clears = graph_doc_clears[0..initialized_graph_doc_clears],
        };
        deinitDerivedBatch(alloc, &tmp);
    }
    for (batch.graph_doc_clears, 0..) |clear, i| {
        var index_names = try alloc.alloc([]const u8, clear.index_names.len);
        for (clear.index_names, 0..) |index_name, j| {
            index_names[j] = try alloc.dupe(u8, index_name);
        }
        graph_doc_clears[i] = .{
            .key = try alloc.dupe(u8, clear.key),
            .index_names = index_names,
        };
        initialized_graph_doc_clears += 1;
    }

    var dense_embeddings = try alloc.alloc(DerivedDenseEmbeddingWrite, batch.dense_embeddings.len);
    var initialized_dense: usize = 0;
    errdefer {
        var tmp = DerivedBatch{
            .dense_embeddings = dense_embeddings[0..initialized_dense],
        };
        deinitDerivedBatch(alloc, &tmp);
    }
    for (batch.dense_embeddings, 0..) |embedding, i| {
        dense_embeddings[i] = .{
            .index_name = try alloc.dupe(u8, embedding.index_name),
            .parent_doc_key = if (embedding.parent_doc_key) |parent_doc_key| try alloc.dupe(u8, parent_doc_key) else null,
            .doc_key = try alloc.dupe(u8, embedding.doc_key),
            .artifact_key = if (embedding.artifact_key) |artifact_key| try alloc.dupe(u8, artifact_key) else null,
            .vector = try alloc.dupe(f32, embedding.vector),
        };
        initialized_dense += 1;
    }

    var sparse_embeddings = try alloc.alloc(DerivedSparseEmbeddingWrite, batch.sparse_embeddings.len);
    var initialized_sparse: usize = 0;
    errdefer {
        var tmp = DerivedBatch{
            .sparse_embeddings = sparse_embeddings[0..initialized_sparse],
        };
        deinitDerivedBatch(alloc, &tmp);
    }
    for (batch.sparse_embeddings, 0..) |embedding, i| {
        sparse_embeddings[i] = .{
            .index_name = try alloc.dupe(u8, embedding.index_name),
            .doc_key = try alloc.dupe(u8, embedding.doc_key),
            .artifact_key = if (embedding.artifact_key) |artifact_key| try alloc.dupe(u8, artifact_key) else null,
            .indices = try alloc.dupe(u32, embedding.indices),
            .values = try alloc.dupe(f32, embedding.values),
        };
        initialized_sparse += 1;
    }

    const generated_enrichment_refs = try enrichment_types.cloneGeneratedRefs(alloc, batch.generated_enrichment_refs);
    errdefer enrichment_types.deinitGeneratedRefs(alloc, generated_enrichment_refs);

    var graph_writes = try alloc.alloc(types.GraphEdgeWrite, batch.graph_writes.len);
    var initialized_graph_writes: usize = 0;
    errdefer {
        var tmp = DerivedBatch{
            .graph_writes = graph_writes[0..initialized_graph_writes],
        };
        deinitDerivedBatch(alloc, &tmp);
    }
    for (batch.graph_writes, 0..) |write, i| {
        graph_writes[i] = .{
            .index_name = try alloc.dupe(u8, write.index_name),
            .source = try alloc.dupe(u8, write.source),
            .target = try alloc.dupe(u8, write.target),
            .edge_type = try alloc.dupe(u8, write.edge_type),
            .weight = write.weight,
            .created_at = write.created_at,
            .updated_at = write.updated_at,
            .metadata_json = if (write.metadata_json.len > 0)
                try alloc.dupe(u8, write.metadata_json)
            else
                "",
        };
        initialized_graph_writes += 1;
    }

    var graph_deletes = try alloc.alloc(types.GraphEdgeDelete, batch.graph_deletes.len);
    var initialized_graph_deletes: usize = 0;
    errdefer {
        var tmp = DerivedBatch{
            .graph_deletes = graph_deletes[0..initialized_graph_deletes],
        };
        deinitDerivedBatch(alloc, &tmp);
    }
    for (batch.graph_deletes, 0..) |delete, i| {
        graph_deletes[i] = .{
            .index_name = try alloc.dupe(u8, delete.index_name),
            .source = try alloc.dupe(u8, delete.source),
            .target = try alloc.dupe(u8, delete.target),
            .edge_type = try alloc.dupe(u8, delete.edge_type),
        };
        initialized_graph_deletes += 1;
    }

    return .{
        .sequence = batch.sequence,
        .documents = docs,
        .deleted_keys = deleted,
        .overwritten_doc_keys = overwritten,
        .changed_artifact_keys = changed_artifact_keys,
        .graph_doc_clears = graph_doc_clears,
        .dense_embeddings = dense_embeddings,
        .sparse_embeddings = sparse_embeddings,
        .generated_enrichment_refs = generated_enrichment_refs,
        .graph_writes = graph_writes,
        .graph_deletes = graph_deletes,
    };
}

const binary_magic = "ADLG";
const binary_version: u16 = 5;
const min_supported_binary_version: u16 = 2;

pub fn encodeLogRecord(alloc: Allocator, batch: DerivedBatch) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    _ = try encodeLogRecordInto(alloc, &out, batch);
    return try out.toOwnedSlice(alloc);
}

pub fn encodeLogRecordInto(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), batch: DerivedBatch) ![]const u8 {
    out.clearRetainingCapacity();

    try out.appendSlice(alloc, binary_magic);
    try appendInt(out, alloc, u16, binary_version);
    try appendInt(out, alloc, u64, batch.sequence);

    try appendInt(out, alloc, u32, @intCast(batch.documents.len));
    for (batch.documents) |doc| {
        try writeBytes(out, alloc, doc.key);
        try out.append(alloc, @intFromEnum(doc.action));
        try writeOptionalBytes(out, alloc, doc.cleaned_value);
        try appendInt(out, alloc, u32, @intCast(doc.targets.len));
        for (doc.targets) |target| {
            try out.append(alloc, @intFromEnum(target.kind));
            try writeBytes(out, alloc, target.index_name);
        }
    }

    try writeStringSlice(out, alloc, batch.deleted_keys);
    try writeStringSlice(out, alloc, batch.overwritten_doc_keys);

    try appendInt(out, alloc, u32, @intCast(batch.graph_doc_clears.len));
    for (batch.graph_doc_clears) |clear| {
        try writeBytes(out, alloc, clear.key);
        try writeStringSlice(out, alloc, clear.index_names);
    }

    try appendInt(out, alloc, u32, @intCast(batch.dense_embeddings.len));
    for (batch.dense_embeddings) |embedding| {
        try writeBytes(out, alloc, embedding.index_name);
        try writeBytes(out, alloc, embedding.doc_key);
        try writeOptionalBytes(out, alloc, embedding.parent_doc_key);
        try writeOptionalBytes(out, alloc, embedding.artifact_key);
        try appendInt(out, alloc, u32, @intCast(embedding.vector.len));
        try out.appendSlice(alloc, std.mem.sliceAsBytes(embedding.vector));
    }

    try appendInt(out, alloc, u32, @intCast(batch.sparse_embeddings.len));
    for (batch.sparse_embeddings) |embedding| {
        try writeBytes(out, alloc, embedding.index_name);
        try writeBytes(out, alloc, embedding.doc_key);
        try writeOptionalBytes(out, alloc, embedding.artifact_key);
        try appendInt(out, alloc, u32, @intCast(embedding.indices.len));
        try out.appendSlice(alloc, std.mem.sliceAsBytes(embedding.indices));
        try appendInt(out, alloc, u32, @intCast(embedding.values.len));
        try out.appendSlice(alloc, std.mem.sliceAsBytes(embedding.values));
    }

    try appendInt(out, alloc, u32, @intCast(batch.generated_enrichment_refs.len));
    for (batch.generated_enrichment_refs) |request| {
        try out.append(alloc, @intFromEnum(request.kind));
        try writeBytes(out, alloc, request.index_name);
        try writeBytes(out, alloc, request.artifact_name);
        try writeBytes(out, alloc, request.embedding_name);
        try writeBytes(out, alloc, request.doc_key);
    }

    try appendInt(out, alloc, u32, @intCast(batch.graph_writes.len));
    for (batch.graph_writes) |write| {
        try writeBytes(out, alloc, write.index_name);
        try writeBytes(out, alloc, write.source);
        try writeBytes(out, alloc, write.target);
        try writeBytes(out, alloc, write.edge_type);
        try appendInt(out, alloc, u64, @bitCast(write.weight));
        try appendInt(out, alloc, u64, write.created_at);
        try appendInt(out, alloc, u64, write.updated_at);
        try writeBytes(out, alloc, write.metadata_json);
    }

    try appendInt(out, alloc, u32, @intCast(batch.graph_deletes.len));
    for (batch.graph_deletes) |delete| {
        try writeBytes(out, alloc, delete.index_name);
        try writeBytes(out, alloc, delete.source);
        try writeBytes(out, alloc, delete.target);
        try writeBytes(out, alloc, delete.edge_type);
    }

    return out.items;
}

pub fn decodeLogRecord(alloc: Allocator, payload: []const u8) !DecodedLogRecord {
    if (std.mem.startsWith(u8, payload, binary_magic)) {
        return try decodeBinaryLogRecord(alloc, payload);
    }

    const parsed = try std.json.parseFromSlice(DerivedLogRecord, alloc, payload, .{
        .allocate = .alloc_always,
    });
    return .{
        .alloc = alloc,
        .version = parsed.value.version,
        .batch = parsed.value.batch,
        .parsed = parsed,
    };
}

fn appendInt(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, comptime T: type, value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try out.appendSlice(alloc, &buf);
}

fn writeBytes(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, bytes: []const u8) !void {
    try appendInt(out, alloc, u32, @intCast(bytes.len));
    try out.appendSlice(alloc, bytes);
}

fn writeOptionalBytes(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, bytes: ?[]const u8) !void {
    if (bytes) |value| {
        try out.append(alloc, 1);
        try writeBytes(out, alloc, value);
    } else {
        try out.append(alloc, 0);
    }
}

fn writeStringSlice(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const []const u8) !void {
    try appendInt(out, alloc, u32, @intCast(values.len));
    for (values) |value| try writeBytes(out, alloc, value);
}

const SliceReader = struct {
    bytes: []const u8,
    offset: usize = 0,

    fn readInt(self: *SliceReader, comptime T: type) !T {
        const size = @sizeOf(T);
        if (self.offset + size > self.bytes.len) return error.EndOfStream;
        const buf: *const [size]u8 = @ptrCast(self.bytes[self.offset..][0..size]);
        const value = std.mem.readInt(T, buf, .little);
        self.offset += size;
        return value;
    }

    fn readByte(self: *SliceReader) !u8 {
        if (self.offset >= self.bytes.len) return error.EndOfStream;
        const value = self.bytes[self.offset];
        self.offset += 1;
        return value;
    }

    fn readBytesAlloc(self: *SliceReader, alloc: Allocator) ![]u8 {
        const len = try self.readInt(u32);
        if (self.offset + len > self.bytes.len) return error.EndOfStream;
        const out = try alloc.dupe(u8, self.bytes[self.offset .. self.offset + len]);
        self.offset += len;
        return out;
    }

    fn readBytesOrEmpty(self: *SliceReader, alloc: Allocator) ![]const u8 {
        const len = try self.readInt(u32);
        if (self.offset + len > self.bytes.len) return error.EndOfStream;
        defer self.offset += len;
        if (len == 0) return "";
        return try alloc.dupe(u8, self.bytes[self.offset .. self.offset + len]);
    }

    fn readMaybeBytesAlloc(self: *SliceReader, alloc: Allocator) !?[]u8 {
        return if ((try self.readByte()) == 0) null else try self.readBytesAlloc(alloc);
    }

    fn readF32SliceAlloc(self: *SliceReader, alloc: Allocator) ![]f32 {
        const len = try self.readInt(u32);
        const byte_len = len * @sizeOf(f32);
        if (self.offset + byte_len > self.bytes.len) return error.EndOfStream;
        const out = try alloc.alloc(f32, len);
        errdefer alloc.free(out);
        @memcpy(std.mem.sliceAsBytes(out), self.bytes[self.offset .. self.offset + byte_len]);
        self.offset += byte_len;
        return out;
    }

    fn readU32SliceAlloc(self: *SliceReader, alloc: Allocator) ![]u32 {
        const len = try self.readInt(u32);
        const byte_len = len * @sizeOf(u32);
        if (self.offset + byte_len > self.bytes.len) return error.EndOfStream;
        const out = try alloc.alloc(u32, len);
        errdefer alloc.free(out);
        @memcpy(std.mem.sliceAsBytes(out), self.bytes[self.offset .. self.offset + byte_len]);
        self.offset += byte_len;
        return out;
    }

    fn readStringSliceAlloc(self: *SliceReader, alloc: Allocator) ![]const []const u8 {
        const len = try self.readInt(u32);
        const out = try alloc.alloc([]const u8, len);
        errdefer alloc.free(out);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |item| alloc.free(@constCast(item));
        }
        for (0..len) |i| {
            out[i] = try self.readBytesAlloc(alloc);
            initialized += 1;
        }
        return out;
    }
};

fn decodeBinaryLogRecord(alloc: Allocator, payload: []const u8) !DecodedLogRecord {
    if (payload.len < binary_magic.len + @sizeOf(u16)) return error.EndOfStream;
    if (!std.mem.eql(u8, payload[0..binary_magic.len], binary_magic)) return error.InvalidDerivedLogRecord;
    var reader = SliceReader{
        .bytes = payload,
        .offset = binary_magic.len,
    };
    const version = try reader.readInt(u16);
    if (version < min_supported_binary_version or version > binary_version) return error.UnsupportedDerivedLogVersion;

    var batch = DerivedBatch{
        .sequence = try reader.readInt(u64),
    };
    errdefer deinitDerivedBatch(alloc, &batch);

    const document_count = try reader.readInt(u32);
    const documents = try alloc.alloc(DerivedDocument, document_count);
    errdefer alloc.free(documents);
    var initialized_docs: usize = 0;
    errdefer {
        for (documents[0..initialized_docs]) |doc| {
            alloc.free(doc.key);
            if (doc.cleaned_value) |value| alloc.free(value);
            for (doc.targets) |target| alloc.free(target.index_name);
            if (doc.targets.len > 0) alloc.free(doc.targets);
        }
    }
    for (documents) |*doc| {
        const key = try reader.readBytesAlloc(alloc);
        errdefer alloc.free(key);
        const action: DerivedAction = @enumFromInt(try reader.readByte());
        const cleaned_value = try reader.readMaybeBytesAlloc(alloc);
        errdefer if (cleaned_value) |value| alloc.free(value);
        const target_count = try reader.readInt(u32);
        const targets = try alloc.alloc(DerivedTargetRef, target_count);
        errdefer alloc.free(targets);
        var initialized_targets: usize = 0;
        errdefer {
            for (targets[0..initialized_targets]) |target| alloc.free(target.index_name);
        }
        for (targets) |*target| {
            target.* = .{
                .kind = @enumFromInt(try reader.readByte()),
                .index_name = try reader.readBytesAlloc(alloc),
            };
            initialized_targets += 1;
        }
        doc.* = .{
            .key = key,
            .action = action,
            .cleaned_value = cleaned_value,
            .targets = targets,
        };
        initialized_docs += 1;
    }
    batch.documents = documents;

    batch.deleted_keys = try reader.readStringSliceAlloc(alloc);
    batch.overwritten_doc_keys = try reader.readStringSliceAlloc(alloc);

    const clear_count = try reader.readInt(u32);
    const graph_doc_clears = try alloc.alloc(DerivedGraphDocClear, clear_count);
    errdefer alloc.free(graph_doc_clears);
    var initialized_clears: usize = 0;
    errdefer {
        for (graph_doc_clears[0..initialized_clears]) |clear| {
            alloc.free(clear.key);
            for (clear.index_names) |index_name| alloc.free(index_name);
            if (clear.index_names.len > 0) alloc.free(clear.index_names);
        }
    }
    for (graph_doc_clears) |*clear| {
        clear.* = .{
            .key = try reader.readBytesAlloc(alloc),
            .index_names = try reader.readStringSliceAlloc(alloc),
        };
        initialized_clears += 1;
    }
    batch.graph_doc_clears = graph_doc_clears;

    const dense_count = try reader.readInt(u32);
    const dense_embeddings = try alloc.alloc(DerivedDenseEmbeddingWrite, dense_count);
    errdefer alloc.free(dense_embeddings);
    var initialized_dense: usize = 0;
    errdefer {
        for (dense_embeddings[0..initialized_dense]) |embedding| {
            alloc.free(embedding.index_name);
            if (embedding.parent_doc_key) |parent_doc_key| alloc.free(parent_doc_key);
            alloc.free(embedding.doc_key);
            if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
            if (embedding.vector.len > 0) alloc.free(embedding.vector);
        }
    }
    for (dense_embeddings) |*embedding| {
        embedding.* = .{
            .index_name = try reader.readBytesAlloc(alloc),
            .doc_key = try reader.readBytesAlloc(alloc),
            .parent_doc_key = if (version >= 3) try reader.readMaybeBytesAlloc(alloc) else null,
            .artifact_key = try reader.readMaybeBytesAlloc(alloc),
            .vector = try reader.readF32SliceAlloc(alloc),
        };
        initialized_dense += 1;
    }
    batch.dense_embeddings = dense_embeddings;

    const sparse_count = try reader.readInt(u32);
    const sparse_embeddings = try alloc.alloc(DerivedSparseEmbeddingWrite, sparse_count);
    errdefer alloc.free(sparse_embeddings);
    var initialized_sparse: usize = 0;
    errdefer {
        for (sparse_embeddings[0..initialized_sparse]) |embedding| {
            alloc.free(embedding.index_name);
            alloc.free(embedding.doc_key);
            if (embedding.artifact_key) |artifact_key| alloc.free(artifact_key);
            if (embedding.indices.len > 0) alloc.free(embedding.indices);
            if (embedding.values.len > 0) alloc.free(embedding.values);
        }
    }
    for (sparse_embeddings) |*embedding| {
        embedding.* = .{
            .index_name = try reader.readBytesAlloc(alloc),
            .doc_key = try reader.readBytesAlloc(alloc),
            .artifact_key = if (version >= 4) try reader.readMaybeBytesAlloc(alloc) else null,
            .indices = try reader.readU32SliceAlloc(alloc),
            .values = try reader.readF32SliceAlloc(alloc),
        };
        initialized_sparse += 1;
    }
    batch.sparse_embeddings = sparse_embeddings;

    const generated_count = try reader.readInt(u32);
    const generated_enrichment_refs = try alloc.alloc(enrichment_types.GeneratedEnrichmentRef, generated_count);
    errdefer alloc.free(generated_enrichment_refs);
    var initialized_generated: usize = 0;
    errdefer {
        for (generated_enrichment_refs[0..initialized_generated]) |request| {
            enrichment_types.freeGeneratedRef(alloc, request);
        }
    }
    for (generated_enrichment_refs) |*request| {
        request.* = .{
            .kind = @enumFromInt(try reader.readByte()),
            .index_name = try reader.readBytesAlloc(alloc),
            .artifact_name = try reader.readBytesOrEmpty(alloc),
            .embedding_name = try reader.readBytesOrEmpty(alloc),
            .doc_key = try reader.readBytesAlloc(alloc),
        };
        if (version < 5) {
            const source_field = try reader.readBytesAlloc(alloc);
            defer alloc.free(source_field);
            const source_template = try reader.readBytesOrEmpty(alloc);
            defer if (source_template.len > 0) alloc.free(source_template);
            _ = try reader.readInt(u32);
            _ = try reader.readInt(u32);
            _ = try reader.readInt(u32);
            const chunker_json = try reader.readBytesOrEmpty(alloc);
            defer if (chunker_json.len > 0) alloc.free(chunker_json);
        }
        initialized_generated += 1;
    }
    batch.generated_enrichment_refs = generated_enrichment_refs;

    const graph_write_count = try reader.readInt(u32);
    const graph_writes = try alloc.alloc(types.GraphEdgeWrite, graph_write_count);
    errdefer alloc.free(graph_writes);
    var initialized_graph_writes: usize = 0;
    errdefer {
        for (graph_writes[0..initialized_graph_writes]) |write| {
            alloc.free(write.index_name);
            alloc.free(write.source);
            alloc.free(write.target);
            alloc.free(write.edge_type);
            if (write.metadata_json.len > 0) alloc.free(write.metadata_json);
        }
    }
    for (graph_writes) |*write| {
        write.* = .{
            .index_name = try reader.readBytesAlloc(alloc),
            .source = try reader.readBytesAlloc(alloc),
            .target = try reader.readBytesAlloc(alloc),
            .edge_type = try reader.readBytesAlloc(alloc),
            .weight = @bitCast(try reader.readInt(u64)),
            .created_at = try reader.readInt(u64),
            .updated_at = try reader.readInt(u64),
            .metadata_json = try reader.readBytesOrEmpty(alloc),
        };
        initialized_graph_writes += 1;
    }
    batch.graph_writes = graph_writes;

    const graph_delete_count = try reader.readInt(u32);
    const graph_deletes = try alloc.alloc(types.GraphEdgeDelete, graph_delete_count);
    errdefer alloc.free(graph_deletes);
    var initialized_graph_deletes: usize = 0;
    errdefer {
        for (graph_deletes[0..initialized_graph_deletes]) |delete| {
            alloc.free(delete.index_name);
            alloc.free(delete.source);
            alloc.free(delete.target);
            alloc.free(delete.edge_type);
        }
    }
    for (graph_deletes) |*delete| {
        delete.* = .{
            .index_name = try reader.readBytesAlloc(alloc),
            .source = try reader.readBytesAlloc(alloc),
            .target = try reader.readBytesAlloc(alloc),
            .edge_type = try reader.readBytesAlloc(alloc),
        };
        initialized_graph_deletes += 1;
    }
    batch.graph_deletes = graph_deletes;

    return .{
        .alloc = alloc,
        .version = version,
        .batch = batch,
    };
}

test "derived log record binary round trips" {
    const alloc = std.testing.allocator;
    const payload = try encodeLogRecord(alloc, .{
        .sequence = 42,
        .documents = &.{
            .{
                .key = "doc:a",
                .action = .upsert,
                .cleaned_value = "{\"body\":\"alpha\"}",
                .targets = &.{
                    .{ .kind = .dense_vector, .index_name = "dv_v1" },
                    .{ .kind = .graph, .index_name = "gr_v1" },
                },
            },
        },
        .deleted_keys = &.{"doc:old"},
        .overwritten_doc_keys = &.{"doc:stale"},
        .graph_doc_clears = &.{
            .{ .key = "doc:a", .index_names = &.{"gr_v1"} },
        },
        .dense_embeddings = &.{
            .{ .index_name = "dv_v1", .parent_doc_key = "doc:a", .doc_key = "chunk:a:0", .vector = &.{ 1.0, 2.0, 3.0 } },
        },
        .sparse_embeddings = &.{
            .{ .index_name = "sp_v1", .doc_key = "doc:a", .indices = &.{ 1, 5 }, .values = &.{ 0.5, 0.75 } },
        },
        .generated_enrichment_refs = &.{
            .{
                .kind = .dense_embedding,
                .index_name = "dv_v1",
                .artifact_name = "body_chunks_v1",
                .embedding_name = "body_dense_v1",
                .doc_key = "doc:a",
            },
        },
        .graph_writes = &.{
            .{ .index_name = "gr_v1", .source = "doc:a", .target = "doc:b", .edge_type = "cites", .weight = 2.0 },
        },
        .graph_deletes = &.{
            .{ .index_name = "gr_v1", .source = "doc:b", .target = "doc:c", .edge_type = "replies" },
        },
    });
    defer alloc.free(payload);

    var decoded = try decodeLogRecord(alloc, payload);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u16, binary_version), decoded.version);
    try std.testing.expectEqual(@as(u64, 42), decoded.batch.sequence);
    try std.testing.expectEqualStrings("doc:a", decoded.batch.documents[0].key);
    try std.testing.expectEqualStrings("dv_v1", decoded.batch.documents[0].targets[0].index_name);
    try std.testing.expectEqualStrings("doc:old", decoded.batch.deleted_keys[0]);
    try std.testing.expectEqualStrings("doc:a", decoded.batch.dense_embeddings[0].parent_doc_key.?);
    try std.testing.expectEqualStrings("chunk:a:0", decoded.batch.dense_embeddings[0].doc_key);
    try std.testing.expectEqual(@as(f32, 3.0), decoded.batch.dense_embeddings[0].vector[2]);
    try std.testing.expectEqual(@as(u32, 5), decoded.batch.sparse_embeddings[0].indices[1]);
    try std.testing.expectEqualStrings("body_chunks_v1", decoded.batch.generated_enrichment_refs[0].artifact_name);
    try std.testing.expectEqual(@as(f64, 2.0), decoded.batch.graph_writes[0].weight);
    try std.testing.expectEqualStrings("replies", decoded.batch.graph_deletes[0].edge_type);
}

test "derived log record decodes legacy json payloads" {
    const alloc = std.testing.allocator;
    const payload = try std.json.Stringify.valueAlloc(alloc, DerivedLogRecord{
        .version = 1,
        .batch = .{
            .sequence = 7,
            .documents = &.{
                .{
                    .key = "doc:legacy",
                    .action = .upsert,
                    .cleaned_value = "{\"body\":\"legacy\"}",
                },
            },
        },
    }, .{});
    defer alloc.free(payload);

    var decoded = try decodeLogRecord(alloc, payload);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u16, 1), decoded.version);
    try std.testing.expectEqual(@as(u64, 7), decoded.batch.sequence);
    try std.testing.expectEqualStrings("doc:legacy", decoded.batch.documents[0].key);
}

test "derived batch deinit tolerates artifact-backed empty payload slices" {
    const alloc = std.testing.allocator;

    var dense = try alloc.alloc(DerivedDenseEmbeddingWrite, 1);
    dense[0] = .{
        .index_name = try alloc.dupe(u8, "dv_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_key = try alloc.dupe(u8, "artifact:dense:doc:a"),
        .vector = &.{},
    };

    var sparse = try alloc.alloc(DerivedSparseEmbeddingWrite, 1);
    sparse[0] = .{
        .index_name = try alloc.dupe(u8, "sp_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_key = try alloc.dupe(u8, "artifact:sparse:doc:a"),
        .indices = &.{},
        .values = &.{},
    };

    var batch: DerivedBatch = .{
        .dense_embeddings = dense,
        .sparse_embeddings = sparse,
    };
    deinitDerivedBatch(alloc, &batch);
}
