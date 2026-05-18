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
const derived_log_mod = @import("derived_log.zig");
const derived_types = @import("derived_types.zig");
const internal_keys = @import("../../internal_keys.zig");
const resource_manager_mod = @import("../../resource_manager.zig");

pub const Journal = derived_log_mod.DerivedLog;
pub const StorageBackend = derived_log_mod.StorageBackend;
pub const OpenOptions = derived_log_mod.OpenOptions;

pub const TargetHint = enum {
    enrichment,
    full_text,
    dense_vector,
    sparse_vector,
    graph,
    algebraic,
};

pub const Record = struct {
    version: u16 = 1,
    sequence: u64 = 0,
    changed_doc_keys: []const []const u8 = &.{},
    deleted_doc_keys: []const []const u8 = &.{},
    overwritten_doc_keys: []const []const u8 = &.{},
    changed_artifact_keys: []const []const u8 = &.{},
    target_hints: []const TargetHint = &.{},

    pub fn jsonStringify(self: Record, jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("sequence");
        try jw.write(self.sequence);
        if (self.version != 1) {
            try jw.objectField("version");
            try jw.write(self.version);
        }
        if (self.target_hints.len > 0) {
            try jw.objectField("target_hints");
            try jw.write(self.target_hints);
        }
        if (self.changed_doc_keys.len > 0) {
            try jw.objectField("changed_doc_keys");
            try jw.write(self.changed_doc_keys);
        }
        if (self.deleted_doc_keys.len > 0) {
            try jw.objectField("deleted_doc_keys");
            try jw.write(self.deleted_doc_keys);
        }
        if (self.overwritten_doc_keys.len > 0) {
            try jw.objectField("overwritten_doc_keys");
            try jw.write(self.overwritten_doc_keys);
        }
        if (self.changed_artifact_keys.len > 0) {
            try jw.objectField("changed_artifact_keys");
            try jw.write(self.changed_artifact_keys);
        }
        try jw.endObject();
    }
};

pub const DecodedRecord = struct {
    alloc: Allocator,
    record: Record,
    parsed: ?std.json.Parsed(Record) = null,

    pub fn deinit(self: *DecodedRecord) void {
        if (self.parsed) |*parsed| {
            parsed.deinit();
        } else {
            deinitRecord(self.alloc, &self.record);
        }
        self.* = undefined;
    }
};

pub const BorrowedBinaryRecord = struct {
    alloc: Allocator,
    record: Record,

    pub fn deinit(self: *BorrowedBinaryRecord) void {
        if (self.record.changed_doc_keys.len > 0) self.alloc.free(self.record.changed_doc_keys);
        if (self.record.deleted_doc_keys.len > 0) self.alloc.free(self.record.deleted_doc_keys);
        if (self.record.overwritten_doc_keys.len > 0) self.alloc.free(self.record.overwritten_doc_keys);
        if (self.record.changed_artifact_keys.len > 0) self.alloc.free(self.record.changed_artifact_keys);
        freeTargetHintsIfOwned(self.alloc, self.record.target_hints);
        self.* = undefined;
    }
};

const binary_magic = "CJ2\x00";
const FieldMask = packed struct(u8) {
    changed_doc_keys: bool = false,
    deleted_doc_keys: bool = false,
    overwritten_doc_keys: bool = false,
    changed_artifact_keys: bool = false,
    _padding: u4 = 0,
};

pub fn deinitRecord(alloc: Allocator, record: *Record) void {
    for (record.changed_doc_keys) |key| alloc.free(key);
    if (record.changed_doc_keys.len > 0) alloc.free(record.changed_doc_keys);

    for (record.deleted_doc_keys) |key| alloc.free(key);
    if (record.deleted_doc_keys.len > 0) alloc.free(record.deleted_doc_keys);

    for (record.overwritten_doc_keys) |key| alloc.free(key);
    if (record.overwritten_doc_keys.len > 0) alloc.free(record.overwritten_doc_keys);

    for (record.changed_artifact_keys) |key| alloc.free(key);
    if (record.changed_artifact_keys.len > 0) alloc.free(record.changed_artifact_keys);

    freeTargetHintsIfOwned(alloc, record.target_hints);
    record.* = undefined;
}

fn appendUniqueString(
    alloc: Allocator,
    list: *std.ArrayListUnmanaged([]const u8),
    value: []const u8,
) !void {
    if (value.len == 0) return;
    for (list.items) |existing| {
        if (std.mem.eql(u8, existing, value)) return;
    }
    const owned = try alloc.dupe(u8, value);
    errdefer alloc.free(owned);
    try list.append(alloc, owned);
}

pub fn recordFromDerivedBatch(alloc: Allocator, batch: derived_types.DerivedBatch, sequence: u64) !Record {
    var changed_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (changed_doc_keys.items) |key| alloc.free(key);
        changed_doc_keys.deinit(alloc);
    }
    var deleted_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (deleted_doc_keys.items) |key| alloc.free(key);
        deleted_doc_keys.deinit(alloc);
    }
    var overwritten_doc_keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (overwritten_doc_keys.items) |key| alloc.free(key);
        overwritten_doc_keys.deinit(alloc);
    }
    var changed_artifact_keys = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (changed_artifact_keys.items) |key| alloc.free(key);
        changed_artifact_keys.deinit(alloc);
    }
    var target_hints = std.ArrayListUnmanaged(TargetHint).empty;
    errdefer target_hints.deinit(alloc);

    for (batch.documents) |doc| {
        if (doc.action == .upsert) try appendUniqueString(alloc, &changed_doc_keys, doc.key);
        if (doc.action == .upsert) {
            try appendUniqueHintAlloc(alloc, &target_hints, .dense_vector);
            try appendUniqueHintAlloc(alloc, &target_hints, .sparse_vector);
            try appendUniqueHintAlloc(alloc, &target_hints, .algebraic);
        }
        for (doc.targets) |target| {
            switch (target.kind) {
                .full_text => try target_hints.append(alloc, .full_text),
                .dense_vector => try target_hints.append(alloc, .dense_vector),
                .sparse_vector => try target_hints.append(alloc, .sparse_vector),
                .graph => try target_hints.append(alloc, .graph),
                .algebraic => try target_hints.append(alloc, .algebraic),
            }
        }
    }
    for (batch.deleted_keys) |key| try appendUniqueString(alloc, &deleted_doc_keys, key);
    for (batch.overwritten_doc_keys) |key| try appendUniqueString(alloc, &overwritten_doc_keys, key);
    if (batch.deleted_keys.len > 0) {
        try appendUniqueHintAlloc(alloc, &target_hints, .full_text);
        try appendUniqueHintAlloc(alloc, &target_hints, .dense_vector);
        try appendUniqueHintAlloc(alloc, &target_hints, .sparse_vector);
        try appendUniqueHintAlloc(alloc, &target_hints, .algebraic);
        try appendUniqueHintAlloc(alloc, &target_hints, .graph);
    }
    if (batch.overwritten_doc_keys.len > 0) {
        try appendUniqueHintAlloc(alloc, &target_hints, .full_text);
        try appendUniqueHintAlloc(alloc, &target_hints, .dense_vector);
        try appendUniqueHintAlloc(alloc, &target_hints, .sparse_vector);
        try appendUniqueHintAlloc(alloc, &target_hints, .algebraic);
    }
    for (batch.changed_artifact_keys) |key| try appendUniqueString(alloc, &changed_artifact_keys, key);
    for (batch.changed_artifact_keys) |key| {
        if (internal_keys.isGraphEdgeArtifactKey(key)) {
            try appendUniqueHintAlloc(alloc, &target_hints, .graph);
        } else if (internal_keys.isEmbeddingArtifactKey(key) or internal_keys.isDerivedEmbeddingArtifactKey(key)) {
            try appendUniqueHintAlloc(alloc, &target_hints, .dense_vector);
            try appendUniqueHintAlloc(alloc, &target_hints, .sparse_vector);
        }
    }

    if (batch.dense_embeddings.len > 0) try appendUniqueHintAlloc(alloc, &target_hints, .dense_vector);
    for (batch.dense_embeddings) |embedding| {
        if (embedding.artifact_key) |artifact_key| try appendUniqueString(alloc, &changed_artifact_keys, artifact_key);
    }

    if (batch.sparse_embeddings.len > 0) try appendUniqueHintAlloc(alloc, &target_hints, .sparse_vector);
    for (batch.sparse_embeddings) |embedding| {
        if (embedding.artifact_key) |artifact_key| try appendUniqueString(alloc, &changed_artifact_keys, artifact_key);
    }

    if (batch.graph_doc_clears.len > 0 or batch.graph_writes.len > 0 or batch.graph_deletes.len > 0) {
        try appendUniqueHintAlloc(alloc, &target_hints, .graph);
    }
    for (batch.graph_doc_clears) |clear| try appendUniqueString(alloc, &changed_doc_keys, clear.key);
    for (batch.graph_writes) |write| try appendUniqueString(alloc, &changed_doc_keys, write.source);
    for (batch.graph_deletes) |delete| try appendUniqueString(alloc, &deleted_doc_keys, delete.source);
    for (batch.graph_writes) |write| {
        const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, write.source, write.index_name, write.edge_type, write.target);
        defer alloc.free(artifact_key);
        try appendUniqueString(alloc, &changed_artifact_keys, artifact_key);
    }
    for (batch.graph_deletes) |delete| {
        const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, delete.source, delete.index_name, delete.edge_type, delete.target);
        defer alloc.free(artifact_key);
        try appendUniqueString(alloc, &changed_artifact_keys, artifact_key);
    }

    if (batch.generated_enrichment_refs.len > 0) try appendUniqueHintAlloc(alloc, &target_hints, .enrichment);
    for (batch.generated_enrichment_refs) |ref| {
        try appendUniqueString(alloc, &changed_doc_keys, ref.doc_key);
        if (ref.kind == .dense_embedding) try appendUniqueHintAlloc(alloc, &target_hints, .dense_vector);
        if (ref.kind == .sparse_embedding) try appendUniqueHintAlloc(alloc, &target_hints, .sparse_vector);
    }

    return .{
        .sequence = sequence,
        .changed_doc_keys = try changed_doc_keys.toOwnedSlice(alloc),
        .deleted_doc_keys = try deleted_doc_keys.toOwnedSlice(alloc),
        .overwritten_doc_keys = try overwritten_doc_keys.toOwnedSlice(alloc),
        .changed_artifact_keys = try changed_artifact_keys.toOwnedSlice(alloc),
        .target_hints = try target_hints.toOwnedSlice(alloc),
    };
}

fn appendUniqueHintAlloc(alloc: Allocator, list: *std.ArrayListUnmanaged(TargetHint), hint: TargetHint) !void {
    for (list.items) |existing| {
        if (existing == hint) return;
    }
    try list.append(alloc, hint);
}

pub fn encodeRecord(alloc: Allocator, record: Record) ![]u8 {
    var payload = std.ArrayListUnmanaged(u8).empty;
    errdefer payload.deinit(alloc);

    const field_mask = FieldMask{
        .changed_doc_keys = record.changed_doc_keys.len > 0,
        .deleted_doc_keys = record.deleted_doc_keys.len > 0,
        .overwritten_doc_keys = record.overwritten_doc_keys.len > 0,
        .changed_artifact_keys = record.changed_artifact_keys.len > 0,
    };

    try payload.appendSlice(alloc, binary_magic);
    try appendInt(&payload, alloc, u16, record.version);
    try appendInt(&payload, alloc, u64, record.sequence);
    try payload.append(alloc, hintMask(record.target_hints));
    try payload.append(alloc, @bitCast(field_mask));

    if (field_mask.changed_doc_keys) try encodeStringList(&payload, alloc, record.changed_doc_keys);
    if (field_mask.deleted_doc_keys) try encodeStringList(&payload, alloc, record.deleted_doc_keys);
    if (field_mask.overwritten_doc_keys) try encodeStringList(&payload, alloc, record.overwritten_doc_keys);
    if (field_mask.changed_artifact_keys) try encodeStringList(&payload, alloc, record.changed_artifact_keys);

    return try payload.toOwnedSlice(alloc);
}

pub fn decodeRecord(alloc: Allocator, raw: []const u8) !DecodedRecord {
    if (looksLikeBinaryRecord(raw)) {
        return .{
            .alloc = alloc,
            .record = try decodeBinaryRecord(alloc, raw),
            .parsed = null,
        };
    }
    const parsed = try std.json.parseFromSlice(Record, alloc, raw, .{ .ignore_unknown_fields = true });
    return .{
        .alloc = alloc,
        .record = parsed.value,
        .parsed = parsed,
    };
}

fn looksLikeEncodedRecord(raw: []const u8) bool {
    if (looksLikeBinaryRecord(raw)) return true;
    const trimmed = std.mem.trim(u8, raw, &std.ascii.whitespace);
    return trimmed.len > 0 and trimmed[0] == '{';
}

pub fn looksLikeBinaryRecord(raw: []const u8) bool {
    return raw.len >= binary_magic.len and std.mem.eql(u8, raw[0..binary_magic.len], binary_magic);
}

fn hintMask(hints: []const TargetHint) u8 {
    var mask: u8 = 0;
    for (hints) |hint| {
        const bit: u3 = @intCast(@intFromEnum(hint));
        mask |= (@as(u8, 1) << bit);
    }
    return mask;
}

pub fn singleHintMask(hint: TargetHint) u8 {
    const bit: u3 = @intCast(@intFromEnum(hint));
    return (@as(u8, 1) << bit);
}

pub fn encodedRecordHintMask(raw: []const u8) !u8 {
    if (looksLikeBinaryRecord(raw)) {
        if (raw.len < binary_magic.len + @sizeOf(u16) + @sizeOf(u64) + @sizeOf(u8)) {
            return error.EndOfStream;
        }
        return raw[binary_magic.len + @sizeOf(u16) + @sizeOf(u64)];
    }

    const decoded = try decodeRecord(std.heap.page_allocator, raw);
    defer {
        var owned = decoded;
        owned.deinit();
    }
    return hintMask(decoded.record.target_hints);
}

fn decodeHintMask(alloc: Allocator, mask: u8) ![]TargetHint {
    var count: usize = 0;
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.enrichment))) != 0) count += 1;
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.full_text))) != 0) count += 1;
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.dense_vector))) != 0) count += 1;
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.sparse_vector))) != 0) count += 1;
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.graph))) != 0) count += 1;
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.algebraic))) != 0) count += 1;
    if (count == 0) return &.{};

    const hints = try alloc.alloc(TargetHint, count);
    var index: usize = 0;
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.enrichment))) != 0) {
        hints[index] = .enrichment;
        index += 1;
    }
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.full_text))) != 0) {
        hints[index] = .full_text;
        index += 1;
    }
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.dense_vector))) != 0) {
        hints[index] = .dense_vector;
        index += 1;
    }
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.sparse_vector))) != 0) {
        hints[index] = .sparse_vector;
        index += 1;
    }
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.graph))) != 0) {
        hints[index] = .graph;
        index += 1;
    }
    if ((mask & (@as(u8, 1) << @intFromEnum(TargetHint.algebraic))) != 0) {
        hints[index] = .algebraic;
        index += 1;
    }
    return hints;
}

const enrichment_hint_slice = [_]TargetHint{.enrichment};
const full_text_hint_slice = [_]TargetHint{.full_text};
const dense_vector_hint_slice = [_]TargetHint{.dense_vector};
const sparse_vector_hint_slice = [_]TargetHint{.sparse_vector};
const graph_hint_slice = [_]TargetHint{.graph};
const algebraic_hint_slice = [_]TargetHint{.algebraic};

fn targetHintsAreStaticSingleton(target_hints: []const TargetHint) bool {
    if (target_hints.len != 1) return false;
    return target_hints.ptr == enrichment_hint_slice[0..].ptr or
        target_hints.ptr == full_text_hint_slice[0..].ptr or
        target_hints.ptr == dense_vector_hint_slice[0..].ptr or
        target_hints.ptr == sparse_vector_hint_slice[0..].ptr or
        target_hints.ptr == graph_hint_slice[0..].ptr or
        target_hints.ptr == algebraic_hint_slice[0..].ptr;
}

fn freeTargetHintsIfOwned(alloc: Allocator, target_hints: []const TargetHint) void {
    if (target_hints.len == 0 or targetHintsAreStaticSingleton(target_hints)) return;
    alloc.free(target_hints);
}

fn decodeHintMaskBorrowed(alloc: Allocator, mask: u8) ![]const TargetHint {
    if (mask == 0) return &.{};
    if (std.math.isPowerOfTwo(mask)) {
        return switch (mask) {
            singleHintMask(.enrichment) => enrichment_hint_slice[0..],
            singleHintMask(.full_text) => full_text_hint_slice[0..],
            singleHintMask(.dense_vector) => dense_vector_hint_slice[0..],
            singleHintMask(.sparse_vector) => sparse_vector_hint_slice[0..],
            singleHintMask(.graph) => graph_hint_slice[0..],
            singleHintMask(.algebraic) => algebraic_hint_slice[0..],
            else => return error.InvalidBinaryRecord,
        };
    }
    return try decodeHintMask(alloc, mask);
}

fn appendInt(payload: *std.ArrayListUnmanaged(u8), alloc: Allocator, comptime T: type, value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try payload.appendSlice(alloc, &buf);
}

fn encodeStringList(payload: *std.ArrayListUnmanaged(u8), alloc: Allocator, values: []const []const u8) !void {
    try appendInt(payload, alloc, u32, @intCast(values.len));
    for (values) |value| {
        try appendInt(payload, alloc, u32, @intCast(value.len));
        try payload.appendSlice(alloc, value);
    }
}

const BinaryDecodeError = error{
    InvalidBinaryRecord,
    UnexpectedEndOfInput,
} || Allocator.Error;

const BinaryCursor = struct {
    raw: []const u8,
    index: usize = 0,

    fn remaining(self: BinaryCursor) usize {
        return self.raw.len - self.index;
    }

    fn readBytes(self: *BinaryCursor, len: usize) BinaryDecodeError![]const u8 {
        if (self.remaining() < len) return error.UnexpectedEndOfInput;
        const value = self.raw[self.index .. self.index + len];
        self.index += len;
        return value;
    }

    fn readInt(self: *BinaryCursor, comptime T: type) BinaryDecodeError!T {
        const bytes = try self.readBytes(@sizeOf(T));
        return std.mem.readInt(T, bytes[0..@sizeOf(T)], .little);
    }
};

fn decodeBinaryStringList(alloc: Allocator, cursor: *BinaryCursor) BinaryDecodeError![]const []const u8 {
    const count = try cursor.readInt(u32);
    if (count == 0) return &.{};

    const values = try alloc.alloc([]const u8, count);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |value| alloc.free(value);
        alloc.free(values);
    }

    var index: usize = 0;
    while (index < count) : (index += 1) {
        const len = try cursor.readInt(u32);
        const bytes = try cursor.readBytes(len);
        values[index] = try alloc.dupe(u8, bytes);
        initialized += 1;
    }
    return values;
}

fn decodeBinaryStringListBorrowed(alloc: Allocator, cursor: *BinaryCursor) BinaryDecodeError![]const []const u8 {
    const count = try cursor.readInt(u32);
    if (count == 0) return &.{};

    const values = try alloc.alloc([]const u8, count);
    errdefer alloc.free(values);

    var index: usize = 0;
    while (index < count) : (index += 1) {
        const len = try cursor.readInt(u32);
        values[index] = try cursor.readBytes(len);
    }
    return values;
}

fn decodeBinaryRecord(alloc: Allocator, raw: []const u8) !Record {
    var cursor = BinaryCursor{
        .raw = raw[binary_magic.len..],
        .index = 0,
    };

    const version = try cursor.readInt(u16);
    const sequence = try cursor.readInt(u64);
    const target_hints = try decodeHintMaskBorrowed(alloc, try cursor.readInt(u8));
    errdefer freeTargetHintsIfOwned(alloc, target_hints);

    const field_mask: FieldMask = @bitCast(try cursor.readInt(u8));

    var record: Record = .{
        .version = version,
        .sequence = sequence,
        .target_hints = target_hints,
    };
    errdefer deinitRecord(alloc, &record);

    if (field_mask.changed_doc_keys) record.changed_doc_keys = try decodeBinaryStringList(alloc, &cursor);
    if (field_mask.deleted_doc_keys) record.deleted_doc_keys = try decodeBinaryStringList(alloc, &cursor);
    if (field_mask.overwritten_doc_keys) record.overwritten_doc_keys = try decodeBinaryStringList(alloc, &cursor);
    if (field_mask.changed_artifact_keys) record.changed_artifact_keys = try decodeBinaryStringList(alloc, &cursor);
    if (cursor.remaining() != 0) return error.InvalidBinaryRecord;
    return record;
}

pub fn decodeBinaryRecordBorrowed(alloc: Allocator, raw: []const u8) !BorrowedBinaryRecord {
    if (!looksLikeBinaryRecord(raw)) return error.InvalidBinaryRecord;

    var cursor = BinaryCursor{
        .raw = raw[binary_magic.len..],
        .index = 0,
    };

    const version = try cursor.readInt(u16);
    const sequence = try cursor.readInt(u64);
    const target_hints = try decodeHintMaskBorrowed(alloc, try cursor.readInt(u8));
    errdefer freeTargetHintsIfOwned(alloc, target_hints);

    const field_mask: FieldMask = @bitCast(try cursor.readInt(u8));

    var record: Record = .{
        .version = version,
        .sequence = sequence,
        .target_hints = target_hints,
    };
    errdefer {
        if (record.changed_doc_keys.len > 0) alloc.free(record.changed_doc_keys);
        if (record.deleted_doc_keys.len > 0) alloc.free(record.deleted_doc_keys);
        if (record.overwritten_doc_keys.len > 0) alloc.free(record.overwritten_doc_keys);
        if (record.changed_artifact_keys.len > 0) alloc.free(record.changed_artifact_keys);
        freeTargetHintsIfOwned(alloc, record.target_hints);
    }

    if (field_mask.changed_doc_keys) record.changed_doc_keys = try decodeBinaryStringListBorrowed(alloc, &cursor);
    if (field_mask.deleted_doc_keys) record.deleted_doc_keys = try decodeBinaryStringListBorrowed(alloc, &cursor);
    if (field_mask.overwritten_doc_keys) record.overwritten_doc_keys = try decodeBinaryStringListBorrowed(alloc, &cursor);
    if (field_mask.changed_artifact_keys) record.changed_artifact_keys = try decodeBinaryStringListBorrowed(alloc, &cursor);
    if (cursor.remaining() != 0) return error.InvalidBinaryRecord;

    return .{
        .alloc = alloc,
        .record = record,
    };
}

const FastParseError = error{
    InvalidCharacter,
    UnexpectedEndOfInput,
    RequiresFullJsonDecode,
    OutOfMemory,
};

const FastRecordParser = struct {
    raw: []const u8,
    index: usize = 0,

    fn skipWhitespace(self: *FastRecordParser) void {
        while (self.index < self.raw.len and std.ascii.isWhitespace(self.raw[self.index])) : (self.index += 1) {}
    }

    fn consumeByte(self: *FastRecordParser, expected: u8) FastParseError!void {
        self.skipWhitespace();
        if (self.index >= self.raw.len or self.raw[self.index] != expected) return error.InvalidCharacter;
        self.index += 1;
    }

    fn tryConsumeByte(self: *FastRecordParser, expected: u8) bool {
        self.skipWhitespace();
        if (self.index >= self.raw.len or self.raw[self.index] != expected) return false;
        self.index += 1;
        return true;
    }

    fn skipJsonString(self: *FastRecordParser) FastParseError!void {
        try self.consumeByte('"');
        while (self.index < self.raw.len) : (self.index += 1) {
            const byte = self.raw[self.index];
            if (byte == '\\') {
                self.index += 1;
                if (self.index >= self.raw.len) return error.UnexpectedEndOfInput;
                continue;
            }
            if (byte == '"') {
                self.index += 1;
                return;
            }
        }
        return error.UnexpectedEndOfInput;
    }

    fn parseRawStringNoEscapes(self: *FastRecordParser) FastParseError![]const u8 {
        self.skipWhitespace();
        if (self.index >= self.raw.len or self.raw[self.index] != '"') return error.InvalidCharacter;
        self.index += 1;
        const start = self.index;
        while (self.index < self.raw.len) : (self.index += 1) {
            const byte = self.raw[self.index];
            if (byte == '\\') return error.RequiresFullJsonDecode;
            if (byte == '"') {
                const value = self.raw[start..self.index];
                self.index += 1;
                return value;
            }
        }
        return error.UnexpectedEndOfInput;
    }

    fn skipNumber(self: *FastRecordParser) FastParseError!void {
        self.skipWhitespace();
        if (self.index >= self.raw.len) return error.UnexpectedEndOfInput;
        const start = self.index;
        while (self.index < self.raw.len) : (self.index += 1) {
            switch (self.raw[self.index]) {
                '0'...'9', '-', '+', '.', 'e', 'E' => {},
                else => break,
            }
        }
        if (self.index == start) return error.InvalidCharacter;
    }

    fn skipLiteral(self: *FastRecordParser, literal: []const u8) FastParseError!void {
        self.skipWhitespace();
        if (!std.mem.startsWith(u8, self.raw[self.index..], literal)) return error.InvalidCharacter;
        self.index += literal.len;
    }

    fn skipArray(self: *FastRecordParser) FastParseError!void {
        try self.consumeByte('[');
        if (self.tryConsumeByte(']')) return;
        while (true) {
            try self.skipValue();
            if (self.tryConsumeByte(']')) return;
            try self.consumeByte(',');
        }
    }

    fn skipObject(self: *FastRecordParser) FastParseError!void {
        try self.consumeByte('{');
        if (self.tryConsumeByte('}')) return;
        while (true) {
            try self.skipJsonString();
            try self.consumeByte(':');
            try self.skipValue();
            if (self.tryConsumeByte('}')) return;
            try self.consumeByte(',');
        }
    }

    fn skipValue(self: *FastRecordParser) FastParseError!void {
        self.skipWhitespace();
        if (self.index >= self.raw.len) return error.UnexpectedEndOfInput;
        switch (self.raw[self.index]) {
            '"' => try self.skipJsonString(),
            '{' => try self.skipObject(),
            '[' => try self.skipArray(),
            't' => try self.skipLiteral("true"),
            'f' => try self.skipLiteral("false"),
            'n' => try self.skipLiteral("null"),
            '-', '0'...'9' => try self.skipNumber(),
            else => return error.InvalidCharacter,
        }
    }

    fn parseHintArray(self: *FastRecordParser, wanted_hint: TargetHint) FastParseError!bool {
        const wanted_name = @tagName(wanted_hint);
        var matched = false;
        try self.consumeByte('[');
        if (self.tryConsumeByte(']')) return false;
        while (true) {
            const value = try self.parseRawStringNoEscapes();
            if (std.mem.eql(u8, value, wanted_name)) matched = true;
            if (self.tryConsumeByte(']')) return matched;
            try self.consumeByte(',');
        }
    }
};

fn recordHasHintFast(raw: []const u8, hint: TargetHint) !bool {
    var parser = FastRecordParser{ .raw = raw };

    try parser.consumeByte('{');
    if (parser.tryConsumeByte('}')) return false;

    while (true) {
        const field_name = parser.parseRawStringNoEscapes() catch |err| switch (err) {
            error.RequiresFullJsonDecode => return error.RequiresFullJsonDecode,
            else => return err,
        };
        try parser.consumeByte(':');

        if (std.mem.eql(u8, field_name, "target_hints")) {
            return parser.parseHintArray(hint);
        }
        try parser.skipValue();

        if (parser.tryConsumeByte('}')) return false;
        try parser.consumeByte(',');
    }
}

pub fn encodedRecordHasHint(raw: []const u8, hint: TargetHint) !bool {
    return try encodedRecordMatchesHintMask(raw, singleHintMask(hint));
}

pub fn encodedRecordMatchesHintMask(raw: []const u8, required_mask: u8) !bool {
    if (required_mask == 0) return looksLikeEncodedRecord(raw);
    if (!looksLikeEncodedRecord(raw)) return false;
    if (looksLikeBinaryRecord(raw)) {
        var cursor = BinaryCursor{
            .raw = raw[binary_magic.len..],
            .index = 0,
        };
        _ = try cursor.readInt(u16);
        _ = try cursor.readInt(u64);
        const target_mask = try cursor.readInt(u8);
        return (target_mask & required_mask) != 0;
    }

    return recordMatchesHintMaskFast(raw, required_mask) catch |err| switch (err) {
        error.RequiresFullJsonDecode => blk: {
            var decoded = try decodeRecord(std.heap.page_allocator, raw);
            defer decoded.deinit();
            break :blk (hintMask(decoded.record.target_hints) & required_mask) != 0;
        },
        else => return err,
    };
}

fn recordMatchesHintMaskFast(raw: []const u8, required_mask: u8) !bool {
    const hints = [_]TargetHint{ .enrichment, .full_text, .dense_vector, .sparse_vector, .graph, .algebraic };
    for (hints) |hint| {
        if ((required_mask & singleHintMask(hint)) == 0) continue;
        if (try recordHasHintFast(raw, hint)) return true;
    }
    return false;
}

fn recordHasHint(record: Record, hint: TargetHint) bool {
    for (record.target_hints) |existing| {
        if (existing == hint) return true;
    }
    return false;
}

test "change journal record derives thin identities from derived batch" {
    const alloc = std.testing.allocator;

    var record = try recordFromDerivedBatch(alloc, .{
        .documents = &.{.{
            .key = "doc:a",
            .targets = &.{
                .{ .kind = .full_text, .index_name = "ft_v1" },
                .{ .kind = .dense_vector, .index_name = "dv_v1" },
            },
        }},
        .deleted_keys = &.{"doc:gone"},
        .dense_embeddings = &.{.{
            .index_name = "dv_v1",
            .parent_doc_key = "doc:a",
            .doc_key = "chunk:1",
            .artifact_key = "artifact:chunk:1:dv_v1",
            .vector = &.{ 1.0, 2.0 },
        }},
        .generated_enrichment_refs = &.{.{
            .kind = .chunk_text,
            .index_name = "dv_v1",
            .artifact_name = "body_chunks_v1",
            .doc_key = "doc:a",
        }},
    }, 42);
    defer deinitRecord(alloc, &record);

    try std.testing.expectEqual(@as(u64, 42), record.sequence);
    try std.testing.expectEqual(@as(usize, 1), record.changed_doc_keys.len);
    try std.testing.expectEqualStrings("doc:a", record.changed_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 1), record.deleted_doc_keys.len);
    try std.testing.expectEqualStrings("doc:gone", record.deleted_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 1), record.changed_artifact_keys.len);
    try std.testing.expectEqualStrings("artifact:chunk:1:dv_v1", record.changed_artifact_keys[0]);
    try std.testing.expect(recordHasHint(record, .full_text));
    try std.testing.expect(recordHasHint(record, .dense_vector));
    try std.testing.expect(recordHasHint(record, .sparse_vector));
    try std.testing.expect(recordHasHint(record, .graph));
}

test "change journal embedding-only batch does not invent primary document replay" {
    const alloc = std.testing.allocator;

    var record = try recordFromDerivedBatch(alloc, .{
        .documents = &.{.{
            .key = "doc:a",
            .action = .preserve_base_document,
            .targets = &.{.{ .kind = .dense_vector, .index_name = "dv_v1" }},
        }},
        .dense_embeddings = &.{.{
            .index_name = "dv_v1",
            .doc_key = "doc:a",
            .artifact_key = "artifact:doc:a:dv_v1",
            .vector = &.{},
        }},
    }, 9);
    defer deinitRecord(alloc, &record);

    try std.testing.expectEqual(@as(usize, 0), record.changed_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), record.changed_artifact_keys.len);
    try std.testing.expectEqualStrings("artifact:doc:a:dv_v1", record.changed_artifact_keys[0]);
    try std.testing.expect(recordHasHint(record, .dense_vector));
}

test "change journal graph-only derived batch encodes graph hint and artifact identity" {
    const alloc = std.testing.allocator;

    var record = try recordFromDerivedBatch(alloc, .{
        .graph_writes = &.{.{
            .index_name = "gr_v1",
            .source = "doc:a",
            .target = "doc:b",
            .edge_type = "links",
            .weight = 1.0,
        }},
    }, 17);
    defer deinitRecord(alloc, &record);

    try std.testing.expectEqual(@as(u64, 17), record.sequence);
    try std.testing.expect(recordHasHint(record, .graph));
    try std.testing.expectEqual(@as(usize, 1), record.changed_doc_keys.len);
    try std.testing.expectEqualStrings("doc:a", record.changed_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 1), record.changed_artifact_keys.len);

    const expected_artifact = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "gr_v1", "links", "doc:b");
    defer alloc.free(expected_artifact);
    try std.testing.expectEqualStrings(expected_artifact, record.changed_artifact_keys[0]);

    const payload = try encodeRecord(alloc, record);
    defer alloc.free(payload);
    try std.testing.expectEqual(singleHintMask(.graph), try encodedRecordHintMask(payload));
}

test "change journal binary codec roundtrips and omits empty lists" {
    const alloc = std.testing.allocator;
    const payload = try encodeRecord(alloc, .{
        .sequence = 7,
        .changed_doc_keys = &.{"doc:a"},
        .deleted_doc_keys = &.{"doc:gone"},
        .target_hints = &.{ .dense_vector, .full_text },
    });
    defer alloc.free(payload);

    try std.testing.expect(std.mem.startsWith(u8, payload, binary_magic));

    var decoded = try decodeRecord(alloc, payload);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u64, 7), decoded.record.sequence);
    try std.testing.expectEqual(@as(usize, 2), decoded.record.target_hints.len);
    try std.testing.expectEqualStrings("doc:a", decoded.record.changed_doc_keys[0]);
    try std.testing.expectEqualStrings("doc:gone", decoded.record.deleted_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 0), decoded.record.overwritten_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 0), decoded.record.changed_artifact_keys.len);
}

test "change journal borrowed binary decode reuses payload string bytes" {
    const alloc = std.testing.allocator;
    const payload = try encodeRecord(alloc, .{
        .sequence = 12,
        .changed_doc_keys = &.{"doc:a"},
        .deleted_doc_keys = &.{"doc:gone"},
        .changed_artifact_keys = &.{"artifact:a"},
        .target_hints = &.{.graph},
    });
    defer alloc.free(payload);

    var decoded = try decodeBinaryRecordBorrowed(alloc, payload);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u64, 12), decoded.record.sequence);
    try std.testing.expectEqualStrings("doc:a", decoded.record.changed_doc_keys[0]);
    try std.testing.expectEqualStrings("doc:gone", decoded.record.deleted_doc_keys[0]);
    try std.testing.expectEqualStrings("artifact:a", decoded.record.changed_artifact_keys[0]);
    const payload_start = @intFromPtr(payload.ptr);
    const payload_end = payload_start + payload.len;
    for ([_][]const u8{
        decoded.record.changed_doc_keys[0],
        decoded.record.deleted_doc_keys[0],
        decoded.record.changed_artifact_keys[0],
    }) |value| {
        const start = @intFromPtr(value.ptr);
        const end = start + value.len;
        try std.testing.expect(start >= payload_start and end <= payload_end);
    }
}

test "change journal binary decode deinit tolerates singleton borrowed hints" {
    const alloc = std.testing.allocator;
    const payload = try encodeRecord(alloc, .{
        .sequence = 13,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.graph},
    });
    defer alloc.free(payload);

    var decoded = try decodeRecord(alloc, payload);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(usize, 1), decoded.record.target_hints.len);
    try std.testing.expectEqual(TargetHint.graph, decoded.record.target_hints[0]);
}

test "change journal borrowed binary decode deinit tolerates singleton borrowed hints" {
    const alloc = std.testing.allocator;
    const payload = try encodeRecord(alloc, .{
        .sequence = 14,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.graph},
    });
    defer alloc.free(payload);

    var decoded = try decodeBinaryRecordBorrowed(alloc, payload);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(usize, 1), decoded.record.target_hints.len);
    try std.testing.expectEqual(TargetHint.graph, decoded.record.target_hints[0]);
}

test "change journal decodes legacy json records" {
    const alloc = std.testing.allocator;
    const payload =
        \\{"sequence":9,"target_hints":["graph"],"changed_doc_keys":["doc:a"],"changed_artifact_keys":["artifact:a"]}
    ;
    var decoded = try decodeRecord(alloc, payload);
    defer decoded.deinit();

    try std.testing.expectEqual(@as(u16, 1), decoded.record.version);
    try std.testing.expectEqual(@as(u64, 9), decoded.record.sequence);
    try std.testing.expectEqual(@as(usize, 1), decoded.record.target_hints.len);
    try std.testing.expectEqual(TargetHint.graph, decoded.record.target_hints[0]);
    try std.testing.expectEqualStrings("doc:a", decoded.record.changed_doc_keys[0]);
    try std.testing.expectEqualStrings("artifact:a", decoded.record.changed_artifact_keys[0]);
}

test "change journal encodedRecordHasHint matches binary payloads" {
    const alloc = std.testing.allocator;
    const payload = try encodeRecord(alloc, .{
        .sequence = 2,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{ .graph, .dense_vector },
    });
    defer alloc.free(payload);

    try std.testing.expect(try encodedRecordHasHint(payload, .graph));
    try std.testing.expect(try encodedRecordHasHint(payload, .dense_vector));
    try std.testing.expect(!(try encodedRecordHasHint(payload, .full_text)));
}

test "change journal encodedRecordHasHint matches legacy json payloads" {
    const payload =
        \\{"sequence":11,"target_hints":["full_text","graph"],"changed_doc_keys":["doc:a"]}
    ;

    try std.testing.expect(try encodedRecordHasHint(payload, .full_text));
    try std.testing.expect(try encodedRecordHasHint(payload, .graph));
    try std.testing.expect(!(try encodedRecordHasHint(payload, .dense_vector)));
}

test "change journal encodedRecordHasHint ignores non-record payloads" {
    try std.testing.expect(!(try encodedRecordHasHint("not-json", .graph)));
    try std.testing.expect(!(try encodedRecordHasHint("", .graph)));
}
