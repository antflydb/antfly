// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
//! Compact, bounded bytes for replicated publication upload. The producer's
//! public JSON wire is deliberately not the Raft staging format: base64 and
//! numeric key encoding can exceed the Raft request ceiling for a legal
//! 64 MiB publication. Decoded slices borrow one immutable owned buffer.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const Position = @import("receipt_position.zig").Position;
const Allocator = std.mem.Allocator;
const magic = "APB1";
const domain = "antfly:artifact-publication-transport:v1:";
pub const max_encoded_bytes: usize = 68 * 1024 * 1024;
pub const chunk_bytes: usize = 1024 * 1024;
pub const max_chunks: usize = (max_encoded_bytes + chunk_bytes - 1) / chunk_bytes;
pub const Digest = publication.Digest;

/// Bounded scheduling hint for locally authored envelopes, not validation or
/// authority. The consumer still authenticates the entire command before use.
/// Invalid/unknown headers cannot claim the reserved control lane.
pub fn controlAdmissionHint(bytes: []const u8) bool {
    if (bytes.len < 36 or bytes.len > max_encoded_bytes or !std.mem.eql(u8, bytes[0..4], magic)) return false;
    const mode = std.enums.fromInt(@FieldType(publication.Command, "mode"), bytes[4]) orelse return false;
    return mode != .publish;
}
comptime {
    if (@typeInfo(publication.Command).@"struct".field_names.len != 18 or
        @typeInfo(publication.Source).@"struct".field_names.len != 5 or
        @typeInfo(publication.ArtifactSource).@"struct".field_names.len != 4 or
        @typeInfo(publication.Mutation).@"struct".field_names.len != 4)
        @compileError("update compact publication transport projection");
}

fn digest(bytes: []const u8) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(domain);
    hash.update(bytes);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

fn add(size: *usize, value: usize) !void {
    size.* = std.math.add(usize, size.*, value) catch return error.ResourceLimitExceeded;
    if (size.* > max_encoded_bytes) return error.ResourceLimitExceeded;
}

fn blobSize(size: *usize, bytes: []const u8) !void {
    if (bytes.len > std.math.maxInt(u32)) return error.ResourceLimitExceeded;
    try add(size, 4);
    try add(size, bytes.len);
}

fn positionSize(size: *usize, value: ?Position) !void {
    try add(size, 1);
    if (value != null) try add(size, Position.encoded_len);
}

pub fn encodedLength(command: publication.Command) !usize {
    var size: usize = 4 + 1 + 1 + 24 + 8 + 32 + 8 + 32 + 4 * 4 + 32 + 4;
    try blobSize(&size, command.producer_name);
    try blobSize(&size, command.producer_artifact_name);
    try blobSize(&size, command.producer_scope_key);
    for (command.sources) |source| {
        try blobSize(&size, source.document_key);
        try add(&size, 1 + 32 + 8);
        try positionSize(&size, source.input_position);
    }
    for (command.artifact_sources) |source| {
        try blobSize(&size, source.key);
        try add(&size, 1 + (if (source.content_digest != null) @as(usize, 32) else 0) + 4);
        try positionSize(&size, source.input_position);
    }
    for (command.mutation_preconditions) |source| {
        try blobSize(&size, source.key);
        try add(&size, 1 + (if (source.content_digest != null) @as(usize, 32) else 0) + 4);
        try positionSize(&size, source.input_position);
    }
    for (command.mutations) |mutation| {
        try add(&size, 1 + 1 + 4);
        try blobSize(&size, mutation.key);
        if (mutation.value) |value| try blobSize(&size, value);
    }
    if (command.baseline) |page| {
        try add(&size, 8 + 8 + 1 + 4);
        try blobSize(&size, page.expected_cursor);
        try blobSize(&size, page.next_cursor);
        try blobSize(&size, page.upper_bound);
        for (page.row_keys) |key| try blobSize(&size, key);
    }
    if (command.validation) |page| {
        try add(&size, 8 + 1 + 4);
        try blobSize(&size, page.expected_cursor);
        try blobSize(&size, page.next_cursor);
        for (page.repair_documents) |document| try blobSize(&size, document);
    }
    if (command.census) |page| {
        try add(&size, 8 + 64);
        try blobSize(&size, page.document_key);
        try blobSize(&size, page.chunk_name);
    }
    if (command.completion) |page| {
        try add(&size, 8 + 64);
        try blobSize(&size, page.document_key);
    }
    return size;
}

const Writer = struct {
    bytes: []u8,
    pos: usize = 0,
    fn write(self: *Writer, value: []const u8) void {
        @memcpy(self.bytes[self.pos..][0..value.len], value);
        self.pos += value.len;
    }
    fn byte(self: *Writer, value: u8) void {
        self.bytes[self.pos] = value;
        self.pos += 1;
    }
    fn writeU32(self: *Writer, value: u32) void {
        std.mem.writeInt(u32, self.bytes[self.pos..][0..4], value, .little);
        self.pos += 4;
    }
    fn writeU64(self: *Writer, value: u64) void {
        std.mem.writeInt(u64, self.bytes[self.pos..][0..8], value, .little);
        self.pos += 8;
    }
    fn blob(self: *Writer, value: []const u8) void {
        self.writeU32(@intCast(value.len));
        self.write(value);
    }
    fn position(self: *Writer, value: ?Position) !void {
        self.byte(@intFromBool(value != null));
        if (value) |position_value| self.write(&try position_value.encode());
    }
};

pub fn encodeAlloc(alloc: Allocator, command: publication.Command) ![]u8 {
    try command.validate(alloc);
    const out = try alloc.alloc(u8, try encodedLength(command));
    errdefer alloc.free(out);
    var writer: Writer = .{ .bytes = out };
    writer.write(magic);
    writer.byte(@backingInt(command.mode));
    writer.byte(@backingInt(command.producer_kind));
    writer.write(&command.namespace);
    writer.writeU64(command.authority_epoch);
    writer.write(&command.catalog_digest);
    writer.writeU64(command.producer_generation);
    writer.write(&command.publication_digest);
    writer.blob(command.producer_name);
    writer.blob(command.producer_artifact_name);
    writer.blob(command.producer_scope_key);
    writer.writeU32(@intCast(command.sources.len));
    writer.writeU32(@intCast(command.artifact_sources.len));
    writer.writeU32(@intCast(command.mutation_preconditions.len));
    writer.writeU32(@intCast(command.mutations.len));
    for (command.sources) |source| {
        writer.blob(source.document_key);
        writer.byte(@intFromBool(source.exists));
        writer.write(&source.content_digest);
        writer.writeU64(source.timestamp);
        try writer.position(source.input_position);
    }
    for (command.artifact_sources) |source| {
        writer.blob(source.key);
        writer.byte(@intFromBool(source.content_digest != null));
        if (source.content_digest) |value| writer.write(&value);
        try writer.position(source.input_position);
        writer.writeU32(source.source_index);
    }
    for (command.mutation_preconditions) |source| {
        writer.blob(source.key);
        writer.byte(@intFromBool(source.content_digest != null));
        if (source.content_digest) |value| writer.write(&value);
        try writer.position(source.input_position);
        writer.writeU32(source.source_index);
    }
    for (command.mutations) |mutation| {
        writer.byte(@backingInt(mutation.family));
        writer.blob(mutation.key);
        writer.byte(@intFromBool(mutation.value != null));
        if (mutation.value) |value| writer.blob(value);
        writer.writeU32(mutation.source_index);
    }
    writer.byte(@intFromBool(command.baseline != null));
    if (command.baseline) |page| {
        writer.writeU64(page.observed_term);
        writer.writeU64(page.observed_index);
        writer.blob(page.expected_cursor);
        writer.blob(page.next_cursor);
        writer.blob(page.upper_bound);
        writer.byte(@intFromBool(page.at_end));
        writer.writeU32(@intCast(page.row_keys.len));
        for (page.row_keys) |key| writer.blob(key);
    }
    writer.byte(@intFromBool(command.validation != null));
    if (command.validation) |page| {
        writer.writeU64(page.mutation_epoch);
        writer.blob(page.expected_cursor);
        writer.blob(page.next_cursor);
        writer.byte(@intFromBool(page.at_end));
        writer.writeU32(@intCast(page.repair_documents.len));
        for (page.repair_documents) |document| writer.blob(document);
    }
    writer.byte(@intFromBool(command.census != null));
    if (command.census) |page| {
        writer.blob(page.document_key);
        writer.blob(page.chunk_name);
        writer.writeU32(page.visits);
        writer.writeU32(page.bytes);
        writer.write(&page.before);
        writer.write(&page.after);
    }
    writer.byte(@intFromBool(command.completion != null));
    if (command.completion) |page| {
        writer.blob(page.document_key);
        writer.writeU32(page.visits);
        writer.writeU32(page.bytes);
        writer.write(&page.before);
        writer.write(&page.after);
    }
    const checksum = digest(out[0..writer.pos]);
    writer.write(&checksum);
    if (writer.pos != out.len) return error.ArtifactCatalogCorrupt;
    return out;
}

const Cursor = struct {
    bytes: []const u8,
    pos: usize = 0,
    fn take(self: *Cursor, len: usize) ![]const u8 {
        if (len > self.bytes.len -| self.pos) return error.InvalidBatchRequest;
        const result = self.bytes[self.pos..][0..len];
        self.pos += len;
        return result;
    }
    fn byte(self: *Cursor) !u8 {
        return (try self.take(1))[0];
    }
    fn readU32(self: *Cursor) !u32 {
        return std.mem.readInt(u32, (try self.take(4))[0..4], .little);
    }
    fn readU64(self: *Cursor) !u64 {
        return std.mem.readInt(u64, (try self.take(8))[0..8], .little);
    }
    fn blob(self: *Cursor) ![]const u8 {
        return self.take(try self.readU32());
    }
    fn position(self: *Cursor) !?Position {
        return switch (try self.byte()) {
            0 => null,
            1 => try Position.decode(try self.take(Position.encoded_len)),
            else => error.InvalidBatchRequest,
        };
    }
};

pub const Decoded = struct {
    alloc: Allocator,
    bytes: []const u8,
    owns_bytes: bool,
    sources: []publication.Source,
    artifact_sources: []publication.ArtifactSource,
    mutation_preconditions: []publication.ArtifactSource,
    mutations: []publication.Mutation,
    baseline_keys: []const []const u8,
    repair_documents: []const []const u8,
    command: publication.Command,

    pub fn deinit(self: *Decoded) void {
        self.alloc.free(self.sources);
        self.alloc.free(self.artifact_sources);
        self.alloc.free(self.mutation_preconditions);
        self.alloc.free(self.mutations);
        self.alloc.free(self.baseline_keys);
        self.alloc.free(self.repair_documents);
        if (self.owns_bytes) self.alloc.free(self.bytes);
        self.* = undefined;
    }
};

/// Takes ownership of `bytes` even on failure; all command byte fields then
/// borrow that one verified allocation until Decoded.deinit.
pub fn decodeOwned(alloc: Allocator, bytes: []u8) !Decoded {
    return decode(alloc, bytes, true);
}

/// Queue jobs already own an immutable buffer for their entire lifetime.
/// Borrow it rather than copying a potentially 68 MiB command a second time.
pub fn decodeBorrowed(alloc: Allocator, bytes: []const u8) !Decoded {
    return decode(alloc, bytes, false);
}

fn decode(alloc: Allocator, bytes: []const u8, owns_bytes: bool) !Decoded {
    errdefer if (owns_bytes) alloc.free(bytes);
    if (bytes.len < 4 + 32 or bytes.len > max_encoded_bytes) return error.InvalidBatchRequest;
    const expected = digest(bytes[0 .. bytes.len - 32]);
    if (!std.mem.eql(u8, &expected, bytes[bytes.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    var cursor: Cursor = .{ .bytes = bytes[0 .. bytes.len - 32] };
    if (!std.mem.eql(u8, try cursor.take(4), magic)) return error.InvalidBatchRequest;
    const mode = std.enums.fromInt(@FieldType(publication.Command, "mode"), try cursor.byte()) orelse return error.InvalidBatchRequest;
    const kind = std.enums.fromInt(@FieldType(publication.Command, "producer_kind"), try cursor.byte()) orelse return error.InvalidBatchRequest;
    const namespace: publication.Namespace = (try cursor.take(24))[0..24].*;
    const authority_epoch = try cursor.readU64();
    const catalog_digest: Digest = (try cursor.take(32))[0..32].*;
    const producer_generation = try cursor.readU64();
    const publication_digest: Digest = (try cursor.take(32))[0..32].*;
    const producer_name = try cursor.blob();
    const producer_artifact_name = try cursor.blob();
    const producer_scope_key = try cursor.blob();
    const source_count = try cursor.readU32();
    const artifact_count = try cursor.readU32();
    const precondition_count = try cursor.readU32();
    const mutation_count = try cursor.readU32();
    if (source_count > publication.max_source_documents or artifact_count > publication.max_source_documents or precondition_count > publication.max_source_documents or
        artifact_count + precondition_count > publication.max_source_documents or mutation_count > publication.max_mutations) return error.InvalidBatchRequest;
    const sources = try alloc.alloc(publication.Source, source_count);
    errdefer alloc.free(sources);
    const artifact_sources = try alloc.alloc(publication.ArtifactSource, artifact_count);
    errdefer alloc.free(artifact_sources);
    const mutation_preconditions = try alloc.alloc(publication.ArtifactSource, precondition_count);
    errdefer alloc.free(mutation_preconditions);
    const mutations = try alloc.alloc(publication.Mutation, mutation_count);
    errdefer alloc.free(mutations);
    for (sources) |*source| {
        const document_key = try cursor.blob();
        const exists = switch (try cursor.byte()) {
            0 => false,
            1 => true,
            else => return error.InvalidBatchRequest,
        };
        source.* = .{ .document_key = document_key, .exists = exists, .content_digest = (try cursor.take(32))[0..32].*, .timestamp = try cursor.readU64(), .input_position = try cursor.position() };
    }
    for (artifact_sources) |*source| {
        const key = try cursor.blob();
        const content_digest: ?Digest = switch (try cursor.byte()) {
            0 => null,
            1 => (try cursor.take(32))[0..32].*,
            else => return error.InvalidBatchRequest,
        };
        source.* = .{ .key = key, .content_digest = content_digest, .input_position = try cursor.position(), .source_index = try cursor.readU32() };
    }
    for (mutation_preconditions) |*source| {
        const key = try cursor.blob();
        const content_digest: ?Digest = switch (try cursor.byte()) {
            0 => null,
            1 => (try cursor.take(32))[0..32].*,
            else => return error.InvalidBatchRequest,
        };
        source.* = .{ .key = key, .content_digest = content_digest, .input_position = try cursor.position(), .source_index = try cursor.readU32() };
    }
    for (mutations) |*mutation| {
        const family = std.enums.fromInt(publication.Family, try cursor.byte()) orelse return error.InvalidBatchRequest;
        const key = try cursor.blob();
        const value: ?[]const u8 = switch (try cursor.byte()) {
            0 => null,
            1 => try cursor.blob(),
            else => return error.InvalidBatchRequest,
        };
        mutation.* = .{ .family = family, .key = key, .value = value, .source_index = try cursor.readU32() };
    }
    var baseline: ?publication.BaselinePage = null;
    var baseline_keys: []const []const u8 = &.{};
    errdefer alloc.free(baseline_keys);
    switch (try cursor.byte()) {
        0 => {},
        1 => {
            const term = try cursor.readU64();
            const index = try cursor.readU64();
            const expected_cursor = try cursor.blob();
            const next_cursor = try cursor.blob();
            const upper_bound = try cursor.blob();
            const at_end = switch (try cursor.byte()) {
                0 => false,
                1 => true,
                else => return error.InvalidBatchRequest,
            };
            const count = try cursor.readU32();
            if (count > 128) return error.InvalidBatchRequest;
            const owned_keys = try alloc.alloc([]const u8, count);
            baseline_keys = owned_keys;
            for (owned_keys) |*key| key.* = try cursor.blob();
            baseline = .{ .observed_term = term, .observed_index = index, .expected_cursor = expected_cursor, .next_cursor = next_cursor, .upper_bound = upper_bound, .at_end = at_end, .row_keys = baseline_keys };
        },
        else => return error.InvalidBatchRequest,
    }
    var validation: ?publication.ValidationPage = null;
    var repair_documents: []const []const u8 = &.{};
    errdefer alloc.free(repair_documents);
    switch (try cursor.byte()) {
        0 => {},
        1 => {
            const mutation_epoch = try cursor.readU64();
            const expected_cursor = try cursor.blob();
            const next_cursor = try cursor.blob();
            const at_end = switch (try cursor.byte()) {
                0 => false,
                1 => true,
                else => return error.InvalidBatchRequest,
            };
            const count = try cursor.readU32();
            if (count > 128) return error.InvalidBatchRequest;
            const documents = try alloc.alloc([]const u8, count);
            repair_documents = documents;
            for (documents) |*document| document.* = try cursor.blob();
            validation = .{ .mutation_epoch = mutation_epoch, .expected_cursor = expected_cursor, .next_cursor = next_cursor, .at_end = at_end, .repair_documents = repair_documents };
        },
        else => return error.InvalidBatchRequest,
    }
    const census: ?publication.CensusPage = switch (try cursor.byte()) {
        0 => null,
        1 => .{ .document_key = try cursor.blob(), .chunk_name = try cursor.blob(), .visits = try cursor.readU32(), .bytes = try cursor.readU32(), .before = (try cursor.take(32))[0..32].*, .after = (try cursor.take(32))[0..32].* },
        else => return error.InvalidBatchRequest,
    };
    const completion: ?publication.CompletionPage = switch (try cursor.byte()) {
        0 => null,
        1 => .{ .document_key = try cursor.blob(), .visits = try cursor.readU32(), .bytes = try cursor.readU32(), .before = (try cursor.take(32))[0..32].*, .after = (try cursor.take(32))[0..32].* },
        else => return error.InvalidBatchRequest,
    };
    if (cursor.pos != cursor.bytes.len) return error.InvalidBatchRequest;
    const command: publication.Command = .{
        .mode = mode,
        .producer_kind = kind,
        .namespace = namespace,
        .authority_epoch = authority_epoch,
        .catalog_digest = catalog_digest,
        .producer_name = producer_name,
        .producer_generation = producer_generation,
        .producer_artifact_name = producer_artifact_name,
        .producer_scope_key = producer_scope_key,
        .sources = sources,
        .artifact_sources = artifact_sources,
        .mutation_preconditions = mutation_preconditions,
        .mutations = mutations,
        .publication_digest = publication_digest,
        .baseline = baseline,
        .validation = validation,
        .census = census,
        .completion = completion,
    };
    try command.validate(alloc);
    return .{ .alloc = alloc, .bytes = bytes, .owns_bytes = owns_bytes, .sources = sources, .artifact_sources = artifact_sources, .mutation_preconditions = mutation_preconditions, .mutations = mutations, .baseline_keys = baseline_keys, .repair_documents = repair_documents, .command = command };
}

test "artifact publication compact transport round trips bounded binary values without JSON expansion" {
    const alloc = std.testing.allocator;
    const key = try @import("../internal_keys.zig").embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "model");
    defer alloc.free(key);
    var command: publication.Command = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_name = "model", .producer_generation = 2, .producer_artifact_name = "model", .sources = &.{.{ .document_key = "doc", .content_digest = @splat(3), .timestamp = 4, .input_position = null }}, .mutations = &.{.{ .family = .base_vector, .key = key, .value = "\x00\xff\xfe\x80bytes", .source_index = 0 }}, .publication_digest = @splat(0) };
    command.publication_digest = command.digest();
    const encoded = try encodeAlloc(alloc, command);
    // Both callers rely on the decoder's authenticated command digest. A
    // same-length payload mutation must fail before exposing a command.
    const payload_at = std.mem.indexOf(u8, encoded, command.mutations[0].value.?).?;
    encoded[payload_at] ^= 1;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeBorrowed(alloc, encoded));
    // Even a valid physical checksum cannot authorize changed logical bytes.
    const tampered_checksum = digest(encoded[0 .. encoded.len - 32]);
    @memcpy(encoded[encoded.len - 32 ..], &tampered_checksum);
    try std.testing.expectError(error.InvalidBatchRequest, decodeBorrowed(alloc, encoded));
    encoded[payload_at] ^= 1;
    const restored_checksum = digest(encoded[0 .. encoded.len - 32]);
    @memcpy(encoded[encoded.len - 32 ..], &restored_checksum);
    {
        var borrowed = try decodeBorrowed(alloc, encoded);
        defer borrowed.deinit();
        try std.testing.expect(!borrowed.owns_bytes);
        try std.testing.expectEqual(encoded.ptr, borrowed.bytes.ptr);
        try std.testing.expectEqualDeep(command, borrowed.command);
    }
    var decoded = try decodeOwned(alloc, encoded);
    defer decoded.deinit();
    try std.testing.expectEqualDeep(command.publication_digest, decoded.command.publication_digest);
    try std.testing.expectEqualSlices(u8, command.mutations[0].value.?, decoded.command.mutations[0].value.?);
}

test "artifact publication compact transport owns tombstones and releases every allocation on failure" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            const key = try @import("../internal_keys.zig").embeddingArtifactKeyForDocumentAlloc(alloc, "deleted", "model");
            defer alloc.free(key);
            var command: publication.Command = .{
                .namespace = @splat(1),
                .authority_epoch = 1,
                .catalog_digest = @splat(2),
                .producer_name = "model",
                .producer_generation = 2,
                .producer_artifact_name = "model",
                .sources = &.{.{ .document_key = "deleted", .exists = false, .content_digest = @splat(0), .timestamp = 0, .input_position = .{ .raft = .{ .term = 2, .index = 9 } } }},
                .mutation_preconditions = &.{.{ .key = key, .content_digest = @splat(4), .input_position = .{ .raft = .{ .term = 2, .index = 8 } }, .source_index = 0 }},
                .mutations = &.{.{ .family = .base_vector, .key = key, .value = null, .source_index = 0 }},
                .publication_digest = @splat(0),
            };
            command.publication_digest = command.digest();
            const encoded = try encodeAlloc(alloc, command);
            var decoded = try decodeOwned(alloc, encoded);
            defer decoded.deinit();
            try std.testing.expectEqualDeep(command, decoded.command);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "artifact publication compact transport baseline owns binary row keys across allocation failures" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            const row = try @import("../internal_keys.zig").relationalRowKeyAlloc(alloc, "doc\x00\xff");
            defer alloc.free(row);
            var command: publication.Command = .{
                .mode = .baseline,
                .namespace = @splat(1),
                .authority_epoch = 1,
                .catalog_digest = @splat(2),
                .producer_name = "",
                .producer_generation = 0,
                .producer_artifact_name = "",
                .sources = &.{},
                .mutations = &.{},
                .publication_digest = @splat(0),
                .baseline = .{ .observed_term = 2, .observed_index = 9, .expected_cursor = "", .next_cursor = row, .upper_bound = row, .row_keys = &.{row}, .at_end = true },
            };
            command.publication_digest = command.digest();
            const encoded = try encodeAlloc(alloc, command);
            var decoded = try decodeOwned(alloc, encoded);
            defer decoded.deinit();
            try std.testing.expectEqualDeep(command, decoded.command);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
