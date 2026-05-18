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
const internal_keys = @import("../internal_keys.zig");

const public_id_prefix = "af1:";

pub const PublicIdentity = struct {
    id: []u8,
    artifact_ref: ?types.ArtifactRef = null,

    pub fn deinit(self: *PublicIdentity, alloc: Allocator) void {
        alloc.free(self.id);
        if (self.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
        self.* = undefined;
    }
};

pub const EmbeddingArtifactIdentity = struct {
    embedding_name: []u8,
    doc_key: []u8,
    parent_doc_key: ?[]u8 = null,
    source_artifact_name: ?[]u8 = null,
    chunk_id: ?u32 = null,

    pub fn deinit(self: *EmbeddingArtifactIdentity, alloc: Allocator) void {
        alloc.free(self.embedding_name);
        alloc.free(self.doc_key);
        if (self.parent_doc_key) |parent_doc_key| alloc.free(parent_doc_key);
        if (self.source_artifact_name) |source_artifact_name| alloc.free(source_artifact_name);
        self.* = undefined;
    }
};

const DecodedInternalComponent = struct {
    value: []u8,
    next: usize,
};

pub fn resolvePublicHitIdentityAlloc(alloc: Allocator, key: []const u8) !PublicIdentity {
    if (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, key)) |doc_id| {
        return .{ .id = doc_id };
    }
    if (try decodeArtifactRefAlloc(alloc, key)) |artifact_ref| {
        errdefer {
            var owned = artifact_ref;
            owned.deinit(alloc);
        }
        return .{
            .id = try artifactPublicIdAlloc(alloc, artifact_ref),
            .artifact_ref = artifact_ref,
        };
    }
    return .{ .id = try alloc.dupe(u8, key) };
}

pub fn resolvePublicArtifactIdentityAlloc(alloc: Allocator, key: []const u8) !PublicIdentity {
    var resolved = try resolvePublicHitIdentityAlloc(alloc, key);
    errdefer resolved.deinit(alloc);
    if (resolved.artifact_ref == null) return error.InvalidInternalUserKey;
    return resolved;
}

pub fn decodeEmbeddingArtifactIdentityAlloc(alloc: Allocator, key: []const u8) !?EmbeddingArtifactIdentity {
    var artifact_ref = (try decodeArtifactRefAlloc(alloc, key)) orelse return null;
    defer artifact_ref.deinit(alloc);
    if (artifact_ref.kind != .embedding) return null;

    var identity = EmbeddingArtifactIdentity{
        .embedding_name = try alloc.dupe(u8, artifact_ref.name),
        .doc_key = undefined,
    };
    errdefer identity.deinit(alloc);

    if (artifact_ref.source) |source| {
        identity.source_artifact_name = try alloc.dupe(u8, source.name);
        identity.chunk_id = source.chunk_id;
        switch (source.kind) {
            .chunk => {
                const chunk_id = source.chunk_id orelse return error.InvalidInternalUserKey;
                identity.parent_doc_key = try alloc.dupe(u8, artifact_ref.document_id);
                identity.doc_key = try internal_keys.chunkArtifactKeyAlloc(alloc, artifact_ref.document_id, source.name, chunk_id);
            },
            .summary, .embedding => {
                identity.doc_key = try alloc.dupe(u8, artifact_ref.document_id);
            },
        }
    } else {
        identity.doc_key = try alloc.dupe(u8, artifact_ref.document_id);
    }

    return identity;
}

pub fn decodeArtifactRefAlloc(alloc: Allocator, key: []const u8) !?types.ArtifactRef {
    if (!internal_keys.isInternalUserKey(key)) return null;

    const doc_term = internal_keys.findComponentTerminator(key, 1) orelse return null;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != internal_keys.artifact_kind) return null;
    pos += 1;

    const document_id = try internal_keys.decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(document_id);

    const type_component = (try decodeInternalKeyComponentAlloc(alloc, key, pos)) orelse return error.InvalidInternalUserKey;
    defer alloc.free(type_component.value);
    pos = type_component.next;

    const name_component = (try decodeInternalKeyComponentAlloc(alloc, key, pos)) orelse return error.InvalidInternalUserKey;
    pos = name_component.next;
    errdefer alloc.free(name_component.value);

    var source_chunk_id: ?u32 = null;
    if (std.mem.eql(u8, type_component.value, "chunk")) {
        if (pos + 1 + @sizeOf(u32) > key.len or key[pos] != internal_keys.chunk_record_kind) return error.InvalidInternalUserKey;
        const chunk_bytes: *const [@sizeOf(u32)]u8 = @ptrCast(key[pos + 1 .. pos + 1 + @sizeOf(u32)].ptr);
        source_chunk_id = std.mem.readInt(u32, chunk_bytes, .big);
        pos += 1 + @sizeOf(u32);
        if (pos == key.len) {
            return .{
                .document_id = document_id,
                .name = name_component.value,
                .kind = .chunk,
                .chunk_id = source_chunk_id,
            };
        }
    }

    if (pos == key.len) {
        return .{
            .document_id = document_id,
            .name = name_component.value,
            .kind = try decodeArtifactKind(type_component.value),
        };
    }

    if (key[pos] != internal_keys.derived_embedding_kind) return error.InvalidInternalUserKey;
    pos += 1;

    const derived_name_component = (try decodeInternalKeyComponentAlloc(alloc, key, pos)) orelse return error.InvalidInternalUserKey;
    errdefer alloc.free(derived_name_component.value);
    if (derived_name_component.next != key.len) return error.InvalidInternalUserKey;

    const source = types.ArtifactSourceRef{
        .kind = try decodeArtifactKind(type_component.value),
        .name = name_component.value,
        .chunk_id = source_chunk_id,
    };

    return .{
        .document_id = document_id,
        .name = derived_name_component.value,
        .kind = .embedding,
        .source = source,
    };
}

pub fn artifactPublicIdAlloc(alloc: Allocator, artifact_ref: types.ArtifactRef) ![]u8 {
    try validateArtifactRef(artifact_ref);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, public_id_prefix);
    try out.appendSlice(alloc, artifactKindLabel(artifact_ref.kind));
    try out.append(alloc, ':');
    try appendBase64UrlComponent(alloc, &out, artifact_ref.document_id);
    try out.append(alloc, ':');
    try appendBase64UrlComponent(alloc, &out, artifact_ref.name);
    if (artifact_ref.chunk_id) |chunk_id| {
        try appendDecimalComponent(alloc, &out, chunk_id);
    }
    if (artifact_ref.source) |source| {
        try out.appendSlice(alloc, ":from:");
        try out.appendSlice(alloc, artifactKindLabel(source.kind));
        try out.append(alloc, ':');
        try appendBase64UrlComponent(alloc, &out, source.name);
        if (source.chunk_id) |chunk_id| {
            try appendDecimalComponent(alloc, &out, chunk_id);
        }
    }

    return try out.toOwnedSlice(alloc);
}

pub fn decodeArtifactPublicIdAlloc(alloc: Allocator, artifact_id: []const u8) !?types.ArtifactRef {
    if (!std.mem.startsWith(u8, artifact_id, public_id_prefix)) return null;

    var parts = std.mem.splitScalar(u8, artifact_id[public_id_prefix.len..], ':');
    const kind_raw = parts.next() orelse return error.InvalidArgument;
    const document_raw = parts.next() orelse return error.InvalidArgument;
    const name_raw = parts.next() orelse return error.InvalidArgument;

    var artifact_ref = types.ArtifactRef{
        .document_id = try decodeBase64UrlComponentAlloc(alloc, document_raw),
        .name = try decodeBase64UrlComponentAlloc(alloc, name_raw),
        .kind = try decodeArtifactKind(kind_raw),
    };
    errdefer artifact_ref.deinit(alloc);

    if (artifact_ref.kind == .chunk) {
        const chunk_raw = parts.next() orelse return error.InvalidArgument;
        artifact_ref.chunk_id = try std.fmt.parseUnsigned(u32, chunk_raw, 10);
    }

    if (parts.next()) |marker| {
        if (!std.mem.eql(u8, marker, "from")) return error.InvalidArgument;

        const source_kind = parts.next() orelse return error.InvalidArgument;
        const source_name = parts.next() orelse return error.InvalidArgument;
        var source = types.ArtifactSourceRef{
            .kind = try decodeArtifactKind(source_kind),
            .name = try decodeBase64UrlComponentAlloc(alloc, source_name),
        };
        errdefer source.deinit(alloc);

        if (source.kind == .chunk) {
            const source_chunk_raw = parts.next() orelse return error.InvalidArgument;
            source.chunk_id = try std.fmt.parseUnsigned(u32, source_chunk_raw, 10);
        }

        artifact_ref.source = source;
    }

    if (parts.next() != null) return error.InvalidArgument;
    try validateArtifactRef(artifact_ref);
    return artifact_ref;
}

pub fn internalKeyForArtifactRefAlloc(alloc: Allocator, artifact_ref: types.ArtifactRef) ![]u8 {
    try validateArtifactRef(artifact_ref);

    return switch (artifact_ref.kind) {
        .chunk => internal_keys.chunkArtifactKeyAlloc(alloc, artifact_ref.document_id, artifact_ref.name, artifact_ref.chunk_id.?),
        .summary => internal_keys.artifactNamedPrefixAlloc(alloc, artifact_ref.document_id, "summary", artifact_ref.name),
        .embedding => {
            if (artifact_ref.source) |source| {
                const base_ref = types.ArtifactRef{
                    .document_id = artifact_ref.document_id,
                    .name = source.name,
                    .kind = source.kind,
                    .chunk_id = source.chunk_id,
                    .source = null,
                };
                const base_key = try internalKeyForArtifactRefAlloc(alloc, base_ref);
                defer alloc.free(base_key);
                return internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, base_key, artifact_ref.name);
            }
            return internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, artifact_ref.document_id, artifact_ref.name);
        },
    };
}

fn decodeInternalKeyComponentAlloc(alloc: Allocator, key: []const u8, start: usize) !?DecodedInternalComponent {
    const term = internal_keys.findComponentTerminator(key, start) orelse return null;
    return .{
        .value = try internal_keys.decodeBodyAlloc(alloc, key[start..term]),
        .next = term + 2,
    };
}

fn decodeArtifactKind(raw_kind: []const u8) !types.ArtifactKind {
    if (std.mem.eql(u8, raw_kind, "chunk")) return .chunk;
    if (std.mem.eql(u8, raw_kind, "summary")) return .summary;
    if (std.mem.eql(u8, raw_kind, "embedding")) return .embedding;
    return error.InvalidInternalUserKey;
}

fn appendBase64UrlComponent(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const encoded_len = encoder.calcSize(bytes.len);
    const start = out.items.len;
    try out.resize(alloc, start + encoded_len);
    _ = encoder.encode(out.items[start .. start + encoded_len], bytes);
}

fn appendDecimalComponent(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [16]u8 = undefined;
    const rendered = try std.fmt.bufPrint(&buf, ":{d}", .{value});
    try out.appendSlice(alloc, rendered);
}

fn decodeBase64UrlComponentAlloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    const decoder = std.base64.url_safe_no_pad.Decoder;
    const decoded_len = try decoder.calcSizeForSlice(encoded);
    const out = try alloc.alloc(u8, decoded_len);
    errdefer alloc.free(out);
    try decoder.decode(out, encoded);
    return out;
}

fn artifactKindLabel(kind: types.ArtifactKind) []const u8 {
    return switch (kind) {
        .chunk => "chunk",
        .summary => "summary",
        .embedding => "embedding",
    };
}

fn validateArtifactRef(artifact_ref: types.ArtifactRef) !void {
    switch (artifact_ref.kind) {
        .chunk => {
            if (artifact_ref.chunk_id == null or artifact_ref.source != null) return error.InvalidArgument;
        },
        .summary => {
            if (artifact_ref.chunk_id != null or artifact_ref.source != null) return error.InvalidArgument;
        },
        .embedding => {
            if (artifact_ref.chunk_id != null) return error.InvalidArgument;
        },
    }

    if (artifact_ref.source) |source| {
        if (artifact_ref.kind != .embedding) return error.InvalidArgument;
        switch (source.kind) {
            .chunk => if (source.chunk_id == null) return error.InvalidArgument,
            .summary, .embedding => if (source.chunk_id != null) return error.InvalidArgument,
        }
    }
}

fn expectArtifactRefEqual(expected: types.ArtifactRef, actual: types.ArtifactRef) !void {
    try std.testing.expectEqualStrings(expected.document_id, actual.document_id);
    try std.testing.expectEqualStrings(expected.name, actual.name);
    try std.testing.expectEqual(expected.kind, actual.kind);
    try std.testing.expectEqual(expected.chunk_id, actual.chunk_id);
    if (expected.source) |expected_source| {
        const actual_source = actual.source orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(expected_source.kind, actual_source.kind);
        try std.testing.expectEqualStrings(expected_source.name, actual_source.name);
        try std.testing.expectEqual(expected_source.chunk_id, actual_source.chunk_id);
    } else {
        try std.testing.expect(actual.source == null);
    }
}

test "artifact public id round trips chunk artifact refs" {
    const alloc = std.testing.allocator;

    var artifact_ref = types.ArtifactRef{
        .document_id = try alloc.dupe(u8, "doc:\x00a"),
        .name = try alloc.dupe(u8, "body_chunks_v1"),
        .kind = .chunk,
        .chunk_id = 7,
    };
    defer artifact_ref.deinit(alloc);

    const public_id = try artifactPublicIdAlloc(alloc, artifact_ref);
    defer alloc.free(public_id);

    var decoded = (try decodeArtifactPublicIdAlloc(alloc, public_id)).?;
    defer decoded.deinit(alloc);
    try expectArtifactRefEqual(artifact_ref, decoded);

    const internal_key = try internalKeyForArtifactRefAlloc(alloc, decoded);
    defer alloc.free(internal_key);

    var decoded_internal = (try decodeArtifactRefAlloc(alloc, internal_key)).?;
    defer decoded_internal.deinit(alloc);
    try expectArtifactRefEqual(artifact_ref, decoded_internal);
}

test "artifact public id round trips derived embedding refs" {
    const alloc = std.testing.allocator;

    var artifact_ref = types.ArtifactRef{
        .document_id = try alloc.dupe(u8, "doc:a"),
        .name = try alloc.dupe(u8, "chunk_dense_v1"),
        .kind = .embedding,
        .source = .{
            .kind = .chunk,
            .name = try alloc.dupe(u8, "body_chunks_v1"),
            .chunk_id = 2,
        },
    };
    defer artifact_ref.deinit(alloc);

    const public_id = try artifactPublicIdAlloc(alloc, artifact_ref);
    defer alloc.free(public_id);

    var decoded = (try decodeArtifactPublicIdAlloc(alloc, public_id)).?;
    defer decoded.deinit(alloc);
    try expectArtifactRefEqual(artifact_ref, decoded);

    const internal_key = try internalKeyForArtifactRefAlloc(alloc, decoded);
    defer alloc.free(internal_key);

    var decoded_internal = (try decodeArtifactRefAlloc(alloc, internal_key)).?;
    defer decoded_internal.deinit(alloc);
    try expectArtifactRefEqual(artifact_ref, decoded_internal);
}

test "decode embedding artifact identity for document embedding" {
    const alloc = std.testing.allocator;

    const key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(key);

    var identity = (try decodeEmbeddingArtifactIdentityAlloc(alloc, key)).?;
    defer identity.deinit(alloc);

    try std.testing.expectEqualStrings("dv_v1", identity.embedding_name);
    try std.testing.expectEqualStrings("doc:a", identity.doc_key);
    try std.testing.expect(identity.parent_doc_key == null);
    try std.testing.expect(identity.source_artifact_name == null);
    try std.testing.expect(identity.chunk_id == null);
}

test "decode embedding artifact identity for chunk embedding" {
    const alloc = std.testing.allocator;

    const chunk_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 3);
    defer alloc.free(chunk_key);
    const key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "body_dense_v1");
    defer alloc.free(key);

    var identity = (try decodeEmbeddingArtifactIdentityAlloc(alloc, key)).?;
    defer identity.deinit(alloc);

    try std.testing.expectEqualStrings("body_dense_v1", identity.embedding_name);
    try std.testing.expectEqualStrings(chunk_key, identity.doc_key);
    try std.testing.expectEqualStrings("doc:a", identity.parent_doc_key.?);
    try std.testing.expectEqualStrings("body_chunks_v1", identity.source_artifact_name.?);
    try std.testing.expectEqual(@as(u32, 3), identity.chunk_id.?);
}
