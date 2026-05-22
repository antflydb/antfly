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

pub const user_namespace: u8 = 0x01;
pub const replay_namespace: u8 = 0x02;
pub const identity_namespace: u8 = 0x03;
pub const replay_all_kind: u8 = 0xfe;

pub const primary_kind: u8 = 0x10;
pub const ttl_kind: u8 = 0x11;
pub const artifact_kind: u8 = 0x20;
pub const chunk_record_kind: u8 = 0x30;
pub const derived_embedding_kind: u8 = 0x31;
pub const graph_edge_record_kind: u8 = 0x32;

pub const replay_key_len: usize = 1 + 1 + @sizeOf(u64);
pub const replay_meta_init_key = [_]u8{ replay_namespace, 0xff, 0x01 };
pub const replay_meta_next_sequence_key = [_]u8{ replay_namespace, 0xff, 0x02 };
pub const replay_meta_latest_sequence_kind: u8 = 0x03;
pub const artifact_presence_key = [_]u8{ replay_namespace, 0xff, 0x20 };
pub const identity_doc_to_ordinal_kind: u8 = 0x01;
pub const identity_ordinal_to_doc_kind: u8 = 0x02;
pub const identity_ordinal_state_kind: u8 = 0x03;
pub const identity_canonical_to_ordinal_kind: u8 = 0x04;
pub const identity_namespace_key = [_]u8{ identity_namespace, 0xff, 0x00 };
pub const identity_next_ordinal_key = [_]u8{ identity_namespace, 0xff, 0x01 };

pub fn isInternalMetadataKey(key: []const u8) bool {
    if (key.len == 0) return false;
    return key[0] == replay_namespace or key[0] == identity_namespace;
}

pub fn isInternalUserKey(key: []const u8) bool {
    return key.len > 0 and key[0] == user_namespace;
}

pub fn encodedBodyLen(bytes: []const u8) usize {
    var extra: usize = 0;
    for (bytes) |b| {
        if (b == 0) extra += 1;
    }
    return bytes.len + extra;
}

pub fn encodedComponentLen(bytes: []const u8) usize {
    return encodedBodyLen(bytes) + 2;
}

pub fn encodeBody(out: []u8, bytes: []const u8) usize {
    var pos: usize = 0;
    for (bytes) |b| {
        if (b == 0) {
            out[pos] = 0;
            out[pos + 1] = 0xff;
            pos += 2;
        } else {
            out[pos] = b;
            pos += 1;
        }
    }
    return pos;
}

pub fn encodeComponent(out: []u8, bytes: []const u8) usize {
    const pos = encodeBody(out, bytes);
    out[pos] = 0;
    out[pos + 1] = 0;
    return pos + 2;
}

pub fn appendEncodedComponent(list: *std.ArrayListUnmanaged(u8), alloc: Allocator, bytes: []const u8) !void {
    const start = list.items.len;
    try list.resize(alloc, start + encodedComponentLen(bytes));
    _ = encodeComponent(list.items[start..], bytes);
}

pub fn findComponentTerminator(key: []const u8, start: usize) ?usize {
    var i = start;
    while (i + 1 < key.len) : (i += 1) {
        if (key[i] != 0) continue;
        if (key[i + 1] == 0) return i;
        if (key[i + 1] == 0xff) {
            i += 1;
            continue;
        }
        return null;
    }
    return null;
}

pub fn decodeBodyAlloc(alloc: Allocator, body: []const u8) ![]u8 {
    var out = try alloc.alloc(u8, maxDecodedLen(body));
    errdefer alloc.free(out);

    var in_pos: usize = 0;
    var out_pos: usize = 0;
    while (in_pos < body.len) {
        const b = body[in_pos];
        if (b != 0) {
            out[out_pos] = b;
            in_pos += 1;
            out_pos += 1;
            continue;
        }

        if (in_pos + 1 >= body.len or body[in_pos + 1] != 0xff) return error.InvalidInternalUserKey;
        out[out_pos] = 0;
        in_pos += 2;
        out_pos += 1;
    }

    return try alloc.realloc(out, out_pos);
}

pub fn decodeBodyView(body: []const u8) !?[]const u8 {
    var i: usize = 0;
    while (i < body.len) : (i += 1) {
        if (body[i] != 0) continue;
        if (i + 1 >= body.len or body[i + 1] != 0xff) return error.InvalidInternalUserKey;
        return null;
    }
    return body;
}

fn maxDecodedLen(body: []const u8) usize {
    return body.len;
}

pub fn appendDocumentPrefix(list: *std.ArrayListUnmanaged(u8), alloc: Allocator, doc_key: []const u8) !void {
    try list.append(alloc, user_namespace);
    try appendEncodedComponent(list, alloc, doc_key);
}

pub fn appendDocumentRangeLower(list: *std.ArrayListUnmanaged(u8), alloc: Allocator, prefix: []const u8) !void {
    try list.append(alloc, user_namespace);
    const start = list.items.len;
    try list.resize(alloc, start + encodedBodyLen(prefix));
    _ = encodeBody(list.items[start..], prefix);
}

pub fn documentKeyAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var key = try alloc.alloc(u8, 1 + encodedComponentLen(doc_key) + 1);
    key[0] = user_namespace;
    const pos = 1 + encodeComponent(key[1..], doc_key);
    key[pos] = primary_kind;
    return key;
}

pub fn ttlKeyAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, ttl_kind);
    return try list.toOwnedSlice(alloc);
}

pub fn documentExactPrefixAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn documentRangeLowerAlloc(alloc: Allocator, prefix: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentRangeLower(&list, alloc, prefix);
    return try list.toOwnedSlice(alloc);
}

pub fn documentRangeUpperAlloc(alloc: Allocator, prefix: []const u8) !?[]u8 {
    const lower = try documentRangeLowerAlloc(alloc, prefix);
    errdefer alloc.free(lower);
    const upper = try nextPrefixAlloc(alloc, lower);
    alloc.free(lower);
    return upper;
}

pub fn artifactRootPrefixAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    return try list.toOwnedSlice(alloc);
}

pub fn artifactTypePrefixAlloc(alloc: Allocator, doc_key: []const u8, artifact_type: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, artifact_type);
    return try list.toOwnedSlice(alloc);
}

pub fn artifactNamedPrefixAlloc(alloc: Allocator, doc_key: []const u8, artifact_type: []const u8, artifact_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, artifact_type);
    try appendEncodedComponent(&list, alloc, artifact_name);

    return try list.toOwnedSlice(alloc);
}

pub fn chunkArtifactKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8, chunk_id: u32) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, "chunk");
    try appendEncodedComponent(&list, alloc, artifact_name);

    try list.append(alloc, chunk_record_kind);
    const be = std.mem.nativeToBig(u32, chunk_id);
    try list.appendSlice(alloc, std.mem.asBytes(&be));

    return try list.toOwnedSlice(alloc);
}

pub fn embeddingArtifactKeyForDocumentAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8) ![]u8 {
    return artifactNamedPrefixAlloc(alloc, doc_key, "embedding", artifact_name);
}

pub fn derivedEmbeddingArtifactKeyAlloc(alloc: Allocator, base_internal_key: []const u8, artifact_name: []const u8) ![]u8 {
    if (!isInternalUserKey(base_internal_key)) return error.InvalidInternalUserKey;

    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, base_internal_key);
    try list.append(alloc, derived_embedding_kind);
    const start = list.items.len;
    try list.resize(alloc, start + encodedComponentLen(artifact_name));
    _ = encodeComponent(list.items[start..], artifact_name);
    return try list.toOwnedSlice(alloc);
}

pub fn derivedEmbeddingArtifactPrefixAlloc(alloc: Allocator, base_internal_key: []const u8, artifact_name: []const u8) ![]u8 {
    return derivedEmbeddingArtifactKeyAlloc(alloc, base_internal_key, artifact_name);
}

pub fn graphArtifactIndexPrefixAlloc(alloc: Allocator, doc_key: []const u8, index_name: []const u8) ![]u8 {
    return artifactNamedPrefixAlloc(alloc, doc_key, "graph", index_name);
}

pub fn graphEdgeArtifactPrefixAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    index_name: []const u8,
    edge_type: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, "graph");
    try appendEncodedComponent(&list, alloc, index_name);
    try list.append(alloc, graph_edge_record_kind);
    if (edge_type.len > 0) try appendEncodedComponent(&list, alloc, edge_type);

    return try list.toOwnedSlice(alloc);
}

pub fn graphEdgeArtifactKeyAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    index_name: []const u8,
    edge_type: []const u8,
    target_doc_key: []const u8,
) ![]u8 {
    const total_len = 1 +
        encodedComponentLen(doc_key) +
        1 +
        encodedComponentLen("graph") +
        encodedComponentLen(index_name) +
        1 +
        encodedComponentLen(edge_type) +
        encodedComponentLen(target_doc_key);
    const out = try alloc.alloc(u8, total_len);
    errdefer alloc.free(out);

    var pos: usize = 0;
    out[pos] = user_namespace;
    pos += 1;
    pos += encodeComponent(out[pos..], doc_key);
    out[pos] = artifact_kind;
    pos += 1;
    pos += encodeComponent(out[pos..], "graph");
    pos += encodeComponent(out[pos..], index_name);
    out[pos] = graph_edge_record_kind;
    pos += 1;
    pos += encodeComponent(out[pos..], edge_type);
    pos += encodeComponent(out[pos..], target_doc_key);
    std.debug.assert(pos == out.len);
    return out;
}

pub fn derivedEmbeddingBaseKeyAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (!isDerivedEmbeddingArtifactKey(key)) return null;

    const doc_term = findComponentTerminator(key, 1).?;
    var pos = doc_term + 2;
    if (key[pos] == artifact_kind) {
        pos += 1;

        const type_term = findComponentTerminator(key, pos).?;
        pos = type_term + 2;

        const name_term = findComponentTerminator(key, pos).?;
        pos = name_term + 2;

        if (key[pos] == chunk_record_kind) {
            pos += 1 + @sizeOf(u32);
        }
    }

    if (key[pos] != derived_embedding_kind) return error.InvalidInternalUserKey;
    return try alloc.dupe(u8, key[0..pos]);
}

pub fn isPrimaryDocumentKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const term = findComponentTerminator(key, 1) orelse return false;
    return term + 3 == key.len and key[term + 2] == primary_kind;
}

pub fn isTtlKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const term = findComponentTerminator(key, 1) orelse return false;
    return term + 3 == key.len and key[term + 2] == ttl_kind;
}

pub fn decodePrimaryDocumentKeyAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (!isPrimaryDocumentKey(key)) return null;
    const term = findComponentTerminator(key, 1).?;
    return try decodeBodyAlloc(alloc, key[1..term]);
}

pub fn decodeDocumentComponentAlloc(alloc: Allocator, key: []const u8) !?[]u8 {
    if (!isInternalUserKey(key)) return null;
    const term = findComponentTerminator(key, 1) orelse return null;
    return try decodeBodyAlloc(alloc, key[1..term]);
}

pub fn isChunkArtifactRecordKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, "chunk")) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    const name_term = findComponentTerminator(key, pos) orelse return false;
    pos = name_term + 2;

    return pos + 5 == key.len and key[pos] == chunk_record_kind;
}

pub fn matchesChunkArtifactName(key: []const u8, artifact_name: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, "chunk")) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    if (!componentEquals(key, pos, artifact_name)) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    return pos + 5 == key.len and key[pos] == chunk_record_kind;
}

pub fn matchesEmbeddingArtifactName(key: []const u8, artifact_name: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, "embedding")) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    if (!componentEquals(key, pos, artifact_name)) return false;
    return findComponentTerminator(key, pos).? + 2 == key.len;
}

pub fn isDerivedEmbeddingArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;

    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len) return false;

    switch (key[pos]) {
        primary_kind, ttl_kind => return false,
        artifact_kind => {
            pos += 1;

            const type_term = findComponentTerminator(key, pos) orelse return false;
            pos = type_term + 2;

            const name_term = findComponentTerminator(key, pos) orelse return false;
            pos = name_term + 2;

            if (pos == key.len) return false;
            if (key[pos] == chunk_record_kind) {
                pos += 1 + @sizeOf(u32);
            }
        },
        else => return false,
    }

    if (pos >= key.len or key[pos] != derived_embedding_kind) return false;
    pos += 1;

    const embedding_term = findComponentTerminator(key, pos) orelse return false;
    return embedding_term + 2 == key.len;
}

pub fn matchesDerivedEmbeddingArtifactName(key: []const u8, artifact_name: []const u8) bool {
    if (!isInternalUserKey(key)) return false;

    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len) return false;

    switch (key[pos]) {
        primary_kind, ttl_kind => return false,
        artifact_kind => {
            pos += 1;

            const type_term = findComponentTerminator(key, pos) orelse return false;
            pos = type_term + 2;

            const name_term = findComponentTerminator(key, pos) orelse return false;
            pos = name_term + 2;

            if (pos == key.len) return false;
            if (key[pos] == chunk_record_kind) {
                pos += 1 + @sizeOf(u32);
            }
        },
        else => return false,
    }

    if (pos >= key.len or key[pos] != derived_embedding_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, artifact_name)) return false;
    return findComponentTerminator(key, pos).? + 2 == key.len;
}

pub fn isGraphEdgeArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, "graph")) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    const index_term = findComponentTerminator(key, pos) orelse return false;
    pos = index_term + 2;

    if (pos >= key.len or key[pos] != graph_edge_record_kind) return false;
    pos += 1;

    const edge_type_term = findComponentTerminator(key, pos) orelse return false;
    pos = edge_type_term + 2;

    const target_term = findComponentTerminator(key, pos) orelse return false;
    return target_term + 2 == key.len;
}

pub fn componentEquals(key: []const u8, start: usize, raw: []const u8) bool {
    const term = findComponentTerminator(key, start) orelse return false;
    var in_pos = start;
    var raw_pos: usize = 0;
    while (in_pos < term) {
        if (raw_pos >= raw.len) return false;
        const b = key[in_pos];
        if (b != 0) {
            if (raw[raw_pos] != b) return false;
            in_pos += 1;
            raw_pos += 1;
            continue;
        }
        if (in_pos + 1 >= term or key[in_pos + 1] != 0xff) return false;
        if (raw[raw_pos] != 0) return false;
        in_pos += 2;
        raw_pos += 1;
    }
    return raw_pos == raw.len;
}

/// Returns true if key is an embedding artifact: [0x01][doc][0x00 0x00][0x20]["embedding"][0x00 0x00][name][0x00 0x00]
pub fn isEmbeddingArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;
    // Check artifact type is "embedding"
    if (!componentEquals(key, pos, "embedding")) return false;
    const type_term = findComponentTerminator(key, pos) orelse return false;
    pos = type_term + 2;
    // Must have exactly one more component (the artifact name)
    const name_term = findComponentTerminator(key, pos) orelse return false;
    return name_term + 2 == key.len;
}

/// Returns true if key is a summary artifact: [0x01][doc][0x00 0x00][0x20]["summary"][0x00 0x00][name][0x00 0x00]
pub fn isSummaryArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;
    if (!componentEquals(key, pos, "summary")) return false;
    const type_term = findComponentTerminator(key, pos) orelse return false;
    pos = type_term + 2;
    const name_term = findComponentTerminator(key, pos) orelse return false;
    return name_term + 2 == key.len;
}

/// Parse an embedding artifact key, returning (doc_key, artifact_name).
/// Returns null if the key is not an embedding artifact key.
pub fn parseEmbeddingArtifactKeyAlloc(alloc: Allocator, key: []const u8) !?struct { doc_key: []u8, artifact_name: []u8 } {
    if (!isEmbeddingArtifactKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1).?;
    const doc_key = try decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);

    // Skip [0x00 0x00][artifact_kind][encoded("embedding")][0x00 0x00]
    var pos = doc_term + 2 + 1; // past artifact_kind byte
    const type_term = findComponentTerminator(key, pos).?;
    pos = type_term + 2;

    // Decode artifact name
    const name_term = findComponentTerminator(key, pos).?;
    const artifact_name = try decodeBodyAlloc(alloc, key[pos..name_term]);

    return .{ .doc_key = doc_key, .artifact_name = artifact_name };
}

pub fn parseEmbeddingArtifactKeyView(key: []const u8) !?struct { doc_key: []const u8, artifact_name: []const u8 } {
    if (!isInternalUserKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1) orelse return null;
    const doc_key = (try decodeBodyView(key[1..doc_term])) orelse return null;

    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return null;
    pos += 1;

    if (!componentEquals(key, pos, "embedding")) return null;
    const type_term = findComponentTerminator(key, pos) orelse return null;
    pos = type_term + 2;

    const name_term = findComponentTerminator(key, pos) orelse return null;
    if (name_term + 2 != key.len) return null;
    const artifact_name = (try decodeBodyView(key[pos..name_term])) orelse return null;

    return .{ .doc_key = doc_key, .artifact_name = artifact_name };
}

pub fn parseGraphEdgeArtifactKeyAlloc(
    alloc: Allocator,
    key: []const u8,
) !?struct { doc_key: []u8, index_name: []u8, edge_type: []u8, target_doc_key: []u8 } {
    if (!isGraphEdgeArtifactKey(key)) return null;

    const doc_term = findComponentTerminator(key, 1).?;
    const doc_key = try decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);

    var pos = doc_term + 2 + 1;
    const type_term = findComponentTerminator(key, pos).?;
    pos = type_term + 2;

    const index_term = findComponentTerminator(key, pos).?;
    const index_name = try decodeBodyAlloc(alloc, key[pos..index_term]);
    errdefer alloc.free(index_name);
    pos = index_term + 2;

    if (key[pos] != graph_edge_record_kind) return error.InvalidInternalUserKey;
    pos += 1;

    const edge_type_term = findComponentTerminator(key, pos).?;
    const edge_type = try decodeBodyAlloc(alloc, key[pos..edge_type_term]);
    errdefer alloc.free(edge_type);
    pos = edge_type_term + 2;

    const target_term = findComponentTerminator(key, pos).?;
    const target_doc_key = try decodeBodyAlloc(alloc, key[pos..target_term]);

    return .{
        .doc_key = doc_key,
        .index_name = index_name,
        .edge_type = edge_type,
        .target_doc_key = target_doc_key,
    };
}

pub fn nextPrefixAlloc(alloc: Allocator, prefix: []const u8) !?[]u8 {
    var out = try alloc.dupe(u8, prefix);
    errdefer alloc.free(out);

    var i = out.len;
    while (i > 0) {
        i -= 1;
        if (out[i] == 0xff) continue;
        out[i] += 1;
        return try alloc.realloc(out, i + 1);
    }

    alloc.free(out);
    return null;
}

pub fn replayEntryKey(hint_ordinal: u8, sequence: u64) [replay_key_len]u8 {
    var key: [replay_key_len]u8 = undefined;
    key[0] = replay_namespace;
    key[1] = hint_ordinal;
    std.mem.writeInt(u64, key[2..], sequence, .big);
    return key;
}

pub fn replayRangeLower(hint_ordinal: u8, from_sequence: u64) [replay_key_len]u8 {
    return replayEntryKey(hint_ordinal, from_sequence);
}

pub fn replayRangeUpper(hint_ordinal: u8) [2]u8 {
    return .{ replay_namespace, hint_ordinal + 1 };
}

pub fn replayLatestSequenceKey(hint_ordinal: u8) [4]u8 {
    return .{ replay_namespace, 0xff, replay_meta_latest_sequence_kind, hint_ordinal };
}

pub fn identityDocToOrdinalKeyAlloc(alloc: Allocator, doc_id: []const u8) ![]u8 {
    var key = try alloc.alloc(u8, 2 + encodedComponentLen(doc_id));
    key[0] = identity_namespace;
    key[1] = identity_doc_to_ordinal_kind;
    _ = encodeComponent(key[2..], doc_id);
    return key;
}

pub fn identityOrdinalToDocKey(ordinal: u32) [1 + 1 + @sizeOf(u32)]u8 {
    var key: [1 + 1 + @sizeOf(u32)]u8 = undefined;
    key[0] = identity_namespace;
    key[1] = identity_ordinal_to_doc_kind;
    std.mem.writeInt(u32, key[2..][0..4], ordinal, .big);
    return key;
}

pub fn identityOrdinalStateKey(ordinal: u32) [1 + 1 + @sizeOf(u32)]u8 {
    var key: [1 + 1 + @sizeOf(u32)]u8 = undefined;
    key[0] = identity_namespace;
    key[1] = identity_ordinal_state_kind;
    std.mem.writeInt(u32, key[2..][0..4], ordinal, .big);
    return key;
}

pub fn identityCanonicalToOrdinalKey(canonical_doc_id: u64) [1 + 1 + @sizeOf(u64)]u8 {
    var key: [1 + 1 + @sizeOf(u64)]u8 = undefined;
    key[0] = identity_namespace;
    key[1] = identity_canonical_to_ordinal_kind;
    std.mem.writeInt(u64, key[2..][0..8], canonical_doc_id, .big);
    return key;
}

pub fn parseIdentityOrdinalKey(key: []const u8, kind: u8) ?u32 {
    if (key.len != 1 + 1 + @sizeOf(u32)) return null;
    if (key[0] != identity_namespace or key[1] != kind) return null;
    return std.mem.readInt(u32, key[2..][0..4], .big);
}

pub fn parseIdentityCanonicalKey(key: []const u8) ?u64 {
    if (key.len != 1 + 1 + @sizeOf(u64)) return null;
    if (key[0] != identity_namespace or key[1] != identity_canonical_to_ordinal_kind) return null;
    return std.mem.readInt(u64, key[2..][0..8], .big);
}

pub fn parseReplayEntrySequence(key: []const u8, hint_ordinal: u8) ?u64 {
    if (key.len != replay_key_len) return null;
    if (key[0] != replay_namespace or key[1] != hint_ordinal) return null;
    return std.mem.readInt(u64, key[2..10], .big);
}

pub fn isReplayEntryKey(key: []const u8) bool {
    return key.len == replay_key_len and key[0] == replay_namespace;
}

pub fn isReplayMetaInitKey(key: []const u8) bool {
    return std.mem.eql(u8, key, &replay_meta_init_key);
}

test "internal key primary round trip with zero bytes" {
    const alloc = std.testing.allocator;
    const raw = "ab\x00cd";
    const key = try documentKeyAlloc(alloc, raw);
    defer alloc.free(key);

    try std.testing.expect(isPrimaryDocumentKey(key));

    const decoded = (try decodePrimaryDocumentKeyAlloc(alloc, key)).?;
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings(raw, decoded);
}

test "internal key prefix bounds preserve raw prefix grouping" {
    const alloc = std.testing.allocator;
    const lower = try documentRangeLowerAlloc(alloc, "ab");
    defer alloc.free(lower);
    const upper = (try documentRangeUpperAlloc(alloc, "ab")).?;
    defer alloc.free(upper);

    const exact = try documentKeyAlloc(alloc, "ab");
    defer alloc.free(exact);
    const extended = try documentKeyAlloc(alloc, "abz");
    defer alloc.free(extended);
    const outside = try documentKeyAlloc(alloc, "ac");
    defer alloc.free(outside);

    try std.testing.expect(std.mem.order(u8, lower, exact) != .gt);
    try std.testing.expect(std.mem.order(u8, lower, extended) != .gt);
    try std.testing.expect(std.mem.order(u8, exact, upper) == .lt);
    try std.testing.expect(std.mem.order(u8, extended, upper) == .lt);
    try std.testing.expect(std.mem.order(u8, outside, upper) != .lt);
}

test "internal key ordering matches raw document id ordering for adversarial bytes" {
    const alloc = std.testing.allocator;
    const raw_ids = [_][]const u8{
        "",
        ":",
        ":i:",
        ":e:",
        ":t",
        "\x00",
        "\x00\x00",
        "\xff",
        "abc\x00def",
        "abc\xffdef",
        "abc:",
    };

    for (raw_ids) |lhs| {
        const lhs_key = try documentKeyAlloc(alloc, lhs);
        defer alloc.free(lhs_key);
        for (raw_ids) |rhs| {
            const rhs_key = try documentKeyAlloc(alloc, rhs);
            defer alloc.free(rhs_key);
            try std.testing.expectEqual(std.mem.order(u8, lhs, rhs), std.mem.order(u8, lhs_key, rhs_key));
        }
    }
}

test "internal key round trips adversarial document ids" {
    const alloc = std.testing.allocator;
    const raw_ids = [_][]const u8{
        "",
        ":",
        ":i:",
        ":e:",
        ":t",
        "\x00",
        "\x00\x00",
        "\xff",
        "abc\x00def",
        "abc\xffdef",
        "abc:",
    };

    for (raw_ids) |raw| {
        const key = try documentKeyAlloc(alloc, raw);
        defer alloc.free(key);
        const decoded = (try decodePrimaryDocumentKeyAlloc(alloc, key)).?;
        defer alloc.free(decoded);
        try std.testing.expectEqualSlices(u8, raw, decoded);
    }
}

test "internal key binary prefix bounds select only matching document ids" {
    const alloc = std.testing.allocator;
    const raw_ids = [_][]const u8{
        "",
        "\x00",
        "\x00a",
        "\x00\x00",
        "\x00\xff",
        "\x01",
        "abc",
        "abc\x00def",
        "abc\xffdef",
        "abd",
    };
    const prefixes = [_][]const u8{
        "\x00",
        "abc",
        "abc\x00",
    };

    for (prefixes) |prefix| {
        const lower = try documentRangeLowerAlloc(alloc, prefix);
        defer alloc.free(lower);
        const upper = try documentRangeUpperAlloc(alloc, prefix);
        defer if (upper) |u| alloc.free(u);

        for (raw_ids) |raw| {
            const key = try documentKeyAlloc(alloc, raw);
            defer alloc.free(key);
            const in_range = std.mem.order(u8, key, lower) != .lt and
                (upper == null or std.mem.order(u8, key, upper.?) == .lt);
            try std.testing.expectEqual(std.mem.startsWith(u8, raw, prefix), in_range);
        }
    }
}

test "internal key encoded shard boundaries contain encoded primary keys" {
    const alloc = std.testing.allocator;
    const lower = try documentRangeLowerAlloc(alloc, "ab\x00");
    defer alloc.free(lower);
    const upper = (try documentRangeUpperAlloc(alloc, "ab\x00")).?;
    defer alloc.free(upper);

    const inside = try documentKeyAlloc(alloc, "ab\x00c");
    defer alloc.free(inside);
    const outside_before = try documentKeyAlloc(alloc, "ab");
    defer alloc.free(outside_before);
    const outside_after = try documentKeyAlloc(alloc, "ab\x01");
    defer alloc.free(outside_after);

    try std.testing.expect(std.mem.order(u8, inside, lower) != .lt);
    try std.testing.expect(std.mem.order(u8, inside, upper) == .lt);
    try std.testing.expect(std.mem.order(u8, outside_before, lower) == .lt);
    try std.testing.expect(std.mem.order(u8, outside_after, upper) != .lt);
}

test "replay entry key round trip" {
    const key = replayEntryKey(3, 42);
    try std.testing.expect(isReplayEntryKey(&key));
    try std.testing.expectEqual(@as(?u64, 42), parseReplayEntrySequence(&key, 3));
    try std.testing.expectEqual(@as(?u64, null), parseReplayEntrySequence(&key, 2));

    const lower = replayRangeLower(3, 42);
    const upper = replayRangeUpper(3);
    try std.testing.expect(std.mem.order(u8, &lower, &key) != .gt);
    try std.testing.expect(std.mem.order(u8, &key, &upper) == .lt);
}

test "isEmbeddingArtifactKey round trip" {
    const alloc = std.testing.allocator;
    const key = try embeddingArtifactKeyForDocumentAlloc(alloc, "my-doc", "my-index");
    defer alloc.free(key);

    try std.testing.expect(isEmbeddingArtifactKey(key));
    try std.testing.expect(!isPrimaryDocumentKey(key));
    try std.testing.expect(!isSummaryArtifactKey(key));
    try std.testing.expect(!isDerivedEmbeddingArtifactKey(key));

    const parsed = (try parseEmbeddingArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.artifact_name);
    try std.testing.expectEqualStrings("my-doc", parsed.doc_key);
    try std.testing.expectEqualStrings("my-index", parsed.artifact_name);

    const view = (try parseEmbeddingArtifactKeyView(key)).?;
    try std.testing.expectEqualStrings("my-doc", view.doc_key);
    try std.testing.expectEqualStrings("my-index", view.artifact_name);
}

test "embedding artifact key round trip with zero bytes in doc key" {
    const alloc = std.testing.allocator;
    const raw = "ab\x00cd";
    const key = try embeddingArtifactKeyForDocumentAlloc(alloc, raw, "dense");
    defer alloc.free(key);

    const parsed = (try parseEmbeddingArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.artifact_name);
    try std.testing.expectEqualStrings(raw, parsed.doc_key);
    try std.testing.expectEqualStrings("dense", parsed.artifact_name);
    try std.testing.expectEqual(null, try parseEmbeddingArtifactKeyView(key));
}

test "embedding artifact key round trip with arbitrary doc and artifact bytes" {
    const alloc = std.testing.allocator;
    const raw_doc = "ab\x00:i:\xff";
    const raw_name = "dense\x00name\xff";
    const key = try embeddingArtifactKeyForDocumentAlloc(alloc, raw_doc, raw_name);
    defer alloc.free(key);

    const parsed = (try parseEmbeddingArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.artifact_name);
    try std.testing.expectEqualSlices(u8, raw_doc, parsed.doc_key);
    try std.testing.expectEqualSlices(u8, raw_name, parsed.artifact_name);
    try std.testing.expectEqual(null, try parseEmbeddingArtifactKeyView(key));
}

test "matchesEmbeddingArtifactName matches exact embedding artifact name" {
    const alloc = std.testing.allocator;
    const key = try embeddingArtifactKeyForDocumentAlloc(alloc, "my-doc", "my-index");
    defer alloc.free(key);

    try std.testing.expect(matchesEmbeddingArtifactName(key, "my-index"));
    try std.testing.expect(!matchesEmbeddingArtifactName(key, "other-index"));
}

test "derivedEmbeddingBaseKeyAlloc returns chunk artifact key" {
    const alloc = std.testing.allocator;
    const chunk_key = try chunkArtifactKeyAlloc(alloc, "doc1", "chunks", 7);
    defer alloc.free(chunk_key);
    const embedding_key = try derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "dense");
    defer alloc.free(embedding_key);

    const base = (try derivedEmbeddingBaseKeyAlloc(alloc, embedding_key)).?;
    defer alloc.free(base);
    try std.testing.expectEqualStrings(chunk_key, base);
}

test "matchesDerivedEmbeddingArtifactName matches exact derived embedding artifact name" {
    const alloc = std.testing.allocator;
    const chunk_key = try chunkArtifactKeyAlloc(alloc, "doc1", "chunks", 7);
    defer alloc.free(chunk_key);
    const embedding_key = try derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "dense");
    defer alloc.free(embedding_key);

    try std.testing.expect(matchesDerivedEmbeddingArtifactName(embedding_key, "dense"));
    try std.testing.expect(!matchesDerivedEmbeddingArtifactName(embedding_key, "other"));
}

test "isSummaryArtifactKey" {
    const alloc = std.testing.allocator;
    const key = try artifactNamedPrefixAlloc(alloc, "doc1", "summary", "my-summary");
    defer alloc.free(key);

    try std.testing.expect(isSummaryArtifactKey(key));
    try std.testing.expect(!isEmbeddingArtifactKey(key));
    try std.testing.expect(!isPrimaryDocumentKey(key));
}

test "parseEmbeddingArtifactKeyAlloc returns null for non-embedding" {
    const alloc = std.testing.allocator;
    const doc_key = try documentKeyAlloc(alloc, "doc1");
    defer alloc.free(doc_key);
    try std.testing.expectEqual(null, try parseEmbeddingArtifactKeyAlloc(alloc, doc_key));
}

test "graph edge artifact key round trip" {
    const alloc = std.testing.allocator;
    const key = try graphEdgeArtifactKeyAlloc(alloc, "doc:a", "gr_v1", "links", "doc:b");
    defer alloc.free(key);

    try std.testing.expect(isGraphEdgeArtifactKey(key));
    try std.testing.expect(!isEmbeddingArtifactKey(key));

    const parsed = (try parseGraphEdgeArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.index_name);
    defer alloc.free(parsed.edge_type);
    defer alloc.free(parsed.target_doc_key);
    try std.testing.expectEqualStrings("doc:a", parsed.doc_key);
    try std.testing.expectEqualStrings("gr_v1", parsed.index_name);
    try std.testing.expectEqualStrings("links", parsed.edge_type);
    try std.testing.expectEqualStrings("doc:b", parsed.target_doc_key);
}

test "graph edge artifact key round trip with arbitrary source and target ids" {
    const alloc = std.testing.allocator;
    const source = "doc\x00:i:\xffsource";
    const target = "\x00target:out:\xff";
    const edge_type = "links\x00typed";
    const key = try graphEdgeArtifactKeyAlloc(alloc, source, "gr\x00v1", edge_type, target);
    defer alloc.free(key);

    try std.testing.expect(isGraphEdgeArtifactKey(key));
    const parsed = (try parseGraphEdgeArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.index_name);
    defer alloc.free(parsed.edge_type);
    defer alloc.free(parsed.target_doc_key);
    try std.testing.expectEqualSlices(u8, source, parsed.doc_key);
    try std.testing.expectEqualSlices(u8, "gr\x00v1", parsed.index_name);
    try std.testing.expectEqualSlices(u8, edge_type, parsed.edge_type);
    try std.testing.expectEqualSlices(u8, target, parsed.target_doc_key);
}
