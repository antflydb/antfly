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
const coverage_identity = @import("coverage_identity.zig");
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
pub const asset_state_kind: u8 = 0x33;
pub const graph_asset_state_kind: u8 = 0x34;
pub const document_unit_record_kind: u8 = 0x35;
pub const derived_coverage_kind: u8 = 0x36;
pub const document_unit_navigation_summary_kind: u8 = 0x37;
pub const document_unit_navigation_block_kind: u8 = 0x38;
pub const derived_coverage_outcome_marker_kind: u8 = 0x00;
pub const derived_coverage_outcome_count_kind: u8 = 0xff;

pub const replay_key_len: usize = 1 + 1 + @sizeOf(u64);
pub const replay_meta_init_key = [_]u8{ replay_namespace, 0xff, 0x01 };
pub const replay_meta_next_sequence_key = [_]u8{ replay_namespace, 0xff, 0x02 };
pub const replay_meta_latest_sequence_kind: u8 = 0x03;
pub const ha_applied_lsn_key = [_]u8{ replay_namespace, 0xff, 0x04 };
pub const artifact_presence_key = [_]u8{ replay_namespace, 0xff, 0x20 };
pub const asset_artifact_source_index_kind: u8 = 0x21;
pub const document_child_range_outbox_kind: u8 = 0x22;
pub const artifact_repair_issue_kind: u8 = 0x23;
pub const artifact_repair_summary_kind: u8 = 0x24;
pub const artifact_repair_kind_issue_kind: u8 = 0x25;
pub const artifact_repair_kind_index_ready_kind: u8 = 0x26;
pub const artifact_repair_kind_index_progress_kind: u8 = 0x27;
pub const artifact_repair_summary_ready_kind: u8 = 0x28;
pub const artifact_repair_summary_progress_kind: u8 = 0x29;
pub const artifact_repair_summary_rebuild_kind: u8 = 0x2a;
pub const managed_index_admission_kind: u8 = 0x2b;
pub const index_artifact_cleanup_kind: u8 = 0x2c;
pub const range_document_count_key = [_]u8{ replay_namespace, 0xff, 0x2d };
pub const artifact_repair_completion_kind: u8 = 0x2e;
/// Private completion marker used to hand resolution artifacts from the
/// resolver to the DB writer. Keep replay metadata kinds centralized here so
/// independently-owned protocols cannot silently alias one another.
pub const resolution_handoff_kind: u8 = 0x2f;
/// Failure-only indexes that preserve every source sequence associated with a
/// coalesced repair issue. The sequence index serves synchronous visibility
/// checks; the issue index supports bounded per-artifact retirement pages.
pub const enrichment_terminal_failure_sequence_kind: u8 = 0x3a;
pub const enrichment_terminal_failure_issue_kind: u8 = 0x3b;
/// Durable incarnation token for one live coalesced terminal enrichment issue.
pub const enrichment_terminal_failure_generation_kind: u8 = 0x3c;
pub const enrichment_terminal_failure_generation_counter_kind: u8 = 0x3d;
/// Latest committed derived-log revision that changed an artifact stream.
/// The artifact name is length-prefixed so arbitrary user names cannot alias
/// another replay metadata protocol or one another.
pub const artifact_source_revision_kind: u8 = 0x3e;
pub const enrichment_terminal_failure_generation_counter_key = [_]u8{
    replay_namespace,
    0xff,
    enrichment_terminal_failure_generation_counter_kind,
};
pub const embedding_artifact_repair_issue_kind: u8 = artifact_repair_issue_kind;
pub const identity_doc_to_ordinal_kind: u8 = 0x01;
pub const identity_ordinal_to_doc_kind: u8 = 0x02;
pub const identity_ordinal_state_kind: u8 = 0x03;
pub const identity_canonical_to_ordinal_kind: u8 = 0x04;
pub const identity_visibility_chunk_kind: u8 = 0x05;
pub const identity_visibility_deleted_chunk_kind: u8 = 0x06;
pub const identity_namespace_key = [_]u8{ identity_namespace, 0xff, 0x00 };
pub const identity_next_ordinal_key = [_]u8{ identity_namespace, 0xff, 0x01 };
pub const identity_visibility_summary_key = [_]u8{ identity_namespace, 0xff, 0x02 };
pub const identity_visibility_manifest_key = [_]u8{ identity_namespace, 0xff, 0x03 };

pub fn isInternalMetadataKey(key: []const u8) bool {
    if (key.len == 0) return false;
    return key[0] == replay_namespace or key[0] == identity_namespace;
}

pub fn isInternalUserKey(key: []const u8) bool {
    return key.len > 0 and key[0] == user_namespace;
}

pub fn identityVisibilityChunkKey(chunk_id: u32) [6]u8 {
    var key: [6]u8 = undefined;
    key[0] = identity_namespace;
    key[1] = identity_visibility_chunk_kind;
    std.mem.writeInt(u32, key[2..6], chunk_id, .big);
    return key;
}

pub fn parseIdentityVisibilityChunkKey(key: []const u8) ?u32 {
    if (key.len != 6) return null;
    if (key[0] != identity_namespace or key[1] != identity_visibility_chunk_kind) return null;
    return std.mem.readInt(u32, key[2..6], .big);
}

pub fn identityVisibilityDeletedChunkKey(chunk_id: u32) [6]u8 {
    var key: [6]u8 = undefined;
    key[0] = identity_namespace;
    key[1] = identity_visibility_deleted_chunk_kind;
    std.mem.writeInt(u32, key[2..6], chunk_id, .big);
    return key;
}

pub fn parseIdentityVisibilityDeletedChunkKey(key: []const u8) ?u32 {
    if (key.len != 6) return null;
    if (key[0] != identity_namespace or key[1] != identity_visibility_deleted_chunk_kind) return null;
    return std.mem.readInt(u32, key[2..6], .big);
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

pub fn assetStateRootPrefixAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, asset_state_kind);
    return try list.toOwnedSlice(alloc);
}

pub fn graphAssetStateRootPrefixAlloc(alloc: Allocator, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, graph_asset_state_kind);
    return try list.toOwnedSlice(alloc);
}

pub fn graphAssetStateIndexPrefixAlloc(alloc: Allocator, doc_key: []const u8, index_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, graph_asset_state_kind);
    try appendEncodedComponent(&list, alloc, index_name);
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

pub fn derivedCoverageGeneration(config_json: []const u8) u64 {
    return coverage_identity.fromHashBits(std.hash.Wyhash.hash(0x6472_636f_7665_7231, config_json));
}

/// Returns a versioned fingerprint of the fields that define generated output.
/// Object order, credentials, rate limits, and top-level execution tuning are
/// intentionally ignored.
pub fn derivedCoverageConfigFingerprint(alloc: Allocator, config_json: []const u8) !u64 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, config_json, .{});
    defer parsed.deinit();

    var hasher = std.hash.Wyhash.init(0x6472_636f_7665_7232);
    hasher.update("antfly-derived-coverage-config-v2\x00");
    try hashCanonicalJsonValue(alloc, &hasher, parsed.value, .root);
    return hasher.final();
}

const CoverageFingerprintContext = enum {
    root,
    embedder,
    other,
};

fn hashCanonicalJsonValue(
    alloc: Allocator,
    hasher: *std.hash.Wyhash,
    value: std.json.Value,
    context: CoverageFingerprintContext,
) !void {
    switch (value) {
        .null => hasher.update("n"),
        .bool => |flag| hasher.update(if (flag) "b1" else "b0"),
        .integer => |number| {
            hasher.update("i");
            var buf: [32]u8 = undefined;
            hashLengthPrefixed(hasher, std.fmt.bufPrint(&buf, "{d}", .{number}) catch unreachable);
        },
        .float => |number| {
            hasher.update("f");
            var buf: [64]u8 = undefined;
            hashLengthPrefixed(hasher, std.fmt.bufPrint(&buf, "{d}", .{number}) catch unreachable);
        },
        .number_string => |number| {
            hasher.update("r");
            hashLengthPrefixed(hasher, number);
        },
        .string => |string| {
            hasher.update("s");
            hashLengthPrefixed(hasher, string);
        },
        .array => |array| {
            hasher.update("a");
            hashUsize(hasher, array.items.len);
            for (array.items) |item| try hashCanonicalJsonValue(alloc, hasher, item, .other);
        },
        .object => |object| {
            var keys = try alloc.alloc([]const u8, object.count());
            defer alloc.free(keys);
            var key_count: usize = 0;
            var iterator = object.iterator();
            while (iterator.next()) |entry| {
                if (!derivedCoverageConfigKeyIsSemantic(context, entry.key_ptr.*)) continue;
                keys[key_count] = entry.key_ptr.*;
                key_count += 1;
            }
            std.mem.sort([]const u8, keys[0..key_count], {}, struct {
                fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
                    return std.mem.order(u8, lhs, rhs) == .lt;
                }
            }.lessThan);

            hasher.update("o");
            hashUsize(hasher, key_count);
            for (keys[0..key_count]) |key| {
                hashLengthPrefixed(hasher, key);
                const child_context: CoverageFingerprintContext = if (context == .root and std.mem.eql(u8, key, "embedder")) .embedder else .other;
                try hashCanonicalJsonValue(alloc, hasher, object.get(key).?, child_context);
            }
        },
    }
}

fn derivedCoverageConfigKeyIsSemantic(context: CoverageFingerprintContext, key: []const u8) bool {
    if (context == .root and std.mem.eql(u8, key, "execution")) return false;
    if (context != .embedder) return true;
    return !std.mem.eql(u8, key, "api_key") and !std.mem.eql(u8, key, "requests_per_minute") and !std.mem.eql(u8, key, "burst");
}

fn hashLengthPrefixed(hasher: *std.hash.Wyhash, bytes: []const u8) void {
    hashUsize(hasher, bytes.len);
    hasher.update(bytes);
}

fn hashUsize(hasher: *std.hash.Wyhash, value: usize) void {
    var buf: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &buf, @intCast(value), .little);
    hasher.update(&buf);
}

pub fn derivedCoverageGenerationForConfig(coverage_generation: u64, config_json: []const u8) u64 {
    if (coverage_generation != 0) return coverage_generation;
    return derivedCoverageGeneration(config_json);
}

test "derived coverage config fingerprint is semantic and execution independent" {
    const alloc = std.testing.allocator;
    const first = try derivedCoverageConfigFingerprint(
        alloc,
        "{\"field\":\"body\",\"dims\":384,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\"},\"embedder\":{\"model\":\"clipclap\",\"api_key\":\"first\",\"requests_per_minute\":10},\"execution\":{\"embedding\":{\"batch_items\":16}}}",
    );
    const reordered = try derivedCoverageConfigFingerprint(
        alloc,
        "{\"generator\":{\"source_field\":\"body\",\"kind\":\"dense_embedding\"},\"execution\":{\"embedding\":{\"batch_items\":1024}},\"embedder\":{\"requests_per_minute\":1000,\"api_key\":\"rotated\",\"model\":\"clipclap\"},\"dims\":384,\"field\":\"body\"}",
    );
    const changed = try derivedCoverageConfigFingerprint(
        alloc,
        "{\"generator\":{\"source_field\":\"content\",\"kind\":\"dense_embedding\"},\"dims\":384,\"field\":\"body\"}",
    );
    const semantic_burst = try derivedCoverageConfigFingerprint(
        alloc,
        "{\"generator\":{\"source_field\":\"body\",\"kind\":\"dense_embedding\",\"burst\":2},\"embedder\":{\"model\":\"clipclap\"},\"dims\":384,\"field\":\"body\"}",
    );

    try std.testing.expectEqual(first, reordered);
    try std.testing.expect(first != changed);
    try std.testing.expect(first != semantic_burst);
}

pub fn derivedCoverageOutcomePrefixAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, derived_coverage_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    return try list.toOwnedSlice(alloc);
}

pub fn derivedCoverageOutcomeGenerationPrefixAlloc(alloc: Allocator, index_name: []const u8, generation: u64) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, derived_coverage_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    var generation_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &generation_buf, generation, .little);
    try list.appendSlice(alloc, &generation_buf);
    return try list.toOwnedSlice(alloc);
}

pub fn derivedCoverageOutcomeMarkerPrefixAlloc(alloc: Allocator, index_name: []const u8, generation: u64) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, derived_coverage_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    var generation_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &generation_buf, generation, .little);
    try list.appendSlice(alloc, &generation_buf);
    try list.append(alloc, derived_coverage_outcome_marker_kind);
    return try list.toOwnedSlice(alloc);
}

pub fn derivedCoverageOutcomeKeyAlloc(alloc: Allocator, index_name: []const u8, generation: u64, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, derived_coverage_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    var generation_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &generation_buf, generation, .little);
    try list.appendSlice(alloc, &generation_buf);
    try list.append(alloc, derived_coverage_outcome_marker_kind);
    try appendEncodedComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn derivedCoverageOutcomeDocKeyAlloc(
    alloc: Allocator,
    index_name: []const u8,
    generation: u64,
    key: []const u8,
) ![]u8 {
    const prefix = try derivedCoverageOutcomeMarkerPrefixAlloc(alloc, index_name, generation);
    defer alloc.free(prefix);
    if (!std.mem.startsWith(u8, key, prefix)) return error.InvalidDerivedCoverageOutcomeKey;
    const component_start = prefix.len;
    const component_end = findComponentTerminator(key, component_start) orelse
        return error.InvalidDerivedCoverageOutcomeKey;
    if (component_end + 2 != key.len) return error.InvalidDerivedCoverageOutcomeKey;
    return decodeBodyAlloc(alloc, key[component_start..component_end]) catch
        return error.InvalidDerivedCoverageOutcomeKey;
}

pub fn derivedCoverageOutcomeCountKeyAlloc(alloc: Allocator, index_name: []const u8, generation: u64, outcome: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, derived_coverage_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    var generation_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &generation_buf, generation, .little);
    try list.appendSlice(alloc, &generation_buf);
    try list.append(alloc, derived_coverage_outcome_count_kind);
    try appendEncodedComponent(&list, alloc, outcome);
    return try list.toOwnedSlice(alloc);
}

pub fn encodeDerivedCoverageOutcomeCount(out: *[8]u8, count: u64) []const u8 {
    std.mem.writeInt(u64, out, count, .little);
    return out[0..];
}

pub fn decodeDerivedCoverageOutcomeCount(raw: []const u8) !u64 {
    if (raw.len != 8) return error.InvalidDerivedCoverageOutcomeCount;
    return std.mem.readInt(u64, raw[0..8], .little);
}

pub fn assetArtifactSourceIndexRootPrefixAlloc(alloc: Allocator) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, asset_artifact_source_index_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn assetArtifactSourceIndexPrefixAlloc(alloc: Allocator, source_artifact: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, asset_artifact_source_index_kind });
    try appendEncodedComponent(&list, alloc, source_artifact);
    return try list.toOwnedSlice(alloc);
}

pub fn assetArtifactSourceIndexKeyAlloc(alloc: Allocator, source_artifact: []const u8, doc_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, asset_artifact_source_index_kind });
    try appendEncodedComponent(&list, alloc, source_artifact);
    try appendEncodedComponent(&list, alloc, doc_key);
    return try list.toOwnedSlice(alloc);
}

pub fn documentChildRangeOutboxRootPrefixAlloc(alloc: Allocator) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, document_child_range_outbox_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn documentChildRangeOutboxKeyAlloc(alloc: Allocator, sequence: u64, ordinal: u32) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, document_child_range_outbox_kind });
    const sequence_be = std.mem.nativeToBig(u64, sequence);
    try list.appendSlice(alloc, std.mem.asBytes(&sequence_be));
    const ordinal_be = std.mem.nativeToBig(u32, ordinal);
    try list.appendSlice(alloc, std.mem.asBytes(&ordinal_be));
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairIssueRootPrefixAlloc(alloc: Allocator) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_issue_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn enrichmentTerminalFailureSequenceRootPrefixAlloc(alloc: Allocator) ![]u8 {
    return try alloc.dupe(u8, &[_]u8{ replay_namespace, 0xff, enrichment_terminal_failure_sequence_kind });
}

pub fn enrichmentTerminalFailureSequencePrefixAlloc(alloc: Allocator, sequence: u64) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, enrichment_terminal_failure_sequence_kind });
    const sequence_be = std.mem.nativeToBig(u64, sequence);
    try list.appendSlice(alloc, std.mem.asBytes(&sequence_be));
    return try list.toOwnedSlice(alloc);
}

pub fn enrichmentTerminalFailureSequenceKeyAlloc(alloc: Allocator, sequence: u64, issue_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    const prefix = try enrichmentTerminalFailureSequencePrefixAlloc(alloc, sequence);
    defer alloc.free(prefix);
    try list.appendSlice(alloc, prefix);
    try appendEncodedComponent(&list, alloc, issue_key);
    return try list.toOwnedSlice(alloc);
}

pub fn enrichmentTerminalFailureSequence(key: []const u8) !u64 {
    const prefix = [_]u8{ replay_namespace, 0xff, enrichment_terminal_failure_sequence_kind };
    if (!std.mem.startsWith(u8, key, &prefix) or key.len < prefix.len + @sizeOf(u64) + 2)
        return error.InvalidInternalUserKey;
    return std.mem.bigToNative(u64, std.mem.bytesToValue(u64, key[prefix.len..][0..@sizeOf(u64)]));
}

pub fn enrichmentTerminalFailureIssuePrefixAlloc(alloc: Allocator, issue_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, enrichment_terminal_failure_issue_kind });
    try appendEncodedComponent(&list, alloc, issue_key);
    return try list.toOwnedSlice(alloc);
}

pub fn enrichmentTerminalFailureIssueKeyAlloc(alloc: Allocator, issue_key: []const u8, sequence: u64) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    const prefix = try enrichmentTerminalFailureIssuePrefixAlloc(alloc, issue_key);
    defer alloc.free(prefix);
    try list.appendSlice(alloc, prefix);
    const sequence_be = std.mem.nativeToBig(u64, sequence);
    try list.appendSlice(alloc, std.mem.asBytes(&sequence_be));
    return try list.toOwnedSlice(alloc);
}

pub fn enrichmentTerminalFailureGenerationKeyAlloc(alloc: Allocator, issue_key: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, enrichment_terminal_failure_generation_kind });
    try appendEncodedComponent(&list, alloc, issue_key);
    return try list.toOwnedSlice(alloc);
}

pub fn managedIndexAdmissionKeyAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, managed_index_admission_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    return try list.toOwnedSlice(alloc);
}

pub fn managedIndexAdmissionRootPrefixAlloc(alloc: Allocator) ![]u8 {
    return try alloc.dupe(u8, &[_]u8{ replay_namespace, 0xff, managed_index_admission_kind });
}

pub fn managedIndexAdmissionNameAlloc(alloc: Allocator, key: []const u8) ![]u8 {
    const prefix = [_]u8{ replay_namespace, 0xff, managed_index_admission_kind };
    if (!std.mem.startsWith(u8, key, &prefix)) return error.InvalidInternalUserKey;
    const name_start = prefix.len;
    const name_end = findComponentTerminator(key, name_start) orelse return error.InvalidInternalUserKey;
    if (name_end + 2 != key.len) return error.InvalidInternalUserKey;
    return try decodeBodyAlloc(alloc, key[name_start..name_end]);
}

pub fn indexArtifactCleanupKeyAlloc(alloc: Allocator, index_name: []const u8, coverage_generation: u64) ![]u8 {
    if (coverage_generation == 0) return error.InvalidInternalUserKey;
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, index_artifact_cleanup_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    const generation_be = std.mem.nativeToBig(u64, coverage_generation);
    try list.appendSlice(alloc, std.mem.asBytes(&generation_be));
    return try list.toOwnedSlice(alloc);
}

pub fn indexArtifactCleanupRootPrefixAlloc(alloc: Allocator) ![]u8 {
    return try alloc.dupe(u8, &[_]u8{ replay_namespace, 0xff, index_artifact_cleanup_kind });
}

pub fn indexArtifactCleanupIndexPrefixAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, index_artifact_cleanup_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    return try list.toOwnedSlice(alloc);
}

pub fn indexArtifactCleanupNameAlloc(alloc: Allocator, key: []const u8) ![]u8 {
    const prefix = [_]u8{ replay_namespace, 0xff, index_artifact_cleanup_kind };
    if (!std.mem.startsWith(u8, key, &prefix)) return error.InvalidInternalUserKey;
    const name_start = prefix.len;
    const name_end = findComponentTerminator(key, name_start) orelse return error.InvalidInternalUserKey;
    if (name_end + 2 + @sizeOf(u64) != key.len) return error.InvalidInternalUserKey;
    return try decodeBodyAlloc(alloc, key[name_start..name_end]);
}

pub fn indexArtifactCleanupCoverageGeneration(key: []const u8) !u64 {
    const prefix = [_]u8{ replay_namespace, 0xff, index_artifact_cleanup_kind };
    if (!std.mem.startsWith(u8, key, &prefix)) return error.InvalidInternalUserKey;
    const name_end = findComponentTerminator(key, prefix.len) orelse return error.InvalidInternalUserKey;
    const generation_start = name_end + 2;
    if (generation_start + @sizeOf(u64) != key.len) return error.InvalidInternalUserKey;
    const generation = std.mem.bigToNative(u64, std.mem.bytesToValue(u64, key[generation_start..][0..@sizeOf(u64)]));
    if (generation == 0) return error.InvalidInternalUserKey;
    return generation;
}

pub fn artifactRepairIssueIndexPrefixAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_issue_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairIssueKeyAlloc(
    alloc: Allocator,
    index_name: []const u8,
    repair_artifact_kind: []const u8,
    issue_id: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_issue_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    try appendEncodedComponent(&list, alloc, repair_artifact_kind);
    try appendEncodedComponent(&list, alloc, issue_id);
    return try list.toOwnedSlice(alloc);
}

pub const ArtifactRepairIssueKeyParts = struct {
    index_name: []u8,
    repair_artifact_kind: []u8,
    issue_id: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.index_name);
        alloc.free(self.repair_artifact_kind);
        alloc.free(self.issue_id);
        self.* = undefined;
    }
};

/// Decode the authoritative identity embedded in a primary repair-issue key.
/// Cleanup uses this when the value is malformed, so callers never have to
/// trust corrupt payload fields to locate secondary or completion metadata.
pub fn artifactRepairIssueKeyPartsAlloc(
    alloc: Allocator,
    key: []const u8,
) !ArtifactRepairIssueKeyParts {
    const prefix = [_]u8{ replay_namespace, 0xff, artifact_repair_issue_kind };
    if (!std.mem.startsWith(u8, key, &prefix)) return error.InvalidInternalUserKey;

    var pos = prefix.len;
    const index_end = findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
    const index_name = try decodeBodyAlloc(alloc, key[pos..index_end]);
    errdefer alloc.free(index_name);

    pos = index_end + 2;
    const kind_end = findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
    const repair_artifact_kind = try decodeBodyAlloc(alloc, key[pos..kind_end]);
    errdefer alloc.free(repair_artifact_kind);

    pos = kind_end + 2;
    const issue_end = findComponentTerminator(key, pos) orelse return error.InvalidInternalUserKey;
    if (issue_end + 2 != key.len) return error.InvalidInternalUserKey;
    const issue_id = try decodeBodyAlloc(alloc, key[pos..issue_end]);
    errdefer alloc.free(issue_id);

    return .{
        .index_name = index_name,
        .repair_artifact_kind = repair_artifact_kind,
        .issue_id = issue_id,
    };
}

/// Durable single-flight fence for producer repair work. Repair issues remain
/// indexed per consumer, while this key is canonical per physical artifact so
/// a successful regeneration is never repeated for sibling consumer records,
/// later cursor pages, or after restart.
pub fn artifactRepairCompletionKeyAlloc(
    alloc: Allocator,
    repair_artifact_kind: []const u8,
    issue_id: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_completion_kind });
    try appendEncodedComponent(&list, alloc, repair_artifact_kind);
    try appendEncodedComponent(&list, alloc, issue_id);
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairIssueKindRootPrefixAlloc(alloc: Allocator, repair_artifact_kind: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_kind_issue_kind });
    try appendEncodedComponent(&list, alloc, repair_artifact_kind);
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairIssueKindIndexPrefixAlloc(
    alloc: Allocator,
    repair_artifact_kind: []const u8,
    index_name: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_kind_issue_kind });
    try appendEncodedComponent(&list, alloc, repair_artifact_kind);
    try appendEncodedComponent(&list, alloc, index_name);
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairIssueKindKeyAlloc(
    alloc: Allocator,
    repair_artifact_kind: []const u8,
    index_name: []const u8,
    issue_id: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_kind_issue_kind });
    try appendEncodedComponent(&list, alloc, repair_artifact_kind);
    try appendEncodedComponent(&list, alloc, index_name);
    try appendEncodedComponent(&list, alloc, issue_id);
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairKindIndexReadyKeyAlloc(alloc: Allocator) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_kind_index_ready_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairKindIndexProgressKeyAlloc(alloc: Allocator) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_kind_index_progress_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairSummaryReadyKeyAlloc(alloc: Allocator) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_summary_ready_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairSummaryProgressKeyAlloc(alloc: Allocator) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_summary_progress_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairSummaryRootKeyAlloc(alloc: Allocator) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_summary_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairSummaryIndexKeyAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_summary_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairSummaryRebuildRootKeyAlloc(alloc: Allocator) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_summary_rebuild_kind });
    return try list.toOwnedSlice(alloc);
}

pub fn artifactRepairSummaryRebuildIndexKeyAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try list.appendSlice(alloc, &[_]u8{ replay_namespace, 0xff, artifact_repair_summary_rebuild_kind });
    try appendEncodedComponent(&list, alloc, index_name);
    return try list.toOwnedSlice(alloc);
}

pub fn embeddingArtifactRepairIssueRootPrefixAlloc(alloc: Allocator) ![]u8 {
    return try artifactRepairIssueRootPrefixAlloc(alloc);
}

pub fn embeddingArtifactRepairIssueIndexPrefixAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    return try artifactRepairIssueIndexPrefixAlloc(alloc, index_name);
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

pub fn documentUnitChunkArtifactKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8, unit_id: []const u8, chunk_id: u32) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, "chunk");
    try appendEncodedComponent(&list, alloc, artifact_name);
    try list.append(alloc, document_unit_record_kind);
    try appendEncodedComponent(&list, alloc, unit_id);

    const be = std.mem.nativeToBig(u32, chunk_id);
    try list.append(alloc, chunk_record_kind);
    try list.appendSlice(alloc, std.mem.asBytes(&be));

    return try list.toOwnedSlice(alloc);
}

pub fn documentUnitArtifactKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8, unit_id: []const u8) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);

    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, artifact_kind);
    try appendEncodedComponent(&list, alloc, "asset");
    try appendEncodedComponent(&list, alloc, artifact_name);
    try list.append(alloc, document_unit_record_kind);
    try appendEncodedComponent(&list, alloc, unit_id);

    return try list.toOwnedSlice(alloc);
}

pub fn embeddingArtifactKeyForDocumentAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8) ![]u8 {
    return artifactNamedPrefixAlloc(alloc, doc_key, "embedding", artifact_name);
}

/// Resolution artifacts record the entity-resolution decisions for a source
/// document. They are stored like asset artifacts but under the "resolution"
/// artifact type so they stay distinct from extractor-produced asset artifacts:
/// [0x01][doc][0x00 0x00][0x20]["resolution"][0x00 0x00][name][0x00 0x00]
pub fn resolutionArtifactKeyAlloc(alloc: Allocator, doc_key: []const u8, artifact_name: []const u8) ![]u8 {
    return artifactNamedPrefixAlloc(alloc, doc_key, "resolution", artifact_name);
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

        pos = skipDerivedEmbeddingBaseRecordSuffix(key, pos) orelse return error.InvalidInternalUserKey;
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

    if (pos < key.len and key[pos] == document_unit_record_kind) {
        pos += 1;
        const unit_term = findComponentTerminator(key, pos) orelse return false;
        pos = unit_term + 2;
    }

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

    if (pos < key.len and key[pos] == document_unit_record_kind) {
        pos += 1;
        const unit_term = findComponentTerminator(key, pos) orelse return false;
        pos = unit_term + 2;
    }

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
            pos = skipDerivedEmbeddingBaseRecordSuffix(key, pos) orelse return false;
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
            pos = skipDerivedEmbeddingBaseRecordSuffix(key, pos) orelse return false;
        },
        else => return false,
    }

    if (pos >= key.len or key[pos] != derived_embedding_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, artifact_name)) return false;
    return findComponentTerminator(key, pos).? + 2 == key.len;
}

fn skipDerivedEmbeddingBaseRecordSuffix(key: []const u8, pos: usize) ?usize {
    var cursor = pos;
    if (cursor < key.len and key[cursor] == document_unit_record_kind) {
        cursor += 1;
        const unit_term = findComponentTerminator(key, cursor) orelse return null;
        cursor = unit_term + 2;
    }
    if (cursor < key.len and key[cursor] == chunk_record_kind) {
        cursor += 1 + @sizeOf(u32);
    }
    if (cursor > key.len) return null;
    return cursor;
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

pub fn matchesGraphEdgeIndexName(key: []const u8, index_name: []const u8) bool {
    if (!isGraphEdgeArtifactKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2 + 1;
    const type_term = findComponentTerminator(key, pos) orelse return false;
    pos = type_term + 2;
    return componentEquals(key, pos, index_name);
}

pub fn isGraphAssetStateKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != graph_asset_state_kind) return false;
    pos += 1;
    const index_term = findComponentTerminator(key, pos) orelse return false;
    pos = index_term + 2;
    const state_term = findComponentTerminator(key, pos) orelse return false;
    return state_term + 2 == key.len;
}

pub fn matchesGraphAssetStateIndexName(key: []const u8, index_name: []const u8) bool {
    if (!isGraphAssetStateKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    const index_start = doc_term + 2 + 1;
    return componentEquals(key, index_start, index_name);
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

pub fn isAssetArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;
    if (!componentEquals(key, pos, "asset")) return false;
    const type_term = findComponentTerminator(key, pos) orelse return false;
    pos = type_term + 2;
    const name_term = findComponentTerminator(key, pos) orelse return false;
    return name_term + 2 == key.len;
}

pub fn matchesAssetArtifactName(key: []const u8, artifact_name: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;

    if (!componentEquals(key, pos, "asset")) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    if (!componentEquals(key, pos, artifact_name)) return false;
    pos = findComponentTerminator(key, pos).? + 2;

    if (pos == key.len) return true;
    if (pos < key.len and key[pos] == document_unit_record_kind) {
        pos += 1;
        const unit_term = findComponentTerminator(key, pos) orelse return false;
        return unit_term + 2 == key.len;
    }
    return false;
}

pub fn isDocumentUnitArtifactRecordKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;
    if (!componentEquals(key, pos, "asset")) return false;
    pos = findComponentTerminator(key, pos).? + 2;
    const name_term = findComponentTerminator(key, pos) orelse return false;
    pos = name_term + 2;
    if (pos >= key.len or key[pos] != document_unit_record_kind) return false;
    pos += 1;
    const unit_term = findComponentTerminator(key, pos) orelse return false;
    return unit_term + 2 == key.len;
}

/// Parent-owned compact hierarchy summary. Keeping navigation metadata outside
/// the unit payload namespace lets sequential browsing seek without loading
/// every page body or reparsing the complete extraction state.
pub fn documentUnitNavigationSummaryKeyAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, document_unit_navigation_summary_kind);
    try appendEncodedComponent(&list, alloc, artifact_name);
    return try list.toOwnedSlice(alloc);
}

/// A fixed-size parent-owned block of unit keys and fingerprints. The block
/// number is big-endian so the keys retain traversal order for diagnostics and
/// repair tooling even though the query path performs direct point reads.
pub fn documentUnitNavigationBlockKeyAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    block_index: u32,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).fromOwnedSlice(
        try documentUnitNavigationBlockPrefixAlloc(alloc, doc_key, artifact_name),
    );
    defer list.deinit(alloc);
    var encoded_index: [4]u8 = undefined;
    std.mem.writeInt(u32, &encoded_index, block_index, .big);
    try list.appendSlice(alloc, &encoded_index);
    return try list.toOwnedSlice(alloc);
}

/// Prefix shared by every compact navigation block for one extraction
/// artifact. Repair and deletion paths use it to reclaim exact persisted keys
/// when the state record is missing or corrupt.
pub fn documentUnitNavigationBlockPrefixAlloc(
    alloc: Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
) ![]u8 {
    var list = std.ArrayListUnmanaged(u8).empty;
    defer list.deinit(alloc);
    try appendDocumentPrefix(&list, alloc, doc_key);
    try list.append(alloc, document_unit_navigation_block_kind);
    try appendEncodedComponent(&list, alloc, artifact_name);
    return try list.toOwnedSlice(alloc);
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

/// Returns true if key is a resolution artifact: [0x01][doc][0x00 0x00][0x20]["resolution"][0x00 0x00][name][0x00 0x00]
pub fn isResolutionArtifactKey(key: []const u8) bool {
    if (!isInternalUserKey(key)) return false;
    const doc_term = findComponentTerminator(key, 1) orelse return false;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return false;
    pos += 1;
    if (!componentEquals(key, pos, "resolution")) return false;
    const type_term = findComponentTerminator(key, pos) orelse return false;
    pos = type_term + 2;
    const name_term = findComponentTerminator(key, pos) orelse return false;
    return name_term + 2 == key.len;
}

/// Parse a resolution artifact key, returning (doc_key, artifact_name).
/// Returns null if the key is not a resolution artifact key.
pub fn parseResolutionArtifactKeyAlloc(alloc: Allocator, key: []const u8) !?struct { doc_key: []u8, artifact_name: []u8 } {
    if (!isResolutionArtifactKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1).?;
    const doc_key = try decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);

    var pos = doc_term + 2 + 1; // past artifact_kind byte
    const type_term = findComponentTerminator(key, pos).?;
    pos = type_term + 2;

    const name_term = findComponentTerminator(key, pos).?;
    const artifact_name = try decodeBodyAlloc(alloc, key[pos..name_term]);

    return .{ .doc_key = doc_key, .artifact_name = artifact_name };
}

/// Parse an asset artifact key, returning (doc_key, artifact_name).
/// Returns null if the key is not an asset artifact key.
pub fn parseAssetArtifactKeyAlloc(alloc: Allocator, key: []const u8) !?struct { doc_key: []u8, artifact_name: []u8 } {
    if (!isAssetArtifactKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1).?;
    const doc_key = try decodeBodyAlloc(alloc, key[1..doc_term]);
    errdefer alloc.free(doc_key);

    var pos = doc_term + 2 + 1; // past artifact_kind byte
    const type_term = findComponentTerminator(key, pos).?;
    pos = type_term + 2;

    const name_term = findComponentTerminator(key, pos).?;
    const artifact_name = try decodeBodyAlloc(alloc, key[pos..name_term]);

    return .{ .doc_key = doc_key, .artifact_name = artifact_name };
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

/// Returns a borrowed artifact stream name for the common unescaped key path.
/// Derived embeddings return their terminal embedding name rather than the
/// chunk/asset source name. Callers that accept arbitrary binary components
/// must fall back to the allocating decoder when this returns null.
pub fn artifactNameView(key: []const u8) !?[]const u8 {
    if (!isInternalUserKey(key)) return null;
    const doc_term = findComponentTerminator(key, 1) orelse return null;
    var pos = doc_term + 2;
    if (pos >= key.len or key[pos] != artifact_kind) return null;
    pos += 1;

    const type_term = findComponentTerminator(key, pos) orelse return null;
    const artifact_type = (try decodeBodyView(key[pos..type_term])) orelse return null;
    pos = type_term + 2;
    const name_term = findComponentTerminator(key, pos) orelse return null;
    const base_name = (try decodeBodyView(key[pos..name_term])) orelse return null;
    pos = name_term + 2;
    if (pos == key.len) return base_name;

    if (std.mem.eql(u8, artifact_type, "graph") or std.mem.eql(u8, artifact_type, "resolution")) return base_name;
    if (pos < key.len and key[pos] == document_unit_record_kind) {
        pos += 1;
        const unit_term = findComponentTerminator(key, pos) orelse return null;
        pos = unit_term + 2;
    }
    if (pos < key.len and key[pos] == chunk_record_kind) {
        if (pos + 1 + @sizeOf(u32) > key.len) return null;
        pos += 1 + @sizeOf(u32);
    }
    if (pos == key.len) return base_name;
    if (key[pos] != derived_embedding_kind) return null;
    pos += 1;
    const derived_term = findComponentTerminator(key, pos) orelse return null;
    if (derived_term + 2 != key.len) return null;
    return (try decodeBodyView(key[pos..derived_term])) orelse null;
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

pub fn artifactSourceRevisionKeyAlloc(alloc: Allocator, artifact_name: []const u8) ![]u8 {
    var key = try alloc.alloc(u8, 3 + encodedComponentLen(artifact_name));
    key[0] = replay_namespace;
    key[1] = 0xff;
    key[2] = artifact_source_revision_kind;
    _ = encodeComponent(key[3..], artifact_name);
    return key;
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

test "document unit chunk artifact key is recognized as chunk record" {
    const alloc = std.testing.allocator;
    const key = try documentUnitChunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks_v1", "page:000001", 3);
    defer alloc.free(key);

    try std.testing.expect(isChunkArtifactRecordKey(key));
    try std.testing.expect(matchesChunkArtifactName(key, "document_chunks_v1"));
    try std.testing.expect(!matchesChunkArtifactName(key, "other_chunks_v1"));
}

test "document unit chunk derived embedding key is recognized" {
    const alloc = std.testing.allocator;
    const chunk_key = try documentUnitChunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks_v1", "page:000001", 3);
    defer alloc.free(chunk_key);
    const embedding_key = try derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "dense");
    defer alloc.free(embedding_key);

    try std.testing.expect(isDerivedEmbeddingArtifactKey(embedding_key));
    try std.testing.expect(matchesDerivedEmbeddingArtifactName(embedding_key, "dense"));
    try std.testing.expect(!matchesDerivedEmbeddingArtifactName(embedding_key, "other"));
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

test "artifact name view resolves direct and derived streams without allocation" {
    const alloc = std.testing.allocator;
    const direct = try embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "document_vectors");
    defer alloc.free(direct);
    try std.testing.expectEqualStrings("document_vectors", (try artifactNameView(direct)).?);

    const chunk = try chunkArtifactKeyAlloc(alloc, "doc:a", "document_chunks", 7);
    defer alloc.free(chunk);
    try std.testing.expectEqualStrings("document_chunks", (try artifactNameView(chunk)).?);
    const derived = try derivedEmbeddingArtifactKeyAlloc(alloc, chunk, "chunk_vectors");
    defer alloc.free(derived);
    try std.testing.expectEqualStrings("chunk_vectors", (try artifactNameView(derived)).?);
}

test "graph edge artifact key round trip" {
    const alloc = std.testing.allocator;
    const key = try graphEdgeArtifactKeyAlloc(alloc, "doc:a", "gr_v1", "links", "doc:b");
    defer alloc.free(key);

    try std.testing.expect(isGraphEdgeArtifactKey(key));
    try std.testing.expect(!isEmbeddingArtifactKey(key));
    try std.testing.expect(matchesGraphEdgeIndexName(key, "gr_v1"));
    try std.testing.expect(!matchesGraphEdgeIndexName(key, "gr_v10"));

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

test "graph asset state key matches exact index name" {
    const alloc = std.testing.allocator;
    const key = try graphAssetStateIndexPrefixAlloc(alloc, "doc:a", "gr_v1");
    defer alloc.free(key);
    var full = std.ArrayListUnmanaged(u8).empty;
    defer full.deinit(alloc);
    try full.appendSlice(alloc, key);
    try appendEncodedComponent(&full, alloc, "relations_v1");

    try std.testing.expect(isGraphAssetStateKey(full.items));
    try std.testing.expect(matchesGraphAssetStateIndexName(full.items, "gr_v1"));
    try std.testing.expect(!matchesGraphAssetStateIndexName(full.items, "gr_v10"));
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

test "resolution artifact key round-trips and is distinct from asset" {
    const alloc = std.testing.allocator;
    const key = try resolutionArtifactKeyAlloc(alloc, "doc:article-123", "resolution_v1");
    defer alloc.free(key);

    try std.testing.expect(isResolutionArtifactKey(key));
    try std.testing.expect(!isAssetArtifactKey(key));
    try std.testing.expect(!isSummaryArtifactKey(key));

    const asset = try artifactNamedPrefixAlloc(alloc, "doc:article-123", "asset", "relations_v1");
    defer alloc.free(asset);
    try std.testing.expect(!isResolutionArtifactKey(asset));

    const parsed = (try parseResolutionArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.artifact_name);
    try std.testing.expectEqualStrings("doc:article-123", parsed.doc_key);
    try std.testing.expectEqualStrings("resolution_v1", parsed.artifact_name);
}

test "parseAssetArtifactKeyAlloc returns doc key and artifact name" {
    const alloc = std.testing.allocator;
    const key = try artifactNamedPrefixAlloc(alloc, "doc:article-123", "asset", "relations_v1");
    defer alloc.free(key);
    const parsed = (try parseAssetArtifactKeyAlloc(alloc, key)).?;
    defer alloc.free(parsed.doc_key);
    defer alloc.free(parsed.artifact_name);
    try std.testing.expectEqualStrings("doc:article-123", parsed.doc_key);
    try std.testing.expectEqualStrings("relations_v1", parsed.artifact_name);

    const res = try resolutionArtifactKeyAlloc(alloc, "doc:article-123", "resolution_v1");
    defer alloc.free(res);
    try std.testing.expect((try parseAssetArtifactKeyAlloc(alloc, res)) == null);
}

test "matchesAssetArtifactName matches top-level and document unit assets" {
    const alloc = std.testing.allocator;
    const asset = try artifactNamedPrefixAlloc(alloc, "doc:article-123", "asset", "document_units_v1");
    defer alloc.free(asset);
    const unit = try documentUnitArtifactKeyAlloc(alloc, "doc:article-123", "document_units_v1", "page:000001");
    defer alloc.free(unit);
    const chunk = try documentUnitChunkArtifactKeyAlloc(alloc, "doc:article-123", "document_units_v1", "page:000001", 0);
    defer alloc.free(chunk);

    try std.testing.expect(matchesAssetArtifactName(asset, "document_units_v1"));
    try std.testing.expect(matchesAssetArtifactName(unit, "document_units_v1"));
    try std.testing.expect(!matchesAssetArtifactName(unit, "other_units_v1"));
    try std.testing.expect(!matchesAssetArtifactName(chunk, "document_units_v1"));
}

test "document unit navigation keys are parent scoped and block ordered" {
    const alloc = std.testing.allocator;
    const doc_key = "doc\x00:a";
    const artifact_name = "document\x00units:v1";
    const summary = try documentUnitNavigationSummaryKeyAlloc(alloc, doc_key, artifact_name);
    defer alloc.free(summary);
    const block_zero = try documentUnitNavigationBlockKeyAlloc(alloc, doc_key, artifact_name, 0);
    defer alloc.free(block_zero);
    const block_one = try documentUnitNavigationBlockKeyAlloc(alloc, doc_key, artifact_name, 1);
    defer alloc.free(block_one);
    const block_large = try documentUnitNavigationBlockKeyAlloc(alloc, doc_key, artifact_name, 0x0100_0000);
    defer alloc.free(block_large);

    try std.testing.expect(isInternalUserKey(summary));
    try std.testing.expect(isInternalUserKey(block_zero));
    try std.testing.expect(!std.mem.eql(u8, summary, block_zero));
    try std.testing.expect(std.mem.lessThan(u8, block_zero, block_one));
    try std.testing.expect(std.mem.lessThan(u8, block_one, block_large));

    const unit = try documentUnitArtifactKeyAlloc(alloc, doc_key, artifact_name, "page:000001");
    defer alloc.free(unit);
    const chunk = try documentUnitChunkArtifactKeyAlloc(alloc, doc_key, artifact_name, "page:000001", 0);
    defer alloc.free(chunk);
    try std.testing.expect(isDocumentUnitArtifactRecordKey(unit));
    try std.testing.expect(!isDocumentUnitArtifactRecordKey(chunk));
    try std.testing.expect(!isDocumentUnitArtifactRecordKey(summary));
}

test "asset artifact source index keys group by source artifact" {
    const alloc = std.testing.allocator;
    const root = try assetArtifactSourceIndexRootPrefixAlloc(alloc);
    defer alloc.free(root);
    const prefix = try assetArtifactSourceIndexPrefixAlloc(alloc, "relations_v1");
    defer alloc.free(prefix);
    const key = try assetArtifactSourceIndexKeyAlloc(alloc, "relations_v1", "doc:article-123");
    defer alloc.free(key);
    const other = try assetArtifactSourceIndexKeyAlloc(alloc, "other_v1", "doc:article-123");
    defer alloc.free(other);

    try std.testing.expect(std.mem.startsWith(u8, prefix, root));
    try std.testing.expect(std.mem.startsWith(u8, key, prefix));
    try std.testing.expect(!std.mem.startsWith(u8, other, prefix));
}

test "terminal enrichment failure indexes preserve sequence order and issue ownership" {
    try std.testing.expect(resolution_handoff_kind != enrichment_terminal_failure_sequence_kind);
    try std.testing.expect(resolution_handoff_kind != enrichment_terminal_failure_issue_kind);
    try std.testing.expect(enrichment_terminal_failure_sequence_kind != enrichment_terminal_failure_issue_kind);
    try std.testing.expect(enrichment_terminal_failure_sequence_kind != enrichment_terminal_failure_generation_kind);
    try std.testing.expect(enrichment_terminal_failure_issue_kind != enrichment_terminal_failure_generation_kind);
    try std.testing.expect(enrichment_terminal_failure_sequence_kind != enrichment_terminal_failure_generation_counter_kind);
    try std.testing.expect(enrichment_terminal_failure_issue_kind != enrichment_terminal_failure_generation_counter_kind);
    try std.testing.expect(enrichment_terminal_failure_generation_kind != enrichment_terminal_failure_generation_counter_kind);

    const alloc = std.testing.allocator;
    const repair_issue_key = "\x02\xff\x23repair\x00issue";
    const root = try enrichmentTerminalFailureSequenceRootPrefixAlloc(alloc);
    defer alloc.free(root);
    const first = try enrichmentTerminalFailureSequenceKeyAlloc(alloc, 10, repair_issue_key);
    defer alloc.free(first);
    const second = try enrichmentTerminalFailureSequenceKeyAlloc(alloc, 20, repair_issue_key);
    defer alloc.free(second);
    const issue_prefix = try enrichmentTerminalFailureIssuePrefixAlloc(alloc, repair_issue_key);
    defer alloc.free(issue_prefix);
    const reverse = try enrichmentTerminalFailureIssueKeyAlloc(alloc, repair_issue_key, 10);
    defer alloc.free(reverse);
    const generation = try enrichmentTerminalFailureGenerationKeyAlloc(alloc, repair_issue_key);
    defer alloc.free(generation);

    try std.testing.expect(std.mem.startsWith(u8, first, root));
    try std.testing.expect(std.mem.lessThan(u8, first, second));
    try std.testing.expectEqual(@as(u64, 10), try enrichmentTerminalFailureSequence(first));
    try std.testing.expectEqual(@as(u64, 20), try enrichmentTerminalFailureSequence(second));
    try std.testing.expect(std.mem.startsWith(u8, reverse, issue_prefix));
    try std.testing.expect(!std.mem.startsWith(u8, generation, issue_prefix));
    try std.testing.expectError(error.InvalidInternalUserKey, enrichmentTerminalFailureSequence(reverse));

    const handoff_prefix = [_]u8{ replay_namespace, 0xff, resolution_handoff_kind };
    try std.testing.expect(!std.mem.startsWith(u8, first, &handoff_prefix));
    try std.testing.expectError(
        error.InvalidInternalUserKey,
        enrichmentTerminalFailureSequence(&handoff_prefix),
    );
}

test "derived coverage outcome keys are generation scoped" {
    const alloc = std.testing.allocator;
    const old_generation = derivedCoverageGeneration("{\"source\":\"body\",\"model\":\"a\"}");
    const new_generation = derivedCoverageGeneration("{\"source\":\"body\",\"model\":\"b\"}");

    const index_prefix = try derivedCoverageOutcomePrefixAlloc(alloc, "semantic_idx");
    defer alloc.free(index_prefix);
    const old_prefix = try derivedCoverageOutcomeMarkerPrefixAlloc(alloc, "semantic_idx", old_generation);
    defer alloc.free(old_prefix);
    const new_prefix = try derivedCoverageOutcomeMarkerPrefixAlloc(alloc, "semantic_idx", new_generation);
    defer alloc.free(new_prefix);
    const old_key = try derivedCoverageOutcomeKeyAlloc(alloc, "semantic_idx", old_generation, "doc:1");
    defer alloc.free(old_key);
    const new_key = try derivedCoverageOutcomeKeyAlloc(alloc, "semantic_idx", new_generation, "doc:1");
    defer alloc.free(new_key);
    const skipped_count_key = try derivedCoverageOutcomeCountKeyAlloc(alloc, "semantic_idx", new_generation, "skipped");
    defer alloc.free(skipped_count_key);
    const produced_count_key = try derivedCoverageOutcomeCountKeyAlloc(alloc, "semantic_idx", new_generation, "produced");
    defer alloc.free(produced_count_key);

    try std.testing.expect(std.mem.startsWith(u8, old_key, index_prefix));
    try std.testing.expect(std.mem.startsWith(u8, new_key, index_prefix));
    try std.testing.expect(std.mem.startsWith(u8, skipped_count_key, index_prefix));
    try std.testing.expect(std.mem.startsWith(u8, produced_count_key, index_prefix));
    try std.testing.expect(!std.mem.eql(u8, skipped_count_key, produced_count_key));
    try std.testing.expect(std.mem.startsWith(u8, old_key, old_prefix));
    try std.testing.expect(std.mem.startsWith(u8, new_key, new_prefix));
    try std.testing.expect(!std.mem.startsWith(u8, skipped_count_key, new_prefix));
    try std.testing.expect(!std.mem.startsWith(u8, old_key, new_prefix));
    try std.testing.expect(!std.mem.eql(u8, old_key, new_key));

    var encoded_count: [8]u8 = undefined;
    const encoded = encodeDerivedCoverageOutcomeCount(&encoded_count, 42);
    try std.testing.expectEqual(@as(u64, 42), try decodeDerivedCoverageOutcomeCount(encoded));
}

test "artifact repair issue keys expose authoritative encoded identity" {
    const alloc = std.testing.allocator;
    const key = try artifactRepairIssueKeyAlloc(
        alloc,
        "semantic\x00index",
        "embedding",
        "artifact\x00identity",
    );
    defer alloc.free(key);

    var parts = try artifactRepairIssueKeyPartsAlloc(alloc, key);
    defer parts.deinit(alloc);
    try std.testing.expectEqualStrings("semantic\x00index", parts.index_name);
    try std.testing.expectEqualStrings("embedding", parts.repair_artifact_kind);
    try std.testing.expectEqualStrings("artifact\x00identity", parts.issue_id);

    const malformed = try std.mem.concat(alloc, u8, &.{ key, "trailing" });
    defer alloc.free(malformed);
    try std.testing.expectError(
        error.InvalidInternalUserKey,
        artifactRepairIssueKeyPartsAlloc(alloc, malformed),
    );
}

test "managed index admission keys round trip encoded names" {
    const alloc = std.testing.allocator;
    const name = "full\x00text";
    const key = try managedIndexAdmissionKeyAlloc(alloc, name);
    defer alloc.free(key);
    const decoded = try managedIndexAdmissionNameAlloc(alloc, key);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings(name, decoded);

    const malformed = try std.mem.concat(alloc, u8, &.{ key, "trailing" });
    defer alloc.free(malformed);
    try std.testing.expectError(error.InvalidInternalUserKey, managedIndexAdmissionNameAlloc(alloc, malformed));
}

test "index artifact cleanup keys round trip encoded names" {
    const alloc = std.testing.allocator;
    const name = "dense\x00index";
    const generation: u64 = 0x1234_5678_9abc_def0;
    const root = try indexArtifactCleanupRootPrefixAlloc(alloc);
    defer alloc.free(root);
    const key = try indexArtifactCleanupKeyAlloc(alloc, name, generation);
    defer alloc.free(key);
    try std.testing.expect(std.mem.startsWith(u8, key, root));
    const decoded = try indexArtifactCleanupNameAlloc(alloc, key);
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings(name, decoded);
    try std.testing.expectEqual(generation, try indexArtifactCleanupCoverageGeneration(key));

    const malformed = try std.mem.concat(alloc, u8, &.{ key, "trailing" });
    defer alloc.free(malformed);
    try std.testing.expectError(error.InvalidInternalUserKey, indexArtifactCleanupNameAlloc(alloc, malformed));
    try std.testing.expectError(error.InvalidInternalUserKey, indexArtifactCleanupCoverageGeneration(malformed));
}

test "decodePrimaryDocumentKeyAlloc round-trips and rejects non-primary keys" {
    const alloc = std.testing.allocator;
    const key = try documentKeyAlloc(alloc, "person/ada_lovelace");
    defer alloc.free(key);
    try std.testing.expect(isPrimaryDocumentKey(key));
    const decoded = (try decodePrimaryDocumentKeyAlloc(alloc, key)).?;
    defer alloc.free(decoded);
    try std.testing.expectEqualStrings("person/ada_lovelace", decoded);

    // An asset artifact key is not a primary document key.
    const asset = try artifactNamedPrefixAlloc(alloc, "person/ada_lovelace", "asset", "relations_v1");
    defer alloc.free(asset);
    try std.testing.expect(!isPrimaryDocumentKey(asset));
    try std.testing.expect((try decodePrimaryDocumentKeyAlloc(alloc, asset)) == null);
}
