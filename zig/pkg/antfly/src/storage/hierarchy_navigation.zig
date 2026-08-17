// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const ant_json = @import("antfly-json");

pub const position_version = "hn3";
const legacy_position_version = "hn2";
const unit_commitment_domain = "antfly-hierarchy-unit-commitment-v1";
/// Private storage metadata used to prove that a payload belongs to the
/// selected hierarchy revision. Public serializers and synthetic projections
/// must remove it even when a caller explicitly requests the field.
pub const unit_fingerprint_field = "_artifact_unit_fingerprint";
/// Private coordinator/shard transport field. It is never persisted and is
/// removed before a grouped-unit response crosses the public API boundary.
pub const grouped_unit_revision_envelope_field = "_hierarchy_unit_revision_token";
pub const block_size: u32 = 128;

pub fn artifactDigestAlloc(alloc: std.mem.Allocator, units: anytype) ![]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-hierarchy-artifact-v1");
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, @intCast(units.len), .big);
    hasher.update(&encoded);
    for (units) |unit| {
        std.mem.writeInt(u64, &encoded, @intCast(unit.key.len), .big);
        hasher.update(&encoded);
        hasher.update(unit.key);
        std.mem.writeInt(u64, &encoded, @intCast(unit.fingerprint.len), .big);
        hasher.update(&encoded);
        hasher.update(unit.fingerprint);
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return try lowerHexAlloc(alloc, &digest);
}

pub fn blockCount(unit_count: u32) u32 {
    if (unit_count == 0) return 0;
    return std.math.divCeil(u32, unit_count, block_size) catch unreachable;
}

pub fn summaryValueAlloc(
    alloc: std.mem.Allocator,
    generation: u64,
    digest: []const u8,
    unit_count: u32,
    block_count: u32,
) ![]u8 {
    if (generation == 0 or !digestIsValid(digest) or block_count != blockCount(unit_count)) {
        return error.InvalidDocumentExtractionState;
    }
    return try std.json.Stringify.valueAlloc(alloc, .{
        .kind = "document_unit_navigation_summary_v1",
        .generation = generation,
        .digest = digest,
        .unit_count = unit_count,
        .block_count = block_count,
        .block_size = block_size,
    }, .{});
}

pub fn blockValueAlloc(
    alloc: std.mem.Allocator,
    block_index: u32,
    units: anytype,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .kind = "document_unit_navigation_block_v1",
        .block_index = block_index,
        .units = units,
    }, .{});
}

pub fn indexMetadataMatches(
    alloc: std.mem.Allocator,
    state_raw: []const u8,
    summary_raw: []const u8,
    expected_generation: u64,
) !bool {
    if (expected_generation == 0) return false;
    var state = std.json.parseFromSlice(std.json.Value, alloc, state_raw, .{}) catch return false;
    defer state.deinit();
    var summary = std.json.parseFromSlice(std.json.Value, alloc, summary_raw, .{}) catch return false;
    defer summary.deinit();
    if (state.value != .object or summary.value != .object) return false;
    const state_digest = state.value.object.get("navigation_digest") orelse return false;
    const state_blocks = state.value.object.get("navigation_block_count") orelse return false;
    const state_block_size = state.value.object.get("navigation_block_size") orelse return false;
    const kind = summary.value.object.get("kind") orelse return false;
    const generation = summary.value.object.get("generation") orelse return false;
    const digest = summary.value.object.get("digest") orelse return false;
    const unit_count = summary.value.object.get("unit_count") orelse return false;
    const blocks = summary.value.object.get("block_count") orelse return false;
    const summary_block_size = summary.value.object.get("block_size") orelse return false;
    if (state_digest != .string or !digestIsValid(state_digest.string) or
        state_blocks != .integer or state_block_size != .integer or
        kind != .string or !std.mem.eql(u8, kind.string, "document_unit_navigation_summary_v1") or
        generation != .integer or generation.integer <= 0 or
        digest != .string or unit_count != .integer or blocks != .integer or summary_block_size != .integer)
    {
        return false;
    }
    const count = std.math.cast(u32, unit_count.integer) orelse return false;
    const block_count = std.math.cast(u32, blocks.integer) orelse return false;
    const summary_generation = std.math.cast(u64, generation.integer) orelse return false;
    return summary_generation == expected_generation and
        std.mem.eql(u8, state_digest.string, digest.string) and
        state_blocks.integer == blocks.integer and
        state_block_size.integer == summary_block_size.integer and
        summary_block_size.integer == block_size and
        block_count == blockCount(count);
}

fn digestIsValid(digest: []const u8) bool {
    if (digest.len != std.crypto.hash.sha2.Sha256.digest_length * 2) return false;
    for (digest) |byte| {
        if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return false;
    }
    return true;
}

/// Removes storage-only hierarchy metadata from an owned JSON value before it
/// crosses a public response boundary. Keeping this operation next to the
/// field definition makes it difficult for new response shapes to accidentally
/// expose the artifact-revision fingerprint.
pub fn stripUnitFingerprintValue(alloc: std.mem.Allocator, value: *std.json.Value) void {
    if (value.* != .object) return;
    removeOwnedObjectField(alloc, &value.object, unit_fingerprint_field);
}

/// Removes every hierarchy field reserved for storage or internal transport
/// before a value crosses the public API boundary. Internal shard encoders
/// must opt into their stricter revision-envelope serializer instead.
pub fn stripPublicInternalFieldsValue(alloc: std.mem.Allocator, value: *std.json.Value) void {
    if (value.* != .object) return;
    removeOwnedObjectField(alloc, &value.object, unit_fingerprint_field);
    removeOwnedObjectField(alloc, &value.object, grouped_unit_revision_envelope_field);
}

fn removeOwnedObjectField(
    alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    field: []const u8,
) void {
    if (object.fetchOrderedRemove(field)) |removed| {
        alloc.free(@constCast(removed.key));
        var removed_value = removed.value;
        deinitJsonValue(alloc, &removed_value);
    }
}

pub const Position = struct {
    source_revision: []const u8,
    artifact_name_hex: []const u8,
    generation: u64,
    ordinal: u64,
    /// Domain-separated commitment to the private storage fingerprint. The
    /// fingerprint itself must never be recoverable from a public cursor.
    unit_commitment: []const u8,
};

pub fn positionAlloc(
    alloc: std.mem.Allocator,
    source_revision: []const u8,
    artifact_name: []const u8,
    generation: u64,
    ordinal: usize,
    unit_fingerprint: []const u8,
) ![]u8 {
    if (!isLowerHex(source_revision) or source_revision.len != std.crypto.hash.sha2.Sha256.digest_length * 2 or
        artifact_name.len == 0 or unit_fingerprint.len == 0)
    {
        return error.InvalidHierarchyNavigationPosition;
    }
    const artifact_name_hex = try lowerHexAlloc(alloc, artifact_name);
    defer alloc.free(artifact_name_hex);
    const unit_commitment = try unitCommitmentAlloc(
        alloc,
        source_revision,
        artifact_name,
        generation,
        @intCast(ordinal),
        unit_fingerprint,
    );
    defer alloc.free(unit_commitment);
    return try std.fmt.allocPrint(
        alloc,
        "{s}/{s}/{s}/{d:0>20}/{d:0>20}/{s}",
        .{ position_version, source_revision, artifact_name_hex, generation, ordinal, unit_commitment },
    );
}

pub fn parsePosition(position: []const u8) !Position {
    var parts = std.mem.splitScalar(u8, position, '/');
    const version = parts.next() orelse return error.InvalidHierarchyNavigationPosition;
    if (std.mem.eql(u8, version, legacy_position_version)) {
        return error.HierarchyNavigationPositionVersionStale;
    }
    const source_revision = parts.next() orelse return error.InvalidHierarchyNavigationPosition;
    const artifact_name_hex = parts.next() orelse return error.InvalidHierarchyNavigationPosition;
    const generation_text = parts.next() orelse return error.InvalidHierarchyNavigationPosition;
    const ordinal_text = parts.next() orelse return error.InvalidHierarchyNavigationPosition;
    const unit_commitment = parts.next() orelse return error.InvalidHierarchyNavigationPosition;
    if (parts.next() != null or !std.mem.eql(u8, version, position_version) or
        source_revision.len != std.crypto.hash.sha2.Sha256.digest_length * 2 or !isLowerHex(source_revision) or
        artifact_name_hex.len == 0 or artifact_name_hex.len % 2 != 0 or !isLowerHex(artifact_name_hex) or
        generation_text.len != 20 or ordinal_text.len != 20 or
        unit_commitment.len != std.crypto.hash.sha2.Sha256.digest_length * 2 or !isLowerHex(unit_commitment))
    {
        return error.InvalidHierarchyNavigationPosition;
    }
    return .{
        .source_revision = source_revision,
        .artifact_name_hex = artifact_name_hex,
        .generation = std.fmt.parseUnsigned(u64, generation_text, 10) catch return error.InvalidHierarchyNavigationPosition,
        .ordinal = std.fmt.parseUnsigned(u64, ordinal_text, 10) catch return error.InvalidHierarchyNavigationPosition,
        .unit_commitment = unit_commitment,
    };
}

pub fn parsePositionForArtifact(artifact_name: []const u8, position: []const u8) !Position {
    const parsed = try parsePosition(position);
    if (!encodedComponentMatches(parsed.artifact_name_hex, artifact_name)) {
        return error.InvalidHierarchyNavigationPosition;
    }
    return parsed;
}

/// Verifies a private storage fingerprint against the non-reversible
/// commitment carried by a public cursor. Binding every hierarchy coordinate
/// prevents commitments from being correlated across revisions or positions.
pub fn positionUnitFingerprintMatches(
    position: Position,
    artifact_name: []const u8,
    unit_fingerprint: []const u8,
) bool {
    if (!encodedComponentMatches(position.artifact_name_hex, artifact_name)) return false;
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    unitCommitment(
        &digest,
        position.source_revision,
        artifact_name,
        position.generation,
        position.ordinal,
        unit_fingerprint,
    );
    return encodedComponentMatches(position.unit_commitment, &digest);
}

fn unitCommitmentAlloc(
    alloc: std.mem.Allocator,
    source_revision: []const u8,
    artifact_name: []const u8,
    generation: u64,
    ordinal: u64,
    unit_fingerprint: []const u8,
) ![]u8 {
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    unitCommitment(&digest, source_revision, artifact_name, generation, ordinal, unit_fingerprint);
    return try lowerHexAlloc(alloc, &digest);
}

fn unitCommitment(
    out: *[std.crypto.hash.sha2.Sha256.digest_length]u8,
    source_revision: []const u8,
    artifact_name: []const u8,
    generation: u64,
    ordinal: u64,
    unit_fingerprint: []const u8,
) void {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(unit_commitment_domain);
    hashLengthPrefixed(&hasher, source_revision);
    hashLengthPrefixed(&hasher, artifact_name);
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, generation, .big);
    hasher.update(&encoded);
    std.mem.writeInt(u64, &encoded, ordinal, .big);
    hasher.update(&encoded);
    hashLengthPrefixed(&hasher, unit_fingerprint);
    hasher.final(out);
}

fn hashLengthPrefixed(hasher: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(u64, &encoded, @intCast(value.len), .big);
    hasher.update(&encoded);
    hasher.update(value);
}

/// Returns true when `encoded` is the canonical lowercase-hex representation
/// of `raw` without allocating or decoding attacker-controlled cursor bytes.
pub fn encodedComponentMatches(encoded: []const u8, raw: []const u8) bool {
    if (encoded.len % 2 != 0 or encoded.len / 2 != raw.len or !isLowerHex(encoded)) return false;
    for (raw, 0..) |byte, i| {
        if (encoded[i * 2] != hexDigit(byte >> 4) or encoded[i * 2 + 1] != hexDigit(byte & 0x0f)) return false;
    }
    return true;
}

pub fn stripUnitFingerprintAlloc(alloc: std.mem.Allocator, stored: []const u8) ![]u8 {
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    var parsed = try std.json.parseFromSlice(std.json.Value, scratch.allocator(), stored, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidDocumentExtractionState;
    stripUnitFingerprintValue(scratch.allocator(), &parsed.value);
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

fn lowerHexAlloc(alloc: std.mem.Allocator, raw: []const u8) ![]u8 {
    const encoded_len = std.math.mul(usize, raw.len, 2) catch return error.OutOfMemory;
    const out = try alloc.alloc(u8, encoded_len);
    for (raw, 0..) |byte, i| {
        out[i * 2] = hexDigit(byte >> 4);
        out[i * 2 + 1] = hexDigit(byte & 0x0f);
    }
    return out;
}

fn hexDigit(value: u8) u8 {
    return if (value < 10) '0' + value else 'a' + (value - 10);
}

fn isLowerHex(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value) |byte| {
        if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return false;
    }
    return true;
}

fn deinitJsonValue(alloc: std.mem.Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .string => |text| alloc.free(text),
        .number_string => |text| alloc.free(text),
        .array => |*array| {
            for (array.items) |*item| deinitJsonValue(alloc, item);
            array.deinit();
        },
        .object => |*object| {
            var iterator = object.iterator();
            while (iterator.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                deinitJsonValue(alloc, entry.value_ptr);
            }
            object.deinit(alloc);
        },
        else => {},
    }
}

test "hierarchy positions round trip arbitrary components and preserve artifact ordering" {
    const alloc = std.testing.allocator;
    const revision = "0000000000000000000000000000000000000000000000000000000000000000";
    const left = try positionAlloc(alloc, revision, "asset/alpha", 7, 3, "finger/print");
    defer alloc.free(left);
    const right = try positionAlloc(alloc, revision, "asset0alpha", 7, 3, "finger/print");
    defer alloc.free(right);
    try std.testing.expect(std.mem.lessThan(u8, left, right));
    const parsed = try parsePositionForArtifact("asset/alpha", left);
    try std.testing.expectEqual(@as(u64, 7), parsed.generation);
    try std.testing.expectEqual(@as(u64, 3), parsed.ordinal);
    try std.testing.expect(positionUnitFingerprintMatches(parsed, "asset/alpha", "finger/print"));
    try std.testing.expect(!encodedComponentMatches(parsed.unit_commitment, "finger/print"));
    try std.testing.expect(std.mem.indexOf(u8, left, "66696e6765722f7072696e74") == null);
    try std.testing.expectError(error.InvalidHierarchyNavigationPosition, parsePositionForArtifact("asset", left));
    try std.testing.expectError(
        error.HierarchyNavigationPositionVersionStale,
        parsePosition("hn2/legacy"),
    );
}

test "hierarchy stored-value sanitizer always removes the unit fingerprint" {
    const alloc = std.testing.allocator;
    const sanitized = try stripUnitFingerprintAlloc(
        alloc,
        "{\"text\":\"alpha\",\"_artifact_unit_fingerprint\":\"secret\"}",
    );
    defer alloc.free(sanitized);
    try ant_json.testing.expectEqualJsonText(alloc, "{\"text\":\"alpha\"}", sanitized);

    var value = try std.json.parseFromSliceLeaky(
        std.json.Value,
        alloc,
        "{\"text\":\"alpha\",\"_artifact_unit_fingerprint\":\"secret\",\"_hierarchy_unit_revision_token\":\"private\"}",
        .{ .allocate = .alloc_always },
    );
    defer deinitJsonValue(alloc, &value);
    stripPublicInternalFieldsValue(alloc, &value);
    try std.testing.expect(value.object.get(unit_fingerprint_field) == null);
    try std.testing.expect(value.object.get(grouped_unit_revision_envelope_field) == null);
    try std.testing.expectEqualStrings("alpha", value.object.get("text").?.string);
}
