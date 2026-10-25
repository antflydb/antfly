// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Logical row identity and value equivalence, independent of file layout.
//! Adapters supply canonical typed keys or authoritative writer lineage. A
//! matching ID alone never authorizes reuse of an old index contribution.
const std = @import("std");
pub const Digest = [32]u8;
pub const Id = Digest;
pub const Adapter = enum(u8) { owned = 1, writer_lineage = 2, stable_key = 3 };
pub const Namespace = struct {
    incarnation: Digest,
    adapter: Adapter,
    recipe: Digest,
    pub fn digest(self: Namespace) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly.logical-row.namespace.v1");
        hash.update(&self.incarnation);
        hash.update(&.{@backingInt(self.adapter)});
        hash.update(&self.recipe);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    pub fn validate(self: Namespace) !void {
        if (std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &self.recipe, 0)) return error.InvalidLogicalRowIdentity;
    }
};

pub const State = struct {
    id: Id,
    /// A digest over authoritative change version or exact canonical input
    /// values, scoped to the index recipe. Different recipes never share proof.
    values: Digest,
    pub fn validate(self: State) !void {
        if (std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.values, 0)) return error.InvalidLogicalRowIdentity;
    }
    pub fn unchanged(self: State, next: State) bool {
        return std.mem.eql(u8, &self.id, &next.id) and std.mem.eql(u8, &self.values, &next.values);
    }
};

pub const Coordinate = struct {
    file: u32,
    group: u32,
    row: u64,
    pub fn encode(self: Coordinate) [16]u8 {
        var result: [16]u8 = undefined;
        std.mem.writeInt(u32, result[0..4], self.file, .big);
        std.mem.writeInt(u32, result[4..8], self.group, .big);
        std.mem.writeInt(u64, result[8..16], self.row, .big);
        return result;
    }
    pub fn decode(bytes: []const u8) !Coordinate {
        if (bytes.len != 16) return error.InvalidLogicalRowIdentity;
        return .{ .file = std.mem.readInt(u32, bytes[0..4], .big), .group = std.mem.readInt(u32, bytes[4..8], .big), .row = std.mem.readInt(u64, bytes[8..16], .big) };
    }
};

/// Key bytes must already use the owning engine's canonical typed encoding.
/// Nulls and duplicate keys are rejected by the streaming state builder.
pub fn keyId(namespace: Namespace, canonical: []const u8) !Id {
    try namespace.validate();
    if (namespace.adapter == .writer_lineage or canonical.len == 0) return error.InvalidLogicalRowIdentity;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.logical-row.key.v1");
    hash.update(&namespace.digest());
    hash.update(canonical);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
pub fn lineageId(namespace: Namespace, row_id: u64) !Id {
    try namespace.validate();
    if (namespace.adapter != .writer_lineage or row_id > std.math.maxInt(i64)) return error.InvalidLogicalRowIdentity;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.logical-row.lineage.v1");
    hash.update(&namespace.digest());
    var word: [8]u8 = undefined;
    std.mem.writeInt(u64, &word, row_id, .big);
    hash.update(&word);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
pub fn contentVersion(recipe: Digest, canonical_values: []const u8) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.logical-row.values.v1");
    hash.update(&recipe);
    hash.update(canonical_values);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
pub fn sequenceVersion(recipe: Digest, sequence: u64) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly.logical-row.sequence.v1");
    hash.update(&recipe);
    var word: [8]u8 = undefined;
    std.mem.writeInt(u64, &word, sequence, .big);
    hash.update(&word);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
/// Iceberg v3 missing/null lineage columns inherit from data-file metadata.
/// Historical upgraded files with no first_row_id cannot invent row identity.
pub fn inheritedLineage(namespace: Namespace, recipe: Digest, explicit_id: ?i64, explicit_sequence: ?i64, first_row_id: ?u64, data_sequence: ?u64, file_position: u64) !?State {
    const id = if (explicit_id) |value| std.math.cast(u64, value) orelse return error.InvalidLogicalRowIdentity else if (first_row_id) |base| std.math.add(u64, base, file_position) catch return error.InvalidLogicalRowIdentity else return null;
    const sequence = if (explicit_sequence) |value| std.math.cast(u64, value) orelse return error.InvalidLogicalRowIdentity else data_sequence orelse return null;
    if (sequence > std.math.maxInt(i64)) return error.InvalidLogicalRowIdentity;
    return .{ .id = try lineageId(namespace, id), .values = sequenceVersion(recipe, sequence) };
}

pub const Public = struct { namespace: Digest, id: Id };
pub fn publicId(a: std.mem.Allocator, namespace: Digest, id: Id) ![]u8 {
    if (std.mem.allEqual(u8, &namespace, 0) or std.mem.allEqual(u8, &id, 0)) return error.InvalidLogicalRowIdentity;
    return std.fmt.allocPrint(a, "lake4:{s}:{s}", .{ std.fmt.bytesToHex(namespace, .lower), std.fmt.bytesToHex(id, .lower) });
}
pub fn parsePublic(bytes: []const u8) !Public {
    if (bytes.len != 135 or !std.mem.startsWith(u8, bytes, "lake4:") or bytes[70] != ':') return error.InvalidLogicalRowIdentity;
    var result: Public = undefined;
    for (bytes[6..70]) |byte| if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.InvalidLogicalRowIdentity;
    for (bytes[71..]) |byte| if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.InvalidLogicalRowIdentity;
    _ = std.fmt.hexToBytes(&result.namespace, bytes[6..70]) catch return error.InvalidLogicalRowIdentity;
    _ = std.fmt.hexToBytes(&result.id, bytes[71..]) catch return error.InvalidLogicalRowIdentity;
    if (std.mem.allEqual(u8, &result.namespace, 0) or std.mem.allEqual(u8, &result.id, 0)) return error.InvalidLogicalRowIdentity;
    return result;
}

test "logical lake identity separates incarnation key values recipe and physical location" {
    const namespace: Namespace = .{ .incarnation = @splat(1), .adapter = .stable_key, .recipe = @splat(2) };
    const id = try keyId(namespace, "typed-key");
    const state: State = .{ .id = id, .values = contentVersion(@splat(3), "old text") };
    try std.testing.expect(state.unchanged(.{ .id = id, .values = state.values }));
    try std.testing.expect(!state.unchanged(.{ .id = id, .values = contentVersion(@splat(3), "new text") }));
    try std.testing.expect(!std.mem.eql(u8, &state.values, &contentVersion(@splat(4), "old text")));
    var replacement = namespace;
    replacement.incarnation = @splat(9);
    try std.testing.expect(!std.mem.eql(u8, &id, &try keyId(replacement, "typed-key")));
    const public = try publicId(std.testing.allocator, namespace.digest(), id);
    defer std.testing.allocator.free(public);
    try std.testing.expectEqual(id, (try parsePublic(public)).id);
    try std.testing.expectEqual(namespace.digest(), (try parsePublic(public)).namespace);
    const location: Coordinate = .{ .file = 9, .group = 5, .row = 9007199254740993 };
    try std.testing.expectEqual(location, try Coordinate.decode(&location.encode()));
    try std.testing.expectError(error.InvalidLogicalRowIdentity, keyId(namespace, ""));
    try std.testing.expectError(error.InvalidLogicalRowIdentity, parsePublic("lake4:invalid"));
}

test "logical lake Iceberg lineage inherits missing values and distinguishes updates from relocation" {
    const namespace: Namespace = .{ .incarnation = @splat(1), .adapter = .writer_lineage, .recipe = @splat(2) };
    const original = (try inheritedLineage(namespace, @splat(3), null, null, 100, 7, 5)).?;
    const moved = (try inheritedLineage(namespace, @splat(3), 105, 7, 9999, 10, 3)).?;
    try std.testing.expect(original.unchanged(moved));
    const updated = (try inheritedLineage(namespace, @splat(3), 105, null, 9999, 10, 3)).?;
    try std.testing.expectEqual(original.id, updated.id);
    try std.testing.expect(!original.unchanged(updated));
    try std.testing.expect(try inheritedLineage(namespace, @splat(3), null, null, null, 7, 0) == null);
    try std.testing.expectError(error.InvalidLogicalRowIdentity, inheritedLineage(namespace, @splat(3), -1, 7, null, null, 0));
    try std.testing.expectError(error.InvalidLogicalRowIdentity, inheritedLineage(namespace, @splat(3), null, null, std.math.maxInt(u64), 7, 1));
}
