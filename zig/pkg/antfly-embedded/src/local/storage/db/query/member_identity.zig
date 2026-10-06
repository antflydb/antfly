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
const types = @import("../types.zig");

/// Borrowed identity shared by raw deduplication and composed member fusion.
pub const Identity = struct {
    source_table: ?[]const u8,
    id: []const u8,
    artifact_ref: ?types.ArtifactRef,

    pub fn fromHit(hit: types.SearchHit) Identity {
        return .{ .source_table = hit.source_table, .id = hit.id, .artifact_ref = hit.artifact_ref };
    }
};

pub const Context = struct {
    pub fn hash(_: Context, identity: Identity) u64 {
        var hasher = std.hash.Wyhash.init(0x4152_5449_4641_4354);
        hashOptionalBytes(&hasher, identity.source_table);
        if (identity.artifact_ref) |artifact_ref| {
            hasher.update(&.{1});
            hashArtifactRef(&hasher, artifact_ref);
        } else {
            hasher.update(&.{0});
            hashLengthPrefixedBytes(&hasher, identity.id);
        }
        return hasher.final();
    }

    pub fn eql(_: Context, left: Identity, right: Identity) bool {
        if (!optionalBytesEqual(left.source_table, right.source_table)) return false;
        if (left.artifact_ref) |left_ref| {
            const right_ref = right.artifact_ref orelse return false;
            return artifactRefsEqual(left_ref, right_ref);
        }
        if (right.artifact_ref != null) return false;
        return std.mem.eql(u8, left.id, right.id);
    }
};

fn hashArtifactRef(hasher: *std.hash.Wyhash, artifact_ref: types.ArtifactRef) void {
    hashLengthPrefixedBytes(hasher, artifact_ref.document_id);
    hashLengthPrefixedBytes(hasher, artifact_ref.name);
    hasher.update(&.{@backingInt(artifact_ref.kind)});
    hashOptionalU32(hasher, artifact_ref.chunk_id);
    hashOptionalBytes(hasher, artifact_ref.unit_id);
    if (artifact_ref.source) |source| {
        hasher.update(&.{1});
        hasher.update(&.{@backingInt(source.kind)});
        hashLengthPrefixedBytes(hasher, source.name);
        hashOptionalU32(hasher, source.chunk_id);
        hashOptionalBytes(hasher, source.unit_id);
    } else {
        hasher.update(&.{0});
    }
}

fn hashLengthPrefixedBytes(hasher: *std.hash.Wyhash, value: []const u8) void {
    var len_bytes: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &len_bytes, value.len, .little);
    hasher.update(&len_bytes);
    hasher.update(value);
}

fn hashOptionalBytes(hasher: *std.hash.Wyhash, value: ?[]const u8) void {
    if (value) |bytes| {
        hasher.update(&.{1});
        hashLengthPrefixedBytes(hasher, bytes);
    } else {
        hasher.update(&.{0});
    }
}

fn hashOptionalU32(hasher: *std.hash.Wyhash, value: ?u32) void {
    if (value) |number| {
        hasher.update(&.{1});
        var bytes: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &bytes, number, .little);
        hasher.update(&bytes);
    } else {
        hasher.update(&.{0});
    }
}

fn artifactRefsEqual(left: types.ArtifactRef, right: types.ArtifactRef) bool {
    if (left.kind != right.kind or
        left.chunk_id != right.chunk_id or
        !std.mem.eql(u8, left.document_id, right.document_id) or
        !std.mem.eql(u8, left.name, right.name) or
        !optionalBytesEqual(left.unit_id, right.unit_id))
    {
        return false;
    }
    if (left.source) |left_source| {
        const right_source = right.source orelse return false;
        return left_source.kind == right_source.kind and
            left_source.chunk_id == right_source.chunk_id and
            std.mem.eql(u8, left_source.name, right_source.name) and
            optionalBytesEqual(left_source.unit_id, right_source.unit_id);
    }
    return right.source == null;
}

fn optionalBytesEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left) |left_bytes| {
        const right_bytes = right orelse return false;
        return std.mem.eql(u8, left_bytes, right_bytes);
    }
    return right == null;
}
