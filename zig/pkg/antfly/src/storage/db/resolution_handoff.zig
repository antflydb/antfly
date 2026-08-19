// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0 (the "License"); you may
// obtain a copy of the License at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations.

//! Private completion metadata shared by the resolution runtime and DB writer.
//! This module is deliberately not re-exported from `storage/db/mod.zig`.

const std = @import("std");
const resolver_lib = @import("antfly_resolver");
const internal_keys = @import("../internal_keys.zig");

const Allocator = std.mem.Allocator;
const digest_len = std.crypto.hash.sha2.Sha256.digest_length;
const key_kind: u8 = 0x2f;

pub const Value = [digest_len]u8;

pub fn keyAlloc(alloc: Allocator, resolution_key: []const u8) ![]u8 {
    var key = std.ArrayListUnmanaged(u8).empty;
    defer key.deinit(alloc);
    try key.appendSlice(alloc, &.{ internal_keys.replay_namespace, 0xff, key_kind });
    try internal_keys.appendEncodedComponent(&key, alloc, resolution_key);
    return try key.toOwnedSlice(alloc);
}

pub fn value(artifact_value: []const u8) Value {
    var digest: Value = undefined;
    std.crypto.hash.sha2.Sha256.hash(artifact_value, &digest, .{});
    return digest;
}

pub fn matches(
    alloc: Allocator,
    store: resolver_lib.ArtifactStore,
    resolution_key: []const u8,
    artifact_value: []const u8,
) !bool {
    const marker_key = try keyAlloc(alloc, resolution_key);
    defer alloc.free(marker_key);
    const stored = try store.get(alloc, marker_key);
    defer if (stored) |bytes| alloc.free(bytes);
    const expected = value(artifact_value);
    return if (stored) |bytes| std.mem.eql(u8, bytes, &expected) else false;
}

pub fn exists(
    alloc: Allocator,
    store: resolver_lib.ArtifactStore,
    resolution_key: []const u8,
) !bool {
    const marker_key = try keyAlloc(alloc, resolution_key);
    defer alloc.free(marker_key);
    const stored = try store.get(alloc, marker_key);
    defer if (stored) |bytes| alloc.free(bytes);
    return stored != null;
}

pub fn save(
    alloc: Allocator,
    store: resolver_lib.ArtifactStore,
    resolution_key: []const u8,
    artifact_value: []const u8,
) !void {
    const marker_key = try keyAlloc(alloc, resolution_key);
    defer alloc.free(marker_key);
    const marker_value = value(artifact_value);
    try store.put(marker_key, &marker_value);
}

pub fn delete(
    alloc: Allocator,
    store: resolver_lib.ArtifactStore,
    resolution_key: []const u8,
) !void {
    const marker_key = try keyAlloc(alloc, resolution_key);
    defer alloc.free(marker_key);
    try store.delete(marker_key);
}

test "resolution handoff keys are private and encode arbitrary artifact keys" {
    const alloc = std.testing.allocator;
    const resolution_key = try internal_keys.resolutionArtifactKeyAlloc(alloc, "doc:\x00article", "resolution\x00v1");
    defer alloc.free(resolution_key);

    const handoff_key = try keyAlloc(alloc, resolution_key);
    defer alloc.free(handoff_key);
    const other_handoff_key = try keyAlloc(alloc, "different\x00artifact");
    defer alloc.free(other_handoff_key);

    try std.testing.expect(internal_keys.isInternalMetadataKey(handoff_key));
    try std.testing.expect(!internal_keys.isInternalUserKey(handoff_key));
    try std.testing.expect(!internal_keys.isResolutionArtifactKey(handoff_key));
    try std.testing.expect(!std.mem.eql(u8, handoff_key, other_handoff_key));
}
