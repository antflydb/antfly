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

//! Shared authenticated roots and ordered-key helpers for paged projections.
const std = @import("std");
const manifest = @import("manifest/mod.zig");
const tree = @import("graph_segment/page_tree.zig");
const PageStore = @import("graph_segment/page_store.zig").PageStore;
const A = std.mem.Allocator;
pub const metadata_version: u16 = 2;
pub const Root = struct {
    format: u8 = 1,
    kind: manifest.ArtifactKind,
    domain: [32]u8,
    index_name: []const u8,
    source_fingerprint: [32]u8,
    docs: ?tree.Ref = null,
    postings: ?tree.Ref = null,
    directory: ?tree.Ref = null,
    centroids: ?tree.Ref = null,
    global_centers: ?tree.Ref = null,
    dims: u32 = 0,
    metric: @import("antfly_vector").vector.DistanceMetric = .cosine,
    base_probe_count: u32 = 2,
    shortlist_multiplier: u32 = 2,
};
/// Immutable native payload referenced by a routing leaf.
pub const Blob = struct {
    digest: [32]u8,
    attempt: [16]u8,
    bytes: u64,
    pub fn identity(self: Blob, domain: [32]u8) ![175]u8 {
        return (@import("artifacts/store.zig").UploadScope{ .domain = domain, .attempt = self.attempt }).artifactId(&std.fmt.bytesToHex(self.digest, .lower));
    }
    pub fn fromMetadata(metadata: @import("artifacts/store.zig").ArtifactMetadata, attempt: [16]u8) !Blob {
        return .{ .digest = try @import("artifacts/store.zig").sha256DigestFromChecksum(metadata.checksum), .attempt = attempt, .bytes = metadata.byte_len };
    }
    pub fn encode(self: Blob) [56]u8 {
        var bytes: [56]u8 = undefined;
        @memcpy(bytes[0..32], &self.digest);
        @memcpy(bytes[32..48], &self.attempt);
        std.mem.writeInt(u64, bytes[48..56], self.bytes, .little);
        return bytes;
    }
    pub fn decode(bytes: []const u8) !Blob {
        if (bytes.len < 56) return error.InvalidPagedProjectionRoute;
        var value: Blob = undefined;
        @memcpy(&value.digest, bytes[0..32]);
        @memcpy(&value.attempt, bytes[32..48]);
        value.bytes = std.mem.readInt(u64, bytes[48..56], .little);
        if (value.bytes == 0) return error.InvalidPagedProjectionRoute;
        return value;
    }
};
pub fn load(a: A, pages: *PageStore, ref: manifest.ArtifactRef) !std.json.Parsed(Root) {
    if (ref.metadata_version != metadata_version or (ref.kind != .vector_segment and ref.kind != .sparse_segment) or ref.byte_len > 1024 * 1024) return error.InvalidPagedProjectionRoot;
    const bytes = try pages.artifacts.getVerifiedAllocWithCancellationUsingAllocator(a, ref.artifact_id, ref.byte_len, ref.checksum, pages.cancellation);
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(Root, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    const root = parsed.value;
    if ((root.format != 1 and root.format != 2) or root.kind != ref.kind or !std.mem.eql(u8, &root.domain, &pages.domain) or (ref.name.len != 0 and !std.mem.eql(u8, root.index_name, ref.name))) return error.InvalidPagedProjectionRoot;
    if (root.kind == .vector_segment) {
        if ((root.format == 1 and (root.postings != null or root.global_centers != null)) or (root.format == 2 and (root.postings == null or root.global_centers == null)) or (root.docs != null and (root.directory == null or root.centroids == null or root.dims == 0))) return error.InvalidPagedProjectionRoot;
    } else if (root.format != 1 or root.global_centers != null or root.directory != null or root.centroids != null or ((root.docs == null) != (root.postings == null))) return error.InvalidPagedProjectionRoot;
    for ([_]?tree.Ref{ root.docs, root.postings, root.directory, root.centroids, root.global_centers }) |page| if (page) |value| try value.validate();
    return parsed;
}
pub fn publish(a: A, pages: *PageStore, root: Root) !manifest.ArtifactRef {
    const bytes = try std.json.Stringify.valueAlloc(a, root, .{ .emit_null_optional_fields = false });
    defer a.free(bytes);
    if (bytes.len > 1024 * 1024) return error.InvalidPagedProjectionRoot;
    var metadata = try pages.artifacts.put(bytes);
    defer metadata.deinit(pages.artifacts.allocator);
    const id = try a.dupe(u8, metadata.artifact_id);
    errdefer a.free(id);
    const checksum = try a.dupe(u8, metadata.checksum);
    errdefer a.free(checksum);
    return .{ .kind = root.kind, .metadata_version = metadata_version, .name = try a.dupe(u8, root.index_name), .artifact_id = id, .checksum = checksum, .byte_len = metadata.byte_len };
}
pub fn fingerprint(a: A, source: anytype) ![32]u8 {
    const json = try std.json.Stringify.valueAlloc(a, source, .{});
    defer a.free(json);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(json, &digest, .{});
    return digest;
}
pub fn applyMap(a: A, scratch: A, store: tree.Store, prior: ?tree.Ref, map: std.StringHashMapUnmanaged(?[]const u8)) !?tree.Ref {
    const mutations = try scratch.alloc(tree.Mutation, map.count());
    var it = map.iterator();
    var i: usize = 0;
    while (it.next()) |entry| : (i += 1) mutations[i] = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
    std.mem.sort(tree.Mutation, mutations, {}, struct {
        fn less(_: void, l: tree.Mutation, rhs: tree.Mutation) bool {
            return std.mem.lessThan(u8, l.key, rhs.key);
        }
    }.less);
    return tree.apply(a, store, prior, mutations);
}
pub fn postingKey(a: A, term: []const u8, id: ?[]const u8) ![]u8 {
    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(a);
    for (term) |byte| {
        try out.append(a, byte);
        if (byte == 0) try out.append(a, 255);
    }
    if (id) |value| {
        try out.appendSlice(a, &.{ 0, 0 });
        try out.appendSlice(a, value);
    }
    return out.toOwnedSlice(a);
}
pub fn postingDocument(key: []const u8) ![]const u8 {
    var i: usize = 0;
    while (i < key.len) : (i += 1) if (key[i] == 0) {
        if (i + 1 >= key.len) return error.InvalidPagedProjectionPosting;
        if (key[i + 1] == 0) return if (i + 2 < key.len) key[i + 2 ..] else error.InvalidPagedProjectionPosting;
        if (key[i + 1] != 255) return error.InvalidPagedProjectionPosting;
        i += 1;
    };
    return error.InvalidPagedProjectionPosting;
}
pub fn upperBound(a: A, prefix: []const u8) !?[]u8 {
    var i = prefix.len;
    while (i != 0) {
        i -= 1;
        if (prefix[i] == 255) continue;
        const upper = try a.dupe(u8, prefix[0 .. i + 1]);
        upper[i] += 1;
        return upper;
    }
    return null;
}
