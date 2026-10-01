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

//! The sole supported nested owner scope is a resolver consuming a chunk or
//! unit artifact. This is not a general recursive internal-key decoder.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const ids = @import("artifact_ids.zig");

pub fn documentAlloc(alloc: std.mem.Allocator, key: []const u8) ![]u8 {
    const document = (try keys.decodeDocumentComponentAlloc(alloc, key)) orelse return error.InvalidBatchRequest;
    errdefer alloc.free(document);
    var nested_allowed = keys.isResolutionArtifactKey(key);
    // Graph edge/state keys have their own suffix grammar, not the public
    // ArtifactRef grammar. Only review assets can add the alternate nested
    // resolver scope; all other admitted families already name their owner.
    if (!nested_allowed and keys.isAssetArtifactKey(key)) {
        var outer = (try ids.decodeArtifactRefAlloc(alloc, key)) orelse return document;
        defer outer.deinit(alloc);
        nested_allowed = outer.kind == .asset and std.mem.startsWith(u8, outer.name, "_resolution_review\x1f");
    }
    if (!nested_allowed) return document;
    if (keys.isInternalMetadataKey(document)) return error.InvalidBatchRequest;
    if (!keys.isInternalUserKey(document)) return document;
    var source = (try ids.decodeArtifactRefAlloc(alloc, document)) orelse return error.InvalidBatchRequest;
    defer source.deinit(alloc);
    if ((source.kind != .chunk and !(source.kind == .asset and source.unit_id != null)) or
        keys.isInternalUserKey(source.document_id) or keys.isInternalMetadataKey(source.document_id)) return error.InvalidBatchRequest;
    const result = try alloc.dupe(u8, source.document_id);
    alloc.free(document);
    return result;
}

test "ordered artifact inventory ownership accepts graph suffixes with binary document identities" {
    const alloc = std.testing.allocator;
    const document = "doc\x00\xff";
    const edge = try keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, document, "g\x00", "mentions", "target\x00", "source\xff");
    defer alloc.free(edge);
    const count = try keys.graphEdgeContenderCountKeyAlloc(alloc, document, "g\x00");
    defer alloc.free(count);
    const Check = struct {
        fn run(a: std.mem.Allocator, key: []const u8) !void {
            const owner = try documentAlloc(a, key);
            defer a.free(owner);
            try std.testing.expectEqualStrings(document, owner);
        }
    };
    for ([_][]const u8{ edge, count }) |key| try std.testing.checkAllAllocationFailures(alloc, Check.run, .{key});
}

test "ordered artifact inventory ownership unwraps only one typed resolver source scope" {
    const alloc = std.testing.allocator;
    const chunk = try keys.chunkArtifactKeyAlloc(alloc, "doc", "extraction", 7);
    defer alloc.free(chunk);
    const unit = try keys.documentUnitArtifactKeyAlloc(alloc, "doc", "extraction", "page:7");
    defer alloc.free(unit);
    for ([_][]const u8{ chunk, unit }) |source| {
        const resolution = try keys.resolutionArtifactKeyAlloc(alloc, source, "entities");
        defer alloc.free(resolution);
        const doc = try documentAlloc(alloc, resolution);
        defer alloc.free(doc);
        try std.testing.expectEqualStrings("doc", doc);
        const review = try keys.artifactNamedPrefixAlloc(alloc, source, "asset", "_resolution_review\x1fextraction\x1fentities");
        defer alloc.free(review);
        const review_doc = try documentAlloc(alloc, review);
        defer alloc.free(review_doc);
        try std.testing.expectEqualStrings("doc", review_doc);
    }
    const primary = try keys.documentKeyAlloc(alloc, "doc");
    defer alloc.free(primary);
    const invalid = try keys.resolutionArtifactKeyAlloc(alloc, primary, "entities");
    defer alloc.free(invalid);
    try std.testing.expectError(error.InvalidBatchRequest, documentAlloc(alloc, invalid));
    const nested_chunk = try keys.chunkArtifactKeyAlloc(alloc, chunk, "extraction", 8);
    defer alloc.free(nested_chunk);
    const recursive = try keys.resolutionArtifactKeyAlloc(alloc, nested_chunk, "entities");
    defer alloc.free(recursive);
    try std.testing.expectError(error.InvalidBatchRequest, documentAlloc(alloc, recursive));
}
