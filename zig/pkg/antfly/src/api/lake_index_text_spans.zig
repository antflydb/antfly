// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Authenticated forward and inverse coordinates for cross-file serving packs.
const std = @import("std");
const state = @import("lake_index_native_state.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
pub const max_spans = 4096;
pub const page_spans = 64;
pub const max_page_bytes = 64 * 1024;
pub const Span = struct {
    file: state.File,
    physical: artifacts.ChunkRef,
    file_rows: u32,
    file_lower: u32,
    pack_lower: u32,
    count: u32,
};
/// Encoded metadata estimate available without loading any span page.
pub fn metadataBytes(spans: []const Span, pages: []const artifacts.ChunkRef) u64 {
    var bytes: u64 = 0;
    if (pages.len != 0) {
        for (pages) |page| bytes +|= page.byte_len;
    } else {
        for (spans) |span| bytes +|= span.file.id.len + 512;
    }
    return bytes;
}
pub fn planningBytes(payload: u64, metadata: u64) u64 {
    // Include copied span/rank maps and decoded strings, not just wire bytes.
    return payload +| (metadata *| 4);
}
pub fn validate(spans: []const Span, count: u32, domain: [32]u8) !void {
    if (spans.len == 0 or spans.len > max_spans) return error.InvalidNativeLakeTextCorpus;
    var lower: u32 = 0;
    for (spans) |span| {
        try state.validate(&.{span.file}, domain);
        if (span.count == 0 or span.pack_lower != lower or @as(u64, span.file_lower) + span.count > span.file_rows or span.physical.byte_len == 0 or span.physical.byte_len > 4 * 1024 * 1024) return error.InvalidNativeLakeTextCorpus;
        try stores.validateSha256ArtifactIdentity(span.physical.artifact_id, span.physical.checksum);
        const scope = (try stores.uploadScopeFromArtifactId(span.physical.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
        if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeTextCorpus;
        lower = try std.math.add(u32, lower, span.count);
    }
    if (lower != count) return error.InvalidNativeLakeTextCorpus;
}

/// v10 packs contain complete single-run members; reject ambiguous subsets.
pub fn fromMembers(a: std.mem.Allocator, members: anytype, domain: [32]u8) ![]const Span {
    const result = try a.alloc(Span, members.len);
    var count: u32 = 0;
    for (result, members) |*span, member| {
        if (member.source_lower != 0) return error.InvalidNativeLakeTextCorpus;
        span.* = .{ .file = member.file, .physical = member.physical, .file_rows = member.count, .file_lower = 0, .pack_lower = member.lower, .count = member.count };
        count = try std.math.add(u32, count, member.count);
    }
    try validate(result, count, domain);
    return result;
}

const Page = struct { version: u8 = 1, spans: []const Span };
pub fn publish(a: std.mem.Allocator, store: *stores.ArtifactStore, spans: []const Span, token: @import("antfly_cancellation").CancellationToken) ![]const artifacts.ChunkRef {
    var refs: std.ArrayList(artifacts.ChunkRef) = .empty;
    var offset: usize = 0;
    while (offset < spans.len) {
        try token.check();
        var count = @min(page_spans, spans.len - offset);
        while (true) {
            const bytes = try std.json.Stringify.valueAlloc(a, Page{ .spans = spans[offset..][0..count] }, .{});
            defer a.free(bytes);
            if (bytes.len > max_page_bytes) {
                if (count == 1) return error.NativeLakeTextCorpusTooLarge;
                count /= 2;
                continue;
            }
            var writer = store.*;
            writer.allocator = a;
            const ref = try writer.putWithCancellation(bytes, token);
            defer if (ref.provider_version) |version| a.free(version);
            try refs.append(a, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len });
            offset += count;
            break;
        }
    }
    return refs.toOwnedSlice(a);
}
pub fn load(a: std.mem.Allocator, store: stores.ArtifactStore, pages: []const artifacts.ChunkRef, count: u32, domain: [32]u8, token: @import("antfly_cancellation").CancellationToken, cached: ?artifacts.CachedRead, remaining: ?*u64) ![]const Span {
    return loadInner(a, store, pages, count, domain, token, cached, remaining, false);
}
pub fn loadPublication(a: std.mem.Allocator, store: stores.ArtifactStore, pages: []const artifacts.ChunkRef, count: u32, domain: [32]u8, token: @import("antfly_cancellation").CancellationToken, remaining: ?*u64) ![]const Span {
    return loadInner(a, store, pages, count, domain, token, null, remaining, true);
}
fn loadInner(a: std.mem.Allocator, store: stores.ArtifactStore, pages: []const artifacts.ChunkRef, count: u32, domain: [32]u8, token: @import("antfly_cancellation").CancellationToken, cached: ?artifacts.CachedRead, remaining: ?*u64, publication: bool) ![]const Span {
    if (count == 0 or count > max_spans or pages.len == 0 or pages.len > count) return error.InvalidNativeLakeTextCorpus;
    const result = try a.alloc(Span, count);
    var offset: usize = 0;
    var allowance: u64 = 4 * 1024 * 1024;
    for (pages) |ref| {
        try token.check();
        try @import("lake_index_text_directory.zig").validateRef(ref, domain);
        if (ref.byte_len > max_page_bytes) return error.InvalidNativeLakeTextCorpus;
        try stores.chargeReadBudget(&allowance, ref.byte_len);
        if (remaining) |budget| try stores.chargeReadBudget(budget, ref.byte_len);
        const bytes = if (publication) try artifacts.readPublicationArtifact(a, store, ref, token) else try artifacts.readArtifact(a, store, ref, token, cached);
        defer a.free(bytes);
        const page = try std.json.parseFromSliceLeaky(Page, a, bytes, .{ .allocate = .alloc_always });
        if (page.version != 1 or page.spans.len == 0 or page.spans.len > page_spans or page.spans.len > count - offset) return error.InvalidNativeLakeTextCorpus;
        @memcpy(result[offset..][0..page.spans.len], page.spans);
        offset += page.spans.len;
    }
    if (offset != count) return error.InvalidNativeLakeTextCorpus;
    return result;
}
