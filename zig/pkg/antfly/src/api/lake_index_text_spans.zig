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

//! Authenticated forward and inverse coordinates for cross-file serving packs.
const std = @import("std");
const state = @import("lake_index_native_state.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const stores = @import("../serverless/artifacts/store.zig");
pub const max_spans = 64 * 1024;
pub const inline_spans = 64;
pub const page_spans = 256;
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
    return planningBytesWithSources(payload, metadata, 0);
}
pub fn planningBytesWithSources(payload: u64, metadata: u64, sources: usize) u64 {
    // Compact wire coordinates must not hide decoded span/rank-map residency.
    return payload +| @max(metadata *| 4, @as(u64, sources) *| (@sizeOf(Span) * 4));
}
pub fn validate(spans: []const Span, count: u32, domain: [32]u8) !void {
    return validateMode(spans, count, domain, false);
}
pub fn validateMode(spans: []const Span, count: u32, domain: [32]u8, masked: bool) !void {
    if (spans.len == 0 or spans.len > max_spans) return error.InvalidNativeLakeTextCorpus;
    var lower: u32 = 0;
    for (spans) |span| {
        try state.validate(&.{span.file}, domain);
        if (span.count == 0 or (if (masked) span.pack_lower < lower else span.pack_lower != lower) or @as(u64, span.file_lower) + span.count > span.file_rows or span.physical.byte_len == 0 or span.physical.byte_len > 4 * 1024 * 1024) return error.InvalidNativeLakeTextCorpus;
        try stores.validateSha256ArtifactIdentity(span.physical.artifact_id, span.physical.checksum);
        const scope = (try stores.uploadScopeFromArtifactId(span.physical.artifact_id)) orelse return error.InvalidNativeLakeTextCorpus;
        if (!std.mem.eql(u8, &scope.domain, &domain)) return error.InvalidNativeLakeTextCorpus;
        lower = try std.math.add(u32, span.pack_lower, span.count);
        if (lower > count) return error.InvalidNativeLakeTextCorpus;
    }
    if (!masked and lower != count) return error.InvalidNativeLakeTextCorpus;
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
const compact_magic = "AFSP002\x00";
const Source = struct { file: state.File, physical: artifacts.ChunkRef, file_rows: u32 };
/// Copy into the caller's owning arena, interning complete source descriptors.
/// Reused ranges share paths rather than materializing per-range JSON strings.
pub fn cloneIntoArena(a: std.mem.Allocator, spans: []const Span) ![]const Span {
    var scratch = std.heap.ArenaAllocator.init(@import("antfly_local_sources").sql_memory_budget.reclaimingAllocator(a));
    defer scratch.deinit();
    const temporary = scratch.allocator();
    var row_scratch = std.heap.ArenaAllocator.init(temporary);
    defer row_scratch.deinit();
    var sources: std.StringHashMapUnmanaged(Source) = .empty;
    const result = try a.alloc(Span, spans.len);
    for (spans, result) |span, *copy| {
        _ = row_scratch.reset(.retain_capacity);
        const source: Source = .{ .file = span.file, .physical = span.physical, .file_rows = span.file_rows };
        const bytes = try std.json.Stringify.valueAlloc(row_scratch.allocator(), source, .{});
        const entry = try sources.getOrPut(temporary, bytes);
        if (!entry.found_existing) {
            entry.key_ptr.* = try temporary.dupe(u8, bytes);
            entry.value_ptr.* = try std.json.parseFromSliceLeaky(Source, a, bytes, .{ .allocate = .alloc_always });
        }
        copy.* = span;
        copy.file = entry.value_ptr.file;
        copy.physical = entry.value_ptr.physical;
    }
    return result;
}
/// Page-local source dictionaries retain every authenticated source field once.
/// Fixed-width native ranges contain no duplicated paths or per-row JSON.
fn encodePage(a: std.mem.Allocator, spans: []const Span) ![]u8 {
    var scratch = std.heap.ArenaAllocator.init(@import("antfly_local_sources").sql_memory_budget.reclaimingAllocator(a));
    defer scratch.deinit();
    const temporary = scratch.allocator();
    var sources: std.ArrayList(Source) = .empty;
    var names: std.StringHashMapUnmanaged(u32) = .empty;
    const ranges = try temporary.alloc(u8, spans.len * 16);
    for (spans, 0..) |span, i| {
        const source: Source = .{ .file = span.file, .physical = span.physical, .file_rows = span.file_rows };
        const key = try std.json.Stringify.valueAlloc(temporary, source, .{});
        const entry = try names.getOrPut(temporary, key);
        if (!entry.found_existing) {
            entry.value_ptr.* = @intCast(sources.items.len);
            try sources.append(temporary, source);
        }
        const record = ranges[i * 16 ..][0..16];
        std.mem.writeInt(u32, record[0..4], entry.value_ptr.*, .little);
        std.mem.writeInt(u32, record[4..8], span.file_lower, .little);
        std.mem.writeInt(u32, record[8..12], span.pack_lower, .little);
        std.mem.writeInt(u32, record[12..16], span.count, .little);
    }
    const dictionary = try std.json.Stringify.valueAlloc(temporary, sources.items, .{});
    const bytes = try a.alloc(u8, 16 + dictionary.len + ranges.len);
    bytes[0..8].* = compact_magic.*;
    std.mem.writeInt(u32, bytes[8..12], @intCast(dictionary.len), .little);
    std.mem.writeInt(u32, bytes[12..16], @intCast(spans.len), .little);
    @memcpy(bytes[16..][0..dictionary.len], dictionary);
    @memcpy(bytes[16 + dictionary.len ..], ranges);
    return bytes;
}
fn decodePageInto(a: std.mem.Allocator, bytes: []const u8, destination: []Span) !usize {
    if (!std.mem.startsWith(u8, bytes, compact_magic)) {
        const page = try std.json.parseFromSliceLeaky(Page, a, bytes, .{ .allocate = .alloc_always });
        if (page.version != 1 or page.spans.len == 0 or page.spans.len > page_spans or page.spans.len > destination.len) return error.InvalidNativeLakeTextCorpus;
        @memcpy(destination[0..page.spans.len], page.spans);
        return page.spans.len;
    }
    if (bytes.len < 16) return error.InvalidNativeLakeTextCorpus;
    const dictionary_len = std.mem.readInt(u32, bytes[8..12], .little);
    const count = std.mem.readInt(u32, bytes[12..16], .little);
    if (count == 0 or count > page_spans or count > destination.len or dictionary_len > bytes.len - 16 or bytes.len - 16 - dictionary_len != count * 16) return error.InvalidNativeLakeTextCorpus;
    const sources = try std.json.parseFromSliceLeaky([]const Source, a, bytes[16..][0..dictionary_len], .{ .allocate = .alloc_always });
    if (sources.len == 0 or sources.len > count) return error.InvalidNativeLakeTextCorpus;
    var used: [page_spans]bool = @splat(false);
    for (destination[0..count], 0..) |*span, i| {
        const record = bytes[16 + dictionary_len + i * 16 ..][0..16];
        const index = std.mem.readInt(u32, record[0..4], .little);
        if (index >= sources.len) return error.InvalidNativeLakeTextCorpus;
        used[index] = true;
        const source = sources[index];
        span.* = .{ .file = source.file, .physical = source.physical, .file_rows = source.file_rows, .file_lower = std.mem.readInt(u32, record[4..8], .little), .pack_lower = std.mem.readInt(u32, record[8..12], .little), .count = std.mem.readInt(u32, record[12..16], .little) };
        if (span.count == 0) return error.InvalidNativeLakeTextCorpus;
    }
    for (used[0..sources.len]) |present| if (!present) return error.InvalidNativeLakeTextCorpus;
    return count;
}
pub fn publish(a: std.mem.Allocator, store: *stores.ArtifactStore, spans: []const Span, token: @import("antfly_cancellation").CancellationToken) ![]const artifacts.ChunkRef {
    var refs: std.ArrayList(artifacts.ChunkRef) = .empty;
    var total_bytes: u64 = 0;
    var offset: usize = 0;
    while (offset < spans.len) {
        try token.check();
        var count = @min(page_spans, spans.len - offset);
        while (true) {
            const bytes = try encodePage(a, spans[offset..][0..count]);
            defer a.free(bytes);
            if (bytes.len > max_page_bytes) {
                if (count == 1) return error.NativeLakeTextCorpusTooLarge;
                count /= 2;
                continue;
            }
            total_bytes = try std.math.add(u64, total_bytes, bytes.len);
            if (total_bytes > 4 * 1024 * 1024) return error.NativeLakeTextCorpusTooLarge;
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
        offset += try decodePageInto(a, bytes, result[offset..]);
    }
    if (offset != count) return error.InvalidNativeLakeTextCorpus;
    return result;
}

test "external lake global frontier compact source pages preserve lineage and bound decoded admission" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    var spans: [64]Span = undefined;
    for (&spans, 0..) |*span, i| span.* = .{
        .file = .{ .id = if (i % 2 == 0) "first" else "second", .digest = @splat(7) },
        .physical = .{ .artifact_id = "same-authenticated-original-physical-manifest", .checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", .byte_len = 4096 },
        .file_rows = 32,
        .file_lower = @intCast(i / 2),
        .pack_lower = @intCast(i),
        .count = 1,
    };
    const compact = try encodePage(a, &spans);
    defer a.free(compact);
    const legacy = try std.json.Stringify.valueAlloc(a, Page{ .spans = &spans }, .{});
    defer a.free(legacy);
    try std.testing.expect(compact.len * 4 < legacy.len);
    var decoded: [64]Span = undefined;
    try std.testing.expectEqual(@as(usize, 64), try decodePageInto(out, compact, &decoded));
    try std.testing.expectEqualDeep(&spans, &decoded);
    try std.testing.expectEqual(@as(usize, 64), try decodePageInto(out, legacy, &decoded));
    try std.testing.expectEqualDeep(&spans, &decoded);
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, decodePageInto(out, compact[0 .. compact.len - 1], &decoded));
    const bad = try out.dupe(u8, compact);
    const dictionary_len = std.mem.readInt(u32, bad[8..12], .little);
    std.mem.writeInt(u32, bad[16 + dictionary_len ..][0..4], 64, .little);
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, decodePageInto(out, bad, &decoded));
    try std.testing.expect(planningBytesWithSources(0, 16, 64) >= @sizeOf(Span) * 64 * 4);
    std.debug.print("COMPACT_SOURCE_PAGE legacy_bytes={d} compact_bytes={d} ranges=64\n", .{ legacy.len, compact.len });
}
