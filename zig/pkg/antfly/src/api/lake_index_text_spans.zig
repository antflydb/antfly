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
pub const Source = struct { file: state.File, physical: artifacts.ChunkRef, file_rows: u32 };
/// Copy into the caller's owning arena, interning complete source descriptors.
/// Reused ranges share paths rather than materializing per-range JSON strings.
pub fn cloneIntoArena(a: std.mem.Allocator, spans: []const Span) ![]const Span {
    var scratch = std.heap.ArenaAllocator.init(@import("antfly_local_sources").sql_memory_budget.reclaimingAllocator(a));
    defer scratch.deinit();
    var sources: Interner = .{ .a = scratch.allocator() };
    defer sources.deinit();
    const result = try a.alloc(Span, spans.len);
    for (spans, result) |span, *copy| {
        const index = try sources.add(.{ .file = span.file, .physical = span.physical, .file_rows = span.file_rows }, a);
        const source = sources.sources.items[index];
        copy.* = span;
        copy.file = source.file;
        copy.physical = source.physical;
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

const dictionary_magic = "AFSD003\x00";
const range_magic = "AFSR003\x00";
pub const max_total_bytes = 4 * 1024 * 1024;
pub const Range = struct {
    source: u32,
    file_lower: u32,
    pack_lower: u32,
    count: u32,
};
/// Query-owned compact coordinates; source descriptors occur once per pack.
pub const View = struct {
    sources: []const Source,
    ranges: []const Range,
    pub fn at(self: View, i: usize) Span {
        const r = self.ranges[i];
        const src = self.sources[r.source];
        return .{ .file = src.file, .physical = src.physical, .file_rows = src.file_rows, .file_lower = r.file_lower, .pack_lower = r.pack_lower, .count = r.count };
    }
    pub fn validateMode(self: View, count: u32, domain: [32]u8, masked: bool) !void {
        if (self.ranges.len == 0 or self.ranges.len > max_spans or self.sources.len == 0 or self.sources.len > self.ranges.len) return error.InvalidNativeLakeTextCorpus;
        for (self.sources) |src| {
            try state.validate(&.{src.file}, domain);
            try @import("lake_index_text_directory.zig").validateRef(src.physical, domain);
        }
        var used: [max_spans / 64]u64 = @splat(0);
        var upper: u32 = 0;
        for (self.ranges) |r| {
            if (r.source >= self.sources.len or r.count == 0 or (if (masked) r.pack_lower < upper else r.pack_lower != upper) or @as(u64, r.file_lower) + r.count > self.sources[r.source].file_rows) return error.InvalidNativeLakeTextCorpus;
            used[r.source / 64] |= @as(u64, 1) << @as(u6, @intCast(r.source % 64));
            upper = try std.math.add(u32, r.pack_lower, r.count);
            if (upper > count) return error.InvalidNativeLakeTextCorpus;
        }
        if (!masked and upper != count) return error.InvalidNativeLakeTextCorpus;
        for (0..self.sources.len) |i| if (used[i / 64] & (@as(u64, 1) << @as(u6, @intCast(i % 64))) == 0) return error.InvalidNativeLakeTextCorpus;
    }
    pub fn expand(self: View, a: std.mem.Allocator) ![]const Span {
        const spans = try a.alloc(Span, self.ranges.len);
        for (spans, 0..) |*span, i| span.* = self.at(i);
        return spans;
    }
};
fn sourceEqual(l: Source, r: Source) bool {
    if (l.file_rows != r.file_rows or !std.mem.eql(u8, l.file.id, r.file.id) or !std.mem.eql(u8, &l.file.digest, &r.file.digest) or !std.meta.eql(l.file.key_digest, r.file.key_digest) or !std.meta.eql(l.file.aggregate_digest, r.file.aggregate_digest) or l.file.count != r.file.count or l.file.key_width != r.file.key_width or l.file.docs.len != r.file.docs.len or !refEqual(l.physical, r.physical)) return false;
    for (l.file.docs, r.file.docs) |x, y| if (!refEqual(x, y)) return false;
    return true;
}
fn refEqual(l: artifacts.ChunkRef, r: artifacts.ChunkRef) bool {
    return l.byte_len == r.byte_len and std.mem.eql(u8, l.artifact_id, r.artifact_id) and std.mem.eql(u8, l.checksum, r.checksum);
}
const Interner = struct {
    a: std.mem.Allocator,
    sources: std.ArrayList(Source) = .empty,
    ids: std.AutoHashMapUnmanaged([64]u8, u32) = .empty,
    fn deinit(self: *@This()) void {
        self.sources.deinit(self.a);
        self.ids.deinit(self.a);
    }
    fn add(self: *@This(), src: Source, own: ?std.mem.Allocator) !u32 {
        var key: [64]u8 = undefined;
        std.crypto.hash.Blake3.hash(src.file.id, key[0..32], .{});
        std.crypto.hash.Blake3.hash(src.physical.artifact_id, key[32..64], .{});
        const entry = try self.ids.getOrPut(self.a, key);
        if (entry.found_existing) {
            if (!sourceEqual(self.sources.items[entry.value_ptr.*], src)) return error.InvalidNativeLakeTextCorpus;
            return entry.value_ptr.*;
        }
        const copy = if (own) |allocator| blk: {
            const bytes = try std.json.Stringify.valueAlloc(self.a, src, .{});
            defer self.a.free(bytes);
            break :blk try std.json.parseFromSliceLeaky(Source, allocator, bytes, .{ .allocate = .alloc_always });
        } else src;
        entry.value_ptr.* = @intCast(self.sources.items.len);
        try self.sources.append(self.a, copy);
        return entry.value_ptr.*;
    }
};
/// Complete encoded admission precedes any artifact/contribution mutation.
pub const Prepared = struct {
    pages: []const []const u8,
    bytes: usize,
    pub fn init(a: std.mem.Allocator, spans: []const Span) !Prepared {
        return initMode(a, spans, false, max_total_bytes);
    }
    pub fn initWithLimit(a: std.mem.Allocator, spans: []const Span, limit: usize) !Prepared {
        return initMode(a, spans, false, @min(limit, max_total_bytes));
    }
    fn initMode(a: std.mem.Allocator, spans: []const Span, force: bool, byte_limit: usize) !Prepared {
        if (spans.len == 0 or spans.len > max_spans) return error.NativeLakeTextCorpusTooLarge;
        if (!force and spans.len <= inline_spans) {
            const inline_bytes = try std.json.Stringify.valueAlloc(a, spans, .{});
            defer a.free(inline_bytes);
            if (inline_bytes.len <= max_page_bytes and inline_bytes.len <= byte_limit) return .{ .pages = &.{}, .bytes = inline_bytes.len };
        }
        var scratch = std.heap.ArenaAllocator.init(@import("antfly_local_sources").sql_memory_budget.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        var intern: Interner = .{ .a = temporary };
        defer intern.deinit();
        const ranges = try temporary.alloc(Range, spans.len);
        for (spans, ranges) |span, *r| r.* = .{ .source = try intern.add(.{ .file = span.file, .physical = span.physical, .file_rows = span.file_rows }, null), .file_lower = span.file_lower, .pack_lower = span.pack_lower, .count = span.count };
        var pages: std.ArrayList([]const u8) = .empty;
        errdefer {
            for (pages.items) |page| a.free(page);
            pages.deinit(a);
        }
        var bytes: usize = 0;
        const dictionary = try std.json.Stringify.valueAlloc(temporary, intern.sources.items, .{});
        if (dictionary.len > byte_limit) return error.NativeLakeTextCorpusTooLarge;
        // Reserve the pointer list first so an append cannot strand an encoded
        // page when a reclaiming allocator declines further growth.
        const dictionary_pages = std.math.divCeil(usize, dictionary.len, max_page_bytes - 20) catch unreachable;
        const range_pages = std.math.divCeil(usize, ranges.len, (max_page_bytes - 16) / 16) catch unreachable;
        try pages.ensureTotalCapacity(a, dictionary_pages + range_pages);
        var offset: usize = 0;
        while (offset < dictionary.len) {
            const count: usize = @min(max_page_bytes - 20, dictionary.len - offset);
            bytes = try std.math.add(usize, bytes, 20 + count);
            if (bytes > byte_limit) return error.NativeLakeTextCorpusTooLarge;
            const page = try a.alloc(u8, 20 + count);
            page[0..8].* = dictionary_magic.*;
            std.mem.writeInt(u32, page[8..12], @intCast(intern.sources.items.len), .little);
            std.mem.writeInt(u32, page[12..16], @intCast(dictionary.len), .little);
            std.mem.writeInt(u32, page[16..20], @intCast(offset), .little);
            @memcpy(page[20..], dictionary[offset..][0..count]);
            pages.appendAssumeCapacity(page);
            offset += count;
        }
        offset = 0;
        while (offset < ranges.len) {
            const count: usize = @min((max_page_bytes - 16) / 16, ranges.len - offset);
            bytes = try std.math.add(usize, bytes, 16 + count * 16);
            if (bytes > byte_limit) return error.NativeLakeTextCorpusTooLarge;
            const page = try a.alloc(u8, 16 + count * 16);
            page[0..8].* = range_magic.*;
            std.mem.writeInt(u32, page[8..12], @intCast(ranges.len), .little);
            std.mem.writeInt(u32, page[12..16], @intCast(offset), .little);
            for (ranges[offset..][0..count], 0..) |r, i| {
                const record = page[16 + i * 16 ..][0..16];
                std.mem.writeInt(u32, record[0..4], r.source, .little);
                std.mem.writeInt(u32, record[4..8], r.file_lower, .little);
                std.mem.writeInt(u32, record[8..12], r.pack_lower, .little);
                std.mem.writeInt(u32, record[12..16], r.count, .little);
            }
            pages.appendAssumeCapacity(page);
            offset += count;
        }
        return .{ .pages = try pages.toOwnedSlice(a), .bytes = bytes };
    }
    pub fn deinit(self: Prepared, a: std.mem.Allocator) void {
        for (self.pages) |page| a.free(page);
        if (self.pages.len != 0) a.free(self.pages);
    }
    pub fn publish(self: Prepared, a: std.mem.Allocator, store: *stores.ArtifactStore, token: @import("antfly_cancellation").CancellationToken) ![]const artifacts.ChunkRef {
        const refs = try a.alloc(artifacts.ChunkRef, self.pages.len);
        for (self.pages, refs) |bytes, *ref| {
            try token.check();
            var writer = store.*;
            writer.allocator = a;
            const value = try writer.putWithCancellation(bytes, token);
            defer if (value.provider_version) |version| a.free(version);
            ref.* = .{ .artifact_id = value.artifact_id, .checksum = value.checksum, .byte_len = value.byte_len };
        }
        return refs;
    }
};
pub fn publish(a: std.mem.Allocator, store: *stores.ArtifactStore, spans: []const Span, token: @import("antfly_cancellation").CancellationToken) ![]const artifacts.ChunkRef {
    const prepared = try Prepared.initMode(a, spans, true, max_total_bytes);
    defer prepared.deinit(a);
    return prepared.publish(a, store, token);
}
pub fn load(a: std.mem.Allocator, store: stores.ArtifactStore, pages: []const artifacts.ChunkRef, count: u32, domain: [32]u8, token: @import("antfly_cancellation").CancellationToken, cached: ?artifacts.CachedRead, remaining: ?*u64) ![]const Span {
    return (try loadView(a, store, pages, count, domain, token, cached, remaining, false)).expand(a);
}
pub fn loadPublication(a: std.mem.Allocator, store: stores.ArtifactStore, pages: []const artifacts.ChunkRef, count: u32, domain: [32]u8, token: @import("antfly_cancellation").CancellationToken, remaining: ?*u64) ![]const Span {
    return (try loadView(a, store, pages, count, domain, token, null, remaining, true)).expand(a);
}
pub fn loadView(a: std.mem.Allocator, store: stores.ArtifactStore, pages: []const artifacts.ChunkRef, count: u32, domain: [32]u8, token: @import("antfly_cancellation").CancellationToken, cached: ?artifacts.CachedRead, remaining: ?*u64, publication: bool) !View {
    if (count == 0 or count > max_spans or pages.len == 0 or pages.len > max_spans + 128) return error.InvalidNativeLakeTextCorpus;
    var scratch = std.heap.ArenaAllocator.init(@import("antfly_local_sources").sql_memory_budget.reclaimingAllocator(a));
    defer scratch.deinit();
    const temporary = scratch.allocator();
    var intern: Interner = .{ .a = temporary };
    defer intern.deinit();
    const ranges = try a.alloc(Range, count);
    var sources: []Source = &.{};
    var dictionary: []u8 = &.{};
    var dictionary_count: u32 = 0;
    var dictionary_offset: usize = 0;
    var range_offset: usize = 0;
    var allowance: u64 = max_total_bytes;
    var modern: ?bool = null;
    for (pages) |ref| {
        try token.check();
        try @import("lake_index_text_directory.zig").validateRef(ref, domain);
        if (ref.byte_len > max_page_bytes) return error.InvalidNativeLakeTextCorpus;
        try stores.chargeReadBudget(&allowance, ref.byte_len);
        if (remaining) |budget| try stores.chargeReadBudget(budget, ref.byte_len);
        const bytes = if (publication) try artifacts.readPublicationArtifact(a, store, ref, token) else try artifacts.readArtifact(a, store, ref, token, cached);
        defer a.free(bytes);
        const dictionary_page = std.mem.startsWith(u8, bytes, dictionary_magic);
        const records = std.mem.startsWith(u8, bytes, range_magic);
        if (modern) |v| {
            if (v != (dictionary_page or records)) return error.InvalidNativeLakeTextCorpus;
        } else modern = dictionary_page or records;
        if (dictionary_page) {
            if (bytes.len <= 20 or range_offset != 0) return error.InvalidNativeLakeTextCorpus;
            const total = std.mem.readInt(u32, bytes[8..12], .little);
            const length = std.mem.readInt(u32, bytes[12..16], .little);
            const lower = std.mem.readInt(u32, bytes[16..20], .little);
            if (total == 0 or total > count or length == 0 or length > max_total_bytes or lower != dictionary_offset or lower > length or bytes.len - 20 > length - lower) return error.InvalidNativeLakeTextCorpus;
            if (dictionary.len == 0) {
                dictionary = try temporary.alloc(u8, length);
                dictionary_count = total;
            }
            if (length != dictionary.len or total != dictionary_count) return error.InvalidNativeLakeTextCorpus;
            @memcpy(dictionary[lower..][0 .. bytes.len - 20], bytes[20..]);
            dictionary_offset += bytes.len - 20;
        } else if (records) {
            if (bytes.len < 16 or (bytes.len - 16) % 16 != 0 or dictionary_offset == 0 or dictionary_offset != dictionary.len or std.mem.readInt(u32, bytes[8..12], .little) != count or std.mem.readInt(u32, bytes[12..16], .little) != range_offset) return error.InvalidNativeLakeTextCorpus;
            if (sources.len == 0) {
                sources = try std.json.parseFromSliceLeaky([]Source, a, dictionary, .{ .allocate = .alloc_always });
                if (sources.len != dictionary_count) return error.InvalidNativeLakeTextCorpus;
            }
            const n = (bytes.len - 16) / 16;
            if (n == 0 or n > ranges.len - range_offset) return error.InvalidNativeLakeTextCorpus;
            for (ranges[range_offset..][0..n], 0..) |*r, i| {
                const record = bytes[16 + i * 16 ..][0..16];
                r.* = .{ .source = std.mem.readInt(u32, record[0..4], .little), .file_lower = std.mem.readInt(u32, record[4..8], .little), .pack_lower = std.mem.readInt(u32, record[8..12], .little), .count = std.mem.readInt(u32, record[12..16], .little) };
                if (r.source >= sources.len or r.count == 0) return error.InvalidNativeLakeTextCorpus;
            }
            range_offset += n;
        } else {
            var region = std.heap.ArenaAllocator.init(temporary);
            defer region.deinit();
            var decoded: [page_spans]Span = undefined;
            const n = try decodePageInto(region.allocator(), bytes, &decoded);
            if (n > ranges.len - range_offset) return error.InvalidNativeLakeTextCorpus;
            for (decoded[0..n], ranges[range_offset..][0..n]) |span, *r| r.* = .{ .source = try intern.add(.{ .file = span.file, .physical = span.physical, .file_rows = span.file_rows }, a), .file_lower = span.file_lower, .pack_lower = span.pack_lower, .count = span.count };
            range_offset += n;
        }
    }
    if (range_offset != count) return error.InvalidNativeLakeTextCorpus;
    if (modern == true) {
        if (dictionary_offset != dictionary.len or sources.len != dictionary_count) return error.InvalidNativeLakeTextCorpus;
    } else sources = try a.dupe(Source, intern.sources.items);
    return .{ .sources = sources, .ranges = ranges };
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

test "external lake global frontier shared source pages admit maximum interleaving and reject malformed coordinates" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const local = @import("antfly_local_sources");
    var directory = try local.common_test_directory.TestDirectory.init("shared-source-pages");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(92), 1, std.testing.io);
    var object = try store.put("physical-manifest");
    defer object.deinit(a);
    var files: [8]state.File = undefined;
    const suffix: [2048]u8 = @splat('x');
    for (&files, 0..) |*file, i| file.* = .{ .id = try std.fmt.allocPrint(out, "source-{d}-{s}", .{ i, suffix }), .digest = @splat(1) };
    const spans = try out.alloc(Span, max_spans);
    for (spans, 0..) |*span, i| span.* = .{ .file = files[i % 8], .physical = .{ .artifact_id = object.artifact_id, .checksum = object.checksum, .byte_len = object.byte_len }, .file_rows = 8192, .file_lower = @intCast(i / 8), .pack_lower = @intCast(i), .count = 1 };
    try validate(spans, max_spans, store.upload_scope.?.domain);
    const old_page = try encodePage(out, spans[0..page_spans]);
    const old_bytes = old_page.len * (max_spans / page_spans);
    try std.testing.expect(old_bytes > max_total_bytes);
    const prepared = try Prepared.init(out, spans);
    try std.testing.expect(prepared.bytes < 2 * 1024 * 1024);
    try std.testing.expect(prepared.pages.len <= 18);
    const refs = try prepared.publish(out, &store, .none);
    const view = try loadView(out, store, refs, max_spans, store.upload_scope.?.domain, .none, null, null, true);
    try view.validateMode(max_spans, store.upload_scope.?.domain, false);
    try std.testing.expectEqual(@as(usize, 8), view.sources.len);
    try std.testing.expectEqual(@as(usize, max_spans), view.ranges.len);
    for (view.ranges, 0..) |r, i| {
        try std.testing.expectEqual(@as(u32, @intCast(i % 8)), r.source);
        try std.testing.expectEqual(@as(u32, @intCast(i / 8)), r.file_lower);
        try std.testing.expectEqual(@as(u32, @intCast(i)), r.pack_lower);
    }
    const corrupted = try out.dupe(u8, prepared.pages[prepared.pages.len - 1]);
    std.mem.writeInt(u32, corrupted[16..20], 8, .little);
    var bad = try store.put(corrupted);
    defer bad.deinit(a);
    const bad_refs = try out.dupe(artifacts.ChunkRef, refs);
    bad_refs[bad_refs.len - 1] = .{ .artifact_id = bad.artifact_id, .checksum = bad.checksum, .byte_len = bad.byte_len };
    try std.testing.expectError(error.InvalidNativeLakeTextCorpus, loadView(out, store, bad_refs, max_spans, store.upload_scope.?.domain, .none, null, null, true));
    std.debug.print("SHARED_LINEAGE legacy_page_dictionary_bytes={d} shared_bytes={d} pages={d} sources=8 ranges=65536 range_bytes={d}\n", .{ old_bytes, prepared.bytes, prepared.pages.len, @sizeOf(Range) * view.ranges.len });
}

test "external lake global frontier shared source admission is complete before publication and retains legacy pages" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const local = @import("antfly_local_sources");
    var directory = try local.common_test_directory.TestDirectory.init("shared-source-admission");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = try stores.UploadScope.forPublication(@splat(93), 1, std.testing.io);
    var object = try store.put("physical-manifest");
    defer object.deinit(a);
    const span: Span = .{ .file = .{ .id = "source", .digest = @splat(1) }, .physical = .{ .artifact_id = object.artifact_id, .checksum = object.checksum, .byte_len = object.byte_len }, .file_rows = 1, .file_lower = 0, .pack_lower = 0, .count = 1 };
    for (0..2) |format| {
        const encoded = if (format == 0) try std.json.Stringify.valueAlloc(out, Page{ .spans = &.{span} }, .{}) else try encodePage(out, &.{span});
        var legacy = try store.put(encoded);
        defer legacy.deinit(a);
        const refs = &.{artifacts.ChunkRef{ .artifact_id = legacy.artifact_id, .checksum = legacy.checksum, .byte_len = legacy.byte_len }};
        const view = try loadView(out, store, refs, 1, store.upload_scope.?.domain, .none, null, null, true);
        try view.validateMode(1, store.upload_scope.?.domain, false);
        try std.testing.expectEqualDeep(span, view.at(0));
    }
    var too_large = span;
    const id = try out.alloc(u8, max_total_bytes);
    @memset(id, 'x');
    too_large.file.id = id;
    try std.testing.expectError(error.NativeLakeTextCorpusTooLarge, Prepared.init(out, &.{too_large}));
    // A descriptor can span dictionary pages; page size is not a per-source cap.
    too_large.file.id = id[0..100000];
    const prepared = try Prepared.init(out, &.{too_large});
    const refs = try prepared.publish(out, &store, .none);
    const view = try loadView(out, store, refs, 1, store.upload_scope.?.domain, .none, null, null, true);
    try view.validateMode(1, store.upload_scope.?.domain, false);
    try std.testing.expectEqualDeep(too_large, view.at(0));
}
