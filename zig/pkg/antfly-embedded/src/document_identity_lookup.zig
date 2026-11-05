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

//! Immutable external-ID routing. Hashes select candidates only: callers must
//! compare their stored IDs before deleting, including every duplicate copy.
const std = @import("std");

pub const version: u8 = 1;
pub const header_size: usize = 5;
pub const entry_size: usize = 12;
pub const Entry = struct {
    hash: u64,
    doc: u32,
    pub fn lessThan(_: void, a: Entry, b: Entry) bool {
        return a.hash < b.hash or (a.hash == b.hash and a.doc < b.doc);
    }
};

pub fn hash(id: []const u8) u64 {
    return std.hash.Wyhash.hash(0, id);
}

pub fn sort(entries: []Entry) void {
    std.sort.pdq(Entry, entries, {}, Entry.lessThan);
}

pub fn lowerBound(entries: []const Entry, needle: u64) usize {
    var lo: usize = 0;
    var hi = entries.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (entries[mid].hash < needle) lo = mid + 1 else hi = mid;
    }
    return lo;
}

pub fn writeHeader(sink: anytype, count: u32) !void {
    var bytes: [header_size]u8 = undefined;
    bytes[0] = version;
    std.mem.writeInt(u32, bytes[1..5], count, .little);
    try sink.appendSlice(&bytes);
}

pub fn writeEntry(sink: anytype, entry: Entry) !void {
    var bytes: [entry_size]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], entry.hash, .little);
    std.mem.writeInt(u32, bytes[8..12], entry.doc, .little);
    try sink.appendSlice(&bytes);
}

pub fn readEntry(view: anytype, ordinal: u32, doc_count: u32) !Entry {
    var bytes: [entry_size]u8 = undefined;
    try view.readInto(header_size + @as(u64, ordinal) * entry_size, &bytes);
    const entry: Entry = .{ .hash = std.mem.readInt(u64, bytes[0..8], .little), .doc = std.mem.readInt(u32, bytes[8..12], .little) };
    if (entry.doc >= doc_count) return error.InvalidSegment;
    return entry;
}

/// Validate the fixed navigation header and seek without loading the corpus.
pub fn seek(view: anytype, doc_count: u32, needle: u64) !u32 {
    var header: [header_size]u8 = undefined;
    try view.readInto(0, &header);
    if (header[0] != version or std.mem.readInt(u32, header[1..5], .little) != doc_count or
        view.length != header_size + @as(u64, doc_count) * entry_size) return error.InvalidSegment;
    var lo: u32 = 0;
    var hi = doc_count;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if ((try readEntry(view, mid, doc_count)).hash < needle) lo = mid + 1 else hi = mid;
    }
    return lo;
}

/// Source-owned navigation for large tables. The fixed cache holds only
/// immutable entries: no view, source callback, allocator, or visibility state
/// escapes the loader. Each access supplies its currently admitted view.
pub const Paged = struct {
    pub const page_entries = 128;
    pub const page_count = 64;
    const Page = struct { first: u32 = 0, count: u16 = 0, touched: u64 = 0 };
    mutex: std.atomic.Mutex = .unlocked,
    doc_count: u32,
    clock: u64 = 0,
    pages: [page_count]Page = @splat(.{}),
    entries: [page_count * page_entries]Entry = undefined,

    pub fn create(a: std.mem.Allocator, view: anytype, doc_count: u32) !*Paged {
        var header: [header_size]u8 = undefined;
        try view.readInto(0, &header);
        if (header[0] != version or std.mem.readInt(u32, header[1..5], .little) != doc_count or
            view.length != header_size + @as(u64, doc_count) * entry_size) return error.InvalidSegment;
        const self = try a.create(Paged);
        self.* = .{ .doc_count = doc_count };
        return self;
    }

    fn entryLocked(self: *Paged, view: anytype, ordinal: u32) !Entry {
        if (ordinal >= self.doc_count) return error.InvalidSegment;
        const first = ordinal - ordinal % page_entries;
        var victim: usize = 0;
        for (&self.pages, 0..) |*page, slot| {
            if (page.count != 0 and page.first == first) {
                self.clock +|= 1;
                page.touched = self.clock;
                return self.entries[slot * page_entries + ordinal - first];
            }
            if (page.touched < self.pages[victim].touched) victim = slot;
        }
        var bytes: [page_entries * entry_size]u8 = undefined;
        const count = @min(page_entries, self.doc_count - first);
        try view.readInto(header_size + @as(u64, first) * entry_size, bytes[0 .. count * entry_size]);
        // Validate before publishing or overwriting a previously valid slot.
        var decoded: [page_entries]Entry = undefined;
        for (decoded[0..count], 0..) |*entry, i| {
            const raw = bytes[i * entry_size ..][0..entry_size];
            entry.* = .{ .hash = std.mem.readInt(u64, raw[0..8], .little), .doc = std.mem.readInt(u32, raw[8..12], .little) };
            if (entry.doc >= self.doc_count or (i != 0 and Entry.lessThan({}, entry.*, decoded[i - 1]))) return error.InvalidSegment;
        }
        @memcpy(self.entries[victim * page_entries ..][0..count], decoded[0..count]);
        self.clock +|= 1;
        self.pages[victim] = .{ .first = first, .count = @intCast(count), .touched = self.clock };
        return decoded[ordinal - first];
    }

    pub fn readEntry(self: *Paged, view: anytype, ordinal: u32) !Entry {
        try view.source.checkReadContext();
        const result = blk: {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            break :blk try self.entryLocked(view, ordinal);
        };
        try view.source.checkReadContext();
        return result;
    }

    pub fn seek(self: *Paged, view: anytype, needle: u64) !u32 {
        try view.source.checkReadContext();
        const result = blk: {
            @import("antfly_platform").sync.lockYielding(&self.mutex);
            defer self.mutex.unlock();
            var lo: u32 = 0;
            var hi = self.doc_count;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if ((try self.entryLocked(view, mid)).hash < needle) lo = mid + 1 else hi = mid;
            }
            break :blk lo;
        };
        try view.source.checkReadContext();
        return result;
    }
};

/// Decode only compact, caller-admitted navigation. Chunked reads authenticate
/// each range once rather than repeatedly fetching one entry per binary probe.
pub fn decode(a: std.mem.Allocator, view: anytype, doc_count: u32) ![]Entry {
    var header: [header_size]u8 = undefined;
    try view.readInto(0, &header);
    if (header[0] != version or std.mem.readInt(u32, header[1..5], .little) != doc_count or
        view.length != header_size + @as(u64, doc_count) * entry_size) return error.InvalidSegment;
    const entries = try a.alloc(Entry, doc_count);
    errdefer a.free(entries);
    var bytes: [128 * entry_size]u8 = undefined;
    var offset: usize = 0;
    while (offset < entries.len) {
        const count = @min(128, entries.len - offset);
        try view.readInto(header_size + @as(u64, offset) * entry_size, bytes[0 .. count * entry_size]);
        for (entries[offset..][0..count], 0..) |*entry, i| {
            const raw = bytes[i * entry_size ..][0..entry_size];
            entry.* = .{ .hash = std.mem.readInt(u64, raw[0..8], .little), .doc = std.mem.readInt(u32, raw[8..12], .little) };
            if (entry.doc >= doc_count or (offset + i != 0 and Entry.lessThan({}, entry.*, entries[offset + i - 1]))) return error.InvalidSegment;
        }
        offset += count;
    }
    return entries;
}
