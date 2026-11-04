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
