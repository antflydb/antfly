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
const Allocator = std.mem.Allocator;

pub const Entry = struct {
    doc_id: []u8,
    body: []u8,
    last_lsn: u64,
    last_timestamp_ns: u64,

    pub fn deinit(self: *Entry, alloc: Allocator) void {
        alloc.free(self.doc_id);
        alloc.free(self.body);
        self.* = undefined;
    }
};

/// Lightweight document identity and body location decoded from the v2
/// segment directory. Query paths can enumerate exact document identities
/// without retaining stored document bodies.
pub const IndexEntry = struct {
    doc_id: []u8,
    body_offset: u64,
    body_len: u32,
    last_lsn: u64,
    last_timestamp_ns: u64,

    pub fn deinit(self: *IndexEntry, alloc: Allocator) void {
        alloc.free(self.doc_id);
        self.* = undefined;
    }
};

pub fn freeIndexEntries(alloc: Allocator, entries: []IndexEntry) void {
    for (entries) |*entry| entry.deinit(alloc);
    alloc.free(entries);
}

pub fn freeEntries(alloc: Allocator, entries: []Entry) void {
    for (entries) |*entry| entry.deinit(alloc);
    alloc.free(entries);
}

test "freeEntries releases owned published document entries" {
    const alloc = std.testing.allocator;
    const entries = try alloc.alloc(Entry, 1);
    entries[0] = .{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .body = try alloc.dupe(u8, "alpha"),
        .last_lsn = 1,
        .last_timestamp_ns = 10,
    };
    freeEntries(alloc, entries);
}

test "freeIndexEntries releases owned document identities" {
    const alloc = std.testing.allocator;
    const entries = try alloc.alloc(IndexEntry, 1);
    entries[0] = .{
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .body_offset = 128,
        .body_len = 5,
        .last_lsn = 1,
        .last_timestamp_ns = 10,
    };
    freeIndexEntries(alloc, entries);
}
