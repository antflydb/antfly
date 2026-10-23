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
