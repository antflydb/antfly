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
const api_types = @import("../api/types.zig");

pub const Entry = struct {
    lsn: u64,
    timestamp_ns: u64,
    kind: api_types.MutationKind,
    doc_id: []u8,
    body: ?[]u8 = null,

    pub fn deinit(self: *Entry, alloc: Allocator) void {
        alloc.free(self.doc_id);
        if (self.body) |body| alloc.free(body);
        self.* = undefined;
    }
};

pub fn freeEntries(alloc: Allocator, entries: []Entry) void {
    for (entries) |*entry| entry.deinit(alloc);
    alloc.free(entries);
}

test "freeEntries releases owned mutation segment entries" {
    const alloc = std.testing.allocator;
    const entries = try alloc.alloc(Entry, 1);
    entries[0] = .{
        .lsn = 1,
        .timestamp_ns = 2,
        .kind = .upsert,
        .doc_id = try alloc.dupe(u8, "doc-a"),
        .body = try alloc.dupe(u8, "alpha"),
    };
    freeEntries(alloc, entries);
}
