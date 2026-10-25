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
