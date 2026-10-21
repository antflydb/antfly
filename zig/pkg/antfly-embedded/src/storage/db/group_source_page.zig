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

//! Leader-selected bounded maintenance pages. Replicated apply never walks
//! replica-local derived keys. Source hashes fence stale primary afterimages.
const std = @import("std");
const reads = @import("group_source_reads.zig");
const A = std.mem.Allocator;
pub const Candidate = struct { key: []const u8, digest: [32]u8 };
pub const Page = struct {
    generation: u64,
    incarnation: u64 = 0,
    phase: enum { clear, scan, retire },
    cursor: []const u8,
    next: []const u8,
    done: bool,
    candidates: []const Candidate,
    pub fn validate(self: Page) !void {
        if (self.cursor.len > 32768 or self.next.len > 32768 or self.candidates.len > 128) return error.InvalidGroupedSourceCommand;
        for (self.candidates) |item| if (item.key.len == 0 or item.key.len > 32768) return error.InvalidGroupedSourceCommand;
    }
};
pub fn prepare(a: A, txn: anytype, generation: u64, incarnation: u64, phase: @FieldType(Page, "phase"), expected: []const u8, prefix: []const u8) !Page {
    const start = if (expected.len > 0) try reads.decode(a, expected) else prefix;
    if (!std.mem.startsWith(u8, start, prefix)) return error.GroupedSourceCorrupt;
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(start);
    if (expected.len > 0 and row != null and std.mem.eql(u8, row.?.key, start)) row = try cursor.next();
    var items: std.ArrayList(Candidate) = .empty;
    var bytes: usize = 0;
    var next = expected;
    while (row) |entry| {
        if (!std.mem.startsWith(u8, entry.key, prefix) or items.items.len == 128 or bytes >= 4 * 1024 * 1024) break;
        bytes += entry.key.len + entry.value.len;
        try items.append(a, .{ .key = try reads.encode(a, entry.key), .digest = @import("group_sources.zig").digest(entry.value) });
        next = items.items[items.items.len - 1].key;
        row = try cursor.next();
    }
    return .{ .generation = generation, .incarnation = incarnation, .phase = phase, .cursor = expected, .next = next, .done = row == null or !std.mem.startsWith(u8, row.?.key, prefix), .candidates = try items.toOwnedSlice(a) };
}
