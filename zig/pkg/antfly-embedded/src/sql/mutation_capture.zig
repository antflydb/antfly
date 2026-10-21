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

//! One visibility cut for mutation inputs and source-aware RETURNING. Operator
//! cursors borrow their disjoint spans; only this owner closes native readers.
const std = @import("std");
const catalog = @import("catalog.zig");
const describe = @import("describe.zig");
const relation = @import("relation_binding.zig");
const Capture = @This();
const Entry = struct { plan: *const relation.Bound, offset: usize };
alloc: std.mem.Allocator,
entries: []const Entry,
read: ?catalog.StatementRead,

pub fn open(a: std.mem.Allocator, backend: catalog.Backend, bound: describe.BoundStatement, parameters: []const std.json.Value) !Capture {
    var entries: std.ArrayList(Entry) = .empty;
    errdefer entries.deinit(a);
    var scans: std.ArrayList(catalog.StatementScan) = .empty;
    defer scans.deinit(a);
    try collect(a, bound, &entries, &scans, 0);
    const owned = try entries.toOwnedSlice(a);
    errdefer a.free(owned);
    var search_arena = std.heap.ArenaAllocator.init(a);
    defer search_arena.deinit();
    const bound_scans = try @import("relation_runtime.zig").bindSearchScans(search_arena.allocator(), scans.items, parameters);
    const read = if (scans.items.len == 0) null else try (backend.vtable.open_statement orelse return error.SqlStatementSnapshotRequired)(backend.ptr, a, bound_scans);
    errdefer if (read) |value| value.close(value.ptr);
    if (read) |value| if (value.cursors.len != scans.items.len) return error.InvalidSqlBackendResponse;
    return .{ .alloc = a, .entries = owned, .read = read };
}

pub fn release(self: *Capture) void {
    if (self.read) |value| value.close(value.ptr);
    self.read = null;
}

pub fn deinit(self: *Capture) void {
    self.release();
    self.alloc.free(self.entries);
}

pub fn cursors(self: *const Capture, plan: *const relation.Bound) ![]const catalog.Cursor {
    for (self.entries) |entry| if (entry.plan == plan) {
        if (plan.scans.len == 0) return &.{};
        const read = self.read orelse return error.InvalidSqlBackendResponse;
        return read.cursors[entry.offset..][0..plan.scans.len];
    };
    return error.InvalidSqlBackendResponse;
}

fn collect(a: std.mem.Allocator, bound: describe.BoundStatement, entries: *std.ArrayList(Entry), scans: *std.ArrayList(catalog.StatementScan), depth: usize) anyerror!void {
    if (depth >= 64) return error.SqlProgramLimitExceeded;
    if (bound.relation) |plan| {
        for (entries.items) |entry| if (entry.plan == plan) return;
        if (entries.items.len >= 64 or plan.scans.len > 64 - scans.items.len) return error.SqlProgramLimitExceeded;
        try entries.append(a, .{ .plan = plan, .offset = scans.items.len });
        try scans.appendSlice(a, plan.scans);
    }
    if (bound.window) |window| try collect(a, window.input.*, entries, scans, depth + 1);
    if (bound.joined_mutation) |mutation| try collect(a, mutation.input.*, entries, scans, depth + 1);
    if (bound.merge_mutation) |mutation| try collect(a, mutation.input.*, entries, scans, depth + 1);
    if (bound.insert_source) |source| try collect(a, source.*, entries, scans, depth + 1);
    if (bound.returning) |output| try collect(a, output.*, entries, scans, depth + 1);
}
