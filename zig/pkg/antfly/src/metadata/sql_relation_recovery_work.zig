// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One ordered recovery feed over public and unpublished native owners. A
//! single durable Boot cursor cannot skip a hidden image merely because it
//! has not acquired a public table catalog row yet.
const std = @import("std");
const directory = @import("sql_relation_lifetimes.zig");
const hidden = @import("sql_relation_hidden_lifetimes.zig");
const A = std.mem.Allocator;
pub const max_page = hidden.max_page;
pub const max_wire_bytes = max_page * 8192 + 1024;
pub const Entry = struct { table: directory.Entry, hidden: ?hidden.Reference = null };
pub const Page = struct { entries: []const Entry, next: ?u64 };

pub fn list(a: A, reader: anytype, group: u64, node: u64, after: ?u64, requested_limit: u16) !Page {
    if (requested_limit == 0 or requested_limit > directory.max_page) return error.InvalidSqlRelationPersistence;
    const limit = @min(requested_limit, max_page);
    const public = try directory.list(a, reader, group, .{ .node = node }, after, limit);
    defer a.free(public.entries);
    const unpublished = try hidden.listNode(a, reader, group, node, after, limit);
    defer a.free(unpublished.entries);
    var merged: std.ArrayList(Entry) = .empty;
    errdefer merged.deinit(a);
    var left: usize = 0;
    var right: usize = 0;
    while (merged.items.len < limit and (left < public.entries.len or right < unpublished.entries.len)) {
        const public_id = if (left < public.entries.len) public.entries[left].table_id else std.math.maxInt(u64);
        const hidden_id = if (right < unpublished.entries.len) unpublished.entries[right].reference.table else std.math.maxInt(u64);
        if (left < public.entries.len and right < unpublished.entries.len and public_id == hidden_id) return error.InvalidSqlRelationPersistence;
        if (left < public.entries.len and (right == unpublished.entries.len or public_id < hidden_id)) {
            try merged.append(a, .{ .table = public.entries[left] });
            left += 1;
        } else {
            const next = unpublished.entries[right];
            try merged.append(a, .{ .table = .{ .table_id = next.reference.table, .descriptor = next.descriptor, .initialized = next.initialized }, .hidden = next.reference });
            right += 1;
        }
    }
    const more = left < public.entries.len or right < unpublished.entries.len or public.next != null or unpublished.next != null;
    const continuation = if (more and merged.items.len != 0) merged.items[merged.items.len - 1].table.table_id else null;
    return .{ .entries = try merged.toOwnedSlice(a), .next = continuation };
}
