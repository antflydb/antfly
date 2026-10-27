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

//! Sparse, bounded discovery of live schema jobs. Pages are scheduling hints,
//! never owner execution permission. No terminal history, immutable plans,
//! table definitions or user rows are materialized to discover work.
const std = @import("std");
const local = @import("antfly_local_sources");
const reads = @import("schema_validation_read.zig");
const ledger = @import("schema_validation_ledger.zig");
const barriers = @import("schema_validation_publication.zig");
const lifecycle = @import("table_lifecycle_lock.zig");

pub const max_locks = 16;
pub const max_tasks = 8;
pub const max_wire_bytes = 16 * 1024;

pub const Cursor = union(enum) {
    /// Opaque canonical lifecycle-key order; decimal keys are NOT numeric order.
    scan: u64,
    owners: struct { lookup: reads.Lookup, ordinal: u16 },
};
pub const Query = struct {
    group: u64,
    metadata_incarnation: [32]u8,
    cursor: Cursor = .{ .scan = 0 },

    pub fn validate(self: @This()) !void {
        if (self.group == 0 or !local.metadata_incarnation.isValid(self.metadata_incarnation)) return error.InvalidGenerationPublication;
        if (self.cursor == .owners) {
            const request = self.cursor.owners;
            try (reads.OwnerReadRequest{ .lookup = request.lookup, .ordinal = request.ordinal }).validate();
            if (request.lookup.group != self.group or !std.mem.eql(u8, &request.lookup.metadata_incarnation, &self.metadata_incarnation)) return error.MetadataIncarnationMismatch;
        }
    }
};
pub const Task = struct { lookup: reads.Lookup, ordinal: u16, group_id: u64 };
pub const Page = struct {
    /// Same immutable metadata cut as the owner hints. Coordinators can
    /// advance all-owner decisions without loading a plan or owner cohort.
    head: ?barriers.Head = null,
    /// Fixed capacity also bounds decoding before any allocator admission.
    tasks: [max_tasks]?Task = @splat(null),
    len: u8 = 0,
    /// Null ends a complete pass. A job's last owner resumes at its lock key.
    next: ?Cursor = null,

    pub fn validate(self: @This(), query: Query) !void {
        try query.validate();
        if (self.len > max_tasks or (query.cursor == .owners and self.len == 0)) return error.InvalidCatalogRecord;
        if ((self.head != null) != (self.len != 0)) return error.InvalidCatalogRecord;
        if (self.head) |head| {
            try head.validate();
            if (head.metadata_group_id != query.group or !std.mem.eql(u8, &head.metadata_incarnation, &query.metadata_incarnation) or head.phase == .succeeded or head.phase == .canceled) return error.InvalidCatalogRecord;
        }
        var previous: ?Task = null;
        for (self.tasks, 0..) |slot, i| {
            if (i >= self.len) {
                if (slot != null) return error.InvalidCatalogRecord;
                continue;
            }
            const task = slot orelse return error.InvalidCatalogRecord;
            try (reads.OwnerReadRequest{ .lookup = task.lookup, .ordinal = task.ordinal }).validate();
            task.lookup.require(self.head.?) catch return error.InvalidCatalogRecord;
            if (task.ordinal >= self.head.?.owner_count) return error.InvalidCatalogRecord;
            if (task.group_id == 0 or task.lookup.group != query.group or
                !std.mem.eql(u8, &task.lookup.metadata_incarnation, &query.metadata_incarnation)) return error.InvalidCatalogRecord;
            if (previous) |prior| {
                if (!std.meta.eql(prior.lookup, task.lookup) or task.ordinal != prior.ordinal + 1) return error.InvalidCatalogRecord;
            } else if (query.cursor == .owners) {
                if (!std.meta.eql(query.cursor.owners.lookup, task.lookup) or task.ordinal != query.cursor.owners.ordinal) return error.InvalidCatalogRecord;
            } else if (task.ordinal != 0) return error.InvalidCatalogRecord;
            previous = task;
        }
        if (self.next) |cursor| {
            try (Query{ .group = query.group, .metadata_incarnation = query.metadata_incarnation, .cursor = cursor }).validate();
            if (cursor == .owners) {
                const last = previous orelse return error.InvalidCatalogRecord;
                if (!std.meta.eql(cursor.owners.lookup, last.lookup) or cursor.owners.ordinal != last.ordinal + 1) return error.InvalidCatalogRecord;
            } else {
                if (cursor.scan == 0) return error.InvalidCatalogRecord;
                if (previous) |last| if (cursor.scan != last.lookup.table) return error.InvalidCatalogRecord;
                if (query.cursor == .scan and query.cursor.scan != 0) {
                    var before: [192]u8 = undefined;
                    var after: [192]u8 = undefined;
                    if (std.mem.order(u8, try lifecycle.key(&after, query.group, cursor.scan), try lifecycle.key(&before, query.group, query.cursor.scan)) != .gt) return error.InvalidCatalogRecord;
                }
            }
        } else if (self.len != 0) return error.InvalidCatalogRecord;
    }
};

fn currentHead(txn: anytype, query: Query, table: u64, id: [16]u8) !barriers.Head {
    var key: [192]u8 = undefined;
    const head = try barriers.Head.decode(try txn.get(try (ledger.Key{ .group = query.group, .table = table, .id = id, .kind = .head }).encode(&key)));
    if (head.metadata_group_id != query.group or head.table_id != table or !std.mem.eql(u8, &head.id, &id) or
        !std.mem.eql(u8, &head.metadata_incarnation, &query.metadata_incarnation)) return error.InvalidCatalogRecord;
    if (head.phase == .succeeded or head.phase == .canceled) return error.GenerationPublicationChanged;
    const lock = (try lifecycle.read(txn, query.group, table)) orelse return error.GenerationPublicationChanged;
    if (lock.kind != .schema_validation or !std.mem.eql(u8, &lock.id, &id)) return error.GenerationPublicationChanged;
    const active = try txn.get(try (ledger.Key{ .group = query.group, .table = table, .id = @splat(0), .kind = .active }).encode(&key));
    if (!std.mem.eql(u8, active, &id)) return error.InvalidCatalogRecord;
    return head;
}

fn owners(txn: anytype, head: barriers.Head, first: u16) !Page {
    if (first >= head.owner_count) return error.GenerationPublicationChanged;
    var result: Page = .{ .head = head };
    var ordinal = first;
    while (ordinal < head.owner_count and result.len < max_tasks) : (ordinal += 1) {
        const owner = try ledger.readOwner(txn, head, ordinal);
        result.tasks[result.len] = .{ .lookup = .fromHead(head), .ordinal = ordinal, .group_id = owner.binding.fence.owner_group_id };
        result.len += 1;
    }
    result.next = if (ordinal == head.owner_count) .{ .scan = head.table_id } else .{ .owners = .{ .lookup = .fromHead(head), .ordinal = ordinal } };
    return result;
}

/// Caller pins current metadata incarnation, decoder floor and mirrored state
/// in this same read transaction, as with targeted schema history discovery.
pub fn read(txn: anytype, query: Query) !Page {
    try query.validate();
    if (query.cursor == .owners) {
        const cursor = query.cursor.owners;
        const head = currentHead(txn, query, cursor.lookup.table, cursor.lookup.id) catch |err| return switch (@as(anyerror, err)) {
            error.NotFound => error.GenerationPublicationChanged,
            else => err,
        };
        try cursor.lookup.require(head);
        const page = try owners(txn, head, cursor.ordinal);
        try page.validate(query);
        return page;
    }
    const after = query.cursor.scan;
    var prefix_buf: [192]u8 = undefined;
    var key_buf: [192]u8 = undefined;
    const prefix = try lifecycle.prefix(&prefix_buf, query.group);
    const seek = if (after == 0) prefix else try lifecycle.key(&key_buf, query.group, after);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(seek);
    if (after != 0) if (row) |item| if (std.mem.eql(u8, item.key, seek)) {
        row = try cursor.next();
    };
    var visited: usize = 0;
    var last = after;
    while (row) |item| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, prefix)) break;
        if (visited == max_locks) return .{ .next = .{ .scan = last } };
        visited += 1;
        if (try lifecycle.groupFromKey(item.key) != query.group) return error.InvalidCatalogRecord;
        const table = (try lifecycle.tableFromKey(item.key)) orelse return error.InvalidCatalogRecord;
        const lock = try lifecycle.Lock.decode(item.value);
        last = table;
        if (lock.kind != .schema_validation) continue;
        const head = currentHead(txn, query, table, lock.id) catch |err| return switch (@as(anyerror, err)) {
            error.GenerationPublicationChanged, error.NotFound => error.InvalidCatalogRecord,
            else => err,
        };
        const page = try owners(txn, head, 0);
        try page.validate(query);
        return page;
    }
    return .{};
}
