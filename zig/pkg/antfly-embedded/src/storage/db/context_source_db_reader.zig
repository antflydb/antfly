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

//! Context-source reads served by a resident DB.
//!
//! `ownTable` serves the DB's own table identity and refuses every other one
//! (`ContextSourceTableUnavailable`): a single DB has no authority to read
//! foreign tables. Hosts with a table registry compose `Registry`, which maps
//! table identities to resident DBs (embedded Lite) or install their own
//! routing reader (the distributed server's `TableReadSource`).
//!
//! Reads use the DB's public read path, so TTL visibility and row policies
//! apply exactly as for any other reader of the table.

const std = @import("std");
const Allocator = std.mem.Allocator;
const context_resolution = @import("enrichment/context_resolution.zig");

pub fn ownTable(comptime DB: type, db: *DB, own_table_id: []const u8) context_resolution.Reader {
    const Impl = Adapter(DB);
    return .{
        .ptr = db,
        .own_table_id = own_table_id,
        .lookup_fn = Impl.lookupOwn,
        .query_fn = Impl.queryOwn,
        .frontier_fn = Impl.frontierOwn,
    };
}

pub fn Adapter(comptime DB: type) type {
    return struct {
        fn lookupOwn(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8) anyerror!?[]u8 {
            const db: *DB = @ptrCast(@alignCast(ptr));
            if (!isOwn(db, table_id)) return error.ContextSourceTableUnavailable;
            return lookup(db, alloc, key);
        }

        fn queryOwn(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, filter: []const u8, offset: u32, limit: u32) anyerror!context_resolution.Page {
            const db: *DB = @ptrCast(@alignCast(ptr));
            if (!isOwn(db, table_id)) return error.ContextSourceTableUnavailable;
            return query(db, alloc, filter, offset, limit);
        }

        fn frontierOwn(ptr: *anyopaque, alloc: Allocator, table_id: []const u8) anyerror![]u8 {
            const db: *DB = @ptrCast(@alignCast(ptr));
            if (!isOwn(db, table_id)) return error.ContextSourceTableUnavailable;
            return frontier(db, alloc);
        }

        fn isOwn(db: *DB, table_id: []const u8) bool {
            if (table_id.len == 0) return true;
            return std.mem.eql(u8, table_id, db.contextSourceTableId());
        }

        pub fn lookup(db: *DB, alloc: Allocator, key: []const u8) !?[]u8 {
            return db.get(alloc, key);
        }

        pub fn query(db: *DB, alloc: Allocator, filter: []const u8, offset: u32, limit: u32) !context_resolution.Page {
            var result = try db.search(alloc, .{
                .filter_query_json = filter,
                .limit = limit,
                .offset = offset,
                .include_stored = true,
                .include_all_fields = true,
            });
            defer result.deinit();
            var rows = std.ArrayListUnmanaged(context_resolution.Row).empty;
            errdefer {
                for (rows.items) |row| {
                    alloc.free(row.key);
                    alloc.free(row.value);
                }
                rows.deinit(alloc);
            }
            for (result.hits) |hit| {
                const value = if (hit.stored_data) |data| try alloc.dupe(u8, data) else (try db.get(alloc, hit.id)) orelse continue;
                errdefer alloc.free(value);
                try rows.append(alloc, .{ .key = try alloc.dupe(u8, hit.id), .value = value });
            }
            const exhausted = result.hits.len < limit or
                (result.total_hits_relation == .exact and @as(u64, offset) + result.hits.len >= result.total_hits);
            return .{ .rows = try rows.toOwnedSlice(alloc), .exhausted = exhausted };
        }

        pub fn frontier(db: *DB, alloc: Allocator) ![]u8 {
            return std.fmt.allocPrint(alloc, "seq:{d}", .{db.core.nextDerivedSequence()});
        }
    };
}

/// Embedded multi-table reader: resolves table identities to resident DBs.
/// The registry callback must return a DB that stays open for the read; Lite
/// table handles outlive the enrichment workers of every table they host.
pub fn Registry(comptime DB: type) type {
    return struct {
        ptr: *anyopaque,
        own: *DB,
        own_table_id: []const u8,
        resolve_fn: *const fn (ptr: *anyopaque, table_id: []const u8) ?*DB,

        const Self = @This();
        const Impl = Adapter(DB);

        pub fn reader(self: *Self) context_resolution.Reader {
            return .{ .ptr = self, .own_table_id = self.own_table_id, .lookup_fn = lookupImpl, .query_fn = queryImpl, .frontier_fn = frontierImpl };
        }

        fn target(self: *Self, table_id: []const u8) !*DB {
            if (table_id.len == 0 or std.mem.eql(u8, table_id, self.own_table_id)) return self.own;
            return self.resolve_fn(self.ptr, table_id) orelse error.ContextSourceTableUnavailable;
        }

        fn lookupImpl(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8) anyerror!?[]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return Impl.lookup(try self.target(table_id), alloc, key);
        }

        fn queryImpl(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, filter: []const u8, offset: u32, limit: u32) anyerror!context_resolution.Page {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return Impl.query(try self.target(table_id), alloc, filter, offset, limit);
        }

        fn frontierImpl(ptr: *anyopaque, alloc: Allocator, table_id: []const u8) anyerror![]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return Impl.frontier(try self.target(table_id), alloc);
        }
    };
}
