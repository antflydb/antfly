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

//! SQL consumes the exact immutable document/graph cut retained by its host.
const std = @import("std");
const types = @import("types.zig");
const View = @import("../relational_read_view.zig").View;
pub fn open(comptime Snapshot: type, comptime DB: type, a: std.mem.Allocator, snapshot: *Snapshot, db: *DB, from: []const u8, to: []const u8, opts: types.ScanOptions) !View {
    const Cursor = struct {
        const Self = @This();
        a: std.mem.Allocator,
        snapshot: *Snapshot,
        session: union(enum) { document: *DB.DocumentReadSession, relational: *DB.RelationalReadSession },
        fn cast(ptr: *anyopaque) *Self {
            return @ptrCast(@alignCast(ptr));
        }
        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !View.Page {
            const self = cast(ptr);
            try self.snapshot.sqlCheckpoint();
            var result = switch (self.session) {
                .document => |session| try session.next(alloc, limit),
                .relational => |session| blk: {
                    var page = try session.nextTypedPage(alloc, null, .{ .rows = limit, .output_bytes = 16 * 1024 * 1024 });
                    errdefer page.deinit();
                    const owned = page.arena.allocator();
                    const rows = try owned.alloc(View.Row, page.rows.len);
                    for (page.rows, rows) |row, *out| out.* = .{ .id = row.key, .version = row.version, .schema_version = session.reader.active.version(), .value = row.typed orelse return error.InvalidResponse, .sql_nulls = row.sql_nulls, .expected_content_digest = row.expected_content_digest };
                    const after = if (page.more) try owned.dupe(u8, session.reader.after.items) else null;
                    break :blk View.Page{ .arena = page.arena, .rows = rows, .after = after };
                },
            };
            errdefer result.deinit();
            try self.snapshot.sqlCheckpoint();
            return result;
        }
        fn rangeProofs(ptr: *anyopaque, alloc: std.mem.Allocator) ![]@import("../range_protection.zig").Proof {
            const self = cast(ptr);
            try self.snapshot.sqlCheckpoint();
            return switch (self.session) {
                .document => |session| session.rangeProofs(alloc),
                .relational => |session| session.rangeProofs(alloc),
            };
        }
        fn close(ptr: *anyopaque) void {
            const self = cast(ptr);
            switch (self.session) {
                .document => |session| session.deinit(),
                .relational => |session| session.deinit(),
            }
            self.snapshot.releaseSql();
            self.a.destroy(self);
        }
    };
    const cursor = try a.create(Cursor);
    errdefer a.destroy(cursor);
    var schema = db.core.acquireSchemaView();
    defer if (schema) |*view| view.release();
    cursor.* = .{ .a = a, .snapshot = snapshot, .session = if (schema == null or schema.?.storageMode() == .document) .{ .document = try db.openDocumentReadSession(a, from, to, opts) } else .{ .relational = try db.openRelationalReadSession(a, from, to, opts) } };
    return .{ .ptr = cursor, .vtable = &.{ .next = Cursor.next, .range_proofs = Cursor.rangeProofs, .close = Cursor.close } };
}
