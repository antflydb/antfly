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

//! Physical query-lease ownership. This private layout is source-backend only.
const std = @import("std");
const Types = @import("read_session_types").types;
const Source = @import("read_session_source_types").types;
const types = Types.types;
pub fn For(comptime DB: type, comptime SearchResult: type) type {
    return struct {
        const QueryReadLease = @This();
        pub const Result = SearchResult;
        pub const Protocol = Types.query_lease.Protocol(SearchResult);
        db: *DB,
        row_policy_lease: Source.row_policy_gate.Gate.Lease,

        /// Capture a primary typed reader under this lease's existing apply
        /// fence. Do not recursively acquire the fence behind a queued writer.
        pub fn relationalRows(self: *const QueryReadLease, alloc: std.mem.Allocator, fields: []const []const u8, schema_version: u32) !DB.RelationalRows.Reader {
            var view = self.db.core.acquireSchemaView() orelse return error.RelationalTableRequired;
            defer view.release();
            var reader = try DB.RelationalRows.Reader.open(alloc, self.db.core.store, view, null, .{
                .fields = fields,
                .expected_schema_version = schema_version,
            }, DB.queryLeaseClockNowNs());
            reader.row_policy_lease = self.row_policy_lease.clone();
            return reader;
        }

        pub fn documentRows(self: *const QueryReadLease, alloc: std.mem.Allocator, schema_version: u32) !DB.DocumentRows {
            var view = self.db.core.acquireSchemaView() orelse return error.PreparedGenerationChanged;
            errdefer view.release();
            if (view.storageMode() != .document or view.version() != schema_version) return error.PreparedGenerationChanged;
            var read = try self.db.core.store.beginReadTxn();
            errdefer read.abort();
            return .{ .db = self.db, .read = read, .schema = view, .policy = self.row_policy_lease.clone(), .window = .init(alloc), .directory_arena = .init(Source.platform.allocator.reclaimingScratch(alloc)), .metadata = .init(Source.platform.allocator.reclaimingScratch(alloc)) };
        }

        pub fn search(self: QueryReadLease, alloc: std.mem.Allocator, req: types.SearchRequest) !SearchResult {
            return self.db.queryLeaseSearchOwned(alloc, req);
        }

        pub fn release(self: *QueryReadLease) void {
            const db = self.db;
            db.core.unlockApplyShared();
            if (db.async_context.resource_manager) |manager| manager.finishForegroundQuery();
            self.row_policy_lease.release();
            self.* = undefined;
        }
    };
}
