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

//! Storage-owned statement snapshot capability shared with the API coordinator.
const std = @import("std");
const callbacks = @import("../runtime_callback_abi.zig");
const types = @import("db/types.zig");

pub const View = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: Abi.Dispatch = Abi.local_dispatch,

    pub const Row = struct {
        id: []const u8,
        version: u64,
        schema_version: u32,
        value: std.json.Value,
        /// Complete native SQL cells, owned by the page arena.
        typed_cells: ?@import("../sql/catalog.zig").Row.TypedCells = null,
        expected_content_digest: ?[32]u8 = null,
        document: ?std.json.Value = null,
        /// SQL NULL flags aligned with value.object insertion order. A JSON
        /// payload containing null has a false flag.
        sql_nulls: ?[]const bool = null,

        /// Borrowed SQL view; payload lifetime is exactly the owning page.
        /// Native and legacy providers share the same cell/null accessor.
        pub fn sqlRow(self: Row) @import("../sql/catalog.zig").Row {
            return .{ .id = self.id, .version = self.version, .value = self.value, .typed_cells = self.typed_cells, .sql_nulls = self.sql_nulls, .expected_content_digest = self.expected_content_digest, .document = self.document };
        }

        pub fn cell(self: Row, name: []const u8) !@import("../sql/catalog.zig").Row.Cell {
            return self.sqlRow().cell(name);
        }
    };
    pub const Page = struct {
        arena: std.heap.ArenaAllocator,
        rows: []const Row,
        after: ?[]const u8,

        /// Leases pin immutable payloads through coordinated merges. The
        /// original page and every output reference release independently.
        lease: ?*Lease = null,
        retained_pages: []Page = &.{},
        columnar_blocks: []const @import("../sql/spill.zig").ColumnarBlock = &.{},
        const Lease = struct { alloc: std.mem.Allocator, refs: std.atomic.Value(usize), page: Page };
        pub fn retain(self: *Page, alloc: std.mem.Allocator) !Page {
            const lease = self.lease orelse blk: {
                const owner = try alloc.create(Lease);
                owner.* = .{ .alloc = alloc, .refs = .init(1), .page = self.* };
                self.lease = owner;
                break :blk owner;
            };
            _ = lease.refs.fetchAdd(1, .monotonic);
            return self.*;
        }
        pub fn deinit(self: *Page) void {
            if (self.lease) |lease| {
                if (lease.refs.fetchSub(1, .acq_rel) == 1) {
                    lease.page.deinit();
                    lease.alloc.destroy(lease);
                }
            } else {
                for (self.retained_pages) |*page| page.deinit();
                for (self.columnar_blocks) |block| block.deinit();
                self.arena.deinit();
            }
            self.* = undefined;
        }
    };
    pub const Admission = @import("../common/read_admission.zig").Port;
    pub const VTable = struct {
        range_proofs: ?*const fn (*anyopaque, std.mem.Allocator) anyerror![]@import("range_protection.zig").Proof = null,
        next: *const fn (*anyopaque, std.mem.Allocator, u32) anyerror!Page,
        normalize: ?*const fn (*anyopaque, std.mem.Allocator, []const types.BatchWrite) anyerror![]types.BatchWrite = null,
        close: *const fn (*anyopaque) void,
        /// Borrowed for one page call only; never retained by the cursor.
        next_with_admission: ?*const fn (*anyopaque, std.mem.Allocator, u32, Admission) anyerror!Page = null,
        range_proofs_with_admission: ?*const fn (*anyopaque, std.mem.Allocator, Admission) anyerror![]@import("range_protection.zig").Proof = null,
        normalize_with_admission: ?*const fn (*anyopaque, std.mem.Allocator, []const types.BatchWrite, Admission) anyerror![]types.BatchWrite = null,
    };
    const Abi = callbacks.Boundary(VTable);

    pub fn next(self: View, alloc: std.mem.Allocator, limit: u32) !Page {
        return Abi.call("next", self.boundary_dispatch, self.vtable.next, .{ self.ptr, alloc, limit });
    }
    pub fn nextWithAdmission(self: View, alloc: std.mem.Allocator, limit: u32, control: Admission) !Page {
        try control.checkpoint();
        var page = if (self.vtable.next_with_admission) |admitted_next|
            try Abi.call("next_with_admission", self.boundary_dispatch, admitted_next, .{ self.ptr, alloc, limit, control })
        else
            try self.next(alloc, limit);
        errdefer page.deinit();
        try control.checkpoint();
        return page;
    }
    pub fn rangeProofs(self: View, alloc: std.mem.Allocator) ![]@import("range_protection.zig").Proof {
        const callback = self.vtable.range_proofs orelse return error.SqlRangeTrackingRequired;
        return Abi.call("range_proofs", self.boundary_dispatch, callback, .{ self.ptr, alloc });
    }
    pub fn normalize(self: View, alloc: std.mem.Allocator, writes: []const types.BatchWrite) ![]types.BatchWrite {
        const callback = self.vtable.normalize orelse return error.UnsupportedSqlExecution;
        return Abi.call("normalize", self.boundary_dispatch, callback, .{ self.ptr, alloc, writes });
    }
    pub fn rangeProofsWithAdmission(self: View, alloc: std.mem.Allocator, control: Admission) ![]@import("range_protection.zig").Proof {
        try control.checkpoint();
        const result = if (self.vtable.range_proofs_with_admission) |callback|
            try Abi.call("range_proofs_with_admission", self.boundary_dispatch, callback, .{ self.ptr, alloc, control })
        else
            try self.rangeProofs(alloc);
        errdefer alloc.free(result);
        try control.checkpoint();
        return result;
    }
    pub fn normalizeWithAdmission(self: View, alloc: std.mem.Allocator, writes: []const types.BatchWrite, control: Admission) ![]types.BatchWrite {
        try control.checkpoint();
        const result = if (self.vtable.normalize_with_admission) |callback|
            try Abi.call("normalize_with_admission", self.boundary_dispatch, callback, .{ self.ptr, alloc, writes, control })
        else
            try self.normalize(alloc, writes);
        errdefer types.freeNormalizedWrites(alloc, result);
        try control.checkpoint();
        return result;
    }
    pub fn deinit(self: View) void {
        Abi.call("close", self.boundary_dispatch, self.vtable.close, .{self.ptr}) catch unreachable;
    }
};
