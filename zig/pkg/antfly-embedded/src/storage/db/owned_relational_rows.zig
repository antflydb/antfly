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

//! Owned, single-use relational postimages prepared without a live DB borrow.
//! The containing request retains the immutable schema and write-plan epochs.
const std = @import("std");
const types = @import("types.zig");
const mapper = @import("document_mapper.zig");
const registry = @import("schema_registry.zig");
const manager = @import("catalog/index_manager.zig");
const resources = @import("execution_resources.zig");
const codec = @import("algebraic/relational_row_codec.zig");
const Allocator = std.mem.Allocator;

pub const OwnedRows = struct {
    alloc: Allocator,
    inputs: std.heap.ArenaAllocator,
    guard: ?*resources.PreparedRowAllocator = null,
    writes: []types.BatchWrite = &.{},
    rows: []?mapper.PreparedRelationalWrite = &.{},
    complete: bool = false,

    pub fn init(a: Allocator) OwnedRows {
        return .{ .alloc = a, .inputs = .init(a) };
    }

    pub fn deinit(self: *OwnedRows) void {
        for (self.rows) |*row| if (row.*) |*value| value.deinit(self.alloc);
        self.alloc.free(self.rows);
        // Row regions use this stable allocator receiver through final commit.
        // Destroy it only after every unconsumed row has released its region.
        if (self.guard) |guard| self.alloc.destroy(guard);
        self.inputs.deinit();
        self.* = undefined;
    }

    pub fn prepare(self: *OwnedRows, io: ?std.Io, schema: registry.SchemaView, plan: manager.IndexManager.WritePlanSnapshot, request: types.BatchRequest) !void {
        if (self.guard != null or self.complete) return error.InvalidArgument;
        if (schema.storageMode() != .relational or request.transforms.len != 0 or request.writes.len > 8192 or request.deletes.len > 8192 or plan.generated_templates.len != 0) return;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        defer seen.deinit(self.alloc);
        var bytes: usize = 0;
        for (request.writes) |write| {
            if (write.key.len != 0 and write.key[0] == 0) return;
            if ((try seen.getOrPut(self.alloc, write.key)).found_existing) return;
            bytes +|= write.key.len +| write.value.len;
            for (write.json_null_fields) |name| bytes +|= name.len;
            if (bytes > 8 * 1024 * 1024) return;
        }
        for (request.deletes) |key| if (seen.contains(key)) return;
        const a = self.inputs.allocator();
        self.writes = try a.alloc(types.BatchWrite, request.writes.len);
        for (request.writes, self.writes) |write, *owned| owned.* = .{
            .key = try a.dupe(u8, write.key),
            .value = try a.dupe(u8, write.value),
            .json_null_fields = try types.cloneJsonNullFields(a, write.json_null_fields),
        };
        self.rows = try self.alloc.alloc(?mapper.PreparedRelationalWrite, self.writes.len);
        @memset(self.rows, null);
        const guard = try self.alloc.create(resources.PreparedRowAllocator);
        self.guard = guard;
        guard.* = .{ .child = self.alloc, .io = io orelse std.Options.debug_io };
        // Reuse the ordinary defaults/generated/CHECK, extraction and packing
        // pipeline. A null DB forbids provider calls and live storage reads.
        try @import("db.zig").prepareOwnedRelationalRows(guard, io, self.writes, schema, plan, @intCast(@max(0, @import("antfly_platform").time.realtimeNs())), self.rows);
        self.complete = true;
    }

    pub fn matchesWrites(self: *const OwnedRows, writes: []const types.BatchWrite) bool {
        if (!self.complete or self.writes.len != writes.len) return false;
        for (self.writes, writes, self.rows) |owned, actual, row| {
            if (row == null or !std.mem.eql(u8, owned.key, actual.key) or !std.mem.eql(u8, owned.value, actual.value) or
                owned.json_null_fields.len != actual.json_null_fields.len) return false;
            for (owned.json_null_fields, actual.json_null_fields) |lhs, rhs| if (!std.mem.eql(u8, lhs, rhs)) return false;
        }
        return true;
    }

    /// Called only after namespace/incarnation, schema contents and immutable
    /// write-plan catalogs match. Exact layout comparison permits rebinding
    /// across independently opened registries without trusting a numeric schema
    /// version. No fallible work or ownership changes occur after publication.
    pub fn take(self: *OwnedRows, writes: []const types.BatchWrite, captured: registry.SchemaView, current: registry.SchemaView, out: []?mapper.PreparedRelationalWrite) !bool {
        if (!self.matchesWrites(writes) or out.len != writes.len or !sameLayout(captured.physicalLayout(), current.physicalLayout())) return false;
        const schema = current.tableSchema().*;
        const layout = current.physicalLayout();
        for (self.rows, out) |*maybe_row, *destination| {
            var row = maybe_row.*.?;
            row.physical_layout = layout;
            row.schema_columns = schema.relational_columns;
            if (row.extracted.logical_source) |source| source.row = try codec.ordinalRowViewTrusted(row.packed_row, schema, layout);
            destination.* = row;
            maybe_row.* = null;
        }
        return true;
    }
};

fn sameLayout(lhs: *const codec.PhysicalLayout, rhs: *const codec.PhysicalLayout) bool {
    if (lhs == rhs) return true;
    inline for (.{ "schema_version", "column_count", "bitmap_len", "fixed_len", "variable_count" }) |field|
        if (@field(lhs, field) != @field(rhs, field)) return false;
    inline for (.{ "fixed_offsets", "variable_indexes", "hash_ordinals", "required_ordinals" }) |field|
        if (!std.mem.eql(u32, @field(lhs, field), @field(rhs, field))) return false;
    return true;
}
