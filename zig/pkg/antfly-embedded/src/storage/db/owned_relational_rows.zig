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

pub var test_prepare_calls: if (@import("builtin").is_test) std.atomic.Value(u64) else void = if (@import("builtin").is_test) .init(0) else {};

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
        if (comptime @import("builtin").is_test) _ = test_prepare_calls.fetchAdd(1, .monotonic);
        try @import("db.zig").prepareOwnedRelationalRows(guard, io, self.writes, schema, plan, @intCast(@max(0, @import("antfly_platform").time.realtimeNs())), self.rows);
        self.complete = true;
    }

    /// Render SQL's normalized postimage from the very same packed authority
    /// that commit will consume. Updating the exact byte witness is permitted
    /// only here, from that authority; arbitrary caller postimages still fail
    /// matchesWrites and must be prepared independently.
    pub fn normalize(self: *OwnedRows, schema: registry.SchemaView, output: Allocator) !?[]types.BatchWrite {
        if (!self.complete) return null;
        const normalized = try output.alloc(types.BatchWrite, self.writes.len);
        var initialized: usize = 0;
        errdefer {
            for (normalized[0..initialized]) |write| {
                output.free(write.key);
                output.free(write.value);
                for (write.json_null_fields) |name| output.free(name);
                if (write.json_null_fields.len != 0) output.free(write.json_null_fields);
            }
            output.free(normalized);
        }
        const retained = self.inputs.allocator();
        var output_bytes: usize = 0;
        for (self.writes, self.rows, normalized) |*write, maybe_row, *out| {
            const row = &(maybe_row orelse return error.InvalidArgument);
            const json = (try row.extracted.logicalJson()) orelse return error.InvalidArgument;
            output_bytes = std.math.add(usize, output_bytes, json.len +| write.key.len) catch return error.RelationalRowResultTooLarge;
            if (output_bytes > 16 * 1024 * 1024) return error.RelationalRowResultTooLarge;
            const typed = try row.typedView(schema.tableSchema().*, schema.physicalLayout());
            var names: std.ArrayListUnmanaged([]const u8) = .empty;
            defer names.deinit(output);
            for (write.json_null_fields) |name| {
                const ordinal = typed.ordinalForName(name) orelse return error.InvalidBatchRequest;
                const cell = (try typed.findCell(ordinal)) orelse continue;
                if (!cell.is_null) try names.append(output, name);
            }
            out.* = blk: {
                const key = try output.dupe(u8, write.key);
                errdefer output.free(key);
                const value = try output.dupe(u8, json);
                errdefer output.free(value);
                const nulls = try types.cloneJsonNullFields(output, names.items);
                break :blk .{ .key = key, .value = value, .json_null_fields = nulls };
            };
            // Transfer the completed result before fallible witness copies.
            initialized += 1;
            write.value = try retained.dupe(u8, out.value);
            write.json_null_fields = try types.cloneJsonNullFields(retained, out.json_null_fields);
        }
        return normalized;
    }

    pub fn matchesWrites(self: *const OwnedRows, writes: []const types.BatchWrite) bool {
        if (!self.matchesPostimages(writes)) return false;
        for (self.rows) |row| if (row == null) return false;
        return true;
    }

    /// The exact postimage witness remains valid after packed-row ownership
    /// transfers to commit. It does not grant another transfer of those rows.
    pub fn matchesPostimages(self: *const OwnedRows, writes: []const types.BatchWrite) bool {
        if (!self.complete or self.writes.len != writes.len) return false;
        for (self.writes, writes) |owned, actual| {
            if (!std.mem.eql(u8, owned.key, actual.key) or !std.mem.eql(u8, owned.value, actual.value) or
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
