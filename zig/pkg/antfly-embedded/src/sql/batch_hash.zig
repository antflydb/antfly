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

//! Column-major semantic hashes shared by join, grouping and scan filters.
//! Hashes never substitute for exact equality. Dictionary/repeated text is
//! hashed once per column; SQL-null treatment differs for joins and grouping.
const std = @import("std");
const scalar = @import("scalar.zig");
const A = std.mem.Allocator;
const Work = @import("operator_work.zig");

// Controlled text lookup never invokes a byte-length hash/equality callback
// inside a mutable hash-map operation. Rehashing touches only fixed-width keys.
const TextMemo = struct {
    legacy: std.StringHashMapUnmanaged(u64) = .empty,
    heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    entries: std.ArrayList(Entry) = .empty,
    const Entry = struct { text: []const u8, semantic: u64, next: ?usize };
    fn deinit(self: *TextMemo, a: A) void {
        self.legacy.deinit(a);
        self.heads.deinit(a);
        self.entries.deinit(a);
    }
    fn getOrHash(self: *TextMemo, a: A, cell: scalar.Datum, control: ?Work.Control) !u64 {
        if (control == null) {
            if (self.legacy.get(cell.value.string)) |cached| return cached;
            const semantic = try scalar.semanticHashDatum(cell);
            if (self.legacy.count() < 4096) try self.legacy.put(a, cell.value.string, semantic);
            return semantic;
        }
        const hash = try Work.hashBytes(control, 0, cell.value.string);
        var slot = self.heads.get(hash);
        while (slot) |index| {
            const entry = self.entries.items[index];
            if (try Work.equalBytes(control, entry.text, cell.value.string)) return entry.semantic;
            slot = entry.next;
        }
        const semantic = try scalar.semanticHashDatumControlled(cell, control);
        if (self.entries.items.len < 4096) {
            try self.entries.ensureUnusedCapacity(a, 1);
            try self.heads.ensureUnusedCapacity(a, 1);
            const index = self.entries.items.len;
            self.entries.appendAssumeCapacity(.{ .text = cell.value.string, .semantic = semantic, .next = self.heads.get(hash) });
            self.heads.putAssumeCapacity(hash, index);
        }
        return semantic;
    }
};
pub fn encodedColumns(a: A, values: []const @import("execution_batch.zig").Batch, count: usize, grouped: bool) ![]?u64 {
    return encodedColumnsControlled(a, values, count, grouped, null);
}
pub fn encodedColumnsControlled(a: A, values: []const @import("execution_batch.zig").Batch, count: usize, grouped: bool, control: ?Work.Control) ![]?u64 {
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const physical = try scratch.allocator().alloc(?@import("execution_batch.zig").PhysicalColumn, values.len);
    for (values, physical) |column, *view| {
        view.* = try column.physicalColumn(scratch.allocator(), 0);
        if (view.*) |native| try native.validate();
    }
    return encodedColumnsWithPhysicalControlled(a, values, physical, count, grouped, control);
}
pub fn encodedColumnsWithPhysical(a: A, values: []const @import("execution_batch.zig").Batch, physical: []const ?@import("execution_batch.zig").PhysicalColumn, count: usize, grouped: bool) ![]?u64 {
    return encodedColumnsWithPhysicalControlled(a, values, physical, count, grouped, null);
}
pub fn encodedColumnsWithPhysicalControlled(a: A, values: []const @import("execution_batch.zig").Batch, physical: []const ?@import("execution_batch.zig").PhysicalColumn, count: usize, grouped: bool, control: ?Work.Control) ![]?u64 {
    if (physical.len != values.len) return error.InvalidSqlBackendResponse;
    const states = try a.alloc(std.hash.Wyhash, count);
    defer a.free(states);
    for (states) |*state| state.* = .init(0);
    const result = try a.alloc(?u64, count);
    errdefer a.free(result);
    @memset(result, 0);
    for (values, physical) |column, native| {
        if (column.len() != count or column.width() != 1) return error.InvalidSqlBackendResponse;
        var memo: std.AutoHashMapUnmanaged(u64, u64) = .empty;
        defer memo.deinit(a);
        var text: TextMemo = .{};
        defer text.deinit(a);
        var numeric_scratch = std.heap.ArenaAllocator.init(a);
        defer numeric_scratch.deinit();
        const decimal = try column.decimalColumnSelected(numeric_scratch.allocator(), 0, null);
        for (states, result, 0..) |*state, *valid, index| {
            try Work.poll(control);
            var fixed: @import("numeric_value.zig").ScaledInteger = undefined;
            var number: @import("numeric_value.zig").Value = undefined;
            const cell = if (native) |view| try view.cell(index) else if (decimal) |view| try view.cell(index, &fixed, &number) else try column.cellControlled(a, index, 0, control);
            if (!grouped and cell.sql_null) valid.* = null;
            if (valid.* == null) continue;
            const semantic = if (cell.sql_null) 0 else if (if (native) |view| view.dictionaryIdentity(index) else try column.dictionaryIdentity(index, 0)) |id| blk: {
                if (memo.get(id)) |hash| break :blk hash;
                const hash = try scalar.semanticHashDatumControlled(cell, control);
                if (memo.count() < 4096) try memo.put(a, id, hash);
                break :blk hash;
            } else if (cell.value == .string)
                try text.getOrHash(a, cell, control)
            else
                try scalar.semanticHashDatumControlled(cell, control);
            var bytes: [9]u8 = undefined;
            bytes[0] = @intFromBool(cell.sql_null);
            std.mem.writeInt(u64, bytes[1..9], semantic, .little);
            state.update(if (grouped) &bytes else bytes[1..9]);
        }
    }
    for (states, result) |*state, *out| if (out.* != null) {
        out.* = state.final();
    };
    return result;
}
pub fn columns(a: A, values: []const []const scalar.Datum, count: usize, grouped: bool) ![]?u64 {
    return columnsControlled(a, values, count, grouped, null);
}
pub fn columnsControlled(a: A, values: []const []const scalar.Datum, count: usize, grouped: bool, control: ?Work.Control) ![]?u64 {
    const hashes = try a.alloc(std.hash.Wyhash, count);
    defer a.free(hashes);
    for (hashes) |*h| h.* = .init(0);
    const result = try a.alloc(?u64, count);
    errdefer a.free(result);
    @memset(result, 0);
    for (values) |column| {
        if (column.len != count) return error.InvalidSqlBackendResponse;
        var text: TextMemo = .{};
        defer text.deinit(a);
        for (column, hashes, result) |cell, *hash, *valid| {
            try Work.poll(control);
            if (!grouped and cell.sql_null) valid.* = null;
            if (valid.* == null) continue;
            const semantic = if (cell.sql_null) 0 else if (cell.value == .string)
                try text.getOrHash(a, cell, control)
            else
                try scalar.semanticHashDatumControlled(cell, control);
            var bytes: [9]u8 = undefined;
            bytes[0] = @intFromBool(cell.sql_null);
            std.mem.writeInt(u64, bytes[1..9], semantic, .little);
            hash.update(if (grouped) &bytes else bytes[1..9]);
        }
    }
    for (hashes, result) |*hash, *out| if (out.* != null) {
        out.* = hash.final();
    };
    return result;
}
pub fn rows(a: A, values: []const []const scalar.Datum, grouped: bool) ![]?u64 {
    return rowsControlled(a, values, grouped, null);
}
pub fn rowsControlled(a: A, values: []const []const scalar.Datum, grouped: bool, control: ?Work.Control) ![]?u64 {
    const width = if (values.len == 0) 0 else values[0].len;
    for (values) |row| if (row.len != width) return error.InvalidSqlBackendResponse;
    const states = try a.alloc(std.hash.Wyhash, values.len);
    defer a.free(states);
    for (states) |*state| state.* = .init(0);
    const result = try a.alloc(?u64, values.len);
    errdefer a.free(result);
    @memset(result, 0);
    // Work directly on the row views: no width*rows Datum transpose.
    for (0..width) |column| {
        var text: TextMemo = .{};
        defer text.deinit(a);
        for (values, states, result) |row, *state, *valid| {
            try Work.poll(control);
            const cell = row[column];
            if (!grouped and cell.sql_null) valid.* = null;
            if (valid.* == null) continue;
            const semantic = if (cell.sql_null) 0 else if (cell.value == .string)
                try text.getOrHash(a, cell, control)
            else
                try scalar.semanticHashDatumControlled(cell, control);
            var bytes: [9]u8 = undefined;
            bytes[0] = @intFromBool(cell.sql_null);
            std.mem.writeInt(u64, bytes[1..9], semantic, .little);
            state.update(if (grouped) &bytes else bytes[1..9]);
        }
    }
    for (states, result) |*state, *out| if (out.* != null) {
        out.* = state.final();
    };
    return result;
}

/// One physical optional grouping accelerator. Payloads remain borrowed from
/// the query's input owner; all IDs/hash state/results/memo metadata live here.
pub const GroupedScratch = struct {
    pub const alignment = std.mem.Alignment.fromByteUnits(@max(@alignOf(usize), @max(@alignOf(std.hash.Wyhash), @alignOf(Memo))));
    const Kind = enum { empty, text, dictionary };
    const Memo = struct { key: u64, semantic: u64, text: []const u8, generation: u32, kind: Kind };
    const Layout = struct { states: usize, result: usize, memo: usize, memo_count: usize, bytes: usize };
    ids: []usize,
    states: []std.hash.Wyhash,
    result: []?u64,
    memo: []Memo,
    generation: u32 = 0,

    fn layout(count: usize, provided_hashes: bool) ?Layout {
        // Native execution batches already have this bound. Larger internal
        // callers retain the serial path instead of growing an accelerator.
        if (count > 4096) return null;
        var offset = count * @sizeOf(usize);
        if (provided_hashes) {
            offset = std.mem.alignForward(usize, offset, alignment.toByteUnits());
            return .{ .states = offset, .result = offset, .memo = offset, .memo_count = 0, .bytes = offset };
        }
        offset = std.mem.alignForward(usize, offset, @alignOf(std.hash.Wyhash));
        const states = offset;
        offset += count * @sizeOf(std.hash.Wyhash);
        offset = std.mem.alignForward(usize, offset, @alignOf(?u64));
        const result = offset;
        offset += count * @sizeOf(?u64);
        offset = std.mem.alignForward(usize, offset, @alignOf(Memo));
        const memo = offset;
        const memo_count = std.math.ceilPowerOfTwo(usize, @max(@as(usize, 8), count * 2)) catch return null;
        offset += memo_count * @sizeOf(Memo);
        return .{ .states = states, .result = result, .memo = memo, .memo_count = memo_count, .bytes = offset };
    }
    pub fn requiredBytes(count: usize, provided_hashes: bool) ?usize {
        return (layout(count, provided_hashes) orelse return null).bytes;
    }
    fn slice(comptime T: type, buffer: []u8, at: usize, count: usize) []T {
        return @alignCast(std.mem.bytesAsSlice(T, buffer[at..][0 .. count * @sizeOf(T)]));
    }
    /// Construction only creates views. Initialize/poll after the enclosing
    /// owner has installed its error cleanup for the live physical buffer.
    pub fn init(buffer: []u8, count: usize, provided_hashes: bool) !GroupedScratch {
        const plan = layout(count, provided_hashes) orelse return error.InvalidSqlLimit;
        if (buffer.len != plan.bytes or @intFromPtr(buffer.ptr) % alignment.toByteUnits() != 0) return error.InvalidSqlBackendResponse;
        return .{
            .ids = slice(usize, buffer, 0, count),
            .states = slice(std.hash.Wyhash, buffer, plan.states, if (provided_hashes) 0 else count),
            .result = slice(?u64, buffer, plan.result, if (provided_hashes) 0 else count),
            .memo = slice(Memo, buffer, plan.memo, plan.memo_count),
        };
    }
    fn reset(self: *GroupedScratch, control: ?Work.Control) !void {
        try Work.poll(control);
        for (self.states, self.result, 0..) |*state, *valid, index| {
            if (index % 64 == 0) try Work.poll(control);
            state.* = .init(0);
            valid.* = 0;
        }
        for (self.memo, 0..) |*entry, index| {
            if (index % 64 == 0) try Work.poll(control);
            entry.generation = 0;
        }
        self.generation = 0;
    }
    fn nextColumn(self: *GroupedScratch, control: ?Work.Control) !void {
        try Work.poll(control);
        self.generation += 1; // At most 256 columns in one Grouped operation.
    }
    fn semantic(self: *GroupedScratch, cell: scalar.Datum, identity: ?u64, control: ?Work.Control) !u64 {
        if (cell.sql_null) return 0;
        const kind: Kind = if (identity != null) .dictionary else if (cell.value == .string) .text else return scalar.semanticHashDatumControlled(cell, control);
        const key = identity orelse try Work.hashBytes(control, 0, cell.value.string);
        var at: usize = @intCast((key *% 0x9e3779b97f4a7c15) & (self.memo.len - 1));
        // At most count unique cells and at least 2*count slots: a free slot
        // always exists. Every collision probe retains original work authority.
        while (true) : (at = (at + 1) & (self.memo.len - 1)) {
            try Work.poll(control);
            const entry = &self.memo[at];
            if (entry.generation != self.generation) {
                const value = try scalar.semanticHashDatumControlled(cell, control);
                entry.* = .{ .key = key, .semantic = value, .text = if (kind == .text) cell.value.string else &.{}, .generation = self.generation, .kind = kind };
                return value;
            }
            if (entry.kind == kind and entry.key == key and (kind != .text or try Work.equalBytes(control, entry.text, cell.value.string))) return entry.semantic;
        }
    }
    fn mix(state: *std.hash.Wyhash, cell: scalar.Datum, hash: u64, grouped: bool) void {
        var bytes: [9]u8 = undefined;
        bytes[0] = @intFromBool(cell.sql_null);
        std.mem.writeInt(u64, bytes[1..9], hash, .little);
        state.update(if (grouped) &bytes else bytes[1..9]);
    }
    fn finish(self: *GroupedScratch, control: ?Work.Control) ![]?u64 {
        for (self.states, self.result, 0..) |*state, *out, index| {
            if (index % 64 == 0) try Work.poll(control);
            if (out.* != null) out.* = state.final();
        }
        return self.result;
    }
    pub fn columns(self: *GroupedScratch, values: []const []const scalar.Datum, grouped: bool, control: ?Work.Control) ![]?u64 {
        if (values.len > 256 or self.states.len != self.ids.len) return error.InvalidSqlBackendResponse;
        try self.reset(control);
        for (values) |column| {
            if (column.len != self.ids.len) return error.InvalidSqlBackendResponse;
            try self.nextColumn(control);
            for (column, self.states, self.result) |cell, *state, *valid| {
                try Work.poll(control);
                if (!grouped and cell.sql_null) valid.* = null;
                if (valid.* != null) mix(state, cell, try self.semantic(cell, null, control), grouped);
            }
        }
        return self.finish(control);
    }
    pub fn encodedColumns(self: *GroupedScratch, decoded: A, values: []const @import("execution_batch.zig").Batch, grouped: bool, control: ?Work.Control) ![]?u64 {
        if (values.len > 256 or self.states.len != self.ids.len) return error.InvalidSqlBackendResponse;
        try self.reset(control);
        for (values) |column| {
            if (column.len() != self.ids.len or column.width() != 1) return error.InvalidSqlBackendResponse;
            try self.nextColumn(control);
            for (self.states, self.result, 0..) |*state, *valid, index| {
                try Work.poll(control);
                // Decoding belongs to the query budget, not this fixed memo.
                // Its OOM/cancellation is an original operation error.
                const cell = try column.cell(decoded, index, 0);
                if (!grouped and cell.sql_null) valid.* = null;
                if (valid.* != null) mix(state, cell, try self.semantic(cell, if (cell.sql_null) null else try column.dictionaryIdentity(index, 0), control), grouped);
            }
        }
        return self.finish(control);
    }
};
