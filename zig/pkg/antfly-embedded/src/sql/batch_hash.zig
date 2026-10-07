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
pub fn encodedColumns(a: A, values: []const @import("execution_batch.zig").Batch, count: usize, grouped: bool) ![]?u64 {
    const states = try a.alloc(std.hash.Wyhash, count);
    defer a.free(states);
    for (states) |*state| state.* = .init(0);
    const result = try a.alloc(?u64, count);
    errdefer a.free(result);
    @memset(result, 0);
    for (values) |column| {
        if (column.len() != count or column.width() != 1) return error.InvalidSqlBackendResponse;
        var memo: std.AutoHashMapUnmanaged(u64, u64) = .empty;
        defer memo.deinit(a);
        var text: std.StringHashMapUnmanaged(u64) = .empty;
        defer text.deinit(a);
        for (states, result, 0..) |*state, *valid, index| {
            const cell = try column.cell(a, index, 0);
            if (!grouped and cell.sql_null) valid.* = null;
            if (valid.* == null) continue;
            const semantic = if (cell.sql_null) 0 else if (try column.dictionaryIdentity(index, 0)) |id| blk: {
                if (memo.get(id)) |hash| break :blk hash;
                const hash = try scalar.semanticHash(cell.value);
                if (memo.count() < 4096) try memo.put(a, id, hash);
                break :blk hash;
            } else if (cell.value == .string) blk: {
                if (text.get(cell.value.string)) |hash| break :blk hash;
                const hash = try scalar.semanticHash(cell.value);
                if (text.count() < 4096) try text.put(a, cell.value.string, hash);
                break :blk hash;
            } else try scalar.semanticHash(cell.value);
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
    const hashes = try a.alloc(std.hash.Wyhash, count);
    defer a.free(hashes);
    for (hashes) |*h| h.* = .init(0);
    const result = try a.alloc(?u64, count);
    errdefer a.free(result);
    @memset(result, 0);
    for (values) |column| {
        if (column.len != count) return error.InvalidSqlBackendResponse;
        var text: std.StringHashMapUnmanaged(u64) = .empty;
        defer text.deinit(a);
        for (column, hashes, result) |cell, *hash, *valid| {
            if (!grouped and cell.sql_null) valid.* = null;
            if (valid.* == null) continue;
            const semantic = if (cell.sql_null) 0 else if (cell.value == .string) blk: {
                if (text.get(cell.value.string)) |cached| break :blk cached;
                const value = try scalar.semanticHashDatum(cell);
                // Bound memo state for high-cardinality identifiers.
                if (text.count() < 4096) try text.put(a, cell.value.string, value);
                break :blk value;
            } else try scalar.semanticHashDatum(cell);
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
        var text: std.StringHashMapUnmanaged(u64) = .empty;
        defer text.deinit(a);
        for (values, states, result) |row, *state, *valid| {
            const cell = row[column];
            if (!grouped and cell.sql_null) valid.* = null;
            if (valid.* == null) continue;
            const semantic = if (cell.sql_null) 0 else if (cell.value == .string) blk: {
                if (text.get(cell.value.string)) |hash| break :blk hash;
                const hash = try scalar.semanticHashDatum(cell);
                if (text.count() < 4096) try text.put(a, cell.value.string, hash);
                break :blk hash;
            } else try scalar.semanticHashDatum(cell);
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
