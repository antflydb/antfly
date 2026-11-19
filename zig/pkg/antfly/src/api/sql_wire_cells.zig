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

//! Shared native cells for socket and server-side COPY output.
const std = @import("std");
const native = @import("antfly_local_sources").sql_runtime;
const wire = @import("../pgwire/backend.zig");
pub const OutputCells = struct {
    output: native.Output,
    pub fn read(raw: *anyopaque, alloc: std.mem.Allocator, row: usize, column: usize) !wire.Cell {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const rows = self.output.native_rows orelse return error.InvalidSqlBackendResponse;
        if (row >= rows.len or column >= self.output.columns.len or rows[row].len != self.output.columns.len) return error.InvalidSqlBackendResponse;
        const datum = rows[row][column];
        if (datum.bytes()) |bytes| {
            try datum.validateTemporal();
            if (self.output.columns[column].type != .bytea) return error.InvalidSqlBackendResponse;
            return .{ .bytea = bytes, .sql_null = false };
        }
        if (datum.enumeration()) |atom| {
            try datum.validateTemporal();
            return .{ .enumeration = atom, .sql_null = false };
        }
        if (datum.range()) |range| {
            try datum.validateTemporal();
            if (@import("antfly_local_sources").sql_range_value.kindFromType(self.output.columns[column].type) != range.kind) return error.InvalidSqlBackendResponse;
            return .{ .range = range, .sql_null = false };
        }
        if (datum.temporal()) |value| {
            try datum.validateTemporal();
            const definition = self.output.columns[column];
            if (definition.type != .datetime or (definition.temporal_kind orelse .timestamp_tz) != value.kind()) return error.InvalidSqlBackendResponse;
            var text: [@import("antfly_local_sources").sql_temporal_text.output_capacity]u8 = undefined;
            return .{ .value = .{ .string = try alloc.dupe(u8, try @import("antfly_local_sources").sql_temporal_text.format(value.*, &text)) }, .sql_null = false };
        }
        return .{ .value = if (!datum.sql_null and self.output.columns[column].type == .datetime and (self.output.columns[column].temporal_kind orelse .timestamp_tz) == .timestamp_tz) try datetimeResult(alloc, datum.value) else datum.value, .sql_null = datum.sql_null, .array = datum.array, .numeric = datum.numeric };
    }
};

fn datetimeResult(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    return @import("../pgwire/values.zig").timestampResult(alloc, value) catch |err| switch (err) {
        error.InvalidResult => error.InvalidSqlBackendResponse,
        else => err,
    };
}
