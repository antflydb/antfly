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

//! Explicit JSON record conversion, not a JSONB-to-array SQL cast. Shared
//! scalar input functions convert each leaf; JSON arrays remain JSON elsewhere.
const std = @import("std");
const arrays = @import("array_value.zig");
const Datum = @import("scalar.zig").Datum;
const Json = std.json.Value;
const A = std.mem.Allocator;

pub const Converter = struct {
    ptr: *anyopaque,
    convert: *const fn (*anyopaque, Json, arrays.ElementType) anyerror!Datum,
    charge: *const fn (*anyopaque, usize) anyerror!void,
    work: *arrays.Budget,
};

const Builder = struct {
    a: A,
    converter: Converter,
    kind: arrays.ElementType,
    axes: [6]arrays.Dimension = undefined,
    rank: usize = 0,
    leaf_rank: ?usize = null,
    cells: std.ArrayList(Datum) = .empty,
    visited: usize = 0,

    fn visit(self: *Builder, input: Json, depth: usize) anyerror!void {
        self.visited += 1;
        if (self.visited > 1_048_576) return error.SqlProgramLimitExceeded;
        try self.converter.charge(self.converter.ptr, 0);
        if (input == .array) {
            if (depth >= self.axes.len) return error.SqlProgramLimitExceeded;
            const length = std.math.cast(u32, input.array.items.len) orelse return error.SqlProgramLimitExceeded;
            if (length > 65536) return error.SqlProgramLimitExceeded;
            if (depth < self.rank) {
                if (self.axes[depth].length != length) return error.SqlArraySubscriptError;
            } else {
                self.axes[depth] = .{ .length = length, .lower = 1 };
                self.rank = depth + 1;
            }
            if (self.leaf_rank) |rank| if (depth >= rank) return error.SqlArraySubscriptError;
            if (length == 0) {
                if (self.leaf_rank) |rank| {
                    if (rank != depth + 1) return error.SqlArraySubscriptError;
                } else self.leaf_rank = depth + 1;
            }
            for (input.array.items) |child| try self.visit(child, depth + 1);
        } else {
            if (depth == 0) return error.InvalidSqlParameters;
            if (self.leaf_rank) |rank| {
                if (rank != depth) return error.SqlArraySubscriptError;
            } else self.leaf_rank = depth;
            if (self.rank != depth) return error.SqlArraySubscriptError;
            if (self.cells.items.len >= 65536) return error.SqlProgramLimitExceeded;
            try self.converter.charge(self.converter.ptr, @sizeOf(Datum));
            const cell = if (input == .null) Datum{} else try self.converter.convert(self.converter.ptr, input, self.kind);
            try self.cells.append(self.a, cell);
        }
    }
};

/// All returned memory belongs to a bounded statement/page region. The
/// converter shares the evaluator's cancellation/work/output quota.
pub fn decode(a: A, input: Json, kind: arrays.ElementType, converter: Converter) !Datum {
    if (input != .array) return error.InvalidSqlParameters;
    var builder: Builder = .{ .a = a, .converter = converter, .kind = kind };
    try builder.visit(input, 0);
    try converter.charge(converter.ptr, @sizeOf(arrays.Value) + builder.rank * @sizeOf(arrays.Dimension));
    const axes = try a.dupe(arrays.Dimension, builder.axes[0..builder.rank]);
    const cells = try builder.cells.toOwnedSlice(a);
    const array = try a.create(arrays.Value);
    array.* = try arrays.Value.initWithBudget(kind, axes, cells, .{}, converter.work);
    return Datum.typedArray(array);
}
