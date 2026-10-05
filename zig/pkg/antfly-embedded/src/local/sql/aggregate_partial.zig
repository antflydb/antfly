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

//! Exact reducer interchange. Integer sums remain i128 until the final SQL
//! result, including when worker-local states spill or merge in stages.
const std = @import("std");
const Datum = @import("scalar.zig").Datum;
pub fn encode(a: std.mem.Allocator, states: []const @import("operators.zig").Aggregate) ![]const Datum {
    const values = try a.alloc(Datum, states.len * 7);
    for (states, 0..) |state, i| {
        const cells = values[i * 7 ..][0..7];
        cells[0] = Datum.json(.{ .integer = if (state.distinct) 0 else @intCast(state.count) });
        cells[1] = Datum.json(.{ .number_string = try std.fmt.allocPrint(a, "{d}", .{state.integer_sum}) });
        cells[2] = Datum.json(.{ .float = state.number_sum });
        cells[3] = Datum.json(.{ .float = state.compensation });
        cells[4] = Datum.json(.{ .float = state.mean });
        cells[5] = Datum.json(.{ .bool = state.boolean });
        cells[6] = if (state.selected) |selected| selected.row.values[0] else .{};
    }
    return values;
}
