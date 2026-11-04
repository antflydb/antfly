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

//! Pointer-free scalar fragment instructions. Execution-only caches, settings
//! captures and external decision providers never cross the worker boundary.
const std = @import("std");
const scalar = @import("antfly_local_sources").sql_scalar;
const A = std.mem.Allocator;
pub const Program = struct {
    version: u16 = 1,
    instructions: []const scalar.Instruction,
    root: u32,
    output: scalar.Type,
    parameters: []const std.json.Value = &.{},
    pub fn validate(self: Program, width: usize) !void {
        if (self.version != 1 or self.instructions.len == 0 or self.instructions.len > 4096 or self.root >= self.instructions.len or self.parameters.len > 256) return error.InvalidSqlDistributedPlan;
        for (self.instructions, 0..) |instruction, index| switch (instruction.operation) {
            .literal => {},
            .column => |ordinal| if (ordinal >= width) return error.InvalidSqlDistributedPlan,
            .parameter => |ordinal| if (ordinal == 0 or ordinal > self.parameters.len) return error.InvalidSqlDistributedPlan,
            .unary => |operation| if (operation.operand >= index) return error.InvalidSqlDistributedPlan,
            .binary => |operation| if (operation.left >= index or operation.right >= index) return error.InvalidSqlDistributedPlan,
            .cast => |operation| if (operation.operand >= index) return error.InvalidSqlDistributedPlan,
            .case_when => |operation| {
                if (operation.otherwise) |ordinal| if (ordinal >= index) return error.InvalidSqlDistributedPlan;
                for (operation.branches) |branch| if (branch.condition >= index or branch.value >= index) return error.InvalidSqlDistributedPlan;
            },
            .in_list => |operation| {
                if (operation.operand >= index) return error.InvalidSqlDistributedPlan;
                for (operation.values) |ordinal| if (ordinal >= index) return error.InvalidSqlDistributedPlan;
            },
            .call => |operation| {
                if (operation.setting_identity != null) return error.SqlDistributedJoinUnsupported;
                switch (operation.function) {
                    .ai_decide, .ai_choice, .ai_score, .ai_probability, .current_setting, .@"$single", .@"$pattern_quantified", .@"$array_pattern_quantified" => return error.SqlDistributedJoinUnsupported,
                    else => {},
                }
                for (operation.args) |ordinal| if (ordinal >= index) return error.InvalidSqlDistributedPlan;
            },
        };
        if (!std.meta.eql(self.instructions[self.root].type, self.output)) return error.InvalidSqlDistributedPlan;
    }
    pub fn matches(self: Program, a: A, cells: []const scalar.Datum) !bool {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const program: scalar.Program = .{ .arena = &arena, .instructions = self.instructions, .root = self.root, .output_type = self.output, .parameter_types = &.{}, .parameter_descriptors = &.{}, .required_columns = &.{} };
        const value = try program.evaluate(arena.allocator(), cells, self.parameters, .{});
        if (value.sql_null) return false;
        if (value.value != .bool) return error.InvalidSqlDistributedPlan;
        return value.value.bool;
    }
};

test "distributed SQL scalar fragments validate DAGs and retain SQL null predicate semantics" {
    const a = std.testing.allocator;
    const t: scalar.Type = .{ .kind = .boolean };
    var program: Program = .{ .instructions = &.{.{ .type = t, .operation = .{ .column = 0 } }}, .root = 0, .output = t };
    try program.validate(1);
    try std.testing.expect(!try program.matches(a, &.{.{}}));
    try std.testing.expect(try program.matches(a, &.{scalar.Datum.json(.{ .bool = true })}));
    program.instructions = &.{.{ .type = t, .operation = .{ .unary = .{ .op = .not, .operand = 0 } } }};
    try std.testing.expectError(error.InvalidSqlDistributedPlan, program.validate(1));
}
