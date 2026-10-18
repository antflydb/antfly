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

//! Immutable decimal execution templates, compiled with the scalar program.
//! Templates contain instruction ordinals only, never batch data or parameters.
const std = @import("std");
const scalar = @import("scalar.zig");
const Binary = @import("ast.zig").Scalar.Binary;
pub const Step = struct {
    index: u32,
    operation: union(enum) {
        constant,
        column: u32,
        alias: u16,
        sign: struct { child: u16, negative: bool },
        binary: struct { left: u16, right: u16, op: Binary },
    },
};
pub const Plan = struct {
    instructions: []const scalar.Instruction,
    root: u32,
    steps: []const Step,
    output: u16,
};
fn constant(instructions: []const scalar.Instruction, index: u32, depth: usize) bool {
    if (depth == 32 or index >= instructions.len) return false;
    return switch (instructions[index].operation) {
        .literal, .parameter => true,
        .cast => |v| constant(instructions, v.operand, depth + 1),
        .unary => |v| constant(instructions, v.operand, depth + 1),
        else => false,
    };
}
const Builder = struct {
    instructions: []const scalar.Instruction,
    steps: [512]Step = undefined,
    count: usize = 0,
    fn add(self: *@This(), index: u32, depth: usize) ?u16 {
        if (depth == 32 or self.count == self.steps.len or index >= self.instructions.len) return null;
        for (self.steps[0..self.count], 0..) |step, slot| if (step.index == index) return @intCast(slot);
        const instruction = self.instructions[index];
        const operation: @FieldType(Step, "operation") = if (constant(self.instructions, index, 0)) .constant else switch (instruction.operation) {
            .column => |ordinal| .{ .column = ordinal },
            .cast => |v| blk: {
                if (v.type != .number or v.element_type != .numeric or v.numeric_modifier != null) return null;
                break :blk .{ .alias = self.add(v.operand, depth + 1) orelse return null };
            },
            .unary => |v| blk: {
                if (instruction.type.element_type != .numeric or (v.op != .positive and v.op != .negative)) return null;
                break :blk .{ .sign = .{ .child = self.add(v.operand, depth + 1) orelse return null, .negative = v.op == .negative } };
            },
            .binary => |v| blk: {
                if (instruction.type.element_type != .numeric) return null;
                switch (v.op) {
                    .add, .subtract, .multiply, .divide, .modulo => {},
                    else => return null,
                }
                break :blk .{ .binary = .{ .left = self.add(v.left, depth + 1) orelse return null, .right = self.add(v.right, depth + 1) orelse return null, .op = v.op } };
            },
            else => return null,
        };
        if (self.count == self.steps.len) return null;
        const slot: u16 = @intCast(self.count);
        self.steps[self.count] = .{ .index = index, .operation = operation };
        self.count += 1;
        return slot;
    }
};
pub fn compile(a: std.mem.Allocator, instructions: []const scalar.Instruction, root: u32) !?Plan {
    if (root >= instructions.len or instructions[root].type.element_type != .numeric or instructions[root].type.numeric_modifier != null) return null;
    var builder: Builder = .{ .instructions = instructions };
    const output = builder.add(root, 0) orelse return null;
    return .{ .instructions = instructions, .root = root, .steps = try a.dupe(Step, builder.steps[0..builder.count]), .output = output };
}
