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

//! Reached integer range lineage. Every expression is checked over the domain
//! on which its stage runs. Restrictions preserve original encounter order;
//! unsupported or fallible programs decline without evaluating query code.
const std = @import("std");
const ast = @import("ast.zig");
const v = @import("value.zig");
const Range = @import("range.zig").Range;
pub const Form = @import("integer_affine.zig").Form;
pub const Known = union(enum) { integer: Form, scalar: v.Value };
pub const Binding = struct { name: []const u8, value: Known };
pub const Context = struct {
    a: std.mem.Allocator,
    sequence: Range,
    bindings: []const Binding,
    positions: std.StringHashMapUnmanaged(usize) = .empty,
    parameters: []const v.Entry,
    ptr: *anyopaque,
    charge: *const fn (*anyopaque, usize) anyerror!void,

    pub fn init(a: std.mem.Allocator, sequence: Range, row: []const v.Entry, alias: []const u8, parameters: []const v.Entry, ptr: *anyopaque, charge: *const fn (*anyopaque, usize) anyerror!void) !Context {
        var bindings: std.ArrayList(Binding) = .empty;
        var replaced = false;
        for (row) |entry| {
            const axis = !replaced and std.mem.eql(u8, entry.name, alias);
            try bindings.append(a, .{ .name = entry.name, .value = if (axis) .{ .integer = Form.column(0) } else known(entry.value) });
            replaced = replaced or axis;
        }
        if (!replaced) try bindings.append(a, .{ .name = alias, .value = .{ .integer = Form.column(0) } });
        var result: Context = .{ .a = a, .sequence = sequence, .bindings = &.{}, .parameters = parameters, .ptr = ptr, .charge = charge };
        try result.setBindings(bindings.items);
        return result;
    }
    fn known(value: v.Value) Known {
        return if (value == .integer) .{ .integer = Form.literal(value.integer) } else .{ .scalar = value };
    }
    fn setBindings(self: *Context, bindings: []const Binding) !void {
        self.bindings = bindings;
        self.positions = .empty;
        for (bindings, 0..) |entry, index| {
            const slot = try self.positions.getOrPut(self.a, entry.name);
            if (!slot.found_existing) slot.value_ptr.* = index;
        }
    }
    fn bounded(self: *Context, form: Form) ?Form {
        if (self.sequence.length == 0) return null;
        const first = @as(i128, form.scale) * self.sequence.start + form.offset;
        const last = @as(i128, form.scale) * self.sequence.at(self.sequence.length - 1).integer + form.offset;
        var result = form;
        result.interval = .{ .low = std.math.cast(i64, @min(first, last)) orelse return null, .high = std.math.cast(i64, @max(first, last)) orelse return null };
        return result;
    }
    pub fn integer(self: *Context, expression_value: *const ast.Expr) !?Form {
        const result = (try self.expression(expression_value)) orelse return null;
        return if (result == .integer) result.integer else null;
    }
    pub fn expression(self: *Context, expression_value: *const ast.Expr) anyerror!?Known {
        try self.charge(self.ptr, 1);
        const result: Known = switch (expression_value.value) {
            .literal => |value| switch (value) {
                .integer => |n| .{ .integer = Form.literal(n) },
                .null => .{ .scalar = .null },
                .boolean => |b| .{ .scalar = .{ .boolean = b } },
                .number => |n| .{ .scalar = .{ .number = n } },
                .string => |s| .{ .scalar = .{ .string = s } },
            },
            .variable => |name| self.bindings[self.positions.get(name) orelse return null].value,
            .parameter => |name| blk: {
                for (self.parameters) |entry| if (std.mem.eql(u8, entry.name, name)) break :blk known(entry.value);
                return null;
            },
            .unary => |u| blk: {
                const form = (try self.integer(u.operand)) orelse return null;
                break :blk .{ .integer = if (u.op == .positive) form else if (u.op == .negative) form.negative() orelse return null else return null };
            },
            .binary => |b| blk: {
                if (b.op != .add and b.op != .subtract and b.op != .multiply) return null;
                const left = (try self.integer(b.left)) orelse return null;
                const right = (try self.integer(b.right)) orelse return null;
                const result = Form.combine(left, right, switch (b.op) {
                    .add => .add,
                    .subtract => .subtract,
                    .multiply => .multiply,
                    else => unreachable,
                }) orelse return null;
                // Check every intermediate before tightening correlated bounds.
                if (result.interval == null) return null;
                break :blk .{ .integer = result };
            },
            else => return null,
        };
        if (result == .scalar) return switch (result.scalar) {
            .null, .boolean, .number, .string => result,
            else => null,
        };
        return .{ .integer = self.bounded(result.integer) orelse return null };
    }
    /// WITH expressions share the old scope. WHERE overlays projected names on
    /// that old scope; its output drops unprojected input names afterwards.
    pub fn project(self: *Context, projection: ast.Projection, names: []const []const u8) !bool {
        if (projection.distinct or projection.order.len != 0 or projection.skip != null or projection.limit != null) return false;
        var output: std.ArrayList(Binding) = .empty;
        if (projection.wildcard) try output.appendSlice(self.a, self.bindings);
        for (projection.items, names) |item, name| try output.append(self.a, .{ .name = name, .value = (try self.expression(item.expression)) orelse return false });
        if (projection.predicate) |predicate_value| {
            var scope: std.ArrayList(Binding) = .empty;
            try scope.appendSlice(self.a, self.bindings);
            var positions = self.positions;
            for (output.items) |entry| {
                const slot = try positions.getOrPut(self.a, entry.name);
                if (slot.found_existing) scope.items[slot.value_ptr.*] = entry else {
                    slot.value_ptr.* = scope.items.len;
                    try scope.append(self.a, entry);
                }
            }
            self.bindings = scope.items;
            self.positions = positions;
            if (!try self.restrict(predicate_value)) return false;
        }
        try self.setBindings(output.items);
        return true;
    }
    fn slice(self: *Context, first: u128, end: u128) void {
        std.debug.assert(first <= end and end <= self.sequence.length);
        if (first != end) self.sequence.start = self.sequence.at(first).integer;
        self.sequence.length = end - first;
    }
    fn difference(self: *Context, left: Form, right: Form, index: u128) i128 {
        const value = self.sequence.at(index).integer;
        return (@as(i128, left.scale) * value + left.offset) - (@as(i128, right.scale) * value + right.offset);
    }
    fn boundary(self: *Context, left: Form, right: Form, descending: bool, upper: bool) !u128 {
        var low: u128 = 0;
        var high = self.sequence.length;
        while (low < high) {
            try self.charge(self.ptr, 1);
            const middle = low + (high - low) / 2;
            const difference_value = self.difference(left, right, middle) * @as(i128, if (descending) -1 else 1);
            if (difference_value < 0 or (upper and difference_value == 0)) low = middle + 1 else high = middle;
        }
        return low;
    }
    fn compare(self: *Context, left: Form, right: Form, operation: ast.Binary) !bool {
        if (self.sequence.length == 0) return true;
        const descending = self.difference(left, right, 0) > self.difference(left, right, self.sequence.length - 1);
        const low = try self.boundary(left, right, descending, false);
        const high = try self.boundary(left, right, descending, true);
        const op = if (descending) switch (operation) {
            .lt => ast.Binary.gt,
            .lte => .gte,
            .gt => .lt,
            .gte => .lte,
            else => operation,
        } else operation;
        switch (op) {
            .lt => self.slice(0, low),
            .lte => self.slice(0, high),
            .eq => self.slice(low, high),
            .gte => self.slice(low, self.sequence.length),
            .gt => self.slice(high, self.sequence.length),
            .neq => if (low == high) {} else if (low == 0) self.slice(high, self.sequence.length) else if (high == self.sequence.length) self.slice(0, low) else return false,
            else => return false,
        }
        return true;
    }
    fn modulo(self: *Context, expression_value: *const ast.Expr, remainder_value: *const ast.Expr) !bool {
        if (expression_value.value != .binary or expression_value.value.binary.op != .modulo) return false;
        const b = expression_value.value.binary;
        const form = (try self.integer(b.left)) orelse return false;
        const divisor = (try self.integer(b.right)) orelse return false;
        const remainder = (try self.integer(remainder_value)) orelse return false;
        if (divisor.scale != 0 or divisor.offset == 0 or remainder.scale != 0) return false;
        const modulus: i128 = @intCast(@abs(@as(i128, divisor.offset)));
        const desired: i128 = remainder.offset;
        if (@abs(desired) >= modulus) {
            self.sequence.length = 0;
            return true;
        }
        // Signed remainder has the dividend's sign, including negative strides.
        if (desired != 0) _ = try self.compare(form, Form.literal(0), if (desired > 0) .gte else .lte);
        if (self.sequence.length == 0) return true;
        const coefficient = @mod(@as(i128, form.scale) * self.sequence.step, modulus);
        const target = @mod(desired - (@as(i128, form.scale) * self.sequence.start + form.offset), modulus);
        var gcd = coefficient;
        var next = modulus;
        var inverse: i128 = 1;
        var next_inverse: i128 = 0;
        while (next != 0) {
            try self.charge(self.ptr, 1);
            const q = @divTrunc(gcd, next);
            const rest = gcd - q * next;
            gcd = next;
            next = rest;
            const result = inverse - q * next_inverse;
            inverse = next_inverse;
            next_inverse = result;
        }
        if (@mod(target, gcd) != 0) {
            self.sequence.length = 0;
            return true;
        }
        const period = @divTrunc(modulus, gcd);
        const first: u128 = @intCast(@mod(@divTrunc(target, gcd) * inverse, period));
        if (first >= self.sequence.length) {
            self.sequence.length = 0;
            return true;
        }
        const length = (self.sequence.length - first - 1) / @as(u128, @intCast(period)) + 1;
        const step = if (length == 1) @as(i64, 1) else std.math.cast(i64, @as(i128, self.sequence.step) * period) orelse return false;
        self.sequence = .{ .start = self.sequence.at(first).integer, .step = step, .length = length };
        return true;
    }
    pub fn restrict(self: *Context, predicate_value: *const ast.Expr) anyerror!bool {
        try self.charge(self.ptr, 1);
        if (self.sequence.length == 0) return true;
        if (predicate_value.value == .binary) {
            const b = predicate_value.value.binary;
            if (b.op == .@"and") {
                // AND evaluates both operands, even when the left is false or
                // NULL. Prove both over the complete incoming domain before
                // intersecting; a rejected row must not conceal a RHS error.
                var left = self.*;
                if (!try left.restrict(b.left)) return false;
                var right = self.*;
                if (!try right.restrict(b.right)) return false;
                self.sequence = left.sequence;
                return self.restrict(b.right);
            }
            if (b.op == .eq) {
                if (b.left.value == .binary and b.left.value.binary.op == .modulo) return self.modulo(b.left, b.right);
                if (b.right.value == .binary and b.right.value.binary.op == .modulo) return self.modulo(b.right, b.left);
            }
            switch (b.op) {
                .eq, .neq, .lt, .lte, .gt, .gte => {},
                else => return false,
            }
            const left = (try self.integer(b.left)) orelse return false;
            const right = (try self.integer(b.right)) orelse return false;
            return self.compare(left, right, b.op);
        }
        const result = (try self.expression(predicate_value)) orelse return false;
        if (result != .scalar) return false;
        switch (result.scalar) {
            .boolean => |b| if (!b) {
                self.sequence.length = 0;
            },
            .null => self.sequence.length = 0,
            else => return false,
        }
        return true;
    }
};
