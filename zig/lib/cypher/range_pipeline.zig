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
    parts: ?[]const Range = null,
    periodic: bool = false,
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
    /// Ordered bag segments; no set normalization may reorder or erase copies.
    pub fn ranges(self: *const Context) []const Range {
        return self.parts orelse @as(*const [1]Range, @ptrCast(&self.sequence));
    }
    pub fn domainLength(self: *const Context) u128 {
        var total: u128 = 0;
        for (self.ranges()) |part| total +|= part.length;
        return total;
    }
    pub fn firstValue(self: *const Context) ?v.Value {
        for (self.ranges()) |part| if (part.length != 0) return part.at(0);
        return null;
    }
    pub fn setRanges(self: *Context, parts: []const Range) void {
        self.parts = parts;
    }
    pub fn bind(self: *Context, name: []const u8, value: v.Value) !void {
        var entries: std.ArrayList(Binding) = .empty;
        try entries.appendSlice(self.a, self.bindings);
        if (self.positions.get(name)) |position| {
            entries.items[position].value = known(value);
        } else try entries.append(self.a, .{ .name = name, .value = known(value) });
        try self.setBindings(entries.items);
    }
    pub fn injective(self: *Context) !bool {
        const size = (try @import("range.zig").distinctCardinalityWork(self.a, self.ranges(), self.ptr, self.charge)) orelse return false;
        return size == self.domainLength();
    }
    pub fn render(self: *const Context, raw: i64) ![]const v.Entry {
        const row = try self.a.alloc(v.Entry, self.bindings.len);
        for (self.bindings, row) |entry, *cell| cell.* = .{ .name = entry.name, .value = switch (entry.value) {
            .scalar => |value| value,
            .integer => |form| .{ .integer = std.math.cast(i64, @as(i128, form.scale) * raw + form.offset) orelse return error.CypherIntegerOverflow },
        } };
        return row;
    }
    fn subrange(part: Range, start: u128, count: u128) Range {
        return .{ .start = if (count == 0) part.start else part.at(start).integer, .step = part.step, .length = count };
    }
    fn countBelow(part: Range, bound: i128) u128 {
        if (part.length == 0) return 0;
        const end = part.at(part.length - 1).integer;
        const low = @min(part.start, end);
        const high = @max(part.start, end);
        if (bound < low) return 0;
        if (bound >= high) return part.length;
        return @intCast(@divTrunc(bound - low, @as(i128, @intCast(@abs(@as(i128, part.step))))) + 1);
    }
    /// Select a raw-value rank boundary, retaining original encounter order.
    /// NULL rank means stable source ties. At most 64 charged binary rounds,
    /// independent of the number of represented rows.
    pub fn window(self: *Context, capacity: u128, ascending: ?bool) ![]const Range {
        var selected: std.ArrayList(Range) = .empty;
        const keep = @min(capacity, self.domainLength());
        if (keep == 0) return &.{};
        if (ascending == null) {
            var left = keep;
            for (self.ranges()) |part| {
                const count = @min(left, part.length);
                if (count != 0) try selected.append(self.a, subrange(part, 0, count));
                left -= count;
                if (left == 0) break;
            }
            return selected.items;
        }
        if (self.ranges().len == 1) {
            const part = self.ranges()[0];
            try selected.append(self.a, subrange(part, if ((part.step > 0) == ascending.?) 0 else part.length - keep, keep));
            return selected.items;
        }
        var low: i128 = std.math.maxInt(i64);
        var high: i128 = std.math.minInt(i64);
        for (self.ranges()) |part| if (part.length != 0) {
            const end = part.at(part.length - 1).integer;
            low = @min(low, @min(part.start, end));
            high = @max(high, @max(part.start, end));
        };
        const ordinal = if (ascending.?) keep else self.domainLength() - keep + 1;
        while (low < high) {
            const middle = low + @divTrunc(high - low, 2);
            var count: u128 = 0;
            for (self.ranges()) |part| {
                try self.charge(self.ptr, 1);
                count +|= countBelow(part, middle);
            }
            if (count < ordinal) low = middle + 1 else high = middle;
        }
        for (self.ranges()) |part| {
            const below = countBelow(part, if (ascending.?) low else low - 1);
            const count = if (ascending.?) below else part.length - below;
            const prefix = (part.step > 0) == ascending.?;
            if (count != 0) try selected.append(self.a, subrange(part, if (prefix) 0 else part.length - count, count));
        }
        return selected.items;
    }
    pub fn paginate(self: *Context, skip: u128, limit: ?u128) !void {
        var output: std.ArrayList(Range) = .empty;
        var left = skip;
        var remaining = limit orelse std.math.maxInt(u128);
        for (self.ranges()) |part| {
            const start = @min(left, part.length);
            left -= start;
            const size = @min(remaining, part.length - start);
            if (size != 0) try output.append(self.a, subrange(part, start, size));
            remaining -= size;
            if (remaining == 0) break;
        }
        self.parts = output.items;
    }
    pub const Page = struct { skip: u128 = 0, limit: ?u128 = null };
    /// Select the page's first value by exact bag rank, then merge only the
    /// bounded retained rows. Duplicate values retain segment encounter order.
    fn sortedPage(self: *Context, ascending: bool, page: Page) !bool {
        const limit = page.limit orelse return false;
        if (limit > 128) return false;
        const length = self.domainLength();
        if (page.skip >= length or limit == 0) {
            self.setRanges(&.{});
            return true;
        }
        var low: i128 = std.math.maxInt(i64);
        var high: i128 = std.math.minInt(i64);
        for (self.ranges()) |part| if (part.length != 0) {
            const last = part.at(part.length - 1).integer;
            low = @min(low, @min(part.start, last));
            high = @max(high, @max(part.start, last));
        };
        const rank = if (ascending) page.skip + 1 else length - page.skip;
        while (low < high) {
            const middle = low + @divTrunc(high - low, 2);
            var count: u128 = 0;
            for (self.ranges()) |part| {
                try self.charge(self.ptr, 1);
                count += countBelow(part, middle);
            }
            if (count < rank) low = middle + 1 else high = middle;
        }
        const Cursor = struct { part: Range, position: u128 = 0 };
        var cursors: std.ArrayList(Cursor) = .empty;
        var preceding: u128 = 0;
        for (self.ranges()) |incoming| {
            if (incoming.length == 0) continue;
            var part = incoming;
            if (part.length > 1 and (part.step > 0) != ascending) {
                part.start = part.at(part.length - 1).integer;
                part.step = std.math.negate(part.step) catch return false;
            }
            const before = if (ascending) countBelow(part, low - 1) else part.length - countBelow(part, low);
            preceding += before;
            try cursors.append(self.a, .{ .part = part, .position = before });
        }
        var tie_skip = page.skip - preceding;
        var output: std.ArrayList(Range) = .empty;
        const keep = @min(limit, length - page.skip);
        while (output.items.len < keep) {
            var choice: ?usize = null;
            for (cursors.items, 0..) |cursor, slot| {
                try self.charge(self.ptr, 1);
                if (cursor.position == cursor.part.length) continue;
                const value = cursor.part.at(cursor.position).integer;
                if (choice == null or (if (ascending) value < cursors.items[choice.?].part.at(cursors.items[choice.?].position).integer else value > cursors.items[choice.?].part.at(cursors.items[choice.?].position).integer)) choice = slot;
            }
            const slot = choice orelse return false;
            const value = cursors.items[slot].part.at(cursors.items[slot].position).integer;
            cursors.items[slot].position += 1;
            if (tie_skip != 0) {
                tie_skip -= 1;
                continue;
            }
            try output.append(self.a, .{ .start = value, .step = 1, .length = 1 });
        }
        self.setRanges(output.items);
        return true;
    }
    /// Sort disjoint monotone runs without enumerating their values. General
    /// interleaving lattices retain ordinary replay rather than losing order.
    fn orderRanges(self: *Context, ascending: bool) !bool {
        const runs = try self.a.dupe(Range, self.ranges());
        for (runs) |*part| if (part.length > 1 and (part.step > 0) != ascending) {
            const step = std.math.negate(part.step) catch return false;
            part.start = part.at(part.length - 1).integer;
            part.step = step;
        };
        const Order = struct {
            fn less(up: bool, x: Range, y: Range) bool {
                return if (up) x.start < y.start else x.start > y.start;
            }
        };
        try self.charge(self.ptr, runs.len *| 8);
        std.mem.sort(Range, runs, ascending, Order.less);
        for (runs[0..runs.len -| 1], runs[@min(1, runs.len)..]) |prior, next| {
            if (prior.length == 0 or next.length == 0) continue;
            const last = prior.at(prior.length - 1).integer;
            if (if (ascending) last >= next.start else last <= next.start) return false;
        }
        self.parts = runs;
        return true;
    }
    fn appendPiece(self: *Context, output: *std.ArrayList(Range), part: Range, start: u128, size: u128) !bool {
        if (size == 0) return true;
        if (output.items.len == 128) return false;
        try self.charge(self.ptr, 1);
        try output.append(self.a, subrange(part, start, size));
        return true;
    }
    /// Stable novel support. Subtract prior runs in encounter order. Periodic
    /// differences may split, but planning work and retained segments are capped.
    pub fn deduplicate(self: *Context) !bool {
        var output: std.ArrayList(Range) = .empty;
        for (self.ranges()) |part| {
            if (part.length == 0) continue;
            var pending: std.ArrayList(Range) = .empty;
            try pending.append(self.a, part);
            for (output.items) |seen| {
                var next: std.ArrayList(Range) = .empty;
                for (pending.items) |candidate| {
                    try self.charge(self.ptr, 1);
                    const hit = @import("range.zig").Run.intersect(@import("range.zig").Run.from(candidate), @import("range.zig").Run.from(seen)) orelse {
                        if (!try self.appendPiece(&next, candidate, 0, candidate.length)) return false;
                        continue;
                    };
                    const first_hit: u128 = @intCast(@divExact((if (candidate.step > 0) hit.low else hit.high) - candidate.start, candidate.step));
                    const last_hit: u128 = @intCast(@divExact((if (candidate.step > 0) hit.high else hit.low) - candidate.start, candidate.step));
                    const stride: u128 = if (hit.low == hit.high) 1 else @intCast(@divExact(hit.step, @as(i128, @intCast(@abs(@as(i128, candidate.step))))));
                    if (!try self.appendPiece(&next, candidate, 0, first_hit)) return false;
                    if (stride == 2 and last_hit > first_hit) {
                        const size = (last_hit - first_hit) / 2;
                        const step = if (size == 1) @as(i64, 1) else std.math.cast(i64, @as(i128, candidate.step) * 2) orelse return false;
                        if (next.items.len == 128) return false;
                        try next.append(self.a, .{ .start = candidate.at(first_hit + 1).integer, .step = step, .length = size });
                    } else if (stride > 1 and last_hit > first_hit) {
                        if ((last_hit - first_hit) / stride > 128) return false;
                        var ordinal = first_hit;
                        while (ordinal < last_hit) : (ordinal += stride) {
                            if (!try self.appendPiece(&next, candidate, ordinal + 1, stride - 1)) return false;
                        }
                    }
                    if (!try self.appendPiece(&next, candidate, last_hit + 1, candidate.length - last_hit - 1)) return false;
                }
                pending = next;
                if (pending.items.len == 0) break;
            }
            if (output.items.len + pending.items.len > 128) return false;
            try output.appendSlice(self.a, pending.items);
        }
        self.parts = output.items;
        return true;
    }
    fn known(value: v.Value) Known {
        return if (value == .integer) .{ .integer = Form.literal(value.integer) } else .{ .scalar = value };
    }
    pub fn setBindings(self: *Context, bindings: []const Binding) !void {
        self.bindings = bindings;
        self.positions = .empty;
        for (bindings, 0..) |entry, index| {
            const slot = try self.positions.getOrPut(self.a, entry.name);
            if (!slot.found_existing) slot.value_ptr.* = index;
        }
    }
    pub fn overlay(self: *const Context, bindings: []const Binding) !Context {
        var result = self.*;
        var entries: std.ArrayList(Binding) = .empty;
        try entries.appendSlice(self.a, self.bindings);
        try result.setBindings(entries.items);
        for (bindings) |entry| {
            if (result.positions.get(entry.name)) |slot| entries.items[slot] = entry else {
                try entries.append(self.a, entry);
                try result.setBindings(entries.items);
            }
        }
        try result.setBindings(entries.items);
        return result;
    }
    fn bounded(self: *Context, form: Form) ?Form {
        if (self.domainLength() == 0) return null;
        var low: i128 = std.math.maxInt(i64);
        var high: i128 = std.math.minInt(i64);
        for (self.ranges()) |part| {
            if (part.length == 0) continue;
            const first_value = @as(i128, form.scale) * part.start + form.offset;
            const last_value = @as(i128, form.scale) * part.at(part.length - 1).integer + form.offset;
            low = @min(low, @min(first_value, last_value));
            high = @max(high, @max(first_value, last_value));
        }
        var result = form;
        result.interval = .{ .low = std.math.cast(i64, low) orelse return null, .high = std.math.cast(i64, high) orelse return null };
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
                if (b.op == .modulo) {
                    const left = (try self.integer(b.left)) orelse return null;
                    const right = (try self.integer(b.right)) orelse return null;
                    if (right.scale != 0 or right.offset == 0) return null;
                    const modulus: i128 = @intCast(@abs(@as(i128, right.offset)));
                    var remainder: ?i64 = null;
                    for (self.ranges()) |part| {
                        if (part.length == 0) continue;
                        const first = std.math.cast(i64, @as(i128, left.scale) * part.start + left.offset) orelse return null;
                        const last = std.math.cast(i64, @as(i128, left.scale) * part.at(part.length - 1).integer + left.offset) orelse return null;
                        const r: i64 = @intCast(@rem(@as(i128, first), modulus));
                        if (part.length > 1 and @rem(@as(i128, left.scale) * part.step, modulus) != 0) return null;
                        if (@rem(@as(i128, last), modulus) != r or (remainder != null and remainder.? != r)) return null;
                        remainder = r;
                    }
                    break :blk .{ .integer = Form.literal(remainder orelse return null) };
                }
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
    pub fn project(self: *Context, projection: ast.Projection, names: []const []const u8, page: ?Page) !bool {
        if (projection.skip != null or projection.limit != null) return false;
        var output: std.ArrayList(Binding) = .empty;
        if (projection.wildcard) try output.appendSlice(self.a, self.bindings);
        for (projection.items, names) |item, name| try output.append(self.a, .{ .name = name, .value = (try self.expression(item.expression)) orelse return false });
        if (projection.predicate != null or projection.order.len != 0) {
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
            if (projection.distinct) try self.setBindings(output.items) else {
                self.bindings = scope.items;
                self.positions = positions;
            }
            if (projection.predicate) |predicate_value| if (!try self.restrict(predicate_value)) return false;
        }
        var order_scope = self.*;
        try self.setBindings(output.items);
        if (projection.distinct and self.domainLength() != 0) {
            var varying = false;
            for (output.items) |entry| {
                if (entry.value == .integer) {
                    varying = varying or entry.value.integer.scale != 0;
                } else switch (entry.value.scalar) {
                    .null, .boolean, .string => {},
                    .number => |n| if (!std.math.isFinite(n)) return false,
                    else => return false,
                }
            }
            if (varying) {
                if (self.periodic) return false;
                if (!try self.deduplicate()) return false;
            } else {
                const selected = try self.a.alloc(Range, 1);
                selected[0] = .{ .start = self.firstValue().?.integer, .step = 1, .length = 1 };
                self.parts = selected;
            }
        }
        if (projection.order.len != 0 and self.domainLength() != 0) {
            if (projection.distinct) order_scope = self.* else order_scope.setRanges(self.ranges());
            var direction: ?bool = null;
            for (projection.order) |order| {
                const key = (try order_scope.expression(order.expression)) orelse return false;
                if (key == .integer) {
                    if (direction == null and key.integer.scale != 0) direction = (key.integer.scale > 0) != order.descending;
                } else if (key.scalar == .number and !std.math.isFinite(key.scalar.number)) return false;
            }
            if (direction) |ascending| if (!try self.orderRanges(ascending)) {
                if (page) |window_page| return self.sortedPage(ascending, window_page);
                return false;
            };
        }
        if (page) |window_page| try self.paginate(window_page.skip, window_page.limit);
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
            .neq => if (low == high) {} else if (low == 0) self.slice(high, self.sequence.length) else if (high == self.sequence.length) self.slice(0, low) else {
                const pieces = try self.a.alloc(Range, 2);
                pieces[0] = .{ .start = self.sequence.start, .step = self.sequence.step, .length = low };
                pieces[1] = .{ .start = self.sequence.at(high).integer, .step = self.sequence.step, .length = self.sequence.length - high };
                self.parts = pieces;
            },
            else => return false,
        }
        return true;
    }
    pub fn residue(self: *Context, alias: []const u8, period: i64, desired: i64) !bool {
        const variable: ast.Expr = .{ .span = .{}, .value = .{ .variable = alias } };
        const divisor: ast.Expr = .{ .span = .{}, .value = .{ .literal = .{ .integer = period } } };
        const remainder: ast.Expr = .{ .span = .{}, .value = .{ .literal = .{ .integer = desired } } };
        const expression_value: ast.Expr = .{ .span = .{}, .value = .{ .binary = .{ .op = .modulo, .left = &variable, .right = &divisor } } };
        var parts: std.ArrayList(Range) = .empty;
        for (self.ranges()) |part| {
            var child = self.*;
            child.parts = null;
            child.sequence = part;
            if (!try child.modulo(&expression_value, &remainder)) return false;
            if (child.sequence.length != 0) try parts.append(self.a, child.sequence);
        }
        self.setRanges(parts.items);
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
    /// Union relative to one incoming sequence, preserving its ordinal order.
    /// Overlapping incompatible lattices decline instead of interleaving badly.
    fn unionParts(self: *Context, left: []const Range, right: []const Range) !bool {
        var pieces: std.ArrayList(Range) = .empty;
        for ([_][]const Range{ left, right }) |runs| for (runs) |part| {
            if (part.length == 0) continue;
            if (pieces.items.len == 128) return false;
            try pieces.append(self.a, part);
        };
        const Order = struct {
            fn less(descending: bool, x: Range, y: Range) bool {
                return if (descending) x.start > y.start else x.start < y.start;
            }
        };
        try self.charge(self.ptr, pieces.items.len *| 8);
        std.mem.sort(Range, pieces.items, self.sequence.step < 0, Order.less);
        var size: usize = 0;
        for (pieces.items) |part| {
            if (size != 0) {
                const prior = &pieces.items[size - 1];
                const end = prior.at(prior.length - 1).integer;
                const distance = (@as(i128, part.start) - end) * @as(i128, if (self.sequence.step < 0) -1 else 1);
                if (distance <= 0) {
                    if (prior.step != part.step or @rem(@as(i128, part.start) - prior.start, prior.step) != 0) return false;
                    const last = part.at(part.length - 1).integer;
                    const span = @divTrunc(@as(i128, last) - prior.start, prior.step) + 1;
                    prior.length = @max(prior.length, @as(u128, @intCast(span)));
                    continue;
                }
            }
            pieces.items[size] = part;
            size += 1;
        }
        self.parts = pieces.items[0..size];
        return true;
    }
    pub fn restrict(self: *Context, predicate_value: *const ast.Expr) anyerror!bool {
        if (self.parts) |parts| {
            var output: std.ArrayList(Range) = .empty;
            for (parts) |part| {
                var leaf = self.*;
                leaf.parts = null;
                leaf.sequence = part;
                if (!try leaf.restrict(predicate_value)) return false;
                for (leaf.ranges()) |selected| if (selected.length != 0) {
                    if (output.items.len == 128) return false;
                    try output.append(self.a, selected);
                };
            }
            self.parts = output.items;
            return true;
        }
        try self.charge(self.ptr, 1);
        if (self.sequence.length == 0) return true;
        if (predicate_value.value == .binary) {
            const b = predicate_value.value.binary;
            if (b.op == .@"or") {
                var left = self.*;
                if (!try left.restrict(b.left)) return false;
                var right = self.*;
                if (!try right.restrict(b.right)) return false;
                return self.unionParts(left.ranges(), right.ranges());
            }
            if (b.op == .@"and") {
                // AND evaluates both operands, even when the left is false or
                // NULL. Prove both over the complete incoming domain before
                // intersecting; a rejected row must not conceal a RHS error.
                var left = self.*;
                if (!try left.restrict(b.left)) return false;
                var right = self.*;
                if (!try right.restrict(b.right)) return false;
                self.sequence = left.sequence;
                self.parts = left.parts;
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
