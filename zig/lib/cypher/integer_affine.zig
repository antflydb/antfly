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

//! Checked integer affine lineage. Keeping the dependency even when its
//! coefficient cancels preserves the domain and errors of intermediate nodes.
const std = @import("std");
pub const bounds = @import("linear_bounds.zig");
pub const Form = struct {
    field: ?usize = null,
    numeric: ?@import("linear_numeric.zig").Form = null,
    floating: bool = false,
    /// Multiple independent marginals certify only first moments. No concrete
    /// coefficient or joint extremum is inferred from this symbolic form.
    multiple: bool = false,
    interval: ?bounds.Interval = null,
    scale: i64 = 0,
    offset: i64 = 0,
    direct: bool = false,
    /// Symbolic forms certify affine shape, never concrete coefficients.
    /// Their integer representatives are evaluated by the ordinary evaluator.
    symbolic: bool = false,
    sign: Sign = .unknown,
    pub const Sign = enum { unknown, nonnegative, nonpositive, zero };
    pub fn invariant() Form {
        return .{ .symbolic = true };
    }
    fn signs(self: Form) Sign {
        if (self.symbolic) return self.sign;
        if (self.scale == 0 and self.offset == 0) return .zero;
        if (self.scale >= 0 and self.offset >= 0) return .nonnegative;
        if (self.scale <= 0 and self.offset <= 0) return .nonpositive;
        return .unknown;
    }
    fn negateSign(sign: Sign) Sign {
        return switch (sign) {
            .nonnegative => .nonpositive,
            .nonpositive => .nonnegative,
            else => sign,
        };
    }
    fn addSign(left: Sign, right: Sign) Sign {
        if (left == .zero) return right;
        if (right == .zero or left == right) return left;
        return .unknown;
    }
    pub fn choice(left: Form, right: Form) ?Form {
        if (left.field != null and right.field != null and left.field.? != right.field.?) return null;
        const l = left.signs();
        const r = right.signs();
        return .{ .numeric = if (left.numeric) |left_numeric| (if (right.numeric) |right_numeric| @import("linear_numeric.zig").Form.choice(left_numeric, right_numeric) else null) else null, .field = left.field orelse right.field, .multiple = left.multiple or right.multiple, .floating = left.floating or right.floating, .interval = if (left.interval != null and right.interval != null) .{ .low = @min(left.interval.?.low, right.interval.?.low), .high = @max(left.interval.?.high, right.interval.?.high) } else null, .symbolic = true, .sign = if (l == r) l else if (l == .zero) r else if (r == .zero) l else .unknown };
    }
    pub fn column(field: usize) Form {
        return .{ .field = field, .scale = 1, .direct = true };
    }
    pub fn literal(value: i64) Form {
        return .{ .offset = value, .interval = .literal(value) };
    }
    pub fn bounded(self: Form, interval: bounds.Interval) Form {
        var result = self;
        result.interval = interval;
        return result;
    }
    pub fn negative(self: Form) ?Form {
        var result: Form = if (self.symbolic) .{ .field = self.field, .symbolic = true, .multiple = self.multiple, .floating = self.floating, .sign = negateSign(self.sign) } else .{ .field = self.field, .scale = std.math.negate(self.scale) catch return null, .offset = std.math.negate(self.offset) catch return null };
        result.interval = if (self.interval) |interval| interval.negative() else null;
        return result;
    }
    const Operation = enum { add, subtract, multiply };
    pub fn combine(left: Form, right: Form, op: Operation) ?Form {
        if (left.multiple or right.multiple) return null;
        var result = combineRaw(left, right, op) orelse return null;
        if (left.interval) |l| if (right.interval) |r| {
            result.interval = bounds.Interval.combine(l, r, @fromBackingInt(@intCast(@backingInt(op))));
        };
        return result;
    }
    /// Independent marginals may share representatives only when all arithmetic
    /// is total over their bounding box. Ordinary evaluation retains actual
    /// coefficients; AVG/SUM never require a fabricated joint distribution.
    pub fn combineMarginals(left: Form, right: Form, op: Operation) ?Form {
        if (!left.multiple and !right.multiple and (left.field == null or right.field == null or left.field.? == right.field.?)) return combine(left, right, op);
        if (op == .multiply and left.field != null and right.field != null) return null;
        const interval = bounds.Interval.combine(left.interval orelse return null, right.interval orelse return null, @fromBackingInt(@intCast(@backingInt(op)))) orelse return null;
        const l = left.signs();
        const r = right.signs();
        return .{ .field = left.field orelse right.field, .multiple = true, .symbolic = true, .interval = interval, .sign = switch (op) {
            .add => addSign(l, r),
            .subtract => addSign(l, negateSign(r)),
            .multiply => if (l == .zero or r == .zero) .zero else if (l == .unknown or r == .unknown) .unknown else if (l == r) .nonnegative else .nonpositive,
        } };
    }
    fn combineRaw(left: Form, right: Form, op: Operation) ?Form {
        if (left.field != null and right.field != null and left.field.? != right.field.?) return null;
        const field = left.field orelse right.field;
        if (op == .multiply and left.field != null and right.field != null) return null;
        if (left.symbolic or right.symbolic) {
            const l = left.signs();
            const r = right.signs();
            return .{ .field = field, .symbolic = true, .sign = switch (op) {
                .add => addSign(l, r),
                .subtract => addSign(l, negateSign(r)),
                .multiply => if (l == .zero or r == .zero) .zero else if (l == .unknown or r == .unknown) .unknown else if (l == r) .nonnegative else .nonpositive,
            } };
        }
        return switch (op) {
            .add => .{ .field = field, .scale = std.math.add(i64, left.scale, right.scale) catch return null, .offset = std.math.add(i64, left.offset, right.offset) catch return null },
            .subtract => .{ .field = field, .scale = std.math.sub(i64, left.scale, right.scale) catch return null, .offset = std.math.sub(i64, left.offset, right.offset) catch return null },
            .multiply => blk: {
                if (left.field != null and right.field != null) return null;
                const variable = if (left.field != null) left else right;
                const constant = if (left.field != null) right.offset else left.offset;
                break :blk .{ .field = field, .scale = std.math.mul(i64, variable.scale, constant) catch return null, .offset = std.math.mul(i64, variable.offset, constant) catch return null };
            },
        };
    }
    /// A one-sign argument preserves Cypher SUM's intermediate overflow.
    pub fn oneSign(self: Form) bool {
        return self.signs() != .unknown;
    }
};

test "affine lineage retains cancelled dependencies and checked intermediates" {
    const x = Form.column(3);
    const cancelled = Form.combine(x, x, .subtract).?;
    try std.testing.expectEqual(@as(?usize, 3), cancelled.field);
    try std.testing.expectEqual(@as(i64, 0), cancelled.scale);
    try std.testing.expect(!cancelled.direct);
    try std.testing.expect(Form.combine(x, Form.column(4), .add) == null);
    try std.testing.expect(Form.combine(Form.literal(std.math.maxInt(i64)), Form.literal(1), .add) == null);
    try std.testing.expect(Form.literal(std.math.minInt(i64)).negative() == null);
}

test "symbolic affine choices retain dependency and conservative signs" {
    const x = Form.column(0);
    const choice = Form.choice(x, Form.combine(x, Form.literal(1), .add).?).?;
    try std.testing.expect(choice.symbolic and choice.oneSign());
    try std.testing.expectEqual(@as(?usize, 0), choice.field);
    try std.testing.expect(!Form.choice(x, x.negative().?).?.oneSign());
    try std.testing.expect(Form.choice(x, Form.column(1)) == null);
    try std.testing.expect(Form.combine(choice, choice, .multiply) == null);
    try std.testing.expect(Form.combine(choice, Form.literal(-2), .multiply).?.oneSign());
    try std.testing.expect(!Form.combine(x, Form.invariant(), .add).?.oneSign());
}

test "independent first moments require total bounded arithmetic" {
    const x = Form.column(0).bounded(.{ .low = 0, .high = 1024 });
    const y = Form.column(1).bounded(.{ .low = 0, .high = 1024 });
    const added = Form.combineMarginals(x, y, .add).?;
    try std.testing.expect(added.multiple and added.symbolic and added.oneSign());
    try std.testing.expectEqual(@as(i64, 2048), added.interval.?.high);
    try std.testing.expect(Form.combine(x, y, .add) == null);
    try std.testing.expect(Form.combineMarginals(x, y, .multiply) == null);
    const difference = Form.combineMarginals(x, y, .subtract).?;
    try std.testing.expect(difference.multiple and !difference.oneSign());
    try std.testing.expectEqual(@as(i64, -1024), difference.interval.?.low);
    const unsafe = Form.combine(x, Form.literal(std.math.maxInt(i64)), .add).?;
    try std.testing.expect(unsafe.interval == null);
    try std.testing.expect(Form.combineMarginals(unsafe, y, .subtract) == null);
    try std.testing.expect(Form.combineMarginals(added, Form.literal(std.math.maxInt(i64)), .multiply) == null);
    try std.testing.expect(Form.combineMarginals(x, Form.column(1), .add) == null);
}
