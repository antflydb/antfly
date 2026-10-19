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

//! Checked integer polynomial certificates and immutable path statistics.
//! Coefficients describe geometry; no independent marginal is treated as a
//! joint distribution. Every original arithmetic intermediate is certified.
const std = @import("std");
const A = std.mem.Allocator;
const Count = @import("multiplicity.zig").Count;
const Interval = @import("linear_bounds.zig").Interval;
pub const Op = enum { add, subtract, multiply };
pub const Kind = enum { count, sum, avg, min, max, distinct };
pub const Form = struct {
    dimensions: usize,
    coefficients: []const i64,
    range: Interval,
    pub fn width(n: usize) usize {
        return 1 + n + n * (n + 1) / 2;
    }
    pub fn pair(n: usize, i: usize, j: usize) usize {
        return 1 + n + j * (j + 1) / 2 + i;
    }
    pub fn literal(a: A, value: i64, n: usize) !Form {
        const c = try a.alloc(i64, width(n));
        @memset(c, 0);
        c[0] = value;
        return .{ .dimensions = n, .coefficients = c, .range = .literal(value) };
    }
    pub fn column(a: A, n: usize, segment: ?usize, depth: i64) !Form {
        var f = try literal(a, 0, n);
        const c = @constCast(f.coefficients);
        for (0..n) |i| c[1 + i] = @intFromBool(segment == null or segment.? == i);
        f.range = .{ .low = 0, .high = depth };
        return f;
    }
    pub fn quadratic(self: Form) bool {
        for (self.coefficients[1 + self.dimensions ..]) |c| if (c != 0) return true;
        return false;
    }
    pub fn total(self: Form) bool {
        if (self.quadratic()) return false;
        for (self.coefficients[1 .. 1 + self.dimensions]) |c| if (c != self.coefficients[1]) return false;
        return true;
    }
    pub fn negative(self: Form, a: A) !?Form {
        return combine(try literal(a, 0, self.dimensions), self, a, .subtract);
    }
    pub fn combine(l: Form, r: Form, a: A, op: Op) !?Form {
        if (l.dimensions != r.dimensions) return null;
        const range = Interval.combine(l.range, r.range, @fromBackingInt(@intCast(@backingInt(op)))) orelse return null;
        const n = l.dimensions;
        var f = try literal(a, 0, n);
        f.range = range;
        const c = @constCast(f.coefficients);
        if (op != .multiply) {
            for (l.coefficients, r.coefficients, c) |lc, rc, *out| out.* = std.math.cast(i64, if (op == .add) @as(i128, lc) + rc else @as(i128, lc) - rc) orelse return null;
            return f;
        }
        // Multiplication by a constant also preserves an existing quadratic.
        for (l.coefficients, 0..) |lc, li| if (lc != 0) {
            for (r.coefficients, 0..) |rc, ri| if (rc != 0) {
                const index = if (li == 0) ri else if (ri == 0) li else if (li <= n and ri <= n) pair(n, @min(li, ri) - 1, @max(li, ri) - 1) else return null;
                c[index] = std.math.cast(i64, @as(i128, c[index]) + @as(i128, lc) * rc) orelse return null;
            };
        };
        return f;
    }
    pub fn eligible(self: Form, kind: Kind) bool {
        if ((kind == .min or kind == .max or kind == .distinct) and self.quadratic()) return false;
        if (kind == .sum and self.range.low < 0 and self.range.high > 0) return false;
        if (kind == .avg and (@abs(@as(i128, self.range.low)) > 1 << 53 or @abs(@as(i128, self.range.high)) > 1 << 53)) return false;
        // A bounded bitset preserves holes rather than inventing intervals.
        if (kind == .distinct and @as(i128, self.range.high) - self.range.low > 65536) return false;
        return true;
    }
};
pub const Request = struct { form: Form, kind: Kind };
pub const Plan = struct {
    requests: []const Request,
    dimensions: usize,
    moments: bool,
    quadratic: bool,
    ptr: ?*anyopaque = null,
    charge: ?*const fn (*anyopaque, usize) anyerror!void = null,
    pub fn init(requests: []const Request) Plan {
        var result: Plan = .{ .requests = requests, .dimensions = requests[0].form.dimensions, .moments = false, .quadratic = false };
        for (requests) |r| {
            result.moments = result.moments or r.kind == .avg or r.kind == .sum or r.kind == .count;
            result.quadratic = result.quadratic or r.form.quadratic();
        }
        return result;
    }
    fn work(self: Plan, amount: usize) !void {
        if (self.charge) |f| try f(self.ptr.?, amount);
    }
};
pub const Signed = struct {
    positive: Count = Count.init(0),
    negative: Count = Count.init(0),
    pub fn add(self: *Signed, a: A, amount: Count, coefficient: i64) !void {
        if (coefficient == 0) return;
        const value = try amount.multiply(a, Count.init(@intCast(@abs(@as(i128, coefficient)))));
        if (coefficient < 0) self.negative = try self.negative.add(a, value) else self.positive = try self.positive.add(a, value);
    }
    pub fn magnitude(self: Signed, a: A) !struct { amount: Count, negative: bool } {
        return if (self.positive.order(self.negative) == .lt) .{ .amount = try self.negative.difference(a, self.positive), .negative = true } else .{ .amount = try self.positive.difference(a, self.negative), .negative = false };
    }
};
pub const Support = struct {
    low: i64 = 0,
    words: []const u64 = &.{1},
    pub fn merge(l: Support, a: A, r: Support, plan: Plan) !Support {
        const low = @min(l.low, r.low);
        const high = @max(l.low + @as(i64, @intCast(l.words.len * 64)), r.low + @as(i64, @intCast(r.words.len * 64)));
        const count: usize = @intCast(@divTrunc(high - low + 63, 64));
        if (count > 1027) return error.CypherStatisticDeclined;
        try plan.work(count + l.words.len + r.words.len);
        const words = try a.alloc(u64, count);
        @memset(words, 0);
        for ([_]Support{ l, r }) |s| {
            const shift: usize = @intCast(s.low - low);
            const index = shift / 64;
            const bits: u6 = @intCast(shift % 64);
            for (s.words, 0..) |w, i| {
                words[index + i] |= w << bits;
                if (bits != 0 and index + i + 1 < words.len) words[index + i + 1] |= w >> @as(u6, @intCast(64 - @as(u7, bits)));
            }
        }
        // Trim slack so repeated unions do not grow the represented interval.
        var end = words.len;
        while (end > 1 and words[end - 1] == 0) end -= 1;
        return .{ .low = low, .words = words[0..end] };
    }
};
pub const Extrema = struct { min: i128 = 0, max: i128 = 0 };
pub const Value = struct {
    count: Count,
    sums: []const Count = &.{},
    crosses: []const Count = &.{},
    extrema: []const Extrema = &.{},
    supports: []const Support = &.{},
    pub fn init(c: Count) Value {
        return .{ .count = c };
    }
    fn sum(self: Value, i: usize) Count {
        return if (i < self.sums.len) self.sums[i] else Count.init(0);
    }
    fn cross(self: Value, i: usize) Count {
        return if (i < self.crosses.len) self.crosses[i] else Count.init(0);
    }
    pub fn extreme(self: Value, i: usize) Extrema {
        return if (i < self.extrema.len) self.extrema[i] else .{};
    }
    pub fn support(self: Value, i: usize) Support {
        return if (i < self.supports.len) self.supports[i] else .{};
    }
    pub fn add(l: Value, a: A, r: Value, plan: Plan) !Value {
        if (l.count.cap(1) == 0) return r;
        if (r.count.cap(1) == 0) return l;
        try plan.work(plan.dimensions + plan.requests.len + @max(l.crosses.len, r.crosses.len));
        var out = init(if (plan.moments) try l.count.add(a, r.count) else Count.one);
        const sums = try a.alloc(Count, @max(l.sums.len, r.sums.len));
        for (sums, 0..) |*v, i| v.* = try l.sum(i).add(a, r.sum(i));
        out.sums = sums;
        const crosses = try a.alloc(Count, @max(l.crosses.len, r.crosses.len));
        for (crosses, 0..) |*v, i| v.* = try l.cross(i).add(a, r.cross(i));
        out.crosses = crosses;
        const ext = try a.alloc(Extrema, plan.requests.len);
        const supports = try a.alloc(Support, plan.requests.len);
        for (plan.requests, ext, supports, 0..) |request, *e, *s, i| {
            const le = l.extreme(i);
            const re = r.extreme(i);
            e.* = .{ .min = @min(le.min, re.min), .max = @max(le.max, re.max) };
            s.* = if (request.kind == .distinct) try Support.merge(l.support(i), a, r.support(i), plan) else .{};
        }
        out.extrema = ext;
        out.supports = supports;
        return out;
    }
    pub fn multiply(l: Value, a: A, r: Value, plan: Plan) !Value {
        if (l.count.cap(1) == 0 or r.count.cap(1) == 0) return init(Count.init(0));
        if (r.sums.len == 0 and r.crosses.len == 0 and r.extrema.len == 0 and r.supports.len == 0 and r.count.exact() == 1) return l;
        if (l.sums.len == 0 and l.crosses.len == 0 and l.extrema.len == 0 and l.supports.len == 0 and l.count.exact() == 1) return r;
        try plan.work(plan.dimensions + plan.requests.len + @max(l.crosses.len, r.crosses.len));
        var out = init(if (plan.moments) try l.count.multiply(a, r.count) else Count.one);
        const sums = try a.alloc(Count, @max(l.sums.len, r.sums.len));
        for (sums, 0..) |*v, i| v.* = try (try l.sum(i).multiply(a, r.count)).add(a, try r.sum(i).multiply(a, l.count));
        out.sums = sums;
        if (plan.quadratic) {
            const crosses = try a.alloc(Count, plan.dimensions * (plan.dimensions + 1) / 2);
            for (0..plan.dimensions) |j| for (0..j + 1) |i| {
                const k = j * (j + 1) / 2 + i;
                crosses[k] = try (try (try l.cross(k).multiply(a, r.count)).add(a, try r.cross(k).multiply(a, l.count))).add(a, try (try l.sum(i).multiply(a, r.sum(j))).add(a, try l.sum(j).multiply(a, r.sum(i))));
            };
            out.crosses = crosses;
        }
        const ext = try a.alloc(Extrema, plan.requests.len);
        const supports = try a.alloc(Support, plan.requests.len);
        for (plan.requests, ext, supports, 0..) |request, *e, *s, index| {
            const le = l.extreme(index);
            const re = r.extreme(index);
            e.* = .{ .min = std.math.add(i128, le.min, re.min) catch return error.CypherStatisticDeclined, .max = std.math.add(i128, le.max, re.max) catch return error.CypherStatisticDeclined };
            s.* = .{};
            if (request.kind == .distinct) {
                const ls = l.support(index);
                const rs = r.support(index);
                var result: ?Support = null;
                for (ls.words, 0..) |word, wi| {
                    var bits = word;
                    while (bits != 0) : (bits &= bits - 1) {
                        try plan.work(1);
                        var shifted = rs;
                        shifted.low += ls.low + @as(i64, @intCast(wi * 64 + @ctz(bits)));
                        result = if (result) |prior| try Support.merge(prior, a, shifted, plan) else shifted;
                    }
                }
                s.* = result.?;
            }
        }
        out.extrema = ext;
        out.supports = supports;
        return out;
    }
    pub fn advance(self: Value, a: A, edge: usize, plan: Plan) !Value {
        try plan.work(plan.dimensions + plan.requests.len + if (plan.quadratic) plan.dimensions * (plan.dimensions + 1) / 2 else @as(usize, 0));
        var out = self;
        if (plan.moments) {
            const sums = try a.alloc(Count, plan.dimensions);
            for (sums, 0..) |*v, i| v.* = if (i == edge) try self.sum(i).add(a, self.count) else self.sum(i);
            out.sums = sums;
            if (plan.quadratic) {
                const crosses = try a.alloc(Count, plan.dimensions * (plan.dimensions + 1) / 2);
                for (0..plan.dimensions) |j| for (0..j + 1) |i| {
                    const k = j * (j + 1) / 2 + i;
                    var c = self.cross(k);
                    if (i == edge) c = try c.add(a, self.sum(j));
                    if (j == edge) c = try c.add(a, self.sum(i));
                    if (i == edge and j == edge) c = try c.add(a, self.count);
                    crosses[k] = c;
                };
                out.crosses = crosses;
            }
        }
        const ext = try a.alloc(Extrema, plan.requests.len);
        const supports = try a.alloc(Support, plan.requests.len);
        for (plan.requests, ext, supports, 0..) |request, *e, *s, i| {
            const cost = request.form.coefficients[1 + edge];
            const prior = self.extreme(i);
            e.* = .{ .min = std.math.add(i128, prior.min, cost) catch return error.CypherStatisticDeclined, .max = std.math.add(i128, prior.max, cost) catch return error.CypherStatisticDeclined };
            s.* = self.support(i);
            if (request.kind == .distinct) s.low = std.math.add(i64, s.low, cost) catch return error.CypherStatisticDeclined;
        }
        out.extrema = ext;
        out.supports = supports;
        return out;
    }
    pub fn total(self: Value, a: A, form: Form) !Signed {
        var result: Signed = .{};
        try result.add(a, self.count, form.coefficients[0]);
        for (0..form.dimensions) |i| try result.add(a, self.sum(i), form.coefficients[1 + i]);
        if (form.quadratic()) for (0..form.dimensions) |j| for (0..j + 1) |i| try result.add(a, self.cross(j * (j + 1) / 2 + i), form.coefficients[Form.pair(form.dimensions, i, j)]);
        return result;
    }
};

test "statistics retain cross moments, signed costs and holes through composition" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const x = try Form.column(a, 2, 0, 10);
    const y = try Form.column(a, 2, 1, 10);
    const xx = (try Form.combine(x, x, a, .multiply)).?;
    const xy = (try Form.combine(x, y, a, .multiply)).?;
    const polynomial = (try Form.combine(xx, xy, a, .add)).?;
    const direction = (try Form.combine(x, y, a, .subtract)).?;
    const requests = [_]Request{ .{ .form = polynomial, .kind = .avg }, .{ .form = direction, .kind = .min }, .{ .form = direction, .kind = .distinct } };
    const plan = Plan.init(&requests);
    const one = Value.init(Count.one);
    const first = try (try one.advance(a, 0, plan)).advance(a, 0, plan); // (2,0)
    const second = try (try one.advance(a, 0, plan)).advance(a, 1, plan); // (1,1)
    const bag = try first.add(a, second, plan);
    const total = try (try bag.total(a, polynomial)).magnitude(a);
    try std.testing.expectEqual(@as(?u64, 6), total.amount.exact());
    try std.testing.expectEqual(@as(i128, 0), bag.extreme(1).min);
    try std.testing.expectEqual(@as(i128, 2), bag.extreme(1).max);
    const support = bag.support(2);
    try std.testing.expectEqual(@as(i64, 0), support.low);
    try std.testing.expectEqual(@as(u64, 5), support.words[0]);
    const product = try second.multiply(a, first, plan); // (3,1), x*x+x*y=12
    try std.testing.expectEqual(@as(?u64, 12), (try (try product.total(a, polynomial)).magnitude(a)).amount.exact());
    try std.testing.expect((try Form.combine(xx, x, a, .multiply)) == null);
    const max = try Form.literal(a, std.math.maxInt(i64), 2);
    try std.testing.expect((try Form.combine(max, x, a, .add)) == null);
}
