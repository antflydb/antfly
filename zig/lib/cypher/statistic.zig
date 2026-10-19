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
/// A row in a negotiated summary stream contributes to exactly one reducer.
/// The producer assigns a request ordinal; relation adapters map it to the
/// owning aggregate slot. It never denotes an ordinary SQL column.
pub const SupportInput = struct {
    value: Support,
    offset: i64 = 0,
    pub fn valid(self: SupportInput) bool {
        if (self.value.words.len == 0 or self.value.words.len > 1027) return false;
        var present = false;
        for (self.value.words, 0..) |word, i| {
            if (word == 0) continue;
            present = true;
            const base = @as(i128, self.value.low) + self.offset + @as(i128, i) * 64;
            if (base + @ctz(word) < std.math.minInt(i64) or base + 63 - @clz(word) > std.math.maxInt(i64)) return false;
        }
        return present;
    }
};
pub const Input = struct {
    slot: usize,
    value: i64 = 0,
    support: ?SupportInput = null,
    pub fn clone(self: Input, a: A) !Input {
        var result = self;
        if (self.support) |support_| {
            result.support.?.value.words = try a.dupe(u64, support_.value.words);
        }
        return result;
    }
};
pub const Plan = struct {
    pub const Slot = struct { extreme: ?usize = null, support: ?usize = null };
    requests: []const Request,
    dimensions: usize,
    moments: bool,
    quadratic: bool,
    /// Dense physical state is independent of the number of moment reducers.
    extrema: []const []const i64,
    supports: []const []const i64,
    slots: []const Slot,
    ptr: ?*anyopaque = null,
    charge: ?*const fn (*anyopaque, usize) anyerror!void = null,
    pub fn init(a: A, requests: []const Request) !Plan {
        const n = requests[0].form.dimensions;
        const slots = try a.alloc(Slot, requests.len);
        errdefer a.free(slots);
        var extrema: std.ArrayList([]const i64) = .empty;
        defer extrema.deinit(a);
        var supports: std.ArrayList([]const i64) = .empty;
        defer supports.deinit(a);
        var moments = false;
        var quadratic = false;
        for (requests, slots) |r, *slot| {
            slot.* = .{};
            moments = moments or r.kind == .avg or r.kind == .sum or r.kind == .count;
            quadratic = quadratic or r.form.quadratic();
            // Constants are applied at emission. MIN/MAX of shifted forms and
            // DISTINCT consumers with different offsets share a cost direction.
            const coefficients = r.form.coefficients[1 .. 1 + n];
            if (r.kind == .min or r.kind == .max) slot.extreme = try intern(a, &extrema, coefficients);
            if (r.kind == .distinct) slot.support = try intern(a, &supports, coefficients);
        }
        const ext = try extrema.toOwnedSlice(a);
        errdefer a.free(ext);
        return .{ .requests = requests, .dimensions = n, .moments = moments, .quadratic = quadratic, .extrema = ext, .supports = try supports.toOwnedSlice(a), .slots = slots };
    }
    fn intern(a: A, directions_: *std.ArrayList([]const i64), coefficients: []const i64) !usize {
        for (directions_.items, 0..) |prior, index| if (std.mem.eql(i64, prior, coefficients)) return index;
        try directions_.append(a, coefficients);
        return directions_.items.len - 1;
    }
    fn work(self: Plan, amount: usize) !void {
        if (self.charge) |f| try f(self.ptr.?, amount);
    }
    fn directions(self: Plan) usize {
        return self.extrema.len + self.supports.len;
    }
};

/// Sparse aligned words keep grouped supports compact even when correlated
/// offsets are far apart. Inserting a support costs words, not distinct values.
pub const SupportSet = struct {
    words: std.AutoHashMapUnmanaged(i64, u64) = .empty,
    pub fn deinit(self: *SupportSet, a: A) void {
        self.words.deinit(a);
    }
    fn putWord(self: *SupportSet, a: A, index: i64, bits: u64) !u64 {
        if (bits == 0) return 0;
        const entry = try self.words.getOrPut(a, index);
        if (!entry.found_existing) entry.value_ptr.* = 0;
        const added: u64 = @popCount(bits & ~entry.value_ptr.*);
        entry.value_ptr.* |= bits;
        return added;
    }
    pub fn insert(self: *SupportSet, a: A, value: i64) !bool {
        return try self.putWord(a, @divFloor(value, 64), @as(u64, 1) << @as(u6, @intCast(@mod(value, 64)))) != 0;
    }
    pub fn add(self: *SupportSet, a: A, support: Support, offset: i64) !u64 {
        const base = @as(i128, support.low) + offset;
        const first: i64 = @intCast(@divFloor(base, 64));
        const shift: u6 = @intCast(@mod(base, 64));
        var added: u64 = 0;
        for (support.words, 0..) |word, i| {
            const index = first + @as(i64, @intCast(i));
            added += try self.putWord(a, index, word << shift);
            if (shift != 0) added += try self.putWord(a, index + 1, word >> @as(u6, @intCast(64 - @as(u7, shift))));
        }
        return added;
    }
    pub fn merge(self: *SupportSet, a: A, other: SupportSet) !u64 {
        var added: u64 = 0;
        var entries = other.words.iterator();
        while (entries.next()) |entry| added += try self.putWord(a, entry.key_ptr.*, entry.value_ptr.*);
        return added;
    }
    /// Peak allocation for inserting additional words, including the old hash
    /// table while a replacement is allocated. Used before aggregate spilling.
    pub fn growth(self: SupportSet, other: SupportSet) usize {
        var additional: usize = 0;
        var entries = other.words.keyIterator();
        while (entries.next()) |key| additional += @intFromBool(!self.words.contains(key.*));
        return self.growthFor(additional);
    }
    pub fn insertionGrowth(self: SupportSet, value: i64) usize {
        return self.growthFor(@intFromBool(!self.words.contains(@divFloor(value, 64))));
    }
    fn growthFor(self: SupportSet, additional: usize) usize {
        if (self.words.count() + additional <= @as(usize, self.words.capacity()) * 80 / 100) return 0;
        return (self.words.count() + additional) *| 64 +| 256;
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
    pub fn extreme(self: Value, plan: Plan, request: usize) Extrema {
        return self.extremeSlot(plan.slots[request].extreme.?);
    }
    fn extremeSlot(self: Value, i: usize) Extrema {
        return if (i < self.extrema.len) self.extrema[i] else .{};
    }
    pub fn support(self: Value, plan: Plan, request: usize) Support {
        return self.supportSlot(plan.slots[request].support.?);
    }
    fn supportSlot(self: Value, i: usize) Support {
        return if (i < self.supports.len) self.supports[i] else .{};
    }
    pub fn add(l: Value, a: A, r: Value, plan: Plan) !Value {
        if (l.count.cap(1) == 0) return r;
        if (r.count.cap(1) == 0) return l;
        try plan.work(plan.dimensions + plan.directions() + @max(l.crosses.len, r.crosses.len));
        var out = init(if (plan.moments) try l.count.add(a, r.count) else Count.one);
        const sums = try a.alloc(Count, @max(l.sums.len, r.sums.len));
        for (sums, 0..) |*v, i| v.* = try l.sum(i).add(a, r.sum(i));
        out.sums = sums;
        const crosses = try a.alloc(Count, @max(l.crosses.len, r.crosses.len));
        for (crosses, 0..) |*v, i| v.* = try l.cross(i).add(a, r.cross(i));
        out.crosses = crosses;
        const ext = try a.alloc(Extrema, plan.extrema.len);
        const supports = try a.alloc(Support, plan.supports.len);
        for (ext, 0..) |*e, i| {
            const le = l.extremeSlot(i);
            const re = r.extremeSlot(i);
            e.* = .{ .min = @min(le.min, re.min), .max = @max(le.max, re.max) };
        }
        for (supports, 0..) |*support_, i| support_.* = try Support.merge(l.supportSlot(i), a, r.supportSlot(i), plan);
        out.extrema = ext;
        out.supports = supports;
        return out;
    }
    pub fn multiply(l: Value, a: A, r: Value, plan: Plan) !Value {
        if (l.count.cap(1) == 0 or r.count.cap(1) == 0) return init(Count.init(0));
        if (r.sums.len == 0 and r.crosses.len == 0 and r.extrema.len == 0 and r.supports.len == 0 and r.count.exact() == 1) return l;
        if (l.sums.len == 0 and l.crosses.len == 0 and l.extrema.len == 0 and l.supports.len == 0 and l.count.exact() == 1) return r;
        try plan.work(plan.dimensions + plan.directions() + @max(l.crosses.len, r.crosses.len));
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
        const ext = try a.alloc(Extrema, plan.extrema.len);
        const supports = try a.alloc(Support, plan.supports.len);
        for (ext, 0..) |*e, index| {
            const le = l.extremeSlot(index);
            const re = r.extremeSlot(index);
            e.* = .{ .min = std.math.add(i128, le.min, re.min) catch return error.CypherStatisticDeclined, .max = std.math.add(i128, le.max, re.max) catch return error.CypherStatisticDeclined };
        }
        for (supports, 0..) |*support_, index| {
            const ls = l.supportSlot(index);
            const rs = r.supportSlot(index);
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
            support_.* = result.?;
        }
        out.extrema = ext;
        out.supports = supports;
        return out;
    }
    pub fn advance(self: Value, a: A, edge: usize, plan: Plan) !Value {
        try plan.work(plan.dimensions + plan.directions() + if (plan.quadratic) plan.dimensions * (plan.dimensions + 1) / 2 else @as(usize, 0));
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
        const ext = try a.alloc(Extrema, plan.extrema.len);
        const supports = try a.alloc(Support, plan.supports.len);
        for (plan.extrema, ext, 0..) |coefficients, *e, i| {
            const cost = coefficients[edge];
            const prior = self.extremeSlot(i);
            e.* = .{ .min = std.math.add(i128, prior.min, cost) catch return error.CypherStatisticDeclined, .max = std.math.add(i128, prior.max, cost) catch return error.CypherStatisticDeclined };
        }
        for (plan.supports, supports, 0..) |coefficients, *support_, i| {
            support_.* = self.supportSlot(i);
            support_.low = std.math.add(i64, support_.low, coefficients[edge]) catch return error.CypherStatisticDeclined;
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
    const plan = try Plan.init(a, &requests);
    const one = Value.init(Count.one);
    const first = try (try one.advance(a, 0, plan)).advance(a, 0, plan); // (2,0)
    const second = try (try one.advance(a, 0, plan)).advance(a, 1, plan); // (1,1)
    const bag = try first.add(a, second, plan);
    const total = try (try bag.total(a, polynomial)).magnitude(a);
    try std.testing.expectEqual(@as(?u64, 6), total.amount.exact());
    try std.testing.expectEqual(@as(i128, 0), bag.extreme(plan, 1).min);
    try std.testing.expectEqual(@as(i128, 2), bag.extreme(plan, 1).max);
    const support = bag.support(plan, 2);
    try std.testing.expectEqual(@as(i64, 0), support.low);
    try std.testing.expectEqual(@as(u64, 5), support.words[0]);
    const product = try second.multiply(a, first, plan); // (3,1), x*x+x*y=12
    try std.testing.expectEqual(@as(?u64, 12), (try (try product.total(a, polynomial)).magnitude(a)).amount.exact());
    try std.testing.expect((try Form.combine(xx, x, a, .multiply)) == null);
    const max = try Form.literal(a, std.math.maxInt(i64), 2);
    try std.testing.expect((try Form.combine(max, x, a, .add)) == null);
}

test "sparse supports preserve holes, overlaps and distant signed offsets" {
    var set: SupportSet = .{};
    defer set.deinit(std.testing.allocator);
    const a = std.testing.allocator;
    const support: Support = .{ .low = -1, .words = &.{0b10101} };
    try std.testing.expectEqual(@as(u64, 3), try set.add(a, support, 0));
    try std.testing.expectEqual(@as(u64, 0), try set.add(a, support, 0));
    try std.testing.expect(!try set.insert(a, -1));
    try std.testing.expect(try set.insert(a, 0));
    try std.testing.expectEqual(@as(u64, 3), try set.add(a, support, 10000000000));
    try std.testing.expectEqual(@as(u64, 3), try set.add(a, support, -10000000000));
    try std.testing.expectEqual(@as(u64, 1), try set.add(a, .{ .low = 0, .words = &.{1} }, std.math.maxInt(i64)));
    try std.testing.expectEqual(@as(u64, 1), try set.add(a, .{ .low = 0, .words = &.{1} }, std.math.minInt(i64)));
    try std.testing.expect(set.words.count() < 12);
}

test "direction slots deduplicate offsets without tracking moment consumers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const x = try Form.column(a, 2, 0, 20);
    const y = try Form.column(a, 2, 1, 20);
    const xx = (try Form.combine(x, x, a, .multiply)).?;
    const offset = (try Form.combine(x, try Form.literal(a, 123456789, 2), a, .add)).?;
    const requests = [_]Request{
        .{ .form = xx, .kind = .avg },
        .{ .form = y, .kind = .avg },
        .{ .form = x, .kind = .min },
        .{ .form = offset, .kind = .max },
        .{ .form = x, .kind = .distinct },
        .{ .form = offset, .kind = .distinct },
        .{ .form = y, .kind = .distinct },
    };
    const plan = try Plan.init(a, &requests);
    try std.testing.expectEqual(@as(usize, 1), plan.extrema.len);
    try std.testing.expectEqual(@as(usize, 2), plan.supports.len);
    const one = Value.init(Count.one);
    const first = try (try one.advance(a, 0, plan)).advance(a, 0, plan);
    const second = try one.advance(a, 1, plan);
    const bag = try first.add(a, second, plan);
    try std.testing.expectEqual(@as(i128, 0), bag.extreme(plan, 2).min);
    try std.testing.expectEqual(@as(i128, 2), bag.extreme(plan, 3).max);
    try std.testing.expectEqualSlices(u64, bag.support(plan, 4).words, bag.support(plan, 5).words);
    try std.testing.expectEqual(@as(u64, 5), bag.support(plan, 4).words[0]);
    try std.testing.expectEqual(@as(u64, 3), bag.support(plan, 6).words[0]);
    const product = try first.multiply(a, second, plan);
    try std.testing.expectEqual(@as(i128, 2), product.extreme(plan, 3).max);
}
