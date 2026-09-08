//! Bounded, deterministic serving-layout plans for immutable cosine postings.
//!
//! `rows` maps serving positions to canonical posting positions. It is a
//! permutation, not new vector ownership: the writer may reorder existing
//! candidate columns and undo that order when reconstructing a WAL patch.
//! Representatives are source-space routing hints, never pruning certificates.
//! The enclosing immutable generation owns identity, checksums, and leases.

const std = @import("std");
const Allocator = std.mem.Allocator;
pub const max_groups = 16;
pub const max_rows = 4096;
pub const max_dims = 4096;
const header_size = 24;

pub const Cancellation = struct {
    context: *const anyopaque,
    cancelled: *const fn (*const anyopaque) bool,

    fn check(self: ?Cancellation) !void {
        if (self) |token| if (token.cancelled(token.context)) return error.Cancelled;
    }
};

pub const View = struct {
    dims: usize,
    rows: []const u32,
    ends: []const u32,
    centers: []const f32,
    /// Optional generation-owned acceleration; not encoded in AFSG or a bound.
    compact: ?@import("compact_subgroups.zig").View = null,

    pub fn range(self: View, group: usize) struct { start: usize, end: usize } {
        return .{ .start = if (group == 0) 0 else self.ends[group - 1], .end = self.ends[group] };
    }

    /// Exact representative scoring; this does NOT certify excluded members.
    /// Stable group order resolves ties. Caller supplies fixed-size scratch.
    pub fn rank(self: View, query: []const f32, order: []u8, scores: []f64) !void {
        if (query.len != self.dims or order.len < self.ends.len or scores.len < self.ends.len)
            return error.InvalidSubgroupPlan;
        for (query) |value| if (!std.math.isFinite(value)) return error.InvalidSubgroupPlan;
        for (0..self.ends.len) |group| {
            var dot: f64 = 0;
            for (query, self.centers[group * self.dims ..][0..self.dims]) |q, c| dot += @as(f64, q) * c;
            scores[group] = -dot;
            var position = group;
            while (position > 0 and scores[order[position - 1]] > scores[group]) : (position -= 1)
                order[position] = order[position - 1];
            order[position] = @intCast(group);
        }
    }

    pub fn validate(self: View) !void {
        if (self.dims == 0 or self.dims > max_dims or self.rows.len == 0 or self.rows.len > max_rows or
            self.ends.len == 0 or self.ends.len > max_groups or
            self.centers.len != self.ends.len * self.dims) return error.InvalidSubgroupPlan;
        var seen: [max_rows / 64]u64 = @splat(0);
        for (self.rows) |row| {
            if (row >= self.rows.len) return error.InvalidSubgroupPlan;
            const bit = @as(u64, 1) << @as(u6, @intCast(row % 64));
            if (seen[row / 64] & bit != 0) return error.InvalidSubgroupPlan;
            seen[row / 64] |= bit;
        }
        var start: usize = 0;
        for (self.ends) |end| {
            if (end <= start or end > self.rows.len) return error.InvalidSubgroupPlan;
            start = end;
        }
        if (start != self.rows.len) return error.InvalidSubgroupPlan;
        for (self.centers) |value| if (!std.math.isFinite(value)) return error.InvalidSubgroupPlan;
    }

    /// Little-endian, length-framed extension. No native ABI padding is stored.
    /// Callers authenticate this together with the candidate rows it permutes.
    pub fn encode(self: View, alloc: Allocator) ![]u8 {
        try self.validate();
        const length = header_size + (self.rows.len + self.ends.len + self.centers.len) * 4;
        const bytes = try alloc.alloc(u8, length);
        @memset(bytes, 0);
        @memcpy(bytes[0..4], "AFSG");
        put(bytes, 4, 1);
        put(bytes, 8, @intCast(length));
        put(bytes, 12, @intCast(self.dims));
        put(bytes, 16, @intCast(self.rows.len));
        put(bytes, 20, @intCast(self.ends.len));
        var cursor: usize = header_size;
        for (self.rows) |row| {
            put(bytes, cursor, row);
            cursor += 4;
        }
        for (self.ends) |end| {
            put(bytes, cursor, end);
            cursor += 4;
        }
        for (self.centers) |center| {
            put(bytes, cursor, @bitCast(center));
            cursor += 4;
        }
        return bytes;
    }
};

pub const Plan = struct {
    alloc: Allocator,
    view: View,

    pub fn deinit(self: *Plan) void {
        self.alloc.free(self.view.rows);
        self.alloc.free(self.view.ends);
        self.alloc.free(self.view.centers);
        self.* = undefined;
    }

    /// Recursive balanced spherical bisection. Training uses only this leaf's
    /// source vectors, never queries or neighbors. Every split keeps equal
    /// row counts (within one), including duplicates and degenerate geometry.
    pub fn build(alloc: Allocator, vectors: []const f32, dims: usize, groups: usize, cancellation: ?Cancellation) !Plan {
        try Cancellation.check(cancellation);
        if (dims == 0 or dims > max_dims or vectors.len == 0 or vectors.len % dims != 0 or
            groups == 0 or groups > max_groups or !std.math.isPowerOfTwo(groups)) return error.InvalidSubgroupPlan;
        const count = vectors.len / dims;
        if (count > max_rows or groups > count) return error.InvalidSubgroupPlan;
        const normalized = try alloc.alloc(f32, vectors.len);
        defer alloc.free(normalized);
        for (0..count) |row| {
            try Cancellation.check(cancellation);
            var norm: f64 = 0;
            for (vectors[row * dims ..][0..dims]) |value| {
                if (!std.math.isFinite(value)) return error.InvalidSubgroupPlan;
                norm += @as(f64, value) * value;
            }
            if (norm == 0) return error.InvalidSubgroupPlan;
            const scale = 1 / @sqrt(norm);
            for (normalized[row * dims ..][0..dims], vectors[row * dims ..][0..dims]) |*out, value|
                out.* = @floatCast(value * scale);
        }
        const rows = try alloc.alloc(u32, count);
        errdefer alloc.free(rows);
        for (rows, 0..) |*row, i| row.* = @intCast(i);
        const ends = try alloc.alloc(u32, groups);
        errdefer alloc.free(ends);
        const centers = try alloc.alloc(f32, groups * dims);
        errdefer alloc.free(centers);
        const scores = try alloc.alloc(f64, count);
        defer alloc.free(scores);
        const sums = try alloc.alloc(f64, dims * 2);
        defer alloc.free(sums);
        var trainer: Trainer = .{ .vectors = normalized, .dims = dims, .scores = scores, .sums = sums, .cancellation = cancellation };
        try trainer.split(rows, 0, groups, ends);
        for (ends, 0..) |end, group| {
            const start = if (group == 0) 0 else ends[group - 1];
            // Canonical order inside each physical range makes construction
            // deterministic and minimizes changes to equal-score insertion.
            std.mem.sort(u32, rows[start..end], {}, std.sort.asc(u32));
            trainer.mean(rows[start..end], sums[0..dims]);
            for (centers[group * dims ..][0..dims], sums[0..dims]) |*out, value| out.* = @floatCast(value);
        }
        return .{ .alloc = alloc, .view = .{ .dims = dims, .rows = rows, .ends = ends, .centers = centers } };
    }

    /// Allocating decoder for mutation/build paths. Mmap serving can use the
    /// aligned decoder below; both reject non-bijective row maps.
    pub fn decode(alloc: Allocator, bytes: []const u8) !Plan {
        const shape = try frame(bytes);
        const rows = try alloc.alloc(u32, shape.count);
        errdefer alloc.free(rows);
        const ends = try alloc.alloc(u32, shape.groups);
        errdefer alloc.free(ends);
        const centers = try alloc.alloc(f32, shape.groups * shape.dims);
        errdefer alloc.free(centers);
        var cursor: usize = header_size;
        for (rows) |*row| {
            row.* = get(bytes, cursor);
            cursor += 4;
        }
        for (ends) |*end| {
            end.* = get(bytes, cursor);
            cursor += 4;
        }
        for (centers) |*center| {
            center.* = @bitCast(get(bytes, cursor));
            cursor += 4;
        }
        const view: View = .{ .dims = shape.dims, .rows = rows, .ends = ends, .centers = centers };
        try view.validate();
        return .{ .alloc = alloc, .view = view };
    }
};

const Trainer = struct {
    vectors: []const f32,
    dims: usize,
    scores: []f64,
    sums: []f64,
    cancellation: ?Cancellation,

    fn mean(self: *Trainer, rows: []const u32, out: []f64) void {
        @memset(out, 0);
        for (rows) |row| for (out, self.vectors[row * self.dims ..][0..self.dims]) |*sum, value| {
            sum.* += value;
        };
        var norm: f64 = 0;
        for (out) |sum| norm += sum * sum;
        // An antipodal group has no preferred direction. Keep a zero center
        // and deterministic rank ties; do not manufacture a pruning bound.
        if (norm > 0) for (out) |*sum| {
            sum.* /= @sqrt(norm);
        };
    }

    fn split(self: *Trainer, rows: []u32, offset: usize, groups: usize, ends: []u32) !void {
        try Cancellation.check(self.cancellation);
        if (groups == 1) {
            ends[0] = @intCast(offset + rows.len);
            return;
        }
        const left = self.sums[0..self.dims];
        const right = self.sums[self.dims..];
        // Farthest-from-first initialization, then balanced Lloyd updates.
        const first = self.vectors[rows[0] * self.dims ..][0..self.dims];
        var farthest = rows[0];
        var smallest: f64 = std.math.inf(f64);
        for (rows) |row| {
            var dot: f64 = 0;
            for (first, self.vectors[row * self.dims ..][0..self.dims]) |a, b| dot += @as(f64, a) * b;
            if (dot < smallest) {
                smallest = dot;
                farthest = row;
            }
        }
        for (left, first) |*out, value| out.* = value;
        for (right, self.vectors[farthest * self.dims ..][0..self.dims]) |*out, value| out.* = value;
        const middle = rows.len / 2;
        for (0..4) |_| {
            try Cancellation.check(self.cancellation);
            for (rows) |row| {
                var score: f64 = 0;
                for (left, right, self.vectors[row * self.dims ..][0..self.dims]) |a, b, value| score += (b - a) * value;
                self.scores[row] = score;
            }
            std.mem.sort(u32, rows, self.scores, struct {
                fn less(scores: []f64, a: u32, b: u32) bool {
                    return scores[a] < scores[b] or (scores[a] == scores[b] and a < b);
                }
            }.less);
            self.mean(rows[0..middle], left);
            self.mean(rows[middle..], right);
        }
        try self.split(rows[0..middle], offset, groups / 2, ends[0 .. groups / 2]);
        try self.split(rows[middle..], offset + middle, groups / 2, ends[groups / 2 ..]);
    }
};

fn put(bytes: []u8, offset: usize, value: u32) void {
    std.mem.writeInt(u32, bytes[offset..][0..4], value, .little);
}
fn get(bytes: []const u8, offset: usize) u32 {
    return std.mem.readInt(u32, bytes[offset..][0..4], .little);
}
fn frame(bytes: []const u8) !struct { dims: usize, count: usize, groups: usize } {
    if (bytes.len < header_size or !std.mem.eql(u8, bytes[0..4], "AFSG") or get(bytes, 4) != 1 or get(bytes, 8) != bytes.len)
        return error.InvalidSubgroupPlan;
    const dims: usize = get(bytes, 12);
    const count: usize = get(bytes, 16);
    const groups: usize = get(bytes, 20);
    if (dims == 0 or dims > max_dims or count == 0 or count > max_rows or groups == 0 or groups > max_groups or
        header_size + (count + groups + groups * dims) * 4 != bytes.len) return error.InvalidSubgroupPlan;
    return .{ .dims = dims, .count = count, .groups = groups };
}

pub fn decodeBorrowed(bytes: []const u8) !View {
    const view = try decodeBorrowedLayout(bytes);
    try view.validate();
    return view;
}

/// Only a validated immutable generation may reuse this structural decoder
/// without repeating the O(rows + groups*dims) semantic validation.
pub fn decodeBorrowedLayout(bytes: []const u8) !View {
    // Native array views are an optimization, not a portable wire assumption.
    if (@import("builtin").cpu.arch.endian() != .little) return error.UnsupportedSubgroupView;
    const shape = try frame(bytes);
    if (@intFromPtr(bytes.ptr) % @alignOf(u32) != 0) return error.InvalidSubgroupPlan;
    const aligned: []align(4) const u8 = @alignCast(bytes);
    const rows_end = header_size + shape.count * 4;
    const ends_end = rows_end + shape.groups * 4;
    const view: View = .{
        .dims = shape.dims,
        .rows = std.mem.bytesAsSlice(u32, aligned[header_size..rows_end]),
        .ends = std.mem.bytesAsSlice(u32, @as([]align(4) const u8, @alignCast(aligned[rows_end..ends_end]))),
        .centers = std.mem.bytesAsSlice(f32, @as([]align(4) const u8, @alignCast(aligned[ends_end..]))),
    };
    return view;
}

fn allocationExercise(alloc: Allocator) !void {
    const vectors = [_]f32{ 1, 0, 0.9, 0.1, 0, 1, 0.1, 0.9, -1, 0, -0.9, -0.1, 0, -1, -0.1, -0.9 };
    var plan = try Plan.build(alloc, &vectors, 2, 4, null);
    defer plan.deinit();
    try plan.view.validate();
    const bytes = try plan.view.encode(alloc);
    defer alloc.free(bytes);
    var restored = try Plan.decode(alloc, bytes);
    defer restored.deinit();
    try std.testing.expectEqualSlices(u32, plan.view.rows, restored.view.rows);
    try std.testing.expectEqualSlices(f32, plan.view.centers, restored.view.centers);
    const borrowed = try decodeBorrowed(bytes);
    try std.testing.expectEqualSlices(u32, plan.view.rows, borrowed.rows);
    var order: [max_groups]u8 = undefined;
    var scores: [max_groups]f64 = undefined;
    try borrowed.rank(&.{ 1, 0 }, &order, &scores);
    try std.testing.expectEqual(@as(u32, 2), borrowed.ends[0]);
}

test "subgroup plans are balanced deterministic and allocation safe" {
    try allocationExercise(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationExercise, .{});
    const vectors = [_]f32{ 1, 0 } ** 17;
    var plan = try Plan.build(std.testing.allocator, &vectors, 2, 16, null);
    defer plan.deinit();
    try plan.view.validate();
    var start: usize = 0;
    for (plan.view.ends) |end| {
        try std.testing.expect(end - start <= 2);
        start = end;
    }
    var repeated = try Plan.build(std.testing.allocator, &vectors, 2, 16, null);
    defer repeated.deinit();
    try std.testing.expectEqualSlices(u32, plan.view.rows, repeated.view.rows);
}

test "subgroup plans reject malformed permutations frames and nonfinite sources" {
    var plan = try Plan.build(std.testing.allocator, &.{ 1, 0, 0, 1, -1, 0, 0, -1 }, 2, 2, null);
    defer plan.deinit();
    const bytes = try plan.view.encode(std.testing.allocator);
    defer std.testing.allocator.free(bytes);
    for (0..bytes.len) |length| try std.testing.expectError(error.InvalidSubgroupPlan, Plan.decode(std.testing.allocator, bytes[0..length]));
    put(bytes, header_size + 4, get(bytes, header_size));
    try std.testing.expectError(error.InvalidSubgroupPlan, Plan.decode(std.testing.allocator, bytes));
    try std.testing.expectError(error.InvalidSubgroupPlan, Plan.build(std.testing.allocator, &.{ 0, 0 }, 2, 1, null));
    try std.testing.expectError(error.InvalidSubgroupPlan, Plan.build(std.testing.allocator, &.{ std.math.nan(f32), 1 }, 2, 1, null));
}

test "subgroup training honours cancellation without allocations" {
    const cancelled = true;
    try std.testing.expectError(error.Cancelled, Plan.build(std.testing.failing_allocator, &.{ 1, 0 }, 2, 1, .{
        .context = &cancelled,
        .cancelled = struct {
            fn check(ptr: *const anyopaque) bool {
                return @as(*const bool, @ptrCast(@alignCast(ptr))).*;
            }
        }.check,
    }));
}
