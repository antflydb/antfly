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
    /// Optional conservative source-space balls, bound to the same immutable
    /// member permutation. Missing certificates must never authorize pruning.
    radii: []const f32 = &.{},
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
        if (self.radii.len != 0 and self.radii.len != self.ends.len) return error.InvalidSubgroupPlan;
        for (self.radii) |value| if (!std.math.isFinite(value) or value < 0) return error.InvalidSubgroupPlan;
    }

    /// Cosine distance lower bound for every authoritative member of a group.
    /// Accumulate in f64 and round outwards, including f32 scoring slack.
    pub fn lowerBound(self: View, group: usize, query: []const f32) ?f32 {
        if (self.radii.len != self.ends.len or group >= self.ends.len or query.len != self.dims) return null;
        return self.lowerBoundScaled(group, query, queryScale(query) orelse return null);
    }

    pub fn queryScale(query: []const f32) ?f64 {
        var norm: f64 = 0;
        for (query) |q| {
            if (!std.math.isFinite(q)) return null;
            norm += @as(f64, q) * q;
        }
        if (norm <= 0 or !std.math.isFinite(norm)) return null;
        return 1 / @sqrt(norm);
    }

    /// `scale` must be queryScale(query); callers can normalize once per query.
    pub fn lowerBoundScaled(self: View, group: usize, query: []const f32, scale: f64) ?f32 {
        if (self.radii.len != self.ends.len or group >= self.ends.len or query.len != self.dims) return null;
        if (!std.math.isFinite(scale) or scale <= 0 or !std.math.isFinite(self.radii[group]) or self.radii[group] < 0) return null;
        const center = self.centers[group * self.dims ..][0..self.dims];
        var dots: @Vector(8, f64) = @splat(0);
        var norms: @Vector(8, f64) = @splat(0);
        var offset: usize = 0;
        while (offset + 8 <= query.len) : (offset += 8) {
            const q: @Vector(8, f32) = query[offset..][0..8].*;
            const c: @Vector(8, f32) = center[offset..][0..8].*;
            const qw: @Vector(8, f64) = @floatCast(q);
            const cw: @Vector(8, f64) = @floatCast(c);
            dots += qw * cw;
            norms += cw * cw;
        }
        var dot = @reduce(.Add, dots);
        var norm = @reduce(.Add, norms);
        while (offset < query.len) : (offset += 1) {
            dot += @as(f64, query[offset]) * center[offset];
            norm += @as(f64, center[offset]) * center[offset];
        }
        if (norm <= 0 or !std.math.isFinite(norm)) return null;
        const center_norm = @sqrt(norm);
        // Intersect the covering ball with the unit sphere instead of using
        // only a Euclidean triangle bound. Expand for normalization of the
        // rounded stored center, so existing certificates remain valid.
        const radius = self.radii[group] + @abs(center_norm - 1);
        const slack = 8 * @as(f64, @floatFromInt(self.dims)) * std.math.floatEps(f32);
        if (radius >= 2) return @floatCast(-slack);
        const cosine = std.math.clamp(dot * scale / center_norm, -1, 1);
        const cap_cosine = 1 - radius * radius * 0.5;
        if (cosine >= cap_cosine) return @floatCast(-slack);
        const maximum_dot = cosine * cap_cosine + @sqrt(@max(0, (1 - cosine * cosine) * (1 - cap_cosine * cap_cosine)));
        return @floatCast(1 - maximum_dot - slack);
    }

    /// Little-endian, length-framed extension. No native ABI padding is stored.
    /// Callers authenticate this together with the candidate rows it permutes.
    pub fn encode(self: View, alloc: Allocator) ![]u8 {
        try self.validate();
        const length = header_size + (self.rows.len + self.ends.len + self.centers.len + self.radii.len) * 4;
        const bytes = try alloc.alloc(u8, length);
        @memset(bytes, 0);
        @memcpy(bytes[0..4], "AFSG");
        put(bytes, 4, if (self.radii.len == 0) 1 else 2);
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
        for (self.radii) |radius| {
            put(bytes, cursor, @bitCast(radius));
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
        self.alloc.free(self.view.radii);
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
        const radii = try alloc.alloc(f32, if (shape.certified) shape.groups else 0);
        errdefer alloc.free(radii);
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
        for (radii) |*radius| {
            radius.* = @bitCast(get(bytes, cursor));
            cursor += 4;
        }
        const view: View = .{ .dims = shape.dims, .rows = rows, .ends = ends, .centers = centers, .radii = radii };
        try view.validate();
        return .{ .alloc = alloc, .view = view };
    }

    pub const SourceError = struct { norm_error: f32, decoded_norm_lower_bound: f32 };

    /// Certify balls against decoded projection vectors plus their stored
    /// authoritative f32 error bounds. No query/truth data enters training.
    pub fn certify(self: *Plan, vectors: []const f32, errors: []const SourceError) !void {
        if (vectors.len != self.view.rows.len * self.view.dims or errors.len != self.view.rows.len)
            return error.InvalidSubgroupPlan;
        for (errors) |error_| {
            // Reject absent/unsafe proof inputs before allocating replacement
            // metadata, retaining any previously validated certificate.
            if (!std.math.isFinite(error_.norm_error) or error_.norm_error < 0 or
                !std.math.isFinite(error_.decoded_norm_lower_bound) or
                error_.decoded_norm_lower_bound <= error_.norm_error)
                return error.UncertifiableSubgroup;
        }
        const radii = try self.alloc.alloc(f32, self.view.ends.len);
        errdefer self.alloc.free(radii);
        for (radii, 0..) |*radius, group| {
            var maximum: f64 = 0;
            const range_ = self.view.range(group);
            for (self.view.rows[range_.start..range_.end]) |row| {
                const error_ = errors[row];
                const vector = vectors[row * self.view.dims ..][0..self.view.dims];
                var norm: f64 = 0;
                for (vector) |value| {
                    if (!std.math.isFinite(value)) return error.UncertifiableSubgroup;
                    norm += @as(f64, value) * value;
                }
                if (norm <= 0) return error.UncertifiableSubgroup;
                const scale = 1 / @sqrt(norm);
                var distance: f64 = 0;
                for (vector, self.view.centers[group * self.view.dims ..][0..self.view.dims]) |value, center| {
                    const diff = value * scale - center;
                    distance += diff * diff;
                }
                // ||normalize(x)-normalize(y)|| <= 2||x-y||/||y||.
                maximum = @max(maximum, @sqrt(distance) + 2 * @as(f64, error_.norm_error) / error_.decoded_norm_lower_bound);
            }
            // Also cover the f32 multiply used to decode scaled half values.
            radius.* = @floatCast(maximum + (1 + maximum) * 32 * std.math.floatEps(f32));
        }
        self.alloc.free(self.view.radii);
        self.view.radii = radii;
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
fn frame(bytes: []const u8) !struct { dims: usize, count: usize, groups: usize, certified: bool } {
    if (bytes.len < header_size or !std.mem.eql(u8, bytes[0..4], "AFSG") or (get(bytes, 4) != 1 and get(bytes, 4) != 2) or get(bytes, 8) != bytes.len)
        return error.InvalidSubgroupPlan;
    const dims: usize = get(bytes, 12);
    const count: usize = get(bytes, 16);
    const groups: usize = get(bytes, 20);
    const certified = get(bytes, 4) == 2;
    if (dims == 0 or dims > max_dims or count == 0 or count > max_rows or groups == 0 or groups > max_groups or
        header_size + (count + groups + groups * dims + (if (certified) groups else @as(usize, 0))) * 4 != bytes.len) return error.InvalidSubgroupPlan;
    return .{ .dims = dims, .count = count, .groups = groups, .certified = certified };
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
    const centers_end = ends_end + shape.groups * shape.dims * 4;
    const view: View = .{
        .dims = shape.dims,
        .rows = std.mem.bytesAsSlice(u32, aligned[header_size..rows_end]),
        .ends = std.mem.bytesAsSlice(u32, @as([]align(4) const u8, @alignCast(aligned[rows_end..ends_end]))),
        .centers = std.mem.bytesAsSlice(f32, @as([]align(4) const u8, @alignCast(aligned[ends_end..centers_end]))),
        .radii = std.mem.bytesAsSlice(f32, @as([]align(4) const u8, @alignCast(aligned[centers_end..]))),
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

fn certifiedAllocationExercise(alloc: Allocator) !void {
    const vectors = [_]f32{ 1, 0, 0.99, 0.01, -1, 0, -0.99, -0.01 };
    var plan = try Plan.build(alloc, &vectors, 2, 2, null);
    defer plan.deinit();
    try std.testing.expect(plan.view.lowerBound(0, &.{ 1, 0 }) == null);
    const errors = [_]Plan.SourceError{.{ .norm_error = 0.001, .decoded_norm_lower_bound = 0.98 }} ** 4;
    try plan.certify(&vectors, &errors);
    const encoded = try plan.view.encode(alloc);
    defer alloc.free(encoded);
    var restored = try Plan.decode(alloc, encoded);
    defer restored.deinit();
    const borrowed = try decodeBorrowed(encoded);
    try std.testing.expectEqualSlices(f32, plan.view.radii, borrowed.radii);
    try std.testing.expectEqualSlices(f32, plan.view.radii, restored.view.radii);
    var useful = false;
    for (0..2) |group| useful = useful or borrowed.lowerBound(group, &.{ 1, 0 }).? > 1.9;
    try std.testing.expect(useful);
    try std.testing.expect(borrowed.lowerBound(0, &.{ 0, 0 }) == null);
    try std.testing.expect(borrowed.lowerBound(0, &.{ std.math.nan(f32), 0 }) == null);
    // Every group certificate covers perturbed authoritative vectors, not
    // merely the lossy training projection. Sweep query directions and scale.
    for (0..101) |i| {
        const angle = @as(f64, @floatFromInt(i)) * 0.062;
        const query = [_]f32{ @floatCast(@cos(angle) * 17), @floatCast(@sin(angle) * 17) };
        for (0..2) |group| {
            const bound = borrowed.lowerBound(group, &query).?;
            const r = borrowed.range(group);
            for (borrowed.rows[r.start..r.end]) |row| {
                const x: f64 = vectors[row * 2] + @as(f32, 0.0001);
                const y: f64 = vectors[row * 2 + 1] - @as(f32, 0.0001);
                const score = 1 - (query[0] * x + query[1] * y) * View.queryScale(&query).? / @sqrt(x * x + y * y);
                try std.testing.expect(bound <= score);
            }
        }
    }
    const invalid = [_]Plan.SourceError{.{ .norm_error = 1, .decoded_norm_lower_bound = 0 }} ** 4;
    try std.testing.expectError(error.UncertifiableSubgroup, plan.certify(&vectors, &invalid));
    // Failed replacement does not discard a previously valid certificate.
    try std.testing.expectEqualSlices(f32, borrowed.radii, plan.view.radii);
}

test "certified subgroup bounds cover authoritative perturbations and allocation failures" {
    try certifiedAllocationExercise(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, certifiedAllocationExercise, .{});
}

test "certified subgroup spherical bounds cover SIMD tails high dimensions and scaled queries" {
    const alloc = std.testing.allocator;
    const simple: View = .{ .dims = 2, .rows = &.{0}, .ends = &.{1}, .centers = &.{ 1, 0 }, .radii = &.{0.2} };
    // The ambient Euclidean bound is about 0.737; intersecting the source
    // ball with the unit sphere certifies a tighter distance above 0.79.
    try std.testing.expect(simple.lowerBound(0, &.{ 0, 1 }).? > 0.79);
    var random = std.Random.DefaultPrng.init(791);
    for ([_]usize{ 3, 8, 17, 768 }) |dims| {
        const vectors = try alloc.alloc(f32, 8 * dims);
        defer alloc.free(vectors);
        for (0..8) |row| {
            vectors[row * dims] = if (row < 4) 1 else -1;
            for (1..dims) |d| vectors[row * dims + d] = @sin(@as(f32, @floatFromInt(row * 13 + d))) * 0.03;
        }
        var plan = try Plan.build(alloc, vectors, dims, 4, null);
        defer plan.deinit();
        const errors = [_]Plan.SourceError{.{ .norm_error = 0.001, .decoded_norm_lower_bound = 0.99 }} ** 8;
        try plan.certify(vectors, &errors);
        const query = try alloc.alloc(f32, dims);
        defer alloc.free(query);
        for (0..24) |trial| {
            for (query) |*q| q.* = (random.random().float(f32) - 0.5) * (if (trial % 2 == 0) @as(f32, 0.01) else 99);
            for (0..4) |group| {
                const bound = plan.view.lowerBound(group, query).?;
                const r = plan.view.range(group);
                for (plan.view.rows[r.start..r.end]) |row| {
                    var norm: f64 = 0;
                    var dot: f64 = 0;
                    for (query, vectors[row * dims ..][0..dims]) |q, decoded| {
                        const authoritative = @as(f64, decoded) + 0.00001 / @sqrt(@as(f64, @floatFromInt(dims)));
                        norm += authoritative * authoritative;
                        dot += @as(f64, q) * authoritative;
                    }
                    const exact = 1 - dot * View.queryScale(query).? / @sqrt(norm);
                    try std.testing.expect(@as(f64, bound) <= exact);
                }
            }
        }
    }
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
