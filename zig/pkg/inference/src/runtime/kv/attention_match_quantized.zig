// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Experimental host adapter combining attention matching with quantization.
//! polar4/turbo3 are legacy scalar baselines. rotated4 adds a paper-inspired
//! structured rotation and Gaussian Lloyd-Max codec, without a QJL correction.
//! No public checkpoint or CUDA serving format is changed by this module.
const std = @import("std");
const checkpoint = @import("attention_match_checkpoint.zig");
const engine = @import("attention_match_engine.zig");
const solver = @import("attention_match.zig");
const turbo = @import("turboquant.zig");
const rotated = @import("turboquant_rotated.zig");
const pool = @import("pool.zig");

pub const Format = enum { polar4, turbo3, rotated4 };

/// Independently owned hybrid head. Exact rows are packed together in logical
/// order with the quantized interval omitted. Bias/positions/coverage are never
/// quantized. Turbo3's projection seed is head-local (codec head index zero).
pub const Head = struct {
    allocator: std.mem.Allocator,
    format: Format,
    dim: u32,
    window: u32 = 0,
    compacted_until: u64,
    first_row: usize = 0,
    quantized_rows: usize = 0,
    positions: []u64 = &.{},
    coverage_ends: []u64 = &.{},
    bias: []f32 = &.{},
    exact_keys: []f16 = &.{},
    exact_values: []f16 = &.{},
    keys: []u8 = &.{},
    values: []u8 = &.{},
    residual: []u8 = &.{},

    pub fn deinit(self: *Head) void {
        inline for (.{ "positions", "coverage_ends", "bias", "exact_keys", "exact_values", "keys", "values", "residual" }) |field|
            self.allocator.free(@field(self, field));
        self.* = undefined;
    }

    pub fn storageBytes(self: Head) usize {
        return self.positions.len * 20 + (self.exact_keys.len + self.exact_values.len) * 2 +
            self.keys.len + self.values.len + self.residual.len;
    }

    pub fn decodeRow(self: Head, row: usize, keys: []f32, values: []f32) !void {
        if (row >= self.positions.len or keys.len != self.dim or values.len != self.dim) return error.InvalidAttentionShape;
        if (row >= self.first_row and row - self.first_row < self.quantized_rows) {
            const i = row - self.first_row;
            const width = keyBytes(self.format, self.dim);
            const encoded = self.keys[i * width ..][0..width];
            switch (self.format) {
                .polar4 => try turbo.decodePolar4Key(encoded, keys, 1, self.dim),
                .turbo3 => try turbo.decodeTurbo3KeyWithResidual(encoded, self.residual[i * 4 ..][0..4], keys, 1, self.dim),
                .rotated4 => try rotated.decode(encoded, keys),
            }
            pool.dequantizeInt8PerHeadToF32(self.values[i * (self.dim + 4) ..][0 .. self.dim + 4], values, 1, self.dim);
        } else {
            const i = if (row < self.first_row) row else row - self.quantized_rows;
            for (keys, values, self.exact_keys[i * self.dim ..][0..self.dim], self.exact_values[i * self.dim ..][0..self.dim]) |*k, *v, ek, ev| {
                k.* = ek;
                v.* = ev;
            }
        }
    }

    /// CPU oracle for the future CUDA adapter. Scores packed keys directly;
    /// does not allocate or expand the key cache. Softcap precedes fitted bias.
    pub fn attend(self: Head, query: []const f32, position: u64, scale: f64, softcap: f64, output: []f32) !f64 {
        if (query.len != self.dim or output.len != self.dim or self.dim > 1024 or
            !std.math.isFinite(scale) or scale <= 0 or !std.math.isFinite(softcap) or softcap < 0) return error.InvalidAttentionShape;
        for (query) |q| if (!std.math.isFinite(q)) return error.NonFiniteAttention;
        var projected: [turbo.turbo3_residual_bits_per_head]f32 = undefined;
        if (self.quantized_rows > 0 and self.format == .turbo3)
            try turbo.projectTurbo3ResidualQuery(query, &projected, self.dim, 0);
        var rotated_query: [1024]f32 = undefined;
        if (self.quantized_rows > 0 and self.format == .rotated4)
            try rotated.prepareQuery(query, rotated_query[0..self.dim]);
        var accumulated: [1024]f64 = @splat(0);
        var decoded_values: [1024]f32 = undefined;
        var maximum: f64 = -std.math.inf(f64);
        var mass: f64 = 0;
        const first = if (self.window > 0 and position >= self.window - 1) position - (self.window - 1) else 0;
        for (self.positions, self.coverage_ends, self.bias, 0..) |pos, end, bias, row| {
            if (end == 0 or end - 1 > position or pos > position or (self.window > 0 and pos < first)) continue;
            var dot: f64 = 0;
            if (row >= self.first_row and row - self.first_row < self.quantized_rows) {
                const i = row - self.first_row;
                const width = keyBytes(self.format, self.dim);
                const encoded = self.keys[i * width ..][0..width];
                dot = switch (self.format) {
                    .polar4 => try turbo.dotPolar4Key(query, encoded, 1, self.dim, 0),
                    .turbo3 => @as(f64, try turbo.dotTurbo3Key(query, encoded, 1, self.dim, 0)) +
                        turbo.turbo3_residual_default_scale * @as(f64, try turbo.dotTurbo3ProjectedResidualSketch(&projected, self.residual[i * 4 ..][0..4], 1, self.dim, 0)),
                    .rotated4 => try rotated.dot(rotated_query[0..self.dim], encoded),
                };
                pool.dequantizeInt8PerHeadToF32(self.values[i * (self.dim + 4) ..][0 .. self.dim + 4], decoded_values[0..self.dim], 1, self.dim);
            } else {
                const i = if (row < self.first_row) row else row - self.quantized_rows;
                for (query, self.exact_keys[i * self.dim ..][0..self.dim], self.exact_values[i * self.dim ..][0..self.dim], decoded_values[0..self.dim]) |q, k, v, *out| {
                    dot += @as(f64, q) * @as(f64, k);
                    out.* = v;
                }
            }
            var score = dot * scale;
            if (softcap > 0) score = softcap * std.math.tanh(score / softcap);
            score += bias;
            if (!std.math.isFinite(score)) return error.NonFiniteAttention;
            const next_max = @max(maximum, score);
            const previous = @exp(maximum - next_max);
            const weight = @exp(score - next_max);
            for (accumulated[0..self.dim], decoded_values[0..self.dim]) |*out, value| out.* = out.* * previous + @as(f64, value) * weight;
            mass = mass * previous + weight;
            maximum = next_max;
        }
        if (mass == 0) return error.NoVisibleKvRows;
        for (output, accumulated[0..self.dim]) |*out, value| out.* = @floatCast(value / mass);
        return maximum + @log(mass);
    }
};

pub const Outcome = enum { accepted, rejected, ineligible, unsupported_dimension };
pub const Attempt = struct {
    head: Head,
    outcome: Outcome,
    /// Combined error against the original head, not against the fitted cache.
    metrics: solver.Metrics = .{},
};

pub const Options = struct {
    format: Format = .polar4,
    prefix_rows: usize = 128,
    recent_rows: usize = 2048,
    logical_length: u64,
    window: u32 = 0,
    scale: f64,
    softcap: f64 = 0,
    /// Additional memory beyond the caller-owned source, fitted head and queries.
    max_workspace_bytes: usize = 512 * 1024 * 1024,
    validation: solver.Config = .{ .target_rows = 1 },
};

fn keyBytes(format: Format, dim: u32) usize {
    return switch (format) {
        .polar4 => turbo.polar4KeyBytes(1, dim),
        .turbo3 => turbo.turbo3KeyBytes(1, dim),
        .rotated4 => rotated.rowBytes(dim),
    };
}

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.CompactionWorkspaceLimit;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.CompactionWorkspaceLimit;
}

fn validateHead(head: checkpoint.Head, options: Options) !void {
    const rows = head.positions.len;
    if (head.dim == 0 or head.dim > 1024 or rows == 0 or rows > options.logical_length or
        head.coverage_ends.len != rows or head.bias.len != rows or
        head.keys.len != try mul(rows, head.dim) or head.values.len != head.keys.len or
        head.compacted_until > options.logical_length) return error.InvalidKvCheckpoint;
    var next_exact = head.compacted_until;
    var prefix_end: u64 = 0;
    for (head.positions, head.coverage_ends, head.bias, 0..) |position, end, bias, i| {
        if (i % 1024 == 0) try options.validation.control.check();
        if (position >= options.logical_length or end <= position or end > options.logical_length or
            !std.math.isFinite(bias) or (i > 0 and (position <= head.positions[i - 1] or end < head.coverage_ends[i - 1]))) return error.InvalidKvCheckpoint;
        if (options.window != 0) {
            const local_rows = @min(options.window, options.logical_length);
            if (head.compacted_until != 0 or rows != local_rows or position != options.logical_length - local_rows + i or end != position + 1 or bias != 0) return error.InvalidKvCheckpoint;
        } else if (position < head.compacted_until) {
            if (end > head.compacted_until) return error.InvalidKvCheckpoint;
            prefix_end = end;
        } else {
            if (position != next_exact or end != position + 1 or bias != 0) return error.InvalidKvCheckpoint;
            next_exact += 1;
        }
    }
    if (options.window == 0 and (next_exact != options.logical_length or prefix_end != head.compacted_until)) return error.InvalidKvCheckpoint;
    for (head.keys, head.values, 0..) |k, v, i| {
        if (i % 4096 == 0) try options.validation.control.check();
        if (!checkpoint.codec.halfExact(k) or !checkpoint.codec.halfExact(v)) return error.InvalidKvCheckpoint;
    }
}

fn allocate(allocator: std.mem.Allocator, fitted: checkpoint.Head, format: Format, first: usize, count: usize, window: u32, control: solver.Control) !Head {
    var result: Head = .{ .allocator = allocator, .format = format, .dim = fitted.dim, .window = window, .compacted_until = fitted.compacted_until, .first_row = first, .quantized_rows = count };
    errdefer result.deinit();
    result.positions = try allocator.dupe(u64, fitted.positions);
    result.coverage_ends = try allocator.dupe(u64, fitted.coverage_ends);
    result.bias = try allocator.dupe(f32, fitted.bias);
    result.exact_keys = try allocator.alloc(f16, (fitted.positions.len - count) * fitted.dim);
    result.exact_values = try allocator.alloc(f16, result.exact_keys.len);
    result.keys = try allocator.alloc(u8, count * keyBytes(format, fitted.dim));
    result.values = try allocator.alloc(u8, count * (fitted.dim + 4));
    result.residual = try allocator.alloc(u8, if (format == .turbo3) count * 4 else 0);
    for (0..fitted.positions.len) |row| {
        try control.check();
        const k = fitted.keys[row * fitted.dim ..][0..fitted.dim];
        const v = fitted.values[row * fitted.dim ..][0..fitted.dim];
        if (row >= first and row - first < count) {
            const i = row - first;
            const width = keyBytes(format, fitted.dim);
            const encoded = result.keys[i * width ..][0..width];
            switch (format) {
                .polar4 => try turbo.encodePolar4Key(k, encoded, 1, fitted.dim),
                .turbo3 => {
                    try turbo.encodeTurbo3Key(k, encoded, 1, fitted.dim);
                    try turbo.encodeTurbo3ResidualSketch(k, encoded, result.residual[i * 4 ..][0..4], 1, fitted.dim);
                },
                .rotated4 => try rotated.encode(k, encoded),
            }
            pool.quantizeF32ToInt8PerHead(v, result.values[i * (fitted.dim + 4) ..][0 .. fitted.dim + 4], 1, fitted.dim);
        } else {
            const i = if (row < first) row else row - count;
            for (result.exact_keys[i * fitted.dim ..][0..fitted.dim], result.exact_values[i * fitted.dim ..][0..fitted.dim], k, v) |*ek, *ev, fk, fv| {
                ek.* = @floatCast(fk);
                ev.* = @floatCast(fv);
            }
        }
    }
    return result;
}

fn convert(allocator: std.mem.Allocator, values: []const f32) ![]f64 {
    const result = try allocator.alloc(f64, values.len);
    for (result, values) |*out, value| out.* = value;
    return result;
}

/// Fitted must be an already validated FP16 engine result. Source/fitted arrays
/// are immutable on every outcome. Numerical rejection
/// returns an independently owned FP16 head; OOM/cancellation/corruption are
/// errors. Callers provide held-out queries from ALL shared-donor consumers.
/// This is a preparation API, not an in-place serving cache replacement.
pub fn prepareHead(allocator: std.mem.Allocator, original: checkpoint.Head, fitted: checkpoint.Head, queries: []const f64, options: Options) !Attempt {
    try options.validation.control.check();
    if (options.logical_length == 0 or options.logical_length > 131072 or
        !std.math.isFinite(options.scale) or options.scale <= 0 or
        !std.math.isFinite(options.softcap) or options.softcap < 0) return error.InvalidFitConfig;
    inline for (.{ "max_relative_mass_error", "max_relative_output_rmse" }) |field| {
        const value = @field(options.validation, field);
        if (!std.math.isFinite(value) or value < 0) return error.InvalidFitConfig;
    }
    try validateHead(original, options);
    try validateHead(fitted, options);
    if (original.dim != fitted.dim or original.compacted_until > fitted.compacted_until or fitted.positions.len > original.positions.len) return error.InvalidKvCheckpoint;
    if (queries.len == 0 or queries.len % fitted.dim != 0) return error.InvalidCompactionQueries;
    for (queries) |q| if (!std.math.isFinite(q)) return error.InvalidCompactionQueries;

    const boundary = options.logical_length - @min(options.logical_length, options.recent_rows);
    if (options.window == 0) {
        if (original.compacted_until > boundary or fitted.compacted_until > boundary) return error.CompactionRecentWindowUnavailable;
        const prefix: usize = @intCast(@min(options.prefix_rows, options.logical_length));
        if (prefix > original.positions.len or prefix > fitted.positions.len) return error.CompactionPrefixUnavailable;
        for (0..prefix) |i| {
            if (original.positions[i] != i or original.coverage_ends[i] != i + 1 or original.bias[i] != 0 or
                fitted.positions[i] != i or fitted.coverage_ends[i] != i + 1 or fitted.bias[i] != 0) return error.CompactionPrefixUnavailable;
        }
    }
    var first: usize = 0;
    while (first < fitted.positions.len and (fitted.positions[first] < options.prefix_rows or fitted.coverage_ends[first] <= original.compacted_until)) : (first += 1) {}
    var end = first;
    while (end < fitted.positions.len and fitted.coverage_ends[end] <= boundary and fitted.coverage_ends[end] <= fitted.compacted_until) : (end += 1) {}
    const eligible = options.window == 0 and fitted.compacted_until > original.compacted_until and end > first;
    const supported = if (options.format == .rotated4) rotated.supported(fitted.dim) else turbo.isSupportedHeadDim(fitted.dim);

    // Protected rows must still be the caller's exact source rows. In
    // particular, never quietly requantize an earlier cycle's fitted history.
    var source_row: usize = 0;
    for (fitted.positions, 0..) |position, row| {
        if (eligible and row >= first and row < end) continue;
        while (source_row < original.positions.len and original.positions[source_row] < position) : (source_row += 1) {}
        if (source_row == original.positions.len or original.positions[source_row] != position or
            fitted.coverage_ends[row] != original.coverage_ends[source_row] or
            @as(u32, @bitCast(fitted.bias[row])) != @as(u32, @bitCast(original.bias[source_row])) or
            !std.mem.eql(u8, std.mem.sliceAsBytes(fitted.keys[row * fitted.dim ..][0..fitted.dim]), std.mem.sliceAsBytes(original.keys[source_row * fitted.dim ..][0..fitted.dim])) or
            !std.mem.eql(u8, std.mem.sliceAsBytes(fitted.values[row * fitted.dim ..][0..fitted.dim]), std.mem.sliceAsBytes(original.values[source_row * fitted.dim ..][0..fitted.dim]))) return error.InvalidKvCheckpoint;
    }

    // The full FP16 result bounds either packed result and its fallback. The
    // rejected candidate is retired before allocating fallback. Conversion and
    // validation scratch are admitted before the first allocation.
    var required = try mul(fitted.positions.len, try add(20, try mul(fitted.dim, 4)));
    if (eligible and supported) {
        required = try add(required, try mul(try add(original.positions.len, fitted.positions.len), try add(try mul(fitted.dim, 16), 8)));
        required = try add(required, try solver.validationBytes(original.positions.len, fitted.positions.len, fitted.dim, queries.len / fitted.dim, options.validation));
    }
    if (required > options.max_workspace_bytes) return error.CompactionWorkspaceLimit;
    if (!eligible or !supported) return .{ .head = try allocate(allocator, fitted, options.format, 0, 0, options.window, options.validation.control), .outcome = if (!eligible) .ineligible else .unsupported_dimension };

    var candidate = try allocate(allocator, fitted, options.format, first, end - first, options.window, options.validation.control);
    var owned = true;
    defer if (owned) candidate.deinit();
    const ok = try convert(allocator, original.keys);
    defer allocator.free(ok);
    const ov = try convert(allocator, original.values);
    defer allocator.free(ov);
    const ob = try convert(allocator, original.bias);
    defer allocator.free(ob);
    const ck = try allocator.alloc(f64, fitted.keys.len);
    defer allocator.free(ck);
    const cv = try allocator.alloc(f64, fitted.values.len);
    defer allocator.free(cv);
    const cb = try convert(allocator, fitted.bias);
    defer allocator.free(cb);
    var keys: [1024]f32 = undefined;
    var values: [1024]f32 = undefined;
    for (0..fitted.positions.len) |row| {
        try options.validation.control.check();
        try candidate.decodeRow(row, keys[0..fitted.dim], values[0..fitted.dim]);
        for (ck[row * fitted.dim ..][0..fitted.dim], cv[row * fitted.dim ..][0..fitted.dim], keys[0..fitted.dim], values[0..fitted.dim]) |*k, *v, dk, dv| {
            k.* = dk;
            v.* = dv;
        }
    }
    const source: solver.Head = .{ .keys = ok, .values = ov, .bias = ob, .rows = original.positions.len, .dim = original.dim, .scale = options.scale, .softcap = options.softcap };
    const compact: solver.Head = .{ .keys = ck, .values = cv, .bias = cb, .rows = fitted.positions.len, .dim = fitted.dim, .scale = options.scale, .softcap = options.softcap };
    try compact.validate();
    const metrics = try solver.validationMetrics(allocator, source, compact, queries, options.validation);
    try options.validation.control.check();
    if (!std.math.isFinite(metrics.max_relative_mass_error) or !std.math.isFinite(metrics.relative_output_rmse) or
        metrics.max_relative_mass_error > options.validation.max_relative_mass_error or
        metrics.relative_output_rmse > options.validation.max_relative_output_rmse)
    {
        candidate.deinit();
        owned = false;
        return .{ .head = try allocate(allocator, fitted, options.format, 0, 0, options.window, options.validation.control), .outcome = .rejected, .metrics = metrics };
    }
    owned = false;
    return .{ .head = candidate, .outcome = .accepted, .metrics = metrics };
}

/// Fit and pack one complete model state. The FP16 candidate is retained as a
/// staging reference for a future device transaction, not counted as a saving.
/// Consumers must not install these host buffers in the current FP16 CUDA ABI.
pub const Candidate = struct {
    fitted: checkpoint.Owned,
    heads: []Attempt,

    pub fn deinit(self: *Candidate) void {
        for (self.heads) |*head| head.head.deinit();
        self.fitted.allocator.free(self.heads);
        self.fitted.deinit();
        self.* = undefined;
    }
};

pub fn prepare(allocator: std.mem.Allocator, source: checkpoint.State, batches: []const engine.ReferenceBatch, layout: engine.Layout, fit: engine.Options, format: Format) !Candidate {
    if (fit.storage_precision != .f16 or fit.validation != .whole_head or fit.fp8_checkpoint or fit.rotated4_checkpoint) return error.InvalidFitConfig;
    var fitted = try engine.prepare(allocator, source, batches, layout, fit);
    errdefer fitted.deinit();
    var count: usize = 0;
    for (source.donors) |donor| count = try add(count, donor.heads.len);
    var resident = try add(try add(try source.residentBytes(), try fitted.state.residentBytes()), try mul(count, @sizeOf(Attempt)));
    if (resident > fit.max_transient_host_bytes) return error.CompactionWorkspaceLimit;
    const heads = try allocator.alloc(Attempt, count);
    errdefer allocator.free(heads);
    var done: usize = 0;
    errdefer for (heads[0..done]) |*head| head.head.deinit();
    for (source.donors, 0..) |donor, di| for (donor.heads, 0..) |original, hi| {
        var query_elements: usize = 0;
        for (batches) |batch| {
            if (source.layer_donors[batch.consumer_layer] == di and batch.query_head / (layout.query_heads_by_layer[batch.consumer_layer] / donor.heads.len) == hi)
                query_elements = try add(query_elements, batch.heldout.len);
        }
        // Local donors have no calibration queries and remain exact.
        const query_bytes = try mul(@max(query_elements, original.dim), 8);
        if (try add(resident, query_bytes) > fit.max_transient_host_bytes) return error.CompactionWorkspaceLimit;
        const queries = try allocator.alloc(f64, query_bytes / 8);
        defer allocator.free(queries);
        @memset(queries, 0);
        var offset: usize = 0;
        for (batches) |batch| {
            if (source.layer_donors[batch.consumer_layer] != di or batch.query_head / (layout.query_heads_by_layer[batch.consumer_layer] / donor.heads.len) != hi) continue;
            for (batch.heldout) |q| {
                queries[offset] = q;
                offset += 1;
            }
        }
        heads[done] = try prepareHead(allocator, original, fitted.state.donors[di].heads[hi], queries, .{
            .format = format,
            .logical_length = source.logical_length,
            .window = donor.window,
            .prefix_rows = fit.prefix_rows,
            .recent_rows = fit.recent_rows,
            .scale = layout.scales_by_donor[di],
            .softcap = layout.softcaps_by_donor[di],
            .max_workspace_bytes = fit.max_transient_host_bytes - resident - query_bytes,
            .validation = fit.fit,
        });
        done += 1;
        resident = try add(resident, heads[done - 1].head.storageBytes());
    };
    return .{ .fitted = fitted, .heads = heads };
}

const Fixture = struct {
    keys: [6 * 64]f32 = @splat(0),
    values: [6 * 64]f32 = @splat(2),
    compact_keys: [4 * 64]f32 = @splat(0),
    compact_values: [4 * 64]f32 = @splat(2),

    fn original(self: *const Fixture) checkpoint.Head {
        return .{ .dim = 64, .positions = &.{ 0, 1, 2, 3, 4, 5 }, .coverage_ends = &.{ 1, 2, 3, 4, 5, 6 }, .keys = &self.keys, .values = &self.values, .bias = &.{ 0, 0, 0, 0, 0, 0 } };
    }
    fn fitted(self: *const Fixture) checkpoint.Head {
        return .{ .dim = 64, .compacted_until = 5, .positions = &.{ 0, 1, 3, 5 }, .coverage_ends = &.{ 1, 3, 5, 6 }, .keys = &self.compact_keys, .values = &self.compact_values, .bias = &.{ 0, @log(@as(f32, 2)), @log(@as(f32, 2)), 0 } };
    }
};

fn testOptions(format: Format) Options {
    return .{ .format = format, .logical_length = 6, .prefix_rows = 1, .recent_rows = 1, .scale = 1 };
}

fn allocationTest(allocator: std.mem.Allocator, format: Format, reject: bool) !void {
    var fixture: Fixture = .{};
    // Signed zero, small subnormals and magnitudes outside the key codebook
    // are preserved bit-for-bit in both protected regions.
    fixture.keys[0] = -0.0;
    fixture.compact_keys[0] = -0.0;
    fixture.values[0] = @as(f16, @bitCast(@as(u16, 1)));
    fixture.compact_values[0] = fixture.values[0];
    fixture.keys[5 * 64] = 4;
    fixture.compact_keys[3 * 64] = 4;
    const before = fixture;
    var queries: [2 * 64]f64 = @splat(0);
    if (reject) @memset(queries[64..], 1);
    var result = try prepareHead(allocator, fixture.original(), fixture.fitted(), &queries, testOptions(format));
    defer result.head.deinit();
    try std.testing.expectEqual(if (reject) Outcome.rejected else Outcome.accepted, result.outcome);
    try std.testing.expectEqual(@as(usize, if (reject) 0 else 2), result.head.quantized_rows);
    try std.testing.expectEqualSlices(u64, fixture.fitted().positions, result.head.positions);
    try std.testing.expectEqualSlices(u64, fixture.fitted().coverage_ends, result.head.coverage_ends);
    try std.testing.expectEqualSlices(f32, fixture.fitted().bias, result.head.bias);
    var keys: [64]f32 = undefined;
    var values: [64]f32 = undefined;
    for ([_]usize{ 0, 3 }) |row| {
        try result.head.decodeRow(row, &keys, &values);
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(fixture.compact_keys[row * 64 ..][0..64]), std.mem.sliceAsBytes(&keys));
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(fixture.compact_values[row * 64 ..][0..64]), std.mem.sliceAsBytes(&values));
    }
    const fp16_bytes: usize = 4 * (64 * 4 + 20);
    if (reject) try std.testing.expectEqual(fp16_bytes, result.head.storageBytes()) else {
        const row_bytes: usize = switch (format) {
            .polar4 => 100,
            .turbo3 => 96,
            .rotated4 => 104,
        };
        try std.testing.expectEqual(4 * 20 + 2 * 64 * 4 + 2 * row_bytes, result.head.storageBytes());
        try std.testing.expect(result.head.storageBytes() < fp16_bytes);
    }
    try std.testing.expectEqualSlices(u8, std.mem.asBytes(&before), std.mem.asBytes(&fixture));
}

test "attention_match quantized codecs preserve metadata and exact rows with allocation rollback" {
    inline for (.{ Format.polar4, Format.turbo3, Format.rotated4 }) |format| {
        try allocationTest(std.testing.allocator, format, false);
        try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationTest, .{ format, false });
    }
}

test "attention_match quantized rejection retains the FP16 fit and never hides allocation failure" {
    inline for (.{ Format.polar4, Format.turbo3 }) |format| {
        try allocationTest(std.testing.allocator, format, true);
        try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationTest, .{ format, true });
    }
}

test "attention_match quantized gate measures combined error against the original head" {
    var fixture: Fixture = .{};
    const fitted_key: f32 = @as(f16, 0.04);
    @memset(fixture.compact_keys[64 .. 3 * 64], fitted_key);
    var query: [64]f64 = @splat(0);
    query[0] = 1;
    var options = testOptions(.polar4);
    options.scale = 1.2;
    // The FP16 fit alone passes. Quantization measured against that fit also
    // passes. Their combined error against the source must still reject.
    const fitted_mass = (2 + 4 * @exp(@as(f64, fitted_key) * options.scale)) / 6;
    const quantized_mass = (2 + 4 * @exp((1.0 / 15.0) * options.scale)) / 6;
    try std.testing.expect(fitted_mass - 1 < 0.05);
    try std.testing.expect(quantized_mass / fitted_mass - 1 < 0.05);
    try std.testing.expect(quantized_mass - 1 > 0.05);
    var result = try prepareHead(std.testing.allocator, fixture.original(), fixture.fitted(), &query, options);
    defer result.head.deinit();
    try std.testing.expectEqual(Outcome.rejected, result.outcome);
    try std.testing.expect(result.metrics.max_relative_mass_error > 0.05);
}

test "attention_match quantized admission corruption and cancellation precede publication" {
    var fixture: Fixture = .{};
    const queries: [64]f64 = @splat(0);
    var fail = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var options = testOptions(.polar4);
    options.max_workspace_bytes = 1;
    try std.testing.expectError(error.CompactionWorkspaceLimit, prepareHead(fail.allocator(), fixture.original(), fixture.fitted(), &queries, options));
    try std.testing.expectEqual(@as(usize, 0), fail.alloc_index);
    options = testOptions(.polar4);
    options.recent_rows = 6;
    try std.testing.expectError(error.CompactionRecentWindowUnavailable, prepareHead(std.testing.allocator, fixture.original(), fixture.fitted(), &queries, options));
    options = testOptions(.polar4);
    options.prefix_rows = 2;
    try std.testing.expectError(error.CompactionPrefixUnavailable, prepareHead(std.testing.allocator, fixture.original(), fixture.fitted(), &queries, options));
    options = testOptions(.polar4);
    fixture.compact_keys[0] = 0.5;
    try std.testing.expectError(error.InvalidKvCheckpoint, prepareHead(std.testing.allocator, fixture.original(), fixture.fitted(), &queries, options));
    fixture.compact_keys[0] = 0;
    fixture.compact_values[64] = std.math.nan(f32);
    try std.testing.expectError(error.InvalidKvCheckpoint, prepareHead(std.testing.allocator, fixture.original(), fixture.fitted(), &queries, options));
    fixture.compact_values[64] = 2;
    options.validation.max_relative_mass_error = std.math.nan(f64);
    try std.testing.expectError(error.InvalidFitConfig, prepareHead(std.testing.allocator, fixture.original(), fixture.fitted(), &queries, options));
    const Cancel = struct {
        remaining: usize,
        fn check(ctx: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            if (self.remaining == 0) return error.Cancelled;
            self.remaining -= 1;
        }
    };
    // Cross every cancellation boundary until one complete operation succeeds.
    var completed = false;
    for (0..100) |n| {
        var cancel: Cancel = .{ .remaining = n };
        options = testOptions(.turbo3);
        options.validation.control = .{ .context = &cancel, .check_fn = Cancel.check };
        var result = prepareHead(std.testing.allocator, fixture.original(), fixture.fitted(), &queries, options) catch |err| {
            try std.testing.expectEqual(error.Cancelled, err);
            continue;
        };
        result.head.deinit();
        completed = true;
        break;
    }
    try std.testing.expect(completed);
}

test "attention_match quantized repeat cycle never requantizes older fitted history" {
    var fixture: Fixture = .{};
    @memset(fixture.keys[64..128], 0.25);
    @memset(fixture.compact_keys[64..128], 0.25);
    var original = fixture.original();
    original.compacted_until = 3;
    original.positions = &.{ 0, 1, 3, 4, 5 };
    original.coverage_ends = &.{ 1, 3, 4, 5, 6 };
    original.bias = &.{ 0, @log(@as(f32, 2)), 0, 0, 0 };
    original.keys = fixture.keys[0 .. 5 * 64];
    original.values = fixture.values[0 .. 5 * 64];
    const queries: [64]f64 = @splat(0);
    var result = try prepareHead(std.testing.allocator, original, fixture.fitted(), &queries, testOptions(.turbo3));
    defer result.head.deinit();
    try std.testing.expectEqual(Outcome.accepted, result.outcome);
    try std.testing.expectEqual(@as(usize, 2), result.head.first_row);
    try std.testing.expectEqual(@as(usize, 1), result.head.quantized_rows);
    var keys: [64]f32 = undefined;
    var values: [64]f32 = undefined;
    try result.head.decodeRow(1, &keys, &values);
    try std.testing.expectEqualSlices(f32, fixture.compact_keys[64..128], &keys);
}

test "attention_match packed scoring matches decoded oracle with coverage softcap and fitted bias" {
    var fixture: Fixture = .{};
    @memset(fixture.values[0..64], 0.5);
    @memset(fixture.compact_values[0..64], 0.5);
    @memset(fixture.values[5 * 64 ..], 3);
    @memset(fixture.compact_values[3 * 64 ..], 3);
    const calibration: [64]f64 = @splat(0);
    var query: [64]f32 = undefined;
    var query64: [64]f64 = undefined;
    for (&query, &query64, 0..) |*q, *q64, i| {
        q.* = @as(f32, @floatFromInt(i)) / 64 - 0.2;
        q64.* = q.*;
    }
    inline for (.{ Format.polar4, Format.turbo3, Format.rotated4 }) |format| {
        var result = try prepareHead(std.testing.allocator, fixture.original(), fixture.fitted(), &calibration, testOptions(format));
        defer result.head.deinit();
        try std.testing.expectEqual(Outcome.accepted, result.outcome);
        for ([_]u64{ 0, 1, 2, 4, 5, 8 }) |position| {
            var keys: [4 * 64]f64 = undefined;
            var values: [4 * 64]f64 = undefined;
            var bias: [4]f64 = undefined;
            var dk: [64]f32 = undefined;
            var dv: [64]f32 = undefined;
            var visible: usize = 0;
            for (result.head.coverage_ends, 0..) |end, row| {
                if (end - 1 > position) continue;
                try result.head.decodeRow(row, &dk, &dv);
                for (keys[visible * 64 ..][0..64], values[visible * 64 ..][0..64], dk, dv) |*k, *v, k32, v32| {
                    k.* = k32;
                    v.* = v32;
                }
                bias[visible] = result.head.bias[row];
                visible += 1;
            }
            const reference: solver.Head = .{ .keys = keys[0 .. visible * 64], .values = values[0 .. visible * 64], .bias = bias[0..visible], .dim = 64, .rows = visible, .scale = 0.125, .softcap = 0.07 };
            var probabilities: [4]f64 = undefined;
            var expected: [64]f64 = undefined;
            const expected_mass = try solver.attend(reference, &query64, probabilities[0..visible], &expected);
            var output: [64]f32 = undefined;
            const actual_mass = try result.head.attend(&query, position, 0.125, 0.07, &output);
            try std.testing.expectApproxEqAbs(expected_mass, actual_mass, 1e-6);
            for (expected, output) |want, got| try std.testing.expectApproxEqAbs(want, @as(f64, got), 1e-6);
        }
    }
}

test "attention_match quantized unsupported dimensions and local heads remain FP16" {
    const fixture: Fixture = .{};
    const queries: [64]f64 = @splat(0);
    var original = fixture.original();
    var fitted = fixture.fitted();
    original.dim = 1;
    original.keys = fixture.keys[0..6];
    original.values = fixture.values[0..6];
    fitted.dim = 1;
    fitted.keys = fixture.compact_keys[0..4];
    fitted.values = fixture.compact_values[0..4];
    var unsupported = try prepareHead(std.testing.allocator, original, fitted, &.{0}, testOptions(.polar4));
    defer unsupported.head.deinit();
    try std.testing.expectEqual(Outcome.unsupported_dimension, unsupported.outcome);
    try std.testing.expectEqual(@as(usize, 0), unsupported.head.quantized_rows);
    var options = testOptions(.polar4);
    options.window = 6;
    var local = try prepareHead(std.testing.allocator, fixture.original(), fixture.original(), &queries, options);
    defer local.head.deinit();
    try std.testing.expectEqual(Outcome.ineligible, local.outcome);
    try std.testing.expectEqual(@as(usize, 6 * (64 * 4 + 20)), local.head.storageBytes());
    const zero: [64]f32 = @splat(0);
    var output: [64]f32 = undefined;
    // At position 9 only source positions 4 and 5 are in the local window.
    try std.testing.expectApproxEqAbs(@log(@as(f64, 2)), try local.head.attend(&zero, 9, 1, 0, &output), 1e-12);
    try std.testing.expectError(error.NoVisibleKvRows, local.head.attend(&zero, 12, 1, 0, &output));
}

fn sharedConsumerTest(allocator: std.mem.Allocator) !void {
    const fixture: Fixture = .{};
    const identity: checkpoint.Identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) };
    const donor_heads = [_]checkpoint.Head{fixture.original()};
    const donors = [_]checkpoint.Donor{.{ .heads = &donor_heads }};
    const source: checkpoint.State = .{ .identity = identity, .logical_length = 6, .tokens = &.{ 0, 0, 0, 0, 0, 0 }, .logits = &.{0}, .layer_donors = &.{ 0, 0 }, .donors = &donors };
    const layout: engine.Layout = .{ .query_heads_by_layer = &.{ 1, 1 }, .scales_by_donor = &.{1}, .softcaps_by_donor = &.{0} };
    const zero: [64]f32 = @splat(0);
    const one: [64]f32 = @splat(1);
    const batches = [_]engine.ReferenceBatch{
        .{ .consumer_layer = 0, .query_head = 0, .train = &zero, .heldout = &zero, .train_positions = &.{6}, .heldout_positions = &.{7} },
        .{ .consumer_layer = 1, .query_head = 0, .train = &zero, .heldout = &one, .train_positions = &.{6}, .heldout_positions = &.{7} },
    };
    const fit: engine.Options = .{ .storage_precision = .f16, .validation = .whole_head, .prefix_rows = 1, .recent_rows = 1, .chunk_rows = 2 };
    var candidate = try prepare(allocator, source, &batches, layout, fit, .polar4);
    defer candidate.deinit();
    try std.testing.expectEqual(@as(usize, 1), candidate.heads.len);
    try std.testing.expectEqual(Outcome.rejected, candidate.heads[0].outcome);
    try std.testing.expect(candidate.heads[0].metrics.max_relative_mass_error > 0.05);
    try std.testing.expectEqual(@as(usize, 4), candidate.fitted.state.donors[0].heads[0].positions.len);
}

test "attention_match quantized end-to-end preparation validates every shared donor consumer and unwinds allocation failures" {
    try sharedConsumerTest(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sharedConsumerTest, .{});
}
