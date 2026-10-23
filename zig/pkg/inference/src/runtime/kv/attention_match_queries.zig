// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded post-RoPE query collector for an isolated repeat-prefill. All output
//! buffers are allocated before execution; capture never allocates. Positions
//! are sampled across the entire repeat, not just its most recent chunk.
const std = @import("std");
const engine = @import("attention_match_engine.zig");

/// Build a bounded self-study sequence from the saved prefix only. Repeating
/// just its tail can calibrate exclusively on boilerplate and miss identifiers
/// earlier in the history. Prefer contiguous windows containing infrequent
/// tokens, keep their original order, and always include the recent suffix.
/// This selects calibration inputs; it never changes the authoritative history
/// or authorizes a candidate without the independent numerical validation.
pub fn studyTokens(allocator: std.mem.Allocator, source: []const i64, maximum: usize) ![]i64 {
    if (source.len < 2 or source.len > 32768 or maximum < 2 or maximum > 32768)
        return error.CompactionQueryLimit;
    for (source) |token| if (token < 0) return error.InvalidCompactionQueries;
    if (source.len <= maximum or maximum < 256)
        return allocator.dupe(i64, source[source.len - @min(source.len, maximum) ..]);
    const window = 64;
    // Split the bounded budget between historical spans and recent context.
    // Two historical windows can both be occupied by tool declarations,
    // excluding rare task data even when it ranks immediately behind them.
    const informative = @min(256, (maximum / 2 / window) * window);
    const recent = maximum - informative;
    const take = (maximum - recent) / window;
    const count = (source.len - recent) / window;
    const order = try allocator.alloc(u32, source.len);
    defer allocator.free(order);
    const frequency = try allocator.alloc(u32, source.len);
    defer allocator.free(frequency);
    for (order, 0..) |*index, i| index.* = @intCast(i);
    const ByToken = struct {
        fn less(tokens: []const i64, left: u32, right: u32) bool {
            return tokens[left] < tokens[right];
        }
    };
    std.mem.sort(u32, order, source, ByToken.less);
    var first: usize = 0;
    while (first < order.len) {
        var end = first + 1;
        while (end < order.len and source[order[end]] == source[order[first]]) : (end += 1) {}
        for (order[first..end]) |index| frequency[index] = @intCast(end - first);
        first = end;
    }
    const scores = try allocator.alloc(f64, count);
    defer allocator.free(scores);
    const selected = try allocator.alloc(bool, count);
    defer allocator.free(selected);
    @memset(selected, false);
    for (scores, 0..) |*value, i| {
        value.* = 0;
        for (frequency[i * window ..][0..window]) |n| value.* += 1.0 / @as(f64, @floatFromInt(n));
    }
    for (0..take) |_| {
        var best: ?usize = null;
        var best_distance: usize = 0;
        for (scores, 0..) |value, i| {
            if (selected[i]) continue;
            // Equal-scoring boilerplate is spread across the prefix instead
            // of filling every remaining slot with adjacent early windows.
            var distance = count - i;
            for (selected, 0..) |chosen, j| if (chosen) {
                distance = @min(distance, if (i > j) i - j else j - i);
            };
            if (best == null or value > scores[best.?] or
                (value == scores[best.?] and distance > best_distance))
            {
                best = i;
                best_distance = distance;
            }
        }
        selected[best orelse return error.InvalidCompactionQueries] = true;
    }
    const result = try allocator.alloc(i64, maximum);
    var written: usize = 0;
    for (selected, 0..) |chosen, i| if (chosen) {
        @memcpy(result[written..][0..window], source[i * window ..][0..window]);
        written += window;
    };
    @memcpy(result[written..], source[source.len - recent ..]);
    return result;
}

pub const Consumer = struct {
    layer: u32,
    donor: u32,
    query_heads: u32,
    kv_heads: u32,
    dim: u32,
};

pub const Options = struct {
    samples_per_consumer: usize = 80,
    /// Every fifth sampled position is held out, starting with position zero.
    heldout_stride: usize = 5,
    max_bytes: usize = 64 * 1024 * 1024,
    max_capture_rows: usize = 512,
};

const Sample = struct { position: u64, heldout: bool, index: usize };
const Plan = struct {
    samples: usize,
    train: usize,
    heldout: usize,
    heads: usize,
    elements: usize,
    bytes: usize,
};

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.CompactionQueryLimit;
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.CompactionQueryLimit;
}

fn plan(consumers: []const Consumer, source_length: usize, repeat_length: usize, options: Options) !Plan {
    if (source_length == 0 or repeat_length < 2 or options.samples_per_consumer < 2 or options.heldout_stride < 2 or
        options.max_capture_rows == 0 or consumers.len == 0 or consumers.len > 256) return error.InvalidCompactionQueries;
    _ = try add(source_length, repeat_length);
    const samples = @min(repeat_length, options.samples_per_consumer);
    if (samples > 1024) return error.CompactionQueryLimit;
    const heldout = (samples - 1) / options.heldout_stride + 1;
    var heads: usize = 0;
    var width: usize = 0;
    var maximum_width: usize = 0;
    for (consumers, 0..) |consumer, i| {
        if (consumer.query_heads == 0 or consumer.query_heads > 256 or consumer.kv_heads == 0 or
            consumer.query_heads % consumer.kv_heads != 0 or consumer.dim == 0 or consumer.dim > 4096 or
            consumer.donor > consumer.layer or (i > 0 and consumers[i - 1].layer >= consumer.layer)) return error.InvalidCompactionQueries;
        heads = try add(heads, consumer.query_heads);
        const w = try mul(consumer.query_heads, consumer.dim);
        width = try add(width, w);
        maximum_width = @max(maximum_width, w);
    }
    const elements = try mul(width, samples);
    var bytes = try mul(elements, @sizeOf(f32));
    bytes = try add(bytes, try mul(heads, @sizeOf(engine.ReferenceBatch)));
    bytes = try add(bytes, try mul(consumers.len, @sizeOf(Consumer)));
    bytes = try add(bytes, try mul(samples, @sizeOf(Sample) + @sizeOf(u64)));
    bytes = try add(bytes, try mul(consumers.len, samples)); // seen flags
    // Backend capture's host materialization exists at the same time as output.
    bytes = try add(bytes, try mul(try mul(maximum_width, options.max_capture_rows), @sizeOf(f32)));
    if (bytes > options.max_bytes) return error.CompactionQueryLimit;
    return .{ .samples = samples, .train = samples - heldout, .heldout = heldout, .heads = heads, .elements = elements, .bytes = bytes };
}

pub fn peakBytes(consumers: []const Consumer, source_length: usize, repeat_length: usize, options: Options) !usize {
    return (try plan(consumers, source_length, repeat_length, options)).bytes;
}

pub const Agreement = struct { max_absolute: f64 = 0, max_relative_l2: f64 = 0 };

pub fn compareRows(actual: []const f32, reference: []const f32, dim: usize) !Agreement {
    if (dim == 0 or actual.len != reference.len or actual.len % dim != 0) return error.InvalidCompactionQueries;
    var result = Agreement{};
    var offset: usize = 0;
    while (offset < actual.len) : (offset += dim) {
        var squared_error: f64 = 0;
        var squared_reference: f64 = 0;
        for (actual[offset..][0..dim], reference[offset..][0..dim]) |a, b| {
            if (!std.math.isFinite(a) or !std.math.isFinite(b)) return error.NonFiniteAttention;
            const difference = @as(f64, a) - @as(f64, b);
            result.max_absolute = @max(result.max_absolute, @abs(difference));
            squared_error += difference * difference;
            squared_reference += @as(f64, b) * @as(f64, b);
        }
        result.max_relative_l2 = @max(result.max_relative_l2, @sqrt(squared_error / @max(squared_reference, 1e-30)));
    }
    return result;
}

pub const Collector = struct {
    allocator: std.mem.Allocator,
    consumers: []Consumer,
    samples: []Sample,
    batches: []engine.ReferenceBatch,
    values: []f32,
    train_positions: []u64,
    heldout_positions: []u64,
    seen: []bool,
    source_length: usize,
    repeat_length: usize,
    max_capture_rows: usize,
    peak_bytes: usize,

    pub fn init(allocator: std.mem.Allocator, consumers: []const Consumer, source_length: usize, repeat_length: usize, options: Options) !Collector {
        const p = try plan(consumers, source_length, repeat_length, options);
        const owned_consumers = try allocator.dupe(Consumer, consumers);
        errdefer allocator.free(owned_consumers);
        const samples = try allocator.alloc(Sample, p.samples);
        errdefer allocator.free(samples);
        const batches = try allocator.alloc(engine.ReferenceBatch, p.heads);
        errdefer allocator.free(batches);
        const values = try allocator.alloc(f32, p.elements);
        errdefer allocator.free(values);
        const train_positions = try allocator.alloc(u64, p.train);
        errdefer allocator.free(train_positions);
        const heldout_positions = try allocator.alloc(u64, p.heldout);
        errdefer allocator.free(heldout_positions);
        const seen = try allocator.alloc(bool, try mul(consumers.len, p.samples));
        errdefer allocator.free(seen);
        @memset(seen, false);
        var nt: usize = 0;
        var nh: usize = 0;
        for (samples, 0..) |*sample, i| {
            // Include both ends of the repeat and keep integer positions unique.
            const position = try add(source_length, (try mul(i, repeat_length - 1)) / (p.samples - 1));
            const heldout = i % options.heldout_stride == 0;
            sample.* = .{ .position = position, .heldout = heldout, .index = if (heldout) nh else nt };
            if (heldout) {
                heldout_positions[nh] = position;
                nh += 1;
            } else {
                train_positions[nt] = position;
                nt += 1;
            }
        }
        var b: usize = 0;
        var v: usize = 0;
        for (consumers) |consumer| for (0..consumer.query_heads) |head| {
            const ntrain = p.train * consumer.dim;
            const nheldout = p.heldout * consumer.dim;
            batches[b] = .{ .consumer_layer = consumer.layer, .query_head = @intCast(head), .train = values[v..][0..ntrain], .heldout = values[v + ntrain ..][0..nheldout], .train_positions = train_positions, .heldout_positions = heldout_positions };
            b += 1;
            v += ntrain + nheldout;
        };
        return .{ .allocator = allocator, .consumers = owned_consumers, .samples = samples, .batches = batches, .values = values, .train_positions = train_positions, .heldout_positions = heldout_positions, .seen = seen, .source_length = source_length, .repeat_length = repeat_length, .max_capture_rows = options.max_capture_rows, .peak_bytes = p.bytes };
    }

    pub fn deinit(self: *Collector) void {
        self.allocator.free(self.consumers);
        self.allocator.free(self.samples);
        self.allocator.free(self.batches);
        self.allocator.free(self.values);
        self.allocator.free(self.train_positions);
        self.allocator.free(self.heldout_positions);
        self.allocator.free(self.seen);
        self.* = undefined;
    }

    /// Exact signature of DecodeContext's post-RoPE query capture hook. A fused
    /// path may decline and retry; identical/overlapping positions replace the
    /// same slots instead of being double-counted. Shape failure is fail-closed.
    pub fn capture(ctx: *anyopaque, donor_layer: usize, consumer_layer: usize, first_position: usize, num_heads: usize, head_dim: usize, values: []const f32) !void {
        const self: *Collector = @ptrCast(@alignCast(ctx));
        var batch_start: usize = 0;
        for (self.consumers, 0..) |consumer, ci| {
            defer batch_start += consumer.query_heads;
            if (consumer.layer != consumer_layer) continue;
            if (consumer.donor != donor_layer or consumer.query_heads != num_heads or consumer.dim != head_dim)
                return error.InvalidCompactionQueries;
            const width = try mul(num_heads, head_dim);
            if (values.len % width != 0 or values.len / width > self.max_capture_rows) return error.CompactionQueryLimit;
            const rows = values.len / width;
            const end = try add(first_position, rows);
            if (first_position < self.source_length or end > self.source_length + self.repeat_length)
                return error.InvalidCompactionQueries;
            for (values) |value| if (!std.math.isFinite(value)) return error.NonFiniteAttention;
            for (self.samples, 0..) |sample, si| {
                if (sample.position < first_position or sample.position >= end) continue;
                const row: usize = @intCast(sample.position - first_position);
                for (0..num_heads) |head| {
                    const batch = self.batches[batch_start + head];
                    const target = @constCast(if (sample.heldout) batch.heldout else batch.train);
                    @memcpy(target[sample.index * head_dim ..][0..head_dim], values[row * width + head * head_dim ..][0..head_dim]);
                }
                self.seen[ci * self.samples.len + si] = true;
            }
            return;
        }
        return error.UnexpectedCompactionConsumer;
    }

    /// Compare every sampled head vector after validating identical coverage.
    pub fn compare(self: *const Collector, reference: *const Collector) !Agreement {
        _ = try self.finish();
        _ = try reference.finish();
        if (self.source_length != reference.source_length or self.repeat_length != reference.repeat_length or
            self.consumers.len != reference.consumers.len or self.values.len != reference.values.len or
            !std.mem.eql(u64, self.train_positions, reference.train_positions) or
            !std.mem.eql(u64, self.heldout_positions, reference.heldout_positions)) return error.InvalidCompactionQueries;
        var result = Agreement{};
        var offset: usize = 0;
        for (self.consumers, reference.consumers) |consumer, other| {
            if (!std.meta.eql(consumer, other)) return error.InvalidCompactionQueries;
            const count = consumer.query_heads * self.samples.len * consumer.dim;
            const row = try compareRows(self.values[offset..][0..count], reference.values[offset..][0..count], consumer.dim);
            result.max_absolute = @max(result.max_absolute, row.max_absolute);
            result.max_relative_l2 = @max(result.max_relative_l2, row.max_relative_l2);
            offset += count;
        }
        return result;
    }

    pub fn finish(self: *const Collector) ![]const engine.ReferenceBatch {
        if (std.mem.indexOfScalar(bool, self.seen, false) != null) return error.MissingCompactionConsumerQueries;
        return self.batches;
    }
};

const fixture = [_]Consumer{ .{ .layer = 0, .donor = 0, .query_heads = 4, .kv_heads = 2, .dim = 2 }, .{ .layer = 2, .donor = 0, .query_heads = 4, .kv_heads = 2, .dim = 2 } };

test "attention_match queries sample entire repeat and split every GQA/shared consumer" {
    var collector = try Collector.init(std.testing.allocator, &fixture, 8, 8, .{ .samples_per_consumer = 4, .max_capture_rows = 4 });
    defer collector.deinit();
    try std.testing.expectEqualSlices(u64, &.{ 10, 12, 15 }, collector.train_positions);
    try std.testing.expectEqualSlices(u64, &.{8}, collector.heldout_positions);
    try std.testing.expectError(error.MissingCompactionConsumerQueries, collector.finish());
    for (fixture) |consumer| for (0..2) |chunk| {
        var input: [32]f32 = undefined;
        for (&input, 0..) |*v, i| v.* = @floatFromInt(consumer.layer * 1000 + (8 + 4 * chunk + i / 8) * 10 + i % 8);
        try Collector.capture(&collector, 0, consumer.layer, 8 + chunk * 4, 4, 2, &input);
        // A fallback replay is idempotent, with no extra allocation/sample count.
        try Collector.capture(&collector, 0, consumer.layer, 8 + chunk * 4, 4, 2, &input);
    };
    const batches = try collector.finish();
    try std.testing.expectEqual(@as(usize, 8), batches.len);
    try std.testing.expectEqualSlices(f32, &.{ 100, 101, 120, 121, 150, 151 }, batches[0].train);
    try std.testing.expectEqualSlices(f32, &.{ 2086, 2087 }, batches[7].heldout);
}

test "attention_match queries reject missing consumers bad shapes nonfinite values and budgets" {
    var collector = try Collector.init(std.testing.allocator, &fixture, 8, 8, .{ .samples_per_consumer = 4, .max_capture_rows = 4 });
    defer collector.deinit();
    const input: [32]f32 = @splat(1);
    try std.testing.expectError(error.InvalidCompactionQueries, Collector.capture(&collector, 1, 0, 8, 4, 2, &input));
    try std.testing.expectError(error.InvalidCompactionQueries, Collector.capture(&collector, 0, 0, 7, 4, 2, &input));
    try std.testing.expectError(error.UnexpectedCompactionConsumer, Collector.capture(&collector, 0, 1, 8, 4, 2, &input));
    try Collector.capture(&collector, 0, 0, 8, 4, 2, &input);
    try std.testing.expectError(error.MissingCompactionConsumerQueries, collector.finish());
    var bad = input;
    bad[1] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteAttention, Collector.capture(&collector, 0, 2, 8, 4, 2, &bad));
    try std.testing.expectError(error.CompactionQueryLimit, Collector.init(std.testing.allocator, &fixture, 8, 8, .{ .max_bytes = 1 }));
    try std.testing.expectError(error.CompactionQueryLimit, peakBytes(&fixture, std.math.maxInt(usize), 8, .{}));
}

fn allocationFailure(allocator: std.mem.Allocator) !void {
    var collector = try Collector.init(allocator, &fixture, 8, 8, .{});
    defer collector.deinit();
}
test "attention_match queries clean up all initialization allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailure, .{});
}

test "attention match query comparison checks every head and coverage" {
    const consumers = [_]Consumer{.{ .layer = 0, .donor = 0, .query_heads = 2, .kv_heads = 1, .dim = 2 }};
    var candidate = try Collector.init(std.testing.allocator, &consumers, 2, 2, .{});
    defer candidate.deinit();
    var reference = try Collector.init(std.testing.allocator, &consumers, 2, 2, .{});
    defer reference.deinit();
    const values = [_]f32{ 1, 0, 0, 2, 3, 0, 0, 4 };
    try Collector.capture(&candidate, 0, 0, 2, 2, 2, &values);
    try std.testing.expectError(error.MissingCompactionConsumerQueries, candidate.compare(&reference));
    try Collector.capture(&reference, 0, 0, 2, 2, 2, &values);
    try std.testing.expectEqual(@as(f64, 0), (try candidate.compare(&reference)).max_relative_l2);
    candidate.values[candidate.values.len - 1] += 0.5;
    const agreement = try candidate.compare(&reference);
    try std.testing.expectEqual(@as(f64, 0.5), agreement.max_absolute);
    try std.testing.expect(agreement.max_relative_l2 > 0.1);
    reference.heldout_positions[0] += 1;
    try std.testing.expectError(error.InvalidCompactionQueries, candidate.compare(&reference));
    reference.heldout_positions[0] -= 1;
    candidate.values[0] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteAttention, candidate.compare(&reference));
}

test "attention match bounded study includes rare earlier spans and exact recent suffix" {
    var source: [32768]i64 = @splat(7);
    for (source[630..650], 0..) |*token, i| token.* = @intCast(100 + i);
    const before = source;
    const study = try studyTokens(std.testing.allocator, &source, 512);
    defer std.testing.allocator.free(study);
    try std.testing.expectEqual(@as(usize, 512), study.len);
    try std.testing.expect(std.mem.indexOf(i64, study, source[630..650]) != null);
    try std.testing.expectEqualSlices(i64, source[source.len - 128 ..], study[study.len - 128 ..]);
    try std.testing.expectEqualSlices(i64, &before, &source);
    const again = try studyTokens(std.testing.allocator, &source, 512);
    defer std.testing.allocator.free(again);
    try std.testing.expectEqualSlices(i64, study, again);
}

test "attention match bounded study is an ordered prefix subsequence at exact budgets" {
    var source: [1024]i64 = undefined;
    for (&source, 0..) |*token, i| token.* = @intCast(i);
    for ([_]usize{ 2, 255, 256, 257, 511, 512, 513, 1023, 1024, 2048 }) |limit| {
        const study = try studyTokens(std.testing.allocator, &source, limit);
        defer std.testing.allocator.free(study);
        try std.testing.expectEqual(@min(source.len, limit), study.len);
        for (study[1..], study[0 .. study.len - 1]) |next, previous|
            try std.testing.expect(next > previous);
        try std.testing.expectEqual(source[source.len - 1], study[study.len - 1]);
        if (limit >= source.len) try std.testing.expectEqualSlices(i64, &source, study);
    }
    try std.testing.expectError(error.CompactionQueryLimit, studyTokens(std.testing.allocator, &source, 1));
    try std.testing.expectError(error.InvalidCompactionQueries, studyTokens(std.testing.allocator, &.{ 1, -1 }, 2));
}

test "attention match bounded study retains rare data behind distinct tool declarations" {
    var source: [32768]i64 = @splat(7);
    // A distinct declaration occupies the two highest-scoring windows. The
    // archived identifier is less novel, but must still enter calibration.
    for (source[0..128], 0..) |*token, i| token.* = @intCast(1000 + i);
    for (source[3352..3372], 0..) |*token, i| token.* = @intCast(100 + i);
    const before = source;
    const study = try studyTokens(std.testing.allocator, &source, 512);
    defer std.testing.allocator.free(study);
    try std.testing.expectEqual(@as(usize, 512), study.len);
    try std.testing.expect(std.mem.indexOf(i64, study, source[3352..3372]) != null);
    try std.testing.expectEqualSlices(i64, source[source.len - 256 ..], study[study.len - 256 ..]);
    try std.testing.expectEqualSlices(i64, &before, &source);
}

fn studyAllocationFailure(allocator: std.mem.Allocator) !void {
    const source: [1024]i64 = @splat(1);
    const study = try studyTokens(allocator, &source, 512);
    defer allocator.free(study);
}

test "attention match bounded study unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, studyAllocationFailure, .{});
}
