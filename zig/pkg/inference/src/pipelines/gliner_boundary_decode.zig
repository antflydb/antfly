// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Deterministic sparse boundary decoding. Spans are half-open; duplicate
//! surfaces at different offsets remain distinct. Policies match Fastino
//! commit 3c913c7369301133d3b7699252074c4303ada50e, inference/overlap.py.
const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub const OverlapPolicy = enum { allow, nested, flat, longest };
pub const Candidate = struct {
    start: usize,
    end: usize,
    probability: f32,
    source_index: usize,
};
pub const Limits = struct { max_candidates: usize = 4096, control: ?Control = null };
const Ranked = struct {
    candidate: Candidate,
    order: usize,
    rank: usize = 0,

    fn byRank(_: void, a: Ranked, b: Ranked) bool {
        if (a.candidate.probability != b.candidate.probability) return a.candidate.probability > b.candidate.probability;
        if (a.candidate.start != b.candidate.start) return a.candidate.start < b.candidate.start;
        if (a.candidate.end != b.candidate.end) return a.candidate.end < b.candidate.end;
        return a.order < b.order;
    }
    fn byEnd(_: void, a: Ranked, b: Ranked) bool {
        if (a.candidate.end != b.candidate.end) return a.candidate.end < b.candidate.end;
        if (a.candidate.start != b.candidate.start) return a.candidate.start < b.candidate.start;
        return byRank({}, a, b);
    }
};

fn contains(a: Candidate, b: Candidate) bool {
    return a.start <= b.start and b.end <= a.end;
}

/// Caller owns the returned slice. Flat decoding uses exact weighted interval
/// scheduling, including count and stable-rank tie breaks. The explicit bound
/// caps its O(n² / 64) selection history and O(n²) deduplication work.
pub fn resolveOverlaps(allocator: std.mem.Allocator, candidates: []const Candidate, policy: OverlapPolicy, limits: Limits) ![]Candidate {
    if (limits.control) |control| try control.check();
    if (candidates.len > limits.max_candidates) return error.ExtractionCandidateLimitExceeded;
    const ranked = try allocator.alloc(Ranked, candidates.len);
    defer allocator.free(ranked);
    for (candidates, 0..) |candidate, i| {
        if (candidate.end <= candidate.start) return error.InvalidBoundarySpan;
        if (!std.math.isFinite(candidate.probability)) return error.NonFiniteBoundaryScore;
        ranked[i] = .{ .candidate = candidate, .order = i };
    }
    std.mem.sort(Ranked, ranked, {}, Ranked.byRank);
    var n: usize = 0;
    for (ranked) |row| {
        if (limits.control) |control| try control.check();
        var duplicate = false;
        for (ranked[0..n]) |existing| {
            if (row.candidate.start == existing.candidate.start and row.candidate.end == existing.candidate.end) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            ranked[n] = row;
            ranked[n].rank = n;
            n += 1;
        }
    }
    const keep = try allocator.alloc(bool, n);
    defer allocator.free(keep);
    @memset(keep, false);
    switch (policy) {
        .allow => @memset(keep, true),
        .nested => for (ranked[0..n], 0..) |row, i| {
            if (limits.control) |control| try control.check();
            keep[i] = true;
            for (ranked[0..i], 0..) |previous, j| {
                if (!keep[j]) continue;
                const a = row.candidate;
                const b = previous.candidate;
                if (a.start < b.end and b.start < a.end and !contains(a, b) and !contains(b, a)) {
                    keep[i] = false;
                    break;
                }
            }
        },
        .longest => for (ranked[0..n], 0..) |row, i| {
            if (limits.control) |control| try control.check();
            keep[i] = true;
            for (ranked[0..n], 0..) |other, j| {
                if (i != j and contains(other.candidate, row.candidate)) {
                    keep[i] = false;
                    break;
                }
            }
        },
        .flat => if (n > 0) {
            const by_end = try allocator.dupe(Ranked, ranked[0..n]);
            defer allocator.free(by_end);
            std.mem.sort(Ranked, by_end, {}, Ranked.byEnd);
            const words = try std.math.divCeil(usize, n, 64);
            const history = try allocator.alloc(u64, try std.math.mul(usize, n + 1, words));
            defer allocator.free(history);
            @memset(history, 0);
            const scores = try allocator.alloc(f64, n + 1);
            defer allocator.free(scores);
            const counts = try allocator.alloc(usize, n + 1);
            defer allocator.free(counts);
            scores[0] = 0;
            counts[0] = 0;
            for (by_end, 0..) |row, i| {
                if (limits.control) |control| try control.check();
                var lo: usize = 0;
                var hi = i;
                while (lo < hi) {
                    const mid = lo + (hi - lo) / 2;
                    if (by_end[mid].candidate.end <= row.candidate.start) lo = mid + 1 else hi = mid;
                }
                const previous = history[lo * words ..][0..words];
                const without = history[i * words ..][0..words];
                const with = history[(i + 1) * words ..][0..words];
                @memcpy(with, previous);
                with[row.rank / 64] |= @as(u64, 1) << @as(u6, @intCast(row.rank % 64));
                const score = scores[lo] + row.candidate.probability;
                const count = counts[lo] + 1;
                var prefer = score > scores[i];
                if (score == scores[i]) {
                    prefer = count > counts[i];
                    if (count == counts[i]) {
                        for (with, without) |a, b| {
                            const different = a ^ b;
                            if (different != 0) {
                                const first: u6 = @intCast(@ctz(different));
                                prefer = a & (@as(u64, 1) << first) != 0;
                                break;
                            }
                        }
                    }
                }
                scores[i + 1] = if (prefer) score else scores[i];
                counts[i + 1] = if (prefer) count else counts[i];
                if (!prefer) @memcpy(with, without);
            }
            const selected = history[n * words ..][0..words];
            for (keep, 0..) |*enabled, i| enabled.* = selected[i / 64] & (@as(u64, 1) << @as(u6, @intCast(i % 64))) != 0;
        },
    }
    var count: usize = 0;
    for (keep) |enabled| count += @intFromBool(enabled);
    const result = try allocator.alloc(Candidate, count);
    var i: usize = 0;
    for (ranked[0..n], keep) |row, enabled| if (enabled) {
        result[i] = row.candidate;
        i += 1;
    };
    return result;
}

pub fn sigmoid(value: f32) f32 {
    if (value >= 0) return 1 / (1 + @exp(-value));
    const e = @exp(value);
    return e / (1 + e);
}

pub const ScoredSpan = struct { start: usize, end: usize, logit: f32, valid: bool = true };
pub const QueryOptions = struct {
    threshold: f32 = 0.5,
    pair_temperature: f32 = 1,
    null_logit: ?f32 = null,
    abstention_threshold: f32 = 0.5,
    count_log_rate: ?f32 = null,
    adaptive_threshold: bool = false,
    policy: OverlapPolicy = .flat,
    limits: Limits = .{},
};

/// Count guidance only adds candidates; it never removes threshold hits.
/// Abstention uses a strict > comparison, as in the pinned boundary engine.
pub fn decodeQuery(allocator: std.mem.Allocator, spans: []const ScoredSpan, options: QueryOptions) ![]Candidate {
    if (options.limits.control) |control| try control.check();
    if (spans.len > options.limits.max_candidates) return error.ExtractionCandidateLimitExceeded;
    if (!std.math.isFinite(options.threshold) or options.threshold < 0 or options.threshold > 1 or
        !std.math.isFinite(options.pair_temperature) or options.pair_temperature <= 0 or
        !std.math.isFinite(options.abstention_threshold) or options.abstention_threshold < 0 or options.abstention_threshold > 1)
        return error.InvalidDecodeThreshold;
    if (options.adaptive_threshold and options.count_log_rate == null) return error.MissingCountLogRate;
    if (options.count_log_rate) |rate| if (!std.math.isFinite(rate)) return error.NonFiniteBoundaryScore;
    if (options.null_logit) |logit| {
        if (!std.math.isFinite(logit)) return error.NonFiniteBoundaryScore;
        if (sigmoid(logit) > options.abstention_threshold) return allocator.alloc(Candidate, 0);
    }
    const ranked = try allocator.alloc(Ranked, spans.len);
    defer allocator.free(ranked);
    var n: usize = 0;
    for (spans, 0..) |span, i| {
        if (!span.valid) continue;
        if (span.start >= span.end) return error.InvalidBoundarySpan;
        if (!std.math.isFinite(span.logit)) return error.NonFiniteBoundaryScore;
        ranked[n] = .{ .candidate = .{ .start = span.start, .end = span.end, .probability = sigmoid(span.logit / options.pair_temperature), .source_index = i }, .order = i };
        n += 1;
    }
    // Count selection ties follow original candidate order, before the final
    // confidence/start/end ordering applied by overlap resolution.
    const CountOrder = struct {
        fn less(_: void, a: Ranked, b: Ranked) bool {
            return if (a.candidate.probability != b.candidate.probability) a.candidate.probability > b.candidate.probability else a.order < b.order;
        }
    };
    std.mem.sort(Ranked, ranked[0..n], {}, CountOrder.less);
    var predicted: usize = 0;
    if (options.adaptive_threshold and n > 0) {
        const rate = @exp(options.count_log_rate.?);
        // Upstream's float->int64 overflow wraps to a negative prediction.
        // Reject that undefined model output instead of silently changing the
        // selection into an empty or all-candidate set.
        if (!std.math.isFinite(rate) or rate >= 9223372036854775808.0) return error.InvalidCountPrediction;
        if (rate >= @as(f32, @floatFromInt(n))) {
            predicted = n;
        } else {
            const floor = @floor(rate);
            predicted = @intFromFloat(floor);
            const fraction = rate - floor;
            if (fraction > 0.5 or (fraction == 0.5 and predicted % 2 == 1)) predicted += 1;
        }
    }
    const eligible = try allocator.alloc(Candidate, n);
    defer allocator.free(eligible);
    var count: usize = 0;
    for (ranked[0..n], 0..) |row, rank| {
        if (row.candidate.probability >= options.threshold or rank < predicted) {
            eligible[count] = row.candidate;
            count += 1;
        }
    }
    return resolveOverlaps(allocator, eligible[0..count], options.policy, options.limits);
}

pub const OffsetUnit = enum { utf8_bytes, unicode_codepoints, utf16_codeunits };
pub const Offsets = struct { start: usize, end: usize };
const OffsetEntry = struct { byte: usize, utf16: usize };

/// One immutable index per document, reused across queries and chunk merges.
pub const OffsetMap = struct {
    allocator: std.mem.Allocator,
    entries: []OffsetEntry,

    pub fn init(allocator: std.mem.Allocator, text: []const u8, max_bytes: usize) !OffsetMap {
        if (text.len > max_bytes) return error.ExtractionTextLimitExceeded;
        const view = try std.unicode.Utf8View.init(text);
        var iter = view.iterator();
        var count: usize = 0;
        while (iter.nextCodepoint() != null) count += 1;
        const entries = try allocator.alloc(OffsetEntry, count + 1);
        iter = view.iterator();
        var utf16: usize = 0;
        var i: usize = 0;
        entries[0] = .{ .byte = 0, .utf16 = 0 };
        while (iter.nextCodepoint()) |cp| {
            utf16 += if (cp > 0xffff) @as(usize, 2) else 1;
            i += 1;
            entries[i] = .{ .byte = iter.i, .utf16 = utf16 };
        }
        return .{ .allocator = allocator, .entries = entries };
    }

    pub fn deinit(self: *OffsetMap) void {
        self.allocator.free(self.entries);
        self.* = undefined;
    }

    fn find(self: OffsetMap, byte: usize) !usize {
        var lo: usize = 0;
        var hi = self.entries.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.entries[mid].byte < byte) lo = mid + 1 else hi = mid;
        }
        if (lo == self.entries.len or self.entries[lo].byte != byte) return error.InvalidUtf8Boundary;
        return lo;
    }

    pub fn convert(self: OffsetMap, bytes: Offsets, unit: OffsetUnit) !Offsets {
        if (bytes.end < bytes.start) return error.InvalidBoundarySpan;
        const start = try self.find(bytes.start);
        const end = try self.find(bytes.end);
        return switch (unit) {
            .utf8_bytes => bytes,
            .unicode_codepoints => .{ .start = start, .end = end },
            .utf16_codeunits => .{ .start = self.entries[start].utf16, .end = self.entries[end].utf16 },
        };
    }

    fn fromUnit(self: OffsetMap, offset: usize, unit: OffsetUnit) !usize {
        if (unit == .utf8_bytes) return self.find(offset);
        if (unit == .unicode_codepoints) {
            if (offset >= self.entries.len) return error.InvalidCodepointBoundary;
            return offset;
        }
        var lo: usize = 0;
        var hi = self.entries.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.entries[mid].utf16 < offset) lo = mid + 1 else hi = mid;
        }
        // A surrogate-pair interior is not a source character boundary.
        if (lo == self.entries.len or self.entries[lo].utf16 != offset) return error.InvalidUtf16Boundary;
        return lo;
    }

    pub fn toBytes(self: OffsetMap, offsets: Offsets, unit: OffsetUnit) !Offsets {
        if (offsets.end < offsets.start) return error.InvalidBoundarySpan;
        const start = try self.fromUnit(offsets.start, unit);
        const end = try self.fromUnit(offsets.end, unit);
        return .{ .start = self.entries[start].byte, .end = self.entries[end].byte };
    }
};

test "gliner boundary overlap flat solves global score and deterministic ties" {
    const a = std.testing.allocator;
    const spans = [_]Candidate{
        .{ .start = 0, .end = 4, .probability = 0.9, .source_index = 0 },
        .{ .start = 0, .end = 2, .probability = 0.6, .source_index = 1 },
        .{ .start = 2, .end = 4, .probability = 0.6, .source_index = 2 },
        .{ .start = 2, .end = 4, .probability = 0.5, .source_index = 3 },
    };
    const flat = try resolveOverlaps(a, &spans, .flat, .{});
    defer a.free(flat);
    try std.testing.expectEqual(@as(usize, 2), flat.len);
    try std.testing.expectEqual(@as(usize, 1), flat[0].source_index);
    try std.testing.expectEqual(@as(usize, 2), flat[1].source_index);
    const longest = try resolveOverlaps(a, &spans, .longest, .{});
    defer a.free(longest);
    try std.testing.expectEqual(@as(usize, 1), longest.len);
    try std.testing.expectEqual(@as(usize, 0), longest[0].source_index);
    const zero = [_]Candidate{
        .{ .start = 0, .end = 2, .probability = 0, .source_index = 0 },
        .{ .start = 0, .end = 1, .probability = 0, .source_index = 1 },
        .{ .start = 1, .end = 2, .probability = 0, .source_index = 2 },
    };
    const tied = try resolveOverlaps(a, &zero, .flat, .{});
    defer a.free(tied);
    try std.testing.expectEqual(@as(usize, 2), tied.len);
}

test "gliner boundary overlap nested rejects crossings but permits containment and repeated surfaces" {
    const a = std.testing.allocator;
    const spans = [_]Candidate{
        .{ .start = 0, .end = 4, .probability = 0.9, .source_index = 0 },
        .{ .start = 1, .end = 3, .probability = 0.8, .source_index = 1 },
        .{ .start = 2, .end = 5, .probability = 0.7, .source_index = 2 },
        .{ .start = 5, .end = 9, .probability = 0.6, .source_index = 3 },
    };
    const result = try resolveOverlaps(a, &spans, .nested, .{});
    defer a.free(result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(usize, 3), result[2].source_index);
}

test "gliner boundary decode applies temperature abstention and additive count guidance" {
    const a = std.testing.allocator;
    const spans = [_]ScoredSpan{
        .{ .start = 0, .end = 1, .logit = 4 },
        .{ .start = 2, .end = 3, .logit = -2 },
        .{ .start = 4, .end = 5, .logit = -3 },
        .{ .start = 0, .end = 0, .logit = std.math.nan(f32), .valid = false },
    };
    const result = try decodeQuery(a, &spans, .{ .adaptive_threshold = true, .count_log_rate = @log(@as(f32, 2)), .pair_temperature = 2, .null_logit = 0 });
    defer a.free(result);
    try std.testing.expectEqual(@as(usize, 2), result.len);
    try std.testing.expectApproxEqAbs(sigmoid(2), result[0].probability, 1e-6);
    const abstained = try decodeQuery(a, &spans, .{ .null_logit = 1 });
    defer a.free(abstained);
    try std.testing.expectEqual(@as(usize, 0), abstained.len);
    try std.testing.expectError(error.MissingCountLogRate, decodeQuery(a, &spans, .{ .adaptive_threshold = true }));
    try std.testing.expectError(error.InvalidCountPrediction, decodeQuery(a, &spans, .{ .adaptive_threshold = true, .count_log_rate = 100 }));
}

test "gliner boundary offsets preserve astral combining and exact utf8 boundaries" {
    var map = try OffsetMap.init(std.testing.allocator, "A😀é中", 64);
    defer map.deinit();
    try std.testing.expectEqual(Offsets{ .start = 1, .end = 2 }, try map.convert(.{ .start = 1, .end = 5 }, .unicode_codepoints));
    try std.testing.expectEqual(Offsets{ .start = 1, .end = 3 }, try map.convert(.{ .start = 1, .end = 5 }, .utf16_codeunits));
    try std.testing.expectEqual(Offsets{ .start = 3, .end = 6 }, try map.convert(.{ .start = 5, .end = 11 }, .utf16_codeunits));
    try std.testing.expectError(error.InvalidUtf8Boundary, map.convert(.{ .start = 2, .end = 5 }, .utf8_bytes));
    try std.testing.expectError(error.InvalidUtf8, OffsetMap.init(std.testing.allocator, "\xff", 64));
    for (map.entries) |start| for (map.entries) |end| {
        if (start.byte > end.byte) continue;
        const bytes = Offsets{ .start = start.byte, .end = end.byte };
        for ([_]OffsetUnit{ .utf8_bytes, .unicode_codepoints, .utf16_codeunits }) |unit|
            try std.testing.expectEqual(bytes, try map.toBytes(try map.convert(bytes, unit), unit));
    };
    try std.testing.expectError(error.InvalidUtf16Boundary, map.toBytes(.{ .start = 2, .end = 3 }, .utf16_codeunits));
    try std.testing.expectError(error.InvalidCodepointBoundary, map.toBytes(.{ .start = 0, .end = 6 }, .unicode_codepoints));
    try std.testing.expectError(error.InvalidBoundarySpan, map.toBytes(.{ .start = 3, .end = 2 }, .unicode_codepoints));
}

test "gliner boundary decoder unwinds allocation failures and cancellation" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const input = [_]ScoredSpan{
                .{ .start = 0, .end = 2, .logit = 0.5 },
                .{ .start = 1, .end = 3, .logit = 0.75 },
                .{ .start = 3, .end = 4, .logit = -0.5 },
            };
            const result = try decodeQuery(a, &input, .{ .threshold = 0 });
            defer a.free(result);
            try std.testing.expectEqual(@as(usize, 2), result.len);
            var offsets = try OffsetMap.init(a, "a😀b", 64);
            defer offsets.deinit();
        }
        fn cancel(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
    try std.testing.expectError(error.Cancelled, resolveOverlaps(std.testing.allocator, &.{}, .flat, .{ .control = .{ .check_fn = Check.cancel } }));
    try std.testing.expectError(error.ExtractionCandidateLimitExceeded, decodeQuery(std.testing.allocator, &.{.{ .start = 0, .end = 1, .logit = 0 }}, .{ .limits = .{ .max_candidates = 0 } }));
}
