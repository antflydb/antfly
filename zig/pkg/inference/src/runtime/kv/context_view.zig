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

//! Immutable descriptor contract for per-owner-layer context storage.
//! Absolute model positions, retained rows and physical capacity are distinct.
//! Rings are represented by two exact spans when their physical range wraps.
const std = @import("std");

pub const SpanKind = enum(u32) { exact = 0, compacted = 1 };
pub const max_spans = 1024;
pub const max_heads = 64;
pub const interleaved_abi_version: u32 = 1;
pub const abi_version: u32 = 2;

/// ABI 2 packs each KV head independently. All offsets/counts address rows of
/// one head, not interleaved rows. Span physical/bias offsets are head-relative.
/// Capacity includes reserved exact continuation, which is not yet in spans.
pub const Head = extern struct {
    kv_start: u32,
    capacity_rows: u32,
    bias_start: u32,
    bias_rows: u32,
    span_start: u32,
    span_count: u32,
};
comptime {
    std.debug.assert(@sizeOf(Head) == 24);
    std.debug.assert(@sizeOf(Span) == 24);
}

/// Mirrored by context_attention.metal. Keys are already rotated at their
/// original positions; compacted spans are only visible after the entire
/// source block. Bias offsets address rows, with one FP32 value per KV head.
pub const Span = extern struct {
    physical_start: u32,
    row_count: u32,
    logical_start: u32,
    logical_count: u32,
    kind: SpanKind = .exact,
    bias_start: u32 = 0,
};

pub const View = struct {
    version: u32 = interleaved_abi_version,
    epoch: u64,
    logical_tokens: u32,
    /// ABI 1: capacity per head, interleaved. ABI 2: sum of head capacities.
    capacity_rows: u32,
    bias_rows: u32 = 0,
    kv_heads: u32,
    head_dim: u32,
    spans: []const Span,
    heads: []const Head = &.{},

    pub fn headSpans(self: View, head: usize) []const Span {
        if (self.version == interleaved_abi_version) return self.spans;
        const h = self.heads[head];
        return self.spans[h.span_start..][0..h.span_count];
    }
    pub fn kvElements(self: View) u64 {
        return @as(u64, self.capacity_rows) * self.head_dim *
            (if (self.version == interleaved_abi_version) self.kv_heads else @as(u32, 1));
    }
    pub fn biasElements(self: View) u64 {
        return @as(u64, self.bias_rows) *
            (if (self.version == interleaved_abi_version) self.kv_heads else @as(u32, 1));
    }

    /// Call on a validated view. Every head must retain the complete exact
    /// interval, including the causal window when physical storage wraps.
    pub fn coversExact(self: View, start: u32, end: u32) bool {
        if (start >= end or end > self.logical_tokens) return false;
        for (0..self.kv_heads) |h| {
            var covered = start;
            for (self.headSpans(h)) |span| {
                if (span.kind != .exact) continue;
                const span_end = @as(u64, span.logical_start) + span.logical_count;
                if (span_end <= covered) continue;
                if (span.logical_start > covered) return false;
                covered = @intCast(span_end);
                if (covered >= end) break;
            }
            if (covered < end) return false;
        }
        return true;
    }

    /// Returns the maximum retained rows of any head, not the sum or average.
    /// Packed descriptors partition both arenas and the span array exactly;
    /// aliases, orphan descriptors and hidden uncharged ranges are rejected.
    pub fn validate(self: View) !u32 {
        if (self.epoch == 0 or self.capacity_rows == 0 or self.kv_heads == 0 or self.kv_heads > max_heads or
            self.head_dim == 0 or self.head_dim > 512 or self.spans.len == 0)
            return error.InvalidContextView;
        if (self.version == interleaved_abi_version) {
            if (self.heads.len != 0) return error.InvalidContextView;
            return self.validateSpans(self.spans, self.capacity_rows, self.bias_rows);
        }
        if (self.version != abi_version or self.kv_heads > max_heads or self.heads.len != self.kv_heads or
            self.spans.len > max_spans * self.kv_heads) return error.InvalidContextView;
        var kv_end: u64 = 0;
        var bias_end: u64 = 0;
        var span_end: u64 = 0;
        var retained: u32 = 0;
        for (self.heads) |h| {
            if (h.kv_start != kv_end or h.bias_start != bias_end or h.span_start != span_end or
                h.capacity_rows == 0 or h.span_count == 0 or h.span_count > max_spans) return error.InvalidContextView;
            kv_end += h.capacity_rows;
            bias_end += h.bias_rows;
            span_end += h.span_count;
            if (kv_end > self.capacity_rows or bias_end > self.bias_rows or span_end > self.spans.len)
                return error.InvalidContextView;
            retained = @max(retained, try self.validateSpans(self.spans[h.span_start..][0..h.span_count], h.capacity_rows, h.bias_rows));
        }
        if (kv_end != self.capacity_rows or bias_end != self.bias_rows or span_end != self.spans.len)
            return error.InvalidContextView;
        return retained;
    }

    fn validateSpans(self: View, spans: []const Span, capacity: u32, biases: u32) !u32 {
        if (spans.len == 0 or spans.len > max_spans) return error.InvalidContextView;
        var logical_end: u32 = 0;
        var retained: u32 = 0;
        for (spans, 0..) |span, index| {
            if (span.row_count == 0 or span.logical_count == 0 or span.logical_start < logical_end)
                return error.InvalidContextView;
            logical_end = std.math.add(u32, span.logical_start, span.logical_count) catch return error.InvalidContextView;
            if (logical_end > self.logical_tokens) return error.InvalidContextView;
            const physical_end = std.math.add(u32, span.physical_start, span.row_count) catch return error.InvalidContextView;
            if (physical_end > capacity) return error.InvalidContextView;
            for (spans[0..index]) |prior| {
                const prior_end = @as(u64, prior.physical_start) + prior.row_count;
                if (span.physical_start < prior_end and prior.physical_start < physical_end) return error.InvalidContextView;
            }
            switch (span.kind) {
                .exact => if (span.row_count != span.logical_count) return error.InvalidContextView,
                .compacted => {
                    if (span.logical_count != 512 or span.row_count >= 512 or
                        (self.version == interleaved_abi_version and span.row_count != 256)) return error.InvalidContextView;
                    const bias_end = std.math.add(u32, span.bias_start, span.row_count) catch return error.InvalidContextView;
                    if (bias_end > biases) return error.InvalidContextView;
                    for (spans[0..index]) |prior| {
                        if (prior.kind == .compacted and span.bias_start < @as(u64, prior.bias_start) + prior.row_count and
                            prior.bias_start < bias_end) return error.InvalidContextView;
                    }
                },
            }
            retained = std.math.add(u32, retained, span.row_count) catch return error.InvalidContextView;
        }
        return retained;
    }
};

/// Sequence-lock-owned publication state. The storage owner keeps both
/// generations allocated until commit/cancel returns and the previous read
/// leases are drained. One transaction covers all owner layers. GPU completion
/// is supplied only by the backend's command-buffer completion path.
pub const Publication = struct {
    epoch: u64 = 1,
    issued_epoch: u64 = 1,
    readers: usize = 0,
    pending: bool = false,
    submitted: bool = false,
    completion: enum { pending, succeeded, failed } = .pending,

    pub fn acquire(self: *Publication) !u64 {
        self.readers = std.math.add(usize, self.readers, 1) catch return error.ContextReadersOverflow;
        return self.epoch;
    }
    pub fn release(self: *Publication, epoch: u64) !void {
        if (epoch != self.epoch or self.readers == 0) return error.StaleContextView;
        self.readers -= 1;
    }
    pub fn begin(self: *Publication) !u64 {
        if (self.pending) return error.ContextTransactionBusy;
        if (self.issued_epoch == std.math.maxInt(u64)) return error.ContextEpochOverflow;
        self.issued_epoch += 1;
        self.pending = true;
        self.submitted = false;
        self.completion = .pending;
        return self.issued_epoch;
    }
    pub fn submit(self: *Publication) !void {
        if (!self.pending or self.submitted) return error.InvalidContextTransaction;
        self.submitted = true;
    }
    pub fn complete(self: *Publication, candidate_epoch: u64, success: bool) !void {
        if (candidate_epoch != self.issued_epoch) return error.StaleContextView;
        if (!self.pending or !self.submitted or self.completion != .pending) return error.InvalidContextTransaction;
        self.completion = if (success) .succeeded else .failed;
    }
    /// Call only after every candidate owner layer has validated. No old
    /// allocation may be retired before this succeeds.
    pub fn commit(self: *Publication, candidate_epoch: u64) !void {
        if (!self.pending or candidate_epoch != self.issued_epoch) return error.StaleContextView;
        if (self.completion == .pending) return error.ContextGpuPending;
        if (self.completion == .failed) return error.ContextGpuFailed;
        if (self.readers != 0) return error.ContextReadersActive;
        self.epoch = candidate_epoch;
        self.pending = false;
    }
    /// Successful cancellation permits freeing staged allocations. Submitted
    /// GPU work must have finished even when its result will be discarded.
    pub fn cancel(self: *Publication) !void {
        if (!self.pending) return error.InvalidContextTransaction;
        if (self.submitted and self.completion == .pending) return error.ContextGpuPending;
        self.pending = false;
    }
};

test "context view keeps wrapped storage independent from absolute positions" {
    const spans = [_]Span{
        .{ .physical_start = 7, .row_count = 3, .logical_start = 97, .logical_count = 3 },
        .{ .physical_start = 0, .row_count = 5, .logical_start = 100, .logical_count = 5 },
    };
    const view = View{ .epoch = 4, .logical_tokens = 105, .capacity_rows = 10, .kv_heads = 1, .head_dim = 256, .spans = &spans };
    try std.testing.expectEqual(@as(u32, 8), try view.validate());
    try std.testing.expect(view.coversExact(98, 105));
    try std.testing.expect(!view.coversExact(96, 105));
    var invalid = spans;
    invalid[1].physical_start = 6;
    var bad = view;
    bad.spans = &invalid;
    try std.testing.expectError(error.InvalidContextView, bad.validate());
    invalid = spans;
    invalid[1].logical_start = 99;
    try std.testing.expectError(error.InvalidContextView, bad.validate());
}

test "context publication never retires pending readers or GPU writes" {
    var state = Publication{};
    const old = try state.acquire();
    const candidate = try state.begin();
    try state.submit();
    try std.testing.expectError(error.ContextGpuPending, state.commit(candidate));
    try std.testing.expectError(error.ContextGpuPending, state.cancel());
    try state.complete(candidate, true);
    try std.testing.expectError(error.ContextReadersActive, state.commit(candidate));
    try state.release(old);
    try state.commit(candidate);
    try std.testing.expectError(error.StaleContextView, state.release(old));
    const failed = try state.begin();
    try state.submit();
    try state.complete(failed, false);
    try std.testing.expectError(error.ContextGpuFailed, state.commit(failed));
    try state.cancel();
    try std.testing.expectEqual(candidate, state.epoch);
    const next = try state.begin();
    try std.testing.expect(next > failed);
    try state.submit();
    try std.testing.expectError(error.StaleContextView, state.complete(failed, true));
    try state.complete(next, true);
    try state.cancel();
}

test "context packed view validates every head and enforces the maximum retained count" {
    var spans = [_]Span{
        .{ .physical_start = 0, .row_count = 128, .logical_start = 0, .logical_count = 512, .kind = .compacted },
        .{ .physical_start = 128, .row_count = 2, .logical_start = 512, .logical_count = 2 },
        .{ .physical_start = 0, .row_count = 384, .logical_start = 0, .logical_count = 512, .kind = .compacted },
        .{ .physical_start = 384, .row_count = 2, .logical_start = 512, .logical_count = 2 },
        .{ .physical_start = 0, .row_count = 514, .logical_start = 0, .logical_count = 514 },
    };
    var heads = [_]Head{
        .{ .kv_start = 0, .capacity_rows = 132, .bias_start = 0, .bias_rows = 128, .span_start = 0, .span_count = 2 },
        .{ .kv_start = 132, .capacity_rows = 388, .bias_start = 128, .bias_rows = 384, .span_start = 2, .span_count = 2 },
        .{ .kv_start = 520, .capacity_rows = 516, .bias_start = 512, .bias_rows = 0, .span_start = 4, .span_count = 1 },
    };
    var view = View{ .version = abi_version, .epoch = 1, .logical_tokens = 514, .capacity_rows = 1036, .bias_rows = 512, .kv_heads = 3, .head_dim = 512, .spans = &spans, .heads = &heads };
    try std.testing.expectEqual(@as(u32, 514), try view.validate());
    try std.testing.expectEqual(@as(u64, 1036 * 512), view.kvElements());
    try std.testing.expectEqual(@as(u64, 512), view.biasElements());
    try std.testing.expect(view.coversExact(512, 514));
    try std.testing.expect(!view.coversExact(0, 514));
    for (0..heads.len) |h| {
        const saved = heads[h];
        heads[h].kv_start +%= 1;
        try std.testing.expectError(error.InvalidContextView, view.validate());
        heads[h] = saved;
        heads[h].bias_start +%= 1;
        try std.testing.expectError(error.InvalidContextView, view.validate());
        heads[h] = saved;
        heads[h].span_start +%= 1;
        try std.testing.expectError(error.InvalidContextView, view.validate());
        heads[h] = saved;
        heads[h].span_count = std.math.maxInt(u32);
        try std.testing.expectError(error.InvalidContextView, view.validate());
        heads[h] = saved;
    }
    view.kv_heads = std.math.maxInt(u32);
    try std.testing.expectError(error.InvalidContextView, view.validate());
    view.kv_heads = 3;
    view.version = 3;
    try std.testing.expectError(error.InvalidContextView, view.validate());
    view.version = interleaved_abi_version;
    try std.testing.expectError(error.InvalidContextView, view.validate());
    view.version = abi_version;
    spans[3].physical_start = 383;
    try std.testing.expectError(error.InvalidContextView, view.validate());
    spans[3].physical_start = 384;
    spans[3].row_count = 1;
    spans[3].logical_count = 1;
    _ = try view.validate();
    try std.testing.expect(!view.coversExact(512, 514));
}

test "context packed view rejects overlapping mass ranges" {
    const spans = [_]Span{
        .{ .physical_start = 0, .row_count = 128, .logical_start = 0, .logical_count = 512, .kind = .compacted },
        .{ .physical_start = 128, .row_count = 128, .logical_start = 512, .logical_count = 512, .kind = .compacted, .bias_start = 127 },
    };
    const heads = [_]Head{.{ .kv_start = 0, .capacity_rows = 256, .bias_start = 0, .bias_rows = 256, .span_start = 0, .span_count = 2 }};
    const view = View{ .version = abi_version, .epoch = 1, .logical_tokens = 1024, .capacity_rows = 256, .bias_rows = 256, .kv_heads = 1, .head_dim = 256, .spans = &spans, .heads = &heads };
    try std.testing.expectError(error.InvalidContextView, view.validate());
}
