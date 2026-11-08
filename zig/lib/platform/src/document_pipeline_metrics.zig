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

//! Process-local, bounded-cardinality timings. Durations are wall time, not
//! GPU event time; overlapping phases must not be added as exclusive CPU time.
const std = @import("std");
const atomic = @import("atomic.zig");
const time = @import("time.zig");
pub const Phase = enum { source_fetch, pdf_prepare, pdf_render, media_encode, reader_http, vision_prepare, vision, prefill, decode, mixed_step, publication, prefetch_wait };
const bounds = [_]u64{ 1_000_000, 10_000_000, 100_000_000, 1_000_000_000, 10_000_000_000, 60_000_000_000 };
const Counter = atomic.Value(u64);
pub const Histogram = struct {
    // One short lock per phase makes count, sum and cumulative buckets one
    // observation. Independent atomics can export bucket > count during a scrape.
    mutex: std.atomic.Mutex = .unlocked,
    count: u64 = 0,
    total_ns: u64 = 0,
    buckets: [bounds.len]u64 = @splat(0),
    const View = struct { count: u64, total_ns: u64, buckets: [bounds.len]u64 };
    fn lock(self: *@This()) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn observe(self: *@This(), ns: u64) void {
        self.lock();
        defer self.mutex.unlock();
        self.total_ns +|= ns;
        for (&self.buckets, bounds) |*bucket, bound| if (ns <= bound) {
            bucket.* +|= 1;
        };
        self.count +|= 1;
    }
    pub fn capture(self: *@This()) View {
        self.lock();
        defer self.mutex.unlock();
        return .{ .count = self.count, .total_ns = self.total_ns, .buckets = self.buckets };
    }
};
pub const PrefetchEvent = enum { started, ready, failed, consumed, reclaimed };
var prefetch_events: [@typeInfo(PrefetchEvent).@"enum".field_names.len]Counter = @splat(.init(0));
pub fn prefetch(event: PrefetchEvent) void {
    _ = prefetch_events[@backingInt(event)].fetchAdd(1, .monotonic);
}
var phases: [@typeInfo(Phase).@"enum".field_names.len]Histogram = @splat(.{});
pub const Timer = struct {
    phase: Phase,
    started_ns: u64,
    pub fn finish(self: @This()) void {
        observe(self.phase, time.monotonicNs() -| self.started_ns);
    }
};
pub fn start(phase: Phase) Timer {
    return .{ .phase = phase, .started_ns = time.monotonicNs() };
}
pub fn observe(phase: Phase, ns: u64) void {
    phases[@backingInt(phase)].observe(ns);
}
pub const Snapshot = struct {
    version: u32 = 1,
    counts: [@typeInfo(Phase).@"enum".field_names.len]u64 = @splat(0),
    totals_ns: [@typeInfo(Phase).@"enum".field_names.len]u64 = @splat(0),
    buckets: [@typeInfo(Phase).@"enum".field_names.len][bounds.len]u64 = @splat(@splat(0)),
    events: [@typeInfo(PrefetchEvent).@"enum".field_names.len]u64 = @splat(0),
    pub fn merge(self: *@This(), other: @This()) !void {
        if (self.version != 1 or other.version != 1) return error.InvalidPipelineMetricsVersion;
        for (&self.counts, other.counts) |*a, b| a.* +|= b;
        for (&self.totals_ns, other.totals_ns) |*a, b| a.* +|= b;
        for (&self.buckets, other.buckets) |*a, b| for (a, b) |*x, y| {
            x.* +|= y;
        };
        for (&self.events, other.events) |*a, b| a.* +|= b;
    }
    pub fn writePrometheus(self: @This(), w: *std.Io.Writer) !void {
        if (self.version != 1) return error.InvalidPipelineMetricsVersion;
        try w.writeAll("# HELP antfly_document_pipeline_phase_duration_ns Wall duration of document pipeline phases, including failed attempts; phases can overlap\n# TYPE antfly_document_pipeline_phase_duration_ns histogram\n");
        try w.writeAll("# HELP antfly_document_prefetch_events_total Bounded cross-document preparation outcomes\n# TYPE antfly_document_prefetch_events_total counter\n");
        inline for (@typeInfo(PrefetchEvent).@"enum".field_names, 0..) |name, i| try w.print("antfly_document_prefetch_events_total{{event=\"{s}\"}} {d}\n", .{ name, self.events[i] });
        inline for (@typeInfo(Phase).@"enum".field_names, 0..) |field, i| {
            for (bounds, 0..) |bound, b| try w.print("antfly_document_pipeline_phase_duration_ns_bucket{{phase=\"{s}\",le=\"{d}\"}} {d}\n", .{ field, bound, self.buckets[i][b] });
            try w.print("antfly_document_pipeline_phase_duration_ns_bucket{{phase=\"{s}\",le=\"+Inf\"}} {d}\nantfly_document_pipeline_phase_duration_ns_count{{phase=\"{s}\"}} {d}\nantfly_document_pipeline_phase_duration_ns_sum{{phase=\"{s}\"}} {d}\n", .{ field, self.counts[i], field, self.counts[i], field, self.totals_ns[i] });
        }
    }
};
pub fn snapshot() Snapshot {
    var result: Snapshot = .{};
    for (&result.counts, &result.totals_ns, &result.buckets, &phases) |*count, *total, *buckets, *h| {
        const view = h.capture();
        count.* = view.count;
        total.* = view.total_ns;
        buckets.* = view.buckets;
    }
    for (&result.events, &prefetch_events) |*out, *in| out.* = in.load(.monotonic);
    return result;
}
pub fn writePrometheus(w: *std.Io.Writer) !void {
    try snapshot().writePrometheus(w);
}

test "pipeline histograms include boundary values and independent phases" {
    var h: Histogram = .{};
    h.observe(bounds[0]);
    h.observe(bounds[0] + 1);
    h.observe(bounds[bounds.len - 1] + 1);
    try std.testing.expectEqual(@as(u64, 3), h.capture().count);
    try std.testing.expectEqual(@as(u64, 1), h.capture().buckets[0]);
    try std.testing.expectEqual(@as(u64, 2), h.capture().buckets[bounds.len - 1]);
    var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer out.deinit();
    try writePrometheus(&out.writer);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "phase=\"source_fetch\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "phase=\"decode\"") != null);
}

test "pipeline metric owner snapshots merge and reject unknown versions" {
    var first: Snapshot = .{};
    first.counts[0] = 1;
    first.buckets[0][0] = 1;
    var second: Snapshot = .{};
    second.counts[0] = 2;
    second.buckets[0][0] = 2;
    second.events[0] = 3;
    try first.merge(second);
    try std.testing.expectEqual(@as(u64, 3), first.counts[0]);
    try std.testing.expectEqual(@as(u64, 3), first.buckets[0][0]);
    try std.testing.expectEqual(@as(u64, 3), first.events[0]);
    second.version = 2;
    try std.testing.expectError(error.InvalidPipelineMetricsVersion, first.merge(second));
}

test "pipeline histogram snapshots stay coherent with concurrent writers" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .async_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    const Worker = struct {
        histogram: *Histogram,
        done: atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            for (0..20000) |_| self.histogram.observe(1);
            self.done.store(true, .release);
        }
    };
    var histogram: Histogram = .{};
    var first: Worker = .{ .histogram = &histogram };
    var second: Worker = .{ .histogram = &histogram };
    var f = try io.concurrent(Worker.run, .{&first});
    defer f.await(io);
    var g = try io.concurrent(Worker.run, .{&second});
    defer g.await(io);
    while (!first.done.load(.acquire) or !second.done.load(.acquire)) {
        const view = histogram.capture();
        try std.testing.expectEqual(view.count, view.total_ns);
        for (view.buckets) |bucket| try std.testing.expectEqual(view.count, bucket);
    }
    const final = histogram.capture();
    try std.testing.expectEqual(@as(u64, 40000), final.count);
    try std.testing.expectEqual(final.count, final.total_ns);
    for (final.buckets) |bucket| try std.testing.expectEqual(final.count, bucket);
}
