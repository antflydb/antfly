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
const platform = @import("antfly_platform");
pub const Phase = enum { source_fetch, pdf_prepare, pdf_render, media_encode, reader_http, vision, prefill, decode, mixed_step, publication, prefetch_wait };
const bounds = [_]u64{ 1_000_000, 10_000_000, 100_000_000, 1_000_000_000, 10_000_000_000, 60_000_000_000 };
const Counter = platform.atomic.Value(u64);
pub const Histogram = struct {
    count: Counter = .init(0),
    total_ns: Counter = .init(0),
    buckets: [bounds.len]Counter = @splat(.init(0)),
    pub fn observe(self: *@This(), ns: u64) void {
        _ = self.total_ns.fetchAdd(ns, .monotonic);
        for (&self.buckets, bounds) |*bucket, bound| if (ns <= bound) {
            _ = bucket.fetchAdd(1, .monotonic);
        };
        _ = self.count.fetchAdd(1, .monotonic);
    }
};
var phases: [@typeInfo(Phase).@"enum".fields.len]Histogram = @splat(.{});
pub const Timer = struct {
    phase: Phase,
    started_ns: u64,
    pub fn finish(self: @This()) void {
        observe(self.phase, platform.time.monotonicNs() -| self.started_ns);
    }
};
pub fn start(phase: Phase) Timer {
    return .{ .phase = phase, .started_ns = platform.time.monotonicNs() };
}
pub fn observe(phase: Phase, ns: u64) void {
    phases[@backingInt(phase)].observe(ns);
}
pub fn writePrometheus(w: *std.Io.Writer) !void {
    try w.writeAll("# HELP antfly_document_pipeline_phase_duration_ns Wall duration of document pipeline phases, including failed attempts; phases can overlap\n# TYPE antfly_document_pipeline_phase_duration_ns histogram\n");
    inline for (@typeInfo(Phase).@"enum".fields, 0..) |field, i| {
        const h = &phases[i];
        for (bounds, 0..) |bound, b| try w.print("antfly_document_pipeline_phase_duration_ns_bucket{{phase=\"{s}\",le=\"{d}\"}} {d}\n", .{ field.name, bound, h.buckets[b].load(.monotonic) });
        try w.print("antfly_document_pipeline_phase_duration_ns_bucket{{phase=\"{s}\",le=\"+Inf\"}} {d}\nantfly_document_pipeline_phase_duration_ns_count{{phase=\"{s}\"}} {d}\nantfly_document_pipeline_phase_duration_ns_sum{{phase=\"{s}\"}} {d}\n", .{ field.name, h.count.load(.monotonic), field.name, h.count.load(.monotonic), field.name, h.total_ns.load(.monotonic) });
    }
}
test "pipeline histograms include boundary values and independent phases" {
    var h: Histogram = .{};
    h.observe(bounds[0]);
    h.observe(bounds[0] + 1);
    h.observe(bounds[bounds.len - 1] + 1);
    try std.testing.expectEqual(@as(u64, 3), h.count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), h.buckets[0].load(.monotonic));
    try std.testing.expectEqual(@as(u64, 2), h.buckets[bounds.len - 1].load(.monotonic));
    var out: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer out.deinit();
    try writePrometheus(&out.writer);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "phase=\"source_fetch\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "phase=\"decode\"") != null);
}
