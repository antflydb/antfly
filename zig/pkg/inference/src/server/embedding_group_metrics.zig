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

//! Grouped embedding outcomes become visible after the caller's cleanup defers.
const std = @import("std");
const prometheus = @import("prometheus");
pub const Metrics = struct {
    active: prometheus.Gauge(i64) = .init("antfly_inference_embedding_groups_active", .{ .help = "Grouped embedding requests whose resource cleanup has not completed" }, .{}),
    completed: prometheus.Counter(u64) = .init("antfly_inference_embedding_groups_completed_total", .{ .help = "Grouped embedding requests completed after resource cleanup" }, .{}),
    cancelled: prometheus.Counter(u64) = .init("antfly_inference_embedding_groups_cancelled_total", .{ .help = "Grouped embedding requests cancelled after resource cleanup" }, .{}),
    timed_out: prometheus.Counter(u64) = .init("antfly_inference_embedding_groups_timed_out_total", .{ .help = "Grouped embedding requests timed out after resource cleanup" }, .{}),
    failed: prometheus.Counter(u64) = .init("antfly_inference_embedding_groups_failed_total", .{ .help = "Grouped embedding requests failed after resource cleanup" }, .{}),
    pub fn begin(self: *Metrics) Trace {
        self.active.incr();
        return .{ .metrics = self };
    }
    pub fn render(self: *Metrics, writer: *std.Io.Writer) !void {
        try prometheus.write(self, writer);
    }
};
pub const Trace = struct {
    metrics: *Metrics,
    failure: ?anyerror = null,
    pub fn finish(self: *Trace) void {
        if (self.failure) |err| switch (err) {
            error.Cancelled, error.Canceled => self.metrics.cancelled.incr(),
            error.Timeout => self.metrics.timed_out.incr(),
            else => self.metrics.failed.incr(),
        } else self.metrics.completed.incr();
        self.metrics.active.incrBy(-1);
    }
};
test "grouped embedding metrics distinguish cleanup outcomes and active owners" {
    var m = Metrics{};
    var first = m.begin();
    var second = m.begin();
    first.failure = error.Cancelled;
    first.finish();
    var writer = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer writer.deinit();
    try m.render(&writer.writer);
    try std.testing.expect(std.mem.indexOf(u8, writer.writer.buffered(), "antfly_inference_embedding_groups_active 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, writer.writer.buffered(), "antfly_inference_embedding_groups_cancelled_total 1\n") != null);
    second.finish();
    var timeout = m.begin();
    timeout.failure = error.Timeout;
    timeout.finish();
    var failed = m.begin();
    failed.failure = error.UnsupportedVideoColor;
    failed.finish();
    var canceled = m.begin();
    canceled.failure = error.Canceled;
    canceled.finish();
    writer.clearRetainingCapacity();
    try m.render(&writer.writer);
    inline for (.{ "active 0", "cancelled_total 2", "completed_total 1", "timed_out_total 1", "failed_total 1" }) |suffix|
        try std.testing.expect(std.mem.indexOf(u8, writer.writer.buffered(), "antfly_inference_embedding_groups_" ++ suffix ++ "\n") != null);
}
