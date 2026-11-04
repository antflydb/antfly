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

//! Linked-runtime logical client calls, including filesystem clients. No keys,
//! credentials or bucket labels. Nested fallback calls overlap; these are not
//! provider HTTP request counts or billing estimates. Separately compiled runtime
//! owners have independent counters; their I/O is not silently attributed here.
const std = @import("std");
pub const Operation = enum { bucket_probe, bucket_create, get, get_batch, put, put_batch, stat, attributes, list, list_versions, delete, conditional_put, conditional_delete, provider_time, retention, version_support };
const count = @typeInfo(Operation).@"enum".field_names.len;
pub const Snapshot = struct { calls: [count]u64, errors: [count]u64, nanoseconds: [count]u64, max_nanoseconds: [count]u64, input_bytes: [count]u64 = @splat(0), output_bytes: [count]u64 = @splat(0), retries: u64 };
pub const Metrics = struct {
    calls: [count]std.atomic.Value(u64) = @splat(.init(0)),
    errors: [count]std.atomic.Value(u64) = @splat(.init(0)),
    nanoseconds: [count]std.atomic.Value(u64) = @splat(.init(0)),
    max_nanoseconds: [count]std.atomic.Value(u64) = @splat(.init(0)),
    input_bytes: [count]std.atomic.Value(u64) = @splat(.init(0)),
    output_bytes: [count]std.atomic.Value(u64) = @splat(.init(0)),
    retries: std.atomic.Value(u64) = .init(0),
    pub const Timer = struct {
        metrics: *Metrics,
        op: Operation,
        started: u64,
        pub fn input(self: Timer, bytes: usize) void {
            _ = self.metrics.input_bytes[@backingInt(self.op)].fetchAdd(bytes, .monotonic);
        }
        pub fn output(self: Timer, bytes: usize) void {
            _ = self.metrics.output_bytes[@backingInt(self.op)].fetchAdd(bytes, .monotonic);
        }
        pub fn failed(self: Timer) void {
            _ = self.metrics.errors[@backingInt(self.op)].fetchAdd(1, .monotonic);
        }
        pub fn finish(self: Timer) void {
            const elapsed = @import("antfly_platform").time.monotonicNs() -| self.started;
            const i = @backingInt(self.op);
            _ = self.metrics.calls[i].fetchAdd(1, .monotonic);
            _ = self.metrics.nanoseconds[i].fetchAdd(elapsed, .monotonic);
            _ = self.metrics.max_nanoseconds[i].fetchMax(elapsed, .monotonic);
        }
    };
    pub fn start(self: *Metrics, op: Operation) Timer {
        return .{ .metrics = self, .op = op, .started = @import("antfly_platform").time.monotonicNs() };
    }
    pub fn snapshot(self: *Metrics) Snapshot {
        var result: Snapshot = .{ .calls = undefined, .errors = undefined, .nanoseconds = undefined, .max_nanoseconds = undefined, .retries = self.retries.load(.monotonic) };
        for (0..count) |i| {
            result.input_bytes[i] = self.input_bytes[i].load(.monotonic);
            result.output_bytes[i] = self.output_bytes[i].load(.monotonic);
            result.calls[i] = self.calls[i].load(.monotonic);
            result.errors[i] = self.errors[i].load(.monotonic);
            result.nanoseconds[i] = self.nanoseconds[i].load(.monotonic);
            result.max_nanoseconds[i] = self.max_nanoseconds[i].load(.monotonic);
        }
        return result;
    }
};
pub var runtime: Metrics = .{};
test "objectstore operation diagnostics retain errors and maximum without identifiers" {
    var metrics: Metrics = .{};
    const timer = metrics.start(.get);
    timer.input(17);
    timer.failed();
    timer.finish();
    const next = metrics.start(.get);
    next.output(23);
    next.finish();
    const snapshot = metrics.snapshot();
    try std.testing.expectEqual(@as(u64, 17), snapshot.input_bytes[@backingInt(Operation.get)]);
    try std.testing.expectEqual(@as(u64, 23), snapshot.output_bytes[@backingInt(Operation.get)]);
    try std.testing.expectEqual(@as(u64, 2), snapshot.calls[@backingInt(Operation.get)]);
    try std.testing.expectEqual(@as(u64, 1), snapshot.errors[@backingInt(Operation.get)]);
    try std.testing.expect(snapshot.max_nanoseconds[@backingInt(Operation.get)] <= snapshot.nanoseconds[@backingInt(Operation.get)]);
}
