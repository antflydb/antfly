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

//! Cheap synchronized rejection accounting with rate-limited OS sampling.
const std = @import("std");

pub const Diagnostic = struct {
    pressure: []const u8 = "none",
    error_code: []const u8 = "",
    retryable: bool = false,
    denied_count: u64 = 0,
    last_denied_ns: u64 = 0,
    request_count: u64 = 0,
    requested_host_bytes: u64 = 0,
    requested_backend_bytes: u64 = 0,
    requested_kv_bytes: u64 = 0,
    requested_scratch_bytes: u64 = 0,
    admitted_host_bytes: u64 = 0,
    admitted_backend_bytes: u64 = 0,
    host_limit_bytes: u64 = 0,
    backend_limit_bytes: u64 = 0,
    combined_limit_bytes: u64 = 0,
    kv_limit_bytes: u64 = 0,
    scratch_limit_bytes: u64 = 0,
    available_memory_bytes: u64 = 0,
    available_memory_sampled_ns: u64 = 0,
    shared_host_limit_bytes: u64 = 0,
    shared_unified_limit_bytes: u64 = 0,
};

pub const Recorder = struct {
    mutex: std.atomic.Mutex = .unlocked,
    diagnostic: Diagnostic = .{},
    last_sample_started_ns: ?u64 = null,

    fn lock(self: *Recorder) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// The sampler runs outside the accounting lock and at most once per
    /// second. Every refusal still refreshes its identity and estimates.
    pub fn record(self: *Recorder, supplied: Diagnostic, context: anytype, comptime sample: anytype) ?Diagnostic {
        var current = supplied;
        self.lock();
        // Callers sample their timestamp before competing for this lock.
        // A delayed older caller still counts, but cannot regress identity.
        if (supplied.last_denied_ns < self.diagnostic.last_denied_ns) current = self.diagnostic;
        current.denied_count = self.diagnostic.denied_count +| 1;
        current.available_memory_bytes = self.diagnostic.available_memory_bytes;
        current.available_memory_sampled_ns = self.diagnostic.available_memory_sampled_ns;
        const allowed = if (self.last_sample_started_ns) |last|
            supplied.last_denied_ns -| last >= std.time.ns_per_s
        else
            true;
        if (allowed) self.last_sample_started_ns = supplied.last_denied_ns;
        self.diagnostic = current;
        self.mutex.unlock();
        if (!allowed) return null;
        if (sample(context)) |bytes| {
            self.publishSample(supplied.last_denied_ns, bytes);
            current.available_memory_bytes = bytes;
            current.available_memory_sampled_ns = supplied.last_denied_ns;
        }
        return current;
    }

    fn publishSample(self: *Recorder, started_ns: u64, bytes: u64) void {
        self.lock();
        defer self.mutex.unlock();
        if (started_ns < self.diagnostic.available_memory_sampled_ns) return;
        self.diagnostic.available_memory_bytes = bytes;
        self.diagnostic.available_memory_sampled_ns = started_ns;
    }

    pub fn snapshot(self: *Recorder) Diagnostic {
        self.lock();
        defer self.mutex.unlock();
        return self.diagnostic;
    }
};

test "admission diagnostic flood bounds sampling and retains every rejection" {
    var recorder: Recorder = .{};
    const Sampler = struct {
        recorder: *Recorder,
        calls: usize = 0,
        fn sample(self: *@This()) ?u64 {
            // Reentrant inspection proves sampling owns no accounting lock.
            _ = self.recorder.snapshot();
            self.calls += 1;
            return 123;
        }
    };
    var sampler = Sampler{ .recorder = &recorder };
    for (0..271_168) |attempt| {
        _ = recorder.record(.{ .last_denied_ns = @intCast(attempt), .requested_host_bytes = @intCast(attempt), .error_code = "ResourceLimitExceeded" }, &sampler, Sampler.sample);
    }
    try std.testing.expectEqual(@as(usize, 1), sampler.calls);
    try std.testing.expectEqual(@as(u64, 271_168), recorder.snapshot().denied_count);
    try std.testing.expectEqual(@as(u64, 271_167), recorder.snapshot().requested_host_bytes);
    try std.testing.expectEqual(@as(u64, 123), recorder.snapshot().available_memory_bytes);
    _ = recorder.record(.{ .last_denied_ns = std.time.ns_per_s }, &sampler, Sampler.sample);
    try std.testing.expectEqual(@as(usize, 2), sampler.calls);
}

test "admission diagnostic late sampling cannot replace a newer denial or sample" {
    var recorder: Recorder = .{};
    recorder.diagnostic = .{ .last_denied_ns = 99, .denied_count = 4, .error_code = "ResourceTemporarilyUnavailable" };
    recorder.publishSample(20, 200);
    recorder.publishSample(10, 100);
    const current = recorder.snapshot();
    try std.testing.expectEqual(@as(u64, 200), current.available_memory_bytes);
    try std.testing.expectEqual(@as(u64, 20), current.available_memory_sampled_ns);
    try std.testing.expectEqual(@as(u64, 99), current.last_denied_ns);
    try std.testing.expectEqual(@as(u64, 4), current.denied_count);
    try std.testing.expectEqualStrings("ResourceTemporarilyUnavailable", current.error_code);
}

test "admission diagnostic delayed refusal preserves latest identity and counts both" {
    var recorder: Recorder = .{};
    const Sampler = struct {
        fn sample(_: void) ?u64 {
            return null;
        }
    };
    _ = recorder.record(.{ .last_denied_ns = 20, .error_code = "ResourceLimitExceeded" }, {}, Sampler.sample);
    _ = recorder.record(.{ .last_denied_ns = 10, .error_code = "ResourceTemporarilyUnavailable" }, {}, Sampler.sample);
    try std.testing.expectEqual(@as(u64, 2), recorder.snapshot().denied_count);
    try std.testing.expectEqual(@as(u64, 20), recorder.snapshot().last_denied_ns);
    try std.testing.expectEqualStrings("ResourceLimitExceeded", recorder.snapshot().error_code);
}
