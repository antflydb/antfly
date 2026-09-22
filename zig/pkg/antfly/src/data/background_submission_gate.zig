// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");

/// Closes scheduling before shutdown joins individual worker owners. A submitter
/// that wins admission must publish its thread/future before close returns.
/// Worker execution is joined separately, without holding this admission.
pub const BackgroundSubmissionGate = struct {
    const closed_bit: usize = @as(usize, 1) << (@bitSizeOf(usize) - 1);
    const count_mask: usize = closed_bit - 1;

    state: std.atomic.Value(usize) = .init(0),

    pub fn begin(self: *@This()) bool {
        var observed = self.state.load(.acquire);
        while (true) {
            if (observed & closed_bit != 0) return false;
            std.debug.assert(observed & count_mask < count_mask);
            if (self.state.cmpxchgWeak(observed, observed + 1, .acq_rel, .acquire)) |actual| {
                observed = actual;
                continue;
            }
            return true;
        }
    }

    pub fn end(self: *@This()) void {
        const previous = self.state.fetchSub(1, .acq_rel);
        std.debug.assert(previous & count_mask > 0);
    }

    pub fn close(self: *@This()) void {
        _ = self.state.fetchOr(closed_bit, .acq_rel);
        while (self.state.load(.acquire) & count_mask != 0) {
            std.Thread.yield() catch {};
        }
    }
};

test "background shutdown waits for admitted submission and rejects worker rescheduling" {
    var gate: BackgroundSubmissionGate = .{};
    try std.testing.expect(gate.begin());
    const Shutdown = struct {
        gate: *BackgroundSubmissionGate,
        returned: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.gate.close();
            self.returned.store(true, .release);
        }
    };
    var shutdown = Shutdown{ .gate = &gate };
    const thread = try std.Thread.spawn(.{}, Shutdown.run, .{&shutdown});
    var submission_active = true;
    defer {
        if (submission_active) gate.end();
        thread.join();
    }
    while (gate.state.load(.acquire) & BackgroundSubmissionGate.closed_bit == 0) {
        std.Thread.yield() catch {};
    }
    try std.testing.expect(!shutdown.returned.load(.acquire));
    // A finishing warmup/bulk worker cannot schedule status refresh after
    // shutdown has begun, even while a previously admitted spawn is finishing.
    try std.testing.expect(!gate.begin());
    gate.end();
    submission_active = false;
    while (!shutdown.returned.load(.acquire)) std.Thread.yield() catch {};
    try std.testing.expect(!gate.begin());
    gate.close();
}
