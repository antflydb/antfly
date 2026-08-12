// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");

/// Process-local, fail-fast admission for one foreground request class.
/// A zero capacity disables the limit while retaining accounting.
pub const RequestAdmission = struct {
    capacity: usize,
    in_flight: std.atomic.Value(usize) = .init(0),
    rejected_total: std.atomic.Value(u64) = .init(0),
    peak_in_flight: std.atomic.Value(usize) = .init(0),

    pub fn init(capacity: usize) RequestAdmission {
        return .{ .capacity = capacity };
    }

    pub fn tryAcquire(self: *RequestAdmission) bool {
        var observed = self.in_flight.load(.acquire);
        while (self.capacity == 0 or observed < self.capacity) {
            if (self.in_flight.cmpxchgWeak(observed, observed + 1, .acq_rel, .acquire) == null) {
                const admitted = observed + 1;
                var peak = self.peak_in_flight.load(.acquire);
                while (peak < admitted) {
                    if (self.peak_in_flight.cmpxchgWeak(peak, admitted, .acq_rel, .acquire) == null) break;
                    peak = self.peak_in_flight.load(.acquire);
                }
                return true;
            }
            observed = self.in_flight.load(.acquire);
        }
        _ = self.rejected_total.fetchAdd(1, .monotonic);
        return false;
    }

    pub fn release(self: *RequestAdmission) void {
        _ = self.in_flight.fetchSub(1, .acq_rel);
    }

    pub const Stats = struct {
        capacity: usize,
        in_flight: usize,
        peak_in_flight: usize,
        rejected_total: u64,
    };

    pub fn stats(self: *const RequestAdmission) Stats {
        return .{
            .capacity = self.capacity,
            .in_flight = self.in_flight.load(.acquire),
            .peak_in_flight = self.peak_in_flight.load(.acquire),
            .rejected_total = self.rejected_total.load(.acquire),
        };
    }
};

test "request admission bounds positive capacity and preserves unlimited mode" {
    var bounded = RequestAdmission.init(1);
    try std.testing.expect(bounded.tryAcquire());
    try std.testing.expect(!bounded.tryAcquire());
    bounded.release();
    try std.testing.expectEqual(@as(u64, 1), bounded.stats().rejected_total);

    var unlimited = RequestAdmission.init(0);
    try std.testing.expect(unlimited.tryAcquire());
    try std.testing.expect(unlimited.tryAcquire());
    unlimited.release();
    unlimited.release();
    try std.testing.expectEqual(@as(usize, 2), unlimited.stats().peak_in_flight);
}
