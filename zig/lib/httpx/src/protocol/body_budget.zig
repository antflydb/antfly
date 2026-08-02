//! Shared request-body memory admission.

const std = @import("std");

/// Process-wide byte budget shared by every inbound HTTP connection.
///
/// Fixed-length HTTP/1 requests reserve once before allocating their body
/// buffer. Chunked HTTP/1 and HTTP/2 DATA reserve incrementally. Reservations
/// remain owned by the parser/stream until request teardown so bytes copied
/// into application request storage cannot escape the process-wide bound.
pub const SharedBodyBudget = struct {
    capacity: usize,
    in_use: std.atomic.Value(usize) = .init(0),
    peak_in_use: std.atomic.Value(usize) = .init(0),
    rejected_total: std.atomic.Value(u64) = .init(0),

    pub fn init(capacity: usize) SharedBodyBudget {
        return .{ .capacity = capacity };
    }

    pub fn tryReserve(self: *@This(), amount: usize) bool {
        if (amount == 0) return true;
        var observed = self.in_use.load(.acquire);
        while (true) {
            const next = std.math.add(usize, observed, amount) catch {
                _ = self.rejected_total.fetchAdd(1, .monotonic);
                return false;
            };
            if (next > self.capacity) {
                _ = self.rejected_total.fetchAdd(1, .monotonic);
                return false;
            }
            if (self.in_use.cmpxchgWeak(observed, next, .acq_rel, .acquire) == null) {
                var peak = self.peak_in_use.load(.acquire);
                while (peak < next) {
                    if (self.peak_in_use.cmpxchgWeak(peak, next, .acq_rel, .acquire) == null) break;
                    peak = self.peak_in_use.load(.acquire);
                }
                return true;
            }
            observed = self.in_use.load(.acquire);
        }
    }

    pub fn release(self: *@This(), amount: usize) void {
        if (amount == 0) return;
        const previous = self.in_use.fetchSub(amount, .acq_rel);
        std.debug.assert(previous >= amount);
    }

    pub const Stats = struct {
        capacity: usize,
        in_use: usize,
        peak_in_use: usize,
        rejected_total: u64,
    };

    pub fn stats(self: *const @This()) Stats {
        return .{
            .capacity = self.capacity,
            .in_use = self.in_use.load(.acquire),
            .peak_in_use = self.peak_in_use.load(.acquire),
            .rejected_total = self.rejected_total.load(.acquire),
        };
    }
};

test "SharedBodyBudget bounds aggregate reservations and records pressure" {
    var budget = SharedBodyBudget.init(5);
    try std.testing.expect(budget.tryReserve(4));
    try std.testing.expect(!budget.tryReserve(2));
    try std.testing.expectEqual(@as(usize, 4), budget.stats().in_use);
    try std.testing.expectEqual(@as(usize, 4), budget.stats().peak_in_use);
    try std.testing.expectEqual(@as(u64, 1), budget.stats().rejected_total);
    budget.release(4);
    try std.testing.expectEqual(@as(usize, 0), budget.stats().in_use);
}
