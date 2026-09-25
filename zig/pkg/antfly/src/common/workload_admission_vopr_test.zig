// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const VoprIo = @import("vopr").vopr_io.VoprIo;
const admission = @import("workload_admission.zig");

test "workload admission VOPR deadline racing grant cannot start expired work" {
    var sim = try VoprIo.init(.{});
    defer sim.deinit();
    var owner = admission.Controller.init(1);
    try owner.configure(.{ .max_wait_ms = 100, .max_queued_requests = 1, .max_queued_bytes = 64, .max_retained_bytes = 128 });
    var blocker = try owner.acquire(.{ .io = sim.io(), .retained_bytes = 64 });
    defer blocker.release();
    const Race = struct {
        sim: *VoprIo,
        blocker: *admission.Controller.Lease,
        calls: usize = 0,
        fn advance(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == 2) {
                self.sim.monotonic_ns = 10 * std.time.ns_per_ms;
                self.blocker.release();
            }
            return false;
        }
    };
    var race: Race = .{ .sim = &sim, .blocker = &blocker };
    try std.testing.expectError(error.DeadlineExceeded, owner.acquire(.{
        .io = sim.io(),
        .retained_bytes = 64,
        .deadline_ns = 10 * std.time.ns_per_ms,
        .cancellation = .{ .ptr = &race, .is_cancelled_fn = Race.advance },
    }));
    const stats = owner.stats();
    try std.testing.expectEqual(@as(u64, 1), stats.expired_total);
    try std.testing.expectEqual(@as(u64, 10 * std.time.ns_per_ms), stats.wait_ns_total);
    try std.testing.expectEqual(@as(u64, 1), stats.wait_completed_total);
    try std.testing.expectEqual(@as(u64, 0), stats.wait_buckets[1]);
    try std.testing.expectEqual(@as(u64, 1), stats.wait_buckets[2]);
    try std.testing.expectEqual(@as(u64, 1), stats.wait_buckets[admission.wait_bucket_ms.len]);
    try std.testing.expectEqual(@as(usize, 1), stats.peak_in_flight);
    try std.testing.expectEqual(@as(usize, 0), stats.in_flight);
    try std.testing.expectEqual(@as(usize, 0), stats.retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), stats.queued);
}
