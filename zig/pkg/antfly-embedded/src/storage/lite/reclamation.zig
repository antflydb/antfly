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

//! Owner shrinking policy and storage admission. Revision 4 retires/reuses pages
//! independently of optional generation shrinking. No integrity walk belongs
//! on the routine commit path.
const std = @import("std");

pub const Options = struct {
    /// Optional physical shrinking. Logical reuse remains independently enabled.
    enabled: bool = true,
    page_reuse: bool = true,
    retirement_work_pages: usize = 128,
    /// Minimum growth interval; exact scans also require proportional change
    /// as the store grows. Retirement uses the smaller of this minimum and
    /// minimum_reclaim_bytes, subject to the same proportional floor.
    assessment_bytes: u64 = 64 * 1024 * 1024,
    minimum_reclaim_bytes: u64 = 256 * 1024 * 1024,
    amplification: u32 = 2,
    retry_ms: u32 = 1000,
    /// Limits combined current, retired and rewrite workspace bytes. Zero is
    /// unlimited. This is an admission limit, not a filesystem free-space probe.
    max_storage_bytes: u64 = 0,
    disk_headroom_bytes: u64 = 64 * 1024 * 1024,
    /// Virtual/borrowed runtimes can supply capacity without escaping their
    /// filesystem. Native owners use the platform probe when supported.
    capacity_probe: ?CapacityProbe = null,

    pub fn validate(self: Options) !void {
        if (self.assessment_bytes == 0 or self.minimum_reclaim_bytes == 0 or
            self.amplification < 2 or self.retry_ms == 0 or self.retirement_work_pages == 0 or
            (self.max_storage_bytes != 0 and self.max_storage_bytes < 4096)) return error.InvalidLiteMaintenanceOptions;
    }
};

pub const CapacityProbe = struct {
    context: *anyopaque,
    available: *const fn (*anyopaque, []const u8) anyerror!u64,
};

pub const State = enum { disabled, idle, assessing, rewriting, deferred, failed };
pub const Reason = enum { none, disabled, concurrency_unavailable, writer_busy, retained_readers, storage_budget, low_disk, canceled, maintenance_error };

pub const Status = struct {
    state: State = .idle,
    reason: Reason = .none,
    current_file_bytes: u64 = 0,
    allocator_enabled: bool = false,
    shrinking_enabled: bool = false,
    reusable_pages: u64 = 0,
    pending_retirement_objects: u64 = 0,
    pending_data_retirement_objects: u64 = 0,
    reused_pages: u64 = 0,
    retired_objects_serviced: u64 = 0,
    retirement_activity_bytes: u64 = 0,
    retired_file_bytes: u64 = 0,
    retired_generations: u64 = 0,
    retained_readers: u64 = 0,
    oldest_reader_age_ms: u64 = 0,
    temporary_bytes: u64 = 0,
    available_disk_bytes: ?u64 = null,
    compact_size_estimate: ?u64 = null,
    live_bytes_estimate: ?u64 = null,
    estimated_at_sequence: ?u64 = null,
    assessment_count: u64 = 0,
    rewrite_count: u64 = 0,
    last_reclaimed_bytes: u64 = 0,
    last_error: ?[]const u8 = null,

    pub fn totalBytes(self: Status) u64 {
        return self.current_file_bytes +| self.retired_file_bytes +| self.temporary_bytes;
    }
};

pub const Policy = struct {
    options: Options = .{},
    status: Status = .{},
    next_assessment_size: u64 = 0,
    assessment_activity_bytes: u64 = 0,
    retirement_bytes: u64 = 0,
    assessed_retirement_bytes: u64 = 0,

    pub fn init(options: Options) !Policy {
        try options.validate();
        return .{ .options = options, .status = .{
            .state = if (options.enabled) .idle else .disabled,
            .reason = if (options.enabled) .none else .disabled,
        } };
    }

    pub fn due(self: Policy, size: u64) bool {
        const activity_due = self.retirement_bytes -| self.assessed_retirement_bytes >=
            self.assessment_activity_bytes;
        return self.options.enabled and size >= self.options.minimum_reclaim_bytes and
            (size >= self.next_assessment_size or activity_due);
    }

    // An exact assessment traverses all live records. Charge at least a
    // quarter of the assessed physical size in growth or committed retirement
    // before another scan, so routine scan work is amortized to changed data.
    // Retirement includes inline deletions, which need not release whole pages.
    fn resetAssessmentBudget(self: *Policy, physical: u64) void {
        const proportional = physical / 4;
        self.next_assessment_size = physical +| @max(self.options.assessment_bytes, proportional);
        self.assessment_activity_bytes = @max(@min(self.options.assessment_bytes, self.options.minimum_reclaim_bytes), proportional);
        self.assessed_retirement_bytes = self.retirement_bytes;
    }

    pub fn assessed(self: *Policy, physical: u64, compact: u64, live: u64, sequence: u64) bool {
        self.resetAssessmentBudget(physical);
        self.status.assessment_count +|= 1;
        self.status.compact_size_estimate = compact;
        self.status.live_bytes_estimate = live;
        self.status.estimated_at_sequence = sequence;
        return self.worthwhile(physical, compact);
    }

    pub fn worthwhile(self: Policy, physical: u64, compact: u64) bool {
        return physical -| compact >= self.options.minimum_reclaim_bytes and
            physical > compact *| self.options.amplification;
    }

    pub fn completed(self: *Policy, physical: u64, reclaimed: u64) void {
        self.resetAssessmentBudget(physical);
        self.status.rewrite_count +|= 1;
        self.status.last_reclaimed_bytes = reclaimed;
        self.status.state = if (self.options.enabled) .idle else .disabled;
        self.status.reason = if (self.options.enabled) .none else .disabled;
        self.status.last_error = null;
        // Catch-up can change the live roots. The pre-copy estimate is stale.
        self.status.estimated_at_sequence = null;
    }
};

test "lite reclamation policy uses hysteresis and saturating size arithmetic" {
    var policy = try Policy.init(.{ .assessment_bytes = 64, .minimum_reclaim_bytes = 256 });
    try std.testing.expect(!policy.due(255));
    try std.testing.expect(policy.due(1024));
    try std.testing.expect(policy.assessed(1024, 300, 200, 7));
    try std.testing.expect(!policy.due(1025));
    policy.completed(300, 724);
    try std.testing.expect(!policy.due(374));
    try std.testing.expect(policy.due(375));
    try std.testing.expect(!policy.assessed(600, 300, 200, 8));
    try std.testing.expect(!policy.assessed(std.math.maxInt(u64), std.math.maxInt(u64) / 2 + 1, 0, 9));
    try std.testing.expectEqual(std.math.maxInt(u64), policy.next_assessment_size);
    try std.testing.expectError(error.InvalidLiteMaintenanceOptions, Policy.init(.{ .assessment_bytes = 0 }));
    try std.testing.expectError(error.InvalidLiteMaintenanceOptions, Policy.init(.{ .max_storage_bytes = 4095 }));
}

test "lite reclamation assessments amortize steady reuse and growth to store size" {
    const mib = 1024 * 1024;
    for ([_]u64{ 1024 * mib, 16 * 1024 * mib, 64 * 1024 * mib }) |physical| {
        var policy = try Policy.init(.{});
        try std.testing.expect(!policy.assessed(physical, physical - 4096, physical - 8192, 1));
        // Identical 2 GiB of updates causes fewer scans as the store grows.
        // Each scan requires proportional retirement, even with no file growth.
        var scanned_bytes: u64 = 0;
        for (0..32) |i| {
            policy.retirement_bytes += 64 * mib;
            if (policy.due(physical)) {
                scanned_bytes += physical;
                try std.testing.expect(!policy.assessed(physical, physical - 4096, physical - 8192, i + 2));
            }
        }
        try std.testing.expect(scanned_bytes <= policy.retirement_bytes * 4);
        const activity = policy.retirement_bytes;
        // File growth obeys the same proportional budget independently.
        try std.testing.expect(!policy.due(policy.next_assessment_size - 1));
        try std.testing.expect(policy.due(policy.next_assessment_size));
        try std.testing.expectEqual(activity, policy.retirement_bytes);
    }
}

test "lite reclamation proportional assessments still observe deletion without growth" {
    const physical = 16 * 1024 * 1024 * 1024;
    var policy = try Policy.init(.{});
    try std.testing.expect(!policy.assessed(physical, physical, physical, 1));
    var compact: u64 = physical;
    // Includes inline retirement: a shrinking trigger must not require free
    // whole pages or increasing file size. At most a quarter-file of activity
    // separates the exact assessments as amplification crosses the threshold.
    for (0..3) |i| {
        policy.retirement_bytes += physical / 4;
        compact -= physical / 4;
        try std.testing.expect(policy.due(physical));
        try std.testing.expectEqual(i == 2, policy.assessed(physical, compact, compact, i + 2));
    }
}
