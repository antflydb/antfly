// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Shared callback contracts for manual and Io-derived execution.

const derived_types = @import("derived_types.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");

pub const RuntimeError = error{AsyncWorkerFailed};

pub const ApplyFn = *const fn (ctx: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef) anyerror!bool;
pub const PersistFn = *const fn (ctx: *anyopaque, index_name: []const u8, sequence: u64, force: bool) anyerror!bool;
pub const TruncateFn = *const fn (ctx: *anyopaque, sequence: u64) anyerror!void;
pub const BeginCatchUpFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) anyerror!void;
pub const FinishCatchUpFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, success: bool) anyerror!void;
pub const CanAdvanceToTargetFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, from_sequence: u64, target_sequence: u64) anyerror!bool;
pub const AppliedSequenceAdvancedFn = *const fn (ctx: *anyopaque, index_name: []const u8, applied_sequence: u64) void;

const std = @import("std");
const types = @import("../types.zig");
const platform_clock = @import("antfly_platform").clock;
const platform_time = @import("antfly_platform").time;

/// Deadline and its clock travel together across executor implementations.
pub const VisibilityWait = struct {
    cancellation: types.CancellationToken = .none,
    deadline_ns: ?u64 = null,
    clock: ?platform_clock.Clock = null,

    pub fn check(self: @This()) !void {
        if (self.cancellation.isCancelled()) return error.EnrichmentWaitCanceled;
        if (self.deadline_ns) |deadline_ns| {
            const now_ns = if (self.clock) |clock|
                clock.nowRealtimeNs()
            else
                platform_time.monotonicNs();
            if (now_ns >= deadline_ns) return error.EnrichmentWaitTimeout;
        }
    }
};

test "derived visibility wait retains its deadline clock and cancellation" {
    var clock = platform_clock.ManualClock{};
    var cancelled = std.atomic.Value(bool).init(false);
    const wait = VisibilityWait{
        .clock = clock.clock(),
        .deadline_ns = 100,
        .cancellation = types.CancellationToken.fromAtomic(&cancelled),
    };
    try wait.check();
    clock.advanceNs(99);
    try wait.check();
    clock.advanceNs(1);
    try std.testing.expectError(error.EnrichmentWaitTimeout, wait.check());
    cancelled.store(true, .release);
    try std.testing.expectError(error.EnrichmentWaitCanceled, wait.check());
}
