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

//! Shared callback contracts for manual and Io-derived execution.

const derived_types = @import("derived_types.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
pub const RuntimeError = error{AsyncWorkerFailed};

/// A batch carries the session opened by BeginCatchUpFn. Its token is explicit
/// even when callbacks resume on another worker or share one cooperative thread.
pub const ApplyFn = *const fn (ctx: *anyopaque, batch: derived_types.DerivedBatch, index_ref: index_manager_mod.ManagedIndexRef, token: CatchUpSessionToken) anyerror!bool;
pub const PersistFn = *const fn (ctx: *anyopaque, index_name: []const u8, sequence: u64, force: bool) anyerror!bool;
pub const TruncateFn = *const fn (ctx: *anyopaque, sequence: u64) anyerror!void;
pub const CatchUpSessionToken = struct {
    value: u64 = 0,

    pub fn isNone(self: @This()) bool {
        return self.value == 0;
    }
};
pub const CatchUpFinishResult = struct {
    applied_sequence_persisted: bool = false,
};
pub const BeginCatchUpFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef) anyerror!CatchUpSessionToken;
pub const FinishCatchUpFn = *const fn (ctx: *anyopaque, index_ref: index_manager_mod.ManagedIndexRef, token: CatchUpSessionToken, applied_sequence: u64, success: bool) anyerror!CatchUpFinishResult;
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
