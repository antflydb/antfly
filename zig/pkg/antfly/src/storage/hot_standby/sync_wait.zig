// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Bounded HA acknowledgement waits. The replication runtime owns polling
//! and session progress; the database only invokes the configured wait hook.
const std = @import("std");
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");
const hot_standby_primary_mod = @import("primary.zig");
const hot_standby_standby_mod = @import("standby.zig");
const hot_standby_session_mod = @import("session.zig");
const hot_standby_commit_gate_mod = @import("commit_gate.zig");

pub const HotStandbyProgressPollFn = *const fn (
    ctx: *anyopaque,
    primary: *hot_standby_primary_mod.Primary,
    target_lsn: u64,
    policy: hot_standby_primary_mod.SyncPolicy,
    round: usize,
) anyerror!void;

pub const HotStandbyPrimaryProgressSyncWait = struct {
    max_rounds: usize = 64,
    sleep_ns: u64 = 0,
    poll_ctx: ?*anyopaque = null,
    poll_fn: ?HotStandbyProgressPollFn = null,

    pub fn wait(ctx: *anyopaque, primary_ctx: *anyopaque, target_lsn: u64, policy: hot_standby_primary_mod.SyncPolicy) !void {
        const primary: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(primary_ctx));
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (policy.mode == .async) return;
        if (self.max_rounds == 0) return error.HASyncCommitWaitLimitExceeded;

        var round: usize = 0;
        while (round < self.max_rounds) : (round += 1) {
            if (self.poll_fn) |poll| {
                const poll_ctx = self.poll_ctx orelse return error.HASyncCommitWaitMissingContext;
                try poll(poll_ctx, primary, target_lsn, policy, round);
            }

            const gate = try hot_standby_commit_gate_mod.evaluate(primary, target_lsn, policy);
            if (gate.shouldAcknowledge()) return;
            if (gate.action == .reject) return error.SyncPolicyUnsatisfied;
            // A wait can only make progress when enough eligible slots exist.
            // After promotion the former primary is intentionally absent or
            // inactive until repair, so polling the full wait budget cannot
            // produce a synchronous acknowledgement. Return the explicit
            // post-commit pending outcome immediately instead of retaining the
            // DB apply lock behind two bounded mirror waits. A present but
            // lagging candidate still receives the ordinary bounded wait.
            if (gate.decision.candidate_count < gate.decision.required_count)
                return error.HASyncCommitWouldBlock;
            if (self.sleep_ns > 0) sleepNs(self.sleep_ns);
        }

        return error.HASyncCommitWouldBlock;
    }
};

pub const HotStandbySessionSyncWait = struct {
    alloc: Allocator,
    slot_name: []const u8,
    standby: *hot_standby_standby_mod.Standby,
    apply_ctx: *anyopaque,
    apply_fn: hot_standby_standby_mod.ApplyFn,
    max_rounds: usize = 8,

    pub fn wait(ctx: *anyopaque, primary_ctx: *anyopaque, target_lsn: u64, policy: hot_standby_primary_mod.SyncPolicy) !void {
        const primary: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(primary_ctx));
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (policy.mode == .async) return;
        if (!hotStandbySyncPolicyIncludesStandby(policy, self.slot_name)) return error.HASyncCommitWaitStandbyNotInPolicy;
        if (self.max_rounds == 0) return error.HASyncCommitWaitLimitExceeded;

        var progress = self.standby.currentProgress();
        var round: usize = 0;
        while (round < self.max_rounds) : (round += 1) {
            const result = hot_standby_session_mod.replicateAvailable(
                self.alloc,
                primary,
                self.slot_name,
                self.standby,
                self.apply_ctx,
                self.apply_fn,
            ) catch |err| {
                if (policy.mode == .remote_write) {
                    const gate = hot_standby_commit_gate_mod.evaluate(primary, target_lsn, policy) catch return err;
                    if (gate.shouldAcknowledge()) return;
                }
                return err;
            };

            const gate = try hot_standby_commit_gate_mod.evaluate(primary, target_lsn, policy);
            if (gate.shouldAcknowledge()) return;
            if (gate.action == .reject) return error.SyncPolicyUnsatisfied;
            if (result.received_count == 0 and result.applied_count == 0) break;

            const next_progress = self.standby.currentProgress();
            if (next_progress.received_lsn == progress.received_lsn and
                next_progress.applied_lsn == progress.applied_lsn and
                next_progress.safe_read_lsn == progress.safe_read_lsn)
            {
                break;
            }
            progress = next_progress;
        }

        return error.HASyncCommitWouldBlock;
    }
};

pub fn hotStandbySyncPolicyIncludesStandby(policy: hot_standby_primary_mod.SyncPolicy, slot_name: []const u8) bool {
    for (policy.standby_names) |name| {
        if (std.mem.eql(u8, name, slot_name)) return true;
    }
    return false;
}

fn sleepNs(duration_ns: u64) void {
    if (comptime builtin.os.tag == .freestanding) {
        return;
    }

    var req = std.posix.timespec{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}
