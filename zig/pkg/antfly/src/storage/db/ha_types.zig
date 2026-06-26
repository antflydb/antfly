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

const std = @import("std");
const platform = @import("antfly_platform");

const docstore_mod = @import("../docstore.zig");
const ha_commit_gate_mod = @import("../ha/commit_gate.zig");
const ha_primary_mod = @import("../ha/primary.zig");
const ha_session_mod = @import("../ha/session.zig");
const ha_standby_mod = @import("../ha/standby.zig");
const ha_write_gate_mod = @import("../ha/write_gate.zig");
const internal_keys = @import("../internal_keys.zig");

const Allocator = std.mem.Allocator;

pub const SyncWaitFn = *const fn (
    ctx: *anyopaque,
    primary: *ha_primary_mod.Primary,
    target_lsn: u64,
    policy: ha_primary_mod.SyncPolicy,
) anyerror!void;

pub const ProgressPollFn = *const fn (
    ctx: *anyopaque,
    primary: *ha_primary_mod.Primary,
    target_lsn: u64,
    policy: ha_primary_mod.SyncPolicy,
    round: usize,
) anyerror!void;

pub const AsyncEffectMirror = struct {
    primary: *ha_primary_mod.Primary,
    last_lsn: ?*std.atomic.Value(u64) = null,
    failure_count: ?*std.atomic.Value(u64) = null,
    sync_policy: ha_primary_mod.SyncPolicy = .{},
    sync_wait_ctx: ?*anyopaque = null,
    sync_wait_fn: ?SyncWaitFn = null,
    last_gate_lsn: ?*std.atomic.Value(u64) = null,
    last_gate_action: ?*std.atomic.Value(u8) = null,
    sync_reject_count: ?*std.atomic.Value(u64) = null,
    sync_wait_count: ?*std.atomic.Value(u64) = null,
    sync_degraded_count: ?*std.atomic.Value(u64) = null,
};

pub const AsyncBatchMirror = AsyncEffectMirror;
pub const AsyncMetadataMirror = AsyncEffectMirror;

pub const PrimaryProgressSyncWait = struct {
    max_rounds: usize = 64,
    sleep_ns: u64 = 0,
    poll_ctx: ?*anyopaque = null,
    poll_fn: ?ProgressPollFn = null,

    pub fn wait(ctx: *anyopaque, primary: *ha_primary_mod.Primary, target_lsn: u64, policy: ha_primary_mod.SyncPolicy) !void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (policy.mode == .async) return;
        if (self.max_rounds == 0) return error.HASyncCommitWaitLimitExceeded;

        var round: usize = 0;
        while (round < self.max_rounds) : (round += 1) {
            if (self.poll_fn) |poll| {
                const poll_ctx = self.poll_ctx orelse return error.HASyncCommitWaitMissingContext;
                try poll(poll_ctx, primary, target_lsn, policy, round);
            }

            const gate = try ha_commit_gate_mod.evaluate(primary, target_lsn, policy);
            if (gate.shouldAcknowledge()) return;
            if (gate.action == .reject) return error.SyncPolicyUnsatisfied;
            if (self.sleep_ns > 0) platform.time.sleepNs(self.sleep_ns);
        }

        return error.HASyncCommitWouldBlock;
    }
};

pub const SessionSyncWait = struct {
    alloc: Allocator,
    slot_name: []const u8,
    standby: *ha_standby_mod.Standby,
    apply_ctx: *anyopaque,
    apply_fn: ha_standby_mod.ApplyFn,
    max_rounds: usize = 8,

    pub fn wait(ctx: *anyopaque, primary: *ha_primary_mod.Primary, target_lsn: u64, policy: ha_primary_mod.SyncPolicy) !void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (policy.mode == .async) return;
        if (!syncPolicyIncludesStandby(policy, self.slot_name)) return error.HASyncCommitWaitStandbyNotInPolicy;
        if (self.max_rounds == 0) return error.HASyncCommitWaitLimitExceeded;

        var progress = self.standby.currentProgress();
        var round: usize = 0;
        while (round < self.max_rounds) : (round += 1) {
            const result = ha_session_mod.replicateAvailable(
                self.alloc,
                primary,
                self.slot_name,
                self.standby,
                self.apply_ctx,
                self.apply_fn,
            ) catch |err| {
                if (policy.mode == .remote_write) {
                    const gate = ha_commit_gate_mod.evaluate(primary, target_lsn, policy) catch return err;
                    if (gate.shouldAcknowledge()) return;
                }
                return err;
            };

            const gate = try ha_commit_gate_mod.evaluate(primary, target_lsn, policy);
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

pub const WriteGate = union(enum) {
    primary: *ha_primary_mod.Primary,
    fenced_primary: ha_write_gate_mod.FencedPrimary,
    standby: *ha_standby_mod.Standby,
};

pub fn writeGateIsStandby(gate: ?WriteGate) bool {
    const configured = gate orelse return false;
    return switch (configured) {
        .primary => false,
        .fenced_primary => false,
        .standby => true,
    };
}

pub const applied_lsn_value_len: usize = @sizeOf(u64);

pub fn appliedReplicationLsnWrite(lsn: u64, value_buf: *[applied_lsn_value_len]u8) docstore_mod.KVPair {
    std.mem.writeInt(u64, value_buf, lsn, .little);
    return .{
        .key = internal_keys.ha_applied_lsn_key[0..],
        .value = value_buf[0..],
    };
}

pub fn readAppliedReplicationLsn(alloc: Allocator, store: *docstore_mod.DocStore) !u64 {
    const raw = store.get(alloc, internal_keys.ha_applied_lsn_key[0..]) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    defer alloc.free(raw);
    if (raw.len != applied_lsn_value_len) return error.CorruptHAAppliedReplicationLsn;
    return std.mem.readInt(u64, raw[0..applied_lsn_value_len], .little);
}

fn syncPolicyIncludesStandby(policy: ha_primary_mod.SyncPolicy, slot_name: []const u8) bool {
    for (policy.standby_names) |name| {
        if (std.mem.eql(u8, name, slot_name)) return true;
    }
    return false;
}
