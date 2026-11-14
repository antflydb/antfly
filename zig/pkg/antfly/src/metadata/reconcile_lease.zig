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

const platform_clock = @import("antfly_platform").clock;

pub const ReconcileLeaseRecord = struct {
    owner_node_id: u64,
    expires_at_ms: u64,
};

pub const Config = struct {
    enabled: bool = true,
    lease_ttl_ms: u64 = 5_000,
    clock: platform_clock.Clock = platform_clock.Clock.real(),
};

pub const Stats = struct {
    enabled: bool = false,
    owner_node_id: u64 = 0,
    expires_at_ms: u64 = 0,

    held_by_local: bool = false,
    acquisition_count: u64 = 0,
    acquire_failures: u64 = 0,
    lost_leases: u64 = 0,
    last_acquired_ms: u64 = 0,
};

pub const State = struct {
    local_node_id: u64,
    config: Config,
    held_by_local: bool = false,
    acquisition_count: u64 = 0,
    acquire_failures: u64 = 0,
    lost_leases: u64 = 0,
    last_acquired_ms: u64 = 0,
    owner_node_id: u64 = 0,
    expires_at_ms: u64 = 0,
    // Process-local proposal debt is shared by reconciliation and CDC. It is
    // never authority: only an applied unexpired lease admits work.
    pending_renewal: ?struct { term: u64, expires_at_ms: u64 } = null,

    pub fn init(local_node_id: u64, config: Config) State {
        return .{
            .local_node_id = local_node_id,
            .config = config,
        };
    }

    pub fn nowMs(self: *const State) u64 {
        return self.config.clock.nowRealtimeMs();
    }

    pub fn shouldRenew(self: *const State, is_local_leader: bool, record: ?ReconcileLeaseRecord, now_ms: u64) bool {
        if (!self.config.enabled or !is_local_leader) return false;
        const current = record orelse return true;
        if (current.owner_node_id != self.local_node_id) return current.expires_at_ms <= now_ms;
        const renew_margin_ms = if (self.config.lease_ttl_ms > 1_000) self.config.lease_ttl_ms / 2 else self.config.lease_ttl_ms;
        return current.expires_at_ms <= now_ms + renew_margin_ms;
    }

    pub fn desiredRecord(self: *const State, now_ms: u64) ReconcileLeaseRecord {
        return .{
            .owner_node_id = self.local_node_id,
            .expires_at_ms = now_ms + self.config.lease_ttl_ms,
        };
    }

    pub fn shouldProposeRenewal(self: *State, leadership_term: ?u64, record: ?ReconcileLeaseRecord, now_ms: u64) bool {
        const term = leadership_term orelse {
            self.pending_renewal = null;
            return false;
        };
        if (self.pending_renewal) |pending| {
            const acknowledged = if (record) |current|
                current.owner_node_id == self.local_node_id and current.expires_at_ms >= pending.expires_at_ms
            else
                false;
            if (pending.term == term and !acknowledged and pending.expires_at_ms > now_ms) return false;
            self.pending_renewal = null;
        }
        return self.shouldRenew(true, record, now_ms);
    }

    pub fn noteRenewalProposed(self: *State, term: u64, record: ReconcileLeaseRecord) void {
        self.pending_renewal = .{ .term = term, .expires_at_ms = record.expires_at_ms };
    }

    pub fn observe(self: *State, is_local_leader: bool, record: ?ReconcileLeaseRecord, now_ms: u64) bool {
        if (!self.config.enabled) {
            const had_lease = self.held_by_local;
            self.owner_node_id = self.local_node_id;
            self.expires_at_ms = 0;
            self.held_by_local = true;
            if (!had_lease) {
                self.acquisition_count += 1;
                self.last_acquired_ms = now_ms;
            }
            return true;
        }

        self.owner_node_id = if (record) |current| current.owner_node_id else 0;
        self.expires_at_ms = if (record) |current| current.expires_at_ms else 0;

        const held_now = blk: {
            const current = record orelse break :blk false;
            break :blk is_local_leader and
                current.owner_node_id == self.local_node_id and
                current.expires_at_ms > now_ms;
        };

        if (held_now and !self.held_by_local) {
            self.acquisition_count += 1;
            self.last_acquired_ms = now_ms;
        } else if (!held_now and self.held_by_local) {
            self.lost_leases += 1;
        }
        self.held_by_local = held_now;
        return held_now;
    }

    pub fn noteAcquireFailure(self: *State) void {
        if (!self.config.enabled) return;
        self.acquire_failures += 1;
    }

    pub fn stats(self: *const State) Stats {
        return .{
            .enabled = self.config.enabled,
            .owner_node_id = self.owner_node_id,
            .expires_at_ms = self.expires_at_ms,
            .held_by_local = self.held_by_local,
            .acquisition_count = self.acquisition_count,
            .acquire_failures = self.acquire_failures,
            .lost_leases = self.lost_leases,
            .last_acquired_ms = self.last_acquired_ms,
        };
    }
};

test "reconcile lease coalesces pending renewals across polling and leadership changes" {
    const std = @import("std");
    var state = State.init(7, .{ .lease_ttl_ms = 5_000 });
    try std.testing.expect(state.shouldProposeRenewal(1, null, 0));
    state.noteRenewalProposed(1, state.desiredRecord(0));
    for (1..3_000) |now| {
        try std.testing.expect(state.shouldRenew(true, null, now));
        try std.testing.expect(!state.shouldProposeRenewal(1, null, now));
    }
    // Proposal debt never grants execution authority.
    try std.testing.expect(!state.observe(true, null, 3_000));
    const old: ReconcileLeaseRecord = .{ .owner_node_id = 7, .expires_at_ms = 5_000 };
    // A delayed acknowledgement can need one fresh renewal immediately.
    try std.testing.expect(state.shouldProposeRenewal(1, old, 3_000));
    state.noteRenewalProposed(1, state.desiredRecord(3_000));
    try std.testing.expect(!state.shouldProposeRenewal(1, old, 3_001));
    // A new term can replace an uncommitted old proposal without waiting for
    // its expiry; observing lost leadership clears the same process-local debt.
    try std.testing.expect(state.shouldProposeRenewal(2, old, 3_002));
    state.noteRenewalProposed(2, state.desiredRecord(3_002));
    try std.testing.expect(!state.shouldProposeRenewal(null, old, 3_003));
    try std.testing.expect(state.pending_renewal == null);
    try std.testing.expect(state.shouldProposeRenewal(3, null, 3_004));
    state.noteRenewalProposed(3, state.desiredRecord(3_004));
    try std.testing.expect(state.shouldProposeRenewal(3, null, 8_004));
    state.noteRenewalProposed(3, state.desiredRecord(8_004));
    const future: ReconcileLeaseRecord = .{ .owner_node_id = 7, .expires_at_ms = 20_000 };
    try std.testing.expect(!state.shouldProposeRenewal(3, future, 8_005));
    try std.testing.expect(state.pending_renewal == null);
}

test "reconcile lease state acquires on observed local leader lease" {
    var state = State.init(7, .{ .enabled = true, .lease_ttl_ms = 500 });
    try @import("std").testing.expect(!state.observe(false, null, 1_000));
    try @import("std").testing.expect(state.observe(true, .{
        .owner_node_id = 7,
        .expires_at_ms = 1_400,
    }, 1_100));
    try @import("std").testing.expectEqual(@as(u64, 1), state.acquisition_count);
}

test "reconcile lease state loses lease on leader loss" {
    var state = State.init(9, .{ .enabled = true, .lease_ttl_ms = 500 });
    _ = state.observe(true, .{
        .owner_node_id = 9,
        .expires_at_ms = 1_400,
    }, 1_100);
    try @import("std").testing.expect(!state.observe(false, .{
        .owner_node_id = 9,
        .expires_at_ms = 1_400,
    }, 1_150));
    try @import("std").testing.expectEqual(@as(u64, 1), state.lost_leases);
}
