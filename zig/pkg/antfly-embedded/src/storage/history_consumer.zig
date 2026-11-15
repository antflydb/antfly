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

//! Private root consumer control. The metadata owner verifies authorization and
//! irrevocable close/reset before submitting these commands through the root's
//! ordered owner. A fixed authority floor rejects late enrollment after cleanup
//! without accumulating a tombstone for every deleted consumer. This module
//! neither authenticates a public request nor invents a coordinated snapshot cut.
const std = @import("std");
const history = @import("committed_history.zig");
const authority = @import("source_authority.zig");
const keys = @import("internal_keys.zig");
pub const Namespace = history.frame.Namespace;
pub const Digest = [32]u8;
pub const Id = [16]u8;
pub const key = history.consumer_authority_key;
const encoded_size = 4 + 24 + 32 + 8 + history.max_consumers * 64 + 32;
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
fn nonzero(value: anytype) bool {
    return !std.mem.allEqual(u8, &value, 0);
}
pub const Grant = struct {
    namespace: Namespace,
    authority: Digest,
    sequence: u64,
    consumer: Id,
    definition: Digest,
    expires_at_seconds: u64,
    policy_revision: u64,
    limit: u64 = history.default_limit,
    pub fn validate(self: Grant) !void {
        if (!nonzero(self.namespace) or !nonzero(self.authority) or self.sequence == 0 or !nonzero(self.consumer) or !nonzero(self.definition) or self.expires_at_seconds == 0 or self.policy_revision == 0) return error.InvalidHistoryAdmission;
    }
};
pub const Lease = struct {
    id: Id = @splat(0),
    epoch: u64 = 0,
    grant: u64 = 0,
    definition: Digest = @splat(0),
};
pub const State = struct {
    namespace: Namespace,
    authority: Digest,
    floor: u64 = 0,
    leases: [history.max_consumers]Lease = @splat(.{}),
    fn find(self: *State, id: Id) ?*Lease {
        for (&self.leases) |*lease| if (lease.epoch != 0 and same(lease.id, id)) return lease;
        return null;
    }
    pub fn encode(self: State) [encoded_size]u8 {
        var out: [encoded_size]u8 = @splat(0);
        @memcpy(out[0..4], "HCA1");
        @memcpy(out[4..28], &self.namespace);
        @memcpy(out[28..60], &self.authority);
        std.mem.writeInt(u64, out[60..68], self.floor, .little);
        var offset: usize = 68;
        for (self.leases) |lease| {
            @memcpy(out[offset..][0..16], &lease.id);
            std.mem.writeInt(u64, out[offset + 16 ..][0..8], lease.epoch, .little);
            std.mem.writeInt(u64, out[offset + 24 ..][0..8], lease.grant, .little);
            @memcpy(out[offset + 32 ..][0..32], &lease.definition);
            offset += 64;
        }
        @memcpy(out[offset..], &history.frame.digest(out[0..offset]));
        return out;
    }
    pub fn decode(raw: []const u8) !State {
        if (raw.len != encoded_size or !std.mem.eql(u8, raw[0..4], "HCA1") or !same(raw[raw.len - 32 ..][0..32].*, history.frame.digest(raw[0 .. raw.len - 32]))) return error.HistoryCorrupt;
        var state: State = .{ .namespace = raw[4..28].*, .authority = raw[28..60].*, .floor = std.mem.readInt(u64, raw[60..68], .little) };
        if (!nonzero(state.namespace) or !nonzero(state.authority)) return error.HistoryCorrupt;
        for (&state.leases, 0..) |*lease, index| {
            const offset = 68 + index * 64;
            lease.* = .{ .id = raw[offset..][0..16].*, .epoch = std.mem.readInt(u64, raw[offset + 16 ..][0..8], .little), .grant = std.mem.readInt(u64, raw[offset + 24 ..][0..8], .little), .definition = raw[offset + 32 ..][0..32].* };
            if (lease.epoch == 0) {
                if (!std.meta.eql(lease.*, Lease{})) return error.HistoryCorrupt;
            } else {
                if (!nonzero(lease.id) or lease.grant == 0 or !nonzero(lease.definition)) return error.HistoryCorrupt;
                for (state.leases[0..index]) |prior| if (prior.epoch != 0 and same(prior.id, lease.id)) return error.HistoryCorrupt;
            }
        }
        return state;
    }
};
pub fn load(txn: anytype) !?State {
    const raw = txn.get(key) catch |err| if (err == error.NotFound) return null else return err;
    return try State.decode(raw);
}
fn requireRoot(txn: anytype, namespace: Namespace) !void {
    const raw = txn.get(&keys.identity_namespace_key) catch |err| if (err == error.NotFound) return error.HistoryResynchronizationRequired else return err;
    if (!std.mem.eql(u8, raw, &namespace)) return error.HistoryResynchronizationRequired;
    const owner = try authority.load(txn) orelse return error.HistoryAuthorityRequired;
    if (!same(owner.namespace, namespace)) return error.HistoryResynchronizationRequired;
    if (try history.load(txn)) |native| if (!same(native.namespace, namespace)) return error.HistoryResynchronizationRequired;
}
fn stateFor(txn: anytype, namespace: Namespace, incarnation: Digest) !State {
    try requireRoot(txn, namespace);
    const state = (try load(txn)) orelse return State{ .namespace = namespace, .authority = incarnation };
    if (!same(state.namespace, namespace) or !same(state.authority, incarnation)) return error.HistoryResynchronizationRequired;
    return state;
}
/// Retry retains the original start and consumer epoch. A newer grant may only
/// refresh the same immutable definition, never silently reset history position.
pub fn enroll(txn: anytype, grant: Grant) !history.Consumer {
    try grant.validate();
    var state = try stateFor(txn, grant.namespace, grant.authority);
    const existing = state.find(grant.consumer);
    if (existing) |lease| {
        if (!same(lease.definition, grant.definition) or grant.sequence < lease.grant) return error.HistoryConsumerConflict;
        var native = (try history.load(txn)) orelse return error.HistoryCorrupt;
        const current = (try native.findConsumer(grant.consumer, lease.epoch)).*;
        // Enrollment retry recovers the actual lease even if its original reply
        // was lost before a later renewal. It cannot shorten or extend expiry;
        // the metadata owner separately renews the returned epoch by CAS.
        const consumer = try history.admit(txn, grant.namespace, grant.consumer, grant.limit, current.expires_at_seconds, grant.policy_revision);
        if (consumer.epoch != lease.epoch) return error.HistoryCorrupt;
        if (grant.sequence != lease.grant) {
            lease.grant = grant.sequence;
            try txn.put(key, &state.encode());
        }
        return consumer;
    }
    if (grant.sequence <= state.floor) return error.HistoryResynchronizationRequired;
    const slot = for (&state.leases) |*lease| {
        if (lease.epoch == 0) break lease;
    } else return error.HistoryConsumerLimit;
    // A legacy uncontrolled consumer cannot be adopted through a new grant.
    if (try history.load(txn)) |native| for (native.consumers) |consumer| {
        if (consumer.epoch != 0 and same(consumer.id, grant.consumer)) return error.HistoryConsumerConflict;
    };
    const consumer = try history.admit(txn, grant.namespace, grant.consumer, grant.limit, grant.expires_at_seconds, grant.policy_revision);
    slot.* = .{ .id = consumer.id, .epoch = consumer.epoch, .grant = grant.sequence, .definition = grant.definition };
    try txn.put(key, &state.encode());
    return consumer;
}
pub const Reference = struct {
    namespace: Namespace,
    authority: Digest,
    consumer: Id,
    epoch: u64,
    grant: u64,
    definition: Digest,
    pub fn validate(self: Reference) !void {
        if (!nonzero(self.namespace) or !nonzero(self.authority) or !nonzero(self.consumer) or self.epoch == 0 or self.grant == 0 or !nonzero(self.definition)) return error.InvalidHistoryAdmission;
    }
};
pub fn inspect(txn: anytype, ref: Reference) !history.Consumer {
    try ref.validate();
    var state = try stateFor(txn, ref.namespace, ref.authority);
    const lease = state.find(ref.consumer) orelse return error.HistoryResynchronizationRequired;
    if (lease.epoch != ref.epoch or lease.grant != ref.grant or !same(lease.definition, ref.definition)) return error.HistoryConsumerConflict;
    var native = (try history.load(txn)) orelse return error.HistoryCorrupt;
    return (try native.findConsumer(ref.consumer, ref.epoch)).*;
}
pub fn acknowledge(txn: anytype, ref: Reference, previous: u64, next: u64) !void {
    const consumer = try inspect(txn, ref);
    if (next < previous) return error.HistoryCheckpointConflict;
    if (consumer.acknowledged == next) return;
    try history.acknowledge(txn, ref.namespace, ref.consumer, ref.epoch, previous, next);
}
/// The metadata owner durably extends its lease before applying this CAS. Roots
/// never autonomously delete expired consumers: a renewal racing deletion must
/// be settled by the metadata close/reset transition first.
pub fn renew(txn: anytype, ref: Reference, previous: u64, next: u64) !void {
    const consumer = try inspect(txn, ref);
    if (next <= previous) return error.InvalidHistoryAdmission;
    if (consumer.expires_at_seconds == next) return;
    if (consumer.expires_at_seconds != previous) return error.HistoryConsumerConflict;
    try history.renew(txn, ref.namespace, ref.consumer, ref.epoch, previous, next);
}
pub const Retirement = struct {
    namespace: Namespace,
    authority: Digest,
    sequence: u64,
    consumer: Id,
    definition: Digest,
    /// Original enrollment grant, including when its reply was lost. Cleanup
    /// does not require the caller to know whether that grant created a lease.
    grant: u64,
};
pub fn retire(txn: anytype, closed: Retirement) !void {
    if (!nonzero(closed.namespace) or !nonzero(closed.authority) or !nonzero(closed.consumer) or !nonzero(closed.definition) or closed.grant == 0 or closed.sequence <= closed.grant) return error.InvalidHistoryAdmission;
    var state = try stateFor(txn, closed.namespace, closed.authority);
    if (state.find(closed.consumer)) |lease| {
        if (!same(lease.definition, closed.definition) or lease.grant > closed.grant) return error.HistoryConsumerConflict;
        try history.release(txn, closed.namespace, lease.id, lease.epoch);
        lease.* = .{};
    } else if (try history.load(txn)) |native| {
        for (native.consumers) |consumer| if (consumer.epoch != 0 and same(consumer.id, closed.consumer)) return error.HistoryConsumerConflict;
    }
    state.floor = @max(state.floor, closed.sequence);
    try txn.put(key, &state.encode());
}

pub const Command = union(enum) {
    enroll: Grant,
    inspect: Reference,
    acknowledge: struct { reference: Reference, previous: u64, next: u64 },
    renew: struct { reference: Reference, previous: u64, next: u64 },
    retire: Retirement,
    reclaim: struct { reference: Reference, max_frames: u8 = 1 },
    pub fn namespace(self: Command) Namespace {
        return switch (self) {
            .enroll => |value| value.namespace,
            .inspect => |value| value.namespace,
            .retire => |value| value.namespace,
            .acknowledge => |value| value.reference.namespace,
            .renew => |value| value.reference.namespace,
            .reclaim => |value| value.reference.namespace,
        };
    }
};
pub const Result = struct { consumer: ?history.Consumer = null, reclaimed_frames: u8 = 0 };
/// Apply inside the ordered owner's one atomic storage transaction. Errors must
/// abort that transaction, including errors after staging an admission/release.
pub fn apply(txn: anytype, command: Command) !Result {
    return switch (command) {
        .enroll => |value| .{ .consumer = try enroll(txn, value) },
        .inspect => |value| .{ .consumer = try inspect(txn, value) },
        .acknowledge => |value| blk: {
            try acknowledge(txn, value.reference, value.previous, value.next);
            break :blk .{ .consumer = try inspect(txn, value.reference) };
        },
        .renew => |value| blk: {
            try renew(txn, value.reference, value.previous, value.next);
            break :blk .{ .consumer = try inspect(txn, value.reference) };
        },
        .retire => |value| blk: {
            try retire(txn, value);
            break :blk .{};
        },
        .reclaim => |value| blk: {
            if (value.max_frames == 0 or value.max_frames > 32) return error.InvalidHistoryAdmission;
            const consumer = try inspect(txn, value.reference);
            var count: u8 = 0;
            while (count < value.max_frames) {
                const before = (try history.load(txn)).?.reclaimed;
                const done = try history.reclaim(txn, value.reference.namespace);
                const after = (try history.load(txn)).?.reclaimed;
                if (after > before) count += 1;
                if (done) break;
            }
            break :blk .{ .consumer = consumer, .reclaimed_frames = count };
        },
    };
}
