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
    /// Authorization/projection generation, independent of root retention revision.
    policy_revision: u64,
    limit: u64 = history.default_limit,
    /// Establish the timestamped row-write protocol before recording the cut.
    require_commit_clock: bool = false,

    pub fn jsonStringify(self: Grant, jw: anytype) !void {
        try jw.beginObject();
        inline for (.{ "namespace", "authority", "sequence", "consumer", "definition", "expires_at_seconds", "policy_revision", "limit" }) |field| {
            try jw.objectField(field);
            try jw.write(@field(self, field));
        }
        if (self.require_commit_clock) {
            try jw.objectField("require_commit_clock");
            try jw.write(true);
        }
        try jw.endObject();
    }

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
/// Read-only admission identity. This observes a lease; it does not certify
/// the payload or outcome of an enrollment. An opted-in observation can also
/// prove the current immutable bootstrap pin for this exact grant.
pub const GrantIdentity = struct {
    namespace: Namespace,
    authority: Digest,
    consumer: Id,
    definition: Digest,
    sequence: u64,
    policy_revision: u64,
    include_bootstrap: bool = false,
    include_cut: bool = false,

    pub fn jsonStringify(self: GrantIdentity, jw: anytype) !void {
        try jw.beginObject();
        inline for (.{ "namespace", "authority", "consumer", "definition", "sequence", "policy_revision" }) |field| {
            try jw.objectField(field);
            try jw.write(@field(self, field));
        }
        if (self.include_bootstrap) {
            try jw.objectField("include_bootstrap");
            try jw.write(true);
        }
        if (self.include_cut) {
            try jw.objectField("include_cut");
            try jw.write(true);
        }
        try jw.endObject();
    }

    pub fn fromGrant(value: Grant) GrantIdentity {
        return .{ .namespace = value.namespace, .authority = value.authority, .consumer = value.consumer, .definition = value.definition, .sequence = value.sequence, .policy_revision = value.policy_revision };
    }
    pub fn validate(self: GrantIdentity) !void {
        if (!nonzero(self.namespace) or !nonzero(self.authority) or !nonzero(self.consumer) or !nonzero(self.definition) or self.sequence == 0 or self.policy_revision == 0) return error.InvalidHistoryAdmission;
    }
};
pub const GrantObservation = union(enum) {
    /// No control authority is bound yet. This is not a reserved admission.
    unbound,
    /// No lease exists at this cut, and the grant is above the retirement floor.
    absent: u64,
    /// No lease exists and this grant is irrevocably fenced by the floor.
    retired: u64,
    active: Active,
    /// Same immutable consumer/definition, but a different grant is current.
    /// Never treat this as confirmation of the requested grant.
    other_grant: Active,
    pub const Active = struct {
        grant: u64,
        consumer: history.Consumer,
        limit: u64,
        bootstrap: ?BootstrapObservation = null,
        quiescent_latest: ?u64 = null,

        pub fn jsonStringify(self: Active, jw: anytype) !void {
            try jw.beginObject();
            inline for (.{ "grant", "consumer", "limit" }) |field| {
                try jw.objectField(field);
                try jw.write(@field(self, field));
            }
            if (self.bootstrap) |pin| {
                try jw.objectField("bootstrap");
                try jw.write(pin);
            }
            if (self.quiescent_latest) |latest| {
                try jw.objectField("quiescent_latest");
                try jw.write(latest);
            }
            try jw.endObject();
        }
    };
    pub const BootstrapObservation = struct { digest: Digest, expires_at_seconds: u64 };
};
/// Observe authority, retirement floor and lease in one stable transaction.
/// This never writes a receipt, enrolls, renews, acknowledges or binds authority.
pub fn observeGrant(txn: anytype, requested: GrantIdentity) !GrantObservation {
    try requested.validate();
    try requireRoot(txn, requested.namespace);
    var native = try history.load(txn);
    var state = (try load(txn)) orelse {
        if (native) |value| for (value.consumers) |consumer| {
            if (consumer.epoch != 0 and same(consumer.id, requested.consumer)) return error.HistoryConsumerConflict;
        };
        return .unbound;
    };
    if (!same(state.namespace, requested.namespace) or !same(state.authority, requested.authority)) return error.HistoryResynchronizationRequired;
    if (state.find(requested.consumer)) |lease| {
        if (!same(lease.definition, requested.definition)) return error.HistoryConsumerConflict;
        const physical = if (native) |*value| value else return error.HistoryCorrupt;
        const consumer = (physical.findConsumer(lease.id, lease.epoch) catch return error.HistoryCorrupt).*;
        if (consumer.policy_revision != requested.policy_revision) return error.HistoryConsumerConflict;
        var result: GrantObservation.Active = .{ .grant = lease.grant, .consumer = consumer, .limit = physical.limit };
        if (lease.grant != requested.sequence) return .{ .other_grant = result };
        if (requested.include_bootstrap) {
            if (try readBootstrapBlob(txn, requested.consumer)) |raw| {
                const pin = try bootstrapPin(requested.namespace, consumer, raw);
                result.bootstrap = .{ .digest = pin.digest, .expires_at_seconds = pin.expires_at_seconds };
            }
        }
        if (requested.include_cut) result.quiescent_latest = history.requireBootstrapCut(txn, requested.namespace) catch |err| switch (err) {
            error.HistoryPreparedTransactionsPresent => null,
            else => return err,
        };
        return .{ .active = result };
    }
    if (native) |value| for (value.consumers) |consumer| {
        if (consumer.epoch != 0 and same(consumer.id, requested.consumer)) return error.HistoryConsumerConflict;
    };
    return if (requested.sequence <= state.floor) .{ .retired = state.floor } else .{ .absent = state.floor };
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
const bootstrap_prefix = "\x00\x00__committed_history__:bootstrap:";
const bootstrap_header_size = 36;
pub const max_bootstrap_bytes = 8 * 1024 * 1024;
pub const max_bootstrap_examined_keys = 65536;

pub const BootstrapPin = struct {
    consumer: history.Consumer,
    digest: Digest,
    expires_at_seconds: u64,
};

fn bootstrapKey(id: Id) [bootstrap_prefix.len + @sizeOf(Id)]u8 {
    return bootstrap_prefix.* ++ id;
}

fn readBootstrapBlob(txn: anytype, id: Id) !?[]const u8 {
    const raw = txn.get(&bootstrapKey(id)) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len < bootstrap_header_size or raw.len > max_bootstrap_bytes or !std.mem.eql(u8, raw[0..4], "HBS1"))
        return error.HistoryCorrupt;
    return raw;
}

fn bootstrapPin(namespace: Namespace, consumer: history.Consumer, raw: []const u8) !BootstrapPin {
    const start = std.mem.readInt(u64, raw[4..12], .little);
    const epoch = std.mem.readInt(u64, raw[12..20], .little);
    const expires = std.mem.readInt(u64, raw[20..28], .little);
    if (start != consumer.start or epoch != consumer.epoch or expires == 0 or expires > consumer.expires_at_seconds)
        return error.HistoryCorrupt;
    var proof = std.crypto.hash.sha2.Sha256.init(.{});
    proof.update("antfly:root-bootstrap-pin:v1");
    proof.update(&namespace);
    proof.update(&consumer.id);
    proof.update(raw);
    return .{ .consumer = consumer, .digest = proof.finalResult(), .expires_at_seconds = expires };
}

/// Materialize an immutable bounded bootstrap image and enroll its continuation
/// in the same root transaction. The caller aborts the transaction on any error.
/// This stores physical historical rows and schema for an authorized projection
/// to decode, and never exposes auxiliary row keys.
pub fn captureBootstrap(a: std.mem.Allocator, txn: anytype, grant: Grant, expires_at_seconds: u64) !BootstrapPin {
    try grant.validate();
    if (expires_at_seconds == 0 or expires_at_seconds > grant.expires_at_seconds) return error.InvalidHistoryAdmission;
    var state = try stateFor(txn, grant.namespace, grant.authority);
    if (try readBootstrapBlob(txn, grant.consumer)) |_| {
        if (state.find(grant.consumer) == null) return error.HistoryCorrupt;
        const consumer = try enroll(txn, grant);
        const pin = try bootstrapPin(grant.namespace, consumer, (try readBootstrapBlob(txn, grant.consumer)) orelse return error.HistoryCorrupt);
        if (pin.expires_at_seconds != expires_at_seconds) return error.HistoryConsumerConflict;
        return pin;
    }
    if (state.find(grant.consumer) != null) return error.HistoryConsumerConflict;
    const frontier = try history.requireBootstrapCut(txn, grant.namespace);
    var image: std.ArrayList(u8) = .empty;
    defer image.deinit(a);
    try image.appendNTimes(a, 0, bootstrap_header_size);
    const schema = txn.get(keys.public_schema_json_key) catch |err| switch (err) {
        error.NotFound => "",
        else => return err,
    };
    if (schema.len > history.frame.max_schema_bytes) return error.HistoryTooLarge;
    try image.appendSlice(a, schema);
    const schema_size: u32 = @intCast(schema.len);
    var rows: u32 = 0;
    {
        var cursor = try txn.openCursor();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(&.{keys.user_namespace});
        var examined: usize = 0;
        while (entry) |item| : (entry = try cursor.next()) {
            if (item.key.len == 0 or item.key[0] != keys.user_namespace) break;
            if (examined == max_bootstrap_examined_keys) return error.HistoryTooLarge;
            examined += 1;
            if (!keys.isStoredDocumentRowKey(item.key)) continue;
            if (8 +| item.key.len +| item.value.len > max_bootstrap_bytes - image.items.len) return error.HistoryTooLarge;
            var lengths: [8]u8 = undefined;
            std.mem.writeInt(u32, lengths[0..4], @intCast(item.key.len), .little);
            std.mem.writeInt(u32, lengths[4..8], @intCast(item.value.len), .little);
            try image.appendSlice(a, &lengths);
            try image.appendSlice(a, item.key);
            try image.appendSlice(a, item.value);
            rows += 1;
        }
    }
    const consumer = try enroll(txn, grant);
    if (consumer.start != frontier) return error.HistoryCorrupt;
    @memcpy(image.items[0..4], "HBS1");
    std.mem.writeInt(u64, image.items[4..12], frontier, .little);
    std.mem.writeInt(u64, image.items[12..20], consumer.epoch, .little);
    std.mem.writeInt(u64, image.items[20..28], expires_at_seconds, .little);
    std.mem.writeInt(u32, image.items[28..32], schema_size, .little);
    std.mem.writeInt(u32, image.items[32..36], rows, .little);
    const pin = try bootstrapPin(grant.namespace, consumer, image.items);
    try txn.put(&bootstrapKey(grant.consumer), image.items);
    return pin;
}

pub const BootstrapImage = struct {
    schema: []const u8,
    rows: Rows,

    pub const Row = struct { key: []const u8, value: []const u8 };
    pub const Rows = struct {
        remaining: []const u8,
        count: u32,

        pub fn next(self: *Rows) !?Row {
            if (self.count == 0) {
                if (self.remaining.len != 0) return error.HistoryCorrupt;
                return null;
            }
            if (self.remaining.len < 8) return error.HistoryCorrupt;
            const key_size = std.mem.readInt(u32, self.remaining[0..4], .little);
            const value_size = std.mem.readInt(u32, self.remaining[4..8], .little);
            const payload = self.remaining[8..];
            if (key_size > payload.len or value_size > payload.len - key_size) return error.HistoryCorrupt;
            const row_key = payload[0..key_size];
            if (!keys.isStoredDocumentRowKey(row_key)) return error.HistoryCorrupt;
            const end = @as(usize, key_size) + value_size;
            self.remaining = payload[end..];
            self.count -= 1;
            return .{ .key = row_key, .value = payload[key_size..end] };
        }
    };

    pub fn decode(raw: []const u8) !BootstrapImage {
        if (raw.len < bootstrap_header_size or raw.len > max_bootstrap_bytes or !std.mem.eql(u8, raw[0..4], "HBS1"))
            return error.HistoryCorrupt;
        const schema_size = std.mem.readInt(u32, raw[28..32], .little);
        const row_count = std.mem.readInt(u32, raw[32..36], .little);
        const payload = raw[bootstrap_header_size..];
        if (schema_size > history.frame.max_schema_bytes or schema_size > payload.len or row_count > max_bootstrap_examined_keys)
            return error.HistoryCorrupt;
        return .{ .schema = payload[0..schema_size], .rows = .{ .count = row_count, .remaining = payload[schema_size..] } };
    }
};

/// Borrow an immutable physical snapshot from the authorized root transaction.
pub fn readBootstrap(txn: anytype, ref: Reference, digest: Digest, now: u64) ![]const u8 {
    const consumer = try inspect(txn, ref);
    const raw = (try readBootstrapBlob(txn, ref.consumer)) orelse return error.HistoryResynchronizationRequired;
    const pin = try bootstrapPin(ref.namespace, consumer, raw);
    if (!same(pin.digest, digest) or now == 0 or now >= pin.expires_at_seconds)
        return error.HistoryResynchronizationRequired;
    return raw;
}

/// Release only the image under an already authorized live native grant.
/// The streaming continuation and its acknowledgment remain intact.
pub fn releaseBootstrap(txn: anytype, ref: Reference, digest: Digest) !void {
    const consumer = try inspect(txn, ref);
    const raw = (try readBootstrapBlob(txn, ref.consumer)) orelse return;
    const pin = try bootstrapPin(ref.namespace, consumer, raw);
    if (!same(pin.digest, digest)) return error.HistoryConsumerConflict;
    try txn.delete(&bootstrapKey(ref.consumer));
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
        if (try readBootstrapBlob(txn, lease.id)) |_| try txn.delete(&bootstrapKey(lease.id));
        lease.* = .{};
    } else if (try history.load(txn)) |native| {
        for (native.consumers) |consumer| if (consumer.epoch != 0 and same(consumer.id, closed.consumer)) return error.HistoryConsumerConflict;
    }
    state.floor = @max(state.floor, closed.sequence);
    try txn.put(key, &state.encode());
}

pub const PolicySnapshot = struct {
    revision: u64,
    limit: u64,
};
pub const PolicyScope = struct { namespace: Namespace, authority: Digest };
pub fn inspectPolicy(txn: anytype, scope: PolicyScope) !PolicySnapshot {
    if (!nonzero(scope.namespace) or !nonzero(scope.authority)) return error.InvalidHistoryAdmission;
    _ = try stateFor(txn, scope.namespace, scope.authority);
    const value = (try history.loadPolicy(txn)) orelse return error.HistoryResynchronizationRequired;
    if (!same(value.namespace, scope.namespace) or !same(value.authority, scope.authority)) return error.HistoryResynchronizationRequired;
    return .{ .revision = value.revision, .limit = value.limit };
}

pub const Command = union(enum) {
    enroll: Grant,
    capture_bootstrap: struct { grant: Grant, expires_at_seconds: u64 },
    release_bootstrap: struct { reference: Reference, digest: Digest },
    inspect: Reference,
    acknowledge: struct { reference: Reference, previous: u64, next: u64 },
    renew: struct { reference: Reference, previous: u64, next: u64 },
    retire: Retirement,
    reclaim: struct { reference: Reference, max_frames: u8 = 1 },
    /// Owner-scoped maintenance remains possible after the final consumer is
    /// released. It never invents an ACK: the shared native floor still rules.
    collect: struct { namespace: Namespace, authority: Digest, max_frames: u8 = 1 },
    configure_policy: history.Policy,
    inspect_policy: PolicyScope,
    pub fn namespace(self: Command) Namespace {
        return switch (self) {
            .configure_policy => |value| value.namespace,
            .inspect_policy => |value| value.namespace,
            .enroll => |value| value.namespace,
            .capture_bootstrap => |value| value.grant.namespace,
            .release_bootstrap => |value| value.reference.namespace,
            .inspect => |value| value.namespace,
            .retire => |value| value.namespace,
            .collect => |value| value.namespace,
            .acknowledge => |value| value.reference.namespace,
            .renew => |value| value.reference.namespace,
            .reclaim => |value| value.reference.namespace,
        };
    }
};
pub const Result = struct {
    consumer: ?history.Consumer = null,
    reclaimed_frames: u8 = 0,
    policy: ?PolicySnapshot = null,
    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.beginObject();
        try jw.objectField("consumer");
        try jw.write(self.consumer);
        try jw.objectField("reclaimed_frames");
        try jw.write(self.reclaimed_frames);
        if (self.policy) |value| {
            try jw.objectField("policy");
            try jw.write(value);
        }
        try jw.endObject();
    }
};
/// Apply inside the ordered owner's one atomic storage transaction. Errors must
/// abort that transaction, including errors after staging an admission/release.
pub fn apply(a: std.mem.Allocator, txn: anytype, command: Command) !Result {
    return switch (command) {
        .configure_policy => |value| blk: {
            const state = try stateFor(txn, value.namespace, value.authority);
            try history.configurePolicy(txn, value);
            try txn.put(key, &state.encode());
            break :blk .{ .policy = try inspectPolicy(txn, .{ .namespace = value.namespace, .authority = value.authority }) };
        },
        .inspect_policy => |value| .{ .policy = try inspectPolicy(txn, value) },
        .enroll => |value| .{ .consumer = try enroll(txn, value) },
        .capture_bootstrap => |value| .{ .consumer = (try captureBootstrap(a, txn, value.grant, value.expires_at_seconds)).consumer },
        .release_bootstrap => |value| blk: {
            try releaseBootstrap(txn, value.reference, value.digest);
            break :blk .{};
        },
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
        .collect => |value| blk: {
            if (!nonzero(value.namespace) or !nonzero(value.authority)) return error.InvalidHistoryAdmission;
            if (try load(txn) == null) return error.HistoryResynchronizationRequired;
            _ = try stateFor(txn, value.namespace, value.authority);
            break :blk .{ .reclaimed_frames = try reclaimFrames(txn, value.namespace, value.max_frames) };
        },
        .reclaim => |value| blk: {
            const consumer = try inspect(txn, value.reference);
            break :blk .{ .consumer = consumer, .reclaimed_frames = try reclaimFrames(txn, value.reference.namespace, value.max_frames) };
        },
    };
}

fn reclaimFrames(txn: anytype, namespace: Namespace, max_frames: u8) !u8 {
    if (max_frames == 0 or max_frames > 32) return error.InvalidHistoryAdmission;
    var count: u8 = 0;
    while (count < max_frames) {
        const before = (try history.load(txn)) orelse break;
        const done = try history.reclaim(txn, namespace);
        const after = (try history.load(txn)) orelse return error.HistoryCorrupt;
        if (after.reclaimed > before.reclaimed) count += 1;
        if (done) break;
    }
    return count;
}
