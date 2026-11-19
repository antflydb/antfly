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

//! Connection-owned delivery of retained, published logical-slot spools.
//! Poll never builds a transaction or acknowledges it. Publication/capture runs
//! in the separate upload owner. Policy authorizes current credentials, the
//! immutable database/table scope and the entire bound publication definition.
const std = @import("std");
const control = @import("changefeed_control.zig");
const slot_control = @import("pg_slot.zig");
const spool = @import("pg_spool.zig");
const artifacts = @import("pg_spool_artifacts.zig");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
pub const Config = struct {
    lease_seconds: u64 = slot_control.max_session_seconds,
    require_shared: bool = true,
    fn validate(self: Config) !void {
        if (self.lease_seconds == 0 or self.lease_seconds > slot_control.max_session_seconds) return error.InvalidChangefeed;
    }
};
pub const Positions = struct { written: u64, flushed: u64, applied: u64 };
pub const Page = struct {
    allocator: A,
    end_lsn: u64,
    start_lsn: u64,
    bytes: ?[]u8 = null,
    pub fn deinit(self: *Page) void {
        if (self.bytes) |bytes| self.allocator.free(bytes);
        self.* = undefined;
    }
};
fn load(svc: anytype, a: A, id: control.Id, request: anytype) !control.Parsed {
    try request.ensureActive();
    const bytes = try control.readOnService(svc, a, id, request);
    defer a.free(bytes);
    var parsed = try control.parse(a, bytes);
    errdefer parsed.deinit();
    if (!same(parsed.value.id, id)) return error.ChangefeedResynchronizationRequired;
    return parsed;
}
fn mutate(svc: anytype, a: A, state: control.State, mutation: slot_control.Mutation, request: anytype) !void {
    const definition = state.definition orelse return error.ChangefeedConflict;
    return control.mutateOnService(svc, a, .{ .id = state.id, .authority = state.authority, .owner = definition.owner, .authorization = definition.authorization, .expected_revision = state.revision, .expected_epoch = state.epoch, .mutation = .{ .slot = mutation } }, request);
}
fn active(policy: anytype, state: control.State, binding: slot_control.Binding, request: anytype) !u64 {
    try request.ensureActive();
    try policy.authorize(state, binding, request);
    const now = try policy.nowSeconds(request);
    if (state.phase != .streaming or now >= state.expires_at) return error.ChangefeedResynchronizationRequired;
    if (state.slot == null) return error.ChangefeedCapabilityUnsupported;
    return now;
}
fn Cancellation(comptime Request: type) type {
    return struct {
        request: Request,
        fn check(raw: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            try self.request.ensureActive();
        }
        fn token(self: *const @This()) CancellationToken {
            return .{ .ptr = self, .check_fn = check };
        }
    };
}
/// Connection-finalizer recovery after an ambiguous open. Actor IDs are
/// private, unique connection capabilities; invoke only after its work drains.
pub fn releaseActor(svc: anytype, policy: anytype, a: A, id: control.Id, actor: control.Id, request: anytype) !void {
    var current = try load(svc, a, id, request);
    defer current.deinit();
    const slot = current.value.slot orelse return;
    const session = slot.session orelse return;
    if (!same(session.actor, actor)) return;
    const now = try policy.nowSeconds(request);
    if (now < slot.observed_at or now >= slot.session_expires_at or (current.value.phase != .streaming and current.value.phase != .bootstrap)) return;
    try mutate(svc, a, current.value, .{ .release = .{ .session = session, .now = now } }, request);
}
pub const Owner = struct {
    id: control.Id,
    epoch: u64,
    authority: control.Digest,
    definition: control.Digest,
    binding: control.Digest,
    session: slot_control.Session,
    config: Config,
    initial_lsn: u64,
    resume_lsn: u64,
    initial_end_lsn: u64,
    frontier: u64,
    end_lsn: u64,
    ceiling: u64,
    closed: bool = false,
    pub fn open(svc: anytype, registry: anytype, policy: anytype, a: A, id: control.Id, name: []const u8, actor: control.Id, requested: u64, binding: slot_control.Binding, config: Config, request: anytype) !Owner {
        try config.validate();
        try binding.validate();
        var before = try load(svc, a, id, request);
        defer before.deinit();
        const now = try active(policy, before.value, binding, request);
        const slot = before.value.slot.?;
        if (!std.mem.eql(u8, name, slot.name)) return error.ChangefeedConflict;
        if (slot.binding) |old| {
            if (!same(old.identity(), binding.identity())) return error.ChangefeedResynchronizationRequired;
        } else if (slot.spool.next_id != 1 or slot.entries.len != 0) return error.ChangefeedCapabilityUnsupported;
        _ = try slot.restartAt(requested);
        try registry.qualify(before.value, config.require_shared, request);
        const expires = @min(before.value.expires_at, now +| config.lease_seconds);
        if (slot.session) |current_session| {
            if (now < slot.session_expires_at and !same(current_session.actor, actor))
                return error.ReplicationSlotActive;
        }
        try mutate(svc, a, before.value, .{ .acquire = .{ .actor = actor, .expected = slot.fence, .now = now, .expires = @max(slot.session_expires_at, expires) } }, request);
        var acquired = try load(svc, a, id, request);
        defer acquired.deinit();
        const acquired_now = try active(policy, acquired.value, binding, request);
        const session = acquired.value.slot.?.session orelse return error.ChangefeedConflict;
        if (!same(session.actor, actor)) return error.ChangefeedConflict;
        try acquired.value.slot.?.verifySession(session, acquired_now);
        if (acquired.value.slot.?.binding == null)
            try mutate(svc, a, acquired.value, .{ .bind = .{ .session = session, .now = acquired_now, .binding = binding } }, request);
        var observed = try load(svc, a, id, request);
        defer observed.deinit();
        const observed_now = try active(policy, observed.value, binding, request);
        const current = observed.value.slot.?;
        try current.verifySession(session, observed_now);
        if (current.binding == null or !same(current.binding.?.identity(), binding.identity())) return error.ChangefeedConflict;
        const initial = try current.restartAt(requested);
        for (current.entries) |entry| {
            const record = current.spool.find(entry.spool_id) orelse return error.ChangefeedCapabilityUnsupported;
            if (record.phase != .live or record.spec.messages < 3 or std.mem.allEqual(u8, &record.spec.wire, 0)) return error.ChangefeedCapabilityUnsupported;
        }
        return .{ .id = id, .epoch = observed.value.epoch, .authority = observed.value.authority, .definition = observed.value.definition.?.identity(), .binding = binding.identity(), .session = session, .config = config, .initial_lsn = initial, .resume_lsn = if (requested == 0) current.confirmed_lsn else requested, .initial_end_lsn = current.next_lsn, .frontier = initial, .end_lsn = current.next_lsn, .ceiling = @max(initial, if (requested == 0) current.confirmed_lsn else requested) };
    }
    fn check(self: *const Owner, policy: anytype, state: control.State, request: anytype) !u64 {
        if (self.closed or state.slot == null or state.slot.?.binding == null) return error.ChangefeedResynchronizationRequired;
        const bound = state.slot.?.binding.?;
        const now = try active(policy, state, bound, request);
        if (state.epoch != self.epoch or !same(state.authority, self.authority) or !same(state.definition.?.identity(), self.definition) or !same(bound.identity(), self.binding)) return error.ChangefeedResynchronizationRequired;
        try state.slot.?.verifySession(self.session, now);
        if (state.slot.?.next_lsn < self.end_lsn) return error.ChangefeedResynchronizationRequired;
        return now;
    }
    pub fn poll(self: *Owner, svc: anytype, registry: anytype, policy: anytype, a: A, after: u64, maximum: u32, request: anytype) !Page {
        var before = try load(svc, a, self.id, request);
        defer before.deinit();
        _ = try self.check(policy, before.value, request);
        if (after != self.frontier or maximum == 0 or maximum > spool.max_message_bytes) return error.PgSpoolCursorInvalid;
        const slot = before.value.slot.?;
        if (after < slot.confirmed_lsn or after > slot.next_lsn) return error.ChangefeedResynchronizationRequired;
        if (after == slot.next_lsn) {
            self.end_lsn = slot.next_lsn;
            return .{ .allocator = a, .end_lsn = self.end_lsn, .start_lsn = after };
        }
        const entry = for (slot.entries) |entry| {
            if (after >= entry.start_lsn and after < entry.end_lsn) break entry;
        } else return error.ChangefeedResynchronizationRequired;
        const record = slot.spool.find(entry.spool_id) orelse return error.PgSpoolCorrupt;
        if (record.phase != .live or record.spec.messages < 3 or std.mem.allEqual(u8, &record.spec.wire, 0)) return error.ChangefeedCapabilityUnsupported;
        const store = try registry.artifactStore(record.scope, request);
        if (store.namespace_identity == null or !same(store.namespace_identity.?, record.scope.namespace)) return error.PgSpoolStorageUnsupported;
        var cancellation: Cancellation(@TypeOf(request)) = .{ .request = request };
        var reader_store = try artifacts.Adapter.init(store, .{ .domain = record.scope.domain, .attempt = record.scope.attempt }, .read, cancellation.token());
        if (self.config.require_shared) try reader_store.requireShared();
        const reader = try spool.Reader.open(a, &reader_store, record.manifest, .{ .feed = record.spec.feed, .epoch = record.spec.epoch, .definition = record.spec.definition, .projection = record.spec.projection, .storage = record.spec.storage });
        if (!record.spec.matchesPlan(reader.seal.plan) or !same(reader.seal.serialization, record.content) or reader.seal.root.messages != record.spec.messages) return error.PgSpoolCorrupt;
        const bytes = (try reader.message(a, &reader_store, after - entry.start_lsn, maximum)) orelse return error.PgSpoolCorrupt;
        errdefer a.free(bytes);
        var current = try load(svc, a, self.id, request);
        defer current.deinit();
        _ = try self.check(policy, current.value, request);
        const retained = current.value.slot.?.spool.find(record.id) orelse return error.ChangefeedConflict;
        if (retained.phase != .live or !same(retained.manifest, record.manifest) or !same(retained.content, record.content)) return error.ChangefeedConflict;
        self.frontier = std.math.add(u64, after, bytes.len) catch return error.PgSpoolCorrupt;
        if (self.frontier > entry.end_lsn) return error.PgSpoolCorrupt;
        self.ceiling = @max(self.ceiling, self.frontier);
        self.end_lsn = current.value.slot.?.next_lsn;
        return .{ .allocator = a, .end_lsn = self.end_lsn, .start_lsn = after, .bytes = bytes };
    }
    pub fn feedback(self: *Owner, svc: anytype, policy: anytype, a: A, positions: Positions, request: anytype) !void {
        if (positions.written > self.ceiling or positions.flushed > self.ceiling or positions.applied > self.ceiling) return error.ChangefeedConflict;
        var rejected_revision: ?u64 = null;
        for (0..4) |_| {
            var before = try load(svc, a, self.id, request);
            defer before.deinit();
            const now = try self.check(policy, before.value, request);
            // Only a provable pre-admission conflict and a newer state warrant
            // rebuilding the feedback CAS. Unknown outcomes go to reconnect.
            if (rejected_revision) |revision| if (before.value.revision <= revision) return error.ChangefeedConflict;
            const slot = before.value.slot.?;
            const expires = @max(slot.session_expires_at, @min(before.value.expires_at, now +| self.config.lease_seconds));
            mutate(svc, a, before.value, .{ .feedback = .{ .session = self.session, .now = now, .expires = expires, .flushed = positions.flushed } }, request) catch |err| switch (err) {
                error.ChangefeedConflict => {
                    rejected_revision = before.value.revision;
                    continue;
                },
                else => return err,
            };
            var observed = try load(svc, a, self.id, request);
            defer observed.deinit();
            _ = try self.check(policy, observed.value, request);
            self.end_lsn = observed.value.slot.?.next_lsn;
            return;
        }
        return error.ChangefeedConflict;
    }
    /// Internal close context must be independent of a canceled client request.
    /// It releases only this connection's lease and never acknowledges history.
    /// Unknown release outcomes remain fenced until reconciliation/lease expiry.
    pub fn release(self: *Owner, svc: anytype, policy: anytype, a: A, request: anytype) !void {
        if (self.closed) return;
        defer self.closed = true;
        var current = try load(svc, a, self.id, request);
        defer current.deinit();
        if (current.value.epoch != self.epoch or !same(current.value.authority, self.authority)) return;
        const slot = current.value.slot orelse return;
        const session = slot.session orelse return;
        if (!std.meta.eql(session, self.session)) return;
        const now = try policy.nowSeconds(request);
        if (now < slot.observed_at or now >= slot.session_expires_at) return;
        if (current.value.phase != .streaming and current.value.phase != .bootstrap) return;
        try mutate(svc, a, current.value, .{ .release = .{ .session = self.session, .now = now } }, request);
    }
};
