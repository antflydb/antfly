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

//! Upload orchestration over observed slot authority. A producer supplies a
//! certified historical transaction, deterministic preflight wire digest, and a
//! bounded message iterator. It must never reconstruct from current rows. The
//! storage registry resolves/protects the original namespace; it cannot choose a
//! replacement scope on an uncertain retry. No native production factory is
//! registered by this module.
const std = @import("std");
const control = @import("changefeed_control.zig");
const ledger = @import("pg_spool_control.zig");
const spool = @import("pg_spool.zig");
const artifacts = @import("pg_spool_artifacts.zig");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
pub const WireHasher = struct {
    sha: std.crypto.hash.sha2.Sha256,
    pub fn init() WireHasher {
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:pg-spool-wire:v1");
        return .{ .sha = sha };
    }
    pub fn add(self: *WireHasher, message: []const u8) !void {
        if (message.len == 0 or message.len > spool.max_message_bytes) return error.InvalidPgSpool;
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, @intCast(message.len), .big);
        self.sha.update(&bytes);
        self.sha.update(message);
    }
    pub fn final(self: *WireHasher) control.Digest {
        return self.sha.finalResult();
    }
};
pub const Config = struct { require_shared: bool = true };
pub const Candidate = struct {
    delivery: control.Delivery,
    projection: control.Digest,
    completion: control.Digest,
    wire: control.Digest,
    messages: u32,
    visible_events: u32,
    bytes: u64,
    xid: u32,
    commit_time: i64,
    pub fn proof(self: Candidate, state: control.State) control.Digest {
        return control.hash(.{ .domain = "antfly:pg-spool-source-plan:v1", .feed = state.id, .epoch = state.epoch, .definition = state.definition.?.identity(), .before = self.delivery.before, .advances = self.delivery.advances, .context = self.delivery.provider_context, .start_lsn = state.slot.?.next_lsn, .projection = self.projection, .completion = self.completion, .wire = self.wire, .messages = self.messages, .visible_events = self.visible_events, .bytes = self.bytes, .xid = self.xid, .commit_time = self.commit_time });
    }
    fn spec(self: Candidate, state: control.State, scope: ledger.Scope) !ledger.Spec {
        if (self.visible_events == 0 or self.visible_events > 65536 or self.messages < 3 or std.mem.allEqual(u8, &self.wire, 0) or self.bytes < 47) return error.InvalidPgSpool;
        if (!same(self.delivery.before, state.slot.?.mapped_cursor) or !same(self.delivery.authority, self.proof(state)) or !same(self.delivery.cursor, self.delivery.identity(state.id, state.epoch))) return error.ChangefeedConflict;
        const value: ledger.Spec = .{ .feed = state.id, .epoch = state.epoch, .definition = state.definition.?.identity(), .projection = self.projection, .storage = scope.storageIdentity(), .before = self.delivery.before, .after = self.delivery.cursor, .completion = self.completion, .start_lsn = state.slot.?.next_lsn, .bytes = self.bytes, .commit_offset = self.bytes - 26, .xid = self.xid, .commit_time = self.commit_time, .wire = self.wire, .messages = self.messages };
        try value.validate();
        return value;
    }
};
pub const Result = struct {
    status: enum { idle, staged, reserved, abandoned, published },
    reservation: u64 = 0,
    manifest: control.Digest = @splat(0),
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
fn active(producer: anytype, state: control.State, session: control.pg_slot.Session, request: anytype) !u64 {
    try request.ensureActive();
    try producer.authorize(state, request);
    const now = try producer.nowSeconds(request);
    if (state.phase != .streaming or now >= state.expires_at) return error.ChangefeedResynchronizationRequired;
    const slot = state.slot orelse return error.ChangefeedCapabilityUnsupported;
    if (slot.binding == null) return error.ChangefeedCapabilityUnsupported;
    try slot.verifySession(session, now);
    return now;
}
fn mutate(svc: anytype, a: A, state: control.State, mutation: control.Mutation, request: anytype) !void {
    const definition = state.definition.?;
    return control.mutateOnService(svc, a, .{ .id = state.id, .authority = state.authority, .owner = definition.owner, .authorization = definition.authorization, .expected_revision = state.revision, .expected_epoch = state.epoch, .mutation = mutation }, request);
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
fn plan(spec: ledger.Spec) spool.Plan {
    return .{ .binding = .{ .feed = spec.feed, .epoch = spec.epoch, .definition = spec.definition, .projection = spec.projection, .storage = spec.storage }, .before = spec.before, .after = spec.after, .completion = spec.completion, .start_lsn = spec.start_lsn, .bytes = spec.bytes, .commit_offset = spec.commit_offset, .xid = spec.xid, .commit_time = spec.commit_time };
}
fn writerFor(registry: anytype, scope: ledger.Scope, config: Config, token: CancellationToken, request: anytype) !artifacts.Adapter {
    const store = try registry.artifactStore(scope, request);
    if (store.namespace_identity == null or !same(store.namespace_identity.?, scope.namespace)) return error.PgSpoolStorageUnsupported;
    var writer = try artifacts.Adapter.init(store, .{ .domain = scope.domain, .attempt = scope.attempt }, .write, token);
    if (config.require_shared) try writer.requireShared();
    return writer;
}
const Lease = struct { observed: u64, expires: u64, feed_expires: u64 };
fn checkBuild(svc: anytype, producer: anytype, a: A, state: control.State, record: ledger.Record, token: control.Id, session: control.pg_slot.Session, lease: *Lease, request: anytype) !void {
    try request.ensureActive();
    try producer.authorize(state, request);
    var now = try producer.nowSeconds(request);
    // Long builds can outlive the original session window while feedback
    // renews the same owner. Refresh only near expiry; retain the original
    // immutable definition for the iterator's borrowed historical schema.
    if (now >= lease.expires -| 5 or now >= lease.feed_expires -| 5) {
        var current = try load(svc, a, state.id, request);
        defer current.deinit();
        now = try active(producer, current.value, session, request);
        if (current.value.epoch != state.epoch or !same(current.value.authority, state.authority) or !same(current.value.definition.?.identity(), state.definition.?.identity())) return error.ChangefeedResynchronizationRequired;
        const found = current.value.slot.?.spool.find(record.id) orelse return error.ChangefeedConflict;
        if (found.phase != .uploading or found.build == null or !same(found.build.?, token) or !std.meta.eql(found.spec, record.spec) or !std.meta.eql(found.scope, record.scope)) return error.ChangefeedConflict;
        lease.* = .{ .observed = current.value.slot.?.observed_at, .expires = current.value.slot.?.session_expires_at, .feed_expires = current.value.expires_at };
    }
    if (now < lease.observed or now >= lease.expires or now >= lease.feed_expires) return error.ChangefeedLeaseExpired;
}
/// Advance one publication stage. The complete transaction is streamed through
/// one bounded message at a time, never accumulated in a transaction arena.
/// Unknown metadata responses stop the stage; retries observe original state.
pub fn drive(svc: anytype, registry: anytype, producer: anytype, a: A, id: control.Id, session: control.pg_slot.Session, config: Config, request: anytype) !Result {
    var before = try load(svc, a, id, request);
    defer before.deinit();
    const state = before.value;
    _ = try active(producer, state, session, request);
    var cancellation: Cancellation(@TypeOf(request)) = .{ .request = request };
    const pending = for (state.slot.?.spool.records) |record| {
        if (record.phase == .uploading) break record;
    } else null;
    if (pending) |record| {
        // Recover this exact plan before asking the provider to prepare new work.
        if (record.spec.messages < 3 or std.mem.allEqual(u8, &record.spec.wire, 0)) return error.ChangefeedCapabilityUnsupported;
        if (record.build != null) {
            const now = try active(producer, state, session, request);
            try mutate(svc, a, state, .{ .slot = .{ .abandon_spool = .{ .session = session, .now = now, .id = record.id } } }, request);
            return .{ .status = .abandoned, .reservation = record.id };
        }
        try producer.verify(state, record.spec, request);
        var writer = try writerFor(registry, record.scope, config, cancellation.token(), request);
        const token: control.Id = try registry.newBuildToken(record, request);
        const start = try active(producer, state, session, request);
        try mutate(svc, a, state, .{ .slot = .{ .begin_spool = .{ .session = session, .now = start, .id = record.id, .token = token } } }, request);
        var granted = try load(svc, a, id, request);
        defer granted.deinit();
        _ = try active(producer, granted.value, session, request);
        const grant = granted.value.slot.?.spool.find(record.id) orelse return error.ChangefeedConflict;
        if (grant.phase != .uploading or grant.build == null or !same(grant.build.?, token) or !std.meta.eql(grant.spec, record.spec) or !std.meta.eql(grant.scope, record.scope)) return error.ChangefeedConflict;
        var iterator = try producer.open(a, granted.value, record.spec, request);
        defer iterator.deinit();
        var lease: Lease = .{ .observed = granted.value.slot.?.observed_at, .expires = granted.value.slot.?.session_expires_at, .feed_expires = granted.value.expires_at };
        var builder = try spool.Builder.init(plan(record.spec));
        var sha = WireHasher.init();
        var count: u32 = 0;
        while (true) {
            try checkBuild(svc, producer, a, granted.value, grant, token, session, &lease, request);
            const message = (try iterator.next(a, spool.max_message_bytes, request)) orelse break;
            defer a.free(message);
            try checkBuild(svc, producer, a, granted.value, grant, token, session, &lease, request);
            if (count == record.spec.messages) return error.PgSpoolWireMismatch;
            count += 1;
            try sha.add(message);
            try builder.append(&writer, message);
        }
        if (count != record.spec.messages or !same(sha.final(), record.spec.wire)) return error.PgSpoolWireMismatch;
        const sealed = try builder.finish(&writer);
        const verified = try spool.Reader.open(a, &writer, sealed.identity, sealed.plan.binding);
        if (!record.spec.matchesPlan(verified.seal.plan) or !same(verified.seal.serialization, sealed.serialization)) return error.PgSpoolCorrupt;
        try producer.verify(granted.value, record.spec, request);
        try registry.protect(granted.value, grant, sealed, &writer, request);
        var current = try load(svc, a, id, request);
        defer current.deinit();
        const now = try active(producer, current.value, session, request);
        if (current.value.epoch != state.epoch or !same(current.value.authority, state.authority) or !same(current.value.definition.?.identity(), state.definition.?.identity())) return error.ChangefeedResynchronizationRequired;
        const observed = current.value.slot.?.spool.find(record.id) orelse return error.ChangefeedConflict;
        if (observed.phase != .uploading or observed.build == null or !same(observed.build.?, token) or !std.meta.eql(observed.scope, record.scope) or !std.meta.eql(observed.spec, record.spec)) return error.ChangefeedConflict;
        try mutate(svc, a, current.value, .{ .slot = .{ .publish_spool = .{ .session = session, .now = now, .id = record.id, .manifest = sealed.identity, .content = sealed.serialization } } }, request);
        return .{ .status = .published, .reservation = record.id, .manifest = sealed.identity };
    }
    var scratch_arena = std.heap.ArenaAllocator.init(a);
    defer scratch_arena.deinit();
    const candidate: Candidate = (try producer.prepare(scratch_arena.allocator(), state, request)) orelse return .{ .status = .idle };
    // Validation uses a dummy storage identity only; no namespace is allocated
    // until the historical receipt has been observed in durable metadata.
    var fake_scope: ledger.Scope = .{ .namespace = @splat(1), .domain = ledger.Scope.domainFor(state.id, state.epoch), .attempt = @splat(1) };
    std.mem.writeInt(u64, fake_scope.attempt[0..8], state.slot.?.spool.next_id, .big);
    const draft = try candidate.spec(state, fake_scope);
    try producer.verify(state, draft, request);
    _ = try active(producer, state, session, request);
    const staged = for (state.cursors) |receipt| {
        if (same(receipt.cursor, candidate.delivery.cursor)) break receipt;
    } else null;
    if (staged) |receipt| {
        if (!same(control.hash(receipt), control.hash(candidate.delivery))) return error.ChangefeedConflict;
    } else {
        if (!same(state.latest_cursor, state.slot.?.mapped_cursor)) return error.ChangefeedConflict;
        try mutate(svc, a, state, .{ .issue = candidate.delivery }, request);
        return .{ .status = .staged };
    }
    const scope: ledger.Scope = try registry.newUploadScope(state, state.slot.?.spool.next_id, request);
    try scope.validate();
    const spec = try candidate.spec(state, scope);
    // Check storage capabilities before admitting an otherwise unreclaimable
    // reservation. Resolving a namespace must not upload any objects.
    _ = try writerFor(registry, scope, config, cancellation.token(), request);
    const now = try active(producer, state, session, request);
    try mutate(svc, a, state, .{ .slot = .{ .reserve_spool = .{ .session = session, .now = now, .id = state.slot.?.spool.next_id, .scope = scope, .spec = spec } } }, request);
    return .{ .status = .reserved, .reservation = state.slot.?.spool.next_id };
}
