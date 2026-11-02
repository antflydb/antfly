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

//! Permanent quiescence evidence for one exact control request. The expected
//! public key must be pinned by authoritative root identity/enrollment, never
//! taken from the response or refreshed from a health observation.
const std = @import("std");
const attempts = @import("remote_attempts.zig");
const wire = @import("remote_attempt_wire.zig");
const membership = @import("remote_membership.zig");
const worker = @import("remote_worker.zig");
const Ed25519 = std.crypto.sign.Ed25519;
pub const Authority = struct {
    local: membership.Local,
    public_key: [32]u8,
    pub fn fromSnapshot(snapshot: anytype, expected: membership.Local) !Authority {
        const key = try membership.replyPublicKey(snapshot, expected);
        _ = Ed25519.PublicKey.fromBytes(key) catch return error.InvalidRemoteMembership;
        return .{ .local = expected, .public_key = key };
    }
};
pub const Outcome = enum(u8) { unknown = 1, active = 2, terminal = 3, fenced = 4, fence_ack = 5, closed = 6 };
pub const Reconciled = enum { retained, retired, duplicate, fenced };
pub const encoded_size = 368;

fn binding(request: wire.Context) ![32]u8 {
    var normalized = request;
    // Late replies can reconcile after client expiry. Remaining duration is
    // not quiescence evidence and is the only field omitted from this binding.
    normalized.remaining_ns = 1;
    const encoded = try normalized.encode();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/remote-control-request/v1");
    hash.update(&encoded);
    return hash.finalResult();
}

pub const Reply = struct {
    local: membership.Local,
    request_binding: [32]u8,
    outcome: Outcome,
    generation: u64,
    signature: [64]u8,

    fn unsigned(self: Reply) [120]u8 {
        var bytes: [120]u8 = @splat(0);
        @memcpy(bytes[0..4], "AFAR");
        std.mem.writeInt(u16, bytes[4..6], 1, .little);
        bytes[6] = @backingInt(self.outcome);
        @memcpy(bytes[8..40], &self.local.metadata_incarnation);
        std.mem.writeInt(u64, bytes[40..48], self.local.node_id, .little);
        std.mem.writeInt(u64, bytes[48..56], self.local.store_id, .little);
        std.mem.writeInt(u128, bytes[56..72], self.local.root_incarnation, .little);
        std.mem.writeInt(u64, bytes[72..80], self.local.reporter_incarnation, .little);
        @memcpy(bytes[80..112], &self.request_binding);
        std.mem.writeInt(u64, bytes[112..120], self.generation, .little);
        return bytes;
    }
    fn digest(self: Reply) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/remote-attempt-reply/v1");
        hash.update(&self.unsigned());
        return hash.finalResult();
    }
    pub fn encode(self: Reply) [encoded_size]u8 {
        var bytes: [184]u8 = undefined;
        @memcpy(bytes[0..120], &self.unsigned());
        @memcpy(bytes[120..184], &self.signature);
        return std.fmt.bytesToHex(bytes, .lower);
    }
    pub fn decode(encoded: []const u8) !Reply {
        if (encoded.len != encoded_size) return error.InvalidRemoteReply;
        var bytes: [184]u8 = undefined;
        _ = std.fmt.hexToBytes(&bytes, encoded) catch return error.InvalidRemoteReply;
        if (!std.mem.eql(u8, bytes[0..4], "AFAR") or std.mem.readInt(u16, bytes[4..6], .little) != 1 or bytes[7] != 0) return error.UnsupportedRemoteReplyProtocol;
        return .{
            .local = .{
                .metadata_incarnation = bytes[8..40].*,
                .node_id = std.mem.readInt(u64, bytes[40..48], .little),
                .store_id = std.mem.readInt(u64, bytes[48..56], .little),
                .root_incarnation = std.mem.readInt(u128, bytes[56..72], .little),
                .reporter_incarnation = std.mem.readInt(u64, bytes[72..80], .little),
            },
            .request_binding = bytes[80..112].*,
            .outcome = std.enums.fromInt(Outcome, bytes[6]) orelse return error.UnsupportedRemoteReplyProtocol,
            .generation = std.mem.readInt(u64, bytes[112..120], .little),
            .signature = bytes[120..184].*,
        };
    }
    pub fn verify(self: Reply, authority: Authority, request: wire.Context) !void {
        _ = try membership.coordinatorIdentity(authority.local.metadata_incarnation, authority.local.node_id);
        if (authority.local.store_id == 0 or authority.local.root_incarnation == 0 or authority.local.reporter_incarnation == 0 or
            !std.meta.eql(authority.local, self.local) or request.worker_incarnation != authority.local.root_incarnation or
            !std.mem.eql(u8, &self.request_binding, &(try binding(request)))) return error.InvalidRemoteReply;
        switch (request.kind) {
            .read, .capabilities => return error.InvalidRemoteReply,
            .fence => if (self.outcome != .fence_ack or self.generation != request.id.generation) return error.InvalidRemoteReply,
            .status, .cancel => switch (self.outcome) {
                .fence_ack => return error.InvalidRemoteReply,
                .fenced => if (self.generation <= request.id.generation) return error.InvalidRemoteReply,
                .active, .terminal, .closed => if (self.generation < request.id.generation) return error.InvalidRemoteReply,
                .unknown => {},
            },
        }
        const verifier = Ed25519.PublicKey.fromBytes(authority.public_key) catch return error.InvalidRemoteReplySignature;
        Ed25519.Signature.fromBytes(self.signature).verify(&self.digest(), verifier) catch return error.InvalidRemoteReplySignature;
    }
    /// Verification is inseparable from the ledger mutation boundary.
    pub fn reconcile(self: Reply, authority: Authority, request: wire.Context, ledger: *attempts.Coordinator, destination: usize, store: *@import("remote_fence_store.zig").Store) !Reconciled {
        return self.reconcileWithWait(authority, request, ledger, destination, store, null);
    }
    pub fn reconcileWithWait(self: Reply, authority: Authority, request: wire.Context, ledger: *attempts.Coordinator, destination: usize, store: *@import("remote_fence_store.zig").Store, wait: ?@import("remote_fence_store.zig").Wait) !Reconciled {
        try self.verify(authority, request);
        try ledger.validateEvidenceDestination(destination, request.id, self.local.root_incarnation);
        try store.withWait(wait).observeCoordinatorGeneration(request.id.coordinator, self.generation);
        switch (self.outcome) {
            .unknown, .active => return .retained,
            .closed => {
                try ledger.observeClosedGeneration(request.id.coordinator, self.generation);
                return .retained;
            },
            .terminal => return if (try ledger.terminal(request.id, self.local.root_incarnation)) .retired else .duplicate,
            .fenced => return if (try ledger.reconcileFenced(destination, request.id, self.local.root_incarnation, self.generation)) .retired else .duplicate,
            .fence_ack => {
                try ledger.acknowledgeFence(destination, .{ .coordinator = request.id.coordinator, .generation = self.generation, .worker_incarnation = self.local.root_incarnation });
                return .fenced;
            },
        }
    }
};

pub const Signer = struct {
    local: membership.Local,
    pair: Ed25519.KeyPair,
    pub fn init(local: membership.Local, public_key: [32]u8, seed: [32]u8) !Signer {
        _ = try membership.coordinatorIdentity(local.metadata_incarnation, local.node_id);
        if (local.store_id == 0 or local.root_incarnation == 0 or local.reporter_incarnation == 0) return error.InvalidRemoteMembership;
        const pair = try Ed25519.KeyPair.generateDeterministic(seed);
        if (!std.mem.eql(u8, &pair.public_key.toBytes(), &public_key)) return error.InvalidRemoteReplySignature;
        return .{ .local = local, .pair = pair };
    }
    /// Reuse storage_db_root_signing_identity.State after linearizable enrollment.
    /// Loading or creating a different local key cannot authorize its replacement.
    pub fn initEnrolled(authority: Authority, signing: anytype) !Signer {
        if (signing.root_incarnation != authority.local.root_incarnation or !std.mem.eql(u8, &signing.public_key, &authority.public_key)) return error.InvalidRemoteMembership;
        return init(authority.local, authority.public_key, signing.seed);
    }
    fn validate(self: *const Signer, registry: *worker.Registry) !void {
        if (!std.meta.eql(self.local, registry.local)) return error.InvalidRemoteMembership;
    }
    fn finish(self: *const Signer, request: wire.Context, outcome: Outcome, generation: u64) !Reply {
        var reply = Reply{ .local = self.local, .request_binding = try binding(request), .outcome = outcome, .generation = generation, .signature = undefined };
        reply.signature = (try self.pair.sign(&reply.digest(), null)).toBytes();
        return reply;
    }
    pub fn status(self: *const Signer, registry: *worker.Registry, verified: wire.Verified, member: membership.Binding) !Reply {
        return self.statusWithWait(registry, verified, member, null);
    }
    pub fn statusWithWait(self: *const Signer, registry: *worker.Registry, verified: wire.Verified, member: membership.Binding, wait: ?@import("remote_fence_store.zig").Wait) !Reply {
        try self.validate(registry);
        const evidence = try registry.statusEvidenceWithWait(verified, member, wait);
        const outcome: Outcome = switch (evidence.status) {
            .unknown => .unknown,
            .active => .active,
            .terminal => .terminal,
            .fenced => .fenced,
            .closed => .closed,
        };
        return self.finish(verified.context, outcome, evidence.generation);
    }
    pub fn cancelWithWait(self: *const Signer, registry: *worker.Registry, verified: wire.Verified, member: membership.Binding, wait: ?@import("remote_fence_store.zig").Wait) !Reply {
        try self.validate(registry);
        const evidence = try registry.cancelWithWait(verified, member, wait);
        return self.finish(verified.context, switch (evidence.status) {
            .unknown => .unknown,
            .active => .active,
            .terminal => .terminal,
            .fenced => .fenced,
            .closed => .closed,
        }, evidence.generation);
    }
    pub fn fence(self: *const Signer, registry: *worker.Registry, verified: wire.Verified, member: membership.Binding) !Reply {
        return self.fenceWithWait(registry, verified, member, null);
    }
    pub fn fenceWithWait(self: *const Signer, registry: *worker.Registry, verified: wire.Verified, member: membership.Binding, wait: ?@import("remote_fence_store.zig").Wait) !Reply {
        try self.validate(registry);
        const ack = try registry.fenceWithWait(verified, member, wait);
        return self.finish(verified.context, .fence_ack, ack.generation);
    }
};

const Clock = struct {
    fn now() u64 {
        return 100;
    }
};
const test_local: membership.Local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 };
fn testMember() membership.Binding {
    return .{ .coordinator_node_id = 7, .worker_node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 };
}
fn testRequest(id: attempts.Id, kind: wire.Kind) wire.Verified {
    return .{ .context = .{ .kind = kind, .id = id, .worker_incarnation = 11, .remaining_ns = 1000, .charged_bytes = if (kind == .read) 4096 else 0, .request_digest = @splat(@backingInt(kind)) }, .local_deadline_ns = 1100 };
}
fn fenceRequest(coordinator: u128, generation: u64) wire.Verified {
    return testRequest(.{ .coordinator = coordinator, .generation = generation, .operation = 0, .sequence = 0 }, .fence);
}
const Fixture = struct {
    tmp: std.testing.TmpDir,
    store: @import("remote_fence_store.zig").Store,
    registry: worker.Registry,
    signer: Signer,
    authority: Authority,
    slots: [3]?attempts.Attempt,
    destinations: [2]attempts.Destination,
    ledger: attempts.Coordinator,
    fn init(self: *Fixture) !void {
        self.tmp = std.testing.tmpDir(.{});
        errdefer self.tmp.cleanup();
        self.store = try @import("remote_fence_store.zig").Store.create(std.testing.allocator, std.testing.io, self.tmp.dir, 11, 2);
        errdefer self.store.deinit();
        self.registry = try worker.Registry.init(std.testing.allocator, &self.store, test_local, .{ .coordinators = 2, .attempts = 2, .per_coordinator_attempts = 2, .duplicate_window = 8, .bytes = 16 << 10, .per_coordinator_bytes = 16 << 10, .resource_references = 4 }, Clock.now);
        errdefer self.registry.deinit() catch unreachable;
        const Recovery = struct {
            fn ready(_: *const anyopaque) bool {
                return true;
            }
        };
        try self.registry.completeRecovery(.{ .context = self, .quiescent = Recovery.ready });
        const seed: [32]u8 = @splat(7);
        const pair = try Ed25519.KeyPair.generateDeterministic(seed);
        self.authority = .{ .local = test_local, .public_key = pair.public_key.toBytes() };
        self.signer = try Signer.init(test_local, self.authority.public_key, seed);
        self.destinations = .{ .{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 8192 }, .{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 8192 } };
        self.ledger = try self.store.startCoordinator(try membership.coordinatorIdentity(test_local.metadata_incarnation, 7), 0, &self.slots, &self.destinations, 16384);
    }
    fn deinit(self: *Fixture) void {
        self.registry.deinit() catch unreachable;
        self.store.deinit();
        self.tmp.cleanup();
    }
    fn activate(self: *Fixture) !Reply {
        const probe = fenceRequest(self.ledger.coordinator, 1);
        const reply = try self.signer.fence(&self.registry, probe, testMember());
        _ = try reply.reconcile(self.authority, probe.context, &self.ledger, 0, &self.store);
        // The second fixture destination has no worker work at startup.
        try self.ledger.acknowledgeFence(1, .{ .coordinator = self.ledger.coordinator, .generation = 1, .worker_incarnation = 22 });
        return reply;
    }
    fn start(self: *Fixture) !worker.Lease {
        const id = try self.ledger.reserve(0, 71, 4096);
        try self.store.prepareDispatch(&self.ledger, id);
        self.ledger.transportLost(id);
        return (try self.registry.begin(testRequest(id, .read), testMember())).started;
    }
};

test "remote reply releases uncertainty only after signed physical terminal evidence" {
    var fixture: Fixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    _ = try fixture.activate();
    var lease = try fixture.start();
    defer lease.close();
    var held = try lease.retainResource();
    defer held.release();
    const bytes = try std.testing.allocator.dupe(u8, "retained output");
    var live = true;
    defer if (live) std.testing.allocator.free(bytes);
    lease.close();
    const probe = testRequest(lease.id, .status);
    const active = try fixture.signer.status(&fixture.registry, probe, testMember());
    try std.testing.expectEqual(Reconciled.retained, try active.reconcile(fixture.authority, probe.context, &fixture.ledger, 0, &fixture.store));
    try std.testing.expectEqual(@as(usize, 4096), fixture.ledger.snapshot().bytes);
    var unknown_probe = probe;
    unknown_probe.context.id.sequence = 999;
    const unknown = try fixture.signer.status(&fixture.registry, unknown_probe, testMember());
    try std.testing.expectEqual(Reconciled.retained, try unknown.reconcile(fixture.authority, unknown_probe.context, &fixture.ledger, 0, &fixture.store));
    std.testing.allocator.free(bytes);
    live = false;
    held.release();
    const terminal = try fixture.signer.status(&fixture.registry, probe, testMember());
    var late = probe.context;
    late.remaining_ns = 0;
    try std.testing.expectEqual(Reconciled.retired, try terminal.reconcile(fixture.authority, late, &fixture.ledger, 0, &fixture.store));
    try std.testing.expectEqual(Reconciled.duplicate, try terminal.reconcile(fixture.authority, late, &fixture.ledger, 0, &fixture.store));
    const replacement = try fixture.ledger.reserve(0, 72, 100);
    try fixture.store.prepareDispatch(&fixture.ledger, replacement);
    try std.testing.expectEqual(Reconciled.duplicate, try terminal.reconcile(fixture.authority, late, &fixture.ledger, 0, &fixture.store));
    try std.testing.expectEqual(@as(usize, 100), fixture.ledger.snapshot().bytes);
}

test "remote reply codec rejects forged identities requests signatures and versions" {
    var fixture: Fixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    _ = try fixture.activate();
    var lease = try fixture.start();
    defer lease.close();
    const probe = testRequest(lease.id, .status);
    const original = try fixture.signer.status(&fixture.registry, probe, testMember());
    const encoded = original.encode();
    const decoded = try Reply.decode(&encoded);
    try decoded.verify(fixture.authority, probe.context);
    var forged = original;
    forged.outcome = .terminal;
    try std.testing.expectError(error.InvalidRemoteReplySignature, forged.reconcile(fixture.authority, probe.context, &fixture.ledger, 0, &fixture.store));
    forged = original;
    forged.signature[0] ^= 1;
    try std.testing.expectError(error.InvalidRemoteReplySignature, forged.verify(fixture.authority, probe.context));
    forged = original;
    forged.local.reporter_incarnation += 1;
    try std.testing.expectError(error.InvalidRemoteReply, forged.verify(fixture.authority, probe.context));
    var different = probe.context;
    different.request_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidRemoteReply, original.verify(fixture.authority, different));
    different = probe.context;
    different.id.operation += 1;
    try std.testing.expectError(error.InvalidRemoteReply, original.verify(fixture.authority, different));
    var authority = fixture.authority;
    authority.public_key = (try Ed25519.KeyPair.generateDeterministic(@splat(8))).public_key.toBytes();
    try std.testing.expectError(error.InvalidRemoteReplySignature, original.verify(authority, probe.context));
    var version = encoded;
    version[8] = '2';
    try std.testing.expectError(error.UnsupportedRemoteReplyProtocol, Reply.decode(&version));
    try std.testing.expectError(error.InvalidRemoteReply, Reply.decode("untrusted body"));
    try std.testing.expectEqual(@as(usize, 4096), fixture.ledger.snapshot().bytes);
}

test "remote reply cannot acknowledge pending fence or apply old fence to a new generation" {
    var fixture: Fixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    const old_ack = try fixture.activate();
    var lease = try fixture.start();
    defer lease.close();
    var held = try lease.retainResource();
    defer held.release();
    lease.close();
    try fixture.ledger.beginGeneration(2);
    const probe = fenceRequest(fixture.ledger.coordinator, 2);
    try std.testing.expectError(error.RemoteFencePending, fixture.signer.fence(&fixture.registry, probe, testMember()));
    try std.testing.expectEqual(@as(usize, 4096), fixture.ledger.snapshot().bytes);
    held.release();
    const ack = try fixture.signer.fence(&fixture.registry, probe, testMember());
    try std.testing.expectEqual(Reconciled.fenced, try ack.reconcile(fixture.authority, probe.context, &fixture.ledger, 0, &fixture.store));
    const next = try fixture.ledger.reserve(0, 72, 100);
    try fixture.store.prepareDispatch(&fixture.ledger, next);
    try std.testing.expectError(error.InvalidRemoteFence, old_ack.reconcile(fixture.authority, fenceRequest(fixture.ledger.coordinator, 1).context, &fixture.ledger, 0, &fixture.store));
    try std.testing.expectEqual(@as(usize, 100), fixture.ledger.snapshot().bytes);
}

test "remote reply higher fence stops stale sends without dropping other destination uncertainty" {
    var fixture: Fixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    _ = try fixture.activate();
    var lease = try fixture.start();
    defer lease.close();
    const other = try fixture.ledger.reserve(1, 72, 2048);
    try fixture.store.prepareDispatch(&fixture.ledger, other);
    fixture.ledger.transportLost(other);
    const unpublished = try fixture.ledger.reserve(0, 74, 64);
    lease.close();
    _ = try fixture.signer.fence(&fixture.registry, fenceRequest(fixture.ledger.coordinator, 2), testMember());
    const probe = testRequest(lease.id, .status);
    const fenced = try fixture.signer.status(&fixture.registry, probe, testMember());
    // Even a genuine signed reply cannot mutate another destination's ledger.
    try std.testing.expectError(error.InvalidRemoteFence, fenced.reconcile(fixture.authority, probe.context, &fixture.ledger, 1, &fixture.store));
    try std.testing.expect(!fixture.ledger.snapshot().generation_fenced);
    try std.testing.expectEqual(@as(usize, 6208), fixture.ledger.snapshot().bytes);
    try std.testing.expectEqual(Reconciled.retired, try fenced.reconcile(fixture.authority, probe.context, &fixture.ledger, 0, &fixture.store));
    try std.testing.expect(fixture.ledger.snapshot().generation_fenced);
    try std.testing.expectError(error.RemoteGenerationFenced, fixture.ledger.dispatch(unpublished));
    try fixture.ledger.abortUnpublished(unpublished);
    try std.testing.expectEqual(@as(usize, 2048), fixture.ledger.snapshot().bytes);
    try std.testing.expectError(error.RemoteGenerationFenced, fixture.ledger.reserve(0, 73, 1));
    try std.testing.expectError(error.InvalidRemoteGeneration, fixture.ledger.beginGeneration(2));
    try std.testing.expectEqual(@as(u64, 2), fixture.ledger.snapshot().observed_generation);
    try fixture.ledger.beginGeneration(3);
    try std.testing.expectEqual(Reconciled.duplicate, try fenced.reconcile(fixture.authority, probe.context, &fixture.ledger, 0, &fixture.store));
    try std.testing.expect(!fixture.ledger.snapshot().generation_fenced);
    try std.testing.expectEqual(@as(usize, 2048), fixture.ledger.snapshot().bytes);
}

test "remote reply signer rejects wrong key before durable mutation" {
    var fixture: Fixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    try std.testing.expectError(error.InvalidRemoteReplySignature, Signer.init(test_local, fixture.authority.public_key, @splat(8)));
    var wrong = fixture.signer;
    wrong.local.reporter_incarnation += 1;
    try std.testing.expectError(error.InvalidRemoteMembership, wrong.fence(&fixture.registry, fenceRequest(fixture.ledger.coordinator, 1), testMember()));
    try std.testing.expectEqual(@as(u64, 0), try fixture.store.floor(fixture.ledger.coordinator));
}

test "remote reply observed generation survives reopen and failed persistence retains uncertainty" {
    const Fault = struct {
        var original: *const fn (?*anyopaque, std.Io.File) std.Io.File.SyncError!void = undefined;
        var calls: usize = 0;
        var fail_at: usize = 0;
        fn sync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
            calls += 1;
            if (calls == fail_at) return error.InputOutput;
            return original(userdata, file);
        }
    };
    const durable = @import("remote_fence_store.zig");
    const io = std.testing.io;
    for ([_]usize{ 1, 2 }) |fail_at| {
        var fixture: Fixture = undefined;
        try fixture.init();
        defer fixture.deinit();
        _ = try fixture.activate();
        var lease = try fixture.start();
        defer lease.close();
        lease.close();
        _ = try fixture.signer.fence(&fixture.registry, fenceRequest(fixture.ledger.coordinator, 7), testMember());
        const probe = testRequest(lease.id, .status);
        const reply = try fixture.signer.status(&fixture.registry, probe, testMember());
        var vtable = io.vtable.*;
        Fault.original = vtable.fileSync;
        Fault.calls = 0;
        Fault.fail_at = fail_at;
        vtable.fileSync = Fault.sync;
        fixture.store.io = .{ .userdata = io.userdata, .vtable = &vtable };
        try std.testing.expectError(error.InputOutput, reply.reconcile(fixture.authority, probe.context, &fixture.ledger, 0, &fixture.store));
        try std.testing.expectEqual(@as(usize, 4096), fixture.ledger.snapshot().bytes);
        try std.testing.expect(!fixture.ledger.snapshot().generation_fenced);
        try std.testing.expectError(error.RemoteFenceStoreUncertain, reply.reconcile(fixture.authority, probe.context, &fixture.ledger, 0, &fixture.store));
        fixture.store.deinit();
        fixture.store = try durable.Store.open(std.testing.allocator, io, fixture.tmp.dir, 11, 2);
        try std.testing.expectEqual(@as(u64, if (fail_at == 1) 1 else 7), fixture.store.coordinator_generation);
        try std.testing.expectEqual(Reconciled.retired, try reply.reconcile(fixture.authority, probe.context, &fixture.ledger, 0, &fixture.store));
        fixture.store.deinit();
        fixture.store = try durable.Store.open(std.testing.allocator, io, fixture.tmp.dir, 11, 2);
        fixture.ledger = try fixture.store.startCoordinator(fixture.ledger.coordinator, 0, &fixture.slots, &fixture.destinations, 16384);
        try std.testing.expectEqual(@as(u64, 8), fixture.ledger.generation);
        try std.testing.expectEqual(@as(usize, 2), fixture.ledger.snapshot().startup_fences_remaining);
        try std.testing.expectError(error.RemoteGenerationNotReady, fixture.ledger.reserve(0, 72, 1));
    }
}

test "remote reply signer binds existing physical key to authoritative enrollment" {
    const seed: [32]u8 = @splat(7);
    const pair = try Ed25519.KeyPair.generateDeterministic(seed);
    const authority: Authority = .{ .local = test_local, .public_key = pair.public_key.toBytes() };
    var signing: struct { root_incarnation: u128, public_key: [32]u8, seed: [32]u8 } = .{ .root_incarnation = test_local.root_incarnation, .public_key = pair.public_key.toBytes(), .seed = seed };
    _ = try Signer.initEnrolled(authority, signing);
    signing.root_incarnation += 1;
    try std.testing.expectError(error.InvalidRemoteMembership, Signer.initEnrolled(authority, signing));
    signing.root_incarnation -= 1;
    signing.public_key = (try Ed25519.KeyPair.generateDeterministic(@splat(8))).public_key.toBytes();
    try std.testing.expectError(error.InvalidRemoteMembership, Signer.initEnrolled(authority, signing));
}

test "remote reply closed equal epoch retains uncertainty and requires a fresh generation" {
    var fixture: Fixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    _ = try fixture.activate();
    var lease = try fixture.start();
    defer lease.close();
    lease.close();
    fixture.registry.deinit() catch unreachable;
    fixture.registry = try worker.Registry.init(std.testing.allocator, &fixture.store, test_local, .{ .coordinators = 2, .attempts = 2, .per_coordinator_attempts = 2, .duplicate_window = 8, .bytes = 16 << 10, .per_coordinator_bytes = 16 << 10, .resource_references = 4 }, Clock.now);
    const cancellation = testRequest(lease.id, .cancel);
    try std.testing.expectError(error.RemoteWorkerRecoveryPending, fixture.signer.cancelWithWait(&fixture.registry, cancellation, testMember(), null));
    const probe = testRequest(lease.id, .status);
    const closed = try fixture.signer.status(&fixture.registry, probe, testMember());
    try std.testing.expectEqual(Outcome.closed, closed.outcome);
    try std.testing.expectEqual(Reconciled.retained, try closed.reconcile(fixture.authority, probe.context, &fixture.ledger, 0, &fixture.store));
    try std.testing.expectEqual(@as(usize, 4096), fixture.ledger.snapshot().bytes);
    try std.testing.expect(fixture.ledger.snapshot().generation_fenced);
    try std.testing.expectError(error.RemoteGenerationFenced, fixture.ledger.reserve(0, 72, 1));
    const Recovery = struct {
        fn ready(_: *const anyopaque) bool {
            return true;
        }
    };
    try fixture.registry.completeRecovery(.{ .context = &fixture, .quiescent = Recovery.ready });
    const canceled_closed = try fixture.signer.cancelWithWait(&fixture.registry, cancellation, testMember(), null);
    try std.testing.expectEqual(Outcome.closed, canceled_closed.outcome);
    try std.testing.expectEqual(Reconciled.retained, try canceled_closed.reconcile(fixture.authority, cancellation.context, &fixture.ledger, 0, &fixture.store));
    try std.testing.expectEqual(@as(usize, 4096), fixture.ledger.snapshot().bytes);
    try std.testing.expectError(error.RemoteGenerationFenced, fixture.registry.begin(testRequest(lease.id, .read), testMember()));
    try fixture.store.advanceCoordinator(&fixture.ledger);
    const fence_probe = fenceRequest(fixture.ledger.coordinator, 2);
    const ack = try fixture.signer.fence(&fixture.registry, fence_probe, testMember());
    try std.testing.expectEqual(Reconciled.fenced, try ack.reconcile(fixture.authority, fence_probe.context, &fixture.ledger, 0, &fixture.store));
    try std.testing.expectEqual(@as(usize, 0), fixture.ledger.snapshot().bytes);
    _ = try closed.reconcile(fixture.authority, probe.context, &fixture.ledger, 0, &fixture.store);
    try std.testing.expect(!fixture.ledger.snapshot().generation_fenced);
}

test "remote reply cancellation retains physical charges and suppresses dispatch arriving later" {
    var fixture: Fixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    _ = try fixture.activate();
    var lease = try fixture.start();
    defer lease.close();
    var resource = try lease.retainResource();
    defer resource.release();
    const signal = try lease.cancellationSignal();
    try std.testing.expect(!signal.load(.acquire));
    try std.testing.expect(try fixture.ledger.abandon(lease.id));
    try std.testing.expectEqual(@as(usize, 4096), fixture.ledger.snapshot().bytes);
    const request = testRequest(lease.id, .cancel);
    var wrong = testMember();
    wrong.coordinator_node_id += 1;
    try std.testing.expectError(error.InvalidRemoteMembership, fixture.signer.cancelWithWait(&fixture.registry, request, wrong, null));
    try std.testing.expect(!signal.load(.acquire));
    const active = try fixture.signer.cancelWithWait(&fixture.registry, request, testMember(), null);
    try std.testing.expectEqual(Outcome.active, active.outcome);
    try std.testing.expect(signal.load(.acquire));
    try std.testing.expectError(error.Canceled, lease.check());
    try std.testing.expectError(error.Canceled, lease.retainResource());
    // A cancellation reply is bound to cancellation, never interchangeable
    // with an ordinary status credential or a different exact attempt.
    try std.testing.expectError(error.InvalidRemoteReply, active.verify(fixture.authority, testRequest(lease.id, .status).context));
    try std.testing.expectEqual(Reconciled.retained, try active.reconcile(fixture.authority, request.context, &fixture.ledger, 0, &fixture.store));
    lease.close();
    const pending = try fixture.signer.cancelWithWait(&fixture.registry, request, testMember(), null);
    try std.testing.expectEqual(Outcome.active, pending.outcome);
    try std.testing.expectEqual(@as(usize, 4096), fixture.registry.snapshot().bytes);
    resource.release();
    const terminal = try fixture.signer.cancelWithWait(&fixture.registry, request, testMember(), null);
    try std.testing.expectEqual(Outcome.terminal, terminal.outcome);
    try std.testing.expectEqual(Reconciled.retired, try terminal.reconcile(fixture.authority, request.context, &fixture.ledger, 0, &fixture.store));
    // The read was published but never admitted. Cancellation must install a
    // tombstone before returning terminal evidence, without allocating a lease.
    const delayed = try fixture.ledger.reserve(0, 72, 1024);
    try fixture.store.prepareDispatch(&fixture.ledger, delayed);
    fixture.ledger.transportLost(delayed);
    const unknown = try fixture.signer.status(&fixture.registry, testRequest(delayed, .status), testMember());
    try std.testing.expectEqual(Outcome.unknown, unknown.outcome);
    try std.testing.expect(try fixture.ledger.abandon(delayed));
    const canceled = try fixture.signer.cancelWithWait(&fixture.registry, testRequest(delayed, .cancel), testMember(), null);
    try std.testing.expectEqual(Outcome.terminal, canceled.outcome);
    try std.testing.expectEqual(Reconciled.retired, try canceled.reconcile(fixture.authority, testRequest(delayed, .cancel).context, &fixture.ledger, 0, &fixture.store));
    try std.testing.expect((try fixture.registry.begin(testRequest(delayed, .read), testMember())) == .terminal);
    try std.testing.expectEqual(@as(usize, 0), fixture.registry.snapshot().attempts);
    try std.testing.expect(!try fixture.ledger.abandon(delayed));
}
