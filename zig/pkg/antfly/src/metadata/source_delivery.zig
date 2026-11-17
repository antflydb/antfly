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

//! Provider-neutral delivery orchestration. The metadata reservation precedes
//! destination admission; destination receipts precede metadata acceptance;
//! coordinator release follows observed durable metadata acceptance.
//!
//! Adapters must resolve the original manifest before consulting current routes.
//! null from resolve means proven absence, never an unknown outcome. Receipt
//! parsing and release are private destination operations, not caller proofs.
const std = @import("std");
const control = @import("source_control.zig");
const A = std.mem.Allocator;
pub const Input = struct {
    batch_id: []const u8,
    checkpoint: []const u8,
    expected_checkpoint: ?[]const u8 = null,
    payload_digest: control.Digest,
    accepted_at_ns: u64,
};
pub const Plan = struct {
    digest: control.Digest,
    transaction_id: [16]u8,
    manifest: []const u8,
    artifact: ?control.artifact_control.Spec = null,
    checkpoint_authority: ?control.checkpoint_authority.Spec = null,
};
pub const PlanOptions = struct { source_sequence: u64, upload_token: u64, source_purpose: control.Digest, checkpoint_authority: ?control.checkpoint_authority.Spec = null };
fn AdapterType(comptime T: type) type {
    return if (@typeInfo(T) == .pointer) @typeInfo(T).pointer.child else T;
}
fn supportsCheckpoint(adapter: anytype) bool {
    if (comptime @hasDecl(AdapterType(@TypeOf(adapter)), "checkpoint_capable")) return AdapterType(@TypeOf(adapter)).checkpoint_capable;
    return false;
}
fn supportsScopeFencing(adapter: anytype) bool {
    if (comptime @hasDecl(AdapterType(@TypeOf(adapter)), "scope_fencing_capable")) return AdapterType(@TypeOf(adapter)).scope_fencing_capable;
    return false;
}
fn requireArtifactAdapter(adapter: anytype, state: control.State) !void {
    if (state.scope_fencing and !supportsScopeFencing(adapter)) return error.UnsupportedOperation;
    if (state.checkpoint_authority != null and !supportsCheckpoint(adapter)) return error.UnsupportedOperation;
    if (state.artifact_control != null and !@hasDecl(AdapterType(@TypeOf(adapter)), "sealArtifact")) return error.UnsupportedOperation;
}
fn options(state: control.State, sequence: u64, purpose: control.Digest) !PlanOptions {
    return .{ .source_sequence = sequence, .source_purpose = purpose, .checkpoint_authority = state.checkpoint_authority, .upload_token = std.math.add(u64, if (state.artifact_control) |ledger| ledger.next_token else 0, 1) catch return error.SourceReceiptExpired };
}
pub const Replan = struct {
    plan: Plan,
    not_admitted_receipt: []const u8,
};
pub const Acceptance = struct {
    reservation_digest: control.Digest,
    receipt: []const u8,
    commit_reference: ?[]const u8 = null,
};
pub const ReceiptIdentity = struct {
    batch_id: []const u8,
    checkpoint: []const u8,
    expected_checkpoint: ?[]const u8,
    payload_digest: control.Digest,
};

fn load(svc: anytype, a: A, scope: control.Scope, request: anytype) !std.json.Parsed(control.State) {
    const bytes = try control.readOnService(svc, a, scope.table_id, scope.source_id, request);
    defer a.free(bytes);
    var state = try control.parse(a, bytes);
    errdefer state.deinit();
    if (state.value.scope) |bound| {
        if (!std.mem.eql(u8, &bound.identity(), &scope.identity()) or state.value.retired) return error.SourceBindingConflict;
    }
    return state;
}
fn mutate(svc: anytype, a: A, scope: control.Scope, catalog: control.Digest, state: control.State, mutation: control.Mutation, request: anytype) !void {
    return control.mutateOnService(svc, a, .{
        .table_id = scope.table_id,
        .source_id = scope.source_id,
        .expected_revision = state.revision,
        .expected_binding = scope.identity(),
        .expected_catalog_digest = catalog,
        .mutation = mutation,
    }, request);
}
fn accept(svc: anytype, a: A, scope: control.Scope, catalog: control.Digest, state: control.State, result: Acceptance, request: anytype) !void {
    const pending = state.pending orelse return error.SourceOutcomeUnresolved;
    if (!std.mem.eql(u8, &pending.identity(scope), &result.reservation_digest)) return error.SourcePayloadConflict;
    try mutate(svc, a, scope, catalog, state, .{ .accept = .{
        .reservation_digest = result.reservation_digest,
        .receipt = result.receipt,
        .commit_reference = result.commit_reference,
    } }, request);
}

fn refreshAfterRelease(svc: anytype, a: A, scope: control.Scope, state: *std.json.Parsed(control.State), request: anytype) !void {
    if (state.value.artifact_control == null) return;
    const accepted = state.value.accepted.reservation_digest;
    var next = try load(svc, a, scope, request);
    errdefer next.deinit();
    if (!std.mem.eql(u8, &accepted, &next.value.accepted.reservation_digest)) return error.SourceControlConflict;
    state.deinit();
    state.* = next;
}

/// Binding changes metadata revision, so confirm/reload before reserving the
/// already frozen plan. Lost binding replies never authorize upload or BEGIN.
fn bindCheckpoint(svc: anytype, adapter: anytype, a: A, scope: control.Scope, catalog: control.Digest, state: *std.json.Parsed(control.State), plan: Plan, request: anytype) !void {
    if (plan.checkpoint_authority) |candidate| {
        try candidate.validate();
        if (!supportsCheckpoint(adapter) or plan.artifact == null or candidate.retirement != null or !std.mem.eql(u8, &candidate.binding, &scope.identity())) return error.UnsupportedOperation;
        if (state.value.checkpoint_authority) |bound| {
            if (!bound.sameAuthority(candidate) or bound.retirement != null) return error.SourceBindingConflict;
            return;
        }
        try mutate(svc, a, scope, catalog, state.value, .{ .bind_checkpoint = candidate }, request);
        var confirmed = try load(svc, a, scope, request);
        errdefer confirmed.deinit();
        const bound = confirmed.value.checkpoint_authority orelse return error.SourceOutcomeUnresolved;
        if (!bound.sameAuthority(candidate) or bound.retirement != null) return error.SourceOutcomeUnresolved;
        state.deinit();
        state.* = confirmed;
    } else if (state.value.checkpoint_authority != null) return error.SourceBindingConflict;
}

fn confirmArtifactSeal(svc: anytype, a: A, scope: control.Scope, catalog: control.Digest, state: *std.json.Parsed(control.State), stamp: control.Digest, request: anytype) !void {
    const pending = state.value.pending orelse return error.SourcePendingWork;
    const record = (state.value.artifact_control orelse return error.InvalidSourceControl).current orelse return error.SourcePendingWork;
    const reservation = pending.identity(scope);
    if (pending.artifact == null or !std.mem.eql(u8, &record.reservation, &reservation)) return error.SourcePayloadConflict;
    if (record.phase != .reserved) return error.SourcePendingWork;
    try request.ensureActive();
    // An unknown metadata result stops this pass before any admission, even
    // when a qualified native read already proved the immutable seal.
    try mutate(svc, a, scope, catalog, state.value, .{ .seal_artifact = .{ .reservation_digest = reservation, .stamp = stamp } }, request);
    var next = try load(svc, a, scope, request);
    errdefer next.deinit();
    const confirmed = next.value.pending orelse return error.SourceOutcomeUnresolved;
    const sealed = (next.value.artifact_control orelse return error.InvalidSourceControl).current orelse return error.SourceOutcomeUnresolved;
    if (!std.mem.eql(u8, &confirmed.identity(scope), &reservation) or sealed.phase != .sealed or !std.mem.eql(u8, &sealed.stamp.?, &stamp)) return error.SourceOutcomeUnresolved;
    state.deinit();
    state.* = next;
}
fn sealArtifact(svc: anytype, adapter: anytype, a: A, scope: control.Scope, catalog: control.Digest, state: *std.json.Parsed(control.State), payload: anytype, request: anytype) !void {
    const pending = state.value.pending orelse return error.SourcePendingWork;
    if (pending.artifact == null) return;
    const record = (state.value.artifact_control orelse return error.InvalidSourceControl).current orelse return error.SourcePendingWork;
    if (!std.mem.eql(u8, &record.reservation, &pending.identity(scope))) return error.SourcePayloadConflict;
    if (record.phase == .sealed) return;
    if (record.phase != .reserved) return error.SourcePendingWork;
    if (comptime @hasDecl(AdapterType(@TypeOf(adapter)), "sealArtifact")) {
        try request.ensureActive();
        const stamp: control.Digest = try adapter.sealArtifact(a, scope, pending, payload, request);
        try confirmArtifactSeal(svc, a, scope, catalog, state, stamp, request);
    } else return error.UnsupportedOperation;
}
fn recoverArtifact(svc: anytype, adapter: anytype, a: A, scope: control.Scope, catalog: control.Digest, state: *std.json.Parsed(control.State), request: anytype) !void {
    if (comptime @hasDecl(AdapterType(@TypeOf(adapter)), "recoverArtifact")) {
        const pending = state.value.pending orelse return;
        if (pending.artifact == null) return;
        const record = (state.value.artifact_control orelse return error.InvalidSourceControl).current orelse return error.SourcePendingWork;
        if (!std.mem.eql(u8, &record.reservation, &pending.identity(scope))) return error.SourcePayloadConflict;
        if (record.phase != .reserved) return;
        try request.ensureActive();
        if (try adapter.recoverArtifact(a, scope, pending, request)) |stamp| {
            try confirmArtifactSeal(svc, a, scope, catalog, state, stamp, request);
        }
    }
}
/// Positive metadata seal authority is mandatory for payload-independent
/// admission. Neither an absent destination receipt nor a native upload alone
/// grants this capability. The adapter restores and verifies the frozen input.
fn admitRetained(adapter: anytype, a: A, scope: control.Scope, state: control.State, request: anytype) !Acceptance {
    if (state.closing) return error.SourcePendingWork;
    const pending = state.pending orelse return error.SourcePendingWork;
    const record = (state.artifact_control orelse return error.UnsupportedOperation).current orelse return error.SourcePendingWork;
    if (pending.artifact == null or record.phase != .sealed or record.stamp == null or !std.mem.eql(u8, &record.reservation, &pending.identity(scope))) return error.SourcePendingWork;
    try request.ensureActive();
    const result: Acceptance = try adapter.admitRetained(a, scope, pending, request);
    try request.ensureActive();
    return result;
}

/// Recovery needs no provider rows after a qualified immutable native seal.
/// Capable adapters may resume only the original confirmed frozen admission.
/// A failed release blocks the next reservation so its durable receipt cannot
/// be overwritten while the destination still retains coordinator ACK debt.
pub fn recover(svc: anytype, adapter: anytype, a: A, scope: control.Scope, catalog: control.Digest, request: anytype) !std.json.Parsed(control.State) {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try request.ensureActive();
    try adapter.validateScope(scope);
    var state = try load(svc, a, scope, request);
    errdefer state.deinit();
    if (state.value.scope == null) return state;
    try requireArtifactAdapter(adapter, state.value);
    if (state.value.scope.?.source_ordinal != scope.source_ordinal) {
        // Ordinals locate catalog entries; relocating the same immutable source
        // must preserve its pending authority and both progress frontiers.
        try mutate(svc, a, scope, catalog, state.value, .{ .configure = scope }, request);
        const next = try load(svc, a, scope, request);
        state.deinit();
        state = next;
    }
    if (state.value.accepted.sequence != 0) {
        try adapter.release(a, scope, state.value.accepted.reference, request);
        try refreshAfterRelease(svc, a, scope, &state, request);
    }
    try recoverArtifact(svc, adapter, a, scope, catalog, &state, request);
    if (state.value.pending) |pending| {
        var resolved = try adapter.resolve(arena.allocator(), scope, pending, request);
        if (comptime @hasDecl(AdapterType(@TypeOf(adapter)), "admitRetained")) {
            if (!state.value.closing and resolved == null and pending.artifact != null and state.value.artifact_control.?.current.?.phase == .sealed)
                resolved = try admitRetained(adapter, arena.allocator(), scope, state.value, request);
        }
        if (resolved) |result| {
            try accept(svc, a, scope, catalog, state.value, result, request);
            const next = try load(svc, a, scope, request);
            state.deinit();
            state = next;
            if (!std.mem.eql(u8, &state.value.accepted.reservation_digest, &result.reservation_digest)) return error.SourceControlConflict;
            try adapter.release(a, scope, state.value.accepted.reference, request);
            try refreshAfterRelease(svc, a, scope, &state, request);
        }
    }
    return state;
}
fn equalOptional(left: ?[]const u8, right: ?[]const u8) bool {
    if (left) |value| return right != null and std.mem.eql(u8, value, right.?);
    return right == null;
}
fn matches(input: Input, identity: ReceiptIdentity) bool {
    return std.mem.eql(u8, input.batch_id, identity.batch_id) and
        std.mem.eql(u8, input.checkpoint, identity.checkpoint) and
        equalOptional(input.expected_checkpoint, identity.expected_checkpoint) and
        std.mem.eql(u8, &input.payload_digest, &identity.payload_digest);
}

/// payload is borrowed only until admission. Durable metadata retains identity
/// and recovery authority, not a second copy of the provider's row WAL.
pub fn deliver(svc: anytype, adapter: anytype, a: A, scope: control.Scope, catalog: control.Digest, input: Input, payload: anytype, request: anytype) !std.json.Parsed(control.State) {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var state = try recover(svc, adapter, a, scope, catalog, request);
    errdefer state.deinit();
    if (state.value.scope == null) {
        try mutate(svc, a, scope, catalog, state.value, .{ .configure = scope }, request);
        const next = try load(svc, a, scope, request);
        state.deinit();
        state = next;
    }
    if (state.value.accepted.sequence != 0) {
        const identity = try adapter.inspect(scratch, scope, state.value.accepted.reference);
        if (std.mem.eql(u8, input.batch_id, identity.batch_id)) {
            if (!matches(input, identity)) return error.SourcePayloadConflict;
            return state;
        }
    }
    if (state.value.closing) return error.SourcePendingWork;
    if (state.value.pending == null) {
        // Validate the predecessor before asking the destination to plan.
        const previous: ?[]const u8 = if (state.value.accepted.sequence == 0) null else state.value.accepted.checkpoint;
        if (!equalOptional(input.expected_checkpoint, previous)) return error.SourceCheckpointConflict;
        const sequence = std.math.add(u64, state.value.accepted.sequence, 1) catch return error.SourceReceiptExpired;
        const plan: Plan = if (comptime @hasDecl(AdapterType(@TypeOf(adapter)), "planFrozen"))
            try adapter.planFrozen(scratch, scope, input, payload, try options(state.value, sequence, try control.inputFingerprint(scope, .{ .source_sequence = sequence, .batch_id = input.batch_id, .checkpoint = input.checkpoint, .expected_checkpoint = input.expected_checkpoint, .payload_digest = input.payload_digest, .accepted_at_ns = input.accepted_at_ns })), request)
        else
            try adapter.plan(scratch, scope, input, payload);
        if (plan.artifact != null and !@hasDecl(AdapterType(@TypeOf(adapter)), "sealArtifact")) return error.UnsupportedOperation;
        if (@hasDecl(AdapterType(@TypeOf(adapter)), "admitRetained") and plan.artifact == null) return error.UnsupportedOperation;
        try bindCheckpoint(svc, adapter, a, scope, catalog, &state, plan, request);
        const reservation: control.Reservation = .{
            .sequence = sequence,
            .batch_id = input.batch_id,
            .checkpoint = input.checkpoint,
            .expected_checkpoint = input.expected_checkpoint,
            .accepted_at_ns = input.accepted_at_ns,
            .payload_digest = input.payload_digest,
            .plan_digest = plan.digest,
            .transaction_id = plan.transaction_id,
            .destination = scope.destination,
            .delivery_manifest = plan.manifest,
            .artifact = plan.artifact,
        };
        try mutate(svc, a, scope, catalog, state.value, .{ .reserve = reservation }, request);
        const next = try load(svc, a, scope, request);
        state.deinit();
        state = next;
    }
    const before_seal = state.value.pending orelse return error.SourceControlConflict;
    if (!matches(input, .{ .batch_id = before_seal.batch_id, .checkpoint = before_seal.checkpoint, .expected_checkpoint = before_seal.expected_checkpoint, .payload_digest = before_seal.payload_digest })) return error.SourcePayloadConflict;
    try sealArtifact(svc, adapter, a, scope, catalog, &state, payload, request);
    const pending = state.value.pending orelse return error.SourceControlConflict;
    if (!matches(input, .{
        .batch_id = pending.batch_id,
        .checkpoint = pending.checkpoint,
        .expected_checkpoint = pending.expected_checkpoint,
        .payload_digest = pending.payload_digest,
    })) return error.SourcePayloadConflict;
    // The adapter verifies payload effects against the persisted plan. It must
    // reuse pending time/sequence/authority rather than today's input timestamp.
    try request.ensureActive();
    const result: Acceptance = if (comptime @hasDecl(AdapterType(@TypeOf(adapter)), "admitRetained"))
        try admitRetained(adapter, scratch, scope, state.value, request)
    else
        try adapter.admit(scratch, scope, pending, payload, request);
    try accept(svc, a, scope, catalog, state.value, result, request);
    const next = try load(svc, a, scope, request);
    state.deinit();
    state = next;
    if (!std.mem.eql(u8, &state.value.accepted.reservation_digest, &result.reservation_digest)) return error.SourceControlConflict;
    try adapter.release(a, scope, state.value.accepted.reference, request);
    try refreshAfterRelease(svc, a, scope, &state, request);
    return state;
}

/// Replan only a proven-unadmitted original attempt. The expected reservation
/// makes a lost metadata reply retryable without closing the newly chosen plan.
/// Prepared/committed work resolves through the original manifest instead.
pub fn replan(svc: anytype, adapter: anytype, a: A, scope: control.Scope, catalog: control.Digest, expected_reservation: control.Digest, input: Input, payload: anytype, request: anytype) !std.json.Parsed(control.State) {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    try request.ensureActive();
    try adapter.validateScope(scope);
    var state = try load(svc, a, scope, request);
    errdefer state.deinit();
    try requireArtifactAdapter(adapter, state.value);
    if (state.value.scope) |bound| if (bound.source_ordinal != scope.source_ordinal) {
        try mutate(svc, a, scope, catalog, state.value, .{ .configure = scope }, request);
        const next = try load(svc, a, scope, request);
        state.deinit();
        state = next;
    };
    if (state.value.accepted.sequence != 0) {
        try adapter.release(a, scope, state.value.accepted.reference, request);
        try refreshAfterRelease(svc, a, scope, &state, request);
        if (state.value.pending == null and matches(input, try adapter.inspect(scratch, scope, state.value.accepted.reference))) return state;
    }
    if (state.value.closing) return error.SourcePendingWork;
    const pending = state.value.pending orelse return error.SourcePendingWork;
    if (!matches(input, .{ .batch_id = pending.batch_id, .checkpoint = pending.checkpoint, .expected_checkpoint = pending.expected_checkpoint, .payload_digest = pending.payload_digest })) return error.SourcePayloadConflict;
    if (!std.mem.allEqual(u8, &pending.previous_reservation_digest, 0) and std.mem.eql(u8, &pending.previous_reservation_digest, &expected_reservation)) return state;
    if (!std.mem.eql(u8, &pending.identity(scope), &expected_reservation)) return error.SourceControlConflict;
    const resolved = adapter.resolve(scratch, scope, pending, request) catch |err| switch (err) {
        error.SourceAttemptRetired => null,
        else => return err,
    };
    if (resolved) |result| {
        try accept(svc, a, scope, catalog, state.value, result, request);
        const next = try recover(svc, adapter, a, scope, catalog, request);
        state.deinit();
        return next;
    }
    const replacement: Replan = if (comptime @hasDecl(AdapterType(@TypeOf(adapter)), "replanFrozen"))
        try adapter.replanFrozen(scratch, scope, pending, payload, try options(state.value, pending.sequence, try pending.inputFingerprint(scope)), request)
    else
        try adapter.replan(scratch, scope, pending, payload, request);
    if (replacement.plan.artifact != null and !@hasDecl(AdapterType(@TypeOf(adapter)), "sealArtifact")) return error.UnsupportedOperation;
    if (@hasDecl(AdapterType(@TypeOf(adapter)), "admitRetained") and replacement.plan.artifact == null) return error.UnsupportedOperation;
    try bindCheckpoint(svc, adapter, a, scope, catalog, &state, replacement.plan, request);
    try mutate(svc, a, scope, catalog, state.value, .{ .replan = .{
        .reservation_digest = expected_reservation,
        .plan_digest = replacement.plan.digest,
        .transaction_id = replacement.plan.transaction_id,
        .delivery_manifest = replacement.plan.manifest,
        .artifact = replacement.plan.artifact,
        .not_admitted_receipt = replacement.not_admitted_receipt,
    } }, request);
    const next = try load(svc, a, scope, request);
    state.deinit();
    state = next;
    if (state.value.pending == null or !std.mem.eql(u8, &state.value.pending.?.previous_reservation_digest, &expected_reservation)) return error.SourceControlConflict;
    return state;
}

test "delivery recovers lost destination and metadata acknowledgments before releasing retention" {
    const a = std.testing.allocator;
    const Context = struct {
        pub fn ensureActive(_: @This()) !void {}
    };
    const Service = struct {
        a: A,
        value: []u8,
        metadata_group_id: u64 = 1,
        lose_accept_ack: bool = false,
        lost: bool = false,
        pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: Context, version: u16) !void {
            try std.testing.expectEqual(control.protocol_version, version);
        }
        pub fn ensureLinearizableReadWithContext(_: *@This(), _: Context) !void {}
        pub fn projectedStore(self: *@This()) ?*@This() {
            return self;
        }
        pub fn getSourceControl(self: *@This(), allocator: A, _: u64, _: u64, _: control.Digest) ![]u8 {
            return allocator.dupe(u8, self.value);
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: anytype) !u64 {
            var write = try control.parseWrite(self.a, command.apply_source_control);
            defer write.deinit();
            var before = try control.parse(self.a, self.value);
            defer before.deinit();
            if (write.value.expected_revision != before.value.revision) return error.SourceControlConflict;
            var arena = std.heap.ArenaAllocator.init(self.a);
            defer arena.deinit();
            const next = try write.value.mutation.apply(arena.allocator(), before.value);
            const bytes = try control.encode(self.a, next);
            self.a.free(self.value);
            self.value = bytes;
            self.lost = self.lose_accept_ack and write.value.mutation == .accept;
            return next.revision;
        }
        pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: Context) !void {
            if (self.lost) return error.TestLostAck;
        }
    };
    const Adapter = struct {
        backing: A,
        receipt: ?[]const u8 = null,
        counter: i64 = 0,
        admissions: usize = 0,
        releases: usize = 0,
        lose_reply: bool = true,
        block_release: bool = false,
        pub fn validateScope(_: *@This(), scope: control.Scope) !void {
            if (scope.destination != .native) return error.SourceBindingConflict;
        }
        pub fn plan(_: *@This(), _: A, _: control.Scope, _: Input, _: i64) !Plan {
            return .{ .digest = @splat(4), .transaction_id = @splat(5), .manifest = "original-owner" };
        }
        fn result(self: *@This(), allocator: A, scope: control.Scope, pending: control.Reservation) !Acceptance {
            var stored = try std.json.parseFromSlice(control.Reservation, allocator, self.receipt.?, .{});
            defer stored.deinit();
            if (!std.mem.eql(u8, &stored.value.identity(scope), &pending.identity(scope))) return error.SourcePayloadConflict;
            return .{ .reservation_digest = pending.identity(scope), .receipt = self.receipt.?, .commit_reference = "native-commit" };
        }
        pub fn resolve(self: *@This(), allocator: A, scope: control.Scope, pending: control.Reservation, _: Context) !?Acceptance {
            if (self.receipt == null) return null;
            return try self.result(allocator, scope, pending);
        }
        pub fn admit(self: *@This(), allocator: A, scope: control.Scope, pending: control.Reservation, amount: i64, _: Context) !Acceptance {
            self.admissions += 1;
            self.counter += amount;
            self.receipt = try std.json.Stringify.valueAlloc(self.backing, pending, .{});
            if (self.lose_reply) return error.TestLostDestinationReply;
            return self.result(allocator, scope, pending);
        }
        pub fn release(self: *@This(), _: A, _: control.Scope, _: []const u8, _: Context) !void {
            if (self.block_release) return error.TestReleaseUnavailable;
            self.releases += 1;
        }
        pub fn inspect(_: *@This(), allocator: A, _: control.Scope, receipt: []const u8) !ReceiptIdentity {
            const parsed = try std.json.parseFromSlice(control.Reservation, allocator, receipt, .{ .allocate = .alloc_always });
            // allocator is the attempt arena; identities borrow it.
            return .{ .batch_id = parsed.value.batch_id, .checkpoint = parsed.value.checkpoint, .expected_checkpoint = parsed.value.expected_checkpoint, .payload_digest = parsed.value.payload_digest };
        }
    };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var scope: control.Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(6) };
    var svc: Service = .{ .a = a, .value = try control.encode(a, .{}) };
    defer a.free(svc.value);
    var adapter: Adapter = .{ .backing = arena.allocator() };
    var input: Input = .{ .batch_id = "one", .checkpoint = "cyclic-cp", .payload_digest = @splat(7), .accepted_at_ns = 123 };
    try std.testing.expectError(error.TestLostDestinationReply, deliver(&svc, &adapter, a, scope, @splat(9), input, @as(i64, 1), Context{}));
    try std.testing.expectEqual(@as(i64, 1), adapter.counter);
    try std.testing.expectEqual(@as(usize, 0), adapter.releases);
    // Relocation while a reservation is pending keeps its original command
    // authority. Restart recovery still has no original input rows.
    scope.source_ordinal = 1;
    var recovered = try recover(&svc, &adapter, a, scope, @splat(9), Context{});
    try std.testing.expectEqual(@as(u64, 1), recovered.value.committed.sequence);
    try std.testing.expect(recovered.value.pending == null);
    recovered.deinit();
    adapter.lose_reply = false;
    // A changed retry timestamp cannot change a persisted admission identity.
    input.accepted_at_ns = 999;
    var duplicate = try deliver(&svc, &adapter, a, scope, @splat(9), input, @as(i64, 1), Context{});
    duplicate.deinit();
    try std.testing.expectEqual(@as(usize, 1), adapter.admissions);
    input.payload_digest = @splat(8);
    try std.testing.expectError(error.SourcePayloadConflict, deliver(&svc, &adapter, a, scope, @splat(9), input, @as(i64, 1), Context{}));
    input.batch_id = "two";
    input.expected_checkpoint = "cyclic-cp";
    svc.lose_accept_ack = true;
    const releases_before = adapter.releases;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, deliver(&svc, &adapter, a, scope, @splat(9), input, @as(i64, 1), Context{}));
    try std.testing.expectEqual(@as(i64, 2), adapter.counter);
    // The pre-delivery release of the previous head is allowed; the new
    // destination receipt is never released after an unknown metadata outcome.
    try std.testing.expectEqual(releases_before + 1, adapter.releases);
    adapter.block_release = true;
    try std.testing.expectError(error.TestReleaseUnavailable, deliver(&svc, &adapter, a, scope, @splat(9), input, @as(i64, 1), Context{}));
    try std.testing.expectEqual(@as(usize, 2), adapter.admissions);
    adapter.block_release = false;
    svc.lose_accept_ack = false;
    var final = try deliver(&svc, &adapter, a, scope, @splat(9), input, @as(i64, 1), Context{});
    defer final.deinit();
    try std.testing.expectEqual(@as(u64, 2), final.value.committed.sequence);
    try std.testing.expectEqualStrings("cyclic-cp", final.value.committed.checkpoint);
    try std.testing.expectEqual(@as(usize, 2), adapter.admissions);
    var moved = scope;
    moved.source_ordinal = 2;
    var relocated = try recover(&svc, &adapter, a, moved, @splat(9), Context{});
    defer relocated.deinit();
    try std.testing.expectEqual(@as(u32, 2), relocated.value.scope.?.source_ordinal);
    try std.testing.expectEqual(@as(u64, 2), relocated.value.committed.sequence);
    try std.testing.expectEqual(@as(usize, 2), adapter.admissions);
}

test "delivery recovers sealed source artifacts after a lost metadata reply without provider rows" {
    for ([_]bool{ false, true }) |native_reply_lost| {
        for ([_]bool{ false, true }) |checkpoint_mode| {
            const a = std.testing.allocator;
            const Context = struct {
                pub fn ensureActive(_: @This()) !void {}
            };
            const Service = struct {
                a: A,
                value: []u8,
                metadata_group_id: u64 = 1,
                lose_seal: bool = true,
                lose_bind: bool = false,
                lost: bool = false,
                floor: u16 = 0,
                pub fn ensureTableTopologyProtocolReadyWithContext(self: *@This(), _: Context, version: u16) !void {
                    self.floor = @max(self.floor, version);
                }
                pub fn ensureLinearizableReadWithContext(_: *@This(), _: Context) !void {}
                pub fn projectedStore(self: *@This()) ?*@This() {
                    return self;
                }
                pub fn getSourceControl(self: *@This(), allocator: A, _: u64, _: u64, _: control.Digest) ![]u8 {
                    return allocator.dupe(u8, self.value);
                }
                pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: anytype) !u64 {
                    var write = try control.parseWrite(self.a, command.apply_source_control);
                    defer write.deinit();
                    var before = try control.parse(self.a, self.value);
                    defer before.deinit();
                    if (write.value.expected_revision != before.value.revision) return error.SourceControlConflict;
                    var arena = std.heap.ArenaAllocator.init(self.a);
                    defer arena.deinit();
                    const next = try write.value.mutation.apply(arena.allocator(), before.value);
                    const bytes = try control.encode(self.a, next);
                    self.a.free(self.value);
                    self.value = bytes;
                    self.lost = (self.lose_seal and write.value.mutation == .seal_artifact) or (self.lose_bind and write.value.mutation == .bind_checkpoint);
                    if (self.lost) {
                        if (write.value.mutation == .bind_checkpoint) self.lose_bind = false else self.lose_seal = false;
                    }
                    return next.revision;
                }
                pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: Context) !void {
                    if (self.lost) return error.TestLostSealReply;
                }
            };
            const Storage = struct { bytes: ?[]u8 = null, receipt: ?[]u8 = null, uploads: usize = 0, admissions: usize = 0, sealed: bool = false, seal_reads: usize = 0 };
            const Adapter = struct {
                pub const checkpoint_capable = true;
                want_checkpoint: bool = false,
                lose_native_reply: bool = false,
                candidate_group: u64 = 17,
                backing: A,
                svc: *Service,
                storage: *Storage,
                pub fn validateScope(_: *@This(), scope: control.Scope) !void {
                    try scope.validate();
                }
                pub fn planFrozen(self: *@This(), _: A, scope: control.Scope, _: Input, payload: []const u8, grant: PlanOptions, _: Context) !Plan {
                    try std.testing.expectEqual(@as(u64, 1), grant.source_sequence);
                    var attempt: [16]u8 = @splat(1);
                    std.mem.writeInt(u64, attempt[0..8], grant.upload_token, .big);
                    return .{ .digest = @splat(4), .transaction_id = @splat(5), .manifest = "original-frozen-owner", .checkpoint_authority = if (self.want_checkpoint) grant.checkpoint_authority orelse control.checkpoint_authority.Spec{ .binding = scope.identity(), .table_name = "checkpoint-owner", .owner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(5), .root_generation = 0, .group_id = self.candidate_group, .topology_epoch = 1 } } else null, .artifact = .{ .namespace = @splat(9), .binding = scope.identity(), .domain = control.artifact_control.domainFor(scope.identity()), .attempt = attempt, .bytes = payload.len, .checksum = control.digest(payload), .plan_digest = @splat(11) } };
                }
                pub fn sealArtifact(self: *@This(), _: A, scope: control.Scope, pending: control.Reservation, payload: []const u8, request: Context) !control.Digest {
                    var journal = try load(self.svc, self.backing, scope, request);
                    defer journal.deinit();
                    if (self.want_checkpoint) try std.testing.expectEqual(@as(u64, 17), journal.value.checkpoint_authority.?.owner.group_id);
                    const record = journal.value.artifact_control.?.current.?;
                    try std.testing.expectEqual(.reserved, record.phase);
                    try std.testing.expectEqual(pending.identity(scope), record.reservation);
                    try std.testing.expectEqual(@as(u64, payload.len), journal.value.artifact_control.?.chargedBytes());
                    try std.testing.expectEqual(record.spec.checksum, control.digest(payload));
                    self.storage.bytes = try self.backing.dupe(u8, payload);
                    self.storage.uploads += 1;
                    self.storage.sealed = true;
                    if (self.lose_native_reply) return error.TestLostNativeSealReply;
                    return @splat(13);
                }
                pub fn resolve(self: *@This(), allocator: A, scope: control.Scope, pending: control.Reservation, request: Context) !?Acceptance {
                    var journal = try load(self.svc, allocator, scope, request);
                    defer journal.deinit();
                    const record = journal.value.artifact_control.?.current orelse return error.SourcePendingWork;
                    if (record.phase == .reserved) return null;
                    const receipt = self.storage.receipt orelse return null;
                    return .{ .reservation_digest = pending.identity(scope), .receipt = receipt, .commit_reference = "native-commit" };
                }
                pub fn recoverArtifact(self: *@This(), allocator: A, scope: control.Scope, pending: control.Reservation, request: Context) !?control.Digest {
                    var journal = try load(self.svc, allocator, scope, request);
                    defer journal.deinit();
                    const record = journal.value.artifact_control.?.current.?;
                    try std.testing.expectEqual(.reserved, record.phase);
                    try std.testing.expectEqual(pending.identity(scope), record.reservation);
                    self.storage.seal_reads += 1;
                    if (!self.storage.sealed) return null;
                    try std.testing.expectEqual(record.spec.checksum, control.digest(self.storage.bytes.?));
                    return @splat(13);
                }
                pub fn admitRetained(self: *@This(), allocator: A, scope: control.Scope, pending: control.Reservation, request: Context) !Acceptance {
                    // No provider bytes enter this callback. The retained store
                    // owns the original input and first materialization.
                    return self.admit(allocator, scope, pending, self.storage.bytes.?, request);
                }
                pub fn admit(self: *@This(), _: A, scope: control.Scope, pending: control.Reservation, payload: []const u8, request: Context) !Acceptance {
                    var journal = try load(self.svc, self.backing, scope, request);
                    defer journal.deinit();
                    try std.testing.expectEqual(.sealed, journal.value.artifact_control.?.current.?.phase);
                    try std.testing.expectEqualSlices(u8, self.storage.bytes.?, payload);
                    try std.testing.expectEqual(@as(usize, 0), self.storage.admissions);
                    self.storage.admissions += 1;
                    self.storage.receipt = try std.json.Stringify.valueAlloc(self.backing, pending, .{});
                    return .{ .reservation_digest = pending.identity(scope), .receipt = self.storage.receipt.?, .commit_reference = "native-commit" };
                }
                pub fn release(self: *@This(), allocator: A, scope: control.Scope, receipt: []const u8, request: Context) !void {
                    var original = try std.json.parseFromSlice(control.Reservation, allocator, receipt, .{});
                    defer original.deinit();
                    const id = original.value.identity(scope);
                    var journal = try load(self.svc, allocator, scope, request);
                    defer journal.deinit();
                    if (journal.value.artifact_control.?.current == null) {
                        try std.testing.expectEqual(id, journal.value.artifact_control.?.last_reclaimed.?.reservation);
                        return;
                    }
                    try std.testing.expectEqual(id, journal.value.accepted.reservation_digest);
                    try mutate(self.svc, allocator, scope, @splat(9), journal.value, .{ .retire_artifact = .{ .reservation_digest = id, .proof = @splat(14), .reason = .committed } }, request);
                    const retired = try load(self.svc, allocator, scope, request);
                    journal.deinit();
                    journal = retired;
                    // Simulate a trusted fenced storage cleanup before returning quota.
                    self.backing.free(self.storage.bytes.?);
                    self.storage.bytes = null;
                    try mutate(self.svc, allocator, scope, @splat(9), journal.value, .{ .reclaim_artifact = .{ .reservation_digest = id, .proof = @splat(15) } }, request);
                }
                pub fn inspect(_: *@This(), allocator: A, _: control.Scope, receipt: []const u8) !ReceiptIdentity {
                    const value = try std.json.parseFromSliceLeaky(control.Reservation, allocator, receipt, .{});
                    return .{ .batch_id = value.batch_id, .checkpoint = value.checkpoint, .expected_checkpoint = value.expected_checkpoint, .payload_digest = value.payload_digest };
                }
            };
            var svc: Service = .{ .a = a, .value = try control.encode(a, .{}) };
            defer a.free(svc.value);
            var storage: Storage = .{};
            defer if (storage.bytes) |bytes| a.free(bytes);
            defer if (storage.receipt) |receipt| a.free(receipt);
            var adapter: Adapter = .{ .backing = a, .svc = &svc, .storage = &storage };
            const scope: control.Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(6) };
            const payload: [128]u8 = @splat(1);
            const input: Input = .{ .batch_id = "one", .checkpoint = "cp1", .payload_digest = control.digest(&payload), .accepted_at_ns = 123 };
            adapter.want_checkpoint = checkpoint_mode;
            adapter.lose_native_reply = native_reply_lost;
            svc.lose_bind = checkpoint_mode;
            if (checkpoint_mode) {
                try std.testing.expectError(error.MetadataMutationOutcomeUnknown, deliver(&svc, &adapter, a, scope, @splat(9), input, &payload, Context{}));
                try std.testing.expectEqual(@as(usize, 0), storage.uploads);
                try std.testing.expectEqual(@as(usize, 0), storage.admissions);
                adapter.candidate_group = 18;
                adapter.want_checkpoint = false;
                try std.testing.expectError(error.SourceBindingConflict, deliver(&svc, &adapter, a, scope, @splat(9), input, &payload, Context{}));
                adapter.want_checkpoint = true;
            }
            try std.testing.expectError(if (native_reply_lost) error.TestLostNativeSealReply else error.MetadataMutationOutcomeUnknown, deliver(&svc, &adapter, a, scope, @splat(9), input, &payload, Context{}));
            try std.testing.expectEqual(@as(usize, 1), storage.uploads);
            try std.testing.expectEqual(@as(usize, 0), storage.admissions);
            try std.testing.expectEqual(if (checkpoint_mode) control.checkpoint_authority.protocol_version else control.artifact_control.protocol_version, svc.floor);
            var pending = try load(&svc, a, scope, Context{});
            defer pending.deinit();
            try std.testing.expectEqual(@as(control.artifact_control.Phase, if (native_reply_lost) .reserved else .sealed), pending.value.artifact_control.?.current.?.phase);
            try std.testing.expectEqual(@as(u64, 0), pending.value.accepted.sequence);
            // Replace the worker while the separate storage owner retains the input.
            var restarted: Adapter = .{ .backing = a, .svc = &svc, .storage = &storage };
            if (native_reply_lost) {
                storage.sealed = false;
                var waiting = try recover(&svc, &restarted, a, scope, @splat(9), Context{});
                defer waiting.deinit();
                try std.testing.expectEqual(.reserved, waiting.value.artifact_control.?.current.?.phase);
                try std.testing.expectEqual(@as(usize, 0), storage.admissions);
                storage.sealed = true;
                // The next metadata seal reply is also lost. Even the observed
                // native seal cannot authorize admission until metadata reload.
                try std.testing.expectError(error.MetadataMutationOutcomeUnknown, recover(&svc, &restarted, a, scope, @splat(9), Context{}));
                try std.testing.expectEqual(@as(usize, 0), storage.admissions);
                try std.testing.expectEqual(@as(usize, 2), storage.seal_reads);
            }
            var recovered = try recover(&svc, &restarted, a, scope, @splat(9), Context{});
            defer recovered.deinit();
            try std.testing.expectEqual(@as(u64, 1), recovered.value.committed.sequence);
            try std.testing.expectEqual(@as(u64, 0), recovered.value.artifact_control.?.chargedBytes());
            try std.testing.expect(!control.hasWork(recovered.value));
            try std.testing.expectEqual(@as(usize, 1), storage.admissions);
            try std.testing.expectEqual(@as(usize, 1), storage.uploads);
            var duplicate = try deliver(&svc, &restarted, a, scope, @splat(9), input, &payload, Context{});
            defer duplicate.deinit();
            try std.testing.expectEqual(@as(usize, 1), storage.admissions);
            const Legacy = struct {
                pub fn validateScope(_: *@This(), _: control.Scope) !void {}
                pub fn release(_: *@This(), _: A, _: control.Scope, _: []const u8, _: Context) !void {
                    return error.UnexpectedLegacyRelease;
                }
                pub fn resolve(_: *@This(), _: A, _: control.Scope, _: control.Reservation, _: Context) !?Acceptance {
                    return error.UnexpectedLegacyResolution;
                }
            };
            var legacy: Legacy = .{};
            try std.testing.expectError(error.UnsupportedOperation, recover(&svc, &legacy, a, scope, @splat(9), Context{}));
        }
    }
}

test "delivery closing source preserves sealed pending authority without fresh admission or replanning" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const Context = struct {
        pub fn ensureActive(_: @This()) !void {}
    };
    const scope: control.Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 1, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(4) };
    var attempt: [16]u8 = @splat(6);
    std.mem.writeInt(u64, attempt[0..8], 1, .big);
    const artifact: control.artifact_control.Spec = .{ .namespace = @splat(5), .binding = scope.identity(), .domain = control.artifact_control.domainFor(scope.identity()), .attempt = attempt, .bytes = 128, .checksum = @splat(7), .plan_digest = @splat(8) };
    const pending: control.Reservation = .{ .sequence = 1, .batch_id = "original", .checkpoint = "one", .accepted_at_ns = 10, .payload_digest = @splat(9), .plan_digest = @splat(10), .transaction_id = @splat(11), .destination = .native, .delivery_manifest = "frozen-original", .artifact = artifact };
    var state = try (control.Mutation{ .configure = scope }).apply(ca, .{});
    state = try (control.Mutation{ .reserve = pending }).apply(ca, state);
    state = try (control.Mutation{ .seal_artifact = .{ .reservation_digest = pending.identity(scope), .stamp = @splat(12) } }).apply(ca, state);
    state = try (control.Mutation{ .close = {} }).apply(ca, state);
    const Service = struct {
        value: []const u8,
        metadata_group_id: u64 = 1,
        pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: Context, _: u16) !void {
            return error.TestUnexpectedMetadataMutation;
        }
        pub fn ensureLinearizableReadWithContext(_: *@This(), _: Context) !void {}
        pub fn projectedStore(self: *@This()) ?*@This() {
            return self;
        }
        pub fn getSourceControl(self: *@This(), allocator: A, _: u64, _: u64, _: control.Digest) ![]u8 {
            return allocator.dupe(u8, self.value);
        }
        pub fn proposeTransitionCommandWithReceipt(_: *@This(), _: anytype) !u64 {
            return error.TestUnexpectedMetadataMutation;
        }
        pub fn waitForTransitionAppliedWithContext(_: *@This(), _: u64, _: Context) !void {
            return error.TestUnexpectedMetadataMutation;
        }
    };
    const Adapter = struct {
        resolves: usize = 0,
        pub fn validateScope(_: *@This(), value: control.Scope) !void {
            try value.validate();
        }
        pub fn resolve(self: *@This(), _: A, _: control.Scope, _: control.Reservation, _: Context) !?Acceptance {
            self.resolves += 1;
            return null;
        }
        pub fn release(_: *@This(), _: A, _: control.Scope, _: []const u8, _: Context) !void {
            return error.TestUnexpectedRelease;
        }
        pub fn admitRetained(_: *@This(), _: A, _: control.Scope, _: control.Reservation, _: Context) !Acceptance {
            return error.TestUnexpectedAdmission;
        }
        pub fn sealArtifact(_: *@This(), _: A, _: control.Scope, _: control.Reservation, _: []const u8, _: Context) !control.Digest {
            return error.TestUnexpectedUpload;
        }
        pub fn plan(_: *@This(), _: A, _: control.Scope, _: Input, _: []const u8) !Plan {
            return error.TestUnexpectedPlan;
        }
        pub fn inspect(_: *@This(), _: A, _: control.Scope, _: []const u8) !ReceiptIdentity {
            return error.TestUnexpectedInspection;
        }
    };
    const bytes = try control.encode(a, state);
    defer a.free(bytes);
    var service: Service = .{ .value = bytes };
    var adapter: Adapter = .{};
    var recovered = try recover(&service, &adapter, a, scope, @splat(13), Context{});
    defer recovered.deinit();
    try std.testing.expect(recovered.value.closing);
    try std.testing.expectEqual(pending.identity(scope), recovered.value.pending.?.identity(scope));
    try std.testing.expectEqual(@as(usize, 1), adapter.resolves);
    try std.testing.expectError(error.SourcePendingWork, admitRetained(&adapter, a, scope, recovered.value, Context{}));
    const input: Input = .{ .batch_id = pending.batch_id, .checkpoint = pending.checkpoint, .payload_digest = pending.payload_digest, .accepted_at_ns = pending.accepted_at_ns };
    try std.testing.expectError(error.SourcePendingWork, deliver(&service, &adapter, a, scope, @splat(13), input, @as([]const u8, ""), Context{}));
}
