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
};
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

/// Recovery needs no source rows if destination admission already happened.
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
    if (state.value.scope.?.source_ordinal != scope.source_ordinal) {
        // Ordinals locate catalog entries; relocating the same immutable source
        // must preserve its pending authority and both progress frontiers.
        try mutate(svc, a, scope, catalog, state.value, .{ .configure = scope }, request);
        const next = try load(svc, a, scope, request);
        state.deinit();
        state = next;
    }
    if (state.value.accepted.sequence != 0)
        try adapter.release(a, scope, state.value.accepted.reference, request);
    if (state.value.pending) |pending| {
        if (try adapter.resolve(arena.allocator(), scope, pending, request)) |result| {
            try accept(svc, a, scope, catalog, state.value, result, request);
            const next = try load(svc, a, scope, request);
            state.deinit();
            state = next;
            if (!std.mem.eql(u8, &state.value.accepted.reservation_digest, &result.reservation_digest)) return error.SourceControlConflict;
            try adapter.release(a, scope, state.value.accepted.reference, request);
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
    if (state.value.pending == null) {
        // Validate the predecessor before asking the destination to plan.
        const previous: ?[]const u8 = if (state.value.accepted.sequence == 0) null else state.value.accepted.checkpoint;
        if (!equalOptional(input.expected_checkpoint, previous)) return error.SourceCheckpointConflict;
        const plan: Plan = try adapter.plan(scratch, scope, input, payload);
        const reservation: control.Reservation = .{
            .sequence = std.math.add(u64, state.value.accepted.sequence, 1) catch return error.SourceReceiptExpired,
            .batch_id = input.batch_id,
            .checkpoint = input.checkpoint,
            .expected_checkpoint = input.expected_checkpoint,
            .accepted_at_ns = input.accepted_at_ns,
            .payload_digest = input.payload_digest,
            .plan_digest = plan.digest,
            .transaction_id = plan.transaction_id,
            .destination = scope.destination,
            .delivery_manifest = plan.manifest,
        };
        try mutate(svc, a, scope, catalog, state.value, .{ .reserve = reservation }, request);
        const next = try load(svc, a, scope, request);
        state.deinit();
        state = next;
    }
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
    const result: Acceptance = try adapter.admit(scratch, scope, pending, payload, request);
    try accept(svc, a, scope, catalog, state.value, result, request);
    const next = try load(svc, a, scope, request);
    state.deinit();
    state = next;
    if (!std.mem.eql(u8, &state.value.accepted.reservation_digest, &result.reservation_digest)) return error.SourceControlConflict;
    try adapter.release(a, scope, state.value.accepted.reference, request);
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
    if (state.value.scope) |bound| if (bound.source_ordinal != scope.source_ordinal) {
        try mutate(svc, a, scope, catalog, state.value, .{ .configure = scope }, request);
        const next = try load(svc, a, scope, request);
        state.deinit();
        state = next;
    };
    if (state.value.accepted.sequence != 0) {
        try adapter.release(a, scope, state.value.accepted.reference, request);
        if (state.value.pending == null and matches(input, try adapter.inspect(scratch, scope, state.value.accepted.reference))) return state;
    }
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
    const replacement: Replan = try adapter.replan(scratch, scope, pending, payload, request);
    try mutate(svc, a, scope, catalog, state.value, .{ .replan = .{
        .reservation_digest = expected_reservation,
        .plan_digest = replacement.plan.digest,
        .transaction_id = replacement.plan.transaction_id,
        .delivery_manifest = replacement.plan.manifest,
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
