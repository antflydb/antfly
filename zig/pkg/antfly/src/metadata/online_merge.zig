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

//! Durable online phases of the existing merge transition, not a second job
//! scheduler. Endpoint observations must be authenticated and linearizable.
//! An effect and the metadata CAS deliberately are separate steps: after an
//! ambiguous response/restart the same immutable attempt is observed again.
const std = @import("std");
const source = @import("../storage/db/online_source_contract.zig");
const snapshot = @import("../storage/source_snapshot.zig");
const page = @import("../storage/db/merge_page_contract.zig");

pub const Phase = enum { admit, publish, snapshot, tail, freeze, final_tail, cutover, release, complete, cancel_receiver, cancel_release, cancelled };
pub const State = struct {
    version: u8 = 1,
    revision: u64 = 1,
    scope: source.Scope,
    phase: Phase = .admit,
    certificate: ?snapshot.Certificate = null,
    acknowledged: u64 = 0,
    final_sequence: u64 = 0,
    final_applied_index: u64 = 0,
    final_cut_digest: [32]u8 = @splat(0),

    pub fn terminal(self: State) bool {
        return self.phase == .complete or self.phase == .cancelled;
    }
    pub fn validate(self: State) !void {
        try self.scope.validate();
        if (self.version != 1 or self.revision == 0) return error.InvalidOnlineMergeState;
        if (self.certificate) |certificate| {
            _ = try certificate.encode();
            if (!certificate.cut.namespace.eql(self.scope.fence.namespace) or self.acknowledged < certificate.cut.retained_start)
                return error.InvalidOnlineMergeState;
        } else if (self.phase != .admit and self.phase != .publish and self.phase != .cancel_receiver and self.phase != .cancel_release and self.phase != .cancelled)
            return error.InvalidOnlineMergeState;
        if (self.final_applied_index != 0) {
            const certificate = self.certificate orelse return error.InvalidOnlineMergeState;
            if (self.final_sequence < self.acknowledged or self.final_applied_index < certificate.cut.applied_index or std.mem.allEqual(u8, &self.final_cut_digest, 0))
                return error.InvalidOnlineMergeState;
        } else if (self.final_sequence != 0 or !std.mem.allEqual(u8, &self.final_cut_digest, 0) or
            self.phase == .final_tail or self.phase == .cutover or self.phase == .release or self.phase == .complete)
            return error.InvalidOnlineMergeState;
    }
    pub fn validateRecord(self: State, record: anytype) !void {
        try self.validate();
        if (record.transition_id != self.scope.fence.transition_id or record.donor_group_id != self.scope.fence.owner_group_id or
            record.receiver_group_id != self.scope.fence.peer_group_id or record.table_contract.table_id != self.scope.fence.namespace.table_id or
            record.table_contract.source_identity.shard_id != self.scope.fence.namespace.shard_id or record.table_contract.source_identity.range_id != self.scope.fence.namespace.range_id or
            record.table_contract.target_identity.shard_id != self.scope.receiver_namespace.shard_id or record.table_contract.target_identity.range_id != self.scope.receiver_namespace.range_id or
            (record.phase == .finalized) != (self.phase == .complete) or (record.phase == .rolled_back) != (self.phase == .cancelled))
            return error.InvalidOnlineMergeState;
    }
    pub fn eql(a: State, b: State) bool {
        return std.meta.eql(a, b);
    }
    pub fn sourceIdentity(self: State) !page.Source {
        const certificate = self.certificate orelse return error.InvalidOnlineMergeState;
        return .{ .namespace = certificate.cut.namespace, .pin_digest = try certificate.digest(), .applied_index = certificate.cut.applied_index, .retention = .{ .epoch = self.scope.consumer_epoch, .after_sequence = certificate.cut.retained_start }, .integrity = certificate.integrity };
    }
};

/// Exact CAS, not last-writer-wins. Old controllers cannot replace an attempt,
/// regress a receipt, change a published certificate, or undo cutover.
pub fn updateAllowed(previous: State, next: State) bool {
    previous.validate() catch return false;
    next.validate() catch return false;
    if (previous.eql(next)) return true;
    if (!std.meta.eql(previous.scope, next.scope) or previous.terminal() or
        previous.revision == std.math.maxInt(u64) or next.revision != previous.revision + 1 or next.acknowledged < previous.acknowledged) return false;
    if (previous.certificate) |certificate| if (next.certificate == null or !certificate.eql(next.certificate.?)) return false;
    if (previous.certificate == null and next.certificate != null and !(previous.phase == .publish and next.phase == .snapshot)) return false;
    if (previous.final_applied_index == 0 and next.final_applied_index != 0 and !(previous.phase == .freeze and next.phase == .final_tail)) return false;
    if (previous.final_applied_index != 0 and (next.final_applied_index != previous.final_applied_index or next.final_sequence != previous.final_sequence or
        !std.mem.eql(u8, &next.final_cut_digest, &previous.final_cut_digest))) return false;
    if (next.phase == previous.phase) return (previous.phase == .tail or previous.phase == .final_tail) and next.acknowledged > previous.acknowledged;
    if (next.phase == .cancel_receiver) return @intFromEnum(previous.phase) < @intFromEnum(Phase.cutover);
    const successor: Phase = switch (previous.phase) {
        .admit => .publish,
        .publish => .snapshot,
        .snapshot => .tail,
        .tail => .freeze,
        .freeze => .final_tail,
        .final_tail => .cutover,
        .cutover => .release,
        .release => .complete,
        .cancel_receiver => .cancel_release,
        .cancel_release => .cancelled,
        .complete, .cancelled => return false,
    };
    return next.phase == successor;
}

pub const Capabilities = struct {
    replicated_source_pins: bool = false,
    native_retained_snapshots: bool = false,
    transferable_artifacts: bool = false,
    receiver_receipts: bool = false,
    transactional_headroom: bool = false,
    pub fn require(self: Capabilities) !void {
        inline for (std.meta.fields(Capabilities)) |field| if (!@field(self, field.name)) return error.OnlineMergeUnavailable;
    }
};

pub const Observation = struct {
    ordinary_conflict: bool = false,
    owned_arena: ?*std.heap.ArenaAllocator = null,
    scope: source.Scope,
    source_progress: ?@import("../storage/db/online_source.zig").Progress = null,
    certificate: ?snapshot.Certificate = null,
    receiver: ?page.Progress = null,
    retained_head: u64 = 0,
    retained_reclaimed: u64 = 0,
    retained_reclaimable: u64 = 0,
    /// Exact scoped topology fence is durably active and participants drained.
    frozen: bool = false,
    /// Metadata range publication for this exact attempt, not local readiness.
    cutover_committed: bool = false,
    /// Native receiver finalization is proven, but ranges are still private.
    /// Advancing cutover->release must publish both ranges atomically in CAS.
    cutover_prepared: bool = false,
    /// Durable receiver rollback receipt for this exact attempt.
    receiver_cancelled: bool = false,
    /// Source authority permanently revokes this admission epoch. Absence of
    /// a consumer alone does not prove an ambiguous admit cannot arrive late.
    source_admission_closed: bool = false,
};
pub const Action = union(enum) {
    source_command: source.Command,
    prepare_certificate,
    snapshot_page,
    tail_page,
    freeze_and_drain,
    cutover,
    cancel_receiver,
    cancel_source_admission,
};
pub const Decision = union(enum) { wait, advance: State, execute: Action };

fn advance(current: State, phase: Phase) !Decision {
    var next = current;
    next.phase = phase;
    next.revision = std.math.add(u64, current.revision, 1) catch return error.InvalidOnlineMergeState;
    if (!updateAllowed(current, next)) return error.InvalidOnlineMergeState;
    return .{ .advance = next };
}

fn nextRevision(current: State) !u64 {
    return std.math.add(u64, current.revision, 1) catch error.InvalidOnlineMergeState;
}

fn verify(current: State, observation: Observation) !void {
    if (!std.meta.eql(current.scope, observation.scope)) return error.OnlineMergeReceiptMismatch;
    if (observation.retained_reclaimable > observation.retained_head or observation.retained_reclaimed > observation.retained_reclaimable) return error.OnlineMergeReceiptMismatch;
    if (observation.source_progress) |progress| {
        if (!std.mem.eql(u8, &progress.namespace, &current.scope.namespace()) or progress.consumer_epoch != current.scope.consumer_epoch or
            !std.mem.eql(u8, &progress.pin, &current.scope.pin())) return error.OnlineMergeReceiptMismatch;
        if (current.certificate) |certificate| if (progress.admitted_applied_index != certificate.cut.applied_index or progress.start != certificate.cut.retained_start or
            !std.mem.eql(u8, &progress.snapshot_certificate, &try certificate.digest())) return error.OnlineMergeReceiptMismatch;
        if (current.final_applied_index != 0 and (progress.through_sequence != current.final_sequence or progress.applied_index != current.final_applied_index or
            !std.mem.eql(u8, &progress.cut_digest, &current.final_cut_digest))) return error.OnlineMergeReceiptMismatch;
    }
    if (observation.receiver) |receipt| {
        if (!receipt.matches(.{ .transition_id = current.scope.fence.transition_id, .donor_group_id = current.scope.fence.owner_group_id, .receiver_group_id = current.scope.fence.peer_group_id, .identity_namespace = current.scope.receiver_namespace, .copy_attempt = current.scope.copy_attempt }) or
            !receipt.source.eql(try current.sourceIdentity()) or receipt.cursor.len > page.max_cursor_bytes or receipt.tail_offset > receipt.tail_total_effects or
            receipt.tail_sequence < current.acknowledged or (receipt.version != 2 and receipt.version != 3 and receipt.version != 4)) return error.OnlineMergeReceiptMismatch;
    }
}

pub fn decide(current: State, observation: Observation, cancel: bool) !Decision {
    try current.validate();
    try verify(current, observation);
    if (current.terminal()) return .wait;
    // An ordinary effect dispatched before metadata mode selection may win
    // native arbitration. Never switch the admitted record back to ordinary;
    // durably cancel, clean the exact legacy tuple, and revoke the new scope.
    if (current.phase == .admit and observation.ordinary_conflict) return advance(current, .cancel_receiver);
    // Once cutover might have been submitted, cancellation is no longer safe.
    if (cancel and @intFromEnum(current.phase) < @intFromEnum(Phase.cutover)) return advance(current, .cancel_receiver);
    const progress = observation.source_progress;
    switch (current.phase) {
        .admit => {
            if (progress) |value| {
                if (value.phase != .retaining) return error.OnlineMergeReceiptMismatch;
                if (value.snapshot_phase != .prepared) return advance(current, .publish);
            }
            return .{ .execute = .{ .source_command = .{ .admit = .{ .scope = current.scope } } } };
        },
        .publish => {
            const value = progress orelse return error.OnlineMergeReceiptMismatch;
            if (value.phase != .retaining or value.snapshot_phase == .prepared) return error.OnlineMergeReceiptMismatch;
            const certificate = observation.certificate orelse return .{ .execute = .prepare_certificate };
            if (!certificate.cut.namespace.eql(current.scope.fence.namespace) or certificate.cut.applied_index != value.admitted_applied_index or
                certificate.cut.retained_start != value.start) return error.OnlineMergeReceiptMismatch;
            if (value.snapshot_phase != .published) return .{ .execute = .{ .source_command = .{ .publish_certificate = .{ .scope = current.scope, .certificate = certificate } } } };
            if (!std.mem.eql(u8, &value.snapshot_certificate, &try certificate.digest())) return error.OnlineMergeReceiptMismatch;
            var next = current;
            next.phase = .snapshot;
            next.certificate = certificate;
            next.acknowledged = value.start;
            next.revision = try nextRevision(current);
            return .{ .advance = next };
        },
        .snapshot => {
            if (observation.receiver) |receipt| if (receipt.phase == .tail) return advance(current, .tail);
            return .{ .execute = .snapshot_page };
        },
        .tail, .final_tail => {
            const value = progress orelse return error.OnlineMergeReceiptMismatch;
            if (value.phase != (if (current.phase == .tail) @as(@TypeOf(value.phase), .retaining) else .fenced)) return error.OnlineMergeReceiptMismatch;
            const receipt = observation.receiver orelse return error.OnlineMergeReceiptMismatch;
            if ((receipt.phase != .tail and receipt.phase != .complete) or receipt.tail_sequence > observation.retained_head or value.acknowledged < current.acknowledged)
                return error.OnlineMergeReceiptMismatch;
            if (receipt.tail_sequence > value.acknowledged) return .{ .execute = .{ .source_command = .{ .acknowledge = .{ .scope = current.scope, .previous = value.acknowledged, .next = receipt.tail_sequence } } } };
            // ACK does not delete frames. Reclaim only the durable minimum
            // across all consumers, and expose actual progress so a slower
            // companion cannot create an endless zero-work maintenance loop.
            if (observation.retained_reclaimed < observation.retained_reclaimable)
                return .{ .execute = .{ .source_command = .{ .reclaim = .{ .scope = current.scope } } } };
            if (receipt.tail_sequence > current.acknowledged) {
                var next = current;
                next.acknowledged = receipt.tail_sequence;
                next.revision = try nextRevision(current);
                return .{ .advance = next };
            }
            if (current.phase == .tail and receipt.tail_offset == 0 and receipt.tail_sequence == observation.retained_head) return advance(current, .freeze);
            if (current.phase == .final_tail and receipt.phase == .complete) {
                if (receipt.tail_offset != 0 or receipt.assembly != null or receipt.tail_sequence != current.final_sequence or receipt.final_applied_index != current.final_applied_index or
                    !std.mem.eql(u8, &receipt.final_cut_digest, &current.final_cut_digest)) return error.OnlineMergeReceiptMismatch;
                return advance(current, .cutover);
            }
            return .{ .execute = .tail_page };
        },
        .freeze => {
            const value = progress orelse return error.OnlineMergeReceiptMismatch;
            if (value.phase == .released) return error.OnlineMergeReceiptMismatch;
            if (!observation.frozen) return .{ .execute = .freeze_and_drain };
            if (value.phase != .fenced) return .{ .execute = .{ .source_command = .{ .final_fence = .{ .scope = current.scope, .expected_sequence = observation.retained_head } } } };
            var next = current;
            next.phase = .final_tail;
            next.final_sequence = value.through_sequence;
            next.final_applied_index = value.applied_index;
            next.final_cut_digest = value.cut_digest;
            next.revision = try nextRevision(current);
            try next.validate();
            return .{ .advance = next };
        },
        .cutover => return if (observation.cutover_committed or observation.cutover_prepared) advance(current, .release) else .{ .execute = .cutover },
        .cancel_receiver => return if (observation.receiver_cancelled) advance(current, .cancel_release) else .{ .execute = .cancel_receiver },
        .release, .cancel_release => {
            // Rollback must both unfreeze the donor and permanently fence a
            // late admission. Dropping retention alone does neither.
            if (current.phase == .cancel_release and !observation.source_admission_closed) return .{ .execute = .cancel_source_admission };
            if (progress == null and current.phase == .cancel_release) return if (observation.source_admission_closed) advance(current, .cancelled) else .{ .execute = .cancel_source_admission };
            const value = progress orelse return error.OnlineMergeReceiptMismatch;
            // Cancellation may leave frames the receiver never needed. Once
            // this consumer is durably released, free the all-consumer-safe
            // prefix before retiring its last maintenance authority.
            if (value.phase == .released and observation.retained_reclaimed < observation.retained_reclaimable)
                return .{ .execute = .{ .source_command = .{ .reclaim = .{ .scope = current.scope } } } };
            if (value.phase == .released and value.local_cleanup_complete) return advance(current, if (current.phase == .release) .complete else .cancelled);
            return .{ .execute = .{ .source_command = .{ .release = current.scope } } };
        },
        .complete, .cancelled => unreachable,
    }
}

/// Installed only by a fully capable authenticated owner adapter. One call is
/// bounded to two endpoint observations, one effect OR one metadata CAS. The
/// callback must confirm the exact persisted successor, not just proposal ACK.
pub const Driver = struct {
    pub const Current = struct { state: State, cancel: bool };
    ptr: *anyopaque,
    capabilities: Capabilities,
    observe: *const fn (*anyopaque, State) anyerror!Observation,
    execute: *const fn (*anyopaque, State, Action) anyerror!void,
    /// Also compare durable rollback-intent presence with expected_cancel;
    /// cancellation and a pending cutover must not race through a state-only CAS.
    compare_and_set: *const fn (*anyopaque, State, State, expected_cancel: bool) anyerror!bool,
    release_observation: ?*const fn (*anyopaque, *Observation) void = null,
    current_state: ?*const fn (*anyopaque, State) anyerror!Current = null,
    /// Null means proven ineligible and permits the existing ordinary path.
    /// Transient discovery/commit errors must never silently fall back.
    admit: ?*const fn (*anyopaque, @import("transition_state.zig").MergeTransitionRecord) anyerror!?State = null,

    pub fn step(self: Driver, current: State, cancel: bool) !State {
        try self.capabilities.require();
        var canceled = cancel;
        if (self.current_state) |refresh| {
            const durable = try refresh(self.ptr, current);
            if (!std.meta.eql(durable.state.scope, current.scope) or durable.state.revision < current.revision) return error.OnlineMergeReceiptMismatch;
            if (!durable.state.eql(current)) return durable.state;
            canceled = durable.cancel;
        }
        var observation = try self.observe(self.ptr, current);
        defer if (self.release_observation) |release| release(self.ptr, &observation);
        switch (try decide(current, observation, canceled)) {
            .wait => return current,
            .execute => |action| {
                try self.execute(self.ptr, current, action);
                return current;
            },
            .advance => |next| {
                if (!updateAllowed(current, next)) return error.InvalidOnlineMergeState;
                if (!try self.compare_and_set(self.ptr, current, next, canceled)) return error.OnlineMergeRevisionChanged;
                return next;
            },
        }
    }
};

fn testState() State {
    return .{ .scope = .{
        .fence = .{ .transition_id = 9, .attempt = 1, .peer_group_id = 3, .owner_group_id = 2, .role = .merge_source, .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 4 }, .catalog_digest = @splat(7) },
        .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 5 },
        .consumer_epoch = 6,
        .copy_attempt = .{ .donor_term = 8, .sequence = 1 },
    } };
}

fn testCertificate(state: State) snapshot.Certificate {
    return .{ .cut = .{ .namespace = state.scope.fence.namespace, .applied_index = 19, .retained_start = 11 }, .objects = 1, .content_bytes = 100, .schema_manifest_digest = @splat(2), .ordered_content_digest = @splat(3) };
}

fn testObservation(state: State) !Observation {
    const certificate = testCertificate(state);
    return .{ .scope = state.scope, .certificate = certificate, .retained_head = 11, .source_progress = .{ .namespace = state.scope.namespace(), .consumer_epoch = state.scope.consumer_epoch, .pin = state.scope.pin(), .start = 11, .acknowledged = 11, .admitted_applied_index = 19, .snapshot_phase = .published, .snapshot_certificate = try certificate.digest() } };
}

fn testReceipt(state: State) !page.Progress {
    return .{ .version = 2, .transition_id = state.scope.fence.transition_id, .donor_group_id = state.scope.fence.owner_group_id, .receiver_group_id = state.scope.fence.peer_group_id, .receiver_namespace = state.scope.receiver_namespace, .attempt = state.scope.copy_attempt, .source = try state.sourceIdentity(), .phase = .tail, .tail_sequence = 11 };
}

test "metadata transition driver online reclaims acknowledged frames before healthy tail consumes quota" {
    var retained: @import("../storage/retained_effects.zig").State = .{ .latest = 30, .reclaimed = 11 };
    retained.consumers[0] = .{ .epoch = 1, .acknowledged = 20 };
    retained.consumers[1] = .{ .epoch = 2, .acknowledged = 15 };
    try std.testing.expectEqual(@as(u64, 15), retained.reclaimableThrough());
    retained.consumers[1] = .{};
    try std.testing.expectEqual(@as(u64, 20), retained.reclaimableThrough());
    retained.consumers[0] = .{};
    try std.testing.expectEqual(@as(u64, 30), retained.reclaimableThrough());
    var current = testState();
    var observed = try testObservation(current);
    current = (try decide(current, observed, false)).advance;
    current = (try decide(current, observed, false)).advance;
    observed.receiver = try testReceipt(current);
    current = (try decide(current, observed, false)).advance;
    observed.retained_head = 30;
    observed.retained_reclaimed = 11;
    observed.retained_reclaimable = 11;
    observed.receiver.?.tail_sequence = 20;
    const ack = (try decide(current, observed, false)).execute.source_command.acknowledge;
    try std.testing.expectEqual(@as(u64, 11), ack.previous);
    try std.testing.expectEqual(@as(u64, 20), ack.next);
    observed.source_progress.?.acknowledged = 20;
    observed.retained_reclaimable = 20;
    const reclaim = (try decide(current, observed, false)).execute.source_command.reclaim;
    try std.testing.expectEqualDeep(current.scope, reclaim.scope);
    try std.testing.expectEqual(@as(u8, 16), reclaim.frame_limit);
    try std.testing.expectEqual(@as(u32, 1024 * 1024), reclaim.byte_limit);
    // Lost ACK and restart repeat only bounded maintenance; its durable
    // watermark, not coordinator process memory, selects subsequent work.
    observed.retained_reclaimed = 18;
    try std.testing.expect((try decide(current, observed, false)).execute.source_command == .reclaim);
    observed.retained_reclaimed = 20;
    current = (try decide(current, observed, false)).advance;
    try std.testing.expectEqual(@as(u64, 20), current.acknowledged);
    try std.testing.expect((try decide(current, observed, false)).execute == .tail_page);
    // Another consumer's lower ACK prevents reclamation, not forward copy.
    observed.retained_reclaimed = 15;
    observed.retained_reclaimable = 15;
    try std.testing.expect((try decide(current, observed, false)).execute == .tail_page);
    observed.retained_reclaimable = 31;
    try std.testing.expectError(error.OnlineMergeReceiptMismatch, decide(current, observed, false));
    current.phase = .cancel_release;
    observed.source_admission_closed = true;
    observed.source_progress.?.phase = .released;
    observed.source_progress.?.local_cleanup_complete = true;
    observed.retained_reclaimable = 30;
    try std.testing.expect((try decide(current, observed, false)).execute.source_command == .reclaim);
    observed.retained_reclaimed = 30;
    try std.testing.expectEqual(Phase.cancelled, (try decide(current, observed, false)).advance.phase);
}

test "metadata transition driver online raw reservation excludes standalone native source authority" {
    const raw = @import("../data/storage/online_topology_arbitration.zig");
    const scope = testState().scope;
    const encoded = try (raw.Reservation{ .scope = scope }).encode();
    try std.testing.expectEqual(@as(usize, 2 + source.scope_encoded_size + 4), encoded.len);
    // The shared accelerated CRC must retain the durable reservation wire format.
    try std.testing.expectEqual(std.hash.Crc32.hash(encoded[0 .. encoded.len - 4]), std.mem.readInt(u32, encoded[encoded.len - 4 ..], .little));
    try std.testing.expectEqualDeep(scope, (try raw.Reservation.decode(&encoded)).scope);
    var old = encoded;
    old[0] = 1;
    try std.testing.expectError(error.InvalidOnlineTopologyReservation, raw.Reservation.decode(&old));
    var native_scope = scope;
    native_scope.authority = .native;
    native_scope.fence.role = .rewrite_source;
    native_scope.receiver_namespace.table_id += 1;
    native_scope.copy_attempt = .{ .donor_term = 0, .sequence = native_scope.consumer_epoch };
    try native_scope.validate();
    var reservation: ?raw.Reservation = null;
    try std.testing.expectEqual(@as(?raw.Rejection, .scope_changed), try raw.decide(&reservation, .{ .index = 1, .action = .{ .source = .{ .admit = .{ .scope = native_scope } } } }, false));
    try std.testing.expect(reservation == null);
    try std.testing.expectError(error.InvalidOnlineTopologyReservation, (raw.Reservation{ .scope = native_scope }).encode());
}

test "metadata transition driver online durable phases authenticate exact receiver receipts" {
    var current = testState();
    var observation = try testObservation(current);
    current = (try decide(current, observation, false)).advance;
    try std.testing.expectEqual(Phase.publish, current.phase);
    current = (try decide(current, observation, false)).advance;
    try std.testing.expectEqual(Phase.snapshot, current.phase);
    observation.receiver = try testReceipt(current);
    // Exercise the actual shared durable page receipt decoder, not a made-up
    // coordinator-only receipt representation.
    const encoded = try page.encode(std.testing.allocator, observation.receiver.?);
    defer std.testing.allocator.free(encoded);
    var decoded = try page.decode(std.testing.allocator, encoded);
    defer decoded.deinit();
    observation.receiver = decoded.value;
    current = (try decide(current, observation, false)).advance;
    try std.testing.expectEqual(Phase.tail, current.phase);
    var wrong = observation;
    wrong.receiver.?.attempt.sequence += 1;
    try std.testing.expectError(error.OnlineMergeReceiptMismatch, decide(current, wrong, false));
    wrong = observation;
    wrong.receiver.?.source.pin_digest[0] ^= 1;
    try std.testing.expectError(error.OnlineMergeReceiptMismatch, decide(current, wrong, false));
    // A partial frame may never authorize ACK of that frame.
    observation.receiver.?.tail_offset = 1;
    observation.receiver.?.tail_total_effects = 2;
    observation.receiver.?.tail_frame_digest = @splat(1);
    try std.testing.expectEqual(Action.tail_page, (try decide(current, observation, false)).execute);
    observation.receiver.?.tail_offset = 0;
    current = (try decide(current, observation, false)).advance;
    try std.testing.expectEqual(Phase.freeze, current.phase);
    try std.testing.expectEqual(Action.freeze_and_drain, (try decide(current, observation, false)).execute);
    observation.frozen = true;
    const final_control = (try decide(current, observation, false)).execute.source_command.final_fence;
    try std.testing.expectEqual(@as(u64, 11), final_control.expected_sequence);
    observation.source_progress.?.phase = .fenced;
    observation.source_progress.?.through_sequence = 11;
    observation.source_progress.?.applied_index = 25;
    observation.source_progress.?.cut_digest = @splat(9);
    current = (try decide(current, observation, false)).advance;
    observation.receiver.?.phase = .complete;
    observation.receiver.?.final_applied_index = 25;
    observation.receiver.?.final_cut_digest = @splat(8);
    try std.testing.expectError(error.OnlineMergeReceiptMismatch, decide(current, observation, false));
    observation.receiver.?.final_cut_digest = @splat(9);
    current = (try decide(current, observation, false)).advance;
    try std.testing.expectEqual(Phase.cutover, current.phase);
    // A cancellation racing ambiguous cutover must complete forward.
    try std.testing.expectEqual(Action.cutover, (try decide(current, observation, true)).execute);
    observation.cutover_committed = true;
    current = (try decide(current, observation, true)).advance;
    try std.testing.expectEqual(Phase.release, current.phase);
    observation.source_progress.?.phase = .released;
    observation.source_progress.?.local_cleanup_complete = false;
    _ = (try decide(current, observation, false)).execute.source_command.release;
    observation.source_progress.?.local_cleanup_complete = true;
    current = (try decide(current, observation, false)).advance;
    try std.testing.expect(current.terminal());
}

test "metadata transition driver online bounded effect retries survive lost metadata CAS" {
    const Fake = struct {
        durable: State,
        observation: Observation,
        effects: usize = 0,
        observations: usize = 0,
        lose_cas: bool = true,
        fn current(ptr: *anyopaque, _: State) !Driver.Current {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .state = self.durable, .cancel = false };
        }
        fn observe(ptr: *anyopaque, _: State) !Observation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.observations += 1;
            return self.observation;
        }
        fn execute(ptr: *anyopaque, _: State, action: Action) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = action.source_command.admit;
            self.effects += 1;
            self.observation = try testObservation(self.durable);
        }
        fn cas(ptr: *anyopaque, previous: State, next: State, _: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.durable.eql(previous)) return self.durable.eql(next);
            self.durable = next;
            if (self.lose_cas) {
                self.lose_cas = false;
                return error.TestLostReply;
            }
            return true;
        }
    };
    const initial = testState();
    var fake: Fake = .{ .durable = initial, .observation = .{ .scope = initial.scope } };
    var driver: Driver = .{ .ptr = &fake, .capabilities = .{}, .observe = Fake.observe, .execute = Fake.execute, .compare_and_set = Fake.cas };
    try std.testing.expectError(error.OnlineMergeUnavailable, driver.step(initial, false));
    try std.testing.expectEqual(@as(usize, 0), fake.observations);
    inline for (std.meta.fields(Capabilities)) |field| @field(driver.capabilities, field.name) = true;
    try std.testing.expect(initial.eql(try driver.step(initial, false)));
    try std.testing.expectEqual(@as(usize, 1), fake.effects);
    try std.testing.expectError(error.TestLostReply, driver.step(initial, false));
    // Reconstructed controller reads its replicated record, not old local phase.
    const recovered = fake.durable;
    try std.testing.expectEqual(Phase.publish, recovered.phase);
    driver.current_state = Fake.current;
    const observations_before_recovery = fake.observations;
    try std.testing.expect((try driver.step(initial, false)).eql(recovered));
    try std.testing.expectEqual(observations_before_recovery, fake.observations);
    try std.testing.expectEqual(Phase.snapshot, (try driver.step(recovered, false)).phase);
    try std.testing.expectEqual(@as(usize, 1), fake.effects);
    var stale = initial;
    stale.scope.copy_attempt.sequence += 1;
    try std.testing.expect(!updateAllowed(recovered, stale));
    var skip = recovered;
    skip.revision += 2;
    try std.testing.expect(!updateAllowed(recovered, skip));
}

test "metadata transition driver online cancellation waits for receiver and source durable cleanup" {
    var current = testState();
    var observation: Observation = .{ .scope = current.scope };
    var collided = observation;
    collided.ordinary_conflict = true;
    const revoked = (try decide(current, collided, false)).advance;
    try std.testing.expectEqual(Phase.cancel_receiver, revoked.phase);
    try std.testing.expect(updateAllowed(current, revoked));
    current = (try decide(current, observation, true)).advance;
    try std.testing.expectEqual(Action.cancel_receiver, (try decide(current, observation, true)).execute);
    observation.receiver_cancelled = true;
    current = (try decide(current, observation, true)).advance;
    try std.testing.expectEqual(Phase.cancel_release, current.phase);
    try std.testing.expectEqual(Action.cancel_source_admission, (try decide(current, observation, true)).execute);
    observation.source_admission_closed = true;
    current = (try decide(current, observation, true)).advance;
    try std.testing.expectEqual(Phase.cancelled, current.phase);
}

test "metadata transition driver online reconstructs every forward phase after effect and CAS reply loss" {
    const Harness = struct {
        durable: State,
        endpoint: Observation,
        fail_before_effect: bool = true,
        effects: usize = 0,
        cas_count: usize = 0,
        observed: usize = 0,
        released_observations: usize = 0,
        snapshots: usize = 0,
        tails: usize = 0,
        releases: usize = 0,
        fn current(ptr: *anyopaque, _: State) !Driver.Current {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .state = self.durable, .cancel = false };
        }
        fn observe(ptr: *anyopaque, _: State) !Observation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.observed += 1;
            // Endpoint receipts use the shared persisted codec, including
            // partial-frame and positioned snapshot forms.
            if (self.endpoint.receiver) |receipt| {
                const bytes = try page.encode(std.testing.allocator, receipt);
                defer std.testing.allocator.free(bytes);
                var decoded = try page.decode(std.testing.allocator, bytes);
                defer decoded.deinit();
                try std.testing.expectEqualDeep(receipt, decoded.value);
            }
            return self.endpoint;
        }
        fn release(ptr: *anyopaque, _: *Observation) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.released_observations += 1;
        }
        fn execute(ptr: *anyopaque, state: State, action: Action) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail_before_effect) {
                self.fail_before_effect = false;
                return error.TestBeforeEffect;
            }
            self.fail_before_effect = true;
            self.effects += 1;
            switch (action) {
                .source_command => |command| switch (command) {
                    .admit => {
                        self.endpoint = try testObservation(state);
                        self.endpoint.certificate = null;
                        self.endpoint.source_progress.?.snapshot_phase = .pinned;
                        self.endpoint.source_progress.?.snapshot_certificate = @splat(0);
                        self.endpoint.retained_head = 13;
                    },
                    .publish_certificate => |value| {
                        self.endpoint.source_progress.?.snapshot_phase = .published;
                        self.endpoint.source_progress.?.snapshot_certificate = try value.certificate.digest();
                    },
                    .acknowledge => |value| {
                        try std.testing.expectEqual(self.endpoint.source_progress.?.acknowledged, value.previous);
                        try std.testing.expectEqual(self.endpoint.receiver.?.tail_sequence, value.next);
                        self.endpoint.source_progress.?.acknowledged = value.next;
                    },
                    .final_fence => |value| {
                        try std.testing.expect(self.endpoint.frozen);
                        try std.testing.expectEqual(self.endpoint.retained_head, value.expected_sequence);
                        self.endpoint.source_progress.?.phase = .fenced;
                        self.endpoint.source_progress.?.through_sequence = value.expected_sequence;
                        self.endpoint.source_progress.?.applied_index = 25;
                        self.endpoint.source_progress.?.cut_digest = @splat(9);
                    },
                    .release => {
                        try std.testing.expect(self.endpoint.cutover_committed);
                        self.releases += 1;
                        self.endpoint.source_progress.?.phase = .released;
                        self.endpoint.source_progress.?.local_cleanup_complete = self.releases == 2;
                    },
                    .reclaim => return error.UnexpectedReclaim,
                },
                .prepare_certificate => self.endpoint.certificate = testCertificate(state),
                .snapshot_page => {
                    self.snapshots += 1;
                    var receipt = try testReceipt(state);
                    if (self.snapshots == 1) {
                        receipt.version = 4;
                        receipt.phase = .rows;
                        receipt.snapshot_position = .{ .object = 1, .offset = 4, .remaining = 1 };
                    }
                    self.endpoint.receiver = receipt;
                },
                .tail_page => {
                    self.tails += 1;
                    const receipt = &self.endpoint.receiver.?;
                    if (state.phase == .final_tail) {
                        receipt.phase = .complete;
                        receipt.tail_sequence = state.final_sequence;
                        receipt.final_applied_index = state.final_applied_index;
                        receipt.final_cut_digest = state.final_cut_digest;
                    } else if (self.tails == 1) {
                        receipt.tail_offset = 1;
                        receipt.tail_total_effects = 2;
                        receipt.tail_frame_digest = @splat(5);
                    } else {
                        receipt.tail_offset = 0;
                        receipt.tail_total_effects = 0;
                        receipt.tail_sequence += 1;
                    }
                },
                .freeze_and_drain => {
                    self.endpoint.frozen = true;
                    // A final ordinary write arrived before the fence.
                    self.endpoint.retained_head = 14;
                },
                .cutover => self.endpoint.cutover_prepared = true,
                .cancel_receiver, .cancel_source_admission => return error.UnexpectedCancellation,
            }
            return error.TestLostEffectReply;
        }
        fn cas(ptr: *anyopaque, previous: State, next: State, cancel: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(!cancel);
            try std.testing.expect(previous.eql(self.durable));
            try std.testing.expect(updateAllowed(previous, next));
            self.durable = next;
            if (next.phase == .release) self.endpoint.cutover_committed = true;
            self.cas_count += 1;
            return error.TestLostCasReply;
        }
        fn driver(self: *@This()) Driver {
            return .{ .ptr = self, .capabilities = .{ .replicated_source_pins = true, .native_retained_snapshots = true, .transferable_artifacts = true, .receiver_receipts = true, .transactional_headroom = true }, .current_state = current, .observe = observe, .release_observation = release, .execute = execute, .compare_and_set = cas };
        }
    };
    const initial = testState();
    var harness: Harness = .{ .durable = initial, .endpoint = .{ .scope = initial.scope } };
    var controller = initial;
    var visited = [_]bool{false} ** std.meta.fields(Phase).len;
    for (0..128) |_| {
        visited[@intFromEnum(controller.phase)] = true;
        if (controller.terminal()) break;
        const prior = harness.effects + harness.cas_count;
        // Reconstruct the adapter for every retry; only endpoint/metadata
        // durability survives. No process-local phase or acknowledgement does.
        controller = harness.driver().step(controller, false) catch |err| switch (err) {
            error.TestBeforeEffect, error.TestLostEffectReply, error.TestLostCasReply => controller,
            else => return err,
        };
        try std.testing.expect(harness.effects + harness.cas_count - prior <= 1);
        try std.testing.expect(std.meta.eql(initial.scope, controller.scope));
    }
    try std.testing.expectEqual(Phase.complete, controller.phase);
    for ([_]Phase{ .admit, .publish, .snapshot, .tail, .freeze, .final_tail, .cutover, .release, .complete }) |phase| try std.testing.expect(visited[@intFromEnum(phase)]);
    try std.testing.expectEqual(@as(usize, 2), harness.snapshots);
    try std.testing.expectEqual(@as(usize, 4), harness.tails);
    try std.testing.expectEqual(@as(usize, 2), harness.releases);
    try std.testing.expectEqual(harness.observed, harness.released_observations);
    const mutations = harness.effects + harness.cas_count;
    try std.testing.expect(controller.eql(try harness.driver().step(controller, true)));
    try std.testing.expectEqual(mutations, harness.effects + harness.cas_count);
}

test "metadata transition driver online cancellation matrix preserves the cutover boundary" {
    for ([_]Phase{ .admit, .publish, .snapshot, .tail, .freeze, .final_tail, .cutover, .release }) |phase| {
        var current = testState();
        current.phase = phase;
        var observation = try testObservation(current);
        if (phase != .admit and phase != .publish) {
            current.certificate = testCertificate(current);
            current.acknowledged = 11;
        }
        if (phase == .final_tail or phase == .cutover or phase == .release) {
            current.final_sequence = 11;
            current.final_applied_index = 25;
            current.final_cut_digest = @splat(9);
            observation.source_progress.?.phase = .fenced;
            observation.source_progress.?.through_sequence = 11;
            observation.source_progress.?.applied_index = 25;
            observation.source_progress.?.cut_digest = @splat(9);
        }
        if (phase == .cutover) {
            try std.testing.expectEqual(Action.cutover, (try decide(current, observation, true)).execute);
            observation.cutover_prepared = true;
            current = (try decide(current, observation, true)).advance;
            try std.testing.expectEqual(Phase.release, current.phase);
        }
        if (current.phase == .release) {
            _ = (try decide(current, observation, true)).execute.source_command.release;
            observation.source_progress.?.phase = .released;
            observation.source_progress.?.local_cleanup_complete = true;
            try std.testing.expectEqual(Phase.complete, (try decide(current, observation, true)).advance.phase);
            continue;
        }
        current = (try decide(current, observation, true)).advance;
        try std.testing.expectEqual(Phase.cancel_receiver, current.phase);
        // No receiver receipt (including never-started copy) is not proof of
        // rollback. Repeated lost responses leave the same cleanup action.
        for (0..2) |_| try std.testing.expectEqual(Action.cancel_receiver, (try decide(current, observation, false)).execute);
        observation.receiver_cancelled = true;
        current = (try decide(current, observation, false)).advance;
        try std.testing.expectEqual(Phase.cancel_release, current.phase);
        try std.testing.expectEqual(Action.cancel_source_admission, (try decide(current, observation, false)).execute);
        observation.source_admission_closed = true;
        observation.source_progress.?.phase = .released;
        observation.source_progress.?.local_cleanup_complete = false;
        _ = (try decide(current, observation, false)).execute.source_command.release;
        observation.source_progress.?.local_cleanup_complete = true;
        current = (try decide(current, observation, false)).advance;
        try std.testing.expectEqual(Phase.cancelled, current.phase);
        try std.testing.expectEqual(Decision.wait, try decide(current, observation, false));
    }
}

test "metadata transition driver online cancellation racing metadata CAS refreshes durable intent" {
    const Race = struct {
        durable: State,
        observation: Observation,
        cancel: bool = false,
        attempts: usize = 0,
        committed: usize = 0,
        fn current(ptr: *anyopaque, _: State) !Driver.Current {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .state = self.durable, .cancel = self.cancel };
        }
        fn observe(ptr: *anyopaque, _: State) !Observation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // Cancellation commits after this controller read current state
            // and before it tries the exact expected-cancel metadata CAS.
            self.cancel = true;
            return self.observation;
        }
        fn execute(_: *anyopaque, _: State, _: Action) !void {
            return error.UnexpectedEffect;
        }
        fn cas(ptr: *anyopaque, previous: State, next: State, expected_cancel: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.attempts += 1;
            if (self.cancel != expected_cancel or !self.durable.eql(previous)) return false;
            try std.testing.expect(updateAllowed(previous, next));
            self.durable = next;
            self.committed += 1;
            return true;
        }
    };
    for ([_]Phase{ .final_tail, .cutover }) |phase| {
        var initial = testState();
        initial.phase = phase;
        initial.certificate = testCertificate(initial);
        initial.acknowledged = 11;
        initial.final_sequence = 11;
        initial.final_applied_index = 25;
        initial.final_cut_digest = @splat(9);
        var observed = try testObservation(initial);
        observed.source_progress.?.phase = .fenced;
        observed.source_progress.?.through_sequence = 11;
        observed.source_progress.?.applied_index = 25;
        observed.source_progress.?.cut_digest = @splat(9);
        observed.receiver = try testReceipt(initial);
        observed.receiver.?.phase = .complete;
        observed.receiver.?.final_applied_index = 25;
        observed.receiver.?.final_cut_digest = @splat(9);
        observed.cutover_prepared = phase == .cutover;
        var race: Race = .{ .durable = initial, .observation = observed };
        const driver: Driver = .{ .ptr = &race, .capabilities = .{ .replicated_source_pins = true, .native_retained_snapshots = true, .transferable_artifacts = true, .receiver_receipts = true, .transactional_headroom = true }, .current_state = Race.current, .observe = Race.observe, .execute = Race.execute, .compare_and_set = Race.cas };
        try std.testing.expectError(error.OnlineMergeRevisionChanged, driver.step(initial, false));
        try std.testing.expectEqual(@as(usize, 0), race.committed);
        try std.testing.expect(initial.eql(race.durable));
        const resumed = try driver.step(initial, false);
        try std.testing.expectEqual(if (phase == .final_tail) Phase.cancel_receiver else Phase.release, resumed.phase);
        try std.testing.expectEqual(@as(usize, 2), race.attempts);
        try std.testing.expectEqual(@as(usize, 1), race.committed);
    }
}
