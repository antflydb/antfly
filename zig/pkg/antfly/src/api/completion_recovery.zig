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

//! Bounded terminal-decision recovery from qualified retained coordinator evidence.
//! Every participant, including guards/checkpoints, must have a qualified
//! resolution and immutable publication before this pass reports completion.
const std = @import("std");
const local = @import("antfly_local_sources");
const decision_ops = @import("history_decision_operations.zig");
const resolution_ops = @import("history_resolution_operations.zig");
const publication_ops = @import("history_publication_operations.zig");
const participant_ops = @import("completion_participant.zig");
const catalog_api = @import("table_catalog.zig");
const metadata = @import("../metadata/api.zig");
const tables = @import("../metadata/table_manager.zig");
const completion = decision_ops.wire.completion;
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;
pub const Outcome = enum { unproven, pending, aborted, incomplete, complete };

pub const Guard = struct { ptr: *anyopaque, check_fn: *const fn (*anyopaque, Context) anyerror!void };

pub const Recovery = struct {
    guard: ?Guard = null,
    catalog: catalog_api.CatalogSource,
    decisions: decision_ops.Port,
    resolutions: resolution_ops.Port,
    publications: publication_ops.Port,
    participants: participant_ops.Port,

    fn checkpoint(self: Recovery, context: Context) !void {
        try context.ensureActive();
        if (self.guard) |guard| try guard.check_fn(guard.ptr, context);
    }

    /// One bounded pass. Unknown mutations return immediately without replay;
    /// a later pass starts again with a fresh authoritative decision read.
    /// Completion is not retention-release authorization or a source ACK.
    pub fn run(self: Recovery, a: A, coordinator: completion.RoutedOwner, coordinator_fence: metadata.CatalogRouteFence, table: []const u8, transaction_id: [16]u8, original: Context) !Outcome {
        return self.runBound(a, coordinator, coordinator_fence, table, transaction_id, null, original);
    }
    pub fn runRetained(self: Recovery, a: A, job: local.storage_history_recovery_wire.Request, original: Context) !Outcome {
        try job.validate();
        var context = try original.platformDeadline();
        context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s);
        try self.checkpoint(context);
        var budget = self.catalog.budget(null);
        budget.deadline_ns = @min(budget.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io }) orelse std.math.maxInt(u64), budget.nowNs() +| 5 * std.time.ns_per_s);
        budget.cancellation = context.cancellation;
        var session = try catalog_api.RoutingSession.initWithBudget(a, self.catalog, budget);
        defer session.deinit();
        const fence = try route(&session, job.owner, job.coordinator_locator, budget, context);
        const locator = local.api_local_transaction_contract.parseParticipantRef(job.coordinator_locator) orelse return error.InvalidParticipant;
        return self.runBound(a, job.owner, fence, locator.table_name, job.transaction_id, job, context);
    }
    fn runBound(self: Recovery, a: A, coordinator: completion.RoutedOwner, coordinator_fence: metadata.CatalogRouteFence, table: []const u8, transaction_id: [16]u8, job: ?local.storage_history_recovery_wire.Request, original: Context) !Outcome {
        var scope = try publication_ops.Scope.init(coordinator_fence, original);
        const context = scope.context();
        try self.checkpoint(context);
        const request: decision_ops.Request = .{ .version = 1, .owner = coordinator, .transaction_id = transaction_id, .minimum_applied_index = 1 };
        var observed = try self.decisions.read(a, coordinator_fence, table, request, context);
        defer observed.deinit();
        try observed.value.validate(request.binding());
        try self.checkpoint(context);
        if (job) |expected| {
            const initial = observed.value.decision orelse return .unproven;
            if (!std.mem.eql(u8, &initial.admission.digest, &expected.admission_digest)) return error.SourceTransactionIdentityConflict;
            if (initial.record.status == .pending and expected.abort_before_ns != 0 and initial.record.created_at_ns < expected.abort_before_ns) {
                try self.checkpoint(context);
                self.participants.abortExpired(a, request.binding(), observed.value, coordinator_fence, expected.abort_before_ns, expected.resolution_timestamp_ns, context) catch |err| switch (err) {
                    error.DecisionConflict => {},
                    else => return err,
                };
                try self.checkpoint(context);
                const refreshed = try self.decisions.read(a, coordinator_fence, table, request, context);
                observed.deinit();
                observed = refreshed;
                try observed.value.validate(request.binding());
                if (observed.value.decision) |fresh| if (!std.mem.eql(u8, &fresh.admission.digest, &expected.admission_digest)) return error.SourceTransactionIdentityConflict;
            }
        }
        const decision = observed.value.decision orelse return .unproven;
        switch (decision.record.status) {
            .pending => return .pending,
            .aborted, .committed => {},
        }
        const admission = decision.admission.admission;
        var budget = self.catalog.budget(null);
        budget.deadline_ns = budget.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io });
        budget.cancellation = context.cancellation;
        var session = try catalog_api.RoutingSession.initWithBudget(a, self.catalog, budget);
        defer session.deinit();
        // Validate and retain every original route before any recovery write.
        const fences = try a.alloc(metadata.CatalogRouteFence, admission.participants.len);
        defer a.free(fences);
        for (admission.participants, fences) |member, *fence| {
            fence.* = try route(&session, try admission.recoveryOwner(member.namespace), member.locator, budget, context);
        }
        if (decision.record.status == .aborted) {
            for (fences, 0..) |fence, index| {
                try self.checkpoint(context);
                try self.checkpoint(context);
                try self.participants.submitRetained(a, request.binding(), observed.value, index, fence, .resolve, context);
            }
            try self.checkpoint(context);
            return .aborted;
        }
        const manifest = try decision.committedManifest();
        var assembly = try resolution_ops.wire.Assembly.init(a, manifest);
        defer assembly.deinit();
        for (admission.participants, fences, 0..) |member, fence, index| {
            try self.checkpoint(context);
            const owner = try admission.recoveryOwner(member.namespace);
            const locator = local.api_local_transaction_contract.parseParticipantRef(member.locator) orelse return error.InvalidParticipant;
            const input: resolution_ops.Request = .{ .version = 1, .authority = .raft, .namespace = member.namespace, .transaction_id = transaction_id, .owner_fence = owner.digest(), .manifest_digest = manifest.digest(), .commit_version = manifest.commit_version, .minimum_applied_index = 1 };
            var receipt = try self.resolutions.read(a, fence, locator.table_name, input, context);
            try receipt.validate(input.binding());
            if (receipt.resolution == null) {
                // A positive durable coordinator COMMIT, not absence, authorizes
                // idempotent terminal resolution. BEGIN/PREPARE are forbidden.
                try self.checkpoint(context);
                try self.participants.submitRetained(a, request.binding(), observed.value, index, fence, .resolve, context);
                receipt = try self.resolutions.read(a, fence, locator.table_name, input, context);
            }
            _ = try assembly.add(input.binding(), receipt);
        }
        try self.checkpoint(context);
        if (assembly.received != admission.participants.len) return .incomplete;
        const bytes = try assembly.encode(a);
        defer a.free(bytes);
        var parsed = try completion.parse(a, bytes);
        defer parsed.deinit();
        const digest = parsed.value.digest;
        var complete = true;
        for (admission.participants, fences, 0..) |member, fence, index| {
            try self.checkpoint(context);
            const owner = try admission.recoveryOwner(member.namespace);
            const locator = local.api_local_transaction_contract.parseParticipantRef(member.locator) orelse return error.InvalidParticipant;
            const input: publication_ops.Request = .{ .version = 1, .owner = owner, .transaction_id = transaction_id, .manifest_digest = manifest.digest(), .commit_version = manifest.commit_version, .minimum_applied_index = 1 };
            var publication = try self.publications.read(a, fence, locator.table_name, input, context);
            try publication.validate(input.binding());
            if (publication.publication == null) {
                try self.checkpoint(context);
                try self.participants.submitRetained(a, request.binding(), observed.value, index, fence, .{ .publish = bytes }, context);
                publication = try self.publications.read(a, fence, locator.table_name, input, context);
                try publication.validate(input.binding());
            }
            if (publication.publication) |evidence| {
                if (!std.mem.eql(u8, &digest, &evidence.digest)) return error.DecisionConflict;
            } else complete = false;
        }
        try self.checkpoint(context);
        return if (complete) .complete else .incomplete;
    }
};

fn route(session: *catalog_api.RoutingSession, owner: completion.RoutedOwner, locator_bytes: []const u8, budget: catalog_api.RoutingBudget, context: Context) !metadata.CatalogRouteFence {
    try context.ensureActive();
    try owner.validate();
    const locator = local.api_local_transaction_contract.parseParticipantRef(locator_bytes) orelse return error.InvalidParticipant;
    if (locator.group_id != owner.group_id or locator.restore_staging_scope != null or locator.restore_staging_plan_id != null) return error.InvalidParticipant;
    const snapshot = session.snapshot.value;
    if (snapshot.metadata_group_id != owner.metadata_group_id or snapshot.metadata_incarnation == null or !std.mem.eql(u8, &snapshot.metadata_incarnation.?, &owner.metadata_incarnation)) return error.TopologyChanged;
    const view = try session.tableRoutingView(locator.table_name, budget);
    if (view.table_id != std.mem.readInt(u64, owner.namespace[0..8], .big) or view.topology_epoch != owner.topology_epoch) return error.TopologyChanged;
    const table = snapshot.tables[session.table_id_indexes.get(view.table_id) orelse return error.TopologyChanged];
    if (table.storage.engine != .local or table.routing_storage_binding == null) return error.ChangefeedCapabilityUnsupported;
    if (!std.mem.eql(u8, &@import("../metadata/changefeed_control.zig").tableBinding(table), &owner.table_binding)) return error.TopologyChanged;
    const range = try session.rangeForGroup(view.table_id, owner.group_id, budget);
    const shard = tables.rangeDocIdentityShardId(range.*);
    const range_id = tables.rangeDocIdentityRangeId(range.*);
    if (shard != std.mem.readInt(u64, owner.namespace[8..16], .big) or range_id != std.mem.readInt(u64, owner.namespace[16..24], .big)) return error.TopologyChanged;
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = owner.metadata_group_id, .metadata_incarnation = owner.metadata_incarnation, .catalog_revision = snapshot.catalog_revision, .table_id = view.table_id, .topology_epoch = owner.topology_epoch, .route = .{ .group_id = owner.group_id, .range_id = range_id, .identity_namespace = .{ .table_id = view.table_id, .shard_id = shard, .range_id = range_id } }, .admission_deadline_ns = context.deadline_ns, .admission_deadline_io = context.deadline_io, .admission_cancellation = context.cancellation };
    try fence.validate();
    try budget.checkpoint();
    return fence;
}

test "data raft ordered history completion recovery retains guard checkpoints and reconciles unknown publication" {
    const a = std.testing.allocator;
    var records = [_]tables.TableRecord{.{ .table_id = 7, .name = "docs", .schema_json = "{}" }};
    records[0].routing_storage_binding = local.common_topology_records.tableStorageBinding(records[0]);
    const ranges = [_]tables.RangeRecord{
        .{ .table_id = 7, .group_id = 9, .range_id = 9, .start_key = "", .end_key = "m", .doc_identity_shard_id = 9, .doc_identity_range_id = 9 },
        .{ .table_id = 7, .group_id = 10, .range_id = 10, .start_key = "m", .end_key = null, .doc_identity_shard_id = 10, .doc_identity_range_id = 10 },
    };
    const generation = try catalog_api.RoutingGeneration.create(a, .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .catalog_revision = 10, .tables = &records, .ranges = @constCast(&ranges) }, .{});
    defer generation.release();
    const view = try generation.indexed.tableRoutingView("docs", .{});
    var owners: [2]completion.RoutedOwner = undefined;
    var members: [2]completion.Participant = undefined;
    const locators = [_][]const u8{ "table2:00000004:docs:9", "table2:00000004:docs:10" };
    for (&owners, &members, 0..) |*owner, *member, i| {
        var namespace: [24]u8 = undefined;
        std.mem.writeInt(u64, namespace[0..8], 7, .big);
        std.mem.writeInt(u64, namespace[8..16], 9 + i, .big);
        std.mem.writeInt(u64, namespace[16..24], 9 + i, .big);
        owner.* = .{ .metadata_group_id = 1, .metadata_incarnation = generation.indexed.snapshot.value.metadata_incarnation.?, .namespace = namespace, .table_binding = @import("../metadata/changefeed_control.zig").tableBinding(records[0]), .root_generation = i, .group_id = 9 + i, .topology_epoch = view.topology_epoch };
        member.* = .{ .namespace = namespace, .owner_fence = owner.digest(), .roles = if (i == 0) completion.Roles.guards else completion.Roles.checkpoint, .input_digest = @splat(4), .locator = locators[i], .routed_owner = owner.* };
    }
    const admission: completion.Admission = .{ .version = 3, .transaction_id = @splat(5), .admission_digest = @splat(6), .coordinator_namespace = owners[0].namespace, .coordinator_fence = owners[0].digest(), .participants = &members };
    const Fake = struct {
        generation: *catalog_api.RoutingGeneration,
        admission: completion.Admission,
        resolved: [2]bool = .{ true, false },
        published: [2]bool = .{ false, false },
        publication_digest: [32]u8 = @splat(0),
        resolves: usize = 0,
        publishes: usize = 0,
        lose_publication: bool = false,
        suppress_resolution: bool = false,
        state: decision_ops.wire.Record = .{ .status = .committed, .begin_timestamp = 10, .created_at_ns = 11, .commit_version = 20, .finalized_at_ns = 20, .prepared = true, .intents_resolved = true, .digest = @splat(7) },
        absent: bool = false,
        cancelled: std.atomic.Value(bool) = .init(false),
        cancel_final_read: bool = false,
        follower_reads: usize = 0,
        fn acquire(ptr: *anyopaque, budget: catalog_api.RoutingBudget, authoritative: bool) !*catalog_api.RoutingGeneration {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try budget.checkpoint();
            try std.testing.expect(authoritative);
            self.generation.retain();
            return self.generation;
        }
        fn admin(_: *anyopaque) !metadata.AdminSnapshot {
            return error.TestUnexpectedResult;
        }
        fn decision(ptr: *anyopaque, alloc: A, _: metadata.CatalogRouteFence, _: []const u8, input: decision_ops.Request, _: Context) !decision_ops.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const raw = try decision_ops.wire.encodeReply(alloc, input.binding(), .{ .request_digest = input.binding().digest(), .cut = .{ .term = 1, .index = 8 }, .decision = if (self.absent) null else .{ .record = self.state, .admission = .{ .admission = self.admission, .digest = self.admission.digest() } } });
            defer alloc.free(raw);
            return decision_ops.decodeReply(alloc, raw, input);
        }
        fn resolution(ptr: *anyopaque, _: A, fence: metadata.CatalogRouteFence, _: []const u8, input: resolution_ops.Request, _: Context) !resolution_ops.wire.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const index: usize = @intCast(fence.route.group_id - 9);
            if (index == 1) {
                self.follower_reads += 1;
                if (self.cancel_final_read and self.follower_reads == 2) self.cancelled.store(true, .release);
            }
            const manifest = try self.admission.manifest(20);
            var position: [32]u8 = @splat(0);
            std.mem.writeInt(u64, position[0..8], 1, .little);
            std.mem.writeInt(u64, position[8..16], 3, .little);
            position[31] = 1;
            const receipt: completion.Resolution = .{ .namespace = input.namespace, .owner_fence = input.owner_fence, .manifest_digest = manifest.digest(), .commit_version = 20, .source_position = position, .outcome = .{ .no_effect = completion.noEffectProof(manifest, input.namespace, position) } };
            return resolution_ops.wire.Reply.init(input.binding(), .{ .term = 1, .index = 8 }, if (self.resolved[index]) receipt else null);
        }
        fn publication(ptr: *anyopaque, _: A, fence: metadata.CatalogRouteFence, _: []const u8, input: publication_ops.Request, _: Context) !publication_ops.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const index: usize = @intCast(fence.route.group_id - 9);
            return publication_ops.Reply.init(input.binding(), .{ .term = 1, .index = 8 }, if (self.published[index]) .{ .digest = self.publication_digest, .retained_bytes = 1024, .position = .{ .term = 1, .index = 7 } } else null);
        }
        fn write(ptr: *anyopaque, alloc: A, _: @import("internal_group_operations.zig").RoutedBatchAuthority, group: u64, _: []const u8, request: local.storage_db_selected_root.db.types.BatchRequest, _: @import("internal_batch_forwarding.zig").Context, _: Context) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const index: usize = @intCast(group - 9);
            try std.testing.expectEqual(.write, request.sync_level);
            switch (request.transaction.?) {
                .resolve => |terminal| {
                    try std.testing.expectEqual(@as(local.storage_db_selected_root.db.types.TxnStatus, if (self.state.status == .committed) .committed else .aborted), terminal.status);
                    try std.testing.expectEqual(@as(u64, if (self.state.status == .committed) 20 else 30), terminal.commit_version);
                    if (self.state.status == .pending and terminal.status == .aborted) {
                        self.state.status = .aborted;
                        self.state.commit_version = 0;
                        self.state.finalized_at_ns = terminal.commit_version;
                        self.state.intents_resolved = true;
                    }
                    self.resolves += 1;
                    if (!self.suppress_resolution) self.resolved[index] = true;
                },
                .publish_completion => |value| {
                    try std.testing.expect(self.resolved[0] and self.resolved[1]);
                    var certificate = try completion.parse(alloc, value.certificate);
                    defer certificate.deinit();
                    try std.testing.expectEqual(@as(usize, 2), certificate.value.certificate.resolutions.len);
                    self.publication_digest = certificate.value.digest;
                    self.published[index] = true;
                    self.publishes += 1;
                    if (self.lose_publication) {
                        self.lose_publication = false;
                        return error.RaftBatchWriteOutcomeUnknown;
                    }
                },
                else => return error.TestUnexpectedResult,
            }
            return {};
        }
    };
    var fake: Fake = .{ .generation = generation, .admission = admission };
    const recovery: Recovery = .{ .catalog = .{ .ptr = &fake, .vtable = &.{ .admin_snapshot = Fake.admin, .free_admin_snapshot = undefined, .acquire_routing_generation = Fake.acquire } }, .decisions = .{ .ptr = &fake, .read_fn = Fake.decision }, .resolutions = .{ .ptr = &fake, .read_fn = Fake.resolution }, .publications = .{ .ptr = &fake, .read_fn = Fake.publication }, .participants = .{ .writer = .{ .ptr = &fake, .write_fn = Fake.write } } };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = owners[0].metadata_incarnation, .catalog_revision = 10, .table_id = 7, .topology_epoch = view.topology_epoch, .route = .{ .group_id = 9, .range_id = 9, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 9 } } };
    try std.testing.expectEqual(.complete, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    try std.testing.expectEqual(@as(usize, 1), fake.resolves);
    try std.testing.expectEqual(@as(usize, 2), fake.publishes);
    try std.testing.expectEqual(.complete, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    try std.testing.expectEqual(@as(usize, 2), fake.publishes);
    fake.published = .{ false, false };
    fake.lose_publication = true;
    try std.testing.expectError(error.RaftBatchWriteOutcomeUnknown, recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    try std.testing.expectEqual(@as(usize, 3), fake.publishes);
    try std.testing.expectEqual(.complete, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    try std.testing.expectEqual(@as(usize, 4), fake.publishes);
    fake.resolved[1] = false;
    fake.published = .{ false, false };
    fake.suppress_resolution = true;
    try std.testing.expectEqual(.incomplete, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    try std.testing.expectEqual(@as(usize, 4), fake.publishes);
    fake.cancel_final_read = true;
    fake.follower_reads = 0;
    try std.testing.expectError(error.Canceled, recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{ .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&fake.cancelled) }));
    try std.testing.expectEqual(@as(usize, 4), fake.publishes);
    fake.cancel_final_read = false;
    fake.cancelled.store(false, .release);
    fake.absent = true;
    try std.testing.expectEqual(.unproven, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    fake.absent = false;
    fake.state.status = .pending;
    fake.state.commit_version = 0;
    fake.state.finalized_at_ns = 0;
    fake.state.intents_resolved = false;
    try std.testing.expectEqual(.pending, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    fake.state.status = .aborted;
    fake.state.finalized_at_ns = 30;
    try std.testing.expectEqual(.aborted, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    try std.testing.expectEqual(@as(usize, 4), fake.publishes);
    try std.testing.expectEqual(@as(usize, 5), fake.resolves);
    var retained: local.storage_history_recovery_wire.Request = .{ .owner = owners[0], .transaction_id = admission.transaction_id, .admission_digest = admission.digest(), .coordinator_locator = members[0].locator, .abort_before_ns = 20, .resolution_timestamp_ns = 30 };
    retained.admission_digest[0] ^= 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, recovery.runRetained(a, retained, .{}));
    try std.testing.expectEqual(@as(usize, 5), fake.resolves);
    retained.admission_digest = admission.digest();
    fake.state.status = .pending;
    fake.state.finalized_at_ns = 0;
    fake.state.intents_resolved = false;
    retained.abort_before_ns = 0;
    try std.testing.expectEqual(.pending, try recovery.runRetained(a, retained, .{}));
    try std.testing.expectEqual(@as(usize, 5), fake.resolves);
    retained.abort_before_ns = 20;
    try std.testing.expectEqual(.aborted, try recovery.runRetained(a, retained, .{}));
    try std.testing.expectEqual(@as(usize, 8), fake.resolves);
    try std.testing.expectEqual(@as(usize, 4), fake.publishes);
}
