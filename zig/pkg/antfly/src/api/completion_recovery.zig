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

pub const SourceExpectation = struct {
    admission_digest: completion.Digest,
    scope: local.storage_source_scope.Reference,
    purpose: [32]u8,
    begin_timestamp: u64,
    commit_version: u64,
    pub fn validate(self: SourceExpectation) !void {
        try self.scope.validate();
        if (std.mem.allEqual(u8, &self.admission_digest, 0) or std.mem.allEqual(u8, &self.purpose, 0) or self.begin_timestamp == 0 or self.commit_version <= self.begin_timestamp) return error.InvalidSourceAdmission;
    }
};

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
        return self.runBound(a, coordinator, coordinator_fence, table, transaction_id, null, null, original);
    }
    /// Completion of an accepted source plan must match the original retained
    /// admission and scoped ownership before any terminal/publication mutation.
    pub fn runSource(self: Recovery, a: A, coordinator: completion.RoutedOwner, coordinator_fence: metadata.CatalogRouteFence, table: []const u8, transaction_id: [16]u8, expected: SourceExpectation, original: Context) !Outcome {
        try expected.validate();
        return self.runBound(a, coordinator, coordinator_fence, table, transaction_id, null, expected, original);
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
        const fence = try routeRetainedOwner(&session, job.owner, job.coordinator_locator, budget, context);
        const locator = local.api_local_transaction_contract.parseParticipantRef(job.coordinator_locator) orelse return error.InvalidParticipant;
        return self.runBound(a, job.owner, fence, locator.table_name, job.transaction_id, job, null, context);
    }
    /// Release only a completed transaction whose owning cursor is durable.
    pub fn releaseRetained(self: Recovery, a: A, job: local.storage_history_recovery_wire.Request, terminal: enum { committed, aborted }, original: Context) !void {
        try job.validate();
        var context = try original.platformDeadline();
        context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s);
        try self.checkpoint(context);
        var budget = self.catalog.budget(null);
        budget.deadline_ns = budget.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io });
        budget.cancellation = context.cancellation;
        var session = try catalog_api.RoutingSession.initWithBudget(a, self.catalog, budget);
        defer session.deinit();
        const fence = try routeRetainedOwner(&session, job.owner, job.coordinator_locator, budget, context);
        const locator = local.api_local_transaction_contract.parseParticipantRef(job.coordinator_locator) orelse return error.InvalidParticipant;
        const query: decision_ops.Request = .{ .version = 1, .owner = job.owner, .transaction_id = job.transaction_id, .minimum_applied_index = 1 };
        var observed = try self.decisions.read(a, fence, locator.table_name, query, context);
        defer observed.deinit();
        try observed.value.validate(query.binding());
        const decision = observed.value.decision orelse return;
        if (!std.mem.eql(u8, &decision.admission.digest, &job.admission_digest)) return error.SourceTransactionIdentityConflict;
        const expected_status: @TypeOf(decision.record.status) = if (terminal == .committed) .committed else .aborted;
        if (decision.record.status != expected_status) return error.SourceOutcomeUnresolved;
        try self.checkpoint(context);
        try self.participants.acknowledgeRetainedCoordinator(a, job.owner, fence, job.coordinator_locator, job.transaction_id, context);
    }

    fn runBound(self: Recovery, a: A, coordinator: completion.RoutedOwner, coordinator_fence: metadata.CatalogRouteFence, table: []const u8, transaction_id: [16]u8, job: ?local.storage_history_recovery_wire.Request, source_expected: ?SourceExpectation, original: Context) !Outcome {
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
            if (initial.record.status == .pending and initial.record.source_purpose == null and expected.abort_before_ns != 0 and initial.record.created_at_ns < expected.abort_before_ns) {
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
        if (source_expected) |expected| {
            if (!std.mem.eql(u8, &decision.admission.digest, &expected.admission_digest) or decision.record.begin_timestamp != expected.begin_timestamp or
                !std.meta.eql(decision.record.source_scope, @as(?local.storage_source_scope.Reference, expected.scope)) or
                !std.meta.eql(decision.record.source_purpose, @as(?[32]u8, expected.purpose))) return error.SourceTransactionIdentityConflict;
            switch (decision.record.status) {
                .pending => return error.SourceOutcomeUnresolved,
                .aborted => return error.DecisionConflict,
                .committed => if (decision.record.commit_version != expected.commit_version) return error.SourceTransactionIdentityConflict,
            }
        }
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
            fence.* = try routeRetainedOwner(&session, (member.routed_owner orelse return error.ChangefeedCapabilityUnsupported), member.locator, budget, context);
        }
        if (decision.record.status == .aborted) {
            for (fences, 0..) |fence, index| {
                try self.checkpoint(context);
                try self.participants.submitRetained(a, request.binding(), observed.value, index, fence, .resolve, context);
            }
            try self.checkpoint(context);
            try self.participants.acknowledgeResolved(a, request.binding(), observed.value, coordinator_fence, context);
            return .aborted;
        }
        const manifest = try decision.committedManifest();
        var assembly = try resolution_ops.wire.Assembly.init(a, manifest);
        defer assembly.deinit();
        for (admission.participants, fences, 0..) |member, fence, index| {
            try self.checkpoint(context);
            const owner = member.routed_owner orelse return error.ChangefeedCapabilityUnsupported;
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
            const owner = member.routed_owner orelse return error.ChangefeedCapabilityUnsupported;
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
        if (!complete) return .incomplete;
        try self.participants.acknowledgeResolved(a, request.binding(), observed.value, coordinator_fence, context);
        return .complete;
    }
};

pub fn routeRetainedOwner(session: *catalog_api.RoutingSession, owner: completion.RoutedOwner, locator_bytes: []const u8, budget: catalog_api.RoutingBudget, context: Context) !metadata.CatalogRouteFence {
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
        acknowledgments: usize = 0,
        expect_coordinator_release: bool = false,
        lose_publication: bool = false,
        suppress_resolution: bool = false,
        state: decision_ops.wire.Record = .{ .status = .committed, .begin_timestamp = 10, .created_at_ns = 11, .commit_version = 20, .finalized_at_ns = 20, .prepared = true, .intents_resolved = true, .digest = @splat(7) },
        absent: bool = false,
        unsupported_owners: [2]bool = .{ false, false },
        retention_limit_bytes: u64 = 1024 * 1024,
        visibility_errors: [2]?anyerror = .{ null, null },
        visibility_calls: [2]usize = .{ 0, 0 },
        decision_reads: usize = 0,
        capture_epoch_delta: u64 = 0,
        admin_frees: usize = 0,
        omit_admin_ranges: bool = false,
        source_recovery_mode: bool = false,
        begins: usize = 0,
        prepares: usize = 0,
        lose_prepare_once: bool = false,
        cancelled: std.atomic.Value(bool) = .init(false),
        cancel_final_read: bool = false,
        follower_reads: usize = 0,
        fn capture(ptr: *anyopaque, _: A, input_fence: metadata.CatalogRouteFence, _: []const u8, _: @import("history_owner_operations.zig").Request, _: Context) !@import("history_owner_operations.zig").Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const index: usize = @intCast(input_fence.route.group_id - 9);
            if (self.unsupported_owners[index]) return error.ChangefeedCapabilityUnsupported;
            var owner = self.admission.participants[index].routed_owner.?;
            owner.topology_epoch += self.capture_epoch_delta;
            return .{ .owner = owner, .observation = try @import("history_owner_operations.zig").wire.Reply.init(.{ .owner = owner }, .{ .term = 1, .index = 8 }, self.retention_limit_bytes) };
        }
        fn acquire(ptr: *anyopaque, budget: catalog_api.RoutingBudget, authoritative: bool) !*catalog_api.RoutingGeneration {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try budget.checkpoint();
            try std.testing.expect(authoritative);
            self.generation.retain();
            return self.generation;
        }
        fn admin(ptr: *anyopaque) !metadata.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const snapshot = self.generation.indexed.snapshot.value;
            return .{ .status = .{ .metadata_group_id = snapshot.metadata_group_id, .metadata_incarnation = snapshot.metadata_incarnation, .metadata_epoch = snapshot.catalog_revision, .metrics = .{} }, .tables = snapshot.tables, .ranges = if (self.omit_admin_ranges) &.{} else snapshot.ranges, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn freeAdmin(ptr: *anyopaque, _: *metadata.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.admin_frees += 1;
        }
        fn decision(ptr: *anyopaque, alloc: A, _: metadata.CatalogRouteFence, _: []const u8, input: decision_ops.Request, _: Context) !decision_ops.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.decision_reads += 1;
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
            if (request.sync_level != .write) {
                try std.testing.expectEqual(.full_index, request.sync_level);
                try std.testing.expect(self.published[0] and self.published[1]);
                try std.testing.expect(request.transaction.? == .resolve);
                try std.testing.expectEqual(.committed, request.transaction.?.resolve.status);
                try std.testing.expectEqualDeep(self.state.source_purpose, request.transaction.?.resolve.source_purpose);
                try std.testing.expectEqual(@as(usize, 0), request.writes.len);
                try std.testing.expectEqual(@as(usize, 0), request.transforms.len);
                self.visibility_calls[index] += 1;
                if (self.visibility_errors[index]) |err| return err;
                return {};
            }

            switch (request.transaction.?) {
                .begin => |begin| {
                    try std.testing.expect(self.source_recovery_mode);
                    try std.testing.expectEqualDeep(self.state.source_scope, begin.source_scope);
                    try std.testing.expectEqualDeep(self.state.source_purpose, begin.source_purpose);
                    try std.testing.expectEqual(self.state.created_at_ns, begin.created_at_ns);
                    self.begins += 1;
                },
                .prepare => |prepare| {
                    try std.testing.expect(self.source_recovery_mode);
                    try std.testing.expectEqualDeep(self.state.source_scope, prepare.source_scope);
                    try std.testing.expectEqualDeep(self.state.source_purpose, prepare.source_purpose);
                    self.prepares += 1;
                    if (self.lose_prepare_once) {
                        self.lose_prepare_once = false;
                        return error.RaftBatchWriteOutcomeUnknown;
                    }
                    if (prepare.source_checkpoint != null) self.state.prepared = true;
                },
                .resolve => |terminal| {
                    if (self.source_recovery_mode and self.state.status == .pending and terminal.status == .committed) {
                        try std.testing.expect(self.state.prepared);
                        self.state.status = .committed;
                        self.state.commit_version = terminal.commit_version;
                        self.state.finalized_at_ns = terminal.commit_version;
                        self.state.intents_resolved = true;
                    }
                    try std.testing.expectEqualDeep(self.state.source_purpose, terminal.source_purpose);
                    try std.testing.expectEqual(@as(local.storage_db_selected_root.db.types.TxnStatus, if (self.state.status == .committed) .committed else .aborted), terminal.status);
                    try std.testing.expectEqual(@as(u64, if (self.state.status == .committed) 20 else 30), terminal.commit_version);
                    if (self.state.status == .pending and terminal.status == .aborted) {
                        self.state.status = .aborted;
                        self.state.commit_version = 0;
                        self.state.finalized_at_ns = terminal.commit_version;
                        self.state.intents_resolved = true;
                    }
                    self.resolves += 1;
                    if (terminal.status == .aborted or !self.suppress_resolution) self.resolved[index] = true;
                },
                .acknowledge_many => |value| {
                    self.acknowledgments += 1;
                    if (self.state.status == .committed) {
                        try std.testing.expect(self.published[0] and self.published[1]);
                    } else {
                        try std.testing.expect(self.state.status == .aborted and self.resolved[0] and self.resolved[1]);
                    }
                    try std.testing.expectEqualSlices(u8, &self.admission.transaction_id, &value.txn_id);
                    const coordinator = self.admission.coordinator_namespace;
                    if (self.expect_coordinator_release) {
                        try std.testing.expectEqual(@as(usize, 1), value.participants.len);
                        try std.testing.expectEqualStrings((try self.admission.member(coordinator)).locator, value.participants[0]);
                        return {};
                    }
                    const expected_count: usize = if (self.state.retain_terminal) 1 else 2;
                    try std.testing.expectEqual(expected_count, value.participants.len);
                    const follower = self.admission.participants[if (std.mem.eql(u8, &coordinator, &self.admission.participants[0].namespace)) 1 else 0];
                    try std.testing.expectEqualStrings(follower.locator, value.participants[0]);
                    if (!self.state.retain_terminal) try std.testing.expectEqualStrings((try self.admission.member(coordinator)).locator, value.participants[1]);
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
    const recovery: Recovery = .{ .catalog = .{ .ptr = &fake, .vtable = &.{ .admin_snapshot = Fake.admin, .free_admin_snapshot = Fake.freeAdmin, .acquire_routing_generation = Fake.acquire } }, .decisions = .{ .ptr = &fake, .read_fn = Fake.decision }, .resolutions = .{ .ptr = &fake, .read_fn = Fake.resolution }, .publications = .{ .ptr = &fake, .read_fn = Fake.publication }, .participants = .{ .writer = .{ .ptr = &fake, .write_fn = Fake.write } } };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = owners[0].metadata_incarnation, .catalog_revision = 10, .table_id = 7, .topology_epoch = view.topology_epoch, .route = .{ .group_id = 9, .range_id = 9, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 9 } } };
    try std.testing.expectEqual(.complete, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    fake.state.retain_terminal = false;
    try std.testing.expectEqual(.complete, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    fake.state.retain_terminal = true;
    try std.testing.expectEqual(@as(usize, 2), fake.acknowledgments);
    fake.state.acknowledgments_pending = false;
    try std.testing.expectEqual(.complete, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    try std.testing.expectEqual(@as(usize, 2), fake.acknowledgments);
    fake.state.acknowledgments_pending = true;
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
    var page_job: local.storage_history_recovery_wire.Request = .{
        .owner = owners[0],
        .transaction_id = admission.transaction_id,
        .admission_digest = admission.digest(),
        .coordinator_locator = members[0].locator,
        .resolution_timestamp_ns = 20,
    };
    const before_release = fake.acknowledgments;
    page_job.admission_digest[0] ^= 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, recovery.releaseRetained(a, page_job, .committed, .{}));
    try std.testing.expectEqual(before_release, fake.acknowledgments);
    page_job.admission_digest = admission.digest();
    fake.expect_coordinator_release = true;
    try recovery.releaseRetained(a, page_job, .committed, .{});
    fake.expect_coordinator_release = false;
    try std.testing.expectEqual(before_release + 1, fake.acknowledgments);
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
    fake.state.retain_terminal = false;
    fake.resolved = .{ false, false };
    try std.testing.expectEqual(.aborted, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    fake.state.retain_terminal = true;
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
    // A stale or incorrectly classified ordinary job cannot expire a source
    // transaction. Ownership is read afresh from the native retained record.
    fake.state.source_purpose = @splat(17);
    try std.testing.expectEqual(.pending, try recovery.runRetained(a, retained, .{}));
    try std.testing.expectEqual(@as(usize, 5), fake.resolves);
    const source_query: decision_ops.Request = .{ .version = 1, .owner = owners[0], .transaction_id = admission.transaction_id, .minimum_applied_index = 1 };
    var source_observation = try recovery.decisions.read(a, fence, "docs", source_query, .{});
    defer source_observation.deinit();
    try std.testing.expectError(error.SourceOutcomeUnresolved, recovery.participants.abortExpired(a, source_query.binding(), source_observation.value, fence, 20, 30, .{}));
    try std.testing.expectEqual(@as(usize, 5), fake.resolves);
    fake.state.source_purpose = null;
    try std.testing.expectEqual(.aborted, try recovery.runRetained(a, retained, .{}));
    try std.testing.expectEqual(@as(usize, 8), fake.resolves);
    try std.testing.expectEqual(@as(usize, 4), fake.publishes);

    const before_abort_release = fake.acknowledgments;
    retained.abort_before_ns = 0;
    try std.testing.expectError(error.SourceOutcomeUnresolved, recovery.releaseRetained(a, retained, .committed, .{}));
    try std.testing.expectEqual(before_abort_release, fake.acknowledgments);
    fake.expect_coordinator_release = true;
    try recovery.releaseRetained(a, retained, .aborted, .{});
    fake.expect_coordinator_release = false;
    try std.testing.expectEqual(before_abort_release + 1, fake.acknowledgments);

    // The normal coordinator uses the same retained two-owner state. A
    // visibility-only retry must finish both barriers, preserve simultaneous
    // repair/retry debt, and never reconstruct a BEGIN or PREPARE.
    const admission_mod = @import("completion_admission.zig");
    const plan_mod = @import("completion_plan.zig");
    const native_types = local.storage_db_selected_root.db.types;
    var frozen_inputs: [2]plan_mod.Input = undefined;
    for (&frozen_inputs, owners, locators) |*input, owner, locator| input.* = .{
        .owner = owner,
        .locator = locator,
        .roles = completion.Roles.guards,
        .prepare = try (native_types.TransactionIntentRequest{ .completion_owner = owner }).completionBatch(admission.transaction_id, 10, owner.topology_epoch),
    };
    var frozen = try plan_mod.Plan.freezeDerived(a, admission.transaction_id, owners[0], &frozen_inputs, 11);
    defer frozen.deinit();
    fake.admission = frozen.admission;
    fake.state = .{ .status = .committed, .begin_timestamp = 10, .created_at_ns = 11, .commit_version = 20, .finalized_at_ns = 20, .prepared = true, .intents_resolved = true, .digest = @splat(7) };
    fake.resolved = .{ true, true };
    fake.published = .{ false, false };
    fake.suppress_resolution = false;
    fake.visibility_errors = .{ error.EnrichmentWorkerFailed, error.EnrichmentRetryInProgress };
    const coordinator: admission_mod.Coordinator = .{ .owners = .{ .ptr = &fake, .read_fn = Fake.capture }, .recovery = recovery };
    const normal_inputs = [_]admission_mod.Input{
        .{ .table_name = "docs", .group_id = 9, .topology_epoch = owners[0].topology_epoch, .intent = .{} },
        .{ .table_name = "docs", .group_id = 10, .topology_epoch = owners[1].topology_epoch, .intent = .{} },
    };
    // Preparation freezes complete input before any destination mutation.
    // Checkpoint authority, rather than namespace order, fixes the coordinator.
    const preparation_reads = fake.decision_reads;
    const preparation_writes = fake.resolves;
    const preparation_publications = fake.publishes;
    const source_control = @import("../metadata/source_control.zig");
    const actor_scope: source_control.Scope = .{ .table_id = 7, .source_id = @splat(31), .source_ordinal = 0, .generation = 2, .definition = @splat(32), .provider = @splat(33), .destination = .native, .destination_binding = @splat(34) };
    var actor_arena = std.heap.ArenaAllocator.init(a);
    defer actor_arena.deinit();
    const actor_alloc = actor_arena.allocator();
    const checkpoint_authority: source_control.checkpoint_authority.Spec = .{ .binding = actor_scope.identity(), .table_name = "docs", .owner = owners[1] };
    var actor_state = try (source_control.Mutation{ .configure = actor_scope }).apply(actor_alloc, .{});
    actor_state = try (source_control.Mutation{ .bind_checkpoint = checkpoint_authority }).apply(actor_alloc, actor_state);
    actor_state = try (source_control.Mutation{ .enable_scope_fencing = {} }).apply(actor_alloc, actor_state);
    const initialized_actor = actor_state;
    var actor_pending: source_control.Reservation = .{ .sequence = 1, .batch_id = "one", .checkpoint = "initial", .accepted_at_ns = 11, .payload_digest = @splat(35), .plan_digest = @splat(36), .transaction_id = admission.transaction_id, .destination = .native, .delivery_manifest = "{}" };
    const scope_ref = (try @import("../metadata/source_scope_binding.zig").reference(actor_state, 1, 1)).?;
    const actor_purpose = try actor_pending.inputFingerprint(actor_scope);
    var source_inputs = normal_inputs;
    for (&source_inputs, 0..) |*input, index| input.intent = .{
        .completion_owner = owners[index],
        .completion_source_purpose = actor_purpose,
        .completion_source_scope = scope_ref,
    };
    source_inputs[1].intent.completion_source_checkpoint = .{ .binding = scope_ref.binding, .purpose = actor_purpose, .sequence = 1, .checkpoint = "initial", .accepted_at_ns = 11, .receipt_window = 3 };
    var prepared = (try coordinator.prepare(a, admission.transaction_id, 10, &source_inputs, .{})).?;
    defer prepared.deinit();
    try std.testing.expectEqualDeep(owners[1], try prepared.plan.admission.coordinatorOwner());
    try std.testing.expectEqualDeep(scope_ref, prepared.plan.source_scope.?);
    const encoded_prepared = try prepared.plan.encodeFrozen(a);
    defer a.free(encoded_prepared);
    var reprepared = (try coordinator.prepare(a, admission.transaction_id, 10, &source_inputs, .{})).?;
    defer reprepared.deinit();
    const rebuilt_bytes = try reprepared.plan.encodeFrozen(a);
    defer a.free(rebuilt_bytes);
    try std.testing.expectEqualSlices(u8, encoded_prepared, rebuilt_bytes);
    var restored_prepared = try plan_mod.Plan.restoreFrozen(a, encoded_prepared, try plan_mod.frozenDigest(encoded_prepared));
    defer restored_prepared.deinit();
    try std.testing.expectEqualDeep(scope_ref, restored_prepared.source_scope.?);
    try std.testing.expectEqual(preparation_reads, fake.decision_reads);
    try std.testing.expectEqual(preparation_writes, fake.resolves);
    try std.testing.expectEqual(preparation_publications, fake.publishes);
    var reopened_prepared = try coordinator.restorePrepared(a, encoded_prepared, try plan_mod.frozenDigest(encoded_prepared), .{});
    defer reopened_prepared.deinit();
    try std.testing.expectEqual(prepared.coordinator_index, reopened_prepared.coordinator_index);
    try std.testing.expectEqualDeep(prepared.plan.admission.digest(), reopened_prepared.plan.admission.digest());
    try std.testing.expectEqualDeep(scope_ref, reopened_prepared.plan.source_scope.?);
    try std.testing.expectEqual(preparation_reads, fake.decision_reads);
    try std.testing.expectEqual(preparation_writes, fake.resolves);
    // Source effects use ordinary routing while the checkpoint enlists even
    // without rows. No decision or destination write is part of planning.
    const routed_source = @import("distributed_txn.zig");
    const grant: routed_source.SourceGrant = .{
        .authority = .{ .binding = scope_ref.binding, .table_name = "docs", .owner = owners[1] },
        .reference = scope_ref,
        .checkpoint = source_inputs[1].intent.completion_source_checkpoint.?,
    };
    var routed = try routed_source.prepareSourceCommit(a, coordinator, admission.transaction_id, 10, &.{.{ .table_name = "docs", .writes = &.{.{ .key = "a", .value = "{\"n\":1}" }}, .deletes = &.{"z"} }}, grant, .{});
    defer routed.deinit();
    try std.testing.expectEqual(@as(usize, 2), routed.plan.participants.len);
    try std.testing.expectEqualDeep(owners[1], try routed.plan.admission.coordinatorOwner());
    var first_input = try routed.plan.prepare(a, 0);
    defer first_input.deinit(a);
    var second_input = try routed.plan.prepare(a, 1);
    defer second_input.deinit(a);
    try std.testing.expectEqualStrings("a", first_input.req.writes[0].key);
    try std.testing.expectEqualStrings("z", second_input.req.deletes[0]);
    try std.testing.expect(first_input.req.transaction.?.prepare.source_checkpoint == null);
    try std.testing.expectEqualDeep(grant.checkpoint, second_input.req.transaction.?.prepare.source_checkpoint.?);
    var empty_source = try routed_source.prepareSourceCommit(a, coordinator, admission.transaction_id, 10, &.{}, grant, .{});
    defer empty_source.deinit();
    try std.testing.expectEqual(@as(usize, 1), empty_source.plan.participants.len);
    try std.testing.expectEqualDeep(scope_ref, empty_source.plan.source_scope.?);
    try std.testing.expectEqualDeep(owners[1], try empty_source.plan.admission.coordinatorOwner());
    // Durable checkpoint identity permits a fresh plan's current fences,
    // without permitting a different physical root generation.
    var old_fences = grant;
    old_fences.authority.owner.topology_epoch += 1;
    var fresh_checkpoint = try routed_source.prepareSourceCommit(a, coordinator, admission.transaction_id, 10, &.{}, old_fences, .{});
    defer fresh_checkpoint.deinit();
    try std.testing.expectEqualDeep(owners[1], try fresh_checkpoint.plan.admission.coordinatorOwner());
    var missing_checkpoint = grant;
    missing_checkpoint.authority.table_name = "missing";
    var releases_before_missing = fake.admin_frees;
    try std.testing.expectError(error.TableNotFound, routed_source.prepareSourceCommit(a, coordinator, admission.transaction_id, 10, &.{}, missing_checkpoint, .{}));
    try std.testing.expectEqual(releases_before_missing + 1, fake.admin_frees);
    fake.omit_admin_ranges = true;
    releases_before_missing = fake.admin_frees;
    try std.testing.expectError(error.TableNotFound, routed_source.prepareSourceCommit(a, coordinator, admission.transaction_id, 10, &.{}, grant, .{}));
    try std.testing.expectEqual(releases_before_missing + 1, fake.admin_frees);
    fake.omit_admin_ranges = false;
    var retired_checkpoint = grant;
    retired_checkpoint.authority.retirement = @splat(34);
    try std.testing.expectError(error.SourceBindingConflict, routed_source.prepareSourceCommit(a, coordinator, admission.transaction_id, 10, &.{}, retired_checkpoint, .{}));
    var legacy_checkpoint = grant;
    legacy_checkpoint.checkpoint.receipt_window = 0;
    try std.testing.expectError(error.SourceBindingConflict, routed_source.prepareSourceCommit(a, coordinator, admission.transaction_id, 10, &.{}, legacy_checkpoint, .{}));
    var wrong_root = grant;
    wrong_root.authority.owner.root_generation += 1;
    try std.testing.expectError(error.SourceBindingConflict, routed_source.prepareSourceCommit(a, coordinator, admission.transaction_id, 10, &.{}, wrong_root, .{}));
    var wrong_sequence = grant;
    wrong_sequence.reference.sequence += 1;
    try std.testing.expectError(error.SourceBindingConflict, routed_source.prepareSourceCommit(a, coordinator, admission.transaction_id, 10, &.{}, wrong_sequence, .{}));
    try std.testing.expectEqual(preparation_reads, fake.decision_reads);
    try std.testing.expectEqual(preparation_writes, fake.resolves);
    try std.testing.expectEqual(preparation_publications, fake.publishes);
    fake.capture_epoch_delta = 1;
    try std.testing.expectError(error.TopologyChanged, coordinator.restorePrepared(a, encoded_prepared, try plan_mod.frozenDigest(encoded_prepared), .{}));
    fake.capture_epoch_delta = 0;
    try std.testing.expectEqual(preparation_reads, fake.decision_reads);
    try std.testing.expectEqual(preparation_writes, fake.resolves);
    source_inputs[1].intent.completion_owner.?.topology_epoch += 1;
    try std.testing.expectError(error.SourceBindingConflict, coordinator.prepare(a, admission.transaction_id, 10, &source_inputs, .{}));
    try std.testing.expectEqual(preparation_reads, fake.decision_reads);
    try std.testing.expectEqual(preparation_writes, fake.resolves);
    const writes_before_visibility = fake.resolves;
    const incomplete_visibility = (try coordinator.commit(a, admission.transaction_id, 10, 20, &normal_inputs, .full_index, true, .{})).?;
    try std.testing.expect(!incomplete_visibility.propagation_pending);
    try std.testing.expect(incomplete_visibility.visibility_pending);
    try std.testing.expect(incomplete_visibility.visibility_retry_pending);
    try std.testing.expect(incomplete_visibility.visibility_repair_required);
    try std.testing.expectEqualDeep([_]usize{ 1, 1 }, fake.visibility_calls);
    try std.testing.expectEqual(writes_before_visibility, fake.resolves);
    const publications_after_completion = fake.publishes;
    fake.visibility_errors = .{ null, null };
    var ordinary_prepared = (try coordinator.prepare(a, admission.transaction_id, 10, &normal_inputs, .{})).?;
    defer ordinary_prepared.deinit();
    const visible = try coordinator.commitPrepared(a, &ordinary_prepared, 20, .full_index, .{});
    try std.testing.expect(!visible.propagation_pending);
    try std.testing.expect(!visible.visibility_pending);
    try std.testing.expect(!visible.visibility_retry_pending);
    try std.testing.expect(!visible.visibility_repair_required);
    try std.testing.expectEqualDeep([_]usize{ 2, 2 }, fake.visibility_calls);
    try std.testing.expectEqual(publications_after_completion, fake.publishes);
    try std.testing.expectEqual(writes_before_visibility, fake.resolves);

    // Failed admission never reaches a decision read or participant mutation.
    const decision_reads_before_rejection = fake.decision_reads;
    fake.unsupported_owners[1] = true;
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, coordinator.commit(a, admission.transaction_id, 10, 20, &normal_inputs, .full_index, true, .{}));
    fake.unsupported_owners[0] = true;
    try std.testing.expectEqual(@as(?admission_mod.Result, null), try coordinator.commit(a, admission.transaction_id, 10, 20, &normal_inputs, .full_index, true, .{}));
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, coordinator.prepare(a, admission.transaction_id, 10, &source_inputs, .{}));
    fake.unsupported_owners = .{ false, false };
    fake.retention_limit_bytes = 1;
    try std.testing.expectError(error.HistoryTooLarge, coordinator.commit(a, admission.transaction_id, 10, 20, &normal_inputs, .full_index, true, .{}));
    try std.testing.expectEqual(decision_reads_before_rejection, fake.decision_reads);
    try std.testing.expectEqualDeep([_]usize{ 2, 2 }, fake.visibility_calls);
    try std.testing.expectEqual(publications_after_completion, fake.publishes);
    try std.testing.expectEqual(writes_before_visibility, fake.resolves);

    // Source ownership must survive recovery of a different original owner,
    // not just an idempotent resolution at the coordinator itself.
    const source_purpose: [32]u8 = @splat(21);
    for (&frozen_inputs, 0..) |*input, index| {
        input.roles = if (index == 0) completion.Roles.checkpoint else completion.Roles.guards;
        input.prepare.transaction.?.prepare.source_purpose = source_purpose;
    }
    frozen_inputs[1].prepare.transaction.?.prepare.source_purpose = @splat(22);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, plan_mod.Plan.freeze(a, admission.transaction_id, @splat(23), owners[0], &frozen_inputs, 11));
    frozen_inputs[1].prepare.transaction.?.prepare.source_purpose = source_purpose;
    frozen_inputs[1].roles = completion.Roles.checkpoint;
    try std.testing.expectError(error.InvalidSourceAdmission, plan_mod.Plan.freeze(a, admission.transaction_id, @splat(23), owners[0], &frozen_inputs, 11));
    frozen_inputs[1].roles = completion.Roles.guards;
    var source_frozen = try plan_mod.Plan.freeze(a, admission.transaction_id, @splat(23), owners[0], &frozen_inputs, 11);
    defer source_frozen.deinit();
    fake.admission = source_frozen.admission;
    fake.state.source_purpose = source_purpose;
    fake.resolved = .{ true, false };
    fake.published = .{ false, false };
    const source_resolves_before = fake.resolves;
    const source_publishes_before = fake.publishes;
    try std.testing.expectEqual(.complete, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    try std.testing.expectEqual(source_resolves_before + 1, fake.resolves);
    try std.testing.expectEqual(source_publishes_before + 2, fake.publishes);
    try std.testing.expectEqual(.complete, try recovery.run(a, owners[0], fence, "docs", admission.transaction_id, .{}));
    try std.testing.expectEqual(source_resolves_before + 1, fake.resolves);
    try std.testing.expectEqual(source_publishes_before + 2, fake.publishes);
    // A sealed retained source plan may finish its exact positive pending
    // coordinator. Ordinary admission and mismatched source authority cannot.
    fake.retention_limit_bytes = 1024 * 1024;
    fake.admission = prepared.plan.admission;
    fake.state = .{ .status = .pending, .begin_timestamp = prepared.plan.begin_timestamp, .created_at_ns = prepared.plan.created_at_ns, .commit_version = 0, .finalized_at_ns = 0, .prepared = false, .intents_resolved = false, .digest = @splat(7), .source_purpose = prepared.plan.source_purpose, .source_scope = prepared.plan.source_scope };
    fake.source_recovery_mode = true;
    fake.resolved = .{ false, false };
    fake.published = .{ false, false };
    try std.testing.expectError(error.CommitDecisionUnknown, coordinator.commitPrepared(a, &prepared, 20, .write, .{}));
    try std.testing.expectEqual(@as(usize, 0), fake.begins);
    fake.state.source_scope.?.token += 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, coordinator.commitSourcePrepared(a, &prepared, 20, .{}));
    fake.state.source_scope = prepared.plan.source_scope;
    fake.state.created_at_ns += 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, coordinator.commitSourcePrepared(a, &prepared, 20, .{}));
    fake.state.created_at_ns = prepared.plan.created_at_ns;
    try std.testing.expectEqual(@as(usize, 0), fake.begins);
    fake.lose_prepare_once = true;
    const source_resolves_before_replay = fake.resolves;
    try std.testing.expectError(error.CommitDecisionUnknown, coordinator.commitSourcePrepared(a, &prepared, 20, .{}));
    try std.testing.expectEqual(@as(usize, 2), fake.begins);
    try std.testing.expectEqual(@as(usize, 1), fake.prepares);
    try std.testing.expectEqual(source_resolves_before_replay, fake.resolves);
    const resumed = try coordinator.commitSourcePrepared(a, &prepared, 20, .{});
    try std.testing.expect(!resumed.propagation_pending);
    try std.testing.expect(!resumed.visibility_pending);
    try std.testing.expectEqual(@as(u64, 10), resumed.coordinator_group_id);
    try std.testing.expectEqual(@as(usize, 4), fake.begins);
    try std.testing.expectEqual(@as(usize, 3), fake.prepares);
    const replayed_resolves = fake.resolves;
    const committed_retry = try coordinator.commitSourcePrepared(a, &prepared, 20, .{});
    try std.testing.expect(!committed_retry.propagation_pending);
    try std.testing.expectEqual(@as(usize, 4), fake.begins);
    try std.testing.expectEqual(@as(usize, 3), fake.prepares);
    try std.testing.expectEqual(replayed_resolves, fake.resolves);
    var source_fence = fence;
    source_fence.route = .{ .group_id = 10, .range_id = 10, .identity_namespace = .{ .table_id = 7, .shard_id = 10, .range_id = 10 } };
    const expected: SourceExpectation = .{ .admission_digest = prepared.plan.admission.digest(), .scope = prepared.plan.source_scope.?, .purpose = prepared.plan.source_purpose.?, .begin_timestamp = prepared.plan.begin_timestamp, .commit_version = 20 };
    var wrong_source = expected;
    wrong_source.scope.token += 1;
    const resolutions_before_bound = fake.resolves;
    const publications_before_bound = fake.publishes;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, recovery.runSource(a, owners[1], source_fence, "docs", admission.transaction_id, wrong_source, .{}));
    wrong_source = expected;
    wrong_source.commit_version += 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, recovery.runSource(a, owners[1], source_fence, "docs", admission.transaction_id, wrong_source, .{}));
    try std.testing.expectEqual(resolutions_before_bound, fake.resolves);
    try std.testing.expectEqual(publications_before_bound, fake.publishes);
    const committed_record = fake.state;
    fake.state.status = .pending;
    fake.state.commit_version = 0;
    fake.state.finalized_at_ns = 0;
    fake.state.intents_resolved = false;
    try std.testing.expectError(error.SourceOutcomeUnresolved, recovery.runSource(a, owners[1], source_fence, "docs", admission.transaction_id, expected, .{}));
    fake.state = committed_record;
    fake.absent = true;
    try std.testing.expectEqual(.unproven, try recovery.runSource(a, owners[1], source_fence, "docs", admission.transaction_id, expected, .{}));
    fake.absent = false;
    try std.testing.expectEqual(resolutions_before_bound, fake.resolves);
    try std.testing.expectEqual(publications_before_bound, fake.publishes);
    fake.resolved = .{ true, false };
    fake.published = .{ false, false };
    const destination = @import("source_destination_plan.zig");
    var artifact_cut: [32]u8 = @splat(0);
    std.mem.writeInt(u64, artifact_cut[0..8], 1, .little);
    std.mem.writeInt(u64, artifact_cut[8..16], 8, .little);
    artifact_cut[31] = 1;
    var retained_source = try destination.Frozen.init(a, &prepared.plan, checkpoint_authority, .{ .root = try @import("../data/source_plan_entry.zig").rootFor(owners[1]), .cut = artifact_cut, .quota = .{} }, 20);
    defer retained_source.deinit();
    const compiled_source = try retained_source.manifest.deliveryPlan(actor_alloc, retained_source.encoded_manifest);
    actor_pending.delivery_manifest = compiled_source.manifest;
    actor_pending.plan_digest = compiled_source.digest;
    actor_pending.artifact = compiled_source.artifact;
    actor_pending.retain_manifest = compiled_source.retain_manifest;
    actor_state = try (source_control.Mutation{ .reserve = actor_pending }).apply(actor_alloc, actor_state);
    const reservation_id = actor_pending.identity(actor_scope);
    actor_state = try (source_control.Mutation{ .seal_artifact = .{ .reservation_digest = reservation_id, .stamp = @splat(37) } }).apply(actor_alloc, actor_state);
    const checkpoint_command = retained_source.manifest.checkpoint;
    const native_head: local.storage_source_checkpoint.Head = .{ .binding = checkpoint_command.binding, .purpose = checkpoint_command.purpose, .transaction_id = admission.transaction_id, .sequence = 1, .checkpoint = checkpoint_command.checkpoint, .accepted_at_ns = checkpoint_command.accepted_at_ns, .receipt_window = checkpoint_command.receipt_window, .request_digest = checkpoint_command.digest() };
    const checkpoint_wire = @import("source_checkpoint_operations.zig").wire;
    const head_bytes = try native_head.encode(actor_alloc);
    const head_evidence = try checkpoint_wire.Evidence.init(head_bytes);
    const native_query: checkpoint_wire.Request = .{ .version = 1, .source_owner = owners[1], .transaction_id = admission.transaction_id, .command = checkpoint_command, .minimum_applied_index = 1 };
    try std.testing.expectEqualDeep(try actor_pending.inputFingerprint(actor_scope), retained_source.manifest.checkpoint.purpose);
    try std.testing.expectEqualDeep(actor_scope.identity(), retained_source.manifest.source_scope.binding);
    try std.testing.expectEqualDeep(actor_state.pending.?.transaction_id, retained_source.manifest.transaction_id);
    try std.testing.expectEqualDeep(source_inputs[1].intent.completion_source_checkpoint.?, checkpoint_command);
    var checked_grant = try @import("source_destination_admission.zig").Grant.init(a, actor_scope, actor_state.pending.?, actor_state.artifact_control.?.current.?);
    defer checked_grant.deinit();
    const accepted_source = try @import("source_destination_receipt.zig").accept(actor_alloc, actor_state, .{ .reservation_digest = reservation_id, .checkpoint = .{ .request_digest = native_query.digest(), .cut = .{ .term = 1, .index = 8 }, .disposition = .committed, .head = head_evidence, .receipt = head_evidence } });
    const sealed_actor = actor_state;
    actor_state = try (source_control.Mutation{ .accept = .{ .reservation_digest = reservation_id, .receipt = accepted_source.receipt } }).apply(actor_alloc, actor_state);
    const completing: @import("source_destination_completion.zig").Completion = .{ .recovery = recovery };
    try std.testing.expectEqual(.complete, try completing.run(a, actor_state, accepted_source.receipt, source_fence, .{}));
    try std.testing.expectEqual(@as(u64, 0), actor_state.committed.sequence);
    try std.testing.expectEqual(resolutions_before_bound + 1, fake.resolves);
    try std.testing.expectEqual(publications_before_bound + 2, fake.publishes);

    const ActorMetadata = struct {
        alloc: A,
        state: source_control.State,
        metadata_group_id: u64 = 1,
        lose_reply: bool = true,

        pub fn ensureLinearizableReadWithContext(_: *@This(), _: Context) !void {}
        pub fn ensureTableTopologyProtocolReadyWithContext(_: *@This(), _: Context, _: u16) !void {}
        pub fn projectedStore(self: *@This()) ?*@This() {
            return self;
        }
        pub fn getSourceControl(self: *@This(), allocator: A, _: u64, _: u64, _: source_control.Digest) ![]u8 {
            return source_control.encode(allocator, self.state);
        }
        pub fn proposeTransitionCommandWithReceipt(self: *@This(), command: @import("../metadata/storage/raft_apply_store.zig").TransitionCommand) !u64 {
            // Copy the mutation-owned reference into the same durable lifetime
            // as the other actor state before simulating a lost reply.
            var parsed = try source_control.parseWrite(self.alloc, command.apply_source_control);
            try std.testing.expectEqual(self.state.revision, parsed.value.expected_revision);
            const next = try parsed.value.mutation.apply(self.alloc, self.state);
            const encoded = try source_control.encode(self.alloc, next);
            const saved = try source_control.parse(self.alloc, encoded);
            parsed.deinit();
            self.state = saved.value;
            return self.state.revision;
        }
        pub fn waitForTransitionAppliedWithContext(self: *@This(), _: u64, _: Context) !void {
            if (!self.lose_reply) return;
            self.lose_reply = false;
            return error.TestLostMetadataReply;
        }
    };
    var journal: ActorMetadata = .{ .alloc = actor_alloc, .state = actor_state };
    const source_completion = @import("source_destination_completion.zig");
    const actor_admission: @import("source_destination_admission.zig").Admission = .{ .coordinator = coordinator, .artifacts = undefined, .checkpoints = undefined };
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, source_completion.recover(&journal, actor_admission, a, actor_scope, @splat(32), .{}));
    try std.testing.expectEqual(@as(u64, 1), journal.state.committed.sequence);
    try std.testing.expectEqual(.accepted, journal.state.artifact_control.?.current.?.phase);
    try source_completion.recover(&journal, actor_admission, a, actor_scope, @splat(32), .{});
    try std.testing.expectEqual(.garbage, journal.state.artifact_control.?.current.?.phase);
    try std.testing.expectEqual(publications_before_bound + 2, fake.publishes);

    const artifact_wire = @import("source_plan_operations.zig").wire;
    const artifact_storage = local.storage_source_plan_storage;
    const ArtifactProbe = struct {
        cut: [32]u8,
        fences: usize = 0,
        begins: usize = 0,
        allow_upload: bool = false,
        store: artifact_storage.test_support.Txn = .{},

        fn execute(ptr: *anyopaque, allocator: A, _: metadata.CatalogRouteFence, _: []const u8, input: artifact_wire.ControlRequest, _: Context) !artifact_wire.ControlReply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            switch (input.command.action) {
                .fence => self.fences += 1,
                .begin => self.begins += 1,
                else => if (!self.allow_upload) return error.TestUnexpectedResult,
            }
            if (!self.allow_upload) return error.SourceOutcomeUnresolved;
            const index = std.mem.readInt(u64, self.cut[8..16], .little) + 1;
            std.mem.writeInt(u64, self.cut[8..16], index, .little);
            var decoded = try artifact_wire.native.fromCommand(allocator, input.command);
            defer decoded.deinit();
            const stored = try artifact_wire.control.apply(allocator, &self.store, decoded.request, self.cut);
            return .{ .request_digest = try input.digest(allocator), .receipt = .{ .term = 1, .index = index }, .outcome = .{ .applied = stored } };
        }

        fn observe(ptr: *anyopaque, allocator: A, _: metadata.CatalogRouteFence, _: []const u8, input: artifact_wire.ObserveRequest, _: Context) !artifact_wire.control.Observation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .root = input.query.root,
                .cut = self.cut,
                .quota = try artifact_storage.readQuota(&self.store, input.query.root),
                .status = if (input.query.spec) |spec| try artifact_storage.inspect(allocator, &self.store, input.query.root, spec) else null,
            };
        }
    };
    var artifact_probe: ArtifactProbe = .{ .cut = artifact_cut };
    defer artifact_probe.store.deinit();
    var reclaim_admission = actor_admission;
    reclaim_admission.artifacts.observations = .{ .ptr = &artifact_probe, .read_fn = ArtifactProbe.observe };
    reclaim_admission.artifacts.controls = .{ .ptr = &artifact_probe, .execute_fn = ArtifactProbe.execute };
    journal.state = try (source_control.Mutation{ .retire_artifact = .{ .reservation_digest = reservation_id, .proof = @splat(38), .reason = .not_admitted } }).apply(actor_alloc, sealed_actor);
    const retirement_revision = journal.state.revision;
    try std.testing.expectError(error.SourceOutcomeUnresolved, source_completion.recover(&journal, reclaim_admission, a, actor_scope, @splat(32), .{}));
    try std.testing.expectEqual(retirement_revision, journal.state.revision);
    try std.testing.expectEqual(@as(usize, 1), artifact_probe.fences);
    try std.testing.expectEqual(.garbage, journal.state.artifact_control.?.current.?.phase);

    journal.state = initialized_actor;
    journal.lose_reply = true;
    const source_input: @import("../metadata/source_delivery.zig").Input = .{ .batch_id = "batch", .checkpoint = "lsn:1", .payload_digest = @splat(40), .accepted_at_ns = 50 };
    const effects: []const @import("distributed_txn.zig").TableCommitRequest = &.{.{ .table_name = "docs", .deletes = &.{"z"} }};
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, source_completion.stage(&journal, reclaim_admission, a, actor_scope, @splat(32), source_input, effects, null, .{}));
    try std.testing.expectEqual(@as(usize, 0), artifact_probe.begins);
    const original = journal.state.pending.?;
    var retry = source_input;
    retry.accepted_at_ns = 99;
    try std.testing.expectError(error.SourceOutcomeUnresolved, source_completion.stage(&journal, reclaim_admission, a, actor_scope, @splat(32), retry, effects, null, .{}));
    try std.testing.expectEqual(@as(usize, 1), artifact_probe.begins);
    try std.testing.expectEqual(original.identity(actor_scope), journal.state.pending.?.identity(actor_scope));
    try std.testing.expectEqual(@as(u64, 0), journal.state.accepted.sequence);
    retry.payload_digest[0] ^= 1;
    try std.testing.expectError(error.SourcePayloadConflict, source_completion.stage(&journal, reclaim_admission, a, actor_scope, @splat(32), retry, effects, null, .{}));
    try std.testing.expectEqual(@as(usize, 1), artifact_probe.begins);
    artifact_probe.allow_upload = true;
    try source_completion.stage(&journal, reclaim_admission, a, actor_scope, @splat(32), source_input, effects, null, .{});
    try std.testing.expectEqual(@as(usize, 2), artifact_probe.begins);
    try std.testing.expectEqual(.sealed, journal.state.artifact_control.?.current.?.phase);
    try std.testing.expectEqual(original.identity(actor_scope), journal.state.pending.?.identity(actor_scope));
    try std.testing.expectEqual(@as(u64, 0), journal.state.accepted.sequence);

    fake.state.status = .aborted;
    fake.state.commit_version = 0;
    try std.testing.expectError(error.DecisionConflict, coordinator.commitSourcePrepared(a, &prepared, 20, .{}));
    const writes_before_aborted_source = fake.resolves;
    try std.testing.expectError(error.DecisionConflict, recovery.runSource(a, owners[1], source_fence, "docs", admission.transaction_id, expected, .{}));
    try std.testing.expectEqual(writes_before_aborted_source, fake.resolves);
    try std.testing.expectError(error.UnsupportedOperation, coordinator.commitSourcePrepared(a, &ordinary_prepared, 20, .{}));
    try std.testing.expectEqual(@as(usize, 4), fake.begins);
}
