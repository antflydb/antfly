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

//! Completion-aware participant commands use canonical private batches, not
//! legacy phase RPCs which cannot carry the frozen owner and complete input.
//! Each call makes one mutation attempt. Unknown outcomes require coordinator
//! recovery; neither an error nor an absent observation authorizes resubmission.
const std = @import("std");
const local = @import("antfly_local_sources");
const batch = local.api_internal_batch;
const types = local.storage_db_selected_root.db.types;
const plan_mod = @import("completion_plan.zig");
const completion = plan_mod.completion;
const metadata = @import("../metadata/api.zig");
const operation = local.api_operation;
const group_ops = @import("internal_group_operations.zig");
const forwarding_mod = @import("internal_batch_forwarding.zig");
const http = @import("../raft/transport/http_common.zig");
const client_mod = @import("http_client.zig");
const time = @import("antfly_platform").time;
const A = std.mem.Allocator;

pub const Phase = union(enum) {
    begin,
    prepare,
    resolve: struct { status: types.TxnStatus, commit_version: u64 },
    publish: []const u8,
};

pub const Port = struct {
    writer: group_ops.RoutedRaftBatchWriter,

    pub fn submit(self: Port, a: A, plan: *const plan_mod.Plan, index: usize, fence: metadata.CatalogRouteFence, phase: Phase, original: operation.RequestContext) !void {
        // This scope owns both cancellation borrows for the synchronous call
        // and preserves the earliest original clock/deadline, including time
        // spent validating and encoding the immutable command.
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try context.ensureActive();
        try fence.validate();
        if (index >= plan.participants.len) return error.InvalidParticipant;
        const participant = plan.participants[index];
        const owner = participant.owner;
        try plan.admission.requireRoutedOwner(owner);
        const locator = local.api_local_transaction_contract.parseParticipantRef(participant.locator) orelse return error.InvalidParticipant;
        try validateParticipantFence(owner, locator.group_id, fence);
        var parsed_prepare: ?batch.OwnedBatchRequest = null;
        defer if (parsed_prepare) |*parsed| parsed.deinit(a);
        var parsed_certificate: ?std.json.Parsed(completion.Envelope) = null;
        defer if (parsed_certificate) |*parsed| parsed.deinit();
        const request: types.BatchRequest = switch (phase) {
            .begin => try plan.begin(index),
            .prepare => blk: {
                parsed_prepare = try plan.prepare(a, index);
                if (parsed_prepare.?.req.sync_level != .write) return error.InvalidBatchRequest;
                break :blk parsed_prepare.?.req;
            },
            .resolve => |terminal| blk: {
                if (terminal.status == .pending or (terminal.status == .committed and terminal.commit_version <= plan.begin_timestamp)) return error.InvalidTxnRecord;
                break :blk .{ .sync_level = .write, .transaction = .{ .resolve = .{ .completion_owner = owner, .source_purpose = plan.source_purpose, .source_scope = plan.source_scope, .txn_id = plan.admission.transaction_id, .status = terminal.status, .commit_version = terminal.commit_version } } };
            },
            .publish => |bytes| blk: {
                parsed_certificate = try completion.parse(a, bytes);
                const manifest = parsed_certificate.?.value.certificate.manifest;
                if (manifest.commit_version <= plan.begin_timestamp or !std.mem.eql(u8, &manifest.admission().digest(), &plan.admission.digest())) return error.SourceTransactionIdentityConflict;
                break :blk .{ .sync_level = .write, .transaction = .{ .publish_completion = .{ .txn_id = plan.admission.transaction_id, .completion_owner = owner, .certificate = bytes } } };
            },
        };
        return self.writeRequest(a, fence, owner, locator.table_name, request, context, scope.deadline_ns);
    }
    /// Recovery submits only the already durable terminal decision or its full
    /// certificate. It never reconstructs BEGIN/PREPARE from current rows.
    pub fn submitRetained(self: Port, a: A, lookup: @import("history_decision_operations.zig").wire.Query, observation: @import("history_decision_operations.zig").wire.Reply, index: usize, fence: metadata.CatalogRouteFence, phase: union(enum) { resolve, publish: []const u8, visibility: types.SyncLevel }, original: operation.RequestContext) !void {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try context.ensureActive();
        const decision = observation.decision orelse return error.SourceOutcomeUnresolved;
        const admission = decision.admission.admission;
        try observation.validate(lookup);
        if (decision.record.status == .pending) return error.SourceOutcomeUnresolved;
        if (index >= admission.participants.len) return error.InvalidParticipant;
        const member = admission.participants[index];
        const owner = member.routed_owner orelse return error.ChangefeedCapabilityUnsupported;
        const locator = local.api_local_transaction_contract.parseParticipantRef(member.locator) orelse return error.InvalidParticipant;
        try validateParticipantFence(owner, locator.group_id, fence);
        var parsed: ?std.json.Parsed(completion.Envelope) = null;
        defer if (parsed) |*value| value.deinit();
        const request: types.BatchRequest = switch (phase) {
            .visibility => |level| blk: {
                if (decision.record.status != .committed) return error.SourceOutcomeUnresolved;
                switch (level) {
                    .full_text, .enrichments, .full_index => {},
                    .propose, .write => return error.InvalidBatchRequest,
                }
                // Re-observe the durable terminal mutation through the normal
                // owner-fenced synchronization boundary. This cannot execute
                // original effects, create admission, or change the decision.
                break :blk .{ .sync_level = level, .transaction = .{ .resolve = .{ .completion_owner = owner, .source_purpose = decision.record.source_purpose, .source_scope = decision.record.source_scope, .txn_id = admission.transaction_id, .status = .committed, .commit_version = decision.record.commit_version } } };
            },
            .resolve => .{ .sync_level = .write, .transaction = .{ .resolve = .{ .completion_owner = owner, .source_purpose = decision.record.source_purpose, .source_scope = decision.record.source_scope, .txn_id = admission.transaction_id, .status = if (decision.record.status == .committed) .committed else .aborted, .commit_version = if (decision.record.status == .committed) decision.record.commit_version else decision.record.finalized_at_ns } } },
            .publish => |bytes| blk: {
                const manifest = try decision.committedManifest();
                parsed = try completion.parse(a, bytes);
                if (!std.mem.eql(u8, &parsed.?.value.certificate.manifest.digest(), &manifest.digest())) return error.SourceTransactionIdentityConflict;
                break :blk .{ .sync_level = .write, .transaction = .{ .publish_completion = .{ .txn_id = admission.transaction_id, .completion_owner = owner, .certificate = bytes } } };
            },
        };
        return self.writeRequest(a, fence, owner, locator.table_name, request, context, scope.deadline_ns);
    }

    /// Settle participant tracking after recovery confirms publication on every
    /// committed owner or rollback on every aborted owner.
    pub fn acknowledgeResolved(self: Port, a: A, lookup: @import("history_decision_operations.zig").wire.Query, observation: @import("history_decision_operations.zig").wire.Reply, fence: metadata.CatalogRouteFence, original: operation.RequestContext) !void {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try observation.validate(lookup);
        const decision = observation.decision orelse return error.SourceOutcomeUnresolved;
        if (decision.record.status == .pending) return error.SourceOutcomeUnresolved;
        if (!decision.record.acknowledgments_pending) return;
        const admission = decision.admission.admission;
        const owner = try admission.coordinatorOwner();
        const coordinator = try admission.coordinatorMember();
        const locator = local.api_local_transaction_contract.parseParticipantRef(coordinator.locator) orelse return error.InvalidParticipant;
        try validateParticipantFence(owner, locator.group_id, fence);
        var pending: [64][]const u8 = undefined;
        var count: usize = 0;
        for (admission.participants) |member| {
            if (std.mem.eql(u8, &member.owner_fence, &admission.coordinator_fence)) continue;
            pending[count] = member.locator;
            count += 1;
            if (count == pending.len) {
                try self.acknowledgeParticipants(a, owner, fence, locator.table_name, admission.transaction_id, pending[0..count], context, scope.deadline_ns);
                count = 0;
            }
        }
        // Keep the coordinator's own lease until every follower was confirmed.
        if (!decision.record.retain_terminal) {
            pending[count] = coordinator.locator;
            count += 1;
        }
        if (count != 0) try self.acknowledgeParticipants(a, owner, fence, locator.table_name, admission.transaction_id, pending[0..count], context, scope.deadline_ns);
    }

    pub fn acknowledgeRetainedCoordinator(self: Port, a: A, owner: completion.RoutedOwner, fence: metadata.CatalogRouteFence, locator_bytes: []const u8, transaction_id: types.TxnId, original: operation.RequestContext) !void {
        const locator = local.api_local_transaction_contract.parseParticipantRef(locator_bytes) orelse return error.InvalidParticipant;
        try validateParticipantFence(owner, locator.group_id, fence);
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        try self.acknowledgeParticipants(a, owner, fence, locator.table_name, transaction_id, &.{locator_bytes}, scope.context(), scope.deadline_ns);
    }

    fn acknowledgeParticipants(self: Port, a: A, owner: completion.RoutedOwner, fence: metadata.CatalogRouteFence, table: []const u8, txn_id: types.TxnId, participants: []const []const u8, context: operation.RequestContext, deadline_ns: u64) !void {
        return self.writeRequest(a, fence, owner, table, .{ .sync_level = .write, .transaction = .{ .acknowledge_many = .{ .txn_id = txn_id, .participants = participants } } }, context, deadline_ns);
    }

    /// Only the original coordinator can win the first terminal decision.
    /// A raced COMMIT is a conflict, never permission to propagate an abort.
    pub fn abortExpired(self: Port, a: A, lookup: @import("history_decision_operations.zig").wire.Query, observation: @import("history_decision_operations.zig").wire.Reply, fence: metadata.CatalogRouteFence, cutoff_ns: u64, timestamp_ns: u64, original: operation.RequestContext) !void {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try context.ensureActive();
        try observation.validate(lookup);
        const decision = observation.decision orelse return error.SourceOutcomeUnresolved;
        if (cutoff_ns == 0 or timestamp_ns < cutoff_ns or decision.record.status != .pending or decision.record.created_at_ns >= cutoff_ns) return error.SourceOutcomeUnresolved;
        if (decision.record.source_purpose != null) return error.SourceOutcomeUnresolved;
        const admission = decision.admission.admission;
        const member = try admission.coordinatorMember();
        const owner = member.routed_owner orelse return error.ChangefeedCapabilityUnsupported;
        const locator = local.api_local_transaction_contract.parseParticipantRef(member.locator) orelse return error.InvalidParticipant;
        try validateParticipantFence(owner, locator.group_id, fence);
        const request: types.BatchRequest = .{ .sync_level = .write, .transaction = .{ .resolve = .{ .completion_owner = owner, .source_purpose = decision.record.source_purpose, .source_scope = decision.record.source_scope, .txn_id = admission.transaction_id, .status = .aborted, .commit_version = timestamp_ns } } };
        return self.writeRequest(a, fence, owner, locator.table_name, request, context, scope.deadline_ns);
    }

    fn writeRequest(self: Port, a: A, fence: metadata.CatalogRouteFence, owner: completion.RoutedOwner, table: []const u8, request: types.BatchRequest, context: operation.RequestContext, deadline_ns: u64) !void {
        try context.ensureActive();
        const remaining = deadline_ns -| time.monotonicNs();
        if (remaining == 0) return error.DeadlineExceeded;
        const budget_ms: u32 = @intCast(@min(@as(u64, forwarding_mod.max_remaining_ms), std.math.divCeil(u64, remaining, std.time.ns_per_ms) catch unreachable));
        var bounded_fence = fence;
        bounded_fence.admission_deadline_ns = deadline_ns;
        bounded_fence.admission_deadline_io = null;
        bounded_fence.admission_cancellation = context.cancellation;
        _ = (try self.writer.write(a, .{ .catalog = bounded_fence }, owner.group_id, table, request, .{ .remaining_ms = budget_ms, .forwards_remaining = forwarding_mod.max_forwards, .campaign_allowed = false }, context)) orelse return error.UnknownGroup;
        // A caller-scope failure after dispatch cannot be reported as a safe
        // pre-proposal failure, even when the transport returned normally.
        context.ensureActive() catch return error.RaftBatchWriteOutcomeUnknown;
    }
};

fn validateParticipantFence(owner: completion.RoutedOwner, locator_group: u64, fence: metadata.CatalogRouteFence) !void {
    try fence.validate();
    const identity = fence.route.identity_namespace;
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], identity.table_id, .big);
    std.mem.writeInt(u64, namespace[8..16], identity.shard_id, .big);
    std.mem.writeInt(u64, namespace[16..24], identity.range_id, .big);
    if (fence.metadata_group_id != owner.metadata_group_id or fence.metadata_incarnation == null or
        !std.mem.eql(u8, &fence.metadata_incarnation.?, &owner.metadata_incarnation) or
        fence.table_id != identity.table_id or fence.topology_epoch != owner.topology_epoch or
        fence.route.group_id != owner.group_id or locator_group != owner.group_id or
        !std.mem.eql(u8, &namespace, &owner.namespace)) return error.TopologyChanged;
}

/// One signed HTTP attempt to the selected participant endpoint. Endpoint
/// discovery belongs to the coordinator's original routing authority.
pub const HttpWriter = struct {
    client: *client_mod.ApiHttpClient,
    base_uri: []const u8,
    pub fn writer(self: *HttpWriter) group_ops.RoutedRaftBatchWriter {
        return .{ .ptr = self, .write_fn = write };
    }
    fn write(ptr: *anyopaque, a: A, authority: group_ops.RoutedBatchAuthority, group: u64, table: []const u8, request: types.BatchRequest, forwarding: forwarding_mod.Context, context: operation.RequestContext) !?void {
        const self: *HttpWriter = @ptrCast(@alignCast(ptr));
        if (authority != .catalog or authority.catalog.route.group_id != group) return error.InvalidCatalogRouteFence;
        try context.ensureActive();
        const encoded = try batch.encodeBatchRequest(a, request);
        defer a.free(encoded);
        const fence = try std.json.Stringify.valueAlloc(a, authority.catalog, .{});
        defer a.free(fence);
        if (fence.len > 16384) return error.InvalidCatalogRouteFence;
        // The shared batch client accepts an already encoded path component.
        const table_path = try client_mod.percentEncodePathComponent(a, table);
        defer a.free(table_path);
        var limited: LimitedExecutor = .{ .upstream = self.client.executor };
        var client = self.client.*;
        client.alloc = a;
        client.executor = .{ .ptr = &limited, .vtable = &.{ .execute = LimitedExecutor.execute }, .clock_io = self.client.executor.clock_io, .realtime_ns_fn = if (self.client.executor.realtime_ns_fn != null) LimitedExecutor.realtimeNs else null };
        var cancellation = http.RequestCancellation.fromToken(context.cancellation);
        const timeout = @min(try context.remainingTimeoutMs() orelse forwarding.remaining_ms, forwarding.remaining_ms);
        try context.ensureActive();
        var response = try client.fetchGroupBatchWithForwarding(self.base_uri, group, table_path, encoded, timeout, forwarding, &cancellation, fence);
        defer response.deinit(a);
        const Counts = struct { inserted: u64, deleted: u64, transformed: u64 };
        var counts = std.json.parseFromSlice(Counts, a, response.body, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch return error.RaftBatchWriteOutcomeUnknown;
        defer counts.deinit();
        if (counts.value.inserted != request.writes.len or counts.value.deleted != request.deletes.len or counts.value.transformed != request.transforms.len) return error.RaftBatchWriteOutcomeUnknown;
        return {};
    }
    const LimitedExecutor = struct {
        upstream: http.RequestExecutor,
        fn realtimeNs(ptr: *anyopaque) i128 {
            const self: *LimitedExecutor = @ptrCast(@alignCast(ptr));
            return self.upstream.realtimeNs().?;
        }
        fn execute(ptr: *anyopaque, a: A, request: http.HttpRequest) !http.HttpResponse {
            const self: *LimitedExecutor = @ptrCast(@alignCast(ptr));
            var bounded = request;
            bounded.max_response_bytes = 4096;
            var response = try self.upstream.execute(a, bounded);
            errdefer response.deinit(a);
            if (response.body.len > 4096) return error.RaftBatchWriteOutcomeUnknown;
            if (response.status == 201) {
                const content_type = response.header("Content-Type") orelse response.content_type orelse return error.RaftBatchWriteOutcomeUnknown;
                if (!std.ascii.eqlIgnoreCase(content_type, "application/json") or response.header(forwarding_mod.outcome_header) != null) return error.RaftBatchWriteOutcomeUnknown;
            }
            return response;
        }
    };
};
