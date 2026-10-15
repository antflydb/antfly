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

//! One source batch, one remote operation per scheduling turn. The coordinator
//! owns leadership cancellation, a bounded allocator, and the immutable cohort
//! for this object's lifetime. Never hold a catalog/apply lock across step().
//! Losing this local preparation is safe: a new producer first resolves the
//! durable metadata head, then obtains independently signed native consumption.
const std = @import("std");
const local = @import("antfly_local_sources");
const execution = @import("relational_index_owner_execution.zig");
const journal = @import("../metadata/relational_index_delivery_journal.zig");
const closure = @import("../metadata/relational_index_source_delivery.zig");
const jobs = local.storage_db_relational_index_owner_job;
const delivery = jobs.private_claims.delivery;
const native = local.storage_db_relational_index_owner_control;
const operation = local.api_operation;
const calls = @import("../system_catalog/server_call.zig");

pub const Phase = enum { status, consume_prefix, source, target, close, accept, resolve_accept, consume_batch, done };
pub const Result = union(enum) {
    pending: Phase,
    idle,
    advanced: struct { consumed: journal.Consumed, eof: bool, drained: bool = false },
};

pub const Producer = struct {
    alloc: std.mem.Allocator,
    /// Borrowed immutable inputs; the scheduling owner pins their lifetime.
    cohort: *const delivery.Cohort,
    table_name: []const u8,
    request: execution.Request,
    context: operation.RequestContext,
    phase: Phase = .status,
    failed: bool = false,
    head: ?journal.Head = null,
    prefix: ?journal.Consumed = null,
    read: ?native.SourceRead = null,
    plan: ?*delivery.Plan = null,
    delivered: ?[]closure.Delivered = null,
    next_target: usize = 0,
    closed: ?std.json.Parsed(execution.ClaimResponse) = null,
    result: ?Result = null,

    pub fn init(alloc: std.mem.Allocator, cohort: *const delivery.Cohort, table_name: []const u8, request: execution.Request, context: operation.RequestContext) !Producer {
        try context.ensureActive();
        try request.validate(request.group_id);
        // A background scheduler must supply its shared end-to-end budget;
        // per-RPC timeout resets cannot bound retained source/proof memory.
        if (request.operation != .source or table_name.len == 0 or table_name.len > 1024 or context.deadline_ns == null) return error.InvalidGenerationPublication;
        const owner = cohort.targets[cohort.by_ordinal[0]].owner;
        if (owner.namespace.table_id != request.table_id or !std.mem.eql(u8, &owner.plan_id, &request.id)) return error.PrivateIndexOwnerJobChanged;
        var found = false;
        for (cohort.targets) |target| if (target.owner.group_id == request.group_id) {
            found = true;
            break;
        };
        if (!found) return error.PrivateIndexOwnerJobChanged;
        return .{ .alloc = alloc, .cohort = cohort, .table_name = table_name, .request = request, .context = context };
    }

    pub fn deinit(self: *Producer) void {
        self.releaseBatch();
        self.releaseClosure();
        self.* = undefined;
    }
    fn releaseBatch(self: *Producer) void {
        if (self.read) |*read| read.deinit();
        self.read = null;
        if (self.plan) |plan| self.alloc.destroy(plan);
        self.plan = null;
        if (self.delivered) |values| self.alloc.free(values);
        self.delivered = null;
    }
    fn releaseClosure(self: *Producer) void {
        if (self.closed) |value| value.deinit();
        self.closed = null;
    }
    fn query(self: *const Producer) journal.Query {
        return .{ .table_id = self.request.table_id, .group_id = self.request.group_id, .id = self.request.id };
    }
    fn metadataCall(self: *Producer, metadata: anytype, call: calls.Call) !journal.Status {
        try self.context.ensureActive();
        var authority = self.context;
        authority.index_publication_authority = true;
        const bytes = try metadata.systemCatalog(self.alloc, authority, call);
        defer self.alloc.free(bytes);
        if (bytes.len == 0 or bytes.len > 16 * 1024) return error.InvalidPrivateIndexClaimReceipt;
        var parsed = try std.json.parseFromSlice(journal.Status, self.alloc, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        defer parsed.deinit();
        if (!std.meta.eql(parsed.value.query, self.query())) return error.InvalidPrivateIndexClaimReceipt;
        if (parsed.value.head) |head| _ = try head.encode();
        return parsed.value;
    }
    fn consume(self: *Producer, transport: anytype) !?journal.Consumed {
        const head = self.head orelse return error.InvalidPrivateIndexClaimReceipt;
        const request: execution.ClaimRequest = .{ .table_id = self.request.table_id, .group_id = self.request.group_id, .id = self.request.id, .consume = .{ .ordinal = head.ordinal, .batch_digest = head.batch_digest } };
        var response = try transport.claims(self.alloc, self.table_name, request, self.context);
        defer response.deinit();
        try response.value.validate(request);
        if (response.value == .acknowledged) return null;
        if (response.value != .consumed) return error.InvalidPrivateIndexClaimReceipt;
        const value = response.value.consumed;
        if (value.outcome.replication_pending or value.outcome.error_name != null or value.proof == null) return null;
        const consumed = try response.value.requireSignedConsumption(request);
        if (consumed.ordinal != head.ordinal or consumed.rows != head.rows or consumed.position.sequence <= head.source_sequence or
            !std.mem.eql(u8, &consumed.metadata_incarnation, &head.metadata_incarnation) or !std.mem.eql(u8, &consumed.binding_digest, &head.binding_digest) or
            !std.mem.eql(u8, &consumed.batch_digest, &head.batch_digest) or !std.mem.eql(u8, &consumed.decision_digest, &head.decision_digest)) return error.InvalidPrivateIndexClaimReceipt;
        return consumed;
    }
    fn acceptance(self: *const Producer) journal.Request {
        const value = self.closed.?.value.closed;
        return .{ .signed = .{ .data = value.data, .proof = value.proof.? }, .consumed = self.prefix };
    }
    fn observeAcceptance(self: *Producer, status: journal.Status) !void {
        if (status.replication_pending) {
            self.phase = .resolve_accept;
            return;
        }
        if (try status.isAccepted(self.acceptance())) {
            self.head = status.head.?;
            self.releaseClosure();
            self.phase = .consume_batch;
            return;
        }
        // A linearizable read may precede a still-in-flight proposal. Reissue
        // only the exact logical decision; metadata deduplicates it atomically.
        if (status.head) |head| {
            const previous = self.head orelse return error.GenerationPublicationChanged;
            if (head.ordinal != previous.ordinal or !std.mem.eql(u8, &head.decision_digest, &previous.decision_digest)) return error.GenerationPublicationChanged;
        } else if (self.head != null) return error.GenerationPublicationChanged;
        self.phase = .accept;
    }

    /// The host interleaves owners between turns and backs off pending states.
    /// Any uncaught error seals this local attempt; restart from durable status,
    /// never skip a target or infer that an uncertain mutation did not commit.
    pub fn step(self: *Producer, transport: anytype, metadata: anytype) !Result {
        if (self.failed) return error.PrivateIndexOwnerJobChanged;
        if (self.phase == .done) return self.result.?;
        errdefer self.failed = true;
        try self.context.ensureActive();
        switch (self.phase) {
            .done => return self.result.?,
            .status => {
                const status = try self.metadataCall(metadata, .{ .relational_index_source_accept_status = self.query() });
                if (status.replication_pending) return .{ .pending = self.phase };
                self.head = status.head;
                self.phase = if (self.head != null) .consume_prefix else .source;
            },
            .consume_prefix => {
                self.prefix = (try self.consume(transport)) orelse return .{ .pending = self.phase };
                self.phase = .source;
            },
            .source => {
                var response = try transport.control(self.alloc, self.table_name, self.request, self.context);
                defer response.deinit();
                try response.value.validate(self.request);
                if (response.value == .acknowledged) {
                    self.result = .idle;
                    self.phase = .done;
                    return self.result.?;
                }
                if (response.value != .source) return error.InvalidPrivateIndexClaimReceipt;
                const value = response.value.source;
                try self.cohort.requireBinding(value.observation.binding);
                if (value.observation.replication_pending) return .{ .pending = self.phase };
                if (value.observation.header == null) {
                    self.result = if (self.head != null and self.head.?.state == .ready) .{ .advanced = .{ .consumed = self.prefix.?, .eof = true, .drained = true } } else .idle;
                    self.phase = .done;
                    return self.result.?;
                }
                if (value.observation.header.?.bytes[184] == @backingInt(jobs.private_claims.SourceStream.State.failed)) return error.InvalidPrivateIndexBuildPage;
                if (value.signed_source == null) return .{ .pending = self.phase };
                self.read = try value.decodeSigned(self.alloc, self.request);
                const batch = &self.read.?.batch.?;
                const ordinal = std.mem.readInt(u64, batch.header.bytes[104..112], .little);
                if (self.head) |head| {
                    if (head.state != .building or head.ordinal == std.math.maxInt(u64) or ordinal != head.ordinal + 1 or
                        !std.mem.eql(u8, batch.header.bytes[256..288], &head.batch_digest)) return error.GenerationPublicationChanged;
                } else if (ordinal != 1) return error.GenerationPublicationChanged;
                if (!std.mem.eql(u8, batch.scope.table_name, self.table_name)) return error.PrivateIndexOwnerJobChanged;
                self.plan = try self.alloc.create(delivery.Plan);
                self.plan.?.* = try delivery.Plan.prepareControlled(self.cohort, self.read.?.binding, delivery.View.fromBatch(batch), self.context);
                self.delivered = try self.alloc.alloc(closure.Delivered, self.plan.?.group_count);
                self.phase = if (self.plan.?.group_count == 0) .close else .target;
            },
            .target => {
                const request = try execution.ClaimRequest.fromDelivery(self.plan.?, self.cohort, self.next_target);
                var response = try transport.claims(self.alloc, self.table_name, request, self.context);
                defer response.deinit();
                try response.value.validate(request);
                if (response.value == .acknowledged) return .{ .pending = self.phase };
                if (response.value != .applied) return error.InvalidPrivateIndexClaimReceipt;
                if (response.value.applied.outcome.replication_pending or response.value.applied.outcome.error_name != null or response.value.applied.signed_delivery == null) return .{ .pending = self.phase };
                const signed = try response.value.requireSignedDelivery(request);
                try signed.verifyPartition(self.plan.?, self.cohort, self.next_target);
                self.delivered.?[self.next_target] = .{ .index = @intCast(self.next_target), .signed = signed };
                self.next_target += 1;
                if (self.next_target == self.plan.?.group_count) self.phase = .close;
            },
            .close => {
                const batch = self.read.?.batch.?;
                const request: execution.ClaimRequest = .{ .table_id = self.request.table_id, .group_id = self.request.group_id, .id = self.request.id, .closure = .{ .ordinal = std.mem.readInt(u64, batch.header.bytes[104..112], .little), .batch_digest = batch.header.digest(), .targets = .{ .items = self.delivered.? } } };
                var response = try transport.claims(self.alloc, self.table_name, request, self.context);
                errdefer response.deinit();
                try response.value.validate(request);
                if (response.value == .acknowledged or (response.value == .closed and response.value.closed.proof == null)) {
                    response.deinit();
                    return .{ .pending = self.phase };
                }
                if (response.value != .closed or !std.mem.eql(u8, &try response.value.closed.data.source.encode(), &try self.read.?.binding.encode()) or
                    !std.mem.eql(u8, &response.value.closed.data.header.bytes, &batch.header.bytes)) return error.InvalidPrivateIndexClaimReceipt;
                self.closed = response;
                // The compact signed closure owns all subsequent authority.
                // Drop decoded rows and target reply storage before proposing.
                self.releaseBatch();
                self.phase = .accept;
            },
            .accept => {
                const status = self.metadataCall(metadata, .{ .relational_index_source_accept = self.acceptance() }) catch |err| switch (@as(anyerror, err)) {
                    error.MetadataMutationOutcomeUnknown, error.Timeout => {
                        self.phase = .resolve_accept;
                        return .{ .pending = self.phase };
                    },
                    else => return err,
                };
                try self.observeAcceptance(status);
            },
            .resolve_accept => try self.observeAcceptance(try self.metadataCall(metadata, .{ .relational_index_source_accept_status = self.query() })),
            .consume_batch => {
                const consumed = (try self.consume(transport)) orelse return .{ .pending = self.phase };
                self.result = .{ .advanced = .{ .consumed = consumed, .eof = self.head.?.state == .ready } };
                self.phase = .done;
                return self.result.?;
            },
        }
        return .{ .pending = self.phase };
    }
};

/// Protocol test data only: this does not perform metadata authority, placement,
/// native-cut or durable transaction verification. Real acceptance uses journal.
pub const testing = struct {
    pub fn acceptedHead(request: journal.Request, previous: ?journal.Head) !journal.Head {
        if (!@import("builtin").is_test) return error.TestUnexpectedResult;
        try request.signed.verify();
        const data = request.signed.data;
        const header = &data.header.bytes;
        const ordinal = std.mem.readInt(u64, header[104..112], .little);
        const logical = try data.logicalDigest();
        if (previous) |prior| if (prior.ordinal == ordinal) {
            if (!std.mem.eql(u8, &prior.batch_digest, &data.header.digest()) or !std.mem.eql(u8, &prior.logical_digest, &logical)) return error.GenerationPublicationChanged;
            return prior;
        };
        var binding_digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(&try data.source.encode(), &binding_digest, .{});
        var head: journal.Head = .{
            .metadata_incarnation = request.signed.proof.identity.metadata_incarnation,
            .binding_digest = binding_digest,
            .scope_digest = header[40..72].*,
            .ordinal = ordinal,
            .rows = (if (previous) |prior| prior.rows else @as(u64, 0)) + std.mem.readInt(u32, header[96..100], .little),
            .batch_rows = std.mem.readInt(u32, header[96..100], .little),
            .source_sequence = (try data.header.position()).sequence,
            .batch_digest = data.header.digest(),
            .logical_digest = logical,
            .previous_decision = if (previous) |prior| prior.decision_digest else @splat(0),
            .decision_digest = @splat(0),
            .accepted_root = try request.signed.proof.commitment(try data.digest()),
            .provenance = @splat(0),
            .previous_rows = if (previous) |prior| prior.rows else 0,
            .previous_provenance = if (previous) |prior| prior.provenance else @splat(0),
            .state = @fromBackingInt(@intCast(header[184])),
        };
        head.decision_digest = head.accepted().computedDigest();
        var provenance = std.crypto.hash.Blake3.init(.{});
        provenance.update("antfly/private-unique-accepted-provenance/v1");
        provenance.update(&head.previous_provenance);
        provenance.update(&head.decision_digest);
        provenance.update(&head.accepted_root);
        provenance.final(&head.provenance);
        _ = try head.encode();
        return head;
    }
};
