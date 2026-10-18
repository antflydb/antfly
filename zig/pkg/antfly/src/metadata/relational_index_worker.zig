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

//! Bounded native-owner scheduling. Only a host holding the actual standalone
//! writer lease may supply Authority. Raft execution is a separate authority;
//! native receipts never substitute for committed data-Raft effects.
const std = @import("std");
const local = @import("antfly_local_sources");
const work = @import("relational_index_work.zig");
const publication = @import("relational_index_publication.zig");
const execution = @import("../api/relational_index_owner_execution.zig");
const producer = @import("../api/relational_index_delivery_producer.zig");
const progress = local.storage_db_relational_index_publication_progress;
const delivery = local.storage_db_relational_index_owner_job.private_claims.delivery;
const operation = local.api_operation;
const resources = local.storage_resource_manager;
const calls = @import("../system_catalog/server_call.zig");

pub const max_jobs = 4;
pub const max_owners_per_job = 4;
const retry_ns = 250 * std.time.ns_per_ms;
const owner_lifetime_ns = 120 * std.time.ns_per_s;
const idle_lifetime_ns = 60 * std.time.ns_per_s;
// Volatile work can always be reconstructed from durable progress. Bound
// residency by successful quanta, not elapsed time: a large cohort must get
// through its bounded capture even on a slowly scheduled replica.
const max_job_quanta = 4096;

/// Reconstructing a large cohort must leave useful work after walking owners
/// that already acknowledged the phase. Otherwise a fixed residency budget can
/// repeatedly expire at the same completed prefix and starve the remaining cut.
fn jobQuantumBudget(owner_count: u16) usize {
    const count: usize = owner_count;
    // A source may have completed EOF while peers are still building. Its
    // idempotent recovery traverses the producer state machine rather than an
    // already-acknowledged control. Reserve that bounded walk as well.
    return max_job_quanta + completed_owner_quanta * count + (count + work.max_routes - 1) / work.max_routes;
}
const completed_owner_quanta = 2 + std.meta.tags(producer.Phase).len;

pub const Authority = struct { metadata_incarnation: [32]u8, binding_digest: [32]u8, writer_generation: u64 };
const Stage = enum { refresh, control, acknowledge, resolve_ack, decide, resolve_decision, cancel, resolve_cancel, build, deliver, inventory, install, release, cleanup, cleanup_complete, cancel_release };
const Owner = struct {
    ordinal: u16,
    stage: Stage = .refresh,
    head: ?progress.Head = null,
    ack: ?publication.Ack = null,
    decision: ?publication.DecidePublish = null,
    cancellation: ?publication.CancelDecision = null,
    stream: ?producer.Producer = null,
    deadline_ns: u64,
    retry_at_ns: u64 = 0,
    fn deinit(self: *Owner) void {
        if (self.stream) |*stream| stream.deinit();
        self.* = undefined;
    }

    /// Only a validated, durable pre-publication failure may request this CAS.
    /// Transient transport/storage errors and post-decision audits must not
    /// turn into cancellation or weaken the irreversible installation barrier.
    fn requestCancellation(self: *Owner, failure: progress.Failure) !void {
        const head = self.head orelse return error.InvalidCatalogRecord;
        try head.validate();
        if (head.phase != .building or failure == .none) return error.GenerationPublicationChanged;
        const request: publication.CancelDecision = .{ .table_id = head.table_id, .id = head.plan_id, .expected_revision = head.revision, .failure = failure };
        try request.validate();
        self.cancellation = request;
        self.stage = .cancel;
    }

    /// One metadata operation per quantum. A committed decision is admission
    /// to cleanup, NOT evidence that artifacts, claims or fences are retired.
    fn cancelStep(self: *Owner, host: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, now_ns: u64) !bool {
        try context.ensureActive();
        if (self.stage != .cancel and self.stage != .resolve_cancel) return error.InvalidCatalogRecord;
        const request = self.cancellation orelse return error.InvalidCatalogRecord;
        const call: calls.Call = if (self.stage == .cancel) .{ .relational_index_cancel = request } else .{ .relational_index_cancel_status = request };
        const bytes = host.systemCatalog(alloc, context, call) catch |err| switch (@as(anyerror, err)) {
            error.MetadataMutationOutcomeUnknown, error.Timeout => {
                self.stage = .resolve_cancel;
                self.retry_at_ns = now_ns +| retry_ns;
                return false;
            },
            error.GenerationPublicationChanged, error.MetadataReplicationPending => {
                self.cancellation = null;
                self.stage = .refresh;
                self.retry_at_ns = now_ns +| retry_ns;
                return false;
            },
            else => return err,
        };
        defer alloc.free(bytes);
        if (bytes.len == 0 or bytes.len > publication.CancellationStatus.max_wire_bytes) return error.InvalidCatalogRecord;
        var response = try std.json.parseFromSlice(publication.CancellationStatus, alloc, bytes, .{ .ignore_unknown_fields = false });
        defer response.deinit();
        const canceled = try response.value.isCanceled(request);
        if (response.value.replication_pending) {
            self.stage = .resolve_cancel;
            self.retry_at_ns = now_ns +| retry_ns;
            return false;
        }
        if (canceled) return true;
        // The proposal may never have committed. Reobserve authoritative
        // progress and the durable failure before choosing another CAS cut.
        self.cancellation = null;
        self.stage = .refresh;
        self.retry_at_ns = now_ns +| retry_ns;
        return false;
    }
};

const Job = struct {
    base: std.mem.Allocator,
    budget: ?resources.BudgetedAllocator,
    identity: work.Identity,
    capture: ?work.Capture,
    cohort: ?delivery.Cohort = null,
    table_name: ?[]u8 = null,
    owners: [max_owners_per_job]?Owner = @splat(null),
    next_owner: u16 = 0,
    next_slot: usize = 0,
    executing: bool = false,
    canceled: std.atomic.Value(bool) = .init(false),
    closing: *const std.atomic.Value(bool),
    touched_ns: u64,
    completed_quanta: usize = 0,
    quantum_budget: usize = max_job_quanta,
    retry_at_ns: u64 = 0,

    fn init(base: std.mem.Allocator, manager: ?*resources.ResourceManager, closing: *const std.atomic.Value(bool), head: progress.Head, now_ns: u64) !*Job {
        const self = try base.create(Job);
        errdefer base.destroy(self);
        self.* = .{ .base = base, .budget = if (manager) |value| resources.BudgetedAllocator.initReclaiming(value, .relational_preparation_working_set, base, 1) else null, .identity = .{ .table_id = head.table_id, .id = head.plan_id }, .capture = null, .closing = closing, .touched_ns = now_ns, .quantum_budget = jobQuantumBudget(head.owner_count) };
        errdefer if (self.budget) |*budget| budget.deinit();
        self.capture = try work.Capture.init(self.allocator(), head);
        return self;
    }
    fn allocator(self: *Job) std.mem.Allocator {
        return if (self.budget) |*budget| budget.allocator() else self.base;
    }
    fn canceledCallback(ptr: *const anyopaque) bool {
        const self: *const Job = @ptrCast(@alignCast(ptr));
        return self.canceled.load(.acquire) or self.closing.load(.acquire);
    }
    fn context(self: *Job, deadline: u64) operation.RequestContext {
        return .{ .deadline_ns = deadline, .cancellation = .{ .ptr = self, .is_cancelled_fn = canceledCallback }, .index_publication_authority = true };
    }
    fn deinit(self: *Job) void {
        std.debug.assert(!self.executing);
        for (&self.owners) |*slot| if (slot.*) |*owner| owner.deinit();
        if (self.capture) |*capture| capture.deinit();
        if (self.cohort) |*cohort| cohort.deinit();
        if (self.table_name) |name| self.allocator().free(name);
        if (self.budget) |*budget| budget.deinit();
        const base = self.base;
        base.destroy(self);
    }
    fn read(self: *Job, host: anytype, query: work.Query) !std.json.Parsed(work.Response) {
        const alloc = self.allocator();
        const bytes = try host.workJson(alloc, query);
        defer alloc.free(bytes);
        if (bytes.len == 0 or bytes.len > work.max_wire_bytes) return error.InvalidCatalogRecord;
        return std.json.parseFromSlice(work.Response, alloc, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .max_value_len = work.max_route_bytes });
    }
    fn status(self: *Job, host: anytype, context_: operation.RequestContext, call: calls.Call) !std.json.Parsed(publication.AckStatus) {
        const alloc = self.allocator();
        const bytes = try host.systemCatalog(alloc, context_, call);
        defer alloc.free(bytes);
        if (bytes.len == 0 or bytes.len > publication.max_ack_status_bytes) return error.InvalidIndexOwnerReceipt;
        return std.json.parseFromSlice(publication.AckStatus, alloc, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    }
    fn retireOwner(self: *Job, index: usize) void {
        self.owners[index].?.deinit();
        self.owners[index] = null;
    }
    fn selectOwner(self: *Job, owner_count: usize, now_ns: u64) ?usize {
        std.debug.assert(owner_count != 0 and owner_count <= 4096);
        for (0..max_owners_per_job) |offset| {
            const index = (self.next_slot + offset) % max_owners_per_job;
            if (self.owners[index]) |*owner| {
                if (owner.deadline_ns <= now_ns) self.retireOwner(index) else if (owner.retry_at_ns > now_ns) continue;
            }
            if (self.owners[index] == null) {
                // Wrapping the ordinal cursor must not create a second actor
                // for a resident owner (especially cohorts smaller than four).
                var ordinal: ?u16 = null;
                for (0..owner_count) |_| {
                    const candidate = self.next_owner;
                    self.next_owner = @intCast((@as(usize, candidate) + 1) % owner_count);
                    var resident = false;
                    for (self.owners) |slot| if (slot) |owner| {
                        if (owner.ordinal == candidate) resident = true;
                    };
                    if (!resident) {
                        ordinal = candidate;
                        break;
                    }
                }
                // Resolve admission before assigning the optional: Zig's
                // result-location initialization may write its tag before an
                // `orelse continue` in a field initializer is evaluated.
                const admitted = ordinal orelse continue;
                self.owners[index] = .{ .ordinal = admitted, .deadline_ns = now_ns +| owner_lifetime_ns };
            }
            self.next_slot = (index + 1) % max_owners_per_job;
            return index;
        }
        return null;
    }
    /// One facade/receiving-port operation, with no scheduler/catalog mutex held.
    fn step(self: *Job, host: anytype, transport: anytype, now_ns: u64) !void {
        if (canceledCallback(self)) return error.Canceled;
        const alloc = self.allocator();
        if (self.capture) |*capture| {
            var response = try self.read(host, try capture.request());
            defer response.deinit();
            if (response.value != .routes) return error.InvalidCatalogRecord;
            try capture.append(response.value.routes);
            if (capture.next_ordinal == null) {
                self.cohort = try capture.finish(alloc);
                errdefer {
                    self.cohort.?.deinit();
                    self.cohort = null;
                }
                for (self.cohort.?.targets) |target| if (target.owner.authority != .native) return error.UnsupportedNativeIndexOwner;
                self.table_name = try alloc.dupe(u8, capture.table_name.?);
                capture.deinit();
                self.capture = null;
            }
            return;
        }
        const cohort = &self.cohort.?;
        // Free owner slots admit the next ordinal. A blocked source does not
        // occupy every owner lane; proof/row memory remains capped by the pool.
        const index = self.selectOwner(cohort.targets.len, now_ns) orelse {
            self.retry_at_ns = now_ns +| retry_ns;
            return;
        };
        const owner = &self.owners[index].?;
        const identity = cohort.targets[cohort.by_ordinal[owner.ordinal]].owner;
        const context_ = self.context(owner.deadline_ns);
        try context_.ensureActive();
        switch (owner.stage) {
            .cancel, .resolve_cancel => {
                if (try owner.cancelStep(host, alloc, context_, now_ns)) self.retireOwner(index);
            },
            .refresh => {
                var response = try self.read(host, .{ .head = self.identity });
                defer response.deinit();
                if (response.value != .head) return error.InvalidCatalogRecord;
                try self.identity.requireHead(response.value.head);
                owner.head = response.value.head;
                if (response.value.head.phase == .building and response.value.head.completed_sources == response.value.head.owner_count) {
                    if (response.value.head.counts[@backingInt(progress.Action.validated)] == response.value.head.owner_count) {
                        owner.decision = .{ .table_id = self.identity.table_id, .id = self.identity.id, .expected_revision = response.value.head.revision };
                        owner.stage = .decide;
                    } else owner.stage = .inventory;
                    return;
                }
                owner.stage = switch (response.value.head.phase) {
                    .fencing, .reserving => .control,
                    .building => .build,
                    .installing => .install,
                    .releasing => .release,
                    .canceling => .cleanup,
                    .cancel_releasing => .cancel_release,
                    else => {
                        // Terminal history has no owner effect left to run.
                        self.retireOwner(index);
                        self.retry_at_ns = now_ns +| std.time.ns_per_s;
                        return;
                    },
                };
            },
            .decide, .resolve_decision => {
                const request = owner.decision orelse return error.InvalidCatalogRecord;
                const call: calls.Call = if (owner.stage == .decide) .{ .relational_index_publish_decide = request } else .{ .relational_index_publish_status = request };
                const bytes = host.systemCatalog(alloc, context_, call) catch |err| switch (@as(anyerror, err)) {
                    error.MetadataMutationOutcomeUnknown, error.Timeout => {
                        owner.stage = .resolve_decision;
                        owner.retry_at_ns = now_ns +| retry_ns;
                        return;
                    },
                    error.GenerationPublicationChanged, error.MetadataReplicationPending => {
                        owner.decision = null;
                        owner.stage = .refresh;
                        owner.retry_at_ns = now_ns +| retry_ns;
                        return;
                    },
                    else => return err,
                };
                defer alloc.free(bytes);
                if (bytes.len == 0 or bytes.len > publication.DecisionStatus.max_wire_bytes) return error.InvalidCatalogRecord;
                var response = try std.json.parseFromSlice(publication.DecisionStatus, alloc, bytes, .{ .ignore_unknown_fields = false });
                defer response.deinit();
                if (response.value.replication_pending) {
                    owner.stage = .resolve_decision;
                    owner.retry_at_ns = now_ns +| retry_ns;
                } else if (try response.value.isDecided(request)) {
                    self.retireOwner(index);
                } else {
                    // Resolve the exact job before choosing a fresh CAS cut;
                    // neither this branch nor recovery replays SQL DDL.
                    owner.decision = null;
                    owner.stage = .refresh;
                    owner.retry_at_ns = now_ns +| retry_ns;
                }
            },
            .control => {
                const request: execution.Request = .{ .table_id = self.identity.table_id, .group_id = identity.group_id, .id = self.identity.id, .operation = if (owner.head.?.phase == .fencing) .fence else .reserve };
                var response = transport.control(alloc, self.table_name.?, request, context_) catch |err| switch (@as(anyerror, err)) {
                    error.MetadataReplicationPending => {
                        owner.retry_at_ns = now_ns +| retry_ns;
                        return;
                    },
                    else => return err,
                };
                defer response.deinit();
                try response.value.validate(request);
                const ack = switch (response.value) {
                    .acknowledged => {
                        self.retireOwner(index);
                        return;
                    },
                    .admitted => null,
                    .raft => |value| value.signed_ack,
                    .controlled => |value| value.signed_ack,
                    .recovered => |value| value.signed_ack,
                    else => return error.InvalidPrivateIndexNativeReceipt,
                };
                if (ack) |value| {
                    try value.validate();
                    if (!std.meta.eql(value.signed.receipt.owner, identity)) return error.InvalidIndexOwnerReceipt;
                    owner.ack = value;
                    owner.stage = .acknowledge;
                } else {
                    owner.stage = .refresh;
                    owner.retry_at_ns = now_ns +| retry_ns;
                }
            },
            .acknowledge, .resolve_ack => {
                const call: calls.Call = if (owner.stage == .acknowledge) .{ .relational_index_owner_ack = owner.ack.? } else .{ .relational_index_owner_ack_status = owner.ack.? };
                var response = self.status(host, context_, call) catch |err| switch (@as(anyerror, err)) {
                    error.MetadataMutationOutcomeUnknown, error.Timeout => {
                        owner.stage = .resolve_ack;
                        owner.retry_at_ns = now_ns +| retry_ns;
                        return;
                    },
                    error.GenerationPublicationChanged, error.IndexOwnerReceiptChanged, error.MetadataReplicationPending => {
                        owner.ack = null;
                        owner.stage = .refresh;
                        owner.retry_at_ns = now_ns +| retry_ns;
                        return;
                    },
                    else => return err,
                };
                defer response.deinit();
                const acknowledged = try response.value.isAcknowledged(owner.ack.?);
                if (response.value.replication_pending) {
                    owner.stage = .resolve_ack;
                    owner.retry_at_ns = now_ns +| retry_ns;
                    return;
                }
                if (acknowledged) self.retireOwner(index) else {
                    // A rejected pre-commit attempt or superseded root needs
                    // fresh permission, not indefinite stale-revision retries.
                    owner.ack = null;
                    owner.stage = .refresh;
                    owner.retry_at_ns = now_ns +| retry_ns;
                }
            },
            .build => {
                const request: execution.Request = .{ .table_id = self.identity.table_id, .group_id = identity.group_id, .id = self.identity.id, .operation = .build, .build_budget = .{ .records = 128 } };
                if (transport.control(alloc, self.table_name.?, request, context_)) |result| {
                    var response = result;
                    defer response.deinit();
                    try response.value.validate(request);
                    if (response.value == .acknowledged) {
                        self.retireOwner(index);
                        return;
                    }
                    const pending = if (response.value == .raft_build) pending: {
                        if (response.value.raft_build.progress.state == .failed) {
                            try owner.requestCancellation(.invalid_candidate);
                            return;
                        }
                        break :pending false;
                    } else if (response.value == .building) switch (response.value.building.outcome) {
                        .inventory, .generation, .audit, .validated => return error.InvalidPrivateIndexNativeReceipt,
                        .advanced => |value| value.replication_pending,
                        .complete => |value| blk: {
                            if (value.replication_pending) break :blk true;
                            if (value.state == .failed) {
                                try owner.requestCancellation(.invalid_candidate);
                                return;
                            }
                            break :blk value.replication_pending;
                        },
                    } else return error.InvalidPrivateIndexNativeReceipt;
                    if (pending) {
                        owner.retry_at_ns = now_ns +| retry_ns;
                        return;
                    }
                } else |err| switch (@as(anyerror, err)) {
                    error.PrivateIndexClaimBacklogFull => {},
                    error.MetadataReplicationPending => {
                        owner.retry_at_ns = now_ns +| retry_ns;
                        return;
                    },
                    else => return err,
                }
                owner.stream = try producer.Producer.init(alloc, cohort, self.table_name.?, .{ .table_id = self.identity.table_id, .group_id = identity.group_id, .id = self.identity.id, .operation = .source }, context_);
                owner.stage = .deliver;
            },
            .deliver => {
                const prior = owner.stream.?.phase;
                const result = try owner.stream.?.step(transport, host);
                if (result == .pending) {
                    if (owner.stream.?.phase == prior) owner.retry_at_ns = now_ns +| retry_ns;
                } else self.retireOwner(index);
            },
            .cleanup, .cleanup_complete, .cancel_release => {
                const request: execution.Request = .{ .table_id = self.identity.table_id, .group_id = identity.group_id, .id = self.identity.id, .operation = switch (owner.stage) {
                    .cleanup => .cancel,
                    .cleanup_complete => .cancel_complete,
                    .cancel_release => .cancel_release,
                    else => unreachable,
                } };
                var response = transport.control(alloc, self.table_name.?, request, context_) catch |err| switch (@as(anyerror, err)) {
                    error.MetadataReplicationPending => {
                        owner.retry_at_ns = now_ns +| retry_ns;
                        return;
                    },
                    else => return err,
                };
                defer response.deinit();
                try response.value.validate(request);
                if (response.value == .acknowledged) {
                    self.retireOwner(index);
                    return;
                }
                if (response.value != .canceling and response.value != .raft_cancellation) return error.InvalidPrivateIndexNativeReceipt;
                if (try response.value.cancellationAck(request)) |ack| {
                    if (!std.meta.eql(ack.signed.receipt.owner, identity)) return error.InvalidIndexOwnerReceipt;
                    owner.ack = ack;
                    owner.stage = .acknowledge;
                } else if (if (response.value == .raft_cancellation) response.value.raft_cancellation.progress == .cancel and response.value.raft_cancellation.progress.cancel.artifacts_retired and response.value.raft_cancellation.progress.cancel.claims_retired else response.value.canceling.step == .retired) {
                    owner.stage = .cleanup_complete;
                } else owner.retry_at_ns = now_ns +| retry_ns;
            },
            .release => {
                const request: execution.Request = .{ .table_id = self.identity.table_id, .group_id = identity.group_id, .id = self.identity.id, .operation = .release };
                var response = transport.control(alloc, self.table_name.?, request, context_) catch |err| switch (@as(anyerror, err)) {
                    error.MetadataReplicationPending => {
                        owner.retry_at_ns = now_ns +| retry_ns;
                        return;
                    },
                    else => return err,
                };
                defer response.deinit();
                try response.value.validate(request);
                if (response.value == .acknowledged) {
                    self.retireOwner(index);
                    return;
                }
                if (response.value != .releasing and response.value != .raft_lifecycle) return error.InvalidPrivateIndexNativeReceipt;
                const ack = (try response.value.releaseAck(request)) orelse {
                    owner.retry_at_ns = now_ns +| retry_ns;
                    return;
                };
                if (!std.meta.eql(ack.signed.receipt.owner, identity)) return error.InvalidIndexOwnerReceipt;
                owner.ack = ack;
                owner.stage = .acknowledge;
            },
            .install => {
                const request: execution.Request = .{ .table_id = self.identity.table_id, .group_id = identity.group_id, .id = self.identity.id, .operation = .install, .build_budget = .{ .records = 128, .bytes = 16 * 1024 * 1024 } };
                var response = transport.control(alloc, self.table_name.?, request, context_) catch |err| switch (@as(anyerror, err)) {
                    error.MetadataReplicationPending => {
                        owner.retry_at_ns = now_ns +| retry_ns;
                        return;
                    },
                    else => return err,
                };
                defer response.deinit();
                try response.value.validate(request);
                if (response.value == .acknowledged) {
                    self.retireOwner(index);
                    return;
                }
                if (response.value == .raft_lifecycle) {
                    if (response.value.raft_lifecycle.progress.install.physical) |physical| {
                        if (physical.audit.inventory.phase == .failed) return error.PrivateIndexTargetInventoryMismatch;
                        if (physical.audit.generation) |fields| if (fields.phase == .failed) return error.PrivateIndexGenerationValidationFailed;
                    }
                } else {
                    if (response.value != .installing) return error.InvalidPrivateIndexNativeReceipt;
                    const outcome = response.value.installing.outcome;
                    if (outcome == .audit) {
                        if (outcome.audit.audit.inventory.phase == .failed) return error.PrivateIndexTargetInventoryMismatch;
                        if (outcome.audit.audit.generation) |fields| if (fields.phase == .failed) return error.PrivateIndexGenerationValidationFailed;
                        return;
                    }
                }
                const ack = (try response.value.installationAck(request)) orelse {
                    owner.retry_at_ns = now_ns +| retry_ns;
                    return;
                };
                if (!std.meta.eql(ack.signed.receipt.owner, identity)) return error.InvalidIndexOwnerReceipt;
                owner.ack = ack;
                owner.stage = .acknowledge;
            },
            .inventory => {
                const request: execution.Request = .{ .table_id = self.identity.table_id, .group_id = identity.group_id, .id = self.identity.id, .operation = .validate, .build_budget = .{ .records = 128, .bytes = 16 * 1024 * 1024 } };
                var response = transport.control(alloc, self.table_name.?, request, context_) catch |err| switch (@as(anyerror, err)) {
                    error.MetadataReplicationPending => {
                        owner.retry_at_ns = now_ns +| retry_ns;
                        return;
                    },
                    else => return err,
                };
                defer response.deinit();
                try response.value.validate(request);
                if (response.value == .acknowledged) {
                    self.retireOwner(index);
                    return;
                }
                if (response.value == .raft_validation) {
                    const value = response.value.raft_validation;
                    const failed = switch (value.state) {
                        .progress => |state| (state.inventory != null and state.inventory.?.fields.phase == .failed) or (state.generation != null and state.generation.?.fields.phase == .failed),
                        .audit => |state| state.outcome.audit.inventory.phase == .failed or (state.outcome.audit.generation != null and state.outcome.audit.generation.?.phase == .failed),
                    };
                    if (failed) {
                        try owner.requestCancellation(.invalid_candidate);
                        return;
                    }
                    if (try response.value.validationAck(request)) |ack| {
                        if (!std.meta.eql(ack.signed.receipt.owner, identity)) return error.InvalidIndexOwnerReceipt;
                        owner.ack = ack;
                        owner.stage = .acknowledge;
                    } else if (value.signing_failure != null) owner.retry_at_ns = now_ns +| retry_ns;
                    return;
                }
                if (response.value != .building) return error.InvalidPrivateIndexNativeReceipt;
                const outcome = response.value.building.outcome;
                const pending = switch (outcome) {
                    .inventory => |value| value.replication_pending,
                    .generation => |value| value.replication_pending,
                    .audit => false,
                    .validated => |value| value.logical.replication_pending,
                    else => return error.InvalidPrivateIndexNativeReceipt,
                };
                if (pending) {
                    owner.retry_at_ns = now_ns +| retry_ns;
                    return;
                }
                switch (outcome) {
                    .validated => {
                        const ack = (try response.value.validationAck(request)) orelse {
                            owner.retry_at_ns = now_ns +| retry_ns;
                            return;
                        };
                        if (!std.meta.eql(ack.signed.receipt.owner, identity)) return error.InvalidIndexOwnerReceipt;
                        owner.ack = ack;
                        owner.stage = .acknowledge;
                    },
                    .inventory => |value| if (value.record.fields.phase == .failed) {
                        try owner.requestCancellation(.invalid_candidate);
                    },
                    .generation => |value| switch (value.record.fields.phase) {
                        .primary, .forward, .ownership => {},
                        .failed => try owner.requestCancellation(.invalid_candidate),
                        // The next validation turn must re-audit this root;
                        // canonical readiness is not local physical evidence.
                        .ready => {},
                    },
                    .audit => |value| {
                        if (value.audit.inventory.phase == .failed) {
                            try owner.requestCancellation(.invalid_candidate);
                            return;
                        }
                        if (value.audit.generation) |fields| switch (fields.phase) {
                            .failed => try owner.requestCancellation(.invalid_candidate),
                            .ready => {
                                if (response.value.building.signed_audit == null) {
                                    owner.retry_at_ns = now_ns +| retry_ns;
                                    return;
                                }
                                // A physical witness alone cannot complete the
                                // stage. Recover its durable seal on a retry.
                                owner.retry_at_ns = now_ns +| retry_ns;
                            },
                            else => {},
                        };
                    },
                    else => unreachable,
                }
            },
        }
    }
};

pub const Coordinator = struct {
    lane: std.Io.Mutex = .init,
    closing: std.atomic.Value(bool) = .init(false),
    authority: ?Authority = null,
    jobs: [max_jobs]?*Job = @splat(null),
    next_slot: usize = 0,
    after_table: u64 = 0,
    discovery_inflight: bool = false,
    prefer_discovery: bool = true,
    discover_at_ns: u64 = 0,

    pub fn deinit(self: *Coordinator, io: std.Io) void {
        self.closing.store(true, .release);
        self.lane.lockUncancelable(io);
        defer self.lane.unlock(io);
        // The service stops/join its control callers before final destruction.
        std.debug.assert(!self.discovery_inflight);
        for (&self.jobs) |*slot| if (slot.*) |job| {
            std.debug.assert(!job.executing);
            job.deinit();
            slot.* = null;
        };
    }
    fn cancel(self: *Coordinator) void {
        for (&self.jobs) |*slot| if (slot.*) |job| {
            job.canceled.store(true, .release);
            if (!job.executing) {
                job.deinit();
                slot.* = null;
            }
        };
        self.after_table = 0;
    }

    /// Lease one quantum under the pool lane, execute outside it, then release.
    /// Concurrent service rounds may run different jobs. No busy job is evicted.
    pub fn step(self: *Coordinator, io: std.Io, alloc: std.mem.Allocator, manager: ?*resources.ResourceManager, host: anytype, transport: anytype, now_ns: u64) !bool {
        if (!self.lane.tryLock()) return false;
        var locked = true;
        defer if (locked) self.lane.unlock(io);
        if (self.closing.load(.acquire)) return false;
        const authority = try host.currentAuthority();
        if (!std.meta.eql(authority, self.authority)) {
            self.cancel();
            self.authority = authority;
        }
        if (authority == null) return false;
        const fence = authority.?;
        var free: ?usize = null;
        for (&self.jobs, 0..) |*slot, index| {
            if (slot.*) |job| {
                if (!job.executing and (job.canceled.load(.acquire) or job.completed_quanta >= job.quantum_budget or now_ns -| job.touched_ns >= idle_lifetime_ns)) {
                    job.deinit();
                    slot.* = null;
                }
            }
            if (slot.* == null) free = index;
        }
        if (free != null and !self.discovery_inflight and now_ns >= self.discover_at_ns and self.prefer_discovery) {
            self.discovery_inflight = true;
            self.prefer_discovery = false;
            const after = self.after_table;
            self.lane.unlock(io);
            locked = false;
            var discovered: ?*Job = null;
            var cursor: u64 = after;
            var ended = false;
            var failure: ?anyerror = null;
            blk: {
                const bytes = host.workJson(alloc, .{ .discover = after }) catch |err| {
                    failure = err;
                    break :blk;
                };
                defer alloc.free(bytes);
                if (bytes.len == 0 or bytes.len > work.max_wire_bytes) {
                    failure = error.InvalidCatalogRecord;
                    break :blk;
                }
                var parsed = std.json.parseFromSlice(work.Response, alloc, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = false }) catch |err| {
                    failure = err;
                    break :blk;
                };
                defer parsed.deinit();
                if (parsed.value != .discovered) {
                    failure = error.InvalidCatalogRecord;
                    break :blk;
                }
                const page = parsed.value.discovered;
                cursor = page.after_table;
                ended = page.exhausted;
                if (page.head) |head| discovered = Job.init(alloc, manager, &self.closing, head, now_ns) catch |err| {
                    failure = err;
                    break :blk;
                };
            }
            self.lane.lockUncancelable(io);
            locked = true;
            self.discovery_inflight = false;
            defer if (discovered) |job| job.deinit();
            if (!std.meta.eql(self.authority, @as(?Authority, fence)) or self.closing.load(.acquire)) return false;
            self.after_table = if (ended) 0 else cursor;
            self.discover_at_ns = now_ns +| @as(u64, if (ended or failure != null) std.time.ns_per_s else 0);
            if (failure) |err| return err;
            if (discovered) |job| {
                for (self.jobs) |existing| if (existing) |value| if (std.meta.eql(value.identity, job.identity)) return true;
                for (&self.jobs) |*slot| if (slot.* == null) {
                    slot.* = job;
                    discovered = null;
                    break;
                };
            }
            return true;
        }
        self.prefer_discovery = true;
        for (0..max_jobs) |offset| {
            const index = (self.next_slot + offset) % max_jobs;
            const job = self.jobs[index] orelse continue;
            if (job.executing or job.canceled.load(.acquire) or job.retry_at_ns > now_ns) continue;
            job.executing = true;
            self.next_slot = (index + 1) % max_jobs;
            self.lane.unlock(io);
            locked = false;
            var failure: ?anyerror = null;
            job.step(host, transport, now_ns) catch |err| {
                failure = if (job.budget != null and job.budget.?.denied()) error.ResourceBudgetExceeded else err;
            };
            self.lane.lockUncancelable(io);
            locked = true;
            job.executing = false;
            if (failure != null or job.canceled.load(.acquire) or !std.meta.eql(self.authority, @as(?Authority, fence)) or self.closing.load(.acquire)) {
                job.deinit();
                self.jobs[index] = null;
                self.discover_at_ns = now_ns +| retry_ns;
                if (failure) |err| return err;
            } else {
                job.touched_ns = now_ns;
                job.completed_quanta += 1;
            }
            return true;
        }
        return false;
    }
};

test "relational index publication scheduler reserves a full completed-owner sweep before useful work" {
    for (1..progress.max_owners + 1) |count| {
        const capture = (count + work.max_routes - 1) / work.max_routes;
        try std.testing.expectEqual(max_job_quanta, jobQuantumBudget(@intCast(count)) - capture - completed_owner_quanta * count);
    }
    try std.testing.expectEqual(@as(usize, 57600), jobQuantumBudget(progress.max_owners));
}

test "relational index publication scheduler resolves failure cancellation without replaying uncertain mutations" {
    const Host = struct {
        head: progress.Head,
        fault: enum { none, before_commit, lost_reply, pending, stale, foreign } = .none,
        mutations: usize = 0,
        reads: usize = 0,
        pub fn systemCatalog(self: *@This(), alloc: std.mem.Allocator, context: operation.RequestContext, call: calls.Call) ![]u8 {
            try context.ensureActive();
            try std.testing.expect(context.index_publication_authority);
            const request = switch (call) {
                .relational_index_cancel => |request| blk: {
                    self.mutations += 1;
                    if (self.fault == .before_commit) return error.Timeout;
                    if (self.fault == .stale) return error.GenerationPublicationChanged;
                    self.head = try self.head.cancel(request.expected_revision, request.failure);
                    if (self.fault == .lost_reply) return error.MetadataMutationOutcomeUnknown;
                    break :blk request;
                },
                .relational_index_cancel_status => |request| blk: {
                    self.reads += 1;
                    break :blk request;
                },
                else => return error.UnexpectedTestOperation,
            };
            try request.validate();
            var status: publication.CancellationStatus = .{ .head = self.head, .replication_pending = self.fault == .pending };
            if (self.fault == .foreign) status.head.plan_id = @splat(9);
            return std.json.Stringify.valueAlloc(alloc, status, .{});
        }
    };
    const a = std.testing.allocator;
    const head: progress.Head = .{ .table_id = 9, .plan_id = @splat(1), .plan_digest = @splat(2), .candidate_digest = @splat(3), .reservation_digest = @splat(4), .owner_count = 1, .counts = .{ 1, 1, 0, 0, 0, 0, 0 }, .phase = .building, .revision = 3 };
    const context: operation.RequestContext = .{ .index_publication_authority = true };
    for (std.enums.values(@FieldType(Host, "fault"))) |fault| {
        var host: Host = .{ .head = head, .fault = fault };
        var owner: Owner = .{ .ordinal = 0, .head = head, .deadline_ns = std.math.maxInt(u64) };
        defer owner.deinit();
        try owner.requestCancellation(.invalid_candidate);
        try std.testing.expectEqual(Stage.cancel, owner.stage);
        if (fault == .foreign) {
            try std.testing.expectError(error.GenerationPublicationChanged, owner.cancelStep(&host, a, context, 1));
            continue;
        }
        const done = try owner.cancelStep(&host, a, context, 1);
        try std.testing.expectEqual(@as(usize, 1), host.mutations);
        if (fault == .none) {
            try std.testing.expect(done);
        } else if (fault == .stale) {
            try std.testing.expect(!done);
            try std.testing.expectEqual(Stage.refresh, owner.stage);
            try std.testing.expectEqual(@as(?publication.CancelDecision, null), owner.cancellation);
        } else {
            try std.testing.expect(!done);
            try std.testing.expectEqual(Stage.resolve_cancel, owner.stage);
            // A pending replicated decision must stay unresolved; it does not
            // authorize cleanup, fence release or another mutation proposal.
            if (fault == .pending) {
                try std.testing.expect(!try owner.cancelStep(&host, a, context, 2));
                try std.testing.expectEqual(Stage.resolve_cancel, owner.stage);
            }
            host.fault = .none;
            const resolved = try owner.cancelStep(&host, a, context, 3);
            try std.testing.expectEqual(fault != .before_commit, resolved);
            try std.testing.expectEqual(@as(usize, 1), host.mutations);
            try std.testing.expect(host.reads != 0);
            if (fault == .before_commit) try std.testing.expectEqual(Stage.refresh, owner.stage);
        }
        // Decision admission is not terminal cleanup or a released fence.
        try std.testing.expect(host.head.phase == .building or host.head.phase == .canceling);
        try std.testing.expectEqual(@as(u16, 0), host.head.counts[5]);
        try std.testing.expectEqual(@as(u16, 0), host.head.counts[6]);
    }
    var invalid: Owner = .{ .ordinal = 0, .head = head, .deadline_ns = 1 };
    try std.testing.expectError(error.GenerationPublicationChanged, invalid.requestCancellation(.none));
    invalid.head.?.counts[2] = 1;
    invalid.head = try invalid.head.?.decidePublish(head.revision);
    try std.testing.expectError(error.GenerationPublicationChanged, invalid.requestCancellation(.invalid_candidate));
    try std.testing.expectEqual(@as(?publication.CancelDecision, null), invalid.cancellation);
    var expired: Owner = .{ .ordinal = 0, .head = head, .deadline_ns = 0 };
    var unused: Host = .{ .head = head };
    try expired.requestCancellation(.invalid_candidate);
    try std.testing.expectError(error.DeadlineExceeded, expired.cancelStep(&unused, a, .{ .deadline_ns = 0, .index_publication_authority = true }, 1));
    try std.testing.expectEqual(@as(usize, 0), unused.mutations);
    try std.testing.expectEqual(@as(usize, 0), unused.reads);
}

test "relational index publication scheduler admits distinct owners and rotates blocked lanes" {
    const alloc = std.testing.allocator;
    var closing = std.atomic.Value(bool).init(false);
    // Selection does not need catalog or cohort storage; no authority/effects
    // are simulated by this scheduling-only fixture.
    var job: Job = .{ .base = alloc, .budget = null, .identity = undefined, .capture = null, .closing = &closing, .touched_ns = 1 };
    defer for (&job.owners) |*slot| if (slot.*) |*owner| owner.deinit();
    for (1..5) |count| {
        for (&job.owners) |*slot| {
            if (slot.*) |*owner| owner.deinit();
            slot.* = null;
        }
        job.next_owner = 0;
        job.next_slot = 0;
        for (0..count) |ordinal| {
            const index = job.selectOwner(count, 1).?;
            try std.testing.expectEqual(@as(u16, @intCast(ordinal)), job.owners[index].?.ordinal);
            job.owners[index].?.retry_at_ns = 100;
        }
        try std.testing.expectEqual(@as(?usize, null), job.selectOwner(count, 2));
        const resumed = job.selectOwner(count, 100).?;
        try std.testing.expectEqual(@as(u16, 0), job.owners[resumed].?.ordinal);
        job.owners[resumed].?.deadline_ns = 100;
        _ = job.selectOwner(count, 101);
        var seen: [4]bool = @splat(false);
        for (job.owners) |slot| if (slot) |owner| {
            try std.testing.expect(!seen[owner.ordinal]);
            seen[owner.ordinal] = true;
        };
    }
}

test "relational index publication scheduler bounds residency without evicting an executing job" {
    const Host = struct {
        head: progress.Head,
        discoveries: usize = 0,
        pub fn currentAuthority(_: *@This()) !?Authority {
            return .{ .metadata_incarnation = @splat('a'), .binding_digest = @splat(1), .writer_generation = 1 };
        }
        pub fn workJson(self: *@This(), alloc: std.mem.Allocator, query: work.Query) ![]u8 {
            if (query != .discover) return error.UnexpectedTestOperation;
            self.discoveries += 1;
            return std.json.Stringify.valueAlloc(alloc, work.Response{ .discovered = .{ .head = self.head, .after_table = self.head.table_id, .exhausted = false } }, .{});
        }
        pub fn systemCatalog(_: *@This(), _: std.mem.Allocator, _: operation.RequestContext, _: calls.Call) ![]u8 {
            return error.UnexpectedTestOperation;
        }
    };
    const Ports = struct {
        fn control(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: u64, _: execution.Request, _: operation.RequestContext) anyerror!execution.Result {
            return error.UnexpectedTestOperation;
        }
        fn claims(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: u64, _: execution.ClaimRequest, _: operation.RequestContext) anyerror!execution.ClaimResult {
            return error.UnexpectedTestOperation;
        }
    };
    const alloc = std.testing.allocator;
    var host: Host = .{ .head = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .candidate_digest = @splat(3), .reservation_digest = @splat(4), .table_id = 999, .owner_count = 1 } };
    var coordinator: Coordinator = .{};
    defer coordinator.deinit(std.testing.io);
    coordinator.authority = try host.currentAuthority();
    for (&coordinator.jobs, 0..) |*slot, index| {
        var head = host.head;
        head.table_id = index + 1;
        slot.* = try Job.init(alloc, null, &coordinator.closing, head, 1);
        slot.*.?.completed_quanta = slot.*.?.quantum_budget;
    }
    const busy = coordinator.jobs[0].?;
    busy.executing = true;
    defer busy.executing = false;
    var transport: @import("../api/relational_index_local_delivery.zig").Transport = .{ .control_port = .{ .ptr = &host, .execute_fn = Ports.control }, .claim_port = .{ .ptr = &host, .execute_fn = Ports.claims } };
    try std.testing.expect(try coordinator.step(std.testing.io, alloc, null, &host, &transport, 2));
    try std.testing.expectEqual(@as(usize, 1), host.discoveries);
    try std.testing.expect(coordinator.jobs[0].? == busy);
    var admitted = false;
    for (coordinator.jobs) |slot| if (slot) |job| {
        if (job.identity.table_id == 999) admitted = true;
    };
    try std.testing.expect(admitted);
}
