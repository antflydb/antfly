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

//! Local SQL participants share the native durability boundary. Constraint and
//! row plans are owned before publication; independent namespace continuations
//! rendezvous only at their final primary batches. The existing runtime pool
//! supplies bounded producer capacity, without creating threads per statement.
const h = @import("handles.zig");
const std = h.std;
const types = h.db_mod.types;
const NativeIntegrity = @import("../storage/db/native_sql_integrity.zig");
const RowPlan = @import("../storage/db/prepared_document_batch.zig").Plan;
const publication = @import("../storage/native_publication.zig");
const resources = @import("../storage/db/execution_resources.zig");
const SnapshotLease = @import("../storage/db/snapshot_admission.zig").SnapshotAdmission.MutationLease;
const receipts = @import("outcome_receipts.zig");
const contract = h.antfly.capi_dependencies.api_distributed_txn_contract;
const catalog = h.antfly.capi_dependencies.sql_catalog;
const outcomes = h.antfly.capi_dependencies.sql_errors;
const Runtime = @import("../storage/background_runtime.zig");
const workers_per_lease = @import("antfly_runtime_fs").threaded_io_limits.request_forward_workers_per_request;
const maximum_participants = 16;

const Work = struct {
    name: []const u8,
    db: *h.db_mod.DB,
    request: types.BatchRequest,
    inputs: ?NativeIntegrity.Inputs = null,
    plan: ?NativeIntegrity.Plan = null,
    rows: ?RowPlan = null,
    snapshot: ?SnapshotLease = null,
    member: publication.AtomicMember = undefined,
    group: publication.AtomicGroup = undefined,
    marker: ?receipts.Marker = null,
    completion: resources.BatchVisibilityCompletion = .{},
    committed: bool = false,
    failure: ?anyerror = null,

    fn run(self: *Work) void {
        self.db.batchTrackedWithNativeSqlIntegrity(self.request, &self.committed, self.marker, if (self.rows) |*rows| rows else null, &self.completion, &self.plan.?) catch |err| {
            self.failure = err;
            // Failure before the rendezvous must release already waiting
            // producers. Cancellation after durable publication is a no-op.
            self.group.cancel();
        };
    }

    fn releaseInputs(self: *Work) void {
        if (self.inputs) |*inputs| inputs.deinit();
        self.inputs = null;
    }

    fn deinit(self: *Work) void {
        self.releaseInputs();
        self.completion.deinit(self.db.alloc);
        if (self.snapshot) |*snapshot| snapshot.release();
        if (self.rows) |*rows| rows.deinit();
        if (self.plan) |*plan| plan.deinit();
    }
};

/// Null means this backend/request requires the existing coordinator. Before
/// starting producers no authored primary effects have been published. Every
/// error after starting them is classified from durable group outcomes.
pub fn tryCommit(adapter: anytype, alloc: std.mem.Allocator, requests: []const contract.TableCommitRequest) !?catalog.MutationOutcome {
    return tryCommitPrepared(adapter, alloc, requests) catch |err| {
        // This identity is produced only after examining every participant's
        // durability result. All other escaping failures precede publication.
        if (err == error.SqlMutationOutcomeUnknown) return err;
        return outcomes.notApplied(err);
    };
}

fn tryCommitPrepared(adapter: anytype, alloc: std.mem.Allocator, requests: []const contract.TableCommitRequest) !?catalog.MutationOutcome {
    var root = adapter.handle orelse return null;
    if (requests.len == 0 or requests.len > maximum_participants) return null;
    var temporary = std.heap.ArenaAllocator.init(alloc);
    defer temporary.deinit();
    // Plans may allocate during their independently fenced publication. Keep
    // all ownership charged to the SQL request budget while serializing its
    // arena, rather than bypassing that budget with a process allocator.
    var synchronized: @import("../sql/parallel_scheduler.zig").LockedAllocator = .{ .backing = temporary.allocator() };
    const owned = synchronized.allocator();
    const work = try owned.alloc(Work, requests.len);
    var initialized: usize = 0;
    defer for (work[0..initialized]) |*item| item.deinit();
    const timestamp: u64 = @intCast(@max(0, std.Io.Clock.real.now(h.handleLockIo()).nanoseconds));
    var shared_store: ?*anyopaque = null;
    for (requests, work) |input, *item| {
        var request = (try NativeIntegrity.batchRequest(owned, input)) orelse return null;
        request.schema_version = request.schema_version orelse request.relational_schema_version;
        request.timestamp_ns = timestamp;
        const db = try @import("tables.zig").get(root, input.table_name);
        for (work[0..initialized]) |prior| if (prior.db == db) return error.InvalidAtomicPublication;
        item.* = .{ .name = input.table_name, .db = db, .request = request };
        initialized += 1;
        item.inputs = (try NativeIntegrity.Inputs.capture(owned, db, request)) orelse return null;
        const store = item.inputs.?.scope.publication_store orelse return null;
        if (shared_store) |prior| {
            if (prior != store) return null;
        } else shared_store = store;
        if (request.writes.len != 0 or request.deletes.len != 0)
            item.rows = try RowPlan.captureIncludingRelational(owned, db);
    }
    // The handoff retains the entire originating root, including its named
    // tables. End all borrowed current scans before refresh can replace it.
    if (adapter.preparation_handoff) |handoff| try handoff.release_admission(handoff.ptr, requests[0].table_name);
    var preparation_failure: ?anyerror = null;
    for (work) |*item| {
        item.plan = item.inputs.?.prepare() catch |err| {
            preparation_failure = err;
            break;
        };
        item.request.integrity_commands = &.{};
        if (item.rows) |*rows| rows.prepareWithIo(item.request, h.handleLockIo()) catch |err| {
            preparation_failure = err;
            break;
        };
    }
    for (work) |*item| item.releaseInputs();
    if (adapter.preparation_handoff) |handoff| {
        root = try handoff.reacquire_admission(handoff.ptr);
        adapter.handle = root;
        adapter.db = root.database();
    }
    if (preparation_failure) |err| return outcomes.notApplied(err);
    // Rebase only owned plans; each exact inode/catalog/readset is validated
    // again by batch execution. Every refreshed runtime must share this store.
    shared_store = null;
    for (work) |*item| {
        item.db = try @import("tables.zig").get(root, item.name);
        var scope = (try item.db.core.store.runtime_store.nativePublicationScope(owned)) orelse return error.PreparedGenerationChanged;
        defer scope.deinit(owned);
        const store = scope.publication_store orelse return error.PreparedGenerationChanged;
        if (shared_store) |prior| {
            if (prior != store) return error.PreparedGenerationChanged;
        } else shared_store = store;
    }
    // Reserve capacity for the complete task graph before submitting any
    // producer. Awaiting a partial group while asking for more capacity can
    // deadlock a saturated pool.
    var lanes: [(maximum_participants + workers_per_lease - 1) / workers_per_lease]Runtime.BackendRuntime.RequestForwardLaneLease = undefined;
    var lane_count: usize = 0;
    defer for (lanes[0..lane_count]) |*lane| lane.release();
    const needed = (work.len - 1 + workers_per_lease - 1) / workers_per_lease;
    while (lane_count < needed) : (lane_count += 1)
        lanes[lane_count] = root.db.backend_runtime.acquireRequestForwardLane() catch |err| return if (err == error.RequestForwardCapacityUnavailable) error.ConcurrencyUnavailable else err;
    const io = if (lane_count != 0) lanes[0].io() else h.handleLockIo();
    var group = (try work[0].db.core.store.runtime_store.createNativeAtomicGroup(work.len)) orelse return null;
    defer {
        group.cancel();
        group.deinit();
    }
    for (work, 0..) |*item, ordinal| {
        item.group = group;
        item.member = group.member(ordinal);
        // Hold each namespace's activation barrier until ALL producers have
        // installed their committed state. Workers retain the caller's shared
        // lease rather than re-entering behind a queued snapshot capture.
        item.snapshot = item.db.core.snapshot_admission.acquireMutation();
        item.plan.?.snapshot_mutation = &item.snapshot.?;
        item.plan.?.atomic_member = item.member;
    }
    const writer = receipts.activeWriter();
    const named = if (std.mem.eql(u8, work[0].name, "default")) null else work[0].name;
    if (writer) |owner| work[0].marker = owner.next(named);
    var tasks: std.Io.Group = .init;
    defer tasks.cancel(io);
    for (work[0 .. work.len - 1]) |*item| tasks.concurrent(io, Work.run, .{item}) catch |err| {
        group.cancel();
        tasks.await(io) catch {};
        return outcomes.notApplied(err);
    };
    work[work.len - 1].run();
    tasks.await(io) catch |err| {
        group.cancel();
        // Cancellation is not proof of a failed commit. Drain all producers
        // before examining outcomes or releasing their plans and barriers.
        tasks.cancel(io);
        if (work[0].failure == null) work[0].failure = err;
    };
    var committed = false;
    var unknown = false;
    var failure: ?anyerror = null;
    var pending = false;
    var repair = false;
    for (work) |item| {
        committed = committed or item.committed;
        if (item.failure) |err| {
            unknown = unknown or outcomes.isUnknownOutcome(err);
            repair = repair or err == error.EnrichmentWorkerFailed;
            if (failure == null or failure.? == error.AtomicPublicationAborted) failure = err;
        }
        pending = pending or item.completion.graph_cleanup_required or item.completion.sync_level != .write;
    }
    if (committed) return if (repair) .committed_repair_required else if (failure != null or pending) .committed_pending else .committed;
    if (unknown) {
        if (writer) |owner| owner.retire(work[0].marker.?, if (named) |name| .{ .name = name, .id = work[0].db.core.identity_namespace.table_id } else null);
        return error.SqlMutationOutcomeUnknown;
    }
    return outcomes.notApplied(failure orelse error.AtomicPublicationAborted);
}
