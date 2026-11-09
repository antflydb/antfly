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
const memory_budget = @import("../sql/memory_budget.zig");
const Runtime = @import("../storage/background_runtime.zig");
const workers_per_lease = @import("antfly_runtime_fs").threaded_io_limits.request_forward_workers_per_request;
const maximum_participants = 16;

fn traceFailure(stage: []const u8, err: anyerror) void {
    if (@import("../storage/dense_perf_experiments.zig").enabled("ANTFLY_BENCH_METRICS"))
        std.debug.print("antfly_bench_sql_native_failure stage={s} error={s}\n", .{ stage, @errorName(err) });
}
/// Replay lifecycle tests must create actual asynchronous SQL source work even
/// when the optional foreground publisher can otherwise service it immediately.
pub var test_force_async_text: if (h.builtin.is_test) bool else void = if (h.builtin.is_test) false else {};

const RetryWitness = struct {
    namespace: @import("../storage/db/doc_identity.zig").Namespace,
    incarnation: u128,
    schema_namespace: u64,
    inode: u64,
    file_namespace: [32]u8,
};

const Work = struct {
    name: []const u8,
    db: *h.db_mod.DB,
    request: types.BatchRequest,
    inputs: ?NativeIntegrity.Inputs = null,
    plan: ?NativeIntegrity.Plan = null,
    rows: ?RowPlan = null,
    borrowed_rows: ?*RowPlan = null,
    snapshot: ?SnapshotLease = null,
    member: publication.AtomicMember = undefined,
    group: publication.AtomicGroup = undefined,
    marker: ?receipts.Marker = null,
    completion: resources.BatchVisibilityCompletion = .{},
    committed: bool = false,
    failure: ?anyerror = null,

    fn run(self: *Work) void {
        self.db.batchTrackedWithNativeSqlIntegrity(self.request, &self.committed, self.marker, self.borrowed_rows orelse (if (self.rows) |*rows| rows else null), &self.completion, &self.plan.?) catch |err| {
            traceFailure("participant", err);
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
    if (adapter.handle == null or requests.len == 0 or requests.len > maximum_participants) return null;
    // A capacity wait may let a peer replace a file or drop/recreate a table.
    // Rebuilding plans must not silently adopt those new logical identities.
    var witnesses: [maximum_participants]?RetryWitness = @splat(null);
    var allow_foreground = if (comptime h.builtin.is_test) !test_force_async_text else true;
    // The caller often supplies an arena. Reclaim through its original quota
    // rather than stacking arena growth factors or retaining every refused
    // attempt in that region. One owned arena serves the complete attempt.
    var attempt = std.heap.ArenaAllocator.init(memory_budget.reclaimingAllocator(alloc));
    defer attempt.deinit();
    while (true) {
        _ = attempt.reset(.retain_capacity);
        var refusal: ?@import("../storage/lite/replay_admission.zig").WaitHint = null;
        return tryCommitPrepared(adapter, attempt.allocator(), requests, &refusal, witnesses[0..requests.len], allow_foreground) catch |err| {
            traceFailure("attempt", err);
            // The attempt has drained every producer and freed all plans and
            // snapshot barriers before admission is released. Retry owned row
            // postimages, never SQL evaluation or an uncertain publication.
            if (err == error.LiteReplayBacklogExceeded and refusal != null) {
                if (adapter.preparation_handoff) |handoff| if (handoff.wait_replay) |wait| {
                    const root = wait(handoff.ptr, refusal.?) catch |wait_err| {
                        // This identity carries a stronger proof than generic
                        // Busy: every participant was drained, none committed,
                        // and the exact native capacity refusal was captured.
                        if (wait_err == error.LiteReplayBacklogExceeded or wait_err == error.FileBusy)
                            return error.SqlNativeCommitCapacityNotApplied;
                        return outcomes.notApplied(wait_err);
                    };
                    adapter.handle = root;
                    adapter.db = root.database();
                    continue;
                };
            }
            if (err == error.OwnedPreparationStale and allow_foreground) {
                // Optional prepared text lost its source witness. Preserve the
                // owned primary effects and predicates, but take the canonical
                // asynchronous path rather than repeatedly rebuilding text
                // against a moving source tip.
                allow_foreground = false;
                continue;
            }
            if (err == error.SqlMutationOutcomeUnknown) return err;
            return outcomes.notApplied(err);
        };
    }
}

fn tryCommitPrepared(adapter: anytype, alloc: std.mem.Allocator, requests: []const contract.TableCommitRequest, refusal: *?@import("../storage/lite/replay_admission.zig").WaitHint, witnesses: []?RetryWitness, allow_foreground: bool) !?catalog.MutationOutcome {
    var root = adapter.handle orelse return null;
    if (requests.len == 0 or requests.len > maximum_participants) return null;
    // Plans may allocate during their independently fenced publication. Keep
    // all ownership charged to the SQL request budget while serializing its
    // arena, rather than bypassing that budget with a process allocator.
    var synchronized: @import("../sql/parallel_scheduler.zig").LockedAllocator = .{ .backing = alloc };
    const owned = synchronized.allocator();
    const work = try owned.alloc(Work, requests.len);
    var initialized: usize = 0;
    defer for (work[0..initialized]) |*item| item.deinit();
    const timestamp: u64 = @intCast(@max(0, std.Io.Clock.real.now(h.handleLockIo()).nanoseconds));
    var shared_store: ?*anyopaque = null;
    var detach_preparation = false;
    for (requests, work, witnesses) |input, *item, *expected| {
        var request = (try NativeIntegrity.batchRequest(owned, input)) orelse return null;
        request.schema_version = request.schema_version orelse request.relational_schema_version;
        request.timestamp_ns = timestamp;
        const db = try @import("tables.zig").get(root, input.table_name);
        for (work[0..initialized]) |prior| if (prior.db == db) return error.InvalidAtomicPublication;
        item.* = .{ .name = input.table_name, .db = db, .request = request };
        initialized += 1;
        item.inputs = (try NativeIntegrity.Inputs.capture(owned, db, request)) orelse return null;
        const witness: RetryWitness = .{
            .namespace = db.core.identity_namespace,
            .incarnation = db.root_incarnation,
            .schema_namespace = db.core.schemaNamespaceGeneration(),
            .inode = item.inputs.?.scope.inode,
            .file_namespace = item.inputs.?.scope.file_namespace,
        };
        if (expected.*) |prior| {
            if (!std.meta.eql(prior, witness)) return error.PreparedGenerationChanged;
        } else expected.* = witness;
        const store = item.inputs.?.scope.publication_store orelse return null;
        if (shared_store) |prior| {
            if (prior != store) return null;
        } else shared_store = store;
        if (request.writes.len != 0 or request.deletes.len != 0) {
            for (adapter.normalized_plans.items) |rows| {
                if (!allow_foreground and rows.text.items.len != 0) continue;
                if (rows.needs_reprepare or !std.meta.eql(rows.namespace, db.core.identity_namespace) or rows.incarnation != db.root_incarnation) continue;
                if (rows.relational) |*relational| if (relational.matchesWrites(request.writes) and try rows.matches(db)) {
                    item.borrowed_rows = rows;
                    // Reuse normalization's exact owned row authority. Simple
                    // catalog validation is bounded; coordinated constraints
                    // still prepare their reference reads off admission.
                    detach_preparation = detach_preparation or rows.schema.?.hasCoordinatedConstraints() or
                        (allow_foreground and rows.catalog.plan().foreground_text.len != 0 and adapter.preparation_handoff != null and adapter.preparation_handoff.?.stage_foreground != null);
                    break;
                };
            }
            if (item.borrowed_rows == null) {
                item.rows = try RowPlan.captureIncludingRelational(owned, db);
                detach_preparation = true;
            }
        }
    }
    // The handoff retains the entire originating root, including its named
    // tables. End all borrowed current scans before refresh can replace it.
    if (detach_preparation) if (adapter.preparation_handoff) |handoff| try handoff.release_admission(handoff.ptr, requests[0].table_name);
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
        if (allow_foreground and detach_preparation) if (adapter.preparation_handoff) |handoff| if (handoff.stage_foreground) |stage| {
            if (item.borrowed_rows orelse (if (item.rows) |*rows| rows else null)) |rows| {
                rows.prepareRelationalForegroundText(item.request) catch |err| {
                    preparation_failure = err;
                    break;
                };
                stage(handoff.ptr, rows) catch |err| {
                    preparation_failure = err;
                    break;
                };
            }
        };
    }
    for (work) |*item| item.releaseInputs();
    if (detach_preparation) if (adapter.preparation_handoff) |handoff| {
        root = try handoff.reacquire_admission(handoff.ptr);
        adapter.handle = root;
        adapter.db = root.database();
    };
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
    // Capture only the finite current witnesses and immutable index inputs.
    // Artifact construction and replacement assembly must finish off writer
    // admission, just like document-batch prepared publication.
    if (allow_foreground) if (adapter.preparation_handoff) |handoff| if (handoff.stage_foreground != null) {
        var finish_text = false;
        for (work) |*item| {
            if (item.borrowed_rows orelse (if (item.rows) |*rows| rows else null)) |rows| {
                try rows.captureForegroundText(item.db);
                finish_text = finish_text or rows.hasCapturedForegroundText();
            }
        }
        if (finish_text) {
            try handoff.release_admission(handoff.ptr, requests[0].table_name);
            var failure: ?anyerror = null;
            for (work) |*item| {
                if (item.borrowed_rows orelse (if (item.rows) |*rows| rows else null)) |rows|
                    rows.finishForegroundText() catch |err| {
                        failure = err;
                        break;
                    };
            }
            root = try handoff.reacquire_admission(handoff.ptr);
            adapter.handle = root;
            adapter.db = root.database();
            if (failure) |err| return err;
            for (work) |*item| item.db = try @import("tables.zig").get(root, item.name);
        }
    };
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
    if (failure != null and failure.? == error.OwnedPreparationStale) return error.OwnedPreparationStale;
    if (failure != null and failure.? == error.LiteReplayBacklogExceeded) {
        refusal.* = group.replayRefusal();
        return error.LiteReplayBacklogExceeded;
    }
    return outcomes.notApplied(failure orelse error.AtomicPublicationAborted);
}
