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

const std = @import("std");
const docstore_mod = @import("../storage/docstore.zig");
const db_mod = @import("../storage/db/mod.zig");
const platform_time = @import("../platform/time.zig");
const platform_sync = @import("antfly_platform").sync;

pub const StoreConfig = struct {
    repair_job_store_path: ?[]const u8 = null,
    repair_job_retention_ms: ?u64 = null,
};

pub const OpenedStore = struct {
    alloc: std.mem.Allocator,
    path_z: [:0]u8,
    docstore: *docstore_mod.DocStore,

    pub fn open(alloc: std.mem.Allocator, path: []const u8) !OpenedStore {
        const path_z = try alloc.dupeZ(u8, path);
        errdefer alloc.free(path_z);
        const docstore = try alloc.create(docstore_mod.DocStore);
        errdefer alloc.destroy(docstore);
        docstore.* = try docstore_mod.DocStore.open(alloc, path_z, .{});
        errdefer docstore.close();
        return .{
            .alloc = alloc,
            .path_z = path_z,
            .docstore = docstore,
        };
    }

    pub fn deinit(self: *OpenedStore) void {
        self.docstore.close();
        self.alloc.destroy(self.docstore);
        self.alloc.free(self.path_z);
        self.* = undefined;
    }
};

pub const JobPhase = enum {
    queued,
    running,
    succeeded,
    failed,
    cancelled,
};

pub const StartRequest = struct {
    target: []const u8 = "artifact",
    kind: ?db_mod.types.ArtifactRepairKind = null,
    index: ?[]const u8 = null,
    cursor: ?[]const u8 = null,
    limit: u32 = 100,
    force: bool = false,
    advance: bool = true,
};

pub const JobState = struct {
    job_id: u64,
    attempt_id: u64 = 0,
    table_name: []const u8,
    phase: []const u8,
    repair_status: []const u8,
    target: []const u8,
    kind: ?db_mod.types.ArtifactRepairKind = null,
    index: ?[]const u8 = null,
    cursor: ?[]const u8 = null,
    limit: u32,
    force: bool = false,
    result: db_mod.types.ArtifactRepairResult = .{},
    last_error: ?[]const u8 = null,
    cancel_requested: bool = false,
    /// Realtime deadline because this state survives process restart.
    next_retry_at_millis: u64 = 0,
    created_at_millis: u64,
    last_updated_at_millis: u64,
    expires_at_millis: u64,
};

pub const BeginAdvanceResult = struct {
    encoded: []u8,
    started: bool,
};

pub const Store = struct {
    const PendingCancelEntry = struct {
        previous_job_id: ?u64 = null,
        next_job_id: ?u64 = null,
    };

    alloc: std.mem.Allocator,
    cfg: StoreConfig,
    opened_store: ?*OpenedStore = null,
    mutex: std.atomic.Mutex = .unlocked,
    jobs: std.AutoHashMapUnmanaged(u64, []u8) = .{},
    next_job_id: u64 = 1,
    last_durable_cleanup_ms: u64 = 0,
    durable_cleanup_cursor: ?[]u8 = null,
    pending_cancel_jobs: std.AutoHashMapUnmanaged(u64, PendingCancelEntry) = .empty,
    pending_cancel_head: ?u64 = null,
    pending_cancel_tail: ?u64 = null,
    /// Process-local round-robin cursor. Durable job state remains the source
    /// of truth; rebuilding the FIFO after restart may reset this cursor
    /// without losing work. Advancing it after every inspection prevents a
    /// backed-off head window from starving later runnable cancellations.
    pending_cancel_scan_cursor: ?u64 = null,

    pub fn init(alloc: std.mem.Allocator, cfg: StoreConfig) Store {
        return .{
            .alloc = alloc,
            .cfg = cfg,
        };
    }

    pub fn deinit(self: *Store) void {
        var it = self.jobs.iterator();
        while (it.next()) |entry| self.alloc.free(entry.value_ptr.*);
        self.jobs.deinit(self.alloc);
        self.pending_cancel_jobs.deinit(self.alloc);
        if (self.durable_cleanup_cursor) |cursor| self.alloc.free(cursor);
        if (self.opened_store) |store| {
            store.deinit();
            self.alloc.destroy(store);
        }
        self.* = undefined;
    }

    fn enqueuePendingCancelLocked(self: *Store, job_id: u64) !bool {
        if (self.pending_cancel_jobs.contains(job_id)) return false;
        try self.pending_cancel_jobs.put(self.alloc, job_id, .{ .previous_job_id = self.pending_cancel_tail });
        if (self.pending_cancel_tail) |tail| {
            self.pending_cancel_jobs.getPtr(tail).?.next_job_id = job_id;
        } else {
            self.pending_cancel_head = job_id;
        }
        self.pending_cancel_tail = job_id;
        return true;
    }

    fn removePendingCancelLocked(self: *Store, job_id: u64) void {
        const removed = self.pending_cancel_jobs.get(job_id) orelse return;
        const removed_scan_cursor = self.pending_cancel_scan_cursor == job_id;
        if (removed.previous_job_id) |previous| {
            self.pending_cancel_jobs.getPtr(previous).?.next_job_id = removed.next_job_id;
        } else {
            self.pending_cancel_head = removed.next_job_id;
        }
        if (removed.next_job_id) |next| {
            self.pending_cancel_jobs.getPtr(next).?.previous_job_id = removed.previous_job_id;
        } else {
            self.pending_cancel_tail = removed.previous_job_id;
        }
        _ = self.pending_cancel_jobs.remove(job_id);
        if (removed_scan_cursor) {
            self.pending_cancel_scan_cursor = removed.next_job_id orelse self.pending_cancel_head;
        }
        if (self.pending_cancel_jobs.count() == 0) self.pending_cancel_scan_cursor = null;
    }

    /// Returns the next runnable durable cancellation without consuming it.
    /// Inspection is round-robin within the bounded FIFO window; `beginAdvance`
    /// removes the entry atomically with the queued-to-running transition, so
    /// supervisor and explicit advances may race safely.
    pub fn nextPendingDurableCancelAlloc(self: *Store, alloc: std.mem.Allocator) !?[]u8 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const now_ms = nowMillis();
        if (self.pending_cancel_jobs.count() == 0) {
            self.pending_cancel_scan_cursor = null;
            return null;
        }
        var job_id_opt = if (self.pending_cancel_scan_cursor) |cursor|
            if (self.pending_cancel_jobs.contains(cursor)) cursor else self.pending_cancel_head
        else
            self.pending_cancel_head;
        var inspected: usize = 0;
        const inspect_limit = @min(self.pending_cancel_jobs.count(), pending_cancel_scan_limit);
        while (job_id_opt) |job_id| {
            if (inspected >= inspect_limit) break;
            inspected += 1;
            const next_job_id = if (self.pending_cancel_jobs.get(job_id)) |entry|
                entry.next_job_id orelse self.pending_cancel_head
            else
                self.pending_cancel_head;
            const encoded = (try self.loadJobLocked(job_id)) orelse {
                self.removePendingCancelLocked(job_id);
                job_id_opt = if (next_job_id) |next|
                    if (self.pending_cancel_jobs.contains(next)) next else self.pending_cancel_head
                else
                    self.pending_cancel_head;
                continue;
            };
            var parsed = std.json.parseFromSlice(JobState, self.alloc, encoded, .{ .ignore_unknown_fields = true }) catch {
                self.removePendingCancelLocked(job_id);
                job_id_opt = if (next_job_id) |next|
                    if (self.pending_cancel_jobs.contains(next)) next else self.pending_cancel_head
                else
                    self.pending_cancel_head;
                continue;
            };
            defer parsed.deinit();
            if (!requiresDurableCancel(parsed.value) or
                !std.mem.eql(u8, parsed.value.phase, phaseString(.queued)))
            {
                self.removePendingCancelLocked(job_id);
                job_id_opt = if (next_job_id) |next|
                    if (self.pending_cancel_jobs.contains(next)) next else self.pending_cancel_head
                else
                    self.pending_cancel_head;
                continue;
            }
            self.pending_cancel_scan_cursor = next_job_id;
            if (parsed.value.next_retry_at_millis > now_ms) {
                job_id_opt = next_job_id;
                continue;
            }
            return try alloc.dupe(u8, encoded);
        }
        return null;
    }

    pub fn retentionMillis(self: *const Store) u64 {
        return self.cfg.repair_job_retention_ms orelse 86_400_000;
    }

    pub fn attachOpenedStore(self: *Store, opened: *OpenedStore) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        self.opened_store = opened;
        errdefer self.opened_store = null;
        try self.recoverPersistedJobsLocked(opened);
    }

    pub fn startJob(
        self: *Store,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: StartRequest,
    ) ![]u8 {
        const now_ms = nowMillis();
        const reserved = self.reserveJobId();
        const limit = if (req.limit == 0) @as(u32, 100) else req.limit;
        const encoded = try encodeState(alloc, .{
            .job_id = reserved.job_id,
            .attempt_id = 0,
            .table_name = table_name,
            .phase = phaseString(.queued),
            .repair_status = repairStatusForPhase(.queued, false, false),
            .target = req.target,
            .kind = req.kind,
            .index = req.index,
            .cursor = req.cursor,
            .limit = limit,
            .force = req.force,
            .result = .{ .limit = limit },
            .cancel_requested = false,
            .created_at_millis = now_ms,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        try self.storeEncoded(reserved.job_id, encoded, reserved.next_job_id);
        return encoded;
    }

    pub fn loadJobAlloc(self: *Store, alloc: std.mem.Allocator, job_id: u64) !?[]u8 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.jobs.get(job_id)) |encoded| return try alloc.dupe(u8, encoded);
        const opened = self.opened_store orelse return null;
        const key = try jobKey(alloc, job_id);
        defer alloc.free(key);
        const body = opened.docstore.get(alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        errdefer alloc.free(body);
        const cached = try self.alloc.dupe(u8, body);
        errdefer self.alloc.free(cached);
        try self.jobs.put(self.alloc, job_id, cached);
        return body;
    }

    pub fn markPhase(
        self: *Store,
        alloc: std.mem.Allocator,
        previous: JobState,
        phase: JobPhase,
        last_error: ?[]const u8,
    ) ![]u8 {
        const now_ms = nowMillis();
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const current_encoded = (try self.loadJobLocked(previous.job_id)) orelse return error.NotFound;
        var parsed_current = try std.json.parseFromSlice(JobState, self.alloc, current_encoded, .{ .ignore_unknown_fields = true });
        defer parsed_current.deinit();
        const current = parsed_current.value;
        if (!jobStateTransitionTokenMatches(current, previous)) {
            return try alloc.dupe(u8, current_encoded);
        }
        if (phase == .cancelled and requiresDurableCancel(current)) {
            const encoded = try encodeState(alloc, .{
                .job_id = current.job_id,
                .attempt_id = current.attempt_id,
                .table_name = current.table_name,
                .phase = phaseString(.queued),
                .repair_status = repairStatusForPhase(.queued, true, true),
                .target = current.target,
                .kind = current.kind,
                .index = current.index,
                .cursor = null,
                .limit = current.limit,
                .force = false,
                .result = current.result,
                .last_error = "cancel_pending",
                .cancel_requested = true,
                .created_at_millis = current.created_at_millis,
                .last_updated_at_millis = now_ms,
                .expires_at_millis = now_ms + self.retentionMillis(),
            });
            errdefer alloc.free(encoded);
            const enqueued = try self.enqueuePendingCancelLocked(current.job_id);
            errdefer if (enqueued) self.removePendingCancelLocked(current.job_id);
            try self.storeEncodedLocked(current.job_id, encoded, null);
            return encoded;
        }
        const cancel_requested = phase == .cancelled or previous.cancel_requested or current.cancel_requested;

        const encoded = try encodeState(alloc, .{
            .job_id = previous.job_id,
            .attempt_id = previous.attempt_id,
            .table_name = previous.table_name,
            .phase = phaseString(phase),
            .repair_status = repairStatusForPhase(phase, previous.result.has_more, previous.result.debt_remaining),
            .target = previous.target,
            .kind = previous.kind,
            .index = previous.index,
            .cursor = previous.cursor,
            .limit = previous.limit,
            .force = previous.force,
            .result = previous.result,
            .last_error = last_error,
            .cancel_requested = cancel_requested,
            .created_at_millis = previous.created_at_millis,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        try self.storeEncodedLocked(previous.job_id, encoded, null);
        return encoded;
    }

    /// Returns an operationally interrupted attempt to the durable queue.
    /// Invalid requests use `markPhase(.failed)`; ownership, routing, storage,
    /// and BackendRuntime availability failures use this bounded backoff path.
    pub fn recordRetryableFailure(
        self: *Store,
        alloc: std.mem.Allocator,
        previous: JobState,
        last_error: []const u8,
    ) ![]u8 {
        const now_ms = nowMillis();
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const current_encoded = (try self.loadJobLocked(previous.job_id)) orelse return error.NotFound;
        var parsed_current = try std.json.parseFromSlice(JobState, self.alloc, current_encoded, .{ .ignore_unknown_fields = true });
        defer parsed_current.deinit();
        const current = parsed_current.value;
        if (!jobStateTransitionTokenMatches(current, previous) or isTerminalPhase(current.phase)) {
            return try alloc.dupe(u8, current_encoded);
        }

        const durable_cancel = current.cancel_requested and
            std.mem.eql(u8, current.target, "index") and current.index != null;
        const encoded = try encodeState(alloc, .{
            .job_id = current.job_id,
            .attempt_id = current.attempt_id,
            .table_name = current.table_name,
            .phase = phaseString(.queued),
            .repair_status = repairStatusForPhase(.queued, true, true),
            .target = current.target,
            .kind = current.kind,
            .index = current.index,
            .cursor = current.cursor,
            .limit = current.limit,
            .force = if (durable_cancel) false else current.force,
            .result = current.result,
            .last_error = last_error,
            .cancel_requested = current.cancel_requested,
            .next_retry_at_millis = now_ms +| retryDelayMs(current.job_id, current.attempt_id),
            .created_at_millis = current.created_at_millis,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        const enqueued = if (durable_cancel) try self.enqueuePendingCancelLocked(current.job_id) else false;
        errdefer if (enqueued) self.removePendingCancelLocked(current.job_id);
        try self.storeEncodedLocked(current.job_id, encoded, null);
        return encoded;
    }

    pub fn recordPass(
        self: *Store,
        alloc: std.mem.Allocator,
        previous: JobState,
        pass: db_mod.types.ArtifactRepairResult,
    ) ![]u8 {
        var total = previous.result;
        total.scanned +|= pass.scanned;
        total.groups_scanned +|= pass.groups_scanned;
        total.reprocessed +|= pass.reprocessed;
        total.repaired +|= pass.repaired;
        total.missing_source_docs +|= pass.missing_source_docs;
        total.failed +|= pass.failed;
        total.unsupported +|= pass.unsupported;
        total.unresolved +|= pass.unresolved;
        total.in_progress +|= pass.in_progress;
        total.indexes_rebuilt +|= pass.indexes_rebuilt;
        total.indexes_degraded +|= pass.indexes_degraded;
        total.controls_applied +|= pass.controls_applied;
        total.limit = pass.limit;
        total.has_more = pass.has_more;
        total.debt_remaining = pass.debt_remaining;
        total.next_cursor = pass.next_cursor;

        const retryable_in_progress = pass.in_progress != 0 and pass.failed == 0 and pass.unsupported == 0 and pass.missing_source_docs == 0;
        const phase: JobPhase = if (previous.cancel_requested)
            .cancelled
        else if (pass.has_more or retryable_in_progress)
            .queued
        else if (pass.debt_remaining)
            .failed
        else
            .succeeded;
        const last_error: ?[]const u8 = if (phase == .cancelled) "cancel_requested" else if (phase == .failed) "repair_debt_remaining" else null;
        const now_ms = nowMillis();

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const current_encoded = (try self.loadJobLocked(previous.job_id)) orelse return error.NotFound;
        var parsed_current = try std.json.parseFromSlice(JobState, self.alloc, current_encoded, .{ .ignore_unknown_fields = true });
        defer parsed_current.deinit();
        const current = parsed_current.value;
        if (!jobStateTransitionTokenMatches(parsed_current.value, previous)) {
            return try alloc.dupe(u8, current_encoded);
        }
        if (requiresDurableCancel(current)) {
            const encoded = try encodeState(alloc, .{
                .job_id = current.job_id,
                .attempt_id = current.attempt_id,
                .table_name = current.table_name,
                .phase = phaseString(.queued),
                .repair_status = repairStatusForPhase(.queued, true, true),
                .target = current.target,
                .kind = current.kind,
                .index = current.index,
                .cursor = null,
                .limit = current.limit,
                .force = false,
                .result = total,
                .last_error = "cancel_pending",
                .cancel_requested = true,
                .created_at_millis = current.created_at_millis,
                .last_updated_at_millis = now_ms,
                .expires_at_millis = now_ms + self.retentionMillis(),
            });
            errdefer alloc.free(encoded);
            const enqueued = try self.enqueuePendingCancelLocked(current.job_id);
            errdefer if (enqueued) self.removePendingCancelLocked(current.job_id);
            try self.storeEncodedLocked(current.job_id, encoded, null);
            return encoded;
        }
        const cancel_requested = previous.cancel_requested or current.cancel_requested;
        const final_phase: JobPhase = if (cancel_requested) .cancelled else phase;
        const final_last_error: ?[]const u8 = if (final_phase == .cancelled) "cancel_requested" else last_error;

        const encoded = try encodeState(alloc, .{
            .job_id = previous.job_id,
            .attempt_id = previous.attempt_id,
            .table_name = previous.table_name,
            .phase = phaseString(final_phase),
            .repair_status = repairStatusForPhase(final_phase, pass.has_more, pass.debt_remaining),
            .target = previous.target,
            .kind = previous.kind,
            .index = previous.index,
            .cursor = pass.next_cursor,
            .limit = previous.limit,
            // Force is an edge-triggered dispatch policy. Keep it only while a
            // bounded table cursor has more groups that have not yet received
            // the generation request. Observation passes must never create a
            // second generation after the first one completes.
            .force = previous.force and pass.has_more,
            .result = total,
            .last_error = final_last_error,
            .cancel_requested = cancel_requested,
            .created_at_millis = previous.created_at_millis,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        try self.storeEncodedLocked(previous.job_id, encoded, null);
        return encoded;
    }

    pub fn beginAdvance(self: *Store, alloc: std.mem.Allocator, expected: JobState) !BeginAdvanceResult {
        const now_ms = nowMillis();
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current_encoded = (try self.loadJobLocked(expected.job_id)) orelse return error.NotFound;
        var parsed_current = try std.json.parseFromSlice(JobState, self.alloc, current_encoded, .{ .ignore_unknown_fields = true });
        defer parsed_current.deinit();
        const current = parsed_current.value;
        if (isTerminalPhase(current.phase)) {
            return .{ .encoded = try alloc.dupe(u8, current_encoded), .started = false };
        }
        const is_running = std.mem.eql(u8, current.phase, phaseString(.running));
        const running_expired = is_running and leaseExpired(now_ms, current.last_updated_at_millis, running_lease_timeout_ms);
        if (current.cancel_requested and !requiresDurableCancel(current) and (!is_running or running_expired)) {
            return .{ .encoded = try self.encodeCancelledCurrentLocked(alloc, current, now_ms), .started = false };
        }
        if (is_running and !running_expired) {
            return .{ .encoded = try alloc.dupe(u8, current_encoded), .started = false };
        }
        if (!is_running and current.next_retry_at_millis > now_ms) {
            return .{ .encoded = try alloc.dupe(u8, current_encoded), .started = false };
        }
        if (!is_running and !jobStateTransitionTokenMatches(current, expected)) {
            return .{ .encoded = try alloc.dupe(u8, current_encoded), .started = false };
        }

        const encoded = try encodeState(alloc, .{
            .job_id = current.job_id,
            .attempt_id = current.attempt_id +| 1,
            .table_name = current.table_name,
            .phase = phaseString(.running),
            .repair_status = repairStatusForPhase(.running, current.result.has_more, current.result.debt_remaining),
            .target = current.target,
            .kind = current.kind,
            .index = current.index,
            .cursor = current.cursor,
            .limit = current.limit,
            .force = current.force,
            .result = current.result,
            .last_error = current.last_error,
            .cancel_requested = current.cancel_requested,
            .next_retry_at_millis = 0,
            .created_at_millis = current.created_at_millis,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        try self.storeEncodedLocked(current.job_id, encoded, null);
        self.removePendingCancelLocked(current.job_id);
        return .{ .encoded = encoded, .started = true };
    }

    pub fn heartbeatRunning(
        self: *Store,
        alloc: std.mem.Allocator,
        job_id: u64,
        attempt_id: u64,
    ) !void {
        const now_ms = nowMillis();
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current_encoded = (try self.loadJobLocked(job_id)) orelse return error.NotFound;
        var parsed_current = try std.json.parseFromSlice(JobState, self.alloc, current_encoded, .{ .ignore_unknown_fields = true });
        defer parsed_current.deinit();
        const current = parsed_current.value;
        if (!std.mem.eql(u8, current.phase, phaseString(.running))) return;
        if (current.attempt_id != attempt_id) return;

        const encoded = try encodeState(alloc, .{
            .job_id = current.job_id,
            .attempt_id = current.attempt_id,
            .table_name = current.table_name,
            .phase = current.phase,
            .repair_status = current.repair_status,
            .target = current.target,
            .kind = current.kind,
            .index = current.index,
            .cursor = current.cursor,
            .limit = current.limit,
            .force = current.force,
            .result = current.result,
            .last_error = current.last_error,
            .cancel_requested = current.cancel_requested,
            .created_at_millis = current.created_at_millis,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        defer alloc.free(encoded);
        try self.storeEncodedLocked(current.job_id, encoded, null);
    }

    pub fn requestCancel(self: *Store, alloc: std.mem.Allocator, expected: JobState) ![]u8 {
        const now_ms = nowMillis();
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current_encoded = (try self.loadJobLocked(expected.job_id)) orelse return error.NotFound;
        var parsed_current = try std.json.parseFromSlice(JobState, self.alloc, current_encoded, .{ .ignore_unknown_fields = true });
        defer parsed_current.deinit();
        const current = parsed_current.value;
        if (isTerminalPhase(current.phase)) return try alloc.dupe(u8, current_encoded);
        if (!std.mem.eql(u8, current.table_name, expected.table_name)) return error.NotFound;
        if (current.cancel_requested) return try alloc.dupe(u8, current_encoded);

        const durable_cancel = std.mem.eql(u8, current.target, "index") and current.index != null;
        const phase: JobPhase = if (std.mem.eql(u8, current.phase, phaseString(.running)))
            .running
        else if (durable_cancel)
            .queued
        else
            .cancelled;
        const encoded = try encodeState(alloc, .{
            .job_id = current.job_id,
            .attempt_id = current.attempt_id,
            .table_name = current.table_name,
            .phase = phaseString(phase),
            .repair_status = repairStatusForPhase(phase, current.result.has_more, current.result.debt_remaining),
            .target = current.target,
            .kind = current.kind,
            .index = current.index,
            // Cancellation must revisit the whole table. Groups before the
            // current repair cursor may already own an active generation.
            .cursor = if (durable_cancel) null else current.cursor,
            .limit = current.limit,
            .force = if (durable_cancel) false else current.force,
            .result = current.result,
            .last_error = "cancel_requested",
            .cancel_requested = true,
            .created_at_millis = current.created_at_millis,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        const enqueued = if (durable_cancel and phase == .queued)
            try self.enqueuePendingCancelLocked(current.job_id)
        else
            false;
        errdefer if (enqueued) self.removePendingCancelLocked(current.job_id);
        try self.storeEncodedLocked(current.job_id, encoded, null);
        return encoded;
    }

    /// Records one bounded traversal that durably pauses every affected index
    /// intent and cancels any active attempt. Cancellation becomes terminal
    /// only after the table cursor has covered all groups.
    pub fn recordDurableCancelPass(
        self: *Store,
        alloc: std.mem.Allocator,
        previous: JobState,
        pass: db_mod.types.ArtifactRepairResult,
    ) ![]u8 {
        const now_ms = nowMillis();
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const current_encoded = (try self.loadJobLocked(previous.job_id)) orelse return error.NotFound;
        var parsed_current = try std.json.parseFromSlice(JobState, self.alloc, current_encoded, .{ .ignore_unknown_fields = true });
        defer parsed_current.deinit();
        const current = parsed_current.value;
        if (!jobStateTransitionTokenMatches(current, previous)) return try alloc.dupe(u8, current_encoded);

        var total = current.result;
        total.scanned +|= pass.scanned;
        total.groups_scanned +|= pass.groups_scanned;
        total.controls_applied +|= pass.controls_applied;
        total.limit = pass.limit;
        total.has_more = pass.has_more;
        total.debt_remaining = pass.has_more;
        total.next_cursor = pass.next_cursor;
        const phase: JobPhase = if (pass.has_more) .queued else .cancelled;
        const encoded = try encodeState(alloc, .{
            .job_id = current.job_id,
            .attempt_id = current.attempt_id,
            .table_name = current.table_name,
            .phase = phaseString(phase),
            .repair_status = repairStatusForPhase(phase, pass.has_more, pass.has_more),
            .target = current.target,
            .kind = current.kind,
            .index = current.index,
            .cursor = pass.next_cursor,
            .limit = current.limit,
            .force = false,
            .result = total,
            .last_error = if (pass.has_more) "cancel_pending" else "cancel_requested",
            .cancel_requested = true,
            .created_at_millis = current.created_at_millis,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        if (pass.has_more) {
            const enqueued = try self.enqueuePendingCancelLocked(current.job_id);
            errdefer if (enqueued) self.removePendingCancelLocked(current.job_id);
            try self.storeEncodedLocked(current.job_id, encoded, null);
        } else {
            try self.storeEncodedLocked(current.job_id, encoded, null);
            self.removePendingCancelLocked(current.job_id);
        }
        return encoded;
    }

    pub fn cleanupExpiredJobs(self: *Store) void {
        const now_ms = nowMillis();
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        var expired = std.ArrayListUnmanaged(u64).empty;
        defer expired.deinit(self.alloc);
        var it = self.jobs.iterator();
        while (it.next()) |entry| {
            var parsed = std.json.parseFromSlice(JobState, self.alloc, entry.value_ptr.*, .{ .ignore_unknown_fields = true }) catch continue;
            defer parsed.deinit();
            if (parsed.value.expires_at_millis == 0 or parsed.value.expires_at_millis > now_ms) continue;
            expired.append(self.alloc, entry.key_ptr.*) catch continue;
        }
        var durable_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer {
            for (durable_delete_keys.items) |key| self.alloc.free(key);
            durable_delete_keys.deinit(self.alloc);
        }
        if (self.opened_store) |opened| durable_cleanup: {
            if (!self.shouldRunDurableCleanupLocked(now_ms)) break :durable_cleanup;
            self.last_durable_cleanup_ms = now_ms;

            const durable_results = opened.docstore.scanPrefixPage(
                self.alloc,
                job_key_prefix,
                self.durable_cleanup_cursor,
                durable_cleanup_scan_limit,
            ) catch break :durable_cleanup;
            defer docstore_mod.DocStore.freeResults(self.alloc, durable_results);
            defer self.advanceDurableCleanupCursorLocked(durable_results);

            for (durable_results) |kv| {
                var parsed = std.json.parseFromSlice(JobState, self.alloc, kv.value, .{ .ignore_unknown_fields = true }) catch continue;
                defer parsed.deinit();
                if (parsed.value.expires_at_millis == 0 or parsed.value.expires_at_millis > now_ms) continue;
                expired.append(self.alloc, parsed.value.job_id) catch continue;
                const delete_key = self.alloc.dupe(u8, kv.key) catch continue;
                durable_delete_keys.append(self.alloc, delete_key) catch {
                    self.alloc.free(delete_key);
                    continue;
                };
                const active_key = activeJobKey(self.alloc, parsed.value.job_id) catch continue;
                durable_delete_keys.append(self.alloc, active_key) catch {
                    self.alloc.free(active_key);
                    continue;
                };
            }
        }
        if (durable_delete_keys.items.len > 0) {
            if (self.opened_store) |opened| opened.docstore.putBatch(&.{}, durable_delete_keys.items) catch {};
        }
        for (expired.items) |job_id| {
            self.removePendingCancelLocked(job_id);
            if (self.jobs.fetchRemove(job_id)) |removed| self.alloc.free(removed.value);
        }
    }

    fn shouldRunDurableCleanupLocked(self: *Store, now_ms: u64) bool {
        return self.last_durable_cleanup_ms == 0 or
            now_ms >= self.last_durable_cleanup_ms +| durable_cleanup_interval_ms;
    }

    fn advanceDurableCleanupCursorLocked(self: *Store, durable_results: []const docstore_mod.OwnedKVPair) void {
        if (durable_results.len == durable_cleanup_scan_limit) {
            const next = self.alloc.dupe(u8, durable_results[durable_results.len - 1].key) catch return;
            if (self.durable_cleanup_cursor) |old| self.alloc.free(old);
            self.durable_cleanup_cursor = next;
            return;
        }
        if (self.durable_cleanup_cursor) |old| {
            self.alloc.free(old);
            self.durable_cleanup_cursor = null;
        }
    }

    pub fn storeEncodedForTest(self: *Store, job_id: u64, encoded: []const u8) !void {
        std.debug.assert(@import("builtin").is_test);
        try self.storeEncoded(job_id, encoded, null);
    }

    const ReservedJobId = struct {
        job_id: u64,
        next_job_id: u64,
    };

    fn reserveJobId(self: *Store) ReservedJobId {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const job_id = self.next_job_id;
        self.next_job_id += 1;
        return .{
            .job_id = job_id,
            .next_job_id = self.next_job_id,
        };
    }

    fn storeEncoded(self: *Store, job_id: u64, encoded: []const u8, next_job_id: ?u64) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        try self.storeEncodedLocked(job_id, encoded, next_job_id);
    }

    fn storeEncodedLocked(self: *Store, job_id: u64, encoded: []const u8, next_job_id: ?u64) !void {
        if (encoded.len > max_job_record_bytes) return error.InvalidRepairJobState;
        const owned = try self.alloc.dupe(u8, encoded);
        errdefer self.alloc.free(owned);
        var parsed = try std.json.parseFromSlice(JobState, self.alloc, encoded, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.job_id != job_id) return error.InvalidRepairJobState;
        try validateJobState(parsed.value);
        // Reserve the process-local cache slot before committing the durable
        // state. Once the store write succeeds, publishing the matching cache
        // value cannot fail and leave scheduler metadata out of sync.
        if (!self.jobs.contains(job_id)) try self.jobs.ensureUnusedCapacity(self.alloc, 1);
        if (self.opened_store) |opened| {
            const key = try jobKey(self.alloc, job_id);
            defer self.alloc.free(key);
            const active_key = try activeJobKey(self.alloc, job_id);
            defer self.alloc.free(active_key);
            var writes: [3]docstore_mod.KVPair = undefined;
            var write_count: usize = 0;
            writes[write_count] = .{ .key = key, .value = encoded };
            write_count += 1;
            if (next_job_id) |next| {
                const durable_next = @max(next, self.next_job_id);
                const next_raw = try encodeNextJobId(self.alloc, durable_next);
                defer self.alloc.free(next_raw);
                writes[write_count] = .{ .key = next_job_id_key, .value = next_raw };
                write_count += 1;
            }
            if (!isTerminalPhase(parsed.value.phase)) {
                // The active index is only a compact lookup index. The primary
                // job record remains authoritative and both records commit in
                // the same DocStore batch.
                writes[write_count] = .{ .key = active_key, .value = active_job_marker_value };
                write_count += 1;
                try opened.docstore.putBatch(writes[0..write_count], &.{});
            } else {
                try opened.docstore.putBatch(writes[0..write_count], &.{active_key});
            }
        }
        if (self.jobs.fetchPutAssumeCapacity(job_id, owned)) |old| self.alloc.free(old.value);
    }

    fn loadJobLocked(self: *Store, job_id: u64) !?[]const u8 {
        if (self.jobs.get(job_id)) |encoded| return encoded;
        const opened = self.opened_store orelse return null;
        const key = try jobKey(self.alloc, job_id);
        defer self.alloc.free(key);
        const body = opened.docstore.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        errdefer self.alloc.free(body);
        try self.jobs.put(self.alloc, job_id, body);
        return body;
    }

    fn recoverPersistedJobsLocked(self: *Store, opened: *OpenedStore) !void {
        var recovered_max_job_id: u64 = 0;
        const results = try opened.docstore.scanPrefix(self.alloc, active_job_key_prefix);
        defer docstore_mod.DocStore.freeResults(self.alloc, results);
        for (results) |kv| {
            const marker_job_id = try activeJobIdFromKey(kv.key);
            const primary_key = try jobKey(self.alloc, marker_job_id);
            defer self.alloc.free(primary_key);
            const primary = opened.docstore.get(self.alloc, primary_key) catch |err| switch (err) {
                error.NotFound => {
                    // A stale secondary marker cannot resurrect a job. Remove
                    // it and continue from authoritative primary state.
                    try opened.docstore.putBatch(&.{}, &.{kv.key});
                    continue;
                },
                else => return err,
            };
            defer self.alloc.free(primary);
            if (primary.len > max_job_record_bytes) return error.InvalidRepairJobState;
            var parsed = std.json.parseFromSlice(JobState, self.alloc, primary, .{ .ignore_unknown_fields = true }) catch
                return error.InvalidRepairJobState;
            defer parsed.deinit();
            if (parsed.value.job_id != marker_job_id) return error.InvalidRepairJobState;
            try validateJobState(parsed.value);
            const now_ms = nowMillis();
            const was_running = std.mem.eql(u8, parsed.value.phase, phaseString(.running));
            const durable_cancel = requiresDurableCancel(parsed.value);
            const cached = if (was_running or durable_cancel) blk: {
                const recovered_phase: JobPhase = if (durable_cancel)
                    .queued
                else if (parsed.value.cancel_requested)
                    .cancelled
                else
                    .queued;
                break :blk try encodeState(self.alloc, .{
                    .job_id = parsed.value.job_id,
                    .attempt_id = parsed.value.attempt_id,
                    .table_name = parsed.value.table_name,
                    .phase = phaseString(recovered_phase),
                    .repair_status = repairStatusForPhase(recovered_phase, parsed.value.result.has_more, parsed.value.result.debt_remaining),
                    .target = parsed.value.target,
                    .kind = parsed.value.kind,
                    .index = parsed.value.index,
                    // A cancellation traversal always restarts from the first
                    // group: repair pages already visited before interruption
                    // may contain the active attempt being stopped.
                    .cursor = if (durable_cancel) null else parsed.value.cursor,
                    .limit = parsed.value.limit,
                    .force = if (durable_cancel) false else parsed.value.force,
                    .result = parsed.value.result,
                    .last_error = if (durable_cancel and !was_running)
                        parsed.value.last_error
                    else if (parsed.value.cancel_requested)
                        "cancel_requested"
                    else
                        "recovered_interrupted_attempt",
                    .cancel_requested = parsed.value.cancel_requested,
                    .next_retry_at_millis = if (durable_cancel and !was_running)
                        parsed.value.next_retry_at_millis
                    else
                        0,
                    .created_at_millis = parsed.value.created_at_millis,
                    .last_updated_at_millis = now_ms,
                    .expires_at_millis = now_ms + self.retentionMillis(),
                });
            } else try self.alloc.dupe(u8, primary);
            errdefer self.alloc.free(cached);
            if (was_running or durable_cancel) {
                const key = try jobKey(self.alloc, parsed.value.job_id);
                defer self.alloc.free(key);
                const active_key = try activeJobKey(self.alloc, parsed.value.job_id);
                defer self.alloc.free(active_key);
                try opened.docstore.putBatch(&.{
                    .{ .key = key, .value = cached },
                    .{ .key = active_key, .value = active_job_marker_value },
                }, &.{});
            } else if (isTerminalPhase(parsed.value.phase)) {
                // Self-heal an impossible/stale marker without making startup
                // fail; job state remains authoritative.
                const active_key = try activeJobKey(self.alloc, parsed.value.job_id);
                defer self.alloc.free(active_key);
                try opened.docstore.putBatch(&.{}, &.{active_key});
            }
            if (durable_cancel) {
                const enqueued = try self.enqueuePendingCancelLocked(parsed.value.job_id);
                errdefer if (enqueued) self.removePendingCancelLocked(parsed.value.job_id);
            }
            if (try self.jobs.fetchPut(self.alloc, parsed.value.job_id, cached)) |old| self.alloc.free(old.value);
            recovered_max_job_id = @max(recovered_max_job_id, parsed.value.job_id);
        }
        var recovered_next_job_id = recovered_max_job_id +| 1;
        if (loadPersistedNextJobId(self.alloc, opened.docstore)) |persisted_next| {
            recovered_next_job_id = @max(recovered_next_job_id, persisted_next);
        } else |_| {}
        self.next_job_id = @max(self.next_job_id, recovered_next_job_id);
        try persistNextJobId(opened.docstore, self.alloc, self.next_job_id);
    }

    fn encodeCancelledCurrentLocked(self: *Store, alloc: std.mem.Allocator, current: JobState, now_ms: u64) ![]u8 {
        const encoded = try encodeState(alloc, .{
            .job_id = current.job_id,
            .attempt_id = current.attempt_id,
            .table_name = current.table_name,
            .phase = phaseString(.cancelled),
            .repair_status = repairStatusForPhase(.cancelled, current.result.has_more, current.result.debt_remaining),
            .target = current.target,
            .kind = current.kind,
            .index = current.index,
            .cursor = current.cursor,
            .limit = current.limit,
            .force = current.force,
            .result = current.result,
            .last_error = "cancel_requested",
            .cancel_requested = true,
            .created_at_millis = current.created_at_millis,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        try self.storeEncodedLocked(current.job_id, encoded, null);
        return encoded;
    }
};

const running_lease_timeout_ms: u64 = 300_000;
const retry_base_ms: u64 = 250;
const retry_max_ms: u64 = 30 * std.time.ms_per_s;
const pending_cancel_scan_limit: usize = 64;

fn retryDelayMs(job_id: u64, attempt_id: u64) u64 {
    const shift: u6 = @intCast(@min(attempt_id, 16));
    const exponential = @min(retry_max_ms, retry_base_ms *| (@as(u64, 1) << shift));
    var hasher = std.hash.Wyhash.init(job_id);
    hasher.update(std.mem.asBytes(&attempt_id));
    const jitter_span = @max(@as(u64, 1), exponential / 4);
    return @min(retry_max_ms, exponential +| (hasher.final() % jitter_span));
}

fn jobStateTransitionTokenMatches(current: JobState, expected: JobState) bool {
    return current.job_id == expected.job_id and
        std.mem.eql(u8, current.phase, expected.phase) and
        current.attempt_id == expected.attempt_id;
}

pub fn encodeState(alloc: std.mem.Allocator, state: JobState) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, state, .{ .emit_null_optional_fields = false });
}

pub fn phaseString(phase: JobPhase) []const u8 {
    return switch (phase) {
        .queued => "queued",
        .running => "running",
        .succeeded => "succeeded",
        .failed => "failed",
        .cancelled => "cancelled",
    };
}

fn jobPhaseFromString(phase: []const u8) ?JobPhase {
    inline for (std.meta.tags(JobPhase)) |candidate| {
        if (std.mem.eql(u8, phase, phaseString(candidate))) return candidate;
    }
    return null;
}

fn validateJobState(state: JobState) !void {
    if (state.job_id == 0 or state.table_name.len == 0 or
        state.table_name.len > max_job_string_bytes or state.target.len > max_job_string_bytes or
        state.repair_status.len > max_job_string_bytes or state.phase.len > max_job_string_bytes or
        state.limit == 0)
    {
        return error.InvalidRepairJobState;
    }
    const phase = jobPhaseFromString(state.phase) orelse return error.InvalidRepairJobState;
    if (std.meta.stringToEnum(db_mod.types.RepairTarget, state.target) == null) {
        return error.InvalidRepairJobState;
    }
    if (!std.mem.eql(
        u8,
        state.repair_status,
        repairStatusForPhase(phase, state.result.has_more, state.result.debt_remaining),
    )) return error.InvalidRepairJobState;
    if (state.index) |value| {
        if (value.len == 0 or value.len > max_job_string_bytes) return error.InvalidRepairJobState;
    }
    if (state.cursor) |value| {
        if (value.len > max_job_string_bytes) return error.InvalidRepairJobState;
    }
    if (state.last_error) |value| {
        if (value.len > max_job_string_bytes) return error.InvalidRepairJobState;
    }
}

pub fn isTerminalPhase(phase: []const u8) bool {
    return std.mem.eql(u8, phase, phaseString(.succeeded)) or
        std.mem.eql(u8, phase, phaseString(.failed)) or
        std.mem.eql(u8, phase, phaseString(.cancelled));
}

pub fn requiresDurableCancel(state: JobState) bool {
    return state.cancel_requested and
        std.mem.eql(u8, state.target, "index") and
        state.index != null and
        !isTerminalPhase(state.phase);
}

pub fn repairStatusForPhase(phase: JobPhase, has_more: bool, debt_remaining: bool) []const u8 {
    return switch (phase) {
        .succeeded => "complete",
        .failed => if (debt_remaining) "debt_remaining" else "stopped",
        .cancelled => "stopped",
        .queued, .running => if (has_more or debt_remaining) "in_progress" else "in_progress",
    };
}

pub fn nowMillis() u64 {
    return @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms);
}

fn leaseExpired(now_ms: u64, last_updated_ms: u64, timeout_ms: u64) bool {
    if (now_ms < last_updated_ms) return false;
    return now_ms >= last_updated_ms +| timeout_ms;
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn jobKey(alloc: std.mem.Allocator, job_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{d}", .{ job_key_prefix, job_id });
}

fn activeJobKey(alloc: std.mem.Allocator, job_id: u64) ![]u8 {
    const key = try alloc.alloc(u8, active_job_key_prefix.len + @sizeOf(u64));
    @memcpy(key[0..active_job_key_prefix.len], active_job_key_prefix);
    std.mem.writeInt(u64, key[active_job_key_prefix.len..][0..8], job_id, .big);
    return key;
}

fn activeJobIdFromKey(key: []const u8) !u64 {
    if (!std.mem.startsWith(u8, key, active_job_key_prefix) or
        key.len != active_job_key_prefix.len + @sizeOf(u64))
    {
        return error.InvalidRepairJobState;
    }
    return std.mem.readInt(u64, key[active_job_key_prefix.len..][0..8], .big);
}

fn encodeNextJobId(alloc: std.mem.Allocator, next_job_id: u64) ![]u8 {
    const out = try alloc.alloc(u8, @sizeOf(u64));
    std.mem.writeInt(u64, out[0..8], next_job_id, .big);
    return out;
}

fn decodeNextJobId(raw: []const u8) !u64 {
    if (raw.len != @sizeOf(u64)) return error.InvalidRepairJobMetadata;
    return std.mem.readInt(u64, raw[0..8], .big);
}

fn loadPersistedNextJobId(alloc: std.mem.Allocator, store: *docstore_mod.DocStore) !u64 {
    const raw = store.get(alloc, next_job_id_key) catch |err| switch (err) {
        error.NotFound => return error.NotFound,
        else => return err,
    };
    defer alloc.free(raw);
    return try decodeNextJobId(raw);
}

fn persistNextJobId(store: *docstore_mod.DocStore, alloc: std.mem.Allocator, next_job_id: u64) !void {
    const raw = try encodeNextJobId(alloc, next_job_id);
    defer alloc.free(raw);
    try store.put(next_job_id_key, raw);
}

const job_key_prefix = "__api_table_repair_jobs__:";
const next_job_id_key = "__api_table_repair_jobs_meta__:next_job_id";
const active_job_key_prefix = "__api_table_repair_jobs_active__:";
const active_job_marker_value = "1";
const max_job_record_bytes: usize = 1024 * 1024;
const max_job_string_bytes: usize = 64 * 1024;
const durable_cleanup_interval_ms: u64 = 60_000;
const durable_cleanup_scan_limit: usize = 128;

test "table repair job records bounded pass and continuation" {
    const alloc = std.testing.allocator;
    var store = Store.init(alloc, .{});
    defer store.deinit();

    const started = try store.startJob(alloc, "docs", .{ .target = "artifact", .limit = 2 });
    defer alloc.free(started);
    var parsed_start = try std.json.parseFromSlice(JobState, alloc, started, .{ .ignore_unknown_fields = true });
    defer parsed_start.deinit();

    const begin = try store.beginAdvance(alloc, parsed_start.value);
    defer alloc.free(begin.encoded);
    try std.testing.expect(begin.started);
    var parsed_running = try std.json.parseFromSlice(JobState, alloc, begin.encoded, .{ .ignore_unknown_fields = true });
    defer parsed_running.deinit();
    try std.testing.expectEqual(@as(u64, 1), parsed_running.value.attempt_id);

    try store.heartbeatRunning(alloc, parsed_running.value.job_id, parsed_running.value.attempt_id);
    const after_heartbeat = (try store.loadJobAlloc(alloc, parsed_running.value.job_id)).?;
    defer alloc.free(after_heartbeat);
    var parsed_after_heartbeat = try std.json.parseFromSlice(JobState, alloc, after_heartbeat, .{ .ignore_unknown_fields = true });
    defer parsed_after_heartbeat.deinit();
    try std.testing.expectEqual(parsed_running.value.attempt_id, parsed_after_heartbeat.value.attempt_id);

    var pass: db_mod.types.ArtifactRepairResult = .{
        .scanned = 2,
        .repaired = 2,
        .limit = 2,
        .has_more = true,
        .debt_remaining = true,
        .next_cursor = try alloc.dupe(u8, "cursor-1"),
    };
    defer pass.deinit(alloc);

    const updated = try store.recordPass(alloc, parsed_running.value, pass);
    defer alloc.free(updated);
    var parsed_update = try std.json.parseFromSlice(JobState, alloc, updated, .{ .ignore_unknown_fields = true });
    defer parsed_update.deinit();
    try std.testing.expectEqualStrings("queued", parsed_update.value.phase);
    try std.testing.expectEqualStrings("cursor-1", parsed_update.value.cursor.?);
    try std.testing.expectEqual(@as(u64, 2), parsed_update.value.result.repaired);
}

test "forced index repair job dispatches force only once" {
    const alloc = std.testing.allocator;
    var store = Store.init(alloc, .{});
    defer store.deinit();

    const started = try store.startJob(alloc, "docs", .{
        .target = "index",
        .index = "semantic",
        .cursor = "group:65",
        .force = true,
    });
    defer alloc.free(started);
    var parsed_started = try std.json.parseFromSlice(JobState, alloc, started, .{ .ignore_unknown_fields = true });
    defer parsed_started.deinit();
    const begin = try store.beginAdvance(alloc, parsed_started.value);
    defer alloc.free(begin.encoded);
    var parsed_running = try std.json.parseFromSlice(JobState, alloc, begin.encoded, .{ .ignore_unknown_fields = true });
    defer parsed_running.deinit();

    const updated = try store.recordPass(alloc, parsed_running.value, .{
        .scanned = 1,
        .in_progress = 1,
        .unresolved = 1,
        .debt_remaining = true,
    });
    defer alloc.free(updated);
    var parsed_updated = try std.json.parseFromSlice(JobState, alloc, updated, .{ .ignore_unknown_fields = true });
    defer parsed_updated.deinit();
    try std.testing.expectEqualStrings("queued", parsed_updated.value.phase);
    try std.testing.expect(!parsed_updated.value.force);
}

test "durable cancellation retries transient failures with backoff" {
    const alloc = std.testing.allocator;
    var store = Store.init(alloc, .{});
    defer store.deinit();

    const started = try store.startJob(alloc, "docs", .{ .target = "index", .index = "semantic" });
    defer alloc.free(started);
    var parsed_started = try std.json.parseFromSlice(JobState, alloc, started, .{ .ignore_unknown_fields = true });
    defer parsed_started.deinit();
    const cancelling = try store.requestCancel(alloc, parsed_started.value);
    defer alloc.free(cancelling);
    var parsed_cancelling = try std.json.parseFromSlice(JobState, alloc, cancelling, .{ .ignore_unknown_fields = true });
    defer parsed_cancelling.deinit();
    const begin = try store.beginAdvance(alloc, parsed_cancelling.value);
    defer alloc.free(begin.encoded);
    var running = try std.json.parseFromSlice(JobState, alloc, begin.encoded, .{ .ignore_unknown_fields = true });
    defer running.deinit();

    const queued = try store.recordRetryableFailure(alloc, running.value, "RepairOwnershipLost");
    defer alloc.free(queued);
    var parsed_queued = try std.json.parseFromSlice(JobState, alloc, queued, .{ .ignore_unknown_fields = true });
    defer parsed_queued.deinit();
    try std.testing.expectEqualStrings("queued", parsed_queued.value.phase);
    try std.testing.expect(parsed_queued.value.next_retry_at_millis > parsed_queued.value.last_updated_at_millis);
    try std.testing.expect((try store.nextPendingDurableCancelAlloc(alloc)) == null);
    const early = try store.beginAdvance(alloc, parsed_queued.value);
    defer alloc.free(early.encoded);
    try std.testing.expect(!early.started);
}

test "durable cancellation scan rotates past a backed off head window" {
    const alloc = std.testing.allocator;
    var store = Store.init(alloc, .{});
    defer store.deinit();

    var runnable_job_id: u64 = 0;
    for (0..pending_cancel_scan_limit + 1) |idx| {
        const started = try store.startJob(alloc, "docs", .{
            .target = "index",
            .index = "semantic",
        });
        defer alloc.free(started);
        var parsed_started = try std.json.parseFromSlice(JobState, alloc, started, .{ .ignore_unknown_fields = true });
        defer parsed_started.deinit();

        const cancelling = try store.requestCancel(alloc, parsed_started.value);
        defer alloc.free(cancelling);
        var parsed_cancelling = try std.json.parseFromSlice(JobState, alloc, cancelling, .{ .ignore_unknown_fields = true });
        defer parsed_cancelling.deinit();
        if (idx == pending_cancel_scan_limit) {
            runnable_job_id = parsed_cancelling.value.job_id;
            continue;
        }

        const current = parsed_cancelling.value;
        const delayed = try encodeState(alloc, .{
            .job_id = current.job_id,
            .attempt_id = current.attempt_id,
            .table_name = current.table_name,
            .phase = current.phase,
            .repair_status = current.repair_status,
            .target = current.target,
            .kind = current.kind,
            .index = current.index,
            .cursor = current.cursor,
            .limit = current.limit,
            .force = current.force,
            .result = current.result,
            .last_error = current.last_error,
            .cancel_requested = current.cancel_requested,
            .next_retry_at_millis = std.math.maxInt(u64),
            .created_at_millis = current.created_at_millis,
            .last_updated_at_millis = current.last_updated_at_millis,
            .expires_at_millis = current.expires_at_millis,
        });
        defer alloc.free(delayed);
        try store.storeEncodedForTest(current.job_id, delayed);
    }

    // The first bounded inspection sees only delayed work and leaves its
    // cursor immediately after that window. The next inspection must select
    // the runnable tail instead of rescanning the same head entries.
    try std.testing.expect((try store.nextPendingDurableCancelAlloc(alloc)) == null);
    const pending = (try store.nextPendingDurableCancelAlloc(alloc)).?;
    defer alloc.free(pending);
    var parsed_pending = try std.json.parseFromSlice(JobState, alloc, pending, .{ .ignore_unknown_fields = true });
    defer parsed_pending.deinit();
    try std.testing.expectEqual(runnable_job_id, parsed_pending.value.job_id);
}

test "named index repair cancellation remains nonterminal until durable controls finish" {
    const alloc = std.testing.allocator;
    var store = Store.init(alloc, .{});
    defer store.deinit();

    const started = try store.startJob(alloc, "docs", .{
        .target = "index",
        .index = "semantic",
    });
    defer alloc.free(started);
    var parsed_started = try std.json.parseFromSlice(JobState, alloc, started, .{ .ignore_unknown_fields = true });
    defer parsed_started.deinit();
    const cancelling = try store.requestCancel(alloc, parsed_started.value);
    defer alloc.free(cancelling);
    var parsed_cancelling = try std.json.parseFromSlice(JobState, alloc, cancelling, .{ .ignore_unknown_fields = true });
    defer parsed_cancelling.deinit();
    try std.testing.expectEqualStrings("queued", parsed_cancelling.value.phase);
    try std.testing.expectEqual(@as(?[]const u8, null), parsed_cancelling.value.cursor);
    try std.testing.expect(requiresDurableCancel(parsed_cancelling.value));
    const first_pending = (try store.nextPendingDurableCancelAlloc(alloc)).?;
    defer alloc.free(first_pending);

    const first_begin = try store.beginAdvance(alloc, parsed_cancelling.value);
    defer alloc.free(first_begin.encoded);
    var parsed_first = try std.json.parseFromSlice(JobState, alloc, first_begin.encoded, .{ .ignore_unknown_fields = true });
    defer parsed_first.deinit();
    try std.testing.expect((try store.nextPendingDurableCancelAlloc(alloc)) == null);
    var first_pass = db_mod.types.ArtifactRepairResult{
        .scanned = 64,
        .groups_scanned = 64,
        .controls_applied = 64,
        .has_more = true,
        .next_cursor = try alloc.dupe(u8, "group:65"),
    };
    defer first_pass.deinit(alloc);
    const continuing = try store.recordDurableCancelPass(alloc, parsed_first.value, first_pass);
    defer alloc.free(continuing);
    var parsed_continuing = try std.json.parseFromSlice(JobState, alloc, continuing, .{ .ignore_unknown_fields = true });
    defer parsed_continuing.deinit();
    try std.testing.expectEqualStrings("queued", parsed_continuing.value.phase);
    try std.testing.expectEqualStrings("group:65", parsed_continuing.value.cursor.?);
    const continuing_pending = (try store.nextPendingDurableCancelAlloc(alloc)).?;
    defer alloc.free(continuing_pending);

    const second_begin = try store.beginAdvance(alloc, parsed_continuing.value);
    defer alloc.free(second_begin.encoded);
    var parsed_second = try std.json.parseFromSlice(JobState, alloc, second_begin.encoded, .{ .ignore_unknown_fields = true });
    defer parsed_second.deinit();
    const cancelled = try store.recordDurableCancelPass(alloc, parsed_second.value, .{
        .scanned = 1,
        .groups_scanned = 1,
        .controls_applied = 1,
    });
    defer alloc.free(cancelled);
    var parsed_cancelled = try std.json.parseFromSlice(JobState, alloc, cancelled, .{ .ignore_unknown_fields = true });
    defer parsed_cancelled.deinit();
    try std.testing.expectEqualStrings("cancelled", parsed_cancelled.value.phase);
    try std.testing.expect(!requiresDurableCancel(parsed_cancelled.value));
}

test "named index repair cancellation restarts its durable traversal after job store recovery" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/repair-job-cancel-recovery", .{tmp.sub_path});
    defer alloc.free(path);

    var job_id: u64 = 0;
    var terminal_job_id: u64 = 0;
    {
        const opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        var store = Store.init(alloc, .{});
        defer store.deinit();
        try store.attachOpenedStore(opened);

        const started = try store.startJob(alloc, "docs", .{
            .target = "index",
            .index = "semantic",
            .cursor = "group:65",
            .force = true,
        });
        defer alloc.free(started);
        var parsed_started = try std.json.parseFromSlice(JobState, alloc, started, .{ .ignore_unknown_fields = true });
        defer parsed_started.deinit();
        const begin = try store.beginAdvance(alloc, parsed_started.value);
        defer alloc.free(begin.encoded);
        var parsed_running = try std.json.parseFromSlice(JobState, alloc, begin.encoded, .{ .ignore_unknown_fields = true });
        defer parsed_running.deinit();
        job_id = parsed_running.value.job_id;
        const cancelling = try store.requestCancel(alloc, parsed_running.value);
        defer alloc.free(cancelling);

        // The active marker is a secondary lookup index, not job state. A
        // damaged marker value and an orphan marker must be harmless on the
        // next attach: primary records are authoritative and the orphan is
        // removed in-place.
        const damaged_marker = try activeJobKey(alloc, job_id);
        defer alloc.free(damaged_marker);
        try opened.docstore.put(damaged_marker, "not-json");
        const orphan_marker = try activeJobKey(alloc, 9_999_999);
        defer alloc.free(orphan_marker);
        try opened.docstore.put(orphan_marker, active_job_marker_value);

        const terminal_started = try store.startJob(alloc, "docs", .{ .target = "artifact" });
        defer alloc.free(terminal_started);
        var parsed_terminal_started = try std.json.parseFromSlice(JobState, alloc, terminal_started, .{ .ignore_unknown_fields = true });
        defer parsed_terminal_started.deinit();
        terminal_job_id = parsed_terminal_started.value.job_id;
        const terminal = try store.markPhase(alloc, parsed_terminal_started.value, .succeeded, null);
        defer alloc.free(terminal);
    }

    {
        const opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        var store = Store.init(alloc, .{});
        defer store.deinit();
        try store.attachOpenedStore(opened);

        // Restart scans only the fixed-width active index and validates each
        // marker against its primary record. Retained terminal history remains
        // lazy and does not consume startup I/O or cache space.
        try std.testing.expectEqual(@as(usize, 1), store.jobs.count());
        const active = try opened.docstore.scanPrefix(alloc, active_job_key_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, active);
        try std.testing.expectEqual(@as(usize, 1), active.len);

        const recovered = (try store.loadJobAlloc(alloc, job_id)) orelse return error.TestUnexpectedResult;
        defer alloc.free(recovered);
        var parsed = try std.json.parseFromSlice(JobState, alloc, recovered, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqualStrings("queued", parsed.value.phase);
        try std.testing.expect(parsed.value.cancel_requested);
        try std.testing.expect(parsed.value.cursor == null);
        try std.testing.expect(!parsed.value.force);
        try std.testing.expect(requiresDurableCancel(parsed.value));
        const pending = (try store.nextPendingDurableCancelAlloc(alloc)).?;
        defer alloc.free(pending);

        const terminal = (try store.loadJobAlloc(alloc, terminal_job_id)) orelse return error.TestUnexpectedResult;
        defer alloc.free(terminal);
        try std.testing.expectEqual(@as(usize, 2), store.jobs.count());
    }
}

test "table repair job recovery rejects corrupt authoritative primary state" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/repair-job-corrupt-primary", .{tmp.sub_path});
    defer alloc.free(path);

    {
        const opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        var store = Store.init(alloc, .{});
        defer store.deinit();
        try store.attachOpenedStore(opened);
        const started = try store.startJob(alloc, "docs", .{ .target = "artifact" });
        defer alloc.free(started);
        var parsed = try std.json.parseFromSlice(JobState, alloc, started, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const primary_key = try jobKey(alloc, parsed.value.job_id);
        defer alloc.free(primary_key);
        try opened.docstore.put(primary_key, "{\"job_id\":1,\"phase\":\"unknown\"}");
    }

    const opened = try alloc.create(OpenedStore);
    opened.* = try OpenedStore.open(alloc, path);
    var store = Store.init(alloc, .{});
    defer store.deinit();
    try std.testing.expectError(error.InvalidRepairJobState, store.attachOpenedStore(opened));
    try std.testing.expectEqual(@as(usize, 0), store.jobs.count());
    opened.deinit();
    alloc.destroy(opened);
}

test "active repair job keys reject malformed secondary entries" {
    try std.testing.expectError(error.InvalidRepairJobState, activeJobIdFromKey(active_job_key_prefix));
    try std.testing.expectError(error.InvalidRepairJobState, activeJobIdFromKey(active_job_key_prefix ++ "short"));
}

test "table repair job store persists monotonic next id across stale durable writes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/repair-jobs-monotonic-next-id", .{tmp.sub_path});
    defer alloc.free(path);

    {
        const opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        var store = Store.init(alloc, .{});
        defer store.deinit();
        try store.attachOpenedStore(opened);

        const first = store.reserveJobId();
        const second = store.reserveJobId();
        try std.testing.expectEqual(@as(u64, 1), first.job_id);
        try std.testing.expectEqual(@as(u64, 2), second.job_id);

        const now_ms = nowMillis();
        const encoded_second = try encodeState(alloc, .{
            .job_id = second.job_id,
            .table_name = "docs",
            .phase = phaseString(.queued),
            .repair_status = repairStatusForPhase(.queued, false, false),
            .target = "artifact",
            .limit = 1,
            .result = .{ .limit = 1 },
            .created_at_millis = now_ms,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + store.retentionMillis(),
        });
        defer alloc.free(encoded_second);
        try store.storeEncoded(second.job_id, encoded_second, second.next_job_id);

        const encoded_first = try encodeState(alloc, .{
            .job_id = first.job_id,
            .table_name = "docs",
            .phase = phaseString(.queued),
            .repair_status = repairStatusForPhase(.queued, false, false),
            .target = "artifact",
            .limit = 1,
            .result = .{ .limit = 1 },
            .created_at_millis = now_ms,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + store.retentionMillis(),
        });
        defer alloc.free(encoded_first);
        try store.storeEncoded(first.job_id, encoded_first, first.next_job_id);
    }

    {
        const opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        var store = Store.init(alloc, .{});
        defer store.deinit();
        try store.attachOpenedStore(opened);

        const third = try store.startJob(alloc, "docs", .{ .target = "artifact", .limit = 1 });
        defer alloc.free(third);
        var parsed_third = try std.json.parseFromSlice(JobState, alloc, third, .{ .ignore_unknown_fields = true });
        defer parsed_third.deinit();
        try std.testing.expectEqual(@as(u64, 3), parsed_third.value.job_id);

        const first = (try store.loadJobAlloc(alloc, 1)) orelse return error.TestUnexpectedResult;
        defer alloc.free(first);
        var parsed_first = try std.json.parseFromSlice(JobState, alloc, first, .{ .ignore_unknown_fields = true });
        defer parsed_first.deinit();
        try std.testing.expectEqual(@as(u64, 1), parsed_first.value.job_id);

        const second = (try store.loadJobAlloc(alloc, 2)) orelse return error.TestUnexpectedResult;
        defer alloc.free(second);
        var parsed_second = try std.json.parseFromSlice(JobState, alloc, second, .{ .ignore_unknown_fields = true });
        defer parsed_second.deinit();
        try std.testing.expectEqual(@as(u64, 2), parsed_second.value.job_id);
    }
}

test "table repair job cleanup pages durable expired jobs" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/repair-jobs-cleanup-page", .{tmp.sub_path});
    defer alloc.free(path);

    const opened = try alloc.create(OpenedStore);
    errdefer alloc.destroy(opened);
    opened.* = try OpenedStore.open(alloc, path);
    var store = Store.init(alloc, .{});
    defer store.deinit();
    try store.attachOpenedStore(opened);

    const now_ms = nowMillis();
    var id: u64 = 1;
    while (id <= @as(u64, @intCast(durable_cleanup_scan_limit + 2))) : (id += 1) {
        const encoded = try encodeState(alloc, .{
            .job_id = id,
            .table_name = "docs",
            .phase = phaseString(.succeeded),
            .repair_status = repairStatusForPhase(.succeeded, false, false),
            .target = "artifact",
            .limit = 1,
            .result = .{ .limit = 1 },
            .created_at_millis = now_ms - 2,
            .last_updated_at_millis = now_ms - 2,
            .expires_at_millis = now_ms - 1,
        });
        defer alloc.free(encoded);
        try store.storeEncoded(id, encoded, null);
    }

    store.cleanupExpiredJobs();
    const first_remaining = try store.opened_store.?.docstore.scanPrefix(alloc, job_key_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, first_remaining);
    try std.testing.expectEqual(@as(usize, 2), first_remaining.len);

    store.cleanupExpiredJobs();
    const throttled_remaining = try store.opened_store.?.docstore.scanPrefix(alloc, job_key_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, throttled_remaining);
    try std.testing.expectEqual(@as(usize, 2), throttled_remaining.len);

    store.last_durable_cleanup_ms = 0;
    store.cleanupExpiredJobs();
    const final_remaining = try store.opened_store.?.docstore.scanPrefix(alloc, job_key_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, final_remaining);
    try std.testing.expectEqual(@as(usize, 0), final_remaining.len);
}
