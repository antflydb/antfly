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
const platform_time = @import("antfly_platform").time;
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
    created_at_millis: u64,
    last_updated_at_millis: u64,
    expires_at_millis: u64,
};

pub const BeginAdvanceResult = struct {
    encoded: []u8,
    started: bool,
};

pub const Store = struct {
    alloc: std.mem.Allocator,
    cfg: StoreConfig,
    opened_store: ?*OpenedStore = null,
    mutex: std.atomic.Mutex = .unlocked,
    jobs: std.AutoHashMapUnmanaged(u64, []u8) = .{},
    next_job_id: u64 = 1,
    last_durable_cleanup_ms: u64 = 0,
    durable_cleanup_cursor: ?[]u8 = null,

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
        if (self.durable_cleanup_cursor) |cursor| self.alloc.free(cursor);
        if (self.opened_store) |store| {
            store.deinit();
            self.alloc.destroy(store);
        }
        self.* = undefined;
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
            .force = previous.force,
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
        if (current.cancel_requested and (!is_running or running_expired)) {
            return .{ .encoded = try self.encodeCancelledCurrentLocked(alloc, current, now_ms), .started = false };
        }
        if (is_running and !running_expired) {
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
            .cancel_requested = false,
            .created_at_millis = current.created_at_millis,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        try self.storeEncodedLocked(current.job_id, encoded, null);
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

        const phase: JobPhase = if (std.mem.eql(u8, current.phase, phaseString(.running))) .running else .cancelled;
        const encoded = try encodeState(alloc, .{
            .job_id = current.job_id,
            .attempt_id = current.attempt_id,
            .table_name = current.table_name,
            .phase = phaseString(phase),
            .repair_status = repairStatusForPhase(phase, current.result.has_more, current.result.debt_remaining),
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
            }
        }
        if (durable_delete_keys.items.len > 0) {
            if (self.opened_store) |opened| opened.docstore.putBatch(&.{}, durable_delete_keys.items) catch {};
        }
        for (expired.items) |job_id| {
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
        const owned = try self.alloc.dupe(u8, encoded);
        errdefer self.alloc.free(owned);
        if (self.opened_store) |opened| {
            const key = try jobKey(self.alloc, job_id);
            defer self.alloc.free(key);
            if (next_job_id) |next| {
                const durable_next = @max(next, self.next_job_id);
                const next_raw = try encodeNextJobId(self.alloc, durable_next);
                defer self.alloc.free(next_raw);
                const writes = [_]docstore_mod.KVPair{
                    .{ .key = key, .value = encoded },
                    .{ .key = next_job_id_key, .value = next_raw },
                };
                try opened.docstore.putBatch(&writes, &.{});
            } else {
                try opened.docstore.put(key, encoded);
            }
        }
        if (try self.jobs.fetchPut(self.alloc, job_id, owned)) |old| self.alloc.free(old.value);
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
        const results = try opened.docstore.scanPrefix(self.alloc, job_key_prefix);
        defer docstore_mod.DocStore.freeResults(self.alloc, results);
        for (results) |kv| {
            var parsed = std.json.parseFromSlice(JobState, self.alloc, kv.value, .{ .ignore_unknown_fields = true }) catch continue;
            defer parsed.deinit();
            const now_ms = nowMillis();
            const cached = if (std.mem.eql(u8, parsed.value.phase, phaseString(.running))) blk: {
                const recovered_phase: JobPhase = if (parsed.value.cancel_requested) .cancelled else .queued;
                break :blk try encodeState(self.alloc, .{
                    .job_id = parsed.value.job_id,
                    .attempt_id = parsed.value.attempt_id,
                    .table_name = parsed.value.table_name,
                    .phase = phaseString(recovered_phase),
                    .repair_status = repairStatusForPhase(recovered_phase, parsed.value.result.has_more, parsed.value.result.debt_remaining),
                    .target = parsed.value.target,
                    .kind = parsed.value.kind,
                    .index = parsed.value.index,
                    .cursor = parsed.value.cursor,
                    .limit = parsed.value.limit,
                    .force = parsed.value.force,
                    .result = parsed.value.result,
                    .last_error = if (parsed.value.cancel_requested) "cancel_requested" else "recovered_interrupted_attempt",
                    .cancel_requested = parsed.value.cancel_requested,
                    .created_at_millis = parsed.value.created_at_millis,
                    .last_updated_at_millis = now_ms,
                    .expires_at_millis = now_ms + self.retentionMillis(),
                });
            } else try self.alloc.dupe(u8, kv.value);
            errdefer self.alloc.free(cached);
            if (std.mem.eql(u8, parsed.value.phase, phaseString(.running))) {
                const key = try jobKey(self.alloc, parsed.value.job_id);
                defer self.alloc.free(key);
                try opened.docstore.put(key, cached);
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

pub fn isTerminalPhase(phase: []const u8) bool {
    return std.mem.eql(u8, phase, phaseString(.succeeded)) or
        std.mem.eql(u8, phase, phaseString(.failed)) or
        std.mem.eql(u8, phase, phaseString(.cancelled));
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
