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
const db_mod = @import("../storage/db/selected_root.zig").db;
const platform_time = @import("antfly_platform").time;

pub const StoreConfig = struct {
    artifact_reprocess_job_store_path: ?[]const u8 = null,
    artifact_reprocess_job_retention_ms: ?u64 = null,
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
    from_key: []const u8 = "",
    to_key: []const u8 = "",
    limit: u32 = 100,
    advance: bool = true,
};

pub const JobState = struct {
    job_id: u64,
    attempt_id: u64 = 0,
    table_name: []const u8,
    artifact_name: []const u8,
    phase: []const u8,
    reprocess_status: []const u8,
    from_key: []const u8,
    to_key: []const u8,
    limit: u32,
    next_key: ?[]const u8 = null,
    scanned: usize = 0,
    reprocessed: usize = 0,
    skipped: usize = 0,
    failed: usize = 0,
    pending_shards: usize = 0,
    failures: []const db_mod.types.DocumentArtifactReprocessFailure = &.{},
    shard_cursors: []const db_mod.types.DocumentArtifactReprocessShardCursor = &.{},
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
        if (self.opened_store) |store| {
            store.deinit();
            self.alloc.destroy(store);
        }
        self.* = undefined;
    }

    pub fn retentionMillis(self: *const Store) u64 {
        return self.cfg.artifact_reprocess_job_retention_ms orelse 86_400_000;
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
        artifact_name: []const u8,
        req: StartRequest,
    ) ![]u8 {
        const now_ms = nowMillis();
        const reserved = self.reserveJobId();
        const encoded = try encodeState(alloc, .{
            .job_id = reserved.job_id,
            .attempt_id = 0,
            .table_name = table_name,
            .artifact_name = artifact_name,
            .phase = phaseString(.queued),
            .reprocess_status = "in_progress",
            .from_key = req.from_key,
            .to_key = req.to_key,
            .limit = if (req.limit == 0) 100 else req.limit,
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
        if (!jobStateTransitionTokenMatches(parsed_current.value, previous)) {
            return try alloc.dupe(u8, current_encoded);
        }
        const current = parsed_current.value;
        const cancel_requested = phase == .cancelled or previous.cancel_requested or current.cancel_requested;

        const encoded = try encodeState(alloc, .{
            .job_id = previous.job_id,
            .attempt_id = previous.attempt_id,
            .table_name = previous.table_name,
            .artifact_name = previous.artifact_name,
            .phase = phaseString(phase),
            .reprocess_status = reprocessStatusForPhase(phase, previous.pending_shards),
            .from_key = previous.from_key,
            .to_key = previous.to_key,
            .limit = previous.limit,
            .next_key = previous.next_key,
            .scanned = previous.scanned,
            .reprocessed = previous.reprocessed,
            .skipped = previous.skipped,
            .failed = previous.failed,
            .pending_shards = previous.pending_shards,
            .failures = previous.failures,
            .shard_cursors = previous.shard_cursors,
            .last_error = if (cancel_requested) "cancel_requested" else last_error,
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
        pass: db_mod.types.DocumentArtifactTableReprocessResult,
    ) ![]u8 {
        const pending_shards = if (pass.shard_cursors.len > 0)
            pass.shard_cursors.len
        else if (pass.next_key != null)
            @as(usize, 1)
        else
            @as(usize, 0);
        const phase: JobPhase = if (pending_shards == 0) .succeeded else .queued;
        const now_ms = nowMillis();

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const current_encoded = (try self.loadJobLocked(previous.job_id)) orelse return error.NotFound;
        var parsed_current = try std.json.parseFromSlice(JobState, self.alloc, current_encoded, .{ .ignore_unknown_fields = true });
        defer parsed_current.deinit();
        if (!jobStateTransitionTokenMatches(parsed_current.value, previous)) {
            return try alloc.dupe(u8, current_encoded);
        }
        const current = parsed_current.value;
        const cancel_requested = previous.cancel_requested or current.cancel_requested;
        const final_phase: JobPhase = if (cancel_requested) .cancelled else phase;

        const encoded = try encodeState(alloc, .{
            .job_id = previous.job_id,
            .attempt_id = previous.attempt_id,
            .table_name = previous.table_name,
            .artifact_name = previous.artifact_name,
            .phase = phaseString(final_phase),
            .reprocess_status = reprocessStatusForPhase(final_phase, pending_shards),
            .from_key = previous.from_key,
            .to_key = previous.to_key,
            .limit = pass.limit,
            .next_key = pass.next_key,
            .scanned = previous.scanned + pass.scanned,
            .reprocessed = previous.reprocessed + pass.reprocessed,
            .skipped = previous.skipped + pass.skipped,
            .failed = previous.failed + pass.failed,
            .pending_shards = pending_shards,
            .failures = pass.failures,
            .shard_cursors = pass.shard_cursors,
            .last_error = if (cancel_requested) "cancel_requested" else null,
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
            .artifact_name = current.artifact_name,
            .phase = phaseString(.running),
            .reprocess_status = reprocessStatusForPhase(.running, current.pending_shards),
            .from_key = current.from_key,
            .to_key = current.to_key,
            .limit = current.limit,
            .next_key = current.next_key,
            .scanned = current.scanned,
            .reprocessed = current.reprocessed,
            .skipped = current.skipped,
            .failed = current.failed,
            .pending_shards = current.pending_shards,
            .failures = current.failures,
            .shard_cursors = current.shard_cursors,
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

    pub fn requestCancel(self: *Store, alloc: std.mem.Allocator, expected: JobState) ![]u8 {
        const now_ms = nowMillis();
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        const current_encoded = (try self.loadJobLocked(expected.job_id)) orelse return error.NotFound;
        var parsed_current = try std.json.parseFromSlice(JobState, self.alloc, current_encoded, .{ .ignore_unknown_fields = true });
        defer parsed_current.deinit();
        const current = parsed_current.value;
        if (isTerminalPhase(current.phase)) return try alloc.dupe(u8, current_encoded);
        if (!std.mem.eql(u8, current.table_name, expected.table_name) or !std.mem.eql(u8, current.artifact_name, expected.artifact_name)) return error.NotFound;
        if (current.cancel_requested) return try alloc.dupe(u8, current_encoded);

        const phase: JobPhase = if (std.mem.eql(u8, current.phase, phaseString(.running))) .running else .cancelled;
        const encoded = try encodeState(alloc, .{
            .job_id = current.job_id,
            .attempt_id = current.attempt_id,
            .table_name = current.table_name,
            .artifact_name = current.artifact_name,
            .phase = phaseString(phase),
            .reprocess_status = reprocessStatusForPhase(phase, current.pending_shards),
            .from_key = current.from_key,
            .to_key = current.to_key,
            .limit = current.limit,
            .next_key = current.next_key,
            .scanned = current.scanned,
            .reprocessed = current.reprocessed,
            .skipped = current.skipped,
            .failed = current.failed,
            .pending_shards = current.pending_shards,
            .failures = current.failures,
            .shard_cursors = current.shard_cursors,
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
        var durable_results: []docstore_mod.OwnedKVPair = &.{};
        var durable_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer durable_delete_keys.deinit(self.alloc);
        if (self.opened_store) |opened| {
            durable_results = opened.docstore.scanPrefix(self.alloc, job_key_prefix) catch &.{};
            for (durable_results) |kv| {
                var parsed = std.json.parseFromSlice(JobState, self.alloc, kv.value, .{ .ignore_unknown_fields = true }) catch continue;
                defer parsed.deinit();
                if (parsed.value.expires_at_millis == 0 or parsed.value.expires_at_millis > now_ms) continue;
                expired.append(self.alloc, parsed.value.job_id) catch continue;
                durable_delete_keys.append(self.alloc, kv.key) catch continue;
            }
        }
        defer if (durable_results.len > 0) docstore_mod.DocStore.freeResults(self.alloc, durable_results);
        if (durable_delete_keys.items.len > 0) {
            if (self.opened_store) |opened| opened.docstore.putBatch(&.{}, durable_delete_keys.items) catch {};
        }
        for (expired.items) |job_id| {
            if (self.jobs.fetchRemove(job_id)) |removed| self.alloc.free(removed.value);
        }
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
                    .artifact_name = parsed.value.artifact_name,
                    .phase = phaseString(recovered_phase),
                    .reprocess_status = reprocessStatusForPhase(recovered_phase, parsed.value.pending_shards),
                    .from_key = parsed.value.from_key,
                    .to_key = parsed.value.to_key,
                    .limit = parsed.value.limit,
                    .next_key = parsed.value.next_key,
                    .scanned = parsed.value.scanned,
                    .reprocessed = parsed.value.reprocessed,
                    .skipped = parsed.value.skipped,
                    .failed = parsed.value.failed,
                    .pending_shards = parsed.value.pending_shards,
                    .failures = parsed.value.failures,
                    .shard_cursors = parsed.value.shard_cursors,
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
            .artifact_name = current.artifact_name,
            .phase = phaseString(.cancelled),
            .reprocess_status = reprocessStatusForPhase(.cancelled, current.pending_shards),
            .from_key = current.from_key,
            .to_key = current.to_key,
            .limit = current.limit,
            .next_key = current.next_key,
            .scanned = current.scanned,
            .reprocessed = current.reprocessed,
            .skipped = current.skipped,
            .failed = current.failed,
            .pending_shards = current.pending_shards,
            .failures = current.failures,
            .shard_cursors = current.shard_cursors,
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

pub fn reprocessStatusForPhase(phase: JobPhase, pending_shards: usize) []const u8 {
    return switch (phase) {
        .succeeded => "complete",
        .failed, .cancelled => "stopped",
        .queued, .running => if (pending_shards == 0) "in_progress" else "in_progress",
    };
}

pub fn nowMillis() u64 {
    return @divTrunc(platform_time.realtimeNs(), std.time.ns_per_ms);
}

fn leaseExpired(now_ms: u64, last_updated_ms: u64, timeout_ms: u64) bool {
    if (now_ms < last_updated_ms) return false;
    return now_ms >= last_updated_ms +| timeout_ms;
}

fn jobKey(alloc: std.mem.Allocator, job_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{d}", .{ job_key_prefix, job_id });
}

const job_key_prefix = "__api_artifact_reprocess_jobs__:";
const next_job_id_key = "__api_artifact_reprocess_jobs_meta__:next_job_id";

fn encodeNextJobId(alloc: std.mem.Allocator, next_job_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{d}", .{next_job_id});
}

fn loadPersistedNextJobId(alloc: std.mem.Allocator, docstore: *docstore_mod.DocStore) !u64 {
    const raw = docstore.get(alloc, next_job_id_key) catch |err| switch (err) {
        error.NotFound => return error.NotFound,
        else => return err,
    };
    defer alloc.free(raw);
    return try std.fmt.parseUnsigned(u64, std.mem.trim(u8, raw, " \t\r\n"), 10);
}

fn persistNextJobId(docstore: *docstore_mod.DocStore, alloc: std.mem.Allocator, next_job_id: u64) !void {
    const raw = try encodeNextJobId(alloc, next_job_id);
    defer alloc.free(raw);
    try docstore.put(next_job_id_key, raw);
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

test "artifact reprocess job store starts and updates a job" {
    const alloc = std.testing.allocator;
    var store = Store.init(alloc, .{});
    defer store.deinit();

    const started = try store.startJob(alloc, "docs", "document_units_v1", .{ .limit = 2 });
    defer alloc.free(started);
    var parsed_start = try std.json.parseFromSlice(JobState, alloc, started, .{ .ignore_unknown_fields = true });
    defer parsed_start.deinit();
    try std.testing.expectEqualStrings("queued", parsed_start.value.phase);

    const pass = db_mod.types.DocumentArtifactTableReprocessResult{
        .scanned = 2,
        .reprocessed = 2,
        .limit = 2,
    };
    const updated = try store.recordPass(alloc, parsed_start.value, pass);
    defer alloc.free(updated);
    var parsed_update = try std.json.parseFromSlice(JobState, alloc, updated, .{ .ignore_unknown_fields = true });
    defer parsed_update.deinit();
    try std.testing.expectEqualStrings("succeeded", parsed_update.value.phase);
    try std.testing.expectEqual(@as(usize, 2), parsed_update.value.scanned);
}

test "artifact reprocess job store guards duplicate advances and stale pass records" {
    const alloc = std.testing.allocator;
    var store = Store.init(alloc, .{});
    defer store.deinit();

    const started = try store.startJob(alloc, "docs", "document_units_v1", .{ .limit = 2 });
    defer alloc.free(started);
    var parsed_start = try std.json.parseFromSlice(JobState, alloc, started, .{ .ignore_unknown_fields = true });
    defer parsed_start.deinit();

    const first_begin = try store.beginAdvance(alloc, parsed_start.value);
    defer alloc.free(first_begin.encoded);
    try std.testing.expect(first_begin.started);
    var parsed_running = try std.json.parseFromSlice(JobState, alloc, first_begin.encoded, .{ .ignore_unknown_fields = true });
    defer parsed_running.deinit();
    try std.testing.expectEqualStrings("running", parsed_running.value.phase);

    const duplicate_begin = try store.beginAdvance(alloc, parsed_start.value);
    defer alloc.free(duplicate_begin.encoded);
    try std.testing.expect(!duplicate_begin.started);
    var parsed_duplicate = try std.json.parseFromSlice(JobState, alloc, duplicate_begin.encoded, .{ .ignore_unknown_fields = true });
    defer parsed_duplicate.deinit();
    try std.testing.expectEqualStrings("running", parsed_duplicate.value.phase);
    try std.testing.expectEqual(parsed_running.value.last_updated_at_millis, parsed_duplicate.value.last_updated_at_millis);

    const pass = db_mod.types.DocumentArtifactTableReprocessResult{
        .scanned = 2,
        .reprocessed = 2,
        .limit = 2,
    };
    const updated = try store.recordPass(alloc, parsed_running.value, pass);
    defer alloc.free(updated);
    var parsed_update = try std.json.parseFromSlice(JobState, alloc, updated, .{ .ignore_unknown_fields = true });
    defer parsed_update.deinit();
    try std.testing.expectEqualStrings("succeeded", parsed_update.value.phase);
    try std.testing.expectEqual(@as(usize, 2), parsed_update.value.scanned);

    const stale_update = try store.recordPass(alloc, parsed_running.value, pass);
    defer alloc.free(stale_update);
    var parsed_stale = try std.json.parseFromSlice(JobState, alloc, stale_update, .{ .ignore_unknown_fields = true });
    defer parsed_stale.deinit();
    try std.testing.expectEqualStrings("succeeded", parsed_stale.value.phase);
    try std.testing.expectEqual(@as(usize, 2), parsed_stale.value.scanned);
}

test "artifact reprocess job store recovers durable jobs and reseeds ids" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifact-reprocess-jobs", .{tmp.sub_path});
    defer alloc.free(path);

    {
        const opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        var store = Store.init(alloc, .{});
        defer store.deinit();
        try store.attachOpenedStore(opened);

        const started = try store.startJob(alloc, "docs", "document_units_v1", .{ .limit = 1 });
        defer alloc.free(started);
        var parsed = try std.json.parseFromSlice(JobState, alloc, started, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        try std.testing.expectEqual(@as(u64, 1), parsed.value.job_id);
    }

    {
        const opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        var store = Store.init(alloc, .{});
        defer store.deinit();
        try store.attachOpenedStore(opened);

        const second = try store.startJob(alloc, "docs", "document_units_v1", .{ .limit = 1 });
        defer alloc.free(second);
        var parsed_second = try std.json.parseFromSlice(JobState, alloc, second, .{ .ignore_unknown_fields = true });
        defer parsed_second.deinit();
        try std.testing.expectEqual(@as(u64, 2), parsed_second.value.job_id);

        const first = (try store.loadJobAlloc(alloc, 1)) orelse return error.TestUnexpectedResult;
        defer alloc.free(first);
        var parsed_first = try std.json.parseFromSlice(JobState, alloc, first, .{ .ignore_unknown_fields = true });
        defer parsed_first.deinit();
        try std.testing.expectEqual(@as(u64, 1), parsed_first.value.job_id);
        try std.testing.expectEqualStrings("queued", parsed_first.value.phase);
    }
}

test "artifact reprocess job store persists monotonic next id across stale durable writes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifact-reprocess-jobs-monotonic-next-id", .{tmp.sub_path});
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
            .artifact_name = "document_units_v1",
            .phase = phaseString(.queued),
            .reprocess_status = "in_progress",
            .from_key = "",
            .to_key = "",
            .limit = 1,
            .created_at_millis = now_ms,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + store.retentionMillis(),
        });
        defer alloc.free(encoded_second);
        try store.storeEncoded(second.job_id, encoded_second, second.next_job_id);

        const encoded_first = try encodeState(alloc, .{
            .job_id = first.job_id,
            .table_name = "docs",
            .artifact_name = "document_units_v1",
            .phase = phaseString(.queued),
            .reprocess_status = "in_progress",
            .from_key = "",
            .to_key = "",
            .limit = 1,
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

        const third = try store.startJob(alloc, "docs", "document_units_v1", .{ .limit = 1 });
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

test "artifact reprocess job cleanup removes recovered durable expired jobs" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/artifact-reprocess-job-cleanup", .{tmp.sub_path});
    defer alloc.free(path);

    {
        var opened = try OpenedStore.open(alloc, path);
        defer opened.deinit();
        const key = try jobKey(alloc, 42);
        defer alloc.free(key);
        const now_ms = nowMillis();
        const expired = try encodeState(alloc, .{
            .job_id = 42,
            .table_name = "docs",
            .artifact_name = "document_units_v1",
            .phase = phaseString(.succeeded),
            .reprocess_status = "complete",
            .from_key = "",
            .to_key = "",
            .limit = 1,
            .created_at_millis = now_ms,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms,
        });
        defer alloc.free(expired);
        try opened.docstore.put(key, expired);
    }

    {
        const opened = try alloc.create(OpenedStore);
        errdefer alloc.destroy(opened);
        opened.* = try OpenedStore.open(alloc, path);
        var store = Store.init(alloc, .{});
        defer store.deinit();
        try store.attachOpenedStore(opened);

        store.cleanupExpiredJobs();
        try std.testing.expect((try store.loadJobAlloc(alloc, 42)) == null);
    }
}
