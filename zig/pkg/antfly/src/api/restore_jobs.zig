// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const docstore_mod = @import("../storage/docstore.zig");
const backend_erased = @import("../storage/backend_erased.zig");
const mem_backend = @import("../storage/mem_backend.zig");
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("../platform/time.zig");

const key_prefix = "\x00\x00__api_restore_jobs__:";
const restore_job_retention_ms: u64 = 7 * 24 * 60 * 60 * 1000;
const restore_job_scan_page_size: usize = 512;
const max_retained_restore_jobs: usize = 10_000;
const max_restore_job_record_bytes: usize = 64 * 1024;
const max_retained_restore_job_bytes: usize = 64 * 1024 * 1024;
const restore_job_prune_interval_ms: u64 = 60 * 1000;
const restore_job_prune_batch_size: usize = 1024;
pub const max_tables_per_job: usize = 256;
const max_restore_string_bytes: usize = 4096;
const max_initial_restore_job_bytes: usize = 56 * 1024;
const max_cluster_result_bytes: usize = 4 * 1024;
const max_cluster_failure_details: usize = 8;
const max_cluster_failure_table_name_bytes: usize = 256;
const max_cluster_failure_error_bytes: usize = 256;
const restore_job_format_version: u32 = 1;

pub const Scope = enum { table, cluster };
pub const Phase = enum { queued, running, succeeded, failed, cancelled };
pub const AttemptState = enum { active, cancelled, fenced };

pub const ClusterResultSummary = struct {
    encoded: []u8,
    succeeded: bool,

    pub fn deinit(self: *ClusterResultSummary, alloc: std.mem.Allocator) void {
        alloc.free(self.encoded);
        self.* = undefined;
    }
};

pub const JobState = struct {
    format_version: u32,
    job_id: u64,
    enqueue_sequence: u64,
    attempt_id: u64 = 0,
    scope: Scope,
    table_name: ?[]const u8 = null,
    backup_id: []const u8,
    location: []const u8,
    connection: []const u8,
    restore_mode: []const u8 = "fail_if_exists",
    table_names: ?[]const []const u8 = null,
    published_table_indexes: ?[]const u16 = null,
    completed_table_indexes: ?[]const u16 = null,
    phase: Phase = .queued,
    cancel_requested: bool = false,
    idempotency_key: []const u8,
    idempotency_explicit: bool = false,
    request_fingerprint: []const u8,
    result_json: ?[]const u8 = null,
    last_error: ?[]const u8 = null,
    created_at_ms: u64,
    updated_at_ms: u64,
    expires_at_ms: u64,
};

pub const StartRequest = struct {
    scope: Scope,
    table_name: ?[]const u8 = null,
    backup_id: []const u8,
    location: []const u8,
    connection: []const u8,
    restore_mode: []const u8 = "fail_if_exists",
    table_names: ?[]const []const u8 = null,
    idempotency_key: ?[]const u8 = null,
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
        return .{ .alloc = alloc, .path_z = path_z, .docstore = docstore };
    }

    pub fn deinit(self: *OpenedStore) void {
        self.docstore.close();
        self.alloc.destroy(self.docstore);
        self.alloc.free(self.path_z);
        self.* = undefined;
    }
};

pub const ReplicatedPersistence = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const OwnedRow = struct { key: []u8, value: []u8 };
    pub const VTable = struct {
        load: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) anyerror![]OwnedRow,
        get: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, key: []const u8) anyerror!?[]u8,
        put: *const fn (ptr: *anyopaque, key: []const u8, value: []const u8) anyerror!void,
        delete: *const fn (ptr: *anyopaque, key: []const u8) anyerror!void,
        delete_many: *const fn (ptr: *anyopaque, keys: []const []const u8) anyerror!void,
    };

    pub fn freeRows(alloc: std.mem.Allocator, rows: []OwnedRow) void {
        for (rows) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(rows);
    }
};

pub const Store = struct {
    const PendingJob = struct {
        job_id: u64,
        enqueue_sequence: u64,
    };

    alloc: std.mem.Allocator,
    io: ?std.Io = null,
    mutex: std.atomic.Mutex = .unlocked,
    jobs: std.AutoHashMapUnmanaged(u64, []u8) = .empty,
    idempotency: std.StringHashMapUnmanaged(u64) = .empty,
    pending: std.ArrayListUnmanaged(PendingJob) = .empty,
    pending_head: usize = 0,
    next_enqueue_sequence: u64 = 1,
    opened: ?*OpenedStore = null,
    runtime: ?*backend_erased.Store = null,
    replicated: ?ReplicatedPersistence = null,
    retained_bytes: usize = 0,
    next_prune_at_ms: u64 = 0,

    pub fn init(alloc: std.mem.Allocator) Store {
        return .{ .alloc = alloc };
    }

    pub fn initWithIo(alloc: std.mem.Allocator, io: std.Io) Store {
        return .{ .alloc = alloc, .io = io };
    }

    pub fn deinit(self: *Store) void {
        var jobs = self.jobs.iterator();
        while (jobs.next()) |entry| self.alloc.free(entry.value_ptr.*);
        self.jobs.deinit(self.alloc);
        var keys = self.idempotency.iterator();
        while (keys.next()) |entry| self.alloc.free(entry.key_ptr.*);
        self.idempotency.deinit(self.alloc);
        self.pending.deinit(self.alloc);
        if (self.opened) |opened| {
            opened.deinit();
            self.alloc.destroy(opened);
        }
        self.* = undefined;
    }

    pub fn hasPersistence(self: *Store) bool {
        self.lock();
        defer self.mutex.unlock();
        return self.opened != null or self.runtime != null or self.replicated != null;
    }

    pub fn attach(self: *Store, opened: *OpenedStore) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.opened != null or self.runtime != null or self.replicated != null or self.jobs.count() != 0) return error.RestoreJobStoreAlreadyAttached;
        self.opened = opened;
        errdefer self.opened = null;
        errdefer self.clearInMemoryLocked();
        var after_key: ?[]u8 = null;
        defer if (after_key) |key| self.alloc.free(key);
        while (true) {
            const rows = try opened.docstore.scanPrefixPage(self.alloc, key_prefix, after_key, restore_job_scan_page_size);
            defer docstore_mod.DocStore.freeResults(self.alloc, rows);
            if (rows.len == 0) break;
            for (rows) |row| {
                if (row.value.len > max_restore_job_record_bytes) return error.RestoreJobRecordTooLarge;
                var parsed = std.json.parseFromSlice(JobState, self.alloc, row.value, .{ .ignore_unknown_fields = true }) catch return error.CorruptRestoreJobStore;
                defer parsed.deinit();
                try validateProgressState(parsed.value);
                try self.validateRowKeyLocked(row.key, parsed.value.job_id);
                if (isTerminal(parsed.value.phase) and parsed.value.expires_at_ms <= nowMillis()) {
                    try opened.docstore.delete(row.key);
                    continue;
                }
                if (self.jobs.count() >= max_retained_restore_jobs) return error.RestoreJobCapacityExceeded;
                const encoded = if (parsed.value.phase == .running) blk: {
                    const recovered = try encode(self.alloc, .{
                        .format_version = restore_job_format_version,
                        .job_id = parsed.value.job_id,
                        .enqueue_sequence = parsed.value.enqueue_sequence,
                        .attempt_id = parsed.value.attempt_id,
                        .scope = parsed.value.scope,
                        .table_name = parsed.value.table_name,
                        .backup_id = parsed.value.backup_id,
                        .location = parsed.value.location,
                        .connection = parsed.value.connection,
                        .restore_mode = parsed.value.restore_mode,
                        .table_names = parsed.value.table_names,
                        .published_table_indexes = parsed.value.published_table_indexes,
                        .completed_table_indexes = parsed.value.completed_table_indexes,
                        .phase = .queued,
                        .cancel_requested = parsed.value.cancel_requested,
                        .idempotency_key = parsed.value.idempotency_key,
                        .idempotency_explicit = parsed.value.idempotency_explicit,
                        .request_fingerprint = parsed.value.request_fingerprint,
                        .last_error = "resuming_after_restart",
                        .created_at_ms = parsed.value.created_at_ms,
                        .updated_at_ms = nowMillis(),
                        .expires_at_ms = parsed.value.expires_at_ms,
                    });
                    try opened.docstore.put(row.key, recovered);
                    break :blk recovered;
                } else try self.alloc.dupe(u8, row.value);
                errdefer self.alloc.free(encoded);
                const next_bytes = std.math.add(usize, self.retained_bytes, encoded.len) catch return error.RestoreJobCapacityExceeded;
                if (next_bytes > max_retained_restore_job_bytes) return error.RestoreJobCapacityExceeded;
                try self.jobs.put(self.alloc, parsed.value.job_id, encoded);
                self.retained_bytes = next_bytes;
                self.observeEnqueueSequenceLocked(parsed.value.enqueue_sequence);
                if (!isTerminal(parsed.value.phase)) try self.pending.append(self.alloc, .{
                    .job_id = parsed.value.job_id,
                    .enqueue_sequence = parsed.value.enqueue_sequence,
                });
                if (parsed.value.idempotency_explicit) {
                    if (self.idempotency.contains(parsed.value.idempotency_key)) return error.CorruptRestoreJobStore;
                    const key = try self.alloc.dupe(u8, parsed.value.idempotency_key);
                    errdefer self.alloc.free(key);
                    try self.idempotency.put(self.alloc, key, parsed.value.job_id);
                }
            }
            if (rows.len < restore_job_scan_page_size) break;
            const next_after = try self.alloc.dupe(u8, rows[rows.len - 1].key);
            if (after_key) |key| self.alloc.free(key);
            after_key = next_after;
        }
        self.sortPendingLocked();
    }

    /// Attaches durability to a storage-engine-owned namespace. The engine owns
    /// `runtime`, which must outlive this store. This is the canonical path for
    /// single-file Lite so restore state remains inside the `.aflite` artifact.
    pub fn attachRuntime(self: *Store, runtime: *backend_erased.Store) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.opened != null or self.runtime != null or self.replicated != null or self.jobs.count() != 0) return error.RestoreJobStoreAlreadyAttached;
        self.runtime = runtime;
        errdefer self.runtime = null;
        errdefer self.clearInMemoryLocked();

        var rows = std.ArrayListUnmanaged(struct { key: []u8, value: []u8 }).empty;
        defer {
            for (rows.items) |row| {
                self.alloc.free(row.key);
                self.alloc.free(row.value);
            }
            rows.deinit(self.alloc);
        }
        var retained_bytes: usize = 0;
        {
            var txn = try runtime.beginCurrentScan();
            defer txn.abort();
            var cursor = try txn.openCursor();
            defer cursor.close();
            var entry = try cursor.seekAtOrAfter(key_prefix);
            while (entry) |row| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, key_prefix)) break;
                if (row.value.len > max_restore_job_record_bytes) return error.RestoreJobRecordTooLarge;
                if (rows.items.len >= max_retained_restore_jobs) return error.RestoreJobCapacityExceeded;
                retained_bytes = std.math.add(usize, retained_bytes, row.value.len) catch return error.RestoreJobCapacityExceeded;
                if (retained_bytes > max_retained_restore_job_bytes) return error.RestoreJobCapacityExceeded;
                try rows.append(self.alloc, .{
                    .key = try self.alloc.dupe(u8, row.key),
                    .value = try self.alloc.dupe(u8, row.value),
                });
            }
        }

        for (rows.items) |row| try self.attachRowLocked(row.key, row.value);
    }

    pub fn attachReplicated(self: *Store, persistence: ReplicatedPersistence) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.opened != null or self.runtime != null or self.replicated != null or self.jobs.count() != 0) return error.RestoreJobStoreAlreadyAttached;
        self.replicated = persistence;
        errdefer self.replicated = null;
        errdefer self.clearInMemoryLocked();
        const rows = try persistence.vtable.load(persistence.ptr, self.alloc);
        defer ReplicatedPersistence.freeRows(self.alloc, rows);
        for (rows) |row| try self.attachReplicatedRowLocked(row.key, row.value);
        self.sortPendingLocked();
    }

    fn attachReplicatedRowLocked(self: *Store, key: []const u8, value: []const u8) !void {
        if (value.len > max_restore_job_record_bytes) return error.RestoreJobRecordTooLarge;
        var parsed = std.json.parseFromSlice(JobState, self.alloc, value, .{ .ignore_unknown_fields = true }) catch return error.CorruptRestoreJobStore;
        defer parsed.deinit();
        try validateProgressState(parsed.value);
        try self.validateRowKeyLocked(key, parsed.value.job_id);
        if (isTerminal(parsed.value.phase) and parsed.value.expires_at_ms <= nowMillis()) return;
        if (self.jobs.count() >= max_retained_restore_jobs) return error.RestoreJobCapacityExceeded;
        const encoded = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(encoded);
        const next_bytes = std.math.add(usize, self.retained_bytes, encoded.len) catch return error.RestoreJobCapacityExceeded;
        if (next_bytes > max_retained_restore_job_bytes) return error.RestoreJobCapacityExceeded;
        try self.jobs.put(self.alloc, parsed.value.job_id, encoded);
        self.retained_bytes = next_bytes;
        self.observeEnqueueSequenceLocked(parsed.value.enqueue_sequence);
        if (parsed.value.phase == .queued) try self.pending.append(self.alloc, .{
            .job_id = parsed.value.job_id,
            .enqueue_sequence = parsed.value.enqueue_sequence,
        });
        if (parsed.value.idempotency_explicit) {
            if (self.idempotency.contains(parsed.value.idempotency_key)) return error.CorruptRestoreJobStore;
            const owned_key = try self.alloc.dupe(u8, parsed.value.idempotency_key);
            errdefer self.alloc.free(owned_key);
            try self.idempotency.put(self.alloc, owned_key, parsed.value.job_id);
        }
    }

    /// Called once for each newly acquired metadata leadership term. Running
    /// attempts from the old leader are fenced by incrementing their attempt on
    /// the next begin and returned to the durable FIFO.
    pub fn prepareReplicatedLeadership(self: *Store, alloc: std.mem.Allocator) !void {
        self.lock();
        defer self.mutex.unlock();
        const persistence = self.replicated orelse return;
        self.clearInMemoryLocked();
        const rows = try persistence.vtable.load(persistence.ptr, self.alloc);
        defer ReplicatedPersistence.freeRows(self.alloc, rows);
        var expired_keys = std.ArrayListUnmanaged([]const u8).empty;
        defer expired_keys.deinit(self.alloc);
        for (rows) |row| {
            if (try replicatedRowExpired(self.alloc, row.value)) {
                try expired_keys.append(self.alloc, row.key);
                continue;
            }
            try self.attachReplicatedRowLocked(row.key, row.value);
        }
        // Expiry cleanup is a bounded number of Raft entries rather than one
        // consensus round per retained job. This keeps leadership acquisition
        // responsive after a large cohort reaches its retention boundary.
        var expired_offset: usize = 0;
        while (expired_offset < expired_keys.items.len) {
            const end = @min(expired_offset + restore_job_prune_batch_size, expired_keys.items.len);
            try persistence.vtable.delete_many(persistence.ptr, expired_keys.items[expired_offset..end]);
            expired_offset = end;
        }
        self.sortPendingLocked();

        var running = std.ArrayListUnmanaged(u64).empty;
        defer running.deinit(alloc);
        var it = self.jobs.iterator();
        while (it.next()) |entry| {
            var parsed = try std.json.parseFromSlice(JobState, alloc, entry.value_ptr.*, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            if (parsed.value.phase == .running) try running.append(alloc, parsed.value.job_id);
        }
        for (running.items) |job_id| {
            const current = self.jobs.get(job_id) orelse continue;
            var parsed = try std.json.parseFromSlice(JobState, alloc, current, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const encoded = try self.updateLocked(alloc, parsed.value, .{ .phase = .queued, .last_error = "resuming_after_leader_change" });
            alloc.free(encoded);
            try self.pending.append(self.alloc, .{ .job_id = job_id, .enqueue_sequence = parsed.value.enqueue_sequence });
        }
        // Recovered in-flight attempts retain their original FIFO position;
        // appending them after the initial queued rebuild would otherwise let
        // every newer queued job jump ahead after a leader change.
        self.sortPendingLocked();
    }

    fn clearInMemoryLocked(self: *Store) void {
        var jobs = self.jobs.iterator();
        while (jobs.next()) |entry| self.alloc.free(entry.value_ptr.*);
        self.jobs.clearRetainingCapacity();
        var keys = self.idempotency.iterator();
        while (keys.next()) |entry| self.alloc.free(entry.key_ptr.*);
        self.idempotency.clearRetainingCapacity();
        self.pending.clearRetainingCapacity();
        self.pending_head = 0;
        self.retained_bytes = 0;
        self.next_enqueue_sequence = 1;
    }

    fn replicatedRowExpired(alloc: std.mem.Allocator, value: []const u8) !bool {
        if (value.len > max_restore_job_record_bytes) return error.RestoreJobRecordTooLarge;
        var parsed = std.json.parseFromSlice(JobState, alloc, value, .{ .ignore_unknown_fields = true }) catch
            return error.CorruptRestoreJobStore;
        defer parsed.deinit();
        try validateProgressState(parsed.value);
        return isTerminal(parsed.value.phase) and parsed.value.expires_at_ms <= nowMillis();
    }

    fn attachRowLocked(self: *Store, key: []const u8, value: []const u8) !void {
        if (value.len > max_restore_job_record_bytes) return error.RestoreJobRecordTooLarge;
        var parsed = std.json.parseFromSlice(JobState, self.alloc, value, .{ .ignore_unknown_fields = true }) catch return error.CorruptRestoreJobStore;
        defer parsed.deinit();
        try validateProgressState(parsed.value);
        try self.validateRowKeyLocked(key, parsed.value.job_id);
        if (isTerminal(parsed.value.phase) and parsed.value.expires_at_ms <= nowMillis()) {
            try self.persistDeleteLocked(key);
            return;
        }
        if (self.jobs.count() >= max_retained_restore_jobs) return error.RestoreJobCapacityExceeded;
        const encoded = if (parsed.value.phase == .running) blk: {
            const recovered = try encode(self.alloc, .{
                .format_version = restore_job_format_version,
                .job_id = parsed.value.job_id,
                .enqueue_sequence = parsed.value.enqueue_sequence,
                .attempt_id = parsed.value.attempt_id,
                .scope = parsed.value.scope,
                .table_name = parsed.value.table_name,
                .backup_id = parsed.value.backup_id,
                .location = parsed.value.location,
                .connection = parsed.value.connection,
                .restore_mode = parsed.value.restore_mode,
                .table_names = parsed.value.table_names,
                .published_table_indexes = parsed.value.published_table_indexes,
                .completed_table_indexes = parsed.value.completed_table_indexes,
                .phase = .queued,
                .cancel_requested = parsed.value.cancel_requested,
                .idempotency_key = parsed.value.idempotency_key,
                .idempotency_explicit = parsed.value.idempotency_explicit,
                .request_fingerprint = parsed.value.request_fingerprint,
                .last_error = "resuming_after_restart",
                .created_at_ms = parsed.value.created_at_ms,
                .updated_at_ms = nowMillis(),
                .expires_at_ms = parsed.value.expires_at_ms,
            });
            try self.persistPutLocked(key, recovered);
            break :blk recovered;
        } else try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(encoded);
        const next_bytes = std.math.add(usize, self.retained_bytes, encoded.len) catch return error.RestoreJobCapacityExceeded;
        if (next_bytes > max_retained_restore_job_bytes) return error.RestoreJobCapacityExceeded;
        try self.jobs.put(self.alloc, parsed.value.job_id, encoded);
        self.retained_bytes = next_bytes;
        self.observeEnqueueSequenceLocked(parsed.value.enqueue_sequence);
        if (!isTerminal(parsed.value.phase)) try self.pending.append(self.alloc, .{
            .job_id = parsed.value.job_id,
            .enqueue_sequence = parsed.value.enqueue_sequence,
        });
        if (parsed.value.idempotency_explicit) {
            if (self.idempotency.contains(parsed.value.idempotency_key)) return error.CorruptRestoreJobStore;
            const owned_key = try self.alloc.dupe(u8, parsed.value.idempotency_key);
            errdefer self.alloc.free(owned_key);
            try self.idempotency.put(self.alloc, owned_key, parsed.value.job_id);
        }
    }

    pub fn start(self: *Store, alloc: std.mem.Allocator, req: StartRequest) ![]u8 {
        try validateStartRequest(req);
        const io = self.io orelse return error.AsyncRestoreUnavailable;
        const fingerprint = try requestFingerprintAlloc(alloc, req);
        defer alloc.free(fingerprint);
        const explicit_idempotency_key = if (req.idempotency_key) |provided| blk: {
            if (provided.len == 0 or provided.len > 256) return error.InvalidIdempotencyKey;
            break :blk provided;
        } else null;
        var entropy: [16]u8 = undefined;
        try io.randomSecure(&entropy);

        self.lock();
        defer self.mutex.unlock();
        const now_for_prune = nowMillis();
        if (now_for_prune >= self.next_prune_at_ms) {
            const more_expired = try self.pruneExpiredLocked(now_for_prune, restore_job_prune_batch_size);
            self.next_prune_at_ms = if (more_expired) now_for_prune else now_for_prune +| restore_job_prune_interval_ms;
        }
        if (explicit_idempotency_key) |key| {
            if (self.idempotency.get(key)) |job_id| {
                const encoded = self.jobs.get(job_id) orelse return error.CorruptRestoreJobStore;
                var parsed = try std.json.parseFromSlice(JobState, alloc, encoded, .{ .ignore_unknown_fields = true });
                defer parsed.deinit();
                if (!std.mem.eql(u8, parsed.value.request_fingerprint, fingerprint)) return error.IdempotencyConflict;
                return try alloc.dupe(u8, encoded);
            }
        }

        if (self.jobs.count() >= max_retained_restore_jobs) return error.RestoreJobCapacityExceeded;
        const now = nowMillis();
        var job_id = std.mem.readInt(u64, entropy[0..8], .little) & std.math.maxInt(i64);
        while (job_id == 0 or self.jobs.contains(job_id)) {
            try io.randomSecure(entropy[0..8]);
            job_id = std.mem.readInt(u64, entropy[0..8], .little) & std.math.maxInt(i64);
        }
        const auto_nonce = std.mem.readInt(u64, entropy[8..16], .little);
        const generated_key = if (explicit_idempotency_key == null)
            try std.fmt.allocPrint(alloc, "auto:{x:0>16}", .{auto_nonce})
        else
            null;
        defer if (generated_key) |key| alloc.free(key);
        const idempotency_key = explicit_idempotency_key orelse generated_key.?;
        const enqueue_sequence = self.next_enqueue_sequence;
        if (enqueue_sequence == 0 or enqueue_sequence == std.math.maxInt(u64)) return error.RestoreJobCapacityExceeded;
        self.next_enqueue_sequence = enqueue_sequence + 1;
        const encoded = try encode(alloc, .{
            .format_version = restore_job_format_version,
            .job_id = job_id,
            .enqueue_sequence = enqueue_sequence,
            .scope = req.scope,
            .table_name = req.table_name,
            .backup_id = req.backup_id,
            .location = req.location,
            .connection = req.connection,
            .restore_mode = req.restore_mode,
            .table_names = req.table_names,
            .idempotency_key = idempotency_key,
            .idempotency_explicit = explicit_idempotency_key != null,
            .request_fingerprint = fingerprint,
            .created_at_ms = now,
            .updated_at_ms = now,
            .expires_at_ms = std.math.maxInt(i64),
        });
        errdefer alloc.free(encoded);
        // Leave room for bounded progress checkpoints and the compact terminal
        // result. Admission must fail before work starts rather than discover
        // after an irreversible restore that the terminal record cannot fit.
        if (encoded.len > max_initial_restore_job_bytes) return error.RestoreJobRecordTooLarge;
        try self.jobs.ensureUnusedCapacity(self.alloc, 1);
        try self.pending.ensureUnusedCapacity(self.alloc, 1);
        if (explicit_idempotency_key != null) try self.idempotency.ensureUnusedCapacity(self.alloc, 1);
        const owned_key = if (explicit_idempotency_key != null) try self.alloc.dupe(u8, idempotency_key) else null;
        errdefer if (owned_key) |key| self.alloc.free(key);
        try self.storeLocked(job_id, encoded);
        self.pending.appendAssumeCapacity(.{ .job_id = job_id, .enqueue_sequence = enqueue_sequence });
        if (owned_key) |key| self.idempotency.putAssumeCapacity(key, job_id);
        return encoded;
    }

    pub fn recordTablePublished(self: *Store, alloc: std.mem.Allocator, job_id: u64, attempt_id: u64, table_index: u16) ![]u8 {
        self.lock();
        defer self.mutex.unlock();
        const current = self.jobs.get(job_id) orelse return error.NotFound;
        var parsed = try std.json.parseFromSlice(JobState, alloc, current, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.phase != .running or parsed.value.attempt_id != attempt_id) return error.RestoreJobFenced;
        if (containsTableIndex(parsed.value.published_table_indexes orelse &.{}, table_index)) return try alloc.dupe(u8, current);
        var published = std.ArrayListUnmanaged(u16).empty;
        defer published.deinit(alloc);
        try published.appendSlice(alloc, parsed.value.published_table_indexes orelse &.{});
        try published.append(alloc, table_index);
        return try self.updateLocked(alloc, parsed.value, .{ .phase = .running, .published_table_indexes = published.items });
    }

    pub fn recordTableCompleted(self: *Store, alloc: std.mem.Allocator, job_id: u64, attempt_id: u64, table_index: u16) ![]u8 {
        self.lock();
        defer self.mutex.unlock();
        const current = self.jobs.get(job_id) orelse return error.NotFound;
        var parsed = try std.json.parseFromSlice(JobState, alloc, current, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.phase != .running or parsed.value.attempt_id != attempt_id) return error.RestoreJobFenced;
        if (!containsTableIndex(parsed.value.published_table_indexes orelse &.{}, table_index)) return error.RestoreJobCheckpointOrder;
        if (containsTableIndex(parsed.value.completed_table_indexes orelse &.{}, table_index)) return try alloc.dupe(u8, current);
        var completed = std.ArrayListUnmanaged(u16).empty;
        defer completed.deinit(alloc);
        try completed.appendSlice(alloc, parsed.value.completed_table_indexes orelse &.{});
        try completed.append(alloc, table_index);
        return try self.updateLocked(alloc, parsed.value, .{ .phase = .running, .completed_table_indexes = completed.items });
    }

    pub fn load(self: *Store, alloc: std.mem.Allocator, job_id: u64) !?[]u8 {
        try self.refreshReplicatedJob(alloc, job_id);
        return try self.loadCached(alloc, job_id);
    }

    pub fn loadCached(self: *Store, alloc: std.mem.Allocator, job_id: u64) !?[]u8 {
        self.lock();
        defer self.mutex.unlock();
        return if (self.jobs.get(job_id)) |encoded| try alloc.dupe(u8, encoded) else null;
    }

    fn refreshReplicatedJob(self: *Store, alloc: std.mem.Allocator, job_id: u64) !void {
        const replicated = self.replicated orelse return;
        const key = try jobKey(alloc, job_id);
        defer alloc.free(key);
        const fresh = try replicated.vtable.get(replicated.ptr, alloc, key);
        defer if (fresh) |value| alloc.free(value);
        self.lock();
        defer self.mutex.unlock();
        if (fresh) |value| {
            var parsed = std.json.parseFromSlice(JobState, alloc, value, .{ .ignore_unknown_fields = true }) catch return error.CorruptRestoreJobStore;
            defer parsed.deinit();
            if (parsed.value.job_id != job_id) return error.CorruptRestoreJobStore;
            try validateProgressState(parsed.value);
            if (isTerminal(parsed.value.phase) and parsed.value.expires_at_ms <= nowMillis()) {
                if (self.jobs.fetchRemove(job_id)) |removed| {
                    self.retained_bytes -= removed.value.len;
                    self.alloc.free(removed.value);
                }
                if (parsed.value.idempotency_explicit) {
                    if (self.idempotency.fetchRemove(parsed.value.idempotency_key)) |removed| self.alloc.free(removed.key);
                }
                return;
            }
            const owned = try self.alloc.dupe(u8, value);
            errdefer self.alloc.free(owned);
            const previous_len = if (self.jobs.get(job_id)) |previous| previous.len else 0;
            const next_bytes = std.math.add(usize, self.retained_bytes - previous_len, owned.len) catch return error.RestoreJobCapacityExceeded;
            if (next_bytes > max_retained_restore_job_bytes) return error.RestoreJobCapacityExceeded;
            if (try self.jobs.fetchPut(self.alloc, job_id, owned)) |previous| self.alloc.free(previous.value);
            self.retained_bytes = next_bytes;
        } else if (self.jobs.fetchRemove(job_id)) |removed| {
            self.retained_bytes -= removed.value.len;
            self.alloc.free(removed.value);
        }
    }

    /// Removes up to `limit` runnable IDs from the FIFO queue. Job records stay
    /// durable until their terminal retention expires; only the small runnable
    /// index is consumed here, avoiding a full JSON scan after every job.
    pub fn takePendingIds(self: *Store, alloc: std.mem.Allocator, limit: usize) ![]u64 {
        self.lock();
        defer self.mutex.unlock();
        var ids = std.ArrayListUnmanaged(u64).empty;
        errdefer ids.deinit(alloc);
        try ids.ensureTotalCapacity(alloc, @min(limit, self.pending.items.len - self.pending_head));
        while (ids.items.len < limit and self.pending_head < self.pending.items.len) {
            const pending = self.pending.items[self.pending_head];
            const encoded = self.jobs.get(pending.job_id) orelse {
                self.pending_head += 1;
                continue;
            };
            var parsed = std.json.parseFromSlice(JobState, alloc, encoded, .{ .ignore_unknown_fields = true }) catch return error.CorruptRestoreJobStore;
            defer parsed.deinit();
            self.pending_head += 1;
            if (parsed.value.phase == .queued) ids.appendAssumeCapacity(pending.job_id);
        }
        self.compactPendingLocked();
        return try ids.toOwnedSlice(alloc);
    }

    pub fn requeuePending(self: *Store, job_id: u64) !void {
        self.lock();
        defer self.mutex.unlock();
        const encoded = self.jobs.get(job_id) orelse return;
        var parsed = try std.json.parseFromSlice(JobState, self.alloc, encoded, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.phase == .queued) try self.pending.append(self.alloc, .{
            .job_id = job_id,
            .enqueue_sequence = parsed.value.enqueue_sequence,
        });
    }

    pub fn begin(self: *Store, alloc: std.mem.Allocator, job_id: u64) !?[]u8 {
        self.lock();
        defer self.mutex.unlock();
        const current = self.jobs.get(job_id) orelse return null;
        var parsed = try std.json.parseFromSlice(JobState, alloc, current, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (isTerminal(parsed.value.phase)) return try alloc.dupe(u8, current);
        if (parsed.value.phase == .running) return null;
        const next_phase: Phase = if (parsed.value.cancel_requested) .cancelled else .running;
        return try self.updateLocked(alloc, parsed.value, .{
            .phase = next_phase,
            .attempt_id = parsed.value.attempt_id +| 1,
            .last_error = if (next_phase == .cancelled) "cancel_requested" else null,
        });
    }

    pub fn finish(self: *Store, alloc: std.mem.Allocator, expected: JobState, result_json: []const u8) ![]u8 {
        return try self.finishAs(alloc, expected, .succeeded, result_json, null);
    }

    pub fn fail(self: *Store, alloc: std.mem.Allocator, expected: JobState, err_name: []const u8) ![]u8 {
        return try self.finishAs(alloc, expected, .failed, null, err_name);
    }

    pub fn failWithResult(self: *Store, alloc: std.mem.Allocator, expected: JobState, result_json: []const u8, err_name: []const u8) ![]u8 {
        return try self.finishAs(alloc, expected, .failed, result_json, err_name);
    }

    pub fn failRunningById(self: *Store, alloc: std.mem.Allocator, job_id: u64, err_name: []const u8) !void {
        self.lock();
        defer self.mutex.unlock();
        const current = self.jobs.get(job_id) orelse return;
        var parsed = try std.json.parseFromSlice(JobState, alloc, current, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.phase != .running) return;
        const encoded = try self.updateLocked(alloc, parsed.value, .{ .phase = .failed, .last_error = err_name });
        alloc.free(encoded);
    }

    fn finishAs(self: *Store, alloc: std.mem.Allocator, expected: JobState, phase: Phase, result_json: ?[]const u8, last_error: ?[]const u8) ![]u8 {
        self.lock();
        defer self.mutex.unlock();
        const current = self.jobs.get(expected.job_id) orelse return error.NotFound;
        var parsed = try std.json.parseFromSlice(JobState, alloc, current, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.attempt_id != expected.attempt_id or parsed.value.phase != .running) return try alloc.dupe(u8, current);
        // Cancellation is cooperative and may race the final irreversible
        // restore boundary. Once the worker reports successful completion, the
        // restored data is authoritative: retain cancel_requested for audit,
        // but never claim that successfully published data was cancelled.
        const cancellation_wins = parsed.value.cancel_requested and phase != .succeeded;
        return try self.updateLocked(alloc, parsed.value, .{
            .phase = if (cancellation_wins) .cancelled else phase,
            .result_json = result_json,
            .last_error = if (cancellation_wins) "cancel_requested" else last_error,
        });
    }

    pub fn cancel(self: *Store, alloc: std.mem.Allocator, job_id: u64) !?[]u8 {
        self.lock();
        defer self.mutex.unlock();
        const current = self.jobs.get(job_id) orelse return null;
        var parsed = try std.json.parseFromSlice(JobState, alloc, current, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (isTerminal(parsed.value.phase)) return try alloc.dupe(u8, current);
        return try self.updateLocked(alloc, parsed.value, .{
            .phase = if (parsed.value.phase == .running) .running else .cancelled,
            .cancel_requested = true,
            .last_error = "cancel_requested",
        });
    }

    pub fn attemptState(self: *Store, alloc: std.mem.Allocator, job_id: u64, attempt_id: u64) !AttemptState {
        self.lock();
        defer self.mutex.unlock();
        const current = self.jobs.get(job_id) orelse return .fenced;
        var parsed = try std.json.parseFromSlice(JobState, alloc, current, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.attempt_id != attempt_id or parsed.value.phase != .running) return .fenced;
        return if (parsed.value.cancel_requested) .cancelled else .active;
    }

    const Update = struct {
        phase: Phase,
        attempt_id: ?u64 = null,
        cancel_requested: ?bool = null,
        result_json: ?[]const u8 = null,
        last_error: ?[]const u8 = null,
        published_table_indexes: ?[]const u16 = null,
        completed_table_indexes: ?[]const u16 = null,
    };

    fn updateLocked(self: *Store, alloc: std.mem.Allocator, current: JobState, update: Update) ![]u8 {
        const next: JobState = .{
            .format_version = restore_job_format_version,
            .job_id = current.job_id,
            .enqueue_sequence = current.enqueue_sequence,
            .attempt_id = update.attempt_id orelse current.attempt_id,
            .scope = current.scope,
            .table_name = current.table_name,
            .backup_id = current.backup_id,
            .location = current.location,
            .connection = current.connection,
            .restore_mode = current.restore_mode,
            .table_names = current.table_names,
            .published_table_indexes = update.published_table_indexes orelse current.published_table_indexes,
            .completed_table_indexes = update.completed_table_indexes orelse current.completed_table_indexes,
            .phase = update.phase,
            .cancel_requested = update.cancel_requested orelse current.cancel_requested,
            .idempotency_key = current.idempotency_key,
            .idempotency_explicit = current.idempotency_explicit,
            .request_fingerprint = current.request_fingerprint,
            .result_json = update.result_json orelse current.result_json,
            .last_error = update.last_error,
            .created_at_ms = current.created_at_ms,
            .updated_at_ms = nowMillis(),
            .expires_at_ms = if (isTerminal(update.phase) and !isTerminal(current.phase))
                nowMillis() +| restore_job_retention_ms
            else
                current.expires_at_ms,
        };
        try validateProgressState(next);
        const encoded = try encode(alloc, next);
        errdefer alloc.free(encoded);
        if (encoded.len > max_restore_job_record_bytes) return error.RestoreJobRecordTooLarge;
        try self.storeLocked(current.job_id, encoded);
        return encoded;
    }

    fn storeLocked(self: *Store, job_id: u64, encoded: []const u8) !void {
        const previous_len = if (self.jobs.get(job_id)) |previous| previous.len else 0;
        const next_bytes = std.math.add(usize, self.retained_bytes - previous_len, encoded.len) catch return error.RestoreJobCapacityExceeded;
        if (next_bytes > max_retained_restore_job_bytes) return error.RestoreJobCapacityExceeded;
        const owned = try self.alloc.dupe(u8, encoded);
        errdefer self.alloc.free(owned);
        const key = try jobKey(self.alloc, job_id);
        defer self.alloc.free(key);
        try self.persistPutLocked(key, encoded);
        if (try self.jobs.fetchPut(self.alloc, job_id, owned)) |previous| self.alloc.free(previous.value);
        self.retained_bytes = next_bytes;
    }

    fn pruneExpiredLocked(self: *Store, now_ms: u64, limit: usize) !bool {
        var expired = std.ArrayListUnmanaged(u64).empty;
        defer expired.deinit(self.alloc);
        var more_expired = false;
        var it = self.jobs.iterator();
        while (it.next()) |entry| {
            var parsed = std.json.parseFromSlice(JobState, self.alloc, entry.value_ptr.*, .{ .ignore_unknown_fields = true }) catch return error.CorruptRestoreJobStore;
            defer parsed.deinit();
            if (isTerminal(parsed.value.phase) and parsed.value.expires_at_ms <= now_ms) {
                if (expired.items.len < limit) {
                    try expired.append(self.alloc, entry.key_ptr.*);
                } else {
                    more_expired = true;
                }
            }
        }
        const keys = try self.alloc.alloc([]u8, expired.items.len);
        var keys_initialized: usize = 0;
        defer {
            for (keys[0..keys_initialized]) |key| self.alloc.free(key);
            self.alloc.free(keys);
        }
        for (expired.items, 0..) |job_id, i| {
            keys[i] = try jobKey(self.alloc, job_id);
            keys_initialized += 1;
        }
        if (keys.len > 0) try self.persistDeleteManyLocked(keys);
        for (expired.items) |job_id| {
            const current = self.jobs.get(job_id) orelse continue;
            var parsed = std.json.parseFromSlice(JobState, self.alloc, current, .{ .ignore_unknown_fields = true }) catch return error.CorruptRestoreJobStore;
            defer parsed.deinit();
            const removed = self.jobs.fetchRemove(job_id) orelse return error.CorruptRestoreJobStore;
            if (self.idempotency.fetchRemove(parsed.value.idempotency_key)) |key_entry| self.alloc.free(key_entry.key);
            self.retained_bytes -= removed.value.len;
            self.alloc.free(removed.value);
        }
        return more_expired;
    }

    fn validateRowKeyLocked(self: *Store, key: []const u8, job_id: u64) !void {
        if (job_id == 0 or self.jobs.contains(job_id)) return error.CorruptRestoreJobStore;
        const expected = try jobKey(self.alloc, job_id);
        defer self.alloc.free(expected);
        if (!std.mem.eql(u8, key, expected)) return error.CorruptRestoreJobStore;
    }

    fn sortPendingLocked(self: *Store) void {
        std.mem.sort(PendingJob, self.pending.items, {}, struct {
            fn lessThan(_: void, a: PendingJob, b: PendingJob) bool {
                if (a.enqueue_sequence != b.enqueue_sequence) return a.enqueue_sequence < b.enqueue_sequence;
                return a.job_id < b.job_id;
            }
        }.lessThan);
    }

    fn observeEnqueueSequenceLocked(self: *Store, sequence: u64) void {
        if (sequence >= self.next_enqueue_sequence) self.next_enqueue_sequence = sequence +| 1;
    }

    fn compactPendingLocked(self: *Store) void {
        if (self.pending_head == 0) return;
        if (self.pending_head < 1024 and self.pending_head * 2 < self.pending.items.len) return;
        const remaining = self.pending.items.len - self.pending_head;
        std.mem.copyForwards(PendingJob, self.pending.items[0..remaining], self.pending.items[self.pending_head..]);
        self.pending.items.len = remaining;
        self.pending_head = 0;
    }

    fn persistPutLocked(self: *Store, key: []const u8, value: []const u8) !void {
        if (self.opened) |opened| return opened.docstore.put(key, value);
        if (self.runtime) |runtime| {
            var txn = try runtime.beginWrite();
            errdefer txn.abort();
            try txn.put(key, value);
            return txn.commit();
        }
        if (self.replicated) |replicated| return replicated.vtable.put(replicated.ptr, key, value);
        return error.RestoreJobPersistenceUnavailable;
    }

    fn persistDeleteLocked(self: *Store, key: []const u8) !void {
        if (self.opened) |opened| return opened.docstore.delete(key);
        if (self.runtime) |runtime| {
            var txn = try runtime.beginWrite();
            errdefer txn.abort();
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            return txn.commit();
        }
        if (self.replicated) |replicated| return replicated.vtable.delete(replicated.ptr, key);
        return error.RestoreJobPersistenceUnavailable;
    }

    fn persistDeleteManyLocked(self: *Store, keys: []const []const u8) !void {
        if (keys.len == 0) return;
        if (self.opened) |opened| {
            var batch = try opened.docstore.beginWriteBatch();
            errdefer batch.abort();
            for (keys) |key| batch.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            return batch.commit();
        }
        if (self.runtime) |runtime| {
            var txn = try runtime.beginWrite();
            errdefer txn.abort();
            for (keys) |key| txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            return txn.commit();
        }
        if (self.replicated) |replicated| return replicated.vtable.delete_many(replicated.ptr, keys);
        return error.RestoreJobPersistenceUnavailable;
    }

    fn lock(self: *Store) void {
        platform_sync.lockYielding(&self.mutex);
    }
};

fn validateStartRequest(req: StartRequest) !void {
    if (req.backup_id.len == 0 or req.backup_id.len > max_restore_string_bytes or
        req.location.len == 0 or req.location.len > max_restore_string_bytes or
        req.connection.len == 0 or req.connection.len > max_restore_string_bytes or
        (req.table_name != null and req.table_name.?.len > max_restore_string_bytes))
        return error.RestoreJobRecordTooLarge;
    if (req.table_names) |names| {
        if (names.len > max_tables_per_job) return error.TooManyRestoreTables;
        for (names, 0..) |name, i| {
            if (name.len == 0 or name.len > max_restore_string_bytes) return error.RestoreJobRecordTooLarge;
            for (names[0..i]) |previous| {
                if (std.mem.eql(u8, previous, name)) return error.DuplicateRestoreTableName;
            }
        }
    }
}

pub fn containsTableIndex(values: []const u16, needle: u16) bool {
    for (values) |value| if (value == needle) return true;
    return false;
}

fn validateProgressState(state: JobState) !void {
    if (state.format_version != restore_job_format_version) return error.UnsupportedRestoreJobFormat;
    const published = state.published_table_indexes orelse &.{};
    const completed = state.completed_table_indexes orelse &.{};
    if (published.len > max_tables_per_job or completed.len > published.len) return error.CorruptRestoreJobStore;
    const table_count: usize = switch (state.scope) {
        .table => 1,
        .cluster => if (state.table_names) |names| names.len else max_tables_per_job,
    };
    for (published, 0..) |table_index, i| {
        if (@as(usize, table_index) >= table_count) return error.CorruptRestoreJobStore;
        if (containsTableIndex(published[0..i], table_index)) return error.CorruptRestoreJobStore;
    }
    for (completed, 0..) |table_index, i| {
        if (!containsTableIndex(published, table_index) or containsTableIndex(completed[0..i], table_index))
            return error.CorruptRestoreJobStore;
    }
}

const ClusterRestoreTableResult = struct {
    name: []const u8,
    status: []const u8,
    @"error": ?[]const u8 = null,
};

const ClusterRestoreResult = struct {
    tables: []const ClusterRestoreTableResult,
};

const ClusterFailureDetail = struct {
    table_name: []const u8,
    @"error": []const u8,
    table_name_truncated: bool = false,
};

/// Converts the detailed execution response into the bounded durable result
/// stored with a restore job. The public job phase is derived from the same
/// table statuses, so a partial restore can never be reported as succeeded.
pub fn summarizeClusterResultAlloc(alloc: std.mem.Allocator, raw: []const u8) !ClusterResultSummary {
    var parsed = std.json.parseFromSlice(ClusterRestoreResult, alloc, raw, .{ .ignore_unknown_fields = true }) catch
        return error.InvalidClusterRestoreResult;
    defer parsed.deinit();
    if (parsed.value.tables.len > max_tables_per_job) return error.InvalidClusterRestoreResult;

    var triggered: usize = 0;
    var committed: usize = 0;
    var skipped: usize = 0;
    var failed: usize = 0;
    for (parsed.value.tables) |table| {
        if (std.mem.eql(u8, table.status, "triggered")) {
            triggered += 1;
        } else if (std.mem.eql(u8, table.status, "committed")) {
            committed += 1;
        } else if (std.mem.eql(u8, table.status, "skipped")) {
            skipped += 1;
        } else if (std.mem.eql(u8, table.status, "failed")) {
            failed += 1;
        } else {
            return error.InvalidClusterRestoreResult;
        }
    }

    const failure_capacity = @min(failed, max_cluster_failure_details);
    const failures = try alloc.alloc(ClusterFailureDetail, failure_capacity);
    defer alloc.free(failures);
    var failure_count: usize = 0;
    var details_truncated = failed > failure_capacity;
    for (parsed.value.tables) |table| {
        if (!std.mem.eql(u8, table.status, "failed") or failure_count == failures.len) continue;
        const bounded_name = boundedUtf8Prefix(table.name, max_cluster_failure_table_name_bytes);
        const name_truncated = bounded_name.len != table.name.len;
        const raw_error = table.@"error" orelse "restore failed";
        const bounded_error = boundedUtf8Prefix(raw_error, max_cluster_failure_error_bytes);
        details_truncated = details_truncated or name_truncated or bounded_error.len != raw_error.len;
        failures[failure_count] = .{
            .table_name = bounded_name,
            .@"error" = bounded_error,
            .table_name_truncated = name_truncated,
        };
        failure_count += 1;
    }

    const successful = triggered + committed;
    const status: []const u8 = if (failed == 0)
        "completed"
    else if (successful == 0)
        "failed"
    else
        "partial";
    const encoded = try std.json.Stringify.valueAlloc(alloc, .{
        .status = status,
        .triggered_table_count = triggered,
        .committed_table_count = committed,
        .skipped_table_count = skipped,
        .failed_table_count = failed,
        .failure_details = failures[0..failure_count],
        .failure_details_truncated = details_truncated,
    }, .{ .emit_null_optional_fields = false });
    errdefer alloc.free(encoded);
    if (encoded.len > max_cluster_result_bytes) return error.ClusterRestoreResultTooLarge;
    return .{ .encoded = encoded, .succeeded = failed == 0 };
}

fn boundedUtf8Prefix(value: []const u8, max_bytes: usize) []const u8 {
    if (value.len <= max_bytes) return value;
    var end = max_bytes;
    while (end > 0 and value[end] & 0xc0 == 0x80) end -= 1;
    return value[0..end];
}

pub fn isTerminal(phase: Phase) bool {
    return phase == .succeeded or phase == .failed or phase == .cancelled;
}

fn encode(alloc: std.mem.Allocator, state: JobState) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, state, .{ .emit_null_optional_fields = false });
}

fn requestFingerprintAlloc(alloc: std.mem.Allocator, req: StartRequest) ![]u8 {
    const canonical = try std.json.Stringify.valueAlloc(alloc, .{
        .scope = req.scope,
        .table_name = req.table_name,
        .backup_id = req.backup_id,
        .location = req.location,
        .connection = req.connection,
        .restore_mode = req.restore_mode,
        .table_names = req.table_names,
    }, .{});
    defer alloc.free(canonical);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(canonical, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    return try alloc.dupe(u8, &hex);
}

fn jobKey(alloc: std.mem.Allocator, job_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{x:0>16}", .{ key_prefix, job_id });
}

fn nowMillis() u64 {
    return platform_time.realtimeNs() / std.time.ns_per_ms;
}

const TestReplicatedPersistence = struct {
    alloc: std.mem.Allocator,
    rows: std.StringHashMapUnmanaged([]u8) = .empty,

    fn init(alloc: std.mem.Allocator) TestReplicatedPersistence {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *TestReplicatedPersistence) void {
        var it = self.rows.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.*);
        }
        self.rows.deinit(self.alloc);
    }

    fn persistence(self: *TestReplicatedPersistence) ReplicatedPersistence {
        return .{ .ptr = self, .vtable = &.{
            .load = load,
            .get = get,
            .put = put,
            .delete = delete,
            .delete_many = deleteMany,
        } };
    }

    fn load(ptr: *anyopaque, alloc: std.mem.Allocator) ![]ReplicatedPersistence.OwnedRow {
        const self: *TestReplicatedPersistence = @ptrCast(@alignCast(ptr));
        const out = try alloc.alloc(ReplicatedPersistence.OwnedRow, self.rows.count());
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |row| {
                alloc.free(row.key);
                alloc.free(row.value);
            }
            alloc.free(out);
        }
        var it = self.rows.iterator();
        while (it.next()) |entry| {
            out[initialized] = .{
                .key = try alloc.dupe(u8, entry.key_ptr.*),
                .value = try alloc.dupe(u8, entry.value_ptr.*),
            };
            initialized += 1;
        }
        return out;
    }

    fn get(ptr: *anyopaque, alloc: std.mem.Allocator, key: []const u8) !?[]u8 {
        const self: *TestReplicatedPersistence = @ptrCast(@alignCast(ptr));
        return if (self.rows.get(key)) |value| try alloc.dupe(u8, value) else null;
    }

    fn put(ptr: *anyopaque, key: []const u8, value: []const u8) !void {
        const self: *TestReplicatedPersistence = @ptrCast(@alignCast(ptr));
        const owned_value = try self.alloc.dupe(u8, value);
        errdefer self.alloc.free(owned_value);
        if (self.rows.getPtr(key)) |existing| {
            self.alloc.free(existing.*);
            existing.* = owned_value;
            return;
        }
        const owned_key = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned_key);
        try self.rows.put(self.alloc, owned_key, owned_value);
    }

    fn delete(ptr: *anyopaque, key: []const u8) !void {
        const self: *TestReplicatedPersistence = @ptrCast(@alignCast(ptr));
        if (self.rows.fetchRemove(key)) |removed| {
            self.alloc.free(removed.key);
            self.alloc.free(removed.value);
        }
    }

    fn deleteMany(ptr: *anyopaque, keys: []const []const u8) !void {
        for (keys) |key| try delete(ptr, key);
    }
};

test "restore job store is idempotent and fenced" {
    var persistence = TestReplicatedPersistence.init(std.testing.allocator);
    defer persistence.deinit();
    var store = Store.initWithIo(std.testing.allocator, std.testing.io);
    defer store.deinit();
    try store.attachReplicated(persistence.persistence());
    const req: StartRequest = .{
        .scope = .cluster,
        .backup_id = "daily",
        .location = "s3://archive/daily",
        .connection = "archive-reader",
        .idempotency_key = "restore-daily",
    };
    const first = try store.start(std.testing.allocator, req);
    defer std.testing.allocator.free(first);
    const second = try store.start(std.testing.allocator, req);
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualStrings(first, second);
    var parsed = try std.json.parseFromSlice(JobState, std.testing.allocator, first, .{});
    defer parsed.deinit();
    const running = (try store.begin(std.testing.allocator, parsed.value.job_id)).?;
    defer std.testing.allocator.free(running);
    var parsed_running = try std.json.parseFromSlice(JobState, std.testing.allocator, running, .{});
    defer parsed_running.deinit();
    const done = try store.finish(std.testing.allocator, parsed_running.value, "{\"status\":\"completed\"}");
    defer std.testing.allocator.free(done);
    var parsed_done = try std.json.parseFromSlice(JobState, std.testing.allocator, done, .{});
    defer parsed_done.deinit();
    try std.testing.expectEqual(Phase.succeeded, parsed_done.value.phase);
}

test "successful restore completion wins a racing cancellation" {
    var persistence = TestReplicatedPersistence.init(std.testing.allocator);
    defer persistence.deinit();
    var store = Store.initWithIo(std.testing.allocator, std.testing.io);
    defer store.deinit();
    try store.attachReplicated(persistence.persistence());

    const started = try store.start(std.testing.allocator, .{
        .scope = .table,
        .table_name = "docs",
        .backup_id = "daily",
        .location = "s3://archive/daily",
        .connection = "archive-reader",
    });
    defer std.testing.allocator.free(started);
    var parsed_started = try std.json.parseFromSlice(JobState, std.testing.allocator, started, .{});
    defer parsed_started.deinit();

    const running = (try store.begin(std.testing.allocator, parsed_started.value.job_id)).?;
    defer std.testing.allocator.free(running);
    var parsed_running = try std.json.parseFromSlice(JobState, std.testing.allocator, running, .{});
    defer parsed_running.deinit();

    try std.testing.expectEqual(AttemptState.active, try store.attemptState(std.testing.allocator, parsed_started.value.job_id, parsed_running.value.attempt_id));
    try std.testing.expectEqual(AttemptState.fenced, try store.attemptState(std.testing.allocator, parsed_started.value.job_id, parsed_running.value.attempt_id + 1));

    const cancelling = (try store.cancel(std.testing.allocator, parsed_started.value.job_id)).?;
    defer std.testing.allocator.free(cancelling);
    try std.testing.expectEqual(AttemptState.cancelled, try store.attemptState(std.testing.allocator, parsed_started.value.job_id, parsed_running.value.attempt_id));
    const completed = try store.finish(std.testing.allocator, parsed_running.value, "{\"restored\":true}");
    defer std.testing.allocator.free(completed);
    var parsed_completed = try std.json.parseFromSlice(JobState, std.testing.allocator, completed, .{});
    defer parsed_completed.deinit();

    try std.testing.expectEqual(Phase.succeeded, parsed_completed.value.phase);
    try std.testing.expect(parsed_completed.value.cancel_requested);
    try std.testing.expectEqualStrings("{\"restored\":true}", parsed_completed.value.result_json.?);
    try std.testing.expect(parsed_completed.value.last_error == null);
}

test "restore job runnable queue drains incrementally and preserves insertion order" {
    var persistence = TestReplicatedPersistence.init(std.testing.allocator);
    defer persistence.deinit();
    var store = Store.initWithIo(std.testing.allocator, std.testing.io);
    defer store.deinit();
    try store.attachReplicated(persistence.persistence());
    var created: [3]u64 = undefined;
    for (&created, 0..) |*job_id, i| {
        const encoded = try store.start(std.testing.allocator, .{
            .scope = .table,
            .table_name = "docs",
            .backup_id = if (i == 0) "one" else if (i == 1) "two" else "three",
            .location = "s3://archive/restore",
            .connection = "archive-reader",
        });
        defer std.testing.allocator.free(encoded);
        var parsed = try std.json.parseFromSlice(JobState, std.testing.allocator, encoded, .{});
        defer parsed.deinit();
        job_id.* = parsed.value.job_id;
    }

    const first = try store.takePendingIds(std.testing.allocator, 2);
    defer std.testing.allocator.free(first);
    try std.testing.expectEqualSlices(u64, created[0..2], first);
    const second = try store.takePendingIds(std.testing.allocator, 2);
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualSlices(u64, created[2..3], second);
    const empty = try store.takePendingIds(std.testing.allocator, 2);
    defer std.testing.allocator.free(empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
}

test "replicated restore leadership rebuild preserves FIFO and recovers running attempts" {
    var persistence = TestReplicatedPersistence.init(std.testing.allocator);
    defer persistence.deinit();
    var store = Store.initWithIo(std.testing.allocator, std.testing.io);
    defer store.deinit();
    try store.attachReplicated(persistence.persistence());

    var created: [3]u64 = undefined;
    for (&created, 0..) |*job_id, i| {
        const idempotency_key = try std.fmt.allocPrint(std.testing.allocator, "replicated-fifo-{d}", .{i});
        defer std.testing.allocator.free(idempotency_key);
        const encoded = try store.start(std.testing.allocator, .{
            .scope = .cluster,
            .backup_id = "daily",
            .location = "s3://archive/daily",
            .connection = "archive-reader",
            .idempotency_key = idempotency_key,
        });
        defer std.testing.allocator.free(encoded);
        var parsed = try std.json.parseFromSlice(JobState, std.testing.allocator, encoded, .{});
        defer parsed.deinit();
        job_id.* = parsed.value.job_id;
    }

    const first = try store.takePendingIds(std.testing.allocator, 1);
    defer std.testing.allocator.free(first);
    try std.testing.expectEqualSlices(u64, created[0..1], first);
    const running = (try store.begin(std.testing.allocator, created[0])).?;
    std.testing.allocator.free(running);

    const expired_key = try jobKey(std.testing.allocator, 999);
    defer std.testing.allocator.free(expired_key);
    const expired = try encode(std.testing.allocator, .{
        .format_version = restore_job_format_version,
        .job_id = 999,
        .enqueue_sequence = 999,
        .scope = .cluster,
        .backup_id = "expired",
        .location = "s3://archive/expired",
        .connection = "archive-reader",
        .phase = .succeeded,
        .idempotency_key = "expired",
        .request_fingerprint = "expired",
        .created_at_ms = 0,
        .updated_at_ms = 0,
        .expires_at_ms = 0,
    });
    defer std.testing.allocator.free(expired);
    try TestReplicatedPersistence.put(&persistence, expired_key, expired);

    try store.prepareReplicatedLeadership(std.testing.allocator);
    try std.testing.expect(!persistence.rows.contains(expired_key));
    const recovered = try store.takePendingIds(std.testing.allocator, 3);
    defer std.testing.allocator.free(recovered);
    try std.testing.expectEqualSlices(u64, &created, recovered);
}

test "restore requests without idempotency keys create independent opaque jobs" {
    var persistence = TestReplicatedPersistence.init(std.testing.allocator);
    defer persistence.deinit();
    var store = Store.initWithIo(std.testing.allocator, std.testing.io);
    defer store.deinit();
    try store.attachReplicated(persistence.persistence());
    const req: StartRequest = .{
        .scope = .table,
        .table_name = "docs",
        .backup_id = "daily",
        .location = "file:///daily",
        .connection = "local-reader",
    };
    const first = try store.start(std.testing.allocator, req);
    defer std.testing.allocator.free(first);
    const second = try store.start(std.testing.allocator, req);
    defer std.testing.allocator.free(second);
    var parsed_first = try std.json.parseFromSlice(JobState, std.testing.allocator, first, .{});
    defer parsed_first.deinit();
    var parsed_second = try std.json.parseFromSlice(JobState, std.testing.allocator, second, .{});
    defer parsed_second.deinit();
    try std.testing.expect(parsed_first.value.job_id != parsed_second.value.job_id);
    try std.testing.expect(parsed_first.value.job_id <= std.math.maxInt(i64));
    try std.testing.expect(parsed_first.value.expires_at_ms > parsed_first.value.created_at_ms);
}

test "restore runtime store persists checkpoints and requeues interrupted work" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "system/api-restore-jobs" });
    defer runtime.deinit();

    var job_id: u64 = 0;
    {
        var first_store = Store.initWithIo(alloc, std.testing.io);
        defer first_store.deinit();
        try first_store.attachRuntime(&runtime);
        const started = try first_store.start(alloc, .{
            .scope = .cluster,
            .backup_id = "daily",
            .location = "s3://archive/daily",
            .connection = "archive-reader",
            .table_names = &.{ "docs", "users" },
            .idempotency_key = "restore-daily",
        });
        defer alloc.free(started);
        var parsed_started = try std.json.parseFromSlice(JobState, alloc, started, .{});
        defer parsed_started.deinit();
        job_id = parsed_started.value.job_id;
        const running = (try first_store.begin(alloc, job_id)).?;
        defer alloc.free(running);
        var parsed_running = try std.json.parseFromSlice(JobState, alloc, running, .{});
        defer parsed_running.deinit();
        try std.testing.expectError(
            error.RestoreJobCheckpointOrder,
            first_store.recordTableCompleted(alloc, job_id, parsed_running.value.attempt_id, 0),
        );
        const published = try first_store.recordTablePublished(alloc, job_id, parsed_running.value.attempt_id, 0);
        defer alloc.free(published);
        const completed = try first_store.recordTableCompleted(alloc, job_id, parsed_running.value.attempt_id, 0);
        defer alloc.free(completed);
    }

    var recovered_store = Store.init(alloc);
    defer recovered_store.deinit();
    try recovered_store.attachRuntime(&runtime);
    const recovered = (try recovered_store.load(alloc, job_id)).?;
    defer alloc.free(recovered);
    var parsed = try std.json.parseFromSlice(JobState, alloc, recovered, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(Phase.queued, parsed.value.phase);
    try std.testing.expectEqualSlices(u16, &.{0}, parsed.value.published_table_indexes.?);
    try std.testing.expectEqualSlices(u16, &.{0}, parsed.value.completed_table_indexes.?);
}

test "restore progress ordinals remain bounded at maximum table count" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(alloc, .{ .name = "system/api-restore-jobs-ordinals" });
    defer runtime.deinit();
    var store = Store.initWithIo(alloc, std.testing.io);
    defer store.deinit();
    try store.attachRuntime(&runtime);
    var table_name_storage: [max_tables_per_job][200]u8 = undefined;
    var table_names: [max_tables_per_job][]const u8 = undefined;
    for (&table_name_storage, 0..) |*name, i| {
        @memset(name, 'x');
        name[0] = @intCast('a' + i / 26);
        name[1] = @intCast('a' + i % 26);
        table_names[i] = name;
    }
    const started = try store.start(alloc, .{
        .scope = .cluster,
        .backup_id = "large-cluster",
        .location = "s3://archive/large-cluster",
        .connection = "archive-reader",
        .table_names = &table_names,
    });
    defer alloc.free(started);
    var parsed_started = try std.json.parseFromSlice(JobState, alloc, started, .{});
    defer parsed_started.deinit();
    const running = (try store.begin(alloc, parsed_started.value.job_id)).?;
    defer alloc.free(running);
    var parsed_running = try std.json.parseFromSlice(JobState, alloc, running, .{});
    defer parsed_running.deinit();

    for (0..max_tables_per_job) |i| {
        const published = try store.recordTablePublished(alloc, parsed_started.value.job_id, parsed_running.value.attempt_id, @intCast(i));
        alloc.free(published);
        const completed = try store.recordTableCompleted(alloc, parsed_started.value.job_id, parsed_running.value.attempt_id, @intCast(i));
        alloc.free(completed);
    }

    const encoded = (try store.load(alloc, parsed_started.value.job_id)).?;
    defer alloc.free(encoded);
    try std.testing.expect(encoded.len <= max_restore_job_record_bytes);
    var parsed = try std.json.parseFromSlice(JobState, alloc, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, max_tables_per_job), parsed.value.published_table_indexes.?.len);
    try std.testing.expectEqual(@as(usize, max_tables_per_job), parsed.value.completed_table_indexes.?.len);

    var long_failure_name: [1024]u8 = undefined;
    @memset(&long_failure_name, 'f');
    var failure_tables: [10]ClusterRestoreTableResult = undefined;
    for (&failure_tables) |*table| table.* = .{ .name = &long_failure_name, .status = "failed", .@"error" = "restore failed" };
    const detailed = try std.json.Stringify.valueAlloc(alloc, .{ .tables = &failure_tables }, .{});
    defer alloc.free(detailed);
    var summary = try summarizeClusterResultAlloc(alloc, detailed);
    defer summary.deinit(alloc);
    const terminal = try store.failWithResult(alloc, parsed_running.value, summary.encoded, "ClusterRestorePartialFailure");
    defer alloc.free(terminal);
    try std.testing.expect(terminal.len <= max_restore_job_record_bytes);
    var parsed_terminal = try std.json.parseFromSlice(JobState, alloc, terminal, .{});
    defer parsed_terminal.deinit();
    try std.testing.expectEqual(Phase.failed, parsed_terminal.value.phase);
}

test "cluster restore summaries are truthful and bounded" {
    const alloc = std.testing.allocator;
    var long_name: [1024]u8 = undefined;
    @memset(&long_name, 'x');
    var tables: [10]ClusterRestoreTableResult = undefined;
    for (&tables) |*table| table.* = .{
        .name = &long_name,
        .status = "failed",
        .@"error" = "restore failed",
    };
    const detailed = try std.json.Stringify.valueAlloc(alloc, .{ .tables = &tables, .status = "failed" }, .{});
    defer alloc.free(detailed);
    var summary = try summarizeClusterResultAlloc(alloc, detailed);
    defer summary.deinit(alloc);
    try std.testing.expect(!summary.succeeded);
    try std.testing.expect(summary.encoded.len <= max_cluster_result_bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, summary.encoded, .{});
    defer parsed.deinit();
    const object = parsed.value.object;
    try std.testing.expectEqualStrings("failed", object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 10), object.get("failed_table_count").?.integer);
    try std.testing.expectEqual(@as(usize, max_cluster_failure_details), object.get("failure_details").?.array.items.len);
    try std.testing.expect(object.get("failure_details_truncated").?.bool);
    try std.testing.expect(object.get("failure_details").?.array.items[0].object.get("table_name").?.string.len <= max_cluster_failure_table_name_bytes);

    var successful = try summarizeClusterResultAlloc(alloc, "{\"status\":\"triggered\",\"tables\":[{\"name\":\"docs\",\"status\":\"triggered\"},{\"name\":\"existing\",\"status\":\"skipped\"}]}");
    defer successful.deinit(alloc);
    try std.testing.expect(successful.succeeded);
}

test "restore job store rejects oversized request state" {
    var store = Store.initWithIo(std.testing.allocator, std.testing.io);
    defer store.deinit();
    const oversized = try std.testing.allocator.alloc(u8, max_restore_string_bytes + 1);
    defer std.testing.allocator.free(oversized);
    @memset(oversized, 'x');
    try std.testing.expectError(error.RestoreJobRecordTooLarge, store.start(std.testing.allocator, .{
        .scope = .cluster,
        .backup_id = "daily",
        .location = oversized,
        .connection = "archive-reader",
    }));
    var large_names_storage: [15][4000]u8 = undefined;
    var large_names: [large_names_storage.len][]const u8 = undefined;
    for (&large_names_storage, 0..) |*name, i| {
        @memset(name, 'x');
        name[0] = @intCast('a' + i);
        large_names[i] = name;
    }
    try std.testing.expectError(error.RestoreJobRecordTooLarge, store.start(std.testing.allocator, .{
        .scope = .cluster,
        .backup_id = "daily",
        .location = "s3://archive/backups",
        .connection = "archive-reader",
        .table_names = &large_names,
    }));
    try std.testing.expectError(error.DuplicateRestoreTableName, store.start(std.testing.allocator, .{
        .scope = .cluster,
        .backup_id = "daily",
        .location = "s3://archive/backups",
        .connection = "archive-reader",
        .table_names = &.{ "docs", "docs" },
    }));
    try std.testing.expectError(error.RestoreJobPersistenceUnavailable, store.start(std.testing.allocator, .{
        .scope = .cluster,
        .backup_id = "daily",
        .location = "s3://archive/backups",
        .connection = "archive-reader",
    }));

    var no_io_store = Store.init(std.testing.allocator);
    defer no_io_store.deinit();
    try std.testing.expectError(error.AsyncRestoreUnavailable, no_io_store.start(std.testing.allocator, .{
        .scope = .cluster,
        .backup_id = "daily",
        .location = "s3://archive/backups",
        .connection = "archive-reader",
    }));
}
