// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const docstore_mod = @import("../storage/docstore.zig");
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("../platform/time.zig");

const key_prefix = "\x00\x00__api_restore_jobs__:";
const restore_job_retention_ms: u64 = 7 * 24 * 60 * 60 * 1000;
const restore_job_scan_page_size: usize = 512;
const max_retained_restore_jobs: usize = 10_000;

pub const Scope = enum { table, cluster };
pub const Phase = enum { queued, running, succeeded, failed, cancelled };

pub const JobState = struct {
    job_id: u64,
    attempt_id: u64 = 0,
    scope: Scope,
    table_name: ?[]const u8 = null,
    backup_id: []const u8,
    location: []const u8,
    connection: []const u8,
    restore_mode: []const u8 = "fail_if_exists",
    table_names: ?[]const []const u8 = null,
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

pub const Store = struct {
    alloc: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    jobs: std.AutoHashMapUnmanaged(u64, []u8) = .empty,
    idempotency: std.StringHashMapUnmanaged(u64) = .empty,
    opened: ?*OpenedStore = null,

    pub fn init(alloc: std.mem.Allocator) Store {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Store) void {
        var jobs = self.jobs.iterator();
        while (jobs.next()) |entry| self.alloc.free(entry.value_ptr.*);
        self.jobs.deinit(self.alloc);
        var keys = self.idempotency.iterator();
        while (keys.next()) |entry| self.alloc.free(entry.key_ptr.*);
        self.idempotency.deinit(self.alloc);
        if (self.opened) |opened| {
            opened.deinit();
            self.alloc.destroy(opened);
        }
        self.* = undefined;
    }

    pub fn attach(self: *Store, opened: *OpenedStore) !void {
        self.lock();
        defer self.mutex.unlock();
        self.opened = opened;
        errdefer self.opened = null;
        var after_key: ?[]u8 = null;
        defer if (after_key) |key| self.alloc.free(key);
        while (true) {
            const rows = try opened.docstore.scanPrefixPage(self.alloc, key_prefix, after_key, restore_job_scan_page_size);
            defer docstore_mod.DocStore.freeResults(self.alloc, rows);
            if (rows.len == 0) break;
            for (rows) |row| {
                var parsed = std.json.parseFromSlice(JobState, self.alloc, row.value, .{ .ignore_unknown_fields = true }) catch return error.CorruptRestoreJobStore;
                defer parsed.deinit();
                if (isTerminal(parsed.value.phase) and parsed.value.expires_at_ms <= nowMillis()) {
                    try opened.docstore.delete(row.key);
                    continue;
                }
                if (self.jobs.count() >= max_retained_restore_jobs) return error.RestoreJobCapacityExceeded;
                const encoded = if (parsed.value.phase == .running) blk: {
                    const recovered = try encode(self.alloc, .{
                        .job_id = parsed.value.job_id,
                        .attempt_id = parsed.value.attempt_id,
                        .scope = parsed.value.scope,
                        .table_name = parsed.value.table_name,
                        .backup_id = parsed.value.backup_id,
                        .location = parsed.value.location,
                        .connection = parsed.value.connection,
                        .restore_mode = parsed.value.restore_mode,
                        .table_names = parsed.value.table_names,
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
                try self.jobs.put(self.alloc, parsed.value.job_id, encoded);
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
    }

    pub fn start(self: *Store, alloc: std.mem.Allocator, req: StartRequest) ![]u8 {
        const fingerprint = try requestFingerprintAlloc(alloc, req);
        defer alloc.free(fingerprint);
        const explicit_idempotency_key = if (req.idempotency_key) |provided| blk: {
            if (provided.len == 0 or provided.len > 256) return error.InvalidIdempotencyKey;
            break :blk provided;
        } else null;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        var entropy: [16]u8 = undefined;
        try io_impl.io().randomSecure(&entropy);

        self.lock();
        defer self.mutex.unlock();
        try self.pruneExpiredLocked(nowMillis());
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
            try io_impl.io().randomSecure(entropy[0..8]);
            job_id = std.mem.readInt(u64, entropy[0..8], .little) & std.math.maxInt(i64);
        }
        const auto_nonce = std.mem.readInt(u64, entropy[8..16], .little);
        const generated_key = if (explicit_idempotency_key == null)
            try std.fmt.allocPrint(alloc, "auto:{x:0>16}", .{auto_nonce})
        else
            null;
        defer if (generated_key) |key| alloc.free(key);
        const idempotency_key = explicit_idempotency_key orelse generated_key.?;
        const encoded = try encode(alloc, .{
            .job_id = job_id,
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
        try self.jobs.ensureUnusedCapacity(self.alloc, 1);
        if (explicit_idempotency_key != null) try self.idempotency.ensureUnusedCapacity(self.alloc, 1);
        const owned_key = if (explicit_idempotency_key != null) try self.alloc.dupe(u8, idempotency_key) else null;
        errdefer if (owned_key) |key| self.alloc.free(key);
        try self.storeLocked(job_id, encoded);
        if (owned_key) |key| self.idempotency.putAssumeCapacity(key, job_id);
        return encoded;
    }

    pub fn load(self: *Store, alloc: std.mem.Allocator, job_id: u64) !?[]u8 {
        self.lock();
        defer self.mutex.unlock();
        return if (self.jobs.get(job_id)) |encoded| try alloc.dupe(u8, encoded) else null;
    }

    pub fn pendingIds(self: *Store, alloc: std.mem.Allocator) ![]u64 {
        self.lock();
        defer self.mutex.unlock();
        var ids = std.ArrayListUnmanaged(u64).empty;
        errdefer ids.deinit(alloc);
        var it = self.jobs.iterator();
        while (it.next()) |entry| {
            var parsed = std.json.parseFromSlice(JobState, alloc, entry.value_ptr.*, .{ .ignore_unknown_fields = true }) catch continue;
            defer parsed.deinit();
            if (parsed.value.phase == .queued or parsed.value.phase == .running) try ids.append(alloc, parsed.value.job_id);
        }
        return try ids.toOwnedSlice(alloc);
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

    fn finishAs(self: *Store, alloc: std.mem.Allocator, expected: JobState, phase: Phase, result_json: ?[]const u8, last_error: ?[]const u8) ![]u8 {
        self.lock();
        defer self.mutex.unlock();
        const current = self.jobs.get(expected.job_id) orelse return error.NotFound;
        var parsed = try std.json.parseFromSlice(JobState, alloc, current, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        if (parsed.value.attempt_id != expected.attempt_id or parsed.value.phase != .running) return try alloc.dupe(u8, current);
        return try self.updateLocked(alloc, parsed.value, .{
            .phase = if (parsed.value.cancel_requested) .cancelled else phase,
            .result_json = result_json,
            .last_error = if (parsed.value.cancel_requested) "cancel_requested" else last_error,
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

    pub fn cancellationRequested(self: *Store, alloc: std.mem.Allocator, job_id: u64, attempt_id: u64) !bool {
        self.lock();
        defer self.mutex.unlock();
        const current = self.jobs.get(job_id) orelse return true;
        var parsed = try std.json.parseFromSlice(JobState, alloc, current, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        return parsed.value.attempt_id != attempt_id or parsed.value.cancel_requested or parsed.value.phase != .running;
    }

    const Update = struct {
        phase: Phase,
        attempt_id: ?u64 = null,
        cancel_requested: ?bool = null,
        result_json: ?[]const u8 = null,
        last_error: ?[]const u8 = null,
    };

    fn updateLocked(self: *Store, alloc: std.mem.Allocator, current: JobState, update: Update) ![]u8 {
        const encoded = try encode(alloc, .{
            .job_id = current.job_id,
            .attempt_id = update.attempt_id orelse current.attempt_id,
            .scope = current.scope,
            .table_name = current.table_name,
            .backup_id = current.backup_id,
            .location = current.location,
            .connection = current.connection,
            .restore_mode = current.restore_mode,
            .table_names = current.table_names,
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
        });
        errdefer alloc.free(encoded);
        try self.storeLocked(current.job_id, encoded);
        return encoded;
    }

    fn storeLocked(self: *Store, job_id: u64, encoded: []const u8) !void {
        const owned = try self.alloc.dupe(u8, encoded);
        errdefer self.alloc.free(owned);
        if (self.opened) |opened| {
            const key = try jobKey(self.alloc, job_id);
            defer self.alloc.free(key);
            try opened.docstore.put(key, encoded);
        }
        if (try self.jobs.fetchPut(self.alloc, job_id, owned)) |previous| self.alloc.free(previous.value);
    }

    fn pruneExpiredLocked(self: *Store, now_ms: u64) !void {
        var expired = std.ArrayListUnmanaged(u64).empty;
        defer expired.deinit(self.alloc);
        var it = self.jobs.iterator();
        while (it.next()) |entry| {
            var parsed = std.json.parseFromSlice(JobState, self.alloc, entry.value_ptr.*, .{ .ignore_unknown_fields = true }) catch return error.CorruptRestoreJobStore;
            defer parsed.deinit();
            if (isTerminal(parsed.value.phase) and parsed.value.expires_at_ms <= now_ms) try expired.append(self.alloc, entry.key_ptr.*);
        }
        for (expired.items) |job_id| {
            const removed = self.jobs.fetchRemove(job_id) orelse continue;
            var parsed = std.json.parseFromSlice(JobState, self.alloc, removed.value, .{ .ignore_unknown_fields = true }) catch {
                self.alloc.free(removed.value);
                return error.CorruptRestoreJobStore;
            };
            if (self.idempotency.fetchRemove(parsed.value.idempotency_key)) |key_entry| self.alloc.free(key_entry.key);
            parsed.deinit();
            self.alloc.free(removed.value);
            if (self.opened) |opened| {
                const key = try jobKey(self.alloc, job_id);
                defer self.alloc.free(key);
                try opened.docstore.delete(key);
            }
        }
    }

    fn lock(self: *Store) void {
        platform_sync.lockYielding(&self.mutex);
    }
};

pub fn isTerminal(phase: Phase) bool {
    return phase == .succeeded or phase == .failed or phase == .cancelled;
}

fn encode(alloc: std.mem.Allocator, state: JobState) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, state, .{});
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

test "restore job store is idempotent and fenced" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();
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

test "restore requests without idempotency keys create independent opaque jobs" {
    var store = Store.init(std.testing.allocator);
    defer store.deinit();
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
