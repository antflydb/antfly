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
    created_at_millis: u64,
    last_updated_at_millis: u64,
    expires_at_millis: u64,
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

    pub fn startJob(
        self: *Store,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        req: StartRequest,
    ) ![]u8 {
        const now_ms = nowMillis();
        const job_id = self.nextJobId();
        const encoded = try encodeState(alloc, .{
            .job_id = job_id,
            .table_name = table_name,
            .artifact_name = artifact_name,
            .phase = phaseString(.queued),
            .reprocess_status = "in_progress",
            .from_key = req.from_key,
            .to_key = req.to_key,
            .limit = if (req.limit == 0) 100 else req.limit,
            .created_at_millis = now_ms,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        try self.storeEncoded(job_id, encoded);
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
        const encoded = try encodeState(alloc, .{
            .job_id = previous.job_id,
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
            .last_error = last_error,
            .created_at_millis = previous.created_at_millis,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        try self.storeEncoded(previous.job_id, encoded);
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
        const encoded = try encodeState(alloc, .{
            .job_id = previous.job_id,
            .table_name = previous.table_name,
            .artifact_name = previous.artifact_name,
            .phase = phaseString(phase),
            .reprocess_status = reprocessStatusForPhase(phase, pending_shards),
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
            .created_at_millis = previous.created_at_millis,
            .last_updated_at_millis = now_ms,
            .expires_at_millis = now_ms + self.retentionMillis(),
        });
        errdefer alloc.free(encoded);
        try self.storeEncoded(previous.job_id, encoded);
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
        for (expired.items) |job_id| {
            if (self.jobs.fetchRemove(job_id)) |removed| self.alloc.free(removed.value);
            self.deletePersisted(job_id);
        }
    }

    fn nextJobId(self: *Store) u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const job_id = self.next_job_id;
        self.next_job_id += 1;
        return job_id;
    }

    fn storeEncoded(self: *Store, job_id: u64, encoded: []const u8) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const owned = try self.alloc.dupe(u8, encoded);
        errdefer self.alloc.free(owned);
        if (try self.jobs.fetchPut(self.alloc, job_id, owned)) |old| self.alloc.free(old.value);
        if (self.opened_store) |opened| {
            const key = try jobKey(self.alloc, job_id);
            defer self.alloc.free(key);
            try opened.docstore.put(key, encoded);
        }
    }

    fn deletePersisted(self: *Store, job_id: u64) void {
        const opened = self.opened_store orelse return;
        const key = jobKey(self.alloc, job_id) catch return;
        defer self.alloc.free(key);
        opened.docstore.delete(key) catch {};
    }
};

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
    return @divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms);
}

fn jobKey(alloc: std.mem.Allocator, job_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "__api_artifact_reprocess_jobs__:{d}", .{job_id});
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
