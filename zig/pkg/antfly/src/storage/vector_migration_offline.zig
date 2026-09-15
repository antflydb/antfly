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

//! Stopped-table migration. The caller also owns catalog admission. Copy every
//! regular file under an exclusive generation lease, recording a durable byte
//! cursor; then use the same source conversion/verifier as online migration.
const std = @import("std");
const db = @import("db/db.zig");
const contract = @import("../common/vector_migration.zig");
const files = @import("../common/migration_files.zig");
const fs = @import("../common/fs_paths.zig");
const platform = @import("antfly_platform");
const Allocator = std.mem.Allocator;
const progress_file = "VECTOR-MIGRATION-COPY.json";
const cancellation_file = "VECTOR-MIGRATION-CANCELLED.json";
const Cancellation = struct { version: u32 = 1, request: contract.Request, identity: []const u8 };

const Entry = struct { path: []const u8, size: u64 };
const Fence = struct {
    version: u32 = 1,
    request: contract.Request,
    identity: []const u8,
    entries: []const Entry,
    source_bytes: u64,
};
const Progress = struct {
    version: u32 = 1,
    job_id: []const u8,
    file: usize = 0,
    offset: u64 = 0,
    copied_bytes: u64 = 0,
    copy_complete: bool = false,
};

pub const Options = struct {
    open: db.OpenOptions = .{},
    /// Bounds work for orchestration/tests. Zero runs until complete. A pending
    /// return deliberately leaves both durable admission and candidate intact.
    max_steps: usize = 0,
    progress_ctx: ?*anyopaque = null,
    progress_fn: ?*const fn (?*anyopaque, []const u8) anyerror!void = null,
};
pub const Result = enum { pending, complete };
pub const Boundary = enum { fenced, chunk_synced, cursor_synced, candidate_complete, before_publication, after_publication };
pub var test_boundary: ?*const fn (Boundary) anyerror!void = null;
fn boundary(point: Boundary) !void {
    if (@import("builtin").is_test) if (test_boundary) |hook| try hook(point);
}

fn readJson(comptime T: type, alloc: Allocator, io: std.Io, path: []const u8) !std.json.Parsed(T) {
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(64 * 1024 * 1024));
    defer alloc.free(bytes);
    return try std.json.parseFromSlice(T, alloc, bytes, .{ .allocate = .alloc_always });
}
fn save(alloc: Allocator, io: std.Io, path: []const u8, value: anytype) !void {
    const encoded = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(encoded);
    try files.writeAtomic(alloc, io, path, encoded);
}
fn exists(io: std.Io, path: []const u8) !bool {
    std.Io.Dir.cwd().access(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}
fn inventory(alloc: Allocator, io: std.Io, root: []const u8) ![]Entry {
    var dir = try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true });
    defer dir.close(io);
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    var result = std.ArrayListUnmanaged(Entry).empty;
    errdefer {
        for (result.items) |entry| alloc.free(entry.path);
        result.deinit(alloc);
    }
    while (try walker.next(io)) |entry| {
        if (entry.kind == .directory) continue;
        if (entry.kind != .file) return error.VectorMigrationUnsupportedFile;
        if (std.mem.eql(u8, entry.path, contract.offline_fence_file) or
            std.mem.eql(u8, entry.path, progress_file) or std.mem.endsWith(u8, entry.path, ".migration-tmp")) continue;
        const stat = try dir.statFile(io, entry.path, .{});
        try result.append(alloc, .{ .path = try alloc.dupe(u8, entry.path), .size = stat.size });
    }
    std.mem.sort(Entry, result.items, {}, struct {
        fn less(_: void, a: Entry, b: Entry) bool {
            return std.mem.lessThan(u8, a.path, b.path);
        }
    }.less);
    return try result.toOwnedSlice(alloc);
}
fn capacity(root: []const u8, budget: contract.Budget, needed: u64) !void {
    const available = try platform.filesystem.capacity(root);
    if (available.available_bytes < budget.disk_reserve_bytes +| needed) return error.VectorMigrationDiskReserve;
}

pub fn run(alloc: Allocator, io: std.Io, root: []const u8, request: contract.Request, options: Options) !Result {
    try request.validate();
    if (request.mode != .offline) return error.InvalidVectorMigrationState;
    var transition = try @import("db/generation_lifecycle.zig").beginProcessExclusiveWithRuntimeAndIo(root, options.open.backend_runtime, io);
    defer transition.deinit();
    try transition.reconcilePublished();
    const live = transition.path;
    const fence_path = try std.fs.path.join(alloc, &.{ live, contract.offline_fence_file });
    defer alloc.free(fence_path);
    const plan = try db.DB.resolveNativeRestoreOpenPlan(live, options.open);
    if (plan.physicalRootMode() != .filesystem_managed) return error.VectorStoreLifecycleUnsupported;
    var source_options = try plan.optionsForTarget(live);
    source_options.exclusive_generation = &transition;
    source_options.staged_generation = null;
    source_options.open_mode = .status_only;
    source_options.start_index_workers = false;
    source_options.start_optional_runtimes = false;
    source_options.start_optional_runtime_workers = false;
    // A completed candidate may already be the live root after a lost response.
    {
        var source = try db.DB.open(alloc, live, source_options);
        defer source.close();
        if (try source.vectorMigrationStatus(alloc)) |raw| {
            defer alloc.free(raw);
            var job = try std.json.parseFromSlice(contract.Job, alloc, raw, .{});
            defer job.deinit();
            if (std.mem.eql(u8, job.value.job_id, request.job_id) and job.value.mode == .offline and job.value.phase == .complete) {
                if (!std.meta.eql(job.value.budget, request.budget)) return error.VectorMigrationIdempotencyConflict;
                return .complete;
            }
            if (job.value.active() or job.value.published()) return error.VectorMigrationAlreadyExists;
        }
        if (source.table_storage.dense_embeddings != .primary_lsm) return error.VectorMigrationAlreadyPublished;
        if (source.primary_backend != .lsm) return error.VectorStoreRequiresLocalSingleShardTable;
        const cancelled_path = try std.fs.path.join(alloc, &.{ live, cancellation_file });
        defer alloc.free(cancelled_path);
        if (try exists(io, cancelled_path)) {
            var cancelled = try readJson(Cancellation, alloc, io, cancelled_path);
            defer cancelled.deinit();
            if (std.mem.eql(u8, cancelled.value.request.job_id, request.job_id)) {
                if (cancelled.value.version != 1 or !(contract.Admission{ .request = cancelled.value.request }).eql(.{ .request = request }))
                    return error.VectorMigrationIdempotencyConflict;
                return error.VectorMigrationCancelled;
            }
        }
        if (!try exists(io, fence_path)) {
            const identity = try std.json.Stringify.valueAlloc(alloc, source.core.identity_namespace, .{});
            defer alloc.free(identity);
            const entries = try inventory(alloc, io, live);
            defer {
                for (entries) |entry| alloc.free(entry.path);
                alloc.free(entries);
            }
            var total: u64 = 0;
            for (entries) |entry| total = try std.math.add(u64, total, entry.size);
            // Reserve overlap for the shadow, source payloads and WAL/ANN
            // rewriting. Individual copy/preparation steps recheck capacity.
            if (total > request.budget.temporary_bytes / 4) return error.VectorMigrationTemporaryBudgetExceeded;
            try capacity(live, request.budget, total * 4);
            try save(alloc, io, fence_path, Fence{ .request = request, .identity = identity, .entries = entries, .source_bytes = total });
            try boundary(.fenced);
        }
    }
    var fence = try readJson(Fence, alloc, io, fence_path);
    defer fence.deinit();
    if (fence.value.version != 1 or !(contract.Admission{ .request = fence.value.request }).eql(.{ .request = request }))
        return error.VectorMigrationIdempotencyConflict;
    var staged = try transition.resumeStaging(request.job_id);
    defer staged.deinit();
    const cursor_path = try std.fs.path.join(alloc, &.{ staged.path(), progress_file });
    defer alloc.free(cursor_path);
    var progress = Progress{ .job_id = request.job_id };
    if (try exists(io, cursor_path)) {
        var previous = try readJson(Progress, alloc, io, cursor_path);
        defer previous.deinit();
        if (previous.value.version != 1 or !std.mem.eql(u8, previous.value.job_id, request.job_id)) return error.InvalidVectorMigrationState;
        progress = previous.value;
        progress.job_id = request.job_id;
    }
    var steps: usize = 0;
    const buffer = try alloc.alloc(u8, @intCast(request.budget.batch_bytes));
    defer alloc.free(buffer);
    const verify = try alloc.alloc(u8, buffer.len);
    defer alloc.free(verify);
    while (!progress.copy_complete) {
        if (options.max_steps != 0 and steps >= options.max_steps) return .pending;
        if (progress.file > fence.value.entries.len) return error.InvalidVectorMigrationState;
        if (progress.file == fence.value.entries.len) {
            progress.copy_complete = true;
            try save(alloc, io, cursor_path, progress);
            break;
        }
        const entry = fence.value.entries[progress.file];
        if (std.fs.path.isAbsolute(entry.path) or std.mem.indexOf(u8, entry.path, "..") != null or progress.offset > entry.size)
            return error.InvalidVectorMigrationState;
        const from = try std.fs.path.join(alloc, &.{ live, entry.path });
        defer alloc.free(from);
        const to = try std.fs.path.join(alloc, &.{ staged.path(), entry.path });
        defer alloc.free(to);
        const count: usize = @intCast(@min(buffer.len, entry.size - progress.offset));
        try capacity(live, request.budget, count);
        var input = try std.Io.Dir.cwd().openFile(io, from, .{});
        defer input.close(io);
        if ((try input.stat(io)).size != entry.size) return error.SourceFileChanged;
        if (try input.readPositionalAll(io, buffer[0..count], progress.offset) != count) return error.SourceFileChanged;
        if (std.fs.path.dirname(to)) |parent| try fs.createDirPathPortable(io, parent);
        var output = try std.Io.Dir.cwd().createFile(io, to, .{ .read = true, .truncate = false });
        defer output.close(io);
        try output.writePositionalAll(io, buffer[0..count], progress.offset);
        if (progress.offset + count == entry.size) try output.setLength(io, entry.size);
        try output.sync(io);
        try fs.syncDirPortable(io, std.fs.path.dirname(to).?);
        try boundary(.chunk_synced);
        if (try output.readPositionalAll(io, verify[0..count], progress.offset) != count or
            !std.mem.eql(u8, buffer[0..count], verify[0..count])) return error.VectorMigrationCopyMismatch;
        progress.offset += count;
        progress.copied_bytes += count;
        if (progress.offset == entry.size) {
            progress.file += 1;
            progress.offset = 0;
        }
        try save(alloc, io, cursor_path, progress);
        try boundary(.cursor_synced);
        steps += 1;
    }
    var target_options = try plan.optionsForStagedGeneration(&staged);
    target_options.staged_generation = &staged;
    target_options.exclusive_generation = null;
    target_options.table_storage = null;
    target_options.open_mode = .writer_no_replay;
    target_options.start_index_workers = false;
    target_options.start_optional_runtimes = false;
    target_options.start_optional_runtime_workers = false;
    {
        var target = try db.DB.open(alloc, staged.path(), target_options);
        defer target.close();
        const identity = try std.json.Stringify.valueAlloc(alloc, target.core.identity_namespace, .{});
        defer alloc.free(identity);
        if (!std.mem.eql(u8, identity, fence.value.identity)) return error.VectorMigrationIdentityMismatch;
        try target.authorizeOfflineVectorMigrationCandidate(&staged);
        // Replay committed index work from the physical copy without requiring
        // an offline operator to instantiate external enrichment providers.
        try target.catchUpPendingDerivedReplay();
        try target.startVectorMigration(request);
        while (true) {
            const raw = (try target.vectorMigrationStatus(alloc)).?;
            defer alloc.free(raw);
            if (options.progress_fn) |callback| try callback(options.progress_ctx, raw);
            var job = try std.json.parseFromSlice(contract.Job, alloc, raw, .{});
            defer job.deinit();
            if (job.value.phase == .complete) break;
            if (options.max_steps != 0 and steps >= options.max_steps) return .pending;
            try capacity(live, request.budget, request.budget.batch_bytes * 4);
            if (job.value.phase == .ready) try target.publishVectorMigration(request.job_id) else try target.advanceVectorMigration(request.job_id);
            steps += 1;
        }
        try target.syncIndexes(true);
        try target.core.store.runtime_store.sync(true);
        try boundary(.candidate_complete);
    }
    try staged.seal();
    try boundary(.before_publication);
    const outcome = try staged.publish();
    try boundary(.after_publication);
    if (outcome == .durability_uncertain) return error.GenerationDurabilityUncertain;
    return .complete;
}

/// Cancel only an unpublished shadow. Reconciliation runs before interpreting
/// the live job, so an ambiguous exchange can never be mistaken for rollback.
pub fn cancel(alloc: Allocator, io: std.Io, root: []const u8, request: contract.Request, options: db.OpenOptions) !void {
    try request.validate();
    if (request.mode != .offline) return error.InvalidVectorMigrationState;
    var transition = try @import("db/generation_lifecycle.zig").beginProcessExclusiveWithRuntimeAndIo(root, options.backend_runtime, io);
    defer transition.deinit();
    try transition.reconcilePublished();
    var open = options;
    open.exclusive_generation = &transition;
    open.staged_generation = null;
    open.open_mode = .status_only;
    open.start_index_workers = false;
    open.start_optional_runtimes = false;
    {
        var source = try db.DB.open(alloc, transition.path, open);
        defer source.close();
        if (source.table_storage.dense_embeddings != .primary_lsm) return error.VectorMigrationAlreadyPublished;
        if (try source.vectorMigrationStatus(alloc)) |raw| {
            defer alloc.free(raw);
            var job = try std.json.parseFromSlice(contract.Job, alloc, raw, .{});
            defer job.deinit();
            if (job.value.active() or job.value.published()) return error.VectorMigrationAlreadyExists;
        }
    }
    const fence_path = try std.fs.path.join(alloc, &.{ transition.path, contract.offline_fence_file });
    defer alloc.free(fence_path);
    if (!try exists(io, fence_path)) return;
    var fence = try readJson(Fence, alloc, io, fence_path);
    defer fence.deinit();
    if (fence.value.version != 1 or !(contract.Admission{ .request = fence.value.request }).eql(.{ .request = request }))
        return error.VectorMigrationIdempotencyConflict;
    var staged = try transition.resumeStaging(request.job_id);
    defer staged.deinit();
    try std.Io.Dir.cwd().deleteTree(io, staged.path());
    try fs.syncDirPortable(io, std.fs.path.dirname(staged.path()).?);
    const cancelled_path = try std.fs.path.join(alloc, &.{ transition.path, cancellation_file });
    defer alloc.free(cancelled_path);
    // Keep a receipt before releasing admission. A lost cancellation response
    // must not let the same ID silently start a new physical migration.
    try save(alloc, io, cancelled_path, Cancellation{ .request = request, .identity = fence.value.identity });
    try std.Io.Dir.cwd().deleteFile(io, fence_path);
    try fs.syncDirPortable(io, transition.path);
}
