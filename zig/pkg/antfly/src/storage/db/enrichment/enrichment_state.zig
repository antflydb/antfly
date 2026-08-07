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
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const docstore_mod = @import("../../docstore.zig");
const lsm_backend = @import("../../lsm_backend.zig");
const mem_backend = @import("../../mem_backend.zig");
const projection_checkpoint_mod = @import("../derived/apply_state.zig");

const applied_seq_prefix = "\x00\x00__metadata__:enrichment_applied:";
const runtime_status_prefix = "\x00\x00__metadata__:enrichment_status:";
const checkpoint_magic = "AFENRCP1";
const checkpoint_format_version: u32 = 1;

pub const ProjectionStatus = projection_checkpoint_mod.ProjectionStatus;
pub const ProjectionCheckpoint = projection_checkpoint_mod.ProjectionCheckpoint;

pub fn projectionStatusName(status: ProjectionStatus) []const u8 {
    return switch (status) {
        .clean => "clean",
        .rebuilding => "rebuilding",
        .degraded => "degraded",
        .repair_required => "repair_required",
    };
}

pub const RuntimeStatus = struct {
    target_sequence: u64 = 0,
    error_count: u64 = 0,
    retryable_error_count: u64 = 0,
    fatal_error_count: u64 = 0,
    skipped_source_count: u64 = 0,
    consecutive_retry_count: u32 = 0,
    next_retry_at_ms: u64 = 0,
    /// Stable identity of the request or compatible batch that owns the
    /// consecutive retry budget. Zero is reserved for pipeline/infrastructure
    /// failures that cannot safely be attributed to one source request.
    retry_failure_fingerprint: u64 = 0,
    retrying: bool = false,
    worker_failed: bool = false,
};

const runtime_status_v1_len = 34;
const runtime_status_v2_len = 42;
const runtime_status_v3_len = 54;
const runtime_status_v4_len = 62;

fn appliedSequenceKey(alloc: Allocator, scope: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ applied_seq_prefix, scope });
}

fn runtimeStatusKey(alloc: Allocator, scope: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ runtime_status_prefix, scope });
}

pub fn loadAppliedSequence(alloc: Allocator, store: anytype, scope: []const u8) !u64 {
    return (try loadProjectionCheckpoint(alloc, store, scope)).applied_sequence;
}

pub fn loadProjectionCheckpoint(alloc: Allocator, store: anytype, scope: []const u8) !ProjectionCheckpoint {
    const key = try appliedSequenceKey(alloc, scope);
    defer alloc.free(key);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{},
        else => return err,
    };
    const raw = try alloc.dupe(u8, borrowed);
    defer alloc.free(raw);
    return try decodeProjectionCheckpoint(raw);
}

pub fn saveAppliedSequence(store: anytype, scope: []const u8, sequence: u64) !void {
    return try saveProjectionCheckpoint(store, scope, .{ .applied_sequence = sequence });
}

pub fn saveProjectionCheckpoint(store: anytype, scope: []const u8, checkpoint: ProjectionCheckpoint) !void {
    const key = try appliedSequenceKey(std.heap.page_allocator, scope);
    defer std.heap.page_allocator.free(key);
    var buf = encodeProjectionCheckpoint(checkpoint);
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, &buf);
    try txn.commit();
}

fn decodeProjectionCheckpoint(raw: []const u8) !ProjectionCheckpoint {
    if (raw.len != checkpoint_magic.len + 4 + 8 + 1 + 8 + 8) return error.InvalidEnrichmentState;
    if (!std.mem.eql(u8, raw[0..checkpoint_magic.len], checkpoint_magic)) return error.InvalidEnrichmentState;
    var pos: usize = checkpoint_magic.len;
    const format_version = readCheckpointInt(raw, &pos, u32);
    if (format_version != checkpoint_format_version) return error.InvalidEnrichmentState;
    const applied_sequence = readCheckpointInt(raw, &pos, u64);
    const status_raw = readCheckpointInt(raw, &pos, u8);
    const generation = readCheckpointInt(raw, &pos, u64);
    const config_hash = readCheckpointInt(raw, &pos, u64);
    const status: ProjectionStatus = switch (status_raw) {
        @intFromEnum(ProjectionStatus.clean) => .clean,
        @intFromEnum(ProjectionStatus.rebuilding) => .rebuilding,
        @intFromEnum(ProjectionStatus.degraded) => .degraded,
        @intFromEnum(ProjectionStatus.repair_required) => .repair_required,
        else => return error.InvalidEnrichmentState,
    };
    return .{
        .applied_sequence = applied_sequence,
        .status = status,
        .generation = generation,
        .config_hash = config_hash,
    };
}

fn encodeProjectionCheckpoint(checkpoint: ProjectionCheckpoint) [37]u8 {
    var out: [checkpoint_magic.len + 4 + 8 + 1 + 8 + 8]u8 = undefined;
    @memcpy(out[0..checkpoint_magic.len], checkpoint_magic);
    var pos: usize = checkpoint_magic.len;
    writeCheckpointInt(&out, &pos, u32, checkpoint_format_version);
    writeCheckpointInt(&out, &pos, u64, checkpoint.applied_sequence);
    writeCheckpointInt(&out, &pos, u8, @intFromEnum(checkpoint.status));
    writeCheckpointInt(&out, &pos, u64, checkpoint.generation);
    writeCheckpointInt(&out, &pos, u64, checkpoint.config_hash);
    return out;
}

fn readCheckpointInt(raw: []const u8, pos: *usize, comptime T: type) T {
    const size = @sizeOf(T);
    const out = std.mem.readInt(T, raw[pos.* .. pos.* + size][0..size], .little);
    pos.* += size;
    return out;
}

fn writeCheckpointInt(out: []u8, pos: *usize, comptime T: type, value: T) void {
    const size = @sizeOf(T);
    std.mem.writeInt(T, out[pos.* .. pos.* + size][0..size], value, .little);
    pos.* += size;
}

pub fn loadRuntimeStatus(alloc: Allocator, store: anytype, scope: []const u8) !RuntimeStatus {
    const key = try runtimeStatusKey(alloc, scope);
    defer alloc.free(key);

    var runtime = try initRuntimeStore(alloc, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginProbe();
    defer txn.abort();
    const borrowed = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .{},
        else => return err,
    };
    const raw = try alloc.dupe(u8, borrowed);
    defer alloc.free(raw);
    if (raw.len != runtime_status_v1_len and raw.len != runtime_status_v2_len and raw.len != runtime_status_v3_len and raw.len != runtime_status_v4_len) return error.InvalidEnrichmentState;
    var status = RuntimeStatus{
        .target_sequence = std.mem.readInt(u64, raw[0..8], .little),
        .error_count = std.mem.readInt(u64, raw[8..16], .little),
        .retryable_error_count = std.mem.readInt(u64, raw[16..24], .little),
        .fatal_error_count = std.mem.readInt(u64, raw[24..32], .little),
        .retrying = raw[raw.len - 2] != 0,
        .worker_failed = raw[raw.len - 1] != 0,
    };
    if (raw.len >= runtime_status_v2_len) {
        status.skipped_source_count = std.mem.readInt(u64, raw[32..40], .little);
    }
    if (raw.len == runtime_status_v3_len or raw.len == runtime_status_v4_len) {
        status.consecutive_retry_count = std.mem.readInt(u32, raw[40..44], .little);
        status.next_retry_at_ms = std.mem.readInt(u64, raw[44..52], .little);
    }
    if (raw.len == runtime_status_v4_len) {
        status.retry_failure_fingerprint = std.mem.readInt(u64, raw[52..60], .little);
    }
    return status;
}

pub fn saveRuntimeStatus(store: anytype, scope: []const u8, status: RuntimeStatus) !void {
    const key = try runtimeStatusKey(std.heap.page_allocator, scope);
    defer std.heap.page_allocator.free(key);
    var buf: [runtime_status_v4_len]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], status.target_sequence, .little);
    std.mem.writeInt(u64, buf[8..16], status.error_count, .little);
    std.mem.writeInt(u64, buf[16..24], status.retryable_error_count, .little);
    std.mem.writeInt(u64, buf[24..32], status.fatal_error_count, .little);
    std.mem.writeInt(u64, buf[32..40], status.skipped_source_count, .little);
    std.mem.writeInt(u32, buf[40..44], status.consecutive_retry_count, .little);
    std.mem.writeInt(u64, buf[44..52], status.next_retry_at_ms, .little);
    std.mem.writeInt(u64, buf[52..60], status.retry_failure_fingerprint, .little);
    buf[60] = if (status.retrying) 1 else 0;
    buf[61] = if (status.worker_failed) 1 else 0;
    var runtime = try initRuntimeStore(std.heap.page_allocator, store);
    defer runtime.deinit();
    var txn = try runtime.store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, &buf);
    try txn.commit();
}

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = false };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

test "enrichment apply state works with memory backend store" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
    try saveAppliedSequence(runtime, "chunks", 19);
    try std.testing.expectEqual(@as(u64, 19), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
}

test "enrichment apply state works with lsm backend store" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try std.testing.expectEqual(@as(u64, 0), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
    try saveAppliedSequence(runtime, "chunks", 23);
    try std.testing.expectEqual(@as(u64, 23), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
}

test "enrichment projection checkpoint persists status and identity fields" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 2 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveProjectionCheckpoint(runtime, "generated", .{
        .applied_sequence = 41,
        .status = .degraded,
        .generation = 5,
        .config_hash = 0x309,
    });

    const checkpoint = try loadProjectionCheckpoint(std.testing.allocator, runtime, "generated");
    try std.testing.expectEqual(@as(u64, 41), checkpoint.applied_sequence);
    try std.testing.expectEqual(ProjectionStatus.degraded, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 5), checkpoint.generation);
    try std.testing.expectEqual(@as(u64, 0x309), checkpoint.config_hash);
    try std.testing.expectEqual(@as(u64, 41), try loadAppliedSequence(std.testing.allocator, runtime, "generated"));
}

test "enrichment projection checkpoint rejects malformed structured state" {
    try std.testing.expectError(error.InvalidEnrichmentState, decodeProjectionCheckpoint("short"));

    var encoded = encodeProjectionCheckpoint(.{ .applied_sequence = 7 });
    encoded[0] = 'X';
    try std.testing.expectError(error.InvalidEnrichmentState, decodeProjectionCheckpoint(&encoded));

    encoded = encodeProjectionCheckpoint(.{ .applied_sequence = 7 });
    encoded[checkpoint_magic.len] = 2;
    try std.testing.expectError(error.InvalidEnrichmentState, decodeProjectionCheckpoint(&encoded));
}

test "enrichment state lsm point loads do not clone mutable snapshot" {
    var backend = lsm_backend.Backend.init(std.testing.allocator, .{ .flush_threshold = 1024 });
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveAppliedSequence(runtime, "chunks", 29);
    try saveRuntimeStatus(runtime, "generated", .{
        .target_sequence = 31,
        .error_count = 1,
        .retryable_error_count = 2,
        .fatal_error_count = 3,
        .skipped_source_count = 4,
        .consecutive_retry_count = 5,
        .next_retry_at_ms = 12_345,
        .retry_failure_fingerprint = 98_765,
        .retrying = true,
    });

    const before = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(@as(u64, 29), try loadAppliedSequence(std.testing.allocator, runtime, "chunks"));
    const status = try loadRuntimeStatus(std.testing.allocator, runtime, "generated");
    try std.testing.expectEqual(@as(u64, 31), status.target_sequence);
    try std.testing.expectEqual(@as(u64, 4), status.skipped_source_count);
    try std.testing.expectEqual(@as(u64, 98_765), status.retry_failure_fingerprint);
    try std.testing.expect(status.retrying);
    const after = backend.snapshotMaintenanceStats();
    try std.testing.expectEqual(before.mutable_snapshot_clone_calls, after.mutable_snapshot_clone_calls);
}

test "enrichment runtime status persists source target sequence" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer runtime.deinit();

    try saveRuntimeStatus(runtime, "generated", .{
        .target_sequence = 7,
        .error_count = 1,
        .retryable_error_count = 2,
        .fatal_error_count = 3,
        .skipped_source_count = 4,
        .consecutive_retry_count = 5,
        .next_retry_at_ms = 12_345,
        .retry_failure_fingerprint = 98_765,
        .retrying = true,
    });
    const loaded = try loadRuntimeStatus(std.testing.allocator, runtime, "generated");
    try std.testing.expectEqual(@as(u64, 7), loaded.target_sequence);
    try std.testing.expectEqual(@as(u64, 1), loaded.error_count);
    try std.testing.expectEqual(@as(u64, 2), loaded.retryable_error_count);
    try std.testing.expectEqual(@as(u64, 3), loaded.fatal_error_count);
    try std.testing.expectEqual(@as(u64, 4), loaded.skipped_source_count);
    try std.testing.expectEqual(@as(u32, 5), loaded.consecutive_retry_count);
    try std.testing.expectEqual(@as(u64, 12_345), loaded.next_retry_at_ms);
    try std.testing.expectEqual(@as(u64, 98_765), loaded.retry_failure_fingerprint);
    try std.testing.expect(loaded.retrying);
    try std.testing.expect(!loaded.worker_failed);
}

test "enrichment runtime status reads the legacy retry format" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();

    var store = try backend.runtimeStore(alloc, .{ .name = "docs" });
    defer store.deinit();
    const key = try runtimeStatusKey(alloc, "generated");
    defer alloc.free(key);
    var legacy: [runtime_status_v3_len]u8 = @splat(0);
    std.mem.writeInt(u64, legacy[0..8], 17, .little);
    std.mem.writeInt(u32, legacy[40..44], 3, .little);
    std.mem.writeInt(u64, legacy[44..52], 42_000, .little);
    legacy[52] = 1;
    var txn = try store.beginWrite();
    errdefer txn.abort();
    try txn.put(key, &legacy);
    try txn.commit();

    const loaded = try loadRuntimeStatus(alloc, store, "generated");
    try std.testing.expectEqual(@as(u64, 17), loaded.target_sequence);
    try std.testing.expectEqual(@as(u32, 3), loaded.consecutive_retry_count);
    try std.testing.expectEqual(@as(u64, 42_000), loaded.next_retry_at_ms);
    try std.testing.expectEqual(@as(u64, 0), loaded.retry_failure_fingerprint);
    try std.testing.expect(loaded.retrying);
}
