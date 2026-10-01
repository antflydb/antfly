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

//! Durable source-ownership migration contract, independent of ANN format.
const std = @import("std");

pub const format_version: u32 = 1;
pub const offline_fence_file = "VECTOR-MIGRATION-OFFLINE.json";
pub const job_key = "\x00\x00__metadata__:vector_migration";
pub const accounting_key = "\x00\x00__metadata__:vector_migration_bytes";
pub const candidate_prefix = "\x00\x00__metadata__:vector_migration_candidate:";
pub const Mode = enum { offline, online };
pub const Phase = enum { backfill, verifying, ready, draining, final_verification, serving, cleanup, reclaiming, complete, cancelling, cancelled };

/// Shared by all API handlers using one standalone catalog owner. Hold a table
/// slot from before reading admission through the DB command and catalog
/// reconciliation. The catalog's process lock excludes another server/offline
/// operator; after process loss the persisted marker is the recovery authority.
pub const CommandAdmissions = struct {
    mutex: std.atomic.Mutex = .unlocked,
    tables: std.StringHashMapUnmanaged(void) = .empty,

    pub fn begin(self: *CommandAdmissions, alloc: std.mem.Allocator, table: []const u8) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.tables.contains(table)) return error.StorageBusy;
        const owned = try alloc.dupe(u8, table);
        errdefer alloc.free(owned);
        try self.tables.put(alloc, owned, {});
    }

    pub fn end(self: *CommandAdmissions, alloc: std.mem.Allocator, table: []const u8) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        const removed = self.tables.fetchRemove(table) orelse unreachable;
        alloc.free(removed.key);
    }

    pub fn deinit(self: *CommandAdmissions, alloc: std.mem.Allocator) void {
        std.debug.assert(self.tables.count() == 0);
        self.tables.deinit(alloc);
    }
};

pub const Budget = struct {
    batch_bytes: u64 = 4 * 1024 * 1024,
    batch_rows: u32 = 1024,
    temporary_bytes: u64 = 64 * 1024 * 1024 * 1024,
    disk_reserve_bytes: u64 = 1024 * 1024 * 1024,

    pub fn validate(self: Budget) !void {
        if (self.batch_rows == 0 or self.batch_rows > 65536 or
            self.batch_bytes < 4096 or self.batch_bytes > 64 * 1024 * 1024 or
            self.temporary_bytes < self.batch_bytes)
            return error.InvalidVectorMigrationBudget;
    }
};

pub const Request = struct {
    job_id: []const u8,
    mode: Mode,
    budget: Budget = .{},

    pub fn validate(self: Request) !void {
        if (self.job_id.len == 0 or self.job_id.len > 128) return error.InvalidVectorMigrationId;
        for (self.job_id) |byte| {
            if (!std.ascii.isAlphanumeric(byte) and byte != '-' and byte != '_')
                return error.InvalidVectorMigrationId;
        }
        try self.budget.validate();
    }
};

/// Catalog admission survives a lost response before the DB job exists. The
/// marker blocks incompatible definition/topology changes until the DB's
/// durable publication or cancellation has been reconciled.
pub const Admission = struct {
    request: Request,

    pub fn eql(a: Admission, b: Admission) bool {
        return std.mem.eql(u8, a.request.job_id, b.request.job_id) and
            a.request.mode == b.request.mode and std.meta.eql(a.request.budget, b.request.budget);
    }
};

pub fn admissionsEqual(a: ?Admission, b: ?Admission) bool {
    if (a) |left| return if (b) |right| left.eql(right) else false;
    return b == null;
}

pub const Job = struct {
    version: u32 = format_version,
    source: @import("table_storage.zig").DenseEmbeddings = .primary_lsm,
    target: @import("table_storage.zig").DenseEmbeddings = .vector_store,
    job_id: []const u8,
    mode: Mode,
    phase: Phase = .backfill,
    budget: Budget,
    /// Full persisted document namespace, not a process-local table name.
    table_identity: []const u8,
    configuration_hash: u64,
    ownership_epoch: u64,
    snapshot_fence: u64,
    replay_cursor: u64,
    publication_fence: ?u64 = null,
    /// Hex-encoded exclusive primary key; empty at a phase boundary.
    cursor: []const u8 = "",
    scanned_rows: u64 = 0,
    prepared_artifacts: u64 = 0,
    prepared_bytes: u64 = 0,
    charged_temporary_bytes: u64 = 0,
    verified_artifacts: u64 = 0,
    rewritten_artifacts: u64 = 0,
    primary_reclamation_requested: bool = false,
    last_error: ?[]const u8 = null,

    pub fn published(self: Job) bool {
        return self.phase == .draining or self.phase == .final_verification or self.phase == .serving or self.phase == .cleanup or self.phase == .reclaiming or self.phase == .complete;
    }

    pub fn active(self: Job) bool {
        return self.phase != .complete and self.phase != .cancelled;
    }

    pub fn captures(self: Job) bool {
        return self.phase == .backfill or self.phase == .verifying or self.phase == .ready;
    }

    pub fn validate(self: Job) !void {
        if (self.version != 1) return error.UnsupportedVectorMigrationVersion;
        if (self.source != .primary_lsm or self.target != .vector_store) return error.UnsupportedVectorMigrationDirection;
        try (Request{ .job_id = self.job_id, .mode = self.mode, .budget = self.budget }).validate();
        if (self.table_identity.len == 0 or self.ownership_epoch == 0 or self.replay_cursor < self.snapshot_fence)
            return error.InvalidVectorMigrationState;
        if (self.published() != (self.publication_fence != null)) return error.InvalidVectorMigrationState;
        if (self.publication_fence) |fence| if (fence < self.snapshot_fence or fence > self.replay_cursor) return error.InvalidVectorMigrationState;
    }
};

pub fn candidateKeyAlloc(alloc: std.mem.Allocator, artifact_key: []const u8) ![]u8 {
    return std.mem.concat(alloc, u8, &.{ candidate_prefix, artifact_key });
}

/// Each step is bounded and durable; the operator may resume with the same ID.
pub const Action = enum { start, step, publish, cancel, status };
pub const Command = struct {
    action: Action,
    request: Request,
};

/// Public job creation is distinct from the internal replay command. Execution
/// mode is selected by transport; the HTTP API admits online jobs only.
pub const CreateRequest = struct {
    job_id: []const u8,
    target: enum { vector_store },
    budget: Budget = .{},
};
pub const JobCommand = struct { action: enum { step, publish, cancel } };

test "source vector migration validates durable identity and publication fencing" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidVectorMigrationId, (Request{ .job_id = "../other", .mode = .offline }).validate());
    try std.testing.expectError(error.InvalidVectorMigrationBudget, (Budget{ .batch_rows = 0 }).validate());
    var job: Job = .{ .job_id = "job", .mode = .online, .budget = .{}, .table_identity = "identity", .configuration_hash = 42, .ownership_epoch = 1, .snapshot_fence = 2, .replay_cursor = 3 };
    try job.validate();
    job.phase = .draining;
    try std.testing.expectError(error.InvalidVectorMigrationState, job.validate());
    job.publication_fence = 1;
    try std.testing.expectError(error.InvalidVectorMigrationState, job.validate());
    job.publication_fence = 3;
    try job.validate();
    const encoded = try std.json.Stringify.valueAlloc(alloc, job, .{});
    defer alloc.free(encoded);
    var reopened = try std.json.parseFromSlice(Job, alloc, encoded, .{});
    defer reopened.deinit();
    try reopened.value.validate();
    try std.testing.expect(reopened.value.published());
    try std.testing.expectEqual(job.publication_fence, reopened.value.publication_fence);
}

test "source vector migration command admission isolates tables and releases slots" {
    var commands: CommandAdmissions = .{};
    defer commands.deinit(std.testing.allocator);
    try commands.begin(std.testing.allocator, "A");
    try std.testing.expectError(error.StorageBusy, commands.begin(std.testing.allocator, "A"));
    try commands.begin(std.testing.allocator, "B");
    commands.end(std.testing.allocator, "A");
    try commands.begin(std.testing.allocator, "A");
    commands.end(std.testing.allocator, "B");
    commands.end(std.testing.allocator, "A");
}
