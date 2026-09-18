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

//! Shared document/typed-row, crash-resumable common-cut capture protocol.
//! Storage codecs and integrity validation differ; lifecycle authority does not.
//! A cohort is not a
//! timestamp: all source owners must reject new work and drain old decisions
//! before the first durable handle is sealed. All handles are journaled before
//! source fences are released; corpus export happens only after writes resume.
//!
//! This is an internal coordinator primitive, not a public backup option. Its
//! driver must durably persist the complete plan before calling step(), use CAS
//! for checkpoints/publication, and adopt (never overwrite) captured artifacts
//! after a lost reply. Public admission must additionally pin the dependency
//! closure and prevent concurrent metadata changes for this job's lifetime.
const std = @import("std");
const topology = @import("../storage/db/relational_integrity_topology.zig");

pub const Phase = enum { freezing, draining, capturing, releasing, exporting, publishing, reclaiming, completed, cancelling, cancel_reclaiming, cancelled };

pub fn locksTables(phase: Phase) bool {
    return switch (phase) {
        .reclaiming, .completed, .cancel_reclaiming, .cancelled => false,
        else => true,
    };
}
pub const Result = enum { advanced, waiting, completed, cancelled };

pub const Owner = struct {
    table_name: []const u8,
    /// Logical range start. Empty must remain empty until after owner routing.
    range_start: []const u8,
    range_end: []const u8,
    fence: topology.Fence,
    /// Preallocated immutable artifact identity, persisted before any capture.
    artifact_id: []const u8,
    /// Replica-local pins must never be exported from a replacement leader.
    capture_node_id: u64 = 0,
};

pub const SealReceipt = struct {
    handle: @import("../storage/db/native_backup_seal.zig").Handle,
    source_node_id: u64,
    pub fn jsonStringify(self: SealReceipt, writer: anytype) @TypeOf(writer.*).Error!void {
        try @import("../storage/db/relational_integrity_json.zig").write(self, writer);
    }
};

pub const State = struct {
    format_version: u32 = 1,
    phase: Phase = .freezing,
    cursor: usize = 0,
    /// Binds ordered owners, schemas, and dependencies from one metadata view.
    metadata_digest: [32]u8,
    owners: []const Owner,

    pub fn validate(self: State) !void {
        if (self.format_version != 1 or self.owners.len == 0 or self.owners.len > 4096 or self.cursor >= self.owners.len)
            return error.InvalidBackupCohort;
        if ((self.phase == .publishing or self.phase == .completed or self.phase == .cancelled) and self.cursor != 0)
            return error.InvalidBackupCohort;
        const first = self.owners[0].fence;
        for (self.owners, 0..) |owner, i| {
            if (owner.table_name.len == 0 or owner.artifact_id.len == 0 or owner.fence.role != .backup_snapshot or
                owner.fence.transition_id != first.transition_id or owner.fence.attempt != first.attempt or
                owner.fence.owner_group_id != owner.fence.peer_group_id)
                return error.InvalidBackupCohort;
            _ = try owner.fence.encode();
            if (owner.range_end.len != 0 and std.mem.order(u8, owner.range_start, owner.range_end) != .lt)
                return error.InvalidBackupCohort;
            // The coordinator persists canonical table/key order. Validation
            // is streaming and O(owners), with no quadratic duplicate search.
            if (i == 0 or !std.mem.eql(u8, self.owners[i - 1].table_name, owner.table_name)) {
                if (owner.range_start.len != 0) return error.IncompleteBackupCohort;
                if (i != 0 and (self.owners[i - 1].range_end.len != 0 or
                    std.mem.order(u8, self.owners[i - 1].table_name, owner.table_name) != .lt))
                    return error.IncompleteBackupCohort;
            } else {
                const previous = self.owners[i - 1];
                if (previous.range_end.len == 0 or !std.mem.eql(u8, previous.range_end, owner.range_start) or
                    previous.fence.namespace.table_id != owner.fence.namespace.table_id or
                    !std.mem.eql(u8, &previous.fence.catalog_digest, &owner.fence.catalog_digest))
                    return error.IncompleteBackupCohort;
            }
        }
        if (self.owners[self.owners.len - 1].range_end.len != 0) return error.IncompleteBackupCohort;
    }
};

pub const Observation = struct { fence: ?topology.Fence, drained: bool };

pub const TableProof = struct {
    table_id: u64,
    name: []const u8,
    definition: [32]u8,
    manifest_definition: ?[32]u8 = null,
};

/// Bind precisely the schema/configuration carried by a table artifact. The
/// full metadata fingerprint additionally covers placement/lifecycle fields
/// which intentionally are not restored as source generation identities.
pub fn manifestDefinition(name: []const u8, description: []const u8, schema: []const u8, read_schema: []const u8, indexes: []const u8, replication_sources: []const u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly cohort table manifest definition v1");
    for ([_][]const u8{ name, description, schema, read_schema, indexes, replication_sources }) |field| {
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, field.len, .little);
        hash.update(&size);
        hash.update(field);
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}

/// Metadata Raft owns this journal. Source table locks and initial admission
/// are one transaction; completion removes locks only after all native owner
/// receipts were durably observed. This is common-cut authority for an existing
/// backup attempt, not a second public backup/restore scheduler.
pub const Job = struct {
    id: u64,
    revision: u64,
    backup_id: []const u8,
    attempt_id: []const u8 = "",
    artifact_format: enum { native, portable } = .native,
    location: []const u8,
    connection: []const u8,
    tables: []const TableProof,
    state: State,
    manifest_sha256: ?[32]u8 = null,
    /// Merged from small owner receipt records on recovery; not part of the
    /// immutable admission plan or rewritten by ordinary progress checkpoints.
    seals: []const SealReceipt = &.{},

    pub const max_encoded_bytes = 2 * 1024 * 1024;

    pub fn jsonStringify(self: Job, writer: anytype) @TypeOf(writer.*).Error!void {
        try @import("../storage/db/relational_integrity_json.zig").write(self, writer);
    }

    pub fn validate(self: Job) !void {
        try self.state.validate();
        if (self.id == 0 or self.revision == 0 or self.backup_id.len == 0 or self.backup_id.len > 128 or
            self.location.len == 0 or self.location.len > 4096 or self.connection.len == 0 or self.connection.len > 4096 or
            self.tables.len == 0 or self.tables.len > 4096 or self.state.owners[0].fence.transition_id != self.id)
            return error.InvalidBackupCohort;
        var owner_index: usize = 0;
        for (self.tables, 0..) |table, i| {
            if (table.table_id == 0 or table.name.len == 0 or table.name.len > 4096 or
                (i != 0 and std.mem.order(u8, self.tables[i - 1].name, table.name) != .lt)) return error.InvalidBackupCohort;
            const start = owner_index;
            while (owner_index < self.state.owners.len and std.mem.eql(u8, self.state.owners[owner_index].table_name, table.name)) : (owner_index += 1) {
                if (self.state.owners[owner_index].fence.namespace.table_id != table.table_id) return error.InvalidBackupCohort;
            }
            if (owner_index == start) return error.IncompleteBackupCohort;
        }
        if (owner_index != self.state.owners.len) return error.IncompleteBackupCohort;
        if ((self.state.phase == .reclaiming or self.state.phase == .completed) and self.manifest_sha256 == null)
            return error.InvalidBackupCohort;
        if ((self.state.phase == .cancelling or self.state.phase == .cancel_reclaiming or self.state.phase == .cancelled) and self.manifest_sha256 != null)
            return error.InvalidBackupCohort;
    }

    pub fn terminal(self: Job) bool {
        return self.state.phase == .completed or self.state.phase == .cancelled;
    }

    pub fn planDigest(self: Job, alloc: std.mem.Allocator) ![32]u8 {
        var immutable = self;
        immutable.revision = 1;
        immutable.state.phase = .freezing;
        immutable.state.cursor = 0;
        immutable.manifest_sha256 = null;
        immutable.seals = &.{};
        const encoded = try std.json.Stringify.valueAlloc(alloc, immutable, .{});
        defer alloc.free(encoded);
        if (encoded.len > max_encoded_bytes) return error.BackupCohortTooLarge;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(encoded, &digest, .{});
        return digest;
    }
};

/// Fixed-size Raft checkpoint. The immutable plan is stored exactly once.
pub const Progress = struct {
    revision: u64,
    phase: Phase,
    cursor: usize,
    owner_count: u32,
    plan_digest: [32]u8,
    manifest_sha256: ?[32]u8 = null,

    pub fn fromJob(job: Job, alloc: std.mem.Allocator) !Progress {
        return .{ .revision = job.revision, .phase = job.state.phase, .cursor = job.state.cursor, .owner_count = @intCast(job.state.owners.len), .plan_digest = try job.planDigest(alloc), .manifest_sha256 = job.manifest_sha256 };
    }

    pub fn applyTo(self: Progress, job: *Job) !void {
        if (self.owner_count != job.state.owners.len or self.cursor >= self.owner_count) return error.InvalidBackupCohort;
        job.revision = self.revision;
        job.state.phase = self.phase;
        job.state.cursor = self.cursor;
        job.manifest_sha256 = self.manifest_sha256;
    }

    pub fn terminal(self: Progress) bool {
        return self.phase == .completed or self.phase == .cancelled;
    }

    pub fn validateNext(old: Progress, replacement: Progress) !void {
        if (old.terminal() or old.revision == std.math.maxInt(u64) or replacement.revision != old.revision + 1 or
            replacement.owner_count != old.owner_count or replacement.owner_count == 0 or replacement.cursor >= replacement.owner_count or
            !std.mem.eql(u8, &replacement.plan_digest, &old.plan_digest)) return error.BackupCohortChanged;
        if ((replacement.phase == .reclaiming or replacement.phase == .completed) and replacement.manifest_sha256 == null)
            return error.BackupCohortChanged;
        if ((replacement.phase == .cancelling or replacement.phase == .cancel_reclaiming or replacement.phase == .cancelled) and replacement.manifest_sha256 != null)
            return error.BackupCohortChanged;
        if (old.manifest_sha256) |digest| if (replacement.manifest_sha256 == null or !std.mem.eql(u8, &digest, &replacement.manifest_sha256.?))
            return error.BackupCohortChanged;
        if (replacement.phase == .cancelling and replacement.cursor == 0 and
            (old.phase == .freezing or old.phase == .draining or old.phase == .capturing or old.phase == .releasing or old.phase == .exporting or old.phase == .publishing)) return;
        const next_phase: Phase = switch (old.phase) {
            .freezing => .draining,
            .draining => .capturing,
            .capturing => .releasing,
            .releasing => .exporting,
            .exporting => .publishing,
            .publishing => .reclaiming,
            .reclaiming => .completed,
            .cancelling => .cancel_reclaiming,
            .cancel_reclaiming => .cancelled,
            .completed, .cancelled => return error.BackupCohortChanged,
        };
        if (old.phase == .publishing) {
            if (replacement.phase != .reclaiming or replacement.cursor != 0) return error.BackupCohortChanged;
        } else if (old.cursor + 1 == old.owner_count) {
            if (replacement.phase != next_phase or replacement.cursor != 0) return error.BackupCohortChanged;
        } else if (replacement.phase != old.phase or replacement.cursor != old.cursor + 1) return error.BackupCohortChanged;
    }
};

pub fn validateReplacement(alloc: std.mem.Allocator, previous: ?Job, replacement: Job) !void {
    try replacement.validate();
    const old = previous orelse {
        if (replacement.revision != 1 or replacement.state.phase != .freezing or replacement.state.cursor != 0 or replacement.manifest_sha256 != null)
            return error.BackupCohortChanged;
        return;
    };
    if (old.terminal() or old.revision == std.math.maxInt(u64) or replacement.revision != old.revision + 1 or
        !std.mem.eql(u8, &(try old.planDigest(alloc)), &(try replacement.planDigest(alloc))))
        return error.BackupCohortChanged;
    if (old.manifest_sha256) |digest| if (replacement.manifest_sha256 == null or !std.mem.eql(u8, &digest, &replacement.manifest_sha256.?))
        return error.BackupCohortChanged;
    const a = old.state;
    const b = replacement.state;
    if (b.phase == .cancelling and b.cursor == 0 and
        (a.phase == .freezing or a.phase == .draining or a.phase == .capturing or a.phase == .releasing or a.phase == .exporting or a.phase == .publishing)) return;
    const next_phase: Phase = switch (a.phase) {
        .freezing => .draining,
        .draining => .capturing,
        .capturing => .releasing,
        .releasing => .exporting,
        .exporting => .publishing,
        .publishing => .reclaiming,
        .reclaiming => .completed,
        .cancelling => .cancel_reclaiming,
        .cancel_reclaiming => .cancelled,
        .completed, .cancelled => return error.BackupCohortChanged,
    };
    if (a.phase == .publishing) {
        if (b.phase != .reclaiming or b.cursor != 0 or replacement.manifest_sha256 == null) return error.BackupCohortChanged;
    } else if (a.cursor + 1 == a.owners.len) {
        if (b.phase != next_phase or b.cursor != 0) return error.BackupCohortChanged;
    } else if (b.phase != a.phase or b.cursor != a.cursor + 1) return error.BackupCohortChanged;
}

fn checkpoint(state: *State, driver: anytype, phase: Phase, cursor: usize) !void {
    var replacement = state.*;
    replacement.phase = phase;
    replacement.cursor = cursor;
    // Do not update memory first: persistence failure must not let a subsequent
    // call skip an action whose checkpoint was never durable.
    try driver.checkpoint(state.*, replacement);
    state.* = replacement;
}

fn next(state: *State, driver: anytype, phase: Phase) !void {
    if (state.cursor + 1 == state.owners.len) {
        try checkpoint(state, driver, phase, 0);
    } else try checkpoint(state, driver, state.phase, state.cursor + 1);
}

fn frozen(driver: anytype, owner: Owner) !bool {
    const status: Observation = try driver.observe(owner);
    const actual = status.fence orelse return error.BackupCohortFenceLost;
    if (!actual.eql(owner.fence)) return error.BackupCohortFenceLost;
    return status.drained;
}

/// The driver's decision atomically fences manifest publication and persists
/// either reclaiming (publication already committed) or cancelling (it cannot
/// subsequently commit). A read-then-write implementation is not sufficient.
/// An ambiguous result leaves this job unchanged and retains all owner fences.
pub fn cancel(state: *State, driver: anytype) !void {
    if (state.phase == .completed or state.phase == .reclaiming) return error.BackupCohortAlreadyCommitted;
    if (state.phase == .cancelled or state.phase == .cancelling or state.phase == .cancel_reclaiming) return;
    const published = try driver.decideCancellation(state.*);
    state.phase = if (published) .reclaiming else .cancelling;
    state.cursor = 0;
}

/// At most one owner-side action and one durable checkpoint per invocation.
/// Waiting never sleeps: the scheduler yields and retries with bounded backoff.
/// The complete plan must be validate()d once at admission or recovery. Driver
/// methods: checkpoint(expected,replacement), control(owner,command),
/// observe(owner), capture(owner), exportOwner(owner), releaseSeal(owner),
/// cancelSeal(owner), publish(state). capture journals a restart-stable sealed
/// handle before returning; it atomically checks the expected drained fence.
/// the earlier coordinator observation is NOT its snapshot authorization.
pub fn step(state: *State, driver: anytype) !Result {
    // Do not rewalk every owner on every scheduling slice (quadratic total
    // work). Immutable plan coverage is validated at admission/recovery.
    if (state.owners.len == 0 or state.cursor >= state.owners.len or state.format_version != 1)
        return error.InvalidBackupCohort;
    switch (state.phase) {
        .freezing => {
            const owner = state.owners[state.cursor];
            try driver.control(owner, .{ .fence = owner.fence, .action = .begin });
            try next(state, driver, .draining);
        },
        .draining => {
            if (!try frozen(driver, state.owners[state.cursor])) return .waiting;
            try next(state, driver, .capturing);
        },
        .capturing => {
            const owner = state.owners[state.cursor];
            if (!try frozen(driver, owner)) return .waiting;
            try driver.capture(owner);
            try next(state, driver, .releasing);
        },
        .exporting => {
            try driver.exportOwner(state.owners[state.cursor]);
            try next(state, driver, .publishing);
        },
        .publishing => {
            // Driver commits the immutable cohort plus every artifact digest
            // conditional on this exact durable job state. A lost reply retries
            // the same publication, never releases an unconfirmed cohort.
            try driver.publish(state.*);
            try checkpoint(state, driver, .reclaiming, 0);
        },
        .releasing => {
            const owner = state.owners[state.cursor];
            try driver.control(owner, .{ .fence = owner.fence, .action = .release });
            try next(state, driver, .exporting);
        },
        .reclaiming => {
            try driver.releaseSeal(state.owners[state.cursor]);
            try next(state, driver, .completed);
        },
        .completed => return .completed,
        .cancelling => {
            // Cancel every planned owner, not just acknowledged ones. A lost
            // begin reply is resolved by consuming its preallocated admission
            // epoch, preventing delayed delivery from resurrecting a fence.
            const owner = state.owners[state.cursor];
            try driver.control(owner, .{ .fence = owner.fence, .action = .cancel });
            try next(state, driver, .cancel_reclaiming);
        },
        .cancel_reclaiming => {
            // The capture replica may be unavailable after failover. Pin GC
            // cannot hold live owner write fences or metadata DDL hostage.
            try driver.cancelSeal(state.owners[state.cursor]);
            try next(state, driver, .cancelled);
        },
        .cancelled => return .cancelled,
    }
    return switch (state.phase) {
        .completed => .completed,
        .cancelled => .cancelled,
        else => .advanced,
    };
}

pub const consumer_tests = consumerTests();
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const test_owner_root = @import("antfly_source_root");
    if (@hasDecl(test_owner_root, "implementation_tests_only") and test_owner_root.implementation_tests_only) return struct {};
    const Suite = struct {
        fn testOwner(group: u64, start: []const u8, end: []const u8) Owner {
            return .{
                .table_name = "rows",
                .range_start = start,
                .range_end = end,
                .fence = .{
                    .transition_id = 700,
                    .admission_epoch = 1,
                    .attempt = 1,
                    .owner_group_id = group,
                    .peer_group_id = group,
                    .role = .backup_snapshot,
                    .namespace = .{ .table_id = 9, .shard_id = group, .range_id = group },
                    .catalog_digest = @splat(19),
                },
                .artifact_id = if (group == 301) "cohort-owner-301" else "cohort-owner-302",
            };
        }

        const TestDriver = struct {
            fences: [2]?topology.Fence = .{ null, null },
            drained: bool = false,
            captures: usize = 0,
            exports: usize = 0,
            pins: [2]bool = .{ false, false },
            published: bool = false,
            checkpoints: usize = 0,
            fail_checkpoint: bool = false,
            lose_publication_reply: bool = false,
            cancelled: bool = false,
            cancellations: usize = 0,
            pin_unavailable: bool = false,

            fn checkpoint(self: *@This(), _: State, _: State) !void {
                if (self.fail_checkpoint) return error.PersistenceUnavailable;
                self.checkpoints += 1;
            }
            fn control(self: *@This(), owner: Owner, command: topology.Command) !void {
                const index = owner.fence.owner_group_id - 301;
                switch (command.action) {
                    .begin => self.fences[index] = command.fence,
                    .release => {
                        try std.testing.expect(self.pins[0] and self.pins[1]);
                        self.fences[index] = null;
                    },
                    .cancel => {
                        try std.testing.expect(self.cancelled and !self.published);
                        self.fences[index] = null;
                        self.cancellations += 1;
                    },
                    else => return error.TestUnexpectedResult,
                }
            }
            fn observe(self: *@This(), owner: Owner) !Observation {
                return .{ .fence = self.fences[owner.fence.owner_group_id - 301], .drained = self.drained };
            }
            fn capture(self: *@This(), owner: Owner) !void {
                try std.testing.expect(self.drained and self.fences[0] != null and self.fences[1] != null);
                self.pins[owner.fence.owner_group_id - 301] = true;
                self.captures += 1;
            }
            fn exportOwner(self: *@This(), owner: Owner) !void {
                try std.testing.expect(self.fences[0] == null and self.fences[1] == null);
                try std.testing.expect(self.pins[owner.fence.owner_group_id - 301]);
                self.exports += 1;
            }
            fn releaseSeal(self: *@This(), owner: Owner) !void {
                try std.testing.expect(self.published);
                self.pins[owner.fence.owner_group_id - 301] = false;
            }
            fn cancelSeal(self: *@This(), owner: Owner) !void {
                try std.testing.expect(self.cancelled and !self.published);
                if (self.pin_unavailable) return error.BackupPinSourceUnavailable;
                self.pins[owner.fence.owner_group_id - 301] = false;
            }
            fn publish(self: *@This(), _: State) !void {
                if (self.cancelled) return error.BackupCohortCancelled;
                try std.testing.expectEqual(@as(usize, 2), self.captures);
                try std.testing.expectEqual(@as(usize, 2), self.exports);
                try std.testing.expect(self.fences[0] == null and self.fences[1] == null);
                self.published = true;
                if (self.lose_publication_reply) return error.ConnectionLost;
            }
            fn decideCancellation(self: *@This(), _: State) !bool {
                if (!self.published) self.cancelled = true;
                return self.published;
            }
        };

        test "relational backup cohort cancellation releases every write fence before unavailable pin cleanup" {
            const owners = [_]Owner{ testOwner(301, "", "k"), testOwner(302, "k", "") };
            var state: State = .{ .metadata_digest = @splat(3), .owners = &owners };
            var driver: TestDriver = .{};
            _ = try step(&state, &driver);
            _ = try step(&state, &driver);
            try cancel(&state, &driver);
            driver.pin_unavailable = true;
            _ = try step(&state, &driver);
            _ = try step(&state, &driver);
            try std.testing.expectEqual(Phase.cancel_reclaiming, state.phase);
            try std.testing.expect(driver.fences[0] == null and driver.fences[1] == null);
            try std.testing.expect(!locksTables(state.phase));
            try std.testing.expectError(error.BackupPinSourceUnavailable, step(&state, &driver));
            try std.testing.expectEqual(@as(usize, 0), state.cursor);
            driver.pin_unavailable = false;
            while (try step(&state, &driver) != .cancelled) {}
        }

        test "relational backup cohort freezes every owner and drains before capture" {
            const owners = [_]Owner{ testOwner(301, "", "\x00"), testOwner(302, "\x00", "") };
            var state: State = .{ .metadata_digest = @splat(3), .owners = &owners };
            var driver: TestDriver = .{};
            try std.testing.expectEqual(.advanced, try step(&state, &driver));
            try std.testing.expectEqual(.advanced, try step(&state, &driver));
            try std.testing.expectEqual(.draining, state.phase);
            try std.testing.expectEqual(.waiting, try step(&state, &driver));
            try std.testing.expectEqual(@as(usize, 0), driver.captures);
            driver.drained = true;
            while (try step(&state, &driver) != .completed) {}
            try std.testing.expect(driver.published);
            try std.testing.expect(driver.fences[0] == null and driver.fences[1] == null);
        }

        test "relational backup cohort retains sealed handles after ambiguous publication" {
            const owners = [_]Owner{ testOwner(301, "", "k"), testOwner(302, "k", "") };
            var state: State = .{ .metadata_digest = @splat(3), .owners = &owners };
            var driver: TestDriver = .{ .drained = true, .lose_publication_reply = true };
            while (state.phase != .publishing) _ = try step(&state, &driver);
            try std.testing.expectError(error.ConnectionLost, step(&state, &driver));
            try std.testing.expectEqual(.publishing, state.phase);
            try std.testing.expect(driver.pins[0] and driver.pins[1]);
            try std.testing.expect(driver.fences[0] == null and driver.fences[1] == null);
            driver.lose_publication_reply = false;
            while (try step(&state, &driver) != .completed) {}
        }

        test "relational backup cohort checkpoint failure cannot skip owner freeze" {
            const owners = [_]Owner{ testOwner(301, "", "k"), testOwner(302, "k", "") };
            var state: State = .{ .metadata_digest = @splat(3), .owners = &owners };
            var driver: TestDriver = .{ .fail_checkpoint = true };
            try std.testing.expectError(error.PersistenceUnavailable, step(&state, &driver));
            try std.testing.expectEqual(@as(usize, 0), state.cursor);
            try std.testing.expect(driver.fences[0] != null);
            driver.fail_checkpoint = false;
            _ = try step(&state, &driver);
            try std.testing.expectEqual(@as(usize, 1), state.cursor);
        }

        test "relational backup cohort refuses gaps overlaps and mismatched catalogs" {
            var owners = [_]Owner{ testOwner(301, "", "k"), testOwner(302, "l", "") };
            const state: State = .{ .metadata_digest = @splat(3), .owners = &owners };
            try std.testing.expectError(error.IncompleteBackupCohort, state.validate());
            owners[1].range_start = "j";
            try std.testing.expectError(error.IncompleteBackupCohort, state.validate());
            owners[1].range_start = "k";
            owners[1].fence.catalog_digest[0] ^= 1;
            try std.testing.expectError(error.IncompleteBackupCohort, state.validate());
        }

        test "relational backup cohort cancellation tombstones unacknowledged owners" {
            const owners = [_]Owner{ testOwner(301, "", "k"), testOwner(302, "k", "") };
            var state: State = .{ .metadata_digest = @splat(3), .owners = &owners };
            var driver: TestDriver = .{ .fail_checkpoint = true };
            try std.testing.expectError(error.PersistenceUnavailable, step(&state, &driver));
            // Owner 301 applied begin, but its acknowledgement was not checkpointed.
            // Owner 302 never observed begin; both must consume the planned epoch.
            try cancel(&state, &driver);
            driver.fail_checkpoint = false;
            while (try step(&state, &driver) != .cancelled) {}
            try std.testing.expectEqual(@as(usize, 2), driver.cancellations);
            try std.testing.expect(driver.fences[0] == null and driver.fences[1] == null);
        }

        test "relational backup cohort cancellation adopts an ambiguous committed manifest" {
            const owners = [_]Owner{ testOwner(301, "", "k"), testOwner(302, "k", "") };
            var state: State = .{ .metadata_digest = @splat(3), .owners = &owners };
            var driver: TestDriver = .{ .drained = true, .lose_publication_reply = true };
            while (state.phase != .publishing) _ = try step(&state, &driver);
            try std.testing.expectError(error.ConnectionLost, step(&state, &driver));
            try cancel(&state, &driver);
            try std.testing.expectEqual(.reclaiming, state.phase);
            while (try step(&state, &driver) != .completed) {}
            try std.testing.expectEqual(@as(usize, 0), driver.cancellations);
        }
    };
    return Suite;
}
comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}
