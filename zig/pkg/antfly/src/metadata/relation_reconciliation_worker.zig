// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! One bounded coordinator intent per control round. Durable job/retirement
//! cuts are the authority; local state only throttles and suppresses duplicate
//! in-flight proposals. This worker never adopts tracking or publishes roots.
const std = @import("std");
const r = @import("antfly_local_sources").system_catalog_relation_reconciliation;
const control = @import("relation_reconciliation_command.zig");

pub const Receipt = struct { term: u64, index: u64 };
pub const Leader = struct { term: u64, applied_index: u64 };
pub const round_interval_ns = 250 * std.time.ns_per_ms;

/// Select only from an observed committed cut. Stale generations are replaced
/// by CAS, including terminal failures; unchanged failed epochs stay stopped.
/// Alternate collection and forward work whenever both are available.
pub fn nextIntent(work: r.Work, group: u64, prefer_gc: bool) !?control.Command {
    const source_epoch = work.epoch orelse return null;
    if (work.current) |state| if (state.group_id != group) return error.InvalidCatalogRecord;
    if (work.garbage) |retired| if (retired.generation.group_id != group) return error.InvalidCatalogRecord;
    if (prefer_gc) if (work.garbage) |retired| return .{ .garbage = retired };
    if (work.current) |state| {
        if (!state.epoch.eql(source_epoch)) return .{ .start = .{
            .next = try r.State.init(group, try r.nextJobId(&state), source_epoch),
            .prior = state,
        } };
        if (state.failure == .none and state.phase != .ready) return .{ .advance = state };
    } else return .{ .start = .{ .next = try r.State.init(group, try r.nextJobId(null), source_epoch) } };
    return if (work.garbage) |retired| .{ .garbage = retired } else null;
}

pub const Worker = struct {
    lane: std.Io.Mutex = .init,
    next_round_at_ns: u64 = 0,
    pending: ?Receipt = null,
    prefer_gc: bool = false,

    /// Host supplies a local leader/applied cut, one pinned bounded work read,
    /// and capability-gated append in the exact captured term. No apply waits,
    /// Raft driving, sleeps or loops occur here. A restart may re-propose a cut;
    /// the durable CAS protocol makes that harmless.
    pub fn step(self: *Worker, host: anytype, group: u64, now_ns: u64) !bool {
        if (!self.lane.tryLock()) return false;
        defer self.lane.unlock(std.Options.debug_io);
        if (now_ns < self.next_round_at_ns) return false;
        self.next_round_at_ns = now_ns +| round_interval_ns;
        const leader: Leader = (try host.leader()) orelse {
            self.pending = null;
            return false;
        };
        if (leader.term == 0) return error.InvalidCatalogRecord;
        if (self.pending) |receipt| {
            if (receipt.term == leader.term and leader.applied_index < receipt.index) return false;
            // Applied is not proof that our intent won. A term change may
            // overwrite it; always observe the actual durable successor.
            self.pending = null;
        }
        const work: r.Work = try host.observe();
        const command = (try nextIntent(work, group, self.prefer_gc)) orelse return false;
        const receipt: Receipt = try host.propose(command, leader.term);
        if (receipt.term != leader.term or receipt.index == 0) return error.InvalidCatalogRecord;
        self.pending = receipt;
        self.prefer_gc = command != .garbage;
        return true;
    }
};

const Fake = struct {
    cut: ?Leader = .{ .term = 7, .applied_index = 0 },
    work: r.Work,
    reads: usize = 0,
    appends: usize = 0,
    last: ?control.Command = null,
    lose_term: bool = false,
    ambiguous: bool = false,
    pub fn leader(self: *@This()) !?Leader {
        return self.cut;
    }
    pub fn observe(self: *@This()) !r.Work {
        self.reads += 1;
        return self.work;
    }
    pub fn propose(self: *@This(), command: control.Command, term: u64) !Receipt {
        if (self.lose_term) self.cut.?.term += 1;
        if (self.cut == null or self.cut.?.term != term) return error.NotLeader;
        self.appends += 1;
        self.last = command;
        if (self.ambiguous) return error.MetadataMutationOutcomeUnknown;
        return .{ .term = term, .index = self.appends };
    }
};
const epoch: r.Epoch = .{ .incarnation = @splat(1), .revision = 1 };

test "relation reconciliation worker budgets pending work and alternates GC" {
    const prior = try r.State.init(41, try r.nextJobId(null), epoch);
    const current = try r.State.init(41, try r.nextJobId(&prior), epoch);
    var host: Fake = .{ .work = .{ .epoch = epoch, .current = current, .root = null, .garbage = r.Retirement.init(r.Generation.of(&prior)) } };
    var worker: Worker = .{};
    try std.testing.expect(try worker.step(&host, 41, 0));
    try std.testing.expect(host.last.? == .advance);
    try std.testing.expect(!try worker.step(&host, 41, 1));
    try std.testing.expect(!try worker.step(&host, 41, round_interval_ns));
    try std.testing.expectEqual(@as(usize, 1), host.reads);
    try std.testing.expectEqual(@as(usize, 1), host.appends);
    host.cut.?.applied_index = 1;
    try std.testing.expect(try worker.step(&host, 41, 2 * round_interval_ns));
    try std.testing.expect(host.last.? == .garbage);
    host.cut.?.applied_index = 2;
    try std.testing.expect(try worker.step(&host, 41, 3 * round_interval_ns));
    try std.testing.expect(host.last.? == .advance);
    try std.testing.expect(worker.lane.tryLock());
    try std.testing.expect(!try worker.step(&host, 41, 4 * round_interval_ns));
    worker.lane.unlock(std.Options.debug_io);
    try std.testing.expectEqual(@as(usize, 3), host.reads);
}

test "relation reconciliation worker retains terminal failures and replaces changed epochs" {
    var failed = try r.State.init(41, try r.nextJobId(null), epoch);
    failed.failure = .name_conflict;
    var work: r.Work = .{ .epoch = epoch, .current = failed, .root = null };
    try std.testing.expect(try nextIntent(work, 41, false) == null);
    work.epoch.?.revision += 1;
    const restart = (try nextIntent(work, 41, false)).?.start;
    try std.testing.expect(std.meta.eql(failed, restart.prior.?));
    try std.testing.expectEqual(r.FailureReason.none, restart.next.failure);
    try std.testing.expect(restart.next.epoch.eql(work.epoch.?));
    try std.testing.expectEqual(@as(u128, 2), std.mem.readInt(u128, &restart.next.job_id, .big));
    work.current = null;
    try std.testing.expect((try nextIntent(work, 41, false)).? == .start);
    work.epoch = null;
    try std.testing.expect(try nextIntent(work, 41, false) == null);
}

test "relation reconciliation worker reobserves after leadership loss restart and ambiguous append" {
    const current = try r.State.init(41, try r.nextJobId(null), epoch);
    var host: Fake = .{ .work = .{ .epoch = epoch, .current = current, .root = null } };
    var worker: Worker = .{};
    try std.testing.expect(try worker.step(&host, 41, 0));
    host.cut = null;
    try std.testing.expect(!try worker.step(&host, 41, round_interval_ns));
    try std.testing.expect(worker.pending == null);
    try std.testing.expectEqual(@as(usize, 1), host.reads);
    host.cut = .{ .term = 8, .applied_index = 0 };
    host.lose_term = true;
    try std.testing.expectError(error.NotLeader, worker.step(&host, 41, 2 * round_interval_ns));
    try std.testing.expectEqual(@as(usize, 1), host.appends);
    host.lose_term = false;
    host.ambiguous = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, worker.step(&host, 41, 3 * round_interval_ns));
    try std.testing.expect(worker.pending == null);
    // An acknowledged-lost append may already have progressed durably.
    host.work.current.?.phase = .verifying_source;
    host.ambiguous = false;
    worker = .{};
    try std.testing.expect(try worker.step(&host, 41, 0));
    try std.testing.expectEqual(r.Phase.verifying_source, host.last.?.advance.phase);
}

test "relation reconciliation worker discards term-local receipts without assuming the intent won" {
    var ready = try r.State.init(41, try r.nextJobId(null), epoch);
    ready.phase = .ready;
    var host: Fake = .{ .cut = .{ .term = 8, .applied_index = 0 }, .work = .{ .epoch = epoch, .current = ready, .root = null } };
    var worker: Worker = .{ .pending = .{ .term = 7, .index = 100 } };
    try std.testing.expect(!try worker.step(&host, 41, 0));
    try std.testing.expect(worker.pending == null);
    try std.testing.expectEqual(@as(usize, 1), host.reads);
    try std.testing.expectEqual(@as(usize, 0), host.appends);
}
