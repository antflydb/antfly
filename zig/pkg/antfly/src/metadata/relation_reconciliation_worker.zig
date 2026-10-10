// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! One bounded coordinator intent per control round. Durable job/retirement
//! cuts are the authority; local state only throttles and suppresses duplicate
//! in-flight proposals. Source tracking is adopted explicitly; verified ready
//! generations are published only after durable membership capability admission.
const std = @import("std");
const r = @import("antfly_local_sources").system_catalog_relation_reconciliation;
const control = @import("relation_reconciliation_command.zig");

pub const Receipt = struct { term: u64, index: u64 };
pub const Leader = struct { term: u64, applied_index: u64 };
pub const Preparation = union(enum) {
    idle,
    pending,
    /// One capability activation was appended, not waited for.
    activated: Receipt,
    /// Local proof preparation completed for this exact observed cut.
    publish: control.Publication,
};
pub const round_interval_ns = 250 * std.time.ns_per_ms;
pub const publication_snapshot_lifetime_ns = 60 * std.time.ns_per_s;

/// One owned bounded scan, serialized by its owner's lane. Evidence is local,
/// never publication authority: the eventual write must recheck its proof.
/// Scan is injected so resource ownership and scheduling are fault-testable.
pub fn PublicationPreparation(comptime Scan: type, comptime Proof: type) type {
    return struct {
        const Cut = struct { state: r.State, root: ?r.Generation, fence: u64 };
        cut: ?Cut = null,
        scan: ?*Scan = null,
        proof: ?Proof = null,
        expires_at_ns: u64 = 0,
        retry_after_ns: u64 = 0,
        stopped: bool = false,

        pub fn cancel(self: *@This()) void {
            self.closeScan();
            self.* = .{};
        }
        fn closeScan(self: *@This()) void {
            if (self.scan) |scan| scan.deinit();
            self.scan = null;
        }
        fn failed(self: *@This(), err: anyerror, now_ns: u64) void {
            self.closeScan();
            self.proof = null;
            // Do not repeatedly rescan/log a corrupt immutable cut. Resource
            // failures are retryable, but cannot create a tight retry loop.
            self.stopped = err == error.InvalidCatalogRecord;
            self.retry_after_ns = now_ns +| std.time.ns_per_s;
        }
        pub fn expire(self: *@This(), now_ns: u64) !void {
            if (self.scan != null and now_ns >= self.expires_at_ns) {
                self.scan.?.renew() catch |err| {
                    self.failed(err, now_ns);
                    return err;
                };
                self.expires_at_ns = now_ns +| publication_snapshot_lifetime_ns;
            }
        }
        /// true consumes this round's work budget; no GC append should race
        /// the in-progress proof. Changed cuts, failures and teardown discard
        /// scans; physical snapshot expiry renews only an unchanged cut.
        pub fn step(self: *@This(), host: anytype, work: r.Work, leader: Leader, now_ns: u64) !bool {
            return self.stepAtFence(host, work, leader.term, now_ns);
        }
        /// Leaders fence by term; replica-local preparation fences by the
        /// pinned applied-log position. Both also bind the exact source/root.
        pub fn stepAtFence(self: *@This(), host: anytype, work: r.Work, fence: u64, now_ns: u64) !bool {
            const state = work.current orelse {
                self.cancel();
                return false;
            };
            if (work.epoch == null or !state.epoch.eql(work.epoch.?) or state.phase != .ready or state.failure != .none) {
                self.cancel();
                return false;
            }
            const next: Cut = .{ .state = state, .root = work.root, .fence = fence };
            if (!std.meta.eql(self.cut, @as(?Cut, next))) {
                self.cancel();
                self.cut = next;
            }
            if (self.proof != null or self.stopped) return false;
            if (self.scan == null) {
                if (now_ns < self.retry_after_ns) return false;
                self.scan = host.beginPublicationScan(state) catch |err| {
                    self.failed(err, now_ns);
                    return err;
                };
                self.expires_at_ns = now_ns +| publication_snapshot_lifetime_ns;
            }
            try self.expire(now_ns);
            const proof = self.scan.?.step(null) catch |err| {
                self.failed(err, now_ns);
                return err;
            };
            if (proof) |value| {
                self.proof = value;
                self.closeScan();
                return false;
            }
            return true;
        }
    };
}

/// Replica-local bounded preparation, independent of leadership. Completed
/// evidence is fixed-size; expensive pinned scans have a separate, smaller
/// capacity. No active scan is evicted to admit another group. Call expire
/// from the owner's control cadence and deinit before closing its database.
pub fn PublicationProofPool(comptime Scan: type, comptime Proof: type, comptime max_slots: usize, comptime max_scans: usize) type {
    if (max_slots == 0 or max_scans == 0 or max_scans > max_slots) @compileError("invalid publication preparation limits");
    return struct {
        const ScanPreparation = PublicationPreparation(Scan, Proof);
        const Slot = struct { group: u64, used_at_ns: u64, preparation: ScanPreparation = .{} };
        lane: std.Io.Mutex = .init,
        slots: [max_slots]?Slot = @splat(null),
        closed: bool = false,

        pub fn deinit(self: *@This(), io: std.Io) void {
            self.lane.lockUncancelable(io);
            defer self.lane.unlock(io);
            for (&self.slots) |*slot| {
                if (slot.*) |*value| value.preparation.cancel();
                slot.* = null;
            }
            self.closed = true;
        }
        pub fn cancel(self: *@This(), io: std.Io, group: u64) !void {
            if (!self.lane.tryLock()) return error.ResourceTemporarilyUnavailable;
            defer self.lane.unlock(io);
            self.cancelGroup(group);
        }
        /// Teardown may wait for the current bounded page, unlike admission.
        pub fn cancelBlocking(self: *@This(), io: std.Io, group: u64) void {
            self.lane.lockUncancelable(io);
            defer self.lane.unlock(io);
            self.cancelGroup(group);
        }
        fn cancelGroup(self: *@This(), group: u64) void {
            for (&self.slots) |*slot| if (slot.*) |*value| if (value.group == group) {
                value.preparation.cancel();
                slot.* = null;
                return;
            };
        }
        fn expireIdle(self: *@This(), now_ns: u64) void {
            for (&self.slots) |*slot| if (slot.*) |*value| {
                if (value.preparation.scan != null and now_ns -| value.used_at_ns >= publication_snapshot_lifetime_ns) {
                    value.preparation.cancel();
                    slot.* = null;
                }
            };
        }
        pub fn expire(self: *@This(), io: std.Io, now_ns: u64) !void {
            if (!self.lane.tryLock()) return error.ResourceTemporarilyUnavailable;
            defer self.lane.unlock(io);
            if (self.closed) return error.CatalogPublicationScanClosed;
            self.expireIdle(now_ns);
            var failure: ?anyerror = null;
            for (&self.slots) |*slot| if (slot.*) |*value| {
                value.preparation.expire(now_ns) catch |err| {
                    failure = failure orelse err;
                };
            };
            if (failure) |err| return err;
        }
        pub fn step(self: *@This(), io: std.Io, host: anytype, work: r.Work, applied_index: u64, now_ns: u64) !?Proof {
            if (!self.lane.tryLock()) return error.ResourceTemporarilyUnavailable;
            defer self.lane.unlock(io);
            if (self.closed) return error.CatalogPublicationScanClosed;
            const state = work.current orelse return error.CatalogGenerationChanged;
            if (work.epoch == null or !state.epoch.eql(work.epoch.?) or state.phase != .ready or state.failure != .none) return error.CatalogGenerationChanged;
            if (work.root) |root| if (root.group_id != state.group_id) return error.InvalidCatalogRecord;
            self.expireIdle(now_ns);
            var selected: ?usize = null;
            var available: ?usize = null;
            var oldest: ?usize = null;
            for (&self.slots, 0..) |*slot, index| {
                if (slot.*) |*value| {
                    if (value.group == state.group_id) {
                        selected = index;
                        break;
                    }
                    if (value.preparation.scan == null and (oldest == null or value.used_at_ns < self.slots[oldest.?].?.used_at_ns)) oldest = index;
                } else available = index;
            }
            const index = selected orelse available orelse oldest orelse return error.ResourceTemporarilyUnavailable;
            if (selected == null) {
                if (self.slots[index]) |*old| old.preparation.cancel();
                self.slots[index] = .{ .group = state.group_id, .used_at_ns = now_ns };
            }
            const slot = &self.slots[index].?;
            if (slot.preparation.cut) |cut| if (cut.fence != applied_index or !std.meta.eql(cut.state, state) or !std.meta.eql(cut.root, work.root)) {
                slot.preparation.cancel();
            };
            slot.used_at_ns = now_ns;
            if (slot.preparation.scan == null and slot.preparation.proof == null and !slot.preparation.stopped and now_ns >= slot.preparation.retry_after_ns) {
                var active: usize = 0;
                for (self.slots) |other| if (other) |value| {
                    if (value.preparation.scan != null) active += 1;
                };
                if (active == max_scans) return error.ResourceTemporarilyUnavailable;
            }
            _ = try slot.preparation.stepAtFence(host, work, applied_index, now_ns);
            if (slot.preparation.stopped) return error.InvalidCatalogRecord;
            return slot.preparation.proof;
        }
    };
}

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
    closing: bool = false,

    /// Host supplies a local leader/applied cut, one pinned bounded work read,
    /// and capability-gated append in the exact captured term. No apply waits,
    /// Raft driving, sleeps or loops occur here. A restart may re-propose a cut;
    /// the durable CAS protocol makes that harmless.
    pub fn step(self: *Worker, host: anytype, group: u64, now_ns: u64) !bool {
        if (!self.lane.tryLock()) return false;
        defer self.lane.unlock(std.Options.debug_io);
        if (self.closing) return false;
        if (now_ns < self.next_round_at_ns) return false;
        self.next_round_at_ns = now_ns +| round_interval_ns;
        // Expiry must not depend on successful leader/status observation.
        // A contended runtime lane cannot retain an old snapshot indefinitely.
        try host.expirePublication(now_ns);
        const leader: Leader = (try host.leader()) orelse {
            self.pending = null;
            host.cancelPublication();
            return false;
        };
        if (leader.term == 0) {
            host.cancelPublication();
            return error.InvalidCatalogRecord;
        }
        if (self.pending) |receipt| {
            if (receipt.term == leader.term and leader.applied_index < receipt.index) return false;
            // Applied is not proof that our intent won. A term change may
            // overwrite it; always observe the actual durable successor.
            self.pending = null;
        }
        const work: r.Work = host.observe() catch |err| {
            host.cancelPublication();
            return err;
        };
        if (work.current) |state| if (state.group_id != group) return error.InvalidCatalogRecord;
        const preparation: Preparation = if (self.prefer_gc and work.garbage != null)
            .idle
        else
            try host.preparePublication(work, leader, now_ns);
        switch (preparation) {
            .pending => return false,
            .activated => |receipt| {
                if (receipt.term != leader.term or receipt.index == 0) return error.InvalidCatalogRecord;
                self.pending = receipt;
                return true;
            },
            else => {},
        }
        if (preparation == .publish) {
            const state = work.current orelse return error.InvalidCatalogRecord;
            const source = work.epoch orelse return error.InvalidCatalogRecord;
            if (!std.meta.eql(preparation.publish.state, state) or !state.epoch.eql(source) or
                !std.meta.eql(preparation.publish.prior, work.root)) return error.CatalogGenerationChanged;
        }
        const command: control.Command = if (preparation == .publish and !(self.prefer_gc and work.garbage != null))
            .{ .publish = preparation.publish }
        else
            (try nextIntent(work, group, self.prefer_gc)) orelse return false;
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
    leader_error: ?anyerror = null,
    cancellations: usize = 0,
    expirations: usize = 0,
    preparation: Preparation = .idle,
    pub fn cancelPublication(self: *@This()) void {
        self.cancellations += 1;
    }
    pub fn expirePublication(self: *@This(), _: u64) !void {
        self.expirations += 1;
    }
    pub fn preparePublication(self: *@This(), _: r.Work, _: Leader, _: u64) !Preparation {
        return self.preparation;
    }
    pub fn leader(self: *@This()) !?Leader {
        if (self.leader_error) |err| return err;
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

const ScanHost = struct {
    a: std.mem.Allocator = std.testing.allocator,
    pages: usize = 2,
    begins: usize = 0,
    steps: usize = 0,
    closes: usize = 0,
    renewals: usize = 0,
    begin_error: ?anyerror = null,
    step_error: ?anyerror = null,
    renew_error: ?anyerror = null,
    const Scan = struct {
        owner: *ScanHost,
        remaining: usize = 2,
        pub fn renew(self: *@This()) !void {
            self.owner.renewals += 1;
            if (self.owner.renew_error) |err| return err;
        }
        pub fn step(self: *@This(), _: ?*const std.atomic.Value(bool)) !?u64 {
            self.owner.steps += 1;
            if (self.owner.step_error) |err| return err;
            self.remaining -= 1;
            return if (self.remaining == 0) 42 else null;
        }
        pub fn deinit(self: *@This()) void {
            self.owner.closes += 1;
            self.owner.a.destroy(self);
        }
    };
    pub fn beginPublicationScan(self: *@This(), _: r.State) !*Scan {
        self.begins += 1;
        if (self.begin_error) |err| return err;
        const scan = try self.a.create(Scan);
        scan.* = .{ .owner = self, .remaining = self.pages };
        return scan;
    }
};
fn publicationWorkForTest() !r.Work {
    var ready = try r.State.init(41, try r.nextJobId(null), epoch);
    ready.phase = .ready;
    return .{ .epoch = epoch, .current = ready, .root = null };
}

test "relation reconciliation worker publication preparation yields closes and retains only completed evidence" {
    var host: ScanHost = .{};
    var preparation: PublicationPreparation(ScanHost.Scan, u64) = .{};
    defer preparation.cancel();
    const work = try publicationWorkForTest();
    const leader: Leader = .{ .term = 7, .applied_index = 1 };
    try std.testing.expect(try preparation.step(&host, work, leader, 0));
    try std.testing.expect(preparation.proof == null and preparation.scan != null);
    try std.testing.expectEqual(@as(usize, 1), host.steps);
    try std.testing.expect(!try preparation.step(&host, work, leader, round_interval_ns));
    try std.testing.expectEqual(@as(?u64, 42), preparation.proof);
    try std.testing.expect(preparation.scan == null);
    for (0..10) |round| try std.testing.expect(!try preparation.step(&host, work, leader, round * round_interval_ns));
    try std.testing.expectEqual(@as(usize, 1), host.begins);
    try std.testing.expectEqual(@as(usize, 2), host.steps);
    try std.testing.expectEqual(@as(usize, 1), host.closes);
}

test "relation reconciliation worker publication preparation discards changed terms roots and source epochs" {
    var host: ScanHost = .{};
    var preparation: PublicationPreparation(ScanHost.Scan, u64) = .{};
    defer preparation.cancel();
    var work = try publicationWorkForTest();
    var leader: Leader = .{ .term = 7, .applied_index = 1 };
    try std.testing.expect(try preparation.step(&host, work, leader, 0));
    leader.term += 1;
    try std.testing.expect(try preparation.step(&host, work, leader, round_interval_ns));
    try std.testing.expectEqual(@as(usize, 1), host.closes);
    work.root = r.Generation.of(&work.current.?);
    try std.testing.expect(try preparation.step(&host, work, leader, 2 * round_interval_ns));
    try std.testing.expectEqual(@as(usize, 2), host.closes);
    work.epoch.?.revision += 1;
    try std.testing.expect(!try preparation.step(&host, work, leader, 3 * round_interval_ns));
    try std.testing.expect(preparation.scan == null and preparation.proof == null and preparation.cut == null);
    try std.testing.expectEqual(@as(usize, 3), host.closes);
    work.current.?.epoch = work.epoch.?;
    try std.testing.expect(try preparation.step(&host, work, leader, 4 * round_interval_ns));
    preparation.cancel(); // Leadership loss/service teardown, idempotently.
    preparation.cancel();
    try std.testing.expectEqual(host.begins, host.closes);
}

test "relation reconciliation worker publication preparation bounds snapshot lifetime and retry failures" {
    var host: ScanHost = .{};
    var preparation: PublicationPreparation(ScanHost.Scan, u64) = .{};
    defer preparation.cancel();
    const work = try publicationWorkForTest();
    var leader: Leader = .{ .term = 7, .applied_index = 1 };
    try std.testing.expect(try preparation.step(&host, work, leader, 0));
    try std.testing.expect(!try preparation.step(&host, work, leader, publication_snapshot_lifetime_ns));
    try std.testing.expect(preparation.scan == null and preparation.proof == 42);
    try std.testing.expectEqual(@as(usize, 1), host.renewals);
    try std.testing.expectEqual(@as(usize, 1), host.begins);
    try std.testing.expectEqual(@as(usize, 1), host.closes);
    preparation.cancel();
    const retry = publication_snapshot_lifetime_ns + 1;
    host.begin_error = error.OutOfMemory;
    try std.testing.expectError(error.OutOfMemory, preparation.step(&host, work, leader, retry));
    host.begin_error = null;
    host.step_error = error.InvalidCatalogRecord;
    try std.testing.expectError(error.InvalidCatalogRecord, preparation.step(&host, work, leader, preparation.retry_after_ns));
    const attempts = host.begins;
    try std.testing.expect(!try preparation.step(&host, work, leader, preparation.retry_after_ns));
    try std.testing.expectEqual(attempts, host.begins);
    host.step_error = null;
    leader.term += 1;
    try std.testing.expect(try preparation.step(&host, work, leader, preparation.retry_after_ns));
    try std.testing.expect(preparation.scan != null and !preparation.stopped);
    for ([_]anyerror{ error.OutOfMemory, error.CatalogGenerationChanged }) |err| {
        host.renew_error = err;
        try std.testing.expectError(err, preparation.expire(preparation.expires_at_ns));
        try std.testing.expect(preparation.scan == null and preparation.proof == null);
        host.renew_error = null;
        try std.testing.expect(try preparation.step(&host, work, leader, preparation.retry_after_ns));
    }
}

fn poolWorkForTest(group: u64) !r.Work {
    var work = try publicationWorkForTest();
    work.current.?.group_id = group;
    return work;
}

test "relation reconciliation worker publication pool bounds active scans and evicts only inactive evidence" {
    const io = std.Options.debug_io;
    var host: ScanHost = .{};
    var pool: PublicationProofPool(ScanHost.Scan, u64, 3, 1) = .{};
    defer pool.deinit(io);
    const one = try poolWorkForTest(41);
    const two = try poolWorkForTest(42);
    const three = try poolWorkForTest(43);
    const four = try poolWorkForTest(44);
    try std.testing.expect((try pool.step(io, &host, one, 7, 0)) == null);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pool.step(io, &host, two, 7, 1));
    try std.testing.expectEqual(@as(usize, 1), host.steps);
    try std.testing.expectEqual(@as(?u64, 42), try pool.step(io, &host, one, 7, 2));
    try std.testing.expect((try pool.step(io, &host, two, 7, 3)) == null);
    try std.testing.expectEqual(@as(?u64, 42), try pool.step(io, &host, two, 7, 4));
    try std.testing.expect((try pool.step(io, &host, three, 7, 5)) == null);
    try std.testing.expectEqual(@as(?u64, 42), try pool.step(io, &host, three, 7, 6));
    try std.testing.expect((try pool.step(io, &host, four, 7, 7)) == null);
    const steps = host.steps;
    try std.testing.expectEqual(@as(?u64, 42), try pool.step(io, &host, two, 7, 8));
    try std.testing.expectEqual(steps, host.steps);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pool.step(io, &host, one, 7, 9));
    try std.testing.expectEqual(@as(usize, 4), host.begins);
    try std.testing.expectEqual(@as(?u64, 42), try pool.step(io, &host, four, 7, 10));
    try std.testing.expect((try pool.step(io, &host, one, 7, 11)) == null);
    try std.testing.expectEqual(@as(usize, 5), host.begins);
    pool.deinit(io);
    try std.testing.expectEqual(host.begins, host.closes);
    try std.testing.expectError(error.CatalogPublicationScanClosed, pool.step(io, &host, one, 7, 12));
}

test "relation reconciliation worker publication pool fences cached cuts without oversubscribing changed proofs" {
    const io = std.Options.debug_io;
    var host: ScanHost = .{};
    var pool: PublicationProofPool(ScanHost.Scan, u64, 3, 1) = .{};
    defer pool.deinit(io);
    var one = try poolWorkForTest(41);
    const two = try poolWorkForTest(42);
    try std.testing.expect((try pool.step(io, &host, one, 7, 0)) == null);
    try std.testing.expect((try pool.step(io, &host, one, 8, 1)) == null);
    try std.testing.expectEqual(@as(usize, 1), host.closes);
    one.root = r.Generation.of(&one.current.?);
    try std.testing.expect((try pool.step(io, &host, one, 8, 2)) == null);
    try std.testing.expectEqual(@as(usize, 2), host.closes);
    one.epoch.?.revision += 1;
    one.current.?.epoch = one.epoch.?;
    try std.testing.expect((try pool.step(io, &host, one, 8, 3)) == null);
    try std.testing.expectEqual(@as(usize, 3), host.closes);
    try std.testing.expectEqual(@as(?u64, 42), try pool.step(io, &host, one, 8, 4));
    try std.testing.expect((try pool.step(io, &host, two, 8, 5)) == null);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pool.step(io, &host, one, 9, 6));
    try std.testing.expectEqual(@as(usize, 5), host.begins);
    try pool.cancel(io, two.current.?.group_id);
    try std.testing.expect((try pool.step(io, &host, one, 9, 7)) == null);
    try std.testing.expectEqual(@as(usize, 6), host.begins);
}

test "relation reconciliation worker publication pool expires idle snapshots renews busy scans and bounds failures" {
    const io = std.Options.debug_io;
    var host: ScanHost = .{ .pages = 4 };
    var pool: PublicationProofPool(ScanHost.Scan, u64, 2, 1) = .{};
    defer pool.deinit(io);
    const work = try poolWorkForTest(41);
    try std.testing.expect((try pool.step(io, &host, work, 7, 0)) == null);
    try std.testing.expect((try pool.step(io, &host, work, 7, publication_snapshot_lifetime_ns - 1)) == null);
    try pool.expire(io, publication_snapshot_lifetime_ns);
    try std.testing.expectEqual(@as(usize, 1), host.renewals);
    try std.testing.expectEqual(@as(usize, 1), host.begins);
    try pool.expire(io, 2 * publication_snapshot_lifetime_ns);
    try std.testing.expectEqual(@as(usize, 1), host.closes);
    host.begin_error = error.OutOfMemory;
    const now = 3 * publication_snapshot_lifetime_ns;
    try std.testing.expectError(error.OutOfMemory, pool.step(io, &host, work, 7, now));
    const begins = host.begins;
    host.begin_error = null;
    try std.testing.expect((try pool.step(io, &host, work, 7, now + 1)) == null);
    try std.testing.expectEqual(begins, host.begins);
    host.step_error = error.InvalidCatalogRecord;
    try std.testing.expectError(error.InvalidCatalogRecord, pool.step(io, &host, work, 7, now + std.time.ns_per_s));
    const failed_begins = host.begins;
    try std.testing.expectError(error.InvalidCatalogRecord, pool.step(io, &host, work, 7, now + 2 * std.time.ns_per_s));
    try std.testing.expectEqual(failed_begins, host.begins);
    host.step_error = null;
    try std.testing.expect((try pool.step(io, &host, work, 8, now + 3 * std.time.ns_per_s)) == null);
    try std.testing.expect(pool.lane.tryLock());
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pool.step(io, &host, work, 8, now));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pool.cancel(io, work.current.?.group_id));
    pool.lane.unlock(io);
}

test "relation reconciliation worker publication pool releases preparation on every allocation failure" {
    const Fault = struct {
        fn run(a: std.mem.Allocator) !void {
            const io = std.Options.debug_io;
            var host: ScanHost = .{ .a = a };
            var pool: PublicationProofPool(ScanHost.Scan, u64, 2, 1) = .{};
            defer pool.deinit(io);
            const work = try poolWorkForTest(41);
            try std.testing.expect((try pool.step(io, &host, work, 7, 0)) == null);
            try std.testing.expectEqual(@as(?u64, 42), try pool.step(io, &host, work, 7, 1));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fault.run, .{});
}

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

test "relation reconciliation worker expires publication work before unavailable leader observations" {
    var host: Fake = .{ .work = try publicationWorkForTest(), .leader_error = error.ResourceTemporarilyUnavailable };
    var worker: Worker = .{};
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, worker.step(&host, 41, 0));
    try std.testing.expectEqual(@as(usize, 1), host.expirations);
    host.leader_error = null;
    host.cut = null;
    try std.testing.expect(!try worker.step(&host, 41, round_interval_ns));
    try std.testing.expectEqual(@as(usize, 1), host.cancellations);
    host.cut = .{ .term = 0, .applied_index = 0 };
    try std.testing.expectError(error.InvalidCatalogRecord, worker.step(&host, 41, 2 * round_interval_ns));
    try std.testing.expectEqual(@as(usize, 2), host.cancellations);
    try std.testing.expectEqual(@as(usize, 3), host.expirations);
    worker.closing = true;
    try std.testing.expect(!try worker.step(&host, 41, 3 * round_interval_ns));
    try std.testing.expectEqual(@as(usize, 3), host.expirations);
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

test "relation reconciliation worker separates activation proof and publication without apply waits" {
    var host: Fake = .{ .work = try publicationWorkForTest(), .preparation = .{ .activated = .{ .term = 7, .index = 9 } } };
    var worker: Worker = .{};
    try std.testing.expect(try worker.step(&host, 41, 0));
    try std.testing.expectEqual(@as(usize, 0), host.appends);
    try std.testing.expectEqual(@as(u64, 9), worker.pending.?.index);
    try std.testing.expect(!try worker.step(&host, 41, round_interval_ns));
    try std.testing.expectEqual(@as(usize, 1), host.reads);
    host.cut.?.applied_index = 9;
    host.preparation = .pending;
    try std.testing.expect(!try worker.step(&host, 41, 2 * round_interval_ns));
    try std.testing.expect(worker.pending == null);
    host.preparation = .{ .publish = .{
        .state = host.work.current.?,
        .activation = .{ .version = @import("topology_protocol.zig").relation_publication_version, .incarnation = "01010101010101010101010101010101".*, .member_count = 1, .membership_fingerprint = @splat(3) },
    } };
    try std.testing.expect(try worker.step(&host, 41, 3 * round_interval_ns));
    try std.testing.expect(host.last.? == .publish);
    try std.testing.expectEqual(@as(usize, 1), host.appends);
    const encoded = try host.last.?.encodeAlloc(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.meta.eql(host.last.?, try control.Command.decode(encoded)));
}

test "relation reconciliation worker rejects changed publication cuts and preserves GC fairness" {
    const prior = try r.State.init(41, try r.nextJobId(null), epoch);
    var ready = try r.State.init(41, try r.nextJobId(&prior), epoch);
    ready.phase = .ready;
    var host: Fake = .{ .work = .{ .epoch = epoch, .current = ready, .root = null, .garbage = r.Retirement.init(r.Generation.of(&prior)) }, .preparation = .pending };
    var worker: Worker = .{ .prefer_gc = true };
    try std.testing.expect(try worker.step(&host, 41, 0));
    try std.testing.expect(host.last.? == .garbage);
    host.cut.?.applied_index = 1;
    try std.testing.expect(!try worker.step(&host, 41, round_interval_ns));
    host.preparation = .{ .activated = .{ .term = 8, .index = 2 } };
    try std.testing.expectError(error.InvalidCatalogRecord, worker.step(&host, 41, 2 * round_interval_ns));
    host.preparation = .{ .publish = .{
        .state = prior,
        .activation = .{ .version = @import("topology_protocol.zig").relation_publication_version, .incarnation = "01010101010101010101010101010101".*, .member_count = 1, .membership_fingerprint = @splat(3) },
    } };
    try std.testing.expectError(error.CatalogGenerationChanged, worker.step(&host, 41, 3 * round_interval_ns));
    host.preparation.publish.state = ready;
    host.preparation.publish.prior = r.Generation.of(&prior);
    try std.testing.expectError(error.CatalogGenerationChanged, worker.step(&host, 41, 4 * round_interval_ns));
    try std.testing.expectEqual(@as(usize, 1), host.appends);
}
