// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded exact cohort-set verification. Small cohorts stay in fixed scratch;
//! large cohorts use the shared private-run sorter, not a cohort-sized hash map.
const std = @import("std");
const spill = @import("../spill_sort.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
pub const Digest = [32]u8;
pub const enlisted: u8 = 1;
pub const legacy_resolved: u8 = 2;
pub const original: u8 = 4;
pub const prefix_enlisted: u8 = 8;
pub const prefix_resolved: u8 = 16;
pub const seen_enlisted: u8 = 32;
pub const seen_resolved: u8 = 64;
pub const indexed_resolved: u8 = 128;
pub const Counts = struct {
    enlisted: u64 = 0,
    resolved: u64 = 0,
    prefix_enlisted: u64 = 0,
    prefix_resolved: u64 = 0,
    prefix_extra_resolved: u64 = 0,
};
pub const Proof = struct {
    alloc: std.mem.Allocator,
    options: spill.Options,
    records: [256][33]u8 = undefined,
    slots: [512]u16 = @splat(0),
    len: usize = 0,
    sort: ?*spill.FixedSorter(33) = null,
    spill_copied: usize = 0,
    finish_started: bool = false,
    grouping: bool = false,
    cursor: ?spill.Cursor = null,
    small_offset: usize = 0,
    counts: Counts = .{},
    previous: ?Digest = null,
    flags: u8 = 0,
    complete: bool = false,
    invalid: bool = false,
    last_grouped_records: usize = 0,

    pub fn init(alloc: std.mem.Allocator, options: spill.Options) Proof {
        return .{ .alloc = alloc, .options = options };
    }
    /// Retained jobs borrow execution control per turn, never from a request
    /// that may have returned. Directory/io still belong to the owning root.
    pub fn initRetained(alloc: std.mem.Allocator, options: spill.Options) Proof {
        var retained_options = options;
        retained_options.cancellation = null;
        return init(alloc, retained_options);
    }
    pub fn deinit(self: *Proof) void {
        if (self.cursor) |*cursor| cursor.deinit();
        if (self.sort) |sort| {
            sort.deinit();
            self.alloc.destroy(sort);
        }
        self.* = undefined;
    }
    fn check(self: *const Proof) !void {
        if (self.options.cancellation) |token| try token.check();
    }
    fn spillError(self: *const Proof, err: anyerror) anyerror {
        if (err == error.Cancelled) {
            self.check() catch |cause| return cause;
            return error.Canceled;
        }
        return err;
    }
    pub fn add(self: *Proof, name: []const u8, flags: u8) !void {
        if (name.len == 0) return error.InvalidTxnRecord;
        var digest: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(name, &digest, .{});
        try self.addDigest(digest, flags);
    }
    pub fn addDigest(self: *Proof, digest: Digest, flags: u8) !void {
        const control = self.options.cancellation orelse Cancellation.none;
        while (!(self.tryAddDigest(digest, flags, control) catch |err| return self.spillError(err)))
            _ = self.work(.{}, control) catch |err| return self.spillError(err);
    }
    /// False means backpressure, not acceptance. The producer advances only
    /// after true; all input and accumulated flags remain owned by this job.
    pub fn tryAddDigest(self: *Proof, digest: Digest, flags: u8, control: anytype) !bool {
        try control.check();
        if (self.invalid or self.finish_started or flags == 0) return error.InvalidTxnRecord;
        errdefer self.invalid = true;
        if (self.sort != null and !try self.copyCache(control)) return false;
        var record: [33]u8 = undefined;
        @memcpy(record[0..32], &digest);
        record[32] = flags;
        var slot: usize = @intCast(std.hash.Wyhash.hash(0, &digest) & (self.slots.len - 1));
        while (self.slots[slot] != 0) : (slot = (slot + 1) & (self.slots.len - 1)) {
            const previous = &self.records[self.slots[slot] - 1];
            if (!std.mem.eql(u8, previous[0..32], &digest)) continue;
            const extra = flags & ~previous[32];
            if (extra == 0) return true;
            if (self.sort) |sort| {
                record[32] = extra;
                if (!try sort.tryAdd(record, control)) return false;
            }
            // Update deduplication only AFTER the incremental sink accepts.
            previous[32] |= extra;
            return true;
        }
        if (self.sort == null and self.len == self.records.len) {
            const sort = try self.alloc.create(spill.FixedSorter(33));
            sort.* = spill.FixedSorter(33).init(self.alloc, self.options) catch |err| {
                self.alloc.destroy(sort);
                return err;
            };
            self.sort = sort;
            if (!try self.copyCache(control)) return false;
        }
        if (self.sort) |sort| return sort.tryAdd(record, control);
        self.records[self.len] = record;
        self.slots[slot] = @intCast(self.len + 1);
        self.len += 1;
        return true;
    }
    fn copyCache(self: *Proof, control: anytype) !bool {
        var copied: usize = 0;
        while (self.spill_copied < self.len and copied < 16) : (copied += 1) {
            if (!try self.sort.?.tryAdd(self.records[self.spill_copied], control)) return false;
            self.spill_copied += 1;
        }
        return self.spill_copied == self.len;
    }
    pub fn work(self: *Proof, budget: spill.WorkBudget, control: anytype) !bool {
        try control.check();
        try budget.validate();
        if (self.invalid or self.finish_started) return error.InvalidTxnRecord;
        errdefer self.invalid = true;
        return if (self.sort) |sort| try sort.step(budget, control) else true;
    }
    fn less(_: void, left: [33]u8, right: [33]u8) bool {
        return std.mem.order(u8, &left, &right) == .lt;
    }
    fn group(counts: *Counts, digest: Digest, flags: u8, visitor: anytype) !void {
        // The immutable index and retained original enlistment must denote
        // the same set; every independently durable ACK must be enlisted.
        if (((flags & enlisted) != 0) != ((flags & original) != 0) or
            ((flags & (legacy_resolved | indexed_resolved)) != 0 and flags & enlisted == 0)) return error.InvalidTxnRecord;
        if (flags & enlisted != 0) counts.enlisted = try std.math.add(u64, counts.enlisted, 1);
        if (flags & (legacy_resolved | indexed_resolved) != 0) counts.resolved = try std.math.add(u64, counts.resolved, 1);
        if (flags & prefix_enlisted != 0) counts.prefix_enlisted = try std.math.add(u64, counts.prefix_enlisted, 1);
        if (flags & prefix_resolved != 0) {
            counts.prefix_resolved = try std.math.add(u64, counts.prefix_resolved, 1);
            if (flags & indexed_resolved == 0) counts.prefix_extra_resolved = try std.math.add(u64, counts.prefix_extra_resolved, 1);
        }
        try visitor.visit(digest, flags);
    }
    pub fn finish(self: *Proof, visitor: anytype) !Counts {
        const control = self.options.cancellation orelse Cancellation.none;
        while (true) if (self.finishStep(visitor, .{}, control) catch |err| return self.spillError(err)) |counts| return counts;
    }
    /// Completion includes the final digest group AND an independent EOF
    /// probe. Partial counters and matching numeric hash prefixes never qualify.
    pub fn finishStep(self: *Proof, visitor: anytype, budget: spill.WorkBudget, control: anytype) !?Counts {
        try control.check();
        try budget.validate();
        if (self.invalid) return error.InvalidTxnRecord;
        if (self.complete) return self.counts;
        self.finish_started = true;
        self.last_grouped_records = 0;
        errdefer self.invalid = true;
        if (!self.grouping) {
            if (self.sort) |sort| {
                if (self.spill_copied != self.len) {
                    _ = try sort.step(budget, control);
                    _ = try self.copyCache(control);
                    return null;
                }
                if (!try sort.finishStep(budget, control)) return null;
                const range = try sort.result();
                if (range) |value| self.cursor = spill.Cursor.init(self.alloc, sort.base.run, value);
            } else std.sort.pdq([33]u8, self.records[0..self.len], {}, less);
            self.grouping = true;
            return null;
        }
        const started = if (budget.io) |io| std.Io.Clock.awake.now(io).nanoseconds else 0;
        while (self.last_grouped_records < budget.units) {
            try control.check();
            const next: ?[33]u8 = if (self.cursor) |*cursor| blk: {
                const row = (try cursor.nextView()) orelse break :blk null;
                if (row.payload.length != 33) return error.InvalidTxnRecord;
                var record: [33]u8 = undefined;
                try row.payload.readInto(0, &record);
                break :blk record;
            } else if (self.small_offset == self.len) null else blk: {
                const record = self.records[self.small_offset];
                self.small_offset += 1;
                break :blk record;
            };
            const record = next orelse {
                if (self.previous) |digest| try group(&self.counts, digest, self.flags, visitor);
                self.complete = true;
                return self.counts;
            };
            const digest = record[0..32].*;
            if (self.previous != null and !std.mem.eql(u8, &self.previous.?, &digest)) {
                try group(&self.counts, self.previous.?, self.flags, visitor);
                self.flags = 0;
            }
            self.previous = digest;
            self.flags |= record[32];
            self.last_grouped_records += 1;
            if (budget.io) |io| if (std.Io.Clock.awake.now(io).nanoseconds - started >= budget.time_ns) break;
        }
        return null;
    }
};

const Noop = struct {
    pub fn visit(_: @This(), _: Digest, _: u8) !void {}
};

test "transaction debt small cohorts allocate no spill state and keep fixed scratch bounded" {
    try std.testing.expect(@sizeOf(Proof) <= 16 * 1024);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var proof = Proof.initRetained(failing.allocator(), .{ .io = std.testing.io, .directory = ".zig-cache" });
    defer proof.deinit();
    for (0..4) |i| {
        var digest: Digest = @splat(0);
        std.mem.writeInt(u64, digest[24..32], @intCast(i), .big);
        try std.testing.expect(try proof.tryAddDigest(digest, enlisted | original, Cancellation.none));
    }
    var counts: ?Counts = null;
    while (counts == null) counts = try proof.finishStep(Noop{}, .{ .units = 1 }, Cancellation.none);
    try std.testing.expectEqual(@as(u64, 4), counts.?.enlisted);
    try std.testing.expect(proof.sort == null);
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
}

test "transaction debt large cohort spill ownership cleans every allocation failure" {
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("cohort-owned-spill-alloc");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const Check = struct {
        fn run(a: std.mem.Allocator, path: []const u8) !void {
            var proof = Proof.initRetained(a, .{ .io = std.testing.io, .directory = path, .chunk_records = 7, .fan_in = 3 });
            defer proof.deinit();
            for (0..257) |i| {
                var digest: Digest = @splat(0);
                std.mem.writeInt(u64, digest[24..32], @intCast(i), .big);
                try proof.addDigest(digest, enlisted | original);
            }
            try std.testing.expectEqual(@as(u64, 257), (try proof.finish(Noop{})).enlisted);
        }
    };
    // Force growth allocations: page remap success varies with neighboring
    // mappings and otherwise changes the fail-index count between runs.
    var stable = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(stable.allocator(), Check.run, .{directory.path()});
}

test "transaction debt retained cohort proof preserves ACK flags under bounded backpressure" {
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("cohort-retained-steps");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    var signal = std.atomic.Value(bool).init(true);
    var proof = Proof.initRetained(std.testing.allocator, .{ .io = std.testing.io, .directory = directory.path(), .chunk_records = 1, .fan_in = 3, .cancellation = Cancellation.fromAtomic(&signal) });
    defer proof.deinit();
    try std.testing.expect(proof.options.cancellation == null);
    const Advance = struct {
        fn add(proof_value: *Proof, digest: Digest, flags: u8, turns: *usize) !void {
            while (!try proof_value.tryAddDigest(digest, flags, Cancellation.none)) {
                _ = try proof_value.work(.{ .units = 1 }, Cancellation.none);
                turns.* += 1;
                if (turns.* > 250_000) return error.TestUnexpectedResult;
                if (proof_value.sort) |sort| try std.testing.expect(sort.last_units <= 1);
            }
        }
    };
    var turns: usize = 0;
    for (0..600) |i| {
        var digest: Digest = @splat(0);
        std.mem.writeInt(u64, digest[24..32], @intCast(i), .big);
        try Advance.add(&proof, digest, enlisted | original, &turns);
    }
    const first: Digest = @splat(0);
    const before = proof.records[0][32];
    try std.testing.expect(!try proof.tryAddDigest(first, legacy_resolved, Cancellation.none));
    try std.testing.expectEqual(before, proof.records[0][32]);
    for (0..600) |i| {
        var digest: Digest = @splat(0);
        std.mem.writeInt(u64, digest[24..32], @intCast(i), .big);
        try Advance.add(&proof, digest, legacy_resolved, &turns);
        try Advance.add(&proof, digest, legacy_resolved, &turns);
    }
    const Visitor = struct {
        groups: usize = 0,
        pub fn visit(self: *@This(), digest: Digest, flags: u8) !void {
            try std.testing.expectEqual(@as(u64, @intCast(self.groups)), std.mem.readInt(u64, digest[24..32], .big));
            try std.testing.expectEqual(enlisted | original | legacy_resolved, flags);
            self.groups += 1;
        }
    };
    var visitor: Visitor = .{};
    var counts: ?Counts = null;
    while (counts == null) {
        counts = try proof.finishStep(&visitor, .{ .units = 1 }, Cancellation.none);
        turns += 1;
        if (turns > 250_000) return error.TestUnexpectedResult;
        try std.testing.expect(proof.last_grouped_records <= 1);
        if (counts == null) try std.testing.expect(!proof.complete);
    }
    try std.testing.expectEqual(@as(u64, 600), counts.?.enlisted);
    try std.testing.expectEqual(@as(u64, 600), counts.?.resolved);
    try std.testing.expectEqual(@as(usize, 600), visitor.groups);
    try std.testing.expectEqualDeep(counts, try proof.finishStep(&visitor, .{ .units = 1 }, Cancellation.none));
    try std.testing.expectEqual(@as(usize, 600), visitor.groups);
}

test "transaction debt retained cohort proof rejects failed visitors and cancelled partial grouping" {
    const options: spill.Options = .{ .io = std.testing.io, .directory = ".zig-cache" };
    const Fail = struct {
        pub fn visit(_: @This(), _: Digest, _: u8) !void {
            return error.InjectedLeaseLost;
        }
    };
    {
        var proof = Proof.initRetained(std.testing.allocator, options);
        defer proof.deinit();
        try std.testing.expect(try proof.tryAddDigest(@splat(1), enlisted | original, Cancellation.none));
        try std.testing.expect(try proof.finishStep(Fail{}, .{ .units = 1 }, Cancellation.none) == null); // sort
        try std.testing.expect(try proof.finishStep(Fail{}, .{ .units = 1 }, Cancellation.none) == null); // record
        try std.testing.expectError(error.InjectedLeaseLost, proof.finishStep(Fail{}, .{ .units = 1 }, Cancellation.none)); // EOF group
        try std.testing.expect(proof.invalid and !proof.complete);
        try std.testing.expectError(error.InvalidTxnRecord, proof.finishStep(Noop{}, .{}, Cancellation.none));
    }
    {
        var proof = Proof.initRetained(std.testing.allocator, options);
        defer proof.deinit();
        try std.testing.expect(try proof.tryAddDigest(@splat(2), enlisted | original, Cancellation.none));
        var signal = std.atomic.Value(bool).init(true);
        try std.testing.expectError(error.Canceled, proof.finishStep(Noop{}, .{}, Cancellation.fromAtomic(&signal)));
        try std.testing.expect(!proof.invalid and !proof.finish_started);
        try std.testing.expect(try proof.finishStep(Noop{}, .{}, Cancellation.none) == null); // sort
        const Control = struct {
            calls: usize = 0,
            pub fn check(self: *@This()) !void {
                self.calls += 1;
                if (self.calls >= 3) return error.Canceled;
            }
        };
        var control: Control = .{};
        try std.testing.expectError(error.Canceled, proof.finishStep(Noop{}, .{}, &control));
        try std.testing.expectEqual(@as(usize, 1), proof.last_grouped_records);
        try std.testing.expect(proof.invalid and !proof.complete);
        try std.testing.expectError(error.InvalidTxnRecord, proof.finishStep(Noop{}, .{}, Cancellation.none));
    }
}

test "transaction debt cohort proof spills complete hash ties and rejects unbound ACKs" {
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("cohort-spill-proof");
    defer directory.cleanup();
    try std.Io.Dir.cwd().createDirPath(std.testing.io, directory.path());
    const options: spill.Options = .{ .io = std.Options.debug_io, .directory = directory.path(), .chunk_records = 7, .chunk_bytes = 1024, .fan_in = 3 };
    var proof = Proof.init(std.testing.allocator, options);
    defer proof.deinit();
    // Every numeric navigation prefix collides, across multiple merge levels.
    // Full 256-bit identity and duplicate flags must still group correctly.
    for (0..600) |i| {
        var digest: Digest = @splat(0);
        std.mem.writeInt(u64, digest[24..32], @intCast(i), .big);
        try proof.addDigest(digest, enlisted | original);
        try proof.addDigest(digest, enlisted | original);
        if (i % 2 == 0) try proof.addDigest(digest, legacy_resolved);
    }
    const counts = try proof.finish(Noop{});
    try std.testing.expectEqual(@as(u64, 600), counts.enlisted);
    try std.testing.expectEqual(@as(u64, 300), counts.resolved);
    try std.testing.expect(proof.sort.?.base.merge_passes > 1);
    var invalid = Proof.init(std.testing.allocator, options);
    defer invalid.deinit();
    try invalid.add("outsider", legacy_resolved);
    try std.testing.expectError(error.InvalidTxnRecord, invalid.finish(Noop{}));
    var canceled = std.atomic.Value(bool).init(true);
    var canceled_options = options;
    canceled_options.cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&canceled);
    var canceled_proof = Proof.init(std.testing.allocator, canceled_options);
    defer canceled_proof.deinit();
    try std.testing.expectError(error.Canceled, canceled_proof.add("member", enlisted | original));
    const Lease = struct {
        calls: usize = 0,
        limit: usize = std.math.maxInt(usize),
        fn check(ptr: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            self.calls += 1;
            if (self.calls >= self.limit) return error.InjectedLeaseLost;
        }
    };
    var lease: Lease = .{};
    var lease_options = options;
    lease_options.cancellation = .{ .ptr = &lease, .check_fn = Lease.check };
    var lease_proof = Proof.init(std.testing.allocator, lease_options);
    defer lease_proof.deinit();
    for (0..300) |i| {
        var digest: Digest = @splat(0);
        std.mem.writeInt(u64, digest[24..32], @intCast(i), .big);
        try lease_proof.addDigest(digest, enlisted | original);
    }
    lease.limit = lease.calls + 5;
    try std.testing.expectError(error.InjectedLeaseLost, lease_proof.finish(Noop{}));
}
