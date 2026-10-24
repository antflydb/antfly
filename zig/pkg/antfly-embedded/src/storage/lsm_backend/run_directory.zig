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

//! Persistent run metadata, separate from mutable cache hints in Backend.runs.
//! A publication copies only changed search paths. Readers pin a root in O(1)
//! and materialize their shared read/planning projection outside the writer lock.
const std = @import("std");
const run_comparison = @import("run_comparison.zig");
const run_summary = @import("run_summary.zig");
const repository = @import("repository.zig");
const state = @import("state.zig");
const Account = @import("memory_account.zig").Account;
const Run = repository.Run;
const generation_index = @import("generation_index.zig");

test "directory accounting pin retention scaling benchmark" {
    if (@import("builtin").mode != .fast) return error.SkipZigTest;
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pins: usize = 0,
        pub fn retainRunSnapshotRef(self: *@This(), _: *Run) !void {
            self.pins += 1;
        }
        pub fn releaseRunSnapshotRef(self: *@This(), _: *Run) void {
            self.pins -= 1;
        }
    };
    const allocator = std.heap.smp_allocator;
    const time = @import("antfly_platform").time;
    for ([_]usize{ 1000, 10000, 50000 }) |count| {
        var fixture = Fixture{ .allocator = allocator };
        const directory = try Directory.create(allocator);
        for (0..count) |i| try directory.put(&fixture, .{ .id = i + 1, .level = 0, .size_bytes = 1024, .path = @constCast("benchmark.sst"), .smallest_namespace_name = null, .smallest_key = @constCast("a"), .largest_namespace_name = null, .largest_key = @constCast("z"), .entry_count = 1, .bloom_filter = null, .state = null });
        const started = time.monotonicNs();
        for (0..10000) |_| {
            var token = directory.pinAccounting();
            std.mem.doNotOptimizeAway(&token);
            token.deinit();
        }
        const token_ns = (time.monotonicNs() - started) / 10000;
        const old_root = try directory.fork(allocator);
        var accounting = directory.pinAccounting();
        defer accounting.deinit();
        const selected = directory.at(0).retain();
        defer selected.release(allocator);
        directory.destroy(allocator);
        const old_pins = fixture.pins;
        const old_bytes = accounting.accountedMemoryBytes(@import("memory_account.zig").nextPass());
        old_root.destroy(allocator);
        const new_bytes = accounting.accountedMemoryBytes(@import("memory_account.zig").nextPass());
        try std.testing.expectEqual(count, old_pins);
        try std.testing.expectEqual(@as(usize, 1), fixture.pins);
        std.debug.print("accounting-retention runs={d} selected=1 old_pins={d} new_pins={d} old_metadata_bytes={d} new_metadata_bytes={d} token_capture_release_ns={d}\n", .{ count, old_pins, fixture.pins, old_bytes, new_bytes, token_ns });
    }
}

test "directory accounting token charges selected payloads without pinning unrelated files" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pins: usize = 0,
        pub fn retainRunSnapshotRef(self: *@This(), _: *Run) !void {
            self.pins += 1;
        }
        pub fn releaseRunSnapshotRef(self: *@This(), _: *Run) void {
            self.pins -= 1;
        }
        fn check(allocator: std.mem.Allocator) !void {
            var fixture = @This(){ .allocator = allocator };
            var accounting: ?Directory.Accounting = null;
            defer if (accounting) |*token| token.deinit();
            var selected: ?Directory.Handle = null;
            defer if (selected) |handle| handle.release(allocator);
            {
                const directory = try Directory.create(allocator);
                defer directory.destroy(allocator);
                for (0..3) |i| try directory.put(&fixture, .{ .id = i + 1, .level = 0, .size_bytes = 1, .path = @constCast("account.sst"), .smallest_namespace_name = null, .smallest_key = @constCast("a"), .largest_namespace_name = null, .largest_key = @constCast("z"), .entry_count = 1, .bloom_filter = null, .state = null });
                accounting = directory.pinAccounting();
                selected = directory.at(0).retain();
            }
            try std.testing.expectEqual(@as(usize, 1), fixture.pins);
            const pass = @import("memory_account.zig").nextPass();
            const bytes = accounting.?.accountedMemoryBytes(pass);
            try std.testing.expect(bytes > 6 * @sizeOf(Account));
            try std.testing.expectEqual(@as(u64, 0), selected.?.accountedMemoryBytes(pass));
            try std.testing.expectEqual(@as(u64, 0), accounting.?.accountedMemoryBytes(pass));
            selected.?.release(allocator);
            selected = null;
            try std.testing.expectEqual(@as(usize, 0), fixture.pins);
            var headers: u64 = 0;
            for (accounting.?.accounts) |maybe| if (maybe) |_| {
                headers += @sizeOf(Account);
            };
            try std.testing.expectEqual(headers, accounting.?.accountedMemoryBytes(@import("memory_account.zig").nextPass()));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}

const Payload = struct {
    refs: std.atomic.Value(usize) = .init(1),
    run: Run,
    owner: *anyopaque,
    release_pin: *const fn (*anyopaque, *Run) void,
    account: *Account,
    bytes: usize,
};

const Entry = struct {
    run: *const Run,
    payload: ?*Payload = null,
    domain: []const u8 = "",

    pub const Summary = run_summary.Summary;
    pub const Preparation = SummaryPreparation(Summary, run_summary.Full);
    pub fn summarize(entry: Entry, left: Summary, right: Summary) Summary {
        var preparation = Preparation.init(entry, left, right);
        return preparation.resumeWithAdmission(null) catch unreachable;
    }

    pub fn retainShared(self: Entry) Entry {
        _ = self.payload.?.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn deinit(self: Entry, allocator: std.mem.Allocator) void {
        const payload = self.payload orelse return;
        if (payload.refs.fetchSub(1, .acq_rel) != 1) return;
        payload.release_pin(payload.owner, &payload.run);
        payload.run.deinit(allocator);
        payload.account.discharge(payload.bytes);
        allocator.destroy(payload);
    }
    pub fn retainedBytes(_: Entry) usize {
        return 0;
    }
};

fn SummaryPreparation(comptime Result: type, comptime Cursor: type) type {
    return struct {
        cursor: Cursor,
        entry: Entry,
        pub fn init(entry: Entry, left: Result, right: Result) @This() {
            return .{ .cursor = .init(entry.run, left, right), .entry = entry };
        }
        pub fn matches(self: *const @This(), entry: Entry, left: Result, right: Result) bool {
            return self.entry.run == entry.run and self.entry.payload == entry.payload and
                self.entry.domain.ptr == entry.domain.ptr and self.entry.domain.len == entry.domain.len and
                std.meta.eql(self.cursor.left, left) and std.meta.eql(self.cursor.right, right);
        }
        pub fn resumeWithAdmission(self: *@This(), original: ?@import("../../common/read_admission.zig").Port) !Result {
            return self.cursor.resumeWithAdmission(original);
        }
    };
}
const BoundsSummary = struct {
    pub const Summary = run_summary.UpperSummary;
    pub const Preparation = SummaryPreparation(Summary, run_summary.Upper);
    pub fn summarize(entry: Entry, left: Summary, right: Summary) Summary {
        var preparation = Preparation.init(entry, left, right);
        return preparation.resumeWithAdmission(null) catch unreachable;
    }
};

fn compareBound(a_ns: ?[]const u8, a: []const u8, b_ns: ?[]const u8, b: []const u8) std.math.Order {
    var cursor = run_comparison.Cursor.bound(a_ns, a, b_ns, b);
    return cursor.resumeWithAdmission(null) catch unreachable;
}
fn comparison(comptime mode: run_comparison.Mode, a: Entry, b: Entry) std.math.Order {
    return run_comparison.compare(mode, .{ .run = a.run, .domain = a.domain }, .{ .run = b.run, .domain = b.domain });
}
fn compareDomain(a: Entry, b: Entry) std.math.Order {
    return comparison(.domain, a, b);
}
fn compareId(a: Entry, b: Entry) std.math.Order {
    return comparison(.id, a, b);
}
fn compareBounds(a: Entry, b: Entry) std.math.Order {
    return comparison(.lower, a, b);
}
fn compareEnds(a: Entry, b: Entry) std.math.Order {
    return comparison(.upper, a, b);
}

pub const LevelAggregate = struct {
    level: u32,
    count: usize = 0,
    bytes: u64 = 0,
    tombstone_runs: usize = 0,
    pub fn retainShared(self: @This()) @This() {
        return self;
    }
    pub fn deinit(_: @This(), _: std.mem.Allocator) void {}
    pub fn retainedBytes(_: @This()) usize {
        return 0;
    }
};

fn compareLevel(a: LevelAggregate, b: LevelAggregate) std.math.Order {
    return std.math.order(a.level, b.level);
}

fn compare(a: Entry, b: Entry) std.math.Order {
    return comparison(.read, a, b);
}

pub const Directory = struct {
    pub fn runLess(a: *const Run, b: *const Run) bool {
        return compare(.{ .run = a }, .{ .run = b }) == .lt;
    }
    pub fn containsReadOrdered(handles: []const Handle, run: *const Run) bool {
        var lo: usize = 0;
        var hi = handles.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (runLess(handles[mid].run, run)) lo = mid + 1 else hi = mid;
        }
        return lo < handles.len and handles[lo].run.id == run.id;
    }
    const Tree = @import("ordered_index.zig").Index(Entry, compare);
    const IdTree = @import("ordered_index.zig").SummarizedIndex(Entry, compareId, void);
    const BoundsTree = @import("ordered_index.zig").SummarizedIndex(Entry, compareBounds, BoundsSummary);
    const EndsTree = @import("ordered_index.zig").SummarizedIndex(Entry, compareEnds, void);
    const LevelTree = @import("ordered_index.zig").Index(LevelAggregate, compareLevel);
    tree: Tree = .{},
    ids: IdTree = .{},
    bounds: BoundsTree = .{},
    ends: EndsTree = .{},
    levels: LevelTree = .{},
    generations: generation_index.Tree = .{},
    total_run_bytes: u64 = 0,
    memory_run_count: usize = 0,
    retired_next: ?*Directory = null,

    fn Comparison(comptime mode: run_comparison.Mode) type {
        return struct {
            cursor: run_comparison.Cursor,
            pub fn init(a: Entry, b: Entry) @This() {
                return .{ .cursor = .init(mode, .{ .run = a.run, .domain = a.domain }, .{ .run = b.run, .domain = b.domain }) };
            }
            pub fn resumeWithAdmission(self: *@This(), original: ?@import("../../common/read_admission.zig").Port) !std.math.Order {
                return self.cursor.resumeWithAdmission(original);
            }
        };
    }
    pub const ReadSearch = Tree.Search(Comparison(.read));
    pub const IdSearch = IdTree.Search(Comparison(.id));
    pub const LowerSearch = BoundsTree.Search(Comparison(.lower));
    pub const UpperSearch = EndsTree.Search(Comparison(.upper));
    pub fn locateRead(self: *const Directory, run: *const Run) ReadSearch {
        return .init(self.tree.root, .{ .run = run });
    }
    pub fn locateId(self: *const Directory, run: *const Run) IdSearch {
        return .init(self.ids.root, .{ .run = run });
    }
    pub fn locateLower(self: *const Directory, run: *const Run) LowerSearch {
        return .init(self.bounds.root, .{ .run = run });
    }
    pub fn locateUpper(self: *const Directory, run: *const Run) UpperSearch {
        return .init(self.ends.root, .{ .run = run });
    }

    pub fn create(allocator: std.mem.Allocator) !*Directory {
        const self = try allocator.create(Directory);
        self.* = .{};
        return self;
    }
    pub fn fork(self: *const Directory, allocator: std.mem.Allocator) !*Directory {
        const out = try create(allocator);
        out.tree = self.tree.fork();
        out.ids = self.ids.fork();
        out.bounds = self.bounds.fork();
        out.ends = self.ends.fork();
        out.levels = self.levels.fork();
        out.generations = self.generations.fork();
        out.total_run_bytes = self.total_run_bytes;
        out.memory_run_count = self.memory_run_count;
        return out;
    }
    pub fn destroy(self: *Directory, allocator: std.mem.Allocator) void {
        self.destroyContents(allocator);
        allocator.destroy(self);
    }

    pub const Reclaimer = struct {
        directory: *Directory,
        tree: Tree.Reclaimer,
        ids: IdTree.Reclaimer,
        bounds: BoundsTree.Reclaimer,
        ends: EndsTree.Reclaimer,
        levels: LevelTree.Reclaimer,
        generations: generation_index.Tree.Reclaimer,

        pub fn init(directory: *Directory) @This() {
            directory.retainAccounting();
            return .{ .directory = directory, .tree = .init(directory.tree), .ids = .init(directory.ids), .bounds = .init(directory.bounds), .ends = .init(directory.ends), .levels = .init(directory.levels), .generations = .init(directory.generations) };
        }
        pub fn step(self: *@This(), allocator: std.mem.Allocator, credits: *usize) bool {
            return self.tree.step(allocator, credits) and self.ids.step(allocator, credits) and self.bounds.step(allocator, credits) and self.ends.step(allocator, credits) and self.levels.step(allocator, credits) and self.generations.step(allocator, credits);
        }
        /// Only after step reports completion, back under the accounting lock.
        pub fn finish(self: *@This(), allocator: std.mem.Allocator) void {
            self.directory.releaseAccounting();
            allocator.destroy(self.directory);
        }
    };

    /// Keep headers immutable while a detached reclamation batch is charged.
    pub fn destroyContents(self: *const Directory, allocator: std.mem.Allocator) void {
        var tree = self.tree;
        tree.deinit(allocator);
        var ids = self.ids;
        ids.deinit(allocator);
        var bounds = self.bounds;
        bounds.deinit(allocator);
        var ends = self.ends;
        ends.deinit(allocator);
        var levels = self.levels;
        levels.deinit(allocator);
        var generations = self.generations;
        generations.deinit(allocator);
    }

    pub fn retainAccounting(self: *const Directory) void {
        if (self.tree.account) |account| _ = account.retain();
        if (self.ids.account) |account| _ = account.retain();
        if (self.bounds.account) |account| _ = account.retain();
        if (self.ends.account) |account| _ = account.retain();
        if (self.levels.account) |account| _ = account.retain();
        if (self.generations.account) |account| _ = account.retain();
    }

    pub fn releaseAccounting(self: *const Directory) void {
        if (self.tree.account) |account| account.release();
        if (self.ids.account) |account| account.release();
        if (self.bounds.account) |account| account.release();
        if (self.ends.account) |account| account.release();
        if (self.levels.account) |account| account.release();
        if (self.generations.account) |account| account.release();
    }

    /// Memory ownership is not read visibility. A parked job can charge its
    /// selected payloads after releasing the discovery root without retaining
    /// tree nodes or the physical SST pins of unrelated runs. Capturing and
    /// releasing this token is O(1), independent of the number of inputs.
    pub const Accounting = struct {
        accounts: [6]?*Account,

        pub fn accountedMemoryBytes(self: *const Accounting, pass: u64) u64 {
            var bytes: u64 = 0;
            for (self.accounts) |maybe| if (maybe) |account| {
                bytes +|= account.chargeOnce(pass);
            };
            return bytes;
        }

        pub fn deinit(self: *Accounting) void {
            for (&self.accounts) |*maybe| {
                if (maybe.*) |account| account.release();
                maybe.* = null;
            }
        }
    };

    pub fn pinAccounting(self: *const Directory) Accounting {
        const result = Accounting{ .accounts = .{ self.tree.account, self.ids.account, self.bounds.account, self.ends.account, self.levels.account, self.generations.account } };
        for (result.accounts) |maybe| if (maybe) |account| {
            _ = account.retain();
        };
        return result;
    }

    /// Prepare allocation pools only. The caller separately performs admitted
    /// key comparisons, payload/pin preparation and atomic index publication.
    /// Candidate/root identities must remain stable until this cursor finishes.
    pub const IndexPreparation = struct {
        directory: *Directory,
        level: u32,
        ends_edits: usize,
        phase: usize = 0,
        pub fn init(directory: *Directory, level: u32, ends_edits: usize) !@This() {
            if (ends_edits == 0 or ends_edits > 2) return error.InvalidArgument;
            return .{ .directory = directory, .level = level, .ends_edits = ends_edits };
        }
        pub fn memoryBound(self: *const @This()) !u64 {
            const d = self.directory;
            var bytes = try d.tree.prepareMemoryBound(1);
            bytes = try std.math.add(u64, bytes, try d.ids.prepareMemoryBound(1));
            bytes = try std.math.add(u64, bytes, try d.bounds.prepareMemoryBound(1));
            bytes = try std.math.add(u64, bytes, try d.ends.prepareMemoryBound(self.ends_edits));
            bytes = try std.math.add(u64, bytes, try d.levels.prepareMemoryBound(1));
            if (self.level == 0) bytes = try std.math.add(u64, bytes, try d.generations.prepareMemoryBound(1));
            return bytes;
        }
        fn prepare(tree: anytype, a: std.mem.Allocator, edits: usize, original: ?@import("../../common/read_admission.zig").Port) !void {
            if (original) |port| try tree.prepareEditsWithAdmission(a, edits, port) else try tree.prepareEdits(a, edits);
        }
        pub fn resumeWithAdmission(self: *@This(), a: std.mem.Allocator, original: ?@import("../../common/read_admission.zig").Port) !void {
            const d = self.directory;
            while (self.phase < 6) {
                if (original) |port| try port.checkpoint();
                switch (self.phase) {
                    0 => try prepare(&d.tree, a, 1, original),
                    1 => try prepare(&d.ids, a, 1, original),
                    2 => try prepare(&d.bounds, a, 1, original),
                    3 => try prepare(&d.ends, a, self.ends_edits, original),
                    4 => try prepare(&d.levels, a, 1, original),
                    5 => if (self.level == 0) {
                        try prepare(&d.generations, a, 1, original);
                    },
                    else => unreachable,
                }
                self.phase += 1;
            }
        }
    };
    /// Two edits in every index cover arbitrary old-ID removal followed by
    /// insertion, including changes of key, level and L0 visibility sequence.
    pub const PutPools = struct {
        directory: *Directory,
        phase: usize = 0,
        pub fn memoryBound(self: *const @This()) !u64 {
            var total: u64 = 0;
            inline for (.{ "tree", "ids", "bounds", "ends", "levels", "generations" }) |field|
                total = try std.math.add(u64, total, try @field(self.directory, field).prepareMemoryBound(2));
            return total;
        }
        pub fn resumeWithAdmission(self: *@This(), allocator: std.mem.Allocator, original: ?@import("../../common/read_admission.zig").Port) !void {
            if (original) |port| try port.checkpoint();
            inline for (.{ "tree", "ids", "bounds", "ends", "levels", "generations" }, 0..) |field, i| {
                if (self.phase == i) {
                    if (original) |port| try @field(self.directory, field).prepareEditsWithAdmission(allocator, 2, port) else try @field(self.directory, field).prepareEdits(allocator, 2);
                    self.phase += 1;
                }
            }
        }
    };

    /// One owned registration, represented independently of backend lifetime.
    pub const EntryPin = struct {
        owner: *anyopaque,
        release: *const fn (*anyopaque, *Run) void,
    };

    /// Owns one immutable payload, pin and accounting reference. Its allocator,
    /// backend and domain authority outlive it. Last-reference release still
    /// needs physical retirement; deinit is an explicit quiesced fallback.
    pub const EntryLease = struct {
        entry: Entry,
        /// The owner supplies a retained pin-release operation. It must not
        /// invoke the legacy path registry under admission: rejection retains
        /// its progress, and success releases exactly this payload's pin.
        pub const PinRelease = struct {
            context: *anyopaque,
            resume_fn: *const fn (*anyopaque, *anyopaque, *Run, ?@import("../../common/read_admission.zig").Port) anyerror!void,
        };
        pub const Retirement = struct {
            payload: ?*Payload,
            pin: PinRelease,
            physical: ?@import("run_snapshot_clone.zig").Retirement = null,
            pin_released: bool = false,
            complete: bool = false,

            /// Pin release precedes path retirement. Keep the payload header
            /// and both accounting references until every owned byte is gone.
            fn retire(self: *@This(), allocator: std.mem.Allocator, original: ?@import("../../common/read_admission.zig").Port) !void {
                if (original) |port| try port.checkpoint();
                if (self.complete) return;
                const payload = self.payload.?;
                if (!self.pin_released) {
                    try self.pin.resume_fn(self.pin.context, payload.owner, &payload.run, original);
                    payload.run.version_ref_pinned = false;
                    self.pin_released = true;
                }
                if (self.physical == null) self.physical = @import("run_snapshot_clone.zig").Retirement.init(payload.run) catch unreachable;
                try self.physical.?.retireWithAdmission(allocator, original);
                if (original) |port| try port.checkpoint();
                self.finish(allocator);
            }
            fn finish(self: *@This(), allocator: std.mem.Allocator) void {
                const payload = self.payload.?;
                const account = payload.account;
                const bytes = payload.bytes;
                allocator.destroy(payload);
                account.discharge(bytes);
                account.release();
                self.payload = null;
                self.complete = true;
            }
            pub fn retireWithAdmission(self: *@This(), allocator: std.mem.Allocator, original: @import("../../common/read_admission.zig").Port) !void {
                try self.retire(allocator, original);
            }
            /// Explicit joined/quiesced fallback drives the same pin cursor,
            /// including any progress retained before admission rejection.
            pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
                self.retire(allocator, null) catch unreachable;
            }
        };

        /// Validate an opaque pin's issuer before a specialized release
        /// operation interprets its owner. Rejection leaves this lease intact.
        pub fn beginRetirementForPinWithAdmission(self: *@This(), expected: *const fn (*anyopaque, *Run) void, pin: PinRelease, original: @import("../../common/read_admission.zig").Port) !Retirement {
            try original.checkpoint();
            if (self.entry.payload.?.release_pin != expected) return error.ExecutionWaitUnsupported;
            return self.beginRetirementWithAdmission(pin, original);
        }

        /// Consumes this lease only after admission and ownership validation.
        /// Other directory/handle owners may still exist; their non-final drop
        /// is constant work. A coordinator keeps a final lease until all such
        /// owners drain so their generic destructors cannot become final.
        pub fn beginRetirementWithAdmission(self: *@This(), pin: PinRelease, original: @import("../../common/read_admission.zig").Port) !Retirement {
            try original.checkpoint();
            const payload = self.entry.payload.?;
            var view = payload.run;
            view.version_ref_pinned = false;
            if (!@import("run_snapshot_clone.zig").Retirement.supports(view)) return error.ExecutionWaitUnsupported;
            const account = payload.account;
            const final = payload.refs.fetchSub(1, .acq_rel) == 1;
            if (!final) account.release();
            self.* = undefined;
            return .{ .payload = if (final) payload else null, .pin = pin, .complete = !final };
        }
        pub fn run(self: *const @This()) *const Run {
            return self.entry.run;
        }
        pub fn accountedMemoryBytes(self: *const @This(), pass: u64) u64 {
            return self.entry.payload.?.account.chargeOnce(pass);
        }
        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            const account = self.entry.payload.?.account;
            self.entry.deinit(allocator);
            account.release();
            self.* = undefined;
        }
    };

    /// Owns an already-prepaid private fork for one removal. Caller freezes
    /// and pins source through publication or complete abort, pins the query
    /// through lookup, prepays this
    /// fixed header, and excludes candidate reads/accounting/mutation. No new
    /// payload is created; the source keeps every payload drop non-final.
    /// Backend generation/durability/inventory receipts and predecessor-root
    /// retirement registration must be prepared by the owning coordinator.
    pub const Removal = struct {
        source: *const Directory,
        candidate: *Directory,
        source_roots: [6]?*const anyopaque,
        total_bytes: u64,
        memory_runs: usize,
        entry: ?Entry = null,
        pending: union(enum) {
            read_search: ReadSearch,
            read_remove: Tree.RankRemoval,
            id_search: IdSearch,
            id_remove: IdTree.RankRemoval,
            lower_search: LowerSearch,
            lower_remove: BoundsTree.RankRemoval,
            upper_search: UpperSearch,
            upper_remove: EndsTree.RankRemoval,
            level_plan,
            level_put: LevelTree.RankInsertion,
            level_remove: LevelTree.RankRemoval,
            generation_plan,
            generation_put: generation_index.Tree.RankInsertion,
            generation_remove: generation_index.Tree.RankRemoval,
            receipt,
            done,
            retiring,
        },
        reclaimer: ?Reclaimer = null,
        published: bool = false,
        taken: bool = false,
        retired: bool = false,

        fn roots(d: *const Directory) [6]?*const anyopaque {
            var result: [6]?*const anyopaque = undefined;
            inline for (.{ "tree", "ids", "bounds", "ends", "levels", "generations" }, 0..) |field, i|
                result[i] = if (@field(d, field).root) |root| @ptrCast(root) else null;
            return result;
        }
        pub fn init(source: *const Directory, candidate: *Directory, run: *const Run) !@This() {
            if (source == candidate or candidate.retired_next != null or
                !std.meta.eql(roots(source), roots(candidate)) or
                source.total_run_bytes != candidate.total_run_bytes or source.memory_run_count != candidate.memory_run_count)
                return error.InvalidArgument;
            return .{ .source = source, .candidate = candidate, .source_roots = roots(source), .total_bytes = source.total_run_bytes, .memory_runs = source.memory_run_count, .pending = .{ .read_search = candidate.locateRead(run) } };
        }
        fn stable(self: *const @This()) bool {
            return std.meta.eql(self.source_roots, roots(self.source)) and
                self.source.total_run_bytes == self.total_bytes and self.source.memory_run_count == self.memory_runs;
        }
        fn check(self: *const @This(), original: ?@import("../../common/read_admission.zig").Port) !void {
            if (original) |port| try port.checkpoint();
            if (!self.stable()) return error.CompactionPlanningStale;
        }
        fn releaseRunNode(self: *@This(), allocator: std.mem.Allocator, node: anytype) void {
            _ = self;
            // Source is frozen and owns this exact immutable payload. Thus
            // release cannot enter the pin registry or destroy run metadata.
            std.debug.assert(node.left == null and node.right == null);
            std.debug.assert(node.entry.payload.?.refs.load(.acquire) > 1);
            node.release(allocator);
        }
        pub fn resumeWithAdmission(self: *@This(), allocator: std.mem.Allocator, original: ?@import("../../common/read_admission.zig").Port) !void {
            if (original) |port| try port.checkpoint();
            if (self.published or self.taken or self.retired or std.meta.activeTag(self.pending) == .retiring) return error.InvalidArgument;
            while (true) {
                try self.check(original);
                switch (self.pending) {
                    .read_search => |*job| {
                        try job.resumeWithAdmission(original);
                        if (job.matched == null) {
                            self.pending = .done;
                            continue;
                        }
                        self.entry = job.matched.?.entry;
                        self.pending = .{ .read_remove = try Tree.RankRemoval.init(&self.candidate.tree, job.rank) };
                    },
                    .read_remove => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        if (try job.takeRetired()) |node| self.releaseRunNode(allocator, node);
                        self.pending = .{ .id_search = self.candidate.locateId(self.entry.?.run) };
                    },
                    .id_search => |*job| {
                        try job.resumeWithAdmission(original);
                        if (job.matched == null or job.matched.?.entry.payload != self.entry.?.payload) return error.CompactionPlanningStale;
                        self.pending = .{ .id_remove = try IdTree.RankRemoval.init(&self.candidate.ids, job.rank) };
                    },
                    .id_remove => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        if (try job.takeRetired()) |node| self.releaseRunNode(allocator, node);
                        self.pending = .{ .lower_search = self.candidate.locateLower(self.entry.?.run) };
                    },
                    .lower_search => |*job| {
                        try job.resumeWithAdmission(original);
                        if (job.matched == null or job.matched.?.entry.payload != self.entry.?.payload) return error.CompactionPlanningStale;
                        self.pending = .{ .lower_remove = try BoundsTree.RankRemoval.init(&self.candidate.bounds, job.rank) };
                    },
                    .lower_remove => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        if (try job.takeRetired()) |node| self.releaseRunNode(allocator, node);
                        self.pending = .{ .upper_search = self.candidate.locateUpper(self.entry.?.run) };
                    },
                    .upper_search => |*job| {
                        try job.resumeWithAdmission(original);
                        if (job.matched == null or job.matched.?.entry.payload != self.entry.?.payload) return error.CompactionPlanningStale;
                        self.pending = .{ .upper_remove = try EndsTree.RankRemoval.init(&self.candidate.ends, job.rank) };
                    },
                    .upper_remove => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        if (try job.takeRetired()) |node| self.releaseRunNode(allocator, node);
                        self.pending = .level_plan;
                    },
                    .level_plan => {
                        const entry = self.entry.?;
                        var level = self.source.levelStats(entry.run.level);
                        const rank = self.candidate.levels.root.?.lowerBound(level);
                        level.count -= 1;
                        level.bytes -= entry.run.size_bytes;
                        level.tombstone_runs -= @intFromBool((entry.run.tombstone_count orelse 0) != 0);
                        self.pending = if (level.count == 0)
                            .{ .level_remove = try LevelTree.RankRemoval.init(&self.candidate.levels, rank) }
                        else
                            .{ .level_put = try LevelTree.RankInsertion.init(&self.candidate.levels, level, rank, true) };
                    },
                    .level_put => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        _ = try job.takeRetired(); // Scalar aggregate has no owned payload.
                        self.pending = .generation_plan;
                    },
                    .level_remove => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        if (try job.takeRetired()) |node| node.release(allocator);
                        self.pending = .generation_plan;
                    },
                    .generation_plan => {
                        const entry = self.entry.?;
                        if (entry.run.level == 0) {
                            const key = generation_index.Generation{ .sequence = generation_index.sequence(entry.run.*) };
                            var generation = generation_index.Tree.find(self.source.generations.root, key).?.entry;
                            const rank = self.candidate.generations.root.?.lowerBound(key);
                            generation.files -= 1;
                            generation.bytes -= entry.run.size_bytes;
                            self.pending = if (generation.files == 0)
                                .{ .generation_remove = try generation_index.Tree.RankRemoval.init(&self.candidate.generations, rank) }
                            else
                                .{ .generation_put = try generation_index.Tree.RankInsertion.init(&self.candidate.generations, generation, rank, true) };
                        } else self.pending = .receipt;
                    },
                    .generation_put => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        _ = try job.takeRetired(); // Scalar aggregate has no owned payload.
                        self.pending = .receipt;
                    },
                    .generation_remove => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        if (try job.takeRetired()) |node| node.release(allocator);
                        self.pending = .receipt;
                    },
                    .receipt => {
                        self.candidate.total_run_bytes = self.total_bytes - self.entry.?.run.size_bytes;
                        self.candidate.memory_run_count = self.memory_runs - @intFromBool(self.entry.?.run.path == null);
                        self.pending = .done;
                    },
                    .done => return,
                    .retiring => unreachable,
                }
            }
        }
        /// Fixed pointer receipt under the caller's publication lock. The
        /// predecessor is returned exactly once for its prearranged retirement;
        /// no destructor or other backend state change occurs here.
        pub fn publishWithAdmission(self: *@This(), live: *?*Directory, original: @import("../../common/read_admission.zig").Port) !*Directory {
            try original.checkpoint();
            if (self.published or self.taken or self.retired or std.meta.activeTag(self.pending) != .done) return error.InvalidArgument;
            if (!self.stable() or live.* != self.source) return error.CompactionPlanningStale;
            live.* = self.candidate;
            self.published = true;
            return @constCast(self.source);
        }
        fn takePrepared(self: *@This(), original: ?@import("../../common/read_admission.zig").Port) !*Directory {
            if (original) |port| try port.checkpoint();
            if (self.published or self.taken or self.retired or std.meta.activeTag(self.pending) != .done) return error.InvalidArgument;
            if (!self.stable()) return error.CompactionPlanningStale;
            self.taken = true;
            return self.candidate;
        }
        pub fn takePreparedWithAdmission(self: *@This(), original: @import("../../common/read_admission.zig").Port) !*Directory {
            return self.takePrepared(original);
        }
        fn retire(self: *@This(), allocator: std.mem.Allocator, original: ?@import("../../common/read_admission.zig").Port) !void {
            if (original) |port| try port.checkpoint();
            if (self.published or self.taken or self.retired) return;
            try self.check(original);
            // A partial tree may still have summary pointers into the removed
            // entry. The independently pinned source keeps those bytes alive
            // while we drop the detached node and retire the partial fork.
            switch (self.pending) {
                .read_remove => |*job| {
                    if (job.detached) |node| self.releaseRunNode(allocator, node);
                    job.detached = null;
                },
                .id_remove => |*job| {
                    if (job.detached) |node| self.releaseRunNode(allocator, node);
                    job.detached = null;
                },
                .lower_remove => |*job| {
                    if (job.detached) |node| self.releaseRunNode(allocator, node);
                    job.detached = null;
                },
                .upper_remove => |*job| {
                    if (job.detached) |node| self.releaseRunNode(allocator, node);
                    job.detached = null;
                },
                .level_remove => |*job| {
                    if (job.detached) |node| node.release(allocator);
                    job.detached = null;
                },
                .generation_remove => |*job| {
                    if (job.detached) |node| node.release(allocator);
                    job.detached = null;
                },
                .level_put, .generation_put => {}, // Retired scalar entries own no resources.
                else => {},
            }
            self.pending = .retiring;
            if (self.reclaimer == null) self.reclaimer = .init(self.candidate);
            while (true) {
                try self.check(original);
                var credits: usize = 1;
                if (self.reclaimer.?.step(allocator, &credits)) break;
            }
            try self.check(original);
            self.reclaimer.?.finish(allocator);
            self.reclaimer = null;
            self.retired = true;
        }
        pub fn retireWithAdmission(self: *@This(), allocator: std.mem.Allocator, original: @import("../../common/read_admission.zig").Port) !void {
            try self.retire(allocator, original);
        }
        /// Explicit joined/quiesced fallback. Ordinary refusal retains the job.
        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            self.retire(allocator, null) catch unreachable;
        }
    };

    /// Owns a prepaid fork and EntryLease. Source and domain authority stay
    /// pinned/frozen, with candidate reads/accounting/mutation excluded. Remove
    /// by ID before inserting, so key, level and sequence changes share one
    /// correct path. The caller separately prepares the backend's other
    /// publication receipts and registers the old-root/entry retirement owners.
    pub const Put = struct {
        source: *const Directory,
        candidate: *Directory,
        source_roots: [6]?*const anyopaque,
        total_bytes: u64,
        memory_runs: usize,
        entry: Entry,
        lease: ?EntryLease,
        pending: union(enum) {
            old: IdSearch,
            remove: Removal,
            read_search: ReadSearch,
            read_put: Tree.RankInsertion,
            id_search: IdSearch,
            id_put: IdTree.RankInsertion,
            lower_search: LowerSearch,
            lower_put: BoundsTree.RankInsertion,
            upper_search: UpperSearch,
            upper_put: EndsTree.RankInsertion,
            level_plan,
            level_put: LevelTree.RankInsertion,
            generation_plan,
            generation_put: generation_index.Tree.RankInsertion,
            receipt,
            done,
        },
        reclaimer: ?Reclaimer = null,
        retiring: bool = false,
        retired: bool = false,
        published: bool = false,

        pub fn init(source: *const Directory, candidate: *Directory, lease: EntryLease) !@This() {
            if (source == candidate or candidate.retired_next != null or
                !std.meta.eql(Removal.roots(source), Removal.roots(candidate)) or
                source.total_run_bytes != candidate.total_run_bytes or source.memory_run_count != candidate.memory_run_count or
                lease.entry.payload.?.account != candidate.tree.account)
                return error.InvalidArgument;
            return .{ .source = source, .candidate = candidate, .source_roots = Removal.roots(source), .total_bytes = source.total_run_bytes, .memory_runs = source.memory_run_count, .entry = lease.entry, .lease = lease, .pending = .{ .old = candidate.locateId(lease.entry.run) } };
        }
        fn stable(self: *const @This()) bool {
            return std.meta.eql(self.source_roots, Removal.roots(self.source)) and self.total_bytes == self.source.total_run_bytes and self.memory_runs == self.source.memory_run_count;
        }
        fn check(self: *const @This(), original: ?@import("../../common/read_admission.zig").Port) !void {
            if (original) |port| try port.checkpoint();
            if (!self.stable()) return error.CompactionPlanningStale;
        }
        pub fn resumeWithAdmission(self: *@This(), allocator: std.mem.Allocator, original: ?@import("../../common/read_admission.zig").Port) !void {
            if (original) |port| try port.checkpoint();
            if (self.published or self.retired or self.retiring) return error.InvalidArgument;
            while (true) {
                try self.check(original);
                switch (self.pending) {
                    .old => |*job| {
                        try job.resumeWithAdmission(original);
                        self.pending = if (job.matched) |node|
                            .{ .remove = try Removal.init(self.source, self.candidate, node.entry.run) }
                        else
                            .{ .read_search = ReadSearch.init(self.candidate.tree.root, self.entry) };
                    },
                    .remove => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        const ready = try job.takePrepared(original);
                        std.debug.assert(ready == self.candidate);
                        self.pending = .{ .read_search = ReadSearch.init(self.candidate.tree.root, self.entry) };
                    },
                    .read_search => |*job| {
                        try job.resumeWithAdmission(original);
                        if (job.matched != null) return error.CompactionPlanningStale;
                        self.pending = .{ .read_put = try Tree.RankInsertion.init(&self.candidate.tree, self.entry, job.rank, false) };
                    },
                    .read_put => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        const retired_entry = try job.takeRetired();
                        std.debug.assert(retired_entry == null);
                        self.pending = .{ .id_search = IdSearch.init(self.candidate.ids.root, self.entry) };
                    },
                    .id_search => |*job| {
                        try job.resumeWithAdmission(original);
                        if (job.matched != null) return error.CompactionPlanningStale;
                        self.pending = .{ .id_put = try IdTree.RankInsertion.init(&self.candidate.ids, self.entry, job.rank, false) };
                    },
                    .id_put => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        const retired_entry = try job.takeRetired();
                        std.debug.assert(retired_entry == null);
                        self.pending = .{ .lower_search = LowerSearch.init(self.candidate.bounds.root, self.entry) };
                    },
                    .lower_search => |*job| {
                        try job.resumeWithAdmission(original);
                        if (job.matched != null) return error.CompactionPlanningStale;
                        self.pending = .{ .lower_put = try BoundsTree.RankInsertion.init(&self.candidate.bounds, self.entry, job.rank, false) };
                    },
                    .lower_put => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        const retired_entry = try job.takeRetired();
                        std.debug.assert(retired_entry == null);
                        self.pending = .{ .upper_search = UpperSearch.init(self.candidate.ends.root, self.entry) };
                    },
                    .upper_search => |*job| {
                        try job.resumeWithAdmission(original);
                        if (job.matched != null) return error.CompactionPlanningStale;
                        self.pending = .{ .upper_put = try EndsTree.RankInsertion.init(&self.candidate.ends, self.entry, job.rank, false) };
                    },
                    .upper_put => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        const retired_entry = try job.takeRetired();
                        std.debug.assert(retired_entry == null);
                        self.pending = .level_plan;
                    },
                    .level_plan => {
                        var level = self.candidate.levelStats(self.entry.run.level);
                        const replace = level.count != 0;
                        const rank = if (self.candidate.levels.root) |root| root.lowerBound(level) else 0;
                        level.count += 1;
                        level.bytes += self.entry.run.size_bytes;
                        level.tombstone_runs += @intFromBool((self.entry.run.tombstone_count orelse 0) != 0);
                        self.pending = .{ .level_put = try LevelTree.RankInsertion.init(&self.candidate.levels, level, rank, replace) };
                    },
                    .level_put => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        _ = try job.takeRetired(); // Scalar aggregate, no payload owner.
                        self.pending = .generation_plan;
                    },
                    .generation_plan => {
                        if (self.entry.run.level != 0) {
                            self.pending = .receipt;
                            continue;
                        }
                        const key = generation_index.Generation{ .sequence = generation_index.sequence(self.entry.run.*) };
                        const old = generation_index.Tree.find(self.candidate.generations.root, key);
                        var generation = if (old) |node| node.entry else key;
                        const rank = if (self.candidate.generations.root) |root| root.lowerBound(key) else 0;
                        generation.files += 1;
                        generation.bytes += self.entry.run.size_bytes;
                        self.pending = .{ .generation_put = try generation_index.Tree.RankInsertion.init(&self.candidate.generations, generation, rank, old != null) };
                    },
                    .generation_put => |*job| {
                        try job.resumeWithAdmission(allocator, original);
                        _ = try job.takeRetired(); // Scalar aggregate, no payload owner.
                        self.pending = .receipt;
                    },
                    .receipt => {
                        self.candidate.total_run_bytes += self.entry.run.size_bytes;
                        self.candidate.memory_run_count += @intFromBool(self.entry.run.path == null);
                        self.pending = .done;
                    },
                    .done => return,
                }
            }
        }
        pub fn publishWithAdmission(self: *@This(), live: *?*Directory, original: @import("../../common/read_admission.zig").Port) !*Directory {
            try original.checkpoint();
            if (self.published or self.retired or self.retiring or std.meta.activeTag(self.pending) != .done) return error.InvalidArgument;
            if (!self.stable() or live.* != self.source) return error.CompactionPlanningStale;
            live.* = self.candidate;
            self.published = true;
            return @constCast(self.source);
        }
        /// Transfer the remaining pin/metadata owner to a prearranged retire
        /// job. Aborting the candidate alone deliberately does not destroy it.
        pub fn takeEntryLeaseWithAdmission(self: *@This(), original: @import("../../common/read_admission.zig").Port) !EntryLease {
            try original.checkpoint();
            if ((!self.published and !self.retired) or self.lease == null) return error.InvalidArgument;
            const out = self.lease.?;
            self.lease = null;
            return out;
        }
        fn retire(self: *@This(), allocator: std.mem.Allocator, original: ?@import("../../common/read_admission.zig").Port) !void {
            if (original) |port| try port.checkpoint();
            if (self.published or self.retired) return;
            try self.check(original);
            self.retiring = true;
            if (std.meta.activeTag(self.pending) == .remove) {
                try self.pending.remove.retire(allocator, original);
                self.retired = true;
                return;
            }
            if (self.reclaimer == null) self.reclaimer = .init(self.candidate);
            while (true) {
                try self.check(original);
                var credits: usize = 1;
                if (self.reclaimer.?.step(allocator, &credits)) break;
            }
            try self.check(original);
            self.reclaimer.?.finish(allocator);
            self.reclaimer = null;
            self.retired = true;
        }
        pub fn retireWithAdmission(self: *@This(), allocator: std.mem.Allocator, original: @import("../../common/read_admission.zig").Port) !void {
            try self.retire(allocator, original);
        }
        /// Explicit joined/quiesced fallback, including last entry retirement.
        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            self.retire(allocator, null) catch unreachable;
            if (self.lease) |*lease| lease.deinit(allocator);
            self.lease = null;
        }
    };

    pub fn indexSpareBytes(self: *const Directory) u64 {
        return (self.tree.spare.capacity + self.ids.spare.capacity + self.bounds.spare.capacity + self.ends.spare.capacity + self.levels.spare.capacity + self.generations.spare.capacity) * @sizeOf(usize);
    }
    pub fn indexAccountBytes(self: *const Directory) u64 {
        var bytes: u64 = 0;
        inline for (.{ "tree", "ids", "bounds", "ends", "levels", "generations" }) |field| {
            if (@field(self, field).account) |account| bytes += account.bytes.load(.acquire);
        }
        return bytes;
    }

    /// Legacy payload/state cloning and pin registration. The owning admitted
    /// coordinator must replace this preparatory step with prepaid retained
    /// payload/pin work; index editing below never invokes this legacy driver.
    pub fn prepareEntryLegacy(self: *Directory, backend: anytype, run: Run) !EntryLease {
        const allocator = backend.allocator;
        const account = self.tree.account orelse return error.InvalidArgument;
        const payload = try allocator.create(Payload);
        errdefer allocator.destroy(payload);
        var owned = try repository.cloneRunCompactionSnapshot(allocator, run);
        errdefer owned.deinit(allocator);
        try owned.ensureReadIdentity();
        const pin: EntryPin = if (comptime @hasDecl(@TypeOf(backend.*), "retainDirectoryRunSnapshotPin")) try backend.retainDirectoryRunSnapshotPin(&owned) else blk: {
            try backend.retainRunSnapshotRef(&owned);
            break :blk .{ .owner = @ptrCast(backend), .release = struct {
                fn call(ctx: *anyopaque, value: *Run) void {
                    if (comptime @hasDecl(@TypeOf(backend.*), "releaseDirectoryRunSnapshotRef")) {
                        @TypeOf(backend.*).releaseDirectoryRunSnapshotRef(value);
                        return;
                    }
                    const owner: @TypeOf(backend) = @ptrCast(@alignCast(ctx));
                    owner.releaseRunSnapshotRef(value);
                }
            }.call };
        };
        var bytes: usize = @sizeOf(Payload) + owned.smallest_key.len + owned.largest_key.len;
        if (owned.path) |path| bytes += path.len;
        if (owned.smallest_namespace_name) |name| bytes += name.len;
        if (owned.largest_namespace_name) |name| bytes += name.len;
        if (owned.state) |*present| bytes += @intCast(present.estimatedMemoryBytes());
        account.charge(bytes);
        payload.* = .{ .run = owned, .owner = pin.owner, .release_pin = pin.release, .account = account, .bytes = bytes };
        const domain = if (comptime @hasField(@TypeOf(backend.*), "options")) blk: {
            if (comptime @hasField(@TypeOf(backend.options), "run_partition_key")) {
                if (backend.options.run_partition_key) |partition| break :blk partition(payload.run.smallest_key);
            }
            break :blk "";
        } else "";
        const entry = Entry{ .run = &payload.run, .payload = payload, .domain = domain };
        _ = account.retain();
        return .{ .entry = entry };
    }

    pub fn put(self: *Directory, backend: anytype, run: Run) !void {
        const allocator = backend.allocator;
        const previous = find(self.tree.root, .{ .run = &run });
        var indexes = try IndexPreparation.init(self, run.level, if (previous) |node| if (compareEnds(node.entry, .{ .run = &run }) != .eq) 2 else 1 else 1);
        try indexes.resumeWithAdmission(allocator, null);
        var prepared_entry = try self.prepareEntryLegacy(backend, run);
        defer prepared_entry.deinit(allocator);
        const entry = prepared_entry.entry;
        self.memory_run_count += @intFromBool(run.path == null);
        if (previous) |node| self.memory_run_count -= @intFromBool(node.entry.run.path == null);
        const old_bytes = if (previous) |node| node.entry.run.size_bytes else 0;
        var level = self.levelStats(run.level);
        level.count += @intFromBool(previous == null);
        level.bytes = level.bytes - old_bytes + run.size_bytes;
        level.tombstone_runs += @intFromBool((run.tombstone_count orelse 0) != 0);
        if (previous) |node| level.tombstone_runs -= @intFromBool((node.entry.run.tombstone_count orelse 0) != 0);
        self.total_run_bytes = self.total_run_bytes - old_bytes + run.size_bytes;
        // Replacing metadata may also change an upper endpoint without
        // changing the read-order key. Remove its old secondary key first.
        if (previous) |node| if (compareEnds(node.entry, entry) != .eq) self.ends.removePrepared(allocator, node.entry);
        self.levels.putPrepared(allocator, level);
        if (run.level == 0) {
            const key = generation_index.Generation{ .sequence = generation_index.sequence(run) };
            var generation = if (generation_index.Tree.find(self.generations.root, key)) |node| node.entry else key;
            generation.files += @intFromBool(previous == null);
            generation.bytes = generation.bytes - old_bytes + run.size_bytes;
            self.generations.putPrepared(allocator, generation);
        }
        self.tree.putPrepared(allocator, entry);
        self.ids.putPrepared(allocator, entry);
        self.bounds.putPrepared(allocator, entry);
        self.ends.putPrepared(allocator, entry);
    }
    pub fn remove(self: *Directory, allocator: std.mem.Allocator, run: *const Run) !void {
        var indexes = try IndexPreparation.init(self, run.level, 1);
        try indexes.resumeWithAdmission(allocator, null);
        const existing = find(self.tree.root, .{ .run = run }) orelse return;
        // All run indexes own this payload until their prepared edits finish.
        const entry = existing.entry.retainShared();
        defer entry.deinit(allocator);
        self.memory_run_count -= @intFromBool(entry.run.path == null);
        var level = self.levelStats(entry.run.level);
        level.count -= 1;
        level.bytes -= entry.run.size_bytes;
        level.tombstone_runs -= @intFromBool((entry.run.tombstone_count orelse 0) != 0);
        self.total_run_bytes -= entry.run.size_bytes;
        if (level.count == 0) self.levels.removePrepared(allocator, level) else self.levels.putPrepared(allocator, level);
        if (entry.run.level == 0) {
            var generation = generation_index.Tree.find(self.generations.root, .{ .sequence = generation_index.sequence(entry.run.*) }).?.entry;
            generation.files -= 1;
            generation.bytes -= entry.run.size_bytes;
            if (generation.files == 0) self.generations.removePrepared(allocator, generation) else self.generations.putPrepared(allocator, generation);
        }
        self.tree.removePrepared(allocator, entry);
        self.ids.removePrepared(allocator, entry);
        self.bounds.removePrepared(allocator, entry);
        self.ends.removePrepared(allocator, entry);
    }
    pub fn count(self: *const Directory) usize {
        return if (self.tree.root) |root| root.count else 0;
    }

    /// Handles borrow immutable payloads from a pinned directory, never slots
    /// in the mutable Backend.runs array. A move/replacement changes identity.
    pub const Handle = struct {
        run: *const Run,
        revision: *const anyopaque,

        pub fn retain(self: @This()) @This() {
            const payload: *Payload = @ptrCast(@alignCast(@constCast(self.revision)));
            _ = payload.account.retain();
            _ = payload.refs.fetchAdd(1, .monotonic);
            return self;
        }

        pub fn release(self: @This(), allocator: std.mem.Allocator) void {
            const payload: *Payload = @ptrCast(@alignCast(@constCast(self.revision)));
            const account = payload.account;
            (Entry{ .run = self.run, .payload = payload }).deinit(allocator);
            account.release();
        }
        pub fn accountedMemoryBytes(self: @This(), pass: u64) u64 {
            const payload: *const Payload = @ptrCast(@alignCast(self.revision));
            return payload.account.chargeOnce(pass);
        }
        pub fn retainAccounting(self: @This()) *Account {
            const payload: *const Payload = @ptrCast(@alignCast(self.revision));
            return payload.account.retain();
        }
    };

    pub fn at(self: *const Directory, rank: usize) Handle {
        const entry = self.tree.root.?.at(rank);
        return .{ .run = entry.run, .revision = entry.payload.? };
    }

    pub fn readLess(_: void, a: Handle, b: Handle) bool {
        return compare(.{ .run = a.run }, .{ .run = b.run }) == .lt;
    }

    /// Walk the read-order tree directly, pruning disjoint subtrees by their
    /// aggregate bounds. No selected-run array, sort or topology is needed.
    /// The caller keeps the immutable directory pinned for the cursor lifetime.
    pub const ReadPointCursor = struct {
        path: [2 * @bitSizeOf(usize)]*const Tree.Node = undefined,
        len: usize = 0,
        descend: ?*const Tree.Node,
        namespace: ?[]const u8,
        key: []const u8,
        visited: usize = 0,

        pub fn next(self: *@This()) ?Handle {
            var budget: usize = std.math.maxInt(usize);
            return self.nextWithBudget(&budget);
        }

        pub fn done(self: *const @This()) bool {
            return self.descend == null and self.len == 0;
        }

        /// A null result can mean quantum exhaustion; consult done before
        /// concluding absence. Both descents and pops spend metadata credits.
        pub fn nextWithBudget(self: *@This(), budget: *usize) ?Handle {
            while (true) {
                while (self.descend) |node| {
                    if (budget.* == 0) return null;
                    budget.* -= 1;
                    self.visited += 1;
                    const lower = node.summary.smallest.?;
                    const upper = node.summary.largest.?;
                    if (compareBound(self.namespace, self.key, lower.smallest_namespace_name, lower.smallest_key) == .lt or
                        compareBound(self.namespace, self.key, upper.largest_namespace_name, upper.largest_key) == .gt)
                    {
                        self.descend = null;
                        break;
                    }
                    self.path[self.len] = node;
                    self.len += 1;
                    self.descend = node.left;
                }
                if (self.len == 0) return null;
                if (budget.* == 0) return null;
                budget.* -= 1;
                self.len -= 1;
                const node = self.path[self.len];
                self.descend = node.right;
                const run = node.entry.run;
                if (compareBound(self.namespace, self.key, run.smallest_namespace_name, run.smallest_key) != .lt and
                    compareBound(self.namespace, self.key, run.largest_namespace_name, run.largest_key) != .gt)
                    return .{ .run = run, .revision = node.entry.payload.? };
            }
        }
    };

    pub fn readPoint(self: *const Directory, namespace: ?[]const u8, key: []const u8) ReadPointCursor {
        return .{ .descend = self.tree.root, .namespace = namespace, .key = key };
    }

    pub fn supportsAsyncPoints(self: *const Directory) bool {
        // L0 is ordered by visibility rather than key. Bound the unordered
        // search cost; larger backlogs retain the bounds-index planner.
        if (self.levelStats(0).count > 64) return false;
        return if (self.tree.root) |root| root.summary.non_async_runs == 0 else true;
    }

    pub const Cursor = struct {
        directory: *const Directory,
        rank: usize = 0,
        path: Tree.Cursor = .{},

        pub fn next(self: *@This()) ?Handle {
            if (self.rank == self.directory.count()) return null;
            const entry = self.path.at(self.directory.tree.root.?, self.rank);
            self.rank += 1;
            return .{ .run = entry.run, .revision = entry.payload.? };
        }
    };

    pub fn readCursor(self: *const Directory) Cursor {
        return .{ .directory = self };
    }

    /// First candidate rank (forward), or one past the last candidate
    /// (reverse), in a disjoint lower level. One AVL descent replaces binary
    /// search over rank lookups, which would otherwise cost O(log² N).
    pub fn levelBoundRank(self: *const Directory, level: u32, namespace: ?[]const u8, key: ?[]const u8, reverse: bool, inclusive: bool) usize {
        std.debug.assert(level != 0);
        var root = self.tree.root;
        var rank: usize = 0;
        while (root) |node| {
            const run = node.entry.run;
            const order = if (key) |target|
                compareBound(if (reverse) run.smallest_namespace_name else run.largest_namespace_name, if (reverse) run.smallest_key else run.largest_key, namespace, target)
            else
                state.compareNamespace(.{ .name = run.smallest_namespace_name }, .{ .name = namespace });
            const past = order == .lt or (order == .eq and (if (reverse) key == null or inclusive else !inclusive));
            if (run.level < level or (run.level == level and past)) {
                rank += 1 + (if (node.left) |left| left.count else 0);
                root = node.right;
            } else root = node.left;
        }
        return rank;
    }

    pub fn resolve(self: *const Directory, handle: Handle) ?usize {
        const node = find(self.tree.root, .{ .run = handle.run }) orelse return null;
        if (@as(*const anyopaque, node.entry.payload.?) != handle.revision) return null;
        return self.tree.root.?.lowerBound(node.entry);
    }

    pub fn rankOf(self: *const Directory, run: *const Run) ?usize {
        const node = find(self.tree.root, .{ .run = run }) orelse return null;
        return self.tree.root.?.lowerBound(node.entry);
    }

    pub const OverlapCursor = struct {
        stack: [2 * @bitSizeOf(usize)]*const BoundsTree.Node = undefined,
        len: usize = 0,
        lower_ns: ?[]const u8,
        lower: []const u8,
        upper_ns: ?[]const u8,
        upper: []const u8,
        visited: usize = 0,

        /// A budget counts visited nodes, not just matches. Call again with a
        /// replenished budget to resume a large overlap without restarting.
        pub fn next(self: *@This(), budget: *usize) ?Handle {
            while (self.len != 0 and budget.* != 0) {
                budget.* -= 1;
                self.visited += 1;
                self.len -= 1;
                const node = self.stack[self.len];
                const largest = node.summary.largest.?;
                if (compareBound(largest.largest_namespace_name, largest.largest_key, self.lower_ns, self.lower) == .lt) continue;
                const run = node.entry.run;
                const starts_before_end = compareBound(run.smallest_namespace_name, run.smallest_key, self.upper_ns, self.upper) != .gt;
                if (starts_before_end) if (node.right) |right| {
                    self.stack[self.len] = right;
                    self.len += 1;
                };
                if (node.left) |left| {
                    self.stack[self.len] = left;
                    self.len += 1;
                }
                if (starts_before_end and compareBound(run.largest_namespace_name, run.largest_key, self.lower_ns, self.lower) != .lt)
                    return .{ .run = run, .revision = node.entry.payload.? };
            }
            return null;
        }

        pub fn done(self: *const @This()) bool {
            return self.len == 0;
        }
    };

    /// Visit the union of sorted point queries once. Ancestor start bounds and
    /// subtree maximum ends prune gaps between sparse keys as well as the
    /// outer range. Handles are borrowed from this pinned directory.
    pub const SortedPointCursor = struct {
        const Frame = struct { node: *const BoundsTree.Node, lower: ?*const Run };
        stack: [2 * @bitSizeOf(usize)]Frame = undefined,
        len: usize = 0,
        namespace: ?[]const u8,
        keys: []const []const u8,
        visited: usize = 0,

        fn lowerBound(self: *const @This(), ns: ?[]const u8, key: []const u8) usize {
            var lo: usize = 0;
            var hi = self.keys.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (compareBound(self.namespace, self.keys[mid], ns, key) == .lt) lo = mid + 1 else hi = mid;
            }
            return lo;
        }

        pub fn next(self: *@This(), budget: *usize) ?Handle {
            while (self.len != 0 and budget.* != 0) {
                budget.* -= 1;
                self.visited += 1;
                self.len -= 1;
                const frame = self.stack[self.len];
                const node = frame.node;
                const largest = node.summary.largest.?;
                const first = if (frame.lower) |lower| self.lowerBound(lower.smallest_namespace_name, lower.smallest_key) else 0;
                if (first == self.keys.len or compareBound(self.namespace, self.keys[first], largest.largest_namespace_name, largest.largest_key) == .gt) continue;
                const run = node.entry.run;
                const key_index = self.lowerBound(run.smallest_namespace_name, run.smallest_key);
                if (key_index != self.keys.len) if (node.right) |right| {
                    self.stack[self.len] = .{ .node = right, .lower = run };
                    self.len += 1;
                };
                if (node.left) |left| {
                    self.stack[self.len] = .{ .node = left, .lower = frame.lower };
                    self.len += 1;
                }
                if (key_index != self.keys.len and compareBound(self.namespace, self.keys[key_index], run.largest_namespace_name, run.largest_key) != .gt)
                    return .{ .run = run, .revision = node.entry.payload.? };
            }
            return null;
        }
        pub fn done(self: *const @This()) bool {
            return self.len == 0;
        }
    };

    pub fn sortedPoints(self: *const Directory, namespace: ?[]const u8, keys: []const []const u8) SortedPointCursor {
        var cursor = SortedPointCursor{ .namespace = namespace, .keys = keys };
        if (keys.len != 0) if (self.bounds.root) |root| {
            cursor.stack[0] = .{ .node = root, .lower = null };
            cursor.len = 1;
        };
        return cursor;
    }

    pub fn overlaps(self: *const Directory, lower_ns: ?[]const u8, lower: []const u8, upper_ns: ?[]const u8, upper: []const u8) OverlapCursor {
        var cursor = OverlapCursor{ .lower_ns = lower_ns, .lower = lower, .upper_ns = upper_ns, .upper = upper };
        if (self.bounds.root) |root| {
            cursor.stack[0] = root;
            cursor.len = 1;
        }
        return cursor;
    }

    /// After the initial overlap query, every unseen interval lies strictly
    /// left (end < initial lower) or right (start > initial upper). Walk those
    /// endpoints once as the closure expands. A separate end index prevents
    /// rescanning long/nested intervals on leftward expansion. Both indexes
    /// share immutable payloads; the cursors allocate nothing and charge each
    /// seek/descent to the caller's quantum.
    pub fn FrontierCursor(comptime reverse: bool) type {
        return struct {
            const Node = if (reverse) EndsTree.Node else BoundsTree.Node;
            path: [2 * @bitSizeOf(usize)]*const Node = undefined,
            len: usize = 0,
            descend: ?*const Node,
            seeking: bool = true,
            initial_ns: ?[]const u8,
            initial: []const u8,
            caught_up: bool = false,

            pub fn init(directory: *const Directory, namespace: ?[]const u8, key: []const u8) @This() {
                return .{ .descend = if (reverse) directory.ends.root else directory.bounds.root, .initial_ns = namespace, .initial = key };
            }

            pub fn next(self: *@This(), namespace: ?[]const u8, key: []const u8, credits: *usize) ?Handle {
                self.caught_up = false;
                while (credits.* != 0) {
                    credits.* -= 1;
                    if (self.descend) |node| {
                        const run = node.entry.run;
                        const qualifies = !self.seeking or compareBound(if (reverse) run.largest_namespace_name else run.smallest_namespace_name, if (reverse) run.largest_key else run.smallest_key, self.initial_ns, self.initial) == (if (reverse) std.math.Order.lt else .gt);
                        if (qualifies) {
                            self.path[self.len] = node;
                            self.len += 1;
                            self.descend = if (reverse) node.right else node.left;
                        } else self.descend = if (reverse) node.left else node.right;
                        continue;
                    }
                    self.seeking = false;
                    if (self.len == 0) {
                        self.caught_up = true;
                        return null;
                    }
                    const node = self.path[self.len - 1];
                    const run = node.entry.run;
                    const order = compareBound(if (reverse) run.largest_namespace_name else run.smallest_namespace_name, if (reverse) run.largest_key else run.smallest_key, namespace, key);
                    if (order == (if (reverse) std.math.Order.lt else .gt)) {
                        self.caught_up = true;
                        return null;
                    }
                    self.len -= 1;
                    self.descend = if (reverse) node.left else node.right;
                    return .{ .run = run, .revision = node.entry.payload.? };
                }
                return null;
            }
        };
    }

    pub fn levelStats(self: *const Directory, number: u32) LevelAggregate {
        const root = self.levels.root orelse return .{ .level = number };
        const rank = root.lowerBound(.{ .level = number });
        if (rank < root.count) {
            const present = root.at(rank);
            if (present.level == number) return present;
        }
        return .{ .level = number };
    }

    pub fn levelCount(self: *const Directory) usize {
        return if (self.levels.root) |root| root.count else 0;
    }

    pub fn levelStart(self: *const Directory, level: u32) usize {
        var start: usize = 0;
        for (0..self.levelCount()) |rank| {
            const item = self.levelAt(rank);
            if (item.level >= level) break;
            start += item.count;
        }
        return start;
    }

    pub fn levelAt(self: *const Directory, rank: usize) LevelAggregate {
        return self.levels.root.?.at(rank);
    }

    pub fn maxLevel(self: *const Directory) u32 {
        return if (self.levels.root) |root| root.at(root.count - 1).level else 0;
    }

    /// Visit only changed search paths. Looking up shared subtree roots in the
    /// other version also skips unchanged subtrees after AVL rotations.
    /// The visitor borrows runs from both pinned roots for the call's lifetime.
    pub fn changesSince(self: *const Directory, previous: *const Directory, visitor: anytype) !void {
        try visitRemoved(previous.tree.root, self.tree.root, visitor);
        try visitAdded(self.tree.root, previous.tree.root, visitor);
    }

    /// Allocation-free persistent-root diff. Shared subtrees are skipped even
    /// across rotations; callers pin both roots and pay one credit per node.
    pub const ChangeCursor = struct {
        pub const Change = struct { kind: enum { remove, put }, run: *const Run };
        previous: ?*Tree.Node,
        current: ?*Tree.Node,
        stack: [2 * @bitSizeOf(usize)]*Tree.Node = undefined,
        len: usize = 0,
        phase: enum { removed, added, done } = .removed,

        pub fn init(previous: *const Directory, current: *const Directory) ChangeCursor {
            var cursor = ChangeCursor{ .previous = previous.tree.root, .current = current.tree.root };
            cursor.push(previous.tree.root);
            return cursor;
        }
        fn push(self: *ChangeCursor, node: ?*Tree.Node) void {
            if (node) |value| {
                self.stack[self.len] = value;
                self.len += 1;
            }
        }
        pub fn done(self: *const ChangeCursor) bool {
            return self.phase == .done;
        }
        pub fn next(self: *ChangeCursor, credits: *usize) ?Change {
            while (credits.* != 0 and !self.done()) {
                if (self.len == 0) {
                    if (self.phase == .removed) {
                        self.phase = .added;
                        self.push(self.current);
                    } else self.phase = .done;
                    continue;
                }
                credits.* -= 1;
                self.len -= 1;
                const node = self.stack[self.len];
                const matched = find(if (self.phase == .removed) self.current else self.previous, node.entry);
                if (matched == node) continue;
                self.push(node.right);
                self.push(node.left);
                if (self.phase == .removed) {
                    if (matched == null) return .{ .kind = .remove, .run = node.entry.run };
                } else if (matched == null or matched.?.entry.payload != node.entry.payload) {
                    return .{ .kind = .put, .run = node.entry.run };
                }
            }
            return null;
        }
    };

    /// A validated predecessor remains valid after removals. Only inserted or
    /// replaced runs and their immediate final neighbors can introduce a new
    /// ordering/overlap violation, including both sides of a level move.
    pub fn validateChangesSince(self: *const Directory, previous: *const Directory, validate: *const fn ([]const Run) anyerror!void) !void {
        const Check = struct {
            directory: *const Directory,
            validate: *const fn ([]const Run) anyerror!void,
            pub fn put(check: *@This(), run: Run) !void {
                const root = check.directory.tree.root.?;
                const rank = root.lowerBound(.{ .run = &run });
                const start = rank -| 1;
                const end = @min(root.count, rank + 2);
                var neighbors: [3]Run = undefined;
                for (start..end, 0..) |position, i| neighbors[i] = root.at(position).run.*;
                try check.validate(neighbors[0 .. end - start]);
            }
        };
        var check = Check{ .directory = self, .validate = validate };
        try visitAdded(self.tree.root, previous.tree.root, &check);
    }

    fn find(root: ?*Tree.Node, entry: Entry) ?*Tree.Node {
        var search = ReadSearch.init(root, entry);
        search.resumeWithAdmission(null) catch unreachable;
        return if (search.matched) |node| @constCast(node) else null;
    }

    fn visitRemoved(root: ?*Tree.Node, other: ?*Tree.Node, visitor: anytype) !void {
        const node = root orelse return;
        const matched = find(other, node.entry);
        if (matched == node) return;
        try visitRemoved(node.left, other, visitor);
        if (matched == null) try visitor.remove(node.entry.run.*);
        try visitRemoved(node.right, other, visitor);
    }

    fn visitAdded(root: ?*Tree.Node, other: ?*Tree.Node, visitor: anytype) !void {
        const node = root orelse return;
        const matched = find(other, node.entry);
        if (matched == node) return;
        try visitAdded(node.left, other, visitor);
        if (matched == null or matched.?.entry.payload != node.entry.payload) try visitor.put(node.entry.run.*);
        try visitAdded(node.right, other, visitor);
    }
    pub fn project(self: *const Directory, allocator: std.mem.Allocator) ![]Run {
        return self.projectWithAdmission(allocator, null);
    }

    pub fn projectWithAdmission(self: *const Directory, allocator: std.mem.Allocator, control: ?@import("../../common/read_admission.zig").Port) ![]Run {
        if (control) |owner| try owner.checkpoint();
        const runs = try allocator.alloc(Run, self.count());
        errdefer allocator.free(runs);
        var cursor: Tree.Cursor = .{};
        for (runs, 0..) |*run, i| {
            if (control) |owner| try owner.checkpoint();
            run.* = cursor.at(self.tree.root.?, i).run.*;
            run.owns_metadata = false;
            run.owns_path = false;
            run.owns_bloom_filter = false;
            run.version_ref_pinned = false;
            run.shared_read_version = true;
        }
        if (control) |owner| try owner.checkpoint();
        return runs;
    }
    pub fn accountedMemoryBytes(self: *const Directory, pass: u64) u64 {
        return @sizeOf(Directory) + (self.tree.spare.capacity + self.ids.spare.capacity + self.bounds.spare.capacity + self.ends.spare.capacity + self.levels.spare.capacity + self.generations.spare.capacity) * @sizeOf(*Tree.Node) +
            (if (self.generations.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.tree.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.ids.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.bounds.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.ends.account) |account| account.chargeOnce(pass) else 0) +
            (if (self.levels.account) |account| account.chargeOnce(pass) else 0);
    }

    pub const PlanningOrder = struct { domain: []usize, bounds: []usize };

    pub fn byId(self: *const Directory, id: u64) ?*const Run {
        const probe: Run = .{ .id = id, .level = 0, .size_bytes = 0, .path = null, .smallest_namespace_name = null, .smallest_key = &.{}, .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 0, .bloom_filter = null, .state = null, .owns_metadata = false };
        var search = self.locateId(&probe);
        search.resumeWithAdmission(null) catch unreachable;
        return if (search.matched) |node| node.entry.run else null;
    }

    pub fn tombstoneGcDelay(self: *const Directory, age: u64, now: u64) ?u64 {
        const summary = (self.tree.root orelse return null).summary;
        if (summary.gc_requested) return 0;
        if (summary.tombstone_runs == 0) return null;
        if (age == 0) return null;
        if (summary.oldest_tombstone == 0 or summary.newest_tombstone > now) return 0;
        return (summary.oldest_tombstone +| age) -| now;
    }

    pub fn hasGcRequest(self: *const Directory) bool {
        return if (self.tree.root) |root| root.summary.gc_requested else false;
    }

    pub fn tombstoneRunCount(self: *const Directory) usize {
        return if (self.tree.root) |root| root.summary.tombstone_runs else 0;
    }

    pub fn unknownTombstoneRunCount(self: *const Directory) usize {
        return if (self.tree.root) |root| root.summary.unknown_tombstone_runs else 0;
    }
    pub fn nextUnknownTombstone(self: *const Directory, after: usize) ?Handle {
        const found = nextUnknown(self.tree.root, after, 0) orelse return null;
        return .{ .run = found.entry.run, .revision = found.entry.payload.? };
    }
    fn nextUnknown(root: ?*const Tree.Node, after: usize, base: usize) ?*const Tree.Node {
        const node = root orelse return null;
        if (node.summary.unknown_tombstone_runs == 0 or base + node.count <= after) return null;
        const rank = base + (if (node.left) |left| left.count else 0);
        if (nextUnknown(node.left, after, base)) |found| return found;
        if (rank >= after and node.entry.run.tombstone_count == null) return node;
        return nextUnknown(node.right, after, rank + 1);
    }
    pub fn generationCount(self: *const Directory) usize {
        return if (self.generations.root) |root| root.count else 0;
    }
    pub fn generationPrefix(self: *const Directory, count_arg: usize) generation_index.Generation.Summary {
        return generation_index.prefix(self.generations.root, count_arg);
    }

    pub const TombstoneCursor = struct {
        directory: *const Directory,
        rank: usize = 0,
        include_requests: bool = false,

        pub fn next(self: *@This()) ?Handle {
            const found = nextMarked(self.directory.tree.root, self.rank, 0, self.include_requests) orelse return null;
            self.rank = found.rank + 1;
            return .{ .run = found.node.entry.run, .revision = found.node.entry.payload.? };
        }
        const Found = struct { node: *const Tree.Node, rank: usize };
        fn nextMarked(root: ?*const Tree.Node, after: usize, base: usize, requests: bool) ?Found {
            const node = root orelse return null;
            if ((node.summary.tombstone_runs == 0 and !(requests and node.summary.gc_requested)) or base + node.count <= after) return null;
            const rank = base + (if (node.left) |left| left.count else 0);
            if (nextMarked(node.left, after, base, requests)) |found| return found;
            if (rank >= after and ((node.entry.run.tombstone_count orelse 0) != 0 or (requests and node.entry.run.gc_requested))) return .{ .node = node, .rank = rank };
            return nextMarked(node.right, after, rank + 1, requests);
        }
    };

    /// Skip tombstone-free subtrees when finding component anchors. Iteration
    /// preserves read precedence, unlike a pre-order walk of marked nodes.
    pub fn tombstoneCursor(self: *const Directory) TombstoneCursor {
        return .{ .directory = self };
    }

    /// Diagnostic/oracle adapter only. Production selection walks immutable
    /// overlap cursors; do not pay for a separate domain-order tree on every
    /// publication just to accelerate an exceptional full projection.
    pub fn planningOrder(self: *const Directory, allocator: std.mem.Allocator) !PlanningOrder {
        const domain = try allocator.alloc(usize, self.count());
        errdefer allocator.free(domain);
        const ordered_bounds = try allocator.alloc(usize, self.count());
        errdefer allocator.free(ordered_bounds);
        if (self.count() == 0) return .{ .domain = domain, .bounds = ordered_bounds };
        const entries = try allocator.alloc(Entry, self.count());
        defer allocator.free(entries);
        var by_level: Tree.Cursor = .{};
        var one_domain = true;
        for (entries, 0..) |*entry, i| {
            entry.* = by_level.at(self.tree.root.?, i);
            if (state.compareNamespace(.{ .name = entries[0].run.smallest_namespace_name }, .{ .name = entry.run.smallest_namespace_name }) != .eq or !std.mem.eql(u8, entries[0].domain, entry.domain)) one_domain = false;
            domain[i] = i;
        }
        if (!one_domain) std.mem.sort(usize, domain, entries, struct {
            fn less(all: []Entry, a: usize, b: usize) bool {
                return compareDomain(all[a], all[b]) == .lt;
            }
        }.less);
        const one_sorted_level = self.levelCount() == 1 and self.levelAt(0).level != 0;
        if (one_domain and one_sorted_level) {
            for (0..self.count()) |i| {
                domain[i] = i;
                ordered_bounds[i] = i;
            }
            return .{ .domain = domain, .bounds = ordered_bounds };
        }
        var positions: std.AutoHashMapUnmanaged(u64, usize) = .empty;
        defer positions.deinit(allocator);
        try positions.ensureTotalCapacity(allocator, @intCast(self.count()));
        for (entries, 0..) |entry, i| positions.putAssumeCapacity(entry.run.id, i);
        var by_bounds: BoundsTree.Cursor = .{};
        for (0..self.count()) |i| {
            ordered_bounds[i] = if (one_sorted_level) i else positions.get(by_bounds.at(self.bounds.root.?, i).run.id).?;
        }
        return .{ .domain = domain, .bounds = ordered_bounds };
    }
};

test "run directory endpoint replacement preserves old cursors and removes old secondary keys" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
        fn check(allocator: std.mem.Allocator) !void {
            var fixture = @This(){ .allocator = allocator };
            const original = try Directory.create(allocator);
            defer original.destroy(allocator);
            var run = Run{ .id = 1, .level = 1, .size_bytes = 1, .path = @constCast("end.sst"), .smallest_namespace_name = null, .smallest_key = @constCast("a"), .largest_namespace_name = null, .largest_key = @constCast("b"), .entry_count = 1, .bloom_filter = null, .state = null };
            try original.put(&fixture, run);
            const changed = try original.fork(allocator);
            defer changed.destroy(allocator);
            var old_cursor = Directory.FrontierCursor(true).init(original, null, "e");
            run.largest_key = @constCast("d");
            try changed.put(&fixture, run);
            try std.testing.expectEqual(@as(usize, 1), changed.ends.root.?.count);
            var cursor = Directory.FrontierCursor(true).init(changed, null, "e");
            var credits: usize = 100;
            try std.testing.expectEqualStrings("d", cursor.next(null, "a", &credits).?.run.largest_key);
            try std.testing.expect(cursor.next(null, "a", &credits) == null);
            try std.testing.expectEqualStrings("b", old_cursor.next(null, "a", &credits).?.run.largest_key);
            try changed.remove(allocator, &run);
            try std.testing.expect(changed.ends.root == null);
            try std.testing.expectEqual(@as(usize, 1), original.count());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}

test "run directory path copies preserve pinned epochs through inserts removals and OOM" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pins: usize = 0,
        pub fn retainRunSnapshotRef(self: *@This(), run: *Run) !void {
            self.pins += 1;
            run.version_ref_pinned = true;
        }
        pub fn releaseRunSnapshotRef(self: *@This(), run: *Run) void {
            std.debug.assert(run.version_ref_pinned);
            self.pins -= 1;
            run.version_ref_pinned = false;
        }
    };
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var backend = Fixture{ .allocator = allocator };
    const original = try Directory.create(allocator);
    defer original.destroy(allocator);
    for (0..256) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .big);
        try original.put(&backend, .{ .id = i + 1, .level = 1, .size_bytes = 1, .path = @constCast("test.sst"), .smallest_namespace_name = null, .smallest_key = &key, .largest_namespace_name = null, .largest_key = &key, .entry_count = 1, .bloom_filter = null, .state = null });
    }
    const baseline_pins = backend.pins;
    const snapshot = try original.fork(allocator);
    defer snapshot.destroy(allocator);
    try std.testing.expectEqual(baseline_pins, backend.pins);
    const projected = try snapshot.project(allocator);
    defer allocator.free(projected);
    {
        const gc = try snapshot.fork(allocator);
        defer gc.destroy(allocator);
        try std.testing.expectEqual(@as(?u64, null), gc.tombstoneGcDelay(100, 1000));
        var old = projected[0];
        old.tombstone_count = 1;
        old.oldest_tombstone_unix_ns = 950;
        try gc.put(&backend, old);
        try std.testing.expectEqual(@as(?u64, 50), gc.tombstoneGcDelay(100, 1000));
        try std.testing.expectEqual(@as(?u64, 0), gc.tombstoneGcDelay(100, 1100));
        try std.testing.expectEqual(@as(?u64, null), gc.tombstoneGcDelay(0, 1000));
        var future = projected[1];
        future.tombstone_count = 1;
        future.oldest_tombstone_unix_ns = 1001;
        try gc.put(&backend, future);
        var deletes = gc.tombstoneCursor();
        try std.testing.expectEqual(old.id, deletes.next().?.run.id);
        try std.testing.expectEqual(future.id, deletes.next().?.run.id);
        try std.testing.expect(deletes.next() == null);
        try std.testing.expect(deletes.next() == null);
        try std.testing.expectEqual(@as(?u64, 0), gc.tombstoneGcDelay(100, 1000));
        try gc.remove(allocator, &future);
        try std.testing.expectEqual(@as(?u64, 50), gc.tombstoneGcDelay(100, 1000));
        old.gc_requested = true;
        try gc.put(&backend, old);
        try std.testing.expectEqual(@as(?u64, 0), gc.tombstoneGcDelay(0, 1000));
        old.gc_requested = false;
        old.oldest_tombstone_unix_ns = 0;
        try gc.put(&backend, old);
        try std.testing.expectEqual(@as(?u64, 0), gc.tombstoneGcDelay(100, 1000));
        old.tombstone_count = 0;
        old.gc_requested = true;
        try gc.put(&backend, old);
        try std.testing.expectEqual(@as(?u64, 0), gc.tombstoneGcDelay(100, 1000));
        try std.testing.expectEqual(@as(?u64, null), snapshot.tombstoneGcDelay(100, 1000));
        var no_deletes = snapshot.tombstoneCursor();
        try std.testing.expect(no_deletes.next() == null);
    }
    {
        const detached = try Directory.create(allocator);
        var alive = true;
        defer if (alive) detached.destroy(allocator);
        try detached.put(&backend, projected[0]);
        const handle = detached.at(0).retain();
        defer handle.release(allocator);
        detached.destroy(allocator);
        alive = false;
        // A selected handle owns both its payload and accounting lifetime,
        // even after every directory from that lineage has been reclaimed.
        try std.testing.expectEqual(projected[0].id, handle.run.id);
        try std.testing.expectEqualStrings(projected[0].smallest_key, handle.run.smallest_key);
    }
    const candidate = try original.fork(allocator);
    defer candidate.destroy(allocator);
    for (projected, 0..) |*run, i| if (i % 2 == 0) {
        try candidate.remove(allocator, run);
    };
    try std.testing.expectEqual(@as(usize, 128), candidate.count());
    try std.testing.expectEqual(@as(usize, 256), snapshot.count());
    const remaining = try candidate.project(allocator);
    defer allocator.free(remaining);
    for (remaining, 0..) |run, i| try std.testing.expectEqual(@as(u64, 2 * i + 2), run.id);
    try std.testing.expect(candidate.resolve(snapshot.at(0)) == null);
    try std.testing.expectEqual(@as(?usize, 0), candidate.resolve(snapshot.at(1)));
    var overlap = candidate.overlaps(null, projected[30].smallest_key, null, projected[40].largest_key);
    var matches: usize = 0;
    while (!overlap.done()) {
        var budget: usize = 1;
        if (overlap.next(&budget)) |handle| {
            try std.testing.expect(handle.run.id >= 31 and handle.run.id <= 41);
            try std.testing.expect(handle.run.id % 2 == 0);
            matches += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 5), matches);
    try std.testing.expect(overlap.visited < candidate.count() / 2);
    const Changes = struct {
        removed: [256]bool = @splat(false),
        pub fn put(_: *@This(), _: Run) !void {
            return error.UnexpectedAddition;
        }
        pub fn remove(self: *@This(), run: Run) !void {
            const index: usize = @intCast(run.id - 1);
            try std.testing.expect(index % 2 == 0);
            try std.testing.expect(!self.removed[index]);
            self.removed[index] = true;
        }
    };
    var changes: Changes = .{};
    // Removing alternating keys rotates shared subtrees. Diffs must skip only
    // genuinely shared roots, without hiding removals or visiting them twice.
    try candidate.changesSince(snapshot, &changes);
    for (changes.removed, 0..) |removed, i| try std.testing.expectEqual(i % 2 == 0, removed);
    var incremental = Directory.ChangeCursor.init(snapshot, candidate);
    var incremental_changes: Changes = .{};
    var no_credit: usize = 0;
    try std.testing.expect(incremental.next(&no_credit) == null);
    while (!incremental.done()) {
        var credit: usize = 1;
        if (incremental.next(&credit)) |change| switch (change.kind) {
            .remove => try incremental_changes.remove(change.run.*),
            .put => try incremental_changes.put(change.run.*),
        };
    }
    try std.testing.expectEqualSlices(bool, &changes.removed, &incremental_changes.removed);
    const Validator = struct {
        pub fn validate(runs: []const Run) !void {
            for (runs, 0..) |run, i| {
                if (run.entry_count == 0) return error.InvalidTableFile;
                if (i > 0 and std.mem.order(u8, runs[i - 1].largest_key, run.smallest_key) != .lt) return error.InvalidTableFile;
            }
        }
    };
    try candidate.validateChangesSince(snapshot, Validator.validate);
    {
        const overlapping = try candidate.fork(allocator);
        defer overlapping.destroy(allocator);
        var replacement = remaining[0];
        replacement.largest_key = remaining[1].smallest_key;
        try overlapping.put(&backend, replacement);
        try std.testing.expectError(error.InvalidTableFile, overlapping.validateChangesSince(candidate, Validator.validate));
        replacement = remaining[remaining.len - 1];
        replacement.entry_count = 0;
        const empty_run = try candidate.fork(allocator);
        defer empty_run.destroy(allocator);
        try empty_run.put(&backend, replacement);
        try std.testing.expectError(error.InvalidTableFile, empty_run.validateChangesSince(candidate, Validator.validate));
    }
    const doomed = try candidate.fork(allocator);
    failing.fail_index = failing.alloc_index;
    try std.testing.expectError(error.OutOfMemory, doomed.remove(allocator, &projected[1]));
    failing.fail_index = std.math.maxInt(usize);
    doomed.destroy(allocator);
    try std.testing.expectEqual(@as(usize, 128), candidate.count());
    for (remaining) |*run| try candidate.remove(allocator, run);
    try std.testing.expectEqual(@as(usize, 0), candidate.count());
    try std.testing.expectEqual(baseline_pins, backend.pins);
}

test "directory sorted point union prunes sparse gaps and visits overlapping runs once" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
    };
    const a = std.testing.allocator;
    var fixture = Fixture{ .allocator = a };
    const directory = try Directory.create(a);
    defer directory.destroy(a);
    var names: [1024][16]u8 = undefined;
    for (&names, 0..) |*name, i| {
        const key = try std.fmt.bufPrint(name, "key:{d:0>4}", .{i});
        try directory.put(&fixture, .{ .id = i + 1, .level = 0, .size_bytes = 1, .path = @constCast("test.sst"), .smallest_namespace_name = @constCast("docs"), .smallest_key = @constCast(key), .largest_namespace_name = @constCast("docs"), .largest_key = @constCast(key), .entry_count = 1, .bloom_filter = null, .state = null });
    }
    const keys = [_][]const u8{ "key:0000", "key:1023" };
    var cursor = directory.sortedPoints("docs", &keys);
    var count: usize = 0;
    while (!cursor.done()) {
        var budget: usize = 1;
        while (cursor.next(&budget)) |handle| {
            try std.testing.expect(handle.run.id == 1 or handle.run.id == 1024);
            count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expect(cursor.visited < 128);
    std.debug.print("directory sorted sparse points: runs=1024 matches=2 visited={d}\n", .{cursor.visited});
    const dense = [_][]const u8{ "key:0000", "key:0000", "key:0001", "key:0002" };
    var dense_cursor = directory.sortedPoints("docs", &dense);
    count = 0;
    while (!dense_cursor.done()) {
        var budget: usize = 1;
        while (dense_cursor.next(&budget)) |_| count += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), count);
    var absent = directory.sortedPoints("absent", &dense);
    var budget: usize = 16384;
    try std.testing.expect(absent.next(&budget) == null);
    var empty = directory.sortedPoints("docs", &.{});
    try std.testing.expect(empty.done());
}

test "directory sorted point union eliminates repeated overlapping candidate visits" {
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
    };
    const a = std.testing.allocator;
    var fixture = Fixture{ .allocator = a };
    const directory = try Directory.create(a);
    defer directory.destroy(a);
    for (0..32) |i| try directory.put(&fixture, .{
        .id = i + 1,
        .level = 0,
        .size_bytes = 1,
        .path = @constCast("test.sst"),
        .smallest_namespace_name = @constCast("docs"),
        .smallest_key = @constCast("a"),
        .largest_namespace_name = @constCast("docs"),
        .largest_key = @constCast("z"),
        .entry_count = 1,
        .bloom_filter = null,
        .state = null,
    });
    var keys: [32][]const u8 = undefined;
    var names: [32][16]u8 = undefined;
    for (&keys, &names, 0..) |*key, *name, i| key.* = try std.fmt.bufPrint(name, "key:{d:0>4}", .{i});
    var old_visits: usize = 0;
    for (keys) |key| {
        var point = directory.overlaps("docs", key, "docs", key);
        while (!point.done()) {
            var budget: usize = 1;
            while (point.next(&budget)) |_| {}
        }
        old_visits += point.visited;
    }
    var union_cursor = directory.sortedPoints("docs", &keys);
    var count: usize = 0;
    while (!union_cursor.done()) {
        var budget: usize = 1;
        while (union_cursor.next(&budget)) |_| count += 1;
    }
    try std.testing.expectEqual(@as(usize, 32), count);
    try std.testing.expectEqual(@as(usize, 32), union_cursor.visited);
    try std.testing.expectEqual(@as(usize, 1024), old_visits);
    std.debug.print("directory overlapping points: keys=32 runs=32 node_visits={d}->{d}\n", .{ old_visits, union_cursor.visited });
}

test "directory direct point traversal matches overlap sort and preserves forked roots" {
    const a = std.testing.allocator;
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
    };
    var fixture: Fixture = .{ .allocator = a };
    const directory = try Directory.create(a);
    defer directory.destroy(a);
    for (0..256) |i| try directory.put(&fixture, .{
        .id = i + 1,
        .visibility_id = if (i % 7 == 0) 1000 + i else 0,
        .level = @intCast(i % 4),
        .size_bytes = 1,
        .path = @constCast("point.sst"),
        .smallest_namespace_name = if (i % 2 == 0) null else @constCast("docs"),
        .largest_namespace_name = if (i % 2 == 0) null else @constCast("docs"),
        .smallest_key = @constCast(if (i % 3 == 0) "a" else "m"),
        .largest_key = @constCast(if (i % 5 == 0) "z" else "n"),
        .entry_count = 1,
        .bloom_filter = null,
        .state = null,
    });
    const pinned = try directory.fork(a);
    defer pinned.destroy(a);
    try std.testing.expect(directory.supportsAsyncPoints());
    const backlog = try pinned.fork(a);
    defer backlog.destroy(a);
    try backlog.put(&fixture, .{ .id = 600, .level = 0, .size_bytes = 1, .path = @constCast("extra.sst"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast("a"), .largest_key = @constCast("z"), .entry_count = 1, .bloom_filter = null, .state = null });
    try std.testing.expect(!backlog.supportsAsyncPoints());
    var removed = directory.at(0).retain();
    defer removed.release(a);
    try directory.remove(a, removed.run);
    try directory.put(&fixture, .{ .id = 500, .level = 0, .size_bytes = 1, .state = .{}, .path = null, .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast("a"), .largest_key = @constCast("z"), .entry_count = 1, .bloom_filter = null });
    try std.testing.expect(!directory.supportsAsyncPoints());
    try std.testing.expect(pinned.supportsAsyncPoints());
    for ([_]*const Directory{ directory, pinned }) |source| {
        for ([_]?[]const u8{ null, "docs", "other" }) |ns| {
            for ([_][]const u8{ "0", "a", "m", "n", "z", "zz" }) |key| {
                var selected: std.ArrayListUnmanaged(Directory.Handle) = .empty;
                defer selected.deinit(a);
                var oracle = source.overlaps(ns, key, ns, key);
                while (!oracle.done()) {
                    var quantum: usize = 16384;
                    while (oracle.next(&quantum)) |handle| try selected.append(a, handle);
                }
                std.mem.sort(Directory.Handle, selected.items, {}, Directory.readLess);
                var direct = source.readPoint(ns, key);
                for (selected.items) |expected| {
                    const actual = direct.next().?;
                    try std.testing.expectEqual(expected.run.id, actual.run.id);
                    try std.testing.expectEqual(expected.revision, actual.revision);
                }
                try std.testing.expect(direct.next() == null);
                for ([_]usize{ 1, 2, 7, 64 }) |limit| {
                    var bounded = source.readPoint(ns, key);
                    var zero: usize = 0;
                    try std.testing.expect(bounded.nextWithBudget(&zero) == null);
                    try std.testing.expectEqual(@as(usize, 0), bounded.visited);
                    var count: usize = 0;
                    var steps: usize = 0;
                    while (!bounded.done()) {
                        var credits = limit;
                        if (bounded.nextWithBudget(&credits)) |handle| {
                            try std.testing.expect(count < selected.items.len);
                            try std.testing.expectEqual(selected.items[count].run.id, handle.run.id);
                            try std.testing.expectEqual(selected.items[count].revision, handle.revision);
                            count += 1;
                        }
                        try std.testing.expect(credits < limit);
                        steps += 1;
                        try std.testing.expect(steps <= 2 * source.count() + 1);
                    }
                    try std.testing.expectEqual(selected.items.len, count);
                    try std.testing.expectEqual(direct.visited, bounded.visited);
                }
                if (ns != null and (std.mem.eql(u8, ns.?, "other") or std.mem.eql(u8, ns.?, "docs") and std.mem.eql(u8, key, "zz"))) try std.testing.expectEqual(@as(usize, 1), direct.visited);
            }
        }
    }
}

test "controlled physical sync retained directory rank searches preserve original rejection and pinned root identity" {
    const a = std.testing.allocator;
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pins: usize = 0,
        pub fn retainRunSnapshotRef(self: *@This(), _: *Run) !void {
            self.pins += 1;
        }
        pub fn releaseRunSnapshotRef(self: *@This(), _: *Run) void {
            self.pins -= 1;
        }
        fn check(comptime mode: run_comparison.Mode, d: *const Directory, probe: *const Run) !void {
            const tree = switch (mode) {
                .read => &d.tree,
                .id => &d.ids,
                .lower => &d.bounds,
                .upper => &d.ends,
                .domain => unreachable,
            };
            var rank: usize = 0;
            var matched: ?u64 = null;
            while (rank < d.count()) : (rank += 1) {
                const entry = tree.root.?.at(rank);
                const order = run_comparison.compare(mode, .{ .run = entry.run }, .{ .run = probe });
                if (order == .lt) continue;
                if (order == .eq) matched = entry.run.id;
                break;
            }
            const Search = switch (mode) {
                .read => Directory.ReadSearch,
                .id => Directory.IdSearch,
                .lower => Directory.LowerSearch,
                .upper => Directory.UpperSearch,
                .domain => unreachable,
            };
            const Fault = struct {
                checks: usize = 0,
                at: usize = std.math.maxInt(usize),
                failure: anyerror = error.Canceled,
                fn check(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    const at = self.checks;
                    self.checks += 1;
                    if (at == self.at) return self.failure;
                }
            };
            var checks: usize = 1;
            var target: usize = 0;
            var visited: usize = 0;
            while (target <= checks) : (target += 1) {
                var search = Search.init(tree.root, .{ .run = probe });
                var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
                    0 => error.Canceled,
                    1 => error.DeadlineExceeded,
                    else => error.FileNotFound,
                } };
                const payload: @import("../../common/read_admission.zig").Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
                const original = @import("../../common/read_admission.zig").Port.init(&payload);
                const result = search.resumeWithAdmission(original);
                if (target == 0) {
                    try result;
                    checks = fault.checks;
                    visited = search.visited;
                } else {
                    try std.testing.expectError(fault.failure, result);
                    try search.resumeWithAdmission(original);
                }
                try std.testing.expect(search.done and search.root == tree.root);
                try std.testing.expectEqual(rank, search.rank);
                try std.testing.expectEqual(matched, if (search.matched) |node| node.entry.run.id else null);
                try std.testing.expectEqual(visited, search.visited);
            }
        }
    };
    var fixture: Fixture = .{ .allocator = a };
    const d = try Directory.create(a);
    defer d.destroy(a);
    const bytes = try a.alloc(u8, 2 * run_comparison.Cursor.quantum + 3);
    defer a.free(bytes);
    @memset(bytes, 'k');
    for (0..8) |i| {
        bytes[bytes.len - 1] = @intCast('a' + i);
        try d.put(&fixture, .{ .id = 2 * i + 2, .level = @intCast(i % 2), .size_bytes = 1, .path = @constCast("/rank-search.tbl"), .smallest_namespace_name = @constCast("docs"), .smallest_key = bytes, .largest_namespace_name = @constCast("docs"), .largest_key = bytes, .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false });
    }
    const pinned = try d.fork(a);
    defer pinned.destroy(a);
    const root = pinned.tree.root;
    // The borrowed searches below apply to this pinned epoch even after live
    // metadata publishes a successor. The rank is never reused on that successor.
    bytes[bytes.len - 1] = 'x';
    try d.put(&fixture, .{ .id = 31, .level = 1, .size_bytes = 1, .path = @constCast("/rank-search.tbl"), .smallest_namespace_name = @constCast("docs"), .smallest_key = bytes, .largest_namespace_name = @constCast("docs"), .largest_key = bytes, .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false });
    try std.testing.expect(pinned.tree.root == root and d.tree.root != root);
    for (0..18) |i| {
        bytes[bytes.len - 1] = @intCast('a' + i / 2);
        const probe: Run = .{ .id = i, .level = @intCast((i / 2 + 1) % 2), .size_bytes = 1, .path = null, .smallest_namespace_name = @constCast("docs"), .smallest_key = bytes, .largest_namespace_name = @constCast("docs"), .largest_key = bytes, .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false };
        inline for (.{ run_comparison.Mode.read, .id, .lower, .upper }) |mode| try Fixture.check(mode, pinned, &probe);
    }
    for (0..pinned.count()) |i| {
        const hit = pinned.at(i).run;
        inline for (.{ run_comparison.Mode.read, .id, .lower, .upper }) |mode| try Fixture.check(mode, pinned, hit);
    }
    try std.testing.expectEqual(@as(usize, 9), fixture.pins);
    var empty: Directory = .{};
    const probe = pinned.at(0).run;
    inline for (.{ run_comparison.Mode.read, .id, .lower, .upper }) |mode| try Fixture.check(mode, &empty, probe);
}

test "controlled physical sync private node summary refresh commits once after original computation and rejects stale shapes" {
    const a = std.testing.allocator;
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
    };
    var fixture: Fixture = .{ .allocator = a };
    const d = try Directory.create(a);
    defer d.destroy(a);
    const bytes = try a.alloc(u8, 2 * run_comparison.Cursor.quantum + 5);
    defer a.free(bytes);
    @memset(bytes, 's');
    for (0..5) |i| {
        bytes[bytes.len - 1] = @intCast('a' + i);
        try d.put(&fixture, .{ .id = i + 1, .level = 1, .size_bytes = 1, .path = @constCast("/refresh.tbl"), .smallest_namespace_name = @constCast("docs"), .smallest_key = bytes, .largest_namespace_name = @constCast("docs"), .largest_key = bytes, .entry_count = 1, .tombstone_count = @intCast(i % 3), .oldest_tombstone_unix_ns = i * 11, .gc_requested = i % 2 == 1, .bloom_filter = null, .state = null, .owns_metadata = false });
    }
    inline for (.{ "tree", "bounds" }) |field| {
        const tree = &@field(d, field);
        const root = tree.root.?;
        const expected = root.summary;
        const shared = try d.fork(a);
        try std.testing.expectError(error.ResourceBudgetExceeded, root.prepareRefresh());
        shared.destroy(a);
        const Fault = struct {
            checks: usize = 0,
            at: usize = std.math.maxInt(usize),
            failure: anyerror = error.Canceled,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                const at = self.checks;
                self.checks += 1;
                if (at == self.at) return self.failure;
            }
        };
        var checkpoints: usize = 1;
        var target: usize = 0;
        while (target <= checkpoints) : (target += 1) {
            root.summary = .{}; // Private stale metadata, never visible to a reader.
            const pending = root.summary;
            var refresh = try root.prepareRefresh();
            var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
                0 => error.Canceled,
                1 => error.DeadlineExceeded,
                else => error.FileNotFound,
            } };
            const payload: @import("../../common/read_admission.zig").Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
            const original = @import("../../common/read_admission.zig").Port.init(&payload);
            const result = refresh.resumeWithAdmission(original);
            if (target == 0) {
                try result;
                checkpoints = fault.checks;
            } else {
                try std.testing.expectError(fault.failure, result);
                try std.testing.expect(!refresh.committed);
                try std.testing.expectEqualDeep(pending, root.summary);
                try refresh.resumeWithAdmission(original);
            }
            try std.testing.expect(refresh.committed);
            try std.testing.expectEqualDeep(expected, root.summary);
        }
        var stale = try root.prepareRefresh();
        const old_left = root.left;
        root.left = null;
        const result = stale.resumeWithAdmission(null);
        root.left = old_left;
        try std.testing.expectError(error.CompactionPlanningStale, result);
        try std.testing.expect(!stale.committed);
        try std.testing.expectEqualDeep(expected, root.summary);
        const Refresh = @TypeOf(stale);
        const Control = struct {
            refresh: *Refresh,
            entry: bool,
            changed: bool = false,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (!self.changed and self.refresh.preparation.cursor.comparison != null) {
                    self.changed = true;
                    const node = self.refresh.node;
                    if (self.entry) node.entry.run = node.left.?.entry.run else node.left.?.summary.largest = null;
                }
            }
        };
        for ([_]bool{ false, true }) |change_entry| {
            var refresh = try root.prepareRefresh();
            var control: Control = .{ .refresh = &refresh, .entry = change_entry };
            const payload: @import("../../common/read_admission.zig").Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check } };
            const original_entry = root.entry.run;
            const original_left_summary = root.left.?.summary;
            const invalidated = refresh.resumeWithAdmission(@import("../../common/read_admission.zig").Port.init(&payload));
            root.entry.run = original_entry;
            root.left.?.summary = original_left_summary;
            try std.testing.expect(control.changed and !refresh.committed);
            try std.testing.expectError(error.CompactionPlanningStale, invalidated);
            try std.testing.expectEqualDeep(expected, root.summary);
        }
    }
}

test "controlled physical sync retained rank insertions preserve pinned epochs through every rotation and rejection" {
    const a = std.testing.allocator;
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pins: usize = 0,
        pub fn retainRunSnapshotRef(self: *@This(), _: *Run) !void {
            self.pins += 1;
        }
        pub fn releaseRunSnapshotRef(self: *@This(), _: *Run) void {
            self.pins -= 1;
        }
    };
    const Fault = struct {
        checks: usize = 0,
        at: usize = std.math.maxInt(usize),
        failure: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const at = self.checks;
            self.checks += 1;
            if (at == self.at) return self.failure;
        }
    };
    var fixture: Fixture = .{ .allocator = a };
    const d = try Directory.create(a);
    defer d.destroy(a);
    const bytes = try a.alloc(u8, 2 * run_comparison.Cursor.quantum + 5);
    defer a.free(bytes);
    @memset(bytes, 'i');
    for (0..3) |i| {
        bytes[bytes.len - 1] = @intCast('a' + i);
        try d.put(&fixture, .{ .id = i + 1, .level = 1, .size_bytes = 1, .path = @constCast("/rank-insert.tbl"), .smallest_namespace_name = @constCast("docs"), .smallest_key = bytes, .largest_namespace_name = @constCast("docs"), .largest_key = bytes, .entry_count = 1, .tombstone_count = @intCast(i % 3), .oldest_tombstone_unix_ns = i * 11, .gc_requested = i % 2 == 1, .bloom_filter = null, .state = null, .owns_metadata = false });
    }
    inline for (.{ Directory.Tree, Directory.BoundsTree, Directory.IdTree }) |Tree| {
        for ([_][3]usize{ .{ 2, 1, 0 }, .{ 0, 1, 2 }, .{ 2, 0, 1 }, .{ 0, 2, 1 } }) |order| {
            var base: Tree = .{};
            defer base.deinit(a);
            for (order[0..2]) |i| {
                const entry = d.tree.root.?.at(i);
                try base.prepare(a);
                base.putPrepared(a, entry);
            }
            const old_root = base.root;
            const old_summary = old_root.?.summary;
            const entry = d.tree.root.?.at(order[2]);
            var checks: usize = 1;
            var target: usize = 0;
            var used: usize = 0;
            while (target <= checks) : (target += 1) {
                var tree = base.fork();
                defer tree.deinit(a);
                try tree.prepare(a);
                const spare = tree.spare.items.len;
                // This rank is computed on the exact root retained by the edit.
                const rank = tree.root.?.lowerBound(entry);
                var edit = try Tree.RankInsertion.init(&tree, entry, rank, false);
                var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
                    0 => error.Canceled,
                    1 => error.DeadlineExceeded,
                    else => error.FileNotFound,
                } };
                const payload: @import("../../common/read_admission.zig").Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
                const original = @import("../../common/read_admission.zig").Port.init(&payload);
                var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
                const result = edit.resumeWithAdmission(failing.allocator(), original);
                if (target == 0) {
                    try result;
                    checks = fault.checks;
                    used = spare - tree.spare.items.len;
                } else {
                    try std.testing.expectError(fault.failure, result);
                    try edit.resumeWithAdmission(failing.allocator(), original);
                }
                try std.testing.expectEqual(used, spare - tree.spare.items.len);
                try std.testing.expectEqual(@as(usize, 3), tree.root.?.count);
                try std.testing.expectEqual(@as(u8, 2), tree.root.?.height);
                for (0..3) |i| try std.testing.expectEqual(i + 1, tree.root.?.at(i).run.id);
                try std.testing.expectEqual(@as(u64, 2), tree.root.?.entry.run.id);
                try std.testing.expect((try edit.takeRetired()) == null);
                const saved = tree.root;
                fault.at = fault.checks;
                try std.testing.expectError(fault.failure, edit.resumeWithAdmission(failing.allocator(), original));
                try std.testing.expect(saved == tree.root);
                try std.testing.expect(base.root == old_root);
                try std.testing.expectEqual(@as(usize, 2), base.root.?.count);
                try std.testing.expectEqualDeep(old_summary, base.root.?.summary);
                if (Tree != Directory.IdTree) {
                    const expected = if (Tree == Directory.Tree) Entry.summarize(tree.root.?.entry, tree.root.?.left.?.summary, tree.root.?.right.?.summary) else BoundsSummary.summarize(tree.root.?.entry, tree.root.?.left.?.summary, tree.root.?.right.?.summary);
                    try std.testing.expectEqualDeep(expected, tree.root.?.summary);
                }
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 3), fixture.pins);
}

test "controlled physical sync private directory removal publishes every index together and retains bounded abort" {
    const a = std.testing.allocator;
    const admission = @import("../../common/read_admission.zig");
    const Fault = struct {
        checks: usize = 0,
        at: usize = std.math.maxInt(usize),
        failure: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const at = self.checks;
            self.checks += 1;
            if (at == self.at) return self.failure;
        }
    };
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pins: usize = 0,
        pub fn retainRunSnapshotRef(self: *@This(), _: *Run) !void {
            self.pins += 1;
        }
        pub fn releaseRunSnapshotRef(self: *@This(), _: *Run) void {
            self.pins -= 1;
        }
        fn prepare(source: *Directory, run: *const Run) !Directory.Removal {
            const candidate = try source.fork(a);
            errdefer candidate.destroy(a);
            var pools = try Directory.IndexPreparation.init(candidate, run.level, 1);
            try pools.resumeWithAdmission(a, null);
            return Directory.Removal.init(source, candidate, run);
        }
        fn perform(job: *Directory.Removal, allocator: std.mem.Allocator, live: *?*Directory, original: admission.Port) !*Directory {
            try job.resumeWithAdmission(allocator, original);
            return job.publishWithAdmission(live, original);
        }
        fn verify(source: *Directory, candidate: *Directory, removed: *const Run) !void {
            try std.testing.expectEqual(source.count() - 1, candidate.count());
            inline for (.{ "ids", "bounds", "ends" }) |field| try std.testing.expectEqual(candidate.count(), @field(candidate, field).root.?.count);
            try std.testing.expectEqual(source.total_run_bytes - removed.size_bytes, candidate.total_run_bytes);
            try std.testing.expectEqual(source.memory_run_count - @intFromBool(removed.path == null), candidate.memory_run_count);
            try std.testing.expectEqual(candidate.memory_run_count, candidate.tree.root.?.summary.non_async_runs);
            for (0..source.count()) |i| {
                const run = source.at(i).run;
                inline for (.{ Directory.locateRead, Directory.locateId, Directory.locateLower, Directory.locateUpper }) |locate| {
                    var search = locate(candidate, run);
                    try search.resumeWithAdmission(null);
                    try std.testing.expectEqual(run.id != removed.id, search.matched != null);
                }
            }
            for (0..3) |level_number| {
                const number: u32 = @intCast(level_number);
                const old = source.levelStats(number);
                const now = candidate.levelStats(number);
                try std.testing.expectEqual(old.count - @intFromBool(removed.level == number), now.count);
                try std.testing.expectEqual(old.bytes - (if (removed.level == number) removed.size_bytes else 0), now.bytes);
                try std.testing.expectEqual(old.tombstone_runs - @intFromBool(removed.level == number and (removed.tombstone_count orelse 0) != 0), now.tombstone_runs);
            }
            for ([_]u64{ 77, 88 }) |seq| {
                const key = generation_index.Generation{ .sequence = seq };
                const old = generation_index.Tree.find(source.generations.root, key).?.entry;
                const removed_here = removed.level == 0 and generation_index.sequence(removed.*) == seq;
                const files = old.files - @intFromBool(removed_here);
                const now = generation_index.Tree.find(candidate.generations.root, key);
                if (files == 0) try std.testing.expect(now == null) else {
                    try std.testing.expectEqual(files, now.?.entry.files);
                    try std.testing.expectEqual(old.bytes - (if (removed_here) removed.size_bytes else 0), now.?.entry.bytes);
                }
            }
            try std.testing.expectEqual(@as(u64, if (removed.id == 6) 5 else 6), candidate.bounds.root.?.summary.largest.?.id);
            try std.testing.expectEqual(@as(u64, if (removed.id == 1) 2 else 1), candidate.tree.root.?.summary.smallest.?.id);
        }
    };
    var fixture: Fixture = .{ .allocator = a };
    var memory_state: state.State = .{};
    defer memory_state.deinit(a);
    try memory_state.appendUpsert(a, .{}, "f", "", true);
    const source = try Directory.create(a);
    defer source.destroy(a);
    for (0..6) |i| {
        var key = [_]u8{@intCast('a' + i)};
        try source.put(&fixture, .{ .id = i + 1, .visibility_id = if (i < 2) 77 else 88, .level = if (i < 3) 0 else if (i < 5) 1 else 2, .size_bytes = 10 + i, .path = if (i == 5) null else @constCast("/directory-remove.tbl"), .smallest_namespace_name = null, .smallest_key = &key, .largest_namespace_name = null, .largest_key = &key, .entry_count = 1, .tombstone_count = @intCast(i % 2), .bloom_filter = null, .state = if (i == 5) memory_state else null, .owns_metadata = false });
    }
    const roots = Directory.Removal.roots(source);
    for (0..source.count()) |rank| {
        const run = source.at(rank).run;
        var checkpoints: usize = 1;
        var target: usize = 0;
        while (target <= checkpoints) : (target += 1) {
            for ([_]bool{ false, true }) |abort| {
                if (target == 0 and abort) continue;
                var job = try Fixture.prepare(source, run);
                defer job.deinit(a);
                var live: ?*Directory = source;
                var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
                    0 => error.Canceled,
                    1 => error.DeadlineExceeded,
                    else => error.FileNotFound,
                } };
                const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
                const port = admission.Port.init(&payload);
                var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
                const result = Fixture.perform(&job, failing.allocator(), &live, port);
                if (target == 0) {
                    try std.testing.expect(try result == source);
                    checkpoints = fault.checks;
                } else {
                    try std.testing.expectError(fault.failure, result);
                    try std.testing.expect(live == source);
                    try std.testing.expectEqual(@as(usize, 6), live.?.count());
                }
                try std.testing.expectEqual(@as(usize, 6), fixture.pins);
                if (abort) {
                    fault.at = fault.checks + target % 3;
                    try std.testing.expectError(fault.failure, job.retireWithAdmission(failing.allocator(), port));
                    try job.retireWithAdmission(failing.allocator(), port);
                    try std.testing.expect(job.retired and live == source);
                } else {
                    if (target != 0) try std.testing.expect(try Fixture.perform(&job, failing.allocator(), &live, port) == source);
                    try std.testing.expect(live == job.candidate);
                    try Fixture.verify(source, live.?, run);
                    try std.testing.expectError(error.InvalidArgument, job.publishWithAdmission(&live, port));
                    live.?.destroy(a);
                }
                try std.testing.expectEqualDeep(roots, Directory.Removal.roots(source));
                try std.testing.expectEqual(@as(usize, 6), fixture.pins);
            }
        }
    }
    // Exhaust every retirement checkpoint after a fully computed private edit.
    const run = source.at(0).run;
    var checks: usize = 1;
    var target: usize = 0;
    while (target <= checks) : (target += 1) {
        var job = try Fixture.prepare(source, run);
        defer job.deinit(a);
        try job.resumeWithAdmission(a, null);
        var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1 };
        const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
        const port = admission.Port.init(&payload);
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        const result = job.retireWithAdmission(failing.allocator(), port);
        if (target == 0) {
            try result;
            checks = fault.checks;
        } else {
            try std.testing.expectError(error.Canceled, result);
            try job.retireWithAdmission(failing.allocator(), port);
        }
        try std.testing.expect(job.retired);
        try std.testing.expectEqual(@as(usize, 6), fixture.pins);
    }
    var job = try Fixture.prepare(source, run);
    defer job.deinit(a);
    var fault: Fault = .{};
    const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
    const port = admission.Port.init(&payload);
    var premature_live: ?*Directory = source;
    try std.testing.expectError(error.InvalidArgument, job.publishWithAdmission(&premature_live, port));
    try std.testing.expect(premature_live == source);
    try std.testing.expectError(error.InvalidArgument, Directory.Removal.init(source, source, run));
    source.total_run_bytes += 1;
    const stale = job.resumeWithAdmission(a, port);
    source.total_run_bytes -= 1;
    try std.testing.expectError(error.CompactionPlanningStale, stale);
    try job.resumeWithAdmission(a, port);
    const other = try source.fork(a);
    defer other.destroy(a);
    var live: ?*Directory = other;
    try std.testing.expectError(error.CompactionPlanningStale, job.publishWithAdmission(&live, port));
    live = source;
    try std.testing.expect(try job.publishWithAdmission(&live, port) == source);
    live.?.destroy(a);
    var missing = run.*;
    missing.id = 999;
    var absent = try Fixture.prepare(source, &missing);
    defer absent.deinit(a);
    try absent.resumeWithAdmission(a, port);
    live = source;
    try std.testing.expect(try absent.publishWithAdmission(&live, port) == source);
    try std.testing.expectEqual(source.count(), live.?.count());
    try std.testing.expectEqual(source.total_run_bytes, live.?.total_run_bytes);
    live.?.destroy(a);
}

test "controlled physical sync private put replaces every index across ordering level and sequence moves" {
    const a = std.testing.allocator;
    const admission = @import("../../common/read_admission.zig");
    const Fault = struct {
        checks: usize = 0,
        at: usize = std.math.maxInt(usize),
        failure: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const at = self.checks;
            self.checks += 1;
            if (at == self.at) return self.failure;
        }
    };
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pins: usize = 0,
        options: struct { run_partition_key: ?*const fn ([]const u8) []const u8 = partition } = .{},
        fn partition(key: []const u8) []const u8 {
            return key[0..@min(key.len, 1)];
        }
        pub fn retainRunSnapshotRef(self: *@This(), _: *Run) !void {
            self.pins += 1;
        }
        pub fn releaseRunSnapshotRef(self: *@This(), _: *Run) void {
            self.pins -= 1;
        }
        fn prepare(self: *@This(), source: *Directory, run: Run) !Directory.Put {
            const candidate = try source.fork(a);
            errdefer candidate.destroy(a);
            var pools: Directory.PutPools = .{ .directory = candidate };
            try std.testing.expect(try pools.memoryBound() > 0);
            try pools.resumeWithAdmission(a, null);
            var lease = try candidate.prepareEntryLegacy(self, run);
            errdefer lease.deinit(a);
            return Directory.Put.init(source, candidate, lease);
        }
        fn perform(job: *Directory.Put, allocator: std.mem.Allocator, live: *?*Directory, original: admission.Port) !*Directory {
            try job.resumeWithAdmission(allocator, original);
            return job.publishWithAdmission(live, original);
        }
        fn verify(source: *Directory, candidate: *Directory, incoming: *const Run) !void {
            var expected: [7]*const Run = undefined;
            var len: usize = 0;
            for (0..source.count()) |i| {
                const run = source.at(i).run;
                if (run.id != incoming.id) {
                    expected[len] = run;
                    len += 1;
                }
            }
            expected[len] = incoming;
            len += 1;
            std.mem.sort(*const Run, expected[0..len], {}, struct {
                fn less(_: void, x: *const Run, y: *const Run) bool {
                    return Directory.runLess(x, y);
                }
            }.less);
            try std.testing.expectEqual(len, candidate.count());
            inline for (.{ "ids", "bounds", "ends" }) |field| try std.testing.expectEqual(len, @field(candidate, field).root.?.count);
            var total: u64 = 0;
            var memory: usize = 0;
            var tombstones: usize = 0;
            var gc = false;
            var levels: [5]LevelAggregate = undefined;
            for (&levels, 0..) |*level, i| level.* = .{ .level = @intCast(i) };
            var gens: [7]generation_index.Generation = undefined;
            var gen_len: usize = 0;
            for (expected[0..len], 0..) |run, i| {
                try std.testing.expect(candidate.at(i).run == run);
                try std.testing.expect(candidate.byId(run.id) == run);
                inline for (.{ Directory.locateRead, Directory.locateId, Directory.locateLower, Directory.locateUpper }) |locate| {
                    var search = locate(candidate, run);
                    try search.resumeWithAdmission(null);
                    try std.testing.expect(search.matched.?.entry.run == run);
                    try std.testing.expectEqualStrings(partition(run.smallest_key), search.matched.?.entry.domain);
                }
                total += run.size_bytes;
                memory += @intFromBool(run.path == null);
                const deletes = (run.tombstone_count orelse 0) != 0;
                tombstones += @intFromBool(deletes);
                gc = gc or run.gc_requested;
                levels[run.level].count += 1;
                levels[run.level].bytes += run.size_bytes;
                levels[run.level].tombstone_runs += @intFromBool(deletes);
                if (run.level == 0) {
                    const seq = generation_index.sequence(run.*);
                    var idx: usize = 0;
                    while (idx < gen_len and gens[idx].sequence != seq) : (idx += 1) {}
                    if (idx == gen_len) {
                        gens[idx] = .{ .sequence = seq };
                        gen_len += 1;
                    }
                    gens[idx].files += 1;
                    gens[idx].bytes += run.size_bytes;
                }
            }
            try std.testing.expectEqual(total, candidate.total_run_bytes);
            try std.testing.expectEqual(memory, candidate.memory_run_count);
            try std.testing.expectEqual(memory, candidate.tree.root.?.summary.non_async_runs);
            try std.testing.expectEqual(tombstones, candidate.tree.root.?.summary.tombstone_runs);
            try std.testing.expectEqual(gc, candidate.tree.root.?.summary.gc_requested);
            var level_count: usize = 0;
            for (levels) |expected_level| {
                try std.testing.expectEqualDeep(expected_level, candidate.levelStats(expected_level.level));
                level_count += @intFromBool(expected_level.count != 0);
            }
            try std.testing.expectEqual(level_count, candidate.levelCount());
            try std.testing.expectEqual(gen_len, if (candidate.generations.root) |root| root.count else 0);
            for (gens[0..gen_len]) |expected_gen| try std.testing.expectEqualDeep(expected_gen, generation_index.Tree.find(candidate.generations.root, expected_gen).?.entry);
            try std.testing.expect(candidate.byId(incoming.id) == incoming);
        }
    };
    var fixture: Fixture = .{ .allocator = a };
    var memory: state.State = .{};
    defer memory.deinit(a);
    try memory.appendUpsert(a, .{}, "f", "", true);
    var next_memory: state.State = .{};
    defer next_memory.deinit(a);
    try next_memory.appendUpsert(a, .{}, "m", "new", false);
    const source = try Directory.create(a);
    defer source.destroy(a);
    for (0..6) |i| {
        var key = [_]u8{@intCast('a' + i)};
        try source.put(&fixture, .{ .id = i + 1, .visibility_id = if (i < 2) 77 else 88, .level = if (i < 3) 0 else if (i < 5) 1 else 2, .size_bytes = 10 + i, .path = if (i == 5) null else @constCast("/put-source.tbl"), .smallest_namespace_name = null, .smallest_key = &key, .largest_namespace_name = null, .largest_key = &key, .entry_count = 1, .tombstone_count = @intCast(i % 2), .oldest_tombstone_unix_ns = i * 11, .bloom_filter = null, .state = if (i == 5) memory else null, .owns_metadata = false });
    }
    const empty = try Directory.create(a);
    defer empty.destroy(a);
    for (0..10) |scenario| {
        const base = if (scenario == 9) empty else source;
        var incoming: Run = .{ .id = 9, .level = 1, .visibility_id = 333, .size_bytes = 37, .path = @constCast("/put-next.tbl"), .smallest_namespace_name = null, .smallest_key = @constCast("m"), .largest_namespace_name = null, .largest_key = @constCast("n"), .entry_count = 1, .tombstone_count = 0, .gc_requested = true, .bloom_filter = null, .state = null, .owns_metadata = false };
        switch (scenario) {
            1, 2 => {
                incoming = base.byId(2).?.*;
                incoming.size_bytes = 37;
                incoming.tombstone_count = 0;
                if (scenario == 2) incoming.largest_key = @constCast("zz");
            },
            3 => {
                incoming.id = 2;
                incoming.smallest_namespace_name = @constCast("docs");
                incoming.largest_namespace_name = @constCast("docs");
            },
            4 => {
                incoming.id = 1;
                incoming.level = 0;
            },
            5 => {
                incoming.id = 3;
                incoming.level = 3;
            },
            6 => {
                incoming.id = 4;
                incoming.level = 0;
                incoming.visibility_id = 444;
            },
            7 => {
                incoming.id = 5;
                incoming.path = null;
                incoming.state = next_memory;
                incoming.largest_key = @constCast("m");
            },
            8 => {
                incoming.id = 6;
                incoming.level = 2;
            },
            else => {},
        }
        const roots = Directory.Removal.roots(base);
        const baseline_pins = fixture.pins;
        var checks: usize = 1;
        var target: usize = 0;
        while (target <= checks) : (target += 1) {
            for ([_]bool{ false, true }) |abort| {
                if (target == 0 and abort) continue;
                var job = try fixture.prepare(base, incoming);
                defer job.deinit(a);
                const prepared_run = job.entry.run;
                var live: ?*Directory = base;
                var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
                    0 => error.Canceled,
                    1 => error.DeadlineExceeded,
                    else => error.FileNotFound,
                } };
                const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
                const port = admission.Port.init(&payload);
                var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
                const result = Fixture.perform(&job, failing.allocator(), &live, port);
                if (target == 0) {
                    try std.testing.expect(try result == base);
                    checks = fault.checks;
                } else {
                    try std.testing.expectError(fault.failure, result);
                    try std.testing.expect(live == base);
                }
                try std.testing.expectEqual(baseline_pins + 1, fixture.pins);
                if (abort) {
                    fault.at = fault.checks + target % 3;
                    try std.testing.expectError(fault.failure, job.retireWithAdmission(failing.allocator(), port));
                    try job.retireWithAdmission(failing.allocator(), port);
                    try std.testing.expect(job.retired and live == base);
                } else {
                    if (target != 0) try std.testing.expect(try Fixture.perform(&job, failing.allocator(), &live, port) == base);
                    try Fixture.verify(base, live.?, prepared_run);
                    const pass = @import("memory_account.zig").nextPass();
                    try std.testing.expect(live.?.accountedMemoryBytes(pass) > 0);
                    try std.testing.expectEqual(@as(u64, 0), job.lease.?.accountedMemoryBytes(pass));
                    try std.testing.expectError(error.InvalidArgument, job.publishWithAdmission(&live, port));
                }
                fault.at = fault.checks;
                try std.testing.expectError(fault.failure, job.takeEntryLeaseWithAdmission(port));
                var returned_lease = try job.takeEntryLeaseWithAdmission(port);
                try std.testing.expectError(error.InvalidArgument, job.takeEntryLeaseWithAdmission(port));
                returned_lease.deinit(a);
                if (!abort) live.?.destroy(a);
                try std.testing.expectEqual(baseline_pins, fixture.pins);
                try std.testing.expectEqualDeep(roots, Directory.Removal.roots(base));
            }
        }
    }
}

test "controlled physical sync directory lease retains final payload pin bytes and charge through retirement rejection" {
    const a = std.testing.allocator;
    const admission = @import("../../common/read_admission.zig");
    const Fault = struct {
        checks: usize = 0,
        at: usize = std.math.maxInt(usize),
        failure: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const at = self.checks;
            self.checks += 1;
            if (at == self.at) return self.failure;
        }
    };
    const Fixture = struct {
        allocator: std.mem.Allocator,
        pins: usize = 0,
        pin_phase: usize = 0,
        releases: usize = 0,
        pub fn retainRunSnapshotRef(self: *@This(), run: *Run) !void {
            self.pins += 1;
            run.version_ref_pinned = true;
        }
        pub fn releaseRunSnapshotRef(self: *@This(), run: *Run) void {
            std.debug.assert(self.pins == 1 and run.version_ref_pinned);
            self.pins -= 1;
            self.releases += 1;
            run.version_ref_pinned = false;
        }
        fn release(raw: *anyopaque, owner: *anyopaque, run: *Run, original: ?admission.Port) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(raw == owner);
            // Model a retained registry lookup: no lock or path allocation is
            // hidden in this fixture. The production registry remains separate.
            while (self.pin_phase < 3) {
                if (original) |port| try port.checkpoint();
                self.pin_phase += 1;
            }
            if (original) |port| try port.checkpoint();
            self.releaseRunSnapshotRef(run);
        }
        fn pin(self: *@This()) Directory.EntryLease.PinRelease {
            return .{ .context = self, .resume_fn = release };
        }
    };
    const bytes = try a.alloc(u8, 3 * 16 * 1024 + 7);
    defer a.free(bytes);
    @memset(bytes, 'l');
    var state_source: state.State = .{};
    defer state_source.deinit(a);
    try state_source.appendUpsert(a, .{ .name = "docs" }, bytes, bytes, true);
    const base = Run{ .id = 301, .level = 0, .size_bytes = 1, .path = null, .smallest_namespace_name = null, .smallest_key = bytes, .largest_namespace_name = null, .largest_key = bytes, .entry_count = 1, .bloom_filter = null, .state = state_source, .owns_metadata = false };
    for (0..3) |mode| {
        var total: usize = 0;
        var target: usize = 0;
        while (target <= total) : (target += 1) {
            for ([_]bool{ false, true }) |quiesced| {
                if (target == 0 and quiesced) continue;
                var source = base;
                if (mode == 0) {
                    source.path = @constCast("/retire/301.sst");
                    source.state = null;
                }
                if (mode == 2) {
                    source = try repository.cloneRunCompactionSnapshot(a, source);
                    try source.shareMemory(a);
                }
                var fixture: Fixture = .{ .allocator = a };
                const directory = try Directory.create(a);
                defer directory.destroy(a);
                try directory.tree.prepare(a);
                const account = directory.tree.account.?;
                const baseline = account.bytes.load(.acquire);
                var lease: ?Directory.EntryLease = try directory.prepareEntryLegacy(&fixture, source);
                defer if (lease) |*value| value.deinit(a);
                if (mode == 2) source.deinit(a); // Payload owns the last shared pin.
                const charged = account.bytes.load(.acquire);
                try std.testing.expect(charged > baseline);
                var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
                    0 => error.Canceled,
                    1 => error.DeadlineExceeded,
                    else => error.FileNotFound,
                } };
                const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
                const original = admission.Port.init(&payload);
                var retirement: ?Directory.EntryLease.Retirement = null;
                defer if (retirement) |*value| value.deinit(a);
                const result: anyerror!void = blk: {
                    retirement = lease.?.beginRetirementWithAdmission(fixture.pin(), original) catch |err| break :blk err;
                    lease = null;
                    retirement.?.retireWithAdmission(a, original) catch |err| break :blk err;
                    break :blk {};
                };
                if (target == 0) {
                    try result;
                    total = fault.checks;
                    try std.testing.expect(total > 10);
                } else {
                    try std.testing.expectError(fault.failure, result);
                    try std.testing.expectEqual(charged, account.bytes.load(.acquire));
                }
                if (lease) |*value| {
                    retirement = try value.beginRetirementWithAdmission(fixture.pin(), original);
                    lease = null;
                }
                if (quiesced) retirement.?.deinit(a) else try retirement.?.retireWithAdmission(a, original);
                try std.testing.expectEqual(baseline, account.bytes.load(.acquire));
                try std.testing.expectEqual(@as(usize, 0), fixture.pins);
                try std.testing.expectEqual(@as(usize, 1), fixture.releases);
                fault.at = fault.checks;
                try std.testing.expectError(fault.failure, retirement.?.retireWithAdmission(a, original));
                try retirement.?.retireWithAdmission(a, original);
                try std.testing.expectEqual(@as(usize, 1), fixture.releases);
            }
        }
    }
    // A directory/handle still owns the payload. Dropping this lease cannot
    // start pin release or physical retirement, even on the quiesced path.
    var fixture: Fixture = .{ .allocator = a };
    const directory = try Directory.create(a);
    defer directory.destroy(a);
    try directory.tree.prepare(a);
    var lease = try directory.prepareEntryLegacy(&fixture, base);
    var other: Directory.EntryLease = .{ .entry = lease.entry.retainShared() };
    _ = other.entry.payload.?.account.retain();
    var fault: Fault = .{};
    const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
    const original = admission.Port.init(&payload);
    lease.entry.payload.?.run.shared_read_version = true;
    fault.at = fault.checks;
    try std.testing.expectError(error.Canceled, lease.beginRetirementWithAdmission(fixture.pin(), original));
    try std.testing.expectError(error.ExecutionWaitUnsupported, lease.beginRetirementWithAdmission(fixture.pin(), original));
    try std.testing.expectEqual(@as(usize, 2), lease.entry.payload.?.refs.load(.acquire));
    lease.entry.payload.?.run.shared_read_version = false;
    var first = try lease.beginRetirementWithAdmission(fixture.pin(), original);
    try std.testing.expect(first.complete);
    try first.retireWithAdmission(a, original);
    first.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), fixture.pins);
    try std.testing.expectEqual(@as(usize, 0), fixture.pin_phase);
    var last = try other.beginRetirementWithAdmission(fixture.pin(), original);
    defer last.deinit(a);
    try last.retireWithAdmission(a, original);
    try std.testing.expectEqual(@as(usize, 1), fixture.releases);
}
