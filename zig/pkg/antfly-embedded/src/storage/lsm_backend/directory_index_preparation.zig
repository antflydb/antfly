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

//! Prepaid directory index pools, not entry publication. Caller permanently
//! fences mutation/accounting of the source and retains its roots, allocator
//! and manager until transfer/retirement. The source stays pinned so private
//! abort never destroys a live payload or enters the file-pin registry.
const std = @import("std");
const Directory = @import("run_directory.zig").Directory;
const admission = @import("../../common/read_admission.zig");
const resources = @import("../resource_manager.zig");

/// Caller prepays this fixed header. All candidate allocations are reserved
/// here. No original port is retained. Successful take adopts only surviving
/// allocations into a preprepared stable caller usage identity; the caller
/// registers the returned directory before allowing competing observation.
pub const Owned = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    source: *const Directory,
    level: u32,
    ends_edits: usize,
    bound: u64,
    baseline_bytes: u64,
    meters: Directory.Accounting,
    directory: ?*Directory = null,
    cpu: ?Directory.IndexPreparation = null,
    credit: ?resources.ConstructionCredit = null,
    reclaimer: ?Directory.Reclaimer = null,
    retiring: bool = false,
    retired: bool = false,
    taken: bool = false,

    pub fn init(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const Directory, level: u32, ends_edits: usize) !Owned {
        const plan = try Directory.IndexPreparation.init(@constCast(source), level, ends_edits);
        const bound = try std.math.add(u64, @sizeOf(Directory), try plan.memoryBound());
        return .{ .allocator = a, .manager = manager, .source = source, .level = level, .ends_edits = ends_edits, .bound = bound, .baseline_bytes = source.indexAccountBytes(), .meters = source.pinAccounting() };
    }
    fn pinMeters(self: *Owned) void {
        const d = self.directory orelse return;
        inline for (.{ "tree", "ids", "bounds", "ends", "levels", "generations" }, 0..) |field, i| {
            if (self.meters.accounts[i] == null) if (@field(d, field).account) |account| {
                self.meters.accounts[i] = account.retain();
            };
        }
    }
    pub fn resumeWithAdmission(self: *Owned, original: admission.Port) !void {
        if (self.retiring or self.retired or self.taken) return error.InvalidArgument;
        try original.checkpoint();
        if (self.credit == null) self.credit = try self.manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, self.bound, original);
        if (self.directory == null) {
            try original.checkpoint();
            self.directory = try self.source.fork(self.allocator);
            self.cpu = try Directory.IndexPreparation.init(self.directory.?, self.level, self.ends_edits);
        }
        self.cpu.?.resumeWithAdmission(self.allocator, original) catch |cause| {
            self.pinMeters();
            return cause;
        };
        self.pinMeters();
    }
    pub fn takeWithAdmission(self: *Owned, destination: resources.Slice, usage: *u64, original: admission.Port) !*Directory {
        if (self.retiring or self.retired or self.taken or self.cpu == null or self.cpu.?.phase != 6) return error.InvalidArgument;
        try original.checkpoint();
        const d = self.directory.?;
        const added = d.indexAccountBytes() - self.baseline_bytes + d.indexSpareBytes() + @sizeOf(Directory);
        std.debug.assert(added <= self.bound);
        try self.credit.?.adoptUsageWithAdmission(destination, usage, added, original);
        // Fixed ownership receipt only after resident accounting moves.
        self.directory = null;
        self.taken = true;
        self.meters.deinit();
        self.credit.?.retire();
        self.credit = null;
        return d;
    }
    fn checkpoint(original: ?admission.Port) !void {
        if (original) |port| try port.checkpoint();
    }
    fn retire(self: *Owned, original: ?admission.Port) !void {
        try checkpoint(original);
        if (self.retired or self.taken) return;
        self.retiring = true;
        self.pinMeters();
        if (self.directory) |d| {
            if (self.reclaimer == null) self.reclaimer = .init(d);
            while (true) {
                try checkpoint(original);
                var steps: usize = 1;
                if (self.reclaimer.?.step(self.allocator, &steps)) break;
            }
            try checkpoint(original);
            self.reclaimer.?.finish(self.allocator);
            self.reclaimer = null;
            self.directory = null;
        }
        self.meters.deinit();
        if (self.credit) |*credit| credit.retire();
        self.credit = null;
        self.retired = true;
    }
    pub fn retireWithAdmission(self: *Owned, original: admission.Port) !void {
        try self.retire(original);
    }
    /// Explicit joined/quiesced fallback; normal rejection retains this owner.
    pub fn deinit(self: *Owned) void {
        self.retire(null) catch unreachable;
    }
};

const Run = @import("repository.zig").Run;
const Fault = struct {
    checks: usize = 0,
    at: usize = std.math.maxInt(usize),
    rejected: bool = false,
    failure: anyerror = error.Canceled,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const at = self.checks;
        self.checks += 1;
        if (!self.rejected and at == self.at) {
            self.rejected = true;
            return self.failure;
        }
    }
    fn payload(self: *@This()) admission.Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = check } };
    }
};
const Witness = struct {
    manager: *resources.ResourceManager,
    baseline: u64 = 0,
    live: usize = 0,
    calls: usize = 0,
    fail_at: usize = std.math.maxInt(usize),
    fn funded(self: *@This()) u64 {
        return self.manager.sliceStats(.lsm_table_builder_working_set).used_bytes + (self.manager.sliceStats(.lsm_in_memory_state).used_bytes -| self.baseline);
    }
    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.funded() >= self.live + len);
        const at = self.calls;
        self.calls += 1;
        if (at == self.fail_at) return null;
        const ptr = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
        self.live += len;
        return ptr;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        std.debug.assert(self.funded() >= self.live);
        std.testing.allocator.rawFree(bytes, alignment, ra);
        self.live -= bytes.len;
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
};
fn sourceRun() Run {
    return .{ .id = 91, .level = 1, .size_bytes = 7, .path = @constCast("/91.tbl"), .smallest_namespace_name = @constCast("docs"), .smallest_key = @constCast("a"), .largest_namespace_name = @constCast("docs"), .largest_key = @constCast("z"), .entry_count = 2, .bloom_filter = null, .state = null, .owns_metadata = false };
}
const Stage = enum { construct, adopt, retire };
fn exercise(stage: Stage, at: usize, abandon: bool, level: u32) !usize {
    const a = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    var fixture: Fixture = .{ .allocator = a };
    const source = source: {
        const d = try Directory.create(a);
        errdefer d.destroy(a);
        try d.put(&fixture, sourceRun());
        break :source d;
    };
    const baseline = source.accountedMemoryBytes(@import("memory_account.zig").nextPass());
    var usage: u64 = 0;
    manager.observeUsage(.lsm_in_memory_state, &usage, baseline);
    defer {
        source.destroy(a);
        manager.observeUsage(.lsm_in_memory_state, &usage, 0);
    }
    var witness: Witness = .{ .manager = &manager, .baseline = baseline };
    var owned = try Owned.init(witness.allocator(), &manager, source, level, 2);
    defer owned.deinit();
    var fault: Fault = .{ .failure = switch (at % 3) {
        0 => error.Canceled,
        1 => error.DeadlineExceeded,
        else => error.FileNotFound,
    } };
    const payload = fault.payload();
    const original = admission.Port.init(&payload);
    if (stage != .construct) try owned.resumeWithAdmission(original);
    fault.checks = 0;
    fault.at = at;
    var output: ?*Directory = null;
    defer if (output) |d| {
        d.destroy(witness.allocator());
        manager.observeUsage(.lsm_in_memory_state, &usage, baseline);
    };
    const result: anyerror!void = switch (stage) {
        .construct => owned.resumeWithAdmission(original),
        .adopt => take: {
            output = owned.takeWithAdmission(.lsm_in_memory_state, &usage, original) catch |cause| break :take cause;
            break :take {};
        },
        .retire => owned.retireWithAdmission(original),
    };
    const checks = fault.checks;
    if (at == std.math.maxInt(usize)) try result else {
        try std.testing.expectError(fault.failure, result);
        try std.testing.expect(fault.rejected);
        try std.testing.expectEqual(baseline, usage);
    }
    if (stage == .retire or (abandon and !owned.taken)) {
        try owned.retireWithAdmission(original);
        try std.testing.expectError(error.InvalidArgument, owned.resumeWithAdmission(original));
    } else {
        if (output == null) {
            try owned.resumeWithAdmission(original);
            output = try owned.takeWithAdmission(.lsm_in_memory_state, &usage, original);
        }
        const d = output.?;
        try std.testing.expectEqual(@as(usize, 1), d.count());
        try std.testing.expect(d.tree.root == source.tree.root and d.ids.root == source.ids.root and d.bounds.root == source.bounds.root and d.ends.root == source.ends.root and d.levels.root == source.levels.root and d.generations.root == source.generations.root);
        const pass = @import("memory_account.zig").nextPass();
        try std.testing.expectEqual(source.accountedMemoryBytes(pass) + d.accountedMemoryBytes(pass), usage);
        try std.testing.expectEqual(@as(u64, @intCast(witness.live)), usage - baseline);
        d.destroy(witness.allocator());
        output = null;
        try manager.adjustUsageWithAdmission(.lsm_in_memory_state, &usage, baseline, original);
    }
    try std.testing.expectEqual(@as(usize, 0), witness.live);
    try std.testing.expectEqual(@as(usize, 1), fixture.pins);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
    return checks;
}

test "controlled physical sync directory index pools retain every original rejection across all six indexes" {
    for ([_]u32{ 0, 2 }) |level| for (std.enums.values(Stage)) |stage| {
        const checks = try exercise(stage, std.math.maxInt(usize), false, level);
        for (0..checks) |at| _ = try exercise(stage, at, at % 2 == 1, level);
    };
}

test "controlled physical sync directory index pool construction retries or abandons every allocation refusal" {
    const a = std.testing.allocator;
    var count: usize = 0;
    for ([_]bool{ false, true }) |abandon| {
        var at: usize = 0;
        while (at < 256) : (at += 1) {
            var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
            defer manager.deinit(a);
            var source: Directory = .{};
            var witness: Witness = .{ .manager = &manager, .fail_at = at };
            var owned = try Owned.init(witness.allocator(), &manager, &source, 0, 2);
            defer owned.deinit();
            var fault: Fault = .{};
            const payload = fault.payload();
            const original = admission.Port.init(&payload);
            const result = owned.resumeWithAdmission(original);
            witness.fail_at = std.math.maxInt(usize);
            if (result) |_| {
                count = witness.calls;
                try owned.retireWithAdmission(original);
                try std.testing.expectEqual(@as(usize, 0), witness.live);
                break;
            } else |cause| {
                try std.testing.expectEqual(error.OutOfMemory, cause);
                try std.testing.expect(owned.credit != null);
                if (!abandon) try owned.resumeWithAdmission(original);
                try owned.retireWithAdmission(original);
                try std.testing.expectEqual(@as(usize, 0), witness.live);
                try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
            }
        }
        try std.testing.expect(at < 256 and count > 6);
    }
}

test "controlled physical sync directory index pools deny unfunded construction and adoption without transferring roots" {
    const a = std.testing.allocator;
    var options: resources.Options = .{ .identity_allocator = a };
    options.budgets[@backingInt(resources.Slice.lsm_table_builder_working_set)] = .{ .hard_limit_bytes = 1 };
    var manager = resources.ResourceManager.init(options);
    defer manager.deinit(a);
    var source: Directory = .{};
    var witness: Witness = .{ .manager = &manager };
    var fault: Fault = .{};
    const payload = fault.payload();
    const original = admission.Port.init(&payload);
    try std.testing.expectError(error.InvalidArgument, Owned.init(witness.allocator(), &manager, &source, 0, 3));
    try std.testing.expectError(error.InvalidArgument, source.tree.prepareEditsWithAdmission(witness.allocator(), 3, original));
    var owned = try Owned.init(witness.allocator(), &manager, &source, 0, 2);
    defer owned.deinit();

    try std.testing.expectError(error.ResourceBudgetExceeded, owned.resumeWithAdmission(original));
    try std.testing.expectEqual(@as(usize, 0), witness.calls);
    try std.testing.expect(owned.credit == null and owned.directory == null);
    manager.slices[@backingInt(resources.Slice.lsm_table_builder_working_set)].budget = resources.Options.defaultBudgets()[@backingInt(resources.Slice.lsm_table_builder_working_set)];
    try owned.resumeWithAdmission(original);
    const candidate = owned.directory.?;
    var usage: u64 = 0;
    try manager.prepareUsageWithAdmission(.lsm_in_memory_state, &usage, original);
    defer manager.observeUsage(.lsm_in_memory_state, &usage, 0);
    const slice = &manager.slices[@backingInt(resources.Slice.lsm_in_memory_state)];
    const budget = slice.budget;
    slice.budget = .{ .hard_limit_bytes = 1 };
    const refused = owned.takeWithAdmission(.lsm_in_memory_state, &usage, original);
    slice.budget = budget;
    try std.testing.expectError(error.ResourceBudgetExceeded, refused);
    try std.testing.expect(owned.directory == candidate and owned.credit != null and !owned.taken);
    try std.testing.expectEqual(@as(u64, 0), usage);
    {
        const d = try owned.takeWithAdmission(.lsm_in_memory_state, &usage, original);
        defer d.destroy(witness.allocator());
        try std.testing.expectEqual(candidate, d);
    }
    try manager.adjustUsageWithAdmission(.lsm_in_memory_state, &usage, 0, original);
    try std.testing.expectEqual(@as(usize, 0), witness.live);
}

test "controlled physical sync directory pool retirement retains exact accounting debt after destruction behind a busy parent" {
    const a = std.testing.allocator;
    var parent = resources.ResourceManager.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    var manager = try resources.metadata_fixture.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    try resources.metadata_fixture.installParent(&manager, &parent);
    var source: Directory = .{};
    var witness: Witness = .{ .manager = &manager };
    var owned = try Owned.init(witness.allocator(), &manager, &source, 0, 2);
    defer owned.deinit();
    var fault: Fault = .{};
    const payload = fault.payload();
    const original = admission.Port.init(&payload);
    try owned.resumeWithAdmission(original);
    const charged = manager.sliceStats(.lsm_table_builder_working_set).used_bytes;
    try std.testing.expect(charged > 0);
    try std.testing.expect(parent.mutex.tryLock());
    const result = owned.retireWithAdmission(original);
    parent.mutex.unlock();
    try result;
    try std.testing.expectEqual(@as(usize, 0), witness.live);
    try std.testing.expectEqual(charged, manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try std.testing.expect(manager.cleanupPending());
    try manager.drainConstructionCleanupWithAdmission(original);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try resources.qualifyNoLiveReservations(&manager);
}
