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

//! Pin-free run snapshot ownership. Caller pins immutable source and separately
//! owns file registration, accounting roots and the fixed continuation header.
const std = @import("std");
const Run = @import("repository.zig").Run;
const State = @import("state.zig").State;
const Metadata = @import("run_metadata_clone.zig").Clone;
const admission = @import("../../common/read_admission.zig");
const resources = @import("../resource_manager.zig");

/// Consumes a pin-free isolated snapshot, including a shared-memory pin. The
/// final shared header is claimed atomically before any owned byte is touched.
/// Writer RunOwner, output tickets and file registrations need other owners.
pub const Retirement = struct {
    shared: @FieldType(Run, "shared_memory") = null,
    backing: ?std.mem.Allocator = null,
    metadata: ?Metadata = null,
    state: ?State.Reclaimer = null,
    filter: ?[]u8 = null,
    filter_offset: usize = 0,
    index: ?@import("../lsm/table_file.zig").TableIndex.Retirement = null,
    complete: bool = false,
    fn isolated(run: Run) bool {
        return run.owner == null and run.output_ticket == null and
            !run.version_ref_pinned and !run.shared_read_version;
    }
    pub fn supports(run: Run) bool {
        if (!isolated(run)) return false;
        if (run.shared_memory) |shared| return run.table_index == null and shared.run.table_index == null and shared.run.shared_memory == null and isolated(shared.run);
        return true;
    }
    pub fn init(run: Run) !@This() {
        if (!supports(run)) return error.InvalidArgument;
        var out: @This() = .{};
        var owned = run;
        if (run.shared_memory) |shared| {
            if (shared.refs.fetchSub(1, .acq_rel) != 1) {
                out.complete = true;
                return out;
            }
            out.shared = shared;
            out.backing = shared.allocator;
            owned = shared.run;
        }
        if (owned.state) |state| out.state = .init(state);
        if (owned.table_index) |index| out.index = .init(index);
        if (owned.owns_bloom_filter) if (owned.bloom_filter) |filter| {
            out.filter = filter.bytes;
        };
        out.metadata = .{ .source = owned, .fields = .{
            if (owned.owns_metadata) owned.smallest_namespace_name else null,
            if (owned.owns_metadata) owned.smallest_key else null,
            if (owned.owns_metadata) owned.largest_namespace_name else null,
            if (owned.owns_metadata) owned.largest_key else null,
            if (owned.owns_metadata or owned.owns_path) owned.path else null,
        }, .retiring = true };
        return out;
    }
    /// One independently yieldable physical quantum. Rejection changes no
    /// cursor or allocation. Shared final ownership was claimed in init().
    pub fn step(self: *@This(), allocator: std.mem.Allocator, original: ?admission.Port) !bool {
        if (original) |port| try port.checkpoint();
        if (self.complete) return true;
        const a = self.backing orelse allocator;
        if (self.state) |*state| {
            var credits: usize = 1;
            if (state.step(a, &credits)) self.state = null;
            return false;
        }
        if (self.metadata) |*metadata| {
            if (try metadata.retireStep(a, original)) self.metadata = null;
            return false;
        }
        if (self.index) |*index| {
            if (try index.step(a, original)) self.index = null;
            return false;
        }
        if (self.filter) |bytes| {
            const end = @min(bytes.len, self.filter_offset + 16 * 1024);
            @memset(bytes[self.filter_offset..end], undefined);
            self.filter_offset = end;
            if (end == bytes.len) {
                if (bytes.len != 0) a.rawFree(bytes, .of(u8), @returnAddress());
                self.filter = null;
            }
            return false;
        }
        if (self.shared) |shared| a.destroy(shared);
        self.shared = null;
        self.complete = true;
        return true;
    }
    pub fn retireWithAdmission(self: *@This(), allocator: std.mem.Allocator, original: ?admission.Port) !void {
        while (!(try self.step(allocator, original))) {}
    }

    pub fn deinit(self: *@This(), a: std.mem.Allocator) void {
        self.retireWithAdmission(a, null) catch unreachable;
    }
};

pub const Clone = struct {
    source: Run,
    metadata: Metadata,
    state: ?State.Clone = null,
    output: ?Run = null,
    retirement: ?Retirement = null,
    bound: ?u64 = null,
    resident_bytes: u64 = 0,
    ready: bool = false,
    taken: bool = false,
    retiring: bool = false,
    retired: bool = false,
    pub fn init(source: Run) @This() {
        return .{ .source = source, .metadata = .init(source) };
    }
    pub fn memoryBoundWithAdmission(self: *@This(), original: ?admission.Port) !u64 {
        if (original) |port| try port.checkpoint();
        if (self.retiring or self.retired or self.taken) return error.InvalidArgument;
        if (self.bound) |bound| return bound;
        if (self.source.shared_memory != null) {
            var view = self.source;
            view.owner = null;
            view.output_ticket = null;
            if (!Retirement.supports(view)) return error.ExecutionWaitUnsupported;
            self.bound = 0;
        } else {
            var bytes = try self.metadata.memoryBound();
            if (self.source.path == null) {
                const source_state = self.source.state orelse return error.RunStateUnavailable;
                if (self.state == null) self.state = .init(&source_state);
                bytes = try std.math.add(u64, bytes, try self.state.?.memoryBoundWithAdmission(original));
            }
            self.bound = bytes;
        }
        return self.bound.?;
    }
    pub fn resumeWithAdmission(self: *@This(), a: std.mem.Allocator, original: ?admission.Port) !void {
        if (original) |port| try port.checkpoint();
        if (self.retiring or self.retired or self.taken) return error.InvalidArgument;
        if (self.bound == null) return error.ResourceBudgetExceeded;
        if (self.ready) return;
        if (self.source.shared_memory != null) {
            self.output = self.source.retainMemory().?;
        } else {
            try self.metadata.resumeWithAdmission(a, original);
            if (self.state) |*state| try state.resumeWithAdmission(a, original);
            if (original) |port| try port.checkpoint();
            var output = self.metadata.take() catch unreachable;
            self.resident_bytes = self.metadata.memoryBound() catch unreachable;
            if (self.state) |*state| {
                output.state = state.takeWithAdmission(null) catch unreachable;
                self.resident_bytes += state.resident_bytes;
            }
            self.output = output;
        }
        self.ready = true;
    }
    pub fn takeWithAdmission(self: *@This(), original: ?admission.Port) !Run {
        if (original) |port| try port.checkpoint();
        if (!self.ready or self.taken or self.retiring or self.retired) return error.InvalidArgument;
        const out = self.output.?;
        self.output = null;
        self.taken = true;
        return out;
    }
    pub fn retireWithAdmission(self: *@This(), a: std.mem.Allocator, original: ?admission.Port) !void {
        if (original) |port| try port.checkpoint();
        if (self.taken or self.retired) return;
        self.retiring = true;
        if (self.output) |output| {
            self.retirement = Retirement.init(output) catch unreachable;
            self.output = null;
        }
        if (self.retirement) |*retirement| try retirement.retireWithAdmission(a, original);
        if (self.state) |*state| try state.retireWithAdmission(a, original);
        try self.metadata.retireWithAdmission(a, original);
        self.resident_bytes = 0;
        self.retired = true;
    }
    pub fn deinit(self: *@This(), a: std.mem.Allocator) void {
        self.retireWithAdmission(a, null) catch unreachable;
    }
};

/// All additional allocations are funded here. The caller retains accounting
/// for shared source roots and registers output/retirement before take.
pub const Owned = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    cpu: Clone,
    credit: ?resources.ConstructionCredit = null,
    taken: bool = false,
    retired: bool = false,
    retiring: bool = false,
    pub fn init(a: std.mem.Allocator, manager: *resources.ResourceManager, source: Run) @This() {
        return .{ .allocator = a, .manager = manager, .cpu = .init(source) };
    }
    pub fn resumeWithAdmission(self: *@This(), original: admission.Port) !void {
        try original.checkpoint();
        if (self.taken or self.retired or self.retiring) return error.InvalidArgument;
        const bound = try self.cpu.memoryBoundWithAdmission(original);
        if (self.credit == null) self.credit = try self.manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, bound, original);
        try self.cpu.resumeWithAdmission(self.allocator, original);
    }
    pub fn takeWithAdmission(self: *@This(), destination: resources.Slice, usage: *u64, original: admission.Port) !Run {
        try original.checkpoint();
        if (self.taken or self.retired or self.retiring or !self.cpu.ready) return error.InvalidArgument;
        try self.credit.?.adoptUsageWithAdmission(destination, usage, self.cpu.resident_bytes, original);
        const out = self.cpu.takeWithAdmission(null) catch unreachable;
        self.credit.?.retire();
        self.credit = null;
        self.taken = true;
        return out;
    }
    pub fn retireWithAdmission(self: *@This(), original: admission.Port) !void {
        try original.checkpoint();
        if (self.taken or self.retired) return;
        self.retiring = true;
        try self.cpu.retireWithAdmission(self.allocator, original);
        if (self.credit) |*credit| credit.retire();
        self.credit = null;
        self.retired = true;
    }
    pub fn deinit(self: *@This()) void {
        if (self.taken or self.retired) return;
        self.cpu.deinit(self.allocator);
        if (self.credit) |*credit| credit.retire();
        self.credit = null;
        self.retired = true;
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
fn inputRun(bytes: []u8) Run {
    return .{ .id = 77, .read_identity = .{ .issuer = 1, .sequence = 44 }, .level = 0, .size_bytes = 901, .path = @constCast("/snapshot.tbl"), .smallest_namespace_name = null, .smallest_key = bytes, .largest_namespace_name = @constCast(""), .largest_key = bytes, .entry_count = 1, .tombstone_count = 1, .visibility_id = 71, .gc_requested = true, .bloom_filter = null, .state = null, .owns_metadata = false };
}
fn verify(source: Run, output: Run) !void {
    try std.testing.expectEqual(source.id, output.id);
    try std.testing.expectEqual(source.level, output.level);
    try std.testing.expectEqual(source.visibility_id, output.visibility_id);
    try std.testing.expectEqual(source.tombstone_count, output.tombstone_count);
    try std.testing.expectEqualDeep(source.read_identity, output.read_identity);
    try std.testing.expectEqualSlices(u8, source.smallest_key, output.smallest_key);
    try std.testing.expectEqualSlices(u8, source.largest_key, output.largest_key);
    try std.testing.expect(output.owner == null and output.output_ticket == null and !output.version_ref_pinned);
    if (source.shared_memory != null) try std.testing.expect(source.shared_memory == output.shared_memory) else {
        try std.testing.expect(source.smallest_key.ptr != output.smallest_key.ptr);
        try std.testing.expect(output.bloom_filter == null and output.table_index == null);
    }
    try std.testing.expectEqual(source.path == null, output.path == null);
    if (source.path == null) {
        try std.testing.expectEqual(source.state.?.entryCount(), output.state.?.entryCount());
        for (0..source.state.?.entryCount()) |i| {
            try std.testing.expectEqualSlices(u8, source.state.?.entryAt(i).key, output.state.?.entryAt(i).key);
            try std.testing.expectEqualSlices(u8, source.state.?.entryAt(i).value, output.state.?.entryAt(i).value);
        }
    }
}

test "controlled physical sync complete run snapshots retain every source mode rejection and output retirement" {
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 2 * Metadata.quantum + 5);
    defer a.free(bytes);
    @memset(bytes, 'r');
    bytes[0] = 0;
    var state: State = .{};
    defer state.deinit(a);
    try state.appendUpsert(a, .{ .name = "docs" }, bytes, bytes, true);
    var disk = inputRun(bytes);
    var memory = inputRun(bytes);
    memory.path = null;
    memory.state = state;
    var owner_copy = Metadata.init(memory);
    defer owner_copy.deinit(a);
    try owner_copy.resumeWithAdmission(a, null);
    var shared = try owner_copy.take();
    shared.state = try state.clone(a);
    try shared.shareMemory(a);
    defer shared.deinit(a);
    for ([_]Run{ disk, memory, shared }) |source| {
        inline for (.{ "plan", "copy", "take", "abort", "output" }) |stage| {
            var checks: usize = 1;
            var target: usize = 0;
            while (target <= checks) : (target += 1) {
                for ([_]bool{ false, true }) |abort| {
                    if (target == 0 and abort) continue;
                    var clone = Clone.init(source);
                    defer clone.deinit(a);
                    if (!std.mem.eql(u8, stage, "plan")) _ = try clone.memoryBoundWithAdmission(null);
                    if (std.mem.eql(u8, stage, "take") or std.mem.eql(u8, stage, "abort") or std.mem.eql(u8, stage, "output")) try clone.resumeWithAdmission(a, null);
                    var output: ?Run = null;
                    defer if (output) |out| {
                        var r = Retirement.init(out) catch unreachable;
                        r.deinit(a);
                    };
                    var retirement: ?Retirement = null;
                    defer if (retirement) |*r| r.deinit(a);
                    if (std.mem.eql(u8, stage, "output")) retirement = try Retirement.init(try clone.takeWithAdmission(null));
                    var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
                        0 => error.Canceled,
                        1 => error.DeadlineExceeded,
                        else => error.FileNotFound,
                    } };
                    const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
                    const port = admission.Port.init(&payload);
                    const result: anyerror!void = if (std.mem.eql(u8, stage, "plan")) blk: {
                        _ = clone.memoryBoundWithAdmission(port) catch |e| break :blk e;
                        break :blk {};
                    } else if (std.mem.eql(u8, stage, "copy")) clone.resumeWithAdmission(a, port) else if (std.mem.eql(u8, stage, "abort")) clone.retireWithAdmission(a, port) else if (std.mem.eql(u8, stage, "output")) retirement.?.retireWithAdmission(a, port) else blk: {
                        output = clone.takeWithAdmission(port) catch |e| break :blk e;
                        break :blk {};
                    };
                    if (target == 0) {
                        try result;
                        checks = fault.checks;
                    } else try std.testing.expectError(fault.failure, result);
                    if (retirement) |*r| try r.retireWithAdmission(a, port) else if (abort or std.mem.eql(u8, stage, "abort")) try clone.retireWithAdmission(a, port) else {
                        if (output == null) {
                            _ = try clone.memoryBoundWithAdmission(port);
                            try clone.resumeWithAdmission(a, port);
                            try std.testing.expect(clone.resident_bytes <= clone.bound.?);
                            output = try clone.takeWithAdmission(port);
                        }
                        try verify(source, output.?);
                    }
                    try std.testing.expectEqualSlices(u8, bytes, source.smallest_key);
                }
            }
        }
    }
    disk.version_ref_pinned = true;
    try std.testing.expectError(error.InvalidArgument, Retirement.init(disk));
    var missing = memory;
    missing.state = null;
    var clone = Clone.init(missing);
    defer clone.deinit(a);
    try std.testing.expectError(error.RunStateUnavailable, clone.memoryBoundWithAdmission(null));
}

test "controlled physical sync shared snapshot last reference retains state metadata filter and header through rejection" {
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 3 * Metadata.quantum + 1);
    defer a.free(bytes);
    @memset(bytes, 'l');
    var checks: usize = 1;
    var target: usize = 0;
    while (target <= checks) : (target += 1) {
        var seed = inputRun(bytes);
        seed.path = null;
        var metadata = Metadata.init(seed);
        defer metadata.deinit(a);
        try metadata.resumeWithAdmission(a, null);
        var source = try metadata.take();
        source.state = .{};
        try source.state.?.appendUpsert(a, .{}, bytes, bytes, true);
        var bloom = try @import("bloom").Builder.init(a, 50000, .{});
        source.bloom_filter = bloom.finish();
        source.owns_bloom_filter = true;
        try source.shareMemory(a);
        var clone = Clone.init(source);
        defer clone.deinit(a);
        try std.testing.expectEqual(@as(u64, 0), try clone.memoryBoundWithAdmission(null));
        try clone.resumeWithAdmission(a, null);
        const output = try clone.takeWithAdmission(null);
        source.deinit(a); // The returned snapshot now owns the final memory pin.
        var retirement = try Retirement.init(output);
        defer retirement.deinit(a);
        try std.testing.expect(!retirement.complete and retirement.shared != null);
        var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
            0 => error.Canceled,
            1 => error.DeadlineExceeded,
            else => error.FileNotFound,
        } };
        const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
        const port = admission.Port.init(&payload);
        const result = retirement.retireWithAdmission(a, port);
        if (target == 0) {
            try result;
            checks = fault.checks;
        } else {
            try std.testing.expectError(fault.failure, result);
            try std.testing.expect(!retirement.complete);
            try retirement.retireWithAdmission(a, port);
        }
        try std.testing.expect(retirement.complete and retirement.shared == null);
        fault.at = fault.checks;
        try std.testing.expectError(fault.failure, retirement.retireWithAdmission(a, port));
    }
}

const Witness = struct {
    manager: *resources.ResourceManager,
    live: usize = 0,
    calls: usize = 0,
    fail_at: usize = std.math.maxInt(usize),
    fn funded(self: *@This()) u64 {
        return self.manager.sliceStats(.lsm_table_builder_working_set).used_bytes + self.manager.sliceStats(.lsm_in_memory_state).used_bytes;
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

fn exerciseOwned(source: Run, stage: enum { construct, adopt, abort, output }, at: usize, abandon: bool) !usize {
    const a = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    var witness: Witness = .{ .manager = &manager };
    var owned = Owned.init(witness.allocator(), &manager, source);
    defer owned.deinit();
    var fault: Fault = .{ .failure = switch (at % 3) {
        0 => error.Canceled,
        1 => error.DeadlineExceeded,
        else => error.FileNotFound,
    } };
    const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
    const original = admission.Port.init(&payload);
    var usage: u64 = 0;
    try manager.prepareUsageWithAdmission(.lsm_in_memory_state, &usage, original);
    defer manager.observeUsage(.lsm_in_memory_state, &usage, 0);
    if (stage != .construct) try owned.resumeWithAdmission(original);
    var output: ?Run = null;
    defer if (output) |out| {
        var r = Retirement.init(out) catch unreachable;
        r.deinit(witness.allocator());
    };
    var retirement: ?Retirement = null;
    defer if (retirement) |*r| r.deinit(witness.allocator());
    if (stage == .output) retirement = try Retirement.init(try owned.takeWithAdmission(.lsm_in_memory_state, &usage, original));
    fault.checks = 0;
    fault.at = at;
    const result: anyerror!void = switch (stage) {
        .construct => owned.resumeWithAdmission(original),
        .adopt => blk: {
            output = owned.takeWithAdmission(.lsm_in_memory_state, &usage, original) catch |err| break :blk err;
            break :blk {};
        },
        .abort => owned.retireWithAdmission(original),
        .output => retirement.?.retireWithAdmission(witness.allocator(), original),
    };
    const checks = fault.checks;
    if (at == std.math.maxInt(usize)) try result else {
        try std.testing.expectError(fault.failure, result);
        if (stage == .construct or stage == .adopt) try std.testing.expectEqual(@as(u64, 0), usage);
        if (stage == .output) try std.testing.expect(usage > 0);
    }
    if (stage == .output) {
        try retirement.?.retireWithAdmission(witness.allocator(), original);
        try std.testing.expectEqual(@as(usize, 0), witness.live);
        try manager.adjustUsageWithAdmission(.lsm_in_memory_state, &usage, 0, original);
    } else if (stage == .abort or (abandon and !owned.taken)) {
        try owned.retireWithAdmission(original);
        try std.testing.expectEqual(@as(usize, 0), witness.live);
    } else {
        if (output == null) {
            try owned.resumeWithAdmission(original);
            output = try owned.takeWithAdmission(.lsm_in_memory_state, &usage, original);
        }
        try verify(source, output.?);
        try std.testing.expectEqual(@as(u64, witness.live), usage);
        var r = try Retirement.init(output.?);
        output = null;
        try r.retireWithAdmission(witness.allocator(), original);
        try std.testing.expectEqual(@as(usize, 0), witness.live);
        try manager.adjustUsageWithAdmission(.lsm_in_memory_state, &usage, 0, original);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    return checks;
}

test "controlled physical sync complete snapshot ownership funds every allocation and retains exact adoption and retirement charge" {
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 2 * Metadata.quantum + 1);
    defer a.free(bytes);
    @memset(bytes, 'f');
    var state: State = .{};
    defer state.deinit(a);
    try state.appendUpsert(a, .{ .name = "docs" }, bytes, bytes, false);
    var source = inputRun(bytes);
    source.path = null;
    source.state = state;
    inline for (.{ .construct, .adopt, .abort, .output }) |stage| {
        const checks = try exerciseOwned(source, stage, std.math.maxInt(usize), false);
        for (0..checks) |at| {
            _ = try exerciseOwned(source, stage, at, false);
            if (stage == .construct or stage == .adopt) _ = try exerciseOwned(source, stage, at, true);
        }
    }
    var allocations: usize = 0;
    {
        var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
        defer manager.deinit(a);
        var witness: Witness = .{ .manager = &manager };
        var owned = Owned.init(witness.allocator(), &manager, source);
        defer owned.deinit();
        var fault: Fault = .{};
        const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
        try owned.resumeWithAdmission(admission.Port.init(&payload));
        allocations = witness.calls;
    }
    for (0..allocations) |at| for ([_]bool{ false, true }) |abort| {
        var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
        defer manager.deinit(a);
        var witness: Witness = .{ .manager = &manager, .fail_at = at };
        var owned = Owned.init(witness.allocator(), &manager, source);
        defer owned.deinit();
        var fault: Fault = .{};
        const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
        const original = admission.Port.init(&payload);
        try std.testing.expectError(error.OutOfMemory, owned.resumeWithAdmission(original));
        try std.testing.expect(owned.credit != null and !owned.taken);
        witness.fail_at = std.math.maxInt(usize);
        if (!abort) try owned.resumeWithAdmission(original);
        try owned.retireWithAdmission(original);
        try std.testing.expectEqual(@as(usize, 0), witness.live);
    };
    var options: resources.Options = .{ .identity_allocator = a };
    options.budgets[@backingInt(resources.Slice.lsm_table_builder_working_set)].hard_limit_bytes = 1;
    var manager = resources.ResourceManager.init(options);
    defer manager.deinit(a);
    var witness: Witness = .{ .manager = &manager };
    var owned = Owned.init(witness.allocator(), &manager, source);
    defer owned.deinit();
    var fault: Fault = .{};
    const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
    try std.testing.expectError(error.ResourceBudgetExceeded, owned.resumeWithAdmission(admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 0), witness.calls);
}

test "controlled physical sync output retirement independently yields file private and shared physical state without allocation" {
    const a = std.testing.allocator;
    const TableIndex = @import("../lsm/table_file.zig").TableIndex;
    for (0..3) |mode| {
        var failing = std.testing.FailingAllocator.init(a, .{});
        const backing = failing.allocator();
        var run: Run = .{ .id = 991, .level = 1, .size_bytes = 1, .path = null, .smallest_namespace_name = null, .smallest_key = try backing.alloc(u8, 65539), .largest_namespace_name = null, .largest_key = &.{}, .entry_count = 1, .bloom_filter = null, .state = null };
        var transferred = false;
        defer if (!transferred) run.deinit(backing);
        @memset(run.smallest_key, 'k');
        run.largest_key = try backing.dupe(u8, "z");
        run.bloom_filter = .{ .bytes = try backing.alloc(u8, 65543), .bit_count = 0, .hash_count = 0 };
        if (mode == 0) {
            run.path = try backing.dupe(u8, "fresh-output.sst");
            run.table_index = TableIndex{ .entry_data_start = 0, .entry_data_len = 0, .filter = .{ .bytes = try backing.alloc(u8, 65541), .bit_count = 0, .hash_count = 0 } };
        } else {
            run.state = .{};
            try run.state.?.appendUpsert(backing, .{ .name = "docs" }, run.smallest_key, run.smallest_key, false);
            if (mode == 2) {
                try run.shareMemory(backing);
                const reader = run.retainMemory().?;
                const first = try Retirement.init(reader);
                try std.testing.expect(first.complete);
                try std.testing.expectEqualStrings("z", run.largest_key);
            }
        }
        var cursor = try Retirement.init(run);
        transferred = true;
        defer cursor.deinit(backing);
        failing.fail_index = failing.alloc_index;
        const allocations = failing.alloc_index;
        var steps: usize = 0;
        while (!cursor.complete) {
            const before = cursor;
            var fault: Fault = .{ .at = 0, .failure = error.DeadlineExceeded };
            const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
            try std.testing.expectError(error.DeadlineExceeded, cursor.step(backing, admission.Port.init(&payload)));
            try std.testing.expectEqual(before.filter_offset, cursor.filter_offset);
            try std.testing.expectEqual(before.complete, cursor.complete);
            try std.testing.expect(before.shared == cursor.shared);
            try std.testing.expectEqual(before.state != null, cursor.state != null);
            try std.testing.expectEqual(before.metadata != null, cursor.metadata != null);
            try std.testing.expectEqual(before.index != null, cursor.index != null);
            if (before.metadata) |metadata| try std.testing.expect(std.meta.eql(metadata, cursor.metadata.?));
            if (before.index) |index| try std.testing.expect(std.meta.eql(index, cursor.index.?));
            if (before.state) |state| {
                try std.testing.expectEqual(state.index, cursor.state.?.index);
                try std.testing.expectEqual(state.vector_offset, cursor.state.?.vector_offset);
            }
            // A shared final free must use the captured allocator, even when
            // a different valid allocator is supplied by the cleanup caller.
            _ = try cursor.step(if (mode == 2) a else backing, null);
            steps += 1;
        }
        try std.testing.expect(steps > 12);
        try std.testing.expectEqual(allocations, failing.alloc_index);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}
