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

//! Prepaid immutable-state clone ownership. Caller prepays this fixed header,
//! pins immutable input through take/abort, and prearranges output usage and
//! physical retirement. Reused shared rows retain their existing accounting
//! roots; only newly allocated vectors/raw bytes move construction credit.
const std = @import("std");
const State = @import("state.zig").State;
const admission = @import("../../common/read_admission.zig");
const resources = @import("../resource_manager.zig");

pub const Owned = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    cpu: State.Clone,
    credit: ?resources.ConstructionCredit = null,
    taken: bool = false,
    retiring: bool = false,
    retired: bool = false,
    pub fn init(a: std.mem.Allocator, manager: *resources.ResourceManager, source: *const State) @This() {
        return .{ .allocator = a, .manager = manager, .cpu = .init(source) };
    }
    pub fn resumeWithAdmission(self: *@This(), original: admission.Port) !void {
        try original.checkpoint();
        if (self.taken or self.retiring or self.retired) return error.InvalidArgument;
        const bound = try self.cpu.memoryBoundWithAdmission(original);
        if (self.credit == null) self.credit = try self.manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, bound, original);
        try self.cpu.resumeWithAdmission(self.allocator, original);
    }
    pub fn takeWithAdmission(self: *@This(), destination: resources.Slice, usage: *u64, original: admission.Port) !State {
        try original.checkpoint();
        if (self.taken or self.retiring or self.retired or !self.cpu.ready) return error.InvalidArgument;
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
    /// Explicit joined/quiesced fallback; refusal retains this owner normally.
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

fn exercise(source: *const State, stage: enum { construct, adopt, abort, output_retirement }, at: usize, abandon: bool) !usize {
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
    const payload = fault.payload();
    const original = admission.Port.init(&payload);
    var usage: u64 = 0;
    try manager.prepareUsageWithAdmission(.lsm_in_memory_state, &usage, original);
    defer manager.observeUsage(.lsm_in_memory_state, &usage, 0);
    if (stage != .construct) try owned.resumeWithAdmission(original);
    var output: ?State = null;
    defer if (output) |*out| out.deinit(witness.allocator());
    var retirement: ?State.Reclaimer = null;
    const drain = struct {
        fn run(value: *State.Reclaimer, allocator: std.mem.Allocator, port: admission.Port) !void {
            while (true) {
                try port.checkpoint();
                var credits: usize = 1;
                if (value.step(allocator, &credits)) return;
            }
        }
    }.run;
    if (stage == .output_retirement) retirement = .init(try owned.takeWithAdmission(.lsm_in_memory_state, &usage, original));
    fault.checks = 0;
    fault.at = at;
    const result: anyerror!void = switch (stage) {
        .construct => owned.resumeWithAdmission(original),
        .adopt => blk: {
            output = owned.takeWithAdmission(.lsm_in_memory_state, &usage, original) catch |err| break :blk err;
            break :blk {};
        },
        .abort => owned.retireWithAdmission(original),
        .output_retirement => drain(&retirement.?, witness.allocator(), original),
    };
    const checks = fault.checks;
    if (at == std.math.maxInt(usize)) try result else {
        try std.testing.expectError(fault.failure, result);
        if (stage == .construct or stage == .adopt) try std.testing.expectEqual(@as(u64, 0), usage);
        if (stage == .output_retirement) try std.testing.expect(usage > 0);
    }
    if (stage == .output_retirement) {
        try drain(&retirement.?, witness.allocator(), original);
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
        try std.testing.expectEqual(@as(u64, witness.live), usage);
        try std.testing.expectEqual(source.entryCount(), output.?.entryCount());
        for (0..source.entryCount()) |i| {
            const input = source.entryAt(i);
            const out = output.?.entryAt(i);
            try std.testing.expectEqualSlices(u8, input.key, out.key);
            try std.testing.expectEqualSlices(u8, input.value, out.value);
        }
        var value = State.Reclaimer.init(output.?);
        output = null;
        try drain(&value, witness.allocator(), original);
        try std.testing.expectEqual(@as(usize, 0), witness.live);
        try manager.adjustUsageWithAdmission(.lsm_in_memory_state, &usage, 0, original);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    return checks;
}

test "controlled physical sync owned state cloning prepays every allocation and preserves rejection through adoption and retirement" {
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 2 * State.Clone.quantum + 5);
    defer a.free(bytes);
    @memset(bytes, 'o');
    var source: State = .{};
    defer source.deinit(a);
    try source.appendUpsert(a, .{ .name = "docs" }, bytes, bytes, true);
    inline for (.{ .construct, .adopt, .abort, .output_retirement }) |stage| {
        const checks = try exercise(&source, stage, std.math.maxInt(usize), false);
        for (0..checks) |at| {
            _ = try exercise(&source, stage, at, false);
            if (stage == .construct or stage == .adopt) _ = try exercise(&source, stage, at, true);
        }
    }
    for (0..4) |at| for ([_]bool{ false, true }) |abort| {
        var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
        defer manager.deinit(a);
        var witness: Witness = .{ .manager = &manager, .fail_at = at };
        var owned = Owned.init(witness.allocator(), &manager, &source);
        defer owned.deinit();
        var fault: Fault = .{};
        const payload = fault.payload();
        const port = admission.Port.init(&payload);
        try std.testing.expectError(error.OutOfMemory, owned.resumeWithAdmission(port));
        try std.testing.expect(owned.credit != null and !owned.taken);
        witness.fail_at = std.math.maxInt(usize);
        if (!abort) try owned.resumeWithAdmission(port);
        try owned.retireWithAdmission(port);
        try std.testing.expectEqual(@as(usize, 0), witness.live);
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    };
    var options: resources.Options = .{ .identity_allocator = a };
    options.budgets[@backingInt(resources.Slice.lsm_table_builder_working_set)].hard_limit_bytes = 1;
    var manager = resources.ResourceManager.init(options);
    defer manager.deinit(a);
    var witness: Witness = .{ .manager = &manager };
    var owned = Owned.init(witness.allocator(), &manager, &source);
    defer owned.deinit();
    var fault: Fault = .{};
    const payload = fault.payload();
    try std.testing.expectError(error.ResourceBudgetExceeded, owned.resumeWithAdmission(admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 0), witness.calls);
}
