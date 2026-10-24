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

//! Retained run metadata ownership. Input metadata is immutable and borrowed
//! until copying finishes. This deliberately excludes filter/index/state and
//! file-pin ownership; callers retain those authorities separately. No original
//! port is stored. Legacy snapshot construction drives this same copier.
const std = @import("std");
const Run = @import("repository.zig").Run;
const admission = @import("../../common/read_admission.zig");
const resources = @import("../resource_manager.zig");

pub const Clone = struct {
    pub const quantum = 16 * 1024;
    source: Run,
    fields: [5]?[]u8 = @splat(null),
    field: usize = 0,
    offset: usize = 0,
    retire_field: usize = 0,
    retire_offset: usize = 0,
    ready: bool = false,
    taken: bool = false,
    retiring: bool = false,
    retired: bool = false,

    pub fn init(source: Run) Clone {
        return .{ .source = source };
    }
    fn input(self: *const Clone, field: usize) ?[]const u8 {
        return switch (field) {
            0 => self.source.smallest_namespace_name,
            1 => self.source.smallest_key,
            2 => self.source.largest_namespace_name,
            3 => self.source.largest_key,
            4 => self.source.path,
            else => unreachable,
        };
    }
    pub fn memoryBound(self: *const Clone) !u64 {
        var bytes: u64 = 0;
        for (0..5) |i| if (self.input(i)) |value| {
            bytes = try std.math.add(u64, bytes, value.len);
        };
        return bytes;
    }
    fn checkpoint(original: ?admission.Port) !void {
        if (original) |port| try port.checkpoint();
    }
    pub fn resumeWithAdmission(self: *Clone, allocator: std.mem.Allocator, original: ?admission.Port) !void {
        try checkpoint(original);
        if (self.retiring or self.taken or self.retired) return error.InvalidArgument;
        while (self.field < self.fields.len) {
            try checkpoint(original);
            const input_bytes = self.input(self.field) orelse {
                self.field += 1;
                continue;
            };
            if (self.fields[self.field] == null) {
                self.fields[self.field] = if (input_bytes.len == 0) try allocator.alloc(u8, 0) else blk: {
                    const ptr = allocator.rawAlloc(input_bytes.len, .of(u8), @returnAddress()) orelse return error.OutOfMemory;
                    break :blk ptr[0..input_bytes.len];
                };
                self.offset = 0;
                continue;
            }
            const n = @min(input_bytes.len - self.offset, quantum);
            @memcpy(self.fields[self.field].?[self.offset..][0..n], input_bytes[self.offset..][0..n]);
            self.offset += n;
            if (self.offset == input_bytes.len) {
                self.field += 1;
                self.offset = 0;
            }
        }
        self.ready = true;
    }
    pub fn take(self: *Clone) !Run {
        if (!self.ready or self.taken or self.retiring or self.retired) return error.InvalidArgument;
        const out: Run = .{
            .id = self.source.id,
            .read_identity = self.source.read_identity,
            .level = self.source.level,
            .size_bytes = self.source.size_bytes,
            .compression_stats = self.source.compression_stats,
            .path = self.fields[4],
            .smallest_namespace_name = self.fields[0],
            .smallest_key = self.fields[1].?,
            .largest_namespace_name = self.fields[2],
            .largest_key = self.fields[3].?,
            .entry_count = self.source.entry_count,
            .tombstone_count = self.source.tombstone_count,
            .oldest_tombstone_unix_ns = self.source.oldest_tombstone_unix_ns,
            .visibility_id = self.source.visibility_id,
            .gc_requested = self.source.gc_requested,
            .bloom_filter = null,
            .owns_bloom_filter = false,
            .state = null,
        };
        self.fields = @splat(null);
        self.taken = true;
        return out;
    }
    /// Consume only isolated metadata, such as the output of take(). All file
    /// pins, filters, indexes and state must already have separate owners.
    pub fn retireOwned(run: Run) !Clone {
        if (!run.owns_metadata or run.owner != null or run.shared_memory != null or run.state != null or run.bloom_filter != null or run.table_index != null or run.output_ticket != null or run.version_ref_pinned or run.shared_read_version) return error.InvalidArgument;
        return .{ .source = run, .fields = .{ run.smallest_namespace_name, run.smallest_key, run.largest_namespace_name, run.largest_key, run.path }, .retiring = true };
    }
    pub fn retireWithAdmission(self: *Clone, allocator: std.mem.Allocator, original: ?admission.Port) !void {
        try checkpoint(original);
        if (self.retired or self.taken) return;
        self.retiring = true;
        while (self.retire_field < self.fields.len) {
            try checkpoint(original);
            if (self.fields[self.retire_field]) |value| {
                // Retain the entire allocation and charge until its bounded
                // byte retirement is complete, including rejected originals.
                const n = @min(value.len - self.retire_offset, quantum);
                @memset(value[self.retire_offset..][0..n], undefined);
                self.retire_offset += n;
                if (self.retire_offset != value.len) continue;
                if (value.len != 0) allocator.rawFree(value, .of(u8), @returnAddress());
                self.fields[self.retire_field] = null;
            }
            self.retire_field += 1;
            self.retire_offset = 0;
        }
        self.retired = true;
    }
    pub fn deinit(self: *Clone, allocator: std.mem.Allocator) void {
        self.retireWithAdmission(allocator, null) catch unreachable;
    }
};

/// Caller prepays this fixed header and keeps allocator/manager/input authority
/// alive. Payload credit precedes every allocation. Successful take moves the
/// exact resident bytes into a preprepared, stable caller usage identity; that
/// caller must retain the charge until its eventual physical retirement.
pub const Owned = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    cpu: Clone,
    credit: ?resources.ConstructionCredit = null,
    retiring: bool = false,
    retired: bool = false,
    taken: bool = false,
    pub fn init(allocator: std.mem.Allocator, manager: *resources.ResourceManager, source: Run) Owned {
        return .{ .allocator = allocator, .manager = manager, .cpu = .init(source) };
    }
    pub fn resumeWithAdmission(self: *Owned, original: admission.Port) !void {
        if (self.retiring or self.retired or self.taken) return error.InvalidArgument;
        try original.checkpoint();
        if (self.credit == null) self.credit = try self.manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, try self.cpu.memoryBound(), original);
        try self.cpu.resumeWithAdmission(self.allocator, original);
    }
    pub fn takeWithAdmission(self: *Owned, destination: resources.Slice, usage: *u64, original: admission.Port) !Run {
        if (!self.cpu.ready or self.retiring or self.retired or self.taken) return error.InvalidArgument;
        try original.checkpoint();
        try self.credit.?.adoptUsageWithAdmission(destination, usage, try self.cpu.memoryBound(), original);
        // No fallible gap after resident accounting moves.
        const out = self.cpu.take() catch unreachable;
        self.credit.?.retire();
        self.credit = null;
        self.taken = true;
        return out;
    }
    pub fn retireWithAdmission(self: *Owned, original: admission.Port) !void {
        try original.checkpoint();
        if (self.retired) return;
        self.retiring = true;
        try self.cpu.retireWithAdmission(self.allocator, original);
        if (self.credit) |*credit| credit.retire();
        self.credit = null;
        self.retired = true;
    }
    /// Explicit joined/quiesced fallback, not scoped refusal cleanup.
    pub fn deinit(self: *Owned) void {
        if (self.retired) return;
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
fn sourceRun(bytes: []u8) Run {
    return .{ .id = 71, .read_identity = .{ .issuer = 17, .sequence = 22 }, .level = 2, .size_bytes = 991, .path = @constCast("/runs/71.tbl"), .smallest_namespace_name = null, .smallest_key = bytes, .largest_namespace_name = @constCast(""), .largest_key = bytes, .entry_count = 5, .tombstone_count = 2, .visibility_id = 19, .gc_requested = true, .bloom_filter = null, .state = null, .owns_metadata = false };
}
fn checkOutput(source: Run, out: Run) !void {
    try std.testing.expectEqual(source.id, out.id);
    try std.testing.expectEqual(source.level, out.level);
    try std.testing.expectEqual(source.size_bytes, out.size_bytes);
    try std.testing.expectEqual(source.entry_count, out.entry_count);
    try std.testing.expectEqual(source.tombstone_count, out.tombstone_count);
    try std.testing.expectEqual(source.visibility_id, out.visibility_id);
    try std.testing.expectEqual(source.gc_requested, out.gc_requested);
    try std.testing.expectEqualDeep(source.read_identity, out.read_identity);
    const Pair = struct { input: ?[]u8, output: ?[]u8 };
    for ([_]Pair{ .{ .input = source.smallest_namespace_name, .output = out.smallest_namespace_name }, .{ .input = source.largest_namespace_name, .output = out.largest_namespace_name } }) |pair| {
        try std.testing.expectEqual(pair.input == null, pair.output == null);
        if (pair.input) |value| try std.testing.expectEqualSlices(u8, value, pair.output.?);
    }
    try std.testing.expectEqualSlices(u8, source.smallest_key, out.smallest_key);
    try std.testing.expectEqualSlices(u8, source.largest_key, out.largest_key);
    try std.testing.expectEqualStrings(source.path.?, out.path.?);
    try std.testing.expect(source.smallest_key.ptr != out.smallest_key.ptr and out.smallest_key.ptr != out.largest_key.ptr);
    try std.testing.expect(out.owner == null and out.output_ticket == null and out.bloom_filter == null and out.table_index == null and out.state == null);
}
const Stage = enum { construct, adopt, abandon, output_retirement };
fn exercise(source: Run, stage: Stage, at: usize, abandon: bool) !usize {
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
    var retirement: ?Clone = null;
    defer if (retirement) |*value| value.deinit(witness.allocator());
    var output: ?Run = null;
    defer if (output) |*value| value.deinit(witness.allocator());
    if (stage == .output_retirement) {
        const out = try owned.takeWithAdmission(.lsm_in_memory_state, &usage, original);
        try checkOutput(source, out);
        retirement = try Clone.retireOwned(out);
    }
    fault.checks = 0;
    fault.at = at;
    const result: anyerror!void = switch (stage) {
        .construct => owned.resumeWithAdmission(original),
        .adopt => take: {
            output = owned.takeWithAdmission(.lsm_in_memory_state, &usage, original) catch |cause| break :take cause;
            break :take {};
        },
        .abandon => owned.retireWithAdmission(original),
        .output_retirement => retirement.?.retireWithAdmission(witness.allocator(), original),
    };
    const checks = fault.checks;
    if (at == std.math.maxInt(usize)) try result else {
        try std.testing.expectError(fault.failure, result);
        try std.testing.expect(fault.rejected);
        if (stage == .construct or stage == .adopt) try std.testing.expect(usage == 0 and !owned.taken);
        if (stage == .output_retirement) try std.testing.expectEqual(try owned.cpu.memoryBound(), usage);
    }
    if (stage == .output_retirement) {
        try retirement.?.retireWithAdmission(witness.allocator(), original);
        retirement = null;
        try std.testing.expectEqual(@as(usize, 0), witness.live);
        try manager.adjustUsageWithAdmission(.lsm_in_memory_state, &usage, 0, original);
    } else if (stage == .abandon or (abandon and !owned.taken)) {
        try owned.retireWithAdmission(original);
        try std.testing.expectError(error.InvalidArgument, owned.resumeWithAdmission(original));
    } else {
        if (output == null) {
            try owned.resumeWithAdmission(original);
            output = try owned.takeWithAdmission(.lsm_in_memory_state, &usage, original);
        }
        try checkOutput(source, output.?);
        var retired = try Clone.retireOwned(output.?);
        output = null;
        defer retired.deinit(witness.allocator());
        try retired.retireWithAdmission(witness.allocator(), original);
        try manager.adjustUsageWithAdmission(.lsm_in_memory_state, &usage, 0, original);
    }
    try std.testing.expectEqual(@as(usize, 0), witness.live);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), usage);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
    return checks;
}

test "controlled physical sync run metadata clone prepays large fields and retains every original rejection" {
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 2 * Clone.quantum + 17);
    defer a.free(bytes);
    @memset(bytes, 'q');
    for ([_]bool{ false, true }) |named| {
        var source = sourceRun(bytes);
        if (named) {
            source.smallest_namespace_name = bytes;
            source.largest_namespace_name = @constCast("docs");
        }
        for (std.enums.values(Stage)) |stage| {
            const checks = try exercise(source, stage, std.math.maxInt(usize), false);
            for (0..checks) |at| _ = try exercise(source, stage, at, at % 2 == 1);
        }
    }
    try std.testing.expectEqual(@as(u8, 'q'), bytes[0]);
    try std.testing.expectEqual(@as(u8, 'q'), bytes[bytes.len - 1]);
}

test "controlled physical sync run metadata clone recovers or abandons every allocation refusal and denies unfunded work" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |named| {
        var source = sourceRun(@constCast("metadata"));
        if (named) {
            source.smallest_namespace_name = @constCast("docs");
            source.largest_namespace_name = @constCast("docs");
        }
        for ([_]bool{ false, true }) |abandon| {
            for (0..@as(usize, if (named) 5 else 3)) |at| {
                var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
                defer manager.deinit(a);
                var witness: Witness = .{ .manager = &manager, .fail_at = at };
                var owned = Owned.init(witness.allocator(), &manager, source);
                defer owned.deinit();
                var fault: Fault = .{};
                const payload = fault.payload();
                const original = admission.Port.init(&payload);
                try std.testing.expectError(error.OutOfMemory, owned.resumeWithAdmission(original));
                try std.testing.expect(owned.credit != null and !owned.taken);
                witness.fail_at = std.math.maxInt(usize);
                if (!abandon) try owned.resumeWithAdmission(original);
                try owned.retireWithAdmission(original);
                try std.testing.expectEqual(@as(usize, 0), witness.live);
                try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
            }
        }
    }
    var options: resources.Options = .{ .identity_allocator = a };
    options.budgets[@backingInt(resources.Slice.lsm_table_builder_working_set)].hard_limit_bytes = 1;
    var manager = resources.ResourceManager.init(options);
    defer manager.deinit(a);
    var witness: Witness = .{ .manager = &manager };
    var owned = Owned.init(witness.allocator(), &manager, sourceRun(@constCast("denied")));
    defer owned.deinit();
    var fault: Fault = .{};
    const payload = fault.payload();
    try std.testing.expectError(error.ResourceBudgetExceeded, owned.resumeWithAdmission(admission.Port.init(&payload)));
    try std.testing.expectEqual(@as(usize, 0), witness.calls);
    try std.testing.expect(owned.credit == null);
}

test "controlled physical sync run metadata destruction retains exact debt behind a busy parent" {
    const a = std.testing.allocator;
    var parent = resources.ResourceManager.init(.{ .identity_allocator = a });
    defer parent.deinit(a);
    var manager = try resources.metadata_fixture.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    try resources.metadata_fixture.installParent(&manager, &parent);
    var witness: Witness = .{ .manager = &manager };
    var owned = Owned.init(witness.allocator(), &manager, sourceRun(@constCast("parent-debt")));
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

test "controlled physical sync metadata copy stops after raw allocation before whole buffer poisoning" {
    const a = std.testing.allocator;
    const Pattern = struct {
        clone: *Clone,
        rejected: bool = false,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const ptr = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
            @memset(ptr[0..len], 0x5a);
            return ptr;
        }
        fn free(_: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            std.testing.allocator.rawFree(bytes, alignment, ra);
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.rejected) if (self.clone.fields[1]) |bytes| {
                try std.testing.expectEqual(@as(usize, 0), self.clone.offset);
                try std.testing.expectEqual(@as(u8, 0x5a), bytes[0]);
                try std.testing.expectEqual(@as(u8, 0x5a), bytes[bytes.len - 1]);
                self.rejected = true;
                return error.Canceled;
            };
        }
    };
    const bytes = try a.alloc(u8, 3 * Clone.quantum + 1);
    defer a.free(bytes);
    @memset(bytes, 'm');
    var clone = Clone.init(sourceRun(bytes));
    var pattern: Pattern = .{ .clone = &clone };
    defer clone.deinit(pattern.allocator());
    const payload: admission.Payload = .{ .ptr = &pattern, .vtable = &.{ .checkpoint = Pattern.check } };
    const port = admission.Port.init(&payload);
    try std.testing.expectError(error.Canceled, clone.resumeWithAdmission(pattern.allocator(), port));
    try std.testing.expect(pattern.rejected);
    try clone.resumeWithAdmission(pattern.allocator(), port);
    var owned = try clone.take();
    var retirement = try Clone.retireOwned(owned);
    owned = undefined;
    try retirement.retireWithAdmission(pattern.allocator(), port);
    pattern.rejected = false;
    // A completed continuation still checks the current original.
    var fault: Fault = .{ .at = 0 };
    const fault_payload = fault.payload();
    try std.testing.expectError(error.Canceled, retirement.retireWithAdmission(pattern.allocator(), admission.Port.init(&fault_payload)));
}
