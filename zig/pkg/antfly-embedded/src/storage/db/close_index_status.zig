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

//! Owned, resumable final status preparation. The DB owns a frozen catalog and
//! joins every producer before capture; no admission borrow escapes a call.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const projection = @import("status_projection.zig");
const Snapshot = @import("../../index.zig").IndexSnapshot;
const state = @import("../lsm_backend/state.zig");
const wal = @import("../lsm_backend/wal.zig");
const resources = @import("../resource_manager.zig");

/// Only this fixed metadata remains after Program frees its payloads. Keeping
/// retained credit here releases the bounded construction slot immediately;
/// many databases can wait for physical close without pinning all 64 slots.
const Credit = struct {
    allocator: std.mem.Allocator,
    reservation: ?resources.Reservation = null,
    retirement: resources.PayloadRetirement = .{},

    fn create(manager: *resources.ResourceManager, slice: resources.Slice, bytes: u64, original: admission.Port) !*Credit {
        var construction = try manager.reserveConstructionWithAdmission(slice, bytes, original);
        defer construction.retire();
        // This allocator belongs to the manager, which outlives published debt;
        // no DB allocator or original execution borrow survives retirement.
        const allocator = manager.identity_allocator;
        const self = try allocator.create(Credit);
        errdefer allocator.destroy(self);
        self.* = .{ .allocator = allocator };
        try manager.registerPayloadRetirementWithAdmission(&self.retirement, &self.reservation, &construction, self, destroy, original);
        return self;
    }

    fn destroy(raw: *anyopaque) void {
        const self: *Credit = @ptrCast(@alignCast(raw));
        self.allocator.destroy(self);
    }
};

pub const Program = struct {
    const Row = state.OwnedEntry;
    const Chunk = struct { next: ?*Chunk = null, used: usize = 0, rows: [32]Row = undefined };
    busy: std.atomic.Value(bool) = .init(false),
    store_preflight: @import("../lsm_backend.zig").Backend.QuiescedClosePreflight = .{},
    complete: bool = false,
    group: usize = 0,
    index: usize = 0,
    head: ?*Chunk = null,
    tail: ?*Chunk = null,
    row_count: usize = 0,
    plan_group: usize = 0,
    plan_index: usize = 0,
    planned_count: usize = 0,
    planned_bytes: u64 = 0,
    record_size: wal.StateRecordSize = .{},
    planning_complete: bool = false,
    capture_reserved: bool = false,
    capture_manager: ?*resources.ResourceManager = null,
    capture_credit: ?*Credit = null,
    input: state.State = .{},
    input_allocator: ?std.mem.Allocator = null,
    input_started: bool = false,
    input_ready: bool = false,
    input_chunk: ?*Chunk = null,
    input_row: usize = 0,
    wal_record: ?[]u8 = null,
    wal_allocator: ?std.mem.Allocator = null,
    wal_credit: ?*Credit = null,
    pending_value: ?projection.IndexStatusSnapshot = null,
    pending_entry: ?Row = null,
    pending_key: ?[]u8 = null,
    copied: usize = 0,
    text_snapshot: ?*Snapshot = null,
    text_segment: usize = 0,
    text_count: u32 = 0,

    pub fn acquire(self: *Program, io: std.Io, original: admission.Port) !void {
        _ = (try original.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
        _ = (try original.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
        const Ready = struct {
            fn get(raw: *anyopaque) !bool {
                const program: *Program = @ptrCast(@alignCast(raw));
                return !program.busy.load(.acquire);
            }
        };
        while (true) {
            try original.checkpoint();
            if (self.busy.cmpxchgStrong(false, true, .acq_rel, .acquire) == null) return;
            try @import("../../common/cleanup_wait.zig").until(io, original, self, Ready.get, null);
        }
    }

    pub fn release(self: *Program) void {
        self.busy.store(false, .release);
    }

    /// The frozen catalog is visited once without allocating or copying names.
    /// Commit the whole row's plan together so overflow can be retried safely.
    pub fn planName(self: *Program, name: []const u8) !void {
        std.debug.assert(!self.planning_complete);
        const key_len = std.math.add(usize, projection.index_status_prefix.len, name.len) catch return error.ResourceBudgetExceeded;
        const payload = try state.sharedEntryAllocationBytes(null, key_len, projection.index_status_encoded_len);
        var bytes = std.math.add(u64, self.planned_bytes, payload) catch return error.ResourceBudgetExceeded;
        if (self.planned_count == 0) bytes = std.math.add(u64, bytes, @sizeOf(Credit)) catch return error.ResourceBudgetExceeded;
        bytes = std.math.add(u64, bytes, @sizeOf(Row)) catch return error.ResourceBudgetExceeded;
        if (self.planned_count % 32 == 0) bytes = std.math.add(u64, bytes, @sizeOf(Chunk)) catch return error.ResourceBudgetExceeded;
        var size = self.record_size;
        try size.add(0, key_len, projection.index_status_encoded_len);
        self.planned_count += 1;
        self.planned_bytes = bytes;
        self.record_size = size;
    }

    pub fn reserveCaptureWithAdmission(self: *Program, manager: ?*resources.ResourceManager, original: admission.Port) !void {
        try original.checkpoint();
        self.planning_complete = true;
        if (self.capture_reserved) {
            std.debug.assert(self.capture_manager == manager);
            return;
        }
        if (manager) |owner| if (self.planned_count != 0) {
            self.capture_credit = try Credit.create(owner, .lsm_table_builder_working_set, self.planned_bytes, original);
        };
        self.capture_manager = manager;
        self.capture_reserved = true;
    }

    pub fn ensureRow(self: *Program, allocator: std.mem.Allocator) !void {
        if (self.tail) |tail| if (tail.used != tail.rows.len) return;
        const chunk = try allocator.create(Chunk);
        chunk.* = .{};
        if (self.tail) |tail| tail.next = chunk else self.head = chunk;
        self.tail = chunk;
    }

    /// Capacity is fixed-size metadata. Keys copy in bounded CPU quanta and
    /// remain owned even if a later checkpoint rejects the original scope.
    pub fn appendPending(self: *Program, allocator: std.mem.Allocator, name: []const u8, original: admission.Port) !void {
        std.debug.assert(self.capture_reserved and self.row_count < self.planned_count);
        try original.checkpoint();
        try self.ensureRow(allocator);
        const prefix = projection.index_status_prefix;
        if (self.pending_key == null) {
            const size = std.math.add(usize, prefix.len, name.len) catch return error.ResourceBudgetExceeded;
            self.pending_entry = try state.allocateSharedEntry(allocator, null, size, projection.index_status_encoded_len, false);
            self.pending_key = self.pending_entry.?.key;
            @memcpy(self.pending_key.?[0..prefix.len], prefix);
            self.copied = prefix.len;
        }
        const key = self.pending_key.?;
        while (self.copied < key.len) {
            try original.checkpoint();
            const end = @min(key.len, self.copied + 16 * 1024);
            @memcpy(key[self.copied..end], name[self.copied - prefix.len .. end - prefix.len]);
            self.copied = end;
        }
        try original.checkpoint();
        const tail = self.tail.?;
        const row = &tail.rows[tail.used];
        projection.encodeIndexStatusSnapshot(self.pending_value.?, self.pending_entry.?.value[0..projection.index_status_encoded_len]);
        row.* = self.pending_entry.?;
        tail.used += 1;
        self.row_count += 1;
        self.pending_key = null;
        self.pending_entry = null;
        self.pending_value = null;
        self.copied = 0;
        self.releaseText();
        self.index += 1;
    }

    pub fn releaseText(self: *Program) void {
        if (self.text_snapshot) |snapshot| snapshot.release();
        self.text_snapshot = null;
        self.text_segment = 0;
        self.text_count = 0;
    }

    pub fn stage(self: *const Program, batch: anytype) !void {
        std.debug.assert(self.complete);
        var next = self.head;
        while (next) |chunk| : (next = chunk.next) {
            for (chunk.rows[0..chunk.used]) |*row| try batch.put(row.key, row.value);
        }
    }

    /// Prepare an append-only WAL/publication view, preserving capture order.
    /// It owns references to the packed rows and does not support sorted State
    /// point lookups. It and any consumer of those references must retire before
    /// this program releases its capture credit. No admission borrow escapes.
    pub fn prepareInputWithAdmission(self: *Program, allocator: std.mem.Allocator, original: admission.Port) !void {
        std.debug.assert(self.complete and self.capture_reserved and self.row_count == self.planned_count);
        try original.checkpoint();
        if (self.input_ready) return;
        if (!self.input_started) {
            try self.input.entries.ensureTotalCapacityPrecise(allocator, self.row_count);
            self.input_allocator = allocator;
            self.input_chunk = self.head;
            self.input_started = true;
        }
        std.debug.assert(self.input_allocator.?.ptr == allocator.ptr and self.input_allocator.?.vtable == allocator.vtable);
        while (self.input_chunk) |chunk| {
            try original.checkpoint();
            if (self.input_row == chunk.used) {
                self.input_chunk = chunk.next;
                self.input_row = 0;
                continue;
            }
            self.input.entries.appendAssumeCapacity(chunk.rows[self.input_row].retainShared());
            self.input_row += 1;
        }
        try original.checkpoint();
        self.input_ready = true;
    }

    /// Reserve encoded bytes before invoking the bounded canonical encoder.
    /// The later append owner separately prepays its frozen copy and physical
    /// resource metadata and must join before this program is destroyed.
    pub fn prepareWalRecordWithAdmission(self: *Program, allocator: std.mem.Allocator, original: admission.Port) !void {
        try self.prepareInputWithAdmission(allocator, original);
        try original.checkpoint();
        if (self.wal_record != null) return;
        if (self.capture_manager) |manager| if (self.wal_credit == null and self.row_count != 0) {
            self.wal_credit = try Credit.create(manager, .lsm_wal_write_working_set, try self.walReservationBytes(), original);
        };
        const record = try wal.prepareStateRecordWithAdmission(allocator, &self.input, original);
        std.debug.assert(record.len == self.record_size.total());
        self.wal_allocator = allocator;
        self.wal_record = record;
    }

    fn walReservationBytes(self: *const Program) !u64 {
        if (self.row_count == 0) return 0;
        return std.math.add(u64, self.record_size.total(), @sizeOf(Credit)) catch return error.ResourceBudgetExceeded;
    }

    /// Physical owner retirement is quiesced and occurs before index teardown.
    pub fn deinit(self: *Program, allocator: std.mem.Allocator) void {
        std.debug.assert(!self.busy.load(.acquire));
        if (self.wal_record) |bytes| self.wal_allocator.?.free(bytes);
        if (self.wal_credit) |credit| credit.retirement.publish();
        if (self.input_allocator) |allocator_| self.input.deinit(allocator_);
        self.releaseText();
        if (self.pending_entry) |*entry| entry.deinit(allocator);
        var next = self.head;
        while (next) |chunk| {
            next = chunk.next;
            for (chunk.rows[0..chunk.used]) |*row| row.deinit(allocator);
            allocator.destroy(chunk);
        }
        if (self.capture_credit) |credit| credit.retirement.publish();
        self.* = .{};
    }
};

const PreparationControl = struct {
    registry: ?*resources.ResourceManager = null,
    calls: usize = 0,
    fail_at: usize = std.math.maxInt(usize),
    failure: anyerror = error.Cancelled,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.registry) |manager| {
            if (!manager.payload_retirement_mutex.tryLock()) return error.TestRegistryCheckpointUnderGuard;
            manager.payload_retirement_mutex.unlock();
        }
        const call = self.calls;
        self.calls += 1;
        if (call == self.fail_at) return self.failure;
    }
    fn lifetime(_: *anyopaque) !void {}
    fn deadline(_: *anyopaque) !?u64 {
        return std.math.maxInt(u64);
    }
    fn begin(_: *anyopaque) !void {
        return error.TestUnexpectedWait;
    }
    fn end(_: *anyopaque) !void {}
};

fn planPreparationFixture(program: *Program, long_name: []const u8) !void {
    try program.planName(long_name);
    for (0..35) |_| try program.planName("row-00");
}

fn runPreparationFixture(program: *Program, allocator: std.mem.Allocator, manager: *resources.ResourceManager, long_name: []const u8, original: admission.Port) !void {
    try program.reserveCaptureWithAdmission(manager, original);
    while (program.index < program.planned_count) {
        if (program.pending_value == null) program.pending_value = .{ .kind = .graph, .doc_count = program.index + 1 };
        var key: [16]u8 = undefined;
        const name = if (program.index == 0) long_name else try std.fmt.bufPrint(&key, "row-{d:0>2}", .{program.index});
        try program.appendPending(allocator, name, original);
    }
    try original.checkpoint();
    program.complete = true;
    try program.prepareWalRecordWithAdmission(allocator, original);
}

test "controlled close status preparation preserves every original rejection and canonical owned input" {
    const a = std.testing.allocator;
    const long_name = try a.alloc(u8, 70 * 1024);
    defer a.free(long_name);
    @memset(long_name, 'n');
    var calls: usize = 0;
    var offset: usize = 0;
    while (offset <= calls) : (offset += 1) {
        for ([_]anyerror{ error.Cancelled, error.DeadlineExceeded, error.FileNotFound }) |failure| {
            var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
            defer manager.deinit(a);
            var program: Program = .{};
            defer program.deinit(a);
            try planPreparationFixture(&program, long_name);
            var control: PreparationControl = .{ .failure = failure };
            const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = PreparationControl.check, .check_lifetime = PreparationControl.lifetime, .deadline_ns = PreparationControl.deadline }, .execution_wait = .{ .ptr = &control, .begin = PreparationControl.begin, .end = PreparationControl.end } };
            const original = admission.Port.init(&payload);
            if (offset != 0) control.fail_at = offset - 1;
            runPreparationFixture(&program, a, &manager, long_name, original) catch |cause| {
                try std.testing.expectEqual(failure, cause);
                control.fail_at = std.math.maxInt(usize);
                try runPreparationFixture(&program, a, &manager, long_name, original);
            };
            if (offset == 0) calls = control.calls;
            try std.testing.expectEqual(program.planned_count, program.input.entryCount());
            try std.testing.expectEqual(program.planned_bytes, manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
            try std.testing.expectEqual(try program.walReservationBytes(), manager.sliceStats(.lsm_wal_write_working_set).used_bytes);
            try std.testing.expectEqual(@as(u32, @intCast(program.row_count)), try wal.validateStateRecordWithAdmission(program.wal_record.?, original));
            var chunk = program.head;
            var index: usize = 0;
            while (chunk) |rows| : (chunk = rows.next) {
                for (rows.rows[0..rows.used]) |row| {
                    const entry = program.input.entryAt(index);
                    try std.testing.expect(entry.shared == row.shared and entry.shared.?.account == null);
                    try std.testing.expectEqual(index + 1, (try projection.decodeIndexStatusSnapshot(entry.value)).doc_count);
                    index += 1;
                }
            }
            // Existing canonical append must produce the exact same bytes.
            if (offset == 0) {
                var memory = @import("../lsm_backend/storage_io.zig").MemoryStorage.init(a);
                defer memory.deinit();
                _ = try wal.appendState(memory.storage(), a, "/status", &program.input, false);
                const raw = try memory.storage().readFileRangeAlloc(a, "/status/wal/00000000000000000001.log", 0, program.wal_record.?.len);
                defer a.free(raw);
                try std.testing.expectEqualSlices(u8, raw, program.wal_record.?);
            }
            try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
        }
    }
    try std.testing.expect(calls > 100);
}

test "controlled close status preparation retries all allocation failures with retained capture credit" {
    const a = std.testing.allocator;
    const name = "long-first-name";
    var allocations: usize = 0;
    var offset: usize = 0;
    while (offset <= allocations) : (offset += 1) {
        var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
        defer manager.deinit(a);
        var failing = std.testing.FailingAllocator.init(a, .{});
        const allocator = failing.allocator();
        var program: Program = .{};
        var live = true;
        defer if (live) program.deinit(allocator);
        try planPreparationFixture(&program, name);
        var control: PreparationControl = .{};
        const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = PreparationControl.check, .check_lifetime = PreparationControl.lifetime, .deadline_ns = PreparationControl.deadline }, .execution_wait = .{ .ptr = &control, .begin = PreparationControl.begin, .end = PreparationControl.end } };
        const original = admission.Port.init(&payload);
        if (offset != 0) failing.fail_index = offset - 1;
        runPreparationFixture(&program, allocator, &manager, name, original) catch |cause| {
            try std.testing.expectEqual(error.OutOfMemory, cause);
            try std.testing.expect(program.capture_reserved and program.capture_credit != null);
            failing.fail_index = std.math.maxInt(usize);
            try runPreparationFixture(&program, allocator, &manager, name, original);
        };
        if (offset == 0) allocations = failing.alloc_index;
        try std.testing.expectEqual(program.planned_count, program.input.entryCount());
        program.deinit(allocator);
        live = false;
        _ = try manager.drainPayloadRetirementWithAdmission(original);
        try manager.drainConstructionCleanupWithAdmission(original);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_wal_write_working_set).used_bytes);
    }
    try std.testing.expect(allocations > 35);
}

test "controlled close status preparation denies capacity before capture and retains cleanup debt independently" {
    const a = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    var failing = std.testing.FailingAllocator.init(a, .{});
    const allocator = failing.allocator();
    var program: Program = .{};
    var live = true;
    defer if (live) program.deinit(allocator);
    const name = try a.dupe(u8, "producer-owned-name");
    var name_live = true;
    defer if (name_live) a.free(name);
    try program.planName(name);
    var control: PreparationControl = .{};
    const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = PreparationControl.check, .check_lifetime = PreparationControl.lifetime, .deadline_ns = PreparationControl.deadline }, .execution_wait = .{ .ptr = &control, .begin = PreparationControl.begin, .end = PreparationControl.end } };
    const original = admission.Port.init(&payload);
    manager.slices[@backingInt(resources.Slice.lsm_table_builder_working_set)].budget.hard_limit_bytes = program.planned_bytes - 1;
    try std.testing.expectError(error.ResourceBudgetExceeded, program.reserveCaptureWithAdmission(&manager, original));
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
    try std.testing.expect(program.head == null and program.pending_entry == null);
    manager.slices[@backingInt(resources.Slice.lsm_table_builder_working_set)].budget.hard_limit_bytes = 0;
    try program.reserveCaptureWithAdmission(&manager, original);
    program.pending_value = .{ .kind = .graph, .doc_count = 19 };
    try program.appendPending(allocator, name, original);
    a.free(name);
    name_live = false;
    program.complete = true;
    manager.slices[@backingInt(resources.Slice.lsm_wal_write_working_set)].budget.hard_limit_bytes = (try program.walReservationBytes()) - 1;
    try std.testing.expectError(error.ResourceBudgetExceeded, program.prepareWalRecordWithAdmission(allocator, original));
    try std.testing.expect(program.input_ready and program.wal_record == null);
    manager.slices[@backingInt(resources.Slice.lsm_wal_write_working_set)].budget.hard_limit_bytes = 0;
    try program.prepareWalRecordWithAdmission(allocator, original);
    try std.testing.expectEqual(@as(u64, 19), (try projection.decodeIndexStatusSnapshot(program.input.entryAt(0).value)).doc_count);
    const capture_bytes = program.planned_bytes;
    const wal_bytes = try program.walReservationBytes();
    std.debug.assert(manager.mutex.tryLock());
    program.deinit(allocator);
    live = false;
    manager.mutex.unlock();
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    try std.testing.expectEqual(capture_bytes, manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try std.testing.expectEqual(wal_bytes, manager.sliceStats(.lsm_wal_write_working_set).used_bytes);
    _ = try manager.drainPayloadRetirementWithAdmission(original);
    try manager.drainConstructionCleanupWithAdmission(original);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_wal_write_working_set).used_bytes);
}

test "controlled close status retained credits leave construction capacity available across many databases" {
    const a = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = a });
    defer manager.deinit(a);
    const programs = try a.alloc(Program, 80);
    defer a.free(programs);
    for (programs) |*program| program.* = .{};
    defer for (programs) |*program| program.deinit(a);
    var control: PreparationControl = .{ .registry = &manager };
    const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = PreparationControl.check, .check_lifetime = PreparationControl.lifetime, .deadline_ns = PreparationControl.deadline }, .execution_wait = .{ .ptr = &control, .begin = PreparationControl.begin, .end = PreparationControl.end } };
    const original = admission.Port.init(&payload);
    for (programs, 0..) |*program, index| {
        try program.planName("status");
        try program.reserveCaptureWithAdmission(&manager, original);
        program.pending_value = .{ .kind = .graph, .doc_count = index + 1 };
        try program.appendPending(a, "status", original);
        program.complete = true;
        try program.prepareWalRecordWithAdmission(a, original);
    }
    // Retaining 160 live credits must not consume the 64 construction slots
    // needed by another close's mutable successor or a decode/control owner.
    var extra = try manager.reserveConstructionWithAdmission(.lsm_read_working_set, 8, original);
    extra.retire();
    for (programs) |*program| program.deinit(a);
    while (manager.cleanupPending()) {
        _ = try manager.drainPayloadRetirementWithAdmission(original);
        try manager.drainConstructionCleanupWithAdmission(original);
    }
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_wal_write_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
}

test "controlled close status program retains partial key bytes and fixed row chunks across rejection" {
    const Control = struct {
        program: *Program,
        reject: bool = true,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.reject and self.program.copied > projection.index_status_prefix.len) return error.NotFound;
        }
        fn deadline(_: *anyopaque) !?u64 {
            return std.math.maxInt(u64);
        }
        fn begin(_: *anyopaque) !void {}
        fn end(_: *anyopaque) !void {}
    };
    const a = std.testing.allocator;
    var program: Program = .{};
    defer program.deinit(a);
    const name = try a.alloc(u8, 70 * 1024);
    defer a.free(name);
    @memset(name, 'n');
    var control: Control = .{ .program = &program };
    const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
    const original = admission.Port.init(&payload);
    try program.planName(name);
    for (0..70) |_| try program.planName("graph");
    try program.reserveCaptureWithAdmission(null, original);
    const Run = struct {
        fn append(owned: *Program, allocator: std.mem.Allocator, name_: []const u8, port: admission.Port) !void {
            try owned.acquire(std.testing.io, port);
            defer owned.release();
            try owned.appendPending(allocator, name_, port);
        }
    };
    program.pending_value = .{ .kind = .full_text, .doc_count = 17, .updated_at_ns = 29 };
    try std.testing.expectError(error.NotFound, Run.append(&program, a, name, original));
    try std.testing.expect(!program.busy.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), program.row_count);
    try std.testing.expectEqual(@as(usize, 0), program.index);
    try std.testing.expectEqual(projection.index_status_prefix.len + 16 * 1024, program.copied);
    const key = program.pending_key.?.ptr;
    control.reject = false;
    try Run.append(&program, a, name, original);
    try std.testing.expect(program.head.?.rows[0].key.ptr == key);
    try std.testing.expectEqualStrings(name, program.head.?.rows[0].key[projection.index_status_prefix.len..]);
    for (0..70) |_| {
        program.pending_value = .{ .kind = .graph, .doc_count = 3, .updated_at_ns = 31 };
        try Run.append(&program, a, "graph", original);
    }
    try std.testing.expectEqual(@as(usize, 71), program.row_count);
    var chunks: usize = 0;
    var cursor = program.head;
    while (cursor) |chunk| : (cursor = chunk.next) chunks += 1;
    try std.testing.expectEqual(@as(usize, 3), chunks);
    program.complete = true;
    const Batch = struct {
        rows: usize = 0,
        fn put(self: *@This(), _: []const u8, value: []const u8) !void {
            const decoded = try projection.decodeIndexStatusSnapshot(value);
            try std.testing.expectEqual(@as(u64, if (self.rows == 0) 17 else 3), decoded.doc_count);
            try std.testing.expectEqual(@as(u64, if (self.rows == 0) 29 else 31), decoded.updated_at_ns);
            self.rows += 1;
        }
    };
    var batch: Batch = .{};
    try program.stage(&batch);
    try std.testing.expectEqual(program.row_count, batch.rows);
}

test "controlled close status program preserves metadata through allocation rejection and competing finisher" {
    const Control = struct {
        fn check(_: *anyopaque) !void {}
        fn deadline(_: *anyopaque) !?u64 {
            return std.math.maxInt(u64);
        }
        fn begin(_: *anyopaque) !void {
            return error.FileNotFound;
        }
        fn end(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    const payload: admission.Payload = .{ .ptr = &marker, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &marker, .begin = Control.begin, .end = Control.end } };
    const original = admission.Port.init(&payload);
    for (0..2) |failure| {
        var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = failure });
        var program: Program = .{};
        var live = true;
        defer if (live) program.deinit(backing.allocator());
        try program.planName("text");
        try program.reserveCaptureWithAdmission(null, original);
        program.pending_value = .{ .kind = .full_text, .doc_count = 7 };
        try std.testing.expectError(error.OutOfMemory, program.appendPending(backing.allocator(), "text", original));
        try std.testing.expectEqual(@as(usize, 0), program.row_count);
        try std.testing.expectEqual(@as(u64, 7), program.pending_value.?.doc_count);
        backing.fail_index = std.math.maxInt(usize);
        try program.acquire(std.testing.io, original);
        try std.testing.expectError(error.FileNotFound, program.acquire(std.testing.io, original));
        try std.testing.expect(program.busy.load(.acquire));
        try program.appendPending(backing.allocator(), "text", original);
        program.release();
        try std.testing.expectEqual(@as(usize, 1), program.row_count);
        program.deinit(backing.allocator());
        live = false;
        try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
    }
}
