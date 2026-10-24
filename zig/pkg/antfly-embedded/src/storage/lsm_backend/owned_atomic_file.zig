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

//! Retained private-file completion on an already admitted durability worker.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const storage_io = @import("storage_io.zig");
var nonce: std.atomic.Value(u64) = .init(1);

/// Caller prepays memoryBound() and retains storage/executor/accounting owners
/// through resource retirement. No original port is retained. Accepted writes
/// freeze their bytes; retry must identify the same immutable command. Publish
/// input/cursor receipts before any subsequent fallible application operation.
pub const File = struct {
    pub const Phase = enum { initial, opening, ready, writing, publishing, published, failed, discarding, discarded };
    resources: ?storage_io.DurabilityResources,
    pending: ?storage_io.SyncOperation = null,
    phase: Phase = .initial,
    failure: ?anyerror = null,
    final: [std.fs.max_path_bytes]u8 = undefined,
    final_len: usize,
    temporary: [std.fs.max_path_bytes]u8 = undefined,
    temporary_len: usize,
    logical_len: u64 = 0,
    pending_offset: u64 = 0,
    pending_len: usize = 0,
    pending_source: ?[*]const u8 = null,
    /// Once true, callers must retain their possibly published-output ticket,
    /// even after failure/abandonment. Discard never removes the destination.
    publication_attempted: bool = false,

    pub fn memoryBound(storage: storage_io.Storage) !u64 {
        return std.math.add(u64, @sizeOf(File), try storage.appendResourceBytes(0)) catch error.ResourceBudgetExceeded;
    }

    /// Transfers the grant only on success. Invalid paths/capabilities leave it
    /// with the caller for admitted retirement, never implicit raw destruction.
    pub fn init(resources: storage_io.DurabilityResources, path: []const u8) !File {
        const v = resources.vtable;
        if (v.begin_private_create == null or v.begin_private_write == null or
            v.begin_private_publish == null or v.begin_private_discard == null or v.retire_with_admission == null)
            return error.ExecutionWaitUnsupported;
        if (path.len == 0 or path.len >= std.fs.max_path_bytes) return error.NameTooLong;
        var self: File = .{ .resources = resources, .final_len = path.len, .temporary_len = 0 };
        @memcpy(self.final[0..path.len], path);
        const tmp = std.fmt.bufPrint(&self.temporary, "{s}.owned-{d}.tmp", .{ path, nonce.fetchAdd(1, .monotonic) }) catch return error.NameTooLong;
        self.temporary_len = tmp.len;
        return self;
    }

    fn requireControl(original: admission.Port) !void {
        _ = (try original.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
        _ = (try original.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
        try original.checkpoint();
    }

    fn observe(self: *File, original: admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        self.pending.?.waitWithAdmission(trace.bind(original).?) catch |cause| {
            if (trace.failure) |rejected| return rejected;
            self.pending.?.deinit();
            self.pending = null;
            self.failure = cause;
            self.phase = .failed;
            return cause;
        };
        self.pending.?.deinit();
        self.pending = null;
        // Fixed physical receipt precedes any subsequent fallible operation.
        self.phase = switch (self.phase) {
            .opening => .ready,
            .writing => blk: {
                self.logical_len = @max(self.logical_len, self.pending_offset + self.pending_len);
                break :blk .ready;
            },
            .publishing => .published,
            .discarding => .discarded,
            else => unreachable,
        };
    }

    pub fn openWithAdmission(self: *File, original: admission.Port) !void {
        try requireControl(original);
        if (self.phase == .failed) return self.failure.?;
        if (self.phase == .initial) {
            self.pending = try self.resources.?.beginPrivateCreate(self.final[0..self.final_len], self.temporary[0..self.temporary_len], original);
            self.phase = .opening;
        }
        if (self.phase == .opening) return self.observe(original);
        if (self.phase != .ready) return error.InvalidArgument;
    }

    /// Append at the current end or backpatch entirely within existing bytes.
    /// Physical failure poisons the private image; only discard is then legal.
    pub fn writeWithAdmission(self: *File, offset: u64, bytes: []const u8, original: admission.Port) !void {
        try requireControl(original);
        if (self.phase == .failed) return self.failure.?;
        if (self.phase == .ready) {
            if (bytes.len > storage_io.max_atomic_write_bytes) return error.ResourceBudgetExceeded;
            const end = std.math.add(u64, offset, bytes.len) catch return error.InvalidAtomicWriteOffset;
            if (offset > self.logical_len or (offset != self.logical_len and end > self.logical_len)) return error.InvalidAtomicWriteOffset;
            self.pending = try self.resources.?.beginPrivateWrite(offset, bytes, original);
            self.pending_offset = offset;
            self.pending_len = bytes.len;
            self.pending_source = bytes.ptr;
            self.phase = .writing;
        } else if (self.phase != .writing or offset != self.pending_offset or bytes.len != self.pending_len or bytes.ptr != self.pending_source.?) return error.InvalidArgument;
        try self.observe(original);
    }

    pub fn finishWithAdmission(self: *File, original: admission.Port) !void {
        try requireControl(original);
        if (self.phase == .failed) return self.failure.?;
        if (self.phase == .published) return;
        if (self.phase == .ready) {
            self.pending = try self.resources.?.beginPrivatePublish(original);
            self.publication_attempted = true;
            self.phase = .publishing;
        }
        if (self.phase != .publishing) return error.InvalidArgument;
        try self.observe(original);
    }

    /// Joins accepted work under the original scope, then discards only the
    /// owned temporary. Physical cleanup errors may be explicitly retried;
    /// rejected waiters retain the exact accepted discard operation.
    pub fn abortWithAdmission(self: *File, original: admission.Port) !void {
        try requireControl(original);
        const was_discarding = self.phase == .discarding;
        if (self.pending != null) self.observe(original) catch |cause| {
            if (self.phase != .failed or was_discarding) return cause;
        };
        if (self.phase == .published or self.phase == .discarded) return;
        if (self.phase == .initial) {
            self.phase = .discarded;
            return;
        }
        self.pending = try self.resources.?.beginPrivateDiscard(original);
        self.phase = .discarding;
        try self.observe(original);
    }

    /// Transfer the still-funded grant between completed outputs in one
    /// completion bundle. The caller retains its reservation; this is not
    /// retirement and requires no fresh descriptor/worker admission.
    pub fn takeResources(self: *File) !storage_io.DurabilityResources {
        if (self.pending != null or (self.phase != .published and self.phase != .discarded)) return error.ResourceBudgetExceeded;
        const resources = self.resources orelse return error.InvalidArgument;
        self.resources = null;
        return resources;
    }

    pub fn retireWithAdmission(self: *File, original: admission.Port) !void {
        try requireControl(original);
        if (self.pending != null or (self.phase != .published and self.phase != .discarded)) return error.ResourceBudgetExceeded;
        if (self.resources) |*resources| {
            try resources.retireWithAdmission(original);
            self.resources = null;
        }
    }

    /// Explicit quiesced fallback only. Accepted physical operations finish
    /// before best-effort temporary cleanup on the worker and grant retirement.
    /// Higher output owners retain any unresolved cleanup/publication debt.
    pub fn deinit(self: *File) void {
        if (self.pending) |*pending| pending.deinit();
        if (self.resources) |*resources| resources.deinit();
        self.* = undefined;
    }
};

const TestControl = struct {
    file: *File,
    mode: enum { none, checkpoint, begin, end } = .none,
    target: File.Phase = .writing,
    failure: anyerror = error.Canceled,
    rejected: bool = false,
    fn reject(self: *@This(), mode: @FieldType(@This(), "mode")) !void {
        if (!self.rejected and self.mode == mode and self.file.phase == self.target) {
            self.rejected = true;
            return self.failure;
        }
    }
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.reject(.checkpoint);
    }
    fn lifetime(_: *anyopaque) !void {}
    fn deadline(_: *anyopaque) !?u64 {
        return std.math.maxInt(u64);
    }
    fn begin(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.reject(.begin);
        storage_io.NativeSyncTestControl.release.store(true, .release);
    }
    fn end(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.reject(.end);
    }
    fn payload(self: *@This()) admission.Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .check_lifetime = lifetime, .deadline_ns = deadline }, .execution_wait = .{ .ptr = self, .begin = begin, .end = end } };
    }
};

fn invoke(file: *File, phase: File.Phase, original: admission.Port) !void {
    switch (phase) {
        .opening => try file.openWithAdmission(original),
        .writing => try file.writeWithAdmission(0, "abcdef", original),
        .publishing => try file.finishWithAdmission(original),
        .discarding => try file.abortWithAdmission(original),
        else => unreachable,
    }
}

test "controlled physical sync private file owns rejected operations and retirement without IO replay" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .windows or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const manager_mod = @import("../resource_manager.zig");
    const a = std.testing.allocator;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.FileNotFound }) |failure| {
        for ([_]@FieldType(TestControl, "mode"){ .checkpoint, .begin, .end }) |mode| {
            for ([_]File.Phase{ .opening, .writing, .publishing, .discarding }) |phase| {
                var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 16);
                defer pool.deinit();
                var native = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
                defer native.deinit();
                var tmp = std.testing.tmpDir(.{});
                defer tmp.cleanup();
                const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/output", .{tmp.sub_path});
                defer a.free(path);
                var manager = manager_mod.ResourceManager.init(.{ .identity_allocator = a });
                defer manager.deinit(a);
                const bound = try File.memoryBound(native.storage());
                var credit = try manager.reserve(.lsm_table_builder_working_set, bound);
                defer credit.release();
                var file: File = undefined;
                var control: TestControl = .{ .file = &file, .failure = failure };
                const payload = control.payload();
                const original = admission.Port.init(&payload);
                const fds = pool.snapshotStats().fd_admitted_descriptors;
                file = try File.init((try native.storage().tryPrepareDurabilityWithAdmission(original)).?, path);
                defer file.deinit();
                if (phase != .opening) try file.openWithAdmission(original);
                if (phase == .publishing or phase == .discarding) try file.writeWithAdmission(0, "abcdef", original);
                control.mode = mode;
                control.target = phase;
                storage_io.NativeSyncTestControl.pause.store(true, .release);
                storage_io.NativeSyncTestControl.release.store(false, .release);
                defer storage_io.NativeSyncTestControl.pause.store(false, .release);
                defer storage_io.NativeSyncTestControl.release.store(true, .release);
                const calls = storage_io.NativeSyncTestControl.metadata.load(.acquire);
                try std.testing.expectError(failure, invoke(&file, phase, original));
                try std.testing.expect(control.rejected and file.phase == phase and file.pending != null);
                try std.testing.expectEqual(fds + 1, pool.snapshotStats().fd_admitted_descriptors);
                try std.testing.expectEqual(bound, manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
                const incomplete: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = TestControl.check, .check_lifetime = TestControl.lifetime } };
                try std.testing.expectError(error.ExecutionWaitUnsupported, invoke(&file, phase, .init(&incomplete)));
                try std.testing.expect(file.phase == phase and file.pending != null);
                control.mode = .none;
                storage_io.NativeSyncTestControl.release.store(true, .release);
                try invoke(&file, phase, original);
                try std.testing.expectEqual(calls + 1, storage_io.NativeSyncTestControl.metadata.load(.acquire));
                storage_io.NativeSyncTestControl.pause.store(false, .release);
                if (phase != .discarding) {
                    if (phase == .opening) try file.writeWithAdmission(0, "abcdef", original);
                    try file.finishWithAdmission(original);
                    const published = storage_io.NativeSyncTestControl.metadata.load(.acquire);
                    try file.finishWithAdmission(original);
                    try std.testing.expectEqual(published, storage_io.NativeSyncTestControl.metadata.load(.acquire));
                    const raw = try native.storage().readFileAlloc(a, path, 64);
                    defer a.free(raw);
                    try std.testing.expectEqualStrings("abcdef", raw);
                    try std.testing.expect(file.publication_attempted);
                } else try std.testing.expectError(error.FileNotFound, native.storage().fileSize(path));
                // Worker join physically completes before rejected CPU return;
                // descriptor and payload credit still belong to this owner.
                control.mode = .end;
                control.target = file.phase;
                control.rejected = false;
                try std.testing.expectError(failure, file.retireWithAdmission(original));
                try std.testing.expect(file.resources != null);
                try std.testing.expectEqual(fds + 1, pool.snapshotStats().fd_admitted_descriptors);
                try std.testing.expectEqual(bound, manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
                control.mode = .none;
                try file.retireWithAdmission(original);
                try std.testing.expect(file.resources == null);
                // Cached native reads may own another FD; the grant itself is
                // gone and all payload credit can now be physically returned.
                credit.release();
                try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_table_builder_working_set).used_bytes);
                try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
            }
        }
    }
}

test "controlled physical sync private file preserves collisions partial writes and ambiguous publication" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .windows or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const Mode = enum { collision, partial, renamed };
    const a = std.testing.allocator;
    for (std.enums.values(Mode)) |mode| {
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 16);
        defer pool.deinit();
        var native = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
        defer native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/output", .{tmp.sub_path});
        defer a.free(path);
        try native.storage().writeFileAbsolute(path, "previous");
        const warm = try native.storage().readFileRangeAlloc(a, path, 0, 8);
        a.free(warm);
        var file: File = undefined;
        var control: TestControl = .{ .file = &file };
        const payload = control.payload();
        const original = admission.Port.init(&payload);
        file = try File.init((try native.storage().tryPrepareDurabilityWithAdmission(original)).?, path);
        defer file.deinit();
        if (mode == .collision) {
            try native.storage().writeFileAbsolute(file.temporary[0..file.temporary_len], "foreign");
            try std.testing.expectError(error.PathAlreadyExists, file.openWithAdmission(original));
        } else {
            try file.openWithAdmission(original);
            if (mode == .partial) storage_io.NativeSyncTestControl.append_failure_after.store(3, .release);
            defer storage_io.NativeSyncTestControl.append_failure_after.store(std.math.maxInt(usize), .release);
            if (mode == .partial) {
                try std.testing.expectError(error.NoSpaceLeft, file.writeWithAdmission(0, "abcdef", original));
                const calls = storage_io.NativeSyncTestControl.metadata.load(.acquire);
                try std.testing.expectError(error.NoSpaceLeft, file.writeWithAdmission(0, "abcdef", original));
                try std.testing.expectError(error.NoSpaceLeft, file.finishWithAdmission(original));
                try std.testing.expectEqual(calls, storage_io.NativeSyncTestControl.metadata.load(.acquire));
                try std.testing.expect(!file.publication_attempted);
            } else {
                try file.writeWithAdmission(0, "abcdef", original);
                storage_io.NativeSyncTestControl.control_failure_after_rename.store(true, .release);
                defer storage_io.NativeSyncTestControl.control_failure_after_rename.store(false, .release);
                try std.testing.expectError(error.InputOutput, file.finishWithAdmission(original));
                try std.testing.expect(file.publication_attempted);
                const calls = storage_io.NativeSyncTestControl.metadata.load(.acquire);
                try std.testing.expectError(error.InputOutput, file.finishWithAdmission(original));
                try std.testing.expectEqual(calls, storage_io.NativeSyncTestControl.metadata.load(.acquire));
            }
        }
        try file.abortWithAdmission(original);
        try file.retireWithAdmission(original);
        const raw = try native.storage().readFileAlloc(a, path, 64);
        defer a.free(raw);
        try std.testing.expectEqualStrings(if (mode == .renamed) "abcdef" else "previous", raw);
        const fresh = try native.storage().readFileRangeAlloc(a, path, 0, raw.len);
        defer a.free(fresh);
        try std.testing.expectEqualSlices(u8, raw, fresh);
        if (mode == .collision) {
            const foreign = try native.storage().readFileAlloc(a, file.temporary[0..file.temporary_len], 64);
            defer a.free(foreign);
            try std.testing.expectEqualStrings("foreign", foreign);
        } else try std.testing.expectError(error.FileNotFound, native.storage().fileSize(file.temporary[0..file.temporary_len]));
    }
}

test "controlled physical sync table metadata publishes through owning file with sole FD headroom and control progress" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .windows or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const table = @import("../lsm/table_file.zig");
    const cpu = @import("../../common/runnable_scheduler.zig");
    const now = @import("antfly_platform").time.monotonicNs;
    const a = std.testing.allocator;
    var memory = table.MemoryTableSink.init(a);
    defer memory.deinit();
    var sink = memory.sink();
    var encoder = try table.StreamingEncoder.init(a, &sink, 192, .{});
    defer encoder.deinit();
    for (0..192) |i| {
        var key: [32]u8 = undefined;
        try encoder.appendEntry(.{ .namespace_name = "docs", .key = try std.fmt.bufPrint(&key, "doc:{d:0>8}", .{i}), .value = "retained value" });
    }
    var block = try encoder.prepareBlock();
    defer block.deinit();
    var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 16);
    defer pool.deinit();
    var native = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
    defer native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/table", .{tmp.sub_path});
    defer a.free(path);
    try native.storage().writeFileAbsolute(path, "old");
    const cached = try native.storage().readFileRangeAlloc(a, path, 0, 3);
    a.free(cached);
    var slots = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
    defer slots.close();
    var execution: cpu.Execution = .{};
    defer execution.finish();
    try execution.reserveCleanupRoot(&slots, .{ .io = std.testing.io, .native_now_ns = now, .allow_wait = true }, 10 * std.time.ns_per_s);
    try execution.activateCleanup();
    var query: cpu.Execution = .{};
    defer query.finish();
    try query.start(&slots, .{ .io = std.testing.io, .native_now_ns = now }, .query);
    const Control = struct {
        execution: *cpu.Execution,
        slots: *cpu.Scheduler,
        file: *File,
        physical_waits: usize = 0,
        reject_block_write: bool = false,
        block_rejected: bool = false,
        reject_metadata_write: bool = false,
        metadata_rejected: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.checkLifetime();
            try std.testing.expectEqual(cpu.State.running, self.slots.state(&self.execution.entry.?));
            if (self.reject_block_write and self.file.phase == .writing) {
                self.reject_block_write = false;
                self.block_rejected = true;
                return error.Canceled;
            }
            if (self.reject_metadata_write and self.file.phase == .writing) {
                self.reject_metadata_write = false;
                self.metadata_rejected = true;
                return error.Canceled;
            }
        }
        fn lifetime(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.checkLifetime();
        }
        fn deadline(raw: *anyopaque) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.execution.entry.?.options.deadline_ns;
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.park(.resource_blocked);
            var control: cpu.Execution = .{};
            defer control.finish();
            try control.start(self.slots, .{ .io = std.testing.io, .native_now_ns = @import("antfly_platform").time.monotonicNs }, .control);
            if (self.file.pending != null) self.physical_waits += 1;
            control.finish();
            storage_io.NativeSyncTestControl.release.store(true, .release);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            storage_io.NativeSyncTestControl.release.store(false, .release);
            try self.execution.enter(.write);
        }
    };
    var file: File = undefined;
    var control: Control = .{ .execution = &execution, .slots = &slots, .file = &file };
    const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.lifetime, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
    const original = admission.Port.init(&payload);
    var reserved: usize = 0;
    while (pool.tryReserveDescriptorsForTest(std.testing.io, 1)) reserved += 1;
    defer pool.releaseDescriptorsForTest(std.testing.io, reserved);
    try std.testing.expect(reserved > 0);
    pool.releaseDescriptorsForTest(std.testing.io, 1);
    reserved -= 1;
    file = try File.init((try native.storage().tryPrepareDurabilityWithAdmission(original)).?, path);
    defer file.deinit();
    storage_io.NativeSyncTestControl.pause.store(true, .release);
    storage_io.NativeSyncTestControl.release.store(false, .release);
    defer storage_io.NativeSyncTestControl.pause.store(false, .release);
    defer storage_io.NativeSyncTestControl.release.store(true, .release);
    try file.openWithAdmission(original);
    var offset: usize = 0;
    while (offset < memory.out.items.len) {
        const n = @min(memory.out.items.len - offset, storage_io.max_atomic_write_bytes);
        try file.writeWithAdmission(offset, memory.out.items[offset..][0..n], original);
        offset += n;
    }
    control.reject_block_write = true;
    const block_writes_before = storage_io.NativeSyncTestControl.metadata.load(.acquire);
    var block_written: usize = 0;
    while (true) {
        const more = block.writeNextWithAdmission(&file, original) catch |cause| {
            try std.testing.expectEqual(error.Canceled, cause);
            try std.testing.expect(control.block_rejected and block.pending_len != 0 and file.pending != null);
            try std.testing.expectEqual(@as(usize, 0), encoder.blocks.items.len);
            continue;
        };
        if (!more) break;
        block_written += 1;
    }
    try std.testing.expect(control.block_rejected and block.published);
    try std.testing.expectEqual(block_writes_before + block_written, storage_io.NativeSyncTestControl.metadata.load(.acquire));
    try block.retireWithAdmission(original);
    var metadata = try encoder.prepareMetadata();
    defer metadata.deinit();
    control.reject_metadata_write = true;
    const writes_before = storage_io.NativeSyncTestControl.metadata.load(.acquire);
    var written: usize = 0;
    while (true) {
        const more = metadata.writeNextWithAdmission(&file, original) catch |cause| {
            try std.testing.expectEqual(error.Canceled, cause);
            try std.testing.expect(control.metadata_rejected and metadata.pending and file.pending != null);
            continue;
        };
        if (!more) break;
        written += 1;
    }
    try std.testing.expect(control.metadata_rejected);
    try std.testing.expectEqual(writes_before + written, storage_io.NativeSyncTestControl.metadata.load(.acquire));
    var result = try metadata.takeResult();
    defer result.filter.deinit(a);
    try std.testing.expectEqual(@as(u64, result.size_bytes), file.logical_len);
    try file.finishWithAdmission(original);
    const held = pool.snapshotStats().fd_admitted_descriptors;
    const next_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/second", .{tmp.sub_path});
    defer a.free(next_path);
    const reusable = try file.takeResources();
    file.deinit();
    file = try File.init(reusable, next_path);
    try std.testing.expectEqual(held, pool.snapshotStats().fd_admitted_descriptors);
    try file.openWithAdmission(original);
    try file.writeWithAdmission(0, "same completion grant", original);
    try file.finishWithAdmission(original);
    try file.retireWithAdmission(original);
    const second = try native.storage().readFileAlloc(a, next_path, 64);
    defer a.free(second);
    try std.testing.expectEqualStrings("same completion grant", second);
    const raw = try native.storage().readFileAlloc(a, path, result.size_bytes + 1);
    defer a.free(raw);
    try std.testing.expectEqual(result.size_bytes, raw.len);
    var index = try table.decodeIndexAlloc(a, raw);
    defer index.deinit(a);
    try std.testing.expectEqual(@as(usize, 192), index.entryCount());
    try std.testing.expect(index.maybeContainsPrefix("docs", "doc:"));
    var decoded = try table.decodeAlloc(a, raw);
    defer decoded.deinit(a);
    try std.testing.expectEqual(@as(usize, 192), decoded.entries.len);
    for (decoded.entries, 0..) |entry, i| {
        var key: [32]u8 = undefined;
        try std.testing.expectEqualStrings(try std.fmt.bufPrint(&key, "doc:{d:0>8}", .{i}), entry.key);
        try std.testing.expectEqualStrings("docs", entry.namespace_name.?);
        try std.testing.expectEqualStrings("retained value", entry.value);
    }
    try std.testing.expect(control.physical_waits >= 5);
    try std.testing.expectEqual(cpu.State.running, slots.state(&query.entry.?));
    try std.testing.expect(file.phase == .published and file.publication_attempted and file.resources == null);
}

test "controlled physical sync private file explicit abandonment joins accepted IO before releasing its grant" {
    if (comptime @import("builtin").os.tag == .freestanding or @import("builtin").os.tag == .windows or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const a = std.testing.allocator;
    var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 16);
    defer pool.deinit();
    var native = try storage_io.NativeStorage.initWithPool(a, .threaded, &pool);
    defer native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/output", .{tmp.sub_path});
    defer a.free(path);
    var file: File = undefined;
    var live = false;
    defer if (live) file.deinit();
    var control: TestControl = .{ .file = &file };
    const payload = control.payload();
    const original = admission.Port.init(&payload);
    const fds = pool.snapshotStats().fd_admitted_descriptors;
    file = try File.init((try native.storage().tryPrepareDurabilityWithAdmission(original)).?, path);
    live = true;
    try file.openWithAdmission(original);
    const temporary = try a.dupe(u8, file.temporary[0..file.temporary_len]);
    defer a.free(temporary);
    storage_io.NativeSyncTestControl.pause.store(true, .release);
    storage_io.NativeSyncTestControl.started.store(false, .release);
    storage_io.NativeSyncTestControl.release.store(false, .release);
    defer storage_io.NativeSyncTestControl.pause.store(false, .release);
    defer storage_io.NativeSyncTestControl.release.store(true, .release);
    control.mode = .checkpoint;
    try std.testing.expectError(error.Canceled, file.writeWithAdmission(0, "abcdef", original));
    const Join = struct {
        file: *File,
        entered: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.entered.store(true, .release);
            self.file.deinit();
            self.done.store(true, .release);
        }
    };
    var join: Join = .{ .file = &file };
    const thread = try std.Thread.spawn(.{}, Join.run, .{&join});
    defer {
        storage_io.NativeSyncTestControl.release.store(true, .release);
        thread.join();
    }
    live = false;
    const until = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s;
    while (!join.entered.load(.acquire) or !storage_io.NativeSyncTestControl.started.load(.acquire)) {
        if (@import("antfly_platform").time.monotonicNs() >= until) return error.TestUnexpectedResult;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(!join.done.load(.acquire));
    try std.testing.expectEqual(fds + 1, pool.snapshotStats().fd_admitted_descriptors);
    storage_io.NativeSyncTestControl.release.store(true, .release);
    while (!join.done.load(.acquire)) {
        if (@import("antfly_platform").time.monotonicNs() >= until) return error.TestUnexpectedResult;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(fds, pool.snapshotStats().fd_admitted_descriptors);
    try std.testing.expectError(error.FileNotFound, native.storage().fileSize(temporary));
    try std.testing.expectError(error.FileNotFound, native.storage().fileSize(path));
}
