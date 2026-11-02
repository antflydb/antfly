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

const std = @import("std");
const client = @import("storage/kernel_wal_client.zig");
pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
extern fn wal_prepared_fixture_retired() callconv(.c) u32;

test "WAL prepared client preserves ignored original refusal and retires granted ownership" {
    const read = @import("antfly_local_sources").common_read_admission;
    const State = struct {
        calls: usize = 0,
        reject: usize = 2,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls >= self.reject) return error.DeadlineExceeded;
        }
    };
    var wal: client.WAL = .{ .clock = @import("antfly_local_sources").storage_sim_runtime.real_clock };
    for (0..4) |mode| {
        wal_borrow_fixture_mode(@intCast(mode));
        var state: State = .{};
        const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
        const retired = wal_prepared_fixture_retired();
        try std.testing.expectError(error.DeadlineExceeded, wal.prepareAppend(4, "tail", read.Port.init(&payload)));
        try std.testing.expectEqual(retired + @as(u32, if (mode == 0 or mode == 3) 1 else 0), wal_prepared_fixture_retired());
    }
    wal_borrow_fixture_mode(0);
    var state: State = .{ .reject = std.math.maxInt(usize) };
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
    const retired = wal_prepared_fixture_retired();
    var prepared = try wal.prepareAppend(4, "tail", read.Port.init(&payload));
    const checks = state.calls;
    state.reject = 0;
    try std.testing.expectEqual(@as(u64, 7), try prepared.publish("head"));
    try std.testing.expectEqual(checks, state.calls);
    prepared.deinit();
    try std.testing.expectEqual(retired + 1, wal_prepared_fixture_retired());
}
extern fn wal_borrow_fixture_mode(u32) callconv(.c) void;
extern fn wal_borrow_fixture_reads() callconv(.c) u32;
extern fn wal_borrow_fixture_retired() callconv(.c) u32;
test "WAL borrowed client boundary retires owner buffers across every exit" {
    var wal: client.WAL = .{ .clock = @import("antfly_local_sources").storage_sim_runtime.real_clock };
    const start_reads = wal_borrow_fixture_reads();
    const start_retired = wal_borrow_fixture_retired();
    const Visit = struct {
        start_reads: u32,
        start_retired: u32,
        calls: usize = 0,
        reject: bool = false,
        fn run(self: *@This(), entry: client.WalEntry) !void {
            try std.testing.expectEqual(@as(u64, 7), entry.lsn);
            try std.testing.expectEqualStrings("wire", entry.data);
            try std.testing.expectEqual(wal_borrow_fixture_reads() - self.start_reads - 1, wal_borrow_fixture_retired() - self.start_retired);
            self.calls += 1;
            if (self.reject) return error.InjectedWalVisitRejection;
        }
    };
    var visit: Visit = .{ .start_reads = start_reads, .start_retired = start_retired };
    wal_borrow_fixture_mode(0);
    try std.testing.expect(try wal.withEntryAt(7, &visit, Visit.run));
    visit.reject = true;
    try std.testing.expectError(error.InjectedWalVisitRejection, wal.withEntryAt(7, &visit, Visit.run));
    for ([_]u32{ 1, 2, 3, 4 }) |mode| {
        wal_borrow_fixture_mode(mode);
        switch (mode) {
            1 => try std.testing.expect(!try wal.withEntryAt(7, &visit, Visit.run)),
            2, 3 => try std.testing.expectError(error.StorageKernelFailure, wal.withEntryAt(7, &visit, Visit.run)),
            4 => try std.testing.expectError(error.OutOfMemory, wal.withEntryAt(7, &visit, Visit.run)),
            else => unreachable,
        }
        try std.testing.expectEqual(wal_borrow_fixture_reads() - start_reads, wal_borrow_fixture_retired() - start_retired);
    }
    try std.testing.expectEqual(@as(usize, 2), visit.calls);
}

test "WAL borrowed client boundary preserves controlled errors and retires populated failure replies" {
    const read = @import("antfly_local_sources").common_read_admission;
    const State = struct {
        checks: usize = 0,
        reject_at: usize = 0,
        cause: anyerror = error.Canceled,
        visits: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return self.cause;
        }
        fn visit(self: *@This(), entry: client.WalEntry) !void {
            try std.testing.expectEqualStrings("wire", entry.data);
            self.visits += 1;
        }
    };
    var wal: client.WAL = .{ .clock = @import("antfly_local_sources").storage_sim_runtime.real_clock };
    var state: State = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
    const control = read.Port.init(&payload);
    wal_borrow_fixture_mode(0);
    const start_reads = wal_borrow_fixture_reads();
    const start_retired = wal_borrow_fixture_retired();
    try std.testing.expect(try wal.withEntryAtWithAdmission(7, control, &state, State.visit));
    try std.testing.expectEqual(@as(usize, 1), state.visits);
    const checks = state.checks;
    try std.testing.expect(checks >= 5);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |reject_at| {
            state = .{ .reject_at = reject_at, .cause = cause };
            try std.testing.expectError(cause, wal.withEntryAtWithAdmission(7, control, &state, State.visit));
            try std.testing.expectEqual(reject_at, state.checks);
        }
    }
    // Every attempt retires exactly once, including rejection before dispatch,
    // a populated error reply, after receive, and after the user callback.
    try std.testing.expectEqual(@as(u32, @intCast(1 + 4 * checks)), wal_borrow_fixture_retired() - start_retired);
    try std.testing.expect(wal_borrow_fixture_reads() > start_reads);
}

extern fn wal_position_fixture_reset(u32) callconv(.c) void;
extern fn wal_position_fixture_committed() callconv(.c) u8;
test "WAL borrowed client boundary recovery position rejects malformed replies and preserves original authority" {
    const read = @import("antfly_local_sources").common_read_admission;
    const State = struct {
        checks: usize = 0,
        reject_at: usize = 0,
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return self.cause;
        }
    };
    var state: State = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
    const control = read.Port.init(&payload);
    var wal: client.WAL = .{ .clock = @import("antfly_local_sources").storage_sim_runtime.real_clock };
    wal_position_fixture_reset(0);
    const position = (try wal.tryRecoveryPosition(control)).?;
    try std.testing.expectEqual(@as(u64, 7), position.next_lsn);
    const checks = state.checks;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |reject_at| {
            wal_position_fixture_reset(0);
            state = .{ .cause = cause, .reject_at = reject_at };
            try std.testing.expectError(cause, wal.tryRecoveryPosition(control));
        }
    }
    state = .{};
    wal_position_fixture_reset(1);
    try std.testing.expectEqual(@as(?client.RecoveryPosition, null), try wal.tryRecoveryPosition(control));
    for ([_]u32{ 3, 4, 5 }) |bad| {
        wal_position_fixture_reset(bad);
        try std.testing.expectError(error.StorageKernelFailure, wal.tryRecoveryPosition(control));
    }
    wal_position_fixture_reset(7);
    try std.testing.expectError(error.OutOfMemory, wal.tryRecoveryPosition(control));
}

test "WAL borrowed client boundary conditional append preserves commit and validates pending replies" {
    const read = @import("antfly_local_sources").common_read_admission;
    const State = struct {
        checks: usize = 0,
        reject_at: usize = 0,
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (wal_position_fixture_committed() != 0) return error.UnsafePostCommitCancellation;
            if (self.checks == self.reject_at) return self.cause;
        }
    };
    var state: State = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
    const control = read.Port.init(&payload);
    var wal: client.WAL = .{ .clock = @import("antfly_local_sources").storage_sim_runtime.real_clock };
    wal_position_fixture_reset(0);
    const position = (try wal.tryRecoveryPosition(control)).?;
    state = .{};
    try std.testing.expectEqual(@as(u64, 7), (try wal.appendIfRecoveryPosition(position, "conditional", control)).appended);
    try std.testing.expectEqual(@as(u8, 1), wal_position_fixture_committed());
    const checks = state.checks;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |reject_at| {
            wal_position_fixture_reset(0);
            state = .{ .cause = cause, .reject_at = reject_at };
            try std.testing.expectError(cause, wal.appendIfRecoveryPosition(position, "conditional", control));
            try std.testing.expectEqual(@as(u8, 0), wal_position_fixture_committed());
        }
    }
    state = .{};
    wal_position_fixture_reset(1);
    try std.testing.expectEqual(@as(client.RecoveryAppendResult, .busy), try wal.appendIfRecoveryPosition(position, "conditional", control));
    wal_position_fixture_reset(2);
    try std.testing.expectEqual(@as(client.RecoveryAppendResult, .changed), try wal.appendIfRecoveryPosition(position, "conditional", control));
    for ([_]u32{ 3, 4, 5, 6 }) |bad| {
        wal_position_fixture_reset(bad);
        try std.testing.expectError(error.StorageKernelFailure, wal.appendIfRecoveryPosition(position, "conditional", control));
    }
    wal_position_fixture_reset(7);
    try std.testing.expectError(error.OutOfMemory, wal.appendIfRecoveryPosition(position, "conditional", control));
}

test "WAL borrowed client boundary rooted read replies retire on rejection malformed and populated failure exits" {
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var wal = try client.WAL.openWithAllocator(tracking.allocator(), "/fixture", client.WalOptions{});
    defer wal.close();
    const baseline = tracking.allocated_bytes - tracking.freed_bytes;
    const retired = wal_borrow_fixture_retired();
    const Visit = struct {
        calls: usize = 0,
        reject: bool = false,
        fn run(self: *@This(), entry: client.WalEntry) !void {
            try std.testing.expectEqualStrings("wire", entry.data);
            self.calls += 1;
            if (self.reject) return error.RejectedRootedVisit;
        }
    };
    var visit: Visit = .{};
    for (0..7) |mode| {
        wal_borrow_fixture_mode(@intCast(mode));
        switch (mode) {
            0 => try std.testing.expect(try wal.withEntryAt(7, &visit, Visit.run)),
            1 => try std.testing.expect(!try wal.withEntryAt(7, &visit, Visit.run)),
            2, 3, 6 => try std.testing.expectError(error.StorageKernelFailure, wal.withEntryAt(7, &visit, Visit.run)),
            4, 5 => try std.testing.expectError(error.OutOfMemory, wal.withEntryAt(7, &visit, Visit.run)),
            else => unreachable,
        }
        try std.testing.expectEqual(baseline, tracking.allocated_bytes - tracking.freed_bytes);
    }
    wal_borrow_fixture_mode(0);
    visit.reject = true;
    try std.testing.expectError(error.RejectedRootedVisit, wal.withEntryAt(7, &visit, Visit.run));
    try std.testing.expectEqual(baseline, tracking.allocated_bytes - tracking.freed_bytes);
    try std.testing.expectEqual(retired, wal_borrow_fixture_retired());
    const read = @import("antfly_local_sources").common_read_admission;
    const Control = struct {
        checks: usize = 0,
        reject_at: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return error.CatalogGenerationChanged;
        }
    };
    var state: Control = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check } };
    visit.reject = false;
    try std.testing.expect(try wal.withEntryAtWithAdmission(7, read.Port.init(&payload), &visit, Visit.run));
    const checks = state.checks;
    for (1..checks + 1) |index| {
        state = .{ .reject_at = index };
        try std.testing.expectError(error.CatalogGenerationChanged, wal.withEntryAtWithAdmission(7, read.Port.init(&payload), &visit, Visit.run));
        try std.testing.expectEqual(baseline, tracking.allocated_bytes - tracking.freed_bytes);
    }
}

test "WAL borrowed client boundary rooted scan and metadata unwind all allocation failures" {
    const Visit = struct {
        fn run(_: *u8, entry: client.WalEntry) !client.WalScanAction {
            try std.testing.expectEqualStrings("wire", entry.data);
            return .stop;
        }
    };
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    {
        var wal = try client.WAL.openWithAllocator(tracking.allocator(), "/fixture", client.WalOptions{});
        defer wal.close();
        const baseline = tracking.allocated_bytes - tracking.freed_bytes;
        var unused: u8 = 0;
        for ([_]u32{ 0, 2, 3, 4, 5 }) |mode| {
            wal_borrow_fixture_mode(mode);
            switch (mode) {
                0 => try wal.iterateFromStreamingWithContext(7, &unused, Visit.run),
                2, 3 => try std.testing.expectError(error.StorageKernelFailure, wal.iterateFromStreamingWithContext(7, &unused, Visit.run)),
                4, 5 => try std.testing.expectError(error.OutOfMemory, wal.iterateFromStreamingWithContext(7, &unused, Visit.run)),
                else => unreachable,
            }
            try std.testing.expectEqual(baseline, tracking.allocated_bytes - tracking.freed_bytes);
        }
    }
    try std.testing.expectEqual(tracking.allocated_bytes, tracking.freed_bytes);
    const Fixture = struct {
        fn run(allocator: std.mem.Allocator) !void {
            wal_borrow_fixture_mode(0);
            var wal = try client.WAL.openWithAllocator(allocator, "/fixture", client.WalOptions{});
            defer wal.close();
            const entry = (try wal.readAt(allocator, 7)).?;
            defer allocator.free(@constCast(entry.data));
            var unused: u8 = 0;
            try wal.iterateFromStreamingWithContext(7, &unused, Visit.run);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "WAL borrowed client boundary rooted reply remains owned when visitor closes WAL" {
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    wal_borrow_fixture_mode(0);
    var wal = try client.WAL.openWithAllocator(tracking.allocator(), "/fixture", client.WalOptions{});
    const Visit = struct {
        fn run(owner: *client.WAL, entry: client.WalEntry) !void {
            owner.close();
            try std.testing.expectEqualStrings("wire", entry.data);
        }
    };
    try std.testing.expect(try wal.withEntryAt(7, &wal, Visit.run));
    try std.testing.expectEqual(tracking.allocated_bytes, tracking.freed_bytes);
}

extern fn wal_owned_fixture_mode(u32) callconv(.c) void;
extern fn wal_owned_fixture_retired() callconv(.c) u32;
test "WAL owned client compiled boundary rejects malformed operation replies and retires independent roots" {
    const read = @import("antfly_local_sources").common_read_admission;
    const Authority = struct {
        fn check(_: *anyopaque) !void {}
    };
    var token: u8 = 0;
    const payload: read.Payload = .{ .ptr = &token, .vtable = &.{ .checkpoint = Authority.check, .check_lifetime = Authority.check } };
    const control = read.Port.init(&payload);
    var wal: client.WAL = .{ .clock = undefined };
    var root = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    for ([_]u32{ 1, 2 }) |mode| {
        wal_owned_fixture_mode(mode);
        try std.testing.expectError(if (mode == 1) error.StorageKernelFailure else error.OutOfMemory, wal.beginOwnedRead(root.allocator(), 7, control));
        try std.testing.expectEqual(root.allocated_bytes, root.freed_bytes);
    }
    for ([_]u32{ 0, 4, 5, 6, 7, 8, 9 }) |mode| {
        wal_owned_fixture_mode(0);
        const operation = try wal.beginOwnedRead(root.allocator(), 7, control);
        const before = wal_owned_fixture_retired();
        wal_owned_fixture_mode(mode);
        if (mode == 4 or mode == 5) {
            try std.testing.expectError(error.StorageKernelFailure, operation.step(17, control));
        } else {
            try std.testing.expectEqual(client.WAL.OwnedRead.Status.ready, try operation.step(17, control));
            var output: [4]u8 = undefined;
            if (mode == 0) {
                const chunk = try operation.readChunk(0, &output, control);
                try std.testing.expect(chunk.done and chunk.len == 4);
                try std.testing.expectEqualStrings("wire", &output);
            } else try std.testing.expectError(error.StorageKernelFailure, operation.readChunk(0, &output, control));
        }
        operation.destroy();
        try std.testing.expectEqual(before + 1, wal_owned_fixture_retired());
        try std.testing.expectEqual(root.allocated_bytes, root.freed_bytes);
    }
}

test "WAL owned client compiled boundary restores ignored original rejection and retires returned handles" {
    const read = @import("antfly_local_sources").common_read_admission;
    const State = struct {
        checks: usize = 0,
        reject_at: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return error.OwnedOriginalAuthority;
        }
    };
    var state: State = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
    const control = read.Port.init(&payload);
    var wal: client.WAL = .{ .clock = undefined };
    var root = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    wal_owned_fixture_mode(0);
    const baseline = try wal.beginOwnedRead(root.allocator(), 7, control);
    const begin_checks = state.checks;
    state = .{};
    _ = try baseline.step(1, control);
    const step_checks = state.checks;
    var output: [4]u8 = undefined;
    state = .{};
    _ = try baseline.readChunk(0, &output, control);
    const chunk_checks = state.checks;
    baseline.destroy();
    for (1..begin_checks + 1) |reject_at| {
        state = .{ .reject_at = reject_at };
        try std.testing.expectError(error.OwnedOriginalAuthority, wal.beginOwnedRead(root.allocator(), 7, control));
        try std.testing.expectEqual(root.allocated_bytes, root.freed_bytes);
    }
    for ([_]bool{ false, true }) |chunk_call| {
        for (1..(if (chunk_call) chunk_checks else step_checks) + 1) |reject_at| {
            state = .{};
            const operation = try wal.beginOwnedRead(root.allocator(), 7, control);
            const before = wal_owned_fixture_retired();
            state = .{ .reject_at = reject_at };
            if (chunk_call) {
                try std.testing.expectError(error.OwnedOriginalAuthority, operation.readChunk(0, &output, control));
            } else try std.testing.expectError(error.OwnedOriginalAuthority, operation.step(1, control));
            state = .{};
            try std.testing.expectError(error.OwnedOriginalAuthority, operation.step(1, control));
            operation.destroy();
            try std.testing.expectEqual(before + 1, wal_owned_fixture_retired());
            try std.testing.expectEqual(root.allocated_bytes, root.freed_bytes);
        }
    }
}
