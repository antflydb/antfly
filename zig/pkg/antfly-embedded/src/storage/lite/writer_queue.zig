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

//! Fair admission in front of the existing safety flock. The fixed slot table
//! contains only sequence/kind hints; kernel OFD locks own slot liveness. A
//! dead or cancelled predecessor is reaped under the enrollment guard, and
//! every wake rechecks all earlier conflicting tickets before proceeding.
//! The safety flock must be closed BEFORE Ticket.close().
const std = @import("std");
const range = @import("antfly_platform").file_range_lock;
const Allocator = std.mem.Allocator;
pub const Kind = range.Kind;

pub const slot_count = 1024;
const header_bytes = 64;
pub const file_bytes = header_bytes + slot_count * 8;
const magic = "AFWQUEUE1";
const guard_offset = 0;
const slots_lock_offset = 16384;
const initial_backoff_ns = 50 * std.time.ns_per_us;
const maximum_backoff_ns = std.time.ns_per_ms;
const Slots = [slot_count]u64;

pub fn pathAlloc(a: Allocator, safety_lock_path: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(safety_lock_path, &digest, .{});
    const name = try std.fmt.allocPrint(a, ".aflite-writer-queue-v1-{s}.lock", .{std.fmt.bytesToHex(digest, .lower)});
    defer a.free(name);
    return std.fs.path.join(a, &.{ std.fs.path.dirname(safety_lock_path) orelse ".", name });
}

fn sequence(raw: u64) u64 {
    return raw >> 1;
}

fn exclusive(raw: u64) bool {
    return raw & 1 != 0;
}

/// The nearest conflicting predecessor is sufficient only as a wake hint.
/// Re-evaluation after every disappearance prevents a cancelled middle node
/// from letting its successor bypass an earlier live writer or reader.
fn predecessor(slots: *const Slots, own_slot: usize, own_raw: u64) !?usize {
    if (slots[own_slot] != own_raw or sequence(own_raw) == 0) return error.InvalidNativeWriterQueue;
    var selected: ?usize = null;
    for (slots, 0..) |raw, index| {
        if (index == own_slot or raw == 0) continue;
        if (sequence(raw) == 0 or sequence(raw) == sequence(own_raw)) return error.InvalidNativeWriterQueue;
        if (sequence(raw) >= sequence(own_raw) or (!exclusive(own_raw) and !exclusive(raw))) continue;
        if (selected == null or sequence(raw) > sequence(slots[selected.?])) selected = index;
    }
    return selected;
}

fn pause(io: std.Io, deadline: ?i96, backoff: *i96) !void {
    const limit = deadline orelse return error.WouldBlock;
    const remaining = limit - std.Io.Clock.awake.now(io).nanoseconds;
    if (remaining <= 0) return error.FileBusy;
    try io.sleep(.fromNanoseconds(@min(remaining, backoff.*)), .awake);
    backoff.* = @min(backoff.* * 2, maximum_backoff_ns);
}

fn requireBudget(io: std.Io, deadline: ?i96) !void {
    if (deadline) |limit| {
        if (std.Io.Clock.awake.now(io).nanoseconds >= limit) return error.FileBusy;
    } else return error.WouldBlock;
}

fn lockGuard(file: std.Io.File, io: std.Io, deadline: ?i96) !bool {
    var backoff: i96 = initial_backoff_ns;
    var waited = false;
    while (true) {
        try io.checkCancel();
        if (waited) try requireBudget(io, deadline);
        if (try range.tryLock(file, guard_offset, 1, .exclusive)) return waited;
        waited = true;
        try pause(io, deadline, &backoff);
    }
}

fn load(file: std.Io.File, io: std.Io) !Slots {
    var bytes: [file_bytes]u8 = undefined;
    const size = (try file.stat(io)).size;
    const length = try file.readPositionalAll(io, &bytes, 0);
    const valid = size == file_bytes and length == file_bytes and
        std.mem.eql(u8, bytes[0..magic.len], magic) and
        std.mem.readInt(u32, bytes[16..20], .little) == slot_count;
    if (!valid) {
        // Initialization can be interrupted before the header is complete.
        // Repair only when every possible live slot is excluded at once.
        // The versioned filename belongs exclusively to this layout.
        if (!try range.tryLock(file, slots_lock_offset, slot_count, .exclusive))
            return error.InvalidNativeWriterQueue;
        var locked = true;
        defer if (locked) range.unlock(file, slots_lock_offset, slot_count) catch {};
        @memset(&bytes, 0);
        @memcpy(bytes[0..magic.len], magic);
        std.mem.writeInt(u32, bytes[16..20], slot_count, .little);
        try file.setLength(io, file_bytes);
        try file.writePositionalAll(io, &bytes, 0);
        try range.unlock(file, slots_lock_offset, slot_count);
        locked = false;
    }
    var slots: Slots = undefined;
    for (&slots, 0..) |*raw, index| raw.* = std.mem.readInt(u64, bytes[header_bytes + index * 8 ..][0..8], .little);
    return slots;
}

fn save(file: std.Io.File, io: std.Io, slots: *const Slots) !void {
    var bytes: [slot_count * 8]u8 = undefined;
    for (slots, 0..) |raw, index| std.mem.writeInt(u64, bytes[index * 8 ..][0..8], raw, .little);
    try file.writePositionalAll(io, &bytes, header_bytes);
}

fn reap(file: std.Io.File, slots: *Slots, own_slot: ?usize) !bool {
    var changed = false;
    for (slots, 0..) |*raw, index| {
        if (raw.* == 0 or own_slot == index) continue;
        if (!try range.isLocked(file, slots_lock_offset + index, 1)) {
            raw.* = 0;
            changed = true;
        } else if (sequence(raw.*) == 0) return error.InvalidNativeWriterQueue;
    }
    return changed;
}

pub const Ticket = struct {
    file: std.Io.File,
    slot: usize,
    raw: u64,
    waited: bool = false,

    pub fn close(self: *Ticket, io: std.Io) void {
        // No lock or metadata write is needed on release. The next enrollment
        // or wake observes the vanished OFD lock and reaps this bounded slot.
        self.file.close(io);
        self.* = undefined;
    }

    pub fn waitTurn(self: *Ticket, io: std.Io, deadline: ?i96) !void {
        var backoff: i96 = initial_backoff_ns;
        var waited = self.waited;
        while (true) {
            if (waited) try requireBudget(io, deadline);
            waited = try lockGuard(self.file, io, deadline) or waited;
            var guarded = true;
            defer if (guarded) range.unlock(self.file, guard_offset, 1) catch {};
            var slots = try load(self.file, io);
            if (try reap(self.file, &slots, self.slot)) try save(self.file, io, &slots);
            const previous = try predecessor(&slots, self.slot, self.raw);
            try range.unlock(self.file, guard_offset, 1);
            guarded = false;
            if (previous == null) {
                if (waited) try requireBudget(io, deadline);
                try io.checkCancel();
                self.waited = waited;
                return;
            }
            const index = previous.?;
            const captured = slots[index];
            // Queries never acquire the predecessor's range. In particular,
            // several successors cannot accidentally keep a dead node alive.
            while (true) {
                try io.checkCancel();
                var bytes: [8]u8 = undefined;
                if (try self.file.readPositionalAll(io, &bytes, header_bytes + index * 8) != bytes.len)
                    return error.InvalidNativeWriterQueue;
                if (std.mem.readInt(u64, &bytes, .little) != captured or
                    !try range.isLocked(self.file, slots_lock_offset + index, 1)) break;
                waited = true;
                try pause(io, deadline, &backoff);
            }
            // A reused slot or an abandoned middle ticket is only a hint to
            // rescan. It never grants authority to take the safety flock.
        }
    }
};

/// Enrollment order is the FIFO linearization point. Null means this platform
/// has no OFD support, or a read-only caller cannot access an existing queue;
/// its existing safety lock remains authoritative. Writers create the queue.
pub fn enroll(a: Allocator, io: std.Io, safety_lock_path: []const u8, kind: Kind, deadline: ?i96, create: bool) !?Ticket {
    if (!range.supported) return null;
    const path = try pathAlloc(a, safety_lock_path);
    defer a.free(path);
    const file = if (create)
        try std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .truncate = false })
    else
        std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound, error.AccessDenied, error.ReadOnlyFileSystem => return null,
            else => return err,
        };
    var file_owned = true;
    errdefer if (file_owned) file.close(io);
    var backoff: i96 = initial_backoff_ns;
    var waited = false;
    while (true) {
        if (waited) try requireBudget(io, deadline);
        waited = try lockGuard(file, io, deadline) or waited;
        var guarded = true;
        defer if (guarded) range.unlock(file, guard_offset, 1) catch {};
        // A failed write can leave an invalid new slot. Retain the guard
        // through descriptor close, which drops both ownership ranges before
        // another participant can inspect that partially written ticket.
        errdefer if (guarded) {
            file.close(io);
            file_owned = false;
            guarded = false;
        };
        var slots = try load(file, io);
        _ = try reap(file, &slots, null);
        var free: ?usize = null;
        var maximum: u64 = 0;
        for (slots, 0..) |raw, index| {
            if (raw == 0 and free == null) free = index;
            maximum = @max(maximum, sequence(raw));
        }
        if (free) |index| {
            if (maximum == std.math.maxInt(u63)) return error.NativeWriterQueueSequenceExhausted;
            if (!try range.tryLock(file, slots_lock_offset + index, 1, .exclusive)) return error.InvalidNativeWriterQueue;
            const raw = ((maximum + 1) << 1) | @as(u64, @intFromBool(kind == .exclusive));
            slots[index] = raw;
            try save(file, io, &slots);
            try range.unlock(file, guard_offset, 1);
            guarded = false;
            if (waited) try requireBudget(io, deadline);
            return .{ .file = file, .slot = index, .raw = raw, .waited = waited };
        }
        try range.unlock(file, guard_offset, 1);
        guarded = false;
        waited = true;
        try pause(io, deadline, &backoff);
    }
}

fn testSafetyPath(a: Allocator, tmp: std.testing.TmpDir) ![:0]const u8 {
    const file = try tmp.dir.createFile(std.testing.io, "database.lock", .{});
    file.close(std.testing.io);
    return tmp.dir.realPathFileAlloc(std.testing.io, "database.lock", a);
}

test "writer queue live predecessors survive abandoned middle and slot reuse" {
    if (!range.supported) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testSafetyPath(a, tmp);
    defer a.free(path);
    var first = (try enroll(a, io, path, .exclusive, null, true)).?;
    var first_open = true;
    defer if (first_open) first.close(io);
    var middle = (try enroll(a, io, path, .exclusive, null, true)).?;
    const middle_slot = middle.slot;
    var middle_open = true;
    defer if (middle_open) middle.close(io);
    var last = (try enroll(a, io, path, .exclusive, null, true)).?;
    defer last.close(io);
    try first.waitTurn(io, null);
    middle.close(io);
    middle_open = false;
    try std.testing.expectError(error.WouldBlock, last.waitTurn(io, null));
    var reused = (try enroll(a, io, path, .exclusive, null, true)).?;
    defer reused.close(io);
    try std.testing.expectEqual(middle_slot, reused.slot);
    try std.testing.expect(sequence(reused.raw) > sequence(last.raw));
    first.close(io);
    first_open = false;
    try last.waitTurn(io, null);
    try std.testing.expectError(error.WouldBlock, reused.waitTurn(io, null));
    try std.testing.expectEqual(@as(u64, file_bytes), (try last.file.stat(io)).size);
}

test "writer queue read cohorts cannot bypass an earlier writer" {
    if (!range.supported) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testSafetyPath(a, tmp);
    defer a.free(path);
    var first = (try enroll(a, io, path, .shared, null, true)).?;
    var first_open = true;
    defer if (first_open) first.close(io);
    var second = (try enroll(a, io, path, .shared, null, true)).?;
    var second_open = true;
    defer if (second_open) second.close(io);
    var writer = (try enroll(a, io, path, .exclusive, null, true)).?;
    var writer_open = true;
    defer if (writer_open) writer.close(io);
    var late = (try enroll(a, io, path, .shared, null, true)).?;
    defer late.close(io);
    try first.waitTurn(io, null);
    try second.waitTurn(io, null);
    try std.testing.expectError(error.WouldBlock, writer.waitTurn(io, null));
    try std.testing.expectError(error.WouldBlock, late.waitTurn(io, null));
    second.close(io);
    second_open = false;
    try std.testing.expectError(error.WouldBlock, writer.waitTurn(io, null));
    first.close(io);
    first_open = false;
    try writer.waitTurn(io, null);
    try std.testing.expectError(error.WouldBlock, late.waitTurn(io, null));
    writer.close(io);
    writer_open = false;
    try late.waitTurn(io, null);
}

test "writer queue slot selection model checks all five actor liveness masks" {
    var slots: Slots = @splat(0);
    for (0..32) |kind_mask| {
        for (0..32) |live_mask| {
            for (slots[0..5], 0..) |*raw, i| raw.* = if (live_mask & (@as(usize, 1) << @intCast(i)) != 0)
                ((i + 1) << 1) | @intFromBool(kind_mask & (@as(usize, 1) << @intCast(i)) != 0)
            else
                0;
            for (slots[0..5], 0..) |raw, i| {
                if (raw == 0) continue;
                var expected: ?usize = null;
                for (slots[0..i], 0..) |earlier, j| if (earlier != 0 and (exclusive(raw) or exclusive(earlier))) {
                    expected = j;
                };
                try std.testing.expectEqual(expected, try predecessor(&slots, i, raw));
            }
        }
    }
}

test "writer queue cancellation drops its ownership without releasing an earlier ticket" {
    if (!range.supported) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testSafetyPath(a, tmp);
    defer a.free(path);
    var first = (try enroll(a, io, path, .exclusive, null, true)).?;
    var first_open = true;
    defer if (first_open) first.close(io);
    const Actor = struct {
        path: []const u8,
        enrolled: std.Io.Event = .unset,
        fn run(self: *@This()) !void {
            const deadline = std.Io.Clock.awake.now(std.testing.io).nanoseconds + 5 * std.time.ns_per_s;
            var ticket = (try enroll(std.testing.allocator, std.testing.io, self.path, .exclusive, deadline, true)).?;
            defer ticket.close(std.testing.io);
            self.enrolled.set(std.testing.io);
            try ticket.waitTurn(std.testing.io, deadline);
        }
    };
    var actor: Actor = .{ .path = path };
    var task = try std.Io.concurrent(io, Actor.run, .{&actor});
    defer _ = task.cancel(io) catch {};
    try actor.enrolled.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectError(error.Canceled, task.cancel(io));
    var last = (try enroll(a, io, path, .exclusive, null, true)).?;
    defer last.close(io);
    try std.testing.expectError(error.WouldBlock, last.waitTurn(io, null));
    first.close(io);
    first_open = false;
    try last.waitTurn(io, null);
}

test "writer queue deadline cannot acquire behind an unreleased holder" {
    if (!range.supported) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testSafetyPath(a, tmp);
    defer a.free(path);
    var first = (try enroll(a, io, path, .exclusive, null, true)).?;
    defer first.close(io);
    {
        const deadline = std.Io.Clock.awake.now(io).nanoseconds + 10 * std.time.ns_per_ms;
        var second = (try enroll(a, io, path, .exclusive, deadline, true)).?;
        defer second.close(io);
        try std.testing.expectError(error.FileBusy, second.waitTurn(io, deadline));
    }
    var next = (try enroll(a, io, path, .exclusive, null, true)).?;
    defer next.close(io);
    try std.testing.expectError(error.WouldBlock, next.waitTurn(io, null));
}

test "writer queue repairs interrupted initialization only without live owners" {
    if (!range.supported) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testSafetyPath(a, tmp);
    defer a.free(path);
    const queue_path = try pathAlloc(a, path);
    defer a.free(queue_path);
    const partial = try std.Io.Dir.cwd().createFile(io, queue_path, .{});
    try partial.writePositionalAll(io, "AFW", 0);
    partial.close(io);
    var first = (try enroll(a, io, path, .exclusive, null, true)).?;
    var first_open = true;
    defer if (first_open) first.close(io);
    try first.waitTurn(io, null);
    try first.file.writePositionalAll(io, "invalid!", 0);
    try std.testing.expectError(error.InvalidNativeWriterQueue, enroll(a, io, path, .exclusive, null, true));
    first.close(io);
    first_open = false;
    var repaired = (try enroll(a, io, path, .exclusive, null, true)).?;
    defer repaired.close(io);
    try repaired.waitTurn(io, null);
    try std.testing.expectEqual(@as(u64, file_bytes), (try repaired.file.stat(io)).size);
}

test "writer queue full table stays bounded and resets after all descriptions close" {
    if (!range.supported) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testSafetyPath(a, tmp);
    defer a.free(path);
    var holder = (try enroll(a, io, path, .exclusive, null, true)).?;
    var holder_open = true;
    defer if (holder_open) holder.close(io);
    // A single description can model every slot's live kernel ownership
    // without opening a thousand process fixtures or retaining their heaps.
    try std.testing.expect(try range.tryLock(holder.file, slots_lock_offset, slot_count, .exclusive));
    var slots: Slots = undefined;
    for (&slots, 0..) |*raw, index| raw.* = ((index + 1) << 1) | 1;
    try save(holder.file, io, &slots);
    try std.testing.expectError(error.WouldBlock, enroll(a, io, path, .exclusive, null, true));
    try std.testing.expectEqual(@as(u64, file_bytes), (try holder.file.stat(io)).size);
    holder.close(io);
    holder_open = false;
    var next = (try enroll(a, io, path, .exclusive, null, true)).?;
    defer next.close(io);
    try std.testing.expectEqual(@as(u64, 1), sequence(next.raw));
    try next.waitTurn(io, null);
}

test "writer queue failed partial enrollment retains its guard through descriptor close" {
    if (!range.supported) return error.SkipZigTest;
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const safety = try tmp.dir.createFile(io, "database.lock", .{});
    safety.close(io);
    const path = try tmp.dir.realPathFileAlloc(io, "database.lock", a);
    defer a.free(path);
    var first = (try enroll(a, io, path, .exclusive, null, true)).?;
    defer first.close(io);
    // The next ticket is 128, whose encoded writer value is 257. Losing the
    // write after its first byte leaves raw=1, an invalid live sequence.
    first.raw = 255;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, first.raw, .little);
    try first.file.writePositionalAll(io, &raw, header_bytes + first.slot * 8);
    const Fault = struct {
        var close_entered: std.Io.Event = .unset;
        var release_close: std.Io.Event = .unset;
        var injected = false;
        fn write(userdata: ?*anyopaque, file: std.Io.File, header: []const u8, data: []const []const u8, splat: usize, offset: u64) std.Io.File.WritePositionalError!usize {
            if (offset == header_bytes and !injected) {
                injected = true;
                try file.writePositionalAll(std.testing.io, data[0][0..9], offset);
                return error.NoSpaceLeft;
            }
            return std.testing.io.vtable.fileWritePositional(userdata, file, header, data, splat, offset);
        }
        fn close(userdata: ?*anyopaque, files: []const std.Io.File) void {
            close_entered.set(std.testing.io);
            release_close.waitUncancelable(std.testing.io);
            std.testing.io.vtable.fileClose(userdata, files);
        }
    };
    Fault.close_entered = .unset;
    Fault.release_close = .unset;
    Fault.injected = false;
    var vtable = io.vtable.*;
    vtable.fileWritePositional = Fault.write;
    vtable.fileClose = Fault.close;
    const fault_io = std.Io{ .userdata = io.userdata, .vtable = &vtable };
    const Worker = struct {
        failure: ?anyerror = null,
        fn run(self: *@This(), path_value: []const u8, input_io: std.Io) void {
            if (enroll(std.testing.allocator, input_io, path_value, .exclusive, null, true)) |maybe| {
                if (maybe) |ticket| {
                    var owned = ticket;
                    owned.close(input_io);
                }
            } else |err| self.failure = err;
        }
    };
    var worker: Worker = .{};
    const thread = try std.Thread.spawn(.{}, Worker.run, .{ &worker, path, fault_io });
    var joined = false;
    defer {
        Fault.release_close.set(io);
        if (!joined) thread.join();
    }
    try Fault.close_entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectError(error.WouldBlock, enroll(a, io, path, .exclusive, null, true));
    Fault.release_close.set(io);
    thread.join();
    joined = true;
    try std.testing.expectEqual(error.NoSpaceLeft, worker.failure.?);
    var successor = (try enroll(a, io, path, .exclusive, null, true)).?;
    defer successor.close(io);
    try std.testing.expectError(error.WouldBlock, successor.waitTurn(io, null));
}
