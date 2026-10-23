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

pub const Program = struct {
    const Row = struct { key: []u8, value: [projection.index_status_encoded_len]u8 };
    const Chunk = struct { next: ?*Chunk = null, used: usize = 0, rows: [32]Row = undefined };
    busy: std.atomic.Value(bool) = .init(false),
    complete: bool = false,
    group: usize = 0,
    index: usize = 0,
    head: ?*Chunk = null,
    tail: ?*Chunk = null,
    row_count: usize = 0,
    pending_value: ?projection.IndexStatusSnapshot = null,
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
        try original.checkpoint();
        try self.ensureRow(allocator);
        const prefix = projection.index_status_prefix;
        if (self.pending_key == null) {
            const size = std.math.add(usize, prefix.len, name.len) catch return error.ResourceBudgetExceeded;
            self.pending_key = try allocator.alloc(u8, size);
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
        row.key = key;
        projection.encodeIndexStatusSnapshot(self.pending_value.?, &row.value);
        tail.used += 1;
        self.row_count += 1;
        self.pending_key = null;
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
            for (chunk.rows[0..chunk.used]) |*row| try batch.put(row.key, &row.value);
        }
    }

    /// Physical owner retirement is quiesced and occurs before index teardown.
    pub fn deinit(self: *Program, allocator: std.mem.Allocator) void {
        std.debug.assert(!self.busy.load(.acquire));
        self.releaseText();
        if (self.pending_key) |key| allocator.free(key);
        var next = self.head;
        while (next) |chunk| {
            next = chunk.next;
            for (chunk.rows[0..chunk.used]) |row| allocator.free(row.key);
            allocator.destroy(chunk);
        }
        self.* = .{};
    }
};

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
