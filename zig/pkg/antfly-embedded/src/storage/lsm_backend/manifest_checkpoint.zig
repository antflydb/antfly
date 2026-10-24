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

//! Retained allocation-free checkpoint serialization over pinned metadata.
const std = @import("std");
const manifest = @import("../lsm/manifest.zig");
const Crc32 = @import("antfly_hash").Crc32;
const admission = @import("../../common/read_admission.zig");
pub const quantum = 16 * 1024;

/// Source is a stable borrowed cursor over pinned immutable metadata. Each
/// cursor callback must itself be bounded and allocation/IO-free. Keep source
/// and its labels alive through retirement; no application port is retained.
/// CPU progress may move, except while an accepted writer borrows its command.
pub fn Encoder(comptime Source: type) type {
    return struct {
        pub const Command = struct { offset: usize, append: bool, bytes: []const u8 };
        const Phase = enum { placeholder, removals, inner_header, run, run_fields, obsolete, obsolete_fields, inner_crc, outer_crc, flush, patch, done };
        source: Source,
        sequence: u64,
        next_run_id: u64,
        run_count: usize,
        obsolete_count: usize,
        limit: usize,
        phase: Phase = .placeholder,
        run: ?manifest.RunMeta = null,
        obsolete: ?manifest.ObsoletePathMeta = null,
        runs: usize = 0,
        paths: usize = 0,
        field: usize = 0,
        offset: usize = 0,
        total: usize = 0,
        inner: Crc32 = .init(),
        outer: Crc32 = .init(),
        scalar: [112]u8 = undefined,
        buffer: [quantum]u8 = undefined,
        used: usize = 0,
        pending: bool = false,
        command_offset: usize = 0,
        command_append: bool = true,
        failure: ?anyerror = null,
        retiring: bool = false,
        retire_slot: usize = 0,
        retired: bool = false,

        const Self = @This();
        pub fn init(source: Source, sequence: u64, next_run_id: u64, limit: usize) !Self {
            const runs = source.runCount();
            const paths = source.obsoleteCount();
            if (runs > std.math.maxInt(u32) or paths > std.math.maxInt(u32) or limit < 76) return error.FileTooBig;
            return .{ .source = source, .sequence = sequence, .next_run_id = next_run_id, .run_count = runs, .obsolete_count = paths, .limit = limit };
        }

        fn advance(self: *Self, phase: Phase) void {
            self.offset = 0;
            self.phase = phase;
        }
        fn freezeAppend(self: *Self) void {
            std.debug.assert(self.used != 0 and !self.pending);
            self.pending = true;
            self.command_offset = self.total - self.used;
            self.command_append = true;
        }
        const Hash = enum { none, outer, both };
        fn emit(self: *Self, bytes: []const u8, hash: Hash) !bool {
            const remaining = bytes[self.offset..];
            if (remaining.len > self.limit -| self.total) return error.FileTooBig;
            const n = @min(remaining.len, self.buffer.len - self.used);
            const part = remaining[0..n];
            @memcpy(self.buffer[self.used..][0..n], part);
            if (hash == .both) self.inner.update(part);
            if (hash != .none) self.outer.update(part);
            self.offset += n;
            self.total += n;
            self.used += n;
            if (self.used == self.buffer.len) self.freezeAppend();
            return self.offset == bytes.len;
        }
        fn checkRun(run: manifest.RunMeta) !void {
            for ([_][]const u8{ run.path, run.smallest_namespace_name orelse &.{}, run.smallest_key, run.largest_namespace_name orelse &.{}, run.largest_key }) |bytes|
                if (bytes.len > std.math.maxInt(u32)) return error.FileTooBig;
        }

        /// One source fetch, fixed header, or at-most-16-KiB copy/hash turn.
        /// Original rejection occurs before mutation; CPU format failures poison
        /// the private producer and cannot resume into a publishable image.
        pub fn step(self: *Self, original: ?admission.Port) !bool {
            if (self.retiring or self.retired) return error.InvalidManifest;
            if (original) |control| try control.checkpoint();
            if (self.failure) |failure| return failure;
            if (self.pending) return true;
            self.advanceOne() catch |cause| {
                self.failure = cause;
                return cause;
            };
            return self.pending or self.phase == .done;
        }
        fn advanceOne(self: *Self) !void {
            switch (self.phase) {
                .placeholder => {
                    const zeros: [32]u8 = @splat(0);
                    if (try self.emit(&zeros, .none)) self.advance(.removals);
                },
                .removals => {
                    const zeros: [8]u8 = @splat(0);
                    if (try self.emit(&zeros, .outer)) self.advance(.inner_header);
                },
                .inner_header => {
                    @memcpy(self.scalar[0..8], manifest.magic);
                    std.mem.writeInt(u32, self.scalar[8..12], manifest.version, .little);
                    std.mem.writeInt(u64, self.scalar[12..20], self.next_run_id, .little);
                    std.mem.writeInt(u32, self.scalar[20..24], @intCast(self.run_count), .little);
                    std.mem.writeInt(u32, self.scalar[24..28], @intCast(self.obsolete_count), .little);
                    if (try self.emit(self.scalar[0..28], .both)) self.advance(.run);
                },
                .run => {
                    const fetched = self.source.nextRun();
                    if (self.runs == self.run_count) {
                        if (fetched != null) return error.InvalidManifest;
                        self.advance(.obsolete);
                    } else {
                        const run = fetched orelse return error.InvalidManifest;
                        try checkRun(run);
                        self.run = run; // Retain the borrowed row before another checkpoint.
                        self.scalar = manifest.runHeader(run);
                        self.field = 0;
                        self.advance(.run_fields);
                    }
                },
                .run_fields => {
                    const run = self.run.?;
                    const bytes: []const u8 = switch (self.field) {
                        0 => &self.scalar,
                        1 => run.path,
                        2 => run.smallest_namespace_name orelse &.{},
                        3 => run.smallest_key,
                        4 => run.largest_namespace_name orelse &.{},
                        5 => run.largest_key,
                        else => unreachable,
                    };
                    if (try self.emit(bytes, .both)) {
                        self.offset = 0;
                        self.field += 1;
                        if (self.field == 6) {
                            self.run = null;
                            self.runs += 1;
                            self.advance(.run);
                        }
                    }
                },
                .obsolete => {
                    const fetched = self.source.nextObsolete();
                    if (self.paths == self.obsolete_count) {
                        if (fetched != null or self.source.runCount() != self.run_count or self.source.obsoleteCount() != self.obsolete_count) return error.InvalidManifest;
                        self.advance(.inner_crc);
                    } else {
                        const path = fetched orelse return error.InvalidManifest;
                        if (path.path.len > std.math.maxInt(u32)) return error.FileTooBig;
                        self.obsolete = path;
                        std.mem.writeInt(u64, self.scalar[0..8], path.delete_after_ns, .little);
                        std.mem.writeInt(u32, self.scalar[8..12], @intCast(path.path.len), .little);
                        self.field = 0;
                        self.advance(.obsolete_fields);
                    }
                },
                .obsolete_fields => {
                    const bytes = if (self.field == 0) self.scalar[0..12] else self.obsolete.?.path;
                    if (try self.emit(bytes, .both)) {
                        self.offset = 0;
                        self.field += 1;
                        if (self.field == 2) {
                            self.obsolete = null;
                            self.paths += 1;
                            self.advance(.obsolete);
                        }
                    }
                },
                .inner_crc => {
                    std.mem.writeInt(u32, self.scalar[0..4], self.inner.final(), .little);
                    if (try self.emit(self.scalar[0..4], .outer)) self.advance(.outer_crc);
                },
                .outer_crc => {
                    std.mem.writeInt(u32, self.scalar[0..4], self.outer.final(), .little);
                    if (try self.emit(self.scalar[0..4], .none)) self.advance(.flush);
                },
                .flush => {
                    if (self.used != 0) self.freezeAppend();
                    self.advance(.patch);
                },
                .patch => {
                    @memcpy(self.buffer[0..8], manifest.journal_magic);
                    std.mem.writeInt(u64, self.buffer[8..16], self.total - 36, .little);
                    std.mem.writeInt(u64, self.buffer[16..24], self.sequence, .little);
                    std.mem.writeInt(u32, self.buffer[24..28], 1, .little);
                    std.mem.writeInt(u32, self.buffer[28..32], Crc32.hash(self.buffer[8..28]), .little);
                    self.used = 32;
                    self.command_offset = 0;
                    self.command_append = false;
                    self.pending = true;
                    self.advance(.done);
                },
                .done => {},
            }
        }

        pub fn next(self: *Self, original: ?admission.Port) !?Command {
            while (!(try self.step(original))) {}
            if (!self.pending) return null;
            return .{ .offset = self.command_offset, .append = self.command_append, .bytes = self.buffer[0..self.used] };
        }
        /// Install this fixed receipt immediately after known IO completion.
        pub fn acknowledge(self: *Self) void {
            std.debug.assert(self.pending and !self.retiring and !self.retired);
            self.pending = false;
            self.used = 0;
        }
        pub fn writeNextWithAdmission(self: *Self, writer: anytype, original: admission.Port) !bool {
            const command = (try self.next(original)) orelse return false;
            try writer.writeWithAdmission(command.offset, command.bytes, original);
            self.acknowledge();
            return true;
        }
        /// Join accepted IO before retiring its immutable command. Each step
        /// erases at most 16 KiB of fixed scratch; pinned input belongs to caller.
        pub fn retireWithAdmission(self: *Self, original: ?admission.Port) !void {
            if (self.retired) return;
            self.retiring = true;
            if (self.retire_slot == 0) {
                if (original) |control| try control.checkpoint();
                @memset(&self.buffer, undefined);
                self.retire_slot = 1;
            }
            if (self.retire_slot == 1) {
                if (original) |control| try control.checkpoint();
                @memset(&self.scalar, undefined);
                self.retire_slot = 2;
            }
            self.pending = false;
            self.retired = true;
        }
    };
}

const TestSource = struct {
    runs: []const manifest.RunMeta,
    paths: []const manifest.ObsoletePathMeta,
    run_index: usize = 0,
    path_index: usize = 0,
    pub fn runCount(self: *@This()) usize {
        return self.runs.len;
    }
    pub fn obsoleteCount(self: *@This()) usize {
        return self.paths.len;
    }
    pub fn nextRun(self: *@This()) ?manifest.RunMeta {
        if (self.run_index == self.runs.len) return null;
        defer self.run_index += 1;
        return self.runs[self.run_index];
    }
    pub fn nextObsolete(self: *@This()) ?manifest.ObsoletePathMeta {
        if (self.path_index == self.paths.len) return null;
        defer self.path_index += 1;
        return self.paths[self.path_index];
    }
};
const Fault = struct {
    checks: usize = 0,
    at: usize = std.math.maxInt(usize),
    failure: anyerror = error.Canceled,
    rejected: bool = false,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const at = self.checks;
        self.checks += 1;
        if (!self.rejected and at == self.at) {
            self.rejected = true;
            return self.failure;
        }
    }
};
fn checkCommands(encoder: anytype, expected: []const u8, original: admission.Port, fault: *Fault) !void {
    var appended: usize = 0;
    var patched = false;
    while (true) {
        const next = encoder.next(original) catch |cause| {
            try std.testing.expectEqual(fault.failure, cause);
            try std.testing.expect(fault.rejected and encoder.failure == null);
            continue;
        };
        const command = next orelse break;
        try std.testing.expect(command.bytes.len <= quantum);
        if (command.append) {
            try std.testing.expectEqual(appended, command.offset);
            const skip = @min(command.bytes.len, 32 -| appended);
            for (command.bytes[0..skip]) |byte| try std.testing.expectEqual(@as(u8, 0), byte);
            try std.testing.expectEqualSlices(u8, expected[appended + skip ..][0 .. command.bytes.len - skip], command.bytes[skip..]);
            appended += command.bytes.len;
        } else {
            try std.testing.expect(!patched and appended == expected.len);
            try std.testing.expectEqual(@as(usize, 0), command.offset);
            try std.testing.expectEqualSlices(u8, expected[0..32], command.bytes);
            patched = true;
        }
        encoder.acknowledge();
    }
    try std.testing.expectEqual(expected.len, appended);
    try std.testing.expectEqual(expected.len, encoder.total);
    try std.testing.expect(patched);
}

test "controlled physical sync manifest producer preserves canonical wire through every original rejection and large pinned labels" {
    const a = std.testing.allocator;
    const key = try a.alloc(u8, 80 * 1024 + 17);
    defer a.free(key);
    for (key, 0..) |*byte, i| byte.* = @intCast(i % 251);
    const runs = [_]manifest.RunMeta{
        .{ .id = 1, .level = 0, .size_bytes = 900, .path = "one.sst", .smallest_namespace_name = null, .smallest_key = key, .largest_namespace_name = "docs", .largest_key = key, .entry_count = 7, .tombstone_count = 2, .oldest_tombstone_unix_ns = 99, .visibility_id = 2, .gc_requested = true, .compression_stats = .{ .logical_entry_bytes = 400, .physical_entry_bytes = 200, .compressed_blocks = 3 } },
        .{ .id = 2, .level = 3, .size_bytes = 42, .path = "two.sst", .smallest_namespace_name = "", .smallest_key = "", .largest_namespace_name = null, .largest_key = "z", .entry_count = 1 },
    };
    const paths = [_]manifest.ObsoletePathMeta{ .{ .path = key, .delete_after_ns = 17 }, .{ .path = "retired", .delete_after_ns = 19 } };
    const expected = try manifest.encodeJournalFrameAlloc(a, 11, true, &.{}, &.{}, .{ .next_run_id = 20, .runs = &runs, .obsolete_paths = &paths });
    defer a.free(expected);
    var baseline: TestSource = .{ .runs = &runs, .paths = &paths };
    var encoder = try Encoder(*TestSource).init(&baseline, 11, 20, expected.len);
    var fault: Fault = .{};
    const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
    const original = admission.Port.init(&payload);
    try checkCommands(&encoder, expected, original, &fault);
    const checkpoints = fault.checks;
    try std.testing.expect(checkpoints > 30);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.FileNotFound }) |failure| {
        for (0..checkpoints) |at| {
            var source: TestSource = .{ .runs = &runs, .paths = &paths };
            var current = try Encoder(*TestSource).init(&source, 11, 20, expected.len);
            fault = .{ .at = at, .failure = failure };
            try checkCommands(&current, expected, original, &fault);
            try std.testing.expect(fault.rejected);
            try current.retireWithAdmission(original);
        }
    }
}

test "controlled physical sync manifest producer freezes commands moves CPU progress and fences rejected retirement" {
    var source: TestSource = .{ .runs = &.{}, .paths = &.{} };
    var first = try Encoder(*TestSource).init(&source, 0, 1, 76);
    try std.testing.expect(!try first.step(null));
    var moved = first;
    const command = (try moved.next(null)).?;
    var saved: [quantum]u8 = undefined;
    @memcpy(saved[0..command.bytes.len], command.bytes);
    var fault: Fault = .{ .at = 0 };
    const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
    const original = admission.Port.init(&payload);
    try std.testing.expectError(error.Canceled, moved.next(original));
    const again = (try moved.next(original)).?;
    try std.testing.expectEqual(command.bytes.ptr, again.bytes.ptr);
    try std.testing.expectEqualSlices(u8, saved[0..command.bytes.len], again.bytes);
    moved.acknowledge();
    while (try moved.next(null)) |_| moved.acknowledge();
    fault = .{ .at = 1 };
    try std.testing.expectError(error.Canceled, moved.retireWithAdmission(original));
    try std.testing.expect(moved.retiring and moved.retire_slot == 1 and !moved.retired);
    try std.testing.expectError(error.InvalidManifest, moved.next(null));
    try moved.retireWithAdmission(original);
    try std.testing.expect(moved.retired);
}

test "controlled physical sync manifest producer validates limits and source counts before unsafe casts or publication" {
    var source: TestSource = .{ .runs = &.{}, .paths = &.{} };
    try std.testing.expectError(error.FileTooBig, Encoder(*TestSource).init(&source, 0, 1, 75));
    const Wrong = struct {
        pub fn runCount(_: *@This()) usize {
            return 1;
        }
        pub fn obsoleteCount(_: *@This()) usize {
            return 0;
        }
        pub fn nextRun(_: *@This()) ?manifest.RunMeta {
            return null;
        }
        pub fn nextObsolete(_: *@This()) ?manifest.ObsoletePathMeta {
            return null;
        }
    };
    var wrong: Wrong = .{};
    var invalid = try Encoder(*Wrong).init(&wrong, 0, 2, 4096);
    try std.testing.expectError(error.InvalidManifest, invalid.next(null));
    try std.testing.expectError(error.InvalidManifest, invalid.next(null));
    try std.testing.expect(!invalid.pending);
    const run: manifest.RunMeta = .{ .id = 1, .level = 0, .size_bytes = 1, .path = "a", .smallest_namespace_name = null, .smallest_key = "", .largest_namespace_name = null, .largest_key = "", .entry_count = 1 };
    var too_small: TestSource = .{ .runs = &.{run}, .paths = &.{} };
    var limited = try Encoder(*TestSource).init(&too_small, 0, 2, 76);
    try std.testing.expectError(error.FileTooBig, limited.next(null));
    try std.testing.expect(limited.failure.? == error.FileTooBig and !limited.pending);
}
