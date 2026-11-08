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

//! Durable, bounded migration of a fenced directory into fixed-record logs.
//! Save the unread directory-reader buffer together with its kernel offset:
//! restarting does not replay an ever-growing prefix. Directory identity and
//! change timestamps fence cookies against mutation and directory replacement.
const std = @import("std");
const builtin = @import("builtin");
const api = @import("store.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const payload_header = "AFUPLOAD1\n";
const temporary_header = "AFTEMP1\n";
const buffer_len = std.Io.Dir.Iterator.reader_buffer_len;
const State = struct {
    version: u8 = 2,
    platform: std.Target.Os.Tag = builtin.os.tag,
    architecture: std.Target.Cpu.Arch = builtin.cpu.arch,
    reader_version: []const u8 = builtin.zig_version_string,
    inode: u64,
    mtime: i96,
    ctime: i96,
    offset: u64 = 0,
    buffer_start: u64 = 0,
    index: usize = 0,
    end: usize = 0,
    buffer: []const u8 = "",
    payload_length: u64 = payload_header.len,
    temporary_length: u64 = temporary_header.len,
    rebuild_payload: bool,
};
const Envelope = struct { body: []const u8, checksum: [32]u8 };

fn matches(state: State, stat: std.Io.File.Stat, rebuild: bool) bool {
    return state.version == 2 and state.platform == builtin.os.tag and state.architecture == builtin.cpu.arch and
        std.mem.eql(u8, state.reader_version, builtin.zig_version_string) and
        state.inode == stat.inode and state.mtime == stat.mtime.toNanoseconds() and
        state.ctime == stat.ctime.toNanoseconds() and state.rebuild_payload == rebuild and
        state.index <= state.end and state.end <= buffer_len and state.buffer.len == buffer_len * 2 and
        state.payload_length >= payload_header.len and (state.payload_length - payload_header.len) % 64 == 0 and
        state.temporary_length >= temporary_header.len and (state.temporary_length - temporary_header.len) % 101 == 0;
}

fn offset(dir: std.Io.Dir) !u64 {
    switch (builtin.os.tag) {
        .linux, .macos => {
            const rc = std.posix.system.lseek(dir.handle, 0, std.posix.SEEK.CUR);
            if (std.posix.errno(rc) != .SUCCESS) return error.ArtifactInventoryCursorUnavailable;
            return @intCast(rc);
        },
        else => return error.ArtifactInventoryCursorUnavailable,
    }
}

fn canonicalize(iterator: *std.Io.Dir.Iterator) !void {
    // getdents/getdirentries need not initialize record padding. Normalize it
    // before hashing/comparing buffers; only entry data identifies a cursor.
    var index: usize = if (builtin.os.tag == .macos) 8 else 0;
    while (index < iterator.reader.end) {
        const end = iterator.reader.end;
        switch (builtin.os.tag) {
            .linux => {
                const Entry = std.os.linux.dirent64;
                const name_offset = @offsetOf(Entry, "name");
                if (end - index < name_offset) return error.InvalidArtifactUploadInventory;
                const entry: *align(1) Entry = @ptrCast(&iterator.reader_buffer[index]);
                const length = entry.reclen;
                if (length <= name_offset or length > end - index) return error.InvalidArtifactUploadInventory;
                const name = iterator.reader_buffer[index + name_offset .. index + length];
                const name_len = std.mem.findScalar(u8, name, 0) orelse return error.InvalidArtifactUploadInventory;
                @memset(iterator.reader_buffer[index + name_offset + name_len + 1 .. index + length], 0);
                index += length;
            },
            .macos => {
                const Entry = std.posix.system.dirent;
                const name_offset = @offsetOf(Entry, "name");
                if (end - index < name_offset) return error.InvalidArtifactUploadInventory;
                const entry: *align(1) Entry = @ptrCast(&iterator.reader_buffer[index]);
                const length = entry.reclen;
                const used = name_offset + @as(usize, entry.namlen) + 1;
                if (length < used or length > end - index) return error.InvalidArtifactUploadInventory;
                @memset(iterator.reader_buffer[index + used .. index + length], 0);
                index += length;
            },
            else => return error.ArtifactInventoryCursorUnavailable,
        }
    }
}

fn save(a: std.mem.Allocator, io: std.Io, metadata: std.Io.Dir, dir: std.Io.Dir, iterator: *std.Io.Dir.Iterator, state: *State, payload: ?std.Io.File, temporaries: std.Io.File) !void {
    // The durable cursor may never get ahead of the records it describes.
    if (payload) |file| try file.sync(io);
    try temporaries.sync(io);
    state.offset = try offset(dir);
    state.index = iterator.reader.index;
    state.end = iterator.reader.end;
    try canonicalize(iterator);
    const encoded = std.fmt.bytesToHex(&iterator.reader_buffer, .lower);
    state.buffer = &encoded;
    defer state.buffer = "";
    const body = try std.json.Stringify.valueAlloc(a, state.*, .{});
    defer a.free(body);
    var checksum: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &checksum, .{});
    const bytes = try std.json.Stringify.valueAlloc(a, Envelope{ .body = body, .checksum = checksum }, .{});
    defer a.free(bytes);
    const pending = try metadata.createFile(io, ".inventory-scan-building", .{});
    defer pending.close(io);
    try pending.writePositionalAll(io, bytes, 0);
    try pending.sync(io);
    try metadata.rename(".inventory-scan-building", metadata, ".inventory-scan-v1", io);
    try fs_paths.syncDirectoryHandlePortable(io, metadata);
}

pub fn build(fs: anytype, io: std.Io, payloads: std.Io.Dir, metadata: std.Io.Dir, rebuild_payload: bool, visitor: ?api.ScopedUploadVisitor, cancellation: Cancellation) !void {
    var arena = std.heap.ArenaAllocator.init(fs.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const stat = try payloads.stat(io);
    var state: State = .{ .inode = @intCast(stat.inode), .mtime = stat.mtime.toNanoseconds(), .ctime = stat.ctime.toNanoseconds(), .rebuild_payload = rebuild_payload };
    const bytes = metadata.readFileAlloc(io, ".inventory-scan-v1", a, .limited(16384)) catch |err| switch (err) {
        error.FileNotFound, error.StreamTooLong => null,
        else => return err,
    };
    const previous: ?State = previous: {
        const encoded = bytes orelse break :previous null;
        const envelope = std.json.parseFromSliceLeaky(Envelope, a, encoded, .{}) catch break :previous null;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(envelope.body, &checksum, .{});
        if (!std.mem.eql(u8, &checksum, &envelope.checksum)) break :previous null;
        break :previous std.json.parseFromSliceLeaky(State, a, envelope.body, .{}) catch null;
    };
    var resuming = if (previous) |value| matches(value, stat, rebuild_payload) else false;
    var iterator = payloads.iterate();
    @memset(&iterator.reader_buffer, 0);
    const payload = if (rebuild_payload) try metadata.createFile(io, ".uploads-building", .{ .read = true, .truncate = false }) else null;
    defer if (payload) |file| file.close(io);
    const temporaries = try metadata.createFile(io, ".temporaries-building", .{ .read = true, .truncate = false });
    defer temporaries.close(io);
    if (resuming) {
        state = previous.?;
        resuming = try temporaries.length(io) >= state.temporary_length and (if (payload) |file| try file.length(io) >= state.payload_length else true);
        if (resuming) {
            _ = std.fmt.hexToBytes(&iterator.reader_buffer, state.buffer) catch {
                resuming = false;
            };
        }
    }
    if (resuming) {
        const directory_file: std.Io.File = .{ .handle = payloads.handle, .flags = .{ .nonblocking = false } };
        try io.vtable.fileSeekTo(io.userdata, directory_file, state.buffer_start);
        if (state.end != 0) {
            // Verify the consumed entry prefix, not the syscall's batch
            // length. A reopened reader may legally return shorter or longer
            // batches. Verification stays bounded by one saved buffer.
            const records_start: usize = if (builtin.os.tag == .macos) 8 else 0;
            if (state.index <= records_start or state.end < state.index) return error.InvalidArtifactUploadInventory;
            var saved = payloads.iterateAssumeFirstIteration();
            saved.reader_buffer = iterator.reader_buffer;
            saved.reader.index = records_start;
            saved.reader.end = state.end;
            var verify = payloads.iterateAssumeFirstIteration();
            @memset(&verify.reader_buffer, 0);
            var start = state.buffer_start;
            while (saved.reader.index < state.index) {
                const expected = (try saved.next(io)) orelse return error.InvalidArtifactUploadInventory;
                if (saved.reader.index > state.index) return error.InvalidArtifactUploadInventory;
                if (verify.reader.index == verify.reader.end) start = try offset(payloads);
                const actual = (try verify.next(io)) orelse return error.ArtifactInventoryCursorUnavailable;
                if (expected.inode != actual.inode or expected.kind != actual.kind or !std.mem.eql(u8, expected.name, actual.name))
                    return error.ArtifactInventoryCursorUnavailable;
            }
            state.buffer_start = start;
            iterator = verify;
        } else {
            iterator.reader.state = .reading;
        }
        if (payload) |file| try file.setLength(io, state.payload_length);
        try temporaries.setLength(io, state.temporary_length);
    } else {
        state = .{ .inode = @intCast(stat.inode), .mtime = stat.mtime.toNanoseconds(), .ctime = stat.ctime.toNanoseconds(), .rebuild_payload = rebuild_payload };
        iterator = payloads.iterate();
        @memset(&iterator.reader_buffer, 0);
        if (payload) |file| {
            try file.setLength(io, 0);
            try file.writePositionalAll(io, payload_header, 0);
        }
        try temporaries.setLength(io, 0);
        try temporaries.writePositionalAll(io, temporary_header, 0);
    }
    var batch: usize = 0;
    while (true) {
        const admission: anyerror!void = admit: {
            cancellation.check() catch |err| break :admit err;
            if (visitor) |v| if (v.maintenance) |admit| admit(v.ptr, .inspect) catch |err| break :admit err;
            break :admit {};
        };
        admission catch |err| {
            // Persistence uses the turn's reserved checkpoint time, not its
            // already-exhausted processing token.
            try save(fs.alloc, io, metadata, payloads, &iterator, &state, payload, temporaries);
            return err;
        };
        if (iterator.reader.index == iterator.reader.end)
            state.buffer_start = if (iterator.reader.state == .reset) 0 else try offset(payloads);
        const entry = (try iterator.next(io)) orelse break;
        if (builtin.is_test) _ = fs.migration_entries_read.fetchAdd(1, .monotonic);
        if (entry.kind == .file) {
            if (payload) |file| {
                const checksum = if (entry.name.len == 64) entry.name else if (entry.name.len == 75 and std.mem.endsWith(u8, entry.name, ".pending-v2")) entry.name[0..64] else "";
                if (api.validateSha256Checksum(checksum)) |_| {
                    try file.writePositionalAll(io, checksum, state.payload_length);
                    state.payload_length += 64;
                } else |_| {}
            }
            if (legacyTemporary(entry.name)) {
                try temporaries.writePositionalAll(io, entry.name, state.temporary_length);
                state.temporary_length += 101;
            }
        }
        batch += 1;
        if (batch == 128) {
            try save(fs.alloc, io, metadata, payloads, &iterator, &state, payload, temporaries);
            batch = 0;
        }
    }
    if (!matchesDirectory(state, try payloads.stat(io))) return error.ArtifactEnumerationPaused;
    if (payload) |file| {
        try file.sync(io);
        try metadata.rename(".uploads-building", metadata, ".uploads-v1", io);
        if (builtin.is_test) _ = fs.journal_backfills.fetchAdd(1, .monotonic);
    }
    try temporaries.sync(io);
    try metadata.rename(".temporaries-building", metadata, ".temporaries-v1", io);
    try fs_paths.syncDirectoryHandlePortable(io, metadata);
    metadata.deleteFile(io, ".inventory-scan-v1") catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
}

fn matchesDirectory(state: State, stat: std.Io.File.Stat) bool {
    return state.inode == stat.inode and state.mtime == stat.mtime.toNanoseconds() and state.ctime == stat.ctime.toNanoseconds();
}

pub fn legacyTemporary(name: []const u8) bool {
    if (name.len != 101 or !std.mem.eql(u8, name[64..69], ".tmp-")) return false;
    api.validateSha256Checksum(name[0..64]) catch return false;
    var nonce: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&nonce, name[69..]) catch return false;
    return true;
}

test "external lake filesystem journal inventory scan resumes across directory buffers on native filesystems" {
    const io = std.testing.io;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    try directory.dir.createDir(io, "payloads", .default_dir);
    try directory.dir.createDir(io, "metadata", .default_dir);
    const metadata = try directory.dir.openDir(io, "metadata", .{});
    defer metadata.close(io);
    {
        const payloads = try directory.dir.openDir(io, "payloads", .{});
        defer payloads.close(io);
        for (0..257) |i| {
            var name: [101]u8 = undefined;
            @memset(name[0..64], '0');
            _ = try std.fmt.bufPrint(name[64..], ".tmp-{x:0>32}", .{i});
            try payloads.writeFile(io, .{ .sub_path = &name, .data = "staging" });
        }
    }
    const Visitor = struct {
        left: usize = 7,
        fn admit(raw: *anyopaque, _: api.ScopedUploadVisitor.Maintenance) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.left == 0) return error.QueryGcBudgetExhausted;
            self.left -= 1;
        }
        fn visit(_: *anyopaque, _: api.UploadScope, _: []const u8) !void {}
    };
    var inspected: usize = 0;
    var complete = false;
    for (0..40) |turn| {
        var fs = .{
            .alloc = std.testing.allocator,
            .migration_entries_read = std.atomic.Value(usize).init(0),
            .journal_backfills = std.atomic.Value(usize).init(0),
        };
        var visitor: Visitor = .{};
        const payloads = try directory.dir.openDir(io, "payloads", .{ .iterate = true });
        defer payloads.close(io);
        build(&fs, io, payloads, metadata, true, .{ .ptr = &visitor, .visit = Visitor.visit, .maintenance = Visitor.admit }, .none) catch |err| {
            if (err != error.QueryGcBudgetExhausted) return err;
            inspected += fs.migration_entries_read.load(.monotonic);
            if (turn == 0) {
                // A filesystem interpreting a reopened cookie differently
                // must not silently skip entries or accept a completed scan.
                const saved = try metadata.readFileAlloc(io, ".inventory-scan-v1", std.testing.allocator, .limited(16384));
                defer std.testing.allocator.free(saved);
                var envelope = try std.json.parseFromSlice(Envelope, std.testing.allocator, saved, .{});
                defer envelope.deinit();
                var changed = try std.json.parseFromSlice(State, std.testing.allocator, envelope.value.body, .{});
                defer changed.deinit();
                changed.value.buffer_start = changed.value.offset;
                const body = try std.json.Stringify.valueAlloc(std.testing.allocator, changed.value, .{});
                defer std.testing.allocator.free(body);
                var checksum: [32]u8 = undefined;
                std.crypto.hash.sha2.Sha256.hash(body, &checksum, .{});
                const incompatible = try std.json.Stringify.valueAlloc(std.testing.allocator, Envelope{ .body = body, .checksum = checksum }, .{});
                defer std.testing.allocator.free(incompatible);
                try metadata.writeFile(io, .{ .sub_path = ".inventory-scan-v1", .data = incompatible });
                const reopened = try directory.dir.openDir(io, "payloads", .{ .iterate = true });
                defer reopened.close(io);
                var probe: Visitor = .{};
                try std.testing.expectError(error.ArtifactInventoryCursorUnavailable, build(&fs, io, reopened, metadata, true, .{ .ptr = &probe, .visit = Visitor.visit, .maintenance = Visitor.admit }, .none));
                try metadata.writeFile(io, .{ .sub_path = ".inventory-scan-v1", .data = saved });
            }
            continue;
        };
        inspected += fs.migration_entries_read.load(.monotonic);
        complete = true;
        break;
    }
    try std.testing.expect(complete);
    try std.testing.expectEqual(@as(usize, 257), inspected);
    const temporaries = try metadata.openFile(io, ".temporaries-v1", .{});
    defer temporaries.close(io);
    try std.testing.expectEqual(@as(u64, temporary_header.len + 257 * 101), try temporaries.length(io));
}

test "external lake filesystem journal migration accepts shorter and longer reopened directory batches" {
    const io = std.testing.io;
    const ShortRead = struct {
        fn read(raw: ?*anyopaque, reader: *std.Io.Dir.Reader, entries: []std.Io.Dir.Entry) std.Io.Dir.Reader.Error!usize {
            const buffer = reader.buffer;
            defer reader.buffer = buffer;
            if (reader.index == reader.end) reader.buffer = buffer[0..std.Io.Dir.Reader.min_buffer_len];
            return std.testing.io.vtable.dirRead(raw, reader, entries);
        }
    };
    var vtable = io.vtable.*;
    vtable.dirRead = ShortRead.read;
    var short_io = io;
    short_io.vtable = &vtable;
    const Visitor = struct {
        left: usize = 27,
        fn admit(raw: *anyopaque, _: api.ScopedUploadVisitor.Maintenance) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.left == 0) return error.QueryGcBudgetExhausted;
            self.left -= 1;
        }
        fn visit(_: *anyopaque, _: api.UploadScope, _: []const u8) !void {}
    };
    for ([_]bool{ false, true }) |short_first| {
        var directory = std.testing.tmpDir(.{});
        defer directory.cleanup();
        try directory.dir.createDir(io, "payloads", .default_dir);
        try directory.dir.createDir(io, "metadata", .default_dir);
        const metadata = try directory.dir.openDir(io, "metadata", .{});
        defer metadata.close(io);
        {
            const payloads = try directory.dir.openDir(io, "payloads", .{});
            defer payloads.close(io);
            for (0..80) |i| {
                var name: [101]u8 = undefined;
                @memset(name[0..64], '0');
                _ = try std.fmt.bufPrint(name[64..], ".tmp-{x:0>32}", .{i});
                try payloads.writeFile(io, .{ .sub_path = &name, .data = "legacy" });
            }
        }
        var fs = .{ .alloc = std.testing.allocator, .migration_entries_read = std.atomic.Value(usize).init(0), .journal_backfills = std.atomic.Value(usize).init(0) };
        {
            const payloads = try directory.dir.openDir(io, "payloads", .{ .iterate = true });
            defer payloads.close(io);
            var visitor: Visitor = .{};
            try std.testing.expectError(error.QueryGcBudgetExhausted, build(&fs, if (short_first) short_io else io, payloads, metadata, true, .{ .ptr = &visitor, .visit = Visitor.visit, .maintenance = Visitor.admit }, .none));
        }
        const reopened = try directory.dir.openDir(io, "payloads", .{ .iterate = true });
        defer reopened.close(io);
        try build(&fs, if (short_first) io else short_io, reopened, metadata, true, null, .none);
        try std.testing.expectEqual(@as(usize, 80), fs.migration_entries_read.load(.monotonic));
        const log = try metadata.openFile(io, ".temporaries-v1", .{});
        defer log.close(io);
        try std.testing.expectEqual(@as(u64, temporary_header.len + 80 * 101), try log.length(io));
    }
}
