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

//! Immutable, bounded workspace generations. Capture owns every byte; query
//! workers borrow a generation until joined. Publication is transactional.
const std = @import("std");
const A = std.mem.Allocator;
const Options = @import("options.zig").Options;
const source = @import("source_cache.zig");
const Watch = @import("source_watch.zig").Watch;
const Token = @import("source_watch.zig").Token;
const Budget = @import("antfly_regex").Budget;
pub const Entry = struct {
    blob: *Blob,
    path: []const u8,
    bytes: []const u8,
    revision: [64]u8,
    explicit_file: bool = false,
    prepared: ?source.Prepared = null,
};
pub const Generation = struct {
    backing: A,
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    entries: std.ArrayList(Entry) = .empty,
    revision: [64]u8 = undefined,
    captured_at: i64 = 0,
    source_bytes: usize = 0,
    watch_fallback: bool = false,
    source_bytes_read: usize = 0,
    source_bytes_reused: usize = 0,
    preparations_built: usize = 0,
    preparations_reused: usize = 0,
    pub fn capture(a: A, io: std.Io, o: *const Options, previous: ?*const Generation) !*Generation {
        return captureWatched(a, io, o, previous, null);
    }
    pub fn captureWatched(a: A, io: std.Io, o: *const Options, previous: ?*const Generation, watcher: ?*Watch) !*Generation {
        if (watcher) |w| w.beginRefresh();
        return captureOnce(a, io, o, previous, watcher) catch |err| {
            if (watcher) |w| if (w.fd >= 0) {
                // Witness resources must never turn a capture that fits without
                // watchers into an error. Retry once with every witness lost.
                w.deinit();
                const next = try captureOnce(a, io, o, previous, watcher);
                next.watch_fallback = true;
                return next;
            };
            return err;
        };
    }
    fn captureOnce(a: A, io: std.Io, o: *const Options, previous: ?*const Generation, watcher: ?*Watch) !*Generation {
        if (o.workspace_memory_limit < @sizeOf(Generation)) return error.OutOfMemory;
        const self = try a.create(Generation);
        self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = o.workspace_memory_limit - @sizeOf(Generation) }, .arena = undefined };
        self.arena = .init(self.budget.allocator());
        errdefer self.deinit();
        var collector = Collector{ .generation = self, .io = io, .options = o, .previous = previous, .watcher = watcher };
        if (o.paths.items.len == 0) try collector.path(".") else for (o.paths.items) |path| try collector.path(path);
        std.mem.sort(Entry, self.entries.items, {}, struct {
            fn less(_: void, x: Entry, y: Entry) bool {
                return std.mem.order(u8, x.path, y.path) == .lt;
            }
        }.less);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly-workspace-v1");
        for (self.entries.items, 0..) |*entry, i| {
            if (i > 0 and std.mem.eql(u8, self.entries.items[i - 1].path, entry.path)) return error.DuplicateWorkspacePath;
            var len: [8]u8 = undefined;
            std.mem.writeInt(u64, &len, entry.path.len, .little);
            hash.update(&len);
            hash.update(entry.path);
            hash.update(&entry.revision);
            hash.update(&.{@intFromBool(entry.explicit_file)});
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        self.revision = std.fmt.bytesToHex(digest, .lower);
        self.captured_at = std.Io.Clock.real.now(io).toSeconds();
        return self;
    }
    /// Shared blobs count in full against every generation that retains them.
    pub fn retainedBytes(self: *const Generation) usize {
        var bytes = self.budget.live + @sizeOf(Generation);
        for (self.entries.items) |entry| bytes += entry.blob.budget.live + @sizeOf(Blob);
        return bytes;
    }
    pub fn deinit(self: *Generation) void {
        const a = self.backing;
        for (self.entries.items) |entry| entry.blob.release();
        self.arena.deinit();
        a.destroy(self);
    }
    fn find(self: *const Generation, path_name: []const u8) ?*const Entry {
        var begin: usize = 0;
        var end = self.entries.items.len;
        while (begin < end) {
            const mid = begin + (end - begin) / 2;
            const entry = &self.entries.items[mid];
            switch (std.mem.order(u8, entry.path, path_name)) {
                .eq => return entry,
                .lt => begin = mid + 1,
                .gt => end = mid,
            }
        }
        return null;
    }
};
/// Independent allocations never borrow a generation allocator. Refresh can
/// release the old generation immediately, including after arbitrarily many reuses.
const Blob = struct {
    backing: A,
    refs: usize = 1, // capture/publication run serially after query workers join
    budget: Budget,
    arena: std.heap.ArenaAllocator,
    bytes: []const u8 = "",
    prepared: ?source.Prepared = null,
    stamp: ?std.Io.File.Stat = null,
    watcher: ?*Watch = null,
    token: ?Token = null,
    fn release(self: *Blob) void {
        self.refs -= 1;
        if (self.refs != 0) return;
        const a = self.backing;
        if (self.watcher) |watcher| if (self.token) |token| watcher.release(token);
        self.arena.deinit();
        a.destroy(self);
    }
};
fn clonePrepared(a: A, temporary: A, bytes: []const u8, value: source.Prepared) !source.Prepared {
    var out = value;
    out.lines = try a.dupe(source.Line, value.lines);
    out.index.declarations = try a.dupe(@import("source.zig").Declaration, value.index.declarations);
    out.index.calls = try a.dupe(@import("source.zig").Call, value.index.calls);
    // Most identifiers are views into the captured bytes. Preserve those
    // offsets instead of allocating a second copy or a large name hash table.
    // Parser-synthesized names still need independent ownership.
    var kinds: std.StringHashMapUnmanaged([]const u8) = .empty;
    defer kinds.deinit(temporary);
    inline for (.{ "declarations", "calls" }) |field| {
        for (@field(out.index, field)) |*item| {
            const start = @intFromPtr(item.name.ptr);
            const base = @intFromPtr(bytes.ptr);
            if (start >= base and start - base <= bytes.len and item.name.len <= bytes.len - (start - base)) {
                item.name = bytes[start - base ..][0..item.name.len];
            } else item.name = try a.dupe(u8, item.name);
            const entry = try kinds.getOrPut(temporary, item.kind);
            if (!entry.found_existing) {
                entry.value_ptr.* = try a.dupe(u8, item.kind);
                entry.key_ptr.* = entry.value_ptr.*;
            }
            item.kind = entry.value_ptr.*;
        }
    }
    return out;
}
const Collector = struct {
    generation: *Generation,
    io: std.Io,
    options: *const Options,
    previous: ?*const Generation,
    watcher: ?*Watch = null,
    fn path(self: *Collector, name: []const u8) !void {
        if (std.mem.eql(u8, name, "-")) return error.WorkspaceStdinUnsupported;
        var scratch = std.heap.ArenaAllocator.init(self.generation.budget.allocator());
        defer scratch.deinit();
        const a = scratch.allocator();
        const info = try std.Io.Dir.cwd().statFile(self.io, name, .{});
        if (info.kind == .directory) {
            const full = try std.Io.Dir.cwd().realPathFileAlloc(self.io, name, a);
            var discovery = @import("discovery.zig").Discovery{ .a = a, .io = self.io, .options = self.options };
            defer discovery.deinit();
            try discovery.prepare(full);
            try discovery.walk(full, if (std.mem.eql(u8, name, ".") and self.options.paths.items.len == 0) "" else name, self);
        } else if (info.kind == .file) try self.addFile(name, name, true) else return error.NotRegularFile;
    }
    pub fn stopped(_: *Collector) bool {
        return false;
    }
    pub fn reportError(_: *Collector, _: []const u8, err: anyerror) anyerror!void {
        // An incomplete capture must never replace the published generation.
        return err;
    }
    pub fn file(self: *Collector, path_name: []const u8, display: []const u8) !void {
        return self.addFile(path_name, display, false);
    }
    fn addFile(self: *Collector, path_name: []const u8, display: []const u8, explicit_file: bool) !void {
        const generation = self.generation;
        if (generation.entries.items.len >= self.options.workspace_max_files) return error.WorkspaceFileLimit;
        var file_handle = try std.Io.Dir.cwd().openFile(self.io, path_name, .{});
        defer file_handle.close(self.io);
        const before = try file_handle.stat(self.io);
        if (before.kind != .file) return error.NotRegularFile;
        if (before.size > self.options.max_file_bytes and self.options.skip_oversize) return;
        const available = generation.budget.limit -| generation.budget.live;
        if (available < @sizeOf(Blob)) return error.OutOfMemory;
        if (self.watcher) |watcher| if (if (self.previous) |old| old.find(display) else null) |old| {
            if (old.blob.token != null and old.blob.watcher == watcher and old.blob.stamp != null and sameStamp(old.blob.stamp.?, before) and watcher.clean(old.blob.token.?, file_handle)) {
                if (old.blob.budget.live + @sizeOf(Blob) > available) return error.OutOfMemory;
                old.blob.refs += 1;
                errdefer old.blob.release();
                try self.append(old.blob, display, explicit_file, old.revision, true, available);
                generation.source_bytes_reused += old.bytes.len;
                return;
            }
        };
        var token = if (self.watcher) |w| w.arm(file_handle) else null;
        defer if (token) |value| self.watcher.?.release(value);
        var blob = try generation.backing.create(Blob);
        // Use the ordinary search allocator for parser/capture arenas so
        // bounded parser outcomes remain identical across both entry points.
        blob.* = .{ .backing = generation.backing, .budget = .{ .backing = generation.backing, .limit = available - @sizeOf(Blob) }, .arena = undefined };
        blob.arena = .init(blob.budget.allocator());
        errdefer blob.release();
        var buffer: [65536]u8 = undefined;
        var reader = file_handle.reader(self.io, &buffer);
        blob.bytes = try reader.interface.allocRemaining(blob.arena.allocator(), .limited(@min(self.options.max_file_bytes, self.options.workspace_memory_limit)));
        const after = try file_handle.stat(self.io);
        if (before.inode != after.inode or before.size != after.size or after.size != blob.bytes.len or
            before.mtime.nanoseconds != after.mtime.nanoseconds or before.ctime.nanoseconds != after.ctime.nanoseconds) return error.WorkspaceChangedDuringCapture;
        generation.source_bytes_read += blob.bytes.len;
        if (token) |value| self.watcher.?.certify(value);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(blob.bytes, &digest, .{});
        const revision = std.fmt.bytesToHex(digest, .lower);
        var reused = false;
        if (if (self.previous) |old| old.find(display) else null) |old| {
            if (std.mem.eql(u8, &old.revision, &revision)) {
                const retained = old.blob.budget.live + @sizeOf(Blob);
                if (retained > available) return error.OutOfMemory;
                blob.release();
                blob = old.blob;
                blob.refs += 1;
                reused = true;
            }
        }
        if (!reused and std.unicode.utf8ValidateSlice(blob.bytes) and std.mem.indexOfScalar(u8, blob.bytes, 0) == null and !@import("antfly_encoding").hasBom(blob.bytes)) {
            var scratch = std.heap.ArenaAllocator.init(blob.budget.allocator());
            defer scratch.deinit();
            const temp = scratch.allocator();
            const prepared: source.Prepared = .{ .lines = try source.lines(temp, blob.bytes, '\n', false), .index = try @import("source.zig").parse(temp, display, blob.bytes) };
            blob.prepared = try clonePrepared(blob.arena.allocator(), temp, blob.bytes, prepared);
        }
        if (blob.watcher) |w| if (blob.token) |old| w.release(old);
        blob.watcher = self.watcher;
        blob.token = token;
        blob.stamp = before;
        token = null;
        try self.append(blob, display, explicit_file, revision, reused, available);
    }
    fn sameStamp(a: std.Io.File.Stat, b: std.Io.File.Stat) bool {
        return a.inode == b.inode and a.kind == b.kind and a.size == b.size and a.mtime.nanoseconds == b.mtime.nanoseconds and a.ctime.nanoseconds == b.ctime.nanoseconds;
    }
    fn append(self: *Collector, blob: *Blob, display: []const u8, explicit_file: bool, revision: [64]u8, reused: bool, available: usize) !void {
        const generation = self.generation;
        const a = generation.arena.allocator();
        const charge = blob.budget.live + @sizeOf(Blob);
        if (charge > available) return error.OutOfMemory;
        generation.budget.limit -= charge;
        errdefer generation.budget.limit += charge;
        try generation.entries.append(a, .{ .blob = blob, .path = try a.dupe(u8, display), .bytes = blob.bytes, .revision = revision, .explicit_file = explicit_file, .prepared = blob.prepared });
        generation.source_bytes += blob.bytes.len;
        if (blob.prepared != null) {
            if (reused) generation.preparations_reused += 1 else generation.preparations_built += 1;
        }
    }
};

test "workspace generations own reused indexes and failed captures release their quota" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "a.py", .data = "def retry():\n    STOP()\n" });
    const path_name = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(path_name);
    var o: Options = .{ .no_ignore = true };
    defer o.deinit(a);
    try o.paths.append(a, path_name);
    var old: ?*Generation = try Generation.capture(a, io, &o, null);
    defer if (old) |value| value.deinit();
    const next = try Generation.capture(a, io, &o, old);
    defer next.deinit();
    try std.testing.expectEqualSlices(u8, &old.?.revision, &next.revision);
    try std.testing.expectEqual(@as(usize, 0), next.preparations_built);
    try std.testing.expectEqual(@as(usize, 1), next.preparations_reused);
    try std.testing.expect(old.?.entries.items[0].blob == next.entries.items[0].blob);
    try std.testing.expect(next.retainedBytes() <= o.workspace_memory_limit);
    old.?.deinit();
    old = null;
    try std.testing.expectEqualStrings("retry", next.entries.items[0].prepared.?.index.declarations[0].name);
    try std.testing.expectEqualStrings("STOP", next.entries.items[0].prepared.?.index.calls[0].name);
    o.workspace_max_files = 0;
    try std.testing.expectError(error.WorkspaceFileLimit, Generation.capture(a, io, &o, next));
    o.workspace_max_files = 10;
    o.workspace_memory_limit = 1;
    try std.testing.expectError(error.OutOfMemory, Generation.capture(a, io, &o, next));
}

test "preparations preserve source name offsets and own parser synthesized names" {
    const a = std.testing.allocator;
    var retained = std.heap.ArenaAllocator.init(a);
    defer retained.deinit();
    const bytes = try retained.allocator().dupe(u8, "def retry():\n    STOP()\n");
    var scratch = std.heap.ArenaAllocator.init(a);
    var scratch_live = true;
    defer if (scratch_live) scratch.deinit();
    const temp = scratch.allocator();
    var calls = [_]@import("source.zig").Call{.{ .name = try temp.dupe(u8, "Receiver.method"), .line = 2, .column = 1, .start_byte = 13, .end_byte = 20 }};
    var declarations = [_]@import("source.zig").Declaration{.{ .name = bytes[4..9], .kind = "function", .start = 1, .end = 2, .label_line = 1, .start_byte = 0, .end_byte = bytes.len }};
    const index = try clonePrepared(retained.allocator(), temp, bytes, .{ .lines = try source.lines(temp, bytes, '\n', false), .index = .{ .declarations = &declarations, .calls = &calls, .complete = true } });
    scratch.deinit();
    scratch_live = false;
    try std.testing.expect(index.index.declarations[0].name.ptr == bytes[4..9].ptr);
    try std.testing.expectEqualStrings("Receiver.method", index.index.calls[0].name);
}
