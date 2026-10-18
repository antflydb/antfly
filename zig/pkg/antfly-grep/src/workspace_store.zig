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
const Budget = @import("antfly_regex").Budget;
pub const Entry = struct {
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
    preparations_built: usize = 0,
    preparations_reused: usize = 0,
    pub fn capture(a: A, io: std.Io, o: *const Options, previous: ?*const Generation) !*Generation {
        const self = try a.create(Generation);
        self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = o.workspace_memory_limit }, .arena = undefined };
        self.arena = .init(self.budget.allocator());
        errdefer self.deinit();
        var collector = Collector{ .generation = self, .io = io, .options = o };
        if (o.paths.items.len == 0) try collector.path(".") else for (o.paths.items) |path| try collector.path(path);
        const alloc = self.arena.allocator();
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
            // Retain only preparations for unchanged raw UTF-8. Other input
            // modes use the ordinary decoder/preparation path during search.
            if (std.unicode.utf8ValidateSlice(entry.bytes) and std.mem.indexOfScalar(u8, entry.bytes, 0) == null and !@import("antfly_encoding").hasBom(entry.bytes)) {
                if (if (previous) |old| old.find(entry.path) else null) |old| {
                    if (std.mem.eql(u8, &old.revision, &entry.revision) and old.prepared != null) {
                        entry.prepared = try clonePrepared(alloc, old.prepared.?);
                        self.preparations_reused += 1;
                        continue;
                    }
                }
                // Parser temporaries must be reclaimed per file rather than
                // retained in the generation arena. Both use the same quota.
                var scratch = std.heap.ArenaAllocator.init(self.budget.allocator());
                defer scratch.deinit();
                const temp = scratch.allocator();
                const prepared: source.Prepared = .{ .lines = try source.lines(temp, entry.bytes, '\n', false), .index = try @import("source.zig").parse(temp, entry.path, entry.bytes) };
                entry.prepared = try clonePrepared(alloc, prepared);
                self.preparations_built += 1;
            }
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        self.revision = std.fmt.bytesToHex(digest, .lower);
        self.captured_at = std.Io.Clock.real.now(io).toSeconds();
        return self;
    }
    pub fn deinit(self: *Generation) void {
        const a = self.backing;
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
fn clonePrepared(a: A, value: source.Prepared) !source.Prepared {
    var out = value;
    out.lines = try a.dupe(source.Line, value.lines);
    out.index.declarations = try a.dupe(@import("source.zig").Declaration, value.index.declarations);
    out.index.calls = try a.dupe(@import("source.zig").Call, value.index.calls);
    // Own names too: old generations and parser scratch may be released
    // immediately after publication. No generation chain is retained.
    for (out.index.declarations) |*decl| {
        decl.name = try a.dupe(u8, decl.name);
        decl.kind = try a.dupe(u8, decl.kind);
    }
    for (out.index.calls) |*call| {
        call.name = try a.dupe(u8, call.name);
        call.kind = try a.dupe(u8, call.kind);
    }
    return out;
}
const Collector = struct {
    generation: *Generation,
    io: std.Io,
    options: *const Options,
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
        const a = generation.arena.allocator();
        var file_handle = try std.Io.Dir.cwd().openFile(self.io, path_name, .{});
        defer file_handle.close(self.io);
        const before = try file_handle.stat(self.io);
        if (before.kind != .file) return error.NotRegularFile;
        if (before.size > self.options.max_file_bytes and self.options.skip_oversize) return;
        var buffer: [65536]u8 = undefined;
        var reader = file_handle.reader(self.io, &buffer);
        const bytes = try reader.interface.allocRemaining(a, .limited(@min(self.options.max_file_bytes, self.options.workspace_memory_limit)));
        const after = try file_handle.stat(self.io);
        if (before.inode != after.inode or before.size != after.size or after.size != bytes.len or
            before.mtime.nanoseconds != after.mtime.nanoseconds or before.ctime.nanoseconds != after.ctime.nanoseconds) return error.WorkspaceChangedDuringCapture;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        try generation.entries.append(a, .{ .path = try a.dupe(u8, display), .bytes = bytes, .revision = std.fmt.bytesToHex(digest, .lower), .explicit_file = explicit_file });
        generation.source_bytes += bytes.len;
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
