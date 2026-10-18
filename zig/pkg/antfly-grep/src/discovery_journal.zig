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

//! Kernel coverage of discovery/configuration dependencies. This is opt-in:
//! kernel-invisible writers still require force_rescan. Incomplete coverage,
//! allocation loss or overflow uses ordinary discovery and source reads.
const std = @import("std");
const Watch = @import("source_watch.zig").Watch;
const Token = @import("source_watch.zig").Token;
pub const Journal = struct {
    io: std.Io,
    budget: @import("antfly_regex").Budget,
    watch: Watch,
    tokens: std.StringHashMapUnmanaged(Token) = .empty,
    coverage: bool = true,
    sources_covered: bool = false,
    source_epoch: u64 = 0,
    dependency_epoch: u64 = 0,
    sealed: bool = false,
    pub fn init(a: std.mem.Allocator, io: std.Io) Journal {
        return .{ .io = io, .budget = .{ .backing = a, .limit = 1024 * 1024 }, .watch = Watch.init() };
    }
    pub fn deinit(self: *Journal) void {
        self.watch.deinit();
        var it = self.tokens.keyIterator();
        while (it.next()) |key| self.budget.allocator().free(key.*);
        self.tokens.deinit(self.budget.allocator());
        self.tokens = .empty;
    }
    pub fn reset(self: *Journal) void {
        self.deinit();
        self.watch.reset();
        self.coverage = true;
        self.sources_covered = false;
        self.sealed = false;
    }
    pub fn clean(self: *Journal) bool {
        self.watch.poll();
        return self.sealed and self.coverage and !self.watch.failed and self.watch.fd >= 0 and self.dependency_epoch == self.watch.epoch;
    }
    fn insert(self: *Journal, path: []const u8, token: ?Token) void {
        const value = token orelse {
            self.coverage = false;
            return;
        };
        const a = self.budget.allocator();
        const key = a.dupe(u8, path) catch {
            self.coverage = false;
            self.watch.release(value);
            return;
        };
        self.tokens.put(a, key, value) catch {
            self.coverage = false;
            a.free(key);
            self.watch.release(value);
        };
    }
    pub fn directory(self: *Journal, path: []const u8) void {
        if (!self.coverage or self.tokens.contains(path)) return;
        if (std.fs.path.dirname(path)) |parent| {
            if (!std.mem.eql(u8, parent, path) and parent.len != 0) self.directory(parent);
        } else if (!std.mem.eql(u8, path, ".")) self.directory(".");
        if (!self.coverage) return;
        var dir = std.Io.Dir.cwd().openDir(self.io, path, .{}) catch {
            self.coverage = false;
            return;
        };
        defer dir.close(self.io);
        self.insert(path, self.watch.armDirectory(dir));
    }
    pub fn dependency(self: *Journal, path: []const u8) void {
        if (!self.coverage) return;
        // Watch the closest existing parent: creation of missing ancestors or
        // replacement of this path then invalidates the discovery certificate.
        var parent = std.fs.path.dirname(path) orelse ".";
        while (true) {
            var dir = std.Io.Dir.cwd().openDir(self.io, parent, .{}) catch |err| {
                if (err == error.FileNotFound or err == error.NotDir) {
                    parent = std.fs.path.dirname(parent) orelse {
                        self.coverage = false;
                        return;
                    };
                    continue;
                }
                self.coverage = false;
                return;
            };
            dir.close(self.io);
            self.directory(parent);
            break;
        }
        if (!self.coverage or self.tokens.contains(path)) return;
        var file = std.Io.Dir.cwd().openFile(self.io, path, .{}) catch |err| {
            if (err != error.FileNotFound and err != error.NotDir) self.coverage = false;
            return;
        };
        defer file.close(self.io);
        const canonical = std.Io.Dir.cwd().realPathFileAlloc(self.io, path, self.budget.allocator()) catch {
            self.coverage = false;
            return;
        };
        defer self.budget.allocator().free(canonical);
        if (std.fs.path.dirname(canonical)) |actual_parent| self.directory(actual_parent);
        const stat = file.stat(self.io) catch {
            self.coverage = false;
            return;
        };
        if (stat.kind == .directory) {
            self.directory(path);
            return;
        }
        self.insert(path, self.watch.arm(file));
    }
    pub fn seal(self: *Journal, sources: *Watch, entries: anytype) void {
        var it = self.tokens.valueIterator();
        while (it.next()) |token| self.watch.certify(token.*);
        self.watch.poll();
        it = self.tokens.valueIterator();
        while (it.next()) |token| if (!self.watch.intact(token.*)) {
            self.coverage = false;
        };
        sources.poll();
        self.sources_covered = sources.fd >= 0 and !sources.failed;
        for (entries) |entry| {
            if (entry.blob.watcher != sources or entry.blob.token == null or !sources.intact(entry.blob.token.?)) self.sources_covered = false;
        }
        if (self.sources_covered) sources.clearEvents();
        self.source_epoch = sources.epoch;
        self.dependency_epoch = self.watch.epoch;
        self.sealed = true;
    }
};

test "journal covers missing optional files, directory membership and lost coverage" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/optional.ignore", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    var journal = Journal.init(std.testing.allocator, io);
    defer journal.deinit();
    if (journal.watch.fd < 0) return error.SkipZigTest;
    var sources = Watch.init();
    defer sources.deinit();
    const Empty = struct { blob: struct { watcher: ?*Watch, token: ?Token } };
    journal.dependency(path);
    journal.seal(&sources, @as([]const Empty, &.{}));
    if (!journal.clean()) return error.SkipZigTest;
    try tmp.dir.writeFile(io, .{ .sub_path = "optional.ignore", .data = "new-rule" });
    try std.testing.expect(!journal.clean());
    journal.reset();
    journal.dependency(path);
    journal.seal(&sources, @as([]const Empty, &.{}));
    try std.testing.expect(journal.clean());
    try tmp.dir.writeFile(io, .{ .sub_path = "optional.ignore", .data = "modified-rule" });
    try std.testing.expect(!journal.clean());
    journal.reset();
    journal.dependency(path);
    journal.seal(&sources, @as([]const Empty, &.{}));
    try std.testing.expect(journal.clean());
    journal.watch.failClosed();
    try std.testing.expect(!journal.clean());
}
