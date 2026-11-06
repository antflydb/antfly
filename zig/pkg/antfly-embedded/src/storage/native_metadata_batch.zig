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

//! Owned metadata effects with exact preimage validation and late-bound values.
const std = @import("std");
const publication = @import("native_publication.zig");
const values = @import("native_publication_value.zig");
const A = std.mem.Allocator;
const Read = struct { key: []const u8, expected: ?[]const u8 };
const Write = struct { key: []const u8, bytes: []const u8, binding: ?values.Value = null };

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    scope: publication.Scope,
    reads: []Read,
    writes: []Write,
    scratch: []u8,
    admitted: bool = false,
    staged: bool = false,

    pub fn deinit(self: *Prepared) void {
        for (self.writes) |*write| if (write.binding) |*binding| binding.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
    pub fn participant(self: *Prepared) publication.Participant {
        return .{ .ptr = self, .validate_fn = validate, .stage = stage };
    }
    /// Descriptors keep their fixed backing and size through native commit.
    /// The producer must seal every bound value before publication begins.
    pub fn bindValue(self: *Prepared, key: []const u8, value: values.Value) !void {
        if (self.admitted or self.staged) return error.InvalidDerivedApplyState;
        for (self.writes) |*write| if (std.mem.eql(u8, write.key, key)) {
            if (write.bytes.len != value.initialization_view.len) return error.InvalidNativePublicationValue;
            const retained = value.retained();
            if (write.binding) |*previous| previous.deinit();
            write.binding = retained;
            write.bytes = value.initialization_view;
            return;
        };
        return error.NotFound;
    }
    fn validate(raw: *anyopaque, view: publication.View) !void {
        const self: *Prepared = @ptrCast(@alignCast(raw));
        if (self.staged) return error.InvalidDerivedApplyState;
        self.admitted = false;
        for (self.writes) |write| {
            if (write.binding) |binding| _ = try binding.read();
        }
        for (self.reads) |read| {
            var scratch = std.heap.FixedBufferAllocator.init(self.scratch);
            const actual: ?[]const u8 = view.getMetadata(scratch.allocator(), self.scope, read.key) catch |err| switch (err) {
                error.NotFound => null,
                // An unchanged value fits the preallocated key/value buffer.
                // A larger foreign replacement is an ordinary read conflict.
                error.OutOfMemory => return error.FileBusy,
                else => return err,
            };
            if (read.expected) |expected| {
                if (actual == null or !std.mem.eql(u8, expected, actual.?)) return error.FileBusy;
            } else if (actual != null) return error.FileBusy;
        }
        self.admitted = true;
    }
    fn stage(raw: *anyopaque, view: publication.View) !void {
        const self: *Prepared = @ptrCast(@alignCast(raw));
        if (!self.admitted or self.staged) return error.InvalidDerivedApplyState;
        for (self.writes) |write| {
            if (write.binding) |binding| _ = try binding.read();
        }
        for (self.writes) |write| try view.putMetadata(self.scope, write.key, write.bytes);
        self.staged = true;
    }
};

/// `source` is borrowed only during this call. Domain planners see an ordinary
/// get/put overlay; the returned publication retains no transaction or DB.
pub fn prepare(a: A, scope: publication.Scope, source: anytype, planner: anytype) !Prepared {
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var overlay: Overlay(@TypeOf(source)) = .{ .alloc = owned, .source = source };
    planner.plan(&overlay) catch |err| return overlay.failure orelse err;
    // Some existing domain helpers deliberately treat a missing get as null.
    // Never let that swallow IO, cancellation or witness-allocation failures.
    if (overlay.failure) |err| return err;
    const reads = try owned.alloc(Read, overlay.reads.count());
    var iterator = overlay.reads.iterator();
    var index: usize = 0;
    var scratch_length: usize = 1;
    while (iterator.next()) |entry| : (index += 1) {
        const key = entry.key_ptr.*;
        const length = try std.math.add(usize, try std.math.add(usize, scope.prefix.len, key.len), try std.math.add(usize, if (entry.value_ptr.*) |bytes| bytes.len else 0, 1));
        scratch_length = @max(scratch_length, length);
        reads[index] = .{ .key = key, .expected = entry.value_ptr.* };
    }
    const writes = try owned.alloc(Write, overlay.writes.count());
    var write_iterator = overlay.writes.iterator();
    index = 0;
    while (write_iterator.next()) |entry| : (index += 1) writes[index] = .{ .key = entry.key_ptr.*, .bytes = entry.value_ptr.* };
    std.mem.sort(Read, reads, {}, struct {
        fn less(_: void, left: Read, right: Read) bool {
            return std.mem.order(u8, left.key, right.key) == .lt;
        }
    }.less);
    std.mem.sort(Write, writes, {}, struct {
        fn less(_: void, left: Write, right: Write) bool {
            return std.mem.order(u8, left.key, right.key) == .lt;
        }
    }.less);
    var captured = scope;
    captured.prefix = try owned.dupe(u8, scope.prefix);
    const scratch = try owned.alloc(u8, scratch_length);
    return .{ .arena = arena, .scope = captured, .reads = reads, .writes = writes, .scratch = scratch };
}

fn Overlay(comptime Source: type) type {
    return struct {
        alloc: A,
        source: Source,
        reads: std.StringHashMapUnmanaged(?[]const u8) = .empty,
        writes: std.StringHashMapUnmanaged([]const u8) = .empty,
        failure: ?anyerror = null,
        fn capture(self: *@This(), key: []const u8) !?[]const u8 {
            if (!@import("control_key.zig").isScopedMetadata(key) and !@import("internal_keys.zig").isControlRecordKey(key)) return error.InvalidCommitMetadata;
            if (self.reads.getEntry(key)) |entry| return entry.value_ptr.*;
            const stable = try self.alloc.dupe(u8, key);
            const raw: ?[]const u8 = self.source.get(key) catch |err| if (@as(anyerror, err) == error.NotFound) null else return err;
            const expected: ?[]const u8 = if (raw) |bytes| try self.alloc.dupe(u8, bytes) else null;
            try self.reads.put(self.alloc, stable, expected);
            return expected;
        }
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            if (self.writes.get(key)) |bytes| return bytes;
            return (self.capture(key) catch |err| {
                self.failure = err;
                return err;
            }) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, bytes: []const u8) anyerror!void {
            self.putImpl(key, bytes) catch |err| {
                self.failure = err;
                return err;
            };
        }
        fn putImpl(self: *@This(), key: []const u8, bytes: []const u8) !void {
            if (!@import("control_key.zig").isScopedMetadata(key)) return error.InvalidCommitMetadata;
            _ = try self.capture(key);
            const stable = self.reads.getEntry(key).?.key_ptr.*;
            try self.writes.put(self.alloc, stable, try self.alloc.dupe(u8, bytes));
        }
    };
}

const fixture_key = "\x00\x00__test__:prepared_metadata";
const SourceFixture = struct {
    value: []const u8 = "before",
    failure: ?anyerror = null,
    fn get(self: *@This(), _: []const u8) ![]const u8 {
        if (self.failure) |err| return err;
        return self.value;
    }
};
const PlannerFixture = struct {
    pub fn plan(_: *@This(), txn: anytype) !void {
        const value = try txn.get(fixture_key);
        if (!std.mem.eql(u8, value, "before")) return error.FileBusy;
        try txn.put(fixture_key, "first");
        try txn.put(fixture_key, "finished");
    }
};

test "native metadata preparation owns exact witnesses and coalesces writes across every allocation failure" {
    const Sweep = struct {
        fn run(a: A) !void {
            var source: SourceFixture = .{};
            var planner: PlannerFixture = .{};
            var prepared = try prepare(a, .{ .inode = 3, .prefix = &.{} }, &source, &planner);
            defer prepared.deinit();
            source.value = "changed";
            try std.testing.expectEqual(@as(usize, 1), prepared.reads.len);
            try std.testing.expectEqual(@as(usize, 1), prepared.writes.len);
            try std.testing.expectEqualStrings("before", prepared.reads[0].expected.?);
            try std.testing.expectEqualStrings("finished", prepared.writes[0].bytes);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Sweep.run, .{});
}

test "native metadata preparation never converts a failed read into an absent owner" {
    const Planner = struct {
        pub fn plan(_: *@This(), txn: anytype) !void {
            _ = txn.get(fixture_key) catch null;
            // A domain helper may decide it has no mutations after a missing
            // read. The latched failure still refuses the entire preparation.
        }
    };
    var planner: Planner = .{};
    var source: SourceFixture = .{ .failure = error.InjectedReadFailure };
    try std.testing.expectError(error.InjectedReadFailure, prepare(std.testing.allocator, .{ .inode = 3, .prefix = &.{} }, &source, &planner));
}

test "native metadata publication validates before primary changes and keeps sealed bindings after producer close" {
    const a = std.testing.allocator;
    const docs = @import("lite/docstore.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/prepared-native-metadata.aflite", .{tmp.sub_path});
    defer a.free(path);
    var store = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
    defer store.close();
    store.maintenance_start_suppressed = true;
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put(fixture_key, "before");
        try write.commit();
    }
    var read = try store.beginRead();
    var read_live = true;
    defer if (read_live) read.abort();
    var planner: PlannerFixture = .{};
    var prepared = try prepare(a, .{ .inode = (try store.file.file.stat(std.testing.io)).inode, .file_namespace = store.file.header.reader_epoch_namespace, .prefix = &.{} }, &read, &planner);
    defer prepared.deinit();
    read.abort();
    read_live = false;
    var builder = try values.Builder.create(a, 8);
    var builder_live = true;
    defer if (builder_live) builder.deinit();
    {
        var value = builder.value();
        defer value.deinit();
        try prepared.bindValue(fixture_key, value);
    }
    const before = store.file.activeCheckpoint().commit_sequence;
    {
        var write = try store.beginWrite();
        defer write.abort();
        try write.put(fixture_key, "primary");
        try write.setNativePublication(prepared.participant());
        try std.testing.expectError(error.NativePublicationValueNotBound, write.commit());
    }
    try std.testing.expectEqual(before, store.file.activeCheckpoint().commit_sequence);
    try builder.write(0, "finished");
    try builder.seal();
    builder.deinit();
    builder_live = false;
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put(fixture_key, "primary");
        try write.setNativePublication(prepared.participant());
        try write.commit();
    }
    try std.testing.expectEqual(before + 1, store.file.activeCheckpoint().commit_sequence);
    const value = (try store.file.getDocumentAlloc(a, fixture_key)).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("finished", value);
    try std.testing.expect((try store.file.check()).valid);
}

test "native metadata read conflicts refuse primary writes including larger foreign metadata" {
    const a = std.testing.allocator;
    const docs = @import("lite/docstore.zig");
    const counter_key = "counter";
    const large: [1024]u8 = @splat('x');
    for ([_][]const u8{ "foreign", &large }) |foreign| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/prepared-native-metadata-conflict.aflite", .{tmp.sub_path});
        defer a.free(path);
        var store = try docs.Store.createWithOptions(a, path, .{ .io = std.testing.io, .no_sync = true, .reclamation = .{ .enabled = false } });
        defer store.close();
        store.maintenance_start_suppressed = true;
        {
            var write = try store.beginWrite();
            errdefer write.abort();
            try write.put(fixture_key, "before");
            try write.commit();
        }
        var source = try store.beginRead();
        var source_live = true;
        defer if (source_live) source.abort();
        var planner: PlannerFixture = .{};
        var prepared = try prepare(a, .{ .inode = (try store.file.file.stat(std.testing.io)).inode, .file_namespace = store.file.header.reader_epoch_namespace, .prefix = &.{} }, &source, &planner);
        defer prepared.deinit();
        source.abort();
        source_live = false;
        {
            var write = try store.beginWrite();
            errdefer write.abort();
            try write.put(fixture_key, foreign);
            try write.commit();
        }
        const before = store.file.activeCheckpoint().commit_sequence;
        {
            var write = try store.beginWrite();
            defer write.abort();
            try write.put(counter_key, "1");
            try write.setNativePublication(prepared.participant());
            try std.testing.expectError(error.FileBusy, write.commit());
        }
        try std.testing.expect(!prepared.admitted and !prepared.staged);
        try std.testing.expectEqual(before, store.file.activeCheckpoint().commit_sequence);
        try std.testing.expect((try store.file.getDocumentAlloc(a, counter_key)) == null);
        const stored = (try store.file.getDocumentAlloc(a, fixture_key)).?;
        defer a.free(stored);
        try std.testing.expectEqualSlices(u8, foreign, stored);
        try std.testing.expect((try store.file.check()).valid);
    }
}
