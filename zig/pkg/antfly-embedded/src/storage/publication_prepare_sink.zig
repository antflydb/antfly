// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Borrowed sink for the actual materialized PREPARE batch. Its owner stages
//! the existing native HA outbox before the same accepting transaction commits.
//! It cannot decide P, commit the batch, execute rows, or retain this view.
const std = @import("std");
pub const Sink = struct {
    ptr: *anyopaque,
    max_bytes: usize,
    stage: *const fn (*anyopaque, @import("commit_participant.zig").View, []const Change) anyerror!void,
};
pub const Change = struct { key: []const u8, value: ?[]const u8 };

/// Owner of the ordered terminal frame. The decision and actual row effects
/// have already been staged when this runs. A failed journal aborts both.
pub const TerminalSink = struct {
    ptr: *anyopaque,
    stage: *const fn (*anyopaque, @import("commit_participant.zig").View) anyerror!void,
};

/// Instantiated only for the captured native PREPARE branch. All other
/// transaction batches keep their original writer and allocate no recorder.
pub fn Recording(comptime Batch: type) type {
    return struct {
        inner: *Batch,
        alloc: std.mem.Allocator,
        maximum: usize,
        bytes: usize = 0,
        changes: std.StringArrayHashMapUnmanaged(?[]const u8) = .empty,
        const Self = @This();
        fn remember(self: *Self, key: []const u8, value: ?[]const u8) !usize {
            if (!std.mem.startsWith(u8, key, "\x00\x00")) return error.InvalidCommitMetadata;
            const previous = self.changes.getIndex(key);
            const old = if (previous) |index| 12 + key.len + (if (self.changes.values()[index]) |bytes| bytes.len else 0) else 0;
            const needed = try std.math.add(usize, 12 + key.len, if (value) |bytes| bytes.len else 0);
            const next = try std.math.add(usize, self.bytes - old, needed);
            if (next > self.maximum) return error.TransactionTooLarge;
            const owned = if (value) |bytes| try self.alloc.dupe(u8, bytes) else null;
            errdefer if (owned) |bytes| self.alloc.free(bytes);
            if (previous) |index| {
                // Keep previous buffers alive until the backend stages the
                // batch, even when the final frame only carries the last value.
                self.changes.values()[index] = owned;
            } else {
                const owned_key = try self.alloc.dupe(u8, key);
                errdefer self.alloc.free(owned_key);
                try self.changes.putNoClobber(self.alloc, owned_key, owned);
            }
            self.bytes = next;
            return previous orelse self.changes.count() - 1;
        }
        pub fn get(self: *Self, key: []const u8) ![]const u8 {
            return self.inner.get(key);
        }
        pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
            const index = try self.remember(key, value);
            try self.inner.put(self.changes.keys()[index], self.changes.values()[index].?);
        }
        pub fn delete(self: *Self, key: []const u8) !void {
            const index = try self.remember(key, null);
            return self.inner.delete(self.changes.keys()[index]);
        }
        pub fn openCursor(self: *Self) @TypeOf(@as(*Batch, undefined).openCursor()) {
            return self.inner.openCursor();
        }
        pub fn setCommitParticipant(self: *Self, participant: @import("commit_participant.zig").Participant) !void {
            return self.inner.setCommitParticipant(participant);
        }
        pub fn setReplayOpaque(self: *Self, sequence: u64, payload: []const u8) !void {
            return self.inner.setReplayOpaque(sequence, payload);
        }
        pub fn finish(self: *Self, sink: Sink, journal: @import("commit_participant.zig").View) !void {
            const output = try self.alloc.alloc(Change, self.changes.count());
            for (self.changes.keys(), self.changes.values(), output) |key, value, *entry| entry.* = .{ .key = key, .value = value };
            std.mem.sort(Change, output, {}, struct {
                fn less(_: void, a: Change, b: Change) bool {
                    return std.mem.order(u8, a.key, b.key) == .lt;
                }
            }.less);
            // This raw restricted view intentionally bypasses recording: the
            // metadata HA outbox must not recursively contain its own payload.
            try sink.stage(sink.ptr, journal, output);
        }
    };
}

test "publication prepare recorder owns sorted final private postimages and excludes its own journal" {
    const Raw = struct {
        puts: usize = 0,
        fail: bool = false,
        pub fn get(_: *@This(), _: []const u8) ![]const u8 {
            return error.NotFound;
        }
        pub fn put(self: *@This(), _: []const u8, _: []const u8) !void {
            if (self.fail) return error.TestWriteFailure;
            self.puts += 1;
        }
        pub fn delete(_: *@This(), _: []const u8) !void {}
    };
    const Check = struct {
        calls: usize = 0,
        fn stage(raw: *anyopaque, view: @import("commit_participant.zig").View, entries: []const Change) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 2), entries.len);
            try std.testing.expectEqualStrings("\x00\x00a", entries[0].key);
            try std.testing.expect(entries[0].value == null);
            try std.testing.expectEqualStrings("\x00\x00z", entries[1].key);
            try std.testing.expectEqualStrings("final", entries[1].value.?);
            // Uses the raw batch, so the journal never recursively includes itself.
            try view.put("\x00\x00ha-journal", "owned-frame");
            self.calls += 1;
        }
        fn run(a: std.mem.Allocator) !void {
            var arena: std.heap.ArenaAllocator = .init(a);
            defer arena.deinit();
            var raw: Raw = .{};
            var recorder: Recording(Raw) = .{ .inner = &raw, .alloc = arena.allocator(), .maximum = 128 };
            var value = [_]u8{ 'o', 'l', 'd' };
            try recorder.put("\x00\x00z", &value);
            @memset(&value, 'X');
            try std.testing.expectEqualStrings("old", recorder.changes.get("\x00\x00z").?.?);
            try recorder.put("\x00\x00z", "final");
            try recorder.put("\x00\x00a", "discarded");
            try recorder.delete("\x00\x00a");
            try std.testing.expectError(error.InvalidCommitMetadata, recorder.put("public-row", "refused"));
            const before = raw.puts;
            try std.testing.expectError(error.TransactionTooLarge, recorder.put("\x00\x00oversized", &(@as([129]u8, @splat(1)))));
            try std.testing.expectEqual(before, raw.puts);
            var check: @This() = .{};
            try recorder.finish(.{ .ptr = &check, .max_bytes = 128, .stage = stage }, @import("commit_participant.zig").View.from(&raw));
            try std.testing.expectEqual(@as(usize, 1), check.calls);
            try std.testing.expectEqual(@as(usize, 2), recorder.changes.count());
            try std.testing.expect(recorder.changes.getIndex("\x00\x00ha-journal") == null);
            raw.fail = true;
            try std.testing.expectError(error.TestWriteFailure, recorder.put("\x00\x00fail", "abort-host-batch"));
            // Host must abort on write failure; finish/commit is never invoked.
            try std.testing.expectEqual(@as(usize, 1), check.calls);
        }
    };
    try Check.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
