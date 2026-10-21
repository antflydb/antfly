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

//! Owned point observations and cursor transcripts. Replaying a transcript
//! detects changed results and scan phantoms without retaining source buffers.
//! This is optimistic validation, not durable exclusion across a decision.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const DocStore = @import("../docstore.zig").DocStore;
const Entry = @import("../backend_erased.zig").Entry;
const A = std.mem.Allocator;

fn checkpoint(control: ?admission.Port, failure: ?*?anyerror) !void {
    if (failure) |slot| if (slot.* != null) return error.ReadAdmissionRejected;
    if (control) |original| original.checkpoint() catch |err| {
        if (failure) |slot| {
            if (slot.* == null) slot.* = err;
            return error.ReadAdmissionRejected;
        }
        return err;
    };
}
fn copy(a: A, bytes: []const u8, control: ?admission.Port, failure: ?*?anyerror) ![]const u8 {
    try checkpoint(control, failure);
    const result = try a.alloc(u8, bytes.len);
    errdefer a.free(result);
    var offset: usize = 0;
    while (offset < bytes.len) {
        try checkpoint(control, failure);
        const end = offset + @min(@as(usize, 4096), bytes.len - offset);
        @memcpy(result[offset..end], bytes[offset..end]);
        offset = end;
    }
    return result;
}
const Fingerprint = struct {
    len: usize,
    digest: [32]u8,
    fn of(bytes: []const u8, control: ?admission.Port, failure: ?*?anyerror) !Fingerprint {
        try checkpoint(control, failure);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        var offset: usize = 0;
        while (offset < bytes.len) {
            try checkpoint(control, failure);
            const end = offset + @min(@as(usize, 4096), bytes.len - offset);
            hash.update(bytes[offset..end]);
            offset = end;
        }
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return .{ .len = bytes.len, .digest = digest };
    }
};
const Row = struct {
    key: Fingerprint,
    value: Fingerprint,
    fn of(entry: Entry, control: ?admission.Port, failure: ?*?anyerror) !Row {
        return .{ .key = try Fingerprint.of(entry.key, control, failure), .value = try Fingerprint.of(entry.value, control, failure) };
    }
};
pub const Kind = enum { seek_after, seek_before, next, prev };
const Operation = struct { kind: Kind, argument: []const u8, expected: ?Row };
const Point = struct { key: []const u8, expected: ?Fingerprint };

pub const Cursor = struct {
    allocator: A,
    control: ?admission.Port,
    failure: ?*?anyerror,
    active: bool = true,
    operations: std.ArrayListUnmanaged(Operation) = .empty,

    pub fn record(self: *Cursor, kind: Kind, argument: []const u8, entry: ?Entry) !void {
        self.recordInner(kind, argument, entry) catch |err| {
            if (self.failure) |slot| if (slot.* == null) {
                slot.* = err;
            };
            return err;
        };
    }
    pub fn check(self: *Cursor) !void {
        if (!self.active) return error.TransactionClosed;
        try checkpoint(self.control, self.failure);
    }
    fn recordInner(self: *Cursor, kind: Kind, argument: []const u8, entry: ?Entry) !void {
        if (!self.active) return error.TransactionClosed;
        try checkpoint(self.control, self.failure);
        const expected = if (entry) |row| try Row.of(row, self.control, self.failure) else null;
        const owned = try copy(self.allocator, argument, self.control, self.failure);
        errdefer self.allocator.free(owned);
        try self.operations.append(self.allocator, .{ .kind = kind, .argument = owned, .expected = expected });
    }
    pub fn close(self: *Cursor) void {
        self.active = false;
    }
    fn deinit(self: *Cursor) void {
        std.debug.assert(!self.active);
        for (self.operations.items) |operation| self.allocator.free(operation.argument);
        self.operations.deinit(self.allocator);
    }
};

pub const Owned = struct {
    allocator: A,
    control: ?admission.Port,
    failure: ?anyerror = null,
    points: std.ArrayListUnmanaged(Point) = .empty,
    positions: std.StringHashMapUnmanaged(usize) = .empty,
    cursors: std.ArrayListUnmanaged(*Cursor) = .empty,
    sealed: bool = false,

    pub fn init(a: A, control: ?admission.Port) Owned {
        return .{ .allocator = a, .control = control };
    }
    pub fn recordPoint(self: *Owned, key: []const u8, value: ?[]const u8) !void {
        self.recordPointInner(key, value) catch |err| {
            if (self.failure == null) self.failure = err;
            return err;
        };
    }
    pub fn check(self: *Owned) !void {
        try checkpoint(self.control, &self.failure);
    }
    fn recordPointInner(self: *Owned, key: []const u8, value: ?[]const u8) !void {
        if (self.sealed) return error.TransactionClosed;
        try checkpoint(self.control, &self.failure);
        const expected = if (value) |bytes| try Fingerprint.of(bytes, self.control, &self.failure) else null;
        if (self.positions.get(key)) |index| {
            if (!std.meta.eql(expected, self.points.items[index].expected)) return error.PreparedReadSetChanged;
            return;
        }
        const owned = try copy(self.allocator, key, self.control, &self.failure);
        errdefer self.allocator.free(owned);
        try self.points.ensureUnusedCapacity(self.allocator, 1);
        try self.positions.put(self.allocator, owned, self.points.items.len);
        self.points.appendAssumeCapacity(.{ .key = owned, .expected = expected });
    }
    pub fn openCursor(self: *Owned) !*Cursor {
        return self.openCursorInner() catch |err| {
            if (self.failure == null) self.failure = err;
            return err;
        };
    }
    fn openCursorInner(self: *Owned) !*Cursor {
        if (self.sealed) return error.TransactionClosed;
        try checkpoint(self.control, &self.failure);
        const cursor = try self.allocator.create(Cursor);
        errdefer self.allocator.destroy(cursor);
        cursor.* = .{ .allocator = self.allocator, .control = self.control, .failure = &self.failure };
        try self.cursors.append(self.allocator, cursor);
        return cursor;
    }
    /// No source cursor or borrowed control/failure address may escape capture.
    pub fn finish(self: *Owned) !void {
        try checkpoint(self.control, &self.failure);
        for (self.cursors.items) |cursor| if (cursor.active) return error.InvalidArgument;
        self.control = null;
        for (self.cursors.items) |cursor| {
            cursor.control = null;
            cursor.failure = null;
        }
        self.sealed = true;
    }
    pub fn take(self: *Owned) !Owned {
        if (!self.sealed or self.failure != null) return error.InvalidArgument;
        const result = self.*;
        self.* = Owned.init(self.allocator, null);
        return result;
    }
    pub fn deinit(self: *Owned) void {
        self.positions.deinit(self.allocator);
        for (self.points.items) |point| self.allocator.free(point.key);
        self.points.deinit(self.allocator);
        for (self.cursors.items) |cursor| {
            cursor.deinit();
            self.allocator.destroy(cursor);
        }
        self.cursors.deinit(self.allocator);
        self.* = undefined;
    }
    /// Caller retains publication/apply exclusion. All observations are compared
    /// with one fresh immutable primary read, including each cursor's stopping row.
    pub fn validate(self: *const Owned, store: *DocStore, original: ?admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        self.validateInner(store, trace.bind(original)) catch |err| return trace.failure orelse err;
        if (trace.failure) |cause| return cause;
    }
    fn validateInner(self: *const Owned, store: *DocStore, control: ?admission.Port) !void {
        try checkpoint(control, null);
        if (!self.sealed or self.failure != null) return error.InvalidArgument;
        if (self.points.items.len == 0 and self.cursors.items.len == 0) return;
        var read = if (control) |original| try store.beginReadTxnWithAdmission(.retain, original) else try store.beginReadTxn();
        defer read.abort();
        for (self.points.items) |point| {
            try checkpoint(control, null);
            const value = read.get(point.key) catch |err| if (err == error.NotFound) null else return err;
            const actual = if (value) |bytes| try Fingerprint.of(bytes, control, null) else null;
            if (!std.meta.eql(actual, point.expected)) return error.PreparedReadSetChanged;
        }
        for (self.cursors.items) |captured| {
            if (captured.active) return error.InvalidArgument;
            var cursor = try read.openPhysicalCursorAdapter();
            defer cursor.close();
            for (captured.operations.items) |operation| {
                try checkpoint(control, null);
                const entry = switch (operation.kind) {
                    .seek_after => try cursor.seekAtOrAfter(operation.argument),
                    .seek_before => try cursor.seekAtOrBefore(operation.argument),
                    .next => try cursor.next(),
                    .prev => try cursor.prev(),
                };
                const actual = if (entry) |row| try Row.of(row, control, null) else null;
                if (!std.meta.eql(actual, operation.expected)) return error.PreparedReadSetChanged;
            }
        }
        try checkpoint(control, null);
    }
};
