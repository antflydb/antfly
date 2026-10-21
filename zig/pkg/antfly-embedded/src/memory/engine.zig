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

//! Engine adapters for the single native memory policy. Table callbacks route
//! through existing Raft/transaction ownership; embedded uses the native DB.
const std = @import("std");
pub const service = @import("service.zig");
const reads_mod = @import("../api/table_read_source.zig");
const writes_mod = @import("../api/table_write_source.zig");
const db_types = @import("../storage/db/types.zig");
const A = std.mem.Allocator;

// Memory policy rows may be objects, scalars, or arrays. Storage adapters own
// one versioned object envelope so routed NDJSON never flattens policy values.
fn encodeRow(a: A, json: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{{\"memory_record_version\":1,\"value\":{s}}}", .{json});
}
pub fn decodeRow(a: A, json: []const u8) ![]const u8 {
    // Preserve full-width integer fields instead of round-tripping via f64.
    const value = std.json.parseFromSliceLeaky(std.json.Value, a, json, .{ .parse_numbers = false }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.MemoryCorruptState,
    };
    return decodeValue(a, value);
}
fn decodeValue(a: A, value: std.json.Value) ![]const u8 {
    if (value != .object) return error.MemoryCorruptState;
    const version = value.object.get("memory_record_version") orelse return error.MemoryCorruptState;
    const current_version = switch (version) {
        .integer => version.integer == 1,
        .number_string => std.mem.eql(u8, version.number_string, "1"),
        else => false,
    };
    if (!current_version) return error.MemoryCorruptState;
    const payload = value.object.get("value") orelse return error.MemoryCorruptState;
    return std.json.Stringify.valueAlloc(a, payload, .{});
}

pub const Routed = struct {
    table: []const u8,
    reads: reads_mod.TableReadSource,
    writes: writes_mod.TableWriteSource,
    context: service.Context,
    resource_ptr: ?*anyopaque = null,
    resource_fn: ?*const fn (*anyopaque, A, service.types.provenance.Resource) anyerror!service.types.provenance.Snapshot = null,
    request: @import("../api/operation.zig").RequestContext = .{},

    pub fn backend(self: *Routed) service.Backend {
        return .{ .ptr = self, .get = get, .scan = scan, .commit = commit, .resource = if (self.resource_fn != null) resource else null };
    }
    fn resource(ptr: *anyopaque, a: A, reference: service.types.provenance.Resource) !service.types.provenance.Snapshot {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        return self.resource_fn.?(self.resource_ptr.?, a, reference);
    }
    fn get(ptr: *anyopaque, a: A, key: []const u8) !?service.Row {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.check();
        const row = (try self.reads.lookup(a, self.table, key, .{ .execution_deadline_ns = self.request.deadline_ns, .execution_io = self.request.deadline_io, .cancellation = self.request.cancellation }, .read_index)) orelse return null;
        return .{ .key = key, .json = try decodeRow(a, row.json), .version = row.version, .digest = row.expected_content_digest };
    }
    fn scan(ptr: *anyopaque, a: A, prefix: []const u8, cursor: ?[]const u8, limit: u32) !service.Page {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.check();
        try validateCursor(prefix, cursor);
        const end = try prefixEnd(a, prefix);
        const response = (try self.reads.scan(a, self.table, cursor orelse prefix, end, .{ .execution_deadline_ns = self.request.deadline_ns, .cancellation = self.request.cancellation, .include_documents = true, .inclusive_from = cursor == null, .exclusive_to = true, .limit = limit + 1 }, .read_index)) orelse return error.MemorySpaceNotFound;
        var rows: std.ArrayList(service.Row) = .empty;
        var lines = std.mem.tokenizeScalar(u8, response.ndjson, '\n');
        while (lines.next()) |line| {
            var value = try std.json.parseFromSliceLeaky(std.json.Value, a, line, .{ .parse_numbers = false });
            const id = value.object.get("_id") orelse return error.MemoryCorruptState;
            if (id != .string or !std.mem.startsWith(u8, id.string, prefix)) return error.MemoryCorruptState;
            _ = value.object.swapRemove("_id");
            _ = value.object.swapRemove("_content_hash");
            const document = value.object.get("_source") orelse value;
            try rows.append(a, .{ .key = id.string, .json = try decodeValue(a, document) });
        }
        const count = @min(rows.items.len, limit);
        return .{ .rows = rows.items[0..count], .next = if (rows.items.len > count) rows.items[count - 1].key else null };
    }
    fn commit(ptr: *anyopaque, a: A, expected: ?service.Row, changes: []const service.Change) !void {
        const self: *Routed = @ptrCast(@alignCast(ptr));
        try self.context.check();
        var writes: std.ArrayList(db_types.TransactionWrite) = .empty;
        var deletes: std.ArrayList([]const u8) = .empty;
        for (changes) |change| {
            if (change.json) |json| try writes.append(a, .{ .key = change.key, .value = try encodeRow(a, json) }) else try deletes.append(a, change.key);
        }
        const result = (try self.writes.commitTransactionWithCancellation(a, &.{.{ .table_name = self.table, .writes = writes.items, .deletes = deletes.items, .predicates = &.{.{ .key = "control", .expected_version = if (expected) |row| row.version else 0, .expected_content_digest = if (expected) |row| row.digest else null, .unique_absence = expected == null }} }}, .write, self.request.cancellation)) orelse return error.MemoryCapabilityUnavailable;
        switch (result) {
            .conflict => return error.MemoryRevisionConflict,
            .committed => |committed| if (committed.propagation_pending or committed.visibility_pending) return error.MemoryCommitPending,
        }
    }
};

pub fn Native(comptime DB: type) type {
    return struct {
        db: *DB,
        context: service.Context,
        resource_ptr: ?*anyopaque = null,
        resource_fn: ?*const fn (*anyopaque, A, service.types.provenance.Resource) anyerror!service.types.provenance.Snapshot = null,
        const Self = @This();
        pub fn backend(self: *Self) service.Backend {
            return .{ .ptr = self, .get = get, .scan = scan, .commit = commit, .resource = if (self.resource_fn != null) resource else null };
        }
        fn resource(ptr: *anyopaque, a: A, reference: service.types.provenance.Resource) !service.types.provenance.Snapshot {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.resource_fn.?(self.resource_ptr.?, a, reference);
        }
        fn get(ptr: *anyopaque, a: A, key: []const u8) !?service.Row {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.check();
            const result = (try self.db.getDocument(a, key, .{})) orelse return null;
            return .{ .key = key, .json = try decodeRow(a, result.json), .version = result.version orelse try self.db.getTimestamp(a, key), .digest = result.expected_content_digest };
        }
        fn scan(ptr: *anyopaque, a: A, prefix: []const u8, cursor: ?[]const u8, limit: u32) !service.Page {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.check();
            try validateCursor(prefix, cursor);
            const result = try self.db.scan(a, cursor orelse prefix, try prefixEnd(a, prefix), .{ .include_documents = true, .inclusive_from = cursor == null, .exclusive_to = true, .limit = limit + 1 });
            const rows = try a.alloc(service.Row, result.documents.len);
            for (result.documents, rows) |document, *row| row.* = .{ .key = document.id, .json = try decodeRow(a, document.json) };
            const count = @min(rows.len, limit);
            return .{ .rows = rows[0..count], .next = if (rows.len > count) rows[count - 1].key else null };
        }
        fn commit(ptr: *anyopaque, a: A, expected: ?service.Row, changes: []const service.Change) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try self.context.check();
            var writes: std.ArrayList(db_types.BatchWrite) = .empty;
            var deletes: std.ArrayList([]const u8) = .empty;
            for (changes) |change| {
                if (change.json) |json| try writes.append(a, .{ .key = change.key, .value = try encodeRow(a, json) }) else try deletes.append(a, change.key);
            }
            self.db.batch(.{ .writes = writes.items, .deletes = deletes.items, .sync_level = .write, .predicates = &.{.{ .key = "control", .expected_version = if (expected) |row| row.version else 0, .expected_content_digest = if (expected) |row| row.digest else null, .unique_absence = expected == null }} }) catch |err| switch (err) {
                error.VersionConflict, error.IntentConflict => return error.MemoryRevisionConflict,
                else => return err,
            };
        }
    };
}

fn validateCursor(prefix: []const u8, cursor: ?[]const u8) !void {
    if (cursor) |value| if (!std.mem.startsWith(u8, value, prefix)) return error.InvalidMemoryCursor;
}
fn prefixEnd(a: A, prefix: []const u8) ![]const u8 {
    if (prefix.len == 0) return "\xff";
    const end = try a.dupe(u8, prefix);
    end[end.len - 1] += 1;
    return end;
}
