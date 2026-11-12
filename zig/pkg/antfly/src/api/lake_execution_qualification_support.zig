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

//! Test-only catalog control shared by runtime and S3 operation qualification.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.metadata_lake_index_catalog;
const publication = @import("lake_index_publication.zig");
const A = std.mem.Allocator;
pub const State = struct {
    allocator: A,
    table: local.common_topology_records.TableRecord,
    owned: ?[]u8 = null,
    now_ms: u64 = 101,
    checkpoints: usize = 0,
    interrupt_after_bulk_source: bool = false,
    interrupt_at_bulk_receipt: ?usize = null,
    bulk_receipts: usize = 0,
    discards: usize = 0,
    mutex: std.Io.Mutex = .init,
    io: std.Io,
    pub fn deinit(self: *@This()) void {
        if (self.owned) |bytes| self.allocator.free(bytes);
    }
    pub fn set(self: *@This(), bytes: []const u8) !void {
        const owned = try self.allocator.dupe(u8, bytes);
        if (self.owned) |old| self.allocator.free(old);
        self.owned = owned;
        self.table.lake_index_catalog_json = owned;
    }
    pub fn lease(self: *@This()) publication.Lease {
        return .{ .ptr = self, .snapshot = snapshot, .save_checkpoint = save, .discard_checkpoint = discard };
    }
    pub fn clock(self: *@This()) publication.Clock {
        return .{ .ptr = self, .now_ms = now };
    }
    fn snapshot(raw: *anyopaque, a: A) !local.common_topology_records.TableRecord {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        var value = self.table;
        value.lake_index_catalog_json = try a.dupe(u8, value.lake_index_catalog_json);
        return value;
    }
    fn save(raw: *anyopaque, saved: catalog.Publication) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        var current = try catalog.parse(self.allocator, self.table.lake_index_catalog_json);
        defer current.deinit();
        const changed_bulk = if (saved.construction.len != 0 and saved.declarations.len == 0) changed: {
            if (current.value.checkpoint) |previous| if (previous.construction.len == saved.construction.len and
                std.mem.eql(u8, previous.construction[0].manifest.artifact_id, saved.construction[0].manifest.artifact_id)) break :changed false;
            break :changed true;
        } else false;
        const bytes = try catalog.encode(self.allocator, try current.value.saveCheckpoint(saved, self.now_ms));
        defer self.allocator.free(bytes);
        try self.set(bytes);
        self.checkpoints += 1;
        if (changed_bulk) {
            self.bulk_receipts += 1;
            if (self.interrupt_after_bulk_source or self.interrupt_at_bulk_receipt == self.bulk_receipts) return error.Cancelled;
        }
    }
    fn discard(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        var current = try catalog.parse(self.allocator, self.table.lake_index_catalog_json);
        defer current.deinit();
        const bytes = try catalog.encode(self.allocator, try current.value.discardCheckpoint(current.value.pending.?.token, self.now_ms));
        defer self.allocator.free(bytes);
        try self.set(bytes);
        self.discards += 1;
    }
    fn now(raw: *const anyopaque) !u64 {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        return self.now_ms;
    }
};
pub fn requiredEnv(a: A, comptime key: [:0]const u8) ![]u8 {
    const value = std.c.getenv(key) orelse return error.MissingIntegrationEnvironment;
    return a.dupe(u8, std.mem.span(value));
}
pub fn enabled(comptime key: [:0]const u8) bool {
    const value = std.c.getenv(key) orelse return false;
    return std.mem.eql(u8, std.mem.span(value), "1");
}
