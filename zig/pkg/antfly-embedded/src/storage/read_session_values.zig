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

//! Shared immutable read transport values; no physical session or reader.
const std = @import("std");

/// Move-only capability; publication locks and manager registrations stay in
/// their creator. Installing a lock transfers it only after success.
pub const PublicationReadLease = extern struct {
    version: u32 = 1,
    context: ?*anyopaque = null,
    close_fn: ?*const fn (*anyopaque) callconv(.c) void = null,

    pub fn validate(self: PublicationReadLease) !void {
        if (self.version != 1) return error.UnsupportedVersion;
        if (self.context == null or self.close_fn == null) return error.InvalidGenerationTransition;
    }
    pub fn release(self: *PublicationReadLease) !void {
        if (self.context == null) return;
        try self.validate();
        const context = self.context.?;
        const close = self.close_fn.?;
        self.* = .{};
        close(context);
    }
    pub fn deinit(self: *PublicationReadLease) void {
        self.release() catch @panic("incompatible publication lease owner");
    }
};

pub const RelationalMetadata = struct { schema_version: u32, index_name: ?[]const u8 };

pub const Budget = struct {
    /// Retain native SQL Datums instead of public JSON projections.
    native_cells: bool = false,
    rows: usize = 128,
    records: usize = 1024,
    output_bytes: usize = 1024 * 1024,
    /// Soft page target; a single legal row may exceed it. output_bytes
    /// remains the hard row and page allocation limit.
    target_bytes: usize = 0,
    time_ns: u64 = 5 * std.time.ns_per_ms,

    pub fn validate(self: Budget) !void {
        if (self.rows == 0 or self.rows > 4096 or self.records == 0 or self.records > 65_536 or
            self.output_bytes == 0 or self.output_bytes > 16 * 1024 * 1024 or
            self.target_bytes > self.output_bytes or
            self.time_ns == 0 or self.time_ns > std.time.ns_per_s) return error.InvalidRelationalRowsBudget;
    }
};

pub const Row = struct {
    key: []const u8,
    json: []const u8,
    typed: ?std.json.Value = null,
    typed_cells: ?@import("../sql/catalog.zig").Row.TypedCells = null,
    sql_nulls: ?[]const bool = null,
    json_null_fields: []const []const u8 = &.{},
    version: u64,
    schema_version: u32,
    semantic_hash: [32]u8,
    expected_content_digest: ?[32]u8 = null,
    cursor: ?[]const u8 = null,
};

pub const Page = struct {
    owner_release: ?@import("read_session_contract.zig").Release = null,
    arena: std.heap.ArenaAllocator,
    rows: []const Row,
    more: bool,
    records_examined: usize,
    /// Continuation after the last examined record, including rejected rows.
    next_cursor: ?[]const u8 = null,
    output_bytes: usize,
    /// Work counters distinguish covering scans from primary-row fallbacks.
    primary_lookups: usize = 0,
    index_only_rows: usize = 0,

    pub fn deinit(self: *Page) void {
        self.arena.deinit();
        if (self.owner_release) |owner| owner.deinit();
        self.* = undefined;
    }
};

test "publication lease validates before touching owner pointers" {
    var lease: PublicationReadLease = .{ .version = 0, .context = @ptrFromInt(1), .close_fn = @ptrFromInt(0x1000) };
    try std.testing.expectError(error.UnsupportedVersion, lease.release());
    lease.version = 1;
    lease.close_fn = null;
    try std.testing.expectError(error.InvalidGenerationTransition, lease.release());
    lease = .{};
    try lease.release();
    try lease.release();
}
