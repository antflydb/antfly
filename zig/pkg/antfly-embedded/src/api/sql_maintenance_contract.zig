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

//! Generation-bound internal maintenance quanta. Statistics are advisory;
//! neither sketches nor completion markers authorize row pruning or writes.
const std = @import("std");
pub const namespace = @import("../storage/db/doc_identity_namespace.zig");
pub const stats_key = "\x00\x00__metadata__:sql_statistics:v1";
pub const progress_key = "\x00\x00__metadata__:sql_maintenance:v1";
pub const max_bytes = 512 * 1024;
pub const Field = struct { name: []const u8, sketch: @import("../sql/catalog.zig").StatisticsSketch = .{} };
pub const Request = struct {
    table_id: u64,
    schema_version: u32,
    request_id: [16]u8,
    operation: enum { vacuum, analyze, statistics },
    full: bool = false,
    analyze: bool = false,
    columns: []const []const u8 = &.{},
    /// Per-quantum row target, independent of durable objective identity.
    max_rows: u32 = 2048,
    pub fn sameObjective(self: Request, other: Request) bool {
        if (self.table_id != other.table_id or self.schema_version != other.schema_version or self.operation != other.operation or self.full != other.full or self.analyze != other.analyze or self.columns.len != other.columns.len) return false;
        for (self.columns, other.columns) |left, right| if (!std.mem.eql(u8, left, right)) return false;
        return true;
    }
    pub fn validate(self: Request) !void {
        if (self.max_rows == 0 or self.max_rows > 2048) return error.InvalidSqlParameters;
        if (self.table_id == 0 or std.mem.allEqual(u8, &self.request_id, 0) or self.columns.len > 256) return error.InvalidSqlParameters;
        if (self.operation != .vacuum and (self.full or self.analyze)) return error.InvalidSqlParameters;
        for (self.columns, 0..) |name, i| {
            if (name.len == 0 or name.len > 256 or !std.unicode.utf8ValidateSlice(name) or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidSqlParameters;
            for (self.columns[0..i]) |prior| if (std.mem.eql(u8, name, prior)) return error.DuplicateSqlColumn;
        }
    }
    pub fn fingerprint(self: Request, a: std.mem.Allocator) ![32]u8 {
        const raw = try std.json.Stringify.valueAlloc(a, self, .{});
        defer a.free(raw);
        var hash: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw, &hash, .{});
        return hash;
    }
};
pub const Statistics = struct {
    identity: namespace.Namespace,
    schema_version: u32,
    rows: u64 = 0,
    fields: []Field = &.{},
    pub fn validate(self: Statistics) !void {
        if (self.identity.table_id == 0 or self.fields.len > 256) return error.InvalidSqlBackendResponse;
        for (self.fields, 0..) |field, i| {
            if (field.name.len == 0 or field.name.len > 256 or !std.unicode.utf8ValidateSlice(field.name) or std.mem.indexOfScalar(u8, field.name, 0) != null) return error.InvalidSqlBackendResponse;
            for (self.fields[0..i]) |previous| if (std.mem.eql(u8, field.name, previous.name)) return error.InvalidSqlBackendResponse;
            _ = try @import("../sql/catalog.zig").StatisticsSketch.decode(&field.sketch.encode());
        }
    }
};
pub const Progress = struct {
    request: Request,
    identity: namespace.Namespace,
    fingerprint: [32]u8,
    phase: enum { vacuum_start, vacuum, analyze, done },
    lease_until_ms: u64 = 0,
    revision: u64 = 0,
    after: ?[]const u8 = null,
    statistics: Statistics,
};
pub const Response = struct {
    table_id: u64,
    schema_version: u32,
    done: bool,
    available: bool = false,
    rows_scanned: u64 = 0,
    statistics: ?Statistics = null,
};

/// An all-owner merge is an optimizer estimate, never an owner identity.
pub const Aggregate = struct { rows: u64 = 0, fields: []Field = &.{} };

test "SQL maintenance contract rejects duplicate columns zero identities and corrupt sketches" {
    const request: Request = .{ .table_id = 5, .schema_version = 1, .request_id = @splat(1), .operation = .analyze };
    try request.validate();
    var changed = request;
    changed.columns = &.{ "x", "x" };
    try std.testing.expectError(error.DuplicateSqlColumn, changed.validate());
    changed = request;
    changed.full = true;
    try std.testing.expectError(error.InvalidSqlParameters, changed.validate());
    var field: Field = .{ .name = "x" };
    var statistics: Statistics = .{ .identity = .{ .table_id = 5 }, .schema_version = 1, .fields = @as(*[1]Field, @ptrCast(&field)) };
    try statistics.validate();
    field.sketch.non_null = 1;
    try std.testing.expectError(error.InvalidSqlBackendResponse, statistics.validate());
    statistics.identity.table_id = 0;
    try std.testing.expectError(error.InvalidSqlBackendResponse, statistics.validate());
}

test "SQL maintenance objective survives producer nonce and work-budget changes" {
    const original: Request = .{ .table_id = 5, .schema_version = 1, .request_id = @splat(1), .operation = .analyze };
    var resumed = original;
    resumed.request_id = @splat(2);
    resumed.max_rows = 1;
    try std.testing.expect(original.sameObjective(resumed));
    const a = std.testing.allocator;
    try std.testing.expect(!std.mem.eql(u8, &(try original.fingerprint(a)), &(try resumed.fingerprint(a))));
    resumed.columns = &.{"x"};
    try std.testing.expect(!original.sameObjective(resumed));
    resumed.max_rows = 0;
    try std.testing.expectError(error.InvalidSqlParameters, resumed.validate());
}
