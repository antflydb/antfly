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
/// Keep deployed maintenance packets byte-identical when no new physical
/// feature is requested. Older owners can still serve ordinary statistics,
/// VACUUM and ANALYZE during a rolling upgrade; old progress hashes still bind.
fn stringify(value: anytype, writer: anytype) !void {
    try writer.beginObject();
    inline for (@typeInfo(@TypeOf(value)).@"struct".field_names) |name| {
        if (comptime std.mem.eql(u8, name, "cluster_index") or std.mem.eql(u8, name, "cluster_admission") or std.mem.eql(u8, name, "row_heap_format") or std.mem.eql(u8, name, "cluster")) {
            if (@field(value, name)) |present| {
                try writer.objectField(name);
                try writer.write(present);
            }
        } else {
            try writer.objectField(name);
            try writer.write(@field(value, name));
        }
    }
    try writer.endObject();
}
pub const Field = struct { name: []const u8, sketch: @import("../sql/catalog.zig").StatisticsSketch = .{} };
pub const Request = struct {
    table_id: u64,
    schema_version: u32,
    request_id: [16]u8,
    operation: enum { vacuum, analyze, statistics, cluster_check, cluster },
    cluster_index: ?[]const u8 = null,
    cluster_admission: ?@import("../storage/pessimistic_lock/participant.zig").Admission = null,
    full: bool = false,
    analyze: bool = false,
    columns: []const []const u8 = &.{},
    /// Per-quantum row target, independent of durable objective identity.
    max_rows: u32 = 2048,
    pub fn jsonStringify(self: Request, writer: anytype) !void {
        try stringify(self, writer);
    }
    pub fn sameObjective(self: Request, other: Request) bool {
        if (self.table_id != other.table_id or self.schema_version != other.schema_version or self.operation != other.operation or self.full != other.full or self.analyze != other.analyze or self.columns.len != other.columns.len) return false;
        for (self.columns, other.columns) |left, right| if (!std.mem.eql(u8, left, right)) return false;
        if ((self.cluster_index == null) != (other.cluster_index == null)) return false;
        if (self.cluster_index) |name| if (!std.mem.eql(u8, name, other.cluster_index.?)) return false;
        if (!std.meta.eql(self.cluster_admission, other.cluster_admission)) return false;
        return true;
    }
    pub fn validate(self: Request) !void {
        if (self.max_rows == 0 or self.max_rows > 2048) return error.InvalidSqlParameters;
        if (self.table_id == 0 or std.mem.allEqual(u8, &self.request_id, 0) or self.columns.len > 256) return error.InvalidSqlParameters;
        if (self.operation != .vacuum and (self.full or self.analyze)) return error.InvalidSqlParameters;
        if (self.cluster_index) |name| {
            if ((self.operation != .cluster and self.operation != .cluster_check) or name.len == 0 or name.len > 256 or !std.unicode.utf8ValidateSlice(name) or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidSqlParameters;
        }
        if (self.operation == .cluster and self.columns.len != 0) return error.InvalidSqlParameters;
        if (self.operation == .cluster) {
            const admission = self.cluster_admission orelse return error.InvalidSqlParameters;
            try admission.validate();
            if (admission.purpose != .execution or admission.binding.identity.table_id != self.table_id) return error.InvalidSqlParameters;
        } else if (self.cluster_admission != null) return error.InvalidSqlParameters;
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
    phase: enum { vacuum_start, vacuum, analyze, cluster, cluster_flush, done },
    lease_until_ms: u64 = 0,
    revision: u64 = 0,
    after: ?[]const u8 = null,
    statistics: Statistics,
    cluster: ?Cluster = null,
    pub fn jsonStringify(self: Progress, writer: anytype) !void {
        try stringify(self, writer);
    }
};
pub const cluster_preferred_key = "\x00\x00__metadata__:sql_cluster_preferred:v1";
pub const cluster_cursor_key = "\x00\x00__metadata__:sql_cluster_cursor:v1";
pub const Cluster = struct {
    index_name: []const u8,
    index_generation: u64,
    index_slot: u32,
    catalog_head: [84]u8,
    generation: [16]u8,
    next_ordinal: u64 = 0,
    /// Raw cursor bytes share the progress transaction. Keeping only this
    /// binding in JSON supports the full admitted 1 MiB ordered-key limit
    /// without sixfold JSON escaping or a second key-size admission contract.
    cursor_digest: ?[32]u8 = null,
    pub fn cursorDigest(self: Cluster, bytes: []const u8) ![32]u8 {
        try @import("../storage/db/relational/indexes/limits.zig").admit(bytes.len);
        if (bytes.len == 0) return error.InvalidSqlBackendResponse;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-cluster-cursor/v1");
        hash.update(&self.generation);
        hash.update(&self.catalog_head);
        hash.update(bytes);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
};
pub const Response = struct {
    table_id: u64,
    schema_version: u32,
    done: bool,
    available: bool = false,
    rows_scanned: u64 = 0,
    statistics: ?Statistics = null,
    row_heap_format: ?u8 = null,
    pub fn jsonStringify(self: Response, writer: anytype) !void {
        try stringify(self, writer);
    }
};

test "row heap maintenance extensions preserve deployed packets and resumable fingerprints" {
    const a = std.testing.allocator;
    const original = "{\"table_id\":5,\"schema_version\":1,\"request_id\":\"1111111111111111\",\"operation\":\"analyze\",\"full\":false,\"analyze\":false,\"columns\":[],\"max_rows\":2048}";
    var parsed = try std.json.parseFromSlice(Request, a, original, .{});
    defer parsed.deinit();
    const encoded = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
    defer a.free(encoded);
    try std.testing.expectEqualStrings(original, encoded);
    var expected: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(original, &expected, .{});
    try std.testing.expectEqualSlices(u8, &expected, &(try parsed.value.fingerprint(a)));
    const old_response = "{\"table_id\":5,\"schema_version\":1,\"done\":true,\"available\":false,\"rows_scanned\":0,\"statistics\":null}";
    const response = try std.json.Stringify.valueAlloc(a, Response{ .table_id = 5, .schema_version = 1, .done = true }, .{});
    defer a.free(response);
    try std.testing.expectEqualStrings(old_response, response);
    const progress = try std.json.Stringify.valueAlloc(a, Progress{ .request = parsed.value, .identity = .{ .table_id = 5 }, .fingerprint = expected, .phase = .analyze, .statistics = .{ .identity = .{ .table_id = 5 }, .schema_version = 1 } }, .{});
    defer a.free(progress);
    try std.testing.expect(std.mem.indexOf(u8, progress, "\"cluster\"") == null);
    var check = parsed.value;
    check.operation = .cluster_check;
    check.cluster_index = "by_status";
    const extension = try std.json.Stringify.valueAlloc(a, check, .{});
    defer a.free(extension);
    try std.testing.expect(std.mem.indexOf(u8, extension, "cluster_index") != null);
    try std.testing.expect(std.mem.indexOf(u8, extension, "cluster_admission") == null);
    try check.validate();
    check.operation = .cluster;
    try std.testing.expectError(error.InvalidSqlParameters, check.validate());
}

test "row heap raw cursor binds the full admitted key to its rewrite and index image" {
    const a = std.testing.allocator;
    const maximum = @import("../storage/db/relational/indexes/limits.zig").max_cursor_key_bytes;
    const bytes = try a.alloc(u8, maximum + 1);
    defer a.free(bytes);
    @memset(bytes, 0);
    const binding: Cluster = .{ .index_name = "by_status", .index_generation = 7, .index_slot = 0, .catalog_head = @splat(1), .generation = @splat(2) };
    const digest = try binding.cursorDigest(bytes[0..maximum]);
    var successor = binding;
    successor.generation[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &digest, &(try successor.cursorDigest(bytes[0..maximum]))));
    successor = binding;
    successor.catalog_head[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &digest, &(try successor.cursorDigest(bytes[0..maximum]))));
    bytes[maximum - 1] = 1;
    try std.testing.expect(!std.mem.eql(u8, &digest, &(try binding.cursorDigest(bytes[0..maximum]))));
    try std.testing.expectError(error.RelationalIndexKeyTooLarge, binding.cursorDigest(bytes));
    try std.testing.expectError(error.InvalidSqlBackendResponse, binding.cursorDigest(&.{}));
}

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
