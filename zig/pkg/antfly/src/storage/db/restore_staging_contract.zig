// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Data-only relational lifecycle contract. Physical execution stays in its owner.
const std = @import("std");
const Allocator = std.mem.Allocator;
const identity = @import("doc_identity_namespace.zig");

pub const key = "\x00\x00__metadata__:restore_staging_owner";

pub const bootstrap_key = "\x00\x00__metadata__:restore_staging_bootstrap";
/// Authenticated by the HA stream and pinned to the immutable reserved owner.
/// New hidden owners created after a seed can therefore be reconstructed before
/// applying their first lifecycle record, without consulting public placement.
pub const OwnerBootstrap = struct {
    scope: Scope,
    table_name: []const u8,
    schema_json: []const u8,
    read_schema_json: []const u8 = "",
    indexes_json: []const u8,
    byte_range: @import("../byte_range.zig").ByteRange,

    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try jw.beginObject();
        try jw.objectField("scope");
        try @import("relational_integrity_json.zig").write(self.scope, jw);
        try jw.objectField("table_name");
        try jw.write(self.table_name);
        try jw.objectField("schema_json");
        try jw.write(self.schema_json);
        try jw.objectField("read_schema_json");
        try jw.write(self.read_schema_json);
        try jw.objectField("indexes_json");
        try jw.write(self.indexes_json);
        try jw.objectField("byte_range");
        try @import("relational_integrity_json.zig").write(self.byte_range, jw);
        try jw.endObject();
    }
    pub fn validate(self: @This()) !void {
        try self.scope.validate();
        if (self.table_name.len == 0 or self.table_name.len > 255 or std.mem.indexOfAny(u8, self.table_name, "/\\\x00") != null or std.mem.eql(u8, self.table_name, ".") or std.mem.eql(u8, self.table_name, "..") or
            self.schema_json.len +| self.read_schema_json.len > 4 * 1024 * 1024 or self.indexes_json.len == 0 or self.indexes_json.len > 4 * 1024 * 1024 or
            !std.unicode.utf8ValidateSlice(self.table_name) or !std.unicode.utf8ValidateSlice(self.schema_json) or !std.unicode.utf8ValidateSlice(self.read_schema_json) or !std.unicode.utf8ValidateSlice(self.indexes_json) or
            self.byte_range.start.len > 1024 * 1024 or self.byte_range.end.len > 1024 * 1024 or (self.byte_range.end.len != 0 and std.mem.order(u8, self.byte_range.start, self.byte_range.end) != .lt)) return error.InvalidRestoreStagingCommand;
    }
    pub fn encode(self: @This(), alloc: Allocator) ![]u8 {
        try self.validate();
        const body = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(body);
        const encoded = try alloc.alloc(u8, body.len + 36);
        @memcpy(encoded[0..4], "ARB1");
        @memcpy(encoded[4..][0..body.len], body);
        @memcpy(encoded[encoded.len - 32 ..], &digest(encoded[0 .. encoded.len - 32]));
        return encoded;
    }
    pub fn decode(alloc: Allocator, bytes: []const u8) !std.json.Parsed(@This()) {
        if (bytes.len < 36 or bytes.len > 64 * 1024 * 1024 or !std.mem.eql(u8, bytes[0..4], "ARB1") or !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidRestoreStagingRecord;
        var parsed = try std.json.parseFromSlice(@This(), alloc, bytes[4 .. bytes.len - 32], .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        try parsed.value.validate();
        return parsed;
    }
};

pub const Digest = [32]u8;

pub const Phase = enum { reserved, importing, imported, validated, published, canceled };

pub const Timestamp = struct { key: []const u8, timestamp: u64 };

pub const ImportPage = struct { expected: Digest, next: []const u8, scope: Digest, timestamps: []const Timestamp };

pub const Control = union(enum) {
    begin: Scope,
    import_page: ImportPage,
    finish: struct { scope: Digest, phase: Phase },
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};

pub const Scope = struct {
    plan_id: [16]u8,
    plan_digest: Digest,
    source_artifact_digest: Digest,
    source_descriptor_digest: Digest = @splat(0),
    source_namespace: identity.Namespace,
    target_namespace: identity.Namespace,
    target_schema_digest: Digest,

    pub fn validateReservation(self: Scope) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            self.target_namespace.table_id == 0 or self.target_namespace.shard_id == 0 or self.target_namespace.range_id == 0)
            return error.InvalidRestoreStagingCommand;
    }

    pub fn validate(self: Scope) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            std.mem.allEqual(u8, &self.source_artifact_digest, 0) or self.target_namespace.table_id == 0 or
            self.target_namespace.shard_id == 0 or self.target_namespace.range_id == 0 or
            self.source_namespace.table_id == 0 or self.source_namespace.table_id == self.target_namespace.table_id)
            return error.InvalidRestoreStagingCommand;
    }
    pub fn digest(self: Scope) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-restore-owner-scope-v1");
        hash.update(&self.plan_id);
        hash.update(&self.plan_digest);
        hash.update(&self.source_artifact_digest);
        hash.update(&self.source_descriptor_digest);
        inline for (.{ self.source_namespace, self.target_namespace }) |namespace| {
            inline for (.{ namespace.table_id, namespace.shard_id, namespace.range_id }) |value| {
                var bytes: [8]u8 = undefined;
                std.mem.writeInt(u64, &bytes, value, .little);
                hash.update(&bytes);
            }
        }
        hash.update(&self.target_schema_digest);
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub const Progress = struct {
    scope: Scope,
    phase: Phase = .importing,
    rows: u64 = 0,
    cursor: []const u8 = "",
    logical_digest: Digest = @splat(0),
    pub fn jsonStringify(self: @This(), jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
    pub fn encode(self: Progress, alloc: Allocator) ![]u8 {
        if (self.phase == .reserved or (self.phase == .canceled and self.scope.source_namespace.table_id == 0 and self.rows == 0 and self.cursor.len == 0)) try self.scope.validateReservation() else try self.scope.validate();
        if (self.cursor.len > 1024 * 1024) return error.InvalidRestoreStagingCommand;
        const body = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(body);
        const out = try alloc.alloc(u8, body.len + 36);
        @memcpy(out[0..4], "ARS1");
        @memcpy(out[4..][0..body.len], body);
        @memcpy(out[out.len - 32 ..], &digest(out[0 .. out.len - 32]));
        return out;
    }
    pub fn decode(alloc: Allocator, bytes: []const u8) !std.json.Parsed(Progress) {
        if (bytes.len < 36 or bytes.len > 8 * 1024 * 1024 or !std.mem.eql(u8, bytes[0..4], "ARS1") or
            !std.mem.eql(u8, bytes[bytes.len - 32 ..], &digest(bytes[0 .. bytes.len - 32]))) return error.InvalidRestoreStagingRecord;
        var parsed = try std.json.parseFromSlice(Progress, alloc, bytes[4 .. bytes.len - 32], .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        if (parsed.value.phase == .reserved or (parsed.value.phase == .canceled and parsed.value.scope.source_namespace.table_id == 0 and parsed.value.rows == 0 and parsed.value.cursor.len == 0)) {
            parsed.value.scope.validateReservation() catch return error.InvalidRestoreStagingRecord;
        } else parsed.value.scope.validate() catch return error.InvalidRestoreStagingRecord;
        if (parsed.value.cursor.len > 1024 * 1024) return error.InvalidRestoreStagingRecord;
        return parsed;
    }
    pub fn receipt(self: Progress) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly-restore-owner-receipt-v1");
        hash.update(&self.scope.digest());
        hash.update(@tagName(self.phase));
        hash.update(&self.logical_digest);
        var count: [8]u8 = undefined;
        std.mem.writeInt(u64, &count, self.rows, .little);
        hash.update(&count);
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
};

pub fn digest(bytes: []const u8) Digest {
    var out: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &out, .{});
    return out;
}
