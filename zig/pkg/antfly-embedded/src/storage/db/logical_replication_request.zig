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

//! The exact immutable logical intent supplied to physical PREPARE. This hash
//! is an additional body binding, NEVER P, actor, trigger or source authority.
//! Fresh admission/proof/timestamp fields remain independently verified by the
//! existing receiver. Their minting can depend on the P owner, so including
//! them would create an admission-digest cycle rather than stronger authority.
const std = @import("std");
const types = @import("types.zig");
pub fn digest(a: std.mem.Allocator, transaction_id: types.TxnId, request: types.TransactionIntentRequest) ![32]u8 {
    const logical = pregrantIntent(request);
    // Hash typed fields with explicit lengths. Arbitrary byte keys/payloads
    // remain byte exact; no JSON Unicode repair/canonicalization is involved.
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/logical-capture-request/v1\x00");
    hash.update(&transaction_id);
    try hashValue(a, &hash, logical);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

/// Canonical PRE-GRANT member body, NOT the final network JSON packet. The
/// shared producer and Data receiver both construct these exact bytes from the
/// typed immutable intent before fresh owner/proof/time materialization. Scope
/// identity is included here; normalized request digest remains a separate
/// transport-independent lossless semantic check. Full P.plan_digest is intact.
pub const MemberScope = struct {
    namespace: @import("doc_identity_namespace.zig").Namespace,
    owner_group_id: u64,
    transaction_id: types.TxnId,
    member: u16,
    topology_epoch: u64,
};
pub fn pregrantBodyDigest(a: std.mem.Allocator, scope: MemberScope, request: types.TransactionIntentRequest) ![32]u8 {
    try validateScope(scope);
    const logical = pregrantIntent(request);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/logical-capture-pregrant-member-body/v1\x00");
    try hashValue(a, &hash, .{ .member = scope, .intent = logical });
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn validateScope(scope: MemberScope) !void {
    if (scope.namespace.table_id == 0 or scope.namespace.shard_id == 0 or scope.namespace.range_id == 0 or scope.owner_group_id == 0 or scope.topology_epoch == 0 or
        scope.member >= @import("../../system_catalog/logical_replication_publisher.zig").max_capture_participants or std.mem.allEqual(u8, &scope.transaction_id, 0)) return error.InvalidLogicalCaptureControl;
}

/// Both pregrant witnesses share the same typed intent. Feed it to both hash
/// states in one pass so validation parses and sorts each JSON row only once.
pub fn digests(a: std.mem.Allocator, scope: MemberScope, request: types.TransactionIntentRequest) !struct { body: [32]u8, request: [32]u8 } {
    try validateScope(scope);
    const Pair = struct {
        body: std.crypto.hash.Blake3 = .init(.{}),
        request: std.crypto.hash.Blake3 = .init(.{}),

        fn update(self: *@This(), bytes: []const u8) void {
            self.body.update(bytes);
            self.request.update(bytes);
        }
    };
    var pair: Pair = .{};
    pair.body.update("antfly/logical-capture-pregrant-member-body/v1\x00");
    length(&pair.body, "member".len);
    pair.body.update("member");
    try hashValue(a, &pair.body, scope);
    length(&pair.body, "intent".len);
    pair.body.update("intent");
    pair.request.update("antfly/logical-capture-request/v1\x00");
    pair.request.update(&scope.transaction_id);
    try hashValue(a, &pair, pregrantIntent(request));
    var body: [32]u8 = undefined;
    var normalized: [32]u8 = undefined;
    pair.body.final(&body);
    pair.request.final(&normalized);
    return .{ .body = body, .request = normalized };
}

/// These fields are separately checked by the actual P owner/fresh grant
/// receiver, and can be authored only after admission. They are never stripped
/// from execution or the durable command; only this pre-grant projection.
fn pregrantIntent(request: types.TransactionIntentRequest) types.TransactionIntentRequest {
    var logical = request;
    logical.publication_capture = null; // independently checked post-admission P grant, never a circular input
    logical.external_decision_owner = null;
    logical.external_activation = false;
    logical.timestamp_ns = 0;
    logical.trigger_preparation_proof = "";
    logical.trigger_preparation_group_id = 0;
    logical.trigger_preparation_command_digest = null;
    logical.trigger_preparation_admitted_at_seconds = 0;
    logical.row_policy_admitted_at_seconds = 0;
    return logical;
}

fn length(hash: anytype, count: usize) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, count, .little);
    hash.update(&bytes);
}
fn hashValue(a: std.mem.Allocator, hash: anytype, value: anytype) anyerror!void {
    const T = @TypeOf(value);
    if (comptime T == std.json.Value) return hashJson(a, hash, value);
    switch (@typeInfo(T)) {
        .bool => hash.update(&.{@intFromBool(value)}),
        .int => |info| {
            const U = @Int(.unsigned, @sizeOf(T) * 8);
            var bytes: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(U, &bytes, @as(@Int(.unsigned, info.bits), @bitCast(value)), .little);
            hash.update(&bytes);
        },
        .float => {
            const U = @Int(.unsigned, @sizeOf(T) * 8);
            var bytes: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(U, &bytes, @as(@Int(.unsigned, @bitSizeOf(T)), @bitCast(value)), .little);
            hash.update(&bytes);
        },
        .@"enum" => {
            const tag = @tagName(value);
            length(hash, tag.len);
            hash.update(tag);
        },
        .optional => if (value) |present| {
            hash.update(&.{1});
            try hashValue(a, hash, present);
        } else hash.update(&.{0}),
        .array => |info| {
            length(hash, value.len);
            if (info.child == u8) hash.update(&value) else for (value) |item| try hashValue(a, hash, item);
        },
        .pointer => |info| switch (info.size) {
            .slice => {
                length(hash, value.len);
                if (info.child == u8) hash.update(value) else for (value) |item| try hashValue(a, hash, item);
            },
            .one => try hashValue(a, hash, value.*),
            else => @compileError("logical capture intent needs a bounded pointer shape: " ++ @typeName(T)),
        },
        .@"struct" => inline for (@typeInfo(T).@"struct".field_names) |field| {
            if (comptime T == types.TransactionIntentRequest and std.mem.eql(u8, field, "replication_source_permit")) continue;
            length(hash, field.len);
            hash.update(field);
            if (comptime T == types.TransactionWrite and std.mem.eql(u8, field, "value")) {
                // Native owner JSON transport parses documents losslessly and
                // may remove whitespace/escapes. Bind their exact logical JSON
                // values while preserving number lexemes and binary row keys.
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const parsed = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), value.value, .{ .allocate = .alloc_always, .parse_numbers = false, .duplicate_field_behavior = .use_last });
                try hashJson(arena.allocator(), hash, parsed);
            } else try hashValue(a, hash, @field(value, field));
        },
        .@"union" => {
            try hashValue(a, hash, std.meta.activeTag(value));
            switch (value) {
                inline else => |payload| try hashValue(a, hash, payload),
            }
        },
        .void => {},
        else => @compileError("logical capture intent needs an explicit semantic hash for " ++ @typeName(T)),
    }
}

fn hashJson(a: std.mem.Allocator, hash: anytype, value: std.json.Value) anyerror!void {
    if (value == .integer or value == .float or value == .number_string) {
        if (value == .float and !std.math.isFinite(value.float)) return error.InvalidLogicalCaptureControl;
        const token = if (value == .number_string) value.number_string else try std.json.Stringify.valueAlloc(a, value, .{});
        defer if (value != .number_string) a.free(token);
        try hashValue(a, hash, @as(std.meta.Tag(std.json.Value), .number_string));
        length(hash, token.len);
        hash.update(token);
        return;
    }
    try hashValue(a, hash, std.meta.activeTag(value));
    switch (value) {
        .null => {},
        .bool => |item| try hashValue(a, hash, item),
        .integer => |item| try hashValue(a, hash, item),
        .float => |item| try hashValue(a, hash, item),
        .number_string, .string => |item| {
            length(hash, item.len);
            hash.update(item);
        },
        .array => |items| {
            length(hash, items.items.len);
            for (items.items) |item| try hashJson(a, hash, item);
        },
        .object => |object| {
            const keys = try a.dupe([]const u8, object.keys());
            defer a.free(keys);
            std.mem.sort([]const u8, keys, {}, struct {
                fn less(_: void, left: []const u8, right: []const u8) bool {
                    return std.mem.lessThan(u8, left, right);
                }
            }.less);
            length(hash, keys.len);
            for (keys) |key| {
                length(hash, key.len);
                hash.update(key);
                try hashJson(a, hash, object.get(key).?);
            }
        },
    }
}

test "logical publication request digest binds actual null intent predicates and all effect fields" {
    const a = std.testing.allocator;
    const id: types.TxnId = @splat(1);
    const base: types.TransactionIntentRequest = .{ .writes = &.{.{ .key = "a", .value = "{\"payload\":null}", .json_null_fields = &.{"payload"} }} };
    const expected = try digest(a, id, base);
    var changed = base;
    changed.writes = &.{.{ .key = "a", .value = "{  \"payload\" : null }", .json_null_fields = &.{"payload"} }};
    try std.testing.expectEqual(expected, try digest(a, id, changed));
    changed = base;
    changed.writes = &.{.{ .key = "a", .value = "{\"payload\":null}" }};
    try std.testing.expect(!std.mem.eql(u8, &expected, &try digest(a, id, changed)));
    changed = base;
    changed.predicates = &.{.{ .key = "a", .expected_version = 3, .expected_content_digest = @splat(2) }};
    try std.testing.expect(!std.mem.eql(u8, &expected, &try digest(a, id, changed)));
    changed = base;
    changed.normalized_graph_rows = true;
    try std.testing.expect(!std.mem.eql(u8, &expected, &try digest(a, id, changed)));
    changed = base;
    changed.writes = &.{.{ .key = "\xff", .value = "{}" }};
    const binary = try digest(a, id, changed);
    changed.writes = &.{.{ .key = "\xfe", .value = "{}" }};
    try std.testing.expect(!std.mem.eql(u8, &binary, &try digest(a, id, changed)));
    changed = base;
    changed.timestamp_ns = 999;
    changed.trigger_preparation_proof = "transport-signature";
    try std.testing.expectEqual(expected, try digest(a, id, changed));
    try std.testing.expect(!std.mem.eql(u8, &expected, &try digest(a, @splat(2), base)));
}

test "logical publication pregrant member binds scope and typed effects without a P proof cycle" {
    const a = std.testing.allocator;
    const scope: MemberScope = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .owner_group_id = 4, .transaction_id = @splat(5), .member = 0, .topology_epoch = 6 };
    const base: types.TransactionIntentRequest = .{ .schema_version = 3, .relational_schema_version = 3, .writes = &.{.{ .key = "\xff-key", .value = "{\"v\":1.00,\"j\":null}", .json_null_fields = &.{"j"} }} };
    const expected = try pregrantBodyDigest(a, scope, base);
    var changed_scope = scope;
    changed_scope.namespace.range_id += 1;
    try std.testing.expect(!std.mem.eql(u8, &expected, &try pregrantBodyDigest(a, changed_scope, base)));
    changed_scope = scope;
    changed_scope.owner_group_id += 1;
    try std.testing.expect(!std.mem.eql(u8, &expected, &try pregrantBodyDigest(a, changed_scope, base)));
    changed_scope = scope;
    changed_scope.member += 1;
    try std.testing.expect(!std.mem.eql(u8, &expected, &try pregrantBodyDigest(a, changed_scope, base)));
    changed_scope = scope;
    changed_scope.topology_epoch += 1;
    try std.testing.expect(!std.mem.eql(u8, &expected, &try pregrantBodyDigest(a, changed_scope, base)));
    var changed = base;
    changed.writes = &.{.{ .key = "\xfe-key", .value = base.writes[0].value, .json_null_fields = &.{"j"} }};
    try std.testing.expect(!std.mem.eql(u8, &expected, &try pregrantBodyDigest(a, scope, changed)));
    changed = base;
    changed.writes = &.{.{ .key = base.writes[0].key, .value = "{\"v\":1.0,\"j\":null}", .json_null_fields = &.{"j"} }};
    try std.testing.expect(!std.mem.eql(u8, &expected, &try pregrantBodyDigest(a, scope, changed)));
    changed = base;
    changed.writes = &.{.{ .key = base.writes[0].key, .value = base.writes[0].value }};
    try std.testing.expect(!std.mem.eql(u8, &expected, &try pregrantBodyDigest(a, scope, changed)));
    changed = base;
    changed.normalized_graph_rows = true;
    try std.testing.expect(!std.mem.eql(u8, &expected, &try pregrantBodyDigest(a, scope, changed)));
    changed = base;
    changed.predicates = &.{.{ .key = base.writes[0].key, .expected_version = 3, .expected_content_digest = @splat(4) }};
    try std.testing.expect(!std.mem.eql(u8, &expected, &try pregrantBodyDigest(a, scope, changed)));
    changed = base;
    changed.schema_version = 4;
    try std.testing.expect(!std.mem.eql(u8, &expected, &try pregrantBodyDigest(a, scope, changed)));
    changed = base;
    changed.row_policy_database = "different-database";
    try std.testing.expect(!std.mem.eql(u8, &expected, &try pregrantBodyDigest(a, scope, changed)));
    // This is only digest projection. Real execution retains/independently
    // validates the actual P owner, minter command binding and native time.
    changed = base;
    changed.external_decision_owner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(7), .admission_digest = @splat(8), .expires_at_unix_ns = 100 };
    changed.trigger_preparation_proof = "new-real-transport-proof";
    changed.trigger_preparation_group_id = scope.owner_group_id;
    changed.trigger_preparation_command_digest = @splat(9);
    changed.trigger_preparation_admitted_at_seconds = 99;
    changed.timestamp_ns = 100;
    try std.testing.expectEqual(expected, try pregrantBodyDigest(a, scope, changed));
}
