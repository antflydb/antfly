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

const std = @import("std");
const db_mod = @import("../storage/db/selected_root.zig").db;
const ant_json = @import("antfly-json");
const document_mapper = @import("../storage/db/document_mapper.zig");
const public_limits = @import("public_limits.zig");
const merge_pages = @import("../storage/db/merge_page_contract.zig");
const MergePageEffects = struct { writes: []db_mod.types.BatchWrite, deletes: [][]const u8 };

pub const BatchFailure = struct {
    code: []const u8,
    message: []const u8,
    reason: ?[]const u8 = null,
    retryable: bool,
};

pub const BatchResult = struct {
    status: []const u8 = "committed",
    inserted: u32,
    deleted: u32,
    transformed: u32 = 0,
    /// Additive details for a durable commit that needs operator action. Keep
    /// the stable status vocabulary so older strict-enum SDKs can still parse
    /// the committed response and safely avoid replaying the mutation.
    failure: ?BatchFailure = null,
};

test "index maintenance internal batch codec owns binary proof and rejects public bypass" {
    const alloc = std.testing.allocator;
    const command = @import("../storage/db/relational_index_maintenance_contract.zig").Command{
        .action = .retry,
        .table_id = 7,
        .owner_group_id = 11,
        .schema_version = 2,
        .index_name = "by_id",
        .generation = 8,
        .slot = 1,
        .owner = @splat(0xff),
        .comparison = @splat(0x80),
        .expected_progress_digest = @splat(0xfe),
        .expected_maintenance_epoch = 3,
        .routing_key = "\x00\xff",
    };
    const request = db_mod.types.BatchRequest{
        .transaction = .{ .prepare = .{ .txn_id = @splat(1), .topology_epoch = 9 } },
        .relational_schema_version = 2,
        .relational_index_maintenance = command,
    };
    const bytes = try encodeBatchRequest(alloc, request);
    defer alloc.free(bytes);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, bytes));
    var decoded = try parseInternalBatchRequest(alloc, bytes);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualDeep(command, decoded.req.relational_index_maintenance.?);
    var zero = request;
    zero.relational_schema_version = 0;
    zero.relational_index_maintenance.?.schema_version = 0;
    const zero_bytes = try encodeBatchRequest(alloc, zero);
    defer alloc.free(zero_bytes);
    var zero_parsed = try parseInternalBatchRequest(alloc, zero_bytes);
    defer zero_parsed.deinit(alloc);
    try std.testing.expectEqual(@as(?u32, 0), zero_parsed.req.relational_schema_version);
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, "{\"_relational_schema_version\":4294967296}"));
    var invalid = request;
    invalid.transaction = null;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, invalid));
    invalid = request;
    invalid.writes = &.{.{ .key = "row", .value = "{}" }};
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, invalid));
}

test "merge page internal codec preserves certified bytes and requires fail closed discriminants" {
    const alloc = std.testing.allocator;
    const namespace: db_mod.DocIdentityNamespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 };
    const source: merge_pages.Source = .{ .namespace = namespace, .pin_digest = @splat(255), .applied_index = 10 };
    var request: db_mod.types.BatchRequest = .{
        .writes = &.{.{ .key = "\x00\xff", .value = "{ \"x\" : 9007199254740993 }" }},
        .merge_replication = .{
            .transition_id = 1,
            .donor_group_id = 8,
            .receiver_group_id = 9,
            .identity_namespace = namespace,
            .copy_attempt = .{ .donor_term = 2, .sequence = 3 },
        },
        .merge_page = .{
            .source = source,
            .sequence = 2,
            .phase = .rows,
            .next = "\x00\xff",
            .timestamps = &.{123},
            .exhausted = true,
            .digest = @splat(0),
        },
    };
    request.merge_page.?.digest = merge_pages.commandDigest(request);
    const bytes = try encodeBatchRequest(alloc, request);
    defer alloc.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "\"kind\":\"page_v1\"") != null);
    try std.testing.expect(std.meta.stringToEnum(db_mod.types.MergeReplicationCheckpoint.Kind, "page_v1") == null);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, bytes));
    var decoded = try parseInternalBatchRequest(alloc, bytes);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualDeep(request.merge_page.?, decoded.req.merge_page.?);
    try std.testing.expectEqualDeep(request.writes, decoded.req.writes);
    try std.testing.expect(decoded.req.merge_checkpoint == null);
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, "{\"_merge_checkpoint\":{\"kind\":\"page_v1\"}}"));
    // Blob-like logical JSON must not become millions of numeric JSON nodes.
    // Bound wire growth as well as retaining exact original row bytes.
    const large_value = try alloc.alloc(u8, 4 * 1024 * 1024);
    defer alloc.free(large_value);
    @memset(large_value, 'A');
    @memcpy(large_value[0..9], "{\"blob\":\"");
    @memcpy(large_value[large_value.len - 2 ..], "\"}");
    var large_request = request;
    large_request.writes = &.{.{ .key = request.writes[0].key, .value = large_value }};
    large_request.merge_page.?.digest = merge_pages.commandDigest(large_request);
    const large_encoded = try encodeBatchRequest(alloc, large_request);
    defer alloc.free(large_encoded);
    try std.testing.expect(large_encoded.len < large_value.len + 4096);
    var large_decoded = try parseInternalBatchRequest(alloc, large_encoded);
    defer large_decoded.deinit(alloc);
    try std.testing.expectEqualStrings(large_value, large_decoded.req.writes[0].value);

    const chunker = try merge_pages.RowChunks(db_mod.types.BatchRequest).init(large_request);
    const chunk_request = try chunker.requestAt(0);
    const chunk_encoded = try encodeBatchRequest(alloc, chunk_request);
    defer alloc.free(chunk_encoded);
    try std.testing.expect(chunk_encoded.len < merge_pages.max_chunk_bytes * 3 / 2);
    try std.testing.expect(std.mem.indexOf(u8, chunk_encoded, "data_base64") != null);
    const chunk_marker = std.mem.indexOf(u8, chunk_encoded, "page_v3") orelse return error.TestUnexpectedResult;
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, chunk_encoded));
    var chunk_decoded = try parseInternalBatchRequest(alloc, chunk_encoded);
    defer chunk_decoded.deinit(alloc);
    try std.testing.expectEqualDeep(chunk_request.merge_page.?, chunk_decoded.req.merge_page.?);
    try std.testing.expectEqual(@as(usize, 0), chunk_decoded.req.writes.len);
    chunk_encoded[chunk_marker + "page_v".len] = '2';
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, chunk_encoded));
    chunk_encoded[chunk_marker + "page_v".len] = '3';
    const payload_marker = std.mem.indexOf(u8, chunk_encoded, "\"data_base64\":\"").?;
    chunk_encoded[payload_marker + "\"data_base64\":\"".len] = '!';
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, chunk_encoded));

    const checkpoint: db_mod.types.BatchRequest = .{ .merge_checkpoint = .{
        .kind = .begin_copy,
        .transition_id = 1,
        .donor_group_id = 8,
        .receiver_group_id = 9,
        .receiver_base_start = "a",
        .receiver_base_end = "m",
        .merged_start = "a",
        .merged_end = "z",
        .copy_attempt = .{ .donor_term = 2, .sequence = 3 },
        .page_source = source,
        .page_receiver_namespace = namespace,
    } };
    const checkpoint_bytes = try encodeBatchRequest(alloc, checkpoint);
    defer alloc.free(checkpoint_bytes);
    try std.testing.expect(std.mem.indexOf(u8, checkpoint_bytes, "page_v1_begin_copy") != null);
    var parsed = try parseInternalBatchRequest(alloc, checkpoint_bytes);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualDeep(checkpoint.merge_checkpoint.?, parsed.req.merge_checkpoint.?);
    var invalid = checkpoint;
    invalid.merge_checkpoint.?.page_receiver_namespace = null;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, invalid));
}

test "online source internal codec rejects public controls and preserves exact scope" {
    const alloc = std.testing.allocator;
    const scope: @import("../storage/db/online_source_contract.zig").Scope = .{
        .fence = .{
            .transition_id = 9,
            .attempt = 2,
            .owner_group_id = 8,
            .peer_group_id = 10,
            .role = .merge_source,
            .namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 },
            .catalog_digest = @splat(255),
        },
        .receiver_namespace = .{ .table_id = 7, .shard_id = 10, .range_id = 11 },
        .consumer_epoch = 9007199254740993,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    };
    const command = @import("../storage/db/online_source_contract.zig").Command{ .admit = .{ .scope = scope } };
    const bytes = try encodeBatchRequest(alloc, .{ .online_source = command });
    defer alloc.free(bytes);
    try std.testing.expect(std.mem.indexOf(u8, bytes, "online_source_v4") != null);
    try std.testing.expect(std.meta.stringToEnum(db_mod.types.MergeReplicationCheckpoint.Kind, "online_source_v4") == null);
    const legacy_bytes = try alloc.dupe(u8, bytes);
    defer alloc.free(legacy_bytes);
    const source_marker = std.mem.indexOf(u8, legacy_bytes, "online_source_v4").?;
    for ([_]u8{ '1', '2', '3' }) |version| {
        legacy_bytes[source_marker + "online_source_v".len] = version;
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, legacy_bytes));
    }
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, bytes));
    var decoded = try parseInternalBatchRequest(alloc, bytes);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualDeep(command, decoded.req.online_source.?);
    try std.testing.expect(decoded.req.merge_checkpoint == null);
    const restore = try encodeBatchRequest(alloc, .{ .restore_staging = .{ .finish = .{ .scope = @splat(3), .phase = .validated } } });
    defer alloc.free(restore);
    var restore_parsed = try parseInternalBatchRequest(alloc, restore);
    defer restore_parsed.deinit(alloc);
    try std.testing.expectEqual(.validated, restore_parsed.req.restore_staging.?.finish.phase);
    const restore_marker = std.mem.indexOf(u8, restore, "restore_v3").?;
    for ([_]u8{ '1', '2' }) |version| {
        restore[restore_marker + "restore_v".len] = version;
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, restore));
    }
    try std.testing.expectError(error.InvalidOnlineSourceCommand, encodeBatchRequest(alloc, .{
        .online_source = command,
        .writes = &.{.{ .key = "row", .value = "{}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, "{\"_merge_checkpoint\":{\"kind\":\"online_source_v1\"}}"));
}

test "merge page tail binding cannot be downgraded to snapshot only wire" {
    const alloc = std.testing.allocator;
    var request: db_mod.types.BatchRequest = .{
        .merge_replication = .{
            .transition_id = 1,
            .donor_group_id = 8,
            .receiver_group_id = 10,
            .identity_namespace = .{ .table_id = 7, .shard_id = 10, .range_id = 11 },
            .copy_attempt = .{ .donor_term = 2, .sequence = 3 },
        },
        .merge_page = .{
            .source = .{ .namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 }, .pin_digest = @splat(255), .applied_index = 10, .retention = .{ .epoch = 1, .after_sequence = 5 } },
            .sequence = 1,
            .phase = .cleanup,
            .exhausted = true,
            .digest = @splat(0),
        },
    };
    request.merge_page.?.digest = merge_pages.commandDigest(request);
    const bytes = try encodeBatchRequest(alloc, request);
    defer alloc.free(bytes);
    const marker = std.mem.indexOf(u8, bytes, "page_v6") orelse return error.TestUnexpectedResult;
    var decoded = try parseInternalBatchRequest(alloc, bytes);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualDeep(request.merge_page.?, decoded.req.merge_page.?);
    bytes[marker + 6] = '1';
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, bytes));
    const checkpoint: db_mod.types.BatchRequest = .{ .merge_checkpoint = .{
        .kind = .begin_copy,
        .transition_id = 1,
        .donor_group_id = 8,
        .receiver_group_id = 10,
        .receiver_base_start = "a",
        .receiver_base_end = "m",
        .merged_start = "a",
        .merged_end = "z",
        .copy_attempt = .{ .donor_term = 2, .sequence = 3 },
        .page_source = request.merge_page.?.source,
        .page_receiver_namespace = request.merge_replication.?.identity_namespace,
    } };
    const bootstrap = try encodeBatchRequest(alloc, checkpoint);
    defer alloc.free(bootstrap);
    const bootstrap_marker = std.mem.indexOf(u8, bootstrap, "page_v4_begin_copy") orelse return error.TestUnexpectedResult;
    var parsed_bootstrap = try parseInternalBatchRequest(alloc, bootstrap);
    defer parsed_bootstrap.deinit(alloc);
    try std.testing.expectEqualDeep(checkpoint.merge_checkpoint, parsed_bootstrap.req.merge_checkpoint);
    for ([_]u8{ '1', '2', '3' }) |version| {
        bootstrap[bootstrap_marker + 6] = version;
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, bootstrap));
    }
    // A durable archive locator changes receiver progress semantics. Older
    // private parsers must reject it rather than silently drop that cursor.
    request.merge_page.?.phase = .rows;
    request.merge_page.?.next_snapshot_position = .{ .object = 9, .offset = 9007199254740993, .remaining = 7 };
    request.merge_page.?.digest = merge_pages.commandDigest(request);
    const located = try encodeBatchRequest(alloc, request);
    defer alloc.free(located);
    const locator_marker = std.mem.indexOf(u8, located, "page_v6") orelse return error.TestUnexpectedResult;
    var parsed_locator = try parseInternalBatchRequest(alloc, located);
    defer parsed_locator.deinit(alloc);
    try std.testing.expectEqualDeep(request.merge_page, parsed_locator.req.merge_page);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, located));
    for ([_]u8{ '1', '2', '3', '4', '5' }) |version| {
        located[locator_marker + 6] = version;
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, located));
    }
}

pub const OwnedBatchRequest = struct {
    writes: []db_mod.types.BatchWrite = &.{},
    deletes: [][]const u8 = &.{},
    transforms: []db_mod.types.DocumentTransform = &.{},
    graph_writes: []db_mod.types.GraphEdgeWrite = &.{},
    graph_deletes: []db_mod.types.GraphEdgeDelete = &.{},
    predicates: []db_mod.types.TransactionVersionPredicate = &.{},
    integrity: []const db_mod.types.TransactionIntegrityOperation = &.{},
    integrity_commands: ?std.json.Parsed([]const @import("../storage/db/relational_integrity_contract.zig").Command) = null,
    relational_activation: ?std.json.Parsed(@import("../storage/db/relational_integrity_activation_contract.zig").Command) = null,
    relational_retirement: ?std.json.Parsed(@import("../storage/db/relational_integrity_retirement_contract.zig").Command) = null,
    relational_index_maintenance: ?std.json.Parsed(@import("../storage/db/relational_index_maintenance_contract.zig").Command) = null,
    relational_topology: ?std.json.Parsed(@import("../storage/db/relational_integrity_topology_contract.zig").Command) = null,
    restore_staging: ?std.json.Parsed(@import("../storage/db/restore_staging_contract.zig").Control) = null,
    online_source: ?std.json.Parsed(@import("../storage/db/online_source_contract.zig").Command) = null,
    transaction_participants: [][]const u8 = &.{},
    split_checkpoint_range_start: ?[]u8 = null,
    split_checkpoint_range_end: ?[]u8 = null,
    split_transition_key: ?[]u8 = null,
    merge_receiver_base_start: ?[]u8 = null,
    merge_receiver_base_end: ?[]u8 = null,
    merge_range_start: ?[]u8 = null,
    merge_range_end: ?[]u8 = null,
    merge_artifacts: ?std.json.Parsed([]const db_mod.types.BatchWrite) = null,
    merge_page: ?std.json.Parsed(merge_pages.Command) = null,
    merge_page_effects: ?std.json.Parsed(MergePageEffects) = null,
    req: db_mod.types.BatchRequest = .{},

    pub fn deinit(self: *OwnedBatchRequest, alloc: std.mem.Allocator) void {
        @import("relational_integrity_wire.zig").free(alloc, self.integrity);
        if (self.integrity_commands) |*commands| commands.deinit();
        if (self.relational_activation) |*activation| activation.deinit();
        if (self.relational_retirement) |*retirement| retirement.deinit();
        if (self.relational_index_maintenance) |*retirement| retirement.deinit();
        if (self.relational_topology) |*topology| topology.deinit();
        if (self.restore_staging) |*control| control.deinit();
        if (self.online_source) |*control| control.deinit();
        if (self.merge_page_effects) |*effects| {
            effects.deinit();
        } else {
            for (self.writes) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
            }
            if (self.writes.len > 0) alloc.free(self.writes);
            for (self.deletes) |key| alloc.free(key);
            if (self.deletes.len > 0) alloc.free(self.deletes);
        }
        for (self.transforms) |transform| {
            alloc.free(@constCast(transform.key));
            for (transform.operations) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (transform.operations.len > 0) alloc.free(transform.operations);
        }
        if (self.transforms.len > 0) alloc.free(self.transforms);
        freeGraphWrites(alloc, self.graph_writes);
        freeGraphDeletes(alloc, self.graph_deletes);
        for (self.predicates) |predicate| alloc.free(@constCast(predicate.key));
        if (self.predicates.len > 0) alloc.free(self.predicates);
        for (self.transaction_participants) |participant| alloc.free(participant);
        if (self.transaction_participants.len > 0) alloc.free(self.transaction_participants);
        if (self.split_checkpoint_range_start) |value| alloc.free(value);
        if (self.split_checkpoint_range_end) |value| alloc.free(value);
        if (self.split_transition_key) |value| alloc.free(value);
        if (self.merge_receiver_base_start) |value| alloc.free(value);
        if (self.merge_receiver_base_end) |value| alloc.free(value);
        if (self.merge_range_start) |value| alloc.free(value);
        if (self.merge_range_end) |value| alloc.free(value);
        if (self.merge_artifacts) |*value| value.deinit();
        if (self.merge_page) |*value| value.deinit();
        self.* = undefined;
    }

    pub fn result(self: OwnedBatchRequest) BatchResult {
        return .{
            .inserted = @intCast(self.writes.len),
            .deleted = @intCast(self.deletes.len),
            .transformed = @intCast(self.transforms.len),
        };
    }

    pub fn resultWithStatus(self: OwnedBatchRequest, status: []const u8) BatchResult {
        var result_value = self.result();
        result_value.status = status;
        return result_value;
    }

    pub fn resultWithFailure(self: OwnedBatchRequest, failure: BatchFailure) BatchResult {
        var result_value = self.resultWithStatus("committed_repair_required");
        result_value.failure = failure;
        return result_value;
    }
};

pub fn parseBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedBatchRequest {
    return try parseBatchRequestWithOptions(alloc, body, .{
        .parse_numbers = false,
        .allocate = .alloc_always,
        .max_value_len = public_limits.max_json_value_len,
    }, false);
}

pub fn parseInternalBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedBatchRequest {
    return try parseBatchRequestWithOptions(alloc, body, .{
        // Row/transform numbers belong to the schema, not the transport.
        // Parsing decimal integer spellings as f64 here irreversibly rounds
        // values before the storage owner's exact typed-row preparation.
        .parse_numbers = false,
        .allocate = .alloc_always,
        .max_value_len = public_limits.max_json_value_len,
    }, true);
}

test "distributed txn batch transport preserves exact row and transform number tokens" {
    const alloc = std.testing.allocator;
    const row = "{\"id\":9007199254740993.0,\"min\":-9223372036854775808.0,\"max\":9223372036854775807e0,\"nested\":[1.234567890123456789]}";
    const body = "{\"inserts\":{\"a\":" ++ row ++ "},\"transforms\":[{\"key\":\"b\",\"operations\":[{\"op\":\"$set\",\"path\":\"id\",\"value\":9007199254740993.0}]}]}";
    var public = try parseBatchRequest(alloc, body);
    defer public.deinit(alloc);
    try std.testing.expectEqualStrings(row, public.req.writes[0].value);
    const encoded = try encodeBatchRequest(alloc, public.req);
    defer alloc.free(encoded);
    var internal = try parseInternalBatchRequest(alloc, encoded);
    defer internal.deinit(alloc);
    try std.testing.expectEqualStrings(row, internal.req.writes[0].value);
    try std.testing.expectEqualStrings("9007199254740993.0", internal.req.transforms[0].operations[0].value_json.?);
}

fn parseBatchRequestWithOptions(
    alloc: std.mem.Allocator,
    body: []const u8,
    options: std.json.ParseOptions,
    allow_internal: bool,
) !OwnedBatchRequest {
    if (body.len == 0) return .{};

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, options) catch |err| switch (err) {
        error.ValueTooLong => return error.ValueTooLong,
        else => return error.InvalidBatchRequest,
    };
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidBatchRequest;
    const root = parsed.value.object;

    var page_effects: ?std.json.Parsed(MergePageEffects) = null;
    errdefer if (page_effects) |*effects| effects.deinit();
    if (root.get("_merge_page_effects")) |value| {
        if (!allow_internal or root.get("_merge_page") == null) return error.InvalidBatchRequest;
        if (root.get("inserts")) |inserts| if (inserts != .object or inserts.object.count() != 0) return error.InvalidBatchRequest;
        if (root.get("deletes")) |deletes| if (deletes != .array or deletes.array.items.len != 0) return error.InvalidBatchRequest;
        page_effects = try std.json.parseFromValue(MergePageEffects, alloc, value, .{ .allocate = .alloc_always });
    } else if (root.get("_merge_page") != null) return error.InvalidBatchRequest;

    const writes: []db_mod.types.BatchWrite = writes: {
        if (page_effects) |effects| break :writes effects.value.writes;
        if (root.get("inserts")) |inserts| {
            if (inserts == .null) break :writes &.{};
            const parsed_writes = try parseInserts(alloc, inserts, !allow_internal);
            errdefer freeWrites(alloc, parsed_writes);
            break :writes parsed_writes;
        }
        break :writes &.{};
    };
    errdefer if (page_effects == null) freeWrites(alloc, writes);

    const deletes: [][]const u8 = deletes: {
        if (page_effects) |effects| break :deletes effects.value.deletes;
        if (root.get("deletes")) |deletes_value| {
            if (deletes_value == .null) break :deletes &.{};
            const parsed_deletes = try parseDeletes(alloc, deletes_value);
            errdefer freeDeletes(alloc, parsed_deletes);
            break :deletes parsed_deletes;
        }
        break :deletes &.{};
    };
    errdefer if (page_effects == null) freeDeletes(alloc, deletes);

    const transforms: []db_mod.types.DocumentTransform = transforms: {
        if (root.get("transforms")) |transforms_value| {
            if (transforms_value == .null) break :transforms &.{};
            const parsed_transforms = try parseTransforms(alloc, transforms_value);
            errdefer freeTransforms(alloc, parsed_transforms);
            break :transforms parsed_transforms;
        }
        break :transforms &.{};
    };
    errdefer freeTransforms(alloc, transforms);

    const integrity = integrity: {
        const value = root.get("_integrity") orelse break :integrity &.{};
        if (!allow_internal) return error.InvalidBatchRequest;
        break :integrity try @import("relational_integrity_wire.zig").parse(alloc, value);
    };
    errdefer @import("relational_integrity_wire.zig").free(alloc, integrity);
    var integrity_commands = if (root.get("_integrity_commands")) |value| commands: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :commands @as(?std.json.Parsed([]const @import("../storage/db/relational_integrity_contract.zig").Command), try @import("relational_integrity_wire.zig").parseCommands(alloc, value));
    } else null;
    errdefer if (integrity_commands) |*commands| commands.deinit();
    var relational_activation = if (root.get("_relational_activation")) |value| activation: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :activation @as(?std.json.Parsed(@import("../storage/db/relational_integrity_activation_contract.zig").Command), try std.json.parseFromValue(@import("../storage/db/relational_integrity_activation_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    errdefer if (relational_activation) |*activation| activation.deinit();
    var relational_retirement = if (root.get("_relational_retirement")) |value| retirement: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :retirement @as(?std.json.Parsed(@import("../storage/db/relational_integrity_retirement_contract.zig").Command), try std.json.parseFromValue(@import("../storage/db/relational_integrity_retirement_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    errdefer if (relational_retirement) |*retirement| retirement.deinit();
    var relational_index_maintenance = if (root.get("_relational_index_maintenance")) |value| retirement: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :retirement @as(?std.json.Parsed(@import("../storage/db/relational_index_maintenance_contract.zig").Command), try std.json.parseFromValue(@import("../storage/db/relational_index_maintenance_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    errdefer if (relational_index_maintenance) |*retirement| retirement.deinit();
    var relational_topology = if (root.get("_relational_topology")) |value| topology: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :topology @as(?std.json.Parsed(@import("../storage/db/relational_integrity_topology_contract.zig").Command), try std.json.parseFromValue(@import("../storage/db/relational_integrity_topology_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    errdefer if (relational_topology) |*topology| topology.deinit();
    var restore_staging = if (root.get("_restore_staging")) |value| control: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string or !std.mem.eql(u8, kind.string, "restore_v3")) return error.InvalidBatchRequest;
        break :control @as(?std.json.Parsed(@import("../storage/db/restore_staging_contract.zig").Control), try std.json.parseFromValue(@import("../storage/db/restore_staging_contract.zig").Control, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    errdefer if (restore_staging) |*control| control.deinit();
    var online_source = if (root.get("_online_source")) |value| control: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string or !std.mem.eql(u8, kind.string, "online_source_v4")) return error.InvalidBatchRequest;
        break :control @as(?std.json.Parsed(@import("../storage/db/online_source_contract.zig").Command), try std.json.parseFromValue(@import("../storage/db/online_source_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    errdefer if (online_source) |*control| control.deinit();
    const relational_schema_version: ?u32 = if (root.get("_relational_schema_version")) |value| blk: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :blk std.math.cast(u32, try parseInternalU64(value)) orelse return error.InvalidBatchRequest;
    } else null;
    const relational_integrity_generation_set: ?[32]u8 = if (root.get("_relational_integrity_generation_set")) |value| blk: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :blk try @import("relational_integrity_wire.zig").parseGenerationSet(value);
    } else null;
    const relational_repair = if (root.get("_relational_repair")) |value| blk: {
        if (!allow_internal or value != .bool) return error.InvalidBatchRequest;
        break :blk value.bool;
    } else false;
    const restore_staging_scope: ?[32]u8 = if (root.get("_restore_staging_scope")) |value| blk: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :blk try @import("relational_integrity_wire.zig").parseGenerationSet(value);
    } else null;
    const restore_staging_plan_id: ?[16]u8 = if (root.get("_restore_staging_plan_id")) |value| blk: {
        if (!allow_internal or restore_staging_scope == null or value != .string or value.string.len != 32) return error.InvalidBatchRequest;
        var id: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&id, value.string) catch return error.InvalidBatchRequest;
        if (std.mem.allEqual(u8, &id, 0)) return error.InvalidBatchRequest;
        break :blk id;
    } else null;
    const graph_writes: []db_mod.types.GraphEdgeWrite = graph_writes: {
        const value = root.get("_graph_writes") orelse break :graph_writes &.{};
        if (!allow_internal) return error.InvalidBatchRequest;
        if (value == .null) break :graph_writes &.{};
        const parsed_graph_writes = try parseGraphWrites(alloc, value);
        errdefer freeGraphWrites(alloc, parsed_graph_writes);
        break :graph_writes parsed_graph_writes;
    };
    errdefer freeGraphWrites(alloc, graph_writes);

    const graph_deletes: []db_mod.types.GraphEdgeDelete = graph_deletes: {
        const value = root.get("_graph_deletes") orelse break :graph_deletes &.{};
        if (!allow_internal) return error.InvalidBatchRequest;
        if (value == .null) break :graph_deletes &.{};
        const parsed_graph_deletes = try parseGraphDeletes(alloc, value);
        errdefer freeGraphDeletes(alloc, parsed_graph_deletes);
        break :graph_deletes parsed_graph_deletes;
    };
    errdefer freeGraphDeletes(alloc, graph_deletes);

    const predicates: []db_mod.types.TransactionVersionPredicate = predicates: {
        const value = root.get("_predicates") orelse break :predicates &.{};
        if (!allow_internal or value != .array) return error.InvalidBatchRequest;
        const items = try alloc.alloc(db_mod.types.TransactionVersionPredicate, value.array.items.len);
        var initialized: usize = 0;
        errdefer {
            for (items[0..initialized]) |predicate| alloc.free(@constCast(predicate.key));
            alloc.free(items);
        }
        for (value.array.items, 0..) |item, i| {
            if (item != .object) return error.InvalidBatchRequest;
            const key_value = item.object.get("key") orelse return error.InvalidBatchRequest;
            const version_value = item.object.get("expected_version") orelse return error.InvalidBatchRequest;
            if (key_value != .string) return error.InvalidBatchRequest;
            const version = try parseInternalU64(version_value);
            var digest: ?[32]u8 = null;
            if (item.object.get("expected_content_digest")) |encoded| {
                if (version == 0 or encoded != .string or encoded.string.len != 64) return error.InvalidBatchRequest;
                var bytes: [32]u8 = undefined;
                _ = std.fmt.hexToBytes(&bytes, encoded.string) catch return error.InvalidBatchRequest;
                digest = bytes;
            }
            items[i] = .{
                .key = try alloc.dupe(u8, key_value.string),
                .expected_version = version,
                .expected_content_digest = digest,
            };
            initialized += 1;
        }
        break :predicates items;
    };
    errdefer {
        for (predicates) |predicate| alloc.free(@constCast(predicate.key));
        if (predicates.len > 0) alloc.free(predicates);
    }

    const sync_level = sync_level: {
        if (root.get("sync_level")) |sync_level_value| {
            if (sync_level_value == .null) break :sync_level db_mod.types.SyncLevel.propose;
            break :sync_level try syncLevelFromValue(sync_level_value);
        }
        break :sync_level db_mod.types.SyncLevel.propose;
    };

    const timestamp_ns = timestamp: {
        const value = root.get("_timestamp_ns") orelse break :timestamp 0;
        if (!allow_internal) return error.InvalidBatchRequest;
        break :timestamp try parseInternalU64(value);
    };

    const reject_graph_transform_projections = reject: {
        const value = root.get("_reject_graph_transform_projections") orelse break :reject false;
        if (!allow_internal or value != .bool) return error.InvalidBatchRequest;
        break :reject value.bool;
    };

    var checkpoint_start: ?[]u8 = null;
    errdefer if (checkpoint_start) |value| alloc.free(value);
    var checkpoint_end: ?[]u8 = null;
    errdefer if (checkpoint_end) |value| alloc.free(value);
    const split_checkpoint: ?db_mod.types.SplitReplicationCheckpoint = checkpoint: {
        const value = root.get("_split_checkpoint") orelse break :checkpoint null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const kind_value = object.get("kind") orelse return error.InvalidBatchRequest;
        const transition_value = object.get("transition_id") orelse return error.InvalidBatchRequest;
        const attempt_value = object.get("attempt_epoch") orelse return error.InvalidBatchRequest;
        const source_value = object.get("source_group_id") orelse return error.InvalidBatchRequest;
        const destination_value = object.get("destination_group_id") orelse return error.InvalidBatchRequest;
        const sequence_value = object.get("delta_sequence") orelse return error.InvalidBatchRequest;
        if (kind_value != .string) return error.InvalidBatchRequest;
        const transition_id = try parseInternalU64(transition_value);
        const attempt_epoch = try parseInternalU64(attempt_value);
        const source_group_id = try parseInternalU64(source_value);
        const destination_group_id = try parseInternalU64(destination_value);
        const delta_sequence = try parseInternalU64(sequence_value);
        const kind: db_mod.types.SplitReplicationCheckpoint.Kind = std.meta.stringToEnum(
            db_mod.types.SplitReplicationCheckpoint.Kind,
            kind_value.string,
        ) orelse return error.InvalidBatchRequest;
        const range_start = if (object.get("range_start")) |item| start: {
            if (item != .string) return error.InvalidBatchRequest;
            break :start item.string;
        } else "";
        const range_end = if (object.get("range_end")) |item| end: {
            if (item != .string) return error.InvalidBatchRequest;
            break :end item.string;
        } else "";
        checkpoint_start = try alloc.dupe(u8, range_start);
        checkpoint_end = try alloc.dupe(u8, range_end);
        if (transition_id == 0 or attempt_epoch == 0) return error.InvalidBatchRequest;
        break :checkpoint .{
            .kind = kind,
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .range_start = checkpoint_start.?,
            .range_end = checkpoint_end.?,
            .delta_sequence = delta_sequence,
        };
    };

    const split_replication: ?db_mod.types.SplitReplicationContext = replication: {
        const value = root.get("_split_replication") orelse break :replication null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const transition_value = object.get("transition_id") orelse return error.InvalidBatchRequest;
        const attempt_value = object.get("attempt_epoch") orelse return error.InvalidBatchRequest;
        const source_value = object.get("source_group_id") orelse return error.InvalidBatchRequest;
        const destination_value = object.get("destination_group_id") orelse return error.InvalidBatchRequest;
        const table_value = object.get("namespace_table_id") orelse return error.InvalidBatchRequest;
        const shard_value = object.get("namespace_shard_id") orelse return error.InvalidBatchRequest;
        const range_value = object.get("namespace_range_id") orelse return error.InvalidBatchRequest;
        const operation_value = object.get("operation") orelse return error.InvalidBatchRequest;
        const sequence_value = object.get("sequence") orelse return error.InvalidBatchRequest;
        if (operation_value != .string) return error.InvalidBatchRequest;
        const transition_id = try parseInternalU64(transition_value);
        const attempt_epoch = try parseInternalU64(attempt_value);
        const source_group_id = try parseInternalU64(source_value);
        const destination_group_id = try parseInternalU64(destination_value);
        const table_id = try parseInternalU64(table_value);
        const shard_id = try parseInternalU64(shard_value);
        const range_id = try parseInternalU64(range_value);
        const sequence = try parseInternalU64(sequence_value);
        const previous_sequence = if (object.get("previous_sequence")) |previous_value|
            try parseInternalU64(previous_value)
        else
            null;
        const bootstrap_sequence = if (object.get("bootstrap_sequence")) |bootstrap_value|
            try parseInternalU64(bootstrap_value)
        else
            null;
        const operation = std.meta.stringToEnum(db_mod.types.SplitReplicationContext.Operation, operation_value.string) orelse
            return error.InvalidBatchRequest;
        if (transition_id == 0 or attempt_epoch == 0 or source_group_id == 0 or destination_group_id == 0 or table_id == 0 or shard_id == 0 or range_id == 0) return error.InvalidBatchRequest;
        if (previous_sequence) |previous| {
            if (operation != .delta or sequence == 0 or previous >= sequence)
                return error.InvalidBatchRequest;
        }
        break :replication .{
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .identity_namespace = .{
                .table_id = table_id,
                .shard_id = shard_id,
                .range_id = range_id,
            },
            .operation = operation,
            .sequence = sequence,
            .previous_sequence = previous_sequence,
            .bootstrap_sequence = bootstrap_sequence,
        };
    };

    const merge_replication: ?db_mod.types.MergeReplicationContext = replication: {
        const value = root.get("_merge_replication") orelse break :replication null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const transition_id = try parseInternalU64(object.get("transition_id") orelse return error.InvalidBatchRequest);
        const donor_group_id = try parseInternalU64(object.get("donor_group_id") orelse return error.InvalidBatchRequest);
        const receiver_group_id = try parseInternalU64(object.get("receiver_group_id") orelse return error.InvalidBatchRequest);
        const table_id = try parseInternalU64(object.get("namespace_table_id") orelse return error.InvalidBatchRequest);
        const shard_id = try parseInternalU64(object.get("namespace_shard_id") orelse return error.InvalidBatchRequest);
        const range_id = try parseInternalU64(object.get("namespace_range_id") orelse return error.InvalidBatchRequest);
        if (transition_id == 0 or donor_group_id == 0 or receiver_group_id == 0 or
            donor_group_id == receiver_group_id or table_id == 0 or shard_id == 0 or range_id == 0)
            return error.InvalidBatchRequest;
        break :replication .{
            .transition_id = transition_id,
            .donor_group_id = donor_group_id,
            .receiver_group_id = receiver_group_id,
            .copy_attempt = .{
                .donor_term = try parseInternalU64(object.get("copy_donor_term") orelse return error.InvalidBatchRequest),
                .sequence = try parseInternalU64(object.get("copy_sequence") orelse return error.InvalidBatchRequest),
            },
            .identity_namespace = .{
                .table_id = table_id,
                .shard_id = shard_id,
                .range_id = range_id,
            },
        };
    };

    var merge_page: ?std.json.Parsed(merge_pages.Command) = null;
    errdefer if (merge_page) |*value| value.deinit();
    if (root.get("_merge_page")) |value| {
        if (!allow_internal or merge_replication == null) return error.InvalidBatchRequest;
        merge_page = @import("../storage/db/merge_page_wire.zig").parseFromValue(alloc, value) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidBatchRequest,
        };
        if (merge_page.?.value.chunk != null and writes.len != 0) return error.InvalidBatchRequest;
        // A discriminant understood by the old checkpoint parser prevents an
        // older node from silently discarding progress and applying bare rows.
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        const expected_marker = if (merge_page.?.value.source.retention != null) "page_v6" else if (merge_page.?.value.source.integrity != null) "page_v5" else if (merge_page.?.value.next_snapshot_position != null) "page_v4" else if (merge_page.?.value.chunk != null) "page_v3" else "page_v1";
        if (kind != .string or !std.mem.eql(u8, kind.string, expected_marker)) return error.InvalidBatchRequest;
    }

    var merge_artifacts: ?std.json.Parsed([]const db_mod.types.BatchWrite) = null;
    errdefer if (merge_artifacts) |*value| value.deinit();
    if (root.get("_merge_artifacts")) |value| {
        if (!allow_internal or merge_replication == null) return error.InvalidBatchRequest;
        merge_artifacts = try std.json.parseFromValue([]const db_mod.types.BatchWrite, alloc, value, .{ .allocate = .alloc_always });
        try db_mod.types.validateMergeArtifacts(.{
            .merge_replication = merge_replication,
            .merge_artifacts = merge_artifacts.?.value,
            .writes = writes,
            .deletes = deletes,
            .transforms = transforms,
            .predicates = predicates,
        });
    }

    var transition_key: ?[]u8 = null;
    errdefer if (transition_key) |value| alloc.free(value);
    const split_transition: ?db_mod.types.SplitTransitionMutation = transition: {
        const value = root.get("_split_transition") orelse break :transition null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const kind_value = object.get("kind") orelse return error.InvalidBatchRequest;
        const transition_value = object.get("transition_id") orelse return error.InvalidBatchRequest;
        const attempt_value = object.get("attempt_epoch") orelse return error.InvalidBatchRequest;
        const destination_value = object.get("destination_group_id") orelse return error.InvalidBatchRequest;
        if (kind_value != .string) return error.InvalidBatchRequest;
        const transition_id = try parseInternalU64(transition_value);
        const attempt_epoch = try parseInternalU64(attempt_value);
        const destination_group_id = try parseInternalU64(destination_value);
        if (transition_id == 0 or attempt_epoch == 0 or destination_group_id == 0) return error.InvalidBatchRequest;
        const kind = std.meta.stringToEnum(db_mod.types.SplitTransitionMutation.Kind, kind_value.string) orelse
            return error.InvalidBatchRequest;
        const split_key = if (object.get("split_key")) |item| key: {
            if (item != .string) return error.InvalidBatchRequest;
            break :key item.string;
        } else "";
        if ((kind == .prepare or kind == .start) and split_key.len == 0) return error.InvalidBatchRequest;
        transition_key = try alloc.dupe(u8, split_key);
        break :transition .{
            .kind = kind,
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .destination_group_id = destination_group_id,
            .split_key = transition_key.?,
        };
    };

    const merge_source_transition: ?db_mod.types.MergeSourceTransitionMutation = transition: {
        const value = root.get("_merge_source_transition") orelse break :transition null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const kind_value = object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind_value != .string) return error.InvalidBatchRequest;
        const kind = std.meta.stringToEnum(
            db_mod.types.MergeSourceTransitionMutation.Kind,
            kind_value.string,
        ) orelse return error.InvalidBatchRequest;
        const transition_id = try parseInternalU64(object.get("transition_id") orelse return error.InvalidBatchRequest);
        const receiver_group_id = try parseInternalU64(object.get("receiver_group_id") orelse return error.InvalidBatchRequest);
        if (transition_id == 0 or receiver_group_id == 0) return error.InvalidBatchRequest;
        break :transition .{
            .kind = kind,
            .transition_id = transition_id,
            .receiver_group_id = receiver_group_id,
        };
    };

    var merge_receiver_base_start: ?[]u8 = null;
    errdefer if (merge_receiver_base_start) |value| alloc.free(value);
    var merge_receiver_base_end: ?[]u8 = null;
    errdefer if (merge_receiver_base_end) |value| alloc.free(value);
    var merge_range_start: ?[]u8 = null;
    errdefer if (merge_range_start) |value| alloc.free(value);
    var merge_range_end: ?[]u8 = null;
    errdefer if (merge_range_end) |value| alloc.free(value);
    const merge_checkpoint: ?db_mod.types.MergeReplicationCheckpoint = merge: {
        const value = root.get("_merge_checkpoint") orelse break :merge null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const kind_value = object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind_value != .string) return error.InvalidBatchRequest;
        if (std.mem.eql(u8, kind_value.string, "restore_v3")) {
            if (restore_staging == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "online_source_v4")) {
            if (online_source == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "page_v1") or std.mem.eql(u8, kind_value.string, "page_v2") or std.mem.eql(u8, kind_value.string, "page_v3") or std.mem.eql(u8, kind_value.string, "page_v4") or std.mem.eql(u8, kind_value.string, "page_v5") or std.mem.eql(u8, kind_value.string, "page_v6")) {
            if (merge_page == null) return error.InvalidBatchRequest;
            break :merge null;
        }
        const integrity_protocol = std.mem.startsWith(u8, kind_value.string, "page_v3_");
        const scope_protocol = std.mem.startsWith(u8, kind_value.string, "page_v4_");
        const tail_protocol = scope_protocol or integrity_protocol or std.mem.startsWith(u8, kind_value.string, "page_v2_");
        const page_protocol = tail_protocol or std.mem.startsWith(u8, kind_value.string, "page_v1_");
        const kind = std.meta.stringToEnum(
            db_mod.types.MergeReplicationCheckpoint.Kind,
            if (page_protocol) kind_value.string["page_v1_".len..] else kind_value.string,
        ) orelse return error.InvalidBatchRequest;
        if (page_protocol != (object.get("page_source") != null) or page_protocol != (object.get("page_receiver_namespace") != null))
            return error.InvalidBatchRequest;
        const page_source: ?merge_pages.Source = if (object.get("page_source")) |source_value| blk: {
            var owned = try std.json.parseFromValue(merge_pages.Source, alloc, source_value, .{});
            defer owned.deinit();
            try owned.value.validate();
            if (tail_protocol != (owned.value.retention != null)) return error.InvalidBatchRequest;
            if (scope_protocol != (owned.value.retention != null)) return error.InvalidBatchRequest;
            if (!scope_protocol and integrity_protocol != (owned.value.integrity != null)) return error.InvalidBatchRequest;
            break :blk owned.value;
        } else null;
        const page_receiver: ?db_mod.DocIdentityNamespace = if (object.get("page_receiver_namespace")) |receiver_value| blk: {
            var owned = try std.json.parseFromValue(db_mod.DocIdentityNamespace, alloc, receiver_value, .{});
            defer owned.deinit();
            if (owned.value.table_id == 0 or owned.value.shard_id == 0 or owned.value.range_id == 0) return error.InvalidBatchRequest;
            break :blk owned.value;
        } else null;
        const transition_id = try parseInternalU64(object.get("transition_id") orelse return error.InvalidBatchRequest);
        const donor_group_id = try parseInternalU64(object.get("donor_group_id") orelse return error.InvalidBatchRequest);
        const receiver_group_id = try parseInternalU64(object.get("receiver_group_id") orelse return error.InvalidBatchRequest);
        if (transition_id == 0 or donor_group_id == 0 or receiver_group_id == 0 or
            donor_group_id == receiver_group_id)
            return error.InvalidBatchRequest;
        const base_start_value = object.get("receiver_base_start") orelse return error.InvalidBatchRequest;
        const base_end_value = object.get("receiver_base_end") orelse return error.InvalidBatchRequest;
        const merged_start_value = object.get("merged_start") orelse return error.InvalidBatchRequest;
        const merged_end_value = object.get("merged_end") orelse return error.InvalidBatchRequest;
        if (base_start_value != .string or base_end_value != .string or
            merged_start_value != .string or merged_end_value != .string)
            return error.InvalidBatchRequest;
        merge_receiver_base_start = try alloc.dupe(u8, base_start_value.string);
        merge_receiver_base_end = try alloc.dupe(u8, base_end_value.string);
        merge_range_start = try alloc.dupe(u8, merged_start_value.string);
        merge_range_end = try alloc.dupe(u8, merged_end_value.string);
        const namespace: ?db_mod.DocIdentityNamespace = namespace: {
            const table_value = object.get("namespace_table_id") orelse break :namespace null;
            const shard_value = object.get("namespace_shard_id") orelse return error.InvalidBatchRequest;
            const range_value = object.get("namespace_range_id") orelse return error.InvalidBatchRequest;
            const table_id = try parseInternalU64(table_value);
            const shard_id = try parseInternalU64(shard_value);
            const range_id = try parseInternalU64(range_value);
            if (table_id == 0 or shard_id == 0 or range_id == 0) return error.InvalidBatchRequest;
            break :namespace .{ .table_id = table_id, .shard_id = shard_id, .range_id = range_id };
        };
        break :merge .{
            .kind = kind,
            .transition_id = transition_id,
            .donor_group_id = donor_group_id,
            .receiver_group_id = receiver_group_id,
            .copy_attempt = .{
                .donor_term = try parseInternalU64(object.get("copy_donor_term") orelse return error.InvalidBatchRequest),
                .sequence = try parseInternalU64(object.get("copy_sequence") orelse return error.InvalidBatchRequest),
            },
            .receiver_base_start = merge_receiver_base_start.?,
            .receiver_base_end = merge_receiver_base_end.?,
            .merged_start = merge_range_start.?,
            .merged_end = merge_range_end.?,
            .bootstrap_applied_index = if (object.get("bootstrap_applied_index")) |item|
                try parseInternalU64(item)
            else
                0,
            .allow_doc_identity_reassignment = if (object.get("allow_doc_identity_reassignment")) |item| switch (item) {
                .bool => |flag| flag,
                else => return error.InvalidBatchRequest,
            } else false,
            .receiver_identity_reassignment_namespace = namespace,
            .page_source = page_source,
            .page_receiver_namespace = page_receiver,
        };
    };

    var transaction_participants: [][]const u8 = &.{};
    var transaction_participants_initialized: usize = 0;
    errdefer {
        for (transaction_participants[0..transaction_participants_initialized]) |participant| alloc.free(participant);
        if (transaction_participants.len > 0) alloc.free(transaction_participants);
    }
    const transaction: ?db_mod.types.TransactionMutation = transaction: {
        const value = root.get("_transaction") orelse break :transaction null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const phase_value = object.get("phase") orelse return error.InvalidBatchRequest;
        const txn_value = object.get("txn_id") orelse return error.InvalidBatchRequest;
        if (phase_value != .string or txn_value != .string or txn_value.string.len != 32) return error.InvalidBatchRequest;
        var txn_id: db_mod.types.TxnId = undefined;
        _ = std.fmt.hexToBytes(&txn_id, txn_value.string) catch return error.InvalidBatchRequest;
        if (std.mem.eql(u8, phase_value.string, "begin")) {
            const participants_value = object.get("participants") orelse return error.InvalidBatchRequest;
            if (participants_value != .array or participants_value.array.items.len == 0) return error.InvalidBatchRequest;
            transaction_participants = try alloc.alloc([]const u8, participants_value.array.items.len);
            for (participants_value.array.items, 0..) |participant, i| {
                if (participant != .string or participant.string.len == 0) return error.InvalidBatchRequest;
                transaction_participants[i] = try alloc.dupe(u8, participant.string);
                transaction_participants_initialized += 1;
            }
            break :transaction .{ .begin = .{
                .txn_id = txn_id,
                .begin_timestamp = try parseInternalU64(object.get("begin_timestamp") orelse return error.InvalidBatchRequest),
                .created_at_ns = try parseInternalU64(object.get("created_at_ns") orelse return error.InvalidBatchRequest),
                .topology_epoch = try parseInternalU64(object.get("topology_epoch") orelse return error.InvalidBatchRequest),
                .retain_terminal = if (object.get("retain_terminal")) |item| switch (item) {
                    .bool => |flag| flag,
                    else => return error.InvalidBatchRequest,
                } else false,
                .participants = transaction_participants,
            } };
        }
        if (std.mem.eql(u8, phase_value.string, "prepare")) break :transaction .{ .prepare = .{
            .txn_id = txn_id,
            .topology_epoch = try parseInternalU64(object.get("topology_epoch") orelse return error.InvalidBatchRequest),
        } };
        if (std.mem.eql(u8, phase_value.string, "resolve")) {
            const status_value = object.get("status") orelse return error.InvalidBatchRequest;
            if (status_value != .string) return error.InvalidBatchRequest;
            const status = std.meta.stringToEnum(db_mod.types.TxnStatus, status_value.string) orelse return error.InvalidBatchRequest;
            if (status == .pending) return error.InvalidBatchRequest;
            break :transaction .{ .resolve = .{
                .txn_id = txn_id,
                .status = status,
                .commit_version = try parseInternalU64(object.get("commit_version") orelse return error.InvalidBatchRequest),
            } };
        }
        if (std.mem.eql(u8, phase_value.string, "acknowledge")) {
            const participant_value = object.get("participant") orelse return error.InvalidBatchRequest;
            if (participant_value != .string or participant_value.string.len == 0) return error.InvalidBatchRequest;
            transaction_participants = try alloc.alloc([]const u8, 1);
            transaction_participants[0] = try alloc.dupe(u8, participant_value.string);
            transaction_participants_initialized = 1;
            break :transaction .{ .acknowledge = .{
                .txn_id = txn_id,
                .participant = transaction_participants[0],
            } };
        }
        if (std.mem.eql(u8, phase_value.string, "cleanup")) break :transaction .{ .cleanup = .{
            .txn_id = txn_id,
            .cutoff_timestamp = try parseInternalU64(object.get("cutoff_timestamp") orelse return error.InvalidBatchRequest),
            .retained_cutoff_timestamp = try parseInternalU64(object.get("retained_cutoff_timestamp") orelse return error.InvalidBatchRequest),
        } };
        return error.InvalidBatchRequest;
    };

    if ((split_transition != null or merge_source_transition != null) and
        (writes.len != 0 or deletes.len != 0 or transforms.len != 0 or
            graph_writes.len != 0 or graph_deletes.len != 0 or
            predicates.len != 0 or split_checkpoint != null or split_replication != null or merge_replication != null or
            split_transition != null and merge_source_transition != null or
            merge_checkpoint != null or transaction != null))
    {
        return error.InvalidBatchRequest;
    }
    if (transaction != null and (split_checkpoint != null or split_replication != null or merge_replication != null or
        split_transition != null or merge_source_transition != null or merge_checkpoint != null))
    {
        return error.InvalidBatchRequest;
    }
    if (predicates.len != 0 and transaction == null) return error.InvalidBatchRequest;
    if (integrity.len != 0 and (transaction == null or transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (integrity_commands != null and (transaction == null or transaction.? != .prepare or integrity.len != 0)) return error.InvalidBatchRequest;
    if (relational_activation != null and (transaction == null or transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (relational_retirement != null and (transaction == null or transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (relational_index_maintenance) |command| {
        try command.value.validate();
        if (transaction == null or transaction.? != .prepare or writes.len != 0 or deletes.len != 0 or transforms.len != 0 or predicates.len != 0 or integrity.len != 0 or integrity_commands != null or relational_activation != null or relational_retirement != null or relational_repair or restore_staging_scope != null) return error.InvalidBatchRequest;
    }
    if (relational_topology != null and (transaction != null or writes.len != 0 or deletes.len != 0 or transforms.len != 0 or predicates.len != 0 or integrity.len != 0 or integrity_commands != null or relational_activation != null or relational_retirement != null or relational_index_maintenance != null or split_checkpoint != null or split_replication != null or split_transition != null or merge_checkpoint != null or merge_replication != null or merge_source_transition != null)) return error.InvalidBatchRequest;
    if (relational_topology != null and (relational_schema_version != null or relational_integrity_generation_set != null or relational_repair)) return error.InvalidBatchRequest;
    if (transaction) |mutation| switch (mutation) {
        .begin, .resolve, .acknowledge, .cleanup => if (writes.len != 0 or deletes.len != 0 or transforms.len != 0 or predicates.len != 0)
            return error.InvalidBatchRequest,
        .prepare => {},
    };
    if (split_checkpoint != null and split_checkpoint.?.kind == .source_ack and
        (writes.len != 0 or deletes.len != 0 or transforms.len != 0 or
            graph_writes.len != 0 or graph_deletes.len != 0 or
            split_replication != null))
    {
        return error.InvalidBatchRequest;
    }
    if (merge_checkpoint != null) {
        if (merge_artifacts != null) return error.InvalidBatchRequest;
        if (split_checkpoint != null or split_replication != null or split_transition != null or
            merge_source_transition != null or
            transforms.len != 0 or predicates.len != 0 or writes.len != 0 or
            deletes.len != 0)
            return error.InvalidBatchRequest;
    }
    if (merge_replication != null and
        (split_checkpoint != null or split_replication != null or split_transition != null or
            merge_source_transition != null or transaction != null or
            transforms.len != 0 or predicates.len != 0))
    {
        return error.InvalidBatchRequest;
    }
    if (merge_replication) |replication| if (merge_checkpoint) |checkpoint| {
        if (replication.transition_id != checkpoint.transition_id or
            replication.donor_group_id != checkpoint.donor_group_id or
            replication.receiver_group_id != checkpoint.receiver_group_id or
            replication.copy_attempt.order(checkpoint.copy_attempt) != .eq)
            return error.InvalidBatchRequest;
    };

    const result_value: OwnedBatchRequest = .{
        .writes = writes,
        .deletes = deletes,
        .transforms = transforms,
        .graph_writes = graph_writes,
        .graph_deletes = graph_deletes,
        .predicates = predicates,
        .integrity = integrity,
        .integrity_commands = integrity_commands,
        .relational_activation = relational_activation,
        .relational_retirement = relational_retirement,
        .relational_index_maintenance = relational_index_maintenance,
        .relational_topology = relational_topology,
        .restore_staging = restore_staging,
        .online_source = online_source,
        .transaction_participants = transaction_participants,
        .split_checkpoint_range_start = checkpoint_start,
        .split_checkpoint_range_end = checkpoint_end,
        .split_transition_key = transition_key,
        .merge_receiver_base_start = merge_receiver_base_start,
        .merge_receiver_base_end = merge_receiver_base_end,
        .merge_range_start = merge_range_start,
        .merge_range_end = merge_range_end,
        .merge_artifacts = merge_artifacts,
        .merge_page = merge_page,
        .merge_page_effects = page_effects,
        .req = .{
            .integrity = integrity,
            .integrity_commands = if (integrity_commands) |commands| commands.value else &.{},
            .relational_activation = if (relational_activation) |activation| activation.value else null,
            .relational_retirement = if (relational_retirement) |retirement| retirement.value else null,
            .relational_index_maintenance = if (relational_index_maintenance) |maintenance| maintenance.value else null,
            .relational_topology = if (relational_topology) |topology| topology.value else null,
            .restore_staging = if (restore_staging) |control| control.value else null,
            .online_source = if (online_source) |control| control.value else null,
            .relational_schema_version = relational_schema_version,
            .relational_integrity_generation_set = relational_integrity_generation_set,
            .restore_staging_scope = restore_staging_scope,
            .restore_staging_plan_id = restore_staging_plan_id,
            .relational_repair = relational_repair,
            .writes = writes,
            .deletes = deletes,
            .transforms = transforms,
            .graph_writes = graph_writes,
            .graph_deletes = graph_deletes,
            .predicates = predicates,
            .timestamp_ns = timestamp_ns,
            .sync_level = sync_level,
            .reject_graph_transform_projections = reject_graph_transform_projections,
            .split_checkpoint = split_checkpoint,
            .split_replication = split_replication,
            .merge_replication = merge_replication,
            .merge_page = if (merge_page) |value| value.value else null,
            .merge_artifacts = if (merge_artifacts) |value| value.value else &.{},
            .split_transition = split_transition,
            .merge_source_transition = merge_source_transition,
            .merge_checkpoint = merge_checkpoint,
            .transaction = transaction,
        },
    };
    try merge_pages.validateRequest(result_value.req);
    try @import("../storage/db/online_source_contract.zig").validateRequest(result_value.req);
    return result_value;
}

pub fn encodeBatchResponse(alloc: std.mem.Allocator, result: BatchResult) ![]u8 {
    return try ant_json.valueAlloc(alloc, result, .{ .emit_null_optional_fields = false });
}

pub fn encodeBatchRequest(alloc: std.mem.Allocator, req: db_mod.types.BatchRequest) ![]u8 {
    try @import("../storage/db/online_source_contract.zig").validateRequest(req);
    try merge_pages.validateRequest(req);
    if (req.merge_checkpoint) |checkpoint| {
        if ((checkpoint.page_source != null) != (checkpoint.page_receiver_namespace != null)) return error.InvalidBatchRequest;
        if (checkpoint.page_source) |source| try source.validate();
    }
    if (req.relational_topology != null and (req.transaction != null or req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or req.predicates.len != 0 or req.integrity.len != 0 or req.integrity_commands.len != 0 or req.relational_activation != null or req.relational_retirement != null or req.relational_index_maintenance != null or req.split_checkpoint != null or req.split_replication != null or req.split_transition != null or req.merge_checkpoint != null or req.merge_replication != null or req.merge_source_transition != null)) return error.InvalidBatchRequest;
    if (req.relational_topology != null and (req.relational_schema_version != null or req.relational_integrity_generation_set != null or req.relational_repair)) return error.InvalidBatchRequest;
    if (req.integrity.len != 0 and (req.transaction == null or req.transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (req.integrity_commands.len != 0 and (req.transaction == null or req.transaction.? != .prepare or req.integrity.len != 0)) return error.InvalidBatchRequest;
    if (req.relational_activation != null and (req.transaction == null or req.transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (req.relational_retirement != null and (req.transaction == null or req.transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (req.relational_index_maintenance) |command| {
        try command.validate();
        if (req.transaction == null or req.transaction.? != .prepare or req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or req.predicates.len != 0 or req.integrity.len != 0 or req.integrity_commands.len != 0 or req.relational_activation != null or req.relational_retirement != null or req.relational_repair or req.restore_staging_scope != null) return error.InvalidBatchRequest;
    }
    try db_mod.types.validateMergeArtifacts(req);
    if (req.predicates.len > 0 and req.transaction == null) {
        return error.UnsupportedBatchRequestEncoding;
    }
    if ((req.split_transition != null or req.merge_source_transition != null) and
        (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or
            req.graph_writes.len != 0 or req.graph_deletes.len != 0 or
            req.predicates.len != 0 or req.split_checkpoint != null or req.split_replication != null or req.merge_replication != null or
            req.split_transition != null and req.merge_source_transition != null or
            req.merge_checkpoint != null or req.transaction != null))
    {
        return error.InvalidBatchRequest;
    }
    if (req.split_checkpoint != null and req.split_checkpoint.?.kind == .source_ack and
        (req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or
            req.graph_writes.len != 0 or req.graph_deletes.len != 0 or req.predicates.len != 0 or
            req.split_replication != null))
    {
        return error.InvalidBatchRequest;
    }
    if (req.merge_checkpoint != null) {
        if (req.split_checkpoint != null or req.split_replication != null or req.split_transition != null or
            req.merge_source_transition != null or
            req.transforms.len != 0 or req.predicates.len != 0 or req.writes.len != 0 or
            req.graph_writes.len != 0 or req.graph_deletes.len != 0 or req.transaction != null or
            req.deletes.len != 0)
            return error.InvalidBatchRequest;
    }
    if (req.merge_replication != null and
        (req.split_checkpoint != null or req.split_replication != null or req.split_transition != null or
            req.merge_source_transition != null or req.transaction != null or
            req.transforms.len != 0 or req.predicates.len != 0 or
            req.graph_writes.len != 0 or req.graph_deletes.len != 0))
    {
        return error.InvalidBatchRequest;
    }
    if (req.merge_replication) |replication| if (req.merge_checkpoint) |checkpoint| {
        if (replication.transition_id != checkpoint.transition_id or
            replication.donor_group_id != checkpoint.donor_group_id or
            replication.receiver_group_id != checkpoint.receiver_group_id or
            replication.copy_attempt.order(checkpoint.copy_attempt) != .eq)
            return error.InvalidBatchRequest;
    };

    var out: std.Io.Writer.Allocating = .init(alloc);
    errdefer out.deinit();
    const writer = &out.writer;

    try writer.writeAll("{\"inserts\":{");
    for (if (req.merge_page != null) &.{} else req.writes, 0..) |write, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}:", .{std.json.fmt(write.key, .{})});
        try writer.writeAll(write.value);
    }
    try writer.writeAll("},\"deletes\":[");
    for (if (req.merge_page != null) &.{} else req.deletes, 0..) |key, i| {
        if (i != 0) try writer.writeByte(',');
        try writer.print("{f}", .{std.json.fmt(key, .{})});
    }
    try writer.writeAll("]");
    if (req.merge_page != null) {
        // Preserve exact JSON bytes and arbitrary binary row keys; reparsing
        // ordinary inserts would change whitespace/numeric spellings under
        // the already-certified page digest.
        // Row values are logical JSON, not opaque physical envelopes. Encode
        // them as JSON strings: decoding preserves their exact bytes without
        // expanding every byte into a numeric JSON node. Only binary keys need
        // arrays. Large typed/blob rows avoid numeric-array transport overhead.
        try writer.writeAll(",\"_merge_page_effects\":{\"writes\":[");
        for (req.writes, 0..) |write, index| {
            if (index != 0) try writer.writeByte(',');
            try writer.print("{{\"key\":{f},\"value\":{f}}}", .{
                std.json.fmt(write.key, .{ .emit_strings_as_arrays = true }),
                std.json.fmt(write.value, .{}),
            });
        }
        try writer.print("],\"deletes\":{f}}}", .{std.json.fmt(req.deletes, .{ .emit_strings_as_arrays = true })});
    }
    if (req.merge_artifacts.len > 0) {
        // Byte arrays preserve binary keys and opaque payloads exactly.
        try writer.print(",\"_merge_artifacts\":{f}", .{std.json.fmt(req.merge_artifacts, .{ .emit_strings_as_arrays = true })});
    }
    if (req.transforms.len > 0) {
        try writer.writeAll(",\"transforms\":[");
        for (req.transforms, 0..) |transform, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"key\":{f},\"operations\":[", .{std.json.fmt(transform.key, .{})});
            for (transform.operations, 0..) |op, op_index| {
                if (op_index != 0) try writer.writeByte(',');
                try writer.print("{{\"op\":{f},\"path\":{f}", .{
                    std.json.fmt(db_mod.types.transformOpText(op.op), .{}),
                    std.json.fmt(op.path, .{}),
                });
                if (op.value_json) |value_json| {
                    try writer.writeAll(",\"value\":");
                    try writer.writeAll(value_json);
                }
                try writer.writeByte('}');
            }
            try writer.writeByte(']');
            if (transform.upsert) try writer.writeAll(",\"upsert\":true");
            try writer.writeByte('}');
        }
        try writer.writeAll("]");
    }
    if (req.relational_schema_version) |version| try writer.print(",\"_relational_schema_version\":{d}", .{version});
    if (req.relational_repair) try writer.writeAll(",\"_relational_repair\":true");
    if (req.restore_staging_scope) |scope| {
        try writer.writeAll(",\"_restore_staging_scope\":");
        const encoded = try @import("relational_integrity_wire.zig").encodeGenerationSet(alloc, scope);
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.restore_staging_plan_id) |id| {
        if (req.restore_staging_scope == null or std.mem.allEqual(u8, &id, 0)) return error.InvalidBatchRequest;
        try writer.print(",\"_restore_staging_plan_id\":\"{s}\"", .{std.fmt.bytesToHex(id, .lower)});
    }
    if (req.relational_integrity_generation_set) |generation_set| {
        try writer.writeAll(",\"_relational_integrity_generation_set\":");
        const encoded = try @import("relational_integrity_wire.zig").encodeGenerationSet(alloc, generation_set);
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.relational_activation) |activation| {
        try writer.writeAll(",\"_relational_activation\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, activation, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.relational_retirement) |retirement| {
        try writer.writeAll(",\"_relational_retirement\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, retirement, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.relational_index_maintenance) |retirement| {
        try writer.writeAll(",\"_relational_index_maintenance\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, retirement, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.relational_topology) |topology| {
        try writer.writeAll(",\"_relational_topology\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, topology, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.restore_staging) |control| {
        if (req.online_source != null or req.merge_checkpoint != null or req.merge_page != null) return error.InvalidBatchRequest;
        try writer.writeAll(",\"_merge_checkpoint\":{\"kind\":\"restore_v3\"},\"_restore_staging\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, control, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.online_source) |control| {
        // Existing parsers reject this mandatory checkpoint discriminator
        // rather than silently dropping a new source authority command.
        try writer.writeAll(",\"_merge_checkpoint\":{\"kind\":\"online_source_v4\"},\"_online_source\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, control, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.integrity_commands.len != 0) {
        var encoded_commands: std.ArrayList(u8) = .empty;
        defer encoded_commands.deinit(alloc);
        try @import("relational_integrity_wire.zig").appendCommands(alloc, &encoded_commands, req.integrity_commands);
        try writer.writeAll(",\"_integrity_commands\":");
        try writer.writeAll(encoded_commands.items);
    }
    if (req.integrity.len != 0) {
        var encoded_integrity: std.ArrayList(u8) = .empty;
        defer encoded_integrity.deinit(alloc);
        try @import("relational_integrity_wire.zig").append(alloc, &encoded_integrity, req.integrity);
        try writer.writeAll(",\"_integrity\":");
        try writer.writeAll(encoded_integrity.items);
    }
    if (req.graph_writes.len > 0) {
        try writer.writeAll(",\"_graph_writes\":[");
        for (req.graph_writes, 0..) |write, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"index_name\":{f},\"source\":{f},\"target\":{f},\"edge_type\":{f},\"weight\":{d},\"created_at\":\"{d}\",\"updated_at\":\"{d}\",\"metadata_json\":{f}}}", .{
                std.json.fmt(write.index_name, .{}),
                std.json.fmt(write.source, .{}),
                std.json.fmt(write.target, .{}),
                std.json.fmt(write.edge_type, .{}),
                write.weight,
                write.created_at,
                write.updated_at,
                std.json.fmt(write.metadata_json, .{}),
            });
        }
        try writer.writeByte(']');
    }
    if (req.graph_deletes.len > 0) {
        try writer.writeAll(",\"_graph_deletes\":[");
        for (req.graph_deletes, 0..) |delete, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"index_name\":{f},\"source\":{f},\"target\":{f},\"edge_type\":{f}}}", .{
                std.json.fmt(delete.index_name, .{}),
                std.json.fmt(delete.source, .{}),
                std.json.fmt(delete.target, .{}),
                std.json.fmt(delete.edge_type, .{}),
            });
        }
        try writer.writeByte(']');
    }
    if (req.predicates.len > 0) {
        try writer.writeAll(",\"_predicates\":[");
        for (req.predicates, 0..) |predicate, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"key\":{f},\"expected_version\":\"{d}\"", .{
                std.json.fmt(predicate.key, .{}), predicate.expected_version,
            });
            if (predicate.expected_content_digest) |digest| try writer.print(",\"expected_content_digest\":\"{s}\"", .{std.fmt.bytesToHex(digest, .lower)});
            try writer.writeByte('}');
        }
        try writer.writeByte(']');
    }
    if (req.split_checkpoint) |checkpoint| {
        try writer.print(",\"_split_checkpoint\":{{\"kind\":{f},\"transition_id\":\"{d}\",\"attempt_epoch\":\"{d}\",\"source_group_id\":\"{d}\",\"destination_group_id\":\"{d}\",\"range_start\":{f},\"range_end\":{f},\"delta_sequence\":\"{d}\"}}", .{
            std.json.fmt(@tagName(checkpoint.kind), .{}),
            checkpoint.transition_id,
            checkpoint.attempt_epoch,
            checkpoint.source_group_id,
            checkpoint.destination_group_id,
            std.json.fmt(checkpoint.range_start, .{}),
            std.json.fmt(checkpoint.range_end, .{}),
            checkpoint.delta_sequence,
        });
    }
    if (req.split_replication) |replication| {
        try writer.print(",\"_split_replication\":{{\"transition_id\":\"{d}\",\"attempt_epoch\":\"{d}\",\"source_group_id\":\"{d}\",\"destination_group_id\":\"{d}\",\"namespace_table_id\":\"{d}\",\"namespace_shard_id\":\"{d}\",\"namespace_range_id\":\"{d}\",\"operation\":{f},\"sequence\":\"{d}\"", .{
            replication.transition_id,
            replication.attempt_epoch,
            replication.source_group_id,
            replication.destination_group_id,
            replication.identity_namespace.table_id,
            replication.identity_namespace.shard_id,
            replication.identity_namespace.range_id,
            std.json.fmt(@tagName(replication.operation), .{}),
            replication.sequence,
        });
        if (replication.bootstrap_sequence) |sequence| {
            try writer.print(",\"bootstrap_sequence\":\"{d}\"", .{sequence});
        }
        if (replication.previous_sequence) |sequence| {
            try writer.print(",\"previous_sequence\":\"{d}\"", .{sequence});
        }
        try writer.writeByte('}');
    }
    if (req.merge_replication) |replication| {
        try writer.print(",\"_merge_replication\":{{\"transition_id\":\"{d}\",\"donor_group_id\":\"{d}\",\"receiver_group_id\":\"{d}\",\"namespace_table_id\":\"{d}\",\"namespace_shard_id\":\"{d}\",\"namespace_range_id\":\"{d}\",\"copy_donor_term\":\"{d}\",\"copy_sequence\":\"{d}\"}}", .{
            replication.transition_id,
            replication.donor_group_id,
            replication.receiver_group_id,
            replication.identity_namespace.table_id,
            replication.identity_namespace.shard_id,
            replication.identity_namespace.range_id,
            replication.copy_attempt.donor_term,
            replication.copy_attempt.sequence,
        });
    }
    if (req.split_transition) |transition| {
        try writer.print(",\"_split_transition\":{{\"kind\":{f},\"transition_id\":\"{d}\",\"attempt_epoch\":\"{d}\",\"destination_group_id\":\"{d}\",\"split_key\":{f}}}", .{
            std.json.fmt(@tagName(transition.kind), .{}),
            transition.transition_id,
            transition.attempt_epoch,
            transition.destination_group_id,
            std.json.fmt(transition.split_key, .{}),
        });
    }
    if (req.timestamp_ns != 0) {
        try writer.print(",\"_timestamp_ns\":\"{d}\"", .{req.timestamp_ns});
    }
    if (req.reject_graph_transform_projections) {
        try writer.writeAll(",\"_reject_graph_transform_projections\":true");
    }
    if (req.merge_source_transition) |transition| {
        try writer.print(",\"_merge_source_transition\":{{\"kind\":{f},\"transition_id\":\"{d}\",\"receiver_group_id\":\"{d}\"}}", .{
            std.json.fmt(@tagName(transition.kind), .{}),
            transition.transition_id,
            transition.receiver_group_id,
        });
    }
    if (req.merge_checkpoint) |checkpoint| {
        const kind = if (checkpoint.page_source) |source|
            try std.fmt.allocPrint(alloc, "{s}_{s}", .{ if (source.retention != null) "page_v4" else if (source.integrity != null) "page_v3" else "page_v1", @tagName(checkpoint.kind) })
        else
            try alloc.dupe(u8, @tagName(checkpoint.kind));
        defer alloc.free(kind);
        try writer.print(",\"_merge_checkpoint\":{{\"kind\":{f},\"transition_id\":\"{d}\",\"donor_group_id\":\"{d}\",\"receiver_group_id\":\"{d}\",\"receiver_base_start\":{f},\"receiver_base_end\":{f},\"merged_start\":{f},\"merged_end\":{f},\"bootstrap_applied_index\":\"{d}\",\"allow_doc_identity_reassignment\":{},\"copy_donor_term\":\"{d}\",\"copy_sequence\":\"{d}\"", .{
            std.json.fmt(kind, .{}),
            checkpoint.transition_id,
            checkpoint.donor_group_id,
            checkpoint.receiver_group_id,
            std.json.fmt(checkpoint.receiver_base_start, .{}),
            std.json.fmt(checkpoint.receiver_base_end, .{}),
            std.json.fmt(checkpoint.merged_start, .{}),
            std.json.fmt(checkpoint.merged_end, .{}),
            checkpoint.bootstrap_applied_index,
            checkpoint.allow_doc_identity_reassignment,
            checkpoint.copy_attempt.donor_term,
            checkpoint.copy_attempt.sequence,
        });
        if (checkpoint.receiver_identity_reassignment_namespace) |namespace| {
            try writer.print(",\"namespace_table_id\":\"{d}\",\"namespace_shard_id\":\"{d}\",\"namespace_range_id\":\"{d}\"", .{
                namespace.table_id,
                namespace.shard_id,
                namespace.range_id,
            });
        }
        if (checkpoint.page_source) |source| {
            try writer.print(",\"page_source\":{f},\"page_receiver_namespace\":{f}", .{
                std.json.fmt(source, .{}), std.json.fmt(checkpoint.page_receiver_namespace.?, .{}),
            });
        }
        try writer.writeByte('}');
    }
    if (req.merge_page) |page| {
        if (page.chunk != null and req.writes.len != 0) return error.InvalidBatchRequest;
        try writer.print(",\"_merge_checkpoint\":{{\"kind\":\"{s}\"}},\"_merge_page\":", .{if (page.source.retention != null) "page_v6" else if (page.source.integrity != null) "page_v5" else if (page.next_snapshot_position != null) "page_v4" else if (page.chunk != null) "page_v3" else "page_v1"});
        const encoded = try @import("../storage/db/merge_page_wire.zig").encodeAlloc(alloc, page);
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.transaction) |mutation| {
        try writer.writeAll(",\"_transaction\":{");
        switch (mutation) {
            .begin => |begin| {
                const txn_hex = std.fmt.bytesToHex(begin.txn_id, .lower);
                try writer.print("\"phase\":\"begin\",\"txn_id\":\"{s}\",\"begin_timestamp\":\"{d}\",\"created_at_ns\":\"{d}\",\"topology_epoch\":\"{d}\",\"retain_terminal\":{},\"participants\":[", .{
                    &txn_hex, begin.begin_timestamp, begin.created_at_ns, begin.topology_epoch, begin.retain_terminal,
                });
                for (begin.participants, 0..) |participant, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writer.print("{f}", .{std.json.fmt(participant, .{})});
                }
                try writer.writeByte(']');
            },
            .prepare => |prepare| {
                const txn_hex = std.fmt.bytesToHex(prepare.txn_id, .lower);
                try writer.print("\"phase\":\"prepare\",\"txn_id\":\"{s}\",\"topology_epoch\":\"{d}\"", .{ &txn_hex, prepare.topology_epoch });
            },
            .resolve => |resolve| {
                const txn_hex = std.fmt.bytesToHex(resolve.txn_id, .lower);
                try writer.print("\"phase\":\"resolve\",\"txn_id\":\"{s}\",\"status\":{f},\"commit_version\":\"{d}\"", .{
                    &txn_hex, std.json.fmt(@tagName(resolve.status), .{}), resolve.commit_version,
                });
            },
            .acknowledge => |ack| {
                const txn_hex = std.fmt.bytesToHex(ack.txn_id, .lower);
                try writer.print("\"phase\":\"acknowledge\",\"txn_id\":\"{s}\",\"participant\":{f}", .{
                    &txn_hex, std.json.fmt(ack.participant, .{}),
                });
            },
            .cleanup => |cleanup| {
                const txn_hex = std.fmt.bytesToHex(cleanup.txn_id, .lower);
                try writer.print("\"phase\":\"cleanup\",\"txn_id\":\"{s}\",\"cutoff_timestamp\":\"{d}\",\"retained_cutoff_timestamp\":\"{d}\"", .{
                    &txn_hex, cleanup.cutoff_timestamp, cleanup.retained_cutoff_timestamp,
                });
            },
        }
        try writer.writeByte('}');
    }
    try writer.print(",\"sync_level\":\"{s}\"}}", .{syncLevelName(req.sync_level)});
    return try out.toOwnedSlice();
}

fn syncLevelName(sync_level: db_mod.types.SyncLevel) []const u8 {
    return switch (sync_level) {
        .propose => "propose",
        .write => "write",
        .full_text => "full_text",
        .enrichments => "enrichments",
        .full_index => "full_index",
    };
}

fn parseInternalU64(value: std.json.Value) !u64 {
    return switch (value) {
        .integer => |number| if (number >= 0) @intCast(number) else error.InvalidBatchRequest,
        .number_string, .string => |text| blk: {
            if (text.len == 0) break :blk error.InvalidBatchRequest;
            for (text) |byte| if (byte < '0' or byte > '9') break :blk error.InvalidBatchRequest;
            break :blk std.fmt.parseUnsigned(u64, text, 10) catch error.InvalidBatchRequest;
        },
        else => error.InvalidBatchRequest,
    };
}

fn parseInserts(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    require_document_objects: bool,
) ![]db_mod.types.BatchWrite {
    if (value != .object) return error.InvalidBatchRequest;
    const inserts = value.object;
    const writes = try alloc.alloc(db_mod.types.BatchWrite, inserts.count());
    var initialized: usize = 0;
    errdefer {
        for (writes[0..initialized]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        alloc.free(writes);
    }

    var it = inserts.iterator();
    while (it.next()) |entry| {
        // Public BatchRequest.inserts is map[string]object in OpenAPI. Enforce
        // that contract before storage so every admitted document can be
        // returned through QueryHit._source. Internal decoding stays opaque to
        // preserve replay and movement of durable data written by older nodes.
        if (require_document_objects and entry.value_ptr.* != .object)
            return error.InvalidBatchRequest;
        writes[initialized] = .{
            .key = try alloc.dupe(u8, entry.key_ptr.*),
            .value = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.*, .{}),
        };
        initialized += 1;
    }
    return writes;
}

fn parseDeletes(alloc: std.mem.Allocator, value: std.json.Value) ![][]const u8 {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const deletes = try alloc.alloc([]const u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (deletes[0..initialized]) |key| alloc.free(key);
        alloc.free(deletes);
    }
    for (values) |item| {
        if (item != .string) return error.InvalidBatchRequest;
        deletes[initialized] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return deletes;
}

fn requiredObjectString(object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidBatchRequest;
    if (value != .string) return error.InvalidBatchRequest;
    return value.string;
}

fn optionalObjectU64(object: std.json.ObjectMap, name: []const u8) !u64 {
    const value = object.get(name) orelse return 0;
    return try parseInternalU64(value);
}

fn optionalObjectF64(object: std.json.ObjectMap, name: []const u8, default: f64) !f64 {
    const value = object.get(name) orelse return default;
    return switch (value) {
        .integer => |number| @floatFromInt(number),
        .float => |number| number,
        .number_string, .string => |text| std.fmt.parseFloat(f64, text) catch error.InvalidBatchRequest,
        else => error.InvalidBatchRequest,
    };
}

fn parseGraphWrites(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.GraphEdgeWrite {
    if (value != .array) return error.InvalidBatchRequest;
    const writes = try alloc.alloc(db_mod.types.GraphEdgeWrite, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        freeGraphWriteElements(alloc, writes[0..initialized]);
        alloc.free(writes);
    }
    for (value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidBatchRequest;
        const metadata_json = if (item.object.get("metadata_json")) |metadata| blk: {
            if (metadata != .string) return error.InvalidBatchRequest;
            break :blk metadata.string;
        } else "";
        const index_name = try alloc.dupe(u8, try requiredObjectString(item.object, "index_name"));
        errdefer alloc.free(index_name);
        const source = try alloc.dupe(u8, try requiredObjectString(item.object, "source"));
        errdefer alloc.free(source);
        const target = try alloc.dupe(u8, try requiredObjectString(item.object, "target"));
        errdefer alloc.free(target);
        const edge_type = try alloc.dupe(u8, try requiredObjectString(item.object, "edge_type"));
        errdefer alloc.free(edge_type);
        const owned_metadata_json = try alloc.dupe(u8, metadata_json);
        errdefer alloc.free(owned_metadata_json);
        writes[i] = .{
            .index_name = index_name,
            .source = source,
            .target = target,
            .edge_type = edge_type,
            .weight = try optionalObjectF64(item.object, "weight", 1.0),
            .created_at = try optionalObjectU64(item.object, "created_at"),
            .updated_at = try optionalObjectU64(item.object, "updated_at"),
            .metadata_json = owned_metadata_json,
        };
        initialized += 1;
    }
    return writes;
}

fn parseGraphDeletes(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.GraphEdgeDelete {
    if (value != .array) return error.InvalidBatchRequest;
    const deletes = try alloc.alloc(db_mod.types.GraphEdgeDelete, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        freeGraphDeleteElements(alloc, deletes[0..initialized]);
        alloc.free(deletes);
    }
    for (value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidBatchRequest;
        const index_name = try alloc.dupe(u8, try requiredObjectString(item.object, "index_name"));
        errdefer alloc.free(index_name);
        const source = try alloc.dupe(u8, try requiredObjectString(item.object, "source"));
        errdefer alloc.free(source);
        const target = try alloc.dupe(u8, try requiredObjectString(item.object, "target"));
        errdefer alloc.free(target);
        const edge_type = try alloc.dupe(u8, try requiredObjectString(item.object, "edge_type"));
        errdefer alloc.free(edge_type);
        deletes[i] = .{
            .index_name = index_name,
            .source = source,
            .target = target,
            .edge_type = edge_type,
        };
        initialized += 1;
    }
    return deletes;
}

fn parseTransforms(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.DocumentTransform {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const transforms = try alloc.alloc(db_mod.types.DocumentTransform, values.len);
    var initialized: usize = 0;
    errdefer {
        freeTransformElements(alloc, transforms[0..initialized]);
        alloc.free(transforms);
    }

    for (values) |item| {
        if (item != .object) return error.InvalidBatchRequest;
        const key_value = item.object.get("key") orelse return error.InvalidBatchRequest;
        if (key_value != .string) return error.InvalidBatchRequest;
        const operations_value = item.object.get("operations") orelse return error.InvalidBatchRequest;
        const operations = try parseTransformOps(alloc, operations_value);
        errdefer freeTransformOps(alloc, operations);
        const key = try alloc.dupe(u8, key_value.string);
        errdefer alloc.free(key);
        const upsert = if (item.object.get("upsert")) |upsert_value| blk: {
            if (upsert_value == .null) break :blk false;
            if (upsert_value != .bool) return error.InvalidBatchRequest;
            break :blk upsert_value.bool;
        } else false;

        transforms[initialized] = .{
            .key = key,
            .operations = operations,
            .upsert = upsert,
        };
        initialized += 1;
    }
    return transforms;
}

fn parseTransformOps(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.TransformOp {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const ops = try alloc.alloc(db_mod.types.TransformOp, values.len);
    var initialized: usize = 0;
    errdefer {
        freeTransformOpElements(alloc, ops[0..initialized]);
        alloc.free(ops);
    }

    for (values) |item| {
        if (item != .object) return error.InvalidBatchRequest;
        const op_value = item.object.get("op") orelse return error.InvalidBatchRequest;
        if (op_value != .string) return error.InvalidBatchRequest;
        const path_value = item.object.get("path") orelse return error.InvalidBatchRequest;
        if (path_value != .string) return error.InvalidBatchRequest;
        const op = try transformOpTypeFromString(op_value.string);
        const value_json = if (item.object.get("value")) |raw| try std.json.Stringify.valueAlloc(alloc, raw, .{}) else null;
        errdefer if (value_json) |json| alloc.free(json);
        const path = try alloc.dupe(u8, path_value.string);
        errdefer alloc.free(path);
        const parsed_op: db_mod.types.TransformOp = .{
            .op = op,
            .path = path,
            .value_json = value_json,
        };
        db_mod.transform.validateDocumentTransform(alloc, .{
            .key = "",
            .operations = @constCast((&[_]db_mod.types.TransformOp{parsed_op})[0..]),
        }) catch return error.InvalidBatchRequest;
        ops[initialized] = parsed_op;
        initialized += 1;
    }
    return ops;
}

fn transformOpTypeFromString(op: []const u8) !db_mod.types.TransformOpType {
    if (std.mem.eql(u8, op, "$set")) return .set;
    if (std.mem.eql(u8, op, "$setOnInsert")) return .set_on_insert;
    if (std.mem.eql(u8, op, "$unset")) return .unset;
    if (std.mem.eql(u8, op, "$inc")) return .inc;
    if (std.mem.eql(u8, op, "$push")) return .push;
    if (std.mem.eql(u8, op, "$pull")) return .pull;
    if (std.mem.eql(u8, op, "$addToSet")) return .add_to_set;
    if (std.mem.eql(u8, op, "$min")) return .min;
    if (std.mem.eql(u8, op, "$max")) return .max;
    return error.InvalidBatchRequest;
}

fn syncLevelFromValue(value: std.json.Value) !db_mod.types.SyncLevel {
    if (value != .string) return error.InvalidBatchRequest;
    const level = value.string;
    if (std.mem.eql(u8, level, "propose")) return .propose;
    if (std.mem.eql(u8, level, "write")) return .write;
    if (std.mem.eql(u8, level, "full_text")) return .full_text;
    if (std.mem.eql(u8, level, "enrichments")) return .enrichments;
    if (std.mem.eql(u8, level, "full_index")) return .full_index;
    return error.InvalidBatchRequest;
}

fn freeWrites(alloc: std.mem.Allocator, writes: []db_mod.types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (writes.len > 0) alloc.free(writes);
}

fn freeDeletes(alloc: std.mem.Allocator, deletes: [][]const u8) void {
    for (deletes) |key| alloc.free(key);
    if (deletes.len > 0) alloc.free(deletes);
}

fn freeTransforms(alloc: std.mem.Allocator, transforms: []db_mod.types.DocumentTransform) void {
    freeTransformElements(alloc, transforms);
    if (transforms.len > 0) alloc.free(transforms);
}

fn freeTransformElements(alloc: std.mem.Allocator, transforms: []db_mod.types.DocumentTransform) void {
    for (transforms) |transform| {
        alloc.free(@constCast(transform.key));
        freeTransformOps(alloc, transform.operations);
    }
}

fn freeGraphWrites(alloc: std.mem.Allocator, writes: []db_mod.types.GraphEdgeWrite) void {
    freeGraphWriteElements(alloc, writes);
    if (writes.len > 0) alloc.free(writes);
}

fn freeGraphWriteElements(alloc: std.mem.Allocator, writes: []db_mod.types.GraphEdgeWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.index_name));
        alloc.free(@constCast(write.source));
        alloc.free(@constCast(write.target));
        alloc.free(@constCast(write.edge_type));
        alloc.free(@constCast(write.metadata_json));
    }
}

fn freeGraphDeletes(alloc: std.mem.Allocator, deletes: []db_mod.types.GraphEdgeDelete) void {
    freeGraphDeleteElements(alloc, deletes);
    if (deletes.len > 0) alloc.free(deletes);
}

fn freeGraphDeleteElements(alloc: std.mem.Allocator, deletes: []db_mod.types.GraphEdgeDelete) void {
    for (deletes) |delete| {
        alloc.free(@constCast(delete.index_name));
        alloc.free(@constCast(delete.source));
        alloc.free(@constCast(delete.target));
        alloc.free(@constCast(delete.edge_type));
    }
}

fn freeTransformOps(alloc: std.mem.Allocator, ops: []const db_mod.types.TransformOp) void {
    freeTransformOpElements(alloc, ops);
    if (ops.len > 0) alloc.free(@constCast(ops));
}

fn freeTransformOpElements(alloc: std.mem.Allocator, ops: []const db_mod.types.TransformOp) void {
    for (ops) |op| {
        alloc.free(@constCast(op.path));
        if (op.value_json) |value_json| alloc.free(@constCast(value_json));
    }
}

pub const consumer_tests = consumerTests();
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const test_owner_root = @import("antfly_source_root");
    if (@hasDecl(test_owner_root, "implementation_tests_only") and test_owner_root.implementation_tests_only) return struct {};
    const Suite = struct {
        test "internal batch integrity commands and schema fences round trip without public injection" {
            const alloc = std.testing.allocator;
            const integrity_mod = @import("../storage/db/relational_integrity_contract.zig");
            const address = try integrity_mod.Address.init(@splat(7), "binary\x00\xfftuple");
            const command: integrity_mod.Command = .{ .address = address, .operation = .{ .release = .{ .parent_table = "parent", .parent_key = "key\x00\xff" } } };
            const encoded = try encodeBatchRequest(alloc, .{
                .relational_schema_version = 17,
                .relational_integrity_generation_set = [_]u8{5} ** 32,
                .integrity_commands = &.{command},
                .transaction = .{ .prepare = .{ .txn_id = @splat(4), .topology_epoch = 1 } },
            });
            defer alloc.free(encoded);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
            var parsed = try parseInternalBatchRequest(alloc, encoded);
            defer parsed.deinit(alloc);
            try std.testing.expectEqual(@as(?u32, 17), parsed.req.relational_schema_version);
            try std.testing.expectEqual(@as(?[32]u8, [_]u8{5} ** 32), parsed.req.relational_integrity_generation_set);
            try std.testing.expectEqualSlices(u8, &address.routing, &parsed.req.integrity_commands[0].address.routing);
            try std.testing.expectEqualStrings("key\x00\xff", parsed.req.integrity_commands[0].operation.release.parent_key);
        }

        test "internal batch topology control preserves binary fences and refuses public injection" {
            const alloc = std.testing.allocator;
            const topology = @import("../storage/db/relational_integrity_topology_contract.zig");
            const fence: topology.Fence = .{
                .transition_id = std.math.maxInt(u64),
                .admission_epoch = 1,
                .attempt = 2,
                .owner_group_id = 301,
                .peer_group_id = 301,
                .role = .backup_snapshot,
                .namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 },
                .catalog_digest = @splat(255),
            };
            const encoded = try encodeBatchRequest(alloc, .{ .relational_topology = .{ .fence = fence, .action = .begin } });
            defer alloc.free(encoded);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
            var parsed = try parseInternalBatchRequest(alloc, encoded);
            defer parsed.deinit(alloc);
            try std.testing.expect(fence.eql(parsed.req.relational_topology.?.fence));
            try std.testing.expectEqual(.begin, parsed.req.relational_topology.?.action);
            try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{
                .relational_topology = .{ .fence = fence, .action = .release },
                .deletes = &.{"user-row"},
            }));
        }

        test "distributed txn public batch rejects isolated coordinator generation evidence spoof" {
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, "{\"_relational_repair\":true}"));
            const alloc = std.testing.allocator;
            const encoded = try @import("relational_integrity_wire.zig").encodeGenerationSet(alloc, [_]u8{7} ** 32);
            defer alloc.free(encoded);
            const body = try std.fmt.allocPrint(alloc, "{{\"_relational_integrity_generation_set\":{s}}}", .{encoded});
            defer alloc.free(body);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, body));
            var internal = try parseInternalBatchRequest(alloc, body);
            defer internal.deinit(alloc);
            try std.testing.expectEqual(@as(?[32]u8, [_]u8{7} ** 32), internal.req.relational_integrity_generation_set);
        }

        test "internal batch parser owns binary staged restore controls and rejects public injection" {
            const alloc = std.testing.allocator;
            const encoded = try encodeBatchRequest(alloc, .{ .restore_staging = .{ .import_page = .{
                .expected = @splat(201),
                .next = &.{ 0, 255, 128, 1 },
                .scope = @splat(222),
                .timestamps = &.{.{ .key = &.{ 0, 255, 127 }, .timestamp = 9_007_199_254_740_993 }},
            } } });
            defer alloc.free(encoded);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
            var parsed = try parseInternalBatchRequest(alloc, encoded);
            defer parsed.deinit(alloc);
            const page = parsed.req.restore_staging.?.import_page;
            try std.testing.expectEqualSlices(u8, &.{ 0, 255, 128, 1 }, page.next);
            try std.testing.expectEqualSlices(u8, &.{ 0, 255, 127 }, page.timestamps[0].key);
            try std.testing.expectEqual(@as(u64, 9_007_199_254_740_993), page.timestamps[0].timestamp);
            const marker = std.mem.indexOf(u8, encoded, "restore_v3").?;
            for ([_]u8{ '1', '2' }) |version| {
                encoded[marker + "restore_v".len] = version;
                try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, encoded));
            }
        }

        test "distributed txn internal batch codec round trips replicated transaction phases" {
            const alloc = std.testing.allocator;
            const txn_id: db_mod.types.TxnId = .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
            const encoded = try encodeBatchRequest(alloc, .{
                .writes = &.{.{ .key = "doc:a", .value = "{}" }},
                .predicates = &.{.{ .key = "doc:a", .expected_version = 41, .expected_content_digest = @splat(7) }},
                .transaction = .{ .prepare = .{ .txn_id = txn_id, .topology_epoch = 7 } },
            });
            defer alloc.free(encoded);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));

            var decoded = try parseInternalBatchRequest(alloc, encoded);
            defer decoded.deinit(alloc);
            const prepare = switch (decoded.req.transaction orelse return error.TestExpectedEqual) {
                .prepare => |value| value,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqual(txn_id, prepare.txn_id);
            try std.testing.expectEqual(@as(u64, 7), prepare.topology_epoch);
            try std.testing.expectEqual(@as(usize, 1), decoded.req.writes.len);
            try std.testing.expectEqual(@as(u64, 41), decoded.req.predicates[0].expected_version);
            try std.testing.expectEqual([_]u8{7} ** 32, decoded.req.predicates[0].expected_content_digest.?);

            const begin_encoded = try encodeBatchRequest(alloc, .{
                .transaction = .{ .begin = .{
                    .txn_id = txn_id,
                    .begin_timestamp = 42,
                    .created_at_ns = 43,
                    .topology_epoch = 7,
                    .retain_terminal = true,
                    .participants = &.{"table2:4:docs:group:7"},
                } },
            });
            defer alloc.free(begin_encoded);
            var begin_decoded = try parseInternalBatchRequest(alloc, begin_encoded);
            defer begin_decoded.deinit(alloc);
            const begin = switch (begin_decoded.req.transaction.?) {
                .begin => |value| value,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expect(begin.retain_terminal);
            try std.testing.expectEqualStrings("table2:4:docs:group:7", begin.participants[0]);

            const ack_encoded = try encodeBatchRequest(alloc, .{
                .transaction = .{ .acknowledge = .{
                    .txn_id = txn_id,
                    .participant = "table2:4:docs:group:8",
                } },
            });
            defer alloc.free(ack_encoded);
            var ack_decoded = try parseInternalBatchRequest(alloc, ack_encoded);
            defer ack_decoded.deinit(alloc);
            const ack = switch (ack_decoded.req.transaction.?) {
                .acknowledge => |value| value,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqualStrings("table2:4:docs:group:8", ack.participant);

            const cleanup_encoded = try encodeBatchRequest(alloc, .{
                .transaction = .{ .cleanup = .{
                    .txn_id = txn_id,
                    .cutoff_timestamp = 100,
                    .retained_cutoff_timestamp = 50,
                } },
            });
            defer alloc.free(cleanup_encoded);
            var cleanup_decoded = try parseInternalBatchRequest(alloc, cleanup_encoded);
            defer cleanup_decoded.deinit(alloc);
            const cleanup = switch (cleanup_decoded.req.transaction.?) {
                .cleanup => |value| value,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqual(@as(u64, 100), cleanup.cutoff_timestamp);
            try std.testing.expectEqual(@as(u64, 50), cleanup.retained_cutoff_timestamp);
        }

        test "batch parser accepts inserts and deletes" {
            var owned = try parseBatchRequest(std.testing.allocator,
                \\{"inserts":{"doc:a":{"title":"alpha"}},"deletes":["doc:b"]}
            );
            defer owned.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(usize, 1), owned.writes.len);
            try std.testing.expectEqual(@as(usize, 1), owned.deletes.len);
        }

        test "public batch parser rejects non-object documents while internal replay remains opaque" {
            const alloc = std.testing.allocator;
            inline for (.{
                \\{"inserts":{"doc:a":"text"}}
                ,
                \\{"inserts":{"doc:a":42}}
                ,
                \\{"inserts":{"doc:a":[1,2]}}
                ,
                \\{"inserts":{"doc:a":null}}
                ,
            }) |body| {
                try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, body));
            }

            var replay = try parseInternalBatchRequest(alloc,
                \\{"inserts":{"doc:a":"legacy durable value"}}
            );
            defer replay.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), replay.writes.len);
            try std.testing.expectEqualStrings("\"legacy durable value\"", replay.writes[0].value);
        }

        test "batch parser preserves oversized value errors" {
            const body =
                \\{"inserts":{"doc:a":{"raw_payload":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}}
            ;
            try std.testing.expectError(error.ValueTooLong, parseBatchRequestWithOptions(std.testing.allocator, body, .{ .allocate = .alloc_always, .max_value_len = 64 }, false));
        }

        test "internal batch parser owns and round trips split checkpoint" {
            const body =
                \\{"inserts":{},"deletes":[],"_split_checkpoint":{"kind":"destination_complete","transition_id":40,"attempt_epoch":1,"source_group_id":41,"destination_group_id":42,"range_start":"doc:m","range_end":"doc:z","delta_sequence":7}}
            ;
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, body));

            var owned = try parseInternalBatchRequest(std.testing.allocator, body);
            defer owned.deinit(std.testing.allocator);
            const checkpoint = owned.req.split_checkpoint orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(db_mod.types.SplitReplicationCheckpoint.Kind.destination_complete, checkpoint.kind);
            try std.testing.expectEqual(@as(u64, 40), checkpoint.transition_id);
            try std.testing.expectEqual(@as(u64, 41), checkpoint.source_group_id);
            try std.testing.expectEqual(@as(u64, 42), checkpoint.destination_group_id);
            try std.testing.expectEqualStrings("doc:m", checkpoint.range_start);
            try std.testing.expectEqualStrings("doc:z", checkpoint.range_end);
            try std.testing.expectEqual(@as(u64, 7), checkpoint.delta_sequence);

            const encoded = try encodeBatchRequest(std.testing.allocator, owned.req);
            defer std.testing.allocator.free(encoded);
            var reparsed = try parseInternalBatchRequest(std.testing.allocator, encoded);
            defer reparsed.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(u64, 7), reparsed.req.split_checkpoint.?.delta_sequence);
        }

        test "internal batch parser owns and round trips graph mutations" {
            const alloc = std.testing.allocator;
            const request: db_mod.types.BatchRequest = .{
                .graph_writes = &.{.{
                    .index_name = "relations_graph",
                    .source = "doc:a",
                    .target = "doc:b",
                    .edge_type = "mentions",
                    .weight = 0.75,
                    .created_at = 11,
                    .updated_at = 12,
                    .metadata_json = "{\"target_table\":\"entities\"}",
                }},
                .graph_deletes = &.{.{
                    .index_name = "relations_graph",
                    .source = "doc:c",
                    .target = "doc:d",
                    .edge_type = "mentions",
                }},
            };
            const encoded = try encodeBatchRequest(alloc, request);
            defer alloc.free(encoded);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));

            var parsed = try parseInternalBatchRequest(alloc, encoded);
            defer parsed.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 1), parsed.req.graph_writes.len);
            try std.testing.expectEqualStrings("relations_graph", parsed.req.graph_writes[0].index_name);
            try std.testing.expectEqualStrings("doc:b", parsed.req.graph_writes[0].target);
            try std.testing.expectEqual(@as(f64, 0.75), parsed.req.graph_writes[0].weight);
            try std.testing.expectEqual(@as(u64, 11), parsed.req.graph_writes[0].created_at);
            try std.testing.expectEqualStrings("{\"target_table\":\"entities\"}", parsed.req.graph_writes[0].metadata_json);
            try std.testing.expectEqual(@as(usize, 1), parsed.req.graph_deletes.len);
            try std.testing.expectEqualStrings("doc:d", parsed.req.graph_deletes[0].target);
        }

        test "internal batch parser requires source acknowledgements to be metadata-only" {
            const mixed =
                \\{"inserts":{"doc:m":{}},"_split_checkpoint":{"kind":"source_ack","transition_id":40,"attempt_epoch":1,"source_group_id":41,"destination_group_id":42,"delta_sequence":7}}
            ;
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(std.testing.allocator, mixed));
            try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(std.testing.allocator, .{
                .writes = &.{.{ .key = "doc:m", .value = "{}" }},
                .split_checkpoint = .{
                    .kind = .source_ack,
                    .transition_id = 40,
                    .attempt_epoch = 1,
                    .source_group_id = 41,
                    .destination_group_id = 42,
                    .delta_sequence = 7,
                },
            }));
        }

        test "internal batch codec preserves timestamps and rejects public injection" {
            const alloc = std.testing.allocator;
            const encoded = try encodeBatchRequest(alloc, .{
                .writes = &.{.{ .key = "doc:a", .value = "{}" }},
                .timestamp_ns = std.math.maxInt(u64),
            });
            defer alloc.free(encoded);

            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
            var decoded = try parseInternalBatchRequest(alloc, encoded);
            defer decoded.deinit(alloc);
            try std.testing.expectEqual(std.math.maxInt(u64), decoded.req.timestamp_ns);

            try std.testing.expectError(
                error.InvalidBatchRequest,
                parseBatchRequest(alloc,
                    \\{"inserts":{},"_timestamp_ns":"123"}
                ),
            );
        }

        test "internal batch codec preserves graph transform projection rejection" {
            const alloc = std.testing.allocator;
            const encoded = try encodeBatchRequest(alloc, .{
                .transforms = &.{.{
                    .key = "doc:a",
                    .operations = &.{.{
                        .op = .push,
                        .path = "$._edges.graph_idx.knows",
                        .value_json = "{\"target\":\"doc:b\"}",
                    }},
                }},
                .reject_graph_transform_projections = true,
            });
            defer alloc.free(encoded);

            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
            var decoded = try parseInternalBatchRequest(alloc, encoded);
            defer decoded.deinit(alloc);
            try std.testing.expect(decoded.req.reject_graph_transform_projections);
        }

        test "internal batch parser rejects public split replication identity" {
            const body =
                \\{"inserts":{"doc:m":{}},"_split_replication":{"transition_id":40,"attempt_epoch":1,"source_group_id":41,"destination_group_id":42,"namespace_table_id":7,"namespace_shard_id":41,"namespace_range_id":4100,"operation":"bootstrap_chunk","sequence":0}}
            ;
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, body));

            var owned = try parseInternalBatchRequest(std.testing.allocator, body);
            defer owned.deinit(std.testing.allocator);
            const replication = owned.req.split_replication orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(@as(u64, 40), replication.transition_id);
            try std.testing.expectEqual(@as(u64, 1), replication.attempt_epoch);
            try std.testing.expectEqual(@as(u64, 41), replication.source_group_id);
            try std.testing.expectEqual(@as(u64, 42), replication.destination_group_id);
            try std.testing.expectEqual(@as(u64, 4100), replication.identity_namespace.range_id);
        }

        test "internal batch split identity round trips the full u64 id space" {
            const max = std.math.maxInt(u64);
            const encoded = try encodeBatchRequest(std.testing.allocator, .{
                .writes = &.{.{ .key = "doc:m", .value = "{}" }},
                .split_replication = .{
                    .transition_id = max - 5,
                    .attempt_epoch = 1,
                    .source_group_id = max - 4,
                    .destination_group_id = max - 3,
                    .identity_namespace = .{
                        .table_id = max,
                        .shard_id = max - 1,
                        .range_id = max - 2,
                    },
                    .operation = .delta,
                    .sequence = max - 6,
                    .previous_sequence = max - 7,
                },
            });
            defer std.testing.allocator.free(encoded);
            try std.testing.expect(std.mem.indexOf(u8, encoded, "\"namespace_table_id\":\"18446744073709551615\"") != null);

            var parsed = try parseInternalBatchRequest(std.testing.allocator, encoded);
            defer parsed.deinit(std.testing.allocator);
            const replication = parsed.req.split_replication orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(max - 5, replication.transition_id);
            try std.testing.expectEqual(max - 4, replication.source_group_id);
            try std.testing.expectEqual(max - 3, replication.destination_group_id);
            try std.testing.expectEqual(max, replication.identity_namespace.table_id);
            try std.testing.expectEqual(max - 1, replication.identity_namespace.shard_id);
            try std.testing.expectEqual(max - 2, replication.identity_namespace.range_id);
            try std.testing.expectEqual(max - 6, replication.sequence);
            try std.testing.expectEqual(max - 7, replication.previous_sequence.?);
        }

        test "internal batch parser rejects invalid split delta predecessor fences" {
            const non_delta =
                \\{"_split_replication":{"transition_id":1,"attempt_epoch":1,"source_group_id":2,"destination_group_id":3,"namespace_table_id":4,"namespace_shard_id":5,"namespace_range_id":6,"operation":"bootstrap","sequence":7,"previous_sequence":6}}
            ;
            try std.testing.expectError(
                error.InvalidBatchRequest,
                parseInternalBatchRequest(std.testing.allocator, non_delta),
            );
            const non_monotonic =
                \\{"_split_replication":{"transition_id":1,"attempt_epoch":1,"source_group_id":2,"destination_group_id":3,"namespace_table_id":4,"namespace_shard_id":5,"namespace_range_id":6,"operation":"delta","sequence":7,"previous_sequence":7}}
            ;
            try std.testing.expectError(
                error.InvalidBatchRequest,
                parseInternalBatchRequest(std.testing.allocator, non_monotonic),
            );
        }

        test "internal batch parser owns and round trips split transition" {
            const body =
                \\{"_split_transition":{"kind":"start","transition_id":40,"attempt_epoch":1,"destination_group_id":42,"split_key":"doc:m"}}
            ;
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, body));

            var owned = try parseInternalBatchRequest(std.testing.allocator, body);
            defer owned.deinit(std.testing.allocator);
            const transition = owned.req.split_transition orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(db_mod.types.SplitTransitionMutation.Kind.start, transition.kind);
            try std.testing.expectEqual(@as(u64, 40), transition.transition_id);
            try std.testing.expectEqual(@as(u64, 1), transition.attempt_epoch);
            try std.testing.expectEqual(@as(u64, 42), transition.destination_group_id);
            try std.testing.expectEqualStrings("doc:m", transition.split_key);

            const encoded = try encodeBatchRequest(std.testing.allocator, owned.req);
            defer std.testing.allocator.free(encoded);
            var reparsed = try parseInternalBatchRequest(std.testing.allocator, encoded);
            defer reparsed.deinit(std.testing.allocator);
            try std.testing.expectEqual(db_mod.types.SplitTransitionMutation.Kind.start, reparsed.req.split_transition.?.kind);
        }

        test "internal batch codec round trips replicated transaction phases" {
            const alloc = std.testing.allocator;
            const txn_id: db_mod.types.TxnId = .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
            const encoded = try encodeBatchRequest(alloc, .{
                .writes = &.{.{ .key = "doc:a", .value = "{}" }},
                .predicates = &.{.{ .key = "doc:a", .expected_version = 41 }},
                .transaction = .{ .prepare = .{ .txn_id = txn_id, .topology_epoch = 7 } },
            });
            defer alloc.free(encoded);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));

            var decoded = try parseInternalBatchRequest(alloc, encoded);
            defer decoded.deinit(alloc);
            const prepare = switch (decoded.req.transaction orelse return error.TestExpectedEqual) {
                .prepare => |value| value,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqual(txn_id, prepare.txn_id);
            try std.testing.expectEqual(@as(u64, 7), prepare.topology_epoch);
            try std.testing.expectEqual(@as(usize, 1), decoded.req.writes.len);
            try std.testing.expectEqual(@as(u64, 41), decoded.req.predicates[0].expected_version);

            const begin_encoded = try encodeBatchRequest(alloc, .{
                .transaction = .{ .begin = .{
                    .txn_id = txn_id,
                    .begin_timestamp = 42,
                    .created_at_ns = 43,
                    .topology_epoch = 7,
                    .retain_terminal = true,
                    .participants = &.{"table2:4:docs:group:7"},
                } },
            });
            defer alloc.free(begin_encoded);
            var begin_decoded = try parseInternalBatchRequest(alloc, begin_encoded);
            defer begin_decoded.deinit(alloc);
            const begin = switch (begin_decoded.req.transaction.?) {
                .begin => |value| value,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expect(begin.retain_terminal);
            try std.testing.expectEqualStrings("table2:4:docs:group:7", begin.participants[0]);

            const ack_encoded = try encodeBatchRequest(alloc, .{
                .transaction = .{ .acknowledge = .{
                    .txn_id = txn_id,
                    .participant = "table2:4:docs:group:8",
                } },
            });
            defer alloc.free(ack_encoded);
            var ack_decoded = try parseInternalBatchRequest(alloc, ack_encoded);
            defer ack_decoded.deinit(alloc);
            const ack = switch (ack_decoded.req.transaction.?) {
                .acknowledge => |value| value,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqualStrings("table2:4:docs:group:8", ack.participant);

            const cleanup_encoded = try encodeBatchRequest(alloc, .{
                .transaction = .{ .cleanup = .{
                    .txn_id = txn_id,
                    .cutoff_timestamp = 100,
                    .retained_cutoff_timestamp = 50,
                } },
            });
            defer alloc.free(cleanup_encoded);
            var cleanup_decoded = try parseInternalBatchRequest(alloc, cleanup_encoded);
            defer cleanup_decoded.deinit(alloc);
            const cleanup = switch (cleanup_decoded.req.transaction.?) {
                .cleanup => |value| value,
                else => return error.TestUnexpectedResult,
            };
            try std.testing.expectEqual(@as(u64, 100), cleanup.cutoff_timestamp);
            try std.testing.expectEqual(@as(u64, 50), cleanup.retained_cutoff_timestamp);
        }

        test "internal batch parser rejects mixed split transition commands" {
            const body =
                \\{"inserts":{"doc:m":{}},"_split_transition":{"kind":"prepare","transition_id":40,"attempt_epoch":1,"destination_group_id":42,"split_key":"doc:m"}}
            ;
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(std.testing.allocator, body));
            try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(std.testing.allocator, .{
                .writes = &.{.{ .key = "doc:m", .value = "{}" }},
                .split_transition = .{
                    .kind = .prepare,
                    .transition_id = 40,
                    .attempt_epoch = 1,
                    .destination_group_id = 42,
                    .split_key = "doc:m",
                },
            }));
        }

        test "batch parser accepts raw payload value under public request cap" {
            const alloc = std.testing.allocator;
            const payload = try alloc.alloc(u8, 6 * 1024 * 1024);
            defer alloc.free(payload);
            @memset(payload, 'x');

            var out: std.Io.Writer.Allocating = .init(alloc);
            defer out.deinit();
            const writer = &out.writer;

            try writer.writeAll("{\"inserts\":{\"doc:a\":{\"raw_payload\":\"");
            try writer.writeAll(payload);
            try writer.writeAll("\"}}}");

            var owned = try parseBatchRequest(alloc, out.written());
            defer owned.deinit(alloc);

            try std.testing.expectEqual(@as(usize, 1), owned.writes.len);
            try std.testing.expect(std.mem.indexOf(u8, owned.writes[0].value, "\"raw_payload\"") != null);
        }

        test "batch parser rejects removed aknn sync level" {
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator,
                \\{"inserts":{"doc:a":{"title":"alpha"}},"sync_level":"aknn"}
            ));
        }

        test "batch parser accepts transforms" {
            var owned = try parseBatchRequest(std.testing.allocator,
                \\{"transforms":[{"key":"doc:a","operations":[{"op":"$min","path":"priority","value":2},{"op":"$max","path":"version","value":3},{"op":"$set","path":"status","value":"updated"}],"upsert":true}]}
            );
            defer owned.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(usize, 1), owned.transforms.len);
            try std.testing.expect(owned.transforms[0].upsert);
            try std.testing.expectEqual(db_mod.types.TransformOpType.min, owned.transforms[0].operations[0].op);
            try std.testing.expectEqualStrings("priority", owned.transforms[0].operations[0].path);
            try std.testing.expectEqual(db_mod.types.TransformOpType.max, owned.transforms[0].operations[1].op);
        }

        test "batch parser accepts supported Go transform op spelling" {
            var owned = try parseBatchRequest(std.testing.allocator,
                \\{"transforms":[{"key":"doc:a","operations":[{"op":"$addToSet","path":"tags","value":"zig"},{"op":"$setOnInsert","path":"created_at","value":"now"}]}]}
            );
            defer owned.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(usize, 1), owned.transforms.len);
            try std.testing.expectEqual(@as(usize, 2), owned.transforms[0].operations.len);
            try std.testing.expectEqual(db_mod.types.TransformOpType.add_to_set, owned.transforms[0].operations[0].op);
            try std.testing.expectEqual(db_mod.types.TransformOpType.set_on_insert, owned.transforms[0].operations[1].op);
        }

        test "batch parser accepts push for graph edge arrays" {
            var owned = try parseBatchRequest(std.testing.allocator,
                \\{"transforms":[{"key":"doc:a","operations":[{"op":"$push","path":"edges","value":{"to":"doc:b"}}]}]}
            );
            defer owned.deinit(std.testing.allocator);
            try std.testing.expectEqual(db_mod.types.TransformOpType.push, owned.transforms[0].operations[0].op);
        }

        test "batch parser accepts pull for exact array values" {
            var owned = try parseBatchRequest(std.testing.allocator,
                \\{"transforms":[{"key":"doc:a","operations":[{"op":"$pull","path":"edges","value":{"target":"doc:b"}}]}]}
            );
            defer owned.deinit(std.testing.allocator);
            try std.testing.expectEqual(db_mod.types.TransformOpType.pull, owned.transforms[0].operations[0].op);
        }

        test "batch parser rejects every recognized but unsupported transform operator" {
            const unsupported = [_][]const u8{
                "$pop",
                "$mul",
                "$currentDate",
                "$rename",
            };
            for (unsupported) |op| {
                const body = try std.fmt.allocPrint(
                    std.testing.allocator,
                    "{{\"transforms\":[{{\"key\":\"doc:missing\",\"operations\":[{{\"op\":\"{s}\",\"path\":\"field\",\"value\":1}}]}}]}}",
                    .{op},
                );
                defer std.testing.allocator.free(body);
                try std.testing.expectError(
                    error.InvalidBatchRequest,
                    parseBatchRequest(std.testing.allocator, body),
                );
            }
        }

        test "batch parser safely rejects unsupported transform after initialized operations" {
            const body =
                \\{"transforms":[{"key":"doc:missing","operations":[{"op":"$set","path":"ready","value":true},{"op":"$pop","path":"items","value":1}]}]}
            ;
            for (0..32) |_| {
                try std.testing.expectError(
                    error.InvalidBatchRequest,
                    parseBatchRequest(std.testing.allocator, body),
                );
            }
        }

        test "batch parser preserves packed embeddings for mapper extraction" {
            var owned = try parseBatchRequest(std.testing.allocator,
                \\{"inserts":{"doc:a":{"title":"alpha","_embeddings":{"dense_idx":"AACAPwAAAEAAAEBA","sparse_idx":{"packed_indices":"AQAAAAUAAAA=","packed_values":"AAAAPwAAQD8="}}}}}
            );
            defer owned.deinit(std.testing.allocator);

            var extracted = try document_mapper.extractWrite(std.testing.allocator, owned.writes[0].key, owned.writes[0].value);
            defer extracted.deinit(std.testing.allocator);

            try std.testing.expectEqual(@as(usize, 1), extracted.dense_embeddings.len);
            try std.testing.expectEqual(@as(usize, 1), extracted.sparse_embeddings.len);
            try std.testing.expect(extracted.cleaned_value != null);
            try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "\"title\":\"alpha\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "_embeddings") == null);
        }

        test "batch parser accepts compact vdbbench-shaped embeddings batch" {
            const alloc = std.testing.allocator;
            var out: std.Io.Writer.Allocating = .init(alloc);
            defer out.deinit();
            const writer = &out.writer;

            try writer.writeAll("{\"inserts\":{");
            for (0..500) |i| {
                if (i != 0) try writer.writeByte(',');
                try writer.print(
                    "\"key:{d}\":{{\"id\":{d},\"metadata\":{{\"source\":\"vdbbench\",\"ordinal\":{d}}},\"vec_data\":[0.1,0.2,0.3],\"_embeddings\":{{\"vec\":[0.1,0.2,0.3]}}}}",
                    .{ i, i, i },
                );
            }
            try writer.writeAll("},\"sync_level\":\"write\"}");

            var owned = try parseBatchRequest(alloc, out.written());
            defer owned.deinit(alloc);

            try std.testing.expectEqual(@as(usize, 500), owned.writes.len);
            try std.testing.expectEqual(db_mod.types.SyncLevel.write, owned.req.sync_level);

            var extracted = try document_mapper.extractWrite(alloc, owned.writes[0].key, owned.writes[0].value);
            defer extracted.deinit(alloc);

            try std.testing.expectEqual(@as(usize, 1), extracted.dense_embeddings.len);
            try std.testing.expectEqualStrings("vec", extracted.dense_embeddings[0].index_name);
            try std.testing.expectEqual(@as(usize, 3), extracted.dense_embeddings[0].vector.len);
            try std.testing.expect(extracted.cleaned_value != null);
            try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "\"vec_data\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, extracted.cleaned_value.?, "_embeddings") == null);
        }
    };
    return Suite;
}
comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}
