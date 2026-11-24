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

const std = @import("std");
pub const DomainHandoff = @import("../schema/domain_handoff.zig");
const ExternalDecisionOwner = @import("../storage/transactions.zig").ExternalDecisionOwner;

fn parseExternalDecisionOwner(a: std.mem.Allocator, value: std.json.Value) !ExternalDecisionOwner {
    var parsed = std.json.parseFromValue(ExternalDecisionOwner, a, value, .{ .ignore_unknown_fields = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidBatchRequest;
    defer parsed.deinit();
    parsed.value.validate() catch return error.InvalidBatchRequest;
    return parsed.value;
}

/// Actual control codec replay must remain neutral across local defaults and
/// untagged historical decode. Rows and an explicit revision mode stay closed.
fn expectIsolatedControlModes(a: std.mem.Allocator, request: db_types.BatchRequest, rejection: anyerror) !void {
    for ([_]db_types.DocumentVersionScheme{ .legacy, .admit_current }) |mode| {
        var selected = request;
        selected.document_version_scheme = mode;
        const bytes = try encodeBatchRequest(a, selected);
        defer a.free(bytes);
        try std.testing.expect(!std.mem.startsWith(u8, bytes, "[\"document-revisions-batch-v1\","));
        var decoded = try parseInternalBatchRequest(a, bytes);
        defer decoded.deinit(a);
        try std.testing.expectEqual(db_types.DocumentVersionScheme.admit_current, decoded.req.document_version_scheme);
        const replay = try encodeBatchRequest(a, decoded.req);
        defer a.free(replay);
        try std.testing.expectEqualSlices(u8, bytes, replay);
        var mixed = selected;
        mixed.writes = &.{.{ .key = "forbidden-row", .value = "{}" }};
        try std.testing.expectError(rejection, encodeBatchRequest(a, mixed));
    }
    var explicit = request;
    explicit.document_version_scheme = .revision_v1;
    try std.testing.expectError(rejection, encodeBatchRequest(a, explicit));
}

test "ordered artifact inventory private batch roundtrip rejects public and mixed requests" {
    const alloc = std.testing.allocator;
    const inventory = @import("../storage/db/artifact_inventory.zig");
    const catalogs: inventory.Catalogs = .{ .indexes = "AIDX\x02\x00\x00\x00\x00\x00\x00\x00", .enrichments = "[]", .resolvers = "[]" };
    const request: db_types.BatchRequest = .{ .artifact_catalog = .{ .namespace = @splat(1), .binding = .{ .epoch = 1, .digest = catalogs.digest(), .semantic_digest = try catalogs.semanticDigest(alloc) }, .catalogs = catalogs } };
    try expectIsolatedControlModes(alloc, request, error.InvalidArtifactCatalogCommand);
    const encoded = try encodeBatchRequest(alloc, request);
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
    var parsed = try parseInternalBatchRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualDeep(request.artifact_catalog.?, parsed.req.artifact_catalog.?);
    var mixed = request;
    mixed.writes = &.{.{ .key = "key", .value = "{}" }};
    try std.testing.expectError(error.InvalidArtifactCatalogCommand, encodeBatchRequest(alloc, mixed));
    var accept = request;
    accept.merge_checkpoint = .{ .kind = .accept, .transition_id = 7, .donor_group_id = 3, .receiver_group_id = 2, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z" };
    accept.merge_replication = .{ .transition_id = 7, .donor_group_id = 3, .receiver_group_id = 2, .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .copy_attempt = .{} };
    accept.artifact_catalog.?.namespace = @import("../storage/db/online_source_contract.zig").namespaceBytes(accept.merge_replication.?.identity_namespace);
    accept.merge_checkpoint.?.page_source = .{ .namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .pin_digest = @splat(5), .applied_index = 4, .retention = .{ .epoch = 1, .after_sequence = 0 }, .artifact_catalog = request.artifact_catalog.?.binding };
    accept.merge_checkpoint.?.page_receiver_namespace = accept.merge_replication.?.identity_namespace;
    const accept_bytes = try encodeBatchRequest(alloc, accept);
    defer alloc.free(accept_bytes);
    var parsed_accept = try parseInternalBatchRequest(alloc, accept_bytes);
    defer parsed_accept.deinit(alloc);
    try std.testing.expectEqualDeep(accept.artifact_catalog, parsed_accept.req.artifact_catalog);
    try std.testing.expectEqualDeep(accept.merge_replication, parsed_accept.req.merge_replication);
    try std.testing.expectEqualDeep(accept.merge_checkpoint.?.page_source, parsed_accept.req.merge_checkpoint.?.page_source);
    accept.artifact_catalog.?.binding.effect_protocol = 15;
    accept.merge_checkpoint.?.page_source.?.artifact_catalog.?.effect_protocol = 15;
    accept.merge_checkpoint.?.page_source_catalogs = catalogs;
    try @import("../storage/db/merge_artifact_catalog.zig").validateCheckpoint(accept.merge_checkpoint.?);
    const extended_bytes = try encodeBatchRequest(alloc, accept);
    defer alloc.free(extended_bytes);
    var extended = try parseInternalBatchRequest(alloc, extended_bytes);
    defer extended.deinit(alloc);
    try std.testing.expectEqualDeep(catalogs, extended.req.merge_checkpoint.?.page_source_catalogs.?);
    var mismatched = accept.merge_checkpoint.?;
    mismatched.page_source_catalogs.?.indexes = "wrong";
    try std.testing.expectError(error.InvalidMergeCheckpoint, @import("../storage/db/merge_artifact_catalog.zig").validateCheckpoint(mismatched));
    mismatched.page_source_catalogs = null;
    try std.testing.expectError(error.InvalidMergeCheckpoint, @import("../storage/db/merge_artifact_catalog.zig").validateCheckpoint(mismatched));
    accept.merge_replication.?.transition_id += 1;
    try std.testing.expectError(error.InvalidArtifactCatalogCommand, encodeBatchRequest(alloc, accept));
}
test "internal isolated controls preserve neutral document versions across owner and forwarded codecs" {
    const a = std.testing.allocator;
    const inventory = @import("../storage/db/artifact_inventory.zig");
    const catalogs: inventory.Catalogs = .{};
    const requests = [_]db_types.BatchRequest{
        .{ .activate_range_tracking = true },
        .{ .artifact_catalog = .{
            .namespace = @splat(1),
            .catalogs = catalogs,
            .binding = .{ .epoch = 1, .digest = catalogs.digest(), .semantic_digest = try catalogs.semanticDigest(a) },
        } },
        .{ .relational_topology = .{ .action = .begin, .fence = .{
            .transition_id = 1,
            .admission_epoch = 1,
            .attempt = 1,
            .owner_group_id = 3,
            .peer_group_id = 4,
            .role = .child_generation_source,
            .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .catalog_digest = @splat(5),
        } } },
    };
    for (requests) |request| {
        for ([_]bool{ false, true }) |owner| {
            const bytes = if (owner) try encodeOwnerBatchRequest(a, request) else try encodeBatchRequest(a, request);
            defer a.free(bytes);
            var decoded = try parseInternalBatchRequest(a, bytes);
            defer decoded.deinit(a);
            try std.testing.expectEqualDeep(request, decoded.req);
            // A second forwarding hop must retain the same private command,
            // including the strict catalog-only validation before encoding.
            const forwarded = try encodeBatchRequest(a, decoded.req);
            defer a.free(forwarded);
            var next = try parseInternalBatchRequest(a, forwarded);
            defer next.deinit(a);
            try std.testing.expectEqualDeep(request, next.req);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, forwarded));
        }
    }
}

test "artifact publication upload control is private, bounded, and single purpose" {
    const alloc = std.testing.allocator;
    const transport = @import("../storage/db/artifact_publication_transport.zig");
    const hash = transport.chunkDigest(0, "data");
    const hashes = [_]transport.Digest{hash};
    const request: db_types.BatchRequest = .{ .artifact_publication_transport = .{ .action = .begin, .namespace = @splat(1), .publication_digest = @splat(2), .command_digest = @splat(3), .encoded_len = 4, .chunk_hashes = &hashes } };
    try expectIsolatedControlModes(alloc, request, error.InvalidBatchRequest);
    const encoded = try encodeBatchRequest(alloc, request);
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
    var parsed = try parseInternalBatchRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualDeep(request.artifact_publication_transport, parsed.req.artifact_publication_transport);
    var mixed = request;
    mixed.writes = &.{.{ .key = "row", .value = "{}" }};
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, mixed));
    const recovery: transport.RecoveryHint = .{ .namespace = @splat(1), .publication_digest = @splat(2), .root = @splat(3), .created_index = 87 };
    const finalize = try encodeBatchRequest(alloc, .{ .artifact_publication_transport = recovery.request() });
    defer alloc.free(finalize);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, finalize));
    var recovered = try parseInternalBatchRequest(alloc, finalize);
    defer recovered.deinit(alloc);
    try std.testing.expectEqualDeep(recovery.request(), recovered.req.artifact_publication_transport.?);
    var abandoned = recovery;
    abandoned.action = .abandon;
    abandoned.observed_progress = @splat(4);
    const retirement = try encodeBatchRequest(alloc, .{ .artifact_publication_transport = abandoned.request() });
    defer alloc.free(retirement);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, retirement));
    var retired = try parseInternalBatchRequest(alloc, retirement);
    defer retired.deinit(alloc);
    try std.testing.expectEqualDeep(abandoned.request(), retired.req.artifact_publication_transport.?);
}
// Wire contracts must not pull a concrete DB owner into control/SQL roots.
const db_types = @import("../storage/db/types.zig");
const document_transform = @import("../storage/db/transform.zig");
const DocIdentityNamespace = @import("../storage/db/doc_identity_namespace.zig").Namespace;
const ant_json = @import("antfly-json");
const document_mapper = @import("../storage/db/document_mapper.zig");
const public_limits = @import("antfly_public_limits");
const merge_pages = @import("../storage/db/merge_page_contract.zig");
const MergePageEffects = struct { writes: []db_types.BatchWrite, deletes: [][]const u8 };

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
    /// Private data-Raft control response; never populated for user rows.
    row_policy_receipt: ?@import("../storage/db/row_policy_bundle.zig").Receipt = null,
};

test "SQL document epoch fence survives internal batch codec and rejects public injection" {
    const alloc = std.testing.allocator;
    for ([_]u32{ 0, 7, std.math.maxInt(u32) }) |version| {
        const body = try encodeBatchRequest(alloc, .{ .schema_version = version, .deletes = &.{"row"} });
        defer alloc.free(body);
        var parsed = try parseInternalBatchRequest(alloc, body);
        defer parsed.deinit(alloc);
        try std.testing.expectEqual(@as(?u32, version), parsed.req.schema_version);
        try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, body));
    }
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, "{\"_schema_version\":4294967296}"));
}

test "row policy publication Raft codec carries immutable owner-fetched bytes privately" {
    const alloc = std.testing.allocator;
    const request: db_types.BatchRequest = .{ .row_policy_publication = .{
        .table_id = 7,
        .expected_generation = 11,
        .expected_catalog_epoch = 13,
        .expected_phase = .pending_install,
        .owner_group_id = 17,
        .expected_descriptor_digest = @splat(0xab),
    }, .row_policy_install_bundle = "{\"table_id\":7}" };
    const encoded = try encodeBatchRequest(alloc, request);
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
    var parsed = try parseInternalBatchRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualDeep(request.row_policy_publication.?, parsed.req.row_policy_publication.?);
    try std.testing.expectEqualStrings(request.row_policy_install_bundle, parsed.req.row_policy_install_bundle);
    var mixed = request;
    mixed.timestamp_ns = 1;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, mixed));
    mixed = request;
    mixed.schema_version = 0;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, mixed));
    mixed = request;
    mixed.sync_level = .full_text;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, mixed));
    mixed = request;
    mixed.relational_schema_version = 0;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, mixed));
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, "{\"inserts\":{},\"deletes\":[],\"_row_policy_publication\":{\"table_id\":7,\"expected_generation\":11,\"expected_catalog_epoch\":13,\"expected_phase\":\"pending_install\"},\"sync_level\":\"write\",\"_schema_version\":1}"));
    const reencoded = try encodeBatchRequest(alloc, parsed.req);
    defer alloc.free(reencoded);
    var reparsed = try parseInternalBatchRequest(alloc, reencoded);
    defer reparsed.deinit(alloc);
    try std.testing.expectEqualDeep(request.row_policy_publication.?, reparsed.req.row_policy_publication.?);
    var timestamped = request;
    timestamped.timestamp_ns = 123;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, timestamped));
    var normalized = request;
    normalized.normalized_graph_rows = true;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, normalized));
    var fenced = request;
    fenced.schema_version = 1;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, fenced));
    var adjunct = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
    defer adjunct.deinit();
    try adjunct.value.object.put(adjunct.arena.allocator(), "_schema_version", .{ .integer = 1 });
    const adjunct_bytes = try std.json.Stringify.valueAlloc(alloc, adjunct.value, .{});
    defer alloc.free(adjunct_bytes);
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, adjunct_bytes));
}

test "signed write admission is internal-only and survives the replicated batch codec" {
    const alloc = std.testing.allocator;
    const req: db_types.BatchRequest = .{
        .writes = &.{.{ .key = "row", .value = "{}" }},
        .row_policy_principal_proof = "v1:opaque:signature",
        .row_policy_database = "main",
        .row_policy_admitted_at_seconds = 123,
    };
    const encoded = try encodeBatchRequest(alloc, req);
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
    var parsed = try parseInternalBatchRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    @memset(encoded, 0xa5);
    try std.testing.expectEqualStrings(req.row_policy_principal_proof, parsed.req.row_policy_principal_proof);
    try std.testing.expectEqualStrings(req.row_policy_database, parsed.req.row_policy_database);
    try std.testing.expectEqual(req.row_policy_admitted_at_seconds, parsed.req.row_policy_admitted_at_seconds);
    const forwarded = try encodeBatchRequest(alloc, parsed.req);
    defer alloc.free(forwarded);
    var next = try parseInternalBatchRequest(alloc, forwarded);
    defer next.deinit(alloc);
    try std.testing.expectEqualStrings(req.row_policy_principal_proof, next.req.row_policy_principal_proof);
    try std.testing.expectEqualStrings(req.row_policy_database, next.req.row_policy_database);
}

test "signed write admission ownership survives every parser allocation failure" {
    const Harness = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var owned = try parseInternalBatchRequest(alloc, "{\"inserts\":{\"row\":{}},\"_row_policy_principal_proof\":\"proof\",\"_row_policy_database\":\"main\",\"_row_policy_admitted_at_seconds\":123}");
            defer owned.deinit(alloc);
            try std.testing.expectEqualStrings("proof", owned.req.row_policy_principal_proof);
            try std.testing.expectEqualStrings("main", owned.req.row_policy_database);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "index maintenance internal batch codec owns binary proof and rejects public bypass" {
    const alloc = std.testing.allocator;
    const command = @import("../storage/db/relational/indexes/maintenance_contract.zig").Command{
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
    const request = db_types.BatchRequest{
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
    const namespace: DocIdentityNamespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 };
    const source: merge_pages.Source = .{ .namespace = namespace, .pin_digest = @splat(255), .applied_index = 10 };
    var request: db_types.BatchRequest = .{
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
    try std.testing.expect(std.meta.stringToEnum(db_types.MergeReplicationCheckpoint.Kind, "page_v1") == null);
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

    const chunker = try merge_pages.RowChunks(db_types.BatchRequest).init(large_request);
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

    const checkpoint: db_types.BatchRequest = .{ .merge_checkpoint = .{
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
    try std.testing.expect(std.meta.stringToEnum(db_types.MergeReplicationCheckpoint.Kind, "online_source_v4") == null);
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
    var request: db_types.BatchRequest = .{
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
    const checkpoint: db_types.BatchRequest = .{ .merge_checkpoint = .{
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
    publication_intent: ?std.json.Parsed(PublicationIntent) = null,
    replication_source_ack: ?std.json.Parsed(ReplicationSourceAck) = null,
    replication_source_intent: ?std.json.Parsed(ReplicationSourceIntent) = null,
    document_revision_handoff: ?std.json.Parsed(@import("../storage/document_revision_reservation.zig").Handoff) = null,
    prepared_domain_rows: ?@import("../schema/domain_handoff.zig").Owned = null,
    writes: []db_types.BatchWrite = &.{},
    prepared_enrichments_json: ?[]u8 = null,
    deletes: [][]const u8 = &.{},
    transforms: []db_types.DocumentTransform = &.{},
    graph_writes: []db_types.GraphEdgeWrite = &.{},
    graph_deletes: []db_types.GraphEdgeDelete = &.{},
    graph_endpoint_cleanup_guards: ?std.json.Parsed([]const @import("../storage/graph_cleanup_contract.zig").Guard) = null,
    predicates: []db_types.TransactionVersionPredicate = &.{},
    integrity: []const db_types.TransactionIntegrityOperation = &.{},
    integrity_commands: ?std.json.Parsed([]const @import("../storage/db/relational/integrity/contract.zig").Command) = null,
    range_guards: ?std.json.Parsed([]const @import("../storage/range_protection.zig").Proof) = null,
    relational_activation: ?std.json.Parsed(@import("../storage/db/relational/integrity/activation_contract.zig").Command) = null,
    relational_retirement: ?std.json.Parsed(@import("../storage/db/relational/integrity/retirement_contract.zig").Command) = null,
    relational_index_maintenance: ?std.json.Parsed(@import("../storage/db/relational/indexes/maintenance_contract.zig").Command) = null,
    relational_index_owner_control: ?std.json.Parsed(@import("../storage/db/relational/indexes/raft_contract.zig").Command) = null,
    relational_topology: ?std.json.Parsed(@import("../storage/db/relational/integrity/topology_contract.zig").Command) = null,
    relational_generation_gc: ?std.json.Parsed(@import("../storage/db/relational/integrity/generation_retirement.zig").GcCommand) = null,
    row_policy_publication: ?std.json.Parsed(@import("../system_catalog/policies.zig").InstallRequest) = null,
    row_policy_install_bundle: ?[]u8 = null,
    row_policy_principal_proof: ?[]u8 = null,
    row_policy_database: ?[]u8 = null,
    graph_preparation: ?std.json.Parsed(@import("../storage/db/prepared_graph_effects.zig").Admission) = null,
    trigger_receipts: ?std.json.Parsed([]const @import("../sql/trigger_handoff.zig").Receipt) = null,
    trigger_preparation_proof: ?[]u8 = null,
    restore_staging: ?std.json.Parsed(@import("../storage/db/restore_staging_contract.zig").Control) = null,
    online_source: ?std.json.Parsed(@import("../storage/db/online_source_contract.zig").Command) = null,
    group_source: ?std.json.Parsed(@import("../storage/db/group_source_contract.zig").Command) = null,
    sql_lock: ?std.json.Parsed(@import("../storage/pessimistic_lock/control.zig").Command) = null,
    resolver_review: ?std.json.Parsed(@import("../storage/db/resolver_review.zig").Command) = null,
    artifact_catalog: ?std.json.Parsed(@import("../storage/db/artifact_inventory.zig").Command) = null,
    artifact_publication: ?std.json.Parsed(@import("../storage/db/artifact_publication.zig").Command) = null,
    artifact_publication_transport: ?std.json.Parsed(@import("../storage/db/artifact_publication_transport.zig").Request) = null,
    leann_record_index: ?[]u8 = null,
    leann_record_bytes: ?[]u8 = null,
    logical_publication_control: ?std.json.Parsed(@import("../system_catalog/logical_replication_capture.zig").Control) = null,
    logical_publication_gc: ?std.json.Parsed(@import("../system_catalog/logical_replication_publisher.zig").GcPermission) = null,
    merge_proof_adoption: ?std.json.Parsed(@import("../storage/db/merge_proof_adoption.zig").Command) = null,
    transaction_participants: [][]const u8 = &.{},
    transaction_completion_admission: ?[]u8 = null,
    transaction_completion_certificate: ?[]u8 = null,
    transaction_source_checkpoint: ?std.json.Parsed(@import("../storage/source_checkpoint.zig").Command) = null,
    split_checkpoint_range_start: ?[]u8 = null,
    split_checkpoint_range_end: ?[]u8 = null,
    split_transition_key: ?[]u8 = null,
    merge_receiver_base_start: ?[]u8 = null,
    merge_receiver_base_end: ?[]u8 = null,
    merge_range_start: ?[]u8 = null,
    merge_range_end: ?[]u8 = null,
    merge_artifacts: ?std.json.Parsed([]const db_types.BatchWrite) = null,
    merge_page: ?std.json.Parsed(merge_pages.Command) = null,
    merge_page_effects: ?std.json.Parsed(MergePageEffects) = null,
    merge_source_catalogs: ?std.json.Parsed(@import("../storage/db/artifact_inventory.zig").Catalogs) = null,
    req: db_types.BatchRequest = .{},

    pub fn deinit(self: *OwnedBatchRequest, alloc: std.mem.Allocator) void {
        if (self.replication_source_ack) |*value| {
            value.deinit();
            self.* = undefined;
            return;
        }
        if (self.replication_source_intent) |*value| {
            value.deinit();
            self.* = undefined;
            return;
        }
        if (self.publication_intent) |*value| {
            value.deinit();
            self.* = undefined;
            return;
        }
        if (self.document_revision_handoff) |*value| value.deinit();
        if (self.transaction_source_checkpoint) |*checkpoint| checkpoint.deinit();
        if (self.prepared_enrichments_json) |json| alloc.free(json);
        @import("relational_integrity_wire.zig").free(alloc, self.integrity);
        if (self.integrity_commands) |*commands| commands.deinit();
        if (self.graph_endpoint_cleanup_guards) |*guards| guards.deinit();
        if (self.range_guards) |*guards| guards.deinit();
        if (self.relational_activation) |*activation| activation.deinit();
        if (self.relational_retirement) |*retirement| retirement.deinit();
        if (self.relational_index_maintenance) |*retirement| retirement.deinit();
        if (self.relational_index_owner_control) |*control| control.deinit();
        if (self.relational_topology) |*topology| topology.deinit();
        if (self.relational_generation_gc) |*page| page.deinit();
        if (self.row_policy_publication) |*publication| publication.deinit();
        if (self.row_policy_install_bundle) |bundle| alloc.free(bundle);
        if (self.row_policy_principal_proof) |proof| alloc.free(proof);
        if (self.row_policy_database) |database| alloc.free(database);
        if (self.trigger_receipts) |*receipts| receipts.deinit();
        if (self.graph_preparation) |*proof| proof.deinit();
        if (self.trigger_preparation_proof) |proof| alloc.free(proof);
        if (self.restore_staging) |*control| control.deinit();
        if (self.online_source) |*control| control.deinit();
        if (self.group_source) |*control| control.deinit();
        if (self.sql_lock) |*control| control.deinit();
        if (self.resolver_review) |*control| control.deinit();
        if (self.artifact_catalog) |*control| control.deinit();
        if (self.artifact_publication) |*control| control.deinit();
        if (self.artifact_publication_transport) |*control| control.deinit();
        if (self.leann_record_index) |name| alloc.free(name);
        if (self.leann_record_bytes) |bytes| alloc.free(bytes);
        if (self.logical_publication_control) |*control| control.deinit();
        if (self.logical_publication_gc) |*collection| collection.deinit();
        if (self.merge_proof_adoption) |*control| control.deinit();
        if (self.merge_page_effects) |*effects| {
            effects.deinit();
        } else {
            for (self.writes) |write| {
                alloc.free(@constCast(write.key));
                alloc.free(@constCast(write.value));
                for (write.json_null_fields) |name| alloc.free(name);
                if (write.json_null_fields.len != 0) alloc.free(write.json_null_fields);
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
        if (self.transaction_completion_admission) |raw| alloc.free(raw);
        if (self.transaction_completion_certificate) |raw| alloc.free(raw);
        for (self.transaction_participants) |participant| alloc.free(participant);
        if (self.transaction_participants.len > 0) alloc.free(self.transaction_participants);
        if (self.split_checkpoint_range_start) |value| alloc.free(value);
        if (self.split_checkpoint_range_end) |value| alloc.free(value);
        if (self.split_transition_key) |value| alloc.free(value);
        if (self.merge_receiver_base_start) |value| alloc.free(value);
        if (self.merge_receiver_base_end) |value| alloc.free(value);
        if (self.merge_range_start) |value| alloc.free(value);
        if (self.merge_range_end) |value| alloc.free(value);
        if (self.prepared_domain_rows) |*value| value.deinit();
        if (self.merge_artifacts) |*value| value.deinit();
        if (self.merge_page) |*value| value.deinit();
        if (self.merge_source_catalogs) |*value| value.deinit();
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

pub const public_codec = @import("batch.zig");
/// Borrow public buffers for a synchronous storage/coordinator call. Every
/// private field remains its default; public data cannot grant owner authority.
pub const fromPublic = db_types.fromPublicMutation;

test "internal batch commit requirements survive native owner and forwarded codecs" {
    const a = std.testing.allocator;
    for ([_]?db_types.CommitRequires{ null, .none, .direct_enrichments }) |requirement| {
        const request: db_types.BatchRequest = .{ .writes = &.{.{ .key = "doc", .value = "{}" }}, .sync_level = .write, .commit_requires = requirement };
        const wire = try encodeBatchRequest(a, request);
        defer a.free(wire);
        var forwarded = try parseInternalBatchRequest(a, wire);
        defer forwarded.deinit(a);
        try std.testing.expectEqual(requirement, forwarded.req.commit_requires);
        const native = try encodeOwnerBatchRequest(a, forwarded.req);
        defer a.free(native);
        var owner = try parseOwnerBatchRequest(a, native);
        defer owner.deinit(a);
        try std.testing.expectEqual(requirement, owner.req.commit_requires);
    }
}

pub fn takePublic(owned: *public_codec.OwnedBatchRequest) OwnedBatchRequest {
    const result_value: OwnedBatchRequest = .{ .writes = owned.writes, .deletes = owned.deletes, .transforms = owned.transforms, .graph_writes = owned.graph_writes, .graph_deletes = owned.graph_deletes, .req = fromPublic(owned.req) };
    owned.* = .{};
    return result_value;
}
pub fn parseBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedBatchRequest {
    var owned = try public_codec.parseBatchRequest(alloc, body);
    return takePublic(&owned);
}

const exact_prepare_envelope = "exact-prepare-batch-v1";

pub fn requiresExactPrepareProtocol(request: db_types.BatchRequest) bool {
    return switch (request.transaction orelse return false) {
        .begin => |begin| begin.completion_owner != null,
        .prepare => |prepare| prepare.completion_owner != null,
        else => false,
    };
}

pub fn parseInternalBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedBatchRequest {
    if (!std.mem.startsWith(u8, body, "[\"" ++ exact_prepare_envelope ++ "\","))
        return parsePrivateBatchRequest(alloc, body, false);
    const envelope = std.json.parseFromSlice([2][]const u8, alloc, body, .{}) catch |err|
        return if (err == error.OutOfMemory) err else error.InvalidBatchRequest;
    defer envelope.deinit();
    var parsed = try parsePrivateBatchRequest(alloc, envelope.value[1], false);
    errdefer parsed.deinit(alloc);
    if (!requiresExactPrepareProtocol(parsed.req)) return error.InvalidBatchRequest;
    return parsed;
}

/// Only a local owner may evaluate unprepared optimistic predicates.
pub fn parseOwnerBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedBatchRequest {
    return parsePrivateBatchRequest(alloc, body, true);
}

fn parsePrivateBatchRequest(alloc: std.mem.Allocator, body: []const u8, local_owner: bool) !OwnedBatchRequest {
    return try parseBatchRequestWithOptions(alloc, body, internal_json_options, true, local_owner);
}

/// Shared by private envelope owners before parseInternalBatchValue. Keep
/// exact tokens and the same input admission as the private text parser.
pub const internal_json_options: std.json.ParseOptions = .{
    // Row/transform numbers belong to the schema, not the transport.
    // Parsing decimal integer spellings as f64 here irreversibly rounds
    // values before the storage owner's exact typed-row preparation.
    .parse_numbers = false,
    .allocate = .alloc_always,
    .max_value_len = public_limits.max_json_value_len,
};

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

test "ordered artifact inventory merge proof adoption is private and round trips its identity" {
    const alloc = std.testing.allocator;
    const request: db_types.BatchRequest = .{ .merge_proof_adoption = .{
        .transition_id = 7,
        .attempt = .{ .donor_term = 3, .sequence = 4 },
        .source_pin = @splat(1),
        .proof_digest = @splat(2),
        .record_digest = @splat(3),
    } };
    const encoded = try encodeBatchRequest(alloc, request);
    defer alloc.free(encoded);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
    var decoded = try parseInternalBatchRequest(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualDeep(request.merge_proof_adoption.?, decoded.req.merge_proof_adoption.?);
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{
        .merge_proof_adoption = request.merge_proof_adoption,
        .writes = &.{.{ .key = "user", .value = "{}" }},
    }));
}

fn parseBatchRequestWithOptions(
    alloc: std.mem.Allocator,
    body: []const u8,
    options: std.json.ParseOptions,
    allow_internal: bool,
    local_owner: bool,
) !OwnedBatchRequest {
    if (body.len == 0) return .{};

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, options) catch |err| switch (err) {
        error.ValueTooLong => return error.ValueTooLong,
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.InvalidBatchRequest,
    };
    defer parsed.deinit();
    if (allow_internal and (isDocumentRevisionPrepare(parsed.value) or isPublicationControl(parsed.value) or isPublicationCollection(parsed.value) or isPublicationPrepare(parsed.value) or isReplicationSourcePrepare(parsed.value) or isReplicationSourceResolution(parsed.value) or isReplicationSourceAck(parsed.value))) return parseInternalBatchValue(alloc, parsed.value);
    return parseBatchValue(alloc, parsed.value, allow_internal, local_owner);
}

/// Internal transport owners may reuse their already parsed envelope. Numeric
/// tokens must have been parsed losslessly (parse_numbers=false). The returned
/// request owns its data independently of the borrowed tree, just as the text
/// ingress parser owns it independently of its temporary parse arena.
fn isDocumentRevisionPrepare(value: std.json.Value) bool {
    return value == .array and value.array.items.len == 2 and value.array.items[0] == .string and
        std.mem.eql(u8, value.array.items[0].string, "document-revision-prepare-batch-v1");
}

fn isPublicationControl(value: std.json.Value) bool {
    return value == .array and value.array.items.len == 2 and value.array.items[0] == .string and
        std.mem.eql(u8, value.array.items[0].string, "logical-publication-control-v1");
}

const publication_collection_tag = "logical-publication-collection-v1";

fn isPublicationCollection(value: std.json.Value) bool {
    return value == .array and value.array.items.len == 2 and value.array.items[0] == .string and
        std.mem.eql(u8, value.array.items[0].string, publication_collection_tag);
}

fn isPublicationPrepare(value: std.json.Value) bool {
    return value == .array and value.array.items.len == 2 and value.array.items[0] == .string and
        std.mem.eql(u8, value.array.items[0].string, "logical-publication-prepare-batch-v1");
}

const replication_source_prepare_tag = "replication-source-prepare-batch-v1";

fn isReplicationSourcePrepare(value: std.json.Value) bool {
    return value == .array and value.array.items.len == 2 and value.array.items[0] == .string and
        std.mem.eql(u8, value.array.items[0].string, replication_source_prepare_tag);
}

const replication_source_resolution_tag = "replication-source-resolve-batch-v1";

fn isReplicationSourceResolution(value: std.json.Value) bool {
    return value == .array and value.array.items.len == 2 and value.array.items[0] == .string and
        std.mem.eql(u8, value.array.items[0].string, replication_source_resolution_tag);
}

const replication_source_ack_tag = "replication-source-ack-batch-v1";

fn isReplicationSourceAck(value: std.json.Value) bool {
    return value == .array and value.array.items.len == 2 and value.array.items[0] == .string and
        std.mem.eql(u8, value.array.items[0].string, replication_source_ack_tag);
}

pub fn parseInternalBatchValue(alloc: std.mem.Allocator, value: std.json.Value) !OwnedBatchRequest {
    if (isDocumentRevisionPrepare(value)) {
        const body = value.array.items[1];
        if (body != .object or body.object.count() != 2) return error.InvalidBatchRequest;
        // Parse ONLY an ordinary native batch here, never recursive control
        // wrappers. Older owners reject the outer discriminator outright.
        var out = try parseBatchValue(alloc, body.object.get("native_batch") orelse return error.InvalidBatchRequest, true, false);
        errdefer out.deinit(alloc);
        const transaction = out.req.transaction orelse return error.InvalidBatchRequest;
        if (transaction != .prepare or transaction.prepare.document_revision_handoff != null) return error.InvalidBatchRequest;
        out.document_revision_handoff = try std.json.parseFromValue(@import("../storage/document_revision_reservation.zig").Handoff, alloc, body.object.get("handoff") orelse return error.InvalidBatchRequest, .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        out.req.transaction.?.prepare.document_revision_handoff = out.document_revision_handoff.?.value;
        try validateDocumentRevisionHandoff(out.req);
        return out;
    }

    if (isReplicationSourceAck(value)) {
        const parsed = try std.json.parseFromValue(ReplicationSourceAck, alloc, value.array.items[1], .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        return .{ .replication_source_ack = parsed, .req = try parsed.value.batch() };
    }
    if (isReplicationSourceResolution(value)) {
        const parsed = try std.json.parseFromValue(ReplicationSourceResolution, alloc, value.array.items[1], .{});
        defer parsed.deinit();
        return .{ .req = try parsed.value.batch() };
    }
    if (isReplicationSourcePrepare(value)) {
        const owned = try std.json.parseFromValue(ReplicationSourceIntent, alloc, value.array.items[1], .{ .allocate = .alloc_always });
        errdefer owned.deinit();
        const body = owned.value;
        if (body.intent.replication_source_permit != null) return error.InvalidBatchRequest;
        var request = body.intent.asBatch(body.permit.reference.txn_id, body.permit.topology_epoch);
        request.transaction.?.prepare.replication_source_permit = body.permit;
        try validateReplicationSourcePrepare(request, body.permit);
        return .{ .replication_source_intent = owned, .req = request };
    }
    if (isPublicationPrepare(value)) {
        var owned = try std.json.parseFromValue(PublicationIntent, alloc, value.array.items[1], .{ .allocate = .alloc_always });
        errdefer owned.deinit();
        const body = owned.value;
        const request = body.intent.asBatch(body.txn_id, body.topology_epoch);
        try validatePublicationPrepare(owned.arena.allocator(), request);
        return .{ .publication_intent = owned, .req = request };
    }
    if (isPublicationCollection(value)) {
        const publisher = @import("../system_catalog/logical_replication_publisher.zig");
        var out: OwnedBatchRequest = .{};
        out.logical_publication_gc = try std.json.parseFromValue(publisher.GcPermission, alloc, value.array.items[1], .{ .allocate = .alloc_always });
        errdefer out.deinit(alloc);
        out.req.logical_publication_gc = out.logical_publication_gc.?.value;
        try publisher.validateCollectionBatch(out.req);
        return out;
    }
    if (isPublicationControl(value)) {
        const capture = @import("../system_catalog/logical_replication_capture.zig");
        var out: OwnedBatchRequest = .{};
        out.logical_publication_control = try std.json.parseFromValue(capture.Control, alloc, value.array.items[1], .{ .allocate = .alloc_always });
        errdefer out.deinit(alloc);
        out.req.logical_publication_control = out.logical_publication_control.?.value;
        try capture.validateControlBatch(out.req);
        return out;
    }
    return parseBatchValue(alloc, value, true, false);
}

fn parseBatchValue(alloc: std.mem.Allocator, input_value: std.json.Value, allow_internal: bool, local_owner: bool) !OwnedBatchRequest {
    const Producer = @import("../storage/source_producer_policy.zig").Producer;
    var source_producer: Producer = .unlabelled;
    var batch_value = input_value;
    if (batch_value == .array and batch_value.array.items.len == 3) {
        const envelope = batch_value.array.items;
        if (!allow_internal or envelope[0] != .string or !std.mem.eql(u8, envelope[0].string, "source-producer-batch-v1") or envelope[1] != .string) return error.InvalidBatchRequest;
        source_producer = if (std.mem.eql(u8, envelope[1].string, "ordinary")) .ordinary else if (std.mem.eql(u8, envelope[1].string, "legacy_cdc")) .legacy_cdc else return error.InvalidBatchRequest;
        batch_value = envelope[2];
    }
    const versioned = batch_value == .array;
    const document_versions_envelope = versioned and batch_value.array.items.len == 2 and batch_value.array.items[0] == .string and (std.mem.eql(u8, batch_value.array.items[0].string, "document-revisions-batch-v1") or std.mem.eql(u8, batch_value.array.items[0].string, "row-semantics-batch-v4"));
    var domain_envelope = false;
    var trigger_envelope = false;
    const prepared_envelope = versioned and batch_value.array.items.len == 2 and batch_value.array.items[0] == .string and std.mem.eql(u8, batch_value.array.items[0].string, "prepared-enrichments-batch-v1");
    const root_value = if (versioned) blk: {
        const entries = batch_value.array.items;
        if (!allow_internal or entries.len != 2 or entries[0] != .string) return error.InvalidBatchRequest;
        trigger_envelope = std.mem.eql(u8, entries[0].string, "row-semantics-batch-v3") or ((prepared_envelope or document_versions_envelope) and entries[1] == .object and entries[1].object.contains("_trigger_receipts"));
        domain_envelope = std.mem.eql(u8, entries[0].string, "row-semantics-batch-v2") or ((prepared_envelope or trigger_envelope or document_versions_envelope) and entries[1] == .object and entries[1].object.contains("_prepared_domain_rows"));
        if (!prepared_envelope and !document_versions_envelope and !trigger_envelope and !domain_envelope and !std.mem.eql(u8, entries[0].string, "row-semantics-batch-v1")) return error.InvalidBatchRequest;
        break :blk entries[1];
    } else batch_value;
    if (root_value != .object) return error.InvalidBatchRequest;
    const root = root_value.object;
    if (domain_envelope != root.contains("_prepared_domain_rows") or (!allow_internal and root.contains("_prepared_domain_rows"))) return error.InvalidBatchRequest;
    if (trigger_envelope != root.contains("_trigger_receipts") or (!trigger_envelope and (root.contains("_trigger_preparation_proof") or root.contains("_trigger_preparation_group_id") or root.contains("_trigger_preparation_admitted_at_seconds") or root.contains("_trigger_preparation_command_digest") or root.contains("_graph_preparation")))) return error.InvalidBatchRequest;
    if (root.contains("relational_index_owner_control")) return error.InvalidBatchRequest;
    if (!allow_internal and (root.get("_graph_writes") != null or root.get("_graph_deletes") != null))
        return error.InvalidBatchRequest;

    // Own every successful partial result immediately. One cleanup path
    // avoids Debug aggregate/error-defer expansion proportional to all private
    // protocols, including protocols absent from an ordinary row batch.
    var out: OwnedBatchRequest = .{};
    errdefer out.deinit(alloc);
    out.req.source_producer = source_producer;
    if (source_producer != .unlabelled and root.contains("_transaction")) return error.InvalidBatchRequest;
    try parseBatchRowsAndControls(alloc, root, allow_internal, versioned, domain_envelope, prepared_envelope, &out);
    try parseTriggerPreparation(alloc, root, trigger_envelope, &out);
    try parseBatchTransitions(alloc, root, allow_internal, local_owner, document_versions_envelope or prepared_envelope, &out);
    try finishBatchRequest(root, allow_internal, local_owner, versioned, &out);
    if (allow_internal and !prepared_envelope and requiresDocumentVersionsEnvelope(out.req) != document_versions_envelope) return error.InvalidBatchRequest;
    if (prepared_envelope != (out.req.prepared_enrichments_json != null or out.req.commit_requires == .direct_enrichments)) return error.InvalidBatchRequest;
    return out;
}

noinline fn parseTriggerPreparation(alloc: std.mem.Allocator, root: std.json.ObjectMap, enabled: bool, out: *OwnedBatchRequest) !void {
    if (!enabled) return;
    const value = root.get("_trigger_receipts") orelse return error.InvalidBatchRequest;
    var counter = std.Io.Writer.Discarding.init(&.{});
    try std.json.Stringify.value(value, .{}, &counter.writer);
    if (counter.fullCount() > @import("../sql/trigger_handoff.zig").max_bytes) return error.InvalidBatchRequest;
    const bytes = try std.json.Stringify.valueAlloc(alloc, value, .{});
    defer alloc.free(bytes);
    // Structural decoding owns all strings independently of the temporary
    // request DOM. Semantic/canonical validation follows the common codec.
    if (bytes.len > @import("../sql/trigger_handoff.zig").max_bytes) return error.InvalidBatchRequest;
    out.trigger_receipts = try std.json.parseFromSlice([]const @import("../sql/trigger_handoff.zig").Receipt, alloc, bytes, .{ .allocate = .alloc_always });
    const canonical = try @import("../sql/trigger_handoff.zig").encodeReceipts(alloc, out.trigger_receipts.?.value);
    defer alloc.free(canonical);
    if (out.trigger_receipts.?.value.len == 0) return error.InvalidBatchRequest;
    if (root.get("_trigger_preparation_proof")) |proof| {
        if (proof != .string or proof.string.len == 0 or proof.string.len > @import("../common/trigger_preparation_authority.zig").maximum_token_bytes) return error.InvalidBatchRequest;
        out.trigger_preparation_proof = try alloc.dupe(u8, proof.string);
    }
    const group = try parseInternalU64(root.get("_trigger_preparation_group_id") orelse return error.InvalidBatchRequest);
    if (group == 0) return error.InvalidBatchRequest;
    out.req.trigger_preparation_group_id = group;
    if (root.get("_trigger_preparation_command_digest")) |digest| {
        const parsed = try std.json.parseFromValue([32]u8, alloc, digest, .{});
        defer parsed.deinit();
        if (std.mem.allEqual(u8, &parsed.value, 0)) return error.InvalidBatchRequest;
        out.req.trigger_preparation_command_digest = parsed.value;
    }
    const instant = try parseInternalU64(root.get("_trigger_preparation_admitted_at_seconds") orelse return error.InvalidBatchRequest);
    if (instant == 0) return error.InvalidBatchRequest;
    out.req.trigger_preparation_admitted_at_seconds = std.math.cast(i64, instant) orelse return error.InvalidBatchRequest;
    if (root.get("_graph_preparation")) |proof| {
        out.graph_preparation = try std.json.parseFromValue(@import("../storage/db/prepared_graph_effects.zig").Admission, alloc, proof, .{ .allocate = .alloc_always });
        try out.graph_preparation.?.value.validate();
        out.req.graph_preparation = out.graph_preparation.?.value;
    }
    out.req.trigger_receipts = out.trigger_receipts.?.value;
    out.req.trigger_preparation_proof = out.trigger_preparation_proof orelse "";
}

noinline fn parseBatchRowsAndControls(alloc: std.mem.Allocator, root: std.json.ObjectMap, allow_internal: bool, versioned: bool, domain_envelope: bool, prepared_envelope: bool, out: *OwnedBatchRequest) !void {
    var control_field_count = root.count();
    if (versioned) {
        if (root.contains("_document_version_scheme")) control_field_count -= 1;
        if (root.get("_timestamp_ns")) |value| if (try parseInternalU64(value) == 0) {
            control_field_count -= 1;
        };
    }
    out.prepared_enrichments_json = if (root.get("_prepared_enrichments")) |value| blk: {
        if (!allow_internal or !prepared_envelope or value != .object) return error.InvalidBatchRequest;
        break :blk try std.json.Stringify.valueAlloc(alloc, value, .{});
    } else null;
    out.req.prepared_enrichments_json = out.prepared_enrichments_json;
    out.req.commit_requires = if (root.get("commit_requires")) |value| try @import("../storage/db/mutation_values.zig").parseCommitRequires(value) else null;
    out.relational_index_owner_control = if (root.get("_relational_index_owner_control")) |value| control: {
        if (!allow_internal or versioned) return error.InvalidBatchRequest;
        // Exact control framing rejects unknown effects before normalization.
        for (root.keys()) |name| {
            if (!std.mem.eql(u8, name, "_relational_index_owner_control") and
                !std.mem.eql(u8, name, "inserts") and !std.mem.eql(u8, name, "deletes") and
                !std.mem.eql(u8, name, "sync_level") and !std.mem.eql(u8, name, "_timestamp_ns")) return error.InvalidBatchRequest;
        }
        if (root.get("inserts")) |inserts| if (inserts != .null and (inserts != .object or inserts.object.count() != 0)) return error.InvalidBatchRequest;
        if (root.get("deletes")) |delete_value| if (delete_value != .null and (delete_value != .array or delete_value.array.items.len != 0)) return error.InvalidBatchRequest;
        // Check the borrowed manifest before typed parsing duplicates it. The
        // general batch string limit is larger than this control's input budget.
        try @import("../storage/db/relational/indexes/raft_contract.zig").Command.validateBorrowed(value);
        const parsed = std.json.parseFromValue(@import("../storage/db/relational/indexes/raft_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always, .ignore_unknown_fields = false }) catch |err| {
            if (err == error.OutOfMemory) return err;
            return error.InvalidBatchRequest;
        };
        break :control @as(?std.json.Parsed(@import("../storage/db/relational/indexes/raft_contract.zig").Command), parsed);
    } else null;

    out.merge_page_effects = null;
    if (root.get("_merge_page_effects")) |value| {
        if (!allow_internal or root.get("_merge_page") == null) return error.InvalidBatchRequest;
        if (root.get("inserts")) |inserts| if (inserts != .object or inserts.object.count() != 0) return error.InvalidBatchRequest;
        if (root.get("deletes")) |delete_value| if (delete_value != .array or delete_value.array.items.len != 0) return error.InvalidBatchRequest;
        out.merge_page_effects = try std.json.parseFromValue(MergePageEffects, alloc, value, .{ .allocate = .alloc_always });
    } else if (root.get("_merge_page") != null) return error.InvalidBatchRequest;

    out.writes = writes: {
        if (out.merge_page_effects) |effects| break :writes effects.value.writes;
        if (root.get("inserts")) |inserts| {
            if (inserts == .null) break :writes &.{};
            const parsed_writes = try parseInserts(alloc, inserts, !allow_internal);
            errdefer freeWrites(alloc, parsed_writes);
            break :writes parsed_writes;
        }
        break :writes &.{};
    };
    out.prepared_domain_rows = null;
    if (root.get("_prepared_domain_rows")) |value| {
        if (!allow_internal or !domain_envelope or out.merge_page_effects != null) return error.InvalidBatchRequest;
        var wire = std.json.parseFromValue(@import("../schema/domain_handoff.zig").Wire, alloc, value, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidBatchRequest;
        defer wire.deinit();
        out.prepared_domain_rows = @import("../schema/domain_handoff.zig").attach(alloc, out.writes, wire.value) catch |err| return if (err == error.OutOfMemory) err else error.InvalidBatchRequest;
    }
    if (root.get("_json_null_fields")) |provenance| {
        if (!allow_internal or out.merge_page_effects != null or provenance != .object) return error.InvalidBatchRequest;
        var entries = provenance.object.iterator();
        while (entries.next()) |entry| {
            const write = for (out.writes) |*write| {
                if (std.mem.eql(u8, write.key, entry.key_ptr.*)) break write;
            } else return error.InvalidBatchRequest;
            if (entry.value_ptr.* != .array or entry.value_ptr.array.items.len > 256) return error.InvalidBatchRequest;
            var names: std.ArrayList([]const u8) = .empty;
            defer names.deinit(alloc);
            const row = try std.json.parseFromSlice(std.json.Value, alloc, write.value, .{});
            defer row.deinit();
            if (row.value != .object) return error.InvalidBatchRequest;
            for (entry.value_ptr.array.items) |name| {
                if (name != .string or name.string.len == 0) return error.InvalidBatchRequest;
                if ((row.value.object.get(name.string) orelse return error.InvalidBatchRequest) != .null) return error.InvalidBatchRequest;
                for (names.items) |prior| if (std.mem.eql(u8, prior, name.string)) return error.InvalidBatchRequest;
                try names.append(alloc, name.string);
            }
            write.json_null_fields = try db_types.cloneJsonNullFields(alloc, names.items);
        }
    }

    out.deletes = deletes: {
        if (out.merge_page_effects) |effects| break :deletes effects.value.deletes;
        if (root.get("deletes")) |deletes_value| {
            if (deletes_value == .null) break :deletes &.{};
            const parsed_deletes = try parseDeletes(alloc, deletes_value);
            errdefer freeDeletes(alloc, parsed_deletes);
            break :deletes parsed_deletes;
        }
        break :deletes &.{};
    };

    out.transforms = transforms: {
        if (root.get("transforms")) |transforms_value| {
            if (transforms_value == .null) break :transforms &.{};
            const parsed_transforms = try parseTransforms(alloc, transforms_value);
            errdefer freeTransforms(alloc, parsed_transforms);
            break :transforms parsed_transforms;
        }
        break :transforms &.{};
    };

    out.integrity = integrity: {
        const value = root.get("_integrity") orelse break :integrity &.{};
        if (!allow_internal) return error.InvalidBatchRequest;
        break :integrity try @import("relational_integrity_wire.zig").parse(alloc, value);
    };
    out.integrity_commands = if (root.get("_integrity_commands")) |value| commands: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :commands @as(?std.json.Parsed([]const @import("../storage/db/relational/integrity/contract.zig").Command), try @import("relational_integrity_wire.zig").parseCommands(alloc, value));
    } else null;
    out.range_guards = if (root.get("_range_guards")) |value| guards: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :guards @as(?std.json.Parsed([]const @import("../storage/range_protection.zig").Proof), try std.json.parseFromValue([]const @import("../storage/range_protection.zig").Proof, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    if (out.range_guards) |guards| if (guards.value.len > @import("../storage/range_protection.zig").max_proofs) return error.InvalidBatchRequest;
    out.relational_activation = if (root.get("_relational_activation")) |value| activation: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :activation @as(?std.json.Parsed(@import("../storage/db/relational/integrity/activation_contract.zig").Command), try std.json.parseFromValue(@import("../storage/db/relational/integrity/activation_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    out.relational_retirement = if (root.get("_relational_retirement")) |value| retirement: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :retirement @as(?std.json.Parsed(@import("../storage/db/relational/integrity/retirement_contract.zig").Command), try std.json.parseFromValue(@import("../storage/db/relational/integrity/retirement_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    out.relational_index_maintenance = if (root.get("_relational_index_maintenance")) |value| retirement: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :retirement @as(?std.json.Parsed(@import("../storage/db/relational/indexes/maintenance_contract.zig").Command), try std.json.parseFromValue(@import("../storage/db/relational/indexes/maintenance_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    out.relational_topology = if (root.get("_relational_topology")) |value| topology: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :topology @as(?std.json.Parsed(@import("../storage/db/relational/integrity/topology_contract.zig").Command), try std.json.parseFromValue(@import("../storage/db/relational/integrity/topology_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    out.relational_generation_gc = if (root.get("_relational_generation_gc")) |value| gc: {
        if (!allow_internal) return error.InvalidBatchRequest;
        var gc_page = try std.json.parseFromValue(@import("../storage/db/relational/integrity/generation_retirement.zig").GcCommand, alloc, value, .{ .allocate = .alloc_always });
        errdefer gc_page.deinit();
        try gc_page.value.validate();
        break :gc gc_page;
    } else null;
    out.row_policy_publication = if (root.get("_row_policy_publication")) |value| publication: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const parsed_publication = std.json.parseFromValue(@import("../system_catalog/policies.zig").InstallRequest, alloc, value, .{ .allocate = .alloc_always }) catch |err| {
            if (err == error.OutOfMemory) return err;
            return error.InvalidBatchRequest;
        };
        break :publication @as(?std.json.Parsed(@import("../system_catalog/policies.zig").InstallRequest), parsed_publication);
    } else null;
    out.row_policy_install_bundle = if (root.get("_row_policy_install_bundle")) |value| bundle: {
        if (!allow_internal or value != .string or value.string.len == 0 or
            value.string.len > @import("../system_catalog/policies.zig").max_install_snapshot_bytes)
            return error.InvalidBatchRequest;
        break :bundle try alloc.dupe(u8, value.string);
    } else null;
    out.req.normalized_graph_rows = if (root.get("_normalized_graph_rows")) |value| blk: {
        if (!allow_internal or value != .bool) return error.InvalidBatchRequest;
        break :blk value.bool;
    } else false;
    out.row_policy_principal_proof = if (root.get("_row_policy_principal_proof")) |value| blk: {
        if (!allow_internal or value != .string or value.string.len == 0 or
            value.string.len > @import("../usermgr/row_policy_authority.zig").maximum_token_bytes)
            return error.InvalidBatchRequest;
        break :blk try alloc.dupe(u8, value.string);
    } else null;
    out.row_policy_database = if (root.get("_row_policy_database")) |value| blk: {
        if (!allow_internal or value != .string or value.string.len == 0 or value.string.len > 256)
            return error.InvalidBatchRequest;
        break :blk try alloc.dupe(u8, value.string);
    } else null;
    out.req.row_policy_principal_proof = out.row_policy_principal_proof orelse "";
    out.req.row_policy_database = out.row_policy_database orelse "";
    out.req.row_policy_admitted_at_seconds = if (root.get("_row_policy_admitted_at_seconds")) |value| blk: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const admitted = try parseInternalU64(value);
        if (admitted == 0 or admitted > std.math.maxInt(i64)) return error.InvalidBatchRequest;
        break :blk @intCast(admitted);
    } else 0;
    if ((out.req.row_policy_principal_proof.len == 0) != (out.req.row_policy_database.len == 0) or
        (out.req.row_policy_principal_proof.len == 0) != (out.req.row_policy_admitted_at_seconds == 0))
        return error.InvalidBatchRequest;
    if (out.row_policy_publication) |publication| {
        // Internal batch encoding always emits empty inserts/deletes. Nothing
        // else may share this control entry, even an otherwise inert field.
        if (publication.value.table_id == 0 or publication.value.expected_generation == 0 or publication.value.expected_catalog_epoch == 0 or publication.value.owner_group_id == 0 or control_field_count != 5 or out.row_policy_install_bundle == null or
            root.get("inserts") == null or root.get("deletes") == null or root.get("sync_level") == null or out.writes.len != 0 or out.deletes.len != 0)
            return error.InvalidBatchRequest;
    } else if (out.row_policy_install_bundle != null) return error.InvalidBatchRequest;
    out.restore_staging = if (root.get("_restore_staging")) |value| control: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string or !std.mem.eql(u8, kind.string, "restore_v3")) return error.InvalidBatchRequest;
        break :control @as(?std.json.Parsed(@import("../storage/db/restore_staging_contract.zig").Control), try std.json.parseFromValue(@import("../storage/db/restore_staging_contract.zig").Control, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    out.online_source = if (root.get("_online_source")) |value| control: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string or !std.mem.eql(u8, kind.string, "online_source_v4")) return error.InvalidBatchRequest;
        break :control @as(?std.json.Parsed(@import("../storage/db/online_source_contract.zig").Command), try std.json.parseFromValue(@import("../storage/db/online_source_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    out.artifact_catalog = if (root.get("_artifact_catalog")) |value| control: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string) return error.InvalidBatchRequest;
        break :control @as(?std.json.Parsed(@import("../storage/db/artifact_inventory.zig").Command), try std.json.parseFromValue(@import("../storage/db/artifact_inventory.zig").Command, alloc, value, .{ .allocate = .alloc_always }));
    } else null;
    out.group_source = if (root.get("_group_source")) |value| control: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string or !std.mem.eql(u8, kind.string, "group_source_v1")) return error.InvalidBatchRequest;
        const parsed_control = try std.json.parseFromValue(@import("../storage/db/group_source_contract.zig").Command, alloc, value, .{ .allocate = .alloc_always });
        errdefer parsed_control.deinit();
        try parsed_control.value.validate();
        break :control @as(?std.json.Parsed(@import("../storage/db/group_source_contract.zig").Command), parsed_control);
    } else null;
    out.sql_lock = if (root.get("_sql_lock")) |value| control: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string or !std.mem.eql(u8, kind.string, "sql_lock_v1")) return error.InvalidBatchRequest;
        const parsed_control = try std.json.parseFromValue(@import("../storage/pessimistic_lock/control.zig").Command, alloc, value, .{ .allocate = .alloc_always });
        errdefer parsed_control.deinit();
        try parsed_control.value.validate();
        break :control @as(?std.json.Parsed(@import("../storage/pessimistic_lock/control.zig").Command), parsed_control);
    } else null;
    out.resolver_review = if (root.get("_resolver_review")) |value| control: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string or !std.mem.eql(u8, kind.string, "resolver_review_v1")) return error.InvalidBatchRequest;
        const parsed_control = try std.json.parseFromValue(@import("../storage/db/resolver_review.zig").Command, alloc, value, .{ .allocate = .alloc_always });
        errdefer parsed_control.deinit();
        try parsed_control.value.validate();
        break :control @as(?std.json.Parsed(@import("../storage/db/resolver_review.zig").Command), parsed_control);
    } else null;
    out.artifact_publication = if (root.get("_artifact_publication")) |value| control: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string or !std.mem.eql(u8, kind.string, "artifact_publication_v1")) return error.InvalidBatchRequest;
        const publication_parsed = try std.json.parseFromValue(@import("../storage/db/artifact_publication.zig").Command, alloc, value, .{ .allocate = .alloc_always });
        errdefer publication_parsed.deinit();
        try publication_parsed.value.validate(alloc);
        break :control @as(?std.json.Parsed(@import("../storage/db/artifact_publication.zig").Command), publication_parsed);
    } else null;
    out.artifact_publication_transport = if (root.get("_artifact_publication_transport")) |value| control: {
        if (!allow_internal) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string or !std.mem.eql(u8, kind.string, "artifact_publication_transport_v1")) return error.InvalidBatchRequest;
        const transport_parsed = try std.json.parseFromValue(@import("../storage/db/artifact_publication_transport.zig").Request, alloc, value, .{ .allocate = .alloc_always });
        errdefer transport_parsed.deinit();
        try transport_parsed.value.validate();
        break :control @as(?std.json.Parsed(@import("../storage/db/artifact_publication_transport.zig").Request), transport_parsed);
    } else null;
    out.leann_record_index = null;
    out.leann_record_bytes = null;
    if (root.get("_leann_record")) |value| {
        if (!allow_internal) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string or !std.mem.eql(u8, kind.string, "leann_record_v1")) return error.InvalidBatchRequest;
        if (value != .object or value.object.count() != 2) return error.InvalidBatchRequest;
        const name = value.object.get("index_name") orelse return error.InvalidBatchRequest;
        const encoded = value.object.get("record_base64") orelse return error.InvalidBatchRequest;
        if (name != .string or encoded != .string) return error.InvalidBatchRequest;
        const decoder = std.base64.standard.Decoder;
        const len = decoder.calcSizeForSlice(encoded.string) catch return error.InvalidBatchRequest;
        if (len == 0 or len > db_types.leann_record_max_bytes) return error.InvalidBatchRequest;
        out.leann_record_index = try alloc.dupe(u8, name.string);
        const bytes = try alloc.alloc(u8, len);
        out.leann_record_bytes = bytes;
        decoder.decode(bytes, encoded.string) catch return error.InvalidBatchRequest;
    }
    out.merge_proof_adoption = if (root.get("_merge_proof_adoption")) |value| control: {
        if (!allow_internal or control_field_count != 5) return error.InvalidBatchRequest;
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind != .string or !std.mem.eql(u8, kind.string, "merge_proof_adoption_v1")) return error.InvalidBatchRequest;
        const adoption_parsed = try std.json.parseFromValue(@import("../storage/db/merge_proof_adoption.zig").Command, alloc, value, .{ .allocate = .alloc_always });
        errdefer adoption_parsed.deinit();
        try adoption_parsed.value.validate();
        break :control @as(?std.json.Parsed(@import("../storage/db/merge_proof_adoption.zig").Command), adoption_parsed);
    } else null;
    out.req.schema_version = if (root.get("_schema_version")) |value| blk: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :blk std.math.cast(u32, try parseInternalU64(value)) orelse return error.InvalidBatchRequest;
    } else null;
    out.req.relational_schema_version = if (root.get("_relational_schema_version")) |value| blk: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :blk std.math.cast(u32, try parseInternalU64(value)) orelse return error.InvalidBatchRequest;
    } else null;
    out.req.relational_integrity_generation_set = if (root.get("_relational_integrity_generation_set")) |value| blk: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :blk try @import("relational_integrity_wire.zig").parseGenerationSet(value);
    } else null;
    out.req.relational_repair = if (root.get("_relational_repair")) |value| blk: {
        if (!allow_internal or value != .bool) return error.InvalidBatchRequest;
        break :blk value.bool;
    } else false;
    out.req.activate_range_tracking = if (root.get("_activate_range_tracking")) |value| blk: {
        if (!allow_internal or value != .bool) return error.InvalidBatchRequest;
        break :blk value.bool;
    } else false;
    out.req.restore_staging_scope = if (root.get("_restore_staging_scope")) |value| blk: {
        if (!allow_internal) return error.InvalidBatchRequest;
        break :blk try @import("relational_integrity_wire.zig").parseGenerationSet(value);
    } else null;
    out.req.restore_staging_plan_id = if (root.get("_restore_staging_plan_id")) |value| blk: {
        if (!allow_internal or out.req.restore_staging_scope == null or value != .string or value.string.len != 32) return error.InvalidBatchRequest;
        var id: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&id, value.string) catch return error.InvalidBatchRequest;
        if (std.mem.allEqual(u8, &id, 0)) return error.InvalidBatchRequest;
        break :blk id;
    } else null;
    out.graph_writes = graph_writes: {
        if (root.get("_graph_writes") != null and root.get("graph_writes") != null) return error.InvalidBatchRequest;
        const value = if (allow_internal)
            root.get("_graph_writes") orelse root.get("graph_writes") orelse break :graph_writes &.{}
        else
            root.get("graph_writes") orelse break :graph_writes &.{};
        if (value == .null) break :graph_writes &.{};
        const parsed_graph_writes = try parseGraphWrites(alloc, value);
        errdefer freeGraphWrites(alloc, parsed_graph_writes);
        break :graph_writes parsed_graph_writes;
    };

    out.graph_deletes = graph_deletes: {
        if (root.get("_graph_deletes") != null and root.get("graph_deletes") != null) return error.InvalidBatchRequest;
        const value = if (allow_internal)
            root.get("_graph_deletes") orelse root.get("graph_deletes") orelse break :graph_deletes &.{}
        else
            root.get("graph_deletes") orelse break :graph_deletes &.{};
        if (value == .null) break :graph_deletes &.{};
        const parsed_graph_deletes = try parseGraphDeletes(alloc, value);
        errdefer freeGraphDeletes(alloc, parsed_graph_deletes);
        break :graph_deletes parsed_graph_deletes;
    };

    out.graph_endpoint_cleanup_guards = null;
    if (root.get("_graph_endpoint_cleanup_guards")) |value| {
        if (!allow_internal or value != .array or value.array.items.len > 256) return error.InvalidBatchRequest;
        out.graph_endpoint_cleanup_guards = try std.json.parseFromValue([]const @import("../storage/graph_cleanup_contract.zig").Guard, alloc, value, .{ .allocate = .alloc_always });
    }

    out.predicates = predicates: {
        const value = root.get("_predicates") orelse break :predicates &.{};
        if (!allow_internal or value != .array) return error.InvalidBatchRequest;
        const items = try alloc.alloc(db_types.TransactionVersionPredicate, value.array.items.len);
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
            const unique_absence = if (item.object.get("unique_absence")) |flag| if (flag == .bool and (version == 0 or !flag.bool)) flag.bool else return error.InvalidBatchRequest else false;
            const policy_requirement: @import("../usermgr/row_policy_authority.zig").MutationRequirement = if (item.object.get("row_policy_requirement")) |flag| requirement: {
                if (flag != .string) return error.InvalidBatchRequest;
                break :requirement std.meta.stringToEnum(@import("../usermgr/row_policy_authority.zig").MutationRequirement, flag.string) orelse return error.InvalidBatchRequest;
            } else .native;
            items[i] = .{
                .key = try alloc.dupe(u8, key_value.string),
                .expected_version = version,
                .expected_content_digest = digest,
                .unique_absence = unique_absence,
                .row_policy_requirement = policy_requirement,
            };
            initialized += 1;
        }
        break :predicates items;
    };
}

noinline fn parseBatchTransitions(alloc: std.mem.Allocator, root: std.json.ObjectMap, allow_internal: bool, local_owner: bool, document_versions_envelope: bool, out: *OwnedBatchRequest) !void {
    out.req.sync_level = sync_level: {
        if (root.get("sync_level")) |sync_level_value| {
            if (sync_level_value == .null) break :sync_level db_types.SyncLevel.propose;
            break :sync_level try syncLevelFromValue(sync_level_value);
        }
        break :sync_level db_types.SyncLevel.propose;
    };

    const graph_endpoint_cleanup = if (root.get("_graph_endpoint_cleanup")) |value| blk: {
        if (!allow_internal or value != .bool) return error.InvalidBatchRequest;
        break :blk value.bool;
    } else false;

    out.req.document_version_scheme = scheme: {
        // Isolated control commands never assign ordinary document versions.
        // Preserve their neutral request default across forwarding and native
        // owner calls: choosing legacy here adds an adjunct that their strict
        // single-purpose validators correctly reject. Ordinary unmarked row
        // replay retains its historical legacy interpretation.
        const isolated_control = graph_endpoint_cleanup or out.req.activate_range_tracking or out.relational_topology != null or out.relational_generation_gc != null or
            out.row_policy_publication != null or out.artifact_catalog != null or out.group_source != null or
            out.resolver_review != null or out.artifact_publication != null or out.artifact_publication_transport != null or
            out.leann_record_index != null or out.merge_proof_adoption != null;
        const value = root.get("_document_version_scheme") orelse break :scheme if (!allow_internal or local_owner or isolated_control) .admit_current else .legacy;
        if (!allow_internal or !document_versions_envelope) return error.InvalidBatchRequest;
        const version = try parseInternalU64(value);
        break :scheme switch (version) {
            0 => .legacy,
            1 => .revision_v1,
            else => return error.InvalidBatchRequest,
        };
    };
    out.req.timestamp_ns = timestamp: {
        const value = root.get("_timestamp_ns") orelse {
            if (out.req.document_version_scheme == .revision_v1) return error.InvalidBatchRequest;
            break :timestamp 0;
        };
        if (!allow_internal) return error.InvalidBatchRequest;
        break :timestamp try parseInternalU64(value);
    };

    out.req.reject_graph_transform_projections = reject: {
        const value = root.get("_reject_graph_transform_projections") orelse break :reject false;
        if (!allow_internal or value != .bool) return error.InvalidBatchRequest;
        break :reject value.bool;
    };

    out.split_checkpoint_range_start = null;
    out.split_checkpoint_range_end = null;
    out.req.split_checkpoint = checkpoint: {
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
        const kind: db_types.SplitReplicationCheckpoint.Kind = std.meta.stringToEnum(
            db_types.SplitReplicationCheckpoint.Kind,
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
        out.split_checkpoint_range_start = try alloc.dupe(u8, range_start);
        out.split_checkpoint_range_end = try alloc.dupe(u8, range_end);
        if (transition_id == 0 or attempt_epoch == 0) return error.InvalidBatchRequest;
        const history_policy = if (object.get("history_policy")) |item| policy: {
            const Policy = @import("../storage/committed_history.zig").Policy;
            if (kind != .destination_complete or item != .string or item.string.len != Policy.encoded_size * 2)
                return error.InvalidBatchRequest;
            var raw: [Policy.encoded_size]u8 = undefined;
            _ = std.fmt.hexToBytes(&raw, item.string) catch return error.InvalidBatchRequest;
            break :policy Policy.decode(&raw) catch return error.InvalidBatchRequest;
        } else null;
        break :checkpoint .{
            .kind = kind,
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .range_start = out.split_checkpoint_range_start.?,
            .range_end = out.split_checkpoint_range_end.?,
            .delta_sequence = delta_sequence,
            .history_policy = history_policy,
        };
    };

    out.req.split_replication = replication: {
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
        const operation = std.meta.stringToEnum(db_types.SplitReplicationContext.Operation, operation_value.string) orelse
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

    out.req.merge_replication = replication: {
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

    out.merge_page = null;
    if (root.get("_merge_page")) |value| {
        if (!allow_internal or out.req.merge_replication == null) return error.InvalidBatchRequest;
        out.merge_page = @import("../storage/db/merge_page_wire.zig").parseFromValue(alloc, value) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidBatchRequest,
        };
        if (out.merge_page.?.value.chunk != null and out.writes.len != 0) return error.InvalidBatchRequest;
        // A discriminant understood by the old checkpoint parser prevents an
        // older node from silently discarding progress and applying bare rows.
        const marker = root.get("_merge_checkpoint") orelse return error.InvalidBatchRequest;
        if (marker != .object or marker.object.count() != 1) return error.InvalidBatchRequest;
        const kind = marker.object.get("kind") orelse return error.InvalidBatchRequest;
        const expected_marker = if (out.merge_page.?.value.source.retention != null) "page_v6" else if (out.merge_page.?.value.source.integrity != null) "page_v5" else if (out.merge_page.?.value.next_snapshot_position != null) "page_v4" else if (out.merge_page.?.value.chunk != null) "page_v3" else "page_v1";
        if (kind != .string or !std.mem.eql(u8, kind.string, expected_marker)) return error.InvalidBatchRequest;
    }

    out.merge_artifacts = null;
    if (root.get("_merge_artifacts")) |value| {
        if (!allow_internal) return error.InvalidBatchRequest;
        out.merge_artifacts = try std.json.parseFromValue([]const db_types.BatchWrite, alloc, value, .{ .allocate = .alloc_always });
        // Validate against the complete request below: cleanup afterimages
        // require the planned page's guards as well as its private flags.
    }

    out.split_transition_key = null;
    out.req.split_transition = transition: {
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
        const kind = std.meta.stringToEnum(db_types.SplitTransitionMutation.Kind, kind_value.string) orelse
            return error.InvalidBatchRequest;
        const split_key = if (object.get("split_key")) |item| key: {
            if (item != .string) return error.InvalidBatchRequest;
            break :key item.string;
        } else "";
        if ((kind == .prepare or kind == .start) and split_key.len == 0) return error.InvalidBatchRequest;
        out.split_transition_key = try alloc.dupe(u8, split_key);
        break :transition .{
            .kind = kind,
            .transition_id = transition_id,
            .attempt_epoch = attempt_epoch,
            .destination_group_id = destination_group_id,
            .split_key = out.split_transition_key.?,
        };
    };

    out.req.merge_source_transition = transition: {
        const value = root.get("_merge_source_transition") orelse break :transition null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const kind_value = object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind_value != .string) return error.InvalidBatchRequest;
        const kind = std.meta.stringToEnum(
            db_types.MergeSourceTransitionMutation.Kind,
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

    out.merge_receiver_base_start = null;
    out.merge_receiver_base_end = null;
    out.merge_range_start = null;
    out.merge_source_catalogs = null;
    out.merge_range_end = null;
    out.req.merge_checkpoint = merge: {
        const value = root.get("_merge_checkpoint") orelse break :merge null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const kind_value = object.get("kind") orelse return error.InvalidBatchRequest;
        if (kind_value != .string) return error.InvalidBatchRequest;
        if (std.mem.eql(u8, kind_value.string, "restore_v3")) {
            if (out.restore_staging == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "artifact_catalog_v1")) {
            if (out.artifact_catalog == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "group_source_v1")) {
            if (out.group_source == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "sql_lock_v1")) {
            if (out.sql_lock == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "resolver_review_v1")) {
            if (out.resolver_review == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "artifact_publication_v1")) {
            if (out.artifact_publication == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "artifact_publication_transport_v1")) {
            if (out.artifact_publication_transport == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "leann_record_v1")) {
            if (out.leann_record_index == null or out.leann_record_bytes == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "merge_proof_adoption_v1")) {
            if (out.merge_proof_adoption == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "online_source_v4")) {
            if (out.online_source == null or object.count() != 1) return error.InvalidBatchRequest;
            break :merge null;
        }
        if (std.mem.eql(u8, kind_value.string, "page_v1") or std.mem.eql(u8, kind_value.string, "page_v2") or std.mem.eql(u8, kind_value.string, "page_v3") or std.mem.eql(u8, kind_value.string, "page_v4") or std.mem.eql(u8, kind_value.string, "page_v5") or std.mem.eql(u8, kind_value.string, "page_v6")) {
            if (out.merge_page == null) return error.InvalidBatchRequest;
            break :merge null;
        }
        const integrity_protocol = std.mem.startsWith(u8, kind_value.string, "page_v3_");
        const scope_protocol = std.mem.startsWith(u8, kind_value.string, "page_v4_");
        const tail_protocol = scope_protocol or integrity_protocol or std.mem.startsWith(u8, kind_value.string, "page_v2_");
        const page_protocol = tail_protocol or std.mem.startsWith(u8, kind_value.string, "page_v1_");
        const kind = std.meta.stringToEnum(
            db_types.MergeReplicationCheckpoint.Kind,
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
        if (object.get("page_source_catalogs")) |catalogs_value| {
            if (page_source == null) return error.InvalidBatchRequest;
            out.merge_source_catalogs = try std.json.parseFromValue(@import("../storage/db/artifact_inventory.zig").Catalogs, alloc, catalogs_value, .{ .allocate = .alloc_always });
        }
        const page_receiver: ?DocIdentityNamespace = if (object.get("page_receiver_namespace")) |receiver_value| blk: {
            var owned = try std.json.parseFromValue(DocIdentityNamespace, alloc, receiver_value, .{});
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
        out.merge_receiver_base_start = try alloc.dupe(u8, base_start_value.string);
        out.merge_receiver_base_end = try alloc.dupe(u8, base_end_value.string);
        out.merge_range_start = try alloc.dupe(u8, merged_start_value.string);
        out.merge_range_end = try alloc.dupe(u8, merged_end_value.string);
        const namespace: ?DocIdentityNamespace = namespace: {
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
            .receiver_base_start = out.merge_receiver_base_start.?,
            .receiver_base_end = out.merge_receiver_base_end.?,
            .merged_start = out.merge_range_start.?,
            .merged_end = out.merge_range_end.?,
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
            .page_source_catalogs = if (out.merge_source_catalogs) |catalogs_owned| catalogs_owned.value else null,
        };
    };

    out.transaction_participants = &.{};
    out.req.transaction = transaction: {
        const value = root.get("_transaction") orelse break :transaction null;
        if (!allow_internal or value != .object) return error.InvalidBatchRequest;
        const object = value.object;
        const phase_value = object.get("phase") orelse return error.InvalidBatchRequest;
        const txn_value = object.get("txn_id") orelse return error.InvalidBatchRequest;
        if (phase_value != .string or txn_value != .string or txn_value.string.len != 32) return error.InvalidBatchRequest;
        var txn_id: db_types.TxnId = undefined;
        _ = std.fmt.hexToBytes(&txn_id, txn_value.string) catch return error.InvalidBatchRequest;
        const external_history_begin = std.mem.eql(u8, phase_value.string, "external_begin_completion_v1");
        const external_history_prepare = std.mem.eql(u8, phase_value.string, "external_prepare_completion_v1");
        const external_vote_admitted_at_ns = if (object.get("external_vote_admitted_at_ns")) |admission_value| admitted: {
            if (!external_history_prepare) return error.InvalidBatchRequest;
            const admitted = try parseInternalU64(admission_value);
            if (admitted == 0) return error.InvalidBatchRequest;
            break :admitted admitted;
        } else 0;
        const external_history_resolve = std.mem.eql(u8, phase_value.string, "external_resolve_completion_v1");
        const revision_begin = std.mem.eql(u8, phase_value.string, "external_begin_revisions_v1") or
            (external_history_begin and object.contains("document_revision_scope"));
        const external_begin = external_history_begin or revision_begin or std.mem.eql(u8, phase_value.string, "external_begin_v1");
        if (revision_begin != object.contains("document_revision_scope") or object.contains("document_revision_handoff") or object.contains("publication_capture")) return error.InvalidBatchRequest;
        const external_prepare = external_history_prepare or std.mem.eql(u8, phase_value.string, "external_prepare_v1") or std.mem.eql(u8, phase_value.string, "external_prepare_ranges_v1");
        const external_resolve = external_history_resolve or std.mem.eql(u8, phase_value.string, "external_resolve_v1");
        const external_ack = std.mem.eql(u8, phase_value.string, "external_acknowledge_v1");
        const external_ack_many = std.mem.eql(u8, phase_value.string, "external_acknowledge_many_v1");
        const external_value = object.get("external_decision_owner");
        if ((external_begin or external_prepare or external_resolve or external_ack or external_ack_many) != (external_value != null) or
            external_prepare != object.contains("external_activation")) return error.InvalidBatchRequest;
        const external_owner: ?ExternalDecisionOwner = if (external_value) |item| try parseExternalDecisionOwner(alloc, item) else null;
        const external_activation = if (external_prepare) switch (object.get("external_activation").?) {
            .bool => |flag| flag,
            else => return error.InvalidBatchRequest,
        } else false;
        const ordinary_completion_begin = std.mem.eql(u8, phase_value.string, "begin_ordinary_completion_v1");
        const ordinary_plain_begin = std.mem.eql(u8, phase_value.string, "begin_ordinary_v1");
        const ordinary_begin = ordinary_completion_begin or ordinary_plain_begin;
        const ordinary_completion_prepare = std.mem.eql(u8, phase_value.string, "prepare_ordinary_completion_v1");
        const ordinary_plain_prepare = std.mem.eql(u8, phase_value.string, "prepare_ordinary_v1");
        const ordinary_prepare = ordinary_completion_prepare or ordinary_plain_prepare;
        const legacy_completion_begin = std.mem.eql(u8, phase_value.string, "begin_legacy_completion_v1");
        const legacy_plain_begin = std.mem.eql(u8, phase_value.string, "begin_legacy_v1");
        const legacy_begin = legacy_completion_begin or legacy_plain_begin;
        const legacy_completion_prepare = std.mem.eql(u8, phase_value.string, "prepare_legacy_completion_v1");
        const legacy_plain_prepare = std.mem.eql(u8, phase_value.string, "prepare_legacy_v1");
        const legacy_prepare = legacy_completion_prepare or legacy_plain_prepare;
        const legacy_phase = legacy_begin or legacy_prepare;
        if (legacy_phase != object.contains("legacy_producer")) return error.InvalidBatchRequest;
        if (legacy_phase) {
            const marker = object.get("legacy_producer").?;
            if (marker != .bool or !marker.bool) return error.InvalidBatchRequest;
        }
        const ordinary_phase = ordinary_begin or ordinary_prepare;
        if (ordinary_phase != object.contains("ordinary_producer")) return error.InvalidBatchRequest;
        if (ordinary_phase) {
            const marker = object.get("ordinary_producer").?;
            if (marker != .bool or !marker.bool) return error.InvalidBatchRequest;
        }
        const scoped_begin = std.mem.eql(u8, phase_value.string, "begin_source_scope_v1");
        const scoped_prepare = std.mem.eql(u8, phase_value.string, "prepare_source_scope_v1");
        const scoped_resolve = std.mem.eql(u8, phase_value.string, "resolve_source_scope_v1");
        const scoped_phase = scoped_begin or scoped_prepare or scoped_resolve;
        if (scoped_phase != object.contains("source_scope")) return error.InvalidBatchRequest;
        var source_scope: ?@import("../storage/source_scope.zig").Reference = null;
        if (scoped_phase) {
            var parsed_scope = std.json.parseFromValue(@import("../storage/source_scope.zig").Reference, alloc, object.get("source_scope").?, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidBatchRequest;
            defer parsed_scope.deinit();
            try parsed_scope.value.validate();
            source_scope = parsed_scope.value;
        }
        const receipts_begin = scoped_begin or std.mem.eql(u8, phase_value.string, "begin_source_checkpoint_v2");
        const receipts_prepare = (scoped_prepare and object.contains("source_checkpoint")) or std.mem.eql(u8, phase_value.string, "prepare_source_checkpoint_v2");
        const checkpoint_begin = receipts_begin or std.mem.eql(u8, phase_value.string, "begin_source_checkpoint_v1");
        const checkpoint_prepare = receipts_prepare or std.mem.eql(u8, phase_value.string, "prepare_source_checkpoint_v1");
        if (receipts_begin != object.contains("source_checkpoint_receipts")) return error.InvalidBatchRequest;
        if (receipts_begin) {
            const marker = object.get("source_checkpoint_receipts").?;
            if (marker != .bool or !marker.bool) return error.InvalidBatchRequest;
        }
        const source_begin = checkpoint_begin or std.mem.eql(u8, phase_value.string, "begin_source_completion_v1");
        const source_prepare = scoped_prepare or checkpoint_prepare or std.mem.eql(u8, phase_value.string, "prepare_source_completion_v1");
        const source_resolve = scoped_resolve or std.mem.eql(u8, phase_value.string, "resolve_source_completion_v1");
        const source_phase = source_begin or source_prepare or source_resolve;
        if (source_phase != object.contains("source_purpose")) return error.InvalidBatchRequest;
        var source_purpose: ?[32]u8 = null;
        if (source_phase) {
            var purpose = std.json.parseFromValue([32]u8, alloc, object.get("source_purpose").?, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidBatchRequest;
            defer purpose.deinit();
            if (std.mem.allEqual(u8, &purpose.value, 0)) return error.InvalidBatchRequest;
            source_purpose = purpose.value;
        }
        if ((checkpoint_begin or checkpoint_prepare) != object.contains("source_checkpoint")) return error.InvalidBatchRequest;
        if (checkpoint_begin) {
            const marker = object.get("source_checkpoint").?;
            if (marker != .bool or !marker.bool) return error.InvalidBatchRequest;
        }
        if (checkpoint_prepare) {
            out.transaction_source_checkpoint = std.json.parseFromValue(@import("../storage/source_checkpoint.zig").Command, alloc, object.get("source_checkpoint").?, .{ .allocate = .alloc_always, .ignore_unknown_fields = false }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidBatchRequest;
            const checkpoint = out.transaction_source_checkpoint.?.value;
            try checkpoint.validate();
            if (receipts_prepare != (checkpoint.receipt_window != 0)) return error.InvalidBatchRequest;
            if (!std.mem.eql(u8, &checkpoint.purpose, &source_purpose.?)) return error.SourceTransactionIdentityConflict;
            if (source_scope) |scope| if (!std.mem.eql(u8, &scope.binding, &checkpoint.binding) or scope.sequence != checkpoint.sequence) return error.SourceTransactionIdentityConflict;
        }
        const completion_begin = external_history_begin or legacy_completion_begin or ordinary_completion_begin or source_begin or std.mem.eql(u8, phase_value.string, "begin_completion_v1");
        if (completion_begin != object.contains("completion_admission")) return error.InvalidBatchRequest;
        const completion_prepare = external_history_prepare or legacy_completion_prepare or ordinary_completion_prepare or source_prepare or std.mem.eql(u8, phase_value.string, "prepare_completion_v1");
        const completion_resolve = external_history_resolve or source_resolve or std.mem.eql(u8, phase_value.string, "resolve_completion_v1");
        const completion_publish = std.mem.eql(u8, phase_value.string, "publish_completion_v1");
        const completion_release = std.mem.eql(u8, phase_value.string, "release_external_completion_v1");
        if (completion_publish != object.contains("completion_certificate") or
            completion_release != object.contains("completion_release")) return error.InvalidBatchRequest;
        if (!completion_begin and !completion_prepare and !completion_resolve and !completion_publish and !completion_release and object.contains("completion_owner")) return error.InvalidBatchRequest;
        var completion_owner: ?@import("../storage/transaction_completion.zig").RoutedOwner = null;
        if (object.get("completion_owner")) |item| {
            var parsed_owner = std.json.parseFromValue(@import("../storage/transaction_completion.zig").RoutedOwner, alloc, item, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidBatchRequest;
            defer parsed_owner.deinit();
            completion_owner = parsed_owner.value;
        }
        if (external_history_begin or external_history_prepare or external_history_resolve) {
            try validateExternalCompletionOwner(external_owner.?, completion_owner orelse return error.InvalidBatchRequest);
        }
        if (source_begin) {
            const retain = object.get("retain_terminal") orelse return error.InvalidBatchRequest;
            if (retain != .bool or !retain.bool) return error.InvalidBatchRequest;
        }
        if (completion_release) {
            const binding = completion_owner orelse return error.InvalidBatchRequest;
            var parsed = try std.json.parseFromValue(db_types.CompletionRelease, alloc, object.get("completion_release").?, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" });
            defer parsed.deinit();
            try parsed.value.validate(binding);
            break :transaction .{ .publish_completion = .{ .txn_id = txn_id, .completion_owner = binding, .release = parsed.value } };
        }
        if (completion_publish) {
            const raw = object.get("completion_certificate").?;
            if (raw != .string) return error.InvalidBatchRequest;
            const binding = completion_owner orelse return error.InvalidBatchRequest;
            try validateCompletionPublication(alloc, txn_id, binding, raw.string);
            out.transaction_completion_certificate = try alloc.dupe(u8, raw.string);
            break :transaction .{ .publish_completion = .{ .txn_id = txn_id, .completion_owner = binding, .certificate = out.transaction_completion_certificate.? } };
        }
        if (std.mem.eql(u8, phase_value.string, "begin") or external_begin or ordinary_plain_begin or legacy_plain_begin or completion_begin) {
            const participants_value = object.get("participants") orelse return error.InvalidBatchRequest;
            if (participants_value != .array or participants_value.array.items.len == 0) return error.InvalidBatchRequest;
            out.transaction_participants = try parseBatchParticipants(alloc, participants_value.array.items);
            const begin_timestamp = try parseInternalU64(object.get("begin_timestamp") orelse return error.InvalidBatchRequest);
            const revision_scope = if (object.get("document_revision_scope")) |scope_value| scope: {
                const parsed_scope = try std.json.parseFromValue(@import("../storage/document_revision_reservation.zig").Scope, alloc, scope_value, .{ .ignore_unknown_fields = false });
                defer parsed_scope.deinit();
                try parsed_scope.value.validate();
                if (!std.mem.eql(u8, &parsed_scope.value.transaction_id, &txn_id) or parsed_scope.value.begin_timestamp != begin_timestamp or
                    !std.mem.eql(u8, &parsed_scope.value.metadata_incarnation, &external_owner.?.metadata_incarnation) or out.req.restore_staging_scope != null) return error.InvalidBatchRequest;
                break :scope parsed_scope.value;
            } else null;
            if (completion_begin) {
                const raw = object.get("completion_admission").?;
                if (raw != .string) return error.InvalidBatchRequest;
                try validateCompletionBegin(alloc, raw.string, txn_id, out.transaction_participants, completion_owner, try parseInternalU64(object.get("topology_epoch") orelse return error.InvalidBatchRequest));
                if (external_history_begin) {
                    const retain = object.get("retain_terminal") orelse return error.InvalidBatchRequest;
                    if (retain != .bool or !retain.bool or completion_owner == null) return error.InvalidBatchRequest;
                }
                if (ordinary_begin or legacy_begin or external_history_begin) try validateOrdinaryCompletionBegin(alloc, raw.string);
                out.transaction_completion_admission = try alloc.dupe(u8, raw.string);
            }
            if ((ordinary_completion_begin or legacy_completion_begin) and completion_owner == null) return error.InvalidBatchRequest;
            break :transaction .{ .begin = .{
                .ordinary_producer = ordinary_begin,
                .legacy_producer = legacy_begin,
                .document_revision_scope = revision_scope,
                .external_decision_owner = external_owner,
                .sql_locks = if (object.get("sql_locks")) |lock_value| ref: {
                    if (!external_begin) return error.InvalidBatchRequest;
                    const parsed_ref = try std.json.parseFromValue(@import("../storage/pessimistic_lock/transaction.zig").Ref, alloc, lock_value, .{});
                    defer parsed_ref.deinit();
                    try parsed_ref.value.validate(txn_id, external_owner.?);
                    break :ref parsed_ref.value;
                } else null,
                .txn_id = txn_id,
                .begin_timestamp = begin_timestamp,
                .created_at_ns = try parseInternalU64(object.get("created_at_ns") orelse return error.InvalidBatchRequest),
                .topology_epoch = try parseInternalU64(object.get("topology_epoch") orelse return error.InvalidBatchRequest),
                .retain_terminal = if (object.get("retain_terminal")) |item| switch (item) {
                    .bool => |flag| flag,
                    else => return error.InvalidBatchRequest,
                } else false,
                .participants = out.transaction_participants,
                .completion_admission = out.transaction_completion_admission,
                .completion_owner = completion_owner,
                .source_purpose = source_purpose,
                .source_scope = source_scope,
                .source_checkpoint = checkpoint_begin,
                .source_checkpoint_receipts = receipts_begin,
            } };
        }
        if (completion_prepare and completion_owner == null) return error.InvalidBatchRequest;
        if (external_history_prepare and external_activation) return error.InvalidBatchRequest;
        if (completion_prepare) {
            if (out.req.timestamp_ns == 0 or out.req.document_version_scheme != .revision_v1) return error.InvalidBatchRequest;
            try completion_owner.?.validate();
            if (completion_owner.?.topology_epoch != try parseInternalU64(object.get("topology_epoch") orelse return error.InvalidBatchRequest)) return error.InvalidBatchRequest;
        }
        const guarded_prepare = std.mem.eql(u8, phase_value.string, "prepare_ranges_v1") or std.mem.eql(u8, phase_value.string, "external_prepare_ranges_v1") or ((completion_prepare or ordinary_plain_prepare or legacy_plain_prepare) and out.range_guards != null and out.range_guards.?.value.len != 0);
        if (guarded_prepare != (out.range_guards != null and out.range_guards.?.value.len != 0)) return error.InvalidBatchRequest;
        if (std.mem.eql(u8, phase_value.string, "prepare") or guarded_prepare or ordinary_plain_prepare or legacy_plain_prepare or completion_prepare or external_prepare) break :transaction .{ .prepare = .{
            .external_decision_owner = external_owner,
            .external_activation = external_activation,
            .external_vote_admitted_at_ns = external_vote_admitted_at_ns,
            .ordinary_producer = ordinary_prepare,
            .legacy_producer = legacy_prepare,
            .txn_id = txn_id,
            .topology_epoch = try parseInternalU64(object.get("topology_epoch") orelse return error.InvalidBatchRequest),
            .completion_owner = completion_owner,
            .source_purpose = source_purpose,
            .source_scope = source_scope,
            .source_checkpoint = if (out.transaction_source_checkpoint) |checkpoint| checkpoint.value else null,
        } };
        if (std.mem.eql(u8, phase_value.string, "resolve") or completion_resolve or external_resolve) {
            if (completion_resolve) {
                const binding = completion_owner orelse return error.InvalidBatchRequest;
                try binding.validate();
            }
            const status_value = object.get("status") orelse return error.InvalidBatchRequest;
            if (status_value != .string) return error.InvalidBatchRequest;
            const status = std.meta.stringToEnum(db_types.TxnStatus, status_value.string) orelse return error.InvalidBatchRequest;
            if (status == .pending) return error.InvalidBatchRequest;
            break :transaction .{ .resolve = .{
                .external_decision_owner = external_owner,
                .completion_owner = completion_owner,
                .source_purpose = source_purpose,
                .source_scope = source_scope,
                .txn_id = txn_id,
                .status = status,
                .commit_version = try parseInternalU64(object.get("commit_version") orelse return error.InvalidBatchRequest),
            } };
        }
        if (std.mem.eql(u8, phase_value.string, "acknowledge_many") or external_ack_many) {
            const values = object.get("participants") orelse return error.InvalidBatchRequest;
            if (values != .array or values.array.items.len == 0 or values.array.items.len > 64) return error.InvalidBatchRequest;
            out.transaction_participants = try parseBatchParticipants(alloc, values.array.items);
            break :transaction .{ .acknowledge_many = .{ .external_decision_owner = external_owner, .txn_id = txn_id, .participants = out.transaction_participants } };
        }
        if (std.mem.eql(u8, phase_value.string, "acknowledge") or external_ack) {
            const participant_value = object.get("participant") orelse return error.InvalidBatchRequest;
            if (participant_value != .string or participant_value.string.len == 0) return error.InvalidBatchRequest;
            out.transaction_participants = try parseBatchParticipants(alloc, &.{participant_value});
            break :transaction .{ .acknowledge = .{
                .external_decision_owner = external_owner,
                .txn_id = txn_id,
                .participant = out.transaction_participants[0],
            } };
        }
        if (std.mem.eql(u8, phase_value.string, "cleanup")) break :transaction .{ .cleanup = .{
            .txn_id = txn_id,
            .cutoff_timestamp = try parseInternalU64(object.get("cutoff_timestamp") orelse return error.InvalidBatchRequest),
            .retained_cutoff_timestamp = try parseInternalU64(object.get("retained_cutoff_timestamp") orelse return error.InvalidBatchRequest),
        } };
        return error.InvalidBatchRequest;
    };
}

noinline fn finishBatchRequest(root: std.json.ObjectMap, allow_internal: bool, local_owner: bool, versioned: bool, out: *OwnedBatchRequest) !void {
    const atomic_owner = local_owner and out.req.transaction == null and out.req.reject_graph_transform_projections;
    if ((out.req.split_transition != null or out.req.merge_source_transition != null) and
        (out.writes.len != 0 or out.deletes.len != 0 or out.transforms.len != 0 or
            out.graph_writes.len != 0 or out.graph_deletes.len != 0 or
            out.predicates.len != 0 or out.req.split_checkpoint != null or out.req.split_replication != null or out.req.merge_replication != null or
            out.req.split_transition != null and out.req.merge_source_transition != null or
            out.req.merge_checkpoint != null or out.req.transaction != null))
    {
        return error.InvalidBatchRequest;
    }
    if (out.req.transaction != null and (out.req.split_checkpoint != null or out.req.split_replication != null or out.req.merge_replication != null or
        out.req.split_transition != null or out.req.merge_source_transition != null or out.req.merge_checkpoint != null))
    {
        return error.InvalidBatchRequest;
    }
    if (out.predicates.len != 0 and out.req.transaction == null and !local_owner) return error.InvalidBatchRequest;
    if (out.range_guards != null and !atomic_owner and (out.req.transaction == null or out.req.transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (out.integrity.len != 0 and !atomic_owner and (out.req.transaction == null or out.req.transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (out.integrity_commands != null and ((!atomic_owner and (out.req.transaction == null or out.req.transaction.? != .prepare)) or out.integrity.len != 0)) return error.InvalidBatchRequest;
    if (out.relational_activation != null and !atomic_owner and (out.req.transaction == null or out.req.transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (out.relational_retirement != null and !atomic_owner and (out.req.transaction == null or out.req.transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (out.relational_index_maintenance) |command| {
        try command.value.validate();
        if ((!atomic_owner and (out.req.transaction == null or out.req.transaction.? != .prepare)) or out.writes.len != 0 or out.deletes.len != 0 or out.transforms.len != 0 or out.predicates.len != 0 or out.integrity.len != 0 or out.integrity_commands != null or out.relational_activation != null or out.relational_retirement != null or out.req.relational_repair or out.req.restore_staging_scope != null) return error.InvalidBatchRequest;
    }
    if (out.relational_topology != null and (out.req.transaction != null or out.writes.len != 0 or out.deletes.len != 0 or out.transforms.len != 0 or out.predicates.len != 0 or out.integrity.len != 0 or out.integrity_commands != null or out.relational_activation != null or out.relational_retirement != null or out.relational_index_maintenance != null or out.req.split_checkpoint != null or out.req.split_replication != null or out.req.split_transition != null or out.req.merge_checkpoint != null or out.req.merge_replication != null or out.req.merge_source_transition != null)) return error.InvalidBatchRequest;
    if (out.relational_generation_gc != null and (out.req.transaction != null or out.relational_topology != null or out.row_policy_publication != null or out.writes.len != 0 or out.deletes.len != 0 or out.transforms.len != 0 or out.predicates.len != 0 or out.integrity.len != 0 or out.integrity_commands != null or out.relational_activation != null or out.relational_retirement != null or out.relational_index_maintenance != null or out.req.split_checkpoint != null or out.req.split_replication != null or out.req.split_transition != null or out.req.merge_checkpoint != null or out.req.merge_replication != null or out.req.merge_source_transition != null)) return error.InvalidBatchRequest;
    if (out.relational_topology != null and (out.req.relational_schema_version != null or out.req.relational_integrity_generation_set != null or out.req.relational_repair)) return error.InvalidBatchRequest;
    if (out.req.transaction) |mutation| switch (mutation) {
        .begin, .resolve, .publish_completion, .acknowledge, .acknowledge_many, .cleanup => if (out.writes.len != 0 or out.deletes.len != 0 or out.transforms.len != 0 or out.predicates.len != 0)
            return error.InvalidBatchRequest,
        .prepare => {},
    };
    if (out.req.split_checkpoint != null and out.req.split_checkpoint.?.kind == .source_ack and
        (out.writes.len != 0 or out.deletes.len != 0 or out.transforms.len != 0 or
            out.graph_writes.len != 0 or out.graph_deletes.len != 0 or
            out.req.split_replication != null))
    {
        return error.InvalidBatchRequest;
    }
    if (out.req.merge_checkpoint != null) {
        if (out.merge_artifacts != null) return error.InvalidBatchRequest;
        if (out.req.split_checkpoint != null or out.req.split_replication != null or out.req.split_transition != null or
            out.req.merge_source_transition != null or
            out.transforms.len != 0 or out.predicates.len != 0 or out.writes.len != 0 or
            out.deletes.len != 0)
            return error.InvalidBatchRequest;
    }
    if (out.req.merge_replication != null and
        (out.req.split_checkpoint != null or out.req.split_replication != null or out.req.split_transition != null or
            out.req.merge_source_transition != null or out.req.transaction != null or
            out.transforms.len != 0 or out.predicates.len != 0))
    {
        return error.InvalidBatchRequest;
    }
    if (out.req.merge_replication) |replication| if (out.req.merge_checkpoint) |checkpoint| {
        if (replication.transition_id != checkpoint.transition_id or
            replication.donor_group_id != checkpoint.donor_group_id or
            replication.receiver_group_id != checkpoint.receiver_group_id or
            replication.copy_attempt.order(checkpoint.copy_attempt) != .eq)
            return error.InvalidBatchRequest;
    };

    out.req.integrity = out.integrity;
    out.req.integrity_commands = if (out.integrity_commands) |commands| commands.value else &.{};
    out.req.range_guards = if (out.range_guards) |guards| guards.value else &.{};
    out.req.relational_activation = if (out.relational_activation) |activation| activation.value else null;
    out.req.relational_retirement = if (out.relational_retirement) |retirement| retirement.value else null;
    out.req.relational_index_maintenance = if (out.relational_index_maintenance) |maintenance| maintenance.value else null;
    out.req.relational_index_owner_control = if (out.relational_index_owner_control) |control| control.value else null;
    out.req.relational_topology = if (out.relational_topology) |topology| topology.value else null;
    out.req.relational_generation_gc = if (out.relational_generation_gc) |page| page.value else null;
    out.req.row_policy_publication = if (out.row_policy_publication) |publication| publication.value else null;
    out.req.row_policy_install_bundle = out.row_policy_install_bundle orelse "";
    out.req.restore_staging = if (out.restore_staging) |control| control.value else null;
    out.req.online_source = if (out.online_source) |control| control.value else null;
    out.req.group_source = if (out.group_source) |control| control.value else null;
    out.req.sql_lock = if (out.sql_lock) |control| control.value else null;
    out.req.resolver_review = if (out.resolver_review) |control| control.value else null;
    out.req.artifact_catalog = if (out.artifact_catalog) |control| control.value else null;
    out.req.artifact_publication = if (out.artifact_publication) |control| control.value else null;
    out.req.artifact_publication_transport = if (out.artifact_publication_transport) |control| control.value else null;
    out.req.leann_record = if (out.leann_record_index) |name| .{ .index_name = name, .record = out.leann_record_bytes.? } else null;
    out.req.merge_proof_adoption = if (out.merge_proof_adoption) |control| control.value else null;
    out.req.writes = out.writes;
    out.req.deletes = out.deletes;
    out.req.transforms = out.transforms;
    out.req.graph_endpoint_cleanup_planned = if (root.get("_graph_endpoint_cleanup_planned")) |value| if (allow_internal and value == .bool) value.bool else return error.InvalidBatchRequest else false;
    out.req.graph_endpoint_cleanup = if (root.get("_graph_endpoint_cleanup")) |value| if (allow_internal and value == .bool) value.bool else return error.InvalidBatchRequest else false;
    out.req.graph_writes = out.graph_writes;
    out.req.graph_deletes = out.graph_deletes;
    out.req.graph_endpoint_cleanup_guards = if (out.graph_endpoint_cleanup_guards) |guards| guards.value else &.{};
    out.req.predicates = out.predicates;
    out.req.merge_page = if (out.merge_page) |value| value.value else null;
    out.req.merge_artifacts = if (out.merge_artifacts) |value| value.value else &.{};
    if (atomic_owner) try out.req.requireAtomicIntentShape();
    try validateRowPolicyPublicationRequest(out.req);
    try @import("../storage/range_protection.zig").validateRequest(out.req);
    try @import("../storage/db/relational/indexes/raft_contract.zig").validateBatchRequest(out.req);
    try db_types.validateLeannRecordCommand(out.req);
    if (!allow_internal) {
        const keys = @import("../storage/internal_keys.zig");
        for (out.req.writes) |write| if (keys.isGraphEndpointCleanupControlKey(write.key)) return error.InvalidBatchRequest;
        for (out.req.deletes) |key| if (keys.isGraphEndpointCleanupControlKey(key)) return error.InvalidBatchRequest;
        for (out.req.transforms) |transform| if (keys.isGraphEndpointCleanupControlKey(transform.key)) return error.InvalidBatchRequest;
    }
    try @import("../storage/db/resolver_review.zig").validateRequest(out.req);
    try validateGraphBatchCommand(out.req);
    try db_types.validateMergeArtifacts(out.req);
    try merge_pages.validateRequest(out.req);
    try @import("../storage/db/merge_proof_adoption.zig").validateRequest(out.req);
    try @import("../storage/db/online_source_contract.zig").validateRequest(out.req);
    try @import("../storage/db/artifact_inventory.zig").validateRequest(out.req);
    if (@import("../schema/domain_handoff.zig").present(out.req.merge_artifacts)) return error.InvalidBatchRequest;
    if (requiresRowSemanticsEnvelope(out.req.writes, out.req.predicates) and !versioned) return error.InvalidBatchRequest;
}

noinline fn parseBatchParticipants(alloc: std.mem.Allocator, items: []const std.json.Value) ![][]const u8 {
    const participants = try alloc.alloc([]const u8, items.len);
    var initialized: usize = 0;
    errdefer {
        for (participants[0..initialized]) |value| alloc.free(value);
        alloc.free(participants);
    }
    for (items, participants) |item, *target| {
        if (item != .string or item.string.len == 0) return error.InvalidBatchRequest;
        target.* = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return participants;
}

pub fn encodeBatchResponse(alloc: std.mem.Allocator, result: BatchResult) ![]u8 {
    return try ant_json.valueAlloc(alloc, result, .{ .emit_null_optional_fields = false });
}

test "internal batch preserves JSON null provenance without public injection" {
    const alloc = std.testing.allocator;
    const encoded = try encodeBatchRequest(alloc, .{ .writes = &.{.{ .key = "row", .value = "{\"j\":null,\"sql_null\":null}", .json_null_fields = &.{"j"} }} });
    defer alloc.free(encoded);
    var parsed = try parseInternalBatchRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.writes[0].json_null_fields.len);
    try std.testing.expectEqualStrings("j", parsed.req.writes[0].json_null_fields[0]);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, "{\"inserts\":{\"row\":{\"j\":1}},\"_json_null_fields\":{\"row\":[\"j\"]}}"));
}

/// Features older object decoders silently discard require a distinct root
/// envelope. Raft and HTTP use this same codec and fail closed on old readers.
pub fn requiresRowSemanticsEnvelope(writes: anytype, predicates: []const db_types.TransactionVersionPredicate) bool {
    if (@import("../schema/domain_handoff.zig").present(writes)) return true;
    for (writes) |write| if (write.json_null_fields.len != 0) return true;
    for (predicates) |predicate| if (predicate.unique_absence) return true;
    return false;
}

// Native mutation validation uses a specific graph error; the wire codec
// retains its invalid-request contract for malformed ownership combinations.
fn validateGraphBatchCommand(req: db_types.BatchRequest) !void {
    db_types.validateGraphEndpointCleanupCommand(req) catch |err| switch (err) {
        error.InvalidGraphEdges => return error.InvalidBatchRequest,
        else => return err,
    };
}

pub fn validateRowPolicyPublicationRequest(req: db_types.BatchRequest) !void {
    const publication = req.row_policy_publication orelse {
        if (req.row_policy_install_bundle.len != 0) return error.InvalidBatchRequest;
        return;
    };
    if (publication.table_id == 0 or publication.expected_generation == 0 or
        publication.expected_catalog_epoch == 0 or publication.owner_group_id == 0 or
        req.row_policy_install_bundle.len == 0 or
        req.row_policy_install_bundle.len > @import("../system_catalog/policies.zig").max_install_snapshot_bytes)
        return error.InvalidBatchRequest;
    // Isolated control commands must round-trip before entering Raft. Compare
    // every other field with its semantic default, including future fields;
    // accepting an encoder-only field could otherwise poison committed apply.
    const defaults: db_types.BatchRequest = .{};
    inline for (comptime std.meta.fieldNames(db_types.BatchRequest)) |name| {
        if (comptime std.mem.eql(u8, name, "row_policy_publication") or std.mem.eql(u8, name, "row_policy_install_bundle")) continue;
        const value = @field(req, name);
        const info = @typeInfo(@TypeOf(value));
        if (comptime info == .pointer and info.pointer.size == .slice) {
            if (comptime @field(defaults, name).len != 0) @compileError("review nonempty row-policy control default: " ++ name);
            if (value.len != 0) return error.InvalidBatchRequest;
        } else if (!@import("../storage/control_field_defaults.zig").isolatedFieldMatchesDefault(name, value, @field(defaults, name))) return error.InvalidBatchRequest;
    }
}

pub fn requiresDocumentVersionsEnvelope(req: db_types.BatchRequest) bool {
    if (req.document_version_scheme == .revision_v1) return true;
    if (req.online_source) |command| if (command == .admit and command.admit.ttl_sources) return true;
    if (req.merge_page) |page| if (page.document_version_floor != 0 or page.ttl_sources.len != 0 or (if (page.chunk) |chunk| chunk.ttl_source != null else false)) return true;
    if (req.restore_staging) |control| switch (control) {
        .import_page, .rewrite_page => |page| {
            if (page.document_timestamps != null) return true;
            for (page.timestamps) |timestamp| if (timestamp.ttl_source_ns != null) return true;
        },
        else => {},
    };
    return false;
}

pub fn encodeBatchRequest(alloc: std.mem.Allocator, req: db_types.BatchRequest) ![]u8 {
    return encodeProducerBatchRequest(alloc, req, false) catch |err| switch (err) {
        // This memory writer can fail only when allocation fails.
        error.WriteFailed => error.OutOfMemory,
        else => err,
    };
}

/// A pre-upgrade leader rejects the new envelope before proposing anything.
/// Preserve the plan's numeric tokens and let the aware proposer enforce the
/// exact PREPARE floor for every applying Raft voter.
pub fn encodeRoutedBatchRequest(alloc: std.mem.Allocator, req: db_types.BatchRequest) ![]u8 {
    const body = try encodeBatchRequest(alloc, req);
    if (!requiresExactPrepareProtocol(req)) return body;
    defer alloc.free(body);
    return std.json.Stringify.valueAlloc(alloc, [2][]const u8{ exact_prepare_envelope, body }, .{});
}

/// A local compiled owner evaluates optimistic predicates under its apply
/// fence. Remote replicated batches still require the prepare protocol and
/// continue to use encodeBatchRequest, which rejects unscoped predicates.
pub fn encodeOwnerBatchRequest(alloc: std.mem.Allocator, req: db_types.BatchRequest) ![]u8 {
    return encodeProducerBatchRequest(alloc, req, true) catch |err| switch (err) {
        error.WriteFailed => error.OutOfMemory,
        else => err,
    };
}

fn encodeProducerBatchRequest(alloc: std.mem.Allocator, req: db_types.BatchRequest, local_owner: bool) ![]u8 {
    // Scoped source authority is intrinsic to validated transaction resolution;
    // a generic row envelope cannot manufacture that authority.
    if (req.source_producer == .scoped_source or (req.source_producer != .unlabelled and req.transaction != null)) return error.InvalidBatchRequest;
    const encoded = try encodeBatchRequestOwned(alloc, req, local_owner);
    if (req.source_producer == .unlabelled) return encoded;
    defer alloc.free(encoded);
    return std.fmt.allocPrint(alloc, "[\"source-producer-batch-v1\",\"{s}\",{s}]", .{ @tagName(req.source_producer), encoded });
}

fn validateDocumentRevisionHandoff(req: db_types.BatchRequest) !void {
    const transaction = req.transaction orelse return error.InvalidBatchRequest;
    if (transaction != .prepare or transaction.prepare.external_activation or req.restore_staging_scope != null or req.restore_staging_plan_id != null or req.document_version_scheme != .revision_v1) return error.InvalidBatchRequest;
    const demand = transaction.prepare.document_revision_handoff orelse return error.InvalidBatchRequest;
    try demand.validate();
    const owner = transaction.prepare.external_decision_owner orelse return error.InvalidBatchRequest;
    try owner.validate();
    if (!std.mem.eql(u8, &demand.scope.transaction_id, &transaction.prepare.txn_id) or !std.mem.eql(u8, &demand.scope.metadata_incarnation, &owner.metadata_incarnation)) return error.InvalidBatchRequest;
}

const ReplicationPermit = @import("replication_source_apply.zig").Permit;
const ReplicationSourceResolution = struct {
    permit: ReplicationPermit,
    status: db_types.TxnStatus,
    version: u64,
    timestamp_ns: u64,

    fn batch(self: @This()) !db_types.BatchRequest {
        try self.permit.validate();
        if (self.status == .pending or self.version == 0) return error.InvalidBatchRequest;
        return .{ .timestamp_ns = self.timestamp_ns, .transaction = .{ .resolve = .{
            .txn_id = self.permit.reference.txn_id,
            .status = self.status,
            .commit_version = self.version,
            .external_decision_owner = self.permit.decision_owner,
            .replication_source_permit = self.permit,
        } } };
    }
};
const ReplicationSourceAck = struct {
    completion: @import("replication_source_apply.zig").AckCompletion,
    participants: []const []const u8,
    timestamp_ns: u64,

    fn batch(self: @This()) !db_types.BatchRequest {
        try self.completion.requireAcknowledgementIds(self.participants);
        const permit = self.completion.permit;
        return .{ .timestamp_ns = self.timestamp_ns, .transaction = .{ .acknowledge_many = .{
            .txn_id = permit.reference.txn_id,
            .external_decision_owner = permit.decision_owner,
            .participants = self.participants,
            .replication_source_ack = self.completion,
        } } };
    }
};
const ReplicationSourceIntent = struct { permit: ReplicationPermit, intent: db_types.TransactionIntentRequest };

fn validateReplicationSourcePrepare(req: db_types.BatchRequest, permit: ReplicationPermit) !void {
    try permit.validate();
    const command = req.transaction orelse return error.InvalidBatchRequest;
    if (command != .prepare or command.prepare.document_revision_handoff != null or
        req.restore_staging_scope != null or req.restore_staging_plan_id != null or
        !std.mem.eql(u8, &command.prepare.txn_id, &permit.reference.txn_id) or command.prepare.topology_epoch != permit.topology_epoch or
        (if (command.prepare.external_activation) req.externalActivationOwner() == null else req.schema_version != permit.schema_version))
        return error.InvalidBatchRequest;
    const owner = command.prepare.external_decision_owner orelse return error.InvalidBatchRequest;
    if (!owner.eql(permit.decision_owner)) return error.InvalidBatchRequest;
}

const PublicationIntent = struct {
    txn_id: db_types.TxnId,
    topology_epoch: u64,
    intent: db_types.TransactionIntentRequest,
};

fn validatePublicationPrepare(a: std.mem.Allocator, req: db_types.BatchRequest) !void {
    const command = req.transaction orelse return error.InvalidBatchRequest;
    if (command != .prepare or command.prepare.external_activation or command.prepare.document_revision_handoff != null or req.restore_staging_scope != null or req.restore_staging_plan_id != null) return error.InvalidBatchRequest;
    const scheme: db_types.DocumentVersionScheme = if (command.prepare.completion_owner != null) .revision_v1 else .admit_current;
    if (req.document_version_scheme != scheme) return error.InvalidBatchRequest;
    if (command.prepare.replication_source_permit != null) return error.InvalidBatchRequest;
    const demand = command.prepare.publication_capture orelse return error.InvalidBatchRequest;
    try demand.validate(a);
    const owner = command.prepare.external_decision_owner orelse return error.InvalidBatchRequest;
    if (!owner.eql(demand.owner) or !std.mem.eql(u8, &command.prepare.txn_id, &demand.transaction_id) or command.prepare.topology_epoch != demand.topology_epoch) return error.InvalidBatchRequest;
    for (req.writes) |write| {
        if (write.key.len == 0) return error.InvalidBatchRequest;
        var parsed = try std.json.parseFromSlice(std.json.Value, a, write.value, .{ .parse_numbers = false });
        defer parsed.deinit();
        if (parsed.value != .object or write.json_null_fields.len > 256) return error.InvalidBatchRequest;
        if (write.prepared_domain) |contract| try contract.validate();
        for (write.json_null_fields, 0..) |name, index| {
            if (name.len == 0 or (parsed.value.object.get(name) orelse return error.InvalidBatchRequest) != .null) return error.InvalidBatchRequest;
            for (write.json_null_fields[0..index]) |previous| if (std.mem.eql(u8, previous, name)) return error.InvalidBatchRequest;
        }
    }
    try DomainHandoff.requireUniqueKeys(a, req.writes);
}

fn encodeBatchRequestOwned(alloc: std.mem.Allocator, req: db_types.BatchRequest, local_owner: bool) ![]u8 {
    if (req.transaction) |command| if (command == .acknowledge_many) if (command.acknowledge_many.replication_source_ack) |completion| {
        const body: ReplicationSourceAck = .{ .completion = completion, .participants = command.acknowledge_many.participants, .timestamp_ns = req.timestamp_ns };
        const round_trip = try body.batch();
        if (!std.meta.eql(req, round_trip)) return error.InvalidBatchRequest;
        return std.fmt.allocPrint(alloc, "[\"{s}\",{f}]", .{ replication_source_ack_tag, std.json.fmt(body, .{}) });
    };
    if (req.transaction) |command| if (command == .resolve) if (command.resolve.replication_source_permit) |permit| {
        const body: ReplicationSourceResolution = .{
            .permit = permit,
            .status = command.resolve.status,
            .version = command.resolve.commit_version,
            .timestamp_ns = req.timestamp_ns,
        };
        const round_trip = try body.batch();
        if (!std.meta.eql(req, round_trip)) return error.InvalidBatchRequest;
        return std.fmt.allocPrint(alloc, "[\"{s}\",{f}]", .{ replication_source_resolution_tag, std.json.fmt(body, .{}) });
    };
    if (req.transaction) |command| if (command == .prepare) if (command.prepare.replication_source_permit) |permit| {
        try validateReplicationSourcePrepare(req, permit);
        const intent = try db_types.TransactionIntentRequest.fromBatch(req);
        var round_trip = intent.asBatch(command.prepare.txn_id, command.prepare.topology_epoch);
        round_trip.transaction.?.prepare.replication_source_permit = permit;
        inline for (@typeInfo(db_types.BatchRequest).@"struct".field_names) |name| {
            if (!std.meta.eql(@field(req, name), @field(round_trip, name))) return error.InvalidBatchRequest;
        }
        var clean_intent = intent;
        clean_intent.replication_source_permit = null;
        const body: ReplicationSourceIntent = .{ .permit = permit, .intent = clean_intent };
        return std.fmt.allocPrint(alloc, "[\"{s}\",{f}]", .{ replication_source_prepare_tag, std.json.fmt(body, .{}) });
    };
    if (req.transaction) |command| if (command == .prepare and command.prepare.publication_capture != null) {
        try validatePublicationPrepare(alloc, req);
        const intent = try db_types.TransactionIntentRequest.fromBatch(req);
        const round_trip = intent.asBatch(command.prepare.txn_id, command.prepare.topology_epoch);
        inline for (@typeInfo(db_types.BatchRequest).@"struct".field_names) |name| {
            if (!std.meta.eql(@field(req, name), @field(round_trip, name))) return error.InvalidBatchRequest;
        }
        const body: PublicationIntent = .{ .txn_id = command.prepare.txn_id, .topology_epoch = command.prepare.topology_epoch, .intent = intent };
        return std.fmt.allocPrint(alloc, "[\"logical-publication-prepare-batch-v1\",{f}]", .{std.json.fmt(body, .{})});
    };
    if (req.logical_publication_gc) |collection| {
        try @import("../system_catalog/logical_replication_publisher.zig").validateCollectionBatch(req);
        return std.fmt.allocPrint(alloc, "[\"{s}\",{f}]", .{ publication_collection_tag, std.json.fmt(collection, .{}) });
    }
    if (req.logical_publication_control) |control| {
        try @import("../system_catalog/logical_replication_capture.zig").validateControlBatch(req);
        return std.fmt.allocPrint(alloc, "[\"logical-publication-control-v1\",{f}]", .{std.json.fmt(control, .{})});
    }
    if (req.transaction) |command| if (command == .prepare and command.prepare.document_revision_handoff != null) {
        // Compiled local owners and routed replicas consume the same explicit
        // PREPARE envelope. Its inner body always uses the strict replicated
        // dialect; local-owner unprepared predicate privileges do not apply.
        try validateDocumentRevisionHandoff(req);
        var clean = req;
        clean.transaction.?.prepare.document_revision_handoff = null;
        const inner = try encodeBatchRequestOwned(alloc, clean, false);
        defer alloc.free(inner);
        const handoff = try std.json.Stringify.valueAlloc(alloc, command.prepare.document_revision_handoff.?, .{});
        defer alloc.free(handoff);
        return std.fmt.allocPrint(alloc, "[\"document-revision-prepare-batch-v1\",{{\"native_batch\":{s},\"handoff\":{s}}}]", .{ inner, handoff });
    };

    const atomic_owner = local_owner and req.transaction == null and req.reject_graph_transform_projections;
    if (atomic_owner) try req.requireAtomicIntentShape();
    try @import("../storage/db/relational/indexes/raft_contract.zig").validateBatchRequest(req);
    try @import("../storage/db/resolver_review.zig").validateRequest(req);
    try db_types.validateLeannRecordCommand(req);
    if (req.sql_lock != null) try @import("../storage/pessimistic_lock/control.zig").validateBatch(req);
    try @import("../storage/db/group_source_contract.zig").validateRequest(req);
    try @import("../storage/db/artifact_publication.zig").validateRequest(alloc, req);
    try @import("../storage/db/artifact_publication_transport.zig").validateBatchRequest(req);
    try validateRowPolicyPublicationRequest(req);
    if (req.transaction) |mutation| switch (mutation) {
        .prepare => |prepare| {
            if (prepare.external_decision_owner) |owner| owner.validate() catch return error.InvalidBatchRequest;
            if (prepare.external_activation and prepare.external_decision_owner == null) return error.InvalidBatchRequest;
        },
        .resolve => |resolve| if (resolve.external_decision_owner) |owner| owner.validate() catch return error.InvalidBatchRequest,
        else => {},
    };
    if (req.range_guards.len != 0 and (req.range_guards.len > @import("../storage/range_protection.zig").max_proofs or (!atomic_owner and (req.transaction == null or req.transaction.? != .prepare)))) return error.InvalidBatchRequest;
    try @import("../storage/range_protection.zig").validateRequest(req);
    try @import("../storage/db/merge_proof_adoption.zig").validateRequest(req);
    try @import("../storage/db/online_source_contract.zig").validateRequest(req);
    try @import("../storage/db/artifact_inventory.zig").validateRequest(req);
    try merge_pages.validateRequest(req);
    if (req.merge_checkpoint) |checkpoint| {
        if ((checkpoint.page_source != null) != (checkpoint.page_receiver_namespace != null)) return error.InvalidBatchRequest;
        if (checkpoint.page_source) |source| try source.validate();
    }
    if (req.relational_topology != null and (req.transaction != null or req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or req.predicates.len != 0 or req.integrity.len != 0 or req.integrity_commands.len != 0 or req.relational_activation != null or req.relational_retirement != null or req.relational_index_maintenance != null or req.split_checkpoint != null or req.split_replication != null or req.split_transition != null or req.merge_checkpoint != null or req.merge_replication != null or req.merge_source_transition != null)) return error.InvalidBatchRequest;
    if (req.relational_generation_gc != null and (req.transaction != null or req.relational_topology != null or req.row_policy_publication != null or req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or req.predicates.len != 0 or req.integrity.len != 0 or req.integrity_commands.len != 0 or req.relational_activation != null or req.relational_retirement != null or req.relational_index_maintenance != null or req.split_checkpoint != null or req.split_replication != null or req.split_transition != null or req.merge_checkpoint != null or req.merge_replication != null or req.merge_source_transition != null)) return error.InvalidBatchRequest;
    try validateGraphBatchCommand(req);
    if (req.relational_generation_gc) |gc| try gc.validate();
    if (req.relational_topology != null and (req.relational_schema_version != null or req.relational_integrity_generation_set != null or req.relational_repair)) return error.InvalidBatchRequest;
    if (req.integrity.len != 0 and !atomic_owner and (req.transaction == null or req.transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (req.integrity_commands.len != 0 and ((!atomic_owner and (req.transaction == null or req.transaction.? != .prepare)) or req.integrity.len != 0)) return error.InvalidBatchRequest;
    if (req.relational_activation != null and !atomic_owner and (req.transaction == null or req.transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (req.relational_retirement != null and !atomic_owner and (req.transaction == null or req.transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (req.relational_index_maintenance) |command| {
        try command.validate();
        if ((!atomic_owner and (req.transaction == null or req.transaction.? != .prepare)) or req.writes.len != 0 or req.deletes.len != 0 or req.transforms.len != 0 or req.predicates.len != 0 or req.integrity.len != 0 or req.integrity_commands.len != 0 or req.relational_activation != null or req.relational_retirement != null or req.relational_repair or req.restore_staging_scope != null) return error.InvalidBatchRequest;
    }
    try db_types.validateMergeArtifacts(req);
    if (req.predicates.len > 0 and req.transaction == null and !local_owner) {
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
    const prepared = req.prepared_enrichments_json != null or req.commit_requires == .direct_enrichments;
    const triggers = req.trigger_receipts.len != 0;
    const document_versions = requiresDocumentVersionsEnvelope(req);
    const versioned = prepared or document_versions or triggers or requiresRowSemanticsEnvelope(req.writes, req.predicates);
    const domains = @import("../schema/domain_handoff.zig").present(req.writes);
    try DomainHandoff.requireUniqueKeys(alloc, req.writes);
    if (domains and req.merge_page != null) return error.InvalidBatchRequest;
    if (prepared) try writer.writeAll("[\"prepared-enrichments-batch-v1\",") else if (document_versions) try writer.writeAll(if (triggers or domains) "[\"row-semantics-batch-v4\"," else "[\"document-revisions-batch-v1\",") else if (triggers) try writer.writeAll("[\"row-semantics-batch-v3\",") else if (domains) try writer.writeAll("[\"row-semantics-batch-v2\",") else if (versioned) try writer.writeAll("[\"row-semantics-batch-v1\",");

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
    var first_null = true;
    for (if (req.merge_page != null) &.{} else req.writes) |write| if (write.json_null_fields.len != 0) {
        if (first_null) try writer.writeAll(",\"_json_null_fields\":{") else try writer.writeByte(',');
        first_null = false;
        try writer.print("{f}:{f}", .{ std.json.fmt(write.key, .{}), std.json.fmt(write.json_null_fields, .{}) });
    };
    if (!first_null) try writer.writeByte('}');
    try @import("../schema/domain_handoff.zig").writeSidecar(writer, req.writes);
    if (triggers) {
        if (req.graph_preparation) |proof| {
            try proof.validate();
            try writer.print(",\"_graph_preparation\":{f}", .{std.json.fmt(proof, .{})});
        }
        if (req.trigger_preparation_group_id == 0 or req.trigger_preparation_admitted_at_seconds <= 0) return error.InvalidBatchRequest;
        const receipts = try @import("../sql/trigger_handoff.zig").encodeReceipts(alloc, req.trigger_receipts);
        defer alloc.free(receipts);
        try writer.writeAll(",\"_trigger_receipts\":");
        try writer.writeAll(receipts);
        try writer.print(",\"_trigger_preparation_group_id\":{d}", .{req.trigger_preparation_group_id});
        try writer.print(",\"_trigger_preparation_admitted_at_seconds\":{d}", .{req.trigger_preparation_admitted_at_seconds});
        if (req.trigger_preparation_command_digest) |digest| try writer.print(",\"_trigger_preparation_command_digest\":{f}", .{std.json.fmt(digest, .{})});
        if (req.trigger_preparation_proof.len != 0) {
            if (req.trigger_preparation_proof.len > @import("../common/trigger_preparation_authority.zig").maximum_token_bytes) return error.InvalidBatchRequest;
            try writer.print(",\"_trigger_preparation_proof\":{f}", .{std.json.fmt(req.trigger_preparation_proof, .{})});
        }
    } else if (req.graph_preparation != null or req.trigger_preparation_group_id != 0 or req.trigger_preparation_command_digest != null or req.trigger_preparation_admitted_at_seconds != 0 or req.trigger_preparation_proof.len != 0) return error.InvalidBatchRequest;

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
            try writer.print("{{\"key\":{f},\"value\":{f},\"json_null_fields\":{f}}}", .{
                std.json.fmt(write.key, .{ .emit_strings_as_arrays = true }),
                std.json.fmt(write.value, .{}),
                std.json.fmt(write.json_null_fields, .{}),
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
                    std.json.fmt(db_types.transformOpText(op.op), .{}),
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
    if (req.schema_version) |version| try writer.print(",\"_schema_version\":{d}", .{version});
    if (req.relational_repair) try writer.writeAll(",\"_relational_repair\":true");
    if (req.activate_range_tracking) try writer.writeAll(",\"_activate_range_tracking\":true");
    if (req.range_guards.len != 0) try writer.print(",\"_range_guards\":{f}", .{std.json.fmt(req.range_guards, .{})});
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
    if (req.relational_index_owner_control) |control| {
        try writer.writeAll(",\"_relational_index_owner_control\":");
        try writer.print("{f}", .{std.json.fmt(control, .{})});
    }
    if (req.relational_topology) |topology| {
        try writer.writeAll(",\"_relational_topology\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, topology, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.relational_generation_gc) |page| {
        try writer.writeAll(",\"_relational_generation_gc\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, page, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.row_policy_publication) |publication| {
        try writer.writeAll(",\"_row_policy_publication\":");
        try writer.print("{f}", .{std.json.fmt(publication, .{})});
        try writer.writeAll(",\"_row_policy_install_bundle\":");
        try writer.print("{f}", .{std.json.fmt(req.row_policy_install_bundle, .{})});
    }
    if (req.normalized_graph_rows) try writer.writeAll(",\"_normalized_graph_rows\":true");
    if (req.row_policy_principal_proof.len != 0) {
        if (req.row_policy_database.len == 0 or req.row_policy_admitted_at_seconds <= 0)
            return error.InvalidBatchRequest;
        try writer.writeAll(",\"_row_policy_principal_proof\":");
        try writer.print("{f}", .{std.json.fmt(req.row_policy_principal_proof, .{})});
        try writer.writeAll(",\"_row_policy_database\":");
        try writer.print("{f}", .{std.json.fmt(req.row_policy_database, .{})});
        try writer.print(",\"_row_policy_admitted_at_seconds\":{d}", .{req.row_policy_admitted_at_seconds});
    } else if (req.row_policy_database.len != 0 or req.row_policy_admitted_at_seconds != 0) return error.InvalidBatchRequest;
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
    if (req.artifact_catalog) |control| {
        if (req.online_source == null and req.merge_checkpoint == null) try writer.writeAll(",\"_merge_checkpoint\":{\"kind\":\"artifact_catalog_v1\"}");
        try writer.writeAll(",\"_artifact_catalog\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, control, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.group_source) |control| {
        try writer.writeAll(",\"_merge_checkpoint\":{\"kind\":\"group_source_v1\"},\"_group_source\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, control, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.sql_lock) |control| {
        try writer.writeAll(",\"_merge_checkpoint\":{\"kind\":\"sql_lock_v1\"},\"_sql_lock\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, control, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.resolver_review) |control| {
        try writer.writeAll(",\"_merge_checkpoint\":{\"kind\":\"resolver_review_v1\"},\"_resolver_review\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, control, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.artifact_publication) |control| {
        try writer.writeAll(",\"_merge_checkpoint\":{\"kind\":\"artifact_publication_v1\"},\"_artifact_publication\":");
        const CompactPublication = struct {
            command: @import("../storage/db/artifact_publication.zig").Command,
            pub fn jsonStringify(self: @This(), stream: anytype) !void {
                try @import("../storage/db/artifact_publication_wire.zig").writeCompact(self.command, stream);
            }
        };
        const encoded = try std.json.Stringify.valueAlloc(alloc, CompactPublication{ .command = control }, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.artifact_publication_transport) |control| {
        try writer.writeAll(",\"_merge_checkpoint\":{\"kind\":\"artifact_publication_transport_v1\"},\"_artifact_publication_transport\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, control, .{});
        defer alloc.free(encoded);
        try writer.writeAll(encoded);
    }
    if (req.leann_record) |command| {
        try writer.writeAll(",\"_merge_checkpoint\":{\"kind\":\"leann_record_v1\"},\"_leann_record\":{\"index_name\":");
        try std.json.Stringify.value(command.index_name, .{}, writer);
        try writer.writeAll(",\"record_base64\":\"");
        const encoder = std.base64.standard.Encoder;
        const encoded = try alloc.alloc(u8, encoder.calcSize(command.record.len));
        defer alloc.free(encoded);
        _ = encoder.encode(encoded, command.record);
        try writer.writeAll(encoded);
        try writer.writeAll("\"}");
    }
    if (req.merge_proof_adoption) |control| {
        try writer.writeAll(",\"_merge_checkpoint\":{\"kind\":\"merge_proof_adoption_v1\"},\"_merge_proof_adoption\":");
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
    if (req.graph_endpoint_cleanup_planned) try writer.writeAll(",\"_graph_endpoint_cleanup_planned\":true");
    if (req.graph_endpoint_cleanup_guards.len != 0) {
        try writer.writeAll(",\"_graph_endpoint_cleanup_guards\":");
        const guards_json = try std.json.Stringify.valueAlloc(alloc, req.graph_endpoint_cleanup_guards, .{});
        defer alloc.free(guards_json);
        try writer.writeAll(guards_json);
    }
    if (req.graph_endpoint_cleanup) try writer.writeAll(",\"_graph_endpoint_cleanup\":true");
    if (req.graph_writes.len > 0) {
        try writer.writeAll(",\"_graph_writes\":[");
        for (req.graph_writes, 0..) |write, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"index_name\":{f},\"source\":{f},\"target\":{f},\"edge_type\":{f},\"weight\":{d},\"created_at\":\"{d}\",\"updated_at\":\"{d}\",\"metadata_json\":{f}", .{
                std.json.fmt(write.index_name, .{}),
                std.json.fmt(write.source, .{}),
                std.json.fmt(write.target, .{}),
                std.json.fmt(write.edge_type, .{}),
                write.weight,
                write.created_at,
                write.updated_at,
                std.json.fmt(write.metadata_json, .{}),
            });
            if (write.owner.len > 0) try writer.print(",\"owner\":{f}", .{std.json.fmt(write.owner, .{})});
            if (write.edge_id.len > 0) try writer.print(",\"edge_id\":{f}", .{std.json.fmt(write.edge_id, .{})});
            if (write.owner_document.len > 0) try writer.print(",\"owner_document\":{f}", .{std.json.fmt(write.owner_document, .{})});
            try writer.writeByte('}');
        }
        try writer.writeByte(']');
    }
    if (req.graph_deletes.len > 0) {
        try writer.writeAll(",\"_graph_deletes\":[");
        for (req.graph_deletes, 0..) |delete, i| {
            if (i != 0) try writer.writeByte(',');
            try writer.print("{{\"index_name\":{f},\"source\":{f},\"target\":{f},\"edge_type\":{f}", .{
                std.json.fmt(delete.index_name, .{}),
                std.json.fmt(delete.source, .{}),
                std.json.fmt(delete.target, .{}),
                std.json.fmt(delete.edge_type, .{}),
            });
            if (delete.edge_id.len > 0) try writer.print(",\"edge_id\":{f}", .{std.json.fmt(delete.edge_id, .{})});
            if (delete.owner.len > 0) try writer.print(",\"owner\":{f}", .{std.json.fmt(delete.owner, .{})});
            if (delete.owner_document.len > 0) try writer.print(",\"owner_document\":{f}", .{std.json.fmt(delete.owner_document, .{})});
            try writer.writeByte('}');
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
            if (predicate.unique_absence) try writer.writeAll(",\"unique_absence\":true");
            if (predicate.row_policy_requirement != .native) try writer.print(",\"row_policy_requirement\":\"{s}\"", .{@tagName(predicate.row_policy_requirement)});
            try writer.writeByte('}');
        }
        try writer.writeByte(']');
    }
    if (req.split_checkpoint) |checkpoint| {
        try writer.print(",\"_split_checkpoint\":{{\"kind\":{f},\"transition_id\":\"{d}\",\"attempt_epoch\":\"{d}\",\"source_group_id\":\"{d}\",\"destination_group_id\":\"{d}\",\"range_start\":{f},\"range_end\":{f},\"delta_sequence\":\"{d}\"", .{
            std.json.fmt(@tagName(checkpoint.kind), .{}),
            checkpoint.transition_id,
            checkpoint.attempt_epoch,
            checkpoint.source_group_id,
            checkpoint.destination_group_id,
            std.json.fmt(checkpoint.range_start, .{}),
            std.json.fmt(checkpoint.range_end, .{}),
            checkpoint.delta_sequence,
        });
        if (checkpoint.history_policy) |policy| {
            if (checkpoint.kind != .destination_complete) return error.InvalidBatchRequest;
            try writer.print(",\"history_policy\":\"{s}\"", .{std.fmt.bytesToHex(try policy.encode(), .lower)});
        }
        try writer.writeByte('}');
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
    if (req.timestamp_ns != 0 or req.document_version_scheme == .revision_v1) {
        try writer.print(",\"_timestamp_ns\":\"{d}\"", .{req.timestamp_ns});
    }
    if (req.document_version_scheme == .revision_v1) try writer.writeAll(",\"_document_version_scheme\":1");
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
        if (checkpoint.page_source_catalogs) |catalogs| {
            try writer.print(",\"page_source_catalogs\":{f}", .{std.json.fmt(catalogs, .{ .emit_strings_as_arrays = true })});
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
                if (begin.external_decision_owner != null and (begin.ordinary_producer or begin.legacy_producer or begin.source_purpose != null or begin.source_scope != null or begin.source_checkpoint or begin.source_checkpoint_receipts)) return error.InvalidBatchRequest;
                if ((begin.ordinary_producer and begin.legacy_producer) or ((begin.ordinary_producer or begin.legacy_producer) and ((begin.completion_admission != null and begin.completion_owner == null) or begin.source_purpose != null or begin.source_scope != null or begin.source_checkpoint or begin.source_checkpoint_receipts))) return error.InvalidBatchRequest;
                if (begin.ordinary_producer or begin.legacy_producer) {
                    if (begin.completion_admission == null) try validateProducerBeginControlRequest(req);
                    if (begin.completion_admission) |raw| try validateOrdinaryCompletionBegin(alloc, raw);
                }
                try validateSourceCompletionPurpose(begin.source_purpose, begin.completion_owner);
                if (begin.external_decision_owner) |external| {
                    if (begin.completion_owner) |owner| try validateExternalCompletionOwner(external, owner);
                }
                if (begin.source_purpose != null and !begin.retain_terminal) return error.InvalidBatchRequest;
                if (begin.source_checkpoint and begin.source_purpose == null) return error.InvalidBatchRequest;
                if (begin.source_checkpoint_receipts and !begin.source_checkpoint) return error.InvalidBatchRequest;
                if (begin.source_scope) |scope| {
                    try scope.validate();
                    if (begin.source_purpose == null or !begin.source_checkpoint_receipts) return error.InvalidBatchRequest;
                }
                if (begin.completion_admission == null and begin.completion_owner != null) return error.InvalidBatchRequest;
                const external_history = begin.external_decision_owner != null and begin.completion_admission != null;
                if (external_history and (!begin.retain_terminal or begin.completion_owner == null)) return error.InvalidBatchRequest;
                const txn_hex = std.fmt.bytesToHex(begin.txn_id, .lower);
                try writer.print("\"phase\":\"{s}\",\"txn_id\":\"{s}\",\"begin_timestamp\":\"{d}\",\"created_at_ns\":\"{d}\",\"topology_epoch\":\"{d}\",\"retain_terminal\":{},\"participants\":[", .{
                    if (external_history) "external_begin_completion_v1" else if (begin.document_revision_scope != null) "external_begin_revisions_v1" else if (begin.external_decision_owner != null) "external_begin_v1" else if (begin.legacy_producer) (if (begin.completion_admission != null) "begin_legacy_completion_v1" else "begin_legacy_v1") else if (begin.ordinary_producer) (if (begin.completion_admission != null) "begin_ordinary_completion_v1" else "begin_ordinary_v1") else if (begin.source_scope != null) "begin_source_scope_v1" else if (begin.source_checkpoint_receipts) "begin_source_checkpoint_v2" else if (begin.source_checkpoint) "begin_source_checkpoint_v1" else if (begin.source_purpose != null) "begin_source_completion_v1" else if (begin.completion_admission != null) "begin_completion_v1" else "begin", &txn_hex, begin.begin_timestamp, begin.created_at_ns, begin.topology_epoch, begin.retain_terminal,
                });
                for (begin.participants, 0..) |participant, i| {
                    if (i != 0) try writer.writeByte(',');
                    try writer.print("{f}", .{std.json.fmt(participant, .{})});
                }
                try writer.writeByte(']');
                if (begin.document_revision_scope) |scope| {
                    try scope.validate();
                    const owner = begin.external_decision_owner orelse return error.InvalidBatchRequest;
                    if (!begin.retain_terminal or (req.restore_staging_scope != null or req.restore_staging_plan_id != null) or scope.begin_timestamp != begin.begin_timestamp or
                        !std.mem.eql(u8, &scope.transaction_id, &begin.txn_id) or !std.mem.eql(u8, &scope.metadata_incarnation, &owner.metadata_incarnation)) return error.InvalidBatchRequest;
                    try writer.print(",\"document_revision_scope\":{f}", .{std.json.fmt(scope, .{})});
                }
                if (begin.sql_locks) |ref| {
                    try ref.validate(begin.txn_id, begin.external_decision_owner orelse return error.InvalidBatchRequest);
                    try writer.print(",\"sql_locks\":{f}", .{std.json.fmt(ref, .{})});
                }
                if (begin.external_decision_owner) |owner| {
                    try owner.validate();
                    if (!begin.retain_terminal) return error.InvalidBatchRequest;
                    try writer.print(",\"external_decision_owner\":{f}", .{std.json.fmt(owner, .{})});
                }
                if (begin.ordinary_producer) try writer.writeAll(",\"ordinary_producer\":true");
                if (begin.legacy_producer) try writer.writeAll(",\"legacy_producer\":true");
                if (begin.completion_admission) |raw| {
                    try validateCompletionControlRequest(req);
                    try validateCompletionBegin(alloc, raw, begin.txn_id, begin.participants, begin.completion_owner, begin.topology_epoch);
                    try writer.print(",\"completion_admission\":{f}", .{std.json.fmt(raw, .{})});
                    if (external_history) try validateOrdinaryCompletionBegin(alloc, raw);
                    if (begin.completion_owner) |owner| try writer.print(",\"completion_owner\":{f}", .{std.json.fmt(owner, .{})});
                    if (begin.source_scope) |scope| try writer.print(",\"source_scope\":{f}", .{std.json.fmt(scope, .{})});
                    if (begin.source_purpose) |purpose| try writer.print(",\"source_purpose\":{f}", .{std.json.fmt(purpose, .{})});
                    if (begin.source_checkpoint) try writer.writeAll(",\"source_checkpoint\":true");
                    if (begin.source_checkpoint_receipts) try writer.writeAll(",\"source_checkpoint_receipts\":true");
                }
            },
            .prepare => |prepare| {
                if (prepare.external_vote_admitted_at_ns != 0 and (prepare.external_decision_owner == null or prepare.completion_owner == null or prepare.external_activation)) return error.InvalidBatchRequest;
                if (prepare.external_decision_owner != null and (prepare.ordinary_producer or prepare.legacy_producer or (prepare.external_activation and prepare.completion_owner != null) or prepare.source_purpose != null or prepare.source_scope != null or prepare.source_checkpoint != null)) return error.InvalidBatchRequest;
                if ((prepare.ordinary_producer and prepare.legacy_producer) or ((prepare.ordinary_producer or prepare.legacy_producer) and (prepare.source_purpose != null or prepare.source_scope != null or prepare.source_checkpoint != null))) return error.InvalidBatchRequest;
                try validateSourceCompletionPurpose(prepare.source_purpose, prepare.completion_owner);
                if (prepare.external_decision_owner) |external| {
                    if (prepare.completion_owner) |owner| try validateExternalCompletionOwner(external, owner);
                }
                if (prepare.source_scope) |scope| {
                    try scope.validate();
                    if (prepare.source_purpose == null) return error.InvalidBatchRequest;
                    if (prepare.source_checkpoint) |checkpoint| if (checkpoint.receipt_window == 0 or !std.mem.eql(u8, &scope.binding, &checkpoint.binding) or scope.sequence != checkpoint.sequence) return error.SourceTransactionIdentityConflict;
                }
                if (prepare.source_checkpoint) |checkpoint| {
                    try checkpoint.validate();
                    const purpose = prepare.source_purpose orelse return error.InvalidBatchRequest;
                    if (!std.mem.eql(u8, &checkpoint.purpose, &purpose)) return error.SourceTransactionIdentityConflict;
                }
                if (prepare.completion_owner) |owner| {
                    if (req.timestamp_ns == 0 or req.document_version_scheme != .revision_v1) return error.InvalidBatchRequest;
                    try owner.validate();
                    if (owner.topology_epoch != prepare.topology_epoch) return error.InvalidBatchRequest;
                }
                const txn_hex = std.fmt.bytesToHex(prepare.txn_id, .lower);
                try writer.print("\"phase\":\"{s}\",\"txn_id\":\"{s}\",\"topology_epoch\":\"{d}\"", .{ if (prepare.external_decision_owner != null) (if (prepare.completion_owner != null) "external_prepare_completion_v1" else if (req.range_guards.len != 0) "external_prepare_ranges_v1" else "external_prepare_v1") else if (prepare.legacy_producer) (if (prepare.completion_owner != null) "prepare_legacy_completion_v1" else "prepare_legacy_v1") else if (prepare.ordinary_producer) (if (prepare.completion_owner != null) "prepare_ordinary_completion_v1" else "prepare_ordinary_v1") else if (prepare.source_scope != null) "prepare_source_scope_v1" else if (prepare.source_checkpoint != null and prepare.source_checkpoint.?.receipt_window != 0) "prepare_source_checkpoint_v2" else if (prepare.source_checkpoint != null) "prepare_source_checkpoint_v1" else if (prepare.source_purpose != null) "prepare_source_completion_v1" else if (prepare.completion_owner != null) "prepare_completion_v1" else if (req.range_guards.len != 0) "prepare_ranges_v1" else "prepare", &txn_hex, prepare.topology_epoch });
                if (prepare.ordinary_producer) try writer.writeAll(",\"ordinary_producer\":true");
                if (prepare.legacy_producer) try writer.writeAll(",\"legacy_producer\":true");
                if (prepare.completion_owner) |owner| try writer.print(",\"completion_owner\":{f}", .{std.json.fmt(owner, .{})});
                if (prepare.source_scope) |scope| try writer.print(",\"source_scope\":{f}", .{std.json.fmt(scope, .{})});
                if (prepare.source_purpose) |purpose| try writer.print(",\"source_purpose\":{f}", .{std.json.fmt(purpose, .{})});
                if (prepare.source_checkpoint) |checkpoint| try writer.print(",\"source_checkpoint\":{f}", .{std.json.fmt(checkpoint, .{})});
                if (prepare.external_decision_owner) |owner| try writer.print(",\"external_decision_owner\":{f},\"external_activation\":{}", .{ std.json.fmt(owner, .{}), prepare.external_activation });
                if (prepare.external_vote_admitted_at_ns != 0) try writer.print(",\"external_vote_admitted_at_ns\":\"{d}\"", .{prepare.external_vote_admitted_at_ns});
            },
            .resolve => |resolve| {
                if (resolve.external_decision_owner != null and (resolve.source_purpose != null or resolve.source_scope != null)) return error.InvalidBatchRequest;
                try validateSourceCompletionPurpose(resolve.source_purpose, resolve.completion_owner);
                if (resolve.external_decision_owner) |external| {
                    if (resolve.completion_owner) |owner| try validateExternalCompletionOwner(external, owner);
                }
                if (resolve.source_scope) |scope| {
                    try scope.validate();
                    if (resolve.source_purpose == null) return error.InvalidBatchRequest;
                }
                const txn_hex = std.fmt.bytesToHex(resolve.txn_id, .lower);
                if (resolve.completion_owner) |owner| {
                    try owner.validate();
                    try validateCompletionControlRequest(req);
                    if (resolve.status == .pending or resolve.commit_version == 0) return error.InvalidBatchRequest;
                }
                try writer.print("\"phase\":\"{s}\",\"txn_id\":\"{s}\",\"status\":{f},\"commit_version\":\"{d}\"", .{
                    if (resolve.external_decision_owner != null) (if (resolve.completion_owner != null) "external_resolve_completion_v1" else "external_resolve_v1") else if (resolve.source_scope != null) "resolve_source_scope_v1" else if (resolve.source_purpose != null) "resolve_source_completion_v1" else if (resolve.completion_owner != null) "resolve_completion_v1" else "resolve", &txn_hex, std.json.fmt(@tagName(resolve.status), .{}), resolve.commit_version,
                });
                if (resolve.external_decision_owner) |owner| try writer.print(",\"external_decision_owner\":{f}", .{std.json.fmt(owner, .{})});
                if (resolve.completion_owner) |owner| try writer.print(",\"completion_owner\":{f}", .{std.json.fmt(owner, .{})});
                if (resolve.source_scope) |scope| try writer.print(",\"source_scope\":{f}", .{std.json.fmt(scope, .{})});
                if (resolve.source_purpose) |purpose| try writer.print(",\"source_purpose\":{f}", .{std.json.fmt(purpose, .{})});
            },
            .publish_completion => |publication| {
                try validateCompletionControlRequest(req);
                const txn_hex = std.fmt.bytesToHex(publication.txn_id, .lower);
                if (publication.release) |release| {
                    if (publication.certificate.len != 0) return error.InvalidBatchRequest;
                    try release.validate(publication.completion_owner);
                    try writer.print("\"phase\":\"release_external_completion_v1\",\"txn_id\":\"{s}\",\"completion_owner\":{f},\"completion_release\":{f}", .{ &txn_hex, std.json.fmt(publication.completion_owner, .{}), std.json.fmt(release, .{}) });
                } else {
                    try validateCompletionPublication(alloc, publication.txn_id, publication.completion_owner, publication.certificate);
                    try writer.print("\"phase\":\"publish_completion_v1\",\"txn_id\":\"{s}\",\"completion_owner\":{f},\"completion_certificate\":{f}", .{ &txn_hex, std.json.fmt(publication.completion_owner, .{}), std.json.fmt(publication.certificate, .{}) });
                }
            },
            .acknowledge => |ack| {
                const txn_hex = std.fmt.bytesToHex(ack.txn_id, .lower);
                try writer.print("\"phase\":\"{s}\",\"txn_id\":\"{s}\",\"participant\":{f}", .{ if (ack.external_decision_owner != null) "external_acknowledge_v1" else "acknowledge", &txn_hex, std.json.fmt(ack.participant, .{}) });
                if (ack.external_decision_owner) |owner| try writer.print(",\"external_decision_owner\":{f}", .{std.json.fmt(owner, .{})});
            },
            .acknowledge_many => |ack| {
                if (ack.participants.len == 0 or ack.participants.len > 64) return error.InvalidBatchRequest;
                const txn_hex = std.fmt.bytesToHex(ack.txn_id, .lower);
                try writer.print("\"phase\":\"{s}\",\"txn_id\":\"{s}\",\"participants\":{f}", .{ if (ack.external_decision_owner != null) "external_acknowledge_many_v1" else "acknowledge_many", &txn_hex, std.json.fmt(ack.participants, .{}) });
                if (ack.external_decision_owner) |owner| try writer.print(",\"external_decision_owner\":{f}", .{std.json.fmt(owner, .{})});
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
    if (req.prepared_enrichments_json) |json| {
        try writer.writeAll(",\"_prepared_enrichments\":");
        try writer.writeAll(json);
    }
    if (req.commit_requires) |requirement| try writer.print(",\"commit_requires\":\"{s}\"", .{@tagName(requirement)});
    try writer.print(",\"sync_level\":\"{s}\"}}", .{syncLevelName(req.sync_level)});
    if (versioned) try writer.writeByte(']');
    return try out.toOwnedSlice();
}

fn syncLevelName(sync_level: db_types.SyncLevel) []const u8 {
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
) ![]db_types.BatchWrite {
    if (value != .object) return error.InvalidBatchRequest;
    const inserts = value.object;
    const writes = try alloc.alloc(db_types.BatchWrite, inserts.count());
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
        const key = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer alloc.free(key);
        writes[initialized] = .{
            .key = key,
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

fn optionalGraphIdentityString(alloc: std.mem.Allocator, object: std.json.ObjectMap, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return "";
    if (value != .string or value.string.len == 0) return error.InvalidBatchRequest;
    return alloc.dupe(u8, value.string);
}

fn parseGraphWrites(alloc: std.mem.Allocator, value: std.json.Value) ![]db_types.GraphEdgeWrite {
    if (value != .array) return error.InvalidBatchRequest;
    const writes = try alloc.alloc(db_types.GraphEdgeWrite, value.array.items.len);
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
        const edge_id = try optionalGraphIdentityString(alloc, item.object, "edge_id");
        errdefer if (edge_id.len > 0) alloc.free(edge_id);
        const owner_document = try optionalGraphIdentityString(alloc, item.object, "owner_document");
        errdefer if (owner_document.len > 0) alloc.free(owner_document);
        const owner = try optionalGraphIdentityString(alloc, item.object, "owner");
        errdefer if (owner.len > 0) alloc.free(owner);
        @import("../graph/mutation_identity.zig").validate(edge_id, owner_document, owner) catch return error.InvalidBatchRequest;
        writes[i] = .{
            .owner = owner,
            .edge_id = edge_id,
            .owner_document = owner_document,
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

fn parseGraphDeletes(alloc: std.mem.Allocator, value: std.json.Value) ![]db_types.GraphEdgeDelete {
    if (value != .array) return error.InvalidBatchRequest;
    const deletes = try alloc.alloc(db_types.GraphEdgeDelete, value.array.items.len);
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
        const edge_id = try optionalGraphIdentityString(alloc, item.object, "edge_id");
        errdefer if (edge_id.len > 0) alloc.free(edge_id);
        const owner_document = try optionalGraphIdentityString(alloc, item.object, "owner_document");
        errdefer if (owner_document.len > 0) alloc.free(owner_document);
        if (owner_document.len > 0 and edge_id.len == 0) return error.InvalidBatchRequest;
        const owner = try optionalGraphIdentityString(alloc, item.object, "owner");
        errdefer if (owner.len > 0) alloc.free(owner);
        if (owner.len > 0 and owner_document.len > 0) return error.InvalidBatchRequest;
        deletes[i] = .{
            .owner = owner,
            .edge_id = edge_id,
            .owner_document = owner_document,
            .index_name = index_name,
            .source = source,
            .target = target,
            .edge_type = edge_type,
        };
        initialized += 1;
    }
    return deletes;
}

fn parseTransforms(alloc: std.mem.Allocator, value: std.json.Value) ![]db_types.DocumentTransform {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const transforms = try alloc.alloc(db_types.DocumentTransform, values.len);
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

fn parseTransformOps(alloc: std.mem.Allocator, value: std.json.Value) ![]db_types.TransformOp {
    if (value != .array) return error.InvalidBatchRequest;
    const values = value.array.items;
    const ops = try alloc.alloc(db_types.TransformOp, values.len);
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
        const parsed_op: db_types.TransformOp = .{
            .op = op,
            .path = path,
            .value_json = value_json,
        };
        document_transform.validateDocumentTransform(alloc, .{
            .key = "",
            .operations = @constCast((&[_]db_types.TransformOp{parsed_op})[0..]),
        }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidBatchRequest,
        };
        ops[initialized] = parsed_op;
        initialized += 1;
    }
    return ops;
}

fn transformOpTypeFromString(op: []const u8) !db_types.TransformOpType {
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

fn syncLevelFromValue(value: std.json.Value) !db_types.SyncLevel {
    if (value != .string) return error.InvalidBatchRequest;
    const level = value.string;
    if (std.mem.eql(u8, level, "propose")) return .propose;
    if (std.mem.eql(u8, level, "write")) return .write;
    if (std.mem.eql(u8, level, "query") or std.mem.eql(u8, level, "full_text")) return .full_text;
    if (std.mem.eql(u8, level, "enrichments")) return .enrichments;
    if (std.mem.eql(u8, level, "full_index")) return .full_index;
    return error.InvalidBatchRequest;
}

fn freeWrites(alloc: std.mem.Allocator, writes: []db_types.BatchWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
        for (write.json_null_fields) |name| alloc.free(name);
        if (write.json_null_fields.len != 0) alloc.free(write.json_null_fields);
    }
    if (writes.len > 0) alloc.free(writes);
}

fn freeDeletes(alloc: std.mem.Allocator, deletes: [][]const u8) void {
    for (deletes) |key| alloc.free(key);
    if (deletes.len > 0) alloc.free(deletes);
}

fn freeTransforms(alloc: std.mem.Allocator, transforms: []db_types.DocumentTransform) void {
    freeTransformElements(alloc, transforms);
    if (transforms.len > 0) alloc.free(transforms);
}

fn freeTransformElements(alloc: std.mem.Allocator, transforms: []db_types.DocumentTransform) void {
    for (transforms) |transform| {
        alloc.free(@constCast(transform.key));
        freeTransformOps(alloc, transform.operations);
    }
}

fn freeGraphWrites(alloc: std.mem.Allocator, writes: []db_types.GraphEdgeWrite) void {
    freeGraphWriteElements(alloc, writes);
    if (writes.len > 0) alloc.free(writes);
}

fn freeGraphWriteElements(alloc: std.mem.Allocator, writes: []db_types.GraphEdgeWrite) void {
    for (writes) |write| {
        alloc.free(@constCast(write.index_name));
        alloc.free(@constCast(write.source));
        alloc.free(@constCast(write.target));
        alloc.free(@constCast(write.edge_type));
        if (write.edge_id.len > 0) alloc.free(@constCast(write.edge_id));
        if (write.owner_document.len > 0) alloc.free(@constCast(write.owner_document));
        if (write.owner.len > 0) alloc.free(@constCast(write.owner));
        alloc.free(@constCast(write.metadata_json));
    }
}

fn freeGraphDeletes(alloc: std.mem.Allocator, deletes: []db_types.GraphEdgeDelete) void {
    freeGraphDeleteElements(alloc, deletes);
    if (deletes.len > 0) alloc.free(deletes);
}

fn freeGraphDeleteElements(alloc: std.mem.Allocator, deletes: []db_types.GraphEdgeDelete) void {
    for (deletes) |delete| {
        alloc.free(@constCast(delete.index_name));
        alloc.free(@constCast(delete.source));
        alloc.free(@constCast(delete.target));
        alloc.free(@constCast(delete.edge_type));
        if (delete.edge_id.len > 0) alloc.free(@constCast(delete.edge_id));
        if (delete.owner_document.len > 0) alloc.free(@constCast(delete.owner_document));
        if (delete.owner.len > 0) alloc.free(@constCast(delete.owner));
    }
}

fn freeTransformOps(alloc: std.mem.Allocator, ops: []const db_types.TransformOp) void {
    freeTransformOpElements(alloc, ops);
    if (ops.len > 0) alloc.free(@constCast(ops));
}

fn freeTransformOpElements(alloc: std.mem.Allocator, ops: []const db_types.TransformOp) void {
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
            const integrity_mod = @import("../storage/db/relational/integrity/contract.zig");
            const address = try integrity_mod.Address.init(@splat(7), "binary\x00\xfftuple");
            const command: integrity_mod.Command = .{ .address = address, .operation = .{ .release = .{ .parent_table = "parent", .parent_key = "key\x00\xff" } } };
            const encoded = try encodeBatchRequest(alloc, .{
                .relational_schema_version = 17,
                .relational_integrity_generation_set = @as([32]u8, @splat(5)),
                .integrity_commands = &.{command},
                .transaction = .{ .prepare = .{ .txn_id = @splat(4), .topology_epoch = 1 } },
            });
            defer alloc.free(encoded);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
            var parsed = try parseInternalBatchRequest(alloc, encoded);
            defer parsed.deinit(alloc);
            try std.testing.expectEqual(@as(?u32, 17), parsed.req.relational_schema_version);
            try std.testing.expectEqual(@as(?[32]u8, @as([32]u8, @splat(5))), parsed.req.relational_integrity_generation_set);
            try std.testing.expectEqualSlices(u8, &address.routing, &parsed.req.integrity_commands[0].address.routing);
            try std.testing.expectEqualStrings("key\x00\xff", parsed.req.integrity_commands[0].operation.release.parent_key);
        }

        test "internal batch topology control preserves binary fences and refuses public injection" {
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, "{\"_activate_range_tracking\":true}"));
            const activation = try encodeBatchRequest(std.testing.allocator, .{ .activate_range_tracking = true });
            defer std.testing.allocator.free(activation);
            var parsed_activation = try parseInternalBatchRequest(std.testing.allocator, activation);
            defer parsed_activation.deinit(std.testing.allocator);
            try std.testing.expect(parsed_activation.req.activate_range_tracking);
            try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(std.testing.allocator, .{ .activate_range_tracking = true, .writes = &.{.{ .key = "a", .value = "{}" }} }));
            const alloc = std.testing.allocator;
            const topology = @import("../storage/db/relational/integrity/topology_contract.zig");
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
            var drop_fence = fence;
            drop_fence.role = .document_retirement_source;
            const drop_request: db_types.BatchRequest = .{ .relational_topology = .{ .fence = drop_fence, .action = .begin, .sql_drop_control = true, .document_schema_digest = @splat(9) } };
            for ([_]db_types.DocumentVersionScheme{ .legacy, .admit_current }) |mode| {
                var selected = drop_request;
                selected.document_version_scheme = mode;
                try topology.validateSqlDropBatch(selected);
                const body = try encodeBatchRequest(alloc, selected);
                defer alloc.free(body);
                var decoded = try parseInternalBatchRequest(alloc, body);
                defer decoded.deinit(alloc);
                try topology.validateSqlDropBatch(decoded.req);
                try std.testing.expect(decoded.req.relational_topology.?.isSqlDropControl());
                var mixed = selected;
                mixed.deletes = &.{"forbidden-row"};
                try std.testing.expectError(error.InvalidBatchRequest, topology.validateSqlDropBatch(mixed));
            }
            var revision_drop = drop_request;
            revision_drop.document_version_scheme = .revision_v1;
            try std.testing.expectError(error.InvalidBatchRequest, topology.validateSqlDropBatch(revision_drop));
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
            const retirement = @import("../storage/db/relational/integrity/generation_retirement.zig");
            const gc_before = try (retirement.GcProgress{ .revision = 1 }).encode(alloc);
            defer alloc.free(gc_before);
            const gc_after = try (retirement.GcProgress{ .revision = 1, .tombstones = true }).encode(alloc);
            defer alloc.free(gc_after);
            const gc = try encodeBatchRequest(alloc, .{ .relational_generation_gc = .{ .owner_group_id = 301, .namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 }, .expected = gc_before, .next = gc_after, .deletions = &.{}, .inspected = 0 } });
            defer alloc.free(gc);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, gc));
            var parsed_gc = try parseInternalBatchRequest(alloc, gc);
            defer parsed_gc.deinit(alloc);
            try std.testing.expectEqual(@as(u64, 301), parsed_gc.req.relational_generation_gc.?.owner_group_id);
            try std.testing.expectEqualSlices(u8, gc_after, parsed_gc.req.relational_generation_gc.?.next);
            try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{ .relational_generation_gc = parsed_gc.req.relational_generation_gc, .deletes = &.{"user-row"} }));
        }

        test "distributed txn public batch rejects isolated coordinator generation evidence spoof" {
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, "{\"_relational_repair\":true}"));
            const alloc = std.testing.allocator;
            const encoded = try @import("relational_integrity_wire.zig").encodeGenerationSet(alloc, @as([32]u8, @splat(7)));
            defer alloc.free(encoded);
            const body = try std.fmt.allocPrint(alloc, "{{\"_relational_integrity_generation_set\":{s}}}", .{encoded});
            defer alloc.free(body);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, body));
            var internal = try parseInternalBatchRequest(alloc, body);
            defer internal.deinit(alloc);
            try std.testing.expectEqual(@as(?[32]u8, @as([32]u8, @splat(7))), internal.req.relational_integrity_generation_set);
        }

        test "internal batch graph endpoint cleanup command cannot enter public or mixed batches" {
            const alloc = std.testing.allocator;
            const encoded = try encodeBatchRequest(alloc, .{ .graph_endpoint_cleanup = true });
            defer alloc.free(encoded);
            var parsed = try parseInternalBatchRequest(alloc, encoded);
            defer parsed.deinit(alloc);
            try std.testing.expect(parsed.req.graph_endpoint_cleanup);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
            try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{ .graph_endpoint_cleanup = true, .deletes = &.{"doc"} }));
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, "{\"_graph_endpoint_cleanup\":true,\"deletes\":[\"doc\"]}"));
            const legacy = try encodeBatchRequest(alloc, .{ .graph_deletes = &.{.{ .index_name = "g", .source = "entity", .target = "hub", .edge_type = "R", .owner = "producer" }} });
            defer alloc.free(legacy);
            var legacy_parsed = try parseInternalBatchRequest(alloc, legacy);
            defer legacy_parsed.deinit(alloc);
            try std.testing.expectEqualStrings("producer", legacy_parsed.req.graph_deletes[0].owner);
            const job = try @import("../storage/internal_keys.zig").graphEndpointCleanupKeyAlloc(alloc, "hub");
            defer alloc.free(job);
            const control_delete = try encodeBatchRequest(alloc, .{ .deletes = &.{job} });
            defer alloc.free(control_delete);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, control_delete));
            var internal_delete = try parseInternalBatchRequest(alloc, control_delete);
            defer internal_delete.deinit(alloc);
            try std.testing.expectEqualStrings(job, internal_delete.req.deletes[0]);
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
            const txn_id: db_types.TxnId = .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
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
            try std.testing.expectEqual(@as([32]u8, @splat(7)), decoded.req.predicates[0].expected_content_digest.?);

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

            const many_encoded = try encodeBatchRequest(alloc, .{ .transaction = .{ .acknowledge_many = .{ .txn_id = txn_id, .participants = &.{ "table2:4:docs:group:8", "table2:4:docs:group:9" } } } });
            defer alloc.free(many_encoded);
            var many_decoded = try parseInternalBatchRequest(alloc, many_encoded);
            defer many_decoded.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 2), many_decoded.req.transaction.?.acknowledge_many.participants.len);
            try std.testing.expectEqualStrings("table2:4:docs:group:9", many_decoded.req.transaction.?.acknowledge_many.participants[1]);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, many_encoded));
            try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{ .transaction = .{ .acknowledge_many = .{ .txn_id = txn_id, .participants = &.{} } } }));

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

        test "public batch parser accepts individual graph edge mutations" {
            var owned = try parseBatchRequest(std.testing.allocator,
                \\{"graph_writes":[{"index_name":"graph","source":"node:a","target":"node:b","edge_type":"KNOWS","metadata_json":"{\"uuid\":\"1\"}"}],"graph_deletes":[{"index_name":"graph","source":"node:a","target":"node:c","edge_type":"KNOWS"}]}
            );
            defer owned.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(usize, 1), owned.req.graph_writes.len);
            try std.testing.expectEqualStrings("node:b", owned.req.graph_writes[0].target);
            try std.testing.expectEqualStrings("{\"uuid\":\"1\"}", owned.req.graph_writes[0].metadata_json);
            try std.testing.expectEqual(@as(usize, 1), owned.req.graph_deletes.len);
            try std.testing.expectEqualStrings("node:c", owned.req.graph_deletes[0].target);
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
            try std.testing.expectError(error.ValueTooLong, parseBatchRequestWithOptions(std.testing.allocator, body, .{ .allocate = .alloc_always, .max_value_len = 64 }, false, false));
        }

        test "internal batch parser owns and round trips split checkpoint" {
            const body =
                \\{"inserts":{},"deletes":[],"_split_checkpoint":{"kind":"destination_complete","transition_id":40,"attempt_epoch":1,"source_group_id":41,"destination_group_id":42,"range_start":"doc:m","range_end":"doc:z","delta_sequence":7}}
            ;
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, body));

            var owned = try parseInternalBatchRequest(std.testing.allocator, body);
            defer owned.deinit(std.testing.allocator);
            const checkpoint = owned.req.split_checkpoint orelse return error.TestExpectedEqual;
            try std.testing.expectEqual(db_types.SplitReplicationCheckpoint.Kind.destination_complete, checkpoint.kind);
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
            const request: db_types.BatchRequest = .{
                .graph_writes = &.{.{
                    .index_name = "relations_graph",
                    .source = "doc:a",
                    .target = "doc:b",
                    .edge_type = "mentions",
                    .edge_id = "fact:1",
                    .owner_document = "fact:1",
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
                    .edge_id = "fact:1",
                    .owner_document = "fact:1",
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
            try std.testing.expectEqualStrings("fact:1", parsed.req.graph_writes[0].edge_id);
            try std.testing.expectEqualStrings("fact:1", parsed.req.graph_deletes[0].owner_document);
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
            try std.testing.expectEqual(db_types.SplitTransitionMutation.Kind.start, transition.kind);
            try std.testing.expectEqual(@as(u64, 40), transition.transition_id);
            try std.testing.expectEqual(@as(u64, 1), transition.attempt_epoch);
            try std.testing.expectEqual(@as(u64, 42), transition.destination_group_id);
            try std.testing.expectEqualStrings("doc:m", transition.split_key);

            const encoded = try encodeBatchRequest(std.testing.allocator, owned.req);
            defer std.testing.allocator.free(encoded);
            var reparsed = try parseInternalBatchRequest(std.testing.allocator, encoded);
            defer reparsed.deinit(std.testing.allocator);
            try std.testing.expectEqual(db_types.SplitTransitionMutation.Kind.start, reparsed.req.split_transition.?.kind);
        }

        test "internal batch codec round trips replicated transaction phases" {
            const alloc = std.testing.allocator;
            const txn_id: db_types.TxnId = .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
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
            try std.testing.expectEqual(db_types.TransformOpType.min, owned.transforms[0].operations[0].op);
            try std.testing.expectEqualStrings("priority", owned.transforms[0].operations[0].path);
            try std.testing.expectEqual(db_types.TransformOpType.max, owned.transforms[0].operations[1].op);
        }

        test "batch parser accepts supported Go transform op spelling" {
            var owned = try parseBatchRequest(std.testing.allocator,
                \\{"transforms":[{"key":"doc:a","operations":[{"op":"$addToSet","path":"tags","value":"zig"},{"op":"$setOnInsert","path":"created_at","value":"now"}]}]}
            );
            defer owned.deinit(std.testing.allocator);
            try std.testing.expectEqual(@as(usize, 1), owned.transforms.len);
            try std.testing.expectEqual(@as(usize, 2), owned.transforms[0].operations.len);
            try std.testing.expectEqual(db_types.TransformOpType.add_to_set, owned.transforms[0].operations[0].op);
            try std.testing.expectEqual(db_types.TransformOpType.set_on_insert, owned.transforms[0].operations[1].op);
        }

        test "batch parser accepts push for graph edge arrays" {
            var owned = try parseBatchRequest(std.testing.allocator,
                \\{"transforms":[{"key":"doc:a","operations":[{"op":"$push","path":"edges","value":{"to":"doc:b"}}]}]}
            );
            defer owned.deinit(std.testing.allocator);
            try std.testing.expectEqual(db_types.TransformOpType.push, owned.transforms[0].operations[0].op);
        }

        test "batch parser accepts pull for exact array values" {
            var owned = try parseBatchRequest(std.testing.allocator,
                \\{"transforms":[{"key":"doc:a","operations":[{"op":"$pull","path":"edges","value":{"target":"doc:b"}}]}]}
            );
            defer owned.deinit(std.testing.allocator);
            try std.testing.expectEqual(db_types.TransformOpType.pull, owned.transforms[0].operations[0].op);
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
            try std.testing.expectEqual(db_types.SyncLevel.write, owned.req.sync_level);

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

test "leann record survives the internal batch wire and is rejected publicly" {
    const alloc = std.testing.allocator;
    const record = [_]u8{ 0x4c, 0, 1, 2, 255, 254, 7 };
    const encoded = try encodeBatchRequest(alloc, .{ .leann_record = .{ .index_name = "embed\"idx", .record = &record }, .sync_level = .write });
    defer alloc.free(encoded);
    var parsed = try parseInternalBatchRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expectEqualStrings("embed\"idx", parsed.req.leann_record.?.index_name);
    try std.testing.expectEqualSlices(u8, &record, parsed.req.leann_record.?.record);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{ .leann_record = .{ .index_name = "idx", .record = &record }, .deletes = &.{"doc"} }));
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, "{\"_merge_checkpoint\":{\"kind\":\"leann_record_v1\"}}"));
    const mixed = try std.mem.replaceOwned(u8, alloc, encoded, "\"deletes\":[]", "\"deletes\":[\"doc\"]");
    defer alloc.free(mixed);
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, mixed));
}

test "internal batch graph endpoint cleanup command isolates planned effects" {
    const alloc = std.testing.allocator;
    const keys = @import("../storage/internal_keys.zig");
    const job = try keys.graphEndpointCleanupKeyAlloc(alloc, "hub");
    defer alloc.free(job);
    const encoded = try encodeBatchRequest(alloc, .{ .graph_endpoint_cleanup = true, .graph_endpoint_cleanup_planned = true, .graph_endpoint_cleanup_guards = &.{.{ .endpoint = "hub", .generation = 0 }}, .deletes = &.{job}, .graph_deletes = &.{.{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R", .owner = "owner" }} });
    defer alloc.free(encoded);
    var parsed = try parseInternalBatchRequest(alloc, encoded);
    defer parsed.deinit(alloc);
    try std.testing.expect(parsed.req.graph_endpoint_cleanup_planned);
    try std.testing.expectEqual(.admit_current, parsed.req.document_version_scheme);
    try db_types.validateGraphEndpointCleanupCommand(parsed.req);
    const forwarded = try encodeBatchRequest(alloc, parsed.req);
    defer alloc.free(forwarded);
    var forwarded_parsed = try parseInternalBatchRequest(alloc, forwarded);
    defer forwarded_parsed.deinit(alloc);
    try db_types.validateGraphEndpointCleanupCommand(forwarded_parsed.req);
    try std.testing.expectEqualStrings("hub", parsed.req.graph_endpoint_cleanup_guards[0].endpoint);
    try std.testing.expectEqual(@as(u64, 0), parsed.req.graph_endpoint_cleanup_guards[0].generation);
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{ .graph_endpoint_cleanup = true, .graph_endpoint_cleanup_planned = true, .deletes = &.{job} }));
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{ .graph_endpoint_cleanup = true, .graph_endpoint_cleanup_planned = true, .deletes = &.{job}, .graph_endpoint_cleanup_guards = &.{.{ .endpoint = "other", .generation = 1 }} }));
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{ .graph_endpoint_cleanup = true, .graph_endpoint_cleanup_planned = true, .graph_endpoint_cleanup_guards = &.{ .{ .endpoint = "hub", .generation = 1 }, .{ .endpoint = "hub", .generation = 2 } } }));
    try std.testing.expectEqualStrings(job, parsed.req.deletes[0]);
    try std.testing.expectEqualStrings("owner", parsed.req.graph_deletes[0].owner);
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{ .graph_writes = &.{.{ .index_name = "g", .source = "a", .target = "b", .edge_type = "R", .edge_id = "fact", .owner = "fact" }} }));
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, "{\"_graph_deletes\":[{\"index_name\":\"g\",\"source\":\"a\",\"target\":\"b\",\"edge_type\":\"R\",\"edge_id\":\"fact\",\"owner\":\"fact\"}]}"));
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, encoded));
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{ .graph_endpoint_cleanup_planned = true }));
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{ .graph_endpoint_cleanup = true, .graph_endpoint_cleanup_planned = true, .writes = &.{.{ .key = "document", .value = "{}" }} }));
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(alloc, .{ .graph_endpoint_cleanup = true, .graph_endpoint_cleanup_planned = true, .graph_deletes = &.{.{ .index_name = "g", .source = "a", .target = "hub", .edge_type = "R", .owner_document = "fact" }} }));
}

test "internal batch owner retains escaped policy authority after input and parser DOM destruction" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const body = try a.dupe(u8, "{\"inserts\":{\"a\":{\"id\":1}},\"_row_policy_principal_proof\":\"proof\\u002fowner\",\"_row_policy_database\":\"tenant\\u002fdatabase\",\"_row_policy_admitted_at_seconds\":1}");
            var body_live = true;
            defer if (body_live) a.free(body);
            var parsed = try parseOwnerBatchRequest(a, body);
            defer parsed.deinit(a);
            @memset(body, 0xa5);
            a.free(body);
            body_live = false;
            // Both decoded strings were allocated by the parser; their
            // authority must survive that arena and the transport buffer.
            try std.testing.expectEqualStrings("proof/owner", parsed.req.row_policy_principal_proof);
            try std.testing.expectEqualStrings("tenant/database", parsed.req.row_policy_database);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "internal batch owner accumulator cleans complete and partial participant parses under allocation failure" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const variants = [_]db_types.TransactionMutation{
                .{ .begin = .{ .txn_id = @splat(1), .begin_timestamp = 42, .created_at_ns = 43, .topology_epoch = 1, .participants = &.{ "one", "two" } } },
                .{ .acknowledge_many = .{ .txn_id = @splat(1), .participants = &.{ "one", "two" } } },
                .{ .acknowledge = .{ .txn_id = @splat(1), .participant = "one" } },
            };
            for (variants) |variant| {
                const encoded = try encodeBatchRequest(a, .{ .transaction = variant });
                defer a.free(encoded);
                var owned = try parseOwnerBatchRequest(a, encoded);
                defer owned.deinit(a);
                try std.testing.expect(owned.transaction_participants.len != 0);
            }
            const invalid = "{\"_transaction\":{\"phase\":\"acknowledge_many\",\"txn_id\":\"01010101010101010101010101010101\",\"participants\":[\"allocated\",false]}}";
            if (parseOwnerBatchRequest(a, invalid)) |value| {
                var unexpected = value;
                unexpected.deinit(a);
                return error.TestExpectedError;
            } else |err| if (err != error.InvalidBatchRequest) return err;
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "internal batch JSON null and insert precondition codecs survive allocation failures" {
    const Harness = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const encoded = try encodeBatchRequest(alloc, .{ .writes = &.{.{ .key = "row", .value = "{\"j\":null}", .json_null_fields = &.{"j"} }}, .predicates = &.{.{ .key = "row", .expected_version = 0, .unique_absence = true }}, .transaction = .{ .prepare = .{ .txn_id = @splat(1), .topology_epoch = 1 } } });
            defer alloc.free(encoded);
            var parsed = try parseInternalBatchRequest(alloc, encoded);
            defer parsed.deinit(alloc);
            try std.testing.expectEqualStrings("j", parsed.req.writes[0].json_null_fields[0]);
            try std.testing.expect(parsed.req.predicates[0].unique_absence);
            const prefix = "[\"row-semantics-batch-v1\",";
            try std.testing.expect(std.mem.startsWith(u8, encoded, prefix));
            // Released decoders accept only an object (or the known range
            // marker); they reject this new envelope before parsing writes.
            var legacy = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{});
            defer legacy.deinit();
            try std.testing.expect(legacy.value == .array);
            if (parseInternalBatchRequest(alloc, encoded[prefix.len .. encoded.len - 1])) |value| {
                var unexpected = value;
                unexpected.deinit(alloc);
                return error.TestExpectedError;
            } else |err| if (err != error.InvalidBatchRequest) return err;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "graph cleanup owner replay afterimages survive internal batch round trip" {
    const alloc = std.testing.allocator;
    const contract = @import("../storage/graph_cleanup_contract.zig");
    const key = try contract.ownerJobKeyAlloc(alloc, "owner");
    defer alloc.free(key);
    const old = try contract.encodeOwnerJobAlloc(alloc, .{ .owner = "owner", .generation = 7 });
    defer alloc.free(old);
    const next = try contract.encodeOwnerJobAlloc(alloc, .{ .owner = "owner", .generation = 7, .phase = .inputs });
    defer alloc.free(next);
    const req: db_types.BatchRequest = .{
        .graph_endpoint_cleanup = true,
        .graph_endpoint_cleanup_planned = true,
        .graph_endpoint_cleanup_guards = &.{.{ .endpoint = "owner", .generation = 7, .kind = .owner_replay, .checkpoint_digest = contract.checkpointDigest(old) }},
        .merge_artifacts = &.{.{ .key = key, .value = next }},
    };
    try db_types.validateGraphEndpointCleanupCommand(req);
    const bytes = try encodeBatchRequest(alloc, req);
    defer alloc.free(bytes);
    var parsed = try parseInternalBatchRequest(alloc, bytes);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.merge_artifacts.len);
    try std.testing.expectEqualSlices(u8, key, parsed.req.merge_artifacts[0].key);
    try std.testing.expectEqualSlices(u8, next, parsed.req.merge_artifacts[0].value);
    try std.testing.expectEqualSlices(u8, &contract.checkpointDigest(old), &parsed.req.graph_endpoint_cleanup_guards[0].checkpoint_digest);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(alloc, bytes));
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(alloc, "{\"_merge_artifacts\":[{\"key\":\"arbitrary\",\"value\":\"payload\"}]}"));
}

test "local owner conditional batch wire retains predicates without exposing remote authority" {
    const a = std.testing.allocator;
    const req: db_types.BatchRequest = .{
        .writes = &.{.{ .key = "entity/ada", .value = "{}" }},
        .predicates = &.{.{ .key = "entity/ada", .expected_version = 7, .expected_content_digest = @splat(11) }},
        .sync_level = .write,
    };
    try std.testing.expectError(error.UnsupportedBatchRequestEncoding, encodeBatchRequest(a, req));
    const encoded = try encodeOwnerBatchRequest(a, req);
    defer a.free(encoded);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, encoded));
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, encoded));
    var decoded = try parseOwnerBatchRequest(a, encoded);
    defer decoded.deinit(a);
    try std.testing.expect(decoded.req.transaction == null);
    try std.testing.expectEqual(@as(u64, 7), decoded.req.predicates[0].expected_version);
    try std.testing.expectEqual(@as([32]u8, @splat(11)), decoded.req.predicates[0].expected_content_digest.?);
}

test "SQL domain internal preparation handoffs round trip shared contracts and reject public or stale envelope layouts" {
    const a = std.testing.allocator;
    const handoff = @import("../schema/domain_handoff.zig");
    const contract: handoff.Contract = .{ .schema_version = 3, .domain_digest = @splat(7) };
    const writes = [_]db_types.BatchWrite{
        .{ .key = "a", .value = "{}", .prepared_domain = &contract },
        .{ .key = "b", .value = "{}" },
        .{ .key = "c", .value = "{}", .prepared_domain = &contract },
    };
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, .{ .writes = &.{ writes[0], writes[0] } }));
    const bytes = try encodeBatchRequest(a, .{ .writes = &writes, .relational_schema_version = 3 });
    defer a.free(bytes);
    try std.testing.expect(std.mem.startsWith(u8, bytes, "[\"row-semantics-batch-v2\","));
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, bytes));
    var restored = try parseInternalBatchRequest(a, bytes);
    defer restored.deinit(a);
    try std.testing.expect(restored.writes[0].prepared_domain != null);
    try std.testing.expect(restored.writes[0].prepared_domain == restored.writes[2].prepared_domain);
    try std.testing.expect(restored.writes[1].prepared_domain == null);
    try std.testing.expect(contract.eql(restored.writes[0].prepared_domain.?.*));
    // Revision admission and SQL domain provenance must survive the same wire
    // round trip; neither envelope may erase the other protocol's authority.
    const combined = try encodeBatchRequest(a, .{ .writes = &writes, .relational_schema_version = 3, .document_version_scheme = .revision_v1, .timestamp_ns = 10 });
    defer a.free(combined);
    try std.testing.expect(std.mem.startsWith(u8, combined, "[\"row-semantics-batch-v4\","));
    var combined_restored = try parseInternalBatchRequest(a, combined);
    defer combined_restored.deinit(a);
    try std.testing.expectEqual(db_types.DocumentVersionScheme.revision_v1, combined_restored.req.document_version_scheme);
    try std.testing.expectEqual(@as(u64, 10), combined_restored.req.timestamp_ns);
    try std.testing.expect(contract.eql(combined_restored.writes[0].prepared_domain.?.*));
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, combined));
    // The owned handle can move; row pointers still refer to its heap contract.
    const pointer = restored.writes[0].prepared_domain.?;
    var moved = restored;
    restored = .{};
    defer moved.deinit(a);
    try std.testing.expect(contract.eql(pointer.*));
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer parsed.deinit();
    parsed.value.array.items[0] = .{ .string = "row-semantics-batch-v1" };
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchValue(a, parsed.value));
    const Check = struct {
        fn run(alloc: std.mem.Allocator, source: []const u8) !void {
            var batch = try parseInternalBatchRequest(alloc, source);
            defer batch.deinit(alloc);
            try std.testing.expect(batch.writes[0].prepared_domain == batch.writes[2].prepared_domain);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Check.run, .{bytes});
}

test "raft batch external ACK codec refuses authority downgrade" {
    const a = std.testing.allocator;
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const req: db_types.BatchRequest = .{ .transaction = .{ .acknowledge_many = .{ .txn_id = @splat(4), .participants = &.{"self"}, .external_decision_owner = owner } } };
    const bytes = try encodeBatchRequest(a, req);
    defer a.free(bytes);
    var parsed = try parseInternalBatchRequest(a, bytes);
    defer parsed.deinit(a);
    try std.testing.expect(parsed.req.transaction.?.acknowledge_many.external_decision_owner.?.eql(owner));
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, bytes));
    const downgrade = try std.mem.replaceOwned(u8, a, bytes, "external_acknowledge_many_v1", "acknowledge_many");
    defer a.free(downgrade);
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgrade));
}

test "raft batch round trips external transaction private phases preserve exact owner and reject public or downgraded envelopes" {
    const a = std.testing.allocator;
    const owner: ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    for ([_]bool{ false, true }) |activation| {
        const request: db_types.BatchRequest = .{ .transaction = .{ .prepare = .{ .txn_id = @splat(4), .topology_epoch = 5, .external_decision_owner = owner, .external_activation = activation } } };
        const encoded = try encodeBatchRequest(a, request);
        defer a.free(encoded);
        var parsed = try parseInternalBatchRequest(a, encoded);
        defer parsed.deinit(a);
        try std.testing.expectEqualDeep(request.transaction.?, parsed.req.transaction.?);
        try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, encoded));
        const downgraded = try std.mem.replaceOwned(u8, a, encoded, "external_prepare_v1", "prepare");
        defer a.free(downgraded);
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
    }
    const resolved: db_types.BatchRequest = .{ .transaction = .{ .resolve = .{ .txn_id = @splat(4), .status = .committed, .commit_version = 9, .external_decision_owner = owner } } };
    const encoded = try encodeBatchRequest(a, resolved);
    defer a.free(encoded);
    var parsed = try parseInternalBatchRequest(a, encoded);
    defer parsed.deinit(a);
    try std.testing.expectEqualDeep(resolved.transaction.?, parsed.req.transaction.?);
    const downgraded = try std.mem.replaceOwned(u8, a, encoded, "external_resolve_v1", "resolve");
    defer a.free(downgraded);
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
}

test "prepared graph internal producer transport preserves tuple and explicit identity and rejects ambiguity" {
    const Case = struct {
        fn roundTrip(a: std.mem.Allocator) !void {
            const request: db_types.BatchRequest = .{ .graph_writes = &.{
                .{ .index_name = "g", .source = "root", .target = "tuple", .edge_type = "R", .owner = "fact:tuple" },
                .{ .index_name = "g", .source = "root", .target = "explicit", .edge_type = "R", .edge_id = "relationship", .owner_document = "fact:explicit" },
                .{ .index_name = "g", .source = "root", .target = "implicit", .edge_type = "R" },
            } };
            const bytes = try encodeBatchRequest(a, request);
            defer a.free(bytes);
            var parsed = try parseInternalBatchRequest(a, bytes);
            defer parsed.deinit(a);
            try std.testing.expectEqualDeep(request.graph_writes, parsed.req.graph_writes);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.roundTrip, .{});
    const private_bytes = try encodeBatchRequest(std.testing.allocator, .{ .graph_writes = &.{.{ .index_name = "g", .source = "root", .target = "tuple", .edge_type = "R", .owner = "producer" }} });
    defer std.testing.allocator.free(private_bytes);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, private_bytes));
    const prefix = "{\"_graph_writes\":[{\"index_name\":\"g\",\"source\":\"root\",\"target\":\"tuple\",\"edge_type\":\"R\",";
    inline for (.{ "\"owner\":\"\"", "\"owner\":42", "\"owner\":null", "\"owner\":\"fact\",\"edge_id\":\"relationship\"", "\"owner\":\"fact\",\"owner_document\":\"fact\",\"edge_id\":\"relationship\"", "\"owner_document\":\"fact\"" }) |fields| {
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(std.testing.allocator, prefix ++ fields ++ "}]}"));
    }
    const tuple: db_types.GraphEdgeWrite = .{ .index_name = "g", .source = "root", .target = "tuple", .edge_type = "R", .owner = "fact" };
    var ambiguous = tuple;
    ambiguous.owner_document = "fact";
    ambiguous.edge_id = "relationship";
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(std.testing.allocator, .{ .graph_writes = &.{ambiguous} }));
}

test "distributed txn public mutation promotion borrows exact values and transfers ownership without serialization" {
    const a = std.testing.allocator;
    var owned = try public_codec.parseBatchRequest(a, "{\"inserts\":{\"row\":{\"n\":9007199254740993}}}");
    const original = owned.req.writes[0].value.ptr;
    var internal = takePublic(&owned);
    defer internal.deinit(a);
    owned.deinit(a);
    try std.testing.expectEqual(original, internal.req.writes[0].value.ptr);
    try std.testing.expect(internal.req.transaction == null);
    try std.testing.expect(internal.req.online_source == null);
    try std.testing.expectEqualStrings("", internal.req.row_policy_principal_proof);
    try std.testing.expectEqualStrings("{\"n\":9007199254740993}", internal.req.writes[0].value);
}

test "time series document revision wire discriminates legacy replay and epoch admission" {
    const a = std.testing.allocator;
    var legacy = try parseInternalBatchRequest(a, "{\"inserts\":{},\"_timestamp_ns\":\"123\"}");
    defer legacy.deinit(a);
    try std.testing.expectEqual(db_types.DocumentVersionScheme.legacy, legacy.req.document_version_scheme);
    for ([_]db_types.DocumentVersionScheme{.revision_v1}) |scheme| {
        const bytes = try encodeBatchRequest(a, .{ .document_version_scheme = scheme, .timestamp_ns = 0, .writes = &.{.{ .key = "event", .value = "{}" }} });
        defer a.free(bytes);
        try std.testing.expect(std.mem.startsWith(u8, bytes, "[\"document-revisions-batch-v1\","));
        try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, bytes));
        var decoded = try parseInternalBatchRequest(a, bytes);
        defer decoded.deinit(a);
        // Unmarked private controls normalize legacy/admission policy to neutral.
        try std.testing.expectEqual(if (scheme == .revision_v1) scheme else .admit_current, decoded.req.document_version_scheme);
        try std.testing.expectEqual(@as(u64, 0), decoded.req.timestamp_ns);
    }
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, "{\"_timestamp_ns\":\"1\",\"_document_version_scheme\":1}"));
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, "[\"document-revisions-batch-v1\",{\"_document_version_scheme\":1}]"));
}

test "time series merge revision floor requires versioned wire and authenticates full u64" {
    const a = std.testing.allocator;
    var req: db_types.BatchRequest = .{
        .merge_replication = .{ .transition_id = 1, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } },
        .merge_page = .{ .source = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .pin_digest = @splat(7), .applied_index = 10 }, .sequence = 1, .phase = .rows, .exhausted = true, .digest = @splat(0), .document_version_floor = 9007199254740993 },
    };
    req.merge_page.?.digest = merge_pages.commandDigest(req);
    try std.testing.expect(requiresDocumentVersionsEnvelope(req));
    const wire = try encodeBatchRequest(a, req);
    defer a.free(wire);
    try std.testing.expect(std.mem.indexOf(u8, wire, "document-revisions-batch-v1") != null);
    var parsed = try parseInternalBatchRequest(a, wire);
    defer parsed.deinit(a);
    try std.testing.expectEqual(req.merge_page.?.document_version_floor, parsed.req.merge_page.?.document_version_floor);
    var tampered = req;
    tampered.merge_page.?.document_version_floor += 1;
    try std.testing.expectError(error.InvalidMergePage, merge_pages.validateRequest(tampered));
    req.merge_page.?.document_version_floor = 0;
    req.merge_page.?.digest = merge_pages.commandDigest(req);
    try std.testing.expect(!requiresDocumentVersionsEnvelope(req));
    const legacy = try encodeBatchRequest(a, req);
    defer a.free(legacy);
    try std.testing.expect(std.mem.indexOf(u8, legacy, "document_version_floor") == null);
}

test "time series staged timestamp wire protects exact sources and retains legacy commands" {
    const a = std.testing.allocator;
    const staging = @import("../storage/db/restore_staging_contract.zig");
    const timestamps = [_]staging.Timestamp{
        .{ .key = "historical", .timestamp = 9007199254740993, .ttl_source_ns = -1 },
        .{ .key = "wide", .timestamp = 9007199254740993, .ttl_source_ns = 253402300799999999999 },
        .{ .key = "epoch", .timestamp = 0, .ttl_source_ns = 0 },
        .{ .key = "missing", .timestamp = 0 },
    };
    for ([_]bool{ false, true }) |rewrite| {
        const page: staging.ImportPage = .{ .expected = @splat(1), .next = "progress", .scope = @splat(2), .timestamps = &timestamps, .document_timestamps = .{ .version_floor = 9007199254740995 } };
        const req: db_types.BatchRequest = .{ .restore_staging = if (rewrite) .{ .rewrite_page = page } else .{ .import_page = page } };
        try std.testing.expect(requiresDocumentVersionsEnvelope(req));
        const wire = try encodeBatchRequest(a, req);
        defer a.free(wire);
        try std.testing.expect(std.mem.startsWith(u8, wire, "[\"document-revisions-batch-v1\","));
        var parsed = try parseInternalBatchRequest(a, wire);
        defer parsed.deinit(a);
        const decoded = if (rewrite) parsed.req.restore_staging.?.rewrite_page else parsed.req.restore_staging.?.import_page;
        try std.testing.expectEqualDeep(page, decoded);
        // Removing the mandatory envelope must never downgrade these effects.
        const unwrapped = wire["[\"document-revisions-batch-v1\",".len .. wire.len - 1];
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, unwrapped));
    }
    const legacy: db_types.BatchRequest = .{ .restore_staging = .{ .import_page = .{ .expected = @splat(1), .next = "progress", .scope = @splat(2), .timestamps = &.{.{ .key = "a", .timestamp = 1 }} } } };
    try std.testing.expect(!requiresDocumentVersionsEnvelope(legacy));
    const wire = try encodeBatchRequest(a, legacy);
    defer a.free(wire);
    try std.testing.expect(std.mem.indexOf(u8, wire, "document_timestamps") == null);
    try std.testing.expect(std.mem.indexOf(u8, wire, "ttl_source_ns") == null);
    var parsed = try parseInternalBatchRequest(a, wire);
    defer parsed.deinit(a);
    try std.testing.expectEqualDeep(legacy.restore_staging, parsed.req.restore_staging);
}

test "range tracking activation codec preserves isolated control across revision admission" {
    const a = std.testing.allocator;
    var request: db_types.BatchRequest = .{ .activate_range_tracking = true, .timestamp_ns = 1 };
    request.document_version_scheme = request.admittedDocumentVersionScheme(true);
    const bytes = try encodeBatchRequest(a, request);
    defer a.free(bytes);
    var restored = try parseInternalBatchRequest(a, bytes);
    defer restored.deinit(a);
    try std.testing.expect(restored.req.activate_range_tracking);
    try std.testing.expectEqual(db_types.DocumentVersionScheme.admit_current, restored.req.document_version_scheme);
    request.document_version_scheme = .revision_v1;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, request));
    request.document_version_scheme = .admit_current;
    request.writes = &.{.{ .key = "a", .value = "{}" }};
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, request));
}

test "distributed txn private control wire keeps document revision metadata independent of effects" {
    const a = std.testing.allocator;
    const inventory = @import("../storage/db/artifact_inventory.zig");
    const catalogs: inventory.Catalogs = .{ .indexes = "AIDX\x02\x00\x00\x00\x00\x00\x00\x00", .enrichments = "[]", .resolvers = "[]" };
    const upload = @import("../storage/db/artifact_publication_transport.zig");
    const hashes = [_]upload.Digest{upload.chunkDigest(0, "data")};
    const requests = [_]db_types.BatchRequest{
        .{ .activate_range_tracking = true, .sync_level = .write },
        .{ .row_policy_publication = .{ .table_id = 7, .expected_generation = 11, .expected_catalog_epoch = 13, .expected_phase = .pending_install, .owner_group_id = 17, .expected_descriptor_digest = @splat(0xab) }, .row_policy_install_bundle = "{\"table_id\":7}" },
        .{ .artifact_catalog = .{ .namespace = @splat(1), .binding = .{ .epoch = 1, .digest = catalogs.digest(), .semantic_digest = try catalogs.semanticDigest(a) }, .catalogs = catalogs } },
        .{ .artifact_publication_transport = .{ .action = .begin, .namespace = @splat(1), .publication_digest = @splat(2), .command_digest = @splat(3), .encoded_len = 4, .chunk_hashes = &hashes } },
        .{ .group_source = .{ .action = .activate_catalog, .artifact = "group", .definition = .{ .table_incarnation = 1, .configuration = @splat(1) }, .namespace = @splat(1), .backfill = .{ .route = .{ .value = "" } } } },
        .{ .resolver_review = .{ .namespace = @splat(1), .resolver = "resolver", .document = "doc", .source_artifact = "source", .resolution_artifact = "resolution", .local_id = "local", .expected_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", .config_generation = 1, .curation_revision = 1, .decision = .suppressed, .table = "table", .key = "" } },
        .{ .leann_record = .{ .index_name = "vector", .record = "record" } },
        .{ .graph_endpoint_cleanup = true },
        .{ .merge_proof_adoption = .{ .transition_id = 7, .attempt = .{ .donor_term = 3, .sequence = 4 }, .source_pin = @splat(1), .proof_digest = @splat(2), .record_digest = @splat(3) } },
    };
    for (requests) |request| for ([_]db_types.DocumentVersionScheme{ .admit_current, .legacy, .revision_v1 }) |scheme| {
        var versioned = request;
        versioned.document_version_scheme = scheme;
        // The isolated controls below require neutral transport. Other
        // controls explicitly accept an effect-free revision wrapper.
        if (scheme == .revision_v1) {
            if (request.artifact_catalog != null) {
                try std.testing.expectError(error.InvalidArtifactCatalogCommand, encodeBatchRequest(a, versioned));
                continue;
            }
            if (request.activate_range_tracking or request.row_policy_publication != null or request.artifact_publication_transport != null) {
                try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, versioned));
                continue;
            }
        }
        const bytes = try encodeBatchRequest(a, versioned);
        defer a.free(bytes);
        try std.testing.expectError(error.InvalidBatchRequest, @import("batch.zig").parseBatchRequest(a, bytes));
        var decoded = try parseInternalBatchRequest(a, bytes);
        defer decoded.deinit(a);
        // Unmarked private controls normalize legacy/admission policy to neutral.
        try std.testing.expectEqual(if (scheme == .revision_v1) scheme else .admit_current, decoded.req.document_version_scheme);
        if (scheme == .revision_v1 and request.merge_proof_adoption != null) {
            // These controls historically forbid an authored timestamp. Only
            // the revision envelope's mandatory zero field is structural.
            var altered = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
            defer altered.deinit();
            try altered.value.array.items[1].object.put(altered.arena.allocator(), "_timestamp_ns", .{ .integer = 1 });
            const nonzero = try std.json.Stringify.valueAlloc(a, altered.value, .{});
            defer a.free(nonzero);
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, nonzero));
        }
        // Excluding revision transport metadata must never relax the sole
        // control entry's prohibition on user mutations.
        versioned.writes = &.{.{ .key = "not-a-control", .value = "{}" }};
        const mixed_error = if (versioned.artifact_catalog != null) error.InvalidArtifactCatalogCommand else if (versioned.group_source != null) error.InvalidGroupedSourceCommand else if (versioned.resolver_review != null) error.InvalidResolverCurationRequest else error.InvalidBatchRequest;
        try std.testing.expectError(mixed_error, encodeBatchRequest(a, versioned));
    };
}

test "document revision reservation private prepare batch roundtrips opaque keys and refuses downgrade or anonymous use" {
    const a = std.testing.allocator;
    const revisions = @import("../storage/document_revision_reservation.zig");
    const id: db_types.TxnId = @splat(44);
    const scope: revisions.Scope = .{ .metadata_incarnation = @splat('1'), .transaction_id = id, .principal_digest = @splat(3), .begin_timestamp = 10, .table_id = 1, .shard_id = 2, .range_id = 3 };
    const owner: @import("../storage/transactions.zig").ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const request: db_types.TransactionIntentRequest = .{ .document_version_scheme = .revision_v1, .external_decision_owner = owner, .document_revision_handoff = .{ .scope = scope, .references = &.{.{ .key = "row\x00\xc3\xbf", .version = 9007199254740993 }} }, .writes = &.{.{ .key = "row\x00\xc3\xbf", .value = "{\"n\":9007199254740993}" }} };
    const encoded = try encodeBatchRequest(a, request.asBatch(id, 7));
    defer a.free(encoded);
    try std.testing.expect(std.mem.startsWith(u8, encoded, "[\"document-revision-prepare-batch-v1\","));
    const local_encoded = try encodeOwnerBatchRequest(a, request.asBatch(id, 7));
    defer a.free(local_encoded);
    try std.testing.expectEqualSlices(u8, encoded, local_encoded);
    var local_parsed = try parseOwnerBatchRequest(a, local_encoded);
    defer local_parsed.deinit(a);
    try std.testing.expect(scope.eql(local_parsed.req.transaction.?.prepare.document_revision_handoff.?.scope));
    try std.testing.expectError(error.InvalidTxnRequest, (try db_types.TransactionIntentRequest.fromBatch(local_parsed.req)).asAtomicBatch());
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, encoded));
    try std.testing.expectError(error.InvalidTxnRequest, request.asAtomicBatch());
    var parsed = try parseInternalBatchRequest(a, encoded);
    defer parsed.deinit(a);
    const recovered = try db_types.TransactionIntentRequest.fromBatch(parsed.req);
    try std.testing.expect(scope.eql(recovered.document_revision_handoff.?.scope));
    try std.testing.expectEqualStrings("row\x00\xc3\xbf", recovered.document_revision_handoff.?.references[0].key);
    try std.testing.expectEqual(@as(u64, 9007199254740993), recovered.document_revision_handoff.?.references[0].version);
    try std.testing.expectEqualStrings(request.writes[0].value, recovered.writes[0].value);
    const again = try encodeBatchRequest(a, parsed.req);
    defer a.free(again);
    try std.testing.expectEqualSlices(u8, encoded, again);
    const invalid = try a.dupe(u8, encoded);
    defer a.free(invalid);
    invalid["[\"document-revision-prepare-batch-v".len] = '2';
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, invalid));
    var wrong = request;
    wrong.document_version_scheme = .legacy;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, wrong.asBatch(id, 7)));
}
test "document revision reservation private begin preserves exact adoption scope and refuses phase downgrade" {
    const Harness = struct {
        fn run(a: std.mem.Allocator) !void {
            const revisions = @import("../storage/document_revision_reservation.zig");
            const id: db_types.TxnId = @splat(49);
            const scope: revisions.Scope = .{ .metadata_incarnation = @splat('1'), .transaction_id = id, .principal_digest = @splat(3), .begin_timestamp = 9007199254740993, .table_id = 1, .shard_id = 2, .range_id = 3 };
            const request: db_types.BatchRequest = .{ .transaction = .{ .begin = .{
                .txn_id = id,
                .external_decision_owner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 },
                .document_revision_scope = scope,
                .begin_timestamp = scope.begin_timestamp,
                .created_at_ns = 1,
                .topology_epoch = 7,
                .retain_terminal = true,
                .participants = &.{"table:docs:group:2"},
            } } };
            const bytes = try encodeBatchRequest(a, request);
            defer a.free(bytes);
            var parsed = try parseInternalBatchRequest(a, bytes);
            defer parsed.deinit(a);
            try std.testing.expect(scope.eql(parsed.req.transaction.?.begin.document_revision_scope.?));
            const cold = try encodeBatchRequest(a, parsed.req);
            defer a.free(cold);
            try std.testing.expectEqualStrings(bytes, cold);
            const downgraded = try std.mem.replaceOwned(u8, a, bytes, "external_begin_revisions_v1", "external_begin_v1");
            defer a.free(downgraded);
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(std.testing.allocator, downgraded));
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(std.testing.allocator, bytes));
            var wrong = request;
            wrong.transaction.?.begin.external_decision_owner = null;
            try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(std.testing.allocator, wrong));
            wrong = request;
            wrong.transaction.?.begin.begin_timestamp += 1;
            try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(std.testing.allocator, wrong));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

/// Encode every durable PREPARE input through the existing private codec.
/// No current-row lookup, schema recompilation or projection is permitted when
/// checking a frozen vote. Caller retains the original encoded input for retry.
pub fn completionPrepareInput(a: std.mem.Allocator, request: db_types.BatchRequest) ![]u8 {
    const mutation = request.transaction orelse return error.InvalidBatchRequest;
    if (mutation != .prepare) return error.InvalidBatchRequest;
    if (std.mem.allEqual(u8, &mutation.prepare.txn_id, 0)) return error.InvalidBatchRequest;
    // The leader prepares this packet after admission freezes the request.
    // The ordered log retains its bytes. Native PREPARE verifies the packet
    // against this request and the physical read set before recording a vote.
    var input = request;
    if (mutation.prepare.completion_owner != null and mutation.prepare.external_decision_owner != null) {
        // The metadata owner commits the manifest that contains this digest.
        // The accepting transaction separately checks that exact owner and
        // publication grant before it writes any intent.
        input.transaction.?.prepare.external_decision_owner = null;
        input.transaction.?.prepare.external_vote_admitted_at_ns = 0;
        input.transaction.?.prepare.publication_capture = null;
        input.transaction.?.prepare.document_revision_handoff = null;
        input.trigger_preparation_proof = "";
        input.trigger_preparation_group_id = 0;
        input.trigger_preparation_command_digest = null;
        input.trigger_preparation_admitted_at_seconds = 0;
    }
    if (input.commit_requires == .direct_enrichments) input.prepared_enrichments_json = null;
    return encodeBatchRequest(a, input);
}

pub fn requireCompletionPrepareInput(a: std.mem.Allocator, plan: @import("../storage/transaction_completion.zig").Admission, namespace: [24]u8, request: db_types.BatchRequest) !void {
    const mutation = request.transaction orelse return error.InvalidBatchRequest;
    if (mutation != .prepare) return error.InvalidBatchRequest;
    if (!std.mem.eql(u8, &mutation.prepare.txn_id, &plan.transaction_id)) return error.SourceTransactionIdentityConflict;
    const encoded = try completionPrepareInput(a, request);
    defer a.free(encoded);
    if (mutation.prepare.completion_owner) |owner| {
        if (!std.mem.eql(u8, &namespace, &owner.namespace)) return error.InvalidParticipant;
        try plan.requireOwnerPreparedInput(owner, encoded);
    } else try plan.requirePreparedInput(namespace, encoded);
}

/// Completion control phases cannot carry another mutation.
/// New private fields fail closed until explicitly reviewed for this phase.
pub fn validateCompletionControlRequest(req: db_types.BatchRequest) !void {
    const defaults: db_types.BatchRequest = .{};
    inline for (comptime std.meta.fieldNames(db_types.BatchRequest)) |name| {
        if (comptime std.mem.eql(u8, name, "transaction") or
            std.mem.eql(u8, name, "timestamp_ns") or
            std.mem.eql(u8, name, "document_version_scheme") or
            std.mem.eql(u8, name, "sync_level") or
            std.mem.eql(u8, name, "schema_version") or
            std.mem.eql(u8, name, "row_policy_principal_proof") or
            std.mem.eql(u8, name, "row_policy_database") or
            std.mem.eql(u8, name, "row_policy_admitted_at_seconds") or
            std.mem.eql(u8, name, "row_policy_install_bundle")) continue;
        const field = @field(req, name);
        if (comptime @typeInfo(@TypeOf(field)) == .pointer and @typeInfo(@TypeOf(field)).pointer.size == .slice) {
            // Decoding owns empty arrays; their addresses are not mutations.
            if (field.len != 0) return error.InvalidBatchRequest;
        } else if (!std.meta.eql(field, @field(defaults, name))) return error.InvalidBatchRequest;
    }
}

/// Non-history producer BEGIN retains the ordinary restore envelope. Restore
/// authority is independently checked by the destination, not by this label.
pub fn validateProducerBeginControlRequest(req: db_types.BatchRequest) !void {
    const mutation = req.transaction orelse return error.InvalidBatchRequest;
    if (mutation != .begin or mutation.begin.completion_admission != null or mutation.begin.completion_owner != null) return error.InvalidBatchRequest;
    var control = req;
    control.restore_staging_scope = null;
    control.restore_staging_plan_id = null;
    try validateCompletionControlRequest(control);
}

/// A completion BEGIN freezes the same complete participant set at every root.
/// Recovery order is coordinator first, then canonical namespace order excluding
/// the coordinator. Unknown phases cannot downgrade to legacy admission.
pub fn validateCompletionPublication(a: std.mem.Allocator, id: [16]u8, owner: @import("../storage/transaction_completion.zig").RoutedOwner, raw: []const u8) !void {
    const completion = @import("../storage/transaction_completion.zig");
    var parsed = completion.parse(a, raw) catch |err| return if (err == error.OutOfMemory) err else error.InvalidBatchRequest;
    defer parsed.deinit();
    if (!std.mem.eql(u8, &parsed.value.certificate.manifest.transaction_id, &id)) return error.InvalidBatchRequest;
    try parsed.value.certificate.manifest.admission().requireRoutedOwner(owner);
}

fn validateExternalCompletionOwner(external: ExternalDecisionOwner, owner: @import("../storage/transaction_completion.zig").RoutedOwner) !void {
    if (!std.mem.eql(u8, &external.metadata_incarnation, &owner.metadata_incarnation)) return error.InvalidBatchRequest;
}

fn validateOrdinaryCompletionBegin(a: std.mem.Allocator, raw: []const u8) !void {
    var parsed = try @import("../storage/transaction_completion.zig").parseAdmission(a, raw);
    defer parsed.deinit();
    if (parsed.value.admission.version < 3) return error.ChangefeedCapabilityUnsupported;
}

pub fn validateCompletionBegin(a: std.mem.Allocator, raw: []const u8, id: [16]u8, participants: []const []const u8, owner: ?@import("../storage/transaction_completion.zig").RoutedOwner, topology_epoch: u64) !void {
    const completion = @import("../storage/transaction_completion.zig");
    var plan = completion.parseAdmission(a, raw) catch |err| return if (err == error.OutOfMemory) err else error.InvalidBatchRequest;
    defer plan.deinit();
    const admission = plan.value.admission;
    if (plan.value.local_owner_fence != null) return error.InvalidBatchRequest;
    if (admission.version >= 2) {
        const binding = owner orelse return error.InvalidBatchRequest;
        try admission.requireRoutedOwner(binding);
        if (binding.topology_epoch != topology_epoch) return error.InvalidBatchRequest;
    } else if (owner != null) return error.InvalidBatchRequest;
    if (!std.mem.eql(u8, &admission.transaction_id, &id) or participants.len != admission.participants.len) return error.InvalidBatchRequest;
    const coordinator = try admission.coordinatorMember();
    if (!std.mem.eql(u8, participants[0], coordinator.locator)) return error.InvalidBatchRequest;
    var index: usize = 1;
    for (admission.participants) |member| {
        if (admission.isCoordinator(member)) continue;
        if (!std.mem.eql(u8, participants[index], member.locator)) return error.InvalidBatchRequest;
        index += 1;
    }
}

test "completion begin wire preserves frozen envelope and rejects downgrade and participant substitution" {
    const a = std.testing.allocator;
    const completion = @import("../storage/transaction_completion.zig");
    const members = [_]completion.Participant{
        .{ .namespace = @splat(1), .owner_fence = @splat(2), .roles = completion.Roles.effects, .locator = "table2:4:docs:group:7" },
        .{ .namespace = @splat(3), .owner_fence = @splat(4), .roles = completion.Roles.guards, .locator = "table2:4:docs:group:9" },
    };
    const admission: completion.Admission = .{ .transaction_id = @splat(5), .admission_digest = @splat(6), .coordinator_namespace = members[1].namespace, .coordinator_fence = members[1].owner_fence, .participants = &members };
    const raw = try completion.encodeAdmission(a, admission);
    defer a.free(raw);
    var request: db_types.BatchRequest = .{ .transaction = .{ .begin = .{ .txn_id = admission.transaction_id, .begin_timestamp = 10, .created_at_ns = 11, .topology_epoch = 12, .participants = &.{ members[1].locator, members[0].locator }, .completion_admission = raw } } };
    const encoded = try encodeBatchRequest(a, request);
    defer a.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "begin_completion_v1") != null);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, encoded));
    var decoded = try parseInternalBatchRequest(a, encoded);
    defer decoded.deinit(a);
    try std.testing.expectEqualStrings(raw, decoded.req.transaction.?.begin.completion_admission.?);
    const downgraded = try std.mem.replaceOwned(u8, a, encoded, "begin_completion_v1", "begin");
    defer a.free(downgraded);
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
    request.transaction.?.begin.participants = &.{ members[0].locator, members[1].locator };
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, request));
    request.transaction.?.begin.participants = &.{members[1].locator};
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, request));
    request.transaction.?.begin.participants = &.{ members[1].locator, members[0].locator };
    request.transaction.?.begin.txn_id[0] ^= 1;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, request));
}

test "completion begin wire input binding includes original effects guards and topology" {
    const a = std.testing.allocator;
    const completion = @import("../storage/transaction_completion.zig");
    var request: db_types.BatchRequest = .{ .transaction = .{ .prepare = .{ .txn_id = @splat(1), .topology_epoch = 2 } }, .writes = &.{.{ .key = "row", .value = "{\"v\":1}" }}, .deletes = &.{"old"} };
    const raw = try completionPrepareInput(a, request);
    defer a.free(raw);
    const namespace: [24]u8 = @splat(2);
    const members = [_]completion.Participant{.{ .namespace = namespace, .owner_fence = @splat(3), .roles = completion.Roles.effects | completion.Roles.guards, .locator = "writer", .input_digest = completion.preparedInputDigest(namespace, raw) }};
    const plan: completion.Admission = .{ .version = 2, .transaction_id = @splat(1), .admission_digest = @splat(4), .coordinator_namespace = namespace, .coordinator_fence = @splat(3), .participants = &members };
    try plan.requirePreparedInput(namespace, raw);
    try requireCompletionPrepareInput(a, plan, namespace, request);
    var decoded = try parseInternalBatchRequest(a, raw);
    defer decoded.deinit(a);
    const replay = try completionPrepareInput(a, decoded.req);
    defer a.free(replay);
    try std.testing.expectEqualStrings(raw, replay);
    request.transaction.?.prepare.topology_epoch += 1;
    const changed_topology = try completionPrepareInput(a, request);
    defer a.free(changed_topology);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, plan.requirePreparedInput(namespace, changed_topology));
    request.transaction.?.prepare.topology_epoch -= 1;
    request.deletes = &.{"different-old"};
    const changed_effect = try completionPrepareInput(a, request);
    defer a.free(changed_effect);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, plan.requirePreparedInput(namespace, changed_effect));
    request.deletes = &.{"old"};
    request.predicates = &.{.{ .key = "guard", .expected_version = 3 }};
    const changed_guard = try completionPrepareInput(a, request);
    defer a.free(changed_guard);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, plan.requirePreparedInput(namespace, changed_guard));
    request.transaction = null;
    try std.testing.expectError(error.InvalidBatchRequest, completionPrepareInput(a, request));
}

test "completion begin wire carries matching v2 routed owner and rejects missing or inconsistent binding" {
    const a = std.testing.allocator;
    const completion = @import("../storage/transaction_completion.zig");
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 7, .topology_epoch = 3 };
    const members = [_]completion.Participant{.{ .namespace = owner.namespace, .owner_fence = owner.digest(), .roles = completion.Roles.guards, .locator = "table2:4:docs:group:7", .input_digest = @splat(5) }};
    const plan: completion.Admission = .{ .version = 2, .transaction_id = @splat(6), .admission_digest = @splat(7), .coordinator_namespace = owner.namespace, .coordinator_fence = owner.digest(), .participants = &members };
    const raw = try completion.encodeAdmission(a, plan);
    defer a.free(raw);
    var request: db_types.BatchRequest = .{ .transaction = .{ .begin = .{ .txn_id = plan.transaction_id, .begin_timestamp = 1, .created_at_ns = 2, .topology_epoch = owner.topology_epoch, .participants = &.{members[0].locator}, .completion_admission = raw, .completion_owner = owner } } };
    const encoded = try encodeBatchRequest(a, request);
    defer a.free(encoded);
    var parsed = try parseInternalBatchRequest(a, encoded);
    defer parsed.deinit(a);
    try std.testing.expectEqualDeep(owner, parsed.req.transaction.?.begin.completion_owner.?);
    const routed = try encodeRoutedBatchRequest(a, request);
    defer a.free(routed);
    var routed_request = try parseInternalBatchRequest(a, routed);
    defer routed_request.deinit(a);
    try std.testing.expectEqualDeep(owner, routed_request.req.transaction.?.begin.completion_owner.?);
    try std.testing.expectError(error.InvalidBatchRequest, parseOwnerBatchRequest(a, routed));
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, "[\"exact-prepare-batch-v1\",\"{}\"]"));
    request.transaction.?.begin.completion_owner = null;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, request));
    request.transaction.?.begin.completion_owner = owner;
    request.transaction.?.begin.completion_owner.?.root_generation += 1;
    try std.testing.expectError(error.TopologyChanged, encodeBatchRequest(a, request));
    request.transaction.?.begin.completion_owner = owner;
    request.transaction.?.begin.topology_epoch += 1;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, request));
}

/// Completion commands require the reserved all-applying-member floor even
/// before the feature is advertised; never treat them as legacy row batches.
pub fn requiresCompletionProtocol(request: db_types.BatchRequest) bool {
    const mutation = request.transaction orelse return false;
    return switch (mutation) {
        .begin => |begin| begin.completion_admission != null or begin.completion_owner != null,
        .prepare => |prepare| prepare.completion_owner != null,
        .resolve => |resolve| resolve.completion_owner != null,
        .publish_completion => true,
        else => false,
    };
}

test "completion begin wire prepare owner survives guarded replay and refuses legacy downgrade" {
    const a = std.testing.allocator;
    const completion = @import("../storage/transaction_completion.zig");
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 7, .topology_epoch = 3 };
    var request: db_types.BatchRequest = .{ .timestamp_ns = 10, .document_version_scheme = .revision_v1, .transaction = .{ .prepare = .{ .txn_id = @splat(4), .topology_epoch = 3, .completion_owner = owner } } };
    for (0..2) |guarded| {
        request.range_guards = if (guarded == 1) &.{.{ .bucket = 0, .generation = null }} else &.{};
        const encoded = try completionPrepareInput(a, request);
        defer a.free(encoded);
        try std.testing.expect(requiresCompletionProtocol(request));
        var parsed = try parseInternalBatchRequest(a, encoded);
        defer parsed.deinit(a);
        try std.testing.expectEqualDeep(owner, parsed.req.transaction.?.prepare.completion_owner.?);
        try std.testing.expectEqualDeep(request.range_guards, parsed.req.range_guards);
        const replay = try completionPrepareInput(a, parsed.req);
        defer a.free(replay);
        try std.testing.expectEqualStrings(encoded, replay);
        const routed = try encodeRoutedBatchRequest(a, request);
        defer a.free(routed);
        var routed_request = try parseInternalBatchRequest(a, routed);
        defer routed_request.deinit(a);
        const routed_prepare = try completionPrepareInput(a, routed_request.req);
        defer a.free(routed_prepare);
        try std.testing.expectEqualStrings(encoded, routed_prepare);
        const downgraded = try std.mem.replaceOwned(u8, a, encoded, "prepare_completion_v1", "prepare");
        defer a.free(downgraded);
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
        try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, encoded));
    }
    request.timestamp_ns = 0;
    try std.testing.expectError(error.InvalidBatchRequest, completionPrepareInput(a, request));
    request.timestamp_ns = 10;
    request.document_version_scheme = .admit_current;
    try std.testing.expectError(error.InvalidBatchRequest, completionPrepareInput(a, request));
    request.document_version_scheme = .revision_v1;
    request.transaction.?.prepare.topology_epoch += 1;
    try std.testing.expectError(error.InvalidBatchRequest, completionPrepareInput(a, request));
}

test "completion begin wire rejects unrelated private mutations" {
    try validateCompletionControlRequest(.{});
    const empty = try std.testing.allocator.alloc(db_types.BatchWrite, 0);
    defer std.testing.allocator.free(empty);
    try validateCompletionControlRequest(.{ .writes = empty });
    try validateCompletionControlRequest(.{ .timestamp_ns = 10, .document_version_scheme = .revision_v1, .schema_version = 2 });
    try std.testing.expectError(error.InvalidBatchRequest, validateCompletionControlRequest(.{ .activate_range_tracking = true }));
    try std.testing.expectError(error.InvalidBatchRequest, validateCompletionControlRequest(.{ .restore_staging_plan_id = @splat(1) }));
    try std.testing.expectError(error.InvalidBatchRequest, validateCompletionControlRequest(.{ .graph_endpoint_cleanup = true }));
    try std.testing.expectError(error.InvalidBatchRequest, validateCompletionControlRequest(.{ .relational_repair = true }));
    try std.testing.expectError(error.InvalidBatchRequest, validateCompletionControlRequest(.{ .deletes = &.{"row"} }));
}

test "completion begin wire resolution fences owner and rejects downgrade" {
    const a = std.testing.allocator;
    const completion = @import("../storage/transaction_completion.zig");
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 7, .topology_epoch = 3 };
    var request: db_types.BatchRequest = .{ .transaction = .{ .resolve = .{ .completion_owner = owner, .txn_id = @splat(4), .status = .committed, .commit_version = 10 } } };
    const raw = try encodeBatchRequest(a, request);
    defer a.free(raw);
    var decoded = try parseInternalBatchRequest(a, raw);
    defer decoded.deinit(a);
    try std.testing.expect(requiresCompletionProtocol(decoded.req));
    try std.testing.expectEqualDeep(owner, decoded.req.transaction.?.resolve.completion_owner.?);
    try validateCompletionControlRequest(decoded.req);
    var json = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    defer json.deinit();
    const transaction = json.value.object.getPtr("_transaction").?;
    try transaction.object.put(json.arena.allocator(), "phase", .{ .string = "resolve" });
    const downgraded = try std.json.Stringify.valueAlloc(a, json.value, .{});
    defer a.free(downgraded);
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
    try transaction.object.put(json.arena.allocator(), "phase", .{ .string = "resolve_completion_v1" });
    _ = transaction.object.swapRemove("completion_owner");
    const missing = try std.json.Stringify.valueAlloc(a, json.value, .{});
    defer a.free(missing);
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, missing));
    request.deletes = &.{"unrelated"};
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, request));
}

test "completion begin wire publication owns complete certificate and rejects downgrade" {
    const a = std.testing.allocator;
    const completion = @import("../storage/transaction_completion.zig");
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 7, .topology_epoch = 3 };
    const members = [_]completion.Participant{.{ .namespace = owner.namespace, .owner_fence = owner.digest(), .roles = completion.Roles.guards, .locator = "table2:4:docs:group:7", .input_digest = @splat(5) }};
    const manifest: completion.Manifest = .{ .version = 2, .transaction_id = @splat(4), .admission_digest = @splat(6), .coordinator_namespace = owner.namespace, .coordinator_fence = owner.digest(), .participants = &members, .commit_version = 10 };
    var position: [32]u8 = @splat(0);
    std.mem.writeInt(u64, position[0..8], 1, .little);
    std.mem.writeInt(u64, position[8..16], 1, .little);
    position[31] = 1;
    const receipt: completion.Resolution = .{ .namespace = owner.namespace, .owner_fence = owner.digest(), .manifest_digest = manifest.digest(), .commit_version = 10, .source_position = position, .outcome = .{ .no_effect = completion.noEffectProof(manifest, owner.namespace, position) } };
    const certificate = try completion.encode(a, .{ .manifest = manifest, .resolutions = &.{receipt} });
    defer a.free(certificate);
    var request: db_types.BatchRequest = .{ .transaction = .{ .publish_completion = .{ .txn_id = manifest.transaction_id, .completion_owner = owner, .certificate = certificate } } };
    const raw = try encodeBatchRequest(a, request);
    defer a.free(raw);
    try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, raw));
    var decoded = try parseInternalBatchRequest(a, raw);
    defer decoded.deinit(a);
    try std.testing.expect(requiresCompletionProtocol(decoded.req));
    try std.testing.expectEqualStrings(certificate, decoded.req.transaction.?.publish_completion.certificate);
    try std.testing.expectEqualDeep(owner, decoded.req.transaction.?.publish_completion.completion_owner);
    request.deletes = &.{"unrelated"};
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, request));
    request.deletes = &.{};
    request.transaction.?.publish_completion.txn_id[0] ^= 1;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, request));
    var json = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    defer json.deinit();
    const transaction = json.value.object.getPtr("_transaction").?;
    try transaction.object.put(json.arena.allocator(), "phase", .{ .string = "resolve" });
    const downgraded = try std.json.Stringify.valueAlloc(a, json.value, .{});
    defer a.free(downgraded);
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
    try transaction.object.put(json.arena.allocator(), "phase", .{ .string = "publish_completion_v1" });
    _ = transaction.object.swapRemove("completion_owner");
    const missing = try std.json.Stringify.valueAlloc(a, json.value, .{});
    defer a.free(missing);
    try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, missing));
}

fn validateSourceCompletionPurpose(purpose: ?[32]u8, owner: ?@import("../storage/transaction_completion.zig").RoutedOwner) !void {
    if (purpose) |value| if (owner == null or std.mem.allEqual(u8, &value, 0)) return error.InvalidBatchRequest;
}

test "completion begin wire source ownership survives every phase and refuses downgrade" {
    const a = std.testing.allocator;
    const completion = @import("../storage/transaction_completion.zig");
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 7, .topology_epoch = 3 };
    const purpose: [32]u8 = @splat(8);
    const id: [16]u8 = @splat(6);
    const prepare = try (db_types.TransactionIntentRequest{ .completion_owner = owner, .completion_source_purpose = purpose }).completionBatch(id, 10, owner.topology_epoch);
    const input = try completionPrepareInput(a, prepare);
    defer a.free(input);
    const members = [_]completion.Participant{.{ .namespace = owner.namespace, .owner_fence = owner.digest(), .roles = completion.Roles.checkpoint, .locator = "table2:4:docs:group:7", .input_digest = completion.preparedInputDigest(owner.namespace, input), .routed_owner = owner }};
    const plan: completion.Admission = .{ .version = 3, .transaction_id = id, .admission_digest = @splat(7), .coordinator_namespace = owner.namespace, .coordinator_fence = owner.digest(), .participants = &members };
    const raw = try completion.encodeAdmission(a, plan);
    defer a.free(raw);
    const requests = [_]db_types.BatchRequest{
        .{ .document_version_scheme = .legacy, .transaction = .{ .begin = .{ .txn_id = id, .begin_timestamp = 10, .created_at_ns = 11, .topology_epoch = owner.topology_epoch, .retain_terminal = true, .participants = &.{members[0].locator}, .completion_admission = raw, .completion_owner = owner, .source_purpose = purpose } } },
        prepare,
        .{ .document_version_scheme = .legacy, .transaction = .{ .resolve = .{ .completion_owner = owner, .source_purpose = purpose, .txn_id = id, .status = .committed, .commit_version = 12 } } },
    };
    const source_phases = [_][]const u8{ "begin_source_completion_v1", "prepare_source_completion_v1", "resolve_source_completion_v1" };
    const ordinary_phases = [_][]const u8{ "begin_completion_v1", "prepare_completion_v1", "resolve_completion_v1" };
    for (requests, source_phases, ordinary_phases) |request, source_phase, ordinary_phase| {
        const bytes = try encodeBatchRequest(a, request);
        defer a.free(bytes);
        var parsed = try parseInternalBatchRequest(a, bytes);
        defer parsed.deinit(a);
        try std.testing.expectEqualDeep(request, parsed.req);
        try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, bytes));
        const downgraded = try std.mem.replaceOwned(u8, a, bytes, source_phase, ordinary_phase);
        defer a.free(downgraded);
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
        var missing = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .allocate = .alloc_always });
        defer missing.deinit();
        const missing_body = if (missing.value == .array) &missing.value.array.items[1] else &missing.value;
        _ = missing_body.object.getPtr("_transaction").?.object.swapRemove("source_purpose");
        const missing_bytes = try std.json.Stringify.valueAlloc(a, missing.value, .{});
        defer a.free(missing_bytes);
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, missing_bytes));
    }
    var invalid = requests[2];
    invalid.transaction.?.resolve.source_purpose = @splat(0);
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, invalid));
    invalid.transaction.?.resolve.source_purpose = purpose;
    invalid.transaction.?.resolve.completion_owner = null;
    try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, invalid));
}

test "completion begin wire source checkpoint tags cannot be downgraded or stripped" {
    const a = std.testing.allocator;
    for ([_]u32{ 0, 3 }) |window| {
        const completion = @import("../storage/transaction_completion.zig");
        const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 7, .topology_epoch = 3 };
        const purpose: [32]u8 = @splat(8);
        const id: [16]u8 = @splat(6);
        const prepare = try (db_types.TransactionIntentRequest{ .completion_owner = owner, .completion_source_purpose = purpose, .completion_source_checkpoint = .{ .binding = @splat(9), .purpose = purpose, .sequence = 1, .checkpoint = "opaque:1", .accepted_at_ns = 11, .receipt_window = window } }).completionBatch(id, 10, owner.topology_epoch);
        const input = try completionPrepareInput(a, prepare);
        defer a.free(input);
        const members = [_]completion.Participant{.{ .namespace = owner.namespace, .owner_fence = owner.digest(), .roles = completion.Roles.checkpoint, .locator = "table2:4:docs:group:7", .input_digest = completion.preparedInputDigest(owner.namespace, input), .routed_owner = owner }};
        const plan: completion.Admission = .{ .version = 3, .transaction_id = id, .admission_digest = @splat(7), .coordinator_namespace = owner.namespace, .coordinator_fence = owner.digest(), .participants = &members };
        const raw = try completion.encodeAdmission(a, plan);
        defer a.free(raw);
        const begin: db_types.BatchRequest = .{ .document_version_scheme = .legacy, .transaction = .{ .begin = .{ .txn_id = id, .begin_timestamp = 10, .created_at_ns = 11, .topology_epoch = owner.topology_epoch, .retain_terminal = true, .participants = &.{members[0].locator}, .completion_admission = raw, .completion_owner = owner, .source_purpose = purpose, .source_checkpoint = true, .source_checkpoint_receipts = window != 0 } } };
        for ([_]db_types.BatchRequest{ begin, prepare }, [_][]const u8{ if (window == 0) "begin_source_checkpoint_v1" else "begin_source_checkpoint_v2", if (window == 0) "prepare_source_checkpoint_v1" else "prepare_source_checkpoint_v2" }, [_][]const u8{ if (window == 0) "begin_source_completion_v1" else "begin_source_checkpoint_v1", if (window == 0) "prepare_source_completion_v1" else "prepare_source_checkpoint_v1" }) |request, tag, older| {
            const bytes = try encodeBatchRequest(a, request);
            defer a.free(bytes);
            var decoded = try parseInternalBatchRequest(a, bytes);
            defer decoded.deinit(a);
            try std.testing.expectEqualDeep(request, decoded.req);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, bytes));
            const downgraded = try std.mem.replaceOwned(u8, a, bytes, tag, older);
            defer a.free(downgraded);
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
            var missing = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .allocate = .alloc_always });
            defer missing.deinit();
            const body = if (missing.value == .array) &missing.value.array.items[1] else &missing.value;
            const transaction = body.object.getPtr("_transaction").?;
            _ = transaction.object.swapRemove("source_checkpoint");
            const missing_bytes = try std.json.Stringify.valueAlloc(a, missing.value, .{});
            defer a.free(missing_bytes);
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, missing_bytes));
        }
        var changed = prepare;
        changed.transaction.?.prepare.source_checkpoint.?.purpose[0] ^= 1;
        try std.testing.expectError(error.SourceTransactionIdentityConflict, encodeBatchRequest(a, changed));
    }
}

test "completion begin wire scoped source phases retain attempts and reject downgrade" {
    const a = std.testing.allocator;
    const completion = @import("../storage/transaction_completion.zig");
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 7, .topology_epoch = 3 };
    const purpose: [32]u8 = @splat(8);
    const id: [16]u8 = @splat(6);
    const scope: @import("../storage/source_scope.zig").Reference = .{ .domain = @splat(10), .binding = @splat(9), .generation = 1, .token = 2, .sequence = 1 };
    const prepare = try (db_types.TransactionIntentRequest{ .completion_owner = owner, .completion_source_purpose = purpose, .completion_source_scope = scope, .completion_source_checkpoint = .{ .binding = scope.binding, .purpose = purpose, .sequence = scope.sequence, .checkpoint = "opaque:1", .accepted_at_ns = 11, .receipt_window = 3 } }).completionBatch(id, 10, owner.topology_epoch);
    const input = try completionPrepareInput(a, prepare);
    defer a.free(input);
    const members = [_]completion.Participant{.{ .namespace = owner.namespace, .owner_fence = owner.digest(), .roles = completion.Roles.checkpoint, .locator = "table2:4:docs:group:7", .input_digest = completion.preparedInputDigest(owner.namespace, input), .routed_owner = owner }};
    const plan: completion.Admission = .{ .version = 3, .transaction_id = id, .admission_digest = @splat(7), .coordinator_namespace = owner.namespace, .coordinator_fence = owner.digest(), .participants = &members };
    const raw = try completion.encodeAdmission(a, plan);
    defer a.free(raw);
    const begin: db_types.BatchRequest = .{ .document_version_scheme = .legacy, .transaction = .{ .begin = .{ .txn_id = id, .begin_timestamp = 10, .created_at_ns = 11, .topology_epoch = owner.topology_epoch, .retain_terminal = true, .participants = &.{members[0].locator}, .completion_admission = raw, .completion_owner = owner, .source_purpose = purpose, .source_scope = scope, .source_checkpoint = true, .source_checkpoint_receipts = true } } };
    const resolve: db_types.BatchRequest = .{ .document_version_scheme = .legacy, .transaction = .{ .resolve = .{ .txn_id = id, .status = .committed, .commit_version = 12, .completion_owner = owner, .source_purpose = purpose, .source_scope = scope } } };
    var row_prepare = prepare;
    row_prepare.transaction.?.prepare.source_checkpoint = null;
    for ([_]db_types.BatchRequest{ begin, prepare, row_prepare, resolve }, [_][]const u8{ "begin_source_scope_v1", "prepare_source_scope_v1", "prepare_source_scope_v1", "resolve_source_scope_v1" }, [_][]const u8{ "begin_source_checkpoint_v2", "prepare_source_checkpoint_v2", "prepare_source_completion_v1", "resolve_source_completion_v1" }) |request, tag, older| {
        const bytes = try encodeBatchRequest(a, request);
        defer a.free(bytes);
        var decoded = try parseInternalBatchRequest(a, bytes);
        defer decoded.deinit(a);
        try std.testing.expectEqualDeep(request, decoded.req);
        try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, bytes));
        const downgraded = try std.mem.replaceOwned(u8, a, bytes, tag, older);
        defer a.free(downgraded);
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
        var missing = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .allocate = .alloc_always });
        defer missing.deinit();
        const body = if (missing.value == .array) &missing.value.array.items[1] else &missing.value;
        _ = body.object.getPtr("_transaction").?.object.swapRemove("source_scope");
        const stripped = try std.json.Stringify.valueAlloc(a, missing.value, .{});
        defer a.free(stripped);
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, stripped));
    }
    var changed = prepare;
    changed.transaction.?.prepare.source_scope.?.sequence = 2;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, encodeBatchRequest(a, changed));
}

test "external completion wire preserves both authorities and rejects downgrade" {
    const a = std.testing.allocator;
    const completion = @import("../storage/transaction_completion.zig");
    const owner: completion.RoutedOwner = .{
        .metadata_group_id = 1,
        .metadata_incarnation = @splat('1'),
        .namespace = @splat(1),
        .table_binding = @splat(2),
        .root_generation = 0,
        .group_id = 2,
        .topology_epoch = 3,
    };
    const external: ExternalDecisionOwner = .{ .metadata_incarnation = owner.metadata_incarnation, .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const id: [16]u8 = @splat(3);
    const prepare = try (db_types.TransactionIntentRequest{ .completion_owner = owner, .writes = &.{.{ .key = "row", .value = "{}" }} }).completionBatch(id, 100, owner.topology_epoch);
    var external_prepare = prepare;
    external_prepare.transaction.?.prepare.external_decision_owner = external;
    const input = try completionPrepareInput(a, prepare);
    defer a.free(input);
    const external_input = try completionPrepareInput(a, external_prepare);
    defer a.free(external_input);
    try std.testing.expectEqualStrings(input, external_input);
    const members = [_]completion.Participant{.{ .namespace = owner.namespace, .owner_fence = owner.digest(), .input_digest = completion.preparedInputDigest(owner.namespace, input), .roles = completion.Roles.effects, .locator = "table2:00000004:docs:2", .routed_owner = owner }};
    const admission: completion.Admission = .{
        .version = completion.physical_participant_version,
        .transaction_id = id,
        .admission_digest = @splat(4),
        .coordinator_namespace = owner.namespace,
        .coordinator_fence = owner.digest(),
        .participants = &members,
    };
    const raw = try completion.encodeAdmission(a, admission);
    defer a.free(raw);
    const begin: db_types.BatchRequest = .{ .sync_level = .write, .transaction = .{ .begin = .{
        .txn_id = id,
        .begin_timestamp = 100,
        .created_at_ns = 101,
        .topology_epoch = owner.topology_epoch,
        .participants = &.{members[0].locator},
        .retain_terminal = true,
        .completion_admission = raw,
        .completion_owner = owner,
        .external_decision_owner = external,
    } } };
    const resolve: db_types.BatchRequest = .{ .sync_level = .write, .transaction = .{ .resolve = .{
        .txn_id = id,
        .status = .committed,
        .commit_version = 102,
        .completion_owner = owner,
        .external_decision_owner = external,
    } } };
    for ([_]db_types.BatchRequest{ begin, external_prepare, resolve }, [_][]const u8{ "begin", "prepare", "resolve" }) |request, phase| {
        const tag = try std.fmt.allocPrint(a, "external_{s}_completion_v1", .{phase});
        defer a.free(tag);
        const old = try std.fmt.allocPrint(a, "external_{s}_v1", .{phase});
        defer a.free(old);
        const bytes = try encodeBatchRequest(a, request);
        defer a.free(bytes);
        var decoded = try parseInternalBatchRequest(a, bytes);
        defer decoded.deinit(a);
        try std.testing.expectEqualDeep(request.transaction.?, decoded.req.transaction.?);
        try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, bytes));
        const downgraded = try std.mem.replaceOwned(u8, a, bytes, tag, old);
        defer a.free(downgraded);
        try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
    }
}

test "completion begin wire producer completion phases preserve absence and reject downgrade" {
    const a = std.testing.allocator;
    const completion = @import("../storage/transaction_completion.zig");
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(2), .root_generation = 0, .group_id = 2, .topology_epoch = 3 };
    const id: [16]u8 = @splat(3);
    for ([_]bool{ false, true }) |legacy| {
        const prepare = try (db_types.TransactionIntentRequest{ .completion_ordinary_producer = !legacy, .completion_legacy_producer = legacy, .completion_owner = owner, .writes = &.{.{ .key = "row", .value = "{}" }} }).completionBatch(id, 100, owner.topology_epoch);
        const input = try completionPrepareInput(a, prepare);
        defer a.free(input);
        const members = [_]completion.Participant{.{ .namespace = owner.namespace, .owner_fence = owner.digest(), .input_digest = completion.preparedInputDigest(owner.namespace, input), .roles = completion.Roles.effects, .locator = "table2:00000004:docs:2", .routed_owner = owner }};
        const admission: completion.Admission = .{ .version = 3, .transaction_id = id, .admission_digest = @splat(4), .coordinator_namespace = owner.namespace, .coordinator_fence = owner.digest(), .participants = &members };
        const raw = try completion.encodeAdmission(a, admission);
        defer a.free(raw);
        const begin: db_types.BatchRequest = .{ .sync_level = .write, .transaction = .{ .begin = .{ .ordinary_producer = !legacy, .legacy_producer = legacy, .txn_id = id, .begin_timestamp = 100, .created_at_ns = 101, .topology_epoch = owner.topology_epoch, .participants = &.{members[0].locator}, .completion_admission = raw, .completion_owner = owner } } };
        for ([_]db_types.BatchRequest{ begin, prepare }, [_][]const u8{ if (legacy) "begin_legacy_completion_v1" else "begin_ordinary_completion_v1", if (legacy) "prepare_legacy_completion_v1" else "prepare_ordinary_completion_v1" }, [_][]const u8{ "begin_completion_v1", "prepare_completion_v1" }) |request, tag, old_tag| {
            const encoded = try encodeBatchRequest(a, request);
            defer a.free(encoded);
            var decoded = try parseInternalBatchRequest(a, encoded);
            defer decoded.deinit(a);
            const labelled = switch (decoded.req.transaction.?) {
                .begin => |value| if (legacy) value.legacy_producer else value.ordinary_producer,
                .prepare => |value| if (legacy) value.legacy_producer else value.ordinary_producer,
                else => false,
            };
            try std.testing.expect(labelled);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, encoded));
            const downgraded = try std.mem.replaceOwned(u8, a, encoded, tag, old_tag);
            defer a.free(downgraded);
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
            const stripped = try std.mem.replaceOwned(u8, a, encoded, if (legacy) ",\"legacy_producer\":true" else ",\"ordinary_producer\":true", "");
            defer a.free(stripped);
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, stripped));
            const false_marker = try std.mem.replaceOwned(u8, a, encoded, if (legacy) "\"legacy_producer\":true" else "\"ordinary_producer\":true", if (legacy) "\"legacy_producer\":false" else "\"ordinary_producer\":false");
            defer a.free(false_marker);
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, false_marker));
            var old = request;
            switch (old.transaction.?) {
                .begin => |*value| {
                    value.ordinary_producer = false;
                    value.legacy_producer = false;
                },
                .prepare => |*value| {
                    value.ordinary_producer = false;
                    value.legacy_producer = false;
                },
                else => unreachable,
            }
            const old_encoded = try encodeBatchRequest(a, old);
            defer a.free(old_encoded);
            var old_decoded = try parseInternalBatchRequest(a, old_encoded);
            defer old_decoded.deinit(a);
            const upgraded = switch (old_decoded.req.transaction.?) {
                .begin => |value| value.ordinary_producer or value.legacy_producer,
                .prepare => |value| value.ordinary_producer or value.legacy_producer,
                else => true,
            };
            try std.testing.expect(!upgraded);
        }
        var mixed = prepare;
        mixed.transaction.?.prepare.source_purpose = @splat(5);
        try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, mixed));
        var dual = prepare;
        dual.transaction.?.prepare.ordinary_producer = true;
        dual.transaction.?.prepare.legacy_producer = true;
        try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, dual));
    }
}

test "completion begin wire row producer envelope retains explicit labels without upgrading absent origin" {
    const a = std.testing.allocator;
    const Producer = @import("../storage/source_producer_policy.zig").Producer;
    for ([_]Producer{ .ordinary, .legacy_cdc }) |producer| {
        const request: db_types.BatchRequest = .{ .source_producer = producer, .document_version_scheme = .revision_v1, .timestamp_ns = 100, .writes = &.{.{ .key = "row", .value = "{\"v\":9007199254740993}" }} };
        const encoded = try encodeBatchRequest(a, request);
        defer a.free(encoded);
        var decoded = try parseInternalBatchRequest(a, encoded);
        defer decoded.deinit(a);
        try std.testing.expectEqual(producer, decoded.req.source_producer);
        try std.testing.expectEqualStrings(request.writes[0].value, decoded.req.writes[0].value);
        try std.testing.expectEqual(db_types.DocumentVersionScheme.revision_v1, decoded.req.document_version_scheme);
        try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, encoded));
        const forwarded = try encodeBatchRequest(a, decoded.req);
        defer a.free(forwarded);
        try std.testing.expectEqualStrings(encoded, forwarded);
        var old = request;
        old.source_producer = .unlabelled;
        const old_bytes = try encodeBatchRequest(a, old);
        defer a.free(old_bytes);
        var old_decoded = try parseInternalBatchRequest(a, old_bytes);
        defer old_decoded.deinit(a);
        try std.testing.expectEqual(Producer.unlabelled, old_decoded.req.source_producer);
        var forged = request;
        forged.source_producer = .scoped_source;
        try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, forged));
    }
    for ([_][]const u8{
        "[\"source-producer-batch-v1\",\"unlabelled\",{}]",
        "[\"source-producer-batch-v1\",\"scoped_source\",{}]",
        "[\"source-producer-batch-v1\",null,{}]",
        "[\"source-producer-batch-v1\",\"ordinary\",{} ,{}]",
        "[\"source-producer-batch-v1\",\"ordinary\",[\"source-producer-batch-v1\",\"legacy_cdc\",{}]]",
    }) |invalid| try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, invalid));
    var public = try parseBatchRequest(a, "{\"source_producer\":\"ordinary\",\"inserts\":{\"row\":{}}}");
    defer public.deinit(a);
    try std.testing.expectEqual(Producer.unlabelled, public.req.source_producer);
}

test "completion begin wire nonhistory producer phases cannot downgrade or acquire completion authority" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |legacy| {
        const begin: db_types.BatchRequest = .{ .transaction = .{ .begin = .{ .txn_id = @splat(17), .begin_timestamp = 100, .created_at_ns = 101, .topology_epoch = 3, .participants = &.{"table2:00000004:docs:2"}, .ordinary_producer = !legacy, .legacy_producer = legacy } } };
        var scoped = begin;
        scoped.restore_staging_scope = @splat(21);
        scoped.restore_staging_plan_id = @splat(22);
        const scoped_bytes = try encodeBatchRequest(a, scoped);
        defer a.free(scoped_bytes);
        var scoped_decoded = try parseInternalBatchRequest(a, scoped_bytes);
        defer scoped_decoded.deinit(a);
        try std.testing.expectEqualDeep(scoped.restore_staging_scope, scoped_decoded.req.restore_staging_scope);
        try std.testing.expectEqualDeep(scoped.restore_staging_plan_id, scoped_decoded.req.restore_staging_plan_id);
        var invalid_control = begin;
        invalid_control.deletes = &.{"row"};
        try std.testing.expectError(error.InvalidBatchRequest, encodeBatchRequest(a, invalid_control));
        const prepare: db_types.BatchRequest = .{ .writes = &.{.{ .key = "row", .value = "{}" }}, .transaction = .{ .prepare = .{ .txn_id = @splat(17), .topology_epoch = 3, .ordinary_producer = !legacy, .legacy_producer = legacy } } };
        for ([_]db_types.BatchRequest{ begin, prepare }, [_][]const u8{ if (legacy) "begin_legacy_v1" else "begin_ordinary_v1", if (legacy) "prepare_legacy_v1" else "prepare_ordinary_v1" }, [_][]const u8{ "begin", "prepare" }) |request, tag, old_tag| {
            const bytes = try encodeBatchRequest(a, request);
            defer a.free(bytes);
            var decoded = try parseInternalBatchRequest(a, bytes);
            defer decoded.deinit(a);
            try std.testing.expectEqualDeep(request.transaction, decoded.req.transaction);
            try std.testing.expectError(error.InvalidBatchRequest, parseBatchRequest(a, bytes));
            const downgraded = try std.mem.replaceOwned(u8, a, bytes, tag, old_tag);
            defer a.free(downgraded);
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, downgraded));
            const stripped = try std.mem.replaceOwned(u8, a, bytes, if (legacy) ",\"legacy_producer\":true" else ",\"ordinary_producer\":true", "");
            defer a.free(stripped);
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, stripped));
            const completion_tag = if (std.mem.eql(u8, old_tag, "begin")) (if (legacy) "begin_legacy_completion_v1" else "begin_ordinary_completion_v1") else (if (legacy) "prepare_legacy_completion_v1" else "prepare_ordinary_completion_v1");
            const promoted = try std.mem.replaceOwned(u8, a, bytes, tag, completion_tag);
            defer a.free(promoted);
            try std.testing.expectError(error.InvalidBatchRequest, parseInternalBatchRequest(a, promoted));
        }
    }
}
