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
const keys = @import("../../internal_keys.zig");
const extraction = @import("extraction_generation.zig");

test "resolver curation artifact inputs preserve physical families and generation guards" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const Reader = struct {
        key: []const u8,
        value: ?[]const u8 = null,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return if (std.mem.eql(u8, self.key, key)) self.value orelse error.NotFound else error.NotFound;
        }
    };
    const unit = try keys.documentUnitArtifactKeyAlloc(scratch, "doc\x00\xff", "extraction", "unit");
    const physical = [_][]const u8{
        try keys.resolutionArtifactKeyAlloc(scratch, "doc", "resolved"),
        try keys.resolutionArtifactKeyAlloc(scratch, unit, "resolved"),
        try keys.artifactNamedPrefixAlloc(scratch, "doc", "summary", "summary"),
        try keys.artifactNamedPrefixAlloc(scratch, "doc", "embedding", "vector"),
        try extraction.headKeyAlloc(scratch, "doc", "extraction"),
    };
    for (physical) |key| {
        var reader: Reader = .{ .key = key };
        var missing = try extraction.captureInput(a, &reader, key);
        defer missing.deinit();
        try std.testing.expect(missing.value == null);
        try std.testing.expect(missing.head_key == null);
        reader.value = "physical value";
        var present = try extraction.captureInput(a, &reader, key);
        defer present.deinit();
        try std.testing.expectEqualStrings("physical value", present.value.?);
        try std.testing.expectEqualStrings(key, present.proofKey(key));
        try std.testing.expect(present.head_key == null);
        try std.testing.expect(!present.require_absence_proof);
    }
    for ([_][]const u8{ try keys.artifactNamedPrefixAlloc(scratch, "doc", "asset", "extraction"), unit }) |key| {
        var reader: Reader = .{ .key = key, .value = "legacy extraction" };
        var input = try extraction.captureInput(a, &reader, key);
        defer input.deinit();
        try std.testing.expectEqualStrings("legacy extraction", input.value.?);
        try std.testing.expect(input.head_key != null);
        try std.testing.expect(input.require_absence_proof);
    }
    const chunk = try keys.chunkArtifactKeyAlloc(scratch, "doc", "chunks", 0);
    var reader: Reader = .{ .key = chunk, .value = "legacy chunk" };
    var input = try extraction.captureInput(a, &reader, chunk);
    defer input.deinit();
    try std.testing.expectEqualStrings("legacy chunk", input.value.?);
    try std.testing.expect(input.head_key != null);
}

test "resolver curation ordered output reads retain absence and value preconditions" {
    const DB = @import("../selected_root.zig").db.DB;
    const Token = @import("producer_context.zig").Token;
    const runtime = @import("../resolution_runtime.zig");
    const publication = @import("publication_resolution.zig");
    const a = std.testing.allocator;
    var tmp = try @import("../../../common/test_directory.zig").TestDirectory.init("resolver-output-read");
    defer tmp.cleanup();
    var db = try DB.open(a, tmp.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    var namespace: [24]u8 = undefined;
    @import("../doc_identity.zig").encodeNamespace(&namespace, db.core.identity_namespace);
    const Scope = struct {
        fn init(alloc: std.mem.Allocator, ns: [24]u8) Token {
            return .{ .arena = std.heap.ArenaAllocator.init(alloc), .namespace = ns, .epoch = 1, .catalog_digest = @splat(2), .producer_kind = .resolver, .producer_name = "concepts", .producer_generation = 1, .artifact_name = "resolved", .source = .{ .document_key = "doc", .content_digest = @splat(3), .timestamp = 1, .input_position = null } };
        }
    };
    const output = try keys.resolutionArtifactKeyAlloc(a, "doc", "resolved");
    defer a.free(output);
    var fallback: runtime.DbArtifactStore(@TypeOf(db.core.store.runtime_store)) = .{ .store = &db.core.store.runtime_store };
    var absent = Scope.init(a, namespace);
    defer absent.deinit();
    var recorder: publication.RecordingStore(@TypeOf(db.core.store.*)) = .{ .store = db.core.store, .fallback = fallback.artifactStore(), .token = &absent, .output_key = output, .upstream_key = "unused", .control_key = "unused" };
    try std.testing.expect(try recorder.artifactStore().get(a, output) == null);
    try std.testing.expectEqual(@as(usize, 0), absent.reads.items.len);
    try std.testing.expectEqual(@as(usize, 1), absent.preconditions.items.len);
    try std.testing.expect(absent.preconditions.items[0].content_digest == null);
    try db.core.store.put(output, "prior resolution");
    try std.testing.expectError(error.EnrichmentSourceChanged, recorder.artifactStore().get(a, output));
    var present = Scope.init(a, namespace);
    defer present.deinit();
    recorder.token = &present;
    const observed = (try recorder.artifactStore().get(a, output)).?;
    defer a.free(observed);
    try std.testing.expectEqualStrings("prior resolution", observed);
    try std.testing.expectEqual(@as(usize, 0), present.reads.items.len);
    try std.testing.expectEqual(@as(usize, 1), present.preconditions.items.len);
    try std.testing.expect(present.preconditions.items[0].content_digest != null);
    try db.core.store.put(output, "newer resolution");
    try std.testing.expectError(error.EnrichmentSourceChanged, recorder.artifactStore().get(a, output));
}

test "resolver curation ordered publication wakes detached replay after its durable receipt" {
    const DB = @import("../selected_root.zig").db.DB;
    const inventory = @import("inventory.zig");
    const publication = @import("publication.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var tmp = try @import("../../../common/test_directory.zig").TestDirectory.init("resolver-publication-wake");
    defer tmp.cleanup();
    var db = try DB.open(a, tmp.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .online_source_authority = .raft, .executor = .{ .backend = .manual }, .start_optional_runtimes = false, .start_index_workers = false, .start_resolver_workers = false, .start_independent_maintenance = false });
    defer db.close();
    try db.addResolver(.{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{uslug _entity.text}}", .promote = false, .config_generation = 1 });
    try db.applyOrderedCommittedMutation(.{ .writes = &.{.{ .key = "doc", .value = "{}" }}, .sync_level = .write }, .{ .term = 1, .index = 1 }, false);
    const source_key = try keys.artifactNamedPrefixAlloc(scratch, "doc", "asset", "kg");
    try db.core.store.put(source_key, "{}");
    if (db.resolution_runtime == null) try db.initResolutionRuntime();
    if (db.promotion_runtime == null) try db.initPromotionRuntime();
    var catalog = try db.artifactInventoryCommand(scratch);
    catalog.binding.effect_protocol = 15;
    {
        var write = try db.core.store.beginWriteTxn();
        errdefer write.abort();
        try inventory.stageOrdered(scratch, &write, catalog, 1);
        try publication.stageAuthority(&write, .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try write.commit();
    }
    const source = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        break :blk try publication.capturePrimarySource(scratch, &read, catalog.namespace, "doc");
    };
    var digest: publication.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash("{}", &digest, .{});
    const output_key = try keys.resolutionArtifactKeyAlloc(scratch, "doc", "resolved");
    var command: publication.Command = .{ .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_kind = .resolver, .producer_name = "concepts", .producer_generation = 1, .producer_artifact_name = "resolved", .sources = (&source)[0..1], .artifact_sources = &.{.{ .key = source_key, .content_digest = digest, .input_position = null, .source_index = 0 }}, .mutations = &.{.{ .family = .resolution, .key = output_key, .value = "{\"config_generation\":1,\"entities\":[]}", .source_index = 0 }}, .publication_digest = @splat(0) };
    command.publication_digest = command.digest();
    const before = db.core.store.lastReplaySequence(0);
    try db.applyOrderedCommittedMutation(.{ .artifact_publication = command }, .{ .term = 1, .index = 2 }, false);
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    const receipt = (try publication.readReceipt(&read, command, source)).?;
    try std.testing.expect(receipt.sequence > before);
    try std.testing.expectEqualStrings(command.mutations[0].value.?, try read.get(output_key));
    try std.testing.expectEqual(receipt.sequence, db.resolution_runtime.?.stats().target_sequence);
    try std.testing.expectEqual(receipt.sequence, db.promotion_runtime.?.stats().target_sequence);
}

test "resolver curation catalog protocol follows producer definitions and retains extensions" {
    const DB = @import("../selected_root.zig").db.DB;
    const inventory = @import("inventory.zig");
    const reader = @import("../catalog/resolver_catalog.zig");
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var tmp = try @import("../../../common/test_directory.zig").TestDirectory.init("resolver-effect-protocol");
    defer tmp.cleanup();
    var db = try DB.open(a, tmp.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .start_index_workers = false, .start_optional_runtimes = false, .start_independent_maintenance = false });
    defer db.close();
    try std.testing.expectEqual(@as(u16, 14), (try db.artifactInventoryCommand(scratch)).binding.effect_protocol);
    try db.addResolver(.{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "concept/{{uslug _entity.text}}", .promote = false, .config_generation = 1 });
    try std.testing.expectEqual(@as(u16, 15), (try db.artifactInventoryCommand(scratch)).binding.effect_protocol);
    // Namespace-reader compatibility does not introduce grouped producers.
    try db.core.store.put(reader.minimum_reader_key, try reader.readerVersionBytes(reader.namespace_curation_reader_version));
    db.core.index_manager.minimum_reader_version.store(reader.namespace_curation_reader_version, .release);
    var extended = try db.artifactInventoryCommand(scratch);
    try std.testing.expectEqual(@as(u16, 15), extended.binding.effect_protocol);
    extended.binding.effect_protocol = 16;
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try inventory.stageOrdered(scratch, &txn, extended, 1);
        try txn.commit();
    }
    try db.addResolver(.{ .name = "other", .table = "entities", .source_artifact = "other_source", .resolution_artifact = "other_resolution", .key_template = "other/{{uslug _entity.text}}", .promote = false, .config_generation = 1 });
    try std.testing.expectEqual(@as(u16, 16), (try db.artifactInventoryCommand(scratch)).binding.effect_protocol);
    const producers = @import("../catalog/enrichment_catalog.zig");
    for ([_][]const u8{ "", "{\"group_by\":\"thread\",\"order_by\":\"ordinal\",\"field\":\"text\"}" }, 0..) |group_json, i| {
        const encoded = try producers.serializeCatalog(scratch, &.{.{ .name = "claims", .kind = .asset, .source_field = "body", .group_source_json = group_json }});
        const catalogs: inventory.Catalogs = .{ .enrichments = encoded };
        try std.testing.expectEqual(@as(u16, if (i == 0) 15 else 16), try catalogs.requiredEffectProtocol(a));
    }
}
