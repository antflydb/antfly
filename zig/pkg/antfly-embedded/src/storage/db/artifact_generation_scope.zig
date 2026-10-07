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

//! Physical generation namespaces. The shared append/publish/retire machinery
//! does not grant semantic producer authority or interpret output payloads.
const std = @import("std");
const keys = @import("../internal_keys.zig");
const chunks = @import("artifact_chunk_manifest.zig");

pub const Family = enum { chunks, extraction };

pub fn extractionKeyAlloc(alloc: std.mem.Allocator, document: []const u8, producer: []const u8) ![]u8 {
    const scope = try chunks.keyAlloc(alloc, document, producer);
    scope[keys.findComponentTerminator(scope, 1).? + 2] = keys.extraction_stream_manifest_kind;
    return scope;
}

fn rootKind(key: []const u8, kind: u8) bool {
    if (!chunks.isScopeKey(key, kind)) return false;
    const start = keys.findComponentTerminator(key, 1).? + 3;
    return keys.findComponentTerminator(key, start).? + 2 == key.len;
}

pub fn family(scope: []const u8) ?Family {
    if (chunks.isKey(scope)) return .chunks;
    if (rootKind(scope, keys.extraction_stream_manifest_kind)) return .extraction;
    return null;
}

pub fn isKey(scope: []const u8) bool {
    return family(scope) != null;
}

pub fn isHead(key: []const u8) bool {
    return chunks.isScopeKey(key, keys.producer_generation_head_kind) or rootKind(key, keys.extraction_generation_head_kind);
}

/// A visibility change invalidates its family-specific inventory, never a
/// sibling chunk/extraction scope that happens to share a producer name.
pub fn manifestKindForHead(key: []const u8) !u8 {
    if (chunks.isScopeKey(key, keys.producer_generation_head_kind)) return keys.producer_stream_manifest_kind;
    if (rootKind(key, keys.extraction_generation_head_kind)) return keys.extraction_stream_manifest_kind;
    return error.InvalidBatchRequest;
}

/// Existing lifecycle callers specify the chunk role; map it to the selected
/// namespace. Scope digest and incarnation clocks remain family-specific.
pub fn physicalKind(scope: []const u8, role: u8) !u8 {
    const selected = family(scope) orelse return error.InvalidBatchRequest;
    const extraction: u8 = switch (role) {
        keys.producer_generation_row_kind => keys.extraction_generation_row_kind,
        keys.producer_generation_head_kind => keys.extraction_generation_head_kind,
        keys.producer_generation_state_kind => keys.extraction_generation_state_kind,
        keys.producer_generation_clock_kind => keys.extraction_generation_clock_kind,
        else => return error.InvalidBatchRequest,
    };
    return if (selected == .chunks) role else extraction;
}

test "ordered artifact inventory extraction generation namespaces cannot alias chunk scopes" {
    _ = @import("artifact_extraction_generation.zig");
    const alloc = std.testing.allocator;
    const root = try extractionKeyAlloc(alloc, "doc\x00\xff", "producer\x80");
    defer alloc.free(root);
    const chunk = try chunks.keyAlloc(alloc, "doc\x00\xff", "producer\x80");
    defer alloc.free(chunk);
    try std.testing.expectEqual(Family.extraction, family(root).?);
    try std.testing.expectEqual(Family.chunks, family(chunk).?);
    try std.testing.expect(!std.mem.eql(u8, root, chunk));
    const kind = keys.findComponentTerminator(root, 1).? + 2;
    root[kind] = try physicalKind(root, keys.producer_generation_head_kind);
    try std.testing.expect(isHead(root));
    try std.testing.expectEqual(keys.extraction_stream_manifest_kind, try manifestKindForHead(root));
    try std.testing.expect(@import("artifact_publication.zig").guardedArtifactKey(root));
    try std.testing.expect(@import("artifact_publication.zig").requiresOrderedMaterialization(root));
    try std.testing.expectEqual(@import("../artifact_footprint.zig").Family.generated, @import("../artifact_footprint.zig").classify(root).?);
    const nested = try chunks.scopedKeyAlloc(alloc, "doc", "producer", "unit");
    defer alloc.free(nested);
    nested[keys.findComponentTerminator(nested, 1).? + 2] = keys.extraction_generation_head_kind;
    try std.testing.expect(!isHead(nested));
    nested[keys.findComponentTerminator(nested, 1).? + 2] = keys.extraction_stream_manifest_kind;
    try std.testing.expect(!isKey(nested));
}

test "ordered artifact inventory extraction generations share resumable lifecycle without chunk visibility" {
    const alloc = std.testing.allocator;
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const generations = @import("artifact_chunk_generation.zig");
    const publication = @import("artifact_publication.zig");
    const Guard = struct {
        pub fn validate(_: @This(), _: anytype) !void {}
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/extraction-generation", .{tmp.sub_path});
    defer alloc.free(path);
    const options: db_mod.OpenOptions = .{ .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    const scope = try extractionKeyAlloc(alloc, "doc\x00", "shared\xff");
    defer alloc.free(scope);
    const sibling = try chunks.keyAlloc(alloc, "doc\x00", "shared\xff");
    defer alloc.free(sibling);
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 1, .catalog_digest = @splat(2) };
    var builder = chunks.Builder.init();
    try builder.append(0, "first named output");
    try builder.append(1, "second named output");
    const spec = try generations.Spec.init(authority, scope, @splat(3), builder.finish(), 1);
    var selected = try generations.Plan.init(alloc, scope, spec);
    defer selected.deinit();
    try std.testing.expectError(error.InvalidBatchRequest, generations.Plan.init(alloc, sibling, spec));
    {
        var db = try db_mod.DB.open(alloc, path, options);
        defer db.close();
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try @import("../source_authority.zig").bind(&txn, .native, authority.namespace);
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        _ = try selected.begin(&txn);
        var page = try generations.PreparedAppend.init(alloc, &selected, try selected.load(&txn), &.{"first named output"});
        defer page.deinit();
        _ = try page.stage(&selected, &txn);
        try std.testing.expectError(error.ArtifactPublicationPending, selected.publish(&txn, null, Guard{}));
        try txn.commit();
    }
    var db = try db_mod.DB.open(alloc, path, options);
    defer db.close();
    const old = read: {
        var txn = try db.core.store.beginReadTxn();
        defer txn.abort();
        try std.testing.expectEqual(@as(u64, 2), try generations.proposeIncarnation(alloc, &txn, authority, scope));
        try std.testing.expectEqual(@as(u64, 1), try generations.proposeIncarnation(alloc, &txn, authority, sibling));
        break :read try selected.load(&txn);
    };
    const Check = struct {
        fn run(a: std.mem.Allocator, plan: *const generations.Plan, previous: generations.State) !void {
            var page = try generations.PreparedAppend.init(a, plan, previous, &.{"second named output"});
            defer page.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{ &selected, old });
    var page = try generations.PreparedAppend.init(alloc, &selected, old, &.{"second named output"});
    defer page.deinit();
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expect(try page.stage(&selected, &txn));
        try std.testing.expect(!try page.stage(&selected, &txn));
        _ = try selected.publish(&txn, null, Guard{});
        try txn.commit();
    }
    const View = generations.View(@import("../docstore.zig").DocStore.Txn);
    var pinned = try db.core.store.beginReadTxn();
    defer pinned.abort();
    var view = (try View.open(alloc, &pinned, scope)).?;
    defer view.deinit();
    try std.testing.expect(try View.open(alloc, &pinned, sibling) == null);
    var cursor = try view.openCursor(alloc, 0);
    defer cursor.close();
    try std.testing.expectEqualStrings("first named output", (try cursor.next()).?.value);
    try std.testing.expectEqualStrings("second named output", (try cursor.next()).?.value);
    try std.testing.expect(try cursor.next() == null);
    var recovery = try @import("artifact_generation_recovery.zig").discover(alloc, db.core.store, scope, null, .{});
    defer recovery.deinit();
    try std.testing.expectEqualDeep(spec.id(), recovery.selected.?);
    try std.testing.expectEqual(@as(usize, 1), recovery.states.len);
    var unrelated = try @import("artifact_generation_recovery.zig").discover(alloc, db.core.store, sibling, null, .{});
    defer unrelated.deinit();
    try std.testing.expectEqual(@as(usize, 0), unrelated.states.len);
    // Empty replacement publishes before bounded old-generation retirement;
    // a pinned old reader continues to see its complete immutable payloads.
    var replacement = try generations.Plan.init(alloc, scope, try generations.Spec.init(authority, scope, @splat(4), chunks.Builder.init().finish(), 2));
    defer replacement.deinit();
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectError(error.ArtifactPublicationPending, selected.retire(&txn, authority, Guard{}));
        _ = try replacement.begin(&txn);
        _ = try replacement.publish(&txn, spec.id(), Guard{});
        _ = try selected.retire(&txn, authority, Guard{});
        try txn.commit();
    }
    for (0..2) |_| {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        var gc = try generations.PreparedRetirement.init(alloc, &selected, authority, try selected.load(&txn), 1);
        defer gc.deinit();
        _ = try gc.stage(&selected, &txn);
        try txn.commit();
    }
    try std.testing.expectEqualStrings("first named output", (try view.get(alloc, 0)).?);
    var current = try db.core.store.beginReadTxn();
    defer current.abort();
    var empty = (try View.open(alloc, &current, scope)).?;
    defer empty.deinit();
    try std.testing.expect(try empty.get(alloc, 0) == null);
    try std.testing.expectError(error.NotFound, selected.load(&current));
}
