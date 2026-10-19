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
const keys = @import("../internal_keys.zig");
const extraction = @import("artifact_extraction_generation.zig");

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
    const DB = @import("selected_root.zig").db.DB;
    const Token = @import("artifact_producer_context.zig").Token;
    const runtime = @import("resolution_runtime.zig");
    const publication = @import("artifact_publication_resolution.zig");
    const a = std.testing.allocator;
    var tmp = try @import("../../common/test_directory.zig").TestDirectory.init("resolver-output-read");
    defer tmp.cleanup();
    var db = try DB.open(a, tmp.path(), .{ .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    var namespace: [24]u8 = undefined;
    @import("doc_identity.zig").encodeNamespace(&namespace, db.core.identity_namespace);
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
