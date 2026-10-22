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

//! Canonical mention-edge projection shared by ordered and local publication.
const std = @import("std");
const resolver = @import("antfly_resolver");
const catalog = @import("catalog/resolver_catalog.zig");
const types = @import("graph_edge_types.zig");
const keys = @import("../internal_keys.zig");

pub fn writesAlloc(a: std.mem.Allocator, index: []const u8, document: []const u8, raw: []const u8, extraction: ?[]const u8, edge_type: []const u8, config: *const catalog.ResolverConfig, evidence_artifacts: bool) ![]types.GraphEdgeWrite {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var resolved = resolver.parseResolution(scratch, raw) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return a.alloc(types.GraphEdgeWrite, 0),
    };
    defer resolved.deinit();
    var extracted = if (extraction) |value| resolver.parseExtractionEntities(scratch, value) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => null,
    } else null;
    defer if (extracted) |*value| value.deinit();
    var confidence: std.StringHashMapUnmanaged(f64) = .empty;
    if (extracted) |value| for (value.entities) |entity| {
        const stored = try confidence.getOrPut(scratch, entity.local_id);
        if (!stored.found_existing) stored.value_ptr.* = entity.confidence;
    };
    const Group = struct { table: []const u8, key: []const u8, confidence: f64 = 0, count: usize = 0, mentions: std.ArrayList([]const u8) = .empty };
    var groups: std.ArrayList(Group) = .empty;
    var positions: std.StringHashMapUnmanaged(usize) = .empty;
    for (resolved.entities) |entity| {
        if ((entity.decision != .new and entity.decision != .match) or entity.doc_ref.key.len == 0) continue;
        const identity = try std.json.Stringify.valueAlloc(scratch, .{ entity.doc_ref.table, entity.doc_ref.key }, .{});
        const position = try positions.getOrPut(scratch, identity);
        if (!position.found_existing) {
            position.value_ptr.* = groups.items.len;
            try groups.append(scratch, .{ .table = entity.doc_ref.table, .key = entity.doc_ref.key });
        }
        const group = &groups.items[position.value_ptr.*];
        group.confidence = @max(group.confidence, confidence.get(entity.local_id) orelse entity.confidence);
        group.count += 1;
        if (evidence_artifacts) {
            const name = try std.fmt.allocPrint(scratch, "_resolution_mention\x1f{s}\x1f{s}\x1f{s}", .{ config.source_artifact, config.resolution_artifact, entity.local_id });
            try group.mentions.append(scratch, try keys.artifactNamedPrefixAlloc(scratch, document, "asset", name));
        }
    }
    const writes = try a.alloc(types.GraphEdgeWrite, groups.items.len);
    var initialized: usize = 0;
    errdefer {
        for (writes[0..initialized]) |*write| write.deinit(a);
        a.free(writes);
    }
    for (groups.items, 0..) |group, i| {
        const metadata = if (evidence_artifacts)
            try std.json.Stringify.valueAlloc(scratch, .{ .target_table = group.table, .mention_count = group.count, .mention_artifact_keys = group.mentions.items }, .{})
        else
            // Ordered publication authenticates the resolution directly. Do
            // not advertise separate evidence rows which it never published.
            try std.json.Stringify.valueAlloc(scratch, .{ .target_table = group.table, .mention_count = group.count, .source_artifact = config.source_artifact, .resolution_artifact = config.resolution_artifact }, .{});
        writes[i] = try (types.GraphEdgeWrite{ .index_name = index, .source = document, .target = group.key, .edge_type = edge_type, .weight = config.fusedMentionWeight(group.confidence), .metadata_json = metadata }).cloneAlloc(a);
        initialized += 1;
    }
    return writes;
}

test "mention projection preserves canonical targets confidence and evidence ownership" {
    const Probe = struct {
        fn run(a: std.mem.Allocator) !void {
            const config: catalog.ResolverConfig = .{ .name = "concepts", .table = "entities", .source_artifact = "kg", .resolution_artifact = "resolved", .key_template = "{{text}}" };
            const raw = "{\"config_generation\":1,\"entities\":[{\"local_id\":\"a\",\"doc_ref\":{\"table\":\"entities\",\"key\":\"canonical\"},\"confidence\":0.4,\"decision\":\"match\"},{\"local_id\":\"b\",\"doc_ref\":{\"table\":\"entities\",\"key\":\"canonical\"},\"confidence\":0.8,\"decision\":\"new\"},{\"local_id\":\"c\",\"doc_ref\":{\"table\":\"entities\",\"key\":\"hidden\"},\"confidence\":1,\"decision\":\"suppressed\"}]}";
            for ([_]bool{ false, true }) |evidence| {
                const writes = try writesAlloc(a, "knowledge", "note", raw, null, "mentions", &config, evidence);
                defer a.free(writes);
                defer for (writes) |*write| write.deinit(a);
                try std.testing.expectEqual(@as(usize, 1), writes.len);
                try std.testing.expectEqualStrings("canonical", writes[0].target);
                try std.testing.expectApproxEqAbs(config.fusedMentionWeight(0.8), writes[0].weight, 0.00001);
                var metadata = try std.json.parseFromSlice(std.json.Value, a, writes[0].metadata_json, .{});
                defer metadata.deinit();
                try std.testing.expectEqual(@as(i64, 2), metadata.value.object.get("mention_count").?.integer);
                try std.testing.expectEqual(evidence, metadata.value.object.contains("mention_artifact_keys"));
                try std.testing.expectEqualStrings("entities", metadata.value.object.get("target_table").?.string);
            }
        }
    };
    try Probe.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}
