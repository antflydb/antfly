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
const builtin = @import("builtin");

const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const graph_mod = @import("../../graph/graph.zig");
const paths_mod = @import("../../graph/paths.zig");
const traversal_mod = @import("../../graph/traversal.zig");
const internal_keys = @import("../internal_keys.zig");
const types = @import("types.zig");

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

fn bytesToHexAlloc(alloc: std.mem.Allocator, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len * 2);
    for (bytes, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

test "db graph runtime index materializes relation asset artifacts into graph edge artifacts" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value =
                \\{"relations":{"relations":[{"type":"mentions","target":{"document_id":"doc:b"},"confidence":0.75}]}}
                ,
            },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(artifact_key);
    const raw_artifact = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(raw_artifact);

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expectApproxEqAbs(@as(f64, 0.75), edges[0].weight, 0.0001);
}

test "db graph index materializes unit-derived chunk artifacts into graph edge artifacts" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.addEnrichment(.{
        .name = "document_chunks_v1",
        .kind = .chunk,
        .field = "text",
        .source_artifact_name = "document_units_v1",
        .chunk_size = 64,
    });
    try db.addIndex(.{
        .name = "chunk_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"document_chunks_v1","format":"extraction_relation"},
        \\  "nodes":{"source":"{{ _artifact.value._parent_unit_key }}","target":"{{ _artifact.value.text }}"},
        \\  "edge":{"type":"mentions","metadata":{"unit":"{{ _artifact.value._parent_unit_id }}","artifact":"{{ _artifact.name }}"}}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,ZG9jOmI=\"}",
        }},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const unit_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc:a", "document_units_v1", "document:000001");
    defer alloc.free(unit_key);
    const edges = try db.getEdges(alloc, "chunk_graph", unit_key, "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"unit\":\"document:000001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"artifact\":\"document_chunks_v1\"") != null);
}

test "db graph runtime relation artifact materializer uses mapping templates" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.items[*]","format":"extraction_relation"},
        \\  "nodes":{"source":"{{ _doc.key }}","target":"{{ _item.to }}"},
        \\  "edge":{"type":"{{ _item.rel }}","weight":"{{ default _item.score 1.0 }}","metadata":{"evidence":"{{ _item.evidence }}","ordinal":"{{ _item_index }}","tenant":"{{ _doc.value.tenant_id }}"}},
        \\  "context":{"doc_fields":["tenant_id"]},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value =
                \\{"tenant_id":"tenant-a","relations":{"items":[{"rel":"cites","to":"doc:b","score":0.5,"evidence":"see section 2"}]}}
                ,
            },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "cites", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), edges[0].weight, 0.0001);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"evidence\":\"see section 2\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"ordinal\":\"0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"tenant\":\"tenant-a\"") != null);
}

test "db graph runtime relation artifact materializer resolves entity refs and artifact template values" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_graph"},
        \\  "nodes":{"source":"{{ _doc.key }}","target":"{{ _item.target.doc_ref.key }}"},
        \\  "edge":{"type":"{{ _item.type }}","metadata":{"artifact":"{{ _artifact.name }}","content_type":"{{ _artifact.content_type }}","source_text":"{{ _item.source.text }}","target_text":"{{ _item.target.text }}"}},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value =
                \\{"relations":{"entities":[{"id":"e0","text":"Ada Lovelace","document_id":"doc:a"},{"id":"e1","text":"Analytical Engine","doc_ref":{"key":"doc:b"}}],"relations":[{"type":"mentions","source":{"entity_id":"e0"},"target":{"entity_id":"e1"}}]}}
                ,
            },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"artifact\":\"relations_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"content_type\":\"application/json\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"source_text\":\"Ada Lovelace\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"target_text\":\"Analytical Engine\"") != null);
}

test "db graph runtime relation artifact materializer replaces stale document edges" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"}}]}}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:c\"}}]}}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:c", edges[0].target);

    const stale_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(stale_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_key));
}

test "db graph runtime relation artifact materializer deletes edges when asset source disappears" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"}}]}}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"relations removed\"}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);

    const asset_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(asset_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, asset_key));

    const stale_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(stale_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_key));
}

test "db graph runtime artifact source lifecycle reuses and protects asset enrichments" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph_a",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    const created = (try db.getEnrichment(alloc, .asset, "relations_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = created;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("relations", created.field);
    try std.testing.expectEqualStrings("application/json", created.content_type);

    try db.addIndex(.{
        .name = "relations_graph_b",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try std.testing.expectError(error.EnrichmentInUse, db.deleteEnrichment(.asset, "relations_v1"));
    try std.testing.expect(try db.deleteIndex("relations_graph_a"));
    try std.testing.expectError(error.EnrichmentInUse, db.deleteEnrichment(.asset, "relations_v1"));

    const still_present = (try db.getEnrichment(alloc, .asset, "relations_v1")) orelse return error.TestUnexpectedResult;
    defer {
        var tmp = still_present;
        tmp.deinit(alloc);
    }
    try std.testing.expectEqualStrings("relations", still_present.field);
}

test "db graph runtime artifact source reuses user enrichment and rejects incompatible shorthand" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "relations_v1",
        .kind = .asset,
        .field = "relations",
        .content_type = "application/json",
    });

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"}
        \\}
        ,
    });

    try std.testing.expectError(error.ConflictingEnrichmentConfig, db.addIndex(.{
        .name = "conflicting_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"body","content_type":"application/json"}
        \\}
        ,
    }));
}

test "db graph runtime source artifact deletion clears materialized graph edges" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"}}]}}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    {
        const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, edges);
        try std.testing.expectEqual(@as(usize, 1), edges.len);
    }

    try db.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 0), edges.len);

    const graph_artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(graph_artifact_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, graph_artifact_key));
}

test "db graph runtime artifact edges are visible to graph search queries" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"}}]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"Beta\"}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    var result = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "mentions",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "relations_graph",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"mentions"} },
                    .include_documents = true,
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:b", result.graph_results[0].hits[0].id);
    try std.testing.expect(result.graph_results[0].hits[0].stored_data != null);
}

test "db graph runtime artifact external node targets return ids without document hydration" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "nodes":{"model":"external","target":"{{ _item.target.entity_id }}"},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"tenant\":\"visible\",\"relations\":{\"relations\":[{\"type\":\"mentions\",\"target\":{\"entity_id\":\"entity:person:ada_lovelace\"}}]}}" },
        },
        .sync_level = .enrichments,
    });
    try db.runUntilIdle();

    var result = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "mentions",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "relations_graph",
                    .start_nodes = .{ .keys = &.{"doc:a"} },
                    .params = .{ .direction = .out, .edge_types = &.{"mentions"} },
                    .include_documents = true,
                },
            },
        },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.graph_results.len);
    try std.testing.expectEqual(@as(u32, 1), result.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("entity:person:ada_lovelace", result.graph_results[0].hits[0].id);
    try std.testing.expect(result.graph_results[0].hits[0].stored_data == null);
    try std.testing.expect(result.graph_results[0].hits[0].doc_ordinal == null);

    var reverse = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "mentioned_by",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "relations_graph",
                    .start_nodes = .{ .keys = &.{"entity:person:ada_lovelace"} },
                    .params = .{ .direction = .in, .edge_types = &.{"mentions"} },
                    .include_documents = true,
                },
            },
        },
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        .limit = 10,
    });
    defer reverse.deinit();

    try std.testing.expectEqual(@as(u32, 1), reverse.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:a", reverse.graph_results[0].hits[0].id);
    try std.testing.expect(reverse.graph_results[0].hits[0].stored_data != null);

    var bidirectional = try db.search(alloc, .{
        .graph_queries = &.{
            .{
                .name = "connected",
                .query = .{
                    .query_type = .neighbors,
                    .index_name = "relations_graph",
                    .start_nodes = .{ .keys = &.{"entity:person:ada_lovelace"} },
                    .params = .{ .direction = .both, .edge_types = &.{"mentions"} },
                },
            },
        },
        .filter_query_json = "{\"term\":{\"tenant\":\"visible\"}}",
        .limit = 10,
    });
    defer bidirectional.deinit();

    try std.testing.expectEqual(@as(u32, 1), bidirectional.graph_results[0].total_hits);
    try std.testing.expectEqualStrings("doc:a", bidirectional.graph_results[0].nodes[0].key);
}

test "db graph runtime async asset producer source materializes through replay" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var fake = TestHelpers.TestAssetProducer{};
    var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{
        .enrichment = .{
            .owner_id = "worker-a",
            .asset_producer = fake.producer(),
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json =
        \\{
        \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
        \\  "edge":{"metadata":{"artifact":"{{ _artifact.name }}","content_type":"{{ _artifact.content_type }}"}},
        \\  "artifact":{"name":"relations_v1","kind":"asset","field":"target_doc","content_type":"application/json","producer_json":{"type":"extractor","config":{"provider":"mock"}}}
        \\}
        ,
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"target_doc\":\"doc:b\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();
    try db.runUntilIdle();

    try std.testing.expectEqual(@as(usize, 1), fake.extractor_calls);
    const edges = try db.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"artifact\":\"relations_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, edges[0].metadata, "\"content_type\":\"application/json\"") != null);
}

test "db graph runtime artifact source replay catches up after reopen" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{
            \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
            \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
            \\}
            ,
        });

        const doc_key = try internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(doc_key);
        const artifact_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
        defer alloc.free(artifact_key);
        var ctx = db.batchContext();
        const sequence = db.core.store.reserveNextReplaySequence(1);
        const replay_payload = try @import("mod.zig").DB.derivedAsyncEncodeChangeRecordPayloadForContext(&ctx, .{
            .changed_artifact_keys = &.{artifact_key},
        }, sequence);
        defer alloc.free(replay_payload);

        try db.core.store.putBatchWithReplay(null, &.{
            .{ .key = doc_key, .value = "{\"title\":\"alpha\"}" },
            .{ .key = artifact_key, .value = "{\"relations\":[{\"type\":\"mentions\",\"target\":{\"document_id\":\"doc:b\"}}]}" },
        }, &.{}, .{
            .sequence = sequence,
            .payload = replay_payload,
        });
    }

    var reopened = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    try reopened.runUntilIdle();

    const edges = try reopened.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
}

test "db graph runtime edge artifact replay catches up after reopen" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json = "{}",
        });

        const graph_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
        defer alloc.free(graph_key);
        const graph_value = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, 0.8, 0, 0, "");
        defer alloc.free(graph_value);
        var ctx = db.batchContext();
        const sequence = db.core.store.reserveNextReplaySequence(1);
        const replay_payload = try @import("mod.zig").DB.derivedAsyncEncodeChangeRecordPayloadForContext(&ctx, .{
            .changed_artifact_keys = &.{graph_key},
        }, sequence);
        defer alloc.free(replay_payload);

        try db.core.store.putBatchWithReplay(null, &.{
            .{ .key = graph_key, .value = graph_value },
        }, &.{}, .{
            .sequence = sequence,
            .payload = replay_payload,
        });
    }

    var reopened = try @import("mod.zig").DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    try reopened.runUntilIdle();

    const edges = try reopened.getEdges(alloc, "relations_graph", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
    try std.testing.expectApproxEqAbs(@as(f64, 0.8), edges[0].weight, 0.0001);
}

test "db artifact repair records corrupt graph edge artifacts during replay" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;
    const artifact_key = blk: {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{
            \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
            \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
            \\}
            ,
        });

        const key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
        errdefer alloc.free(key);
        var ctx = db.batchContext();
        const sequence = db.core.store.reserveNextReplaySequence(1);
        const replay_payload = try DB.derivedAsyncEncodeChangeRecordPayloadForContext(&ctx, .{
            .changed_artifact_keys = &.{key},
        }, sequence);
        defer alloc.free(replay_payload);
        try db.core.store.putBatchWithReplay(null, &.{
            .{ .key = key, .value = "bad-graph-artifact" },
        }, &.{}, .{
            .sequence = sequence,
            .payload = replay_payload,
        });
        appended_sequence = sequence;
        break :blk key;
    };
    defer alloc.free(artifact_key);

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const issues = try reopened.listArtifactRepairIssues(alloc, .graph, "relations_graph", 0);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.graph, issues[0].artifact_kind);
    try std.testing.expectEqual(.corrupt_artifact, issues[0].reason);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("mentions:doc:b", issues[0].artifact_name);
    const artifact_key_hex = try bytesToHexAlloc(alloc, artifact_key);
    defer alloc.free(artifact_key_hex);
    try std.testing.expectEqualStrings(artifact_key_hex, issues[0].artifact_key);

    const raw_artifact = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(raw_artifact);
    try std.testing.expectEqualStrings("bad-graph-artifact", raw_artifact);

    const graph_applied = try reopened.core.loadAppliedSequence(alloc, "relations_graph");
    try std.testing.expectEqual(@as(u64, 0), graph_applied);
    try std.testing.expect(appended_sequence > graph_applied);
}

test "db artifact repair records corrupt graph source asset artifacts during replay" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var appended_sequence: u64 = 0;
    const artifact_key = blk: {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();

        try db.addIndex(.{
            .name = "relations_graph",
            .kind = .graph,
            .config_json =
            \\{
            \\  "source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},
            \\  "artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json"}
            \\}
            ,
        });

        const key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
        errdefer alloc.free(key);
        var ctx = db.batchContext();
        const sequence = db.core.store.reserveNextReplaySequence(1);
        const replay_payload = try DB.derivedAsyncEncodeChangeRecordPayloadForContext(&ctx, .{
            .changed_artifact_keys = &.{key},
        }, sequence);
        defer alloc.free(replay_payload);
        try db.core.store.putBatchWithReplay(null, &.{
            .{ .key = key, .value = "bad-json" },
        }, &.{}, .{
            .sequence = sequence,
            .payload = replay_payload,
        });
        appended_sequence = sequence;
        break :blk key;
    };
    defer alloc.free(artifact_key);

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const issues = try reopened.listArtifactRepairIssues(alloc, .asset, "relations_graph", 0);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.asset, issues[0].artifact_kind);
    try std.testing.expectEqual(.corrupt_artifact, issues[0].reason);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("relations_v1", issues[0].artifact_name);
    const artifact_key_hex = try bytesToHexAlloc(alloc, artifact_key);
    defer alloc.free(artifact_key_hex);
    try std.testing.expectEqualStrings(artifact_key_hex, issues[0].artifact_key);

    const raw_artifact = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(raw_artifact);
    try std.testing.expectEqualStrings("bad-json", raw_artifact);

    const graph_applied = try reopened.core.loadAppliedSequence(alloc, "relations_graph");
    try std.testing.expectEqual(@as(u64, 0), graph_applied);
    try std.testing.expect(appended_sequence > graph_applied);
}

test "db graph runtime helpers expose edges neighbors and shortest path" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "citations",
        .kind = .graph,
        .config_json = "{}",
    });
    try db.addIndex(.{
        .name = "citations_alg",
        .kind = .graph,
        .config_json = "{\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "citations", .source = "a", .target = "b", .edge_type = "cites", .weight = 1.0 },
            .{ .index_name = "citations", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations", .source = "b", .target = "d", .edge_type = "cites", .weight = 3.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "b", .edge_type = "cites", .weight = 1.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "b", .target = "d", .edge_type = "cites", .weight = 3.0 },
        },
        .sync_level = .full_index,
    });

    const edges = try db.getEdges(alloc, "citations", "a", "", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 2), edges.len);

    const neighbors = try db.getNeighbors(alloc, "citations", "a", "cites", .out);
    defer traversal_mod.freeOwnedResults(alloc, neighbors);
    try std.testing.expectEqual(@as(usize, 2), neighbors.len);

    const traversed = try db.traverseEdges(alloc, "citations", "a", .{
        .direction = .out,
        .edge_types = &.{"cites"},
        .max_depth = 2,
    });
    defer traversal_mod.freeOwnedResults(alloc, traversed);
    try std.testing.expectEqual(@as(usize, 3), traversed.len);

    const shortest = (try db.findShortestPath(alloc, "citations", "a", "d", &.{"cites"}, .out, .min_hops, 8, 0, 0)).?;
    defer paths_mod.freePath(alloc, shortest);
    try std.testing.expectEqual(@as(u32, 2), shortest.length);
    try std.testing.expectEqual(@as(usize, 3), shortest.nodes.len);
    try std.testing.expectEqualStrings("a", shortest.nodes[0]);
    try std.testing.expectEqualStrings("d", shortest.nodes[2]);

    const algebraic_shortest = (try db.findShortestPath(alloc, "citations_alg", "a", "d", &.{"cites"}, .out, .min_hops, 8, 0, 0)).?;
    defer paths_mod.freePath(alloc, algebraic_shortest);
    try std.testing.expectEqual(@as(u32, 2), algebraic_shortest.length);
    try std.testing.expectEqual(@as(usize, 3), algebraic_shortest.nodes.len);
    try std.testing.expectEqualStrings("a", algebraic_shortest.nodes[0]);
    try std.testing.expectEqualStrings("b", algebraic_shortest.nodes[1]);
    try std.testing.expectEqualStrings("d", algebraic_shortest.nodes[2]);
    try std.testing.expectEqual(@as(usize, 2), algebraic_shortest.edges.len);
    try std.testing.expectEqualStrings("cites", algebraic_shortest.edges[0].edge_type);

    const algebraic_k_one = try db.findKShortestPaths(alloc, "citations_alg", "a", "d", 1, &.{"cites"}, .out, .min_hops, 8, 0, 0);
    defer paths_mod.freePaths(alloc, algebraic_k_one);
    try std.testing.expectEqual(@as(usize, 1), algebraic_k_one.len);
    try std.testing.expectEqual(@as(u32, 2), algebraic_k_one[0].length);
    try std.testing.expectEqualStrings("d", algebraic_k_one[0].nodes[2]);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    var found_alg_stats = false;
    for (stats.indexes) |item| {
        if (!std.mem.eql(u8, item.name, "citations_alg")) continue;
        found_alg_stats = true;
        try std.testing.expect(item.algebraic_graph_traversal_attempt_count > 0);
        try std.testing.expect(item.algebraic_graph_traversal_proven_count > 0);
        try std.testing.expect(item.algebraic_graph_traversal_result_node_count > 0);
    }
    try std.testing.expect(found_alg_stats);
}

test "db graph runtime algebraic shortest path applies exact min-hop edge weight filters" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "citations_alg",
        .kind = .graph,
        .config_json = "{\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}}}",
    });

    try db.batch(.{
        .graph_writes = &.{
            .{ .index_name = "citations_alg", .source = "a", .target = "b", .edge_type = "cites", .weight = 0.5 },
            .{ .index_name = "citations_alg", .source = "b", .target = "d", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "a", .target = "c", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "c", .target = "e", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "e", .target = "d", .edge_type = "cites", .weight = 2.0 },
            .{ .index_name = "citations_alg", .source = "c", .target = "d", .edge_type = "cites", .weight = 5.0 },
        },
        .sync_level = .full_index,
    });

    const shortest = (try db.findShortestPath(alloc, "citations_alg", "a", "d", &.{"cites"}, .out, .min_hops, 4, 1.0, 3.0)).?;
    defer paths_mod.freePath(alloc, shortest);
    try std.testing.expectEqual(@as(u32, 3), shortest.length);
    try std.testing.expectEqual(@as(usize, 4), shortest.nodes.len);
    try std.testing.expectEqualStrings("a", shortest.nodes[0]);
    try std.testing.expectEqualStrings("c", shortest.nodes[1]);
    try std.testing.expectEqualStrings("e", shortest.nodes[2]);
    try std.testing.expectEqualStrings("d", shortest.nodes[3]);

    const algebraic_k_one = try db.findKShortestPaths(alloc, "citations_alg", "a", "d", 1, &.{"cites"}, .out, .min_hops, 4, 1.0, 3.0);
    defer paths_mod.freePaths(alloc, algebraic_k_one);
    try std.testing.expectEqual(@as(usize, 1), algebraic_k_one.len);
    try std.testing.expectEqual(@as(u32, 3), algebraic_k_one[0].length);
    try std.testing.expectEqualStrings("c", algebraic_k_one[0].nodes[1]);
    try std.testing.expectEqualStrings("d", algebraic_k_one[0].nodes[3]);
}
