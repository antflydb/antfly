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

//! Named enrichments of a serverless table (#1791). They are declared as on a
//! distributed table: the reserved top-level `enrichments` array of the
//! table's `indexes_json`, holding public enrichment configs. The array
//! travels with the table definition through the catalog, the WAL catalog
//! updates and the manifest unchanged, so no persisted format changes.
//! Index-walking code skips it (`isReservedEntry`).
const std = @import("std");
const Allocator = std.mem.Allocator;
const local = @import("antfly_local_sources");

pub const EnrichmentConfig = local.storage_db_types.EnrichmentConfig;

/// The reserved `indexes_json` member that holds the declarations.
pub const reserved_key = "enrichments";

/// Entries of `indexes_json` that are not index declarations.
pub fn isReservedEntry(name: []const u8) bool {
    return local.api_index_metadata.isReservedEntry(name);
}

/// An asset with no producer and no input is maintained outside Antfly:
/// only seeds write it (the public-config form of
/// `enrichment_catalog.externallyMaintained`).
pub fn externallyMaintained(cfg: EnrichmentConfig) bool {
    return cfg.kind == .asset and cfg.producer_json.len == 0 and cfg.field.len == 0 and cfg.template.len == 0 and
        cfg.source_artifact_name.len == 0 and cfg.prior_context == null and cfg.neighbor_context == null and cfg.context_sources_json.len == 0;
}

pub const Declarations = struct {
    configs: []EnrichmentConfig = &.{},

    pub fn deinit(self: *Declarations, alloc: Allocator) void {
        for (self.configs) |*cfg| cfg.deinit(alloc);
        if (self.configs.len != 0) alloc.free(self.configs);
        self.* = undefined;
    }

    pub fn find(self: Declarations, name: []const u8) ?*const EnrichmentConfig {
        for (self.configs) |*cfg| if (std.mem.eql(u8, cfg.name, name)) return cfg;
        return null;
    }
};

/// The table's named enrichments, validated. An absent or empty array is no
/// declarations.
pub fn parseAlloc(alloc: Allocator, indexes_json: []const u8) !Declarations {
    if (indexes_json.len == 0) return .{};
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{}) catch return error.InvalidEnrichmentConfig;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidEnrichmentConfig;
    const declared = parsed.value.object.get(reserved_key) orelse return .{};
    if (declared == .null) return .{};
    if (declared != .array) return error.InvalidEnrichmentConfig;
    if (declared.array.items.len == 0) return .{};
    // Parse only the reserved array: index objects (embedding indexes with
    // an embedder) drive the built-in pipeline, not these. This needs no
    // embedder configuration, so the hosted runtime parses it too.
    var out = std.ArrayListUnmanaged(EnrichmentConfig).empty;
    errdefer {
        for (out.items) |*cfg| cfg.deinit(alloc);
        out.deinit(alloc);
    }
    try local.api_local_indexes.collectArtifactEnrichmentArray(alloc, declared, null, &out);
    try local.api_local_indexes.validateArtifactEnrichmentConfigs(alloc, out.items);
    for (out.items) |cfg| try requireServerlessSupported(cfg);
    return .{ .configs = try out.toOwnedSlice(alloc) };
}

/// Serverless has no grouped sources, committed graph adjacency or
/// cross-document context: producers needing them are refused at
/// declaration rather than failing at generation time.
fn requireServerlessSupported(cfg: EnrichmentConfig) !void {
    if (cfg.kind == .group or cfg.prior_context != null) return error.UnsupportedServerlessEnrichment;
    if (cfg.neighbor_context != null or cfg.context_sources_json.len != 0) return error.UnsupportedServerlessEnrichment;
    if (std.mem.startsWith(u8, cfg.name, "_")) return error.InvalidEnrichmentConfig;
}

test "serverless named enrichments parse the reserved array only" {
    const alloc = std.testing.allocator;
    var none = try parseAlloc(alloc, "{\"body\":{\"type\":\"full_text\"}}");
    defer none.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), none.configs.len);

    var declared = try parseAlloc(alloc,
        \\{"vectors":{"type":"embeddings","embedder":{"provider":"antfly","model":"m"}},
        \\ "enrichments":[
        \\   {"name":"summary_v1","kind":"asset","field":"body","content_type":"text/plain","producer":{"type":"generator","config":{"provider":"antfly","model":"g"}}},
        \\   {"name":"notes","kind":"asset","content_type":"text/plain"}
        \\ ]}
    );
    defer declared.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), declared.configs.len);
    try std.testing.expect(declared.find("summary_v1") != null);
    try std.testing.expect(declared.find("vectors") == null);

    for ([_][]const u8{
        "{\"enrichments\":{}}",
        "{\"enrichments\":[{\"name\":\"x\",\"kind\":\"asset\",\"field\":\"body\",\"neighbor_context\":{\"graph_index\":\"g\"},\"producer\":{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"g\"}}}]}",
    }) |bad| {
        if (parseAlloc(alloc, bad)) |ok| {
            var owned = ok;
            owned.deinit(alloc);
            return error.TestUnexpectedResult;
        } else |_| {}
    }
}
